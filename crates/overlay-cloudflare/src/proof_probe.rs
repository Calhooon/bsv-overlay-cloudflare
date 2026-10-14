//! Authenticated proof evidence; explicit readmit resets a fault record.

use serde::Deserialize;
use serde_json::{json, Value};
use worker::{Env, Request, Response};

pub(crate) const STORED_PROOF_SQL: &str =
    "SELECT hex(beef) AS beef FROM transactions WHERE txid = ?1";

pub(crate) fn valid_txid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(crate) fn stored_proof(beef: Option<&[u8]>, txid: &str) -> Value {
    match beef {
        None => json!({"present": false, "carriesBump": false}),
        Some(bytes) => match overlay_engine::beef_limits::own_proof(bytes, txid) {
            Ok(proof) => json!({"present": true, "carriesBump": proof.is_some()}),
            Err(_) => json!({"present": true, "carriesBump": null, "error": "invalid-beef"}),
        },
    }
}

/// The most ancestors one probe walk asks the courier for (engine 0.1.9):
/// the courier budget of the walk, and the bound on its subrequests with
/// one node request per configured peer (four) inside a Worker's limits.
pub(crate) const PROBE_WALK_MAX_ANCESTORS: usize = 24;

/// How deep the probe walks an unproven ancestry: the root's inputs are
/// depth 1. Deeper than three, a production walk's fault is read off the
/// steps the probe already shows.
pub(crate) const PROBE_WALK_MAX_DEPTH: u32 = 3;

/// The deadline of each request of the walk, as the rungs' probe above.
pub(crate) const PROBE_WALK_STEP_MS: u64 = 5_000;

/// The outpoints a transaction spends, from its raw hex (`None`: it does
/// not parse).
fn spent_outpoints(raw_hex: &str) -> Option<Vec<(String, u32)>> {
    let tx = bsv_rs::transaction::Transaction::from_hex(raw_hex.trim()).ok()?;
    Some(
        tx.inputs
            .iter()
            .map(|input| {
                (
                    input.get_source_txid().unwrap_or_default(),
                    input.source_output_index,
                )
            })
            .filter(|(txid, _)| !txid.is_empty())
            .collect(),
    )
}

/// THE probe's walk (engine 0.1.9, `GET /admin/proof-probe?txid=&walk=1`):
/// what a GASP walk of the outpoint `txid.vout` would be served, step by
/// step, WITHOUT writing anything. Each configured peer is asked for the
/// root's node (its presence, its proof, the inputs it names); of the nodes
/// served, the earliest proven one is kept, else the earliest; an unproven
/// node's inputs are asked of the courier, level by level, as the sync's
/// walk asks them when a courier is installed (it never asks the peer for
/// ancestors then), down to [`PROBE_WALK_MAX_DEPTH`] and at most
/// [`PROBE_WALK_MAX_ANCESTORS`] asks. A proven node ends its branch. A
/// READING of the walk, not the walk: the topic manager's named inputs of a
/// proven node are not followed, and nothing is appended or verified.
pub(crate) async fn probe_walk(
    peers: &[(String, Box<dyn overlay_engine::gasp::GASPRemote>)],
    courier: &dyn overlay_engine::gasp::AncestorFetcher,
    txid: &str,
    vout: u32,
    deadline: &dyn Fn() -> overlay_engine::engine::SleepFuture,
) -> Value {
    use overlay_engine::gasp::race_or_deadline;
    let outpoint = format!("{txid}.{vout}");
    let mut steps = Vec::new();
    let mut served: Option<overlay_engine::types::GASPNode> = None;
    for (peer, remote) in peers {
        let answer =
            race_or_deadline(remote.request_node(&outpoint, txid, vout, true), deadline()).await;
        let step = match answer {
            None => json!({"step": "root", "peer": peer, "present": false, "error": "deadline"}),
            Some(Err(e)) => json!({
                "step": "root", "peer": peer, "present": false,
                "notHeld": matches!(e, overlay_engine::gasp::GASPError::NodeNotFound(_)),
                "error": e.to_string().chars().take(200).collect::<String>(),
            }),
            Some(Ok(node)) => {
                let step = json!({
                    "step": "root", "peer": peer, "present": true,
                    "rawTxBytes": node.raw_tx.len() / 2,
                    "proof": node.proof.is_some(),
                    "inputs": node.inputs.as_ref().map(|i| {
                        let mut named: Vec<&String> = i.keys().collect();
                        named.sort();
                        named
                    }),
                });
                if served
                    .as_ref()
                    .is_none_or(|s| s.proof.is_none() && node.proof.is_some())
                {
                    served = Some(node);
                }
                step
            }
        };
        steps.push(step);
    }
    let mut truncated = false;
    if let Some(root) = served.filter(|n| n.proof.is_none()) {
        let mut queue: std::collections::VecDeque<(String, u32, String)> =
            spent_outpoints(&root.raw_tx)
                .unwrap_or_default()
                .into_iter()
                .map(|(t, v)| (format!("{t}.{v}"), 1, txid.to_string()))
                .collect();
        let mut asked = std::collections::HashSet::new();
        while let Some((input, depth, spent_by)) = queue.pop_front() {
            let ancestor = input.split('.').next().unwrap_or_default().to_string();
            if !asked.insert(ancestor.clone()) {
                continue;
            }
            if asked.len() > PROBE_WALK_MAX_ANCESTORS {
                truncated = true;
                break;
            }
            let answer = race_or_deadline(courier.fetch_ancestor(&ancestor), deadline()).await;
            let step = match answer {
                None => {
                    json!({"step": "ancestor", "depth": depth, "input": input, "spentBy": spent_by, "courier": "deadline"})
                }
                Some(Err(e)) => json!({
                    "step": "ancestor", "depth": depth, "input": input, "spentBy": spent_by,
                    "courier": "error",
                    "error": e.to_string().chars().take(200).collect::<String>(),
                }),
                Some(Ok(found)) => {
                    if found.proof.is_none() {
                        if depth < PROBE_WALK_MAX_DEPTH {
                            for (t, v) in spent_outpoints(&found.raw_tx).unwrap_or_default() {
                                queue.push_back((format!("{t}.{v}"), depth + 1, ancestor.clone()));
                            }
                        } else {
                            truncated = true;
                        }
                    }
                    json!({
                        "step": "ancestor", "depth": depth, "input": input, "spentBy": spent_by,
                        "courier": "served",
                        "rawTxBytes": found.raw_tx.trim().len() / 2,
                        "proof": found.proof.is_some(),
                    })
                }
            };
            steps.push(step);
        }
    }
    json!({"outpoint": outpoint, "steps": steps, "truncated": truncated})
}

pub(crate) async fn handle(env: &Env, req: &Request) -> worker::Result<Response> {
    let url = req.url()?;
    let values: Vec<String> = url
        .query_pairs()
        .filter(|(k, _)| k == "txid")
        .map(|(_, v)| v.into_owned())
        .collect();
    let txid = match values.as_slice() {
        [value] if valid_txid(value) => value.to_ascii_lowercase(),
        _ => return Response::error("one 64-character hex txid is required", 400),
    };
    let param = |name: &str| {
        url.query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
    };
    if param("walk").as_deref() == Some("1") {
        let Ok(vout) = param("vout").unwrap_or_else(|| "0".into()).parse::<u32>() else {
            return Response::error("vout must be an output index", 400);
        };
        let topic = param("topic").unwrap_or_else(|| "tm_uhrp".into());
        if topic.len() > 64
            || !topic
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Response::error("topic must be a topic manager name", 400);
        }
        let cleared = if param("readmit").as_deref() == Some("1") {
            let db = env.d1("OVERLAY_DB")?;
            crate::gasp_deferred::readmit_fault(&db, &topic, &format!("{txid}.{vout}")).await?
        } else {
            0
        };
        // The configured peers only (the SHIP and SLAP trackers, which
        // carry the UHRP history): never a URL from the request.
        let peers: Vec<(String, Box<dyn overlay_engine::gasp::GASPRemote>)> =
            crate::ship_slap_bootstrap()
                .into_iter()
                .map(|peer| {
                    let remote: Box<dyn overlay_engine::gasp::GASPRemote> = Box::new(
                        crate::gasp_remote::WorkerGASPRemote::new(peer.as_str(), topic.as_str()),
                    );
                    (peer, remote)
                })
                .collect();
        // No fault store: the walk writes nothing (`courier_fetcher_unrecorded`).
        let courier =
            crate::courier_fetcher_unrecorded(env, crate::lookup_service_chain_tracker(env))
                .with_budget(PROBE_WALK_MAX_ANCESTORS as u32);
        let deadline = || -> overlay_engine::engine::SleepFuture {
            Box::pin(crate::broadcaster::sleep_ms(PROBE_WALK_STEP_MS))
        };
        let walk = probe_walk(&peers, &courier, &txid, vout, &deadline).await;
        return Response::from_json(
            &json!({"txid": txid, "topic": topic, "walk": walk, "faultRecordsCleared": cleared}),
        );
    }
    // This path bypasses migrations and engine construction. A missing table
    // is an explicit read error, never a reason to initialize storage here.
    #[derive(Deserialize)]
    struct Row {
        beef: String,
    }
    let stored = match env.d1("OVERLAY_DB") {
        Ok(db) => match crate::d1::Query::new(STORED_PROOF_SQL)
            .bind(txid.as_str())
            .fetch_optional::<Row>(&db)
            .await
        {
            Ok(None) => stored_proof(None, &txid),
            Ok(Some(row)) => match crate::d1::beef_of_hex_column(Some(row.beef)) {
                Some(bytes) => stored_proof(Some(&bytes), &txid),
                None => json!({"error": "invalid-storage-encoding"}),
            },
            Err(_) => json!({"error": "storage-read-failed"}),
        },
        Err(_) => json!({"error": "storage-unavailable"}),
    };
    let fetcher = crate::courier_fetcher(env, crate::lookup_service_chain_tracker(env));
    let mut rungs = Vec::new();
    for rung in crate::proof_fetcher::PROOF_RUNGS {
        let mut probe = crate::proof_fetcher::ProofProbe::new(rung);
        if overlay_engine::gasp::race_or_deadline(
            fetcher.diagnose_rung_into(&mut probe, &txid),
            crate::broadcaster::sleep_ms(5_000),
        )
        .await
        .is_none()
        {
            probe.parse_result = "deadline";
            probe.judge = "not-run";
            if let Some(request) = probe.requests.last_mut() {
                if request.parse_result == "pending" {
                    request.parse_result = "deadline";
                }
            }
        }
        rungs.push(serde_json::to_value(probe)?);
    }
    Response::from_json(&json!({"txid": txid, "stored": stored, "rungs": rungs}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proofs_probe_validates_identifier_and_reads_sqlite_without_writes() {
        assert!(valid_txid(&"ab".repeat(32)));
        assert!(!valid_txid("../query"));
        assert!(!valid_txid(&"gg".repeat(32)));
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE transactions (txid TEXT PRIMARY KEY, beef BLOB); INSERT INTO transactions VALUES ('sample', X'0102');").unwrap();
        db.execute_batch("PRAGMA query_only = ON;").unwrap();
        let bytes: String = db
            .query_row(STORED_PROOF_SQL, ["sample"], |r| r.get(0))
            .unwrap();
        assert_eq!(bytes, "0102");
        assert_eq!(stored_proof(None, "sample")["carriesBump"], false);
        assert_eq!(
            stored_proof(Some(&[1, 2]), "sample")["error"],
            "invalid-beef"
        );
    }

    /// A transaction spending `source_txid:vout` (one output of 1,000 sat).
    fn child_of(source_txid: &str, vout: u32) -> bsv_rs::transaction::Transaction {
        let mut prev = hex::decode(source_txid).unwrap();
        prev.reverse();
        let raw = format!(
            "0100000001{}{}00ffffffff01{}015100000000",
            hex::encode(prev),
            hex::encode(vout.to_le_bytes()),
            hex::encode(1000u64.to_le_bytes())
        );
        bsv_rs::transaction::Transaction::from_hex(&raw).unwrap()
    }

    /// A peer that holds one node, or none.
    struct OnePeer(Option<overlay_engine::types::GASPNode>);

    #[async_trait::async_trait(?Send)]
    impl overlay_engine::gasp::GASPRemote for OnePeer {
        async fn get_initial_response(
            &self,
            _: &overlay_engine::types::GASPInitialRequest,
        ) -> Result<overlay_engine::types::GASPInitialResponse, overlay_engine::gasp::GASPError>
        {
            unreachable!("the probe never pages")
        }
        async fn get_initial_reply(
            &self,
            _: &overlay_engine::types::GASPInitialResponse,
        ) -> Result<overlay_engine::types::GASPInitialReply, overlay_engine::gasp::GASPError>
        {
            unreachable!("the probe never pages")
        }
        async fn request_node(
            &self,
            graph_id: &str,
            _: &str,
            _: u32,
            _: bool,
        ) -> Result<overlay_engine::types::GASPNode, overlay_engine::gasp::GASPError> {
            let mut node = self
                .0
                .clone()
                .ok_or_else(|| overlay_engine::gasp::GASPError::NodeNotFound("HTTP 400".into()))?;
            node.graph_id = graph_id.to_string();
            Ok(node)
        }
        async fn submit_node(
            &self,
            _: &overlay_engine::types::GASPNode,
        ) -> Result<Option<overlay_engine::types::GASPNodeResponse>, overlay_engine::gasp::GASPError>
        {
            unreachable!("the probe never submits")
        }
    }

    /// A courier serving some ancestors (raw, proven or not), faulting on
    /// the rest; it records what it was asked.
    struct Courier {
        serves: std::collections::HashMap<String, (String, bool)>,
        asked: std::cell::RefCell<Vec<String>>,
    }

    #[async_trait::async_trait(?Send)]
    impl overlay_engine::gasp::AncestorFetcher for Courier {
        async fn fetch_ancestor(
            &self,
            txid: &str,
        ) -> Result<overlay_engine::gasp::FetchedAncestor, overlay_engine::gasp::GASPError>
        {
            self.asked.borrow_mut().push(txid.to_string());
            match self.serves.get(txid) {
                Some((raw_tx, proven)) => Ok(overlay_engine::gasp::FetchedAncestor {
                    raw_tx: raw_tx.clone(),
                    proof: proven.then(|| "00".to_string()),
                }),
                None => Err(overlay_engine::gasp::GASPError::RemoteError(
                    "no rung served the raw".into(),
                )),
            }
        }
    }

    /// Engine 0.1.9: the probe's walk of an unadmitted root. One peer does
    /// not hold it, the other serves it unproven; its parent is served by
    /// the courier unproven, and the grandparent by no rung (the fault the
    /// keeper could not read on production, fact 2). Each step says so,
    /// and the walk stops at the fault.
    #[tokio::test]
    async fn unanchorable_019_the_probe_walks_the_root_and_its_ancestry() {
        let grand = child_of(&"ab".repeat(32), 0);
        let parent = child_of(&grand.id(), 1);
        let root = child_of(&parent.id(), 0);
        let node = overlay_engine::types::GASPNode {
            graph_id: String::new(),
            raw_tx: root.to_hex(),
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        let peers: Vec<(String, Box<dyn overlay_engine::gasp::GASPRemote>)> = vec![
            ("https://a.example".into(), Box::new(OnePeer(None))),
            ("https://b.example".into(), Box::new(OnePeer(Some(node)))),
        ];
        let courier = Courier {
            serves: std::collections::HashMap::from([(parent.id(), (parent.to_hex(), false))]),
            asked: std::cell::RefCell::default(),
        };
        let deadline =
            || -> overlay_engine::engine::SleepFuture { Box::pin(std::future::pending()) };
        let walk = probe_walk(&peers, &courier, &root.id(), 0, &deadline).await;
        let raw_bytes = root.to_hex().len() / 2;
        assert_eq!(
            walk,
            json!({
                "outpoint": format!("{}.0", root.id()),
                "truncated": false,
                "steps": [
                    {"step": "root", "peer": "https://a.example", "present": false,
                     "notHeld": true, "error": "node not found: HTTP 400"},
                    {"step": "root", "peer": "https://b.example", "present": true,
                     "rawTxBytes": raw_bytes, "proof": false, "inputs": null},
                    {"step": "ancestor", "depth": 1, "input": format!("{}.0", parent.id()),
                     "spentBy": root.id(), "courier": "served",
                     "rawTxBytes": parent.to_hex().len() / 2, "proof": false},
                    {"step": "ancestor", "depth": 2, "input": format!("{}.1", grand.id()),
                     "spentBy": parent.id(), "courier": "error",
                     "error": "remote error: no rung served the raw"},
                ],
            })
        );
        assert_eq!(*courier.asked.borrow(), vec![parent.id(), grand.id()]);
    }

    /// Engine 0.1.9: a proven root needs no ancestry; the walk asks the
    /// courier nothing. And the walk's path in the route writes nothing:
    /// a courier with no fault store, the configured peers only.
    #[tokio::test]
    async fn unanchorable_019_the_probe_walk_stops_at_a_proof_and_writes_nothing() {
        let root = child_of(&"cd".repeat(32), 0);
        let node = overlay_engine::types::GASPNode {
            graph_id: String::new(),
            raw_tx: root.to_hex(),
            output_index: 0,
            proof: Some("00".into()),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        let peers: Vec<(String, Box<dyn overlay_engine::gasp::GASPRemote>)> =
            vec![("https://a.example".into(), Box::new(OnePeer(Some(node))))];
        let courier = Courier {
            serves: std::collections::HashMap::new(),
            asked: std::cell::RefCell::default(),
        };
        let deadline =
            || -> overlay_engine::engine::SleepFuture { Box::pin(std::future::pending()) };
        let walk = probe_walk(&peers, &courier, &root.id(), 0, &deadline).await;
        assert_eq!(walk["steps"].as_array().unwrap().len(), 1);
        assert_eq!(walk["steps"][0]["proof"], true);
        assert!(courier.asked.borrow().is_empty());
        let source = include_str!("proof_probe.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        let walk_arm = &source[source.find("param(\"walk\")").unwrap()..];
        assert!(walk_arm.contains("courier_fetcher_unrecorded"));
        assert!(walk_arm.contains("crate::ship_slap_bootstrap()"));
        for write in [
            "with_proof_fault_store",
            "bump_counter",
            "INSERT",
            "UPDATE",
            "DELETE",
        ] {
            assert!(!source.contains(write), "{write}");
        }
        let lib = include_str!("lib.rs");
        let unrecorded = &lib[lib
            .find("pub(crate) fn courier_fetcher_unrecorded")
            .unwrap()..];
        assert!(!unrecorded[..unrecorded.find("\n}\n").unwrap()].contains("with_proof_fault_store"));
    }

    #[test]
    fn proofs_probe_auth_precedes_storage_and_migrations() {
        let source = include_str!("lib.rs");
        let dispatch = source.split("async fn dispatch(").nth(1).unwrap();
        let probe = dispatch.find("proof_probe::handle").unwrap();
        assert!(dispatch[..probe].contains("check_admin_auth(&req, &env)"));
        assert!(probe < dispatch.find("ensure_overlay_migrations").unwrap());
        assert!(!include_str!("proof_probe.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap()
            .contains("bump_counter"));
    }
}
