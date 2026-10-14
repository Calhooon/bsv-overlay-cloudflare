//! Authenticated, read-only evidence for one transaction's proof ladder.

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

pub(crate) async fn handle(env: &Env, req: &Request) -> worker::Result<Response> {
    let values: Vec<String> = req
        .url()?
        .query_pairs()
        .filter(|(k, _)| k == "txid")
        .map(|(_, v)| v.into_owned())
        .collect();
    let txid = match values.as_slice() {
        [value] if valid_txid(value) => value.to_ascii_lowercase(),
        _ => return Response::error("one 64-character hex txid is required", 400),
    };
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
