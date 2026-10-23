//! bsv-low #618 (LOW words map L2) on the FLEET fixture (`support/d1_fleet.rs`, the budget tier's #638 fleet): the
//! whale's 554 hops, every one spent by a pot, hydrated through the REAL `hops_view_sql` over the shipped migrations,
//! assembled by the real `assemble_hops_view` and judged by the real `derive_owed_rows`, before (the `network_seen`
//! latch's pair as hydrated) and after (`owed::apply_word_arm` over the coin words). The fleet carries no spender
//! latch of its own (its 27,000 latches are keyed apart), so the pin seeds the third arm's four shapes over the whale's
//! spenders: latch and final bytes, latch and non-final, final with no latch, neither. Every row whose word AGREES
//! with its arm is byte-identical; the rows a disagreeing word changes are exactly the named classes.
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_tracker::{Evidence, Header, Hint, HintStatus, HostAction, Input, Params, Proof, State};
use low_app_layer::hops_view::{assemble_hops_view, hops_view_sql, HopsViewRow};
use low_app_layer::owed::{apply_word_arm, derive_owed_rows, OwedFamily, OwedInputs, OwedRow};
use low_app_layer::tracker_host::{served, HeaderSnapshot};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};

const NOW: u64 = 1_790_000_000;

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(NOW) {
        d1_fleet::execute_row(&db, &sql);
    }
    // the third arm's four shapes over the whale's hop spenders (hop i < 554 is the whale's; its spender is a pot)
    for i in 0..554u32 {
        let target = if (500..554).contains(&i) {
            14_000 + i
        } else {
            i
        };
        let (hop, spender) = (format!("d1{i:062x}"), format!("d0{target:062x}"));
        let (latch, fin) = match i % 4 {
            0 => (true, true),
            1 => (true, false),
            2 => (false, true),
            _ => (false, false),
        };
        if latch {
            db.execute(
                "INSERT INTO network_seen (txid, seenAt) VALUES (?1, ?2)",
                params![spender, NOW],
            )
            .unwrap();
        }
        db.execute(
            "UPDATE pot_records SET spenderFinal = ?1 WHERE txid = ?2 AND outputIndex = 0",
            params![i64::from(fin), hop],
        )
        .unwrap();
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

/// The whale's hop rows, page by page of the view (each page is assembled on its own, as the route assembles its
/// chunks).
fn hop_rows(db: &Connection) -> Vec<Vec<HopsViewRow>> {
    let mut pages: Vec<Vec<HopsViewRow>> = Vec::new();
    loop {
        let after = pages
            .iter()
            .flatten()
            .map(|r: &HopsViewRow| (r.hop_txid.clone(), r.hop_vout))
            .collect::<HashSet<_>>()
            .len();
        let mut stmt = db.prepare(&hops_view_sql(false, None, after)).unwrap();
        let page: Vec<HopsViewRow> = stmt
            .query_map(params![d1_fleet::WHALE], |r| {
                Ok(HopsViewRow {
                    identity: r.get("identity")?,
                    game_id: r.get("gameId")?,
                    hop_txid: r.get("hopTxid")?,
                    hop_vout: r.get::<_, i64>("hopVout")? as u32,
                    hop_sats: r.get::<_, i64>("hopSats")? as u64,
                    opponent_identity: r.get("opponentIdentity")?,
                    seat_settle_pubkey: r.get("seatSettlePubkey")?,
                    seat_sig_hex: r.get("seatSigHex")?,
                    identity_sig_hex: r.get("identitySigHex")?,
                    marker_txid: r.get("markerTxid")?,
                    marker_vout: r.get::<_, i64>("markerVout")? as u32,
                    spent: r.get::<_, Option<i64>>("spent")?.map(|v| v != 0),
                    spending_txid: r.get("spendingTxid")?,
                    spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.map(|v| v != 0),
                    spender_proof_verified: r
                        .get::<_, Option<i64>>("spenderProofVerified")?
                        .map(|v| v != 0),
                    spender_seen: r.get::<_, Option<i64>>("spenderSeen")?.map(|v| v != 0),
                    spender_final: r.get::<_, Option<i64>>("spenderFinal")?.map(|v| v != 0),
                    hop_lock_hex: r
                        .get::<_, Option<String>>("hopLockHex")?
                        .filter(|s| !s.is_empty()),
                    hop_sats_on_chain: r.get::<_, Option<i64>>("hopSatsOnChain")?.map(|v| v as u64),
                    container_outputs: r.get::<_, i64>("containerOutputs")? as u32,
                    marker_valid: r.get::<_, Option<i64>>("markerValid")?.map(|v| v != 0),
                    marker_created_at: r.get::<_, Option<i64>>("markerCreatedAt")?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        if page.is_empty() {
            return pages;
        }
        // the view reads one outpoint past the page to say "truncated"; the page is its first 100 outpoints
        let mut keep = Vec::new();
        for r in &page {
            let key = (r.hop_txid.clone(), r.hop_vout);
            if !keep.contains(&key)
                && keep.len() < low_app_layer::hops_view::HOPS_VIEW_MAX_OUTPOINTS
            {
                keep.push(key);
            }
        }
        pages.push(
            page.into_iter()
                .filter(|r| keep.contains(&(r.hop_txid.clone(), r.hop_vout)))
                .collect(),
        );
    }
}

fn seen_word(id: &str) -> Value {
    let headers = HeaderSnapshot {
        tip: Ok(0),
        headers: Default::default(),
    };
    let p = Params { age_threshold: 60 };
    let mut st = State::new(id);
    st.step(&p, &headers, Input::Host(HostAction::Build))
        .unwrap();
    st.step(
        &p,
        &headers,
        Input::Hint(Hint::new("arcade", HintStatus::Seen, 5)),
    )
    .unwrap();
    served::fields(&st)
}
fn announced_word(id: &str) -> Value {
    let headers = HeaderSnapshot {
        tip: Ok(0),
        headers: Default::default(),
    };
    let p = Params { age_threshold: 60 };
    let mut st = State::new(id);
    st.step(&p, &headers, Input::Host(HostAction::Build))
        .unwrap();
    st.step(
        &p,
        &headers,
        Input::Hint(Hint::new("arcade", HintStatus::Accepted, 5)),
    )
    .unwrap();
    served::fields(&st)
}
fn mined_word(id: &str) -> Value {
    let headers = HeaderSnapshot {
        tip: Ok(958_900),
        headers: BTreeMap::from([(
            958_900,
            Ok(Some(Header {
                hash: "77".repeat(32),
                merkle_root: id.to_string(),
            })),
        )]),
    };
    let mut st = State::new(id);
    st.step(
        &Params { age_threshold: 60 },
        &headers,
        Input::Evidence(Evidence::Proof(
            Proof::new(
                id,
                bsv_rs::transaction::MerklePath::from_coinbase_txid(id, 958_900),
            )
            .unwrap(),
        )),
    )
    .unwrap();
    served::fields(&st)
}

/// The owed rows of the whale's hops for `views`, the arm's pair from the latch (`l2 = false`) or the coin word.
fn owed(pages: &[Vec<HopsViewRow>], views: &BTreeMap<String, Value>, l2: bool) -> Vec<OwedRow> {
    let mut hops = Vec::new();
    for page in pages {
        let mut rows = page.clone();
        if l2 {
            apply_word_arm(&mut [], &mut [], &mut rows, views);
        }
        let (entries, truncated) = assemble_hops_view(rows);
        assert!(!truncated, "a view page holds at most its own outpoints");
        hops.extend(entries);
    }
    let pot_spenders: HashSet<String> = pages
        .iter()
        .flatten()
        .filter_map(|r| r.spending_txid.clone())
        .collect();
    let (none, refunds, sweeps, chain, door, outs, ins, pkhs, homes) = (
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    derive_owed_rows(&OwedInputs {
        spender_words: Some(views),
        identity_lc: d1_fleet::WHALE,
        tip: Some(958_900),
        now_ms: (NOW as i64 + 3_600) * 1000,
        results: &[],
        refunds: &[],
        hops: &hops,
        valid_refunds: &refunds,
        collected_verified: &none,
        collected_present: &none,
        held_verified: &none,
        pot_spenders: &pot_spenders,
        pot_spenders_faulted: false,
        hop_chain: &chain,
        evicted_hop_outpoints: &none,
        door_refused_hop_outpoints: &door,
        hop_sweeps: &sweeps,
        evicted_pots: &none,
        spender_outputs: &outs,
        spender_inputs: &ins,
        courier_spenders: &none,
        my_pkh_by_game: &pkhs,
        home_spends: &homes,
        retired_spenders: &none,
        proof_mined_spenders: &none,
        funding_unheld: &none,
        join_spent_hops: &Default::default(),
    })
}

#[test]
fn l2_fleet_owed_rows_are_byte_identical_where_the_word_agrees_and_change_only_by_name() {
    let db = fixture();
    let pages = hop_rows(&db);
    let rows: Vec<HopsViewRow> = pages.iter().flatten().cloned().collect();
    let outpoints: HashSet<_> = rows
        .iter()
        .map(|r| (r.hop_txid.clone(), r.hop_vout))
        .collect();
    assert_eq!(outpoints.len(), 554, "the whale's hops, every page");
    let shape = |r: &HopsViewRow| (r.spender_seen == Some(true), r.spender_final == Some(true));
    let spender = |r: &HopsViewRow| r.spending_txid.clone().unwrap();
    // the four seeded shapes all arrive through the real SQL
    let shapes: HashSet<_> = rows.iter().map(shape).collect();
    assert_eq!(shapes.len(), 4, "{shapes:?}");

    // AGREE: latch and final bytes → seen; latch and non-final → seen (withheld on both arms); final, no latch →
    // announced; neither → no word.
    let agree: BTreeMap<String, Value> = rows
        .iter()
        .filter_map(|r| match shape(r) {
            (true, _) => Some((spender(r), seen_word(&spender(r)))),
            (false, true) => Some((spender(r), announced_word(&spender(r)))),
            (false, false) => None,
        })
        .collect();
    let legacy = owed(&pages, &agree, false);
    let after = owed(&pages, &agree, true);
    assert_eq!(
        serde_json::to_string(&legacy).unwrap(),
        serde_json::to_string(&after).unwrap(),
        "every fleet row whose word agrees with its arm is byte-identical"
    );
    // the latched, final shape lands by the pot (no row), the rest are the recorded-not-confirmed sentence
    assert!(legacy.iter().all(|r| r.family == OwedFamily::Unbound));
    let unbound = legacy.len();
    let landed = rows
        .iter()
        .filter(|r| shape(r) == (true, true))
        .map(|r| (r.hop_txid.clone(), r.hop_vout))
        .collect::<HashSet<_>>()
        .len();
    assert_eq!(unbound + landed, 554);

    // DISAGREE: flip every word, then name exactly the rows that change.
    let flipped: BTreeMap<String, Value> = rows
        .iter()
        .filter_map(|r| match shape(r) {
            // class 1: the latch without its word (a dropped `seen` event) withholds the landing
            (true, true) => None,
            // class 2: a checked mined word lands though no latch was written
            (false, true) => Some((spender(r), mined_word(&spender(r)))),
            // a non-final latch read with an announced word: withheld on both arms
            (true, false) => Some((spender(r), announced_word(&spender(r)))),
            // class 3: a stale word (a chain word this pass cannot hold as checked) awaits the chain
            (false, false) => {
                let mut w = mined_word(&spender(r));
                w["word"] = json!("stale");
                w["verifyPath"] = Value::Null;
                Some((spender(r), w))
            }
        })
        .collect();
    let legacy = owed(&pages, &flipped, false);
    let after = owed(&pages, &flipped, true);
    let by_outpoint = |rows: &[OwedRow]| -> BTreeMap<String, String> {
        rows.iter()
            .map(|r| (r.outpoint.clone(), serde_json::to_string(r).unwrap()))
            .collect()
    };
    let (before, after) = (by_outpoint(&legacy), by_outpoint(&after));
    let shape_of: HashMap<String, (bool, bool)> = rows
        .iter()
        .map(|r| {
            (
                format!("{}:{}", r.hop_txid.to_ascii_lowercase(), r.hop_vout),
                shape(r),
            )
        })
        .collect();
    let mut changed = BTreeMap::<(bool, bool), usize>::new();
    for key in before
        .keys()
        .chain(after.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        if before.get(key) == after.get(key) {
            continue;
        }
        let s = shape_of[key];
        *changed.entry(s).or_default() += 1;
        match s {
            // class 1: no row (the JOIN took it) → the recorded-not-confirmed sentence
            (true, true) => {
                assert!(!before.contains_key(key), "{key}");
                assert!(
                    after[key].contains("the hop's spend is recorded but not confirmed"),
                    "{key}"
                );
            }
            // classes 2 and 3: the sentence → no row (the pot's own row tells the story)
            (false, _) => {
                assert!(
                    before[key].contains("the hop's spend is recorded but not confirmed"),
                    "{key}"
                );
                assert!(!after.contains_key(key), "{key}");
            }
            other => panic!("{key}: a {other:?} row changed (a non-final latch must judge alike)"),
        }
    }
    let count = |s: (bool, bool)| {
        rows.iter()
            .filter(|r| shape(r) == s)
            .map(|r| (r.hop_txid.clone(), r.hop_vout))
            .collect::<HashSet<_>>()
            .len()
    };
    assert_eq!(
        changed.get(&(true, true)).copied().unwrap_or(0),
        count((true, true))
    );
    assert_eq!(
        changed.get(&(false, true)).copied().unwrap_or(0),
        count((false, true))
    );
    assert_eq!(
        changed.get(&(false, false)).copied().unwrap_or(0),
        count((false, false))
    );
    assert_eq!(changed.get(&(true, false)), None);
    // no row is claimable on either arm: the change is the outcome's telling, never a press
    assert!(legacy
        .iter()
        .chain(owed(&pages, &flipped, true).iter())
        .all(|r| r.facts["claimable"] != true));
}
