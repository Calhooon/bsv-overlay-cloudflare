//! bsv-low #736 on the FLEET fixture (`support/d1_fleet.rs`, the #638 fleet the owed pins use): the owed judgement
//! reads the index's proven spend BEFORE the spender's tracker word. The rule (NS-3): the index's verified proof
//! decides claimable first; a checked mined word is also enough; a `stale` or `cannotCheck` word never vetoes the
//! index's proof; a courier's bare `spentConfirmed` is no proof.
//!
//! The whale's hops are hydrated through the REAL `hops_view_sql` over the shipped migrations, assembled by the real
//! `assemble_hops_view` and judged by the real `derive_owed_rows`. Twelve of them are re-pointed at a sweep that pays
//! the whale's home (an unfiled sweep the index holds the bytes of), and the index's proof is written by the overlay's
//! own confirm statement (`confirm_spend_cas_sql`) with NO height, the case the issue names (an unparseable bump).
//! Four more are unspent in the index and named spent and confirmed by the courier ladder alone.
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::confirm_spend_cas_sql;
use bsv_tracker::{Evidence, Header, Input, Params, Proof, State};
use low_app_layer::hops_view::{assemble_hops_view, hops_view_sql, HopsViewRow};
use low_app_layer::owed::{
    checked_tracker_mined, derive_owed_rows, HopChainWord, OwedFamily, OwedInputs, OwedRow,
    SpenderOutput,
};
use low_app_layer::tracker_host::{served, HeaderSnapshot};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};

const NOW: u64 = 1_790_000_000;
const MY_PKH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
/// The whale's hops whose sweep the index has proven (`spentConfirmed = 1`, no `spentHeight`).
const PROVEN: std::ops::Range<u32> = 0..12;
/// The whale's hops the index holds unspent, named spent and confirmed by the courier ladder alone.
const COURIER: std::ops::Range<u32> = 12..16;

fn hop(i: u32) -> String {
    format!("d1{i:062x}")
}
fn game(i: u32) -> String {
    format!("d0{i:062x}")
}
fn sweep(i: u32) -> String {
    format!("f7{i:062x}")
}
fn outpoint(i: u32) -> String {
    format!("{}:0", hop(i))
}

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
    for i in PROVEN {
        // the sweep is recorded as the hop's spender, unconfirmed ...
        db.execute(
            "UPDATE pot_records SET spent = 1, spendingTxid = ?1, spentConfirmed = 0, spentHeight = NULL \
             WHERE txid = ?2 AND outputIndex = 0",
            params![sweep(i), hop(i)],
        )
        .unwrap();
        // ... then the index proves it through the overlay's own confirm, with no height to store
        let hit: Option<String> = db
            .query_row(
                confirm_spend_cas_sql(),
                params![Option::<i64>::None, hop(i), 0i64, sweep(i)],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(hit.as_deref(), Some(hop(i).as_str()), "the confirm hits");
    }
    for i in COURIER {
        db.execute(
            "UPDATE pot_records SET spent = 0, spendingTxid = NULL, spentConfirmed = 0 \
             WHERE txid = ?1 AND outputIndex = 0",
            params![hop(i)],
        )
        .unwrap();
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

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

/// A CHECKED mined word of `id` (a merkle path against our header at its height), as the tracker serves it.
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

/// The three word shapes that are not a checked mined word: what this pass could not hold as checked.
#[derive(Clone, Copy, Debug)]
enum Unchecked {
    /// the recheck could not run this pass (`cannotCheck` beside the stored `mined`)
    CannotCheck,
    /// a chain word gone stale
    Stale,
    /// a stored `mined` row with no verify path (the #693 reading: not mined)
    MinedNoPath,
}

fn unchecked_word(id: &str, shape: Unchecked) -> Value {
    let mut w = mined_word(id);
    w["verifyPath"] = Value::Null;
    match shape {
        Unchecked::CannotCheck => w["cannotCheck"] = json!("header snapshot changed"),
        Unchecked::Stale => w["word"] = json!("stale"),
        Unchecked::MinedNoPath => {}
    }
    w
}

/// The owed rows of the whale's hops for `views`.
fn owed(
    pages: &[Vec<HopsViewRow>],
    db: &Connection,
    views: &BTreeMap<String, Value>,
) -> Vec<OwedRow> {
    let mut hops = Vec::new();
    for page in pages {
        let (entries, truncated) = assemble_hops_view(page.clone());
        assert!(!truncated, "a view page holds at most its own outpoints");
        hops.extend(entries);
    }
    // the spenders that ARE pots (the JOIN took the hop), as the route reads them from the index
    let pot_spenders: HashSet<String> = pages
        .iter()
        .flatten()
        .filter_map(|r| r.spending_txid.clone())
        .filter(|s| {
            db.query_row(
                "SELECT 1 FROM pot_records WHERE txid = ?1 AND lockKind = 'covenant'",
                params![s],
                |_| Ok(()),
            )
            .is_ok()
        })
        .collect();
    // each sweep's stored bytes pay the whale's committed home for the game
    let outs: HashMap<String, Vec<SpenderOutput>> = PROVEN
        .chain(COURIER)
        .map(|i| {
            (
                sweep(i),
                vec![SpenderOutput {
                    vout: 0,
                    pkh_hex: Some(MY_PKH.into()),
                    sats: 20_000,
                    spent: Some(false),
                    pot_lock: false,
                }],
            )
        })
        .collect();
    let pkhs: HashMap<String, String> = PROVEN
        .chain(COURIER)
        .map(|i| (game(i), MY_PKH.to_string()))
        .collect();
    // the courier ladder's answer for the hops the index holds unspent: spent by the sweep, confirmed
    let chain: HashMap<String, HopChainWord> = COURIER
        .map(|i| {
            (
                outpoint(i),
                HopChainWord {
                    looked: true,
                    spent: Some(true),
                    spending_txid: Some(sweep(i)),
                    spent_confirmed: Some(true),
                    stale: false,
                    age_ms: None,
                },
            )
        })
        .collect();
    let (none, refunds, sweeps, door, ins, homes) = (
        HashSet::new(),
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
    })
}

/// The row of one hop, its coin and spender words taken out (they name the word row, which is the variable here).
fn row_without_words(rows: &[OwedRow], i: u32) -> Value {
    let row = rows
        .iter()
        .find(|r| r.outpoint == outpoint(i))
        .unwrap_or_else(|| panic!("hop {i} has an owed row"));
    let mut v = serde_json::to_value(row).unwrap();
    let facts = v["facts"].as_object_mut().unwrap();
    facts.remove("coinWord");
    facts.remove("spenderWord");
    v
}

fn words_for(range: std::ops::Range<u32>, shape: Unchecked) -> BTreeMap<String, Value> {
    range
        .map(|i| (sweep(i), unchecked_word(&sweep(i), shape)))
        .collect()
}

/// Pins 1 to 3: the index has proven the sweep (no stored height); the sweep's word is not a checked mined word.
fn the_index_proof_is_read_before(shape: Unchecked) {
    let db = fixture();
    let pages = hop_rows(&db);
    // the index's own proof arrives through the real SQL: spent, the sweep named, confirmed, and no height stored
    for i in PROVEN {
        let r = pages
            .iter()
            .flatten()
            .find(|r| r.hop_txid == hop(i))
            .unwrap();
        assert_eq!(
            (r.spent, r.spending_txid.as_deref(), r.spent_confirmed),
            (Some(true), Some(sweep(i).as_str()), Some(true))
        );
        let height: Option<i64> = db
            .query_row(
                "SELECT spentHeight FROM pot_records WHERE txid = ?1 AND outputIndex = 0",
                params![hop(i)],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(height, None, "the latch holds no height");
    }
    let no_row = owed(&pages, &db, &BTreeMap::new());
    let with_word = owed(&pages, &db, &words_for(PROVEN, shape));
    for i in PROVEN {
        let baseline = row_without_words(&no_row, i);
        assert_eq!(baseline["family"], json!(OwedFamily::Payout));
        assert_eq!(
            baseline["facts"]["claimable"], true,
            "with no word row the index's proof makes the payout claimable"
        );
        assert_eq!(baseline["facts"]["sweepSource"], "index-bytes");
        assert_eq!(baseline["facts"]["confirmedSource"], "index");
        let judged = row_without_words(&with_word, i);
        assert_eq!(
            judged["facts"]["claimable"], baseline["facts"]["claimable"],
            "hop {i}: a {shape:?} word vetoed the index's proven spend: {judged}"
        );
        assert_eq!(
            judged, baseline,
            "hop {i}: the row under a {shape:?} word is the row with no word row"
        );
    }
}

#[test]
fn i736_pin1_a_cannot_check_word_never_vetoes_the_index_proven_spend() {
    the_index_proof_is_read_before(Unchecked::CannotCheck);
}

#[test]
fn i736_pin2_a_stale_word_never_vetoes_the_index_proven_spend() {
    the_index_proof_is_read_before(Unchecked::Stale);
}

#[test]
fn i736_pin3_a_mined_word_with_no_verify_path_never_vetoes_the_index_proven_spend() {
    // the #693 reading stays for the re-ask predicate: such a word is NOT a checked mined word
    let w = BTreeMap::from([(sweep(0), unchecked_word(&sweep(0), Unchecked::MinedNoPath))]);
    assert_eq!(checked_tracker_mined(Some(&w), &sweep(0)), Some(false));
    let checked = BTreeMap::from([(sweep(0), mined_word(&sweep(0)))]);
    assert_eq!(checked_tracker_mined(Some(&checked), &sweep(0)), Some(true));
    the_index_proof_is_read_before(Unchecked::MinedNoPath);
}

#[test]
fn i736_pin4_a_couriers_bare_confirmation_still_yields_to_the_word() {
    let db = fixture();
    let pages = hop_rows(&db);
    for i in COURIER {
        let r = pages
            .iter()
            .flatten()
            .find(|r| r.hop_txid == hop(i))
            .unwrap();
        assert_eq!(
            (r.spent, r.spent_confirmed),
            (Some(false), Some(false)),
            "the index holds no spend and no proof of hop {i}"
        );
    }
    let no_row = owed(&pages, &db, &BTreeMap::new());
    for shape in [
        Unchecked::CannotCheck,
        Unchecked::Stale,
        Unchecked::MinedNoPath,
    ] {
        let with_word = owed(&pages, &db, &words_for(COURIER, shape));
        for i in COURIER {
            // with no word row the courier's claim stands, as before #736
            let baseline = row_without_words(&no_row, i);
            assert_eq!(baseline["facts"]["claimable"], true);
            assert_eq!(baseline["facts"]["confirmedSource"], "chain-probe");
            // a word that is not a checked mined word still holds the courier's bare claim back
            let judged = row_without_words(&with_word, i);
            assert_eq!(
                judged["facts"]["claimable"], false,
                "hop {i}: a courier's bare spentConfirmed gained the index's standing under a {shape:?} word"
            );
            assert_eq!(judged["facts"]["chainWait"], "block");
        }
    }
    // and the index-proven hops in the same pass are judged by the index (the rule does not leak between sources)
    let mixed: BTreeMap<String, Value> = PROVEN
        .chain(COURIER)
        .map(|i| (sweep(i), unchecked_word(&sweep(i), Unchecked::CannotCheck)))
        .collect();
    let rows = owed(&pages, &db, &mixed);
    for i in PROVEN {
        assert_eq!(row_without_words(&rows, i)["facts"]["claimable"], true);
    }
    for i in COURIER {
        assert_eq!(row_without_words(&rows, i)["facts"]["claimable"], false);
    }
}

#[test]
fn i736_pin5_the_confirm_stores_the_height_it_has_and_keeps_it_on_a_heightless_reconfirm() {
    let db = fixture();
    let read = |i: u32| -> (i64, Option<i64>) {
        db.query_row(
            "SELECT spentConfirmed, spentHeight FROM pot_records WHERE txid = ?1 AND outputIndex = 0",
            params![hop(i)],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    };
    // the index proved the spend with no height (an unparseable bump): the latch is the proof, with no height
    assert_eq!(read(0), (1, None));
    // a proof whose bump parses stores its block height on the latch
    let cas = |i: u32, h: Option<i64>| -> bool {
        db.query_row(
            confirm_spend_cas_sql(),
            params![h, hop(i), 0i64, sweep(i)],
            |r| r.get::<_, String>(0),
        )
        .is_ok()
    };
    assert!(cas(0, Some(958_845)));
    assert_eq!(read(0), (1, Some(958_845)));
    // a later heightless re-confirm of the same spender keeps the stored height
    assert!(cas(0, None));
    assert_eq!(read(0), (1, Some(958_845)));
    // the view reads the latch as the index's proof whether or not it holds a height, and the row is the same
    let pages = hop_rows(&db);
    let r = pages
        .iter()
        .flatten()
        .find(|r| r.hop_txid == hop(0))
        .unwrap();
    assert_eq!(r.spent_confirmed, Some(true));
    let rows = owed(&pages, &db, &words_for(0..2, Unchecked::CannotCheck));
    assert_eq!(
        row_without_words(&rows, 0)["facts"]["claimable"],
        row_without_words(&rows, 1)["facts"]["claimable"],
        "a latch with a height and one without are the same proof"
    );
    assert_eq!(row_without_words(&rows, 0)["facts"]["claimable"], true);
}
