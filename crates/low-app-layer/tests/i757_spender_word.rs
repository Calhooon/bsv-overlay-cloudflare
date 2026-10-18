//! bsv-low #757 (loop 37, RC-L37-OWED-WORD-BUILT): a pot SPENDER's tracker word (the settle, the refund) never left
//! `built` on beta. A settle admits no output, so the engine never writes it a `transactions` row (its bytes are
//! `pot_beefs`, `overlay-discovery` `pot::lookup_service`), and every word event's authority was a `transactions`
//! row alone: the overlay's `note_held` (`callback`, `accepted`) and the app layer's `live_hold` (`seen`, `proof`,
//! `callback`, the scheduled `DUE` refresh) dropped the settle's events, and only the gated `submit` event (which
//! needs no hold) wrote, `built`. The live hold is now one predicate (`stored::LIVE_HOLD`, the overlay's `tracker_events::LIVE_HOLD_SQL`): a `transactions` row OR the
//! pot index's own record of the subject as a live pot coin's spender (`pot_records.spendingTxid`).
//!
//! The pins run the overlay's real hook sender (`tracker_events::note`, `drain`, `deliver`, the frame
//! `ops::latch_network_seen` and `proof_fetcher::verify_bump_detailed` note) into the `/internal/tx-word` handler's
//! own steps (`runtime::ingest` -> `event` -> `event_held`: the shipped `stored::LIVE_HOLD`, equal to the overlay's `LIVE_HOLD_SQL`, `event_for_subject`,
//! `stored::seed_hint`, `runtime::event_hints`, `runtime::pushed_proof`, the tracker's checked proof,
//! `stored::project`, `stored::UPSERT`) over SQLite under the overlay's real migrations. The handler's D1 binding and
//! `worker::Request` are the only parts not run; `i757_the_handler_runs_these_steps` pins the source to them.
use bsv_overlay_cloudflare::tracker_events;
use bsv_rs::transaction::MerklePath;
use bsv_tracker::{Evidence, Header, Input, Params, Proof};
use low_app_layer::owed::{coin_landing, word_arm, CoinLanding};
use low_app_layer::tracker_host::{
    runtime,
    stored::{self, Row},
    HeaderSnapshot,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// The predicate on `e86c168` (main), verbatim: `transactions` alone.
const MAIN_LIVE_HOLD: &str = "SELECT 1 AS held FROM transactions WHERE txid = ? LIMIT 1";

fn db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{sql}: {e}");
        }
    }
    db.execute_batch(stored::CREATE).unwrap();
    db
}

fn held(db: &Connection, sql: &str, txid: &str) -> bool {
    db.query_row(sql, [txid], |r| r.get::<_, f64>(0))
        .optional()
        .unwrap()
        .is_some_and(|h| h == 1.0)
}

fn read(db: &Connection, txid: &str) -> Option<Row> {
    db.query_row(
        "SELECT txid,word,tier,height,proof_root,proof_height,proof_hex,header_hash,hint_source,hint_at,updated_at,legacy_body FROM tx_words WHERE txid = ?",
        [txid],
        |r| {
            Ok(Row {
                txid: r.get(0)?,
                word: r.get(1)?,
                tier: r.get(2)?,
                height: r.get(3)?,
                proof_root: r.get(4)?,
                proof_height: r.get(5)?,
                proof_hex: r.get(6)?,
                header_hash: r.get(7)?,
                hint_source: r.get(8)?,
                hint_at: r.get(9)?,
                updated_at: r.get(10)?,
                legacy_body: r.get(11)?,
            })
        },
    )
    .optional()
    .unwrap()
}

fn write(db: &Connection, row: &Row) {
    db.execute(
        stored::UPSERT,
        params![
            row.txid,
            row.word,
            row.tier,
            row.height,
            row.proof_root,
            row.proof_height,
            row.proof_hex,
            row.header_hash,
            row.hint_source,
            row.hint_at,
            row.updated_at,
            row.legacy_body
        ],
    )
    .unwrap();
}

/// The settle's word as beta stored it: the gated `submit` event's `built`, no hint.
fn seed_built(db: &Connection, txid: &str, at: u64) {
    let mut state = stored::seed_hint(None, txid);
    runtime::event_hints(&mut state, "submit", true, None, at);
    let row = stored::project(None, &state, at, None);
    assert_eq!(
        (
            row.word.as_str(),
            row.tier.as_str(),
            row.hint_source.as_deref()
        ),
        ("built", "host", None)
    );
    write(db, &row);
}

fn pot_spent_by(db: &Connection, pot: &str, vout: u32, spender: &str) {
    db.execute(
        "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed) VALUES (?, ?, 1, ?, 1)",
        params![pot, vout, spender],
    )
    .unwrap();
}

/// A checked proof's header: the block whose root the path computes, at its height.
fn headers_for(proof: &Proof) -> HeaderSnapshot {
    HeaderSnapshot {
        tip: Ok(proof.height() + 1),
        headers: BTreeMap::from([(
            proof.height(),
            Ok(Some(Header {
                hash: "ab".repeat(32),
                merkle_root: proof.root().unwrap(),
            })),
        )]),
    }
}

/// The `/internal/tx-word` handler's steps for one shipped frame (`runtime::ingest` -> `event` -> `event_held`):
/// the frame's txid and kind, the live hold, the authority gate, the stored row's seed, the event's hints, a pushed
/// proof checked against our header, the projection and the guarded upsert. `held_proof` (the subject's own
/// retained proof) and the Arcade GETs of `callback` / `tick` are not on the `seen`, `proof` or `submit` path's
/// decision and are not modelled.
async fn tx_word(db: &Connection, hold_sql: &str, body: String, now: u64) -> Result<(), String> {
    let frame: Value = serde_json::from_str(&body).unwrap();
    let txid = frame["txid"].as_str().unwrap().to_ascii_lowercase();
    let kind = frame["kind"].as_str().unwrap_or("refresh").to_string();
    let live = held(db, hold_sql, &txid);
    let callback = frame.get("body").cloned();
    let row = runtime::event_for_subject(live, kind == "submit", || async {
        let old = read(db, &txid);
        let mut state = stored::seed_hint(old.as_ref(), &txid);
        runtime::event_hints(&mut state, &kind, live, callback.as_ref(), now);
        if let Some(proof) = runtime::pushed_proof(&txid, callback.as_ref()) {
            let headers = headers_for(&proof);
            state
                .step(
                    &Params { age_threshold: 60 },
                    &headers,
                    Input::Evidence(Evidence::Proof(proof)),
                )
                .map_err(|e| worker::Error::RustError(e.to_string()))?;
        }
        Ok(stored::project(old.as_ref(), &state, now, None))
    })
    .await
    .map_err(|e| e.to_string())?;
    if let Some(row) = row {
        write(db, &row);
    }
    Ok(())
}

/// Ship what the overlay noted, through its real `drain` and `deliver`, into the handler.
async fn ship(db: &Connection, hold_sql: &str, now: u64) {
    for frame in tracker_events::drain() {
        tracker_events::deliver(frame, |body| tx_word(db, hold_sql, body, now)).await;
    }
}

fn latch_seen(txid: &str, at_ms: u64) {
    // `ops::latch_network_seen`'s note, as shipped
    tracker_events::note(txid, "seen", Some(json!({"source": "arcade", "at": at_ms})));
}

fn pot(n: u8) -> String {
    format!("{n:02x}").repeat(32)
}

/// (1) The latch-to-word bound: a settle (a pot's recorded spender, no `transactions` row) stored `built` reads
/// `seen` after ONE delivery of the latch's event, with the latch's broadcaster and time. RED on main's predicate:
/// the event is dropped and the word stays `built`. The M1 bar holds: a stranger's `seen` writes no row, and an
/// evicted pot's spender is no longer held.
#[tokio::test]
async fn i757_a_the_settles_seen_event_writes_seen_within_one_delivery() {
    for (hold, want) in [(stored::LIVE_HOLD, "seen"), (MAIN_LIVE_HOLD, "built")] {
        let db = db();
        let (funding, settle) = (pot(1), pot(2));
        pot_spent_by(&db, &funding, 0, &settle);
        seed_built(&db, &settle, 1_000);
        tracker_events::drain();
        latch_seen(&settle, 1_000_500);
        ship(&db, hold, 1_002).await;
        let row = read(&db, &settle).unwrap();
        assert_eq!(
            row.word, want,
            "the settle's word after one delivery ({hold})"
        );
        if want == "seen" {
            assert_eq!(row.hint_source.as_deref(), Some("arcade"));
            assert_eq!(row.hint_at, Some(1_000.0));
            assert_eq!(row.tier, "hint");
        }
    }
    // the authority bar (NS3D2-M1) is kept: a stranger's latch, and an evicted pot's spender, write nothing
    let db = db();
    let stranger = pot(3);
    tracker_events::drain();
    latch_seen(&stranger, 1_000_000);
    ship(&db, stored::LIVE_HOLD, 1_002).await;
    assert!(
        read(&db, &stranger).is_none(),
        "a stranger's seen planted a row"
    );
    let (funding, settle) = (pot(4), pot(5));
    pot_spent_by(&db, &funding, 0, &settle);
    assert!(held(&db, stored::LIVE_HOLD, &settle));
    db.execute("DELETE FROM pot_records WHERE txid = ?", [&funding])
        .unwrap();
    assert!(
        !held(&db, stored::LIVE_HOLD, &settle),
        "an evicted pot's spender is still held"
    );
}

/// (2) After a MINED proof (the overlay's checked bump, `verify_bump_detailed`'s `proof` event) the settle's word is
/// a checked `mined` with its verify path; on main's predicate the event is dropped and the word stays where it was.
#[tokio::test]
async fn i757_b_the_settles_proof_event_writes_mined_with_its_verify_path() {
    for (hold, want) in [(stored::LIVE_HOLD, "mined"), (MAIN_LIVE_HOLD, "built")] {
        let db = db();
        let (funding, settle) = (pot(6), pot(7));
        pot_spent_by(&db, &funding, 0, &settle);
        seed_built(&db, &settle, 1_000);
        tracker_events::drain();
        latch_seen(&settle, 1_000_500);
        let path = MerklePath::from_coinbase_txid(&settle, 971_643);
        tracker_events::note(&settle, "proof", Some(json!({"merklePath": path.to_hex()})));
        ship(&db, hold, 1_700).await;
        let row = read(&db, &settle).unwrap();
        assert_eq!(row.word, want, "the settle's word after its proof ({hold})");
        if want == "mined" {
            assert_eq!(row.tier, "chain");
            assert_eq!(row.height, Some(971_643.0));
            let view = row.view();
            assert!(
                !view["verifyPath"].is_null(),
                "mined without a verify path: {view}"
            );
            assert_eq!(view["verifyPath"]["height"], 971_643);
        }
    }
}

/// (3) The owed landing bar (#618's third arm, the coin word) on a settled pot through the spender path: the
/// settle's stored word, read as the owed path's view, withholds while `built` (beta's state), tells the outcome
/// once the latch's `seen` lands with final bytes, and lands on the checked `mined`.
#[tokio::test]
async fn i757_c_the_owed_bar_tells_the_outcome_on_the_settles_seen_word() {
    let db = db();
    let (funding, settle) = (pot(8), pot(9));
    pot_spent_by(&db, &funding, 0, &settle);
    seed_built(&db, &settle, 1_000);
    let words =
        |db: &Connection| BTreeMap::from([(settle.clone(), read(db, &settle).unwrap().view())]);
    let w = words(&db);
    assert_eq!(
        coin_landing(w.get(&settle), Some(true)),
        CoinLanding::Withheld
    );
    assert_eq!(
        word_arm(Some(&settle), Some(true), &w),
        (Some(false), Some(true))
    );
    tracker_events::drain();
    latch_seen(&settle, 1_000_500);
    ship(&db, stored::LIVE_HOLD, 1_002).await;
    let w = words(&db);
    assert_eq!(
        coin_landing(w.get(&settle), Some(true)),
        CoinLanding::Awaiting
    );
    assert_eq!(
        word_arm(Some(&settle), Some(true), &w),
        (Some(true), Some(true))
    );
    // a non-final seen still withholds (#323)
    assert_eq!(
        coin_landing(w.get(&settle), Some(false)),
        CoinLanding::Withheld
    );
    let path = MerklePath::from_coinbase_txid(&settle, 971_643);
    tracker_events::note(&settle, "proof", Some(json!({"merklePath": path.to_hex()})));
    ship(&db, stored::LIVE_HOLD, 1_700).await;
    let w = words(&db);
    assert_eq!(coin_landing(w.get(&settle), None), CoinLanding::Landed);
}

/// Loop 37's sample (beta, the overlay and app layer at `81a7ee3`, read 2026-10-18): ten settled pots whose settle
/// is a pure spender (no `transactions` row, funds no pot), the overlay's SEEN latched, the index's spend
/// SPV-confirmed, and the word `built` with no source. (pot, settle, seenAt, the word's updated_at)
const LOOP37: [(&str, &str, u64, u64); 10] = [
    (
        "1c8fd415b73fb374d61559f5cf77f39b76da27294c86ee0272b4b52e147aaa6c",
        "3c2ad7dea3bc9b6afe430e1fce930c02caaf124a16b54485598b6d64b204bec2",
        1792348992,
        1792348994,
    ),
    (
        "e916be9ec0d1a5103d3f26cdc78226a2d5f04590a1c03a25f829e9bc10b32119",
        "cd0976777f11353698444ac85f67687bbf3f91d755a89b6a73432cf90b38fbdb",
        1792352386,
        1792352387,
    ),
    (
        "c538f358bae0e35b4f6cea49e41762f1a3e921fa4591ba128da42e7dd70045f0",
        "54bfa3d815862883df21f003902df53eed054b1d68a3ff8d176f40ee4d398617",
        1792352210,
        1792352210,
    ),
    (
        "89305c0eb85eb33ab30b9824796d8a1caaaa44c5bc938695e095c45f6003ebf3",
        "9b551da64591372d00994938d9324f652b8843a2aafdb97418d2e85f0dbb34e2",
        1792352033,
        1792352033,
    ),
    (
        "ddf6706d3b3a2f365cd96d22537ac264e93203d1e30a4fb224f05c0ce1b1caab",
        "26f11c49bdfe2ff865583dbd8a275dabd263fd6476be90e15e0c728141273961",
        1792351686,
        1792351685,
    ),
    (
        "a385e90b969c3dba3bf8ee3e8d68f4e97764d6514df73062ae37eed575f6e59c",
        "bc177b327048e9ca4e34bd14cd2ec311cd5fdfe0ec8e6354e2042278a2190b23",
        1792351610,
        1792351610,
    ),
    (
        "d40b6e21799f905bdbbf8d5406b47d59a90412c407b44f0e958a1745be82ed57",
        "7898e1ac5435e1bd0b8ec3dbe700a5945ee2bc504add04a54bf1c933501ca733",
        1792351503,
        1792351499,
    ),
    (
        "3f145afac54dd297dde4626d75f0b5f20b80de54cb47b8e978baffeb905813fe",
        "7cf0b2bdb9ee8e0545cc35d340c19417f7d93ae5298a64cc0e06e1175f8a247a",
        1792351386,
        1792351385,
    ),
    (
        "295f88655a47f34cdffbbff11137c6a28c4c0d4963da1fc2f426ca9b139ec5f8",
        "265e5c56c8e45484b59cfe8c926b1f4a282a3ace4ba0dae6717cbe50227cfe1f",
        1792351290,
        1792351289,
    ),
    (
        "f23cc606ab7ea1da22ed55215887ca25b8d191afb06310a927a73061ff05cd9c",
        "b40a1751ee4ed2568286e818638173162240a02c277bd34e4e3049b1a9d4e2d9",
        1792351182,
        1792351184,
    ),
];

/// (4) The ten sampled pots' shape as a regression fixture: main's predicate holds none of the ten settles (the
/// cause), the live hold holds all ten; replaying each latch's `seen` event writes `seen` at the latch's time; an
/// aged row is due for the scheduled refresh and spared by the stranger sweep.
#[tokio::test]
async fn i757_d_loop37s_ten_settles_are_held_and_their_seen_lands() {
    let db = db();
    for (pot, settle, seen_at, updated) in LOOP37 {
        pot_spent_by(&db, pot, 0, settle);
        db.execute(
            "INSERT INTO network_seen (txid, seenAt) VALUES (?, ?)",
            params![settle, seen_at],
        )
        .unwrap();
        seed_built(&db, settle, updated);
    }
    let main_held = LOOP37
        .iter()
        .filter(|(_, s, _, _)| held(&db, MAIN_LIVE_HOLD, s))
        .count();
    assert_eq!(
        main_held, 0,
        "main's predicate held a settle: the fixture is not loop 37's shape"
    );
    for (_, settle, _, _) in LOOP37 {
        assert!(held(&db, stored::LIVE_HOLD, settle), "{settle} not held");
    }
    // aged: the scheduled pass refreshes them (two a pass) and the sweep spares them
    let due: Vec<String> = db
        .prepare(stored::DUE)
        .unwrap()
        .query_map([1_792_360_000_u64], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(due.len(), 2, "aged settles are not due: {due:?}");
    assert_eq!(
        db.execute(stored::SWEEP, [1_792_360_000_u64 + 8 * 86_400])
            .unwrap(),
        0
    );
    tracker_events::drain();
    for (_, settle, seen_at, _) in LOOP37 {
        latch_seen(settle, seen_at * 1000);
    }
    ship(&db, stored::LIVE_HOLD, 1_792_352_400).await;
    for (_, settle, seen_at, _) in LOOP37 {
        let row = read(&db, settle).unwrap();
        assert_eq!(row.word, "seen", "{settle}");
        assert_eq!(row.hint_at, Some(seen_at as f64), "{settle}");
    }
}

/// The handler's source runs the steps the model above composes: the live hold is the shared predicate, the event
/// hints and the pushed proof are the extracted functions, and the overlay's two route doors still read the hold.
#[test]
fn i757_the_handler_runs_these_steps() {
    let runtime = include_str!("../src/tracker_host/runtime.rs");
    let live = &runtime[runtime.find("pub async fn live_hold(").unwrap()..];
    let live = &live[..live.find("\n}\n").unwrap()];
    assert!(live.contains(".prepare(stored::LIVE_HOLD)"));
    assert_eq!(
        stored::LIVE_HOLD,
        tracker_events::LIVE_HOLD_SQL,
        "one live-hold predicate"
    );
    let held = &runtime[runtime.find("async fn event_held(").unwrap()..];
    let held = &held[..held.find("\npub async fn ingest(").unwrap()];
    for call in [
        "stored::seed_hint(old.as_ref(), txid)",
        "event_hints(&mut state, kind, held || raw_bytes, callback, now);",
        "let pushed = pushed_proof(txid, callback);",
        "stored::project(old.as_ref(), &state, now, legacy)",
        "stored::write(env, &row)",
    ] {
        assert!(held.contains(call), "event_held lost {call}");
    }
    let events = include_str!("../../overlay-cloudflare/src/tracker_events.rs");
    let note_held = &events[events.find("pub async fn note_held(").unwrap()..];
    assert!(note_held[..note_held.find("\n}\n").unwrap()].contains("Query::new(LIVE_HOLD_SQL)"));
    for sql in [stored::DUE, stored::SWEEP] {
        assert!(
            sql.contains("pot_records p WHERE p.spendingTxid=w.txid"),
            "{sql}"
        );
    }
}

/// The live hold is two index probes (D1 cost): the `transactions` primary key and `idx_pot_spending`, never a scan.
#[test]
fn i757_the_live_hold_is_two_index_probes() {
    let db = db();
    let plan: Vec<String> = db
        .prepare(&format!("EXPLAIN QUERY PLAN {}", stored::LIVE_HOLD))
        .unwrap()
        .query_map([pot(1)], |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        plan.iter().any(|p| p.contains("transactions USING")),
        "{plan:?}"
    );
    assert!(
        plan.iter().any(|p| p.contains("idx_pot_spending")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|p| p.starts_with("SCAN")), "{plan:?}");
}
