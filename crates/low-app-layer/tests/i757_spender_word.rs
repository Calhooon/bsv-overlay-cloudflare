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
//! `stored::seed_hint`, `runtime::held_proof`'s reads (`HELD_PROOF_READS`, `proof_of_held_beef`), `runtime::event_hints`
//! given that answer as its bytes, `runtime::pushed_proof`, the tracker's checked proof, `stored::project`,
//! `stored::UPSERT`) over SQLite under the overlay's real migrations; the scheduled pass's `stored::DUE` and its
//! `tick` steps; an eviction and a readmission through the overlay's own statements (`admit_fast::move_sql`,
//! `restore_sql`, `release_pot_spends_sql`, `remark_pot_spend_sql`). The handler's D1 binding, `worker::Request`, the
//! tick's Arcade re-ask and the latch's own D1 insert are the parts not run; `i757_the_handler_runs_these_steps` pins
//! the source to them.
use bsv_overlay_cloudflare::admit_fast::{self, ColumnInfo};
use bsv_overlay_cloudflare::ops::SeenSource;
use bsv_overlay_cloudflare::tracker_events;
use bsv_rs::transaction::{MerklePath, Transaction};
use bsv_tracker::{Evidence, Header, HostAction, Input, Params, Proof};
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

/// The settle's retained bytes, as the pot lookup service keeps them (`pot_beefs`): the handler's `held_proof` finds
/// this row, so its `bytes` answer is true, and reads the subject's proof out of it when the BEEF carries one.
fn retain(db: &Connection, txid: &str, beef: &[u8]) {
    db.execute(
        "INSERT INTO pot_beefs (txid, beef) VALUES (?, ?)",
        params![txid, beef],
    )
    .unwrap();
}

/// `runtime::held_proof` over SQLite: its own reads in its order, its own parse of the first row found.
fn held_proof(db: &Connection, txid: &str) -> (bool, Option<Proof>) {
    for sql in runtime::HELD_PROOF_READS {
        let row: Option<Option<Vec<u8>>> =
            db.query_row(sql, [txid], |r| r.get(0)).optional().unwrap();
        if let Some(beef) = row {
            return (true, runtime::proof_of_held_beef(txid, beef));
        }
    }
    (false, None)
}

/// A settle-shaped transaction of its own txid, its BEEF raw only or carrying its own proof at `height`.
fn settle_tx(n: u32, height: Option<u32>) -> (String, Vec<u8>) {
    let mut tx = Transaction::from_hex(
        include_str!(
            "fixtures/176885049985a858244993ab5591bfeb61b9368ac12fc7f12540d0824e5f891e.hex"
        )
        .trim(),
    )
    .unwrap();
    tx.lock_time = n;
    tx.invalidate_caches();
    let id = tx.id();
    tx.merkle_path = height.map(|h| MerklePath::from_coinbase_txid(&id, h));
    (id, tx.to_beef(true).unwrap())
}

fn cols(db: &Connection, table: &str) -> Vec<ColumnInfo> {
    db.prepare(&format!("PRAGMA table_info({table})"))
        .unwrap()
        .query_map([], |r| {
            Ok(ColumnInfo {
                name: r.get(1)?,
                ty: r.get(2)?,
                notnull: r.get(3)?,
                dflt_value: r.get(4)?,
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
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

/// The `/internal/tx-word` handler's steps for one frame (`runtime::ingest` -> `event` -> `event_held`), and the
/// scheduled pass's (`runtime::tick` -> `event(.., "tick", ..)`): the frame's txid and kind, the live hold, the
/// authority gate, the stored row's seed, a tick's clock and spend-attempt inputs, `held_proof`'s answer (its bytes
/// flag as `event_hints`' `bytes`, as the handler passes `held || raw_bytes`; no legacy body on these paths, so
/// `raw_bytes` is false), a pushed proof then the held one checked against our header, the projection, the mined
/// marker and the guarded upsert. Not modelled: the Arcade GETs of `callback` and a tick's re-ask (`hook_may_reask`,
/// reached only when no proof checked), and a stranger tick's courier leg (a held subject is first party).
async fn tx_word(db: &Connection, hold_sql: &str, body: String, now: u64) -> Result<(), String> {
    let frame: Value = serde_json::from_str(&body).unwrap();
    let txid = frame["txid"].as_str().unwrap().to_ascii_lowercase();
    let kind = frame["kind"].as_str().unwrap_or("refresh").to_string();
    let live = held(db, hold_sql, &txid);
    let callback = frame.get("body").cloned();
    let row = runtime::event_for_subject(live, kind == "submit", || async {
        let old = read(db, &txid);
        let mut state = stored::seed_hint(old.as_ref(), &txid);
        let params = Params { age_threshold: 60 };
        let empty = HeaderSnapshot {
            tip: Ok(0),
            headers: Default::default(),
        };
        if kind == "tick" {
            state
                .step(&params, &empty, Input::Host(HostAction::Tick(now)))
                .unwrap();
            if matches!(
                state.word(),
                bsv_tracker::Word::Unknown | bsv_tracker::Word::Built
            ) {
                state
                    .step(&params, &empty, Input::Host(HostAction::SpendAttempt))
                    .unwrap();
            }
        }
        let (bytes, proof) = if kind == "proof" {
            (true, None)
        } else {
            held_proof(db, &txid)
        };
        runtime::event_hints(&mut state, &kind, bytes, callback.as_ref(), now);
        let pushed = runtime::pushed_proof(&txid, callback.as_ref());
        for proof in [pushed, proof].into_iter().flatten() {
            let headers = headers_for(&proof);
            if state
                .step(&params, &headers, Input::Evidence(Evidence::Proof(proof)))
                .is_ok()
            {
                break;
            }
        }
        let mut row = stored::project(old.as_ref(), &state, now, None);
        if row.word == "mined" {
            row.legacy_body = Some(json!({"trackerTerminal": false}).to_string());
        }
        Ok(row)
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

/// `ops::latch_network_seen`'s note, its body as shipped (pinned by `i757_the_handler_runs_these_steps`). The latch
/// itself cannot run here: it takes a `worker::D1Database` for its `network_seen` insert before the note.
fn latch_seen(txid: &str, at_ms: u64) {
    tracker_events::note(
        txid,
        "seen",
        Some(json!({"source": SeenSource::Arcade.wire(), "at": at_ms})),
    );
}

fn pot(n: u8) -> String {
    format!("{n:02x}").repeat(32)
}

/// (1) The latch-to-word bound: a settle (a pot's recorded spender, no `transactions` row, its bytes in `pot_beefs`)
/// stored `built` reads `seen` after ONE delivery of the latch's event, with the latch's broadcaster and time. RED on
/// main's predicate: the event is dropped and the word stays `built`. The M1 bar holds: a stranger's `seen` writes no
/// row; an evicted spender (its UNCONFIRMED pointer released by the overlay's own `release_pot_spends_sql`) and an
/// evicted pot (its row shadow-moved by `move_sql`) leave the hold, and their readmissions (`remark_pot_spend_sql`,
/// `restore_sql`) re-hold the spender.
#[tokio::test]
async fn i757_a_the_settles_seen_event_writes_seen_within_one_delivery() {
    for (hold, want) in [(stored::LIVE_HOLD, "seen"), (MAIN_LIVE_HOLD, "built")] {
        let db = db();
        let funding = pot(1);
        let (settle, beef) = settle_tx(1, None);
        pot_spent_by(&db, &funding, 0, &settle);
        retain(&db, &settle, &beef);
        assert!(held_proof(&db, &settle).0, "held bytes");
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
    // an evicted SPENDER: the eviction releases its unconfirmed pointer, its events are dropped, the readmission
    // re-marks it and its next event lands
    let pot_cols = cols(&db, "pot_records");
    let (funding, settle) = (pot(4), pot(5));
    db.execute(
        "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed) VALUES (?, 0, 1, ?, 0)",
        params![funding, settle],
    )
    .unwrap();
    seed_built(&db, &settle, 1_000);
    assert!(held(&db, stored::LIVE_HOLD, &settle));
    db.execute(&admit_fast::release_pot_spends_sql(&pot_cols), [&settle])
        .unwrap();
    assert!(
        !held(&db, stored::LIVE_HOLD, &settle),
        "an evicted spender is still held"
    );
    tracker_events::drain();
    latch_seen(&settle, 1_000_500);
    ship(&db, stored::LIVE_HOLD, 1_002).await;
    assert_eq!(
        read(&db, &settle).unwrap().word,
        "built",
        "an evicted spender's seen was written"
    );
    let (remark, with_at) = admit_fast::remark_pot_spend_sql(&pot_cols);
    assert!(with_at);
    db.execute(&remark, params![settle, 1_003, funding, 0])
        .unwrap();
    assert!(
        held(&db, stored::LIVE_HOLD, &settle),
        "a readmitted spender is not re-held"
    );
    latch_seen(&settle, 1_003_500);
    ship(&db, stored::LIVE_HOLD, 1_004).await;
    assert_eq!(read(&db, &settle).unwrap().word, "seen");
    // an evicted POT: its row shadow-moved out of `pot_records`, then restored byte for byte
    let (funding, settle) = (pot(10), pot(11));
    pot_spent_by(&db, &funding, 0, &settle);
    assert!(held(&db, stored::LIVE_HOLD, &settle));
    db.execute_batch(&admit_fast::create_shadow_sql("pot_records", &pot_cols))
        .unwrap();
    let (copy, delete) = admit_fast::move_sql("pot_records", "txid", &pot_cols);
    db.execute(&copy, params![1_005, "refused", funding])
        .unwrap();
    db.execute(&delete, [&funding]).unwrap();
    assert!(
        !held(&db, stored::LIVE_HOLD, &settle),
        "an evicted pot's spender is still held"
    );
    let (back, drop) = admit_fast::restore_sql("pot_records", "txid", &pot_cols);
    db.execute(&back, [&funding]).unwrap();
    db.execute(&drop, [&funding]).unwrap();
    assert!(
        held(&db, stored::LIVE_HOLD, &settle),
        "a readmitted pot's spender is not re-held"
    );
}

/// (2) After a MINED proof (the overlay's checked bump, `verify_bump_detailed`'s `proof` event) the settle's word is
/// a checked `mined` with its verify path; on main's predicate the event is dropped and the word stays where it was.
#[tokio::test]
async fn i757_b_the_settles_proof_event_writes_mined_with_its_verify_path() {
    for (hold, want) in [(stored::LIVE_HOLD, "mined"), (MAIN_LIVE_HOLD, "built")] {
        let db = db();
        let funding = pot(6);
        let (settle, beef) = settle_tx(2, None);
        pot_spent_by(&db, &funding, 0, &settle);
        retain(&db, &settle, &beef);
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

/// (5) The FIX-757 lens's LOW-4 gap converges at the block: a settle whose only `seen` reached the app layer BEFORE
/// the pot index recorded the spend (the S2 replay after a write fault; a readmitted spender's MINED callback) stays
/// `built`; once its retained BEEF carries its proof the scheduled pass (`stored::DUE`, then the handler's `tick`
/// steps and `held_proof`) writes a checked `mined`. On main's `DUE` the settle is never chosen.
#[tokio::test]
async fn i757_e_a_seen_dropped_before_the_mark_converges_through_due_and_the_held_proof() {
    const MAIN_DUE: &str = "SELECT w.txid FROM tx_words w WHERE w.word IN ('unknown','built','announced','seen') AND w.updated_at <= ?1 AND EXISTS (SELECT 1 FROM transactions t WHERE t.txid=w.txid) AND COALESCE(json_extract(w.legacy_body,'$.trackerTerminal'),0) = 0 ORDER BY w.updated_at,w.txid LIMIT 2";
    let db = db();
    let funding = pot(12);
    let (settle, unproven) = settle_tx(3, None);
    seed_built(&db, &settle, 1_000);
    retain(&db, &settle, &unproven);
    // the seen arrives before the mark: dropped
    tracker_events::drain();
    latch_seen(&settle, 1_000_500);
    ship(&db, stored::LIVE_HOLD, 1_001).await;
    assert_eq!(read(&db, &settle).unwrap().word, "built");
    // the mark lands, then the block: the retained BEEF is stitched with its proof
    pot_spent_by(&db, &funding, 0, &settle);
    let (_, proven) = settle_tx(3, Some(971_700));
    db.execute(
        "UPDATE pot_beefs SET beef = ? WHERE txid = ?",
        params![proven, settle],
    )
    .unwrap();
    let due = |sql: &str| -> Vec<String> {
        db.prepare(sql)
            .unwrap()
            .query_map([1_000 + 900_u64], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    assert!(due(MAIN_DUE).is_empty(), "main's DUE chose the settle");
    assert_eq!(due(stored::DUE), vec![settle.clone()]);
    for txid in due(stored::DUE) {
        tx_word(
            &db,
            stored::LIVE_HOLD,
            json!({"txid": txid, "kind": "tick"}).to_string(),
            1_900,
        )
        .await
        .unwrap();
    }
    let row = read(&db, &settle).unwrap();
    assert_eq!((row.word.as_str(), row.height), ("mined", Some(971_700.0)));
    assert_eq!(row.view()["verifyPath"]["height"], 971_700);
    // mined: the scheduled pass is done with it, and the sweep no longer spares it (MEDIUM-1)
    assert!(due(stored::DUE).is_empty());
    assert_eq!(db.execute(stored::SWEEP, [1_900 + 1]).unwrap(), 1);
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
    let reads = &runtime[runtime.find("pub async fn held_proof(").unwrap()..];
    let reads = &reads[..reads.find("\n}\n").unwrap()];
    assert!(reads.contains("for sql in HELD_PROOF_READS {"));
    assert!(reads.contains("proof_of_held_beef(txid, row.beef)"));
    let tick = &runtime[runtime.find("pub async fn tick(").unwrap()..];
    let tick = &tick[..tick.find("\n}\n").unwrap()];
    assert!(tick.contains(".prepare(stored::DUE)"));
    assert!(tick.contains("event(env, &row.txid, \"tick\", None, None)"));
    for call in [
        "held_proof(env, txid).await?",
        "Input::Host(HostAction::Tick(now))",
        "Input::Host(HostAction::SpendAttempt)",
        "for proof in [pushed, proof].into_iter().flatten() {",
        "stored::seed_hint(old.as_ref(), txid)",
        "event_hints(&mut state, kind, held || raw_bytes, callback, now);",
        "let pushed = pushed_proof(txid, callback);",
        "stored::project(old.as_ref(), &state, now, legacy)",
        "stored::write(env, &row)",
    ] {
        assert!(held.contains(call), "event_held lost {call}");
    }
    let ops = include_str!("../../overlay-cloudflare/src/ops.rs");
    let latch = &ops[ops.find("pub async fn latch_network_seen(").unwrap()..];
    assert!(latch[..latch.find("\n}\n").unwrap()]
        .contains("Some(serde_json::json!({\"source\": source.wire(), \"at\": at_ms})),"));
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
