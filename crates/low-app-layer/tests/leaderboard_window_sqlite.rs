//! `/leaderboard` proofs — REAL SQLite, PRODUCTION schema, rows written by
//! the REAL producers (bsv-low #332).
//!
//! # Why this file exists
//!
//! Two lessons converge here.
//!
//! **#323 (the outage):** `recovery_view_sql()` shipped SYNTACTICALLY INVALID
//! and took its route down for every valid request, because every pin on it
//! asserted `sql.contains(…)` against a STRING. *A `contains` assertion on a
//! query must never be the only thing standing behind it.* So the two SQL
//! builders #332 introduces — [`leaderboard_markers_sql`] and
//! [`proof_pointers_sql`] — are EXECUTED here against the overlay's
//! production migration list, with markers inserted through the shapes the
//! overlay's own storage layer writes.
//!
//! **#332 (the defect):** `/leaderboard` counted a win on the PRESENCE of a
//! nullable `loserSigHex` column, so one dust `OP_RETURN` carrying junk bytes
//! plus a copied, public `(potTxid, settleTxid)` could inflate a record or
//! erase an honest win — and the flat `ORDER BY createdAt DESC LIMIT ?`
//! window it read through was itself a flood-to-evict primitive with no
//! incompleteness signal. The end-to-end cells below drive the WHOLE route
//! path — window SQL → the real `pot_records` anchor join → the aggregate —
//! because a rule proven only at the primitive is a proof about a primitive
//! (Rule 6b): the pure-logic attack cells live in `logic.rs`, and these
//! prove the shipped QUERY delivers the honest rows to them.
//!
//! Markers here are signed with REAL keys through the exported
//! `results::result_challenge_bytes` / `results::result_protocol` — the same
//! functions the verifier uses, never a copied format string (Rule 16: a
//! duplicated convention is a boundary with no pin).

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{mark_spent_sql, store_record_sql};
use bsv_rs::primitives::ec::PrivateKey;
use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, ProtoWallet};
use low_app_layer::logic::{
    aggregate_leaderboard, aggregate_leaderboard_attributed, assemble_statuses, leaderboard_body,
    leaderboard_markers_sql, leaderboard_pot_outpoints, leaderboard_unknown_pot_quota,
    leaderboard_window_cut, proof_pointers_sql, Leaderboard, OutpointStatus, PotRecordRow,
    ResultMarkerRow, LEADERBOARD_UNKNOWN_POT_MAX_AGE_SECS,
};
use low_app_layer::results::{
    attribute_seats, potparty_v2_challenge, seat_markers_sql, seatsig_preimage, CovenantParams,
    PotVerdict, SeatAttribution, SeatMarkerRow, LEADERBOARD_SEAT_CANDIDATES, SEAT_MARKERS_PER_KEY,
};
use rusqlite::{params, params_from_iter, Connection};

/// The chain world (verdict + verified attribution) the ROUTE derives for a
/// pot from `classify_spent_pots` + `seat_attributions`. Those pipelines have
/// their OWN real-producer cells (`results.rs` seat-marker roundtrip,
/// `classifier_real_txs.rs`); this harness proves the WINDOW SQL → anchor
/// join → aggregate path, so it supplies the world directly. `WinnerA` +
/// attribution naming `winner` in seat A.
fn win_world(
    entries: &[(&str, &str, &str)],
) -> (
    std::collections::HashMap<String, PotVerdict>,
    std::collections::HashMap<String, SeatAttribution>,
) {
    let mut v = std::collections::HashMap::new();
    let mut a = std::collections::HashMap::new();
    for (pot, winner, loser) in entries {
        v.insert(pot.to_ascii_lowercase(), PotVerdict::WinnerA);
        a.insert(
            pot.to_ascii_lowercase(),
            SeatAttribution {
                identity_a: Some(winner.to_ascii_lowercase()),
                identity_b: Some(loser.to_ascii_lowercase()),
            },
        );
    }
    (v, a)
}

fn agg_world(
    markers: &[ResultMarkerRow],
    statuses: &[OutpointStatus],
    world: &(
        std::collections::HashMap<String, PotVerdict>,
        std::collections::HashMap<String, SeatAttribution>,
    ),
) -> Leaderboard {
    // The identity path doesn't read params, so an empty params map suffices
    // for the identity-attributed harness cells (the settle-key FALLBACK is
    // exercised end-to-end by the potparty-eviction cell).
    aggregate_leaderboard_attributed(
        markers,
        statuses,
        &std::collections::HashMap::new(),
        200,
        &world.0,
        &world.1,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    )
}

/// A fresh in-memory SQLite carrying the REAL production schema (the same
/// tolerance discipline as the sibling suites: only the re-run additive-ALTER
/// error class the production runner tolerates).
fn production_schema_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            let msg = e.to_string().to_ascii_lowercase();
            assert!(
                msg.contains("duplicate column"),
                "production migration failed under real SQLite: {e}\n{sql}"
            );
        }
    }
    conn
}

fn h64(seed: u8) -> String {
    format!("{seed:02x}").repeat(32)
}

fn wallet(seed: u8) -> ProtoWallet {
    ProtoWallet::new(Some(PrivateKey::from_hex(&format!("{seed:064x}")).unwrap()))
}

fn identity(seed: u8) -> String {
    wallet(seed).identity_key_hex().to_ascii_lowercase()
}

/// Sign the canonical result challenge exactly as the client does — through
/// the crate's OWN exported protocol + challenge builders.
fn sign_result(seed: u8, game_lc: &str, challenge: &[u8]) -> String {
    let sig = wallet(seed)
        .create_signature(CreateSignatureArgs {
            data: Some(challenge.to_vec()),
            hash_to_directly_sign: None,
            protocol_id: low_app_layer::results::result_protocol(),
            key_id: game_lc.to_string(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap();
    hex::encode(sig.signature)
}

/// Admit a pot via the REAL `store_record_sql()` upsert (bare/legacy row —
/// the leaderboard join only reads the spend status + admission stamp).
fn admit_pot(conn: &Connection, txid: &str, created_at: i64) {
    conn.execute(
        store_record_sql(),
        params![
            txid,
            0i64,                   // outputIndex
            0i64,                   // spent
            Option::<String>::None, // spendingTxid
            0i64,                   // spentConfirmed
            created_at,             // createdAt
            Option::<String>::None, // lockKind
            Option::<String>::None, // pubA
            Option::<String>::None, // pubB
            Option::<String>::None, // pubTower
            Option::<String>::None, // payPkhA
            Option::<String>::None, // payPkhB
            Option::<String>::None, // rakePkh
            Option::<i64>::None,    // stakeA
            Option::<i64>::None,    // stakeB
            Option::<i64>::None,    // feeSats
            Option::<i64>::None,    // recoveryHeight
            Option::<i64>::None,    // potSats
            0i64,                   // paramsDecoded (NOT NULL)
        ],
    )
    .expect("store_record_sql");
}

/// Record a CONFIRMED spend through the REAL `mark_spent_sql()` writer
/// (#371 finality CASE binds ride every variant; NULL here — the
/// leaderboard never reads the third arm).
fn mark_spent_confirmed(conn: &Connection, txid: &str, spender: &str) {
    conn.execute(
        mark_spent_sql(true, false),
        params![
            spender,
            spender,
            900_000i64,
            900_000i64,
            spender,
            Option::<i64>::None,
            Option::<i64>::None,
            txid,
            0i64
        ],
    )
    .expect("mark_spent_sql");
}

/// File a `result_markers_v2` row through the overlay's OWN
/// `D1ResultStorage::store_record` shape (`INSERT OR IGNORE` on the marker
/// outpoint). `sigs` supplies the two pushes — real or junk.
#[allow(clippy::too_many_arguments)]
fn file_result(
    conn: &Connection,
    game: &str,
    winner: &str,
    loser: &str,
    pot: &str,
    settle: &str,
    winner_sig: &str,
    loser_sig: Option<&str>,
    marker_txid: &str,
    at: i64,
) {
    conn.execute(
        "INSERT OR IGNORE INTO result_markers_v2 \
         (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
          loserSigHex, cardsHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, 0, ?9)",
        params![
            game,
            winner,
            loser,
            pot,
            settle,
            winner_sig,
            loser_sig,
            marker_txid,
            at
        ],
    )
    .expect("insert result_markers_v2");
}

/// A REAL-SIGNED honest marker (winner seed 0x11, loser seed 0x22).
#[allow(clippy::too_many_arguments)]
fn file_honest_result(
    conn: &Connection,
    game: &str,
    winner_seed: u8,
    loser_seed: u8,
    pot: &str,
    settle: &str,
    countersigned: bool,
    marker_txid: &str,
    at: i64,
) {
    let (w, l) = (identity(winner_seed), identity(loser_seed));
    let challenge =
        low_app_layer::results::result_challenge_bytes(game, &w, &l, pot, settle, None).unwrap();
    let ws = sign_result(winner_seed, game, &challenge);
    let ls = countersigned.then(|| sign_result(loser_seed, game, &challenge));
    file_result(
        conn,
        game,
        &w,
        &l,
        pot,
        settle,
        &ws,
        ls.as_deref(),
        marker_txid,
        at,
    );
}

/// A plausibly-DER-shaped JUNK signature — the bytes the pre-#332 presence
/// gate accepted as a countersignature.
fn junk_sig() -> String {
    format!("3045{}", "ab".repeat(69))
}

/// EXECUTE the shipped window query, mapping rows exactly as the worker's
/// `ResultRowD1` does, and apply the route's own truncation cut.
fn query_window(conn: &Connection, limit: usize) -> (Vec<ResultMarkerRow>, bool) {
    let sql = leaderboard_markers_sql(None);
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("leaderboard_markers_sql did not PREPARE: {e}\n{sql}"));
    let per_pot = low_app_layer::logic::LEADERBOARD_RESULT_ROWS_PER_POT;
    let rows: Vec<(Option<String>, ResultMarkerRow)> = stmt
        .query_map(
            params![
                (limit + 1) as i64,
                leaderboard_unknown_pot_quota(limit) as i64,
                ((limit + 1) * per_pot) as i64,
            ],
            |r| {
                let pot: Option<String> = r.get("potTxid")?;
                Ok((
                    pot.clone(),
                    ResultMarkerRow {
                        game_id: r.get("gameId")?,
                        winner: r.get("winner")?,
                        loser: r.get("loser")?,
                        pot_txid: pot.unwrap_or_default(),
                        settle_txid: r
                            .get::<_, Option<String>>("settleTxid")?
                            .unwrap_or_default(),
                        winner_sig_hex: r
                            .get::<_, Option<String>>("winnerSigHex")?
                            .unwrap_or_default(),
                        loser_sig_hex: r.get("loserSigHex")?,
                        cards_hex: r.get("cardsHex")?,
                        txid: r.get("txid")?,
                        created_at: r.get("createdAt")?,
                        claim_valid: r.get("claimValid")?,
                    },
                ))
            },
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let keys: Vec<Option<String>> = rows.iter().map(|(k, _)| k.clone()).collect();
    let (cut, truncated) = leaderboard_window_cut(&keys, limit);
    (
        rows.into_iter().take(cut).map(|(_, m)| m).collect(),
        truncated,
    )
}

/// Build the `pot_records` spend-status join for a marker set through the
/// SHIPPED `batch_where_sql` (per-outpoint chunks of 1 — the route's step 2
/// chunking aside), so the #371 `network_seen` witness join is exercised by
/// the production string, not a hand-rolled twin (Rule 6b).
fn statuses_from_db(conn: &Connection, markers: &[ResultMarkerRow]) -> Vec<OutpointStatus> {
    let ops = leaderboard_pot_outpoints(markers);
    let mut rows: Vec<PotRecordRow> = Vec::new();
    for op in &ops {
        let mut stmt = conn
            .prepare(&low_app_layer::logic::batch_where_sql(1))
            .unwrap();
        let mut got = stmt
            .query_map(params![op.db_txid(), op.vout as i64], |r| {
                Ok(PotRecordRow {
                    txid: r.get("txid")?,
                    vout: r.get::<_, i64>("outputIndex")? as u32,
                    spent: r.get::<_, i64>("spent")? != 0,
                    spending_txid: r.get("spendingTxid")?,
                    spent_confirmed: r.get::<_, i64>("spentConfirmed")? != 0,
                    spender_final: r.get::<_, Option<i64>>("spenderFinal")?.map(|v| v != 0),
                    spender_seen: r.get::<_, Option<i64>>("spenderSeen")?.map(|v| v != 0),
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.append(&mut got);
    }
    assemble_statuses(&ops, &rows)
}

/// File a `proof_markers` pointer through the overlay's own writer shape.
fn file_proof(conn: &Connection, game: &str, winner: &str, marker_txid: &str, at: i64) {
    conn.execute(
        "INSERT OR IGNORE INTO proof_markers \
         (gameId, winner, sigHex, bundle, bundleB64, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, '3045ab', X'7B7D', 'e30=', ?3, 0, ?4)",
        params![game, winner, marker_txid, at],
    )
    .expect("insert proof_markers");
}

fn query_proof_pointers(
    conn: &Connection,
    pairs: &[(String, String)],
) -> std::collections::HashMap<(String, String), Vec<String>> {
    let sql = proof_pointers_sql(pairs.len());
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("proof_pointers_sql did not PREPARE: {e}\n{sql}"));
    let binds: Vec<String> = pairs
        .iter()
        .flat_map(|(g, w)| [g.clone(), w.clone()])
        .collect();
    let mut out: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    let rows = stmt
        .query_map(params_from_iter(binds), |r| {
            Ok((
                r.get::<_, String>("gameId")?,
                r.get::<_, String>("winner")?,
                r.get::<_, String>("txid")?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for (g, w, txid) in rows {
        out.entry((g.to_ascii_lowercase(), w.to_ascii_lowercase()))
            .or_default()
            .push(txid);
    }
    out
}

/// THE #323 REGRESSION BAR for the two new builders: they must PREPARE
/// against the production schema. (`sql_prepares_sqlite.rs` carries the
/// crate-wide backstop; this is the local one that fails first.)
#[test]
fn the_new_leaderboard_queries_prepare_against_the_production_schema() {
    let conn = production_schema_db();
    let sql = leaderboard_markers_sql(None);
    conn.prepare(&sql)
        .unwrap_or_else(|e| panic!("leaderboard_markers_sql is not valid SQL: {e}\n{sql}"));
    for n in [1usize, 2, 5, 45] {
        let sql = proof_pointers_sql(n);
        let stmt = conn
            .prepare(&sql)
            .unwrap_or_else(|e| panic!("proof_pointers_sql({n}) is not valid SQL: {e}\n{sql}"));
        assert_eq!(
            stmt.parameter_count(),
            n * 2,
            "proof_pointers_sql({n}) binds one (gameId, winner) pair per key"
        );
    }
}

/// #332 END TO END, attack (a) INFLATION — through the SHIPPED QUERY.
///
/// A real, confirmed `(potTxid, settleTxid)` is public. The attacker files
/// junk-sig markers copying it into invented gameIds. Pre-#332 each copy was
/// a "confirmed" single-winner group and minted a win. The window must
/// deliver the honest marker, and the aggregate must give the attacker zero.
#[test]
fn inflation_copies_reach_the_aggregate_and_mint_nothing() {
    let conn = production_schema_db();
    let pot = h64(0xaa);
    let settle = h64(0xbb);
    let honest_game = h64(0x01);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    file_honest_result(
        &conn,
        &honest_game,
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xf0),
        1_001,
    );
    let attacker = identity(0xcc);
    for i in 0..10u8 {
        file_result(
            &conn,
            &format!("{:064x}", 0x9000 + u32::from(i)),
            &attacker,
            &identity(0x11),
            &pot,
            &settle,
            &junk_sig(),
            Some(&junk_sig()),
            &format!("{:064x}", 0xe000 + u32::from(i)),
            2_000 + i64::from(i),
        );
    }

    let (markers, _) = query_window(&conn, 200);
    assert!(
        markers.iter().any(|m| m.game_id == honest_game),
        "the honest marker must survive the window (a flood must not evict it)"
    );
    let statuses = statuses_from_db(&conn, &markers);
    let world = win_world(&[(&pot, &identity(0x11), &identity(0x22))]);
    let lb = agg_world(&markers, &statuses, &world);
    let wins_of = |id: &str| {
        lb.board
            .iter()
            .find(|r| r.identity == *id)
            .map_or(0, |r| r.wins)
    };
    assert_eq!(
        wins_of(&attacker),
        0,
        "junk-sig copies of a public (pot, settle) mint no wins"
    );
    assert_eq!(
        wins_of(&identity(0x11)),
        1,
        "the honest attributed win scores exactly once"
    );
}

/// #332 END TO END, attack (b) ERASURE — through the SHIPPED QUERY. A junk-sig
/// marker naming the attacker as winner of the victim's real pot/settle
/// reaches the aggregate alongside the honest marker; the chain spine awards
/// the win to the attributed victim regardless.
#[test]
fn erasure_marker_reaches_the_aggregate_and_the_honest_win_survives() {
    let conn = production_schema_db();
    let pot = h64(0xac);
    let settle = h64(0xbd);
    let game = h64(0x02);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    file_honest_result(
        &conn,
        &game,
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xf1),
        1_001,
    );
    file_result(
        &conn,
        &game,
        &identity(0xcc),
        &identity(0x11),
        &pot,
        &settle,
        &junk_sig(),
        Some(&junk_sig()),
        &h64(0xf2),
        2_000,
    );

    let (markers, _) = query_window(&conn, 200);
    assert_eq!(markers.len(), 2, "both markers reach the aggregate");
    let statuses = statuses_from_db(&conn, &markers);
    let world = win_world(&[(&pot, &identity(0x11), &identity(0x22))]);
    let lb = agg_world(&markers, &statuses, &world);
    let honest = lb
        .board
        .iter()
        .find(|r| r.identity == identity(0x11))
        .expect("the honest win must still be on the board");
    assert_eq!(honest.wins, 1);
    assert!(honest.proven, "its VERIFIED-countersigned tier survives");
}

/// #371 owner ruling (2026-08-06) END TO END — "we shouldn't have to wait
/// for confirm to see it on the leaderboard": an UNCONFIRMED settle the
/// overlay ITSELF witnessed the network accept (`network_seen`) with FINAL
/// bytes (`spenderFinal = 1`) COUNTS the win, through the production
/// writers + the shipped `batch_where_sql` witness join. The two controls
/// prove the conjunction: the same shape with NO witness latch (the
/// ungated-/submit plant, epoch Rule 21) counts NOTHING, and witnessed but
/// NON-FINAL (a parked refund pointer) counts nothing.
#[test]
fn a_seen_and_final_unconfirmed_settle_counts_on_the_board() {
    let conn = production_schema_db();
    let fin = |c: &Connection, pot: &str, settle: &str, f: i64| {
        // The #371 unconfirmed mark_spent shape (no verdict): pointer +
        // finality CASE binds, through the production SQL.
        c.execute(
            mark_spent_sql(false, false),
            params![settle, settle, f, f, pot, 0i64],
        )
        .expect("mark_spent_sql");
    };

    // Seen + final, UNCONFIRMED: counts.
    let pot = h64(0xa1);
    let settle = h64(0xb1);
    let game = h64(0x03);
    admit_pot(&conn, &pot, 1_000);
    fin(&conn, &pot, &settle, 1);
    conn.execute(
        bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
        params![settle],
    )
    .expect("network_seen latch");
    file_honest_result(
        &conn,
        &game,
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xf5),
        1_001,
    );

    // Final but UNWITNESSED: must not count.
    let pot2 = h64(0xa2);
    let settle2 = h64(0xb2);
    let game2 = h64(0x04);
    admit_pot(&conn, &pot2, 1_000);
    fin(&conn, &pot2, &settle2, 1);
    file_honest_result(
        &conn,
        &game2,
        0x33,
        0x44,
        &pot2,
        &settle2,
        true,
        &h64(0xf6),
        1_002,
    );

    // Witnessed but NON-final: must not count.
    let pot3 = h64(0xa3);
    let settle3 = h64(0xb3);
    let game3 = h64(0x05);
    admit_pot(&conn, &pot3, 1_000);
    fin(&conn, &pot3, &settle3, 0);
    conn.execute(
        bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
        params![settle3],
    )
    .expect("network_seen latch");
    file_honest_result(
        &conn,
        &game3,
        0x55,
        0x66,
        &pot3,
        &settle3,
        true,
        &h64(0xf7),
        1_003,
    );

    let (markers, _) = query_window(&conn, 200);
    let statuses = statuses_from_db(&conn, &markers);
    let world = win_world(&[
        (&pot, &identity(0x11), &identity(0x22)),
        (&pot2, &identity(0x33), &identity(0x44)),
        (&pot3, &identity(0x55), &identity(0x66)),
    ]);
    let lb = agg_world(&markers, &statuses, &world);
    assert!(
        lb.board
            .iter()
            .any(|r| r.identity == identity(0x11) && r.wins == 1),
        "a SEEN + FINAL unconfirmed settle counts at the ruling-3 bar"
    );
    assert!(
        !lb.board.iter().any(|r| r.identity == identity(0x33)),
        "final bytes with NO overlay witness never count (Rule 21 plant)"
    );
    assert!(
        !lb.board.iter().any(|r| r.identity == identity(0x55)),
        "a witnessed NON-FINAL spender never counts (#323 verbatim)"
    );
}

/// CRITICAL-1 END TO END — the EARLIER-spam eviction that the interim design
/// could not survive. The opponent files the 4 OLDEST rows for the victim's
/// pot DURING the hand (junk sig, garbage settle); the honest winner's
/// countersigned marker, published later, is EVICTED from the per-pot window.
/// The window returns only junk, yet the chain spine (confirmed spend +
/// verdict + attribution) still awards the win to the victim.
#[test]
fn earlier_spam_evicts_the_honest_marker_but_the_chain_win_survives() {
    let conn = production_schema_db();
    let pot = h64(0xea);
    let settle = h64(0xeb);
    let victim = identity(0x11);
    let opponent = identity(0x22);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    // 4 attacker rows stamped EARLIER (garbage settle, junk sigs) — the
    // RESULT_ROWS_PER_POT oldest, so the honest marker is evicted.
    for i in 0..4u8 {
        file_result(
            &conn,
            &format!("{:064x}", 0xea00 + u32::from(i)),
            &opponent,
            &victim,
            &pot,
            &h64(0x99), // garbage settle
            &junk_sig(),
            Some(&junk_sig()),
            &format!("{:064x}", 0xee00 + u32::from(i)),
            10 + i64::from(i),
        );
    }
    // The honest countersigned marker, published LATER.
    file_honest_result(
        &conn,
        &h64(0x07),
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xef),
        9_000,
    );

    let (markers, _) = query_window(&conn, 200);
    assert!(
        markers.iter().all(|m| m.settle_txid == h64(0x99)),
        "the per-pot window kept the 4 EARLIER junk rows; the honest marker is evicted"
    );
    let statuses = statuses_from_db(&conn, &markers);
    let world = win_world(&[(&pot, &victim, &opponent)]);
    let lb = agg_world(&markers, &statuses, &world);
    let honest = lb
        .board
        .iter()
        .find(|r| r.identity == victim)
        .expect("the chain win survives eviction");
    assert_eq!(honest.wins, 1, "eviction costs cards, never the win");
    assert!(
        honest.evidence.is_empty(),
        "no honest marker survived ⇒ no evidence"
    );
    assert!(
        lb.board.iter().all(|r| r.identity != opponent),
        "the opponent's junk rows mint nothing"
    );
}

/// CRITICAL-2 END TO END — evict-then-claim on an UNATTRIBUTED pot credits
/// nobody. The attacker files 4 REAL-signed countersigned claims naming
/// itself over the victim's real pot/settle; the pot is bare (no verdict/
/// attribution). The window delivers the attacker rows to the aggregate and
/// the aggregate ranks NOBODY — no public wrong-winner.
#[test]
fn evict_then_claim_on_a_bare_pot_credits_nobody_end_to_end() {
    let conn = production_schema_db();
    let pot = h64(0xfa);
    let settle = h64(0xfb);
    let attacker = 0x33u8;
    let sock = 0x44u8;
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    for i in 0..4u8 {
        file_honest_result(
            &conn,
            &format!("{:064x}", 0xfa00 + u32::from(i)),
            attacker,
            sock,
            &pot,
            &settle,
            true,
            &format!("{:064x}", 0xfe00 + u32::from(i)),
            10 + i64::from(i),
        );
    }
    let (markers, _) = query_window(&conn, 200);
    let statuses = statuses_from_db(&conn, &markers);
    // NO chain world — a bare/legacy pot.
    let lb = aggregate_leaderboard(&markers, &statuses, &std::collections::HashMap::new(), 200);
    assert!(
        lb.board.is_empty(),
        "an unattributed pot is UNRANKED — the forger's real-signed claims credit nobody"
    );
}

/// The per-pot superset, EXECUTED: a marker flood on ONE pot occupies one
/// pot slot and yields at most `RESULT_ROWS_PER_POT` rows, oldest-first —
/// so the honest settle-time marker is always IN the answer for the
/// verifier to find (verification before collapse: SQL never picks the real
/// row).
#[test]
fn a_flood_on_one_pot_collapses_to_one_pot_slot_oldest_first() {
    let conn = production_schema_db();
    let pot = h64(0xa1);
    let settle = h64(0xb1);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    let honest_game = h64(0x03);
    file_honest_result(
        &conn,
        &honest_game,
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xf3),
        1_001,
    );
    for i in 0..50u16 {
        file_result(
            &conn,
            &format!("{:064x}", 0xa000 + u32::from(i)),
            &identity(0xcc),
            &identity(0x11),
            &pot,
            &settle,
            &junk_sig(),
            Some(&junk_sig()),
            &format!("{:064x}", 0xd000 + u32::from(i)),
            5_000 + i64::from(i),
        );
    }
    let (markers, truncated) = query_window(&conn, 200);
    assert!(
        markers.len() <= low_app_layer::logic::LEADERBOARD_RESULT_ROWS_PER_POT,
        "≤ RESULT_ROWS_PER_POT rows for one pot"
    );
    assert!(
        markers.iter().any(|m| m.game_id == honest_game),
        "oldest-first keeps the settle-time marker in the superset"
    );
    assert!(!truncated, "one pot is one slot — nothing was cut");
}

/// The unknown-pot tier + quota, EXECUTED: markers naming INVENTED pots
/// (free — no `pot_records` row) are demoted behind every indexed pot except
/// the fresh quota, so a ghost flood cannot take the page from real hands.
#[test]
fn invented_pot_markers_are_quota_bounded_behind_indexed_pots() {
    let conn = production_schema_db();
    // One real, indexed pot, filed EARLIEST (naive newest-first would rank
    // it last).
    let pot = h64(0xa2);
    let settle = h64(0xb2);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    file_honest_result(
        &conn,
        &h64(0x04),
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xf4),
        1_000,
    );
    // A flood of markers naming pots that do not exist, all NEWER. `now` is
    // used so they are FRESH unknowns competing for the promoted quota.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let limit = 20usize;
    let ghosts = limit * 3;
    for i in 0..ghosts {
        file_result(
            &conn,
            &format!("{:064x}", 0xb000 + i),
            &identity(0xcc),
            &identity(0x11),
            &format!("{:064x}", 0xc000 + i), // invented pot
            &h64(0xbe),
            &junk_sig(),
            Some(&junk_sig()),
            &format!("{:064x}", 0xf000 + i),
            now,
        );
    }
    let (markers, truncated) = query_window(&conn, limit);
    let pos = markers
        .iter()
        .position(|m| m.pot_txid == pot)
        .expect("the indexed pot must still be served under a ghost flood");
    let ghosts_ahead = markers[..pos]
        .iter()
        .map(|m| m.pot_txid.clone())
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert!(
        ghosts_ahead <= leaderboard_unknown_pot_quota(limit),
        "at most {} fresh ghosts may rank ahead of an indexed pot; {ghosts_ahead} did",
        leaderboard_unknown_pot_quota(limit)
    );
    assert!(
        truncated,
        "a page that cannot hold every pot must SAY so — the bit that makes \
         a flood detectable instead of a complete-looking wrong answer"
    );
}

/// The `truncated` bit is HONEST in both directions, end to end into the
/// wire body — and an un-truncated page never claims otherwise.
#[test]
fn truncation_is_reported_only_when_the_page_is_short() {
    let conn = production_schema_db();
    for i in 0..5u8 {
        let pot = format!("{:064x}", 0x7000 + u32::from(i));
        let settle = format!("{:064x}", 0x8000 + u32::from(i));
        admit_pot(&conn, &pot, 1_000 + i64::from(i));
        mark_spent_confirmed(&conn, &pot, &settle);
        file_honest_result(
            &conn,
            &format!("{:064x}", 0x100 + u32::from(i)),
            0x11,
            0x22,
            &pot,
            &settle,
            true,
            &format!("{:064x}", 0x200 + u32::from(i)),
            1_100 + i64::from(i),
        );
    }
    let (all, truncated) = query_window(&conn, 10);
    assert_eq!(all.len(), 5);
    assert!(!truncated, "5 pots inside a 10-pot page is complete");
    let statuses = statuses_from_db(&conn, &all);
    let world: Vec<(String, String, String)> = (0..5u8)
        .map(|i| {
            (
                format!("{:064x}", 0x7000 + u32::from(i)),
                identity(0x11),
                identity(0x22),
            )
        })
        .collect();
    let world_refs: Vec<(&str, &str, &str)> = world
        .iter()
        .map(|(p, w, l)| (p.as_str(), w.as_str(), l.as_str()))
        .collect();
    let w = win_world(&world_refs);
    let lb = agg_world(&all, &statuses, &w);
    assert_eq!(lb.board[0].wins, 5, "five distinct pots, five wins");
    let v: serde_json::Value =
        serde_json::from_str(&leaderboard_body(&lb, 1, all.len(), truncated)).unwrap();
    assert_eq!(v["truncated"], false);

    let (page, truncated) = query_window(&conn, 2);
    assert!(truncated, "a 2-pot page over 5 pots is INCOMPLETE");
    let pots: std::collections::HashSet<&str> = page.iter().map(|m| m.pot_txid.as_str()).collect();
    assert_eq!(pots.len(), 2, "the cut lands exactly on the limit-th pot");
    let v: serde_json::Value =
        serde_json::from_str(&leaderboard_body(&lb, 1, page.len(), truncated)).unwrap();
    assert_eq!(v["truncated"], true);
}

/// `proof_pointers_sql`, EXECUTED (#332 HIGH-1): keyed to the caller's own
/// pairs (so an unrelated flood is irrelevant — the pre-#332 flat
/// `LIMIT 2000` scan was floodable), and returning a bounded SUPERSET per key
/// rather than a single squattable slot. The honest pointer survives inside
/// the superset; the CLIENT filters by transcript validity.
#[test]
fn proof_pointers_are_key_bound_and_return_a_superset() {
    use low_app_layer::logic::PROOF_POINTERS_PER_KEY;
    let conn = production_schema_db();
    let game = h64(0x05);
    let winner = identity(0x11);
    file_proof(&conn, &game, &winner, "txHONEST", 5_000);
    // A repoint attempt AND a couple more for the same key — all land in the
    // superset (no single-winner slot to squat).
    file_proof(&conn, &game, &winner, "txREPOINT", 9_000);
    // A large UNRELATED flood for a different key — must not touch this key.
    for i in 0..3_000u32 {
        file_proof(
            &conn,
            &format!("{:064x}", 0xd000 + i),
            &identity(0xcc),
            &format!("txJUNK{i:05}"),
            8_000 + i64::from(i),
        );
    }
    let map = query_proof_pointers(&conn, &[(game.clone(), winner.clone())]);
    let set = map
        .get(&(game.clone(), winner.clone()))
        .expect("the requested key returns its superset");
    assert!(
        set.contains(&"txHONEST".to_string()),
        "the honest pointer is IN the superset the client will verify"
    );
    assert!(
        set.len() <= PROOF_POINTERS_PER_KEY,
        "the superset is bounded to PROOF_POINTERS_PER_KEY"
    );
    assert_eq!(map.len(), 1, "only the requested key is returned");
    // A key with no pointer simply yields nothing — never a fabricated hint.
    let none = query_proof_pointers(&conn, &[(h64(0x06), winner)]);
    assert!(none.is_empty());
}

/// The squat is STRUCTURALLY impossible now (Rule 3): even a pre-filed junk
/// pointer for the victim's own key cannot own the drill-down, because the
/// honest pointer still enters the bounded superset alongside it (until the
/// key is flooded past the cap — a documented display-hint residual, never a
/// false proof: the client verifies each candidate).
#[test]
fn a_prefiled_junk_pointer_cannot_own_a_victims_drilldown() {
    let conn = production_schema_db();
    let game = h64(0x08);
    let winner = identity(0x11);
    // Attacker pre-files (earliest) a junk pointer for the victim's key.
    file_proof(&conn, &game, &winner, "txSQUAT", 1);
    // The honest pointer publishes later.
    file_proof(&conn, &game, &winner, "txHONEST", 9_999);
    let map = query_proof_pointers(&conn, &[(game.clone(), winner.clone())]);
    let set = &map[&(game, winner)];
    assert!(
        set.contains(&"txHONEST".to_string()),
        "the honest pointer is served despite the earlier squat (no exclusive slot)"
    );
}

// ── #332 v3: the ATTRIBUTION-EVICTION close, end to end through the real
// producer (`seat_markers_sql` → `attribute_seats` → the aggregate) ──────────

/// A committed CovenantParams with the two seat settle keys set.
fn params_with_keys(pub_a_hex: &str, pub_b_hex: &str) -> CovenantParams {
    let mut a = [0u8; 33];
    let mut b = [0u8; 33];
    a.copy_from_slice(&hex::decode(pub_a_hex).unwrap());
    b.copy_from_slice(&hex::decode(pub_b_hex).unwrap());
    CovenantParams {
        pub_a: a,
        pub_b: b,
        pub_tower: [2u8; 33],
        pay_pkh_a: [0u8; 20],
        pay_pkh_b: [0u8; 20],
        rake_pkh: [0u8; 20],
        stake_a: 500,
        stake_b: 500,
        fee_sats: 8,
        recovery_height: 1,
    }
}

/// File one `potparty_records` row (the topic manager's INSERT OR IGNORE
/// shape) — `seat_settle_pubkey`/`seat_sig`/`id_sig` supplied by the caller
/// (real or junk).
#[allow(clippy::too_many_arguments)]
fn file_potparty(
    conn: &Connection,
    identity: &str,
    opponent: &str,
    game: &str,
    pot: &str,
    seat_pub: &str,
    seat_sig: &str,
    id_sig: &str,
    marker_txid: &str,
    at: i64,
) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, 900000, ?5, ?6, ?7, ?8, 0, ?9)",
        params![
            identity,
            opponent,
            game,
            pot,
            id_sig,
            seat_pub,
            seat_sig,
            marker_txid,
            at
        ],
    )
    .expect("insert potparty_records");
}

/// File an HONEST v2 potparty marker: a REAL seatSig under the committed
/// settle key, and a REAL identity sig by `identity_seed`'s wallet.
#[allow(clippy::too_many_arguments)]
fn file_honest_potparty(
    conn: &Connection,
    settle_key: &PrivateKey,
    settle_pub: &str,
    identity_seed: u8,
    opponent: &str,
    game: &str,
    pot: &str,
    marker_txid: &str,
    at: i64,
) {
    let idw = wallet(identity_seed);
    let id_hex = identity(identity_seed);
    let preimage = seatsig_preimage(game, pot, 0, &id_hex).unwrap();
    let hash = bsv_rs::primitives::hash::sha256(&preimage);
    let seat_sig = hex::encode(settle_key.sign(&hash).unwrap().to_der());
    // The identity sig is over the v2 challenge — build a SeatMarkerRow to
    // reconstruct exactly the bytes `potparty_v2_challenge` hashes.
    let probe = SeatMarkerRow {
        identity: id_hex.clone(),
        opponent_identity: opponent.to_string(),
        game_id: game.to_string(),
        pot_txid: pot.to_string(),
        pot_vout: 0,
        recovery_height: 900_000,
        seat_settle_pubkey: settle_pub.to_string(),
        seat_sig_hex: seat_sig.clone(),
        identity_sig_hex: String::new(),
        sig_valid: None, // fixture: the compute arm
    };
    let challenge = potparty_v2_challenge(&probe).unwrap();
    let id_sig = idw
        .create_signature(CreateSignatureArgs {
            data: Some(challenge),
            hash_to_directly_sign: None,
            protocol_id: bsv_rs::wallet::Protocol::new(
                bsv_rs::wallet::SecurityLevel::App,
                "low potparty",
            ),
            key_id: game.to_string(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap();
    file_potparty(
        conn,
        &id_hex,
        opponent,
        game,
        pot,
        settle_pub,
        &seat_sig,
        &hex::encode(id_sig.signature),
        marker_txid,
        at,
    );
}

/// EXECUTE the shipped `seat_markers_sql` at `cap` and `attribute_seats` — the
/// real producer path the route runs (`seat_attributions`), for one pot.
fn attribution_from_db(
    conn: &Connection,
    pot: &str,
    params: &CovenantParams,
    cap: usize,
) -> SeatAttribution {
    let sql = seat_markers_sql(1, cap);
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("seat_markers_sql did not PREPARE: {e}\n{sql}"));
    let rows: Vec<SeatMarkerRow> = stmt
        .query_map(
            params![
                pot,
                0i64,
                hex::encode(params.pub_a),
                hex::encode(params.pub_b)
            ],
            |r| {
                Ok(SeatMarkerRow {
                    identity: r.get::<_, String>("identity")?.to_ascii_lowercase(),
                    opponent_identity: r.get::<_, String>("opponentIdentity")?.to_ascii_lowercase(),
                    game_id: r.get::<_, String>("gameId")?.to_ascii_lowercase(),
                    pot_txid: r.get::<_, String>("potTxid")?.to_ascii_lowercase(),
                    pot_vout: r.get::<_, i64>("potVout")? as u32,
                    recovery_height: r.get::<_, i64>("recoveryHeight")? as u32,
                    seat_settle_pubkey: r
                        .get::<_, String>("seatSettlePubkey")?
                        .to_ascii_lowercase(),
                    seat_sig_hex: r.get::<_, String>("seatSigHex")?.to_ascii_lowercase(),
                    identity_sig_hex: r.get::<_, String>("sigHex")?.to_ascii_lowercase(),
                    sig_valid: r.get::<_, Option<i64>>("sigValid")?.map(|v| v != 0),
                })
            },
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    attribute_seats(params, &pot.to_ascii_lowercase(), 0, &rows)
}

/// A raw settle key + its compressed pubkey hex (the covenant's committed
/// seat key). Distinct from an IDENTITY key.
fn settle_key(seed: u8) -> (PrivateKey, String) {
    let k = PrivateKey::from_hex(&format!("{seed:064x}")).unwrap();
    let pub_hex = k.public_key().to_hex();
    (k, pub_hex)
}

/// THE HIGH, END TO END (the delta re-gate's finding). An attacker who knows
/// the committed settle key files `SEAT_MARKERS_PER_KEY` junk potparty rows
/// under it, stamped EARLIER, evicting the honest v2 seat marker from an
/// rn<=cap window. At the OLD leaderboard cap (`SEAT_MARKERS_PER_KEY`) the
/// honest marker is gone and `attribute_seats` returns NO identity — under the
/// v2 spine that ERASED the win. Under #332 v3 the win is minted from the
/// verdict + committed KEY, so it SURVIVES (under the settle key). RED-verify:
/// injecting the v2 counting body makes this cell fail (wins 0).
#[test]
fn potparty_flood_evicts_the_identity_but_never_the_chain_win() {
    let conn = production_schema_db();
    let pot = h64(0xca);
    let settle = h64(0xcb);
    let game = h64(0x0a);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    // The pot's committed keys: seat A is a real settle key the WINNER holds.
    let (key_a, pub_a) = settle_key(0x31);
    let (_key_b, pub_b) = settle_key(0x32);
    let params = params_with_keys(&pub_a, &pub_b);
    let winner_seed = 0x11u8;
    let opponent = identity(0x22);
    // The honest RESULT marker puts the pot in the leaderboard window (the
    // board is seeded by published results); the attacker's flood is on the
    // separate potparty_records table.
    file_honest_result(
        &conn,
        &game,
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xc0),
        1_001,
    );

    // The attacker floods SEAT_MARKERS_PER_KEY junk rows under seat A's key,
    // stamped EARLIER than the honest marker (byte-format junk — no valid sig).
    for i in 0..SEAT_MARKERS_PER_KEY {
        file_potparty(
            &conn,
            &identity(0x22),
            &identity(0x11),
            &game,
            &pot,
            &pub_a, // the committed key — passes the SQL prefilter
            "3045abababab",
            "3044cdcdcd",
            &format!("{:064x}", 0xca00 + i),
            10 + i as i64,
        );
    }
    // The honest v2 marker, published LATER (the #252 backfill timing).
    file_honest_potparty(
        &conn,
        &key_a,
        &pub_a,
        winner_seed,
        &opponent,
        &game,
        &pot,
        &h64(0xcf),
        9_000,
    );

    let verdicts = std::collections::HashMap::from([(pot.clone(), PotVerdict::WinnerA)]);
    let params_map = std::collections::HashMap::from([(pot.clone(), params.clone())]);
    let (markers, _) = query_window(&conn, 200);
    let statuses = statuses_from_db(&conn, &markers);

    // At the OLD cap the honest marker is EVICTED ⇒ no identity.
    let attr_old = attribution_from_db(&conn, &pot, &params, SEAT_MARKERS_PER_KEY);
    assert!(
        attr_old.winner_for(PotVerdict::WinnerA).is_none(),
        "SEAT_MARKERS_PER_KEY junk rows evict the honest marker (identity lost)"
    );
    let attrs = std::collections::HashMap::from([(pot.clone(), attr_old)]);
    let lb = aggregate_leaderboard_attributed(
        &markers,
        &statuses,
        &std::collections::HashMap::new(),
        200,
        &verdicts,
        &attrs,
        &params_map,
        &std::collections::HashMap::new(),
    );
    // #332 v3: the win SURVIVES the eviction, under the committed settle key.
    let row = lb
        .board
        .iter()
        .find(|r| r.identity == pub_a)
        .expect("the win survives the identity eviction, under the settle key");
    assert_eq!(row.wins, 1);
    assert!(
        row.identity_is_key,
        "identity unknown ⇒ keyed by the settle key"
    );
    assert!(row.chain_proven);
}

/// The DISPLAY is preserved for a REALISTIC flood: at the WIDE leaderboard
/// candidate cap, `attribute_seats` validity-filters the same 8 junk rows and
/// finds the honest verified marker filed later — so the win counts AND
/// attributes to the honest IDENTITY.
#[test]
fn a_realistic_flood_still_attributes_to_the_honest_identity() {
    let conn = production_schema_db();
    let pot = h64(0xda);
    let settle = h64(0xdb);
    let game = h64(0x0b);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    let (key_a, pub_a) = settle_key(0x41);
    let (_key_b, pub_b) = settle_key(0x42);
    let params = params_with_keys(&pub_a, &pub_b);
    let winner_seed = 0x11u8;
    let winner_id = identity(winner_seed);
    let opponent = identity(0x22);
    file_honest_result(
        &conn,
        &game,
        0x11,
        0x22,
        &pot,
        &settle,
        true,
        &h64(0xd0),
        1_001,
    );
    for i in 0..SEAT_MARKERS_PER_KEY {
        file_potparty(
            &conn,
            &identity(0x22),
            &identity(0x11),
            &game,
            &pot,
            &pub_a,
            "3045abababab",
            "3044cdcdcd",
            &format!("{:064x}", 0xda00 + i),
            10 + i as i64,
        );
    }
    file_honest_potparty(
        &conn,
        &key_a,
        &pub_a,
        winner_seed,
        &opponent,
        &game,
        &pot,
        &h64(0xdf),
        9_000,
    );

    // The WIDE candidate cap the route uses for the leaderboard: the honest
    // marker (rn = 9 ≤ cap) enters the candidate set and the validity filter
    // keeps it, dropping the 8 junk.
    let attr = attribution_from_db(&conn, &pot, &params, LEADERBOARD_SEAT_CANDIDATES);
    assert_eq!(
        attr.winner_for(PotVerdict::WinnerA).map(str::to_string),
        Some(winner_id.clone()),
        "the verified honest marker survives the flood at the wide cap"
    );
    let verdicts = std::collections::HashMap::from([(pot.clone(), PotVerdict::WinnerA)]);
    let params_map = std::collections::HashMap::from([(pot.clone(), params)]);
    let attrs = std::collections::HashMap::from([(pot.clone(), attr)]);
    let (markers, _) = query_window(&conn, 200);
    let statuses = statuses_from_db(&conn, &markers);
    let lb = aggregate_leaderboard_attributed(
        &markers,
        &statuses,
        &std::collections::HashMap::new(),
        200,
        &verdicts,
        &attrs,
        &params_map,
        &std::collections::HashMap::new(),
    );
    let row = lb
        .board
        .iter()
        .find(|r| r.identity == winner_id)
        .expect("the win attributes to the honest identity");
    assert_eq!(row.wins, 1);
    assert!(
        !row.identity_is_key,
        "identity resolved ⇒ not key-attributed"
    );
}

/// Rule 16 — the freshness/quota constants this crate DUPLICATES from the
/// overlay must agree with the overlay's own. A duplicated value with no pin
/// is a boundary with no pin; both sides are called here.
#[test]
fn the_unknown_pot_constants_agree_with_the_overlay() {
    assert_eq!(
        LEADERBOARD_UNKNOWN_POT_MAX_AGE_SECS,
        bsv_overlay_cloudflare::d1_discovery::UNKNOWN_POT_PROMOTION_MAX_AGE_SECS,
        "the leaderboard window's freshness bound must equal the overlay's"
    );
    for limit in [1usize, 9, 10, 200, 500] {
        assert_eq!(
            leaderboard_unknown_pot_quota(limit),
            bsv_overlay_cloudflare::d1_discovery::unknown_pot_quota(limit),
            "quota must agree with the overlay's at limit {limit}"
        );
    }
}

// ── #375 — the pre-launch era write-off ─────────────────────────────────────

/// The DISTINCT pot txids the window serves under an era cutoff (`?4`).
fn window_pot_txids_era(conn: &Connection, limit: usize, era_ms: i64) -> Vec<String> {
    let sql = leaderboard_markers_sql(Some(era_ms));
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("leaderboard_markers_sql(era) did not PREPARE: {e}\n{sql}"));
    let per_pot = low_app_layer::logic::LEADERBOARD_RESULT_ROWS_PER_POT;
    let mut pots: Vec<String> = stmt
        .query_map(
            params![
                (limit + 1) as i64,
                leaderboard_unknown_pot_quota(limit) as i64,
                ((limit + 1) * per_pot) as i64,
                era_ms,
            ],
            |r| r.get::<_, Option<String>>("potTxid"),
        )
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows")
        .into_iter()
        .flatten()
        .collect();
    pots.dedup();
    pots
}

/// #375 through the SHIPPED `/leaderboard` marker window — the spine the
/// whole board counts from: a pot admitted one second before the cutoff is
/// DROPPED (its post-cutoff result marker cannot resurrect it), the
/// at-cutoff pot is KEPT (the `>=` boundary + the seconds→ms unit pin), an
/// unknown pot anchors on its OLDEST marker stamp (per-pot MIN — later spam
/// cannot move it), and the `None` arm serves the full window as today.
#[test]
fn the_written_off_era_is_dropped_and_the_unset_cutoff_is_inert() {
    let conn = production_schema_db();
    const CUT_MS: i64 = 1_754_500_000_000;
    const CUT_SECS: i64 = CUT_MS / 1000;
    let (w, l) = (identity(0x11), identity(0x22));

    let pre = h64(0xd2);
    let post = h64(0xd3);
    admit_pot(&conn, &pre, CUT_SECS - 1);
    admit_pot(&conn, &post, CUT_SECS);
    for (i, (pot, at)) in [(&pre, CUT_SECS + 5), (&post, CUT_SECS + 6)]
        .into_iter()
        .enumerate()
    {
        file_result(
            &conn,
            &h64(0x21 + i as u8),
            &w,
            &l,
            pot,
            &h64(0xfe),
            &junk_sig(),
            None,
            &format!("{:02x}", 0xe1 + i).repeat(32),
            at,
        );
    }
    // Unknown pots (no pot_records row): the oldest marker stamp anchors.
    let unknown_pre = h64(0xd4);
    let unknown_post = h64(0xd5);
    file_result(
        &conn,
        &h64(0x23),
        &w,
        &l,
        &unknown_pre,
        &h64(0xfe),
        &junk_sig(),
        None,
        &"e3".repeat(32),
        CUT_SECS - 10,
    );
    file_result(
        &conn,
        &h64(0x24),
        &w,
        &l,
        &unknown_post,
        &h64(0xfe),
        &junk_sig(),
        None,
        &"e4".repeat(32),
        CUT_SECS + 10,
    );

    let served = window_pot_txids_era(&conn, 10, CUT_MS);
    assert!(served.contains(&post), "the at-cutoff pot is KEPT (>= bar)");
    assert!(
        served.contains(&unknown_post),
        "a fresh unknown pot with a post-cutoff marker is KEPT"
    );
    assert_eq!(
        served.len(),
        2,
        "the pre-cutoff pot and the pre-cutoff unknown marker are DROPPED: {served:?}"
    );

    // The inert arm: the shipped None window serves all four pots.
    let (markers, truncated) = query_window(&conn, 10);
    assert!(!truncated);
    let mut all: Vec<String> = markers.iter().map(|m| m.pot_txid.clone()).collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), 4, "None serves the full window: {all:?}");
    for pot in [&pre, &post, &unknown_pre, &unknown_post] {
        assert!(all.contains(pot), "{pot} missing from the None arm");
    }
}

/// Brain-cutover M1 — the seat-attribution dual-arm, both directions:
/// `Some(true)` attributes a row WITHOUT touching its signatures (garbage
/// sigs still attribute — proof the two ECDSA checks were skipped),
/// `Some(false)` refuses a row whose signatures WOULD verify (the latch is
/// authoritative; the relatch sweep repairs a wrong one and its `demoted`
/// count is the alarm), and `None` computes both checks exactly as before
/// (the garbage row is refused — the arm the rest of this suite pins).
#[test]
fn the_sig_valid_latch_arm_short_circuits_attribute_seats_and_a_zero_latch_refuses() {
    let (_key_a, pub_a) = settle_key(0x41);
    let (_key_b, pub_b) = settle_key(0x42);
    let params = params_with_keys(&pub_a, &pub_b);
    let pot = h64(0xd1);
    let game = h64(0x0b);

    // Garbage sigs, latched TRUE → attributed (the latch arm, no ECDSA).
    let garbage_latched_true = SeatMarkerRow {
        identity: identity(0x11),
        opponent_identity: identity(0x22),
        game_id: game.clone(),
        pot_txid: pot.clone(),
        pot_vout: 0,
        recovery_height: 900_000,
        seat_settle_pubkey: pub_a.clone(),
        seat_sig_hex: "3045abababab".into(),
        identity_sig_hex: "3044cdcdcd".into(),
        sig_valid: Some(true),
    };
    let attr = attribute_seats(
        &params,
        &pot,
        0,
        std::slice::from_ref(&garbage_latched_true),
    );
    assert_eq!(
        attr.identity_a,
        Some(identity(0x11).to_ascii_lowercase()),
        "a latched-true row attributes without any signature computation"
    );

    // The same garbage row with NO latch → the compute arm refuses it.
    let mut garbage_unlatched = garbage_latched_true.clone();
    garbage_unlatched.sig_valid = None;
    let attr = attribute_seats(&params, &pot, 0, std::slice::from_ref(&garbage_unlatched));
    assert_eq!(attr.identity_a, None, "the compute arm still refuses junk");

    // An HONEST marker (real seatSig under the committed key + real identity
    // sig) latched FALSE → refused: the latch is authoritative when present.
    let (key_a2, pub_a2) = settle_key(0x43);
    let params2 = params_with_keys(&pub_a2, &pub_b);
    let idw = wallet(0x33);
    let id_hex = identity(0x33);
    let preimage = seatsig_preimage(&game, &pot, 0, &id_hex).unwrap();
    let seat_sig = hex::encode(
        key_a2
            .sign(&bsv_rs::primitives::hash::sha256(&preimage))
            .unwrap()
            .to_der(),
    );
    let mut honest = SeatMarkerRow {
        identity: id_hex.clone(),
        opponent_identity: identity(0x22),
        game_id: game.clone(),
        pot_txid: pot.clone(),
        pot_vout: 0,
        recovery_height: 900_000,
        seat_settle_pubkey: pub_a2.clone(),
        seat_sig_hex: seat_sig,
        identity_sig_hex: String::new(),
        sig_valid: None,
    };
    let challenge = potparty_v2_challenge(&honest).unwrap();
    let id_sig = idw
        .create_signature(CreateSignatureArgs {
            data: Some(challenge),
            hash_to_directly_sign: None,
            protocol_id: bsv_rs::wallet::Protocol::new(
                bsv_rs::wallet::SecurityLevel::App,
                "low potparty",
            ),
            key_id: game.clone(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap()
        .signature;
    honest.identity_sig_hex = hex::encode(id_sig);
    let attr = attribute_seats(&params2, &pot, 0, std::slice::from_ref(&honest));
    assert_eq!(
        attr.identity_a,
        Some(id_hex.to_ascii_lowercase()),
        "sanity: the honest marker attributes on the compute arm"
    );
    honest.sig_valid = Some(false);
    let attr = attribute_seats(&params2, &pot, 0, std::slice::from_ref(&honest));
    assert_eq!(
        attr.identity_a, None,
        "a latched 0 is authoritative — the relatch sweep repairs, the serve path never second-guesses"
    );
}

/// #399 (OWNER RULED 2026-08-21) — a claim-less chain win is LISTED.
///
/// The counting spine has minted from chain facts alone since #332 v3; what
/// this pins is the NEW candidate source feeding it: `chain_win_pots_sql`
/// returns a classified winner pot with ZERO result markers, and the
/// aggregate then counts it — the exact shape of the 2026-08-13 beta hand
/// that settled, confirmed, classified winner-b, paid… and vanished from the
/// board because one marker publish raced teardown.
///
/// The window's guards are pinned in the same cell, each with its own pot:
///  - a pot whose stored verdict is STALE (`verdictTxid` ≠ the current
///    spender — the documented reader-side guard) is NOT a candidate;
///  - an UNSPENT pot with a leftover verdict is NOT a candidate;
///  - a tie/refund verdict is NOT a candidate (it attributes nobody).
#[test]
fn a_claimless_chain_win_is_a_candidate_and_counts() {
    let conn = production_schema_db();
    let (_key_a, pub_a) = settle_key(0x61);
    let (_key_b, pub_b) = settle_key(0x62);

    // THE win: classified, confirmed, params decoded, NO markers anywhere.
    let pot = h64(0xe1);
    let settle = h64(0xe2);
    admit_pot(&conn, &pot, 1_000);
    mark_spent_confirmed(&conn, &pot, &settle);
    conn.execute(
        "UPDATE pot_records SET verdict = 'winner-a', verdictTxid = ?1, \
         paramsDecoded = 1, pubA = ?2, pubB = ?3 WHERE txid = ?4",
        params![settle, pub_a, pub_b, pot],
    )
    .unwrap();

    // Guard 1: stale verdict — verdictTxid names a DIFFERENT (superseded) spender.
    let stale = h64(0xe3);
    admit_pot(&conn, &stale, 1_001);
    mark_spent_confirmed(&conn, &stale, &h64(0xe4));
    conn.execute(
        "UPDATE pot_records SET verdict = 'winner-a', verdictTxid = ?1, \
         paramsDecoded = 1, pubA = ?2, pubB = ?3 WHERE txid = ?4",
        params![h64(0xe5), pub_a, pub_b, stale],
    )
    .unwrap();

    // Guard 2: unspent pot carrying a leftover verdict.
    let unspent = h64(0xe6);
    admit_pot(&conn, &unspent, 1_002);
    conn.execute(
        "UPDATE pot_records SET verdict = 'winner-a', verdictTxid = ?1, \
         paramsDecoded = 1, pubA = ?2, pubB = ?3 WHERE txid = ?4",
        params![h64(0xe7), pub_a, pub_b, unspent],
    )
    .unwrap();

    // Guard 3: a refund verdict.
    let refund = h64(0xe8);
    admit_pot(&conn, &refund, 1_003);
    mark_spent_confirmed(&conn, &refund, &h64(0xe9));
    conn.execute(
        "UPDATE pot_records SET verdict = 'refund', verdictTxid = ?1, \
         paramsDecoded = 1, pubA = ?2, pubB = ?3 WHERE txid = ?4",
        params![h64(0xe9), pub_a, pub_b, refund],
    )
    .unwrap();

    // The SHIPPED window: exactly the one candidate.
    let sql = low_app_layer::logic::chain_win_pots_sql(None);
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("chain_win_pots_sql did not PREPARE: {e}\n{sql}"));
    let candidates: Vec<String> = stmt
        .query_map(params![10i64], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        candidates,
        vec![pot.clone()],
        "the claim-less win is the ONLY candidate; stale/unspent/refund are refused"
    );

    // …and the spine counts it with ZERO markers, keyed by the winner's
    // identity when attribution resolves (win_world supplies the verified
    // attribution exactly as `seat_attributions` would).
    let winner = identity(0x11);
    let statuses = assemble_statuses(
        &[low_app_layer::logic::Outpoint {
            txid: pot.clone(),
            vout: 0,
        }],
        &query_pot_rows(&conn, &pot),
    );
    let world = win_world(&[(&pot, &winner, &identity(0x22))]);
    let lb = agg_world(&[], &statuses, &world);
    let row = lb
        .board
        .iter()
        .find(|b| b.identity == winner)
        .expect("the claim-less winner is ON the board");
    assert_eq!(row.wins, 1);
    assert_eq!(row.chain_wins.len(), 1);
    assert_eq!(row.chain_wins[0].pot_txid, pot);
    assert!(
        row.evidence.is_empty(),
        "no marker ⇒ no evidence row — the win counts anyway (that is #399)"
    );
}

/// The window's rows drive `assemble_statuses` exactly like the marker-window
/// rows do (shared shape) — a tiny helper so the cell above reads the pot
/// back through the SHIPPED batch reader rather than hand-building a status.
fn query_pot_rows(conn: &Connection, pot: &str) -> Vec<PotRecordRow> {
    let sql = low_app_layer::logic::batch_where_sql(1);
    let mut stmt = conn.prepare(&sql).expect("batch_where_sql prepares");
    stmt.query_map(params![pot, 0i64], |r| {
        Ok(PotRecordRow {
            txid: r.get("txid")?,
            vout: r.get::<_, i64>("outputIndex")? as u32,
            spent: r.get::<_, Option<i64>>("spent")?.unwrap_or(0) != 0,
            spending_txid: r.get("spendingTxid")?,
            spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.unwrap_or(0) != 0,
            spender_final: r.get::<_, Option<i64>>("spenderFinal")?.map(|v| v != 0),
            spender_seen: r.get::<_, Option<i64>>("spenderSeen")?.map(|v| v != 0),
        })
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

// ═══════════════════════════════════════════════════════════════════════════
// #411 round 2 — the WRITE-TIME spine (`lb_marker_rows`), proven against the
// stage-1 window it replaces, through the SHIPPED strings only.
// ═══════════════════════════════════════════════════════════════════════════

/// Execute the SHIPPED companion write (`lb_row_insert_sql`) with the same
/// values a `file_result` seeded — the write-time half of the spine.
#[allow(clippy::too_many_arguments)]
fn lb_companion(
    conn: &Connection,
    game: &str,
    winner: &str,
    loser: &str,
    pot: &str,
    settle: &str,
    winner_sig: &str,
    loser_sig: Option<&str>,
    marker_txid: &str,
    at: i64,
) {
    let per_pot = low_app_layer::logic::LEADERBOARD_RESULT_ROWS_PER_POT as i64;
    conn.execute(
        bsv_overlay_cloudflare::d1_discovery::result_write::lb_row_insert_sql(),
        params![
            marker_txid,
            0i64,
            game,
            winner,
            loser,
            pot,
            settle,
            winner_sig,
            loser_sig,
            Option::<String>::None, // cardsHex (mirrors file_result)
            at,
            Option::<i64>::None, // claimValid NULL (mirrors file_result's legacy shape)
            pot,
            pot,
            pot,
            at,
            pot,
            pot,
            at,
            pot,
            pot,
            per_pot
        ],
    )
    .expect("lb companion insert");
}

/// Read the spine pages through the SHIPPED `lb_page_sql` strings and run
/// the route's own combiner — the fast path exactly as `/leaderboard` runs it.
fn lb_pages(conn: &Connection, limit: usize, now: i64) -> (Vec<ResultMarkerRow>, bool, usize) {
    let per_pot = low_app_layer::logic::LEADERBOARD_RESULT_ROWS_PER_POT;
    let read = |unknown: bool, cap: usize| -> Vec<low_app_layer::logic::LbPageRow> {
        let sql = low_app_layer::logic::lb_page_sql(unknown, None);
        let mut stmt = conn
            .prepare(&sql)
            .unwrap_or_else(|e| panic!("lb_page_sql did not PREPARE: {e}\n{sql}"));
        stmt.query_map(params![cap as i64], |r| {
            let pot: Option<String> = r.get("potTxid")?;
            Ok(low_app_layer::logic::LbPageRow {
                marker: ResultMarkerRow {
                    game_id: r.get("gameId")?,
                    winner: r.get("winner")?,
                    loser: r.get("loser")?,
                    pot_txid: pot.unwrap_or_default(),
                    settle_txid: r
                        .get::<_, Option<String>>("settleTxid")?
                        .unwrap_or_default(),
                    winner_sig_hex: r
                        .get::<_, Option<String>>("winnerSigHex")?
                        .unwrap_or_default(),
                    loser_sig_hex: r.get("loserSigHex")?,
                    cards_hex: r.get("cardsHex")?,
                    txid: r.get("txid")?,
                    created_at: r.get("createdAt")?,
                    claim_valid: r.get("claimValid")?,
                },
                marker_rowid: r.get("markerRowid")?,
                pot_first_marker_at: r.get("potFirstMarkerAt")?,
                order_at: r.get("orderAt")?,
                unknown_pot: r.get::<_, i64>("unknownPot")? != 0,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    let known = read(false, (limit + 1) * per_pot);
    let unknown = read(true, low_app_layer::logic::LB_UNKNOWN_PAGE_ROWS);
    low_app_layer::logic::lb_window_from_pages(
        known,
        unknown,
        limit,
        leaderboard_unknown_pot_quota(limit),
        now,
    )
}

/// Shared world: 4 known pots seeded pot-first (writer A sees the pot), one
/// known pot seeded markers-first + the SHIPPED flip (writer B), one
/// unknown-FRESH pot, one unknown-STALE pot, and one pot with 5 markers
/// (the rn ≤ 4 cap). `with_companions` decides whether the write-time half
/// runs (T1) or the spine starts empty (T2's backfill subject).
fn seed_spine_world(conn: &Connection, now: i64, with_companions: bool) {
    let file = |game: &str, pot: &str, m: &str, at: i64| {
        let (w, l) = (h64(0xaa), h64(0xbb));
        let settle = h64(0x77);
        let ws = junk_sig();
        file_result(conn, game, &w, &l, pot, &settle, &ws, None, m, at);
        if with_companions {
            lb_companion(conn, game, &w, &l, pot, &settle, &ws, None, m, at);
        }
    };
    // 4 known pots, pot admitted BEFORE its markers.
    for i in 0..4u8 {
        let pot = h64(0x30 + i);
        admit_pot(conn, &pot, now - 5_000 + i as i64 * 100);
        file(
            &h64(0x40 + i),
            &pot,
            &h64(0x50 + i),
            now - 4_000 + i as i64 * 100,
        );
    }
    // 1 known pot, markers FIRST (unknown at write), pot admitted after +
    // the SHIPPED flip — exactly what the pot-admission writer runs.
    let late_pot = h64(0x38);
    file(&h64(0x48), &late_pot, &h64(0x58), now - 3_000);
    admit_pot(conn, &late_pot, now - 2_900);
    if with_companions {
        conn.execute(
            bsv_overlay_cloudflare::d1_discovery::lb_pot_flip_sql(),
            params![late_pot, late_pot, late_pot],
        )
        .expect("shipped flip");
    }
    // The rn cap subject: 5 markers on one known pot — the spine must hold 4.
    let fat_pot = h64(0x39);
    admit_pot(conn, &fat_pot, now - 2_500);
    for j in 0..5u8 {
        file(&h64(0x49), &fat_pot, &h64(0x60 + j), now - 2_400 + j as i64);
    }
    // Unknown pots: one FRESH (inside the quota hour), one STALE.
    file(&h64(0x4a), &h64(0x3a), &h64(0x6a), now - 100);
    file(&h64(0x4b), &h64(0x3b), &h64(0x6b), now - 90_000);
}

fn assert_windows_equal(
    fast: &(Vec<ResultMarkerRow>, bool, usize),
    old: &(Vec<ResultMarkerRow>, bool),
) {
    let fast_seq: Vec<(String, String)> = fast
        .0
        .iter()
        .map(|m| (m.txid.clone(), m.pot_txid.clone()))
        .collect();
    let old_seq: Vec<(String, String)> = old
        .0
        .iter()
        .map(|m| (m.txid.clone(), m.pot_txid.clone()))
        .collect();
    assert_eq!(
        fast_seq, old_seq,
        "spine window rows/order diverge from stage-1"
    );
    assert_eq!(fast.1, old.1, "truncated bit diverges from stage-1");
}

/// T1 — a PROVEN-OVERFULL board: the write-time spine (both shipped writers)
/// serves the SAME rows, order and truncated bit as the stage-1 window.
#[test]
fn spine_fast_path_matches_stage1_on_proven_overfull_board() {
    let conn = production_schema_db();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64; // REAL clock: stage-1's freshness CASE uses SQL unixepoch()
    seed_spine_world(&conn, now, true);
    let limit = 3usize;
    let fast = lb_pages(&conn, limit, now);
    assert!(
        fast.2 > limit,
        "world must prove over-full (distinct {} < {})",
        fast.2,
        limit + 1
    );
    let old = query_window(&conn, limit);
    assert_windows_equal(&fast, &old);
    // The rn cap held: the 5-marker pot materialized exactly 4 spine rows.
    let fat: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM lb_marker_rows WHERE potTxid = ?1",
            params![h64(0x39)],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(fat, 4, "rn cap must hold at RESULT_ROWS_PER_POT");
}

/// T2 — SELF-HEAL: an empty spine refuses the fast path (the zero-lie rule),
/// the shipped BULK backfill converges it, and the healed spine matches
/// stage-1 exactly.
#[test]
fn spine_backfill_converges_and_then_matches_stage1() {
    let conn = production_schema_db();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64; // REAL clock: stage-1's freshness CASE uses SQL unixepoch()
    seed_spine_world(&conn, now, false); // no companions — the spine is empty
    let limit = 3usize;
    let before = lb_pages(&conn, limit, now);
    assert!(
        before.2 < limit + 1,
        "an empty spine must REFUSE the fast path, never serve a sparse page"
    );
    conn.execute(&low_app_layer::logic::lb_backfill_sql(), [])
        .expect("shipped backfill");
    let after = lb_pages(&conn, limit, now);
    assert!(after.2 > limit, "backfill must converge the spine");
    let old = query_window(&conn, limit);
    assert_windows_equal(&after, &old);
}

/// T3 — the unknown→known FLIP: a marker admitted before its pot rides
/// unknown-tiered; the pot admission's shipped flip moves it to tier 0 and
/// stamps potCreatedAt/orderAt from the pot's own admission stamp.
#[test]
fn spine_pot_flip_moves_unknown_to_known() {
    let conn = production_schema_db();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64; // REAL clock: stage-1's freshness CASE uses SQL unixepoch()
    let pot = h64(0x71);
    let (w, l) = (h64(0xaa), h64(0xbb));
    let ws = junk_sig();
    file_result(
        &conn,
        &h64(0x72),
        &w,
        &l,
        &pot,
        &h64(0x77),
        &ws,
        None,
        &h64(0x73),
        now - 50,
    );
    lb_companion(
        &conn,
        &h64(0x72),
        &w,
        &l,
        &pot,
        &h64(0x77),
        &ws,
        None,
        &h64(0x73),
        now - 50,
    );
    let (unk, created): (i64, Option<i64>) = conn
        .query_row(
            "SELECT unknownPot, potCreatedAt FROM lb_marker_rows WHERE potTxid = ?1",
            params![pot],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(unk, 1, "pre-pot marker must ride unknown");
    assert_eq!(created, None);
    admit_pot(&conn, &pot, now - 40);
    conn.execute(
        bsv_overlay_cloudflare::d1_discovery::lb_pot_flip_sql(),
        params![pot, pot, pot],
    )
    .expect("shipped flip");
    let (unk2, created2, order2): (i64, Option<i64>, Option<i64>) = conn
        .query_row(
            "SELECT unknownPot, potCreatedAt, orderAt FROM lb_marker_rows WHERE potTxid = ?1",
            params![pot],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(unk2, 0, "flip must move the row to the known tier");
    assert_eq!(
        created2, order2,
        "orderAt must adopt the pot admission stamp"
    );
    assert!(created2.is_some());
}
