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
use low_app_layer::results::{PotVerdict, SeatAttribution};
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
    aggregate_leaderboard_attributed(
        markers,
        statuses,
        &std::collections::HashMap::new(),
        200,
        &world.0,
        &world.1,
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

/// Record a CONFIRMED spend through the REAL `mark_spent_sql()` writer.
fn mark_spent_confirmed(conn: &Connection, txid: &str, spender: &str) {
    conn.execute(
        mark_spent_sql(true, false),
        params![spender, spender, 900_000i64, 900_000i64, txid, 0i64],
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
    let sql = leaderboard_markers_sql();
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

/// Build the `pot_records` spend-status join for a marker set, straight out
/// of the SAME database the window read (the route's step 2, chunk logic
/// aside — `batch_where_sql` is executed by `sql_prepares_sqlite`).
fn statuses_from_db(conn: &Connection, markers: &[ResultMarkerRow]) -> Vec<OutpointStatus> {
    let ops = leaderboard_pot_outpoints(markers);
    let mut rows: Vec<PotRecordRow> = Vec::new();
    for op in &ops {
        let mut stmt = conn
            .prepare(
                "SELECT txid, outputIndex, spent, spendingTxid, spentConfirmed \
                 FROM pot_records WHERE txid = ?1 AND outputIndex = ?2",
            )
            .unwrap();
        let mut got = stmt
            .query_map(params![op.db_txid(), op.vout as i64], |r| {
                Ok(PotRecordRow {
                    txid: r.get("txid")?,
                    vout: r.get::<_, i64>("outputIndex")? as u32,
                    spent: r.get::<_, i64>("spent")? != 0,
                    spending_txid: r.get("spendingTxid")?,
                    spent_confirmed: r.get::<_, i64>("spentConfirmed")? != 0,
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
    let sql = leaderboard_markers_sql();
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
