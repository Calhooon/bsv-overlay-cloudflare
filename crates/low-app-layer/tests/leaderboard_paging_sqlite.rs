//! bsv-low #403 BOARD PAGING (2026-08-29) — the whole-era CHAIN-WINS SPINE,
//! EXECUTED against the production overlay migrations (rusqlite, in-memory).
//!
//! The 500-pot marker window used to bound the leaderboard's COUNTING
//! spine; past it every page came back `truncated` and the client fell
//! back to the whole-history gather (layer 10). These cells prove the
//! replacement: the spine counts from `pot_records` facts alone for the
//! WHOLE era, pages OWNERS by rank, applies exactly the fold's landing bar
//! and `attribute_seats`' latched slot rule in SQL, and ranks the era's
//! lowest hands with a pure-SQL score that equals `hand_score`.
//!
//! Writers are the overlay's OWN statement shapes where one exists
//! (`store_record_sql`, `mark_spent_sql`, `verdict_cas_sql`); the decoded
//! committed keys and the #371 unconfirmed spend shape are set by direct
//! column writes documented at each helper (the decode/unconfirmed writers
//! have their own producer cells in the overlay crate).

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{mark_spent_sql, store_record_sql, verdict_cas_sql};
use low_app_layer::logic::{
    attributions_from_pot_rows, chain_wins_owners_sql, chain_wins_spine_sql,
    clamp_leaderboard_after, clamp_leaderboard_page, era_hands_sql, hand_score,
    leaderboard_next_after, pot_markers_sql, sql_hand_score_expr, ChainWinPotRow,
    LEADERBOARD_AFTER_MAX, LEADERBOARD_MAX_LIMIT, LEADERBOARD_PAGE_DEFAULT, LEADERBOARD_PAGE_MAX,
};
use low_app_layer::results::{attribute_seats, CovenantParams, SeatMarkerRow};
use rusqlite::{params, params_from_iter, Connection};

fn production_schema_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            // ALTER TABLE ADD COLUMN re-runs are refused by SQLite ("duplicate
            // column") — the overlay tolerates exactly that class per the M9
            // rerun rule; everything else must apply.
            let msg = e.to_string();
            assert!(
                msg.contains("duplicate column"),
                "migration failed: {msg}\n{sql}"
            );
        }
    }
    conn
}

fn h64(seed: u32) -> String {
    format!("{seed:064x}")
}

/// A 33-byte-looking compressed key, hex (66 chars), distinct per seed.
fn key(seed: u32) -> String {
    format!("02{seed:064x}")
}

fn identity(seed: u32) -> String {
    format!("03{seed:064x}")
}

fn admit_pot(conn: &Connection, txid: &str, created_at: i64) {
    conn.execute(
        store_record_sql(),
        params![
            txid,
            0i64,
            0i64,
            Option::<String>::None,
            0i64,
            created_at,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            0i64,
        ],
    )
    .expect("store_record_sql");
}

/// A CONFIRMED spend through the real `mark_spent_sql()` writer.
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

/// The #371 UNCONFIRMED spend shape: pointer + bytes-finality latch, no
/// confirmation (the overlay's unconfirmed writer stamps exactly these
/// columns; `witnessed` additionally records the overlay's own SEEN in
/// `network_seen`).
fn mark_spent_unconfirmed(
    conn: &Connection,
    txid: &str,
    spender: &str,
    spender_final: Option<i64>,
    witnessed: bool,
) {
    conn.execute(
        "UPDATE pot_records SET spent = 1, spendingTxid = ?1, spentConfirmed = 0, \
         spenderFinal = ?2 WHERE txid = ?3 AND outputIndex = 0",
        params![spender, spender_final, txid],
    )
    .expect("unconfirmed mark");
    if witnessed {
        conn.execute(
            "INSERT OR IGNORE INTO network_seen (txid, seenAt) VALUES (?1, 1)",
            params![spender.to_ascii_lowercase()],
        )
        .expect("network_seen");
    }
}

/// The decoded committed keys (the #284 decode-at-write columns) plus the
/// verdict through the real `verdict_cas_sql()` (pointer-guarded).
fn set_verdict(
    conn: &Connection,
    pot: &str,
    spender: &str,
    verdict: &str,
    pub_a: &str,
    pub_b: &str,
    signers: Option<&str>,
) {
    conn.execute(
        "UPDATE pot_records SET pubA = ?1, pubB = ?2, paramsDecoded = 1 \
         WHERE txid = ?3 AND outputIndex = 0",
        params![pub_a, pub_b, pot],
    )
    .expect("decoded params");
    let n = conn
        .execute(
            verdict_cas_sql(),
            params![verdict, spender, signers, pot, 0i64, spender],
        )
        .expect("verdict_cas_sql");
    assert_eq!(n, 1, "the verdict CAS must hit the recorded spender");
}

/// A `potparty_records` v2 row with the admission-latched `sigValid`.
fn file_potparty(
    conn: &Connection,
    identity: &str,
    pot: &str,
    seat_pub: &str,
    sig_valid: Option<i64>,
    marker_txid: &str,
) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt, sigValid) \
         VALUES (?1, ?2, ?3, ?4, 0, 900000, 'idsig', ?5, 'seatsig', ?6, 0, 1, ?7)",
        params![identity, identity, h64(7), pot, seat_pub, marker_txid, sig_valid],
    )
    .expect("insert potparty_records");
}

/// A `result_markers_v2` row with the admission-latched `claimValid` tier.
#[allow(clippy::too_many_arguments)]
fn file_result(
    conn: &Connection,
    game: &str,
    winner: &str,
    loser: &str,
    pot: &str,
    settle: &str,
    cards_hex: Option<&str>,
    claim_valid: Option<i64>,
    marker_txid: &str,
    at: i64,
) {
    conn.execute(
        "INSERT OR IGNORE INTO result_markers_v2 \
         (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, cardsHex, \
          txid, outputIndex, createdAt, claimValid) \
         VALUES (?1, ?2, ?3, ?4, ?5, 'wsig', NULL, ?6, ?7, 0, ?8, ?9)",
        params![game, winner, loser, pot, settle, cards_hex, marker_txid, at, claim_valid],
    )
    .expect("insert result_markers_v2");
}

/// One COUNTED win: pot admitted, confirmed-spent by `spender`, verdict
/// winner-a with `pub_a` = the winner's settle key, and (when `latched`)
/// the winner identity's `sigValid = 1` potparty row on that key.
fn counted_win(
    conn: &Connection,
    seed: u32,
    winner_identity: &str,
    winner_key: &str,
    created_at: i64,
    latched: Option<i64>,
) -> (String, String) {
    let pot = h64(10_000 + seed);
    let spender = h64(20_000 + seed);
    admit_pot(conn, &pot, created_at);
    mark_spent_confirmed(conn, &pot, &spender);
    set_verdict(conn, &pot, &spender, "winner-a", winner_key, &key(999), Some("coop"));
    if latched.is_some() || latched.is_none() {
        file_potparty(conn, winner_identity, &pot, winner_key, latched, &h64(30_000 + seed));
    }
    (pot, spender)
}

fn page(conn: &Connection, era: Option<i64>, limit: usize, after: usize) -> (Vec<(String, bool, i64)>, bool) {
    let mut stmt = conn.prepare(&chain_wins_spine_sql(era)).unwrap();
    let mut binds: Vec<rusqlite::types::Value> = vec![
        ((limit + 1) as i64).into(),
        (after as i64).into(),
    ];
    if let Some(ms) = era {
        binds.push(ms.into());
    }
    let mut rows: Vec<(String, bool, i64)> = stmt
        .query_map(params_from_iter(binds), |r| {
            Ok((
                r.get::<_, String>("owner")?,
                r.get::<_, i64>("identityIsKey")? != 0,
                r.get::<_, i64>("wins")?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let truncated = rows.len() > limit;
    rows.truncate(limit);
    (rows, truncated)
}

fn owners_pots(conn: &Connection, era: Option<i64>, owners: &[&str]) -> Vec<ChainWinPotRow> {
    let mut stmt = conn
        .prepare(&chain_wins_owners_sql(owners.len(), era))
        .unwrap();
    let mut binds: Vec<rusqlite::types::Value> = vec![era.unwrap_or(0).into()];
    for o in owners {
        binds.push((*o).to_string().into());
    }
    stmt.query_map(params_from_iter(binds), |r| {
        Ok(ChainWinPotRow {
            owner: r.get("owner")?,
            identity_is_key: r.get::<_, i64>("identityIsKey")? != 0,
            pot_txid: r.get("potTxid")?,
            settle_txid: r.get("settleTxid")?,
            verdict: r.get("verdict")?,
            pub_a: r.get("pubA")?,
            pub_b: r.get("pubB")?,
            settle_signers: r.get("settleSigners")?,
            identity_a: r.get("identityA")?,
            identity_b: r.get("identityB")?,
        })
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

fn hands(conn: &Connection, era: Option<i64>, limit: usize) -> Vec<(String, String)> {
    let mut stmt = conn.prepare(&era_hands_sql(era)).unwrap();
    let mut binds: Vec<rusqlite::types::Value> = vec![(limit as i64).into()];
    if let Some(ms) = era {
        binds.push(ms.into());
    }
    stmt.query_map(params_from_iter(binds), |r| {
        Ok((r.get::<_, String>("gameId")?, r.get::<_, String>("cardsHex")?))
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

#[test]
fn the_paging_queries_prepare_against_the_production_schema() {
    let conn = production_schema_db();
    for era in [None, Some(1_700_000_000_000i64)] {
        conn.prepare(&chain_wins_spine_sql(era)).expect("spine");
        conn.prepare(&chain_wins_owners_sql(3, era)).expect("owners");
        conn.prepare(&era_hands_sql(era)).expect("hands");
    }
    conn.prepare(&pot_markers_sql(5)).expect("markers");
    // The bounds the route derives from.
    assert_eq!(clamp_leaderboard_page(None), LEADERBOARD_PAGE_DEFAULT);
    assert_eq!(clamp_leaderboard_page(Some(0)), 1);
    assert_eq!(clamp_leaderboard_page(Some(500)), LEADERBOARD_PAGE_MAX);
    assert_eq!(clamp_leaderboard_after(None), 0);
    assert_eq!(
        clamp_leaderboard_after(Some(u32::MAX)),
        LEADERBOARD_AFTER_MAX
    );
    assert_eq!(leaderboard_next_after(0, 50, true), Some(50));
    assert_eq!(leaderboard_next_after(0, 50, false), None);
    assert_eq!(
        leaderboard_next_after(LEADERBOARD_AFTER_MAX - 10, 50, true),
        None,
        "the walk stops at the ceiling instead of re-clamping forever"
    );
}

/// THE CLIFF, red→green: more counted pots than the old window could ever
/// hold, and the spine still answers the WHOLE era on ONE page — no
/// `truncated`, exact counts, rank order.
#[test]
fn the_whole_era_spine_counts_past_the_old_window_on_one_page() {
    let conn = production_schema_db();
    let total = LEADERBOARD_MAX_LIMIT + 200; // 700 > the old 500-pot window
    let ids = [identity(1), identity(2), identity(3)];
    let keys = [key(1), key(2), key(3)];
    for i in 0..total {
        let w = i % 3;
        counted_win(&conn, i as u32, &ids[w], &keys[w], 1_000 + i as i64, Some(1));
    }
    let (rows, truncated) = page(&conn, None, 50, 0);
    assert!(!truncated, "three owners fit one page whatever the pot count");
    assert_eq!(
        rows,
        vec![
            (ids[0].clone(), false, 234),
            (ids[1].clone(), false, 233),
            (ids[2].clone(), false, 233),
        ],
        "wins DESC, owner ASC; every one of the {total} pots counted"
    );
    // The owners query returns exactly their pots.
    let pots = owners_pots(&conn, None, &[ids[0].as_str()]);
    assert_eq!(pots.len(), 234);
    assert!(pots.iter().all(|p| p.owner == ids[0] && !p.identity_is_key));
    assert!(pots
        .iter()
        .all(|p| p.identity_a.as_deref() == Some(ids[0].as_str())));
}

#[test]
fn pages_walk_owners_in_rank_order_without_overlap() {
    let conn = production_schema_db();
    // Owner k (1..=7) has k wins.
    let mut seed = 0u32;
    for k in 1..=7u32 {
        for _ in 0..k {
            counted_win(&conn, seed, &identity(k), &key(k), 1_000 + seed as i64, Some(1));
            seed += 1;
        }
    }
    let page_size = 3;
    let mut after = 0;
    let mut walked: Vec<(String, i64)> = Vec::new();
    loop {
        let (rows, truncated) = page(&conn, None, page_size, after);
        walked.extend(rows.iter().map(|(o, _, w)| (o.clone(), *w)));
        match leaderboard_next_after(after, page_size, truncated) {
            Some(next) => after = next,
            None => break,
        }
    }
    let expected: Vec<(String, i64)> = (1..=7u32)
        .rev()
        .map(|k| (identity(k), i64::from(k)))
        .collect();
    assert_eq!(walked, expected, "rank order, no overlap, no gap, three pages");
    assert_eq!(after, 6, "two truncated pages then the last");
}

/// The landing bar is the fold's `is_confirmed_landing`, in SQL: confirmed
/// counts; unconfirmed counts ONLY with the overlay's own witness AND
/// bytes-finality; a stale verdict (pointer moved) or a non-winner verdict
/// never counts.
#[test]
fn the_landing_bar_is_is_confirmed_landing_and_the_verdict_must_be_fresh() {
    let conn = production_schema_db();
    let id = identity(1);
    let k = key(1);
    let mk = |seed: u32| {
        let pot = h64(100 + seed);
        let spender = h64(200 + seed);
        admit_pot(&conn, &pot, 1_000 + i64::from(seed));
        (pot, spender)
    };
    // 1: confirmed → counted.
    let (p1, s1) = mk(1);
    mark_spent_confirmed(&conn, &p1, &s1);
    set_verdict(&conn, &p1, &s1, "winner-a", &k, &key(9), None);
    file_potparty(&conn, &id, &p1, &k, Some(1), &h64(301));
    // 2: unconfirmed, unwitnessed → NOT counted.
    let (p2, s2) = mk(2);
    mark_spent_unconfirmed(&conn, &p2, &s2, Some(1), false);
    set_verdict(&conn, &p2, &s2, "winner-a", &k, &key(9), None);
    file_potparty(&conn, &id, &p2, &k, Some(1), &h64(302));
    // 3: unconfirmed, witnessed + final → counted.
    let (p3, s3) = mk(3);
    mark_spent_unconfirmed(&conn, &p3, &s3, Some(1), true);
    set_verdict(&conn, &p3, &s3, "winner-a", &k, &key(9), None);
    file_potparty(&conn, &id, &p3, &k, Some(1), &h64(303));
    // 4: unconfirmed, witnessed but NON-final (a parked refund shape) → NOT.
    let (p4, s4) = mk(4);
    mark_spent_unconfirmed(&conn, &p4, &s4, Some(0), true);
    set_verdict(&conn, &p4, &s4, "winner-a", &k, &key(9), None);
    file_potparty(&conn, &id, &p4, &k, Some(1), &h64(304));
    // 5: confirmed, but the verdict was written for a DIFFERENT spender
    //    (pointer moved after classification) → stale → NOT counted.
    let (p5, s5) = mk(5);
    mark_spent_confirmed(&conn, &p5, &s5);
    set_verdict(&conn, &p5, &s5, "winner-a", &k, &key(9), None);
    conn.execute(
        "UPDATE pot_records SET spendingTxid = ?1 WHERE txid = ?2",
        params![h64(555), p5],
    )
    .unwrap();
    file_potparty(&conn, &id, &p5, &k, Some(1), &h64(305));
    // 6: confirmed tie → NOT counted (attributes nobody).
    let (p6, s6) = mk(6);
    mark_spent_confirmed(&conn, &p6, &s6);
    set_verdict(&conn, &p6, &s6, "tie", &k, &key(9), None);
    file_potparty(&conn, &id, &p6, &k, Some(1), &h64(306));

    let (rows, _) = page(&conn, None, 10, 0);
    assert_eq!(rows, vec![(id.clone(), false, 2)], "pots 1 and 3 only");
    let pots = owners_pots(&conn, None, &[id.as_str()]);
    let mut got: Vec<String> = pots.iter().map(|p| p.pot_txid.clone()).collect();
    got.sort();
    let mut want = vec![p1, p3];
    want.sort();
    assert_eq!(got, want);
}

/// The SQL attribution is `attribute_seats`' slot rule on the LATCHED
/// column: one verified identity on the winning key ⇒ that identity; an
/// unlatched (NULL) row, a foreign key, a conflicting pair, or a degenerate
/// lock ⇒ the win counts under the SETTLE KEY (`identityIsKey`), never
/// dropped. Cross-checked against `attribute_seats` itself.
#[test]
fn attribution_in_sql_matches_attribute_seats_slot_rule() {
    let conn = production_schema_db();
    let k_win = key(1);
    let k_other = key(2);
    let honest = identity(1);
    let rival = identity(2);

    // A: latched identity → owner = identity.
    let (pa, _) = counted_win(&conn, 1, &honest, &k_win, 1_001, Some(1));
    // B: unlatched (NULL) row → owner = key.
    let (pb, _) = counted_win(&conn, 2, &honest, &k_win, 1_002, None);
    // C: two DIFFERENT verified identities on the winning key → poisoned → key.
    let (pc, _) = counted_win(&conn, 3, &honest, &k_win, 1_003, Some(1));
    file_potparty(&conn, &rival, &pc, &k_win, Some(1), &h64(40_003));
    // D: a verified row under a FOREIGN key only → key.
    let pd = h64(10_004);
    let sd = h64(20_004);
    admit_pot(&conn, &pd, 1_004);
    mark_spent_confirmed(&conn, &pd, &sd);
    set_verdict(&conn, &pd, &sd, "winner-a", &k_win, &k_other, None);
    file_potparty(&conn, &honest, &pd, &key(777), Some(1), &h64(40_004));
    // E: degenerate lock pubA == pubB → key even with a verified row.
    let pe = h64(10_005);
    let se = h64(20_005);
    admit_pot(&conn, &pe, 1_005);
    mark_spent_confirmed(&conn, &pe, &se);
    set_verdict(&conn, &pe, &se, "winner-a", &k_win, &k_win, None);
    file_potparty(&conn, &honest, &pe, &k_win, Some(1), &h64(40_005));
    // F: the SAME verified identity twice (a replayed marker) → identity.
    let (pf, _) = counted_win(&conn, 6, &honest, &k_win, 1_006, Some(1));
    file_potparty(&conn, &honest, &pf, &k_win, Some(1), &h64(40_006));
    // G: winner-b, latched on pubB → identity (the other slot).
    let pg = h64(10_007);
    let sg = h64(20_007);
    admit_pot(&conn, &pg, 1_007);
    mark_spent_confirmed(&conn, &pg, &sg);
    set_verdict(&conn, &pg, &sg, "winner-b", &k_other, &k_win, None);
    file_potparty(&conn, &honest, &pg, &k_win, Some(1), &h64(40_007));

    let (rows, _) = page(&conn, None, 10, 0);
    assert_eq!(
        rows,
        vec![(k_win.clone(), true, 4), (honest.clone(), false, 3)],
        "B, C, D, E under the key; A, F, G under the identity"
    );

    // The fold's attribution input, from the same rows, agrees with
    // `attribute_seats` on every latched case.
    let pots = owners_pots(&conn, None, &[honest.as_str(), k_win.as_str()]);
    let attr = attributions_from_pot_rows(&pots);
    let params = |a: &str, b: &str| CovenantParams {
        pub_a: hex33(a),
        pub_b: hex33(b),
        pub_tower: [0u8; 33],
        pay_pkh_a: [0u8; 20],
        pay_pkh_b: [0u8; 20],
        rake_pkh: [0u8; 20],
        stake_a: 0,
        stake_b: 0,
        fee_sats: 0,
        recovery_height: 0,
    };
    let marker = |id: &str, pot: &str, pk: &str| SeatMarkerRow {
        identity: id.to_string(),
        opponent_identity: id.to_string(),
        game_id: h64(7),
        pot_txid: pot.to_string(),
        pot_vout: 0,
        recovery_height: 900_000,
        seat_settle_pubkey: pk.to_string(),
        seat_sig_hex: "seatsig".into(),
        identity_sig_hex: "idsig".into(),
        sig_valid: Some(true),
    };
    let via_fold = attribute_seats(&params(&k_win, &key(999)), &pa, 0, &[marker(&honest, &pa, &k_win)]);
    assert_eq!(attr[&pa].identity_a, via_fold.identity_a);
    assert_eq!(attr[&pa].identity_a.as_deref(), Some(honest.as_str()));
    let via_fold = attribute_seats(
        &params(&k_win, &key(999)),
        &pc,
        0,
        &[marker(&honest, &pc, &k_win), marker(&rival, &pc, &k_win)],
    );
    assert_eq!(attr[&pc].identity_a, via_fold.identity_a);
    assert_eq!(attr[&pc].identity_a, None, "conflict poisons the slot both ways");
    let via_fold = attribute_seats(&params(&k_win, &k_win), &pe, 0, &[marker(&honest, &pe, &k_win)]);
    assert_eq!(attr[&pe].identity_a, via_fold.identity_a);
    assert_eq!(attr[&pe].identity_a, None, "degenerate lock attributes nobody");
    assert_eq!(attr[&pb].identity_a, None, "unlatched row: the key, until the relatch sweep");
}

fn hex33(h: &str) -> [u8; 33] {
    let v = hex::decode(h).unwrap();
    let mut out = [0u8; 33];
    out.copy_from_slice(&v);
    out
}

/// The pure-SQL hand score IS `hand_score`: every card, then random hands.
#[test]
fn sql_hand_score_matches_hand_score() {
    let conn = production_schema_db();
    let expr = sql_hand_score_expr("?1");
    let sql = format!("SELECT ({expr}) AS s");
    let mut stmt = conn.prepare(&sql).unwrap();
    let mut score = |cards: &[u8; 5]| -> i64 {
        let hexs = hex::encode(cards);
        stmt.query_row(params![hexs], |r| r.get::<_, i64>("s")).unwrap()
    };
    for c in 0u8..52 {
        let hand = [c; 5];
        assert_eq!(score(&hand), i64::from(hand_score(&hand)), "card {c}");
    }
    // Deterministic LCG "random" distinct hands.
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for _ in 0..300 {
        let mut hand = [0u8; 5];
        let mut i = 0;
        while i < 5 {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let c = ((x >> 33) % 52) as u8;
            if !hand[..i].contains(&c) {
                hand[i] = c;
                i += 1;
            }
        }
        assert_eq!(score(&hand), i64::from(hand_score(&hand)), "{hand:?}");
    }
    // An over-range byte scores 0 in SQL (it can only lose its own hand):
    // four '2's and a 0 ⇒ 8 (the fold re-decodes and drops such a marker).
    let bad = [60u8, 0, 0, 0, 0];
    assert_eq!(score(&bad), 8);
    // Uppercase hex is accepted (lower() in the expression).
    let up: i64 = stmt
        .query_row(params!["0001020304".to_ascii_uppercase()], |r| r.get("s"))
        .unwrap();
    assert_eq!(up, i64::from(hand_score(&[0, 1, 2, 3, 4])));
}

/// The hands board is ERA-WIDE: the lowest verified winner claims of
/// counted, identity-resolved pots — a latched-invalid claim, a key-keyed
/// win, an unanchored settle and a cardless claim never rank, whatever
/// their score.
#[test]
fn hands_are_the_era_wide_lowest_verified_winner_claims() {
    let conn = production_schema_db();
    let id = identity(1);
    let k = key(1);
    // Scores: cards 0..4 → 20; 0,1,2,3,12 → 15; 8..12 → 41.
    let (p20, s20) = counted_win(&conn, 1, &id, &k, 1_001, Some(1));
    file_result(&conn, &h64(51), &id, &identity(9), &p20, &s20, Some("0001020304"), Some(2), &h64(61), 10);
    let (p15, s15) = counted_win(&conn, 2, &id, &k, 1_002, Some(1));
    file_result(&conn, &h64(52), &id, &identity(9), &p15, &s15, Some("000102030c"), Some(1), &h64(62), 11);
    let (p41, s41) = counted_win(&conn, 3, &id, &k, 1_003, Some(1));
    file_result(&conn, &h64(53), &id, &identity(9), &p41, &s41, Some("08090a0b0c"), Some(2), &h64(63), 12);
    // A LOWER hand that must never rank: claimValid 0 (latched invalid).
    let (pbad, sbad) = counted_win(&conn, 4, &id, &k, 1_004, Some(1));
    file_result(&conn, &h64(54), &id, &identity(9), &pbad, &sbad, Some("0c0d1a2733"), Some(0), &h64(64), 13);
    // A low hand on a KEY-keyed win (unlatched potparty) → never ranks.
    let (pkey, skey) = counted_win(&conn, 5, &id, &k, 1_005, None);
    file_result(&conn, &h64(55), &id, &identity(9), &pkey, &skey, Some("0c0d1a2733"), Some(2), &h64(65), 14);
    // A low hand whose settle does not match the recorded spend → never.
    let (pun, _sun) = counted_win(&conn, 6, &id, &k, 1_006, Some(1));
    file_result(&conn, &h64(56), &id, &identity(9), &pun, &h64(777), Some("0c0d1a2733"), Some(2), &h64(66), 15);
    // A cardless claim → never.
    let (pnc, snc) = counted_win(&conn, 7, &id, &k, 1_007, Some(1));
    file_result(&conn, &h64(57), &id, &identity(9), &pnc, &snc, None, Some(2), &h64(67), 16);

    let got = hands(&conn, None, 2);
    assert_eq!(
        got,
        vec![(h64(52), "000102030c".to_string()), (h64(51), "0001020304".to_string())],
        "15 then 20; 41 cut by the limit; the four decoys never rank"
    );
    let all = hands(&conn, None, 10);
    assert_eq!(all.len(), 3);
    assert_eq!(all[2].0, h64(53));
}

/// The #375 era cutoff rides the spine on the pot's own admission stamp.
#[test]
fn the_era_cutoff_filters_the_spine_and_the_hands() {
    let conn = production_schema_db();
    let id = identity(1);
    let k = key(1);
    let (old_pot, old_s) = counted_win(&conn, 1, &id, &k, 1_000, Some(1));
    file_result(&conn, &h64(51), &id, &identity(9), &old_pot, &old_s, Some("0001020304"), Some(2), &h64(61), 10);
    let (new_pot, new_s) = counted_win(&conn, 2, &id, &k, 5_000, Some(1));
    file_result(&conn, &h64(52), &id, &identity(9), &new_pot, &new_s, Some("08090a0b0c"), Some(2), &h64(62), 11);
    let cutoff_ms = Some(3_000i64 * 1000);
    let (rows, _) = page(&conn, cutoff_ms, 10, 0);
    assert_eq!(rows, vec![(id.clone(), false, 1)]);
    let pots = owners_pots(&conn, cutoff_ms, &[id.as_str()]);
    assert_eq!(pots.len(), 1);
    assert_eq!(pots[0].pot_txid, new_pot);
    let h = hands(&conn, cutoff_ms, 10);
    assert_eq!(h, vec![(h64(52), "08090a0b0c".to_string())], "the old-era 20 is gone; only the new-era 41 remains");
    let (rows, _) = page(&conn, None, 10, 0);
    assert_eq!(rows, vec![(id, false, 2)], "no cutoff ⇒ both");
}

/// The per-pot marker decoration query keeps the oldest RESULT_ROWS_PER_POT
/// rows per pot and nothing else — bounded by the page's pots.
#[test]
fn pot_markers_are_bounded_per_pot_oldest_first() {
    let conn = production_schema_db();
    let pot = h64(1);
    let per_pot = overlay_discovery::result::storage::RESULT_ROWS_PER_POT;
    for i in 0..(per_pot as u32 + 3) {
        file_result(&conn, &h64(100 + i), &identity(1), &identity(2), &pot, &h64(9), None, Some(1), &h64(200 + i), 1_000 + i64::from(i));
    }
    // A marker for ANOTHER pot never rides.
    file_result(&conn, &h64(999), &identity(1), &identity(2), &h64(2), &h64(9), None, Some(1), &h64(998), 1);
    let mut stmt = conn.prepare(&pot_markers_sql(1)).unwrap();
    let rows: Vec<(String, i64)> = stmt
        .query_map(params![pot], |r| Ok((r.get("gameId")?, r.get("createdAt")?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), per_pot);
    let ats: Vec<i64> = rows.iter().map(|r| r.1).collect();
    assert_eq!(ats, (0..per_pot as i64).map(|i| 1_000 + i).collect::<Vec<_>>(), "oldest first, later spam cut");
}
