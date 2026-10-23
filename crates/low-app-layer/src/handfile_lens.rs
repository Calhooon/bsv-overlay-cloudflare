//! FAST-C-LENS (2026-10-23): the lens's negative cases over PR 151's `handfile` family (bsv-low #848, #461
//! design C). The original lens cases, with regression coverage for the FAST-C-FOLD fixes.
//!
//! The questions a forger asks (the lane's brief), each a test below:
//!   a. can a batch signed by identity X explode a row that names identity Y, or bind Y's settle key to X?
//!   b. can the same marker's bytes be filed twice under two batches with different content keys, and is a
//!      re-filing of the same batch the no-op the wire doc promises?
//!   c. does a batch with one refusable payload write nothing?
//!   d. can a poster exceed a family's cap by batching?
//!   e. is the served `batch` absent for a direct filing, and consistent when a direct filing and a batch share
//!      a row?
//!   f. a maximal refund admitted by the direct door also fits the handfile body cap.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::handfile::tests::{explode_under_sqlite, Hand, HOP_SATS, HOP_TXID, HOP_VOUT};
use crate::handfile::*;
use crate::record_post::tests::{junk_refund_raw, pot_ctx, sign, w25c_db};
use crate::record_post::{
    bind_refund_backup, RecordKind, RecordRefusal, VerifiedRecord, POTPARTY_FILE_SQL,
    POTREFUND_FILE_SQL, RECORD_FILINGS_PER_IDENTITY_PER_DAY, RECORD_POST_MAX_BODY_BYTES,
};
use overlay_discovery::hopparty::validity::expected_hop_lock_hex;
use overlay_discovery::potparty::validity::{potparty_protocol, potparty_v2_challenge};

fn settle_key(scalar: u8) -> bsv_rs::primitives::ec::PrivateKey {
    let mut b = [0u8; 32];
    b[31] = scalar;
    bsv_rs::primitives::ec::PrivateKey::from_bytes(&b).unwrap()
}

/// Execute the route's own cap queries and decision, with the exact binds, against real SQLite.
fn route_cap_failure(
    conn: &rusqlite::Connection,
    v: &VerifiedHandfile,
    write: &[bool; HANDFILE_PAYLOADS],
    day_floor: i64,
) -> Option<(&'static str, RecordRefusal)> {
    for query in handfile_cap_queries(v, write, day_floor) {
        let count = |(sql, binds): (&str, Vec<String>)| -> i64 {
            conn.query_row(sql, rusqlite::params_from_iter(binds.iter()), |r| r.get(0))
                .unwrap()
        };
        let rows = count(query.rows.clone());
        let day = count(query.day.clone());
        if let Some(r) = query.refusal(rows, day) {
            return Some((query.family, r));
        }
    }
    None
}

fn route_cap_verdict(conn: &rusqlite::Connection, v: &VerifiedHandfile) -> Option<RecordRefusal> {
    route_cap_failure(conn, v, &[true; HANDFILE_PAYLOADS], 0).map(|(_, r)| r)
}

fn hop_facts(hand: &Hand) -> HopFacts {
    HopFacts {
        lock_hex: expected_hop_lock_hex(&hand.settle_pub()).unwrap(),
        sats: HOP_SATS,
        outputs: 2,
    }
}

fn file_party(
    conn: &rusqlite::Connection,
    r: &overlay_discovery::potparty::storage::PotpartyRecord,
) {
    conn.execute(
        POTPARTY_FILE_SQL,
        rusqlite::params![
            r.identity,
            r.opponent_identity,
            r.game_id,
            r.pot_txid,
            r.pot_vout,
            r.recovery_height,
            r.sig_hex,
            r.seat_settle_pubkey,
            r.seat_sig_hex,
            r.txid,
            0,
            1_000i64,
            1,
        ],
    )
    .unwrap();
}

#[cfg(test)]
#[test]
fn fold_m1_direct_versions_and_daily_boundary_count_each_content_once() {
    let hand = Hand::new(28);
    let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
    for first in [&v.potparty_v1, &v.potparty_v2] {
        let conn = w25c_db();
        file_party(&conn, first);
        assert_eq!(
            route_cap_verdict(&conn, &v),
            None,
            "either direct version may land first"
        );
    }
    let conn = w25c_db();
    for i in 0..198 {
        let mut row = v.potparty_v1.clone();
        row.txid = format!("filed:day-party-{i}");
        row.game_id = "another-game".into();
        file_party(&conn, &row);
    }
    assert_eq!(
        route_cap_verdict(&conn, &v),
        None,
        "198 plus the pair is exactly 200"
    );
    let mut extra = v.potparty_v1.clone();
    extra.txid = "filed:one-more".into();
    extra.game_id = "another-game".into();
    file_party(&conn, &extra);
    assert_eq!(
        route_cap_failure(&conn, &v, &[true; HANDFILE_PAYLOADS], 0),
        Some((HANDFILE_FAMILY_POTPARTY, RecordRefusal::DailyCapReached)),
        "199 plus the pair exceeds 200",
    );
    conn.execute(
        "DELETE FROM potparty_records WHERE txid = ?1",
        [&extra.txid],
    )
    .unwrap();
    file_party(&conn, &v.potparty_v1);
    assert_eq!(
        route_cap_verdict(&conn, &v),
        None,
        "199 plus the missing v2 is exactly 200"
    );
    file_party(&conn, &v.potparty_v2);
    assert_eq!(
        route_cap_verdict(&conn, &v),
        None,
        "a re-file adds nothing to the full day"
    );
    // An older landed row is neither charged as new nor counted in today's window.
    let queries = handfile_cap_queries(&v, &[true; HANDFILE_PAYLOADS], 1_001);
    let party = queries
        .iter()
        .find(|q| q.family == HANDFILE_FAMILY_POTPARTY)
        .unwrap();
    let day: i64 = conn
        .query_row(party.day.0, rusqlite::params_from_iter(&party.day.1), |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(day, 0);
}

#[test]
fn fold_m1_chain_noops_reserve_only_active_slots_and_count_other_landed_rows() {
    let conn = w25c_db();
    let hand = Hand::new(29);
    let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
    let mut other = v.potparty_v1.clone();
    other.txid = "filed:other-content".into();
    file_party(&conn, &other);
    let only_v2 = [false, false, true, false];
    assert_eq!(route_cap_failure(&conn, &v, &only_v2, 0), None);
    // Even if the chain now covers v1, its older filed row still occupies a place.
    file_party(&conn, &v.potparty_v1);
    assert_eq!(
        route_cap_failure(&conn, &v, &only_v2, 0),
        Some((HANDFILE_FAMILY_POTPARTY, RecordRefusal::TooManyFiled)),
    );
    assert!(handfile_cap_queries(&v, &[false; HANDFILE_PAYLOADS], 0).is_empty());
}

#[test]
fn fold_l2_noop_refile_and_refund_promotion_have_different_write_outcomes() {
    let conn = w25c_db();
    let hand = Hand::new(30);
    let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
    assert!(explode_under_sqlite(&conn, &v, &hop_facts(&hand), false) > 0);
    let changes = explode_under_sqlite(&conn, &v, &hop_facts(&hand), false);
    assert_eq!(changes, 0, "an identical re-file changes no row or trigger");
    let first = handfile_answer(&v, false, true);
    let retry = handfile_answer(&v, false, changes > 0);
    assert_eq!(retry["filed"], false);
    assert_eq!(retry["alreadyFiled"], true);
    assert_eq!(retry["key"], first["key"]);
    assert_eq!(retry["rows"], first["rows"]);
    assert!(
        explode_under_sqlite(&conn, &v, &hop_facts(&hand), true) > 0,
        "a refund promotion is a real change"
    );
    let rank: i64 = conn
        .query_row(
            "SELECT refundValid FROM potrefund_records WHERE txid = ?1",
            [&v.potrefund.txid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rank, 1);
    assert_eq!(explode_under_sqlite(&conn, &v, &hop_facts(&hand), true), 0);
}

#[test]
fn fold_l5_cap_bodies_name_each_family_and_keep_the_direct_error_shape() {
    for family in [
        HANDFILE_FAMILY_HOPPARTY,
        HANDFILE_FAMILY_POTPARTY,
        HANDFILE_FAMILY_POTREFUND,
    ] {
        for refusal in [RecordRefusal::TooManyFiled, RecordRefusal::DailyCapReached] {
            let body = crate::record_post::record_refusal_body(refusal, Some(family));
            assert_eq!(body["family"], family);
            assert_eq!(body["error"], refusal.as_str());
            assert_eq!(
                crate::record_post::record_refusal_body(refusal, None),
                serde_json::json!({ "error": refusal.as_str() }),
            );
        }
    }
}

#[test]
fn fold_l1_der_then_parsers_then_consistency_then_poster_then_signatures() {
    let hand = Hand::new(31);
    let mut body = hand.body();
    body.payloads[0] = "00".into();
    body.identity_sig_hex = "00".into();
    assert_eq!(
        verify_handfile(&body, &hand.poster()).unwrap_err(),
        RecordRefusal::SignatureInvalid
    );
    body.identity_sig_hex = hand.body().identity_sig_hex;
    body.settle_sig_hex = "00".into();
    assert_eq!(
        verify_handfile(&body, &hand.poster()).unwrap_err(),
        RecordRefusal::SettleSignatureInvalid
    );
    body.settle_sig_hex = hand.body().settle_sig_hex;
    assert_eq!(
        verify_handfile(&body, "stranger").unwrap_err(),
        RecordRefusal::HandfilePayloadNotAMarker
    );
    let mut p = hand.payloads();
    p[1][15 + 98] ^= 0xff;
    body = hand.body_of(&p);
    assert_eq!(
        verify_handfile(&body, "stranger").unwrap_err(),
        RecordRefusal::HandfileInconsistent
    );
    body = hand.body();
    assert_eq!(
        verify_handfile(&body, "stranger").unwrap_err(),
        RecordRefusal::PosterMismatch
    );
    // Canonical signatures over another digest reach ECDSA only after the earlier checks pass.
    let other = Hand::new(32).body();
    body.identity_sig_hex = other.identity_sig_hex;
    body.settle_sig_hex = other.settle_sig_hex;
    assert_eq!(
        verify_handfile(&body, &hand.poster()).unwrap_err(),
        RecordRefusal::SignatureInvalid
    );
    body.identity_sig_hex = hand.body().identity_sig_hex;
    assert_eq!(
        verify_handfile(&body, &hand.poster()).unwrap_err(),
        RecordRefusal::SettleSignatureInvalid
    );
}

/// (a) A batch binds ONE identity to ONE settle key, and only the holder of both can file it.
#[test]
fn lens_a_a_batch_cannot_bind_another_identitys_hop_or_key() {
    // X forges a batch whose every payload names Y (consistent), signs it with X's identity key and X's settle key
    // X's opponent is Hand 22's identity; Y is a third identity, so the forged batch is not self-paired
    let x = Hand::new(21);
    let y = Hand::new(40);
    let y_id = y.identity();
    let mut p = x.payloads();
    p[0][15..48].copy_from_slice(&y_id);
    p[1][15..48].copy_from_slice(&y_id);
    p[2][15..48].copy_from_slice(&y_id);
    p[3][16..49].copy_from_slice(&y_id);
    let forged = x.body_of(&p);
    assert_eq!(
        verify_handfile(&forged, &x.poster()).unwrap_err(),
        RecordRefusal::PosterMismatch,
        "posted as X: the payloads name Y"
    );
    assert_eq!(
        verify_handfile(&forged, &y.poster()).unwrap_err(),
        RecordRefusal::SignatureInvalid,
        "posted as Y (an anonymous ?identity=Y): the identity signature is X's"
    );
    // X names its own identity but Y's settle key in both the hop and the v2 payload: X cannot sign for K_Y
    let mut p = x.payloads();
    let k_y = y.settle_pub();
    p[0][15 + 110..15 + 143].copy_from_slice(&k_y);
    p[2][15 + 138..15 + 171].copy_from_slice(&k_y);
    let stolen_key = x.body_of(&p); // the settle signature is by K_X
    assert_eq!(
        verify_handfile(&stolen_key, &x.poster()).unwrap_err(),
        RecordRefusal::SettleSignatureInvalid
    );
    // the hop payload names K_Y while the v2 payload names K_X (X holds both signatures' keys for K_X only)
    let mut p = x.payloads();
    p[0][15 + 110..15 + 143].copy_from_slice(&k_y);
    assert_eq!(
        verify_handfile(&x.body_of(&p), &x.poster()).unwrap_err(),
        RecordRefusal::HandfileInconsistent
    );
    // the hop payload names identity Y, the potparty payloads X
    let mut p = x.payloads();
    p[0][15..48].copy_from_slice(&y_id);
    assert_eq!(
        verify_handfile(&x.body_of(&p), &x.poster()).unwrap_err(),
        RecordRefusal::HandfileInconsistent
    );
    // the refund payload names identity Y, the rest X
    let mut p = x.payloads();
    p[3][16..49].copy_from_slice(&y_id);
    assert_eq!(
        verify_handfile(&x.body_of(&p), &x.poster()).unwrap_err(),
        RecordRefusal::HandfileInconsistent
    );
    // Y's honest batch re-signed by X's identity key over the same digest, posted as Y
    let mut resigned = y.body();
    let pre = y.preimage_of(&y.payloads());
    resigned.identity_sig_hex = hex::encode(sign(
        &x.w,
        potparty_protocol(),
        &hex::encode(y.game_id),
        &pre,
    ));
    assert_eq!(
        verify_handfile(&resigned, &y.poster()).unwrap_err(),
        RecordRefusal::SignatureInvalid
    );
    // the honest batch, as a control
    assert!(verify_handfile(&x.body(), &x.poster()).is_ok());
}

/// (b) Two batches that carry one marker's bytes (the same v1 and v2, another refund raw) share that marker's row
/// under the one content key; the row's served batch is the FIRST batch, whose payload at `index` is still the
/// row's own bytes. And a re-filing of the SAME batch must be the no-op the wire doc promises: the route's cap
/// arithmetic transcribed over the shipped statements.
#[test]
fn lens_b_one_marker_under_two_batches_is_one_row_and_a_refile_is_not_refused() {
    let conn = w25c_db();
    let hand = Hand::new(23);
    let first = verify_handfile(&hand.body(), &hand.poster()).unwrap();
    explode_under_sqlite(&conn, &first, &hop_facts(&hand), false);
    // a second batch: the same hop, v1 and v2 payloads, another refund raw that spends the pot
    let mut p = hand.payloads();
    p[3].truncate(POTREFUND_PAYLOAD_HEAD);
    p[3].extend_from_slice(&junk_refund_raw(&hand.pot_txid, hand.pot_vout));
    let second = verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap();
    assert_ne!(second.key, first.key);
    assert_eq!(
        second.potparty_v2.txid, first.potparty_v2.txid,
        "one marker, one key"
    );
    assert_eq!(second.potparty_v1.txid, first.potparty_v1.txid);
    assert_ne!(second.potrefund.txid, first.potrefund.txid);
    // the second batch's refund is another content for the pot: the potrefund cap (1) refuses the whole batch,
    // exactly as the direct door refuses a second refund content
    assert_eq!(
        route_cap_verdict(&conn, &second),
        Some(RecordRefusal::TooManyFiled),
        "a second refund content is refused by the refund family's cap"
    );
    assert_eq!(
        route_cap_failure(&conn, &second, &[true; HANDFILE_PAYLOADS], 0),
        Some((HANDFILE_FAMILY_POTREFUND, RecordRefusal::TooManyFiled)),
    );
    // were it written (the statements alone), the shared rows stay one row each and point at the FIRST batch
    explode_under_sqlite(&conn, &second, &hop_facts(&hand), false);
    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert_eq!(count("SELECT COUNT(*) FROM potparty_records"), 2);
    assert_eq!(count("SELECT COUNT(*) FROM hopparty_records"), 1);
    assert_eq!(count("SELECT COUNT(*) FROM potrefund_records"), 2);
    assert_eq!(count("SELECT COUNT(*) FROM handfile_batches"), 2);
    assert_eq!(count("SELECT COUNT(*) FROM handfile_rows"), 5);
    let (batch_key, idx, payloads_json): (String, i64, String) = conn
        .query_row(
            HANDFILE_BATCH_FOR_ROW_SQL,
            rusqlite::params![HANDFILE_FAMILY_POTPARTY, first.potparty_v2.txid, 0],
            |r| Ok((r.get(3)?, r.get(2)?, r.get(5)?)),
        )
        .unwrap();
    assert_eq!(batch_key, first.key, "the row's batch is the first batch");
    let payloads: Vec<String> = serde_json::from_str(&payloads_json).unwrap();
    assert_eq!(
        payloads[idx as usize],
        hex::encode(&hand.payloads()[2]),
        "inclusion holds"
    );

    // THE RE-FILING. A fresh database: the batch landed once; the client's outbox re-files it (a retry after a
    // timeout, a second device). The wire doc: "a re-filing is a no-op", "the same answer". The route's cap
    // arithmetic over the landed rows:
    let conn = w25c_db();
    explode_under_sqlite(&conn, &first, &hop_facts(&hand), false);
    assert_eq!(
        route_cap_verdict(&conn, &first),
        None,
        "a re-filing of the same batch is the no-op the wire doc promises, never a cap refusal"
    );
}

/// (c) One refusable payload refuses the batch before anything is written: the refund that spends the pot but is
/// not its pre-signed spend (the index holds the pot), and the hop the index does not hold.
#[test]
fn lens_c_one_bad_payload_refuses_the_whole_batch_before_any_write() {
    let hand = Hand::new(24);
    let mut p = hand.payloads();
    p[3].truncate(POTREFUND_PAYLOAD_HEAD);
    p[3].extend_from_slice(&junk_refund_raw(&hand.pot_txid, hand.pot_vout));
    // the pure verifier lets it through (it spends the pot), as the direct door does before the bind
    let v = verify_handfile(&hand.body_of(&p), &hand.poster()).expect("spends the pot");
    // the bind over the indexed pot: not the pre-signed spend by both committed keys
    assert_eq!(
        bind_refund_backup(&v.potrefund, Some(&pot_ctx())).unwrap_err(),
        RecordRefusal::RefundNotThePresignedSpend
    );
    // an unindexed pot files at rank 0, as the direct door does
    assert_eq!(bind_refund_backup(&v.potrefund, None), Ok(false));
    // the honest refund over the indexed pot binds at rank 1
    let honest = verify_handfile(&hand.body(), &hand.poster()).unwrap();
    assert_eq!(
        bind_refund_backup(&honest.potrefund, Some(&pot_ctx())),
        Ok(true)
    );
    // the hop: a BEEF that does not carry the hop txid reads as no facts (the route's 425)
    let other = {
        let mut tx = bsv_rs::transaction::Transaction::new();
        tx.add_input(bsv_rs::transaction::TransactionInput {
            source_txid: Some(hex::encode([0x11u8; 32])),
            source_output_index: 0,
            unlocking_script: Some(bsv_rs::script::UnlockingScript::from_hex("00").unwrap()),
            sequence: 0xffff_ffff,
            ..Default::default()
        })
        .unwrap();
        tx.add_output(bsv_rs::transaction::TransactionOutput {
            satoshis: Some(1),
            locking_script: bsv_rs::script::LockingScript::from_hex("51").unwrap(),
            change: false,
        })
        .unwrap();
        let mut beef = bsv_rs::transaction::Beef::new();
        beef.merge_transaction(tx);
        beef.to_binary()
    };
    assert!(hop_facts_of_beef(&other, &hex::encode(HOP_TXID), HOP_VOUT).is_none());
    assert!(hop_facts_of_beef(b"not a beef", &hex::encode(HOP_TXID), HOP_VOUT).is_none());
    // the hop paying another key the sats named: refuted
    let mut facts = hop_facts(&hand);
    facts.lock_hex = expected_hop_lock_hex(&settle_key(200).public_key().to_compressed()).unwrap();
    assert!(!hop_facts_agree(&honest.hopparty, &facts));
}

/// (d) A poster cannot exceed a family's cap by batching: the exploded rows are counted exactly as direct rows.
#[cfg(test)]
#[test]
fn lens_d_a_poster_cannot_exceed_a_family_cap_by_batching() {
    let conn = w25c_db();
    let hand = Hand::new(25);
    let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
    // two DIRECT potparty filings of other contents for the same (poster, game, pot) fill the potparty cap (2)
    for (i, key) in ["filed:direct-one", "filed:direct-two"].iter().enumerate() {
        let r = &v.potparty_v1;
        conn.execute(
            POTPARTY_FILE_SQL,
            rusqlite::params![
                r.identity,
                r.opponent_identity,
                r.game_id,
                r.pot_txid,
                r.pot_vout,
                r.recovery_height + 1 + i as u32,
                r.sig_hex,
                Option::<String>::None,
                Option::<String>::None,
                key,
                0,
                1_000i64,
                1
            ],
        )
        .unwrap();
    }
    assert_eq!(
        route_cap_verdict(&conn, &v),
        Some(RecordRefusal::TooManyFiled),
        "the batch's potparty rows are counted against the direct rows"
    );
    // ONE direct row of another content leaves one place: the batch needs two (v1 and v2) and is refused too
    let conn = w25c_db();
    let r = &v.potparty_v1;
    conn.execute(
        POTPARTY_FILE_SQL,
        rusqlite::params![
            r.identity,
            r.opponent_identity,
            r.game_id,
            r.pot_txid,
            r.pot_vout,
            r.recovery_height + 7,
            r.sig_hex,
            Option::<String>::None,
            Option::<String>::None,
            "filed:direct-other",
            0,
            1_000i64,
            1
        ],
    )
    .unwrap();
    assert_eq!(
        route_cap_verdict(&conn, &v),
        Some(RecordRefusal::TooManyFiled)
    );
    // the day cap: 200 filed refund rows of the poster today, then the batch's refund slot is refused
    let conn = w25c_db();
    for i in 0..RECORD_FILINGS_PER_IDENTITY_PER_DAY {
        let r = &v.potrefund;
        conn.execute(
            POTREFUND_FILE_SQL,
            rusqlite::params![
                r.identity,
                hex::encode([i as u8; 32]),
                hex::encode([0x99u8; 32]),
                0,
                "00",
                r.sig_hex,
                format!("filed:day-{i}"),
                0,
                1_000i64,
                0
            ],
        )
        .unwrap();
    }
    assert_eq!(
        route_cap_verdict(&conn, &v),
        Some(RecordRefusal::DailyCapReached),
        "the day's 200 per family counts the batch's refund row"
    );
    assert_eq!(
        route_cap_failure(&conn, &v, &[true; HANDFILE_PAYLOADS], 0),
        Some((HANDFILE_FAMILY_POTREFUND, RecordRefusal::DailyCapReached)),
    );
    conn.execute(
        "DELETE FROM potrefund_records WHERE txid = ?1",
        ["filed:day-199"],
    )
    .unwrap();
    assert_eq!(
        route_cap_verdict(&conn, &v),
        None,
        "199 refunds leave room for one"
    );
    explode_under_sqlite(&conn, &v, &hop_facts(&hand), false);
    assert_eq!(
        route_cap_verdict(&conn, &v),
        None,
        "the 200th refund can be re-filed"
    );
    // The hop cap is per (poster, hop outpoint): a second game cannot add another row for that hop.
    let conn = w25c_db();
    explode_under_sqlite(&conn, &v, &hop_facts(&hand), false);
    let mut other_game = Hand::new(25);
    other_game.game_id = [0xeeu8; 32];
    let v2 = verify_handfile(&other_game.body(), &other_game.poster()).unwrap();
    let n: i64 = conn
        .query_row(
            HOPPARTY_FILED_ROWS_SQL,
            rusqlite::params![v2.identity, v2.hop_txid, HOP_VOUT, v2.hop_row_vout],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1, "another game's row for the same hop is counted");
    assert_eq!(
        route_cap_failure(&conn, &v2, &[true; HANDFILE_PAYLOADS], 0),
        Some((HANDFILE_FAMILY_HOPPARTY, RecordRefusal::TooManyFiled)),
    );
    let n: i64 = conn
        .query_row(
            HOPPARTY_FILED_TODAY_SQL,
            rusqlite::params![v2.identity, 0i64],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1, "the day cap counts it");
}

/// (e) A direct filing carries no batch; a direct filing and a batch of one marker share the row, and the served
/// batch's payload at `index` is the row's own bytes whichever landed first.
#[test]
fn lens_e_a_direct_row_has_no_batch_and_a_shared_row_serves_a_consistent_one() {
    let conn = w25c_db();
    let hand = Hand::new(26);
    // the v2 marker filed DIRECTLY first (its own two signatures)
    let direct = crate::record_post::verify_record_post(
        RecordKind::Potparty,
        &hand.direct_v2_script(),
        &hand.poster(),
    )
    .unwrap();
    let VerifiedRecord::Potparty(direct, true) = direct else {
        panic!("a v2 filing")
    };
    conn.execute(
        POTPARTY_FILE_SQL,
        rusqlite::params![
            direct.identity,
            direct.opponent_identity,
            direct.game_id,
            direct.pot_txid,
            direct.pot_vout,
            direct.recovery_height,
            direct.sig_hex,
            direct.seat_settle_pubkey,
            direct.seat_sig_hex,
            direct.txid,
            0,
            900i64,
            1
        ],
    )
    .unwrap();
    assert!(
        conn.query_row(
            HANDFILE_BATCH_FOR_ROW_SQL,
            rusqlite::params![HANDFILE_FAMILY_POTPARTY, direct.txid, 0],
            |r| r.get::<_, String>(3),
        )
        .is_err(),
        "a direct filing has no batch"
    );
    // then the batch: the v2 row is the direct one (INSERT OR IGNORE keeps its signatures), pointed at the batch
    let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
    assert_eq!(v.potparty_v2.txid, direct.txid);
    explode_under_sqlite(&conn, &v, &hop_facts(&hand), false);
    let (sig_hex, seat_sig_hex, sig_valid): (String, String, i64) = conn
        .query_row(
            "SELECT sigHex, seatSigHex, sigValid FROM potparty_records WHERE txid = ?1",
            rusqlite::params![direct.txid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (sig_hex.as_str(), seat_sig_hex.as_str(), sig_valid),
        (
            direct.sig_hex.as_str(),
            direct.seat_sig_hex.as_deref().unwrap(),
            1
        )
    );
    let (batch_key, idx, payloads_json): (String, i64, String) = conn
        .query_row(
            HANDFILE_BATCH_FOR_ROW_SQL,
            rusqlite::params![HANDFILE_FAMILY_POTPARTY, direct.txid, 0],
            |r| Ok((r.get(3)?, r.get(2)?, r.get(5)?)),
        )
        .unwrap();
    assert_eq!(batch_key, v.key);
    let payloads: Vec<String> = serde_json::from_str(&payloads_json).unwrap();
    // the client's inclusion check: the payload at `index` is the row's canonical bytes rebuilt from its fields
    let rebuilt = potparty_v2_challenge(
        &hex::decode(&direct.identity).unwrap(),
        &hex::decode(&direct.opponent_identity).unwrap(),
        &hex::decode(&direct.game_id).unwrap(),
        &hex::decode(&direct.pot_txid).unwrap(),
        direct.pot_vout,
        direct.recovery_height,
        &hex::decode(direct.seat_settle_pubkey.as_deref().unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(payloads[idx as usize], hex::encode(rebuilt));
    // the potparty count is two (v1 new, v2 shared); a third direct content is then refused by the cap
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM potparty_records", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);
}

/// (f) The shared body cap admits the direct door's maximum refund raw in a handfile too.
/// Use maximum DER lengths for the JSON size bound, then verify a real signed maximal payload.
#[test]
fn lens_f_the_body_cap_admits_a_maximal_refund_the_direct_door_admits() {
    let hand = Hand::new(27);
    let body_len = |n: usize| {
        let mut p = hand.payloads();
        p[3].truncate(POTREFUND_PAYLOAD_HEAD);
        p[3].extend(std::iter::repeat_n(0u8, n));
        serde_json::json!({
            "hopTxid": hex::encode(HOP_TXID),
            "payloads": p.iter().map(hex::encode).collect::<Vec<_>>(),
            "identitySigHex": "30".repeat(72),
            "settleSigHex": "30".repeat(72),
        })
        .to_string()
        .len()
    };
    let max = overlay_discovery::potrefund::POTREFUND_REFUND_MAX_LEN;
    let at_max = body_len(max);
    assert!(
        at_max <= RECORD_POST_MAX_BODY_BYTES,
        "{at_max} > {RECORD_POST_MAX_BODY_BYTES}"
    );
    assert!(body_len(RECORD_POST_MAX_BODY_BYTES) > RECORD_POST_MAX_BODY_BYTES);
    let honest = body_len(hand.refund_raw.len());
    assert!(honest < 16_384, "{honest}");

    // A well-formed transaction that spends the named pot; an unindexed pot admits it at rank 0.
    let mut tx = bsv_rs::transaction::Transaction::from_binary(&junk_refund_raw(
        &hand.pot_txid,
        hand.pot_vout,
    ))
    .unwrap();
    let mut script_len = max - tx.to_binary().len() + 1;
    tx.inputs[0].unlocking_script =
        Some(bsv_rs::script::UnlockingScript::from_binary(&vec![0; script_len]).unwrap());
    tx.invalidate_caches();
    script_len -= tx.to_binary().len() - max;
    tx.inputs[0].unlocking_script =
        Some(bsv_rs::script::UnlockingScript::from_binary(&vec![0; script_len]).unwrap());
    tx.invalidate_caches();
    let raw = tx.to_binary();
    assert_eq!(raw.len(), max);
    let mut p = hand.payloads();
    p[3].truncate(POTREFUND_PAYLOAD_HEAD);
    p[3].extend_from_slice(&raw);
    let v = verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap();
    assert_eq!(hex::decode(v.potrefund.refund_raw_hex).unwrap().len(), max);
    p[3].push(0);
    assert_eq!(
        verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap_err(),
        RecordRefusal::HandfilePayloadNotAMarker,
    );
}
