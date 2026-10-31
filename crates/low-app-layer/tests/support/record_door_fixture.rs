//! The workerd fixture uses the same signing/building helpers as the door's crypto pins.
use super::{tests::*, *};
use crate::handfile::tests::Hand;
use serde_json::json;

struct PausedJudge<'a> {
    db: &'a crate::pot_hook::tests::SqliteSql,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl crate::pot_hook::SqlPort for PausedJudge<'_> {
    async fn run_batch(
        &self,
        stmts: Vec<crate::pot_hook::Stmt>,
    ) -> Result<Vec<crate::pot_hook::StmtOut>, String> {
        self.db.run_batch(stmts).await
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.db.counters(deltas).await;
    }
    fn now_ms(&self) -> u64 {
        self.db.now_ms()
    }
    fn log(&self, line: &str) {
        self.db.log(line);
    }
}
impl crate::owed_judge::JudgeDb for PausedJudge<'_> {
    fn tip(&self) -> Option<u64> {
        Some(900_000)
    }
    async fn canonical_root(&self, _: u64) -> Option<String> {
        None
    }
    async fn word_views(
        &self,
        rows: &std::collections::BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>, String> {
        self.db.word_views(rows).await
    }
    async fn push(&self, _: &str, _: serde_json::Value) {}
    async fn hydrate(
        &self,
        id: &str,
        games: &[String],
    ) -> Result<crate::owed_judge::JudgeInputs, String> {
        // Freeze an old snapshot after its generation read; a new filing lands before the old ack.
        let inputs = self.db.hydrate(id, games).await?;
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(inputs)
    }
}

#[tokio::test]
async fn a_second_filing_during_judgement_keeps_its_generation_for_the_drain() {
    let db = crate::pot_hook::tests::SqliteSql::migrated();
    let hand = Hand::new(19);
    let mut party = verify_record_post(
        RecordKind::Potparty,
        &hand.direct_v2_script(),
        &hand.poster(),
    )
    .unwrap();
    file_verified(&db, &mut party, &hand.poster(), 2_000)
        .await
        .unwrap();
    let paused = PausedJudge {
        db: &db,
        entered: Default::default(),
        resume: Default::default(),
    };
    let sig = sign(
        &hand.w,
        collected_protocol(),
        &hex::encode(hand.game_id),
        &collected_challenge(&hex::encode(hand.game_id), &hand.poster()),
    );
    let mut second = verify_record_post(
        RecordKind::Collected,
        &script(&[b"LOW/collected/v1", &hand.game_id, &hand.identity(), &sig]),
        &hand.poster(),
    )
    .unwrap();
    let post = async {
        paused.entered.notified().await;
        file_verified(&db, &mut second, &hand.poster(), 2_001)
            .await
            .unwrap();
        paused.resume.notify_one();
    };
    let (first, ()) = tokio::join!(
        super::detached::judge(&paused, std::slice::from_ref(&party), "test"),
        post
    );
    assert_eq!(first.unwrap().subjects, 1);
    let pending: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM owed_dirty WHERE source='collected'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        pending, 1,
        "the old snapshot cannot acknowledge the newer filing"
    );
    let drained = super::detached::judge(&db, std::slice::from_ref(&second), "test")
        .await
        .unwrap();
    assert_eq!(drained.subjects, 1);
    assert_eq!(
        db.conn
            .query_row("SELECT COUNT(*) FROM owed_dirty", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    // A cancelled judgement cannot acknowledge any mark either.
    db.conn.execute("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,?2,'pin','pin',1)",rusqlite::params![hand.poster(),hex::encode(hand.game_id)]).unwrap();
    let paused = PausedJudge {
        db: &db,
        entered: Default::default(),
        resume: Default::default(),
    };
    let started = std::time::Instant::now();
    let deadline = crate::pot_hook::Deadline::new(
        10,
        0,
        Box::new(move || started.elapsed().as_millis() as u64),
        Box::new(|ms| Box::pin(tokio::time::sleep(std::time::Duration::from_millis(ms)))),
    );
    assert!(deadline
        .run_reserving(super::detached::judge(
            &paused,
            std::slice::from_ref(&second),
            "test"
        ))
        .await
        .is_none());

    assert!(
        db.conn
            .query_row("SELECT COUNT(*) FROM owed_dirty", [], |r| r
                .get::<_, i64>(0))
            .unwrap()
            > 0
    );
}

#[test]
#[ignore = "exports the fleet SQLite and signed requests for the workerd record-door pin"]
fn export_record_door_fixture() {
    let path = std::env::var("RECORD_DOOR_FIXTURE").expect("fixture output outside the checkout");
    let sqlite = format!("{path}.sqlite");
    let db = crate::pot_hook::tests::SqliteSql::migrated();
    db.conn.execute_batch("BEGIN").unwrap();
    for sql in budget_fleet::rows(2_000)
        .into_iter()
        .chain(budget_fleet::owed_rows(2_000))
    {
        budget_fleet::execute_row(&db.conn, &sql);
    }
    db.conn
        .execute_batch("COMMIT; DELETE FROM owed_dirty")
        .unwrap();
    // D2-ACTOR-A-FOLD-4: certified as the overlay's boot certifies its list, so the app layer's publish gate
    // (`owed_events::configured`, the lens's LOW-1) opens and the door enqueues; an uncertified fixture kept the
    // inline judge (the cell's red `judge;desc=detached`, first reached once the route cell went green)
    db.conn
        .execute(
            bsv_overlay_cloudflare::d1::MIGRATION_CERTIFY_SQL,
            rusqlite::params![
                bsv_overlay_cloudflare::d1::OVERLAY_MIGRATION_COUNT as f64,
                bsv_overlay_cloudflare::d1::migration_list_fingerprint() as f64
            ],
        )
        .unwrap();
    let mut hand = Hand::new(15);
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
        satoshis: Some(crate::handfile::tests::HOP_SATS),
        locking_script: bsv_rs::script::LockingScript::from_hex(
            &overlay_discovery::hopparty::validity::expected_hop_lock_hex(&hand.settle_pub())
                .unwrap(),
        )
        .unwrap(),
        change: false,
    })
    .unwrap();
    tx.add_output(bsv_rs::transaction::TransactionOutput {
        satoshis: Some(1),
        locking_script: bsv_rs::script::LockingScript::from_hex("51").unwrap(),
        change: false,
    })
    .unwrap();
    hand.hop_txid = hex::decode(tx.id()).unwrap().try_into().unwrap();
    let mut beef = bsv_rs::transaction::Beef::new();
    beef.merge_transaction(tx);
    db.conn
        .execute(
            "INSERT INTO pot_beefs(txid,beef,createdAt) VALUES (?1,?2,2000)",
            rusqlite::params![hex::encode(hand.hop_txid), beef.to_binary()],
        )
        .unwrap();
    db.conn
        .execute(
            "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES (?1,0,0,2000)",
            [hex::encode(hand.hop_txid)],
        )
        .unwrap();
    let verified = crate::handfile::verify_handfile(&hand.body(), &hand.poster()).unwrap();
    let mut scenarios = vec![json!({
        "name":"handfile", "kind":"handfile", "identity":hand.poster(), "body":{"hopTxid":hand.body().hop_txid,"payloads":hand.body().payloads,"identitySigHex":hand.body().identity_sig_hex,"settleSigHex":hand.body().settle_sig_hex},
        "answer":crate::handfile::handfile_answer(&verified,false,true),
        "read":"SELECT COUNT(*) AS n FROM handfile_rows WHERE batchKey=?", "binds":[verified.key],
    })];
    let add = |scenarios: &mut Vec<serde_json::Value>,
               name: &str,
               kind: RecordKind,
               bytes: Vec<u8>,
               poster: String| {
        let v = verify_record_post(kind, &bytes, &poster).unwrap();
        let mut answer = json!({"filed":true,"kind":kind.as_str(),"key":v.key()});
        let table = match &v {
            VerifiedRecord::Potparty(..) => "potparty_records",
            VerifiedRecord::Potrefund(..) => {
                answer["refundValid"] = json!(0);
                "potrefund_records"
            }
            VerifiedRecord::Result(..) => "result_markers_v2",
            VerifiedRecord::Collected(..) => "collected_markers_v2",
            VerifiedRecord::Held(_, pay) => {
                answer["heldTxid"] = json!(pay);
                "collected_markers_v2"
            }
            VerifiedRecord::Hopsweep(..) => "hopsweep_records",
        };
        scenarios.push(json!({"name":name,"kind":kind.as_str(),"identity":poster,"body":{"scriptHex":hex::encode(bytes)},"answer":answer,"read":format!("SELECT COUNT(*) AS n FROM {table} WHERE txid=?"),"binds":[v.key()]}));
    };
    let party = Hand::new(16);
    add(
        &mut scenarios,
        "potparty",
        RecordKind::Potparty,
        party.direct_v2_script(),
        party.poster(),
    );
    let refund = Hand::new(17);
    add(
        &mut scenarios,
        "potrefund",
        RecordKind::Potrefund,
        potrefund_script(&refund.w, &refund.game_id, &refund.refund_raw),
        refund.poster(),
    );
    let id = hand.identity();
    let gid = hex::encode(hand.game_id);
    let opp = hex::encode(&hand.opponent);
    let settle = [0x99; 32];
    let challenge = overlay_discovery::result::validity::result_challenge_bytes(
        &gid,
        &hand.poster(),
        &opp,
        &hex::encode(hand.pot_txid),
        &hex::encode(settle),
        None,
    )
    .unwrap();
    let sig = sign(
        &hand.w,
        overlay_discovery::result::validity::result_protocol(),
        &gid,
        &challenge,
    );
    add(
        &mut scenarios,
        "result",
        RecordKind::Result,
        script(&[
            b"LOW/result/v1",
            &hand.game_id,
            &id,
            &hand.opponent,
            &hand.pot_txid,
            &settle,
            &sig,
            &[],
        ]),
        hand.poster(),
    );
    let sig = sign(
        &hand.w,
        collected_protocol(),
        &gid,
        &collected_challenge(&gid, &hand.poster()),
    );
    add(
        &mut scenarios,
        "collected",
        RecordKind::Collected,
        script(&[b"LOW/collected/v1", &hand.game_id, &id, &sig]),
        hand.poster(),
    );
    let pay = [0x66; 32];
    let sig = sign(
        &hand.w,
        collected_protocol(),
        &gid,
        &held_challenge(&gid, &hand.poster(), &hex::encode(pay)),
    );
    add(
        &mut scenarios,
        "held",
        RecordKind::Collected,
        script(&[HELD_TAG, &hand.game_id, &id, &pay, &sig]),
        hand.poster(),
    );
    use crate::hopsweep::tests as hs;
    let w = hs::wallet(0x43);
    let seat = hs::skey(12);
    md_index_the_hop(&db, &hs::hop_ctx(&w, &seat, true), Some(1));
    let raw = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &hs::DEST);
    add(
        &mut scenarios,
        "hopsweep",
        RecordKind::Hopsweep,
        md_sweep_script(&hs::identity(&w), &hs::GID, &raw),
        hex::encode(hs::identity(&w)),
    );
    // D2-ACTOR-F-FOLD (bsv-low #959): 32 SEATS FOR THE CARRIED FLOOD (`tools/lane-912/owed_carried_flood.mjs`), each
    // its own identity and game: a first `collected` filing (the cold apply that warms the game) and a `held` filing
    // naming a payout txid no row of the record names yet (a NEW filing: its new keys come with the carried closure).
    let mut carried_seats = Vec::new();
    for n in 0..32u8 {
        let seat = Hand::new(100 + n);
        let gid = hex::encode(seat.game_id);
        let id = seat.identity();
        let mut first = Vec::new();
        let sig = sign(
            &seat.w,
            collected_protocol(),
            &gid,
            &collected_challenge(&gid, &seat.poster()),
        );
        add(
            &mut first,
            "carried-first",
            RecordKind::Collected,
            script(&[b"LOW/collected/v1", &seat.game_id, &id, &sig]),
            seat.poster(),
        );
        let pay = [0xa0u8.wrapping_add(n); 32];
        let sig = sign(
            &seat.w,
            collected_protocol(),
            &gid,
            &held_challenge(&gid, &seat.poster(), &hex::encode(pay)),
        );
        add(
            &mut first,
            "carried-held",
            RecordKind::Collected,
            script(&[HELD_TAG, &seat.game_id, &id, &pay, &sig]),
            seat.poster(),
        );
        carried_seats.push(json!({"identity": seat.poster(), "game": gid, "pay": hex::encode(pay), "first": first[0], "held": first[1]}));
    }
    db.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
    let _ = std::fs::remove_file(&sqlite);
    db.conn.execute("VACUUM INTO ?1", [&sqlite]).unwrap();
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&json!({"sqlite":sqlite,"scenarios":scenarios,"carriedSeats":carried_seats})).unwrap(),
    )
    .unwrap();
}
