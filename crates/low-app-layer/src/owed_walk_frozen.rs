//! bsv-low #893 section 6.2 (lane D2-OWED-RETIRE): the OLD owed walk is deleted; what it STORED for each pin that
//! ran it beside the judge (the D2-OWED lens's MEDIUM-3 and its folds) is frozen in `tests/fixtures/old_walk_893`,
//! written by the walk itself at `0dae1fba` (`OLD_WALK_FREEZE=1`, twice, byte-identical). Each pin judges the same D1
//! data with the shipped judge and compares the rows it stores with the frozen walk's, byte for byte, the word facts
//! and the old ring's cursor facts stripped: the regression guard on the judge after the walk is gone.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod pins {
    use crate::pot_hook::tests::SqliteSql;
    use std::collections::BTreeMap;

    pub(crate) const NOW_MS: u64 = 1_790_000_000_000;
    pub(crate) const TIP: u64 = 900_000;

    /// The stored rows of `identity`, the word facts and the write-path stamps stripped (the D2-OWED lens's
    /// MEDIUM-3: "the stored rows compared byte for byte, the word facts stripped").
    pub(super) fn stored(conn: &rusqlite::Connection, identity: &str) -> BTreeMap<String, String> {
        let mut s = conn
            .prepare("SELECT outpoint,family,gameId,sats,opponentIdentity,atHeight,reason,facts FROM owed_rows WHERE identity=?1")
            .unwrap();
        s.query_map([identity], |r| {
            let mut facts: serde_json::Value =
                serde_json::from_str(&r.get::<_, String>(7)?).unwrap();
            // the word facts (joined at the read), the write-path stamps, and the old markerless RING's own cursor
            // facts (the ring is replaced by the eviction's evidence row, design section 8.1: no per-game form)
            for k in [
                "coinWord",
                "spenderWord",
                "fundingWord",
                "tracker",
                "beltNextAtMs",
                "judgedAtMs",
                "markerlessCandidates",
                "markerlessCursor",
                "markerlessEvidence",
                "markerlessExamined",
                "markerlessJoin",
            ] {
                facts.as_object_mut().unwrap().remove(k);
            }
            Ok((
                r.get::<_, String>(0)?,
                serde_json::json!([
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    facts
                ])
                .to_string(),
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    /// What the OLD walk stored for one identity of one pin: its rows (`stored`), whether its own bound cut it, and
    /// how many rows it derived. Frozen at the walk's retirement (bsv-low #893 section 6.2, lane D2-OWED-RETIRE) in
    /// `tests/fixtures/old_walk_893/<key>.json`, so each walk-against-judge pin keeps comparing the judge with the
    /// rows the walk served, after the walk's text is gone.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub(super) struct Walked {
        pub rows: BTreeMap<String, String>,
        pub cut: bool,
        pub derived: usize,
    }

    pub(super) fn frozen_path(key: &str) -> std::path::PathBuf {
        let name: String = key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/old_walk_893")
            .join(format!("{name}.json"))
    }

    /// The frozen walk's rows for `key`.
    pub(super) fn walk_rows(key: &str) -> Walked {
        let path = frozen_path(key);
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        serde_json::from_str(&text).unwrap()
    }

    mod fleet {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/d1_fleet.rs"
        ));
    }

    /// One fixture: its seed statements (run through the overlay's migrations and triggers), the identities compared.
    pub(crate) struct Fixture {
        pub name: &'static str,
        pub seed: Vec<String>,
        pub identities: Vec<String>,
    }

    fn h(prefix: &str, n: usize) -> String {
        format!("{prefix}{n:0>width$x}", width = 64 - prefix.len())
    }

    /// A JOIN spending two hops, each paying P2PKH(hash160(its seat key)): (join txid, BEEF, hops, keys).
    fn eviction_join() -> (String, Vec<u8>, Vec<String>, Vec<String>) {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
        let keys = vec![
            format!("02{}", "aa".repeat(32)),
            format!("03{}", "bb".repeat(32)),
        ];
        let mut beef = Beef::new();
        let mut hops = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            let mut tx = Transaction::new();
            tx.inputs
                .push(TransactionInput::new(format!("{:064x}", i + 7), 0));
            tx.outputs.push(TransactionOutput {
                satoshis: Some(20_190),
                locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(
                    &bsv_rs::primitives::hash::hash160(&hex::decode(key).unwrap()),
                ))
                .unwrap(),
                change: false,
            });
            hops.push(tx.id());
            beef.merge_raw_tx(tx.to_binary(), None);
        }
        let mut join = Transaction::new();
        for hop in &hops {
            join.inputs.push(TransactionInput::new(hop.clone(), 0));
        }
        join.outputs.push(TransactionOutput {
            satoshis: Some(40_000),
            locking_script: LockingScript::from_hex("51").unwrap(),
            change: false,
        });
        let id = join.id();
        beef.merge_raw_tx(join.to_binary(), None);
        (id, beef.to_binary(), hops, keys)
    }

    pub(crate) fn fixtures() -> Vec<Fixture> {
        let now_s = NOW_MS / 1000;
        let made = now_s as i64 - 7_200;
        let mut out = Vec::new();
        // The #638 fleet and the owed judge's lived-in seat (250 pots, 270 hops).
        let mut seed = fleet::rows(now_s);
        seed.extend(fleet::owed_rows(now_s));
        out.push(Fixture {
            name: "d1_fleet",
            seed,
            identities: [fleet::WHALE, fleet::OWED_SEAT]
                .into_iter()
                .map(str::to_owned)
                .chain([0u32, 1, 57, 119].map(|k| format!("02{k:064x}")))
                .collect(),
        });
        // Stranded hops of every arm the belt fixtures seed: index-unspent, an unverified marker, a hop the index
        // holds spent by a transaction it does not hold, by a pot, a door refusal, and a young hop.
        let mut seed = Vec::new();
        for n in 0..6usize {
            let hop = h("b3", n);
            let (spent, spender, valid, at) = match n {
                1 => (0, "NULL".to_string(), 0, made),
                2 => (1, format!("'{}'", h("c4", 2)), 1, made),
                3 => (1, format!("'{}'", h("c5", 3)), 1, made),
                5 => (0, "NULL".to_string(), 1, now_s as i64 - 60),
                _ => (0, "NULL".to_string(), 1, made),
            };
            seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,createdAt,lockKind) VALUES ('{hop}',0,{spent},{spender},{at},'p2pkh')"));
            seed.push(format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',2,'{hop}',1,{at},{valid})", h("9a", n)));
        }
        seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight) VALUES ('{}',0,0,{made},'covenant',1,900100)", h("c5", 3)));
        seed.push(format!("INSERT INTO submit_refusals(hopTxid,hopVout,refusedTxid,reason,refusedAt) VALUES ('{}',0,'{}','script-refused',{})", h("b3", 4), h("c6", 4), NOW_MS - 60_000));
        out.push(Fixture {
            name: "stranded hops",
            seed,
            identities: vec!["owner".into()],
        });
        // Two games naming one pot (the representative rule), and a released hop of an eviction.
        let pot = h("d7", 1);
        let mut seed = vec![
            format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight) VALUES ('{pot}',0,0,{made},'covenant',1,900100)"),
            format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES ('owner','other','{}','{pot}',0,900100,'00','{}',1,{made},1)", h("9b", 1), h("e8", 1)),
            format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES ('owner','other','{}','{pot}',0,900100,'00','{}',1,{},0)", h("9b", 2), h("e8", 2), made + 10),
        ];
        // The eviction's post-state, as `eviction_write::evict` leaves it: the JOIN's twins (its covenant row with
        // its decoded keys, both seats' verified party rows), the hops' pointers released, the release list, the
        // JOIN's stored BEEF (each hop paying hash160 of its seat key), and the evidence row the judge reads.
        let (join, beef, hops, keys) = eviction_join();
        let evicted_at = NOW_MS - 3_600_000;
        let released: Vec<String> = hops
            .iter()
            .map(|h| format!("{{\"table\":\"pot_records\",\"txid\":\"{h}\",\"vout\":0}}"))
            .collect();
        seed.push(format!("INSERT INTO pot_records_evicted(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight,pubA,pubB,payPkhA,payPkhB) VALUES ('{join}',0,0,{made},'covenant',1,900100,'{}','{}','{}','{}')", keys[0], keys[1], "dd".repeat(20), "ee".repeat(20)));
        for (n, key) in keys.iter().enumerate() {
            let who = if n == 0 { "owner" } else { "other" };
            seed.push(format!("INSERT INTO potparty_records_evicted(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid,seatSettlePubkey) VALUES ('{who}','x','{}','{join}',0,900100,'00','{}',1,{made},1,'{key}')", h("9b", 3), h("e9", n)));
            seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{}',0,0,{made},'p2pkh')", hops[n]));
        }
        seed.push(format!(
            "INSERT INTO pot_beefs(txid,beef,createdAt) VALUES ('{join}',x'{}',{made})",
            hex::encode(&beef)
        ));
        seed.push(format!("INSERT INTO pot_evictions(txid,evictedAt,reason,releasedSpends) VALUES('{join}',{evicted_at},'fixture','[{}]')", released.join(",")));
        seed.push(format!("INSERT INTO released_hop_evidence(hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) VALUES('{}',0,'owner','{}','{join}','{}',20190,{evicted_at})", hops[0], h("9b", 3), keys[0]));
        out.push(Fixture {
            name: "representatives and a released hop",
            seed,
            identities: vec!["owner".into()],
        });
        // One hop output named by two of its author's markers under two games (the lens's MEDIUM-3 divergence).
        let hop = h("b5", 1);
        out.push(Fixture {
            name: "a hop under two games",
            seed: vec![
                format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{hop}',0,0,{made},'p2pkh')"),
                format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',3,'{hop}',1,{made},1)", h("9c", 1)),
                format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',3,'{hop}',2,{},1)", h("9c", 2), made + 10),
            ],
            identities: vec!["owner".into()],
        });
        out
    }

    /// The fixture seeded on the judge's side (the walk's side was the same D1 data, copied whole).
    pub(crate) fn seeded(f: &Fixture) -> SqliteSql {
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        judge.conn.execute_batch("BEGIN").unwrap();
        for row in &f.seed {
            if f.name == "d1_fleet" {
                fleet::execute_row(&judge.conn, row);
            } else {
                judge.conn.execute_batch(row).unwrap();
            }
        }
        judge.conn.execute_batch("COMMIT").unwrap();
        judge
    }

    /// Every game of `identity`, judged a batch of 16 at a time (the backfill's shape).
    pub(super) async fn judge_identity(judge: &SqliteSql, identity: &str) {
        let games: Vec<String> = judge
            .conn
            .prepare("SELECT gameId FROM potparty_records WHERE identity=?1 UNION SELECT gameId FROM hopparty_records WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence WHERE identity=?1")
            .unwrap()
            .query_map([identity], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for chunk in games.chunks(crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION) {
            crate::owed_judge::judge_subjects(judge, identity, chunk, "walk-pin")
                .await
                .unwrap();
        }
    }

    /// The D2-OWED lens's MEDIUM-3: the OLD walk (`owed_recompute_at_tip`, its cursor, ring, probes and write) and the
    /// judge over the same D1 data store the same rows, byte for byte, the word facts stripped. A walk its own page
    /// bound cut serves a subset: each row it stored is the judge's, byte for byte.
    ///
    /// The walk's text was main's (the delta lens's MEDIUM-3 gap, `tools/lane-893/old_walk_text_diff.py` against
    /// main `8e5e73f7`, retired with the walk); its rows are frozen (the module's head). The write path is compared
    /// too: `d2delta_medium3_a_filing_judged_by_the_write_path_*` enters a filing through `record_post` and compares
    /// the rows its `judge_with_write` stored with the walk's.
    ///
    /// The word facts (`coinWord`, `spenderWord`, `fundingWord`, `tracker`) are stripped on purpose, NOT a gap: a
    /// stored word fact is never served. The read removes the stored copies and joins the words in their place,
    /// identically for every row (`owed_read::Snapshot::body`, over `owed_row_words` and `tx_words`), and a row's word
    /// roles are the txids of its own facts (`settleTxid`, `spendingTxid`, `payTxid`, `chainSpender`, `sweepTxid`, the
    /// outpoint's funding: `owed_judge::row_statements`), which ARE compared here; so what is served is the same
    /// function of the same word rows for both sides.
    #[tokio::test]
    async fn d2fold_medium3_the_old_walk_and_the_judge_store_the_same_rows() {
        let mut compared = 0;
        let mut diffs = Vec::new();
        for f in fixtures() {
            let judge = seeded(&f);
            for id in &f.identities {
                let walked = walk_rows(&format!("medium3-{}-{id}", f.name));
                if f.name == "d1_fleet" {
                    judge_identity(&judge, id).await;
                } else {
                    // the shipped drain, one subject a judge, as every source write's marks are judged
                    for _ in 0..100 {
                        let r = crate::owed_judge::drain_dirty(&judge, 64).await.unwrap();
                        assert!(r.faults.is_empty(), "{:?}", r.faults);
                        if r.subjects == 0 {
                            break;
                        }
                    }
                }
                let w = walked.rows.clone();
                let j = stored(&judge.conn, id);
                assert_eq!(w.len(), walked.derived.min(crate::owed::OWED_MAX_ROWS));
                // A walk its page bound cut, or a list past the stored ceiling, keeps a different subset by design
                // (the walk's cursor pages against the judge's ranked 500): the rows both hold are compared.
                let cut = walked.cut || j.len() >= crate::owed::OWED_MAX_ROWS;
                for (outpoint, row) in &w {
                    if cut && !j.contains_key(outpoint) {
                        continue;
                    }
                    compared += 1;
                    if j.get(outpoint) != Some(row) {
                        diffs.push(format!(
                            "{} {id} {outpoint}\n  walk  {row}\n  judge {:?}",
                            f.name,
                            j.get(outpoint)
                        ));
                    }
                }
                if !cut {
                    for outpoint in j.keys().filter(|o| !w.contains_key(*o)) {
                        diffs.push(format!(
                            "{} {id} {outpoint}: the judge stores a row the walk does not: {}",
                            f.name, j[outpoint]
                        ));
                    }
                }
                eprintln!(
                    "{}: {id}: walk {} rows{}, judge {} rows",
                    f.name,
                    w.len(),
                    if cut { " (cut)" } else { "" },
                    j.len()
                );
            }
        }
        assert!(
            diffs.is_empty(),
            "{} of {compared} rows differ:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
        assert!(compared > 0);
    }

    /// The delta lens's MEDIUM-3 gap (the fixtures above enter by SQL and are judged by the shipped drain): a filing
    /// that enters through `record_post` (a signed potrefund marker for a seat of an indexed pot, through
    /// `verify_record_post` and `file_verified`) is judged by the WRITE path (`judge_with_write`, the filing's source
    /// statements and the judge in one batch), with no drain after it; the old walk over the same D1 data stores the
    /// same rows, byte for byte, the word facts stripped.
    #[tokio::test]
    async fn d2delta_medium3_a_filing_judged_by_the_write_path_stores_the_walks_rows() {
        use crate::record_post::{self, tests as rp, DoorOutcome, RecordKind};
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        let victim = rp::wallet(0x41);
        let id = hex::encode(rp::identity(&victim));
        let game = [0x7a; 32];
        rp::md_index_the_pot(&judge);
        rp::md_seat(&judge, &id, &game);
        judge.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        let script = rp::potrefund_script(&victim, &game, &rp::presigned_refund());
        let mut filing =
            record_post::verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        let out = record_post::file_verified(&judge, &mut filing, &id, (NOW_MS / 1000) as i64)
            .await
            .unwrap();
        record_post::detached::judge(&judge, std::slice::from_ref(&filing), "test").await.unwrap();
        assert!(
            matches!(out, DoorOutcome::Written { row_written: true }),
            "{out:?}"
        );
        assert_eq!(
            judge.counter(crate::owed_judge::COUNTER_FILING_FAULTS),
            0,
            "judged by the write path"
        );
        let pending: i64 = judge
            .conn
            .query_row(
                "SELECT COUNT(*) FROM owed_dirty WHERE identity=?1",
                [&id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            pending, 0,
            "the write path acknowledged the filer's marks: nothing left for a drain"
        );
        let j = stored(&judge.conn, &id);
        assert!(
            !j.is_empty(),
            "the filing's judgement stored the seat's rows"
        );
        // the same D1 data on the walk's side, the judge's rows for the identity taken away first
        let walked = walk_rows("write-path-filing");
        assert!(!walked.cut);
        let w = walked.rows;
        eprintln!("filing: walk {w:#?}\njudge {j:#?}");
        assert_eq!(
            w, j,
            "the walk and the write path's judgement store the same rows"
        );
    }

    /// The second delta lens's D2-N2: the write-path comparison above covers one row and it is not a press. Here a
    /// PRESS: the seat's own index-UNSPENT stranded hop (its `pot_records` row unspent, its marker verified) and the
    /// seat's pre-signed sweep of it filed through `record_post` (`verify_record_post`, `file_verified`), judged by the
    /// WRITE path with no drain after it: the judge stores the `hop-stranded` row `claim: "sweep-hop"`, `claimable:
    /// true`, and the old walk over the same D1 data stores the same row, byte for byte, the word facts stripped.
    #[tokio::test]
    async fn d2delta2_n2_a_press_filed_through_record_post_judged_by_the_write_path_stores_the_walks_row(
    ) {
        use crate::hopsweep::tests as hs;
        use crate::record_post::{self, tests as rp, DoorOutcome, RecordKind};
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        let w = hs::wallet(0x47);
        let identity = hs::identity(&w);
        let id = hex::encode(&identity);
        let seat = hs::skey(16);
        let made = (NOW_MS as i64 - 2 * 3_600_000) / 1000;
        judge
            .conn
            .execute_batch(&format!(
                "INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{}',0,0,{made},'p2pkh')",
                hex::encode(hs::HOP)
            ))
            .unwrap();
        rp::md_index_the_hop(&judge, &hs::hop_ctx(&w, &seat, true), Some(1));
        judge.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        let raw = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &hs::DEST);
        let script = rp::md_sweep_script(&identity, &hs::GID, &raw);
        let mut filing =
            record_post::verify_record_post(RecordKind::Hopsweep, &script, &id).unwrap();
        let out = record_post::file_verified(&judge, &mut filing, &id, (NOW_MS / 1000) as i64)
            .await
            .unwrap();
        record_post::detached::judge(&judge, std::slice::from_ref(&filing), "test").await.unwrap();
        assert!(
            matches!(out, DoorOutcome::Written { row_written: true }),
            "{out:?}"
        );
        assert_eq!(
            judge.counter(crate::owed_judge::COUNTER_FILING_FAULTS),
            0,
            "judged by the write path"
        );
        let pending: i64 = judge
            .conn
            .query_row(
                "SELECT COUNT(*) FROM owed_dirty WHERE identity=?1",
                [&id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            pending, 0,
            "the write path acknowledged the filer's marks: nothing left for a drain"
        );
        let j = stored(&judge.conn, &id);
        let press = j
            .values()
            .map(|row| serde_json::from_str::<serde_json::Value>(row).unwrap())
            .find(|row| row[0] == "hop-stranded")
            .unwrap_or_else(|| panic!("no hop-stranded row: {j:#?}"));
        assert_eq!(press[6]["claim"], "sweep-hop", "{press}");
        assert_eq!(
            press[6]["claimable"], true,
            "the write path judged a press: {press}"
        );
        assert_eq!(press[6]["status"], "unspent", "{press}");
        let walked = walk_rows("write-path-press");
        assert!(!walked.cut);
        let w = walked.rows;
        eprintln!("press: walk {w:#?}\njudge {j:#?}");
        assert_eq!(
            w, j,
            "the walk and the write path's judgement store the same press"
        );
    }

    /// The third delta lens's D3-N1: the press above does not rest on its filing (the hop's index-unspent press stands
    /// without the sweep), so dropping the filing's facts from `judge_with_write` left it green. Here a press that
    /// RESTS on the filed fact: the seat of an indexed pot whose funding the network holds (`network_seen`) and whose
    /// recovery gate is open, with NO refund backup until the seat files one through `record_post`; judged by the
    /// write path with no drain after it, the row is `refund-due`, `claimable: true`, and the old walk over the same
    /// D1 data stores the same row. Before the filing the same judge stores no press for that pot.
    #[tokio::test]
    async fn d3n1_a_press_resting_on_the_filed_backup_judged_by_the_write_path_stores_the_walks_row(
    ) {
        use crate::record_post::{self, tests as rp, DoorOutcome, RecordKind};
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        let victim = rp::wallet(0x43);
        let id = hex::encode(rp::identity(&victim));
        let game = [0x7b; 32];
        rp::md_index_the_pot(&judge);
        rp::md_seat(&judge, &id, &game);
        judge
            .conn
            .execute(
                "INSERT INTO network_seen (txid, seenAt) VALUES (?1, ?2)",
                rusqlite::params![hex::encode(rp::POT_TXID), (NOW_MS / 1000) as i64],
            )
            .unwrap();
        // before the filing: the drain's judgement holds no press for the pot
        for _ in 0..100 {
            let r = crate::owed_judge::drain_dirty(&judge, 64).await.unwrap();
            assert!(r.faults.is_empty(), "{:?}", r.faults);
            if r.subjects == 0 {
                break;
            }
        }
        let rows = |conn: &rusqlite::Connection| -> Vec<serde_json::Value> {
            stored(conn, &id)
                .values()
                .map(|row| serde_json::from_str::<serde_json::Value>(row).unwrap())
                .collect()
        };
        let presses =
            |rows: &[serde_json::Value]| rows.iter().filter(|r| r[6]["claimable"] == true).count();
        let before = rows(&judge.conn);
        assert_eq!(
            presses(&before),
            0,
            "no press before the backup is filed: {before:#?}"
        );
        let script = rp::potrefund_script(&victim, &game, &rp::presigned_refund());
        let mut filing =
            record_post::verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        let out = record_post::file_verified(&judge, &mut filing, &id, (NOW_MS / 1000) as i64)
            .await
            .unwrap();
        record_post::detached::judge(&judge, std::slice::from_ref(&filing), "test").await.unwrap();
        assert!(
            matches!(out, DoorOutcome::Written { row_written: true }),
            "{out:?}"
        );
        assert_eq!(
            judge.counter(crate::owed_judge::COUNTER_FILING_FAULTS),
            0,
            "judged by the write path"
        );
        let pending: i64 = judge
            .conn
            .query_row(
                "SELECT COUNT(*) FROM owed_dirty WHERE identity=?1",
                [&id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            pending, 0,
            "the write path acknowledged the filer's marks: nothing left for a drain"
        );
        let after = rows(&judge.conn);
        let press = after
            .iter()
            .find(|r| r[0] == "refund-due")
            .unwrap_or_else(|| {
                panic!("the filing's judgement stores the refund press: {after:#?}")
            });
        assert_eq!(
            press[6]["claimable"], true,
            "the press rests on the filed backup: {press}"
        );
        let j = stored(&judge.conn, &id);
        let walked = walk_rows("write-path-refund-press");
        assert!(!walked.cut);
        let w = walked.rows;
        eprintln!("refund press: walk {w:#?}\njudge {j:#?}");
        assert_eq!(
            w, j,
            "the walk and the write path's judgement store the same press"
        );
    }

    /// The fourth delta lens's D4-N3 (the ruling: a word refusing a refund's funding is a DOWNGRADE for the refund
    /// press). The seat files its backup over an indexed pot whose funding the network holds (`network_seen`) and
    /// whose gate is open: the stored row is a `refund-due` press. Arcade then answers the pot REJECTED on its
    /// callback, the word stored as this tip's producer stores it (#598's mark, the tracker keeping `seen`): the read
    /// withholds the press (`pendingSource: "word"`) until the drain, whose judgement is FIX-831's (`funding-not-held`,
    /// `fundingHeld: refused`). RED on ae41223c: the word's mark was plain and the read served the stored press.
    #[tokio::test]
    async fn d4n3_a_refused_funding_word_withholds_the_refund_press_until_the_drain() {
        use crate::record_post::{self, tests as rp, DoorOutcome, RecordKind};
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        let victim = rp::wallet(0x44);
        let id = hex::encode(rp::identity(&victim));
        let game = [0x7c; 32];
        let pot = hex::encode(rp::POT_TXID);
        rp::md_index_the_pot(&judge);
        rp::md_seat(&judge, &id, &game);
        judge
            .conn
            .execute(
                "INSERT INTO network_seen (txid, seenAt) VALUES (?1, ?2)",
                rusqlite::params![pot, (NOW_MS / 1000) as i64],
            )
            .unwrap();
        // the overlay's `seen` word for the pot, as the producer stores it
        crate::owed::tests::produce_words(&judge.conn, &pot, &[], true, NOW_MS / 1000 - 600);
        let script = rp::potrefund_script(&victim, &game, &rp::presigned_refund());
        let mut filing =
            record_post::verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        let out = record_post::file_verified(&judge, &mut filing, &id, (NOW_MS / 1000) as i64)
            .await
            .unwrap();
        record_post::detached::judge(&judge, std::slice::from_ref(&filing), "test").await.unwrap();
        assert!(
            matches!(out, DoorOutcome::Written { row_written: true }),
            "{out:?}"
        );
        let drain = || async {
            for _ in 0..100 {
                let r = crate::owed_judge::drain_dirty(&judge, 64).await.unwrap();
                assert!(r.faults.is_empty(), "{:?}", r.faults);
                if r.subjects == 0 {
                    break;
                }
            }
        };
        drain().await;
        let served = || async {
            let snapshot = crate::owed_read::read(&judge, &id).await.unwrap();
            let words: BTreeMap<_, _> = snapshot
                .word_rows()
                .unwrap()
                .into_iter()
                .map(|(t, row)| (t, row.view()))
                .collect();
            let body = snapshot.body(&id, Some(TIP), &words).unwrap();
            body["rows"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["family"] == "refund-due" || r["facts"]["claim"] == "funding-not-held")
                .cloned()
                .unwrap_or_else(|| panic!("the pot's refund row: {body}"))
        };
        let row = served().await;
        assert_eq!(row["family"], "refund-due", "{row}");
        assert_eq!(
            row["facts"]["claimable"], true,
            "the press before the word: {row}"
        );

        // Arcade refuses the funding: the callback's word, stored by the producer
        let rejected = format!(
            r#"{{"txid":"{pot}","txStatus":"REJECTED","extraInfo":"arc error 461: missing inputs"}}"#
        );
        let word = crate::owed::tests::produce_words(
            &judge.conn,
            &pot,
            &[rejected.as_str()],
            false,
            NOW_MS / 1000 - 300,
        );
        assert_eq!(
            word.word, "seen",
            "the tracker keeps the word (NS-3): only the mark says refused"
        );
        let row = served().await;
        assert_eq!(
            row["facts"]["claimable"], false,
            "a refused funding word withholds the refund press until the drain: {row}"
        );
        assert_eq!(row["facts"]["pendingSource"], "word", "{row}");
        let class: i64 = judge
            .conn
            .query_row(
                "SELECT MAX(downgrade) FROM owed_dirty WHERE identity=?1 AND source='word'",
                [&id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(class, 1, "the refusal's word mark is a downgrade");

        drain().await;
        let row = served().await;
        assert_eq!(
            row["facts"]["claim"],
            crate::owed::FUNDING_NOT_HELD_CLAIM,
            "{row}"
        );
        assert_eq!(
            row["facts"]["fundingHeld"],
            crate::owed::FUNDING_HELD_REFUSED,
            "{row}"
        );
        assert_eq!(row["facts"]["claimable"], false, "{row}");
        assert!(row["facts"].get("refresh").is_none(), "{row}");
    }

    /// bsv-low #937 (L52 pair 13): the pot's row stays in `pot_records` while its eviction's OPEN marker stands (the
    /// gated arm's write landed after the marker, before the write-side guard's move). The judge asks the ledger of
    /// every result pot, so the refund press is withdrawn at the drain the marker's own dirty mark triggers, exactly
    /// as for the evicted twin (#518); the readmission's stamp restores it at the next drain. RED on eaaee0ec: the
    /// ledger was asked only of row-less pots and the press stood (`claimable: true`).
    #[tokio::test]
    async fn fix937_e_the_judge_drops_the_press_of_a_pot_under_an_open_marker() {
        use crate::record_post::{self, tests as rp, RecordKind};
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        let victim = rp::wallet(0x45);
        let id = hex::encode(rp::identity(&victim));
        let game = [0x7d; 32];
        let pot = hex::encode(rp::POT_TXID);
        rp::md_index_the_pot(&judge);
        rp::md_seat(&judge, &id, &game);
        judge
            .conn
            .execute(
                "INSERT INTO network_seen (txid, seenAt) VALUES (?1, ?2)",
                rusqlite::params![pot, (NOW_MS / 1000) as i64],
            )
            .unwrap();
        crate::owed::tests::produce_words(&judge.conn, &pot, &[], true, NOW_MS / 1000 - 600);
        let script = rp::potrefund_script(&victim, &game, &rp::presigned_refund());
        let mut filing =
            record_post::verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        record_post::file_verified(&judge, &mut filing, &id, (NOW_MS / 1000) as i64)
            .await
            .unwrap();
        record_post::detached::judge(&judge, std::slice::from_ref(&filing), "test")
            .await
            .unwrap();
        let drain = || async {
            for _ in 0..100 {
                let r = crate::owed_judge::drain_dirty(&judge, 64).await.unwrap();
                assert!(r.faults.is_empty(), "{:?}", r.faults);
                if r.subjects == 0 {
                    break;
                }
            }
        };
        let refund_press = || {
            stored(&judge.conn, &id)
                .values()
                .map(|row| serde_json::from_str::<serde_json::Value>(row).unwrap())
                .any(|r| r[0] == "refund-due" && r[6]["claimable"] == true)
        };
        drain().await;
        assert!(refund_press(), "the press before the marker");

        // the eviction's step 1, its own statement; the pot's row stays (the late write the guard has not moved)
        judge
            .conn
            .execute(
                bsv_overlay_cloudflare::admit_fast::OPEN_EVICTION_MARKER_SQL,
                rusqlite::params![
                    pot,
                    "REJECTED (arcade live REJECTED: bad script)",
                    (NOW_MS - 60_000) as i64
                ],
            )
            .unwrap();
        let held: i64 = judge
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pot_records WHERE txid=?1",
                [&pot],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(held, 1, "the row is still in pot_records");
        drain().await;
        assert!(
            !refund_press(),
            "no refund press over a pot under an open marker: {:#?}",
            stored(&judge.conn, &id)
        );

        judge
            .conn
            .execute(
                bsv_overlay_cloudflare::admit_fast::READMIT_EVICTION_SQL,
                rusqlite::params![NOW_MS as i64, pot],
            )
            .unwrap();
        drain().await;
        assert!(refund_press(), "readmitted: the press again");
    }

    #[tokio::test]
    async fn d2fold_medium3_smoke_the_old_walk_runs_natively() {
        let made = (NOW_MS as i64 - 2 * 3_600_000) / 1000;
        let seed = format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{h}',0,0,{made},'p2pkh'); INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','g00',0,20190,'seat','00','00',2,'{h}',1,{made},1);", h = "b3".to_string() + &"00".repeat(31));
        let w = walk_rows("smoke").rows;
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.conn.execute_batch(&seed).unwrap();
        crate::owed_judge::drain_dirty(&judge, 64).await.unwrap();
        let j = stored(&judge.conn, "owner");
        eprintln!("walk {w:#?}\njudge {j:#?}");
        assert_eq!(w, j);
    }

    mod l23 {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/l23_pair13.rs"
        ));
    }

    /// The tip the fold-7 finding judged the captured pair at (its seats' recovery height is 970,758).
    pub(crate) const FOLD8_TIP: u64 = 971_000;
    /// The finding's three identities, and the cached-press seat of the synthetic missing-bytes JOIN beside them.
    const FOLD8_TWIN_ONLY: &str =
        "030d9e9fbe045a8130f190b103609c8a6bf13b1887d5c4f15e2f918d68aca8ad67";
    const FOLD8_P28: &str = "02f9f80a92a92677d97fce5bb0a03d0321425b918868b3ef5773120d9cf5364e49";
    fn fold8_identities() -> Vec<String> {
        vec![
            FOLD8_TWIN_ONLY.into(),
            FOLD8_P28.into(),
            format!("02{}", "f1".repeat(32)),
            format!("02{}", "f2".repeat(32)),
        ]
    }

    /// Every pass of the shipped `owed-v2` backfill to its stamp, then the shipped drain to empty.
    pub(crate) async fn backfill_to_the_stamp(
        judge: &SqliteSql,
    ) -> Vec<crate::owed_backfill::BackfillReport> {
        judge.conn.execute_batch(&format!(
            "INSERT INTO ops_counters(name,value) VALUES ('overlay_migration_count',{}) ON CONFLICT(name) DO UPDATE SET value=excluded.value",
            crate::owed_backfill::OWED_TRIGGERS_MIGRATION_COUNT
        )).unwrap();
        let mut reports = Vec::new();
        for _ in 0..20 {
            let deadline = crate::pot_hook::Deadline::new(
                crate::owed_backfill::OWED_BACKFILL_WALL_MS,
                crate::owed_backfill::OWED_BACKFILL_SAVE_RESERVE_MS,
                Box::new(|| 0),
                Box::new(|_| Box::pin(std::future::pending())),
            );
            let r = crate::owed_backfill::backfill_pass(judge, &deadline)
                .await
                .unwrap();
            let done = r.stop == crate::owed_backfill::Stop::Finished;
            reports.push(r);
            if done {
                break;
            }
        }
        assert_eq!(
            reports.last().map(|r| r.stop),
            Some(crate::owed_backfill::Stop::Finished),
            "{reports:?}"
        );
        for _ in 0..100 {
            let r = crate::owed_judge::drain_dirty(judge, 64).await.unwrap();
            assert!(r.faults.is_empty(), "{:?}", r.faults);
            if r.subjects == 0 {
                break;
            }
        }
        reports
    }

    /// D2-OWED-FOLD-8 (bsv-low `docs/audit/D2-OWED-FOLD-7-FINDING-2026-10-26.md`): an eviction made BEFORE the eviction
    /// writer existed has no `released_hop_evidence` row, and the judge's only markerless source is that row. Over the
    /// tier's captured beta pair 13 (cfce07ce..., evicted 25 h before the clock, its JOIN's bytes stored) and the
    /// synthetic JOIN whose bytes are absent (dddd...), after the `owed-v2` backfill and the drain the judge stores the
    /// OLD walk's rows for the twin-only seat 030d9e9f... (the 20,190 sat hop sweep), p28 (its planted unverified
    /// marker replaced by the twin's verified evidence: spent elsewhere), f1 (the could-not-judge row beside its
    /// healthy game) and f2 (its cached press replaced by the could-not-judge rows), field by field, the word facts
    /// and the old ring's cursor facts stripped.
    #[tokio::test]
    async fn fold8_a_pre_writer_eviction_is_judged_as_the_old_walk_served_it() {
        let f = Fixture {
            name: "pre-writer eviction",
            seed: l23::l23_r2_route_rows(NOW_MS / 1000),
            identities: fold8_identities(),
        };
        let judge = seeded(&f);
        judge.tip.set(Some(FOLD8_TIP));
        // the captured rows were written before the dirty triggers existed: they carry no mark
        judge.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        backfill_to_the_stamp(&judge).await;
        let mut diffs = Vec::new();
        for id in &f.identities {
            let w = walk_rows(&format!("fold8-a-{id}")).rows;
            let j = stored(&judge.conn, id);
            eprintln!("{id}:\n  walk  {w:#?}\n  judge {j:#?}");
            assert!(!w.is_empty(), "{id}: the walk serves a row");
            if w != j {
                diffs.push(format!("{id}\n  walk  {w:?}\n  judge {j:?}"));
            }
        }
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
        // A game judged alone (a later source mark) never lays the missing JOIN's could-not-judge row over the row
        // the seat's own pot is judged from: f1's healthy game keeps its outpoint.
        let f1 = format!("02{}", "f1".repeat(32));
        let game = "9d88aaaa348acae127efd08e133822201ecae56513831c37124837f3ef9ae17f".to_string();
        crate::owed_judge::judge_subjects(&judge, &f1, &[game], "fold8")
            .await
            .unwrap();
        let j = stored(&judge.conn, &f1);
        let fresh = format!("{}:0", "ee".repeat(32));
        assert!(j[&fresh].contains("healthy-game"), "{}", j[&fresh]);
        let w = walk_rows(&format!("fold8-a-{f1}")).rows;
        assert_eq!(w, j);
    }

    /// D2-OWED-FOLD-8: the backfill visits a seat whose only party row the eviction moved (the evicted twin of an
    /// open eviction, the #610 rule), so an old worker's row of that seat is judged as the old walk judges it (here:
    /// no source names it, so no row), never served on as it stood.
    #[tokio::test]
    async fn fold8_b_the_backfill_visits_a_twin_only_seat() {
        let id = format!("02{}", "c7".repeat(32));
        let join = "c8".repeat(32);
        let game = "c9".repeat(32);
        let at = NOW_MS - 3_600_000;
        let f = Fixture {
            name: "twin-only seat",
            seed: vec![
                format!("INSERT INTO potparty_records_evicted(identity,potTxid,gameId,opponentIdentity,seatSettlePubkey,sigValid,af_evictedAt) VALUES ('{id}','{join}','{game}','opponent','{}',1,{at})", "03".repeat(33)),
                format!("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES ('{join}','REJECTED',{at},'[]')"),
                format!("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) VALUES ('{id}','{}:0','hop-stranded','{game}','{{\"claim\":\"sweep-hop\",\"claimable\":true}}',{at})", "ca".repeat(32)),
            ],
            identities: vec![id.clone()],
        };
        let judge = seeded(&f);
        // rows written before the dirty triggers existed carry no mark
        judge.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        assert_eq!(
            stored(&judge.conn, &id).len(),
            1,
            "the old worker's press stands before the backfill"
        );
        backfill_to_the_stamp(&judge).await;
        let w = walk_rows("fold8-b").rows;
        let j = stored(&judge.conn, &id);
        assert!(w.is_empty(), "{w:?}");
        assert_eq!(j, w, "the old worker's press is judged away");
    }
    /// The seat of the captured pair 13 that pays its committed home (`pubA` 023186e1..., `payPkhA` 3e7bb94f...) and
    /// its hop. In hand 3's shape (D2-OWED-HOME-MARKER) it has NO party row, live or evicted, only its VERIFIED hop
    /// marker; its hop is swept around the overlay to the committed home, the sweep's bytes a courier's.
    pub(crate) const HOME_MARKER_SEAT: &str = FOLD8_TWIN_ONLY;
    const HOME_MARKER_KEY: &str =
        "023186e1d618b96dca2583a21aa8c5e2227ba49f3f671f59e3f2be1c346ed41763";
    const HOME_MARKER_PKH: &str = "3e7bb94f261568ca8d457d8b4f935b0a3f8ccf89";
    pub(crate) const HOME_MARKER_HOP: &str =
        "9b238eed0177c714b1f9a608243e8f1bf79e35e9597110f83cefa1b658785c62";
    pub(crate) const HOME_MARKER_GAME: &str =
        "9d88aaaa348acae127efd08e133822201ecae56513831c37124837f3ef9ae17f";

    /// Hand 3's courier sweep: the seat's hop to its committed home (FIX-988's pin serves its raw to the belt).
    pub(crate) fn home_marker_sweep() -> bsv_rs::transaction::Transaction {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
        let pkh: [u8; 20] = hex::decode(HOME_MARKER_PKH).unwrap().try_into().unwrap();
        let mut sweep = Transaction::new();
        sweep
            .inputs
            .push(TransactionInput::new(HOME_MARKER_HOP.to_string(), 0));
        sweep.outputs.push(TransactionOutput {
            satoshis: Some(20_000),
            locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&pkh))
                .unwrap(),
            change: false,
        });
        sweep
    }

    /// Hand 3's rows over the captured pair 13: the seat's party rows gone, its verified marker, the courier sweep.
    pub(crate) fn home_marker_seed() -> (Vec<String>, String) {
        let mut seed = l23::l23_r2_route_rows(NOW_MS / 1000);
        let sweep = home_marker_sweep();
        let sweep_txid = sweep.id();
        let bytes = crate::owed::StoredSpenderBytes::from_parts(
            &[(HOME_MARKER_HOP.to_string(), 0)],
            &crate::routes::spender_outputs_of(&sweep),
        )
        .to_json_capped()
        .unwrap();
        let now_s = NOW_MS / 1000;
        seed.push(format!(
            "DELETE FROM potparty_records_evicted WHERE identity='{HOME_MARKER_SEAT}'"
        ));
        seed.push(format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,markerValid,txid,outputIndex,createdAt) VALUES ('{HOME_MARKER_SEAT}','{FOLD8_P28}','{HOME_MARKER_GAME}',0,20190,'{HOME_MARKER_KEY}','00','00',2,1,'{HOME_MARKER_HOP}',1,{})", now_s - 2 * 3600));
        seed.push(format!("INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed,tipHeight) VALUES ('{HOME_MARKER_HOP}.0',{NOW_MS},1,'{sweep_txid}',1,{FOLD8_TIP})"));
        seed.push(format!(
            "INSERT INTO owed_spender_bytes(txid,bytesJson,createdAtMs) VALUES ('{sweep_txid}','{}',{NOW_MS})",
            bytes.replace('\'', "''")
        ));
        (seed, sweep_txid)
    }

    /// FIX-1001 (bsv-low #1001, L55's S2): the captured pair 13's twin-only seat (030d9e9f..., NO hop marker: its
    /// hop is its eviction's released evidence) FILES its sweep of the hop (9b238eed...:0) to its committed home, the
    /// discovery pass's pointer names the sweep (unconfirmed) and the engine holds the sweep's bytes unproven
    /// (`transactions.has_proof` 0). Answers the rows, the sweep's txid and the filing's.
    pub(crate) fn fix1001_seed() -> (Vec<String>, String) {
        let mut seed = l23::l23_r2_route_rows(NOW_MS / 1000);
        let sweep = home_marker_sweep();
        let s = sweep.id();
        let mut beef = bsv_rs::transaction::Beef::new();
        beef.merge_raw_tx(sweep.to_binary(), None);
        let made = NOW_MS / 1000 - 3_600;
        seed.push(format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES ('{HOME_MARKER_SEAT}','{HOME_MARKER_GAME}','{HOME_MARKER_HOP}',0,'{s}','{}','{}',0,{made})", hex::encode(sweep.to_binary()), "f1".repeat(32)));
        seed.push(format!(
            "INSERT INTO transactions(txid,beef,has_proof,created_at) VALUES ('{s}',x'{}',0,{made})",
            hex::encode(beef.to_binary())
        ));
        seed.push(format!("UPDATE pot_records SET spent=1,spendingTxid='{s}',spentConfirmed=0 WHERE txid='{HOME_MARKER_HOP}' AND outputIndex=0"));
        (seed, s)
    }

    fn home_marker_row(judge: &SqliteSql) -> (String, serde_json::Value, Option<String>) {
        judge
            .conn
            .query_row(
                "SELECT family,facts,reason FROM owed_rows WHERE identity=?1 AND outpoint=?2",
                [HOME_MARKER_SEAT, &format!("{HOME_MARKER_HOP}:0")],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        serde_json::from_str(&r.get::<_, String>(1)?).unwrap(),
                        r.get(2)?,
                    ))
                },
            )
            .unwrap_or_else(|e| panic!("no row for the seat's hop: {e}"))
    }

    /// D2-OWED-HOME-MARKER (c): beta's courier-retire hand 3 (game 39cf..., JOIN e494f0b5..., 2026-10-26): a released
    /// hop with a VERIFIED hop marker and NO party row, swept around the overlay to the seat's committed home. The
    /// eviction's evidence is written from the marker's K (the shared statements, here by the `owed-v2` backfill's
    /// evidence phase over the captured JOIN's real covenant bytes), so the judge binds the home from the twin's params
    /// and serves the design's row, `payout` `courier-bytes`, never "cannot tell which committed home is yours".
    #[tokio::test]
    async fn home_marker_c_a_marker_only_seat_swept_home_is_a_courier_bytes_payout() {
        let (seed, sweep) = home_marker_seed();
        let f = Fixture {
            name: "home marker",
            seed,
            identities: vec![HOME_MARKER_SEAT.into()],
        };
        let judge = seeded(&f);
        judge.tip.set(Some(FOLD8_TIP));
        judge.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        backfill_to_the_stamp(&judge).await;
        let (family, facts, reason) = home_marker_row(&judge);
        assert_eq!(
            (family.as_str(), facts["creditKind"].as_str()),
            ("payout", Some("courier-bytes")),
            "{reason:?} {facts}"
        );
        assert_ne!(reason.as_deref(), Some(crate::owed::HOME_UNKNOWN_REASON));
        assert!(facts.to_string().contains(&sweep), "{facts}");
        let evidence: (String, String, String) = judge
            .conn
            .query_row(
                "SELECT identity,gameId,seatSettlePubkey FROM released_hop_evidence WHERE hopTxid=?1 AND hopVout=0",
                [HOME_MARKER_HOP],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("the marker's evidence row");
        assert_eq!(
            evidence,
            (
                HOME_MARKER_SEAT.into(),
                HOME_MARKER_GAME.into(),
                HOME_MARKER_KEY.into()
            )
        );
    }

    /// D2-OWED-HOME-MARKER (c2): an eviction the writer made BEFORE it took the marker (beta's hand 3 itself, and
    /// prod's), on a deployment whose `owed-v2` backfill and evidence phase are long done: the judge serves the
    /// finding's row ("cannot tell which committed home is yours") until the drain's age repair walks the ledger
    /// once, writes the marker seat's evidence through the shipped statements, and the next drain serves the payout.
    /// Then the repair is done: one read a pass.
    #[tokio::test]
    async fn home_marker_c2_the_age_repair_writes_a_pre_fix_evictions_marker_seat() {
        let (seed, _) = home_marker_seed();
        let f = Fixture {
            name: "home marker, pre-fix",
            seed,
            identities: vec![HOME_MARKER_SEAT.into()],
        };
        let judge = seeded(&f);
        judge.tip.set(Some(FOLD8_TIP));
        judge.conn.execute_batch(&format!(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('{HOME_MARKER_SEAT}','{HOME_MARKER_GAME}','hop','{HOME_MARKER_HOP}:0',{NOW_MS})"
        )).unwrap_or(());
        for _ in 0..10 {
            if crate::owed_judge::drain_dirty(&judge, 64)
                .await
                .unwrap()
                .subjects
                == 0
            {
                break;
            }
        }
        let (family, _, reason) = home_marker_row(&judge);
        assert_eq!(
            (family.as_str(), reason.as_deref()),
            ("unbound", Some(crate::owed::HOME_UNKNOWN_REASON)),
            "before the repair: the finding's row"
        );
        let repair = crate::owed_backfill::age_repair(&judge).await.unwrap();
        assert_eq!(
            repair.marker_repaired,
            vec!["cfce07ce1295866bad55be28657f19d9c74648d58a012cd05ae07400d99c5233".to_string()]
        );
        assert_eq!(
            repair.marker_cursor.as_deref(),
            Some(crate::owed_backfill::EVICTIONS_DONE)
        );
        for _ in 0..10 {
            if crate::owed_judge::drain_dirty(&judge, 64)
                .await
                .unwrap()
                .subjects
                == 0
            {
                break;
            }
        }
        let (family, facts, reason) = home_marker_row(&judge);
        assert_eq!(
            (family.as_str(), facts["creditKind"].as_str()),
            ("payout", Some("courier-bytes")),
            "{reason:?} {facts}"
        );
        let again = crate::owed_backfill::age_repair(&judge).await.unwrap();
        assert!(again.marker_repaired.is_empty());
    }
}
