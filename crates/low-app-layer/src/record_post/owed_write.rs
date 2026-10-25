//! Apply verified filings to the same hydrated views used by the owed judge.
use super::{VerifiedRecord, *};
use crate::owed_judge::{self as judge, FilingMark, JudgeDb, JudgeInputs, Subject};
use crate::pot_hook::{SqlPort, Stmt, StmtOut};
use serde_json::Value;
use std::collections::BTreeMap;

struct FilingJudge<'a, P> {
    db: &'a P,
    filings: &'a [VerifiedRecord],
    hops: &'a [Value],
    stamp: i64,
}
impl<P: JudgeDb> SqlPort for FilingJudge<'_, P> {
    async fn run_batch(&self, statements: Vec<Stmt>) -> std::result::Result<Vec<StmtOut>, String> {
        self.db.run_batch(statements).await
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
impl<P: JudgeDb> JudgeDb for FilingJudge<'_, P> {
    fn tip(&self) -> Option<u64> {
        self.db.tip()
    }
    fn era_ms(&self) -> Option<i64> {
        self.db.era_ms()
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> std::result::Result<BTreeMap<String, Value>, String> {
        self.db.word_views(rows).await
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        self.db.canonical_root(height).await
    }
    async fn push(&self, identity: &str, body: Value) {
        self.db.push(identity, body).await;
    }
    async fn hydrate(
        &self,
        identity: &str,
        games: &[String],
    ) -> std::result::Result<JudgeInputs, String> {
        judge::hydrate_filings(
            self.db,
            identity,
            games,
            self.filings,
            self.hops,
            self.stamp,
        )
        .await
    }
}

pub(crate) fn mark(v: &VerifiedRecord) -> FilingMark {
    FilingMark {
        source: match v {
            VerifiedRecord::Potparty(..) => "party",
            VerifiedRecord::Potrefund(..) => "refund",
            VerifiedRecord::Result(..) => "result",
            VerifiedRecord::Collected(..) | VerifiedRecord::Held(..) => "collected",
            VerifiedRecord::Hopsweep(..) => "sweep",
        }
        .into(),
        source_key: v.key().into(),
    }
}

pub(crate) async fn subjects(
    db: &impl SqlPort,
    v: &VerifiedRecord,
) -> std::result::Result<Vec<Subject>, String> {
    let pair = |identity: &str, game: &str| Subject {
        identity: identity.to_ascii_lowercase(),
        game_id: game.to_ascii_lowercase(),
    };
    Ok(match v {
        VerifiedRecord::Potparty(r, _) => vec![pair(&r.identity, &r.game_id)],
        VerifiedRecord::Potrefund(r, _) => {
            return judge::refund_subjects(db, &r.pot_txid, r.pot_vout, &r.identity, &r.game_id)
                .await
        }
        VerifiedRecord::Result(r, _) => {
            vec![pair(&r.winner, &r.game_id), pair(&r.loser, &r.game_id)]
        }
        VerifiedRecord::Collected(r) | VerifiedRecord::Held(r, _) => {
            vec![pair(&r.identity, &r.game_id)]
        }
        VerifiedRecord::Hopsweep(_, r) => vec![pair(&r.identity, &r.game_id)],
    })
}

/// The filing's source statements and its subjects' judgement in ONE batch. A judge that cannot run (a hydration
/// read, the batch's own fault) never refuses the filing (the D2-OWED lens's MEDIUM-2): the source statements land
/// ALONE, their triggers' marks stand for the drain, and the fault is counted (`owed_judge_filing_faults_total`).
/// Only a fault of that lone write refuses the filing.
pub(crate) async fn judge_filings(
    db: &impl JudgeDb,
    filings: &[VerifiedRecord],
    hops: &[Value],
    stamp: i64,
    writes: Vec<Stmt>,
    extra_marks: &[FilingMark],
) -> std::result::Result<judge::JudgeReport, String> {
    let alone = writes.clone();
    let judged = async {
        let mut selected = Vec::new();
        for filing in filings {
            selected.extend(subjects(db, filing).await?);
        }
        let adapter = FilingJudge {
            db,
            filings,
            hops,
            stamp,
        };
        judge::judge_with_write(
            &adapter,
            &selected,
            "filing",
            writes,
            extra_marks,
            |inputs| {
                for i in inputs {
                    for filing in filings {
                        apply(i, filing);
                    }
                }
                Ok(())
            },
        )
        .await
    }
    .await;
    match judged {
        Ok(report) => Ok(report),
        Err(error) => {
            db.log(&format!(
                "[owed] a filing's judge faulted; the filing lands alone and its marks wait for the drain: {error}"
            ));
            let landed = if alone.is_empty() {
                Vec::new()
            } else {
                db.run_batch(alone).await?
            };
            db.counters(vec![(judge::COUNTER_FILING_FAULTS.to_owned(), 1)])
                .await;
            Ok(judge::JudgeReport {
                source_changes: landed.iter().map(|r| r.changes).collect(),
                judge_fault: Some(error),
                ..Default::default()
            })
        }
    }
}

fn apply(i: &mut JudgeInputs, filing: &VerifiedRecord) {
    match filing {
        VerifiedRecord::Potrefund(r, valid) => {
            let key = crate::owed::outpoint_key(&r.pot_txid, r.pot_vout);
            for e in &mut i.refunds {
                if e.pot_txid == r.pot_txid && e.pot_vout == r.pot_vout {
                    e.backup_marker_present = true;
                }
            }
            if *valid {
                let home = i
                    .results
                    .iter()
                    .find(|e| e.pot_txid == r.pot_txid && e.pot_vout == r.pot_vout)
                    .and_then(|e| e.committed_keys.as_ref().zip(e.my_seat))
                    .map(|(keys, seat)| match seat {
                        crate::results::SeatLetter::A => &keys.pay_pkh_a,
                        crate::results::SeatLetter::B => &keys.pay_pkh_b,
                    });
                // The read keeps the first valid backup in its existing order.
                i.valid_refunds
                    .entry(key)
                    .or_insert_with(|| crate::owed::ValidRefund {
                        raw_hex: r.refund_raw_hex.clone(),
                        my_sats: home.and_then(|pkh| {
                            crate::owed::refund_output_sats(&r.refund_raw_hex, pkh)
                        }),
                    });
            }
        }
        VerifiedRecord::Collected(r) | VerifiedRecord::Held(r, _) if i.identity == r.identity => {
            let mut fold = crate::owed::CollectedFold::default();
            let pay = match filing {
                VerifiedRecord::Held(_, pay) => Some(pay.as_str()),
                _ => None,
            };
            fold.row(
                &r.identity,
                &r.game_id,
                r.sig_hex.as_deref(),
                Some(&r.txid),
                pay,
            );
            i.collected_verified.extend(fold.verified);
            i.collected_present.extend(fold.present);
            i.held_verified.extend(fold.held);
        }
        _ => {}
    }
}

pub(crate) fn statements(v: &VerifiedRecord, now: i64) -> Vec<Stmt> {
    let mut writes = Vec::new();
    match v {
        VerifiedRecord::Potparty(r, valid) => writes.push(stmt(
            POTPARTY_FILE_SQL,
            vec![
                text(&r.identity),
                text(&r.opponent_identity),
                text(&r.game_id),
                text(&r.pot_txid),
                int(i64::from(r.pot_vout)),
                int(i64::from(r.recovery_height)),
                text(&r.sig_hex),
                text_opt(r.seat_settle_pubkey.as_deref()),
                text_opt(r.seat_sig_hex.as_deref()),
                text(&r.txid),
                int(0),
                int(now),
                int(i64::from(*valid)),
            ],
        )),
        VerifiedRecord::Potrefund(r, valid) => {
            writes.push(stmt(
                POTREFUND_FILE_SQL,
                vec![
                    text(&r.identity),
                    text(&r.game_id),
                    text(&r.pot_txid),
                    int(i64::from(r.pot_vout)),
                    text(&r.refund_raw_hex),
                    text(&r.sig_hex),
                    text(&r.txid),
                    int(0),
                    int(now),
                    int(i64::from(*valid)),
                ],
            ));
            if *valid {
                writes.push(stmt(POTREFUND_LATCH_SQL, vec![text(&r.txid)]));
            }
        }
        VerifiedRecord::Result(r, tier) => {
            writes.push(stmt(
                RESULT_FILE_SQL,
                vec![
                    text(&r.game_id),
                    text(&r.winner),
                    text(&r.loser),
                    text(&r.pot_txid),
                    text(&r.settle_txid),
                    text(&r.winner_sig_hex),
                    text_opt(r.loser_sig_hex.as_deref()),
                    text_opt(r.cards_hex.as_deref()),
                    text(&r.txid),
                    int(0),
                    int(now),
                    int(*tier),
                ],
            ));
            let pot = &r.pot_txid;
            writes.push(stmt(
                lb_row_file_sql(),
                vec![
                    text(&r.txid),
                    int(0),
                    text(&r.game_id),
                    text(&r.winner),
                    text(&r.loser),
                    text(pot),
                    text(&r.settle_txid),
                    text(&r.winner_sig_hex),
                    text_opt(r.loser_sig_hex.as_deref()),
                    text_opt(r.cards_hex.as_deref()),
                    int(now),
                    int(*tier),
                    text(pot),
                    text(pot),
                    text(pot),
                    int(now),
                    text(pot),
                    text(pot),
                    int(now),
                    text(pot),
                    text(pot),
                    int(overlay_discovery::result::storage::RESULT_ROWS_PER_POT as i64),
                ],
            ));
        }
        VerifiedRecord::Collected(r) | VerifiedRecord::Held(r, _) => {
            let pay = match v {
                VerifiedRecord::Held(_, pay) => Some(pay.as_str()),
                _ => None,
            };
            writes.push(stmt(
                COLLECTED_FILE_SQL,
                vec![
                    text(&r.identity),
                    text(&r.game_id),
                    text(&r.txid),
                    int(0),
                    text_opt(r.sig_hex.as_deref()),
                    int(now),
                    text_opt(pay),
                ],
            ));
        }
        VerifiedRecord::Hopsweep(_, r) => writes.push(stmt(
            crate::hopsweep::HOPSWEEP_FILE_SQL,
            vec![
                text(&r.identity),
                text(&r.game_id),
                text(&r.hop_txid),
                int(i64::from(r.hop_vout)),
                text(&r.sweep_txid),
                text(&r.sweep_raw_hex),
                text(&r.txid),
                int(0),
                int(now),
            ],
        )),
    }
    writes
}
