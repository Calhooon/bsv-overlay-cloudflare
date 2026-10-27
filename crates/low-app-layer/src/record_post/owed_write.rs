//! Source writes and subject selection for verified record filings.
use super::{VerifiedRecord, *};
use crate::owed_judge::{self as judge, Subject};
use crate::pot_hook::{SqlPort, Stmt};

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

/// Commit only the source rows. Their triggers mark owed_dirty in this same atomic batch.
/// No judge or subject reads are part of the request's write.
pub(crate) async fn write_filings(
    db: &impl SqlPort,
    writes: Vec<Stmt>,
) -> std::result::Result<Vec<Option<u64>>, String> {
    Ok(db
        .run_batch(writes)
        .await?
        .iter()
        .map(|r| r.changes)
        .collect())
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The detached judge reads the newest durable backup through its production selector.
    #[tokio::test]
    async fn d2fold_low4_the_filed_backup_replaces_the_hydrated_one() {
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let mut filing = overlay_discovery::potrefund::storage::PotrefundRecord {
            identity: "owner".into(),
            game_id: "game".into(),
            pot_txid: "aa".repeat(32),
            pot_vout: 0,
            refund_raw_hex: "older".into(),
            sig_hex: "00".into(),
            txid: "filed:older".into(),
            output_index: 0,
            created_at: 1,
        };
        write_filings(
            &db,
            statements(&VerifiedRecord::Potrefund(filing.clone(), true), 1),
        )
        .await
        .unwrap();
        filing.txid = "filed:newer".into();
        filing.refund_raw_hex = "newer".into();
        write_filings(
            &db,
            statements(&VerifiedRecord::Potrefund(filing.clone(), true), 2),
        )
        .await
        .unwrap();
        let newest: String = db
            .conn
            .query_row(
                &crate::owed::valid_refunds_sql(1),
                rusqlite::params![filing.pot_txid, 0],
                |r| r.get("refundRawHex"),
            )
            .unwrap();
        assert_eq!(newest, "newer");
    }
}
