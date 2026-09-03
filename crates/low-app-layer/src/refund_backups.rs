//! `/refund-backups` — the per-identity REFUND-BACKUP BYTES view (bsv-low W2
//! client batch B(a); owner ruling 2026-09-03: "the app-layer is for fast
//! reads … batched reads too as it has all the stuff decoded").
//!
//! For every pot the identity is a party to (the same `party_candidates_sql`
//! window every identity view uses), the indexed `potrefund_records` rows —
//! `refundRawHex` INCLUDED. This REVISES the `/refund-view` decision (#252
//! stage-2 plan §4: presence only, "recovery paths keep their per-pot
//! `lookupPotRefund`") for exactly one purpose: the wiped-device seeding pass
//! asked the overlay `ls_potrefund byPot` once per seeded marker (13 `/lookup`
//! POSTs per home mount, bsv-low runs 8/9); this is that read, batched and
//! served once.
//!
//! The TRUST MODEL IS UNCHANGED — the reader verifies. `tm_potrefund`
//! admission is byte-format-only (either seat or any third party can file a
//! marker row for any pot outpoint for one dust `OP_RETURN`), so a served row
//! is a CANDIDATE, never a fact: the client keeps its existing selection (the
//! raw must spend the pot outpoint; `selectRefundBackupRaw`) and the overlay
//! per-pot lookup stays its fallback rung for a pot absent from this batch.
//! Where the bytes are fetched from changed; what is believed did not.
//!
//! Bounds: at most [`REFUND_BACKUPS_MAX_ROWS`] rows are read (newest first,
//! one probe row past the cap decides `truncated`), and at most
//! [`REFUND_BACKUPS_ROWS_PER_POT`] rows are served per pot. A row with no
//! bytes is dropped here (presence is `/refund-view`'s question). Fail-safe
//! shape mirrors `/refund-view`: an invalid identity is an EMPTY 200; a D1
//! fault is a 503.

use serde::Deserialize;

/// Row cap for one read (a wiped wallet with 13 pots × 2 seats' backups is
/// ~26 rows; 400 bounds the BLOB payload at ~1.4 MB for a pathological
/// identity).
pub const REFUND_BACKUPS_MAX_ROWS: usize = 400;
/// Rows served per pot (both seats' backups + a couple of re-files).
pub const REFUND_BACKUPS_ROWS_PER_POT: usize = 4;

/// The ONE bounded query: every `potrefund_records` row whose pot outpoint is
/// in the caller's party window, newest first, probing one row past the cap.
/// `?1` = identity; `?2` = the #375 era cutoff (ms) iff configured, anchored
/// on the party marker's `createdAt` (the pot's own admission stamp is not
/// joined here — this read serves bytes, not verdicts).
pub fn refund_backups_sql(written_off_before_ms: Option<i64>) -> String {
    format!(
        "SELECT pr.potTxid AS potTxid, pr.potVout AS potVout, pr.gameId AS gameId, \
                pr.identity AS identity, pr.refundRawHex AS refundRawHex, \
                pr.sigHex AS sigHex, pr.txid AS txid, pr.outputIndex AS outputIndex, \
                pr.createdAt AS createdAt \
         FROM potrefund_records pr \
         WHERE EXISTS (SELECT 1 FROM {party} pp \
                        WHERE pp.identity = ?1{era} \
                          AND pp.potTxid = pr.potTxid AND pp.potVout = pr.potVout) \
         ORDER BY pr.createdAt DESC, pr.rowid DESC \
         LIMIT {probe}",
        party = crate::logic::party_candidates_sql(),
        era = crate::logic::era_filter_sql("pp.createdAt", "?2", written_off_before_ms),
        probe = REFUND_BACKUPS_MAX_ROWS + 1,
    )
}

/// One row as D1 returns it (numbers as f64 — the codebase convention).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RefundBackupRowD1 {
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "gameId")]
    game_id: String,
    identity: String,
    #[serde(rename = "refundRawHex", default)]
    refund_raw_hex: Option<String>,
    #[serde(rename = "sigHex", default)]
    sig_hex: Option<String>,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt", default)]
    created_at: Option<f64>,
}

impl RefundBackupRowD1 {
    pub(crate) fn into_row(self) -> RefundBackupRow {
        RefundBackupRow {
            pot_txid: self.pot_txid.to_lowercase(),
            pot_vout: self.pot_vout.max(0.0) as u32,
            game_id: self.game_id,
            identity: self.identity.to_lowercase(),
            refund_raw_hex: self.refund_raw_hex.filter(|s| !s.is_empty()),
            sig_hex: self.sig_hex.filter(|s| !s.is_empty()),
            txid: self.txid.to_lowercase(),
            output_index: self.output_index.max(0.0) as u32,
            created_at: self.created_at.map(|v| v as i64),
        }
    }
}

/// One host-typed backup row (the `refund_backups_sql` shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefundBackupRow {
    pub pot_txid: String,
    pub pot_vout: u32,
    pub game_id: String,
    pub identity: String,
    pub refund_raw_hex: Option<String>,
    pub sig_hex: Option<String>,
    pub txid: String,
    pub output_index: u32,
    pub created_at: Option<i64>,
}

/// One pot's served backups (rows newest first, ≤ `REFUND_BACKUPS_ROWS_PER_POT`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotBackups {
    pub pot_txid: String,
    pub pot_vout: u32,
    pub game_id: String,
    pub rows: Vec<RefundBackupRow>,
}

/// Group newest-first rows by pot outpoint (first-seen order = newest pot
/// first), drop rows without bytes, cap rows per pot.
pub fn assemble_refund_backups(rows: Vec<RefundBackupRow>) -> Vec<PotBackups> {
    let mut out: Vec<PotBackups> = Vec::new();
    for r in rows {
        if r.refund_raw_hex.is_none() {
            continue; // presence without bytes is `/refund-view`'s question
        }
        match out
            .iter_mut()
            .find(|p| p.pot_txid == r.pot_txid && p.pot_vout == r.pot_vout)
        {
            Some(p) => {
                if p.rows.len() < REFUND_BACKUPS_ROWS_PER_POT {
                    p.rows.push(r);
                }
            }
            None => out.push(PotBackups {
                pot_txid: r.pot_txid.clone(),
                pot_vout: r.pot_vout,
                game_id: r.game_id.clone(),
                rows: vec![r],
            }),
        }
    }
    out
}

/// The wire body. `truncated` = the row cap was hit; a client must then fall
/// back per pot for anything missing (never assume absence).
pub fn refund_backups_body(identity: &str, backups: &[PotBackups], truncated: bool) -> String {
    let entries: Vec<serde_json::Value> = backups
        .iter()
        .map(|p| {
            serde_json::json!({
                "potTxid": p.pot_txid,
                "potVout": p.pot_vout,
                "gameId": p.game_id,
                "rows": p.rows.iter().map(|r| serde_json::json!({
                    "identity": r.identity,
                    "refundRawHex": r.refund_raw_hex,
                    "sigHex": r.sig_hex,
                    "txid": r.txid,
                    "outputIndex": r.output_index,
                    "createdAt": r.created_at,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::json!({
        "identity": identity,
        "backups": entries,
        "truncated": truncated,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pot: &str, vout: u32, who: &str, raw: Option<&str>, at: i64) -> RefundBackupRow {
        RefundBackupRow {
            pot_txid: pot.repeat(32),
            pot_vout: vout,
            game_id: "g".repeat(64),
            identity: format!("02{}", who.repeat(32)),
            refund_raw_hex: raw.map(str::to_string),
            sig_hex: Some("30".to_string()),
            txid: format!("{at:064x}"),
            output_index: 0,
            created_at: Some(at),
        }
    }

    #[test]
    fn sql_is_the_party_window_over_potrefund_records_with_one_identity_bind() {
        let sql = refund_backups_sql(None);
        assert!(sql.contains("FROM potrefund_records pr"));
        assert!(sql.contains("potparty_records"), "the caller's party window bounds the read");
        assert!(sql.contains("pp.identity = ?1"));
        assert!(sql.contains("pr.refundRawHex AS refundRawHex"), "this read SERVES the bytes");
        assert!(sql.contains(&format!("LIMIT {}", REFUND_BACKUPS_MAX_ROWS + 1)), "one probe row past the cap");
        assert!(!sql.contains("?2"), "no era bind when no cutoff is configured");
    }

    #[test]
    fn era_cutoff_adds_exactly_one_more_bind_anchored_on_the_marker() {
        let sql = refund_backups_sql(Some(1_754_000_000_000));
        assert!(sql.contains("pp.createdAt * 1000 >= ?2"));
        assert_eq!(sql.matches("?2").count(), 1);
    }

    #[test]
    fn assembly_groups_by_pot_newest_first_caps_per_pot_and_drops_byteless_rows() {
        // Query order: newest first. Pot "aa" has 6 rows, pot "bb" has 1, and
        // one "aa" row carries no bytes (a presence-only marker).
        let rows = vec![
            row("aa", 0, "a", Some("01"), 60),
            row("bb", 1, "b", Some("02"), 55),
            row("aa", 0, "b", None, 50),
            row("aa", 0, "a", Some("03"), 40),
            row("aa", 0, "b", Some("04"), 30),
            row("aa", 0, "a", Some("05"), 20),
            row("aa", 0, "b", Some("06"), 10),
        ];
        let out = assemble_refund_backups(rows);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].pot_txid, "aa".repeat(32)); // first seen = newest pot
        assert_eq!(out[0].rows.len(), REFUND_BACKUPS_ROWS_PER_POT); // capped, byteless row skipped
        assert_eq!(
            out[0].rows.iter().map(|r| r.refund_raw_hex.clone().unwrap()).collect::<Vec<_>>(),
            vec!["01", "03", "04", "05"]
        );
        assert_eq!(out[1].pot_vout, 1);
        assert_eq!(out[1].rows.len(), 1);
    }

    #[test]
    fn body_shape_is_stable_and_truncation_is_explicit() {
        let out = assemble_refund_backups(vec![row("cc", 0, "a", Some("beef"), 7)]);
        let body = refund_backups_body(&format!("03{}", "1".repeat(64)), &out, true);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["truncated"], true);
        assert_eq!(v["backups"].as_array().unwrap().len(), 1);
        let p = &v["backups"][0];
        assert_eq!(p["potTxid"], "cc".repeat(32));
        assert_eq!(p["potVout"], 0);
        assert_eq!(p["rows"][0]["refundRawHex"], "beef");
        assert_eq!(p["rows"][0]["createdAt"], 7);
        // An identity with nothing: the empty, honest body.
        let empty: serde_json::Value = serde_json::from_str(&refund_backups_body("", &[], false)).unwrap();
        assert_eq!(empty["backups"].as_array().unwrap().len(), 0);
        assert_eq!(empty["truncated"], false);
    }

    #[test]
    fn d1_row_conversion_lowercases_and_drops_empty_bytes() {
        let d1 = RefundBackupRowD1 {
            pot_txid: "AB".repeat(32),
            pot_vout: 1.0,
            game_id: "g".into(),
            identity: "02AB".into(),
            refund_raw_hex: Some(String::new()),
            sig_hex: None,
            txid: "CD".repeat(32),
            output_index: 0.0,
            created_at: Some(12.0),
        };
        let r = d1.into_row();
        assert_eq!(r.pot_txid, "ab".repeat(32));
        assert_eq!(r.identity, "02ab");
        assert_eq!(r.refund_raw_hex, None, "an empty string is no bytes");
        assert_eq!(r.created_at, Some(12));
    }
}
