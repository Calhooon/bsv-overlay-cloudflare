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
///
/// bsv-low M19 D1 (#428, 2026-09-08): DRIVEN FROM THE PARTY WINDOW. The
/// first shape was `FROM potrefund_records pr WHERE EXISTS (party …)`, an
/// EXISTS evaluated per potrefund row: SQLite walked the WHOLE
/// `potrefund_records` table (every seat's backups, newest first) until it
/// had 401 matches — ~980k rows read per call at 811 ms on beta in loop 8,
/// the single heaviest query behind the D1 `overloaded` answers. Now the
/// identity's party outpoints (an indexed identity scan, a few hundred rows
/// for the busiest seat) are the driving set and `potrefund_records` is
/// reached by its `(potTxid, potVout)` index; `refundRawHex` is read only
/// for the rows that survive to the served page. Same served rows, same
/// order, same binds.
///
/// bsv-low M18-2 B (2026-09-10, the filing gate's HIGH-1): `refundValid`
/// LEADS the order. A filed backup carries 1 only when `POST /record`
/// verified its raw as the pot's PRE-SIGNED spend by BOTH committed settle
/// keys (height-gated, non-final); a chain-admitted row is NULL (sorts as 0).
/// Before this, the served 4-per-pot page was newest-first with no rank, so
/// four free rows naming a victim's pot under a stranger's identity were the
/// whole page and the wiped device seeded a junk raw (never broadcast — the
/// client's own validity gate — but the #191 belt was disabled for that pot).
/// The rank is unforgeable in this window (it needs a key the attacker does
/// not hold) and immutable (written once), so it is safe as a leading term.
pub fn refund_backups_sql(written_off_before_ms: Option<i64>) -> String {
    format!(
        "SELECT pr.potTxid AS potTxid, pr.potVout AS potVout, pr.gameId AS gameId, \
                pr.identity AS identity, pr.refundRawHex AS refundRawHex, \
                pr.sigHex AS sigHex, pr.txid AS txid, pr.outputIndex AS outputIndex, \
                pr.createdAt AS createdAt, pr.refundValid AS refundValid \
         FROM (SELECT DISTINCT pp.potTxid AS potTxid, pp.potVout AS potVout \
                 FROM {party} pp \
                WHERE pp.identity = ?1{era}) party \
         JOIN potrefund_records pr \
           ON pr.potTxid = party.potTxid AND pr.potVout = party.potVout \
         ORDER BY COALESCE(pr.refundValid, 0) DESC, pr.createdAt DESC, pr.rowid DESC \
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
    /// 1 = the filing verified the raw as the pot's pre-signed spend (M18-2 B);
    /// 0 = filed before the pot was indexed; NULL = a chain-admitted row.
    #[serde(rename = "refundValid", default)]
    refund_valid: Option<f64>,
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
            refund_valid: self.refund_valid.map(|v| v as i64),
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
    /// See `RefundBackupRowD1::refund_valid`; served as `refundValid` so an
    /// operator (and a probe) can see which backups carry the committed-key
    /// verdict. No client money path reads it.
    pub refund_valid: Option<i64>,
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
                    "refundValid": r.refund_valid,
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
            refund_valid: None,
        }
    }

    #[test]
    fn sql_is_the_party_window_over_potrefund_records_with_one_identity_bind() {
        let sql = refund_backups_sql(None);
        // bsv-low M19 D1 (#428): the party window DRIVES the read; the
        // potrefund table is JOINED on its (potTxid, potVout) index, never
        // walked with an EXISTS per row (the 980k-rows-per-call shape).
        assert!(sql.contains("JOIN potrefund_records pr"));
        assert!(sql.contains("ON pr.potTxid = party.potTxid AND pr.potVout = party.potVout"));
        assert!(
            !sql.contains("WHERE EXISTS"),
            "no per-row EXISTS over potrefund_records"
        );
        assert!(sql.contains("SELECT DISTINCT pp.potTxid AS potTxid, pp.potVout AS potVout"));
        assert!(
            sql.contains("potparty_records"),
            "the caller's party window bounds the read"
        );
        assert!(sql.contains("pp.identity = ?1"));
        assert!(
            sql.contains("pr.refundRawHex AS refundRawHex"),
            "this read SERVES the bytes"
        );
        assert!(
            sql.contains(&format!("LIMIT {}", REFUND_BACKUPS_MAX_ROWS + 1)),
            "one probe row past the cap"
        );
        assert!(
            !sql.contains("?2"),
            "no era bind when no cutoff is configured"
        );
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
            out[0]
                .rows
                .iter()
                .map(|r| r.refund_raw_hex.clone().unwrap())
                .collect::<Vec<_>>(),
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
        let empty: serde_json::Value =
            serde_json::from_str(&refund_backups_body("", &[], false)).unwrap();
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
            refund_valid: None,
        };
        let r = d1.into_row();
        assert_eq!(r.pot_txid, "ab".repeat(32));
        assert_eq!(r.identity, "02ab");
        assert_eq!(r.refund_raw_hex, None, "an empty string is no bytes");
        assert_eq!(r.created_at, Some(12));
    }

    // ── bsv-low M19 D1 (#428, 2026-09-08): the JOIN shape ────────────────────

    fn migrated() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
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

    fn plan_lines(
        conn: &rusqlite::Connection,
        sql: &str,
        binds: &[rusqlite::types::Value],
    ) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("prepare");
        stmt.query_map(rusqlite::params_from_iter(binds.iter()), |r| {
            r.get::<_, String>(3)
        })
        .expect("plan")
        .map(|r| r.expect("row"))
        .collect()
    }

    /// The pre-M19 shape, kept ONLY as the equivalence oracle below: SQLite
    /// planned it as `SCAN pr USING INDEX idx_potrefund_createdAt` with a
    /// correlated party subquery per row — every seat's backups walked,
    /// newest first, until 401 matched (loop 8: ~980k rows read per call).
    fn pre_m19_sql(written_off_before_ms: Option<i64>) -> String {
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

    /// EXPLAIN pin under real SQLite on the shipped schema: the identity's
    /// party window DRIVES (an identity-index search on both party tables),
    /// `potrefund_records` is reached by its `(potTxid, potVout)` index, and
    /// nothing scans `potrefund_records` — with and without the era bind.
    #[test]
    fn plan_drives_from_the_party_window_and_probes_potrefund_by_index_real_sqlite() {
        let conn = migrated();
        let identity = rusqlite::types::Value::Text("02".repeat(33));
        for (sql, binds) in [
            (refund_backups_sql(None), vec![identity.clone()]),
            (
                refund_backups_sql(Some(1_000)),
                vec![identity.clone(), rusqlite::types::Value::Integer(1_000)],
            ),
        ] {
            let plan = plan_lines(&conn, &sql, &binds);
            let joined = plan.join("\n");
            assert!(
                plan.iter().any(|l| l
                    .contains("SEARCH pr USING INDEX idx_potrefund_pot (potTxid=? AND potVout=?)")),
                "potrefund_records is probed by its outpoint index:\n{joined}"
            );
            assert!(
                plan.iter().any(
                    |l| l.contains("SEARCH potparty_records USING INDEX idx_potparty_identity")
                ),
                "the potparty arm is an identity-index search:\n{joined}"
            );
            assert!(
                plan.iter()
                    .any(|l| l.contains("SEARCH hp USING INDEX idx_hopparty_identity")),
                "the hopparty arm is an identity-index search:\n{joined}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|l| l.starts_with("SCAN pr") || l.contains("SCAN potrefund_records")),
                "never a walk over potrefund_records:\n{joined}"
            );
        }
        // the oracle's shape really was the scan (the defect this pins against)
        let old = plan_lines(&conn, &pre_m19_sql(None), &[identity]);
        assert!(
            old.iter()
                .any(|l| l.starts_with("SCAN pr USING INDEX idx_potrefund_createdAt")),
            "{old:?}"
        );
    }

    /// The served rows are IDENTICAL to the pre-M19 shape's — same rows, same
    /// order, same binds — over a seeded window that exercises every arm:
    /// duplicate party markers for one pot (the JOIN needs DISTINCT where
    /// EXISTS deduped for free), both seats' backup rows for a pot, another
    /// identity's pot, a pot no party names, a hop-derived pot (the UNION's
    /// second arm), and the #375 era cutoff.
    #[test]
    fn served_rows_equal_the_pre_m19_shape_real_sqlite() {
        let conn = migrated();
        let me = "02".repeat(33);
        let other = "03".repeat(33);
        let (p1, p2, p3, p4, p5, hop) = (
            "11".repeat(32),
            "22".repeat(32),
            "33".repeat(32),
            "44".repeat(32),
            "55".repeat(32),
            "66".repeat(32),
        );
        let party = |identity: &str, pot: &str, marker: &str, created: i64| {
            conn.execute(
                "INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt) \
                 VALUES (?1, 'opp', 'g', ?2, 0, 100, 'sig', ?3, 0, ?4)",
                rusqlite::params![identity, pot, marker, created],
            )
            .unwrap();
        };
        party(&me, &p1, &"a1".repeat(32), 1_000);
        party(&me, &p1, &"a2".repeat(32), 1_500); // a duplicate marker for the same pot
        party(&me, &p2, &"a3".repeat(32), 2_000);
        party(&other, &p3, &"a4".repeat(32), 2_000);
        // the hop arm: my hop marker, its container spent by pot p5 whose committed pubA is my seat key
        conn.execute(
            "INSERT INTO hopparty_records (identity, opponentIdentity, gameId, hopVout, hopSats, seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, hopSatsOnChain, containerOutputs, txid, outputIndex, createdAt, markerValid) \
             VALUES (?1, 'opp', 'g5', 0, 20000, 'pkA', 'ss', 'is', 'aa', 20000, 3, ?2, 1, 2_500, 1)",
            rusqlite::params![me, hop],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, createdAt) VALUES (?1, 0, 1, ?2, 2_400)",
            rusqlite::params![hop, p5],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, paramsDecoded, recoveryHeight, pubA, pubB) VALUES (?1, 0, 0, 2_600, 1, 100, 'pkA', 'pkB')",
            rusqlite::params![p5],
        )
        .unwrap();
        let backup = |pot: &str, identity: &str, marker: &str, created: i64| {
            conn.execute(
                "INSERT INTO potrefund_records (identity, gameId, potTxid, potVout, refundRawHex, sigHex, txid, outputIndex, createdAt) \
                 VALUES (?1, 'g', ?2, 0, ?3, 'sig', ?4, 0, ?5)",
                rusqlite::params![identity, pot, format!("raw-{marker}"), marker, created],
            )
            .unwrap();
        };
        backup(&p1, &me, &"b1".repeat(32), 10);
        backup(&p1, &other, &"b2".repeat(32), 30); // the counterparty's backup for MY pot: served (both seats file)
        backup(&p2, &me, &"b3".repeat(32), 20);
        backup(&p3, &other, &"b4".repeat(32), 40); // the other identity's pot: never mine
        backup(&p4, &me, &"b5".repeat(32), 50); // a pot no party names: never served
        backup(&p5, &me, &"b6".repeat(32), 5); // the hop-derived pot
        backup(&p2, &me, &"b7".repeat(32), 20); // same stamp as b3: the rowid tie-break

        type Row = (
            String,
            i64,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            i64,
            Option<i64>,
        );
        let read = |sql: &str, binds: &[rusqlite::types::Value]| -> Vec<Row> {
            let mut stmt = conn.prepare(sql).unwrap();
            stmt.query_map(rusqlite::params_from_iter(binds.iter()), |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
        };
        let id = rusqlite::types::Value::Text(me.clone());
        let new = read(&refund_backups_sql(None), std::slice::from_ref(&id));
        let old = read(&pre_m19_sql(None), std::slice::from_ref(&id));
        assert_eq!(new, old, "the served rows and their order are unchanged");
        let served: Vec<&str> = new.iter().map(|r| r.6.as_str()).collect();
        assert_eq!(
            served,
            ["b2".repeat(32), "b7".repeat(32), "b3".repeat(32), "b1".repeat(32), "b6".repeat(32)]
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            "newest first, rowid DESC on a tie; both seats' rows; p3/p4 absent; the hop pot present; p1 ONCE"
        );
        // the era cutoff (ms) on the party marker's stamp: p1's markers (1000/1500 s) drop, p2 (2000 s) and the hop (2500 s) stay
        let era = [id, rusqlite::types::Value::Integer(1_600_000)];
        let new = read(&refund_backups_sql(Some(1_600_000)), &era);
        let old = read(&pre_m19_sql(Some(1_600_000)), &era);
        assert_eq!(new, old);
        let served: Vec<&str> = new.iter().map(|r| r.6.as_str()).collect();
        assert_eq!(
            served,
            ["b7".repeat(32), "b3".repeat(32), "b6".repeat(32)]
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
    }
}
