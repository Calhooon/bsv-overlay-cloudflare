//! Pure, host-testable helpers — outpoint parsing, batched-SQL assembly,
//! and wire-body assembly. NO worker/D1 types in here: everything compiles
//! and unit-tests natively (`cargo test -p low-app-layer`), and the route
//! handlers in `routes.rs` are thin worker glue over these functions.

use serde_json::json;

/// Hard cap on outpoints per `/utxo-status` request. Over the cap → 400
/// (bounds the per-request D1 work; a client with more splits the call).
pub const MAX_OUTPOINTS: usize = 64;

/// A txid is exactly 32 bytes → 64 hex chars (either case accepted; DB
/// lookups lowercase separately).
pub fn valid_txid(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// One parsed `<txid>.<vout>` entry from the `outpoints=` query parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outpoint {
    /// The caller's original txid spelling — echoed verbatim in the response
    /// so the caller can correlate entries without re-normalizing.
    pub txid: String,
    pub vout: u32,
}

impl Outpoint {
    /// The txid as stored in D1 (`pot_records.txid` is lowercase hex).
    pub fn db_txid(&self) -> String {
        self.txid.to_ascii_lowercase()
    }
}

/// Parse the full `outpoints=` parameter: comma-separated `<txid>.<vout>`,
/// capped at [`MAX_OUTPOINTS`]. Any malformed entry or an over-cap list is
/// a single `Err` (the route maps it to 400) — a partially-parsed request
/// is never served.
pub fn parse_outpoints(param: &str) -> Result<Vec<Outpoint>, String> {
    if param.is_empty() {
        return Err("empty outpoints parameter".to_string());
    }
    let parts: Vec<&str> = param.split(',').collect();
    if parts.len() > MAX_OUTPOINTS {
        return Err(format!(
            "too many outpoints: {} (max {MAX_OUTPOINTS})",
            parts.len()
        ));
    }
    parts.into_iter().map(parse_outpoint).collect()
}

/// Parse one `<txid>.<vout>` entry. Strict: 64-hex txid, all-digit decimal
/// vout that fits u32 (no sign, no whitespace, no extra dots).
fn parse_outpoint(s: &str) -> Result<Outpoint, String> {
    let Some((txid, vout)) = s.split_once('.') else {
        return Err(format!("malformed outpoint (expect <txid>.<vout>): {s:?}"));
    };
    if !valid_txid(txid) {
        return Err(format!("malformed txid (expect 64 hex chars): {txid:?}"));
    }
    // `u32::from_str` alone would accept a leading '+' — require pure digits.
    if vout.is_empty() || !vout.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("malformed vout (expect decimal digits): {vout:?}"));
    }
    let vout: u32 = vout
        .parse()
        .map_err(|_| format!("vout out of u32 range: {vout:?}"))?;
    Ok(Outpoint {
        txid: txid.to_string(),
        vout,
    })
}

/// One `/utxo-status` response entry, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutpointStatus {
    /// Caller's original txid spelling (echoed).
    pub txid: String,
    pub vout: u32,
    /// Whether `pot_records` has a row for this outpoint.
    pub known: bool,
    /// `Some(bool)` for a known row, `None` (wire `null`) when unknown —
    /// FAIL-SAFE: an unknown outpoint is never asserted unspent.
    pub spent: Option<bool>,
    /// The landing-proof spender, when the row records one.
    pub spending_txid: Option<String>,
}

impl OutpointStatus {
    /// No `pot_records` row: `known:false, spent:null, spendingTxid:null`.
    pub fn unknown(op: &Outpoint) -> Self {
        Self {
            txid: op.txid.clone(),
            vout: op.vout,
            known: false,
            spent: None,
            spending_txid: None,
        }
    }

    /// A found row: `known:true` with the row's spent flag + spender.
    pub fn known(op: &Outpoint, spent: bool, spending_txid: Option<String>) -> Self {
        Self {
            txid: op.txid.clone(),
            vout: op.vout,
            known: true,
            spent: Some(spent),
            spending_txid,
        }
    }
}

/// Assemble the `/utxo-status` wire body: an input-ordered JSON array of
/// `{"txid","vout","known","spent","spendingTxid"}` (same shape as
/// zanaadu's `/utxo-status`).
pub fn utxo_status_body(entries: &[OutpointStatus]) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            json!({
                "txid": e.txid,
                "vout": e.vout,
                "known": e.known,
                "spent": e.spent,
                "spendingTxid": e.spending_txid,
            })
        })
        .collect();
    serde_json::Value::Array(arr).to_string()
}

/// The single batched `/utxo-status` SQL: one `(txid = ? AND outputIndex = ?)`
/// disjunct per requested outpoint (2 binds each, input order). ONE D1 query
/// answers the whole batch — the query-collapse that replaces per-outpoint
/// round trips (and the flaky edge cache) as the scaling mechanism.
pub fn batch_where_sql(n: usize) -> String {
    debug_assert!((1..=MAX_OUTPOINTS).contains(&n), "parse_outpoints bounds n");
    let clause = vec!["(txid = ? AND outputIndex = ?)"; n].join(" OR ");
    format!("SELECT txid, outputIndex, spent, spendingTxid FROM pot_records WHERE {clause}")
}

/// One `pot_records` row, host-typed (the route converts D1's f64s here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotRecordRow {
    /// Stored lowercase hex.
    pub txid: String,
    pub vout: u32,
    pub spent: bool,
    pub spending_txid: Option<String>,
}

/// Map the batch-query rows back onto the REQUESTED outpoints, input-ordered.
/// Rows are keyed by `(lowercase txid, vout)`; a requested outpoint with no
/// row is the fail-safe [`OutpointStatus::unknown`] (never asserted unspent).
pub fn assemble_statuses(outpoints: &[Outpoint], rows: &[PotRecordRow]) -> Vec<OutpointStatus> {
    outpoints
        .iter()
        .map(|op| {
            let key_txid = op.db_txid();
            match rows
                .iter()
                .find(|r| r.txid.eq_ignore_ascii_case(&key_txid) && r.vout == op.vout)
            {
                Some(r) => OutpointStatus::known(op, r.spent, r.spending_txid.clone()),
                None => OutpointStatus::unknown(op),
            }
        })
        .collect()
}

/// Decode the `hex(beef)` column read back from D1 (SQLite `hex()` emits
/// UPPERCASE; `hex::decode` accepts either case). An empty or undecodable
/// value is `None` — the engine treats an empty BEEF row as un-hydrated, so
/// serving it would hand the client unusable bytes.
pub fn decode_beef_hex(hex_str: &str) -> Option<Vec<u8>> {
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.is_empty() {
        None
    } else {
        Some(bytes)
    }
}

/// Assemble the `/beef/:txid` wire body: `{"txid","beef":[<bytes>]}` (bytes
/// as a JSON number array, the legacy wire shape zanaadu's `/beef` serves).
pub fn beef_body(txid: &str, beef: &[u8]) -> String {
    json!({ "txid": txid, "beef": beef }).to_string()
}

/// Parse a rust-chaintracks `GET /getPresentHeight` response frame:
/// `{"status":"success","value":<height>}` → the height. Anything else
/// (error frame, missing/negative value) → `None`.
pub fn parse_present_height(v: &serde_json::Value) -> Option<u64> {
    if v.get("status")?.as_str()? != "success" {
        return None;
    }
    v.get("value")?.as_u64()
}

/// Assemble the `/tip` wire body: `{"height":<n>}`.
pub fn tip_body(height: u64) -> String {
    json!({ "height": height }).to_string()
}

/// Assemble the `/health` wire body.
pub fn health_body() -> String {
    json!({ "ok": true, "service": "low-app-layer" }).to_string()
}


#[cfg(test)]
mod tests {
    use super::*;

    fn txid_a() -> String {
        "ab".repeat(32)
    }

    fn txid_b() -> String {
        "cd".repeat(32)
    }

    // ── txid validation ────────────────────────────────────────────────

    #[test]
    fn txid_validation() {
        assert!(valid_txid(&"a".repeat(64)));
        assert!(valid_txid(&"0123456789abcdef".repeat(4)));
        // Either case accepted (DB lookups lowercase separately).
        assert!(valid_txid(&"A".repeat(64)));
        // Wrong width / non-hex / traversal.
        assert!(!valid_txid(&"a".repeat(63)));
        assert!(!valid_txid(&"a".repeat(65)));
        assert!(!valid_txid(""));
        assert!(!valid_txid(&"g".repeat(64)));
        assert!(!valid_txid("../etc/passwd"));
    }

    // ── outpoint parsing ───────────────────────────────────────────────

    #[test]
    fn parse_single_outpoint() {
        let ops = parse_outpoints(&format!("{}.0", txid_a())).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].txid, txid_a());
        assert_eq!(ops[0].vout, 0);
    }

    #[test]
    fn parse_multiple_outpoints_preserves_order() {
        let param = format!("{}.1,{}.0", txid_b(), txid_a());
        let ops = parse_outpoints(&param).unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!((ops[0].txid.as_str(), ops[0].vout), (txid_b().as_str(), 1));
        assert_eq!((ops[1].txid.as_str(), ops[1].vout), (txid_a().as_str(), 0));
    }

    #[test]
    fn parse_preserves_caller_case_but_db_txid_lowercases() {
        let upper = "AB".repeat(32);
        let ops = parse_outpoints(&format!("{upper}.3")).unwrap();
        // Echoed spelling is the caller's original…
        assert_eq!(ops[0].txid, upper);
        // …while the D1 key is lowercase.
        assert_eq!(ops[0].db_txid(), "ab".repeat(32));
    }

    #[test]
    fn parse_cap_is_64() {
        let one = format!("{}.0", txid_a());
        let at_cap = vec![one.clone(); MAX_OUTPOINTS].join(",");
        assert_eq!(parse_outpoints(&at_cap).unwrap().len(), 64);
        let over_cap = vec![one; MAX_OUTPOINTS + 1].join(",");
        let err = parse_outpoints(&over_cap).unwrap_err();
        assert!(err.contains("too many outpoints"), "{err}");
    }

    #[test]
    fn parse_rejects_malformed() {
        // Empty parameter / empty entry (trailing comma).
        assert!(parse_outpoints("").is_err());
        assert!(parse_outpoints(&format!("{}.0,", txid_a())).is_err());
        // Missing dot.
        assert!(parse_outpoints(&txid_a()).is_err());
        // Bad txid width / non-hex.
        assert!(parse_outpoints("abc.0").is_err());
        assert!(parse_outpoints(&format!("{}.0", "g".repeat(64))).is_err());
        // Bad vout: empty, sign, hex, whitespace, extra dot.
        assert!(parse_outpoints(&format!("{}.", txid_a())).is_err());
        assert!(parse_outpoints(&format!("{}.+5", txid_a())).is_err());
        assert!(parse_outpoints(&format!("{}.-1", txid_a())).is_err());
        assert!(parse_outpoints(&format!("{}.0x1", txid_a())).is_err());
        assert!(parse_outpoints(&format!("{}. 1", txid_a())).is_err());
        assert!(parse_outpoints(&format!("{}.0.1", txid_a())).is_err());
    }

    #[test]
    fn parse_vout_u32_bounds() {
        // u32::MAX parses…
        let ops = parse_outpoints(&format!("{}.4294967295", txid_a())).unwrap();
        assert_eq!(ops[0].vout, u32::MAX);
        // …u32::MAX + 1 does not.
        assert!(parse_outpoints(&format!("{}.4294967296", txid_a())).is_err());
    }

    // ── response assembly ──────────────────────────────────────────────

    #[test]
    fn utxo_status_body_shapes_known_and_unknown() {
        let op_a = Outpoint {
            txid: txid_a(),
            vout: 0,
        };
        let op_b = Outpoint {
            txid: txid_b(),
            vout: 1,
        };
        let entries = vec![
            OutpointStatus::known(&op_a, true, Some("f0".repeat(32))),
            OutpointStatus::known(&op_a, false, None),
            OutpointStatus::unknown(&op_b),
        ];
        let v: serde_json::Value = serde_json::from_str(&utxo_status_body(&entries)).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        // Spent row with landing proof.
        assert_eq!(arr[0]["txid"], txid_a());
        assert_eq!(arr[0]["vout"], 0);
        assert_eq!(arr[0]["known"], true);
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], "f0".repeat(32));
        // Known-unspent row.
        assert_eq!(arr[1]["known"], true);
        assert_eq!(arr[1]["spent"], false);
        assert!(arr[1]["spendingTxid"].is_null());
        // Unknown row: fail-safe nulls, never asserted unspent.
        assert_eq!(arr[2]["txid"], txid_b());
        assert_eq!(arr[2]["vout"], 1);
        assert_eq!(arr[2]["known"], false);
        assert!(arr[2]["spent"].is_null());
        assert!(arr[2]["spendingTxid"].is_null());
    }

    #[test]
    fn utxo_status_body_is_input_ordered() {
        let mk = |txid: String, vout: u32| Outpoint { txid, vout };
        let entries: Vec<OutpointStatus> = [mk(txid_b(), 5), mk(txid_a(), 0)]
            .iter()
            .map(OutpointStatus::unknown)
            .collect();
        let v: serde_json::Value = serde_json::from_str(&utxo_status_body(&entries)).unwrap();
        assert_eq!(v[0]["txid"], txid_b());
        assert_eq!(v[1]["txid"], txid_a());
    }

    // ── batched SQL + input-order assembly ─────────────────────────────

    #[test]
    fn batch_sql_shapes() {
        assert_eq!(
            batch_where_sql(1),
            "SELECT txid, outputIndex, spent, spendingTxid FROM pot_records \
             WHERE (txid = ? AND outputIndex = ?)"
        );
        let three = batch_where_sql(3);
        assert_eq!(three.matches("(txid = ? AND outputIndex = ?)").count(), 3);
        assert_eq!(three.matches(" OR ").count(), 2);
    }

    #[test]
    fn assemble_maps_rows_input_ordered_and_fail_safe() {
        let ops = vec![
            Outpoint { txid: txid_b(), vout: 1 }, // spent row
            Outpoint { txid: txid_a(), vout: 0 }, // no row → unknown
            Outpoint { txid: txid_a(), vout: 2 }, // unspent row
        ];
        // Rows arrive in ARBITRARY DB order — assembly must re-order.
        let rows = vec![
            PotRecordRow {
                txid: txid_a(),
                vout: 2,
                spent: false,
                spending_txid: None,
            },
            PotRecordRow {
                txid: txid_b(),
                vout: 1,
                spent: true,
                spending_txid: Some("f0".repeat(32)),
            },
        ];
        let out = assemble_statuses(&ops, &rows);
        assert_eq!(out.len(), 3);
        assert_eq!((out[0].known, out[0].spent), (true, Some(true)));
        assert_eq!(out[0].spending_txid.as_deref(), Some("f0".repeat(32).as_str()));
        // Fail-safe middle: no row → known:false, spent:null.
        assert_eq!((out[1].known, out[1].spent), (false, None));
        assert_eq!((out[2].known, out[2].spent), (true, Some(false)));
    }

    #[test]
    fn assemble_is_case_insensitive_on_txid() {
        // Caller sent UPPER hex; the DB row is lowercase — must still match,
        // and the echoed spelling stays the caller's.
        let upper = "AB".repeat(32);
        let ops = vec![Outpoint { txid: upper.clone(), vout: 0 }];
        let rows = vec![PotRecordRow {
            txid: "ab".repeat(32),
            vout: 0,
            spent: true,
            spending_txid: None,
        }];
        let out = assemble_statuses(&ops, &rows);
        assert!(out[0].known);
        assert_eq!(out[0].txid, upper);
    }

    // ── BEEF ───────────────────────────────────────────────────────────

    #[test]
    fn decode_beef_hex_cases() {
        // SQLite hex() emits UPPERCASE — must decode.
        assert_eq!(decode_beef_hex("BEEF"), Some(vec![0xBE, 0xEF]));
        // Lowercase too.
        assert_eq!(decode_beef_hex("beef"), Some(vec![0xbe, 0xef]));
        // Empty = un-hydrated row → None (served as 404, never as bytes).
        assert_eq!(decode_beef_hex(""), None);
        // Odd length / non-hex → None.
        assert_eq!(decode_beef_hex("abc"), None);
        assert_eq!(decode_beef_hex("zz"), None);
    }

    #[test]
    fn beef_body_is_number_array() {
        let v: serde_json::Value =
            serde_json::from_str(&beef_body(&txid_a(), &[0, 1, 255])).unwrap();
        assert_eq!(v["txid"], txid_a());
        assert_eq!(v["beef"], serde_json::json!([0, 1, 255]));
    }

    // ── tip ────────────────────────────────────────────────────────────

    #[test]
    fn present_height_parse() {
        // rust-chaintracks success frame → the height.
        let ok = serde_json::json!({"status": "success", "value": 812_345});
        assert_eq!(parse_present_height(&ok), Some(812_345));
        // Error frame / missing value / wrong types → None.
        let err = serde_json::json!({"status": "error", "code": "ERR"});
        assert_eq!(parse_present_height(&err), None);
        assert_eq!(
            parse_present_height(&serde_json::json!({"status": "success"})),
            None
        );
        assert_eq!(parse_present_height(&serde_json::json!({})), None);
        assert_eq!(
            parse_present_height(&serde_json::json!({"status": "success", "value": -1})),
            None
        );
    }

    #[test]
    fn tip_and_health_bodies() {
        let v: serde_json::Value = serde_json::from_str(&tip_body(812_345)).unwrap();
        assert_eq!(v["height"], 812_345);
        let h: serde_json::Value = serde_json::from_str(&health_body()).unwrap();
        assert_eq!(h["ok"], true);
        assert_eq!(h["service"], "low-app-layer");
    }
}
