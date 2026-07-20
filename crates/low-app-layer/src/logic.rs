//! Pure, host-testable helpers — outpoint parsing, batched-SQL assembly,
//! and wire-body assembly. NO worker/D1 types in here: everything compiles
//! and unit-tests natively (`cargo test -p low-app-layer`), and the route
//! handlers in `routes.rs` are thin worker glue over these functions.

use serde_json::json;

/// Hard cap on outpoints per `/utxo-status` request. Over the cap → 400
/// (bounds the per-request D1 work; a client with more splits the call).
pub const MAX_OUTPOINTS: usize = 64;

/// Cloudflare D1 caps a single prepared statement at 100 bound parameters.
pub const D1_MAX_BOUND_PARAMS: usize = 100;

/// The batch WHERE binds 2 params per outpoint (`txid` + `outputIndex`).
pub const BINDS_PER_OUTPOINT: usize = 2;

/// The largest number of outpoints one D1 statement may carry.
///
/// Derived from the D1 cap: `floor(100 / 2) = 50` outpoints × 2 binds = 100
/// bound params, exactly at the ceiling — so a *single* query of >50 outpoints
/// is the mainnet 503 bug (51 × 2 = 102 > 100). We chunk at **45**, below the
/// 50 hard boundary, to keep margin: a future column added to the batch WHERE,
/// or any stray extra bind in the statement, must not silently reintroduce the
/// cap. A request of up to [`MAX_OUTPOINTS`] is served by `ceil(n / 45)`
/// internal D1 queries — each ≤ 45 outpoints ⇒ ≤ 90 binds ⇒ always under 100.
/// The public request contract (input, output shape, [`MAX_OUTPOINTS`] cap)
/// is unchanged; only the internal D1 execution is chunked, so the server can
/// never 503 on a legitimately-sized request regardless of client chunk size.
pub const D1_CHUNK_OUTPOINTS: usize = 45;

// Compile-time proof the chunk size can never exceed the D1 param cap. If
// someone bumps D1_CHUNK_OUTPOINTS (or BINDS_PER_OUTPOINT) past the ceiling,
// the crate stops building — the invariant is enforced, not merely commented.
const _: () = assert!(D1_CHUNK_OUTPOINTS * BINDS_PER_OUTPOINT <= D1_MAX_BOUND_PARAMS);

/// Split a requested outpoint batch into D1-safe sub-batches of at most
/// [`D1_CHUNK_OUTPOINTS`], preserving input order. The route handlers run ONE
/// D1 query per returned chunk and merge the rows into the single response
/// (`assemble_statuses` / `assemble_pots_view` re-key rows onto the requested
/// outpoints, so cross-chunk row order is irrelevant).
///
/// FAIL-SAFE granularity (money-truth, unchanged): if ANY chunk's D1 query
/// errors, the handler surfaces the SAME 503 the caller already handles and
/// serves NO body — a failed chunk is "unknown for those rows", never a
/// fabricated all-unknown/empty result a caller could misread as authoritative
/// (the same batch-failure discipline the client uses). Only after every chunk
/// succeeds are the merged rows assembled, so an absent outpoint is reported
/// unknown/not-spent per the existing contract, never invented.
pub fn chunk_outpoints(outpoints: &[Outpoint]) -> std::slice::Chunks<'_, Outpoint> {
    outpoints.chunks(D1_CHUNK_OUTPOINTS)
}

/// A txid is exactly 32 bytes → 64 hex chars (either case accepted; DB
/// lookups lowercase separately).
pub fn valid_txid(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A compressed secp256k1 identity pubkey is 33 bytes → 66 hex chars (either
/// case accepted; `potparty_records.identity` is lowercase hex, so the query
/// lowercases separately). An empty or wrong-width/non-hex value is NOT a
/// valid identity — `/recovery-view` treats it as an empty result, never an
/// error.
pub fn valid_identity(s: &str) -> bool {
    s.len() == 66 && s.bytes().all(|b| b.is_ascii_hexdigit())
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
    /// Whether the recorded spend was SPV-CONFIRMED when recorded (an
    /// unconfirmed claim can never overwrite a confirmed pointer — see the
    /// overlay's pot `mark_spent`). `Some(bool)` for a known row, `None`
    /// (wire `null`) when unknown — same fail-safe shape as `spent`.
    pub spent_confirmed: Option<bool>,
}

impl OutpointStatus {
    /// No `pot_records` row: `known:false, spent:null, spendingTxid:null,
    /// spentConfirmed:null`.
    pub fn unknown(op: &Outpoint) -> Self {
        Self {
            txid: op.txid.clone(),
            vout: op.vout,
            known: false,
            spent: None,
            spending_txid: None,
            spent_confirmed: None,
        }
    }

    /// A found row: `known:true` with the row's spent flag + spender +
    /// confirmation flag.
    pub fn known(
        op: &Outpoint,
        spent: bool,
        spending_txid: Option<String>,
        spent_confirmed: bool,
    ) -> Self {
        Self {
            txid: op.txid.clone(),
            vout: op.vout,
            known: true,
            spent: Some(spent),
            spending_txid,
            spent_confirmed: Some(spent_confirmed),
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
                "spentConfirmed": e.spent_confirmed,
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
    format!(
        "SELECT txid, outputIndex, spent, spendingTxid, spentConfirmed \
         FROM pot_records WHERE {clause}"
    )
}

/// One `pot_records` row, host-typed (the route converts D1's f64s here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotRecordRow {
    /// Stored lowercase hex.
    pub txid: String,
    pub vout: u32,
    pub spent: bool,
    pub spending_txid: Option<String>,
    /// Whether the recorded spend was SPV-confirmed when recorded.
    pub spent_confirmed: bool,
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
                Some(r) => {
                    OutpointStatus::known(op, r.spent, r.spending_txid.clone(), r.spent_confirmed)
                }
                None => OutpointStatus::unknown(op),
            }
        })
        .collect()
}

// ── /pots-view — the batched DERIVED view (GH bsv-low#163) ────────────────
//
// The zanaadu model completed: the app-layer serves the JOIN the client used
// to assemble itself (per-outpoint /utxo-status + a /beef fan-out per
// spender + /tip). One request → one D1 query answers "which pots moved, by
// what, paying whom" for a whole home/History surface pass.
//
// TRUST POSTURE (unchanged): `spenderRawHex` is served from the same
// `pot_beefs` store `/beef` reads — the CLIENT verifies it hashes to
// `spendingTxid` before use (a lying server can't poison), and unconfirmed
// pointers remain hints; money decisions still require anchored evidence.

/// The single batched `/pots-view` SQL: the `/utxo-status` batch WHERE plus a
/// LEFT JOIN to `pot_beefs` on the recorded spender, so the spender's stored
/// BEEF rides back in the same query. `lower()` defends against a mixed-case
/// spendingTxid write (pot_beefs keys are lowercase); the join still resolves
/// via the pot_beefs PRIMARY KEY per matched row.
pub fn pots_view_join_sql(n: usize) -> String {
    debug_assert!((1..=MAX_OUTPOINTS).contains(&n), "parse_outpoints bounds n");
    let clause = vec!["(p.txid = ? AND p.outputIndex = ?)"; n].join(" OR ");
    format!(
        "SELECT p.txid, p.outputIndex, p.spent, p.spendingTxid, p.spentConfirmed, \
                hex(b.beef) AS spenderBeef \
         FROM pot_records p \
         LEFT JOIN pot_beefs b ON b.txid = lower(p.spendingTxid) \
         WHERE {clause}"
    )
}

/// One `/pots-view` joined row, host-typed: the pot record plus the spender's
/// stored BEEF (as the `hex(beef)` read-back), when the join found one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotsViewRow {
    pub record: PotRecordRow,
    /// `hex(pot_beefs.beef)` for the recorded spender, `None` when the join
    /// missed (no spender recorded, or its BEEF was never stored).
    pub spender_beef_hex: Option<String>,
}

/// One `/pots-view` response entry: the `/utxo-status` fields plus the raw
/// spending tx, extracted server-side from the spender's stored BEEF.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotsViewEntry {
    pub status: OutpointStatus,
    /// The spending tx's RAW bytes as lowercase hex — a HINT the client must
    /// verify (hash == `spendingTxid`) before trusting. `None` whenever the
    /// spender or its bytes aren't available; never a guessed value.
    pub spender_raw_hex: Option<String>,
}

/// Extract the raw bytes of `txid`'s tx from BEEF bytes, as lowercase hex.
/// `None` when the BEEF doesn't parse or carries the tx as txid-only. The
/// BEEF's own txid index is computed by hashing each carried tx, so a hit
/// here is hash-consistent by construction — the client still re-verifies.
pub fn extract_raw_tx_hex(beef_bytes: &[u8], txid: &str) -> Option<String> {
    let mut beef = bsv_rs::transaction::Beef::from_binary(beef_bytes).ok()?;
    let btx = beef.find_txid_mut(&txid.to_ascii_lowercase())?;
    btx.raw_tx_or_compute().map(hex::encode)
}

/// Map the joined batch-query rows back onto the REQUESTED outpoints,
/// input-ordered, extracting each found spender's raw tx from its BEEF. The
/// fail-safe shape mirrors [`assemble_statuses`]: a missing row is
/// `known:false` with all-null facts, and any beef decode/extract failure
/// degrades that entry's `spenderRawHex` to null (never a wrong byte).
pub fn assemble_pots_view(outpoints: &[Outpoint], rows: &[PotsViewRow]) -> Vec<PotsViewEntry> {
    outpoints
        .iter()
        .map(|op| {
            let key_txid = op.db_txid();
            match rows
                .iter()
                .find(|r| r.record.txid.eq_ignore_ascii_case(&key_txid) && r.record.vout == op.vout)
            {
                Some(r) => {
                    let status = OutpointStatus::known(
                        op,
                        r.record.spent,
                        r.record.spending_txid.clone(),
                        r.record.spent_confirmed,
                    );
                    let spender_raw_hex = match (&r.record.spending_txid, &r.spender_beef_hex) {
                        (Some(spender), Some(beef_hex)) => decode_beef_hex(beef_hex)
                            .and_then(|bytes| extract_raw_tx_hex(&bytes, spender)),
                        _ => None,
                    };
                    PotsViewEntry {
                        status,
                        spender_raw_hex,
                    }
                }
                None => PotsViewEntry {
                    status: OutpointStatus::unknown(op),
                    spender_raw_hex: None,
                },
            }
        })
        .collect()
}

/// Assemble the `/pots-view` wire body:
/// `{"tip":<height|null>,"entries":[{…utxo-status fields…,"spenderRawHex"}]}`.
/// `tip` is `null` on a chaintracks fault — the entries are still served
/// (spent-status is D1 truth), and the client falls back to its own `/tip`.
pub fn pots_view_body(entries: &[PotsViewEntry], tip: Option<u64>) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            json!({
                "txid": e.status.txid,
                "vout": e.status.vout,
                "known": e.status.known,
                "spent": e.status.spent,
                "spendingTxid": e.status.spending_txid,
                "spentConfirmed": e.status.spent_confirmed,
                "spenderRawHex": e.spender_raw_hex,
            })
        })
        .collect();
    json!({ "tip": tip, "entries": arr }).to_string()
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

// ── /recovery-view — the seed-only BY-IDENTITY recovery view (bsv-low#189) ─
//
// A seed-only LOW client holds only its identity key. `tm_potparty` /
// `ls_potparty` (bsv-low#188) index "identity X is a party to pot P"; the
// overlay wrote those rows to `potparty_records`. This endpoint answers the
// recovery question in ONE call: the caller's potparty rows JOINed to each
// pot's on-chain spend status (`pot_records`) and its spender bytes
// (`pot_beefs`) — so a recovering client gets its pots + their exit status
// without a lookup-then-per-outpoint `/pots-view` fan-out.
//
// TRUST POSTURE (unchanged from `/pots-view`): `spenderRawHex` is a HINT the
// client hash-verifies against `spendingTxid` before use; an un-indexed pot
// output (no `pot_records` row) is `spent:null` — the fail-safe shape that
// never asserts "unspent" for something this surface hasn't seen spent.

/// The single `/recovery-view` SQL: the caller's `potparty_records` rows,
/// LEFT-JOINed to `pot_records` on the pot outpoint (spend status) and to
/// `pot_beefs` on the recorded spender (its stored BEEF), newest first.
/// Keyed by ONE identity (not a batch of outpoints), so the WHERE is fixed
/// (one `?` bind). `lower()` on the join key defends a mixed-case
/// spendingTxid write (pot_beefs keys are lowercase). `rowid DESC` breaks
/// same-second `createdAt` ties in insertion order (mirrors the overlay's
/// own `list_for_identity`).
pub fn recovery_view_sql() -> &'static str {
    "SELECT pp.gameId, pp.potTxid, pp.potVout, pp.recoveryHeight, \
            pp.opponentIdentity, \
            r.spent, r.spendingTxid, r.spentConfirmed, \
            hex(b.beef) AS spenderBeef \
     FROM potparty_records pp \
     LEFT JOIN pot_records r ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
     LEFT JOIN pot_beefs b ON b.txid = lower(r.spendingTxid) \
     WHERE pp.identity = ? \
     ORDER BY pp.createdAt DESC, pp.rowid DESC"
}

/// One `/recovery-view` joined row, host-typed: the caller's potparty facts
/// plus the LEFT-JOINed pot-spend status and the spender's stored BEEF. The
/// spend fields are `Option` because the join can MISS — a pot the overlay
/// has a party-marker for but no `pot_records` row yet (spend never indexed)
/// yields `None` (fail-safe: never asserted unspent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRow {
    /// Game ID (32 bytes, lowercase hex).
    pub game_id: String,
    /// The pot funding txid (32 bytes, lowercase hex).
    pub pot_txid: String,
    /// The pot output index within `pot_txid`.
    pub pot_vout: u32,
    /// The pre-signed refund's recovery height.
    pub recovery_height: u32,
    /// The opponent seat's compressed identity pubkey (33 bytes, lowercase
    /// hex).
    pub opponent_identity: String,
    /// `pot_records.spent`, or `None` when the pot output has no row yet.
    pub spent: Option<bool>,
    /// The landing-proof spender, when the pot row records one.
    pub spending_txid: Option<String>,
    /// `pot_records.spentConfirmed`, or `None` when the pot output has no row.
    pub spent_confirmed: Option<bool>,
    /// `hex(pot_beefs.beef)` for the recorded spender, `None` when the join
    /// missed (unspent, or the spender's BEEF was never stored).
    pub spender_beef_hex: Option<String>,
}

/// One `/recovery-view` response entry: the caller's potparty facts plus the
/// pot's spend status and the spender's raw tx (extracted server-side from
/// its stored BEEF — a HINT the client hash-verifies against `spendingTxid`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryEntry {
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    pub recovery_height: u32,
    pub opponent_identity: String,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// The spending tx's RAW bytes as lowercase hex — a HINT the client must
    /// verify (hash == `spendingTxid`) before trusting. `None` whenever the
    /// spender or its bytes aren't available; never a guessed value.
    pub spender_raw_hex: Option<String>,
}

/// Map the joined rows to response entries, extracting each recorded
/// spender's raw tx from its stored BEEF. Order is preserved (the SQL already
/// returns newest-first). Any beef decode/extract failure degrades that
/// entry's `spenderRawHex` to null (never a wrong byte) — the same fail-safe
/// as [`assemble_pots_view`].
pub fn assemble_recovery_view(rows: Vec<RecoveryRow>) -> Vec<RecoveryEntry> {
    rows.into_iter()
        .map(|r| {
            let spender_raw_hex = match (&r.spending_txid, &r.spender_beef_hex) {
                (Some(spender), Some(beef_hex)) => {
                    decode_beef_hex(beef_hex).and_then(|bytes| extract_raw_tx_hex(&bytes, spender))
                }
                _ => None,
            };
            RecoveryEntry {
                game_id: r.game_id,
                pot_txid: r.pot_txid,
                pot_vout: r.pot_vout,
                recovery_height: r.recovery_height,
                opponent_identity: r.opponent_identity,
                spent: r.spent,
                spending_txid: r.spending_txid,
                spent_confirmed: r.spent_confirmed,
                spender_raw_hex,
            }
        })
        .collect()
}

/// Assemble the `/recovery-view` wire body:
/// `{"tip":<height|null>,"entries":[{gameId,potTxid,potVout,recoveryHeight,
/// opponentIdentity,spent,spendingTxid,spentConfirmed,spenderRawHex}]}`.
/// `tip` mirrors `/pots-view` (the recovery-height gate needs it) and is
/// `null` on a chaintracks fault — the D1 facts still serve, and the client
/// falls back to its own `/tip`.
pub fn recovery_view_body(entries: &[RecoveryEntry], tip: Option<u64>) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            json!({
                "gameId": e.game_id,
                "potTxid": e.pot_txid,
                "potVout": e.pot_vout,
                "recoveryHeight": e.recovery_height,
                "opponentIdentity": e.opponent_identity,
                "spent": e.spent,
                "spendingTxid": e.spending_txid,
                "spentConfirmed": e.spent_confirmed,
                "spenderRawHex": e.spender_raw_hex,
            })
        })
        .collect();
    json!({ "tip": tip, "entries": arr }).to_string()
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

    // ── D1-safe chunking (the 100-bound-param cap fix) ─────────────────

    /// Build `n` distinct outpoints (unique vouts) to feed the chunker.
    fn n_outpoints(n: usize) -> Vec<Outpoint> {
        (0..n)
            .map(|i| Outpoint {
                txid: txid_a(),
                vout: i as u32,
            })
            .collect()
    }

    // (The chunk size vs the D1 100-param cap is enforced at COMPILE TIME by
    // the `const _: () = assert!(…)` next to D1_CHUNK_OUTPOINTS — a runtime
    // test of those constants would be redundant. The per-N test below proves
    // the derived bound holds for every produced chunk.)

    /// Every chunk is non-empty, ≤ the D1-safe bound, order-preserving, and the
    /// chunks concatenate back to the exact input — for every N up to the cap.
    #[test]
    fn chunk_outpoints_never_exceeds_the_bound_for_any_n() {
        for n in 1..=MAX_OUTPOINTS {
            let ops = n_outpoints(n);
            let chunks: Vec<&[Outpoint]> = chunk_outpoints(&ops).collect();
            // Count = ceil(n / chunk).
            let expected = n.div_ceil(D1_CHUNK_OUTPOINTS);
            assert_eq!(chunks.len(), expected, "n={n}");
            // Every chunk ≤ the D1-safe bound (⇒ ≤ 100 binds), none empty.
            for c in &chunks {
                assert!(!c.is_empty(), "n={n}: empty chunk");
                assert!(c.len() <= D1_CHUNK_OUTPOINTS, "n={n}: chunk too big");
                assert!(
                    c.len() * BINDS_PER_OUTPOINT <= D1_MAX_BOUND_PARAMS,
                    "n={n}: chunk would exceed D1 param cap"
                );
            }
            // Sizes sum to n, and order is preserved (flatten == input).
            let flat: Vec<&Outpoint> = chunks.iter().flat_map(|c| c.iter()).collect();
            assert_eq!(flat.len(), n, "n={n}");
            assert!(flat.iter().zip(ops.iter()).all(|(a, b)| *a == b), "n={n}");
        }
    }

    #[test]
    fn chunk_single_outpoint_is_one_batch() {
        let ops = n_outpoints(1);
        let chunks: Vec<&[Outpoint]> = chunk_outpoints(&ops).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
    }

    #[test]
    fn chunk_at_and_around_the_boundary() {
        // Exactly the chunk size → one batch.
        let exact = n_outpoints(D1_CHUNK_OUTPOINTS);
        let exact_chunks: Vec<&[Outpoint]> = chunk_outpoints(&exact).collect();
        assert_eq!(exact_chunks.len(), 1);
        assert_eq!(exact_chunks[0].len(), D1_CHUNK_OUTPOINTS);
        // One over → two batches: full + remainder of 1.
        let over_ops = n_outpoints(D1_CHUNK_OUTPOINTS + 1);
        let over: Vec<&[Outpoint]> = chunk_outpoints(&over_ops).collect();
        assert_eq!(over.len(), 2);
        assert_eq!(over[0].len(), D1_CHUNK_OUTPOINTS);
        assert_eq!(over[1].len(), 1);
        // The old single-query 503 boundary (51 outpoints) now splits cleanly
        // — the first chunk (45) is well under the 100-param cap.
        let fifty_one_ops = n_outpoints(51);
        let fifty_one: Vec<&[Outpoint]> = chunk_outpoints(&fifty_one_ops).collect();
        assert_eq!(fifty_one.len(), 2);
        assert_eq!(fifty_one[0].len(), 45);
        assert_eq!(fifty_one[1].len(), 6);
    }

    #[test]
    fn chunk_at_max_outpoints_splits_correctly() {
        // A full-cap request (64) → ceil(64/45) = 2 chunks (45 + 19), each
        // under the D1 param cap — the whole cap is servable without a 503.
        let ops = n_outpoints(MAX_OUTPOINTS);
        let chunks: Vec<&[Outpoint]> = chunk_outpoints(&ops).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 45);
        assert_eq!(chunks[1].len(), 19);
        assert_eq!(chunks[0].len() + chunks[1].len(), MAX_OUTPOINTS);
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
            OutpointStatus::known(&op_a, true, Some("f0".repeat(32)), true),
            OutpointStatus::known(&op_a, false, None, false),
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
        assert_eq!(arr[0]["spentConfirmed"], true);
        // Known-unspent row.
        assert_eq!(arr[1]["known"], true);
        assert_eq!(arr[1]["spent"], false);
        assert!(arr[1]["spendingTxid"].is_null());
        assert_eq!(arr[1]["spentConfirmed"], false);
        // Unknown row: fail-safe nulls, never asserted unspent.
        assert_eq!(arr[2]["txid"], txid_b());
        assert_eq!(arr[2]["vout"], 1);
        assert_eq!(arr[2]["known"], false);
        assert!(arr[2]["spent"].is_null());
        assert!(arr[2]["spendingTxid"].is_null());
        assert!(arr[2]["spentConfirmed"].is_null());
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
            "SELECT txid, outputIndex, spent, spendingTxid, spentConfirmed FROM pot_records \
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
                spent_confirmed: false,
            },
            PotRecordRow {
                txid: txid_b(),
                vout: 1,
                spent: true,
                spending_txid: Some("f0".repeat(32)),
                spent_confirmed: true,
            },
        ];
        let out = assemble_statuses(&ops, &rows);
        assert_eq!(out.len(), 3);
        assert_eq!((out[0].known, out[0].spent), (true, Some(true)));
        assert_eq!(out[0].spending_txid.as_deref(), Some("f0".repeat(32).as_str()));
        assert_eq!(out[0].spent_confirmed, Some(true));
        // Fail-safe middle: no row → known:false, spent:null,
        // spentConfirmed:null.
        assert_eq!((out[1].known, out[1].spent), (false, None));
        assert_eq!(out[1].spent_confirmed, None);
        assert_eq!((out[2].known, out[2].spent), (true, Some(false)));
        assert_eq!(out[2].spent_confirmed, Some(false));
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
            spent_confirmed: false,
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

    // ── /pots-view ─────────────────────────────────────────────────────

    /// A minimal real tx (1 input, 1 output) + its BEEF bytes + txid, built
    /// with the same bsv-rs the extraction uses — the fixture exercises the
    /// REAL producer path (BEEF round-trip), not hand-fed bytes.
    fn beef_fixture() -> (Vec<u8>, String, String) {
        use bsv_rs::transaction::Beef;
        // A syntactically-valid raw tx: version 1, 1 input (null outpoint,
        // empty script, seq ffffffff), 1 output (1 sat, empty script), lock 0.
        let raw_hex = "0100000001".to_string()
            + &"00".repeat(32)
            + "ffffffff"
            + "00"
            + "ffffffff"
            + "01"
            + "0100000000000000"
            + "00"
            + "00000000";
        let raw = hex::decode(&raw_hex).unwrap();
        let mut beef = Beef::new();
        let txid = beef.merge_raw_tx(raw.clone(), None).txid();
        (beef.to_binary(), raw_hex, txid)
    }

    #[test]
    fn pots_view_sql_shapes() {
        let one = pots_view_join_sql(1);
        assert!(one.contains("LEFT JOIN pot_beefs b ON b.txid = lower(p.spendingTxid)"));
        assert!(one.contains("hex(b.beef) AS spenderBeef"));
        assert_eq!(one.matches("(p.txid = ? AND p.outputIndex = ?)").count(), 1);
        let three = pots_view_join_sql(3);
        assert_eq!(three.matches("(p.txid = ? AND p.outputIndex = ?)").count(), 3);
        assert_eq!(three.matches(" OR ").count(), 2);
    }

    #[test]
    fn extract_raw_tx_hex_roundtrip_and_misses() {
        let (beef_bytes, raw_hex, txid) = beef_fixture();
        // The carried tx extracts to its exact raw bytes (either txid case).
        assert_eq!(extract_raw_tx_hex(&beef_bytes, &txid), Some(raw_hex.clone()));
        assert_eq!(
            extract_raw_tx_hex(&beef_bytes, &txid.to_ascii_uppercase()),
            Some(raw_hex)
        );
        // A txid the BEEF doesn't carry → None.
        assert_eq!(extract_raw_tx_hex(&beef_bytes, &"ab".repeat(32)), None);
        // Garbage bytes → None, never a panic.
        assert_eq!(extract_raw_tx_hex(&[0x00, 0x01, 0x02], &txid), None);
        assert_eq!(extract_raw_tx_hex(&[], &txid), None);
    }

    #[test]
    fn assemble_pots_view_joins_and_fail_safes() {
        let (beef_bytes, raw_hex, spender) = beef_fixture();
        let beef_hex_upper = hex::encode(&beef_bytes).to_ascii_uppercase(); // SQLite hex() shape
        let ops = vec![
            Outpoint { txid: txid_a(), vout: 0 }, // spent, beef joined
            Outpoint { txid: txid_a(), vout: 1 }, // spent, beef row MISSING
            Outpoint { txid: txid_b(), vout: 0 }, // unknown outpoint
            Outpoint { txid: txid_b(), vout: 2 }, // known-unspent
        ];
        let rows = vec![
            PotsViewRow {
                record: PotRecordRow {
                    txid: txid_a(),
                    vout: 0,
                    spent: true,
                    spending_txid: Some(spender.clone()),
                    spent_confirmed: true,
                },
                spender_beef_hex: Some(beef_hex_upper),
            },
            PotsViewRow {
                record: PotRecordRow {
                    txid: txid_a(),
                    vout: 1,
                    spent: true,
                    spending_txid: Some(spender.clone()),
                    spent_confirmed: false,
                },
                spender_beef_hex: None,
            },
            PotsViewRow {
                record: PotRecordRow {
                    txid: txid_b(),
                    vout: 2,
                    spent: false,
                    spending_txid: None,
                    spent_confirmed: false,
                },
                spender_beef_hex: None,
            },
        ];
        let out = assemble_pots_view(&ops, &rows);
        assert_eq!(out.len(), 4);
        // Joined: the raw rides back.
        assert_eq!(out[0].status.spent, Some(true));
        assert_eq!(out[0].status.spending_txid.as_deref(), Some(spender.as_str()));
        assert_eq!(out[0].spender_raw_hex.as_deref(), Some(raw_hex.as_str()));
        // Spender recorded but no stored BEEF → pointer yes, raw null.
        assert_eq!(out[1].status.spending_txid.as_deref(), Some(spender.as_str()));
        assert_eq!(out[1].spender_raw_hex, None);
        // Unknown: fail-safe nulls (never asserted unspent).
        assert_eq!((out[2].status.known, out[2].status.spent), (false, None));
        assert_eq!(out[2].spender_raw_hex, None);
        // Known-unspent: no spender, no raw.
        assert_eq!((out[3].status.known, out[3].status.spent), (true, Some(false)));
        assert_eq!(out[3].spender_raw_hex, None);
    }

    #[test]
    fn assemble_pots_view_degrades_on_corrupt_beef() {
        let ops = vec![Outpoint { txid: txid_a(), vout: 0 }];
        let rows = vec![PotsViewRow {
            record: PotRecordRow {
                txid: txid_a(),
                vout: 0,
                spent: true,
                spending_txid: Some("f0".repeat(32)),
                spent_confirmed: true,
            },
            spender_beef_hex: Some("not-hex!!".to_string()),
        }];
        let out = assemble_pots_view(&ops, &rows);
        // The pointer facts survive; only the raw degrades to null.
        assert_eq!(out[0].status.spent, Some(true));
        assert_eq!(out[0].spender_raw_hex, None);
    }

    #[test]
    fn pots_view_body_shape() {
        let op = Outpoint { txid: txid_a(), vout: 0 };
        let entries = vec![
            PotsViewEntry {
                status: OutpointStatus::known(&op, true, Some("f0".repeat(32)), true),
                spender_raw_hex: Some("aabb".to_string()),
            },
            PotsViewEntry {
                status: OutpointStatus::unknown(&Outpoint { txid: txid_b(), vout: 1 }),
                spender_raw_hex: None,
            },
        ];
        let v: serde_json::Value = serde_json::from_str(&pots_view_body(&entries, Some(958_123))).unwrap();
        assert_eq!(v["tip"], 958_123);
        let arr = v["entries"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["txid"], txid_a());
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], "f0".repeat(32));
        assert_eq!(arr[0]["spentConfirmed"], true);
        assert_eq!(arr[0]["spenderRawHex"], "aabb");
        assert_eq!(arr[1]["known"], false);
        assert!(arr[1]["spent"].is_null());
        assert!(arr[1]["spenderRawHex"].is_null());
        // A chaintracks fault serves entries with a null tip.
        let v2: serde_json::Value = serde_json::from_str(&pots_view_body(&entries, None)).unwrap();
        assert!(v2["tip"].is_null());
        assert_eq!(v2["entries"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn tip_and_health_bodies() {
        let v: serde_json::Value = serde_json::from_str(&tip_body(812_345)).unwrap();
        assert_eq!(v["height"], 812_345);
        let h: serde_json::Value = serde_json::from_str(&health_body()).unwrap();
        assert_eq!(h["ok"], true);
        assert_eq!(h["service"], "low-app-layer");
    }

    // ── /recovery-view (bsv-low#189) ───────────────────────────────────

    #[test]
    fn identity_validation() {
        // 66 hex chars (33-byte compressed pubkey), either case.
        assert!(valid_identity(&format!("02{}", "a1".repeat(32))));
        assert!(valid_identity(&"A".repeat(66)));
        // Wrong width / non-hex / empty → not valid (→ empty result, not err).
        assert!(!valid_identity(&"a".repeat(64))); // a txid is not an identity
        assert!(!valid_identity(&"a".repeat(65)));
        assert!(!valid_identity(&"a".repeat(67)));
        assert!(!valid_identity(""));
        assert!(!valid_identity(&"g".repeat(66)));
    }

    #[test]
    fn recovery_view_sql_shape() {
        let sql = recovery_view_sql();
        // JOINs the pot outpoint for spend status and the spender's BEEF.
        assert!(sql.contains(
            "LEFT JOIN pot_records r ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout"
        ));
        assert!(sql.contains("LEFT JOIN pot_beefs b ON b.txid = lower(r.spendingTxid)"));
        assert!(sql.contains("hex(b.beef) AS spenderBeef"));
        // Keyed by ONE identity; newest first.
        assert!(sql.contains("WHERE pp.identity = ?"));
        assert!(sql.contains("ORDER BY pp.createdAt DESC"));
        // Exactly one bind placeholder (single-identity query, not batched).
        assert_eq!(sql.matches('?').count(), 1);
    }

    #[test]
    fn assemble_recovery_view_joins_and_fail_safes() {
        let (beef_bytes, raw_hex, spender) = beef_fixture();
        let beef_hex_upper = hex::encode(&beef_bytes).to_ascii_uppercase(); // SQLite hex() shape
        let rows = vec![
            // Pot spent, spender BEEF joined → raw rides back.
            RecoveryRow {
                game_id: "11".repeat(32),
                pot_txid: txid_a(),
                pot_vout: 0,
                recovery_height: 958_504,
                opponent_identity: format!("03{}", "bb".repeat(32)),
                spent: Some(true),
                spending_txid: Some(spender.clone()),
                spent_confirmed: Some(true),
                spender_beef_hex: Some(beef_hex_upper),
            },
            // Pot spent, spender recorded but no stored BEEF → raw null.
            RecoveryRow {
                game_id: "22".repeat(32),
                pot_txid: txid_b(),
                pot_vout: 1,
                recovery_height: 958_600,
                opponent_identity: format!("03{}", "cc".repeat(32)),
                spent: Some(true),
                spending_txid: Some(spender.clone()),
                spent_confirmed: Some(false),
                spender_beef_hex: None,
            },
            // Party marker but NO pot_records row (spend never indexed) →
            // fail-safe: spent:null, never asserted unspent.
            RecoveryRow {
                game_id: "33".repeat(32),
                pot_txid: "ef".repeat(32),
                pot_vout: 2,
                recovery_height: 958_700,
                opponent_identity: format!("03{}", "dd".repeat(32)),
                spent: None,
                spending_txid: None,
                spent_confirmed: None,
                spender_beef_hex: None,
            },
        ];
        let out = assemble_recovery_view(rows);
        assert_eq!(out.len(), 3);
        // Joined spent pot: the raw rides back, order preserved.
        assert_eq!(out[0].pot_txid, txid_a());
        assert_eq!(out[0].recovery_height, 958_504);
        assert_eq!(out[0].spent, Some(true));
        assert_eq!(out[0].spending_txid.as_deref(), Some(spender.as_str()));
        assert_eq!(out[0].spent_confirmed, Some(true));
        assert_eq!(out[0].spender_raw_hex.as_deref(), Some(raw_hex.as_str()));
        // Spender recorded, no BEEF stored → pointer yes, raw null.
        assert_eq!(out[1].spending_txid.as_deref(), Some(spender.as_str()));
        assert_eq!(out[1].spender_raw_hex, None);
        // No pot row → fail-safe nulls (never asserted unspent).
        assert_eq!(out[2].spent, None);
        assert_eq!(out[2].spending_txid, None);
        assert_eq!(out[2].spent_confirmed, None);
        assert_eq!(out[2].spender_raw_hex, None);
    }

    #[test]
    fn assemble_recovery_view_degrades_on_corrupt_beef() {
        let rows = vec![RecoveryRow {
            game_id: "11".repeat(32),
            pot_txid: txid_a(),
            pot_vout: 0,
            recovery_height: 958_504,
            opponent_identity: format!("03{}", "bb".repeat(32)),
            spent: Some(true),
            spending_txid: Some("f0".repeat(32)),
            spent_confirmed: Some(true),
            spender_beef_hex: Some("not-hex!!".to_string()),
        }];
        let out = assemble_recovery_view(rows);
        // Pointer facts survive; only the raw degrades to null.
        assert_eq!(out[0].spent, Some(true));
        assert_eq!(out[0].spender_raw_hex, None);
    }

    #[test]
    fn recovery_view_body_shape() {
        let entries = vec![
            RecoveryEntry {
                game_id: "11".repeat(32),
                pot_txid: txid_a(),
                pot_vout: 0,
                recovery_height: 958_504,
                opponent_identity: format!("03{}", "bb".repeat(32)),
                spent: Some(true),
                spending_txid: Some("f0".repeat(32)),
                spent_confirmed: Some(true),
                spender_raw_hex: Some("aabb".to_string()),
            },
            RecoveryEntry {
                game_id: "33".repeat(32),
                pot_txid: "ef".repeat(32),
                pot_vout: 2,
                recovery_height: 958_700,
                opponent_identity: format!("03{}", "dd".repeat(32)),
                spent: None,
                spending_txid: None,
                spent_confirmed: None,
                spender_raw_hex: None,
            },
        ];
        let v: serde_json::Value =
            serde_json::from_str(&recovery_view_body(&entries, Some(958_800))).unwrap();
        assert_eq!(v["tip"], 958_800);
        let arr = v["entries"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["gameId"], "11".repeat(32));
        assert_eq!(arr[0]["potTxid"], txid_a());
        assert_eq!(arr[0]["potVout"], 0);
        assert_eq!(arr[0]["recoveryHeight"], 958_504);
        assert_eq!(arr[0]["opponentIdentity"], format!("03{}", "bb".repeat(32)));
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], "f0".repeat(32));
        assert_eq!(arr[0]["spentConfirmed"], true);
        assert_eq!(arr[0]["spenderRawHex"], "aabb");
        // Un-indexed pot: fail-safe nulls.
        assert_eq!(arr[1]["recoveryHeight"], 958_700);
        assert!(arr[1]["spent"].is_null());
        assert!(arr[1]["spendingTxid"].is_null());
        assert!(arr[1]["spentConfirmed"].is_null());
        assert!(arr[1]["spenderRawHex"].is_null());
        // A chaintracks fault serves entries with a null tip.
        let v2: serde_json::Value =
            serde_json::from_str(&recovery_view_body(&entries, None)).unwrap();
        assert!(v2["tip"].is_null());
        assert_eq!(v2["entries"].as_array().unwrap().len(), 2);
        // An empty result (invalid/empty identity) is a well-formed body.
        let v3: serde_json::Value =
            serde_json::from_str(&recovery_view_body(&[], None)).unwrap();
        assert!(v3["tip"].is_null());
        assert_eq!(v3["entries"].as_array().unwrap().len(), 0);
    }
}
