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

// ── /leaderboard — the server-side join + rank (bsv-low #38) ───────────────
//
// The zanaadu model completed for the leaderboard: the app-layer serves the
// aggregation the client's `result.ts gatherBoard` used to assemble itself.
// TODAY the client does 1 `ls_result` lookup + up to 50 `ls_proof` lookups +
// ~57 `/beef` fetches + a `/utxo-status` batch, then verifies + ranks
// client-side (~110 round trips). The app-layer already holds the result
// markers (`result_markers_v2`), the pot spend-status (`pot_records`, the same
// table `/utxo-status` reads) and the proof pointers (`proof_markers`) — so it
// JOINs + ranks server-side and answers the whole board in ONE request.
//
// TRUST DECISION (documented, deliberate — the record surface must never
// lie): the overlay ADMITS result markers by BYTE FORMAT ONLY and NEVER
// verifies signatures; a marker's ECDSA sigs are BRC-42 `'anyone'`-keyed
// (protocolID [1,'low result'], keyID = gameId), whose exact ProtoWallet
// verify round-trip lives in the client (`result.ts verifyResultRow`).
// Reproducing that key-derivation + verify in-worker is impractical and would
// risk a SUBTLY-WRONG re-implementation on a money-adjacent surface. So this
// endpoint COUNTS a win on the presence of BOTH signature pushes
// (`winnerSigHex` AND `loserSigHex`) plus an on-chain ANCHOR (the pot spent by
// the named settle txid, from `pot_records`) — the SAME anchor `/utxo-status`
// reports — and RETURNS both sig hexes + the anchor flag in `evidence` so the
// CLIENT re-verifies the sigs (and re-checks the covenant + anchor) and can
// FALSIFY any win the server counted but did not cryptographically verify.
// The backend organizes; the client verifies. It never asserts a verification
// it did not perform, and every counted win is reconstructible from the
// returned evidence. A singly-signed (unconfirmed) or un-anchored marker is
// STILL returned in evidence (with `anchored`) but does NOT count.
//
// The counting + dedup + ranking rules MIRROR the client's
// `aggregateBoard` / `lowestHands` EXACTLY (a divergence is a bug):
//  - drop un-anchored markers before grouping;
//  - per gameId: a single distinct CONFIRMED (both-sig) winner counts +1;
//    two conflicting confirmed winners (collusion garbage) count for NOBODY;
//    with no confirmed claim, a single distinct winner counts +1 UNCONFIRMED
//    (which never adds to `wins`); conflicting unconfirmed → nobody;
//  - `wins` = the confirmed count; `proven` = wins > 0 (the identity has a
//    doubly-signed, anchored win — the contract's stated proven rule);
//  - `hands` = the lowest-score confirmed + anchored v2 (cards-carrying)
//    hands, one per single-winner game, score ascending then earliest first.

/// Default `?limit` for `/leaderboard` (contract default). Bounds how many
/// recent result markers are scanned — mirrors the client's `recentResults`.
pub const LEADERBOARD_DEFAULT_LIMIT: usize = 200;
/// Hard cap on `?limit` — the same clamp the overlay's `ls_result` lookup
/// service applies (1..=500).
pub const LEADERBOARD_MAX_LIMIT: usize = 500;
/// The pot lock lives at vout 0 (the funding tx's covenant output) — the
/// client anchors on `potTxid:0` (`result.ts gatherBoard`). We join the same.
pub const LEADERBOARD_POT_VOUT: u32 = 0;

/// Clamp a raw `?limit` to `1..=LEADERBOARD_MAX_LIMIT`; absent ⇒ the default.
pub fn clamp_leaderboard_limit(raw: Option<u32>) -> usize {
    match raw {
        Some(n) => (n as usize).clamp(1, LEADERBOARD_MAX_LIMIT),
        None => LEADERBOARD_DEFAULT_LIMIT,
    }
}

/// One `result_markers_v2` row, host-typed — every byte field carried verbatim
/// (the overlay never verifies; the client does). `loser_sig_hex`/`cards_hex`
/// are `None` for an unconfirmed / v1 marker. `created_at` is `None` only for a
/// malformed NULL-`createdAt` row (mirrors the client's nullable `createdAt`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultMarkerRow {
    pub game_id: String,
    pub winner: String,
    pub loser: String,
    pub pot_txid: String,
    pub settle_txid: String,
    pub winner_sig_hex: String,
    pub loser_sig_hex: Option<String>,
    pub cards_hex: Option<String>,
    /// The marker OP_RETURN txid (half its outpoint) — carried for reference.
    pub txid: String,
    pub created_at: Option<i64>,
}

/// The distinct pot outpoints (`potTxid:0`) to spent-status-join, in
/// first-seen marker order (many markers can share a pot — one funding tx,
/// one settle). The route chunks these at [`D1_CHUNK_OUTPOINTS`] exactly like
/// `/utxo-status`, so a large result set never trips D1's 100-bound-param cap.
pub fn leaderboard_pot_outpoints(markers: &[ResultMarkerRow]) -> Vec<Outpoint> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for m in markers {
        if seen.insert(m.pot_txid.to_ascii_lowercase()) {
            out.push(Outpoint {
                txid: m.pot_txid.clone(),
                vout: LEADERBOARD_POT_VOUT,
            });
        }
    }
    out
}

/// Parse a `cardsHex` push (10 lowercase hex chars → 5 card ordinals): five
/// DISTINCT indices 0..=51 (mirrors the client's `cardsFromHex` / the overlay
/// parser's `parse_cards`). `None` on any malformation — such a marker never
/// enters the hands board (fail-safe: an unverifiable hand never counts).
pub fn leaderboard_cards_from_hex(cards_hex: &str) -> Option<[u8; 5]> {
    if cards_hex.len() != 10 {
        return None;
    }
    let bytes = hex::decode(cards_hex).ok()?;
    if bytes.len() != 5 {
        return None;
    }
    let mut arr = [0u8; 5];
    let mut seen = 0u64;
    for (i, &c) in bytes.iter().enumerate() {
        if c > 51 || seen & (1u64 << c) != 0 {
            return None;
        }
        seen |= 1u64 << c;
        arr[i] = c;
    }
    Some(arr)
}

/// The LOW hand score — SUM of card values (Ace=1, 2..10 face value,
/// J/Q/K=10; rank = ordinal % 13 with 0='2'…12='A'). Lowest wins. Byte-for-
/// byte the client's `handScore` (`result.ts`).
pub fn hand_score(cards: &[u8; 5]) -> u32 {
    cards
        .iter()
        .map(|&c| {
            let r = u32::from(c % 13);
            if r == 12 {
                1
            } else if r >= 9 {
                10
            } else {
                r + 2
            }
        })
        .sum()
}

/// One `board[i].evidence[j]` entry — a marker naming this identity as winner,
/// carried verbatim (sigs + anchor) so the client re-verifies WITHOUT
/// re-fetching. `anchored` = the pot spent by the named settle txid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardEvidence {
    pub game_id: String,
    pub winner: String,
    pub loser: String,
    pub pot_txid: String,
    pub settle_txid: String,
    pub winner_sig_hex: String,
    pub loser_sig_hex: Option<String>,
    pub anchored: bool,
    /// The `proof_markers` (ls_proof) marker txid for (gameId, winner), when
    /// one is indexed — a POINTER the client fetches + transcript-verifies,
    /// NOT a server assertion the bundle is valid. `None` when absent.
    pub proof_txid: Option<String>,
}

/// One `board[i]` row — an identity's wins + its evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardBoardRow {
    pub identity: String,
    /// Confirmed (doubly-signed) + anchored wins, deduped per game.
    pub wins: u32,
    /// True iff `wins > 0` (the identity has a doubly-signed, anchored win).
    pub proven: bool,
    pub evidence: Vec<LeaderboardEvidence>,
}

/// One `hands[i]` row — a lowest-winning-hand entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardHandRow {
    pub game_id: String,
    pub score: u32,
    pub cards_hex: String,
    pub winner: String,
    /// Always `true` for a hand row (only anchored + confirmed hands qualify).
    pub anchored: bool,
}

/// The assembled leaderboard, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leaderboard {
    pub board: Vec<LeaderboardBoardRow>,
    pub hands: Vec<LeaderboardHandRow>,
}

/// True iff the marker is anchored: its `potTxid:0` is recorded spent by the
/// named `settleTxid` in `pot_records` — the SAME anchor `/utxo-status`
/// reports. An unknown/unspent/differently-spent pot is NOT anchored
/// (fail-safe: this surface never asserts a win the chain doesn't back).
fn marker_anchored(
    m: &ResultMarkerRow,
    status_by_pot: &std::collections::HashMap<String, &OutpointStatus>,
) -> bool {
    match status_by_pot.get(&m.pot_txid.to_ascii_lowercase()) {
        Some(st) => {
            st.spent == Some(true)
                && st
                    .spending_txid
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(&m.settle_txid))
        }
        None => false,
    }
}

/// Aggregate + rank the leaderboard server-side, mirroring the client's
/// `aggregateBoard` / `lowestHands` (see the module note for the exact rules
/// and the trust decision). `statuses` come from the chunked `pot_records`
/// join (vout 0); `proof_by_game_winner` maps (gameId_lc, winner_lc) → the
/// newest `proof_markers` txid (empty when the join was unavailable — a
/// fail-safe that only drops the `proofTxid` hint, never a count).
pub fn aggregate_leaderboard(
    markers: &[ResultMarkerRow],
    statuses: &[OutpointStatus],
    proof_by_game_winner: &std::collections::HashMap<(String, String), String>,
    hands_limit: usize,
) -> Leaderboard {
    use std::collections::{HashMap, HashSet};

    // Pot spend-status keyed by lowercase txid (we only join vout 0).
    let mut status_by_pot: HashMap<String, &OutpointStatus> = HashMap::new();
    for s in statuses {
        if s.vout == LEADERBOARD_POT_VOUT {
            status_by_pot
                .entry(s.txid.to_ascii_lowercase())
                .or_insert(s);
        }
    }
    // Anchor each marker once.
    let anchored: Vec<bool> = markers
        .iter()
        .map(|m| marker_anchored(m, &status_by_pot))
        .collect();
    // A marker is CONFIRMED (backend sense) when BOTH sig pushes are present.
    let confirmed = |i: usize| markers[i].loser_sig_hex.is_some();

    // ── wins: per-game dedup over ANCHORED markers (client aggregateBoard) ──
    let mut by_game: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, m) in markers.iter().enumerate() {
        if anchored[i] {
            by_game
                .entry(m.game_id.to_ascii_lowercase())
                .or_default()
                .push(i);
        }
    }
    // identity_lc → (confirmed_wins, unconfirmed_wins).
    let mut tally: HashMap<String, (u32, u32)> = HashMap::new();
    for idxs in by_game.values() {
        let confirmed_winners: HashSet<String> = idxs
            .iter()
            .filter(|&&i| confirmed(i))
            .map(|&i| markers[i].winner.to_ascii_lowercase())
            .collect();
        if confirmed_winners.len() == 1 {
            let w = confirmed_winners.into_iter().next().unwrap();
            tally.entry(w).or_default().0 += 1;
            continue;
        }
        if confirmed_winners.len() > 1 {
            continue; // conflicting confirmed claims → count nobody
        }
        // No confirmed claim: a single distinct winner counts UNCONFIRMED.
        let unconfirmed_winners: HashSet<String> = idxs
            .iter()
            .map(|&i| markers[i].winner.to_ascii_lowercase())
            .collect();
        if unconfirmed_winners.len() == 1 {
            let w = unconfirmed_winners.into_iter().next().unwrap();
            tally.entry(w).or_default().1 += 1;
        }
        // conflicting unconfirmed → count nobody.
    }

    // Evidence: every marker (anchored or not) naming this identity as winner.
    let mut ev_by_identity: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, m) in markers.iter().enumerate() {
        ev_by_identity
            .entry(m.winner.to_ascii_lowercase())
            .or_default()
            .push(i);
    }

    let mut rows: Vec<(LeaderboardBoardRow, u32)> = tally
        .iter()
        .map(|(id, &(conf, unconf))| {
            let mut ev_idx = ev_by_identity.get(id).cloned().unwrap_or_default();
            // Anchored+confirmed first, then anchored, then the rest; newest
            // (highest createdAt) first within a tier — a display-friendly
            // drill-down order, not a ranking rule.
            let rank = |i: usize| match (anchored[i], confirmed(i)) {
                (true, true) => 0,
                (true, false) => 1,
                _ => 2,
            };
            ev_idx.sort_by(|&a, &b| {
                rank(a)
                    .cmp(&rank(b))
                    .then(markers[b].created_at.cmp(&markers[a].created_at))
            });
            let evidence = ev_idx
                .iter()
                .map(|&i| {
                    let m = &markers[i];
                    let g = m.game_id.to_ascii_lowercase();
                    let w = m.winner.to_ascii_lowercase();
                    let proof_txid = proof_by_game_winner.get(&(g.clone(), w.clone())).cloned();
                    LeaderboardEvidence {
                        game_id: g,
                        winner: w,
                        loser: m.loser.to_ascii_lowercase(),
                        pot_txid: m.pot_txid.to_ascii_lowercase(),
                        settle_txid: m.settle_txid.to_ascii_lowercase(),
                        winner_sig_hex: m.winner_sig_hex.to_ascii_lowercase(),
                        loser_sig_hex: m.loser_sig_hex.as_ref().map(|s| s.to_ascii_lowercase()),
                        anchored: anchored[i],
                        proof_txid,
                    }
                })
                .collect();
            (
                LeaderboardBoardRow {
                    identity: id.clone(),
                    wins: conf,
                    proven: conf > 0,
                    evidence,
                },
                unconf,
            )
        })
        .collect();
    // Client rank: confirmed desc, then unconfirmed desc, then identity asc
    // (lowercase hex — byte order == localeCompare).
    rows.sort_by(|(a, au), (b, bu)| {
        b.wins
            .cmp(&a.wins)
            .then(bu.cmp(au))
            .then_with(|| a.identity.cmp(&b.identity))
    });
    let board = rows.into_iter().map(|(r, _)| r).collect();

    // ── hands: lowest-score confirmed + anchored v2 hands (lowestHands) ─────
    let mut hand_by_game: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, m) in markers.iter().enumerate() {
        if !anchored[i] || !confirmed(i) {
            continue;
        }
        let Some(ch) = &m.cards_hex else { continue };
        if leaderboard_cards_from_hex(ch).is_none() {
            continue;
        }
        hand_by_game
            .entry(m.game_id.to_ascii_lowercase())
            .or_default()
            .push(i);
    }
    // (row, created_at) so the score-tie break is the earliest claim.
    let mut hands: Vec<(LeaderboardHandRow, Option<i64>)> = Vec::new();
    for idxs in hand_by_game.values() {
        let winners: HashSet<String> = idxs
            .iter()
            .map(|&i| markers[i].winner.to_ascii_lowercase())
            .collect();
        if winners.len() != 1 {
            continue; // conflicting confirmed → count nobody (same as wins)
        }
        // idxs is in ascending marker order; markers are newest-first, so the
        // first index is the newest claim for the game (the client's claims[0]).
        let i = *idxs.iter().min().unwrap();
        let m = &markers[i];
        let cards = leaderboard_cards_from_hex(m.cards_hex.as_ref().unwrap()).unwrap();
        hands.push((
            LeaderboardHandRow {
                game_id: m.game_id.to_ascii_lowercase(),
                score: hand_score(&cards),
                cards_hex: m.cards_hex.as_ref().unwrap().to_ascii_lowercase(),
                winner: m.winner.to_ascii_lowercase(),
                anchored: true,
            },
            m.created_at,
        ));
    }
    // Score ascending; tie → earliest createdAt (None sorts LAST, == the
    // client's `?? Infinity`).
    hands.sort_by(|(a, ac), (b, bc)| {
        a.score.cmp(&b.score).then_with(|| {
            let ak = ac.unwrap_or(i64::MAX);
            let bk = bc.unwrap_or(i64::MAX);
            ak.cmp(&bk)
        })
    });
    let hands = hands
        .into_iter()
        .take(hands_limit)
        .map(|(h, _)| h)
        .collect();

    Leaderboard { board, hands }
}

/// Assemble the `/leaderboard` wire body (the endpoint CONTRACT):
/// `{"board":[…],"hands":[…],"computedAt":<unix>,"resultCount":<int>}`.
pub fn leaderboard_body(lb: &Leaderboard, computed_at: i64, result_count: usize) -> String {
    let board: Vec<serde_json::Value> = lb
        .board
        .iter()
        .map(|r| {
            let evidence: Vec<serde_json::Value> = r
                .evidence
                .iter()
                .map(|e| {
                    json!({
                        "gameId": e.game_id,
                        "winner": e.winner,
                        "loser": e.loser,
                        "potTxid": e.pot_txid,
                        "settleTxid": e.settle_txid,
                        "winnerSigHex": e.winner_sig_hex,
                        "loserSigHex": e.loser_sig_hex,
                        "anchored": e.anchored,
                        "proofTxid": e.proof_txid,
                    })
                })
                .collect();
            json!({
                "identity": r.identity,
                "wins": r.wins,
                "proven": r.proven,
                "evidence": evidence,
            })
        })
        .collect();
    let hands: Vec<serde_json::Value> = lb
        .hands
        .iter()
        .map(|h| {
            json!({
                "gameId": h.game_id,
                "score": h.score,
                "cardsHex": h.cards_hex,
                "winner": h.winner,
                "anchored": h.anchored,
            })
        })
        .collect();
    json!({
        "board": board,
        "hands": hands,
        "computedAt": computed_at,
        "resultCount": result_count,
    })
    .to_string()
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

    // ── /leaderboard aggregation (bsv-low #38) ─────────────────────────────

    use std::collections::HashMap;

    /// 64-hex txid / gameId from a byte.
    fn tx(b: u8) -> String {
        format!("{b:02x}").repeat(32)
    }
    /// 66-hex compressed identity pubkey from a byte (02 prefix + 64 hex).
    fn ident(b: u8) -> String {
        format!("02{}", format!("{b:02x}").repeat(32))
    }

    /// A result marker. `confirmed` ⇒ a loserSig push is present (backend's
    /// "confirmed"); `cards` is a 10-hex v2 cards push or None (v1). The
    /// marker txid is derived from game+winner+seq so distinct markers for the
    /// same (game, winner) are distinct outpoints (the censorship-fix shape).
    #[allow(clippy::too_many_arguments)]
    fn mk(
        game: u8,
        winner: &str,
        loser: &str,
        pot: u8,
        settle: u8,
        confirmed: bool,
        cards: Option<&str>,
        created: i64,
        seq: u8,
    ) -> ResultMarkerRow {
        ResultMarkerRow {
            game_id: tx(game),
            winner: winner.to_string(),
            loser: loser.to_string(),
            pot_txid: tx(pot),
            settle_txid: tx(settle),
            winner_sig_hex: "3045abababab".to_string(),
            loser_sig_hex: confirmed.then(|| "3044cdcdcd".to_string()),
            cards_hex: cards.map(str::to_string),
            txid: format!("{game:02x}{seq:02x}").repeat(16),
            created_at: Some(created),
        }
    }

    /// Build `pot_records`-derived statuses through the REAL producer path
    /// (`leaderboard_pot_outpoints` → PotRecordRow → `assemble_statuses`). Each
    /// entry of `spent_by` (pot txid byte → settle txid byte) marks that pot
    /// spent by that settle txid; pots absent from the map have NO row (unknown
    /// ⇒ un-anchored).
    fn statuses_for(markers: &[ResultMarkerRow], spent_by: &HashMap<u8, u8>) -> Vec<OutpointStatus> {
        let ops = leaderboard_pot_outpoints(markers);
        let mut rows: Vec<PotRecordRow> = Vec::new();
        for op in &ops {
            for (pot, settle) in spent_by {
                if op.db_txid() == tx(*pot) {
                    rows.push(PotRecordRow {
                        txid: op.txid.clone(),
                        vout: 0,
                        spent: true,
                        spending_txid: Some(tx(*settle)),
                        spent_confirmed: true,
                    });
                }
            }
        }
        assemble_statuses(&ops, &rows)
    }

    fn no_proofs() -> HashMap<(String, String), String> {
        HashMap::new()
    }

    #[test]
    fn hand_score_matches_client() {
        // Ace=1, 2..10 face, J/Q/K=10 (rank = ordinal % 13; 0='2'…12='A').
        // cards 0,1,2,3,4 → 2+3+4+5+6 = 20.
        assert_eq!(hand_score(&[0, 1, 2, 3, 4]), 20);
        // cards 0,1,2,3,12(A) → 2+3+4+5+1 = 15.
        assert_eq!(hand_score(&[0, 1, 2, 3, 12]), 15);
        // 8='10'(10), 9='J'(10), 10='Q'(10), 11='K'(10), 12='A'(1) → 41.
        assert_eq!(hand_score(&[8, 9, 10, 11, 12]), 41);
        // cardsHex parse: 10 hex, five distinct 0..=51.
        assert_eq!(leaderboard_cards_from_hex("000102030c"), Some([0, 1, 2, 3, 12]));
        assert_eq!(leaderboard_cards_from_hex("0001020303"), None); // dup
        assert_eq!(leaderboard_cards_from_hex("0001020334"), None); // 0x34=52 > 51
        assert_eq!(leaderboard_cards_from_hex("0102030405060708"), None); // wrong len
    }

    #[test]
    fn counts_a_confirmed_anchored_win() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].identity, a);
        assert_eq!(lb.board[0].wins, 1);
        assert!(lb.board[0].proven);
        assert_eq!(lb.board[0].evidence.len(), 1);
        let ev = &lb.board[0].evidence[0];
        assert!(ev.anchored);
        assert_eq!(ev.winner, a);
        assert_eq!(ev.loser, b);
        assert!(ev.loser_sig_hex.is_some());
    }

    #[test]
    fn singly_signed_marker_does_not_count() {
        // A winnerSig-only (unconfirmed) anchored marker: the identity appears
        // (unconfirmed win) but wins == 0 and proven == false.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].wins, 0, "a singly-signed marker never adds a win");
        assert!(!lb.board[0].proven);
        // The marker is STILL surfaced in evidence (anchored, loserSig null).
        assert_eq!(lb.board[0].evidence.len(), 1);
        assert!(lb.board[0].evidence[0].anchored);
        assert_eq!(lb.board[0].evidence[0].loser_sig_hex, None);
    }

    #[test]
    fn unanchored_marker_does_not_count() {
        // The RED-verified rule: a fully doubly-signed marker whose pot is NOT
        // spent-by-settle contributes NO win and NO board row.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        // Pot 1 has NO pot_records row at all → unknown → un-anchored.
        let statuses = statuses_for(&markers, &HashMap::new());
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert!(
            lb.board.is_empty(),
            "an un-anchored win must not appear on the board"
        );
        assert!(lb.hands.is_empty());

        // Also un-anchored: pot IS spent, but by a DIFFERENT txid than settle.
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 9u8)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert!(
            lb.board.is_empty(),
            "spent-by-a-different-txid is not anchored to this settle"
        );
    }

    #[test]
    fn dedups_a_game_claimed_twice() {
        // Two distinct markers (different outpoints) for the SAME game + winner
        // count as ONE win (per-game dedup, mirroring aggregateBoard).
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(1, &a, &b, 1, 2, true, None, 100, 0),
            mk(1, &a, &b, 1, 2, true, None, 101, 1),
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].wins, 1, "a game claimed twice counts once");
        // Both markers still surface in evidence.
        assert_eq!(lb.board[0].evidence.len(), 2);
    }

    #[test]
    fn conflicting_confirmed_counts_nobody() {
        // Two confirmed anchored markers for the same game name DIFFERENT
        // winners (collusion garbage) → neither counts, no board row.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(1, &a, &b, 1, 2, true, None, 100, 0),
            mk(1, &b, &a, 1, 2, true, None, 101, 1),
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert!(
            lb.board.is_empty(),
            "conflicting confirmed claims count for nobody"
        );
        assert!(lb.hands.is_empty());
    }

    #[test]
    fn board_ranks_by_wins_desc_then_identity() {
        // A: 2 confirmed wins; B: 1; C: 1. Ordered A, then B/C by identity asc.
        let a = ident(0xaa);
        let b = ident(0x0b);
        let c = ident(0x0c);
        let z = ident(0xff); // shared loser
        let markers = vec![
            mk(1, &a, &z, 1, 2, true, None, 100, 0),
            mk(2, &a, &z, 3, 4, true, None, 101, 0),
            mk(3, &b, &z, 5, 6, true, None, 102, 0),
            mk(4, &c, &z, 7, 8, true, None, 103, 0),
        ];
        let statuses = statuses_for(
            &markers,
            &HashMap::from([(1u8, 2u8), (3, 4), (5, 6), (7, 8)]),
        );
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb.board.len(), 3);
        assert_eq!(lb.board[0].identity, a);
        assert_eq!(lb.board[0].wins, 2);
        // b (0x0b…) sorts before c (0x0c…) at equal wins.
        assert_eq!(lb.board[1].identity, b);
        assert_eq!(lb.board[2].identity, c);
    }

    #[test]
    fn lowest_hands_ordering() {
        // Two confirmed anchored v2 hands: game 1 scores 15, game 2 scores 20.
        // Lowest (15) first.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(2, &a, &b, 3, 4, true, Some("0001020304"), 200, 0), // score 20
            mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0), // score 15
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8), (3, 4)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb.hands.len(), 2);
        assert_eq!(lb.hands[0].score, 15);
        assert_eq!(lb.hands[0].game_id, tx(1));
        assert!(lb.hands[0].anchored);
        assert_eq!(lb.hands[1].score, 20);

        // A v1 (no-cards) or an un-anchored hand never appears.
        let markers2 = vec![
            mk(1, &a, &b, 1, 2, true, None, 100, 0), // no cards
            mk(2, &a, &b, 3, 4, true, Some("0001020304"), 200, 0), // un-anchored below
        ];
        let statuses2 = statuses_for(&markers2, &HashMap::from([(1u8, 2u8)])); // pot 3 unspent
        let lb2 = aggregate_leaderboard(&markers2, &statuses2, &no_proofs(), 200);
        assert!(lb2.hands.is_empty(), "no-cards + un-anchored ⇒ no hands");
    }

    #[test]
    fn score_tie_breaks_on_earliest_created_at() {
        // Same score, different games — earlier createdAt ranks first.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(2, &a, &b, 3, 4, true, Some("0001020304"), 500, 0), // score 20, later
            mk(1, &a, &b, 1, 2, true, Some("0001020304"), 100, 0), // score 20, earlier
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8), (3, 4)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb.hands.len(), 2);
        assert_eq!(lb.hands[0].game_id, tx(1), "earlier claim wins the score tie");
        assert_eq!(lb.hands[1].game_id, tx(2));
    }

    #[test]
    fn proof_pointer_is_carried_but_never_gates_a_count() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let proofs =
            HashMap::from([((tx(1), a.clone()), "proof-txid".to_string())]);
        let lb = aggregate_leaderboard(&markers, &statuses, &proofs, 200);
        assert_eq!(lb.board[0].evidence[0].proof_txid.as_deref(), Some("proof-txid"));
        // Absent proof → null, count unchanged.
        let lb2 = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb2.board[0].evidence[0].proof_txid, None);
        assert_eq!(lb2.board[0].wins, 1);
    }

    #[test]
    fn chunked_spent_status_join_over_45_pots() {
        // >45 distinct pots exceed a single D1 statement's 100-bound-param cap;
        // the route chunks. Drive the REAL producer path here: build 50 markers
        // (distinct game+pot each), chunk the outpoints, build each chunk's
        // pot_records rows separately, merge, and assemble — the anchoring must
        // resolve identically across the chunk boundary.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let mut markers = Vec::new();
        // Anchor every EVEN-indexed pot; leave the odd ones un-anchored.
        let mut spent_by: HashMap<u8, u8> = HashMap::new();
        for i in 0u8..50 {
            // Distinct game/pot/settle bytes per marker (0..50 all distinct).
            let game = i;
            let pot = 100 + i; // 100..150, distinct from settle range
            let settle = 200u16.wrapping_add(u16::from(i)) as u8; // just a distinct byte
            markers.push(mk(game, &a, &b, pot, settle, true, None, i64::from(i), 0));
            if i % 2 == 0 {
                spent_by.insert(pot, settle);
            }
        }
        // Sanity: 50 distinct outpoints ⇒ 2 chunks (45 + 5), crossing the cap.
        let ops = leaderboard_pot_outpoints(&markers);
        assert_eq!(ops.len(), 50);
        let chunks: Vec<&[Outpoint]> = chunk_outpoints(&ops).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!((chunks[0].len(), chunks[1].len()), (45, 5));

        // Build pot rows PER CHUNK (as the route does) then merge + assemble.
        let mut pot_rows: Vec<PotRecordRow> = Vec::new();
        for chunk in &chunks {
            for op in *chunk {
                for (pot, settle) in &spent_by {
                    if op.db_txid() == tx(*pot) {
                        pot_rows.push(PotRecordRow {
                            txid: op.txid.clone(),
                            vout: 0,
                            spent: true,
                            spending_txid: Some(tx(*settle)),
                            spent_confirmed: true,
                        });
                    }
                }
            }
        }
        let statuses = assemble_statuses(&ops, &pot_rows);
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);

        // 25 even pots anchored ⇒ 25 confirmed wins for identity a.
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].identity, a);
        assert_eq!(lb.board[0].wins, 25, "only the 25 anchored pots count");
        // A pot from the SECOND chunk (index 46, even ⇒ anchored) must count —
        // proves the merge crosses the chunk boundary. Its evidence entry is
        // anchored; an odd (un-anchored) one is not.
        let anchored_games: std::collections::HashSet<String> = lb.board[0]
            .evidence
            .iter()
            .filter(|e| e.anchored)
            .map(|e| e.game_id.clone())
            .collect();
        assert!(anchored_games.contains(&tx(46)), "2nd-chunk even pot anchored");
        assert!(!anchored_games.contains(&tx(47)), "odd pot un-anchored");
    }

    #[test]
    fn leaderboard_body_shape() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let proofs = HashMap::from([((tx(1), a.clone()), "px".to_string())]);
        let lb = aggregate_leaderboard(&markers, &statuses, &proofs, 200);
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1)).unwrap();
        assert_eq!(v["computedAt"], 1_700_000_000_i64);
        assert_eq!(v["resultCount"], 1);
        let board = v["board"].as_array().unwrap();
        assert_eq!(board.len(), 1);
        assert_eq!(board[0]["identity"], a);
        assert_eq!(board[0]["wins"], 1);
        assert_eq!(board[0]["proven"], true);
        let ev = board[0]["evidence"].as_array().unwrap();
        assert_eq!(ev[0]["gameId"], tx(1));
        assert_eq!(ev[0]["winner"], a);
        assert_eq!(ev[0]["loser"], b);
        assert_eq!(ev[0]["potTxid"], tx(1));
        assert_eq!(ev[0]["settleTxid"], tx(2));
        assert!(ev[0]["winnerSigHex"].is_string());
        assert!(ev[0]["loserSigHex"].is_string());
        assert_eq!(ev[0]["anchored"], true);
        assert_eq!(ev[0]["proofTxid"], "px");
        let hands = v["hands"].as_array().unwrap();
        assert_eq!(hands.len(), 1);
        assert_eq!(hands[0]["gameId"], tx(1));
        assert_eq!(hands[0]["score"], 15);
        assert_eq!(hands[0]["cardsHex"], "000102030c");
        assert_eq!(hands[0]["winner"], a);
        assert_eq!(hands[0]["anchored"], true);
    }

    #[test]
    fn clamp_limit_defaults_and_bounds() {
        assert_eq!(clamp_leaderboard_limit(None), LEADERBOARD_DEFAULT_LIMIT);
        assert_eq!(clamp_leaderboard_limit(Some(50)), 50);
        assert_eq!(clamp_leaderboard_limit(Some(0)), 1);
        assert_eq!(
            clamp_leaderboard_limit(Some(99_999)),
            LEADERBOARD_MAX_LIMIT
        );
    }
}
