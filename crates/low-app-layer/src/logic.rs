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
    /// Whether this surface has a verified answer for the outpoint.
    ///
    /// The MEANING IS PRODUCER-SPECIFIC, and conflating the two caused a
    /// real misdiagnosis (#323 defect 2 was filed as a `pot_records`
    /// coverage hole on a route that never reads that table):
    /// - `/utxo-status` (D1-backed): `known` = a `pot_records` row exists.
    /// - `/spent-any` (live WoC+Bitails): `known` = the providers gave an
    ///   answer this surface could VERIFY. `pot_records` is never consulted.
    ///   A provider fault therefore yields `known:false` — see [`Self::reason`].
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
    /// WHY this answer is `known:false` — `None` on every `known:true` row
    /// and on the D1-backed `/utxo-status` path (where absence genuinely
    /// means "no row"). Set by `/spent-any` so an upstream OUTAGE is legible
    /// instead of masquerading as "there is nothing there" (#323 defect 2).
    ///
    /// This SURFACES the ambiguity rather than resolving it: the fail-safe
    /// answer is unchanged (an unverifiable pointer is never asserted), but
    /// the caller — and the next auditor — can now tell a provider fault
    /// from a corroborated negative. Deliberately NOT resolved by falling
    /// back to `pot_records`: that table can hold a PARKED, never-mined
    /// pointer (#323 defect 1), so a fallback would trade an honest unknown
    /// for a confident wrong answer, and would break this route's contract
    /// that every positive is raw-hash + input-match verified.
    pub reason: Option<&'static str>,
}

/// `/spent-any`: an upstream provider faulted — we could not look. NOT a
/// statement about the outpoint.
pub const SPENT_ANY_REASON_PROVIDER_FAULT: &str = "provider-fault";
/// `/spent-any`: a spender was reported but its raw could not be fetched or
/// did not hash/input-match, so the pointer is unverifiable.
pub const SPENT_ANY_REASON_UNVERIFIED_SPENDER: &str = "unverified-spender";
/// `/spent-any`: reported unspent, but without the independent corroboration
/// this surface requires before serving a negative.
pub const SPENT_ANY_REASON_UNCORROBORATED: &str = "uncorroborated-unspent";

impl OutpointStatus {
    /// No verified answer: `known:false, spent:null, spendingTxid:null,
    /// spentConfirmed:null`, and no reason (the D1 `/utxo-status` shape,
    /// where absence of a row IS the answer). `/spent-any` uses
    /// [`Self::unknown_because`] so its faults stay legible.
    pub fn unknown(op: &Outpoint) -> Self {
        Self {
            txid: op.txid.clone(),
            vout: op.vout,
            known: false,
            spent: None,
            spending_txid: None,
            spent_confirmed: None,
            reason: None,
        }
    }

    /// [`Self::unknown`] carrying WHY — the `/spent-any` shape.
    pub fn unknown_because(op: &Outpoint, reason: &'static str) -> Self {
        Self {
            reason: Some(reason),
            ..Self::unknown(op)
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
            reason: None,
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
                // #323 defect 2 — present only when this surface could not
                // verify an answer; null everywhere else (including every
                // known:true row and the whole D1 /utxo-status path).
                "reason": e.reason,
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
/// # Why this is not a plain `ORDER BY … LIMIT` (#323 HIGH-1)
///
/// `potparty_records.identity` is ATTACKER-WRITABLE: the overlay admits
/// those markers by BYTE FORMAT with no signature check, and the production
/// write is `INSERT OR IGNORE` on the marker OUTPOINT, so every distinct
/// `(txid, outputIndex)` lands — anyone can file unlimited rows naming
/// anyone. A naive `ORDER BY pp.createdAt DESC LIMIT n` is therefore a
/// FLOOD-TO-EVICT primitive: ~`n` fresh dust markers naming the victim take
/// every slot, and the victim's real pots vanish from their own recovery
/// view **while the response still looks complete**. That is strictly worse
/// than the unbounded read it replaced (which was merely noisy — every
/// honest row was still present), i.e. a self-healing failure traded for a
/// permanent one on a money-visible surface.
///
/// So this mirrors `results_sql`'s three defences verbatim:
///
/// 1. **Dedupe in SQL, keeping the OLDEST marker per pot**
///    (`PARTITION BY pp.potTxid, pp.potVout ORDER BY pp.createdAt ASC` →
///    `rn = 1`). Oldest-wins is anti-squat: a later marker cannot displace
///    the original, and one pot can occupy only one slot however many
///    markers exist for it.
/// 2. **Rank by the POT's own admission stamp**
///    (`COALESCE(potCreatedAt, markerCreatedAt) DESC`). `pot_records.createdAt`
///    is written by the overlay's own admission, so an attacker cannot
///    backdate or advance it by filing markers — the ordering an attacker
///    controls is used only as the fallback for pots with no row yet.
/// 3. **Reserve a quota for unknown pots** (`tier`): rows with no
///    `pot_records` row are demoted behind every indexed pot EXCEPT for the
///    newest [`RECOVERY_VIEW_UNKNOWN_QUOTA`], so ghost rows are bounded to a
///    small reserved slice instead of the whole page, while a real-but-not-
///    yet-indexed pot (the one a recovering client most needs) is not erased.
///
/// The BEEF join sits OUTSIDE the window, on the survivors only, so a flood
/// can never drag real BLOBs along with it.
pub fn recovery_view_sql() -> String {
    // NOTE: any change here must keep the `w.`-qualified outer ORDER BY —
    // SQLite does not guarantee ordering survives a join otherwise.
    //
    // The window takes MAX_ROWS + 1 so the caller can detect truncation
    // without a second COUNT query (see `assemble_recovery_view`).
    format!(
        "SELECT w.gameId AS gameId, w.potTxid AS potTxid, w.potVout AS potVout, \
            w.recoveryHeight AS recoveryHeight, \
            w.covRecoveryHeight AS covRecoveryHeight, \
            w.opponentIdentity AS opponentIdentity, \
            w.spent AS spent, w.spendingTxid AS spendingTxid, \
            w.spentConfirmed AS spentConfirmed, \
            hex(b.beef) AS spenderBeef \
     FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
              opponentIdentity, spent, spendingTxid, spentConfirmed, \
              markerCreatedAt, markerRowid, potCreatedAt, \
              CASE WHEN unknownPot = 0 OR potRank <= {quota} THEN 0 ELSE 1 END AS tier \
       FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
                opponentIdentity, spent, spendingTxid, spentConfirmed, \
                markerCreatedAt, markerRowid, potCreatedAt, unknownPot, \
                ROW_NUMBER() OVER (PARTITION BY unknownPot \
                                   ORDER BY COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                                            markerCreatedAt DESC, markerRowid DESC) AS potRank \
         FROM (SELECT pp.gameId AS gameId, pp.potTxid AS potTxid, \
                  pp.potVout AS potVout, pp.recoveryHeight AS recoveryHeight, \
                  r.recoveryHeight AS covRecoveryHeight, \
                  pp.opponentIdentity AS opponentIdentity, \
                  r.spent AS spent, r.spendingTxid AS spendingTxid, \
                  r.spentConfirmed AS spentConfirmed, \
                  pp.createdAt AS markerCreatedAt, pp.rowid AS markerRowid, \
                  r.createdAt AS potCreatedAt, \
                  CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                  ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout \
                                     ORDER BY pp.createdAt ASC, pp.rowid ASC) AS rn \
           FROM potparty_records pp \
           LEFT JOIN pot_records r \
                  ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
           WHERE pp.identity = ?) \
         WHERE rn = 1) \
       ORDER BY tier ASC, COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                markerCreatedAt DESC, markerRowid DESC \
       LIMIT {probe}) w \
     LEFT JOIN pot_beefs b ON b.txid = lower(w.spendingTxid) \
     ORDER BY w.tier ASC, COALESCE(w.potCreatedAt, w.markerCreatedAt) DESC, \
              w.markerCreatedAt DESC, w.markerRowid DESC",
        quota = RECOVERY_VIEW_UNKNOWN_QUOTA,
        probe = RECOVERY_VIEW_MAX_ROWS + 1,
    )
}

/// Hard bound on `/recovery-view` DISTINCT POTS served per request (#323).
/// The SQL dedupes per pot before the window, so this is a pot cap, not a
/// marker-row cap — a flood of markers for one pot occupies exactly one slot.
///
/// Matches [`crate::results::RESULTS_MAX_ROWS`] (100): the two views are
/// driven by the same `potparty_records` scope for the same identity, so a
/// caller whose `/results` page is complete has a complete `/recovery-view`
/// page too.
pub const RECOVERY_VIEW_MAX_ROWS: usize = 100;

/// How many of the newest pots ABSENT from `pot_records` are promoted into
/// the main `/recovery-view` tier instead of being demoted behind every
/// indexed pot. Mirrors [`crate::results::RESULTS_UNKNOWN_POT_QUOTA`] and
/// exists for the same reason: a strict existence tier silently becomes a
/// FILTER once `LIMIT` binds, dropping the genuinely fresh pot whose `tm_pot`
/// admission is still in flight — precisely the pot a recovering client most
/// needs — while an unbounded promotion would let free, invented-pot rows
/// occupy the whole page.
pub const RECOVERY_VIEW_UNKNOWN_QUOTA: usize = 10;

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
    /// The caller's OWN potparty marker height — byte-format-admitted, a
    /// HINT. #323 MEDIUM-3: under oldest-marker-wins an attacker who files
    /// a marker BEFORE the victim's own becomes the sole source of this
    /// field, and `/recovery-view` serves no `sigHex` for the client to
    /// collapse candidates itself — so the covenant value below is preferred.
    pub recovery_height: u32,
    /// The COVENANT-COMMITTED recoveryHeight decoded from the admitted
    /// funding lock (#284) — CHAIN TRUTH, unforgeable by a marker filer.
    /// `None` for bare/legacy rows.
    pub cov_recovery_height: Option<u64>,
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
    /// The height this view SERVES: the covenant-committed value when
    /// decoded and in range (CHAIN TRUTH, unforgeable by a marker filer),
    /// else the caller's marker value verbatim (#323 MEDIUM-3).
    ///
    /// DELIBERATELY NOT `Option` — this field is in the client parser's
    /// STRICT ENUMERATION CORE (`chainReads.ts::parseRecoveryView`), where a
    /// non-number makes the WHOLE view return null, not the row. Serving
    /// `null` would therefore be attacker-triggerable denial of the collapsed
    /// recovery read: a marker filed with `recoveryHeight = 0` on a
    /// bare/legacy pot (no covenant height) resolves to no valid height, and
    /// under oldest-marker-wins that hostile marker survives dedupe — one
    /// dust marker naming a victim would drop them to the slow overlay
    /// enumeration on every app open. Fail-safe (no pot is hidden) but a
    /// permanent degradation of the wiped-device money-discovery path.
    ///
    /// So when NEITHER source is in range we serve the marker value exactly
    /// as before this change: the preference is strictly an improvement, and
    /// the wire contract is byte-identical.
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
pub fn assemble_recovery_view(rows: Vec<RecoveryRow>) -> (Vec<RecoveryEntry>, bool) {
    // #323 — the SQL already dedupes per pot (`rn = 1`), so this belt only
    // catches a future SQL change that drops that window; it is NOT the
    // primary defence and must not be mistaken for one. Dedupe here alone
    // would still let a marker flood evict honest pots inside the LIMIT.
    //
    // TRUNCATION: the window takes MAX_ROWS + 1, so more than MAX_ROWS
    // surviving rows means the page is incomplete. That bit is load-bearing,
    // not cosmetic — it is what makes a flood DETECTABLE rather than a
    // silently-short answer that looks complete.
    // #323 LOW-2 — the Rust key must not be NARROWER than the SQL's, or the
    // belt could collapse two rows the SQL kept and the truncation bit would
    // then say COMPLETE when it is not (the one direction that lies).
    // SQL dedupes on case-sensitive `(potTxid, potVout)`; this adds `gameId`
    // and lowercases. Lowercasing is safe (it can only MERGE rows the SQL
    // treated as distinct, and `pot_records`/`pot_beefs` keys are lowercase
    // by write convention). Adding `gameId` makes the key strictly WIDER, so
    // it can only ever keep more rows, never fewer — and any extra row is
    // counted before `truncated` is computed.
    let mut seen = std::collections::HashSet::new();
    let deduped: Vec<RecoveryRow> = rows
        .into_iter()
        .filter(|r| {
            seen.insert((
                r.game_id.to_ascii_lowercase(),
                r.pot_txid.to_ascii_lowercase(),
                r.pot_vout,
            ))
        })
        .collect();
    let truncated = deduped.len() > RECOVERY_VIEW_MAX_ROWS;
    let entries = deduped
        .into_iter()
        .take(RECOVERY_VIEW_MAX_ROWS)
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
                recovery_height: crate::refund_view::served_recovery_height(
                    r.cov_recovery_height,
                    r.recovery_height,
                )
                .and_then(|h| u32::try_from(h).ok())
                .unwrap_or(r.recovery_height),
                opponent_identity: r.opponent_identity,
                spent: r.spent,
                spending_txid: r.spending_txid,
                spent_confirmed: r.spent_confirmed,
                spender_raw_hex,
            }
        })
        .collect();
    (entries, truncated)
}

/// Assemble the `/recovery-view` wire body:
/// `{"tip":<height|null>,"entries":[{gameId,potTxid,potVout,recoveryHeight,
/// opponentIdentity,spent,spendingTxid,spentConfirmed,spenderRawHex}]}`.
/// `tip` mirrors `/pots-view` (the recovery-height gate needs it) and is
/// `null` on a chaintracks fault — the D1 facts still serve, and the client
/// falls back to its own `/tip`.
pub fn recovery_view_body(entries: &[RecoveryEntry], tip: Option<u64>, truncated: bool) -> String {
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
    // #323 HIGH-1 — `truncated` is what makes a marker FLOOD detectable
    // instead of a silently-short page that looks complete. `potparty_records`
    // is attacker-writable (byte-format admission, no signature), so a caller
    // seeing `truncated: true` must treat the page as INCOMPLETE rather than
    // as "these are all my pots".
    json!({ "tip": tip, "entries": arr, "truncated": truncated }).to_string()
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
//  - (#230) drop chain-CONTRADICTED markers (settle classified tie/refund,
//    or the pot's winner chain-attributed to a DIFFERENT identity);
//  - per gameId, FIRST (#230): a chain-attributed winner whose own anchored
//    marker names it counts +1 (`chainProven` tier — countersigned or not;
//    the #276 tower-enforced winner). Otherwise the claim rules:
//  - a single distinct CONFIRMED (both-sig) winner counts +1;
//    two conflicting confirmed winners (collusion garbage) count for NOBODY;
//    with no confirmed claim, a single distinct winner counts +1 UNCONFIRMED
//    (which never adds to `wins`); conflicting unconfirmed → nobody;
//  - `wins` = countersigned + chain-attributed counted games; `proven` =
//    ≥1 countersigned win (unchanged meaning); `chainProven` = ≥1
//    chain-attributed win (the honest new tier — see the row docs);
//  - `hands` = the lowest-score confirmed + anchored v2 (cards-carrying)
//    hands, one per single-winner game, score ascending then earliest first
//    (deliberately still countersigned-only — an accepted residual).

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
    /// The winner's five revealed cards, canonical 10-hex — the v2 claim byte
    /// the SIGNATURES bind (bsv-low #276). Carried verbatim from the marker;
    /// `None` for a v1 claim, which is a real distinction the client relies on
    /// (`row.cardsHex != null` is what makes it reconstruct a v2 challenge).
    ///
    /// WHY IT MUST SHIP: the client re-verifies every evidence row itself
    /// (`verifyResultRow`) by rebuilding the signed challenge. Without this
    /// field a v2 claim reconstructs as v1, the winner-sig check FAILS,
    /// `tier = invalid`, and `gatherBoardFast` DROPS the row — the identity
    /// renders as a zero-win ghost with an empty drill-down. That was
    /// invisible while every counted row was CONFIRMED and re-fetched, and it
    /// bites exactly the row #276 exists to make count: an UNCONFIRMED
    /// (loser-quit, tower-adjudicated) win, which is served from this
    /// evidence alone.
    pub cards_hex: Option<String>,
    pub anchored: bool,
    /// The `proof_markers` (ls_proof) marker txid for (gameId, winner), when
    /// one is indexed — a POINTER the client fetches + transcript-verifies,
    /// NOT a server assertion the bundle is valid. `None` when absent.
    pub proof_txid: Option<String>,
    /// The server-derived CHAIN classification of this pot's recorded spend
    /// (bsv-low #227): which mandated covenant template the settle paid.
    /// `None` = not classified (legacy bare pot, missing bytes, or ambiguous
    /// — the classifier never guesses). See `results.rs` for the trust model:
    /// a covenant spend is co-signed by construction and can only pay a
    /// mandated shape, so this is chain truth, not a claim.
    pub server_verdict: Option<crate::results::PotVerdict>,
    /// #230: the identity the server ATTRIBUTED as this pot's winner via a
    /// verified `LOW/potparty/v2` seat-binding marker joined to the chain
    /// verdict (the winning seat's committed settle key, proven held by this
    /// identity). `None` when unattributed. The client can falsify it:
    /// `ls_potparty byPot` serves the v2 marker, `/beef` the committed lock,
    /// and `serverVerdict` names the winning template.
    pub chain_attributed_winner: Option<String>,
}

/// One `board[i]` row — an identity's wins + its evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardBoardRow {
    pub identity: String,
    /// Counted wins, deduped per game: countersigned (doubly-signed +
    /// anchored) wins PLUS #230 chain-attributed wins (anchored winner
    /// verdict + verified seat-binding marker naming this identity).
    pub wins: u32,
    /// True iff the identity has ≥1 COUNTERSIGNED (doubly-signed, anchored)
    /// win — the original contract's proven rule, deliberately NOT widened
    /// (#230): a countersignature is the loser's own attestation, a distinct
    /// trust fact worth surfacing separately.
    pub proven: bool,
    /// True iff the identity has ≥1 CHAIN-ATTRIBUTED win (#230): the pot's
    /// covenant verdict named a winning seat and a verified v2 seat-binding
    /// marker proved this identity held that seat's committed settle key.
    /// The honest new tier for a tower-enforced win whose loser never
    /// countersigned — chain truth, not a claim, but a different fact than
    /// `proven` (hence a separate flag rather than overloading it).
    pub chain_proven: bool,
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

/// Is this outpoint status a CONFIRMED landing — the one basis on which a
/// verdict may be derived (#323)?
///
/// Extracted from `routes::classify_spent_pots` (MEDIUM-5) because that file
/// is the one this repo flags as silently deletable: the 2026-07-28 re-gate
/// showed a whole delivery there could be removed with the suite still
/// green. A money-relevant predicate must live where the other pure rules
/// live, and be pinned there.
///
/// `spent = 1` ALONE is not enough: the overlay's unconfirmed write path
/// stamps `verdict`/`verdictTxid` while leaving `spentConfirmed = 0`, so a
/// PARKED spender would otherwise mint a real verdict — which can both
/// create a chain-attributed win and erase an honest claim.
pub fn is_confirmed_landing(status: &OutpointStatus) -> bool {
    status.spent == Some(true) && status.spent_confirmed == Some(true)
}

/// Is a recorded spend a CONFIRMED LANDING for the two per-identity MONEY
/// views (`/results` and `/refund-view`)?
///
/// The bar: the `spentConfirmed` flag OR a chaintracks-VERIFIED spender proof
/// (`pot_beefs.proof_verified`). The second signal exists because the column
/// was added by migration with default 0, so a pre-existing row whose spend
/// genuinely MINED can carry `spentConfirmed = 0`; a parked tx that never
/// mined can never acquire a verified proof, so this widens toward CHAIN
/// TRUTH, never away from it.
///
/// # Why this is hoisted (#323, the fourth instance of one pattern)
///
/// This rule previously existed as TWO INLINE COPIES — one in
/// `results::assemble_results`, one in `refund_view::derive_refund_status` —
/// and a cell named for the two views "agreeing" called only ONE of them, so
/// breaking the other side left the agreement cell green. **An executable
/// claim is only as strong as the surface it executes against**, and that is
/// nastier than a false comment because a green cell with the right name
/// reads as stronger evidence than prose. The durable fix is not a second
/// test: it is DELETING one of the copies so agreement is structural and a
/// single pin covers both.
///
/// Note the sibling [`is_confirmed_landing`] is deliberately STRICTER (flag
/// only): it serves the leaderboard/classifier pair, which has no spender
/// BEEF join to read a proof latch from.
pub fn is_confirmed_landing_with_proof(
    spent_confirmed: Option<bool>,
    spender_proof_verified: Option<bool>,
) -> bool {
    spent_confirmed == Some(true) || spender_proof_verified == Some(true)
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
            // #323 HIGH-3 — the spend must be CONFIRMED. `anchored` is the
            // leaderboard's COUNTING gate, so an unconfirmed pointer here
            // publishes a `chainProven` win on the public board from a
            // displaceable intent. The concrete shape: a coop settle that
            // never mined, displaced by the tower-enforced settle that paid
            // the OPPONENT, would still count as a win for the wrong player.
            // Same bar as `refund_view::derive_refund_status` and
            // `assemble_results` (#323 defect 1).
            is_confirmed_landing(st)
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
    aggregate_leaderboard_with_verdicts(
        markers,
        statuses,
        proof_by_game_winner,
        hands_limit,
        &std::collections::HashMap::new(),
    )
}

/// [`aggregate_leaderboard_with_verdicts`] without seat attributions (#230)
/// — the pre-#230 behaviour, kept for callers/tests without the join.
pub fn aggregate_leaderboard_with_verdicts(
    markers: &[ResultMarkerRow],
    statuses: &[OutpointStatus],
    proof_by_game_winner: &std::collections::HashMap<(String, String), String>,
    hands_limit: usize,
    verdict_by_pot: &std::collections::HashMap<String, crate::results::PotVerdict>,
) -> Leaderboard {
    aggregate_leaderboard_attributed(
        markers,
        statuses,
        proof_by_game_winner,
        hands_limit,
        verdict_by_pot,
        &std::collections::HashMap::new(),
    )
}

/// [`aggregate_leaderboard`] plus the server-derived CHAIN classifications
/// (bsv-low #227) and the #230 SEAT ATTRIBUTIONS: `verdict_by_pot` maps a
/// lowercase pot txid to the classified template its recorded spend paid
/// (see `results.rs`); `attr_by_pot` maps a lowercase pot txid to the
/// verified `LOW/potparty/v2` seat → identity attribution for that pot.
///
/// The fold is ADDITIVE truth, applied conservatively:
/// - a marker whose anchored settle is chain-classified as a **refund** or a
///   **tie** is EXCLUDED from win/hand counting — the chain says nobody won
///   that pot, so a claim naming it as a win is contradicted (the server's
///   presence-only sig check could otherwise be gamed by a fabricated marker
///   pointing at a real refund txid);
/// - (#230) when a winner verdict's winning-seat settle key is PROVEN held
///   by an identity (verified v2 marker), that identity is the pot's
///   CHAIN-ATTRIBUTED winner: an anchored marker by that identity counts a
///   WIN even without a countersignature (`chainProven` tier — the #276
///   tower-enforced winner), and an anchored claim naming a DIFFERENT
///   winner is chain-contradicted and counts for nobody. The attribution
///   alone never mints a board row — a win still requires (and is
///   reconstructible from) the winner's own anchored marker in `evidence`;
/// - a winner-template classification WITHOUT an attribution (or no
///   classification at all) leaves counting EXACTLY as before — backward
///   compatible; client claims keep working for legacy/pre-covenant games;
/// - every marker still appears in `evidence` (with `serverVerdict` +
///   `chainAttributedWinner`) so the client can re-verify and falsify.
pub fn aggregate_leaderboard_attributed(
    markers: &[ResultMarkerRow],
    statuses: &[OutpointStatus],
    proof_by_game_winner: &std::collections::HashMap<(String, String), String>,
    hands_limit: usize,
    verdict_by_pot: &std::collections::HashMap<String, crate::results::PotVerdict>,
    attr_by_pot: &std::collections::HashMap<String, crate::results::SeatAttribution>,
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
    // The chain classification of marker i's pot spend, when one exists.
    let verdict_of = |i: usize| {
        verdict_by_pot
            .get(&markers[i].pot_txid.to_ascii_lowercase())
            .copied()
    };
    // #230: the CHAIN-ATTRIBUTED winner of marker i's pot — the identity
    // that provably held the winning seat's committed settle key, when the
    // verdict names a winning seat and a verified v2 marker attributes it.
    let attributed_winner_of = |i: usize| -> Option<String> {
        let pot = markers[i].pot_txid.to_ascii_lowercase();
        let v = verdict_by_pot.get(&pot).copied()?;
        attr_by_pot
            .get(&pot)
            .and_then(|a| a.winner_for(v))
            .map(str::to_string)
    };
    // Chain-contradicted: the settle this marker claims as a WIN is
    // classified as a tie or refund — nobody won that pot; OR (#230) the
    // pot's winner is chain-attributed to a DIFFERENT identity than the
    // marker claims. Such a marker never counts (wins OR hands); it stays
    // in evidence with its verdict + attribution.
    let chain_contradicted = |i: usize| {
        if matches!(
            verdict_of(i),
            Some(crate::results::PotVerdict::Tie) | Some(crate::results::PotVerdict::Refund)
        ) {
            return true;
        }
        matches!(attributed_winner_of(i), Some(w) if !w.eq_ignore_ascii_case(&markers[i].winner))
    };

    // ── wins: per-game dedup over ANCHORED markers (client aggregateBoard) ──
    let mut by_game: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, m) in markers.iter().enumerate() {
        if anchored[i] && !chain_contradicted(i) {
            by_game
                .entry(m.game_id.to_ascii_lowercase())
                .or_default()
                .push(i);
        }
    }
    // identity_lc → per-tier tallies (a game counts into `wins` at most once).
    #[derive(Default)]
    struct Tally {
        /// Counted wins (countersigned OR chain-attributed), per-game deduped.
        wins: u32,
        /// Unconfirmed single-winner games (never added to `wins`).
        unconf: u32,
        /// Games whose counted win carries the loser's countersignature.
        countersigned: u32,
        /// Games whose counted win is #230 chain-attributed.
        chain: u32,
    }
    let mut tally: HashMap<String, Tally> = HashMap::new();
    for idxs in by_game.values() {
        // ── #230 first: a CHAIN-ATTRIBUTED winner that also claimed the pot
        // (its own anchored marker names it — chain_contradicted already
        // dropped every claim the attribution disagrees with) counts a WIN
        // outright, countersigned or not. This is the #276 tower-enforced
        // winner: verdict winner-X + proven holder of seat X's committed key.
        let chain_winners: HashSet<String> = idxs
            .iter()
            .filter(|&&i| {
                matches!(attributed_winner_of(i),
                    Some(w) if w.eq_ignore_ascii_case(&markers[i].winner))
            })
            .map(|&i| markers[i].winner.to_ascii_lowercase())
            .collect();
        if chain_winners.len() == 1 {
            let w = chain_winners.into_iter().next().unwrap();
            let countersigned = idxs
                .iter()
                .any(|&i| confirmed(i) && markers[i].winner.eq_ignore_ascii_case(&w));
            let e = tally.entry(w).or_default();
            e.wins += 1;
            e.chain += 1;
            if countersigned {
                e.countersigned += 1;
            }
            continue;
        }
        // (>1 chain winner is only reachable with garbage markers over
        //  DIFFERENT pots inside one gameId — fall through to the
        //  conservative claim rules rather than guess.)

        let confirmed_winners: HashSet<String> = idxs
            .iter()
            .filter(|&&i| confirmed(i))
            .map(|&i| markers[i].winner.to_ascii_lowercase())
            .collect();
        if confirmed_winners.len() == 1 {
            let w = confirmed_winners.into_iter().next().unwrap();
            let e = tally.entry(w).or_default();
            e.wins += 1;
            e.countersigned += 1;
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
            tally.entry(w).or_default().unconf += 1;
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
        .map(|(id, t)| {
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
                        cards_hex: m.cards_hex.as_ref().map(|s| s.to_ascii_lowercase()),
                        anchored: anchored[i],
                        proof_txid,
                        server_verdict: verdict_of(i),
                        chain_attributed_winner: attributed_winner_of(i),
                    }
                })
                .collect();
            (
                LeaderboardBoardRow {
                    identity: id.clone(),
                    wins: t.wins,
                    proven: t.countersigned > 0,
                    chain_proven: t.chain > 0,
                    evidence,
                },
                t.unconf,
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
        if !anchored[i] || !confirmed(i) || chain_contradicted(i) {
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
                        // bsv-low #276: the v2 cards the sigs bind. ALWAYS
                        // emitted (null for a v1 claim) — the client treats an
                        // ABSENT field as v1, so omitting it on a v2 row is a
                        // silent downgrade to `tier = invalid`.
                        "cardsHex": e.cards_hex,
                        "anchored": e.anchored,
                        "proofTxid": e.proof_txid,
                        "serverVerdict": e.server_verdict.map(crate::results::PotVerdict::as_str),
                        // bsv-low #230: the identity attributed as this pot's
                        // winner via the verified seat-binding marker + chain
                        // verdict (null when unattributed) — the falsifiable
                        // fact behind the row's `chainProven` tier.
                        "chainAttributedWinner": e.chain_attributed_winner,
                    })
                })
                .collect();
            json!({
                "identity": r.identity,
                "wins": r.wins,
                "proven": r.proven,
                // bsv-low #230: ≥1 counted win is CHAIN-ATTRIBUTED (covenant
                // verdict + seat-binding proof, no countersignature). A
                // deliberate separate tier — `proven` keeps meaning "the
                // loser countersigned".
                "chainProven": r.chain_proven,
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
            Outpoint {
                txid: txid_b(),
                vout: 1,
            }, // spent row
            Outpoint {
                txid: txid_a(),
                vout: 0,
            }, // no row → unknown
            Outpoint {
                txid: txid_a(),
                vout: 2,
            }, // unspent row
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
        assert_eq!(
            out[0].spending_txid.as_deref(),
            Some("f0".repeat(32).as_str())
        );
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
        let ops = vec![Outpoint {
            txid: upper.clone(),
            vout: 0,
        }];
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
        assert_eq!(
            three.matches("(p.txid = ? AND p.outputIndex = ?)").count(),
            3
        );
        assert_eq!(three.matches(" OR ").count(), 2);
    }

    #[test]
    fn extract_raw_tx_hex_roundtrip_and_misses() {
        let (beef_bytes, raw_hex, txid) = beef_fixture();
        // The carried tx extracts to its exact raw bytes (either txid case).
        assert_eq!(
            extract_raw_tx_hex(&beef_bytes, &txid),
            Some(raw_hex.clone())
        );
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
            Outpoint {
                txid: txid_a(),
                vout: 0,
            }, // spent, beef joined
            Outpoint {
                txid: txid_a(),
                vout: 1,
            }, // spent, beef row MISSING
            Outpoint {
                txid: txid_b(),
                vout: 0,
            }, // unknown outpoint
            Outpoint {
                txid: txid_b(),
                vout: 2,
            }, // known-unspent
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
        assert_eq!(
            out[0].status.spending_txid.as_deref(),
            Some(spender.as_str())
        );
        assert_eq!(out[0].spender_raw_hex.as_deref(), Some(raw_hex.as_str()));
        // Spender recorded but no stored BEEF → pointer yes, raw null.
        assert_eq!(
            out[1].status.spending_txid.as_deref(),
            Some(spender.as_str())
        );
        assert_eq!(out[1].spender_raw_hex, None);
        // Unknown: fail-safe nulls (never asserted unspent).
        assert_eq!((out[2].status.known, out[2].status.spent), (false, None));
        assert_eq!(out[2].spender_raw_hex, None);
        // Known-unspent: no spender, no raw.
        assert_eq!(
            (out[3].status.known, out[3].status.spent),
            (true, Some(false))
        );
        assert_eq!(out[3].spender_raw_hex, None);
    }

    #[test]
    fn assemble_pots_view_degrades_on_corrupt_beef() {
        let ops = vec![Outpoint {
            txid: txid_a(),
            vout: 0,
        }];
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
        let op = Outpoint {
            txid: txid_a(),
            vout: 0,
        };
        let entries = vec![
            PotsViewEntry {
                status: OutpointStatus::known(&op, true, Some("f0".repeat(32)), true),
                spender_raw_hex: Some("aabb".to_string()),
            },
            PotsViewEntry {
                status: OutpointStatus::unknown(&Outpoint {
                    txid: txid_b(),
                    vout: 1,
                }),
                spender_raw_hex: None,
            },
        ];
        let v: serde_json::Value =
            serde_json::from_str(&pots_view_body(&entries, Some(958_123))).unwrap();
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

    /// #323 MEDIUM-3 — the SERVED recoveryHeight prefers the COVENANT-
    /// COMMITTED value over the marker's unverified one.
    ///
    /// This matters more under oldest-marker-wins: an attacker who files a
    /// marker BEFORE the victim's own becomes the sole surviving source of
    /// this field, and `/recovery-view` serves no `sigHex` for the client to
    /// collapse candidates itself. The covenant value is chain truth and a
    /// marker filer cannot forge it.
    #[test]
    fn the_served_recovery_height_prefers_covenant_truth() {
        let row = |cov: Option<u64>, marker: u32| RecoveryRow {
            game_id: "11".repeat(32),
            pot_txid: txid_a(),
            pot_vout: 0,
            recovery_height: marker,
            cov_recovery_height: cov,
            opponent_identity: format!("03{}", "bb".repeat(32)),
            spent: None,
            spending_txid: None,
            spent_confirmed: None,
            spender_beef_hex: None,
        };
        // Covenant truth wins over a hostile marker's value.
        let (out, _) = assemble_recovery_view(vec![row(Some(958_504), 1)]);
        assert_eq!(out[0].recovery_height, 958_504);
        // No covenant value (bare/legacy) ⇒ the marker hint is served.
        let (out, _) = assemble_recovery_view(vec![row(None, 958_600)]);
        assert_eq!(out[0].recovery_height, 958_600);
        // NEITHER source in range ⇒ the marker value VERBATIM, never null.
        let (out, _) = assemble_recovery_view(vec![row(None, 0)]);
        assert_eq!(out[0].recovery_height, 0);

        // #323 — the WIRE must always carry a NUMBER here. This field is in
        // the client's STRICT enumeration core (`parseRecoveryView`), where a
        // non-number returns null for the WHOLE view, not the row. Serving
        // null would be attacker-triggerable denial of the collapsed recovery
        // read: a marker filed with recoveryHeight 0 against a bare/legacy pot
        // resolves to no valid height, and under oldest-marker-wins that
        // hostile marker survives dedupe.
        let (out, _) = assemble_recovery_view(vec![row(None, 0)]);
        let body: serde_json::Value =
            serde_json::from_str(&recovery_view_body(&out, Some(958_800), false)).unwrap();
        assert!(
            body["entries"][0]["recoveryHeight"].is_number(),
            "recoveryHeight must be a NUMBER on the wire: {body}"
        );
    }

    /// The SQL must FETCH the covenant height, else the preference above is
    /// inoperative in production (producer-level check).
    #[test]
    fn recovery_view_sql_fetches_the_covenant_height() {
        let sql = recovery_view_sql();
        assert!(
            sql.contains("r.recoveryHeight AS covRecoveryHeight"),
            "covenant height must be SELECTed from pot_records: {sql}"
        );
        assert!(sql.contains("w.covRecoveryHeight AS covRecoveryHeight"));
    }

    /// #323 MEDIUM-5 — the confirmed-landing predicate, pinned where it
    /// lives rather than inside `routes.rs`.
    /// #323 — the ONE bar both money views use. Because `/results` and
    /// `/refund-view` now CALL this rather than each carrying a copy,
    /// agreement is structural and this single pin covers both. The previous
    /// round tested the two sides separately under a cell NAMED for their
    /// agreement, and breaking one side left that cell green.
    #[test]
    fn is_confirmed_landing_with_proof_is_the_one_money_view_bar() {
        // The flag alone.
        assert!(is_confirmed_landing_with_proof(Some(true), None));
        // A chaintracks-VERIFIED spender proof alone (the migrated-row case).
        assert!(is_confirmed_landing_with_proof(Some(false), Some(true)));
        assert!(is_confirmed_landing_with_proof(None, Some(true)));
        // A PARKED intent: neither signal.
        assert!(!is_confirmed_landing_with_proof(Some(false), None));
        assert!(!is_confirmed_landing_with_proof(None, None));
        // An UNVERIFIED latch is not a signal (never a guess).
        assert!(!is_confirmed_landing_with_proof(Some(false), Some(false)));
        // And it is strictly WIDER than the leaderboard's flag-only rule,
        // never narrower — the two must not be confused.
        let op = Outpoint {
            txid: txid_a(),
            vout: 0,
        };
        let mut st = OutpointStatus::known(&op, true, Some("ab".repeat(32)), false);
        st.spent_confirmed = Some(false);
        assert!(!is_confirmed_landing(&st));
        assert!(is_confirmed_landing_with_proof(
            st.spent_confirmed,
            Some(true)
        ));
    }

    /// Both money views must CALL the shared bar — not re-implement it. This
    /// is the structural half: a future inline copy re-opens the divergence
    /// the hoist closed.
    #[test]
    fn both_money_views_call_the_shared_bar() {
        let results_src = include_str!("results.rs");
        let refund_src = include_str!("refund_view.rs");
        let needle = ["is_confirmed_landing", "_with_proof("].concat();
        assert_eq!(
            results_src.matches(needle.as_str()).count(),
            1,
            "results.rs must call the shared bar exactly once"
        );
        assert_eq!(
            refund_src.matches(needle.as_str()).count(),
            1,
            "refund_view.rs must call the shared bar exactly once"
        );
        // And neither may carry an inline re-implementation. The needle is
        // the DISJUNCTION that IS the bar (`… || … proof_verified == Some(true)`),
        // not any mention of the field — `results.rs` legitimately passes the
        // same latch to `verified_beef_block_height` for the at.height
        // fallback, and a broader needle matched that unrelated call. Scoping
        // an assertion wider than the construct is its own failure mode.
        let disjunction = ["|| ", "spender_proof_verified == Some(true)"].concat();
        for (name, src) in [("results.rs", results_src), ("refund_view.rs", refund_src)] {
            let flat = src.split_whitespace().collect::<Vec<_>>().join(" ");
            assert_eq!(
                flat.matches(disjunction.as_str()).count(),
                0,
                "{name} must not re-implement the bar inline"
            );
            // `r.`-qualified spelling of the same disjunction.
            let qualified = ["|| ", "r.spender_proof_verified == Some(true)"].concat();
            assert_eq!(
                flat.matches(qualified.as_str()).count(),
                0,
                "{name} must not re-implement the bar inline (qualified form)"
            );
        }
    }

    #[test]
    fn is_confirmed_landing_requires_both_flags() {
        let op = Outpoint {
            txid: txid_a(),
            vout: 0,
        };
        let mk = |spent, confirmed| {
            let mut s = OutpointStatus::known(&op, spent, Some("ab".repeat(32)), confirmed);
            s.spent = Some(spent);
            s.spent_confirmed = Some(confirmed);
            s
        };
        assert!(is_confirmed_landing(&mk(true, true)));
        assert!(!is_confirmed_landing(&mk(true, false)), "parked intent");
        assert!(!is_confirmed_landing(&mk(false, true)));
        // Unknown (no pot_records row) is never a landing.
        assert!(!is_confirmed_landing(&OutpointStatus::unknown(&op)));
    }

    #[test]
    fn recovery_view_sql_shape() {
        let sql = recovery_view_sql();
        // JOINs the pot outpoint for spend status; the BEEF join now sits
        // OUTSIDE the window, on survivors only, so a marker flood can never
        // drag real BLOBs along with it (#323 HIGH-1).
        assert!(sql.contains(
            "LEFT JOIN pot_records r \
                  ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout"
        ));
        assert!(sql.contains("LEFT JOIN pot_beefs b ON b.txid = lower(w.spendingTxid)"));
        assert!(sql.contains("hex(b.beef) AS spenderBeef"));
        // Keyed by ONE identity.
        assert!(sql.contains("WHERE pp.identity = ?"));
        // Exactly one bind placeholder (single-identity query, not batched).
        assert_eq!(sql.matches('?').count(), 1);

        // #323 HIGH-1 — the three anti-flood properties, asserted
        // individually so losing any ONE of them fails loudly. A marker
        // flood on an attacker-writable identity must not be able to evict
        // the caller's real pots.
        //
        // 1. dedupe in SQL, OLDEST marker per pot wins (anti-squat: a later
        //    marker cannot displace the original, and one pot takes one slot).
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout \
                                     ORDER BY pp.createdAt ASC, pp.rowid ASC) AS rn"
            ),
            "per-pot dedupe window missing: {sql}"
        );
        assert!(sql.contains("WHERE rn = 1"), "dedupe filter missing: {sql}");
        // 2. rank by the POT's own admission stamp — an attacker cannot
        //    backdate or advance `pot_records.createdAt` by filing markers.
        assert!(
            sql.contains("ORDER BY COALESCE(potCreatedAt, markerCreatedAt) DESC"),
            "pot-stamp ranking missing: {sql}"
        );
        // 3. a reserved quota for unknown pots, so ghost rows occupy a
        //    bounded slice instead of the whole page.
        assert!(
            sql.contains(&format!(
                "CASE WHEN unknownPot = 0 OR potRank <= {RECOVERY_VIEW_UNKNOWN_QUOTA} THEN 0 ELSE 1 END AS tier"
            )),
            "unknown-pot quota tier missing or not tied to the const: {sql}"
        );
        // The window takes MAX_ROWS + 1 so truncation is DETECTABLE.
        //
        // #323 MEDIUM-2 — PARSE the integer and compare NUMERICALLY. The
        // previous version called itself "an exact equality against the
        // const" while remaining a `contains`, which is a PREFIX match:
        // `LIMIT 1010` contains `LIMIT 101`, so a 10x window shipped green
        // under an assertion whose own comment warned against exactly that.
        // The `count() == 1` guard closed a different hole and did not help.
        let limits: Vec<u64> = sql
            .match_indices("LIMIT ")
            .map(|(i, _)| {
                sql[i + "LIMIT ".len()..]
                    .chars()
                    .take_while(char::is_ascii_digit)
                    .collect::<String>()
                    .parse::<u64>()
                    .expect("LIMIT must be followed by an integer")
            })
            .collect();
        assert_eq!(limits.len(), 1, "exactly one LIMIT: {sql}");
        assert_eq!(
            limits[0],
            (RECOVERY_VIEW_MAX_ROWS + 1) as u64,
            "probe LIMIT must be RECOVERY_VIEW_MAX_ROWS + 1: {sql}"
        );
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
                cov_recovery_height: None,
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
                cov_recovery_height: None,
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
                cov_recovery_height: None,
                opponent_identity: format!("03{}", "dd".repeat(32)),
                spent: None,
                spending_txid: None,
                spent_confirmed: None,
                spender_beef_hex: None,
            },
        ];
        let (out, _truncated) = assemble_recovery_view(rows);
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

    /// #323 defect 3 — `/recovery-view` must DEDUPE by pot and must be
    /// BOUNDED. `potparty_records` is keyed on the MARKER outpoint
    /// (`PRIMARY KEY (txid, outputIndex)`), so one identity legitimately
    /// holds many marker rows for one pot (each republish, and any third
    /// party's marker, is its own row) — the prod audit saw 18 rows for 12
    /// games. `assemble_results` already dedupes on exactly this key; this
    /// is the same rule for the one identity-scoped view that lacked it.
    #[test]
    fn recovery_view_dedupes_by_pot_and_is_bounded() {
        let dup = |g: u8| RecoveryRow {
            game_id: format!("{:02x}", g).repeat(32),
            pot_txid: txid_a(),
            pot_vout: 0,
            recovery_height: 958_504,
            cov_recovery_height: None,
            opponent_identity: format!("03{}", "bb".repeat(32)),
            spent: None,
            spending_txid: None,
            spent_confirmed: None,
            spender_beef_hex: None,
        };
        // Three marker rows for ONE (game, pot, vout) — the republish shape.
        let rows = vec![dup(0x11), dup(0x11), dup(0x11)];
        let (entries, _truncated) = assemble_recovery_view(rows);
        assert_eq!(entries.len(), 1, "duplicate marker rows must collapse");

        // Distinct pots are NOT collapsed (dedupe must not eat real rows).
        let (entries, _) = assemble_recovery_view(vec![dup(0x11), dup(0x22)]);
        assert_eq!(entries.len(), 2, "distinct games must survive dedupe");

        // A flood of DISTINCT ghost pots cannot silently shorten the page:
        // past the cap the truncation bit fires, so an incomplete answer is
        // never served as a complete one (#323 HIGH-1).
        let flood: Vec<RecoveryRow> = (0..(RECOVERY_VIEW_MAX_ROWS + 5))
            .map(|i| {
                let mut r = dup(0x11);
                r.game_id = format!("{i:064x}");
                r.pot_txid = format!("{:064x}", 0x1000 + i);
                r
            })
            .collect();
        let (entries, truncated) = assemble_recovery_view(flood);
        assert_eq!(entries.len(), RECOVERY_VIEW_MAX_ROWS, "page is capped");
        assert!(truncated, "a page past the cap MUST report truncated");
        // Exactly at the cap is NOT truncated (no false alarm).
        let exact: Vec<RecoveryRow> = (0..RECOVERY_VIEW_MAX_ROWS)
            .map(|i| {
                let mut r = dup(0x11);
                r.game_id = format!("{i:064x}");
                r.pot_txid = format!("{:064x}", 0x2000 + i);
                r
            })
            .collect();
        let (entries, truncated) = assemble_recovery_view(exact);
        assert_eq!(entries.len(), RECOVERY_VIEW_MAX_ROWS);
        assert!(
            !truncated,
            "a full-but-complete page must not claim truncation"
        );

        // The SQL is bounded — an identity-scoped view with no window is an
        // unbounded D1 read on every app open (#255 amplifier class).
        // (SQL shape + the LIMIT/quota pins live in `recovery_view_sql_shape`.)
    }

    #[test]
    fn assemble_recovery_view_degrades_on_corrupt_beef() {
        let rows = vec![RecoveryRow {
            game_id: "11".repeat(32),
            pot_txid: txid_a(),
            pot_vout: 0,
            recovery_height: 958_504,
            cov_recovery_height: None,
            opponent_identity: format!("03{}", "bb".repeat(32)),
            spent: Some(true),
            spending_txid: Some("f0".repeat(32)),
            spent_confirmed: Some(true),
            spender_beef_hex: Some("not-hex!!".to_string()),
        }];
        let (out, _truncated) = assemble_recovery_view(rows);
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
            serde_json::from_str(&recovery_view_body(&entries, Some(958_800), false)).unwrap();
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
            serde_json::from_str(&recovery_view_body(&entries, None, false)).unwrap();
        assert!(v2["tip"].is_null());
        assert_eq!(v2["entries"].as_array().unwrap().len(), 2);
        // An empty result (invalid/empty identity) is a well-formed body.
        let v3: serde_json::Value =
            serde_json::from_str(&recovery_view_body(&[], None, false)).unwrap();
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
    /// Like [`statuses_for`] but with the confirmation flag EXPLICIT.
    /// `statuses_for` hardcodes `spent_confirmed: true`, so before #323 no
    /// leaderboard cell could express a PARKED spend at all — which is why
    /// the counting gate shipped without one.
    fn statuses_for_confirmed(
        markers: &[ResultMarkerRow],
        spent_by: &HashMap<u8, u8>,
        confirmed: bool,
    ) -> Vec<OutpointStatus> {
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
                        spent_confirmed: confirmed,
                    });
                }
            }
        }
        assemble_statuses(&ops, &rows)
    }

    /// #323 HIGH-3 — an UNCONFIRMED (parked) settle must never count on the
    /// public leaderboard. `marker_anchored` is the counting gate, so a coop
    /// settle that never mined — displaced by the tower-enforced settle that
    /// paid the OPPONENT — would otherwise publish a `chainProven` win for
    /// the wrong player.
    #[test]
    fn an_unconfirmed_settle_never_counts_on_the_leaderboard() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0)];
        let spent = HashMap::from([(1u8, 2u8)]);

        // CONTROL: confirmed ⇒ it counts (so the test discriminates).
        let lb = aggregate_leaderboard(
            &markers,
            &statuses_for_confirmed(&markers, &spent, true),
            &no_proofs(),
            200,
        );
        assert_eq!(lb.hands.len(), 1, "a CONFIRMED settle counts");
        assert!(lb.hands[0].anchored);
        assert!(
            lb.board.iter().any(|r| r.wins > 0),
            "the confirmed win is on the board"
        );

        // THE DEFECT: same rows, spend recorded but NOT confirmed.
        let lb = aggregate_leaderboard(
            &markers,
            &statuses_for_confirmed(&markers, &spent, false),
            &no_proofs(),
            200,
        );
        assert!(
            lb.hands.is_empty(),
            "a PARKED settle must not publish a hand"
        );
        assert!(
            lb.board.iter().all(|r| r.wins == 0),
            "a PARKED settle must not publish a win"
        );
    }

    fn statuses_for(
        markers: &[ResultMarkerRow],
        spent_by: &HashMap<u8, u8>,
    ) -> Vec<OutpointStatus> {
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
        assert_eq!(
            leaderboard_cards_from_hex("000102030c"),
            Some([0, 1, 2, 3, 12])
        );
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
        assert_eq!(
            lb.board[0].wins, 0,
            "a singly-signed marker never adds a win"
        );
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
        assert_eq!(
            lb.hands[0].game_id,
            tx(1),
            "earlier claim wins the score tie"
        );
        assert_eq!(lb.hands[1].game_id, tx(2));
    }

    #[test]
    fn proof_pointer_is_carried_but_never_gates_a_count() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let proofs = HashMap::from([((tx(1), a.clone()), "proof-txid".to_string())]);
        let lb = aggregate_leaderboard(&markers, &statuses, &proofs, 200);
        assert_eq!(
            lb.board[0].evidence[0].proof_txid.as_deref(),
            Some("proof-txid")
        );
        // Absent proof → null, count unchanged.
        let lb2 = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(lb2.board[0].evidence[0].proof_txid, None);
        assert_eq!(lb2.board[0].wins, 1);
    }

    // ── the #227 chain-classification fold ─────────────────────────────

    /// A claim whose anchored settle is chain-classified as a REFUND or TIE
    /// never counts (wins OR hands) — the chain says nobody won that pot —
    /// while a winner classification (or no classification) leaves counting
    /// exactly as before. The evidence row carries the verdict either way.
    #[test]
    fn chain_refund_or_tie_classification_defeats_a_claimed_win() {
        use crate::results::PotVerdict;
        let a = ident(0xaa);
        let b = ident(0xbb);
        // A doubly-signed, ANCHORED v2 marker — counts 1 win + 1 hand today.
        let markers = vec![mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));

        // No classification → unchanged counting (backward compatible).
        let lb = aggregate_leaderboard_with_verdicts(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &HashMap::new(),
        );
        assert_eq!(lb.board[0].wins, 1);
        assert_eq!(lb.hands.len(), 1);
        assert_eq!(lb.board[0].evidence[0].server_verdict, None);

        // Winner classification → still counts, verdict carried.
        let verdicts = HashMap::from([(tx(1), PotVerdict::WinnerA)]);
        let lb =
            aggregate_leaderboard_with_verdicts(&markers, &statuses, &no_proofs(), 200, &verdicts);
        assert_eq!(lb.board[0].wins, 1);
        assert_eq!(
            lb.board[0].evidence[0].server_verdict,
            Some(PotVerdict::WinnerA)
        );

        // REFUND classification → the claimed win is chain-contradicted:
        // no wins, no hands, but the evidence (with verdict) survives for
        // the client to falsify.
        let verdicts = HashMap::from([(tx(1), PotVerdict::Refund)]);
        let lb =
            aggregate_leaderboard_with_verdicts(&markers, &statuses, &no_proofs(), 200, &verdicts);
        assert!(lb.board.is_empty(), "a refund is never a win");
        assert!(lb.hands.is_empty());

        // TIE classification → same exclusion.
        let verdicts = HashMap::from([(tx(1), PotVerdict::Tie)]);
        let lb =
            aggregate_leaderboard_with_verdicts(&markers, &statuses, &no_proofs(), 200, &verdicts);
        assert!(lb.board.is_empty(), "a tie is never a win");
        assert!(lb.hands.is_empty());
    }

    /// The wire body carries `serverVerdict` per evidence row (null when
    /// unclassified) so clients can re-verify the chain classification.
    #[test]
    fn leaderboard_body_carries_server_verdict() {
        use crate::results::PotVerdict;
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let verdicts = HashMap::from([(tx(1), PotVerdict::WinnerA)]);
        let lb =
            aggregate_leaderboard_with_verdicts(&markers, &statuses, &no_proofs(), 200, &verdicts);
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1)).unwrap();
        assert_eq!(v["board"][0]["evidence"][0]["serverVerdict"], "winner-a");
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
        assert!(
            anchored_games.contains(&tx(46)),
            "2nd-chunk even pot anchored"
        );
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
        assert_eq!(ev[0]["cardsHex"], "000102030c");
        let hands = v["hands"].as_array().unwrap();
        assert_eq!(hands.len(), 1);
        assert_eq!(hands[0]["gameId"], tx(1));
        assert_eq!(hands[0]["score"], 15);
        assert_eq!(hands[0]["cardsHex"], "000102030c");
        assert_eq!(hands[0]["winner"], a);
        assert_eq!(hands[0]["anchored"], true);
    }

    /// bsv-low #276 — an UNCONFIRMED (loser-quit / tower-adjudicated) v2 claim
    /// MUST carry `cardsHex` in its evidence.
    ///
    /// This is the row a tower-enforced winner gets: the loser is gone, so no
    /// countersignature exists and the client can never "upgrade" it by
    /// re-fetching a confirmed twin — this evidence IS the row. The client
    /// re-verifies it by rebuilding the exact challenge the winner signed, and
    /// it treats an ABSENT `cardsHex` as a v1 claim; a v2 claim missing the
    /// field therefore fails its own winner-sig check, is scored `invalid`,
    /// and is DROPPED, rendering an honest winner as a zero-win ghost with an
    /// empty drill-down. `null` (a genuine v1 claim) must stay distinguishable
    /// from absent, so the field is always emitted.
    #[test]
    fn unconfirmed_v2_evidence_carries_cards_hex() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        // confirmed=false ⇒ no loserSigHex: exactly the tower-enforced shape.
        let markers = vec![mk(1, &a, &b, 1, 2, false, Some("000102030c"), 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        assert_eq!(
            lb.board[0].evidence[0].cards_hex.as_deref(),
            Some("000102030c"),
            "the v2 cards the winner's signature binds must survive aggregation"
        );
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1)).unwrap();
        let ev = &v["board"][0]["evidence"][0];
        assert_eq!(
            ev["loserSigHex"],
            serde_json::Value::Null,
            "unconfirmed row"
        );
        assert_eq!(ev["anchored"], true);
        assert_eq!(ev["cardsHex"], "000102030c");
        // The win still tallies as UNCONFIRMED (wins counts confirmed only) —
        // this test pins the EVIDENCE contract, not the ranking rule.
        assert_eq!(lb.board[0].wins, 0);

        // A genuine v1 claim emits an explicit null — never an absent key
        // (absent and null must stay distinguishable on the wire).
        let markers = vec![mk(1, &a, &b, 1, 2, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard(&markers, &statuses, &no_proofs(), 200);
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1)).unwrap();
        let ev = v["board"][0]["evidence"][0].as_object().unwrap();
        assert!(ev.contains_key("cardsHex"), "the key is always present");
        assert_eq!(ev["cardsHex"], serde_json::Value::Null);
    }

    // ── #230 chain-attributed wins (the potparty-v2 seat-binding tier) ──────

    /// attr map: pot-txid byte → (identity_a, identity_b).
    fn attrs_of(
        entries: &[(u8, Option<&str>, Option<&str>)],
    ) -> HashMap<String, crate::results::SeatAttribution> {
        entries
            .iter()
            .map(|(pot, a, b)| {
                (
                    tx(*pot),
                    crate::results::SeatAttribution {
                        identity_a: a.map(str::to_string),
                        identity_b: b.map(str::to_string),
                    },
                )
            })
            .collect()
    }

    fn verdicts_of(
        entries: &[(u8, crate::results::PotVerdict)],
    ) -> HashMap<String, crate::results::PotVerdict> {
        entries.iter().map(|(pot, v)| (tx(*pot), *v)).collect()
    }

    /// THE #276 case: a tower-enforced winner's UNCONFIRMED claim (the loser
    /// is gone — no countersignature exists and never will) COUNTS as a win
    /// once the chain verdict + verified seat-binding marker attribute the
    /// winning seat to the claimed winner. Tier honesty: `proven` stays
    /// false (no countersig), `chainProven` reports the new fact.
    #[test]
    fn unconfirmed_enforced_win_counts_when_chain_attributed() {
        let w = ident(0xaa);
        let l = ident(0xbb);
        let markers = vec![mk(1, &w, &l, 1, 2, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let verdicts = verdicts_of(&[(1, crate::results::PotVerdict::WinnerA)]);
        let attrs = attrs_of(&[(1, Some(&w), Some(&l))]);

        // WITHOUT the attribution: today's behaviour — an unconfirmed row,
        // wins = 0 (the exact #276 injustice).
        let lb =
            aggregate_leaderboard_with_verdicts(&markers, &statuses, &no_proofs(), 200, &verdicts);
        assert_eq!(lb.board[0].wins, 0);
        assert!(!lb.board[0].proven);
        assert!(!lb.board[0].chain_proven);

        // WITH it: the win counts, on the honest new tier.
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
        );
        assert_eq!(lb.board[0].identity, w);
        assert_eq!(
            lb.board[0].wins, 1,
            "a chain-attributed enforced win COUNTS"
        );
        assert!(
            !lb.board[0].proven,
            "no countersignature ⇒ proven stays false"
        );
        assert!(
            lb.board[0].chain_proven,
            "the chainProven tier reports the chain fact"
        );
        // The wire body carries both tier flags + the falsifiable attribution.
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1)).unwrap();
        assert_eq!(v["board"][0]["wins"], 1);
        assert_eq!(v["board"][0]["proven"], false);
        assert_eq!(v["board"][0]["chainProven"], true);
        assert_eq!(v["board"][0]["evidence"][0]["chainAttributedWinner"], w);

        // A countersigned + attributed win carries BOTH tiers (counted once).
        let markers = vec![mk(1, &w, &l, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
        );
        assert_eq!(lb.board[0].wins, 1);
        assert!(lb.board[0].proven);
        assert!(lb.board[0].chain_proven);
    }

    /// Adversarial (risk register B1/B5): a fabricated claim naming a THIEF
    /// as winner of a chain-attributed pot is CONTRADICTED and counts for
    /// nobody — even countersigned by an accomplice — while the honest
    /// attributed winner's own claim still counts. And an attribution with
    /// NO claim by the attributed winner mints NOTHING (every counted win
    /// stays reconstructible from evidence).
    #[test]
    fn chain_attribution_beats_fabricated_and_never_mints_rowless_wins() {
        let honest = ident(0xaa);
        let loser = ident(0xbb);
        let thief = ident(0xcc);
        let verdicts = verdicts_of(&[(1, crate::results::PotVerdict::WinnerA)]);
        let attrs = attrs_of(&[(1, Some(&honest), Some(&loser))]);

        // Thief's countersigned fabrication vs the honest unconfirmed claim:
        // pre-#230 the conflicting-set rules would let the CONFIRMED thief
        // claim win the game. With attribution the thief is contradicted.
        let markers = vec![
            mk(1, &thief, &loser, 1, 2, true, None, 200, 0),
            mk(1, &honest, &loser, 1, 2, false, None, 100, 1),
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
        );
        let row_of = |id: &str| lb.board.iter().find(|r| r.identity == id);
        assert!(
            row_of(&thief).is_none(),
            "the contradicted thief counts NOTHING"
        );
        let h = row_of(&honest).expect("the attributed winner counts");
        assert_eq!(h.wins, 1);
        assert!(h.chain_proven);
        assert!(!h.proven);

        // Attribution alone, with NO claim by the attributed winner: nobody
        // counts (a win must stay reconstructible from an anchored marker in
        // evidence), and the thief's contradicted claim stays excluded.
        let markers = vec![mk(1, &thief, &loser, 1, 2, true, None, 200, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
        );
        assert!(
            lb.board.is_empty(),
            "no self-claim ⇒ no row, and the thief is contradicted"
        );

        // An UN-anchored claim never counts even when attributed (the anchor
        // rule is untouched).
        let markers = vec![mk(1, &honest, &loser, 1, 2, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::new()); // pot unknown
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
        );
        assert!(lb.board.is_empty());

        // Attribution for a NONEXISTENT/unclassified pot contributes nothing:
        // verdictless pots fall through to the claim rules verbatim.
        let markers = vec![mk(3, &honest, &loser, 3, 4, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(3u8, 4u8)]));
        let attrs_wrong = attrs_of(&[(9, Some(&honest), Some(&loser))]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &HashMap::new(),
            &attrs_wrong,
        );
        assert_eq!(
            lb.board[0].wins, 0,
            "unconfirmed stays unconfirmed without a verdict"
        );
        assert!(!lb.board[0].chain_proven);
    }

    #[test]
    fn clamp_limit_defaults_and_bounds() {
        assert_eq!(clamp_leaderboard_limit(None), LEADERBOARD_DEFAULT_LIMIT);
        assert_eq!(clamp_leaderboard_limit(Some(50)), 50);
        assert_eq!(clamp_leaderboard_limit(Some(0)), 1);
        assert_eq!(clamp_leaderboard_limit(Some(99_999)), LEADERBOARD_MAX_LIMIT);
    }
}
