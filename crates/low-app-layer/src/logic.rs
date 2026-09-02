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
// STRICT `<`, not `<=`: `pots_view_join_sql`'s #375 era arm binds 2n+1, so
// one slot past the outpoint binds must stay free — the proof covers the
// WIDEST arm. With `<=`, bumping the chunk size to exactly the cap would
// build green and 503 every full-chunk /pots-view, but ONLY once the era
// cutoff is set (review LOW-1, the Rule 14 latent shape).
const _: () = assert!(D1_CHUNK_OUTPOINTS * BINDS_PER_OUTPOINT < D1_MAX_BOUND_PARAMS);

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
    /// #371 witness pair — populated only by the D1 batch producer
    /// (`batch_where_sql` → [`assemble_statuses`]): the overlay's own
    /// `network_seen` witness for the recorded spender, and the spender's
    /// bytes-finality latch. `None` from every other producer (`/spent-any`,
    /// legacy constructors), which keeps those rows on the strict confirmed
    /// bar.
    ///
    /// SERIALIZED to `/utxo-status` as `spenderSeen`/`spenderFinal` (2026-08-15,
    /// for `low-monitor`'s theft-alarm spender-selection). `spenderSeen` IS
    /// ARC/Arcade's `SEEN_ON_NETWORK` verdict (the overlay's broadcast-gate
    /// latches `network_seen` only at that status — `broadcaster.rs`), so it is
    /// the mempool-acceptance bar with double-spends excluded: the monitor
    /// trusts a spender pointer only when `spenderSeen` or `spentConfirmed`,
    /// which is exactly what a bare (un-broadcast) `historical-tx-no-spv` plant
    /// can NEVER earn. `null` on any non-D1 producer is the strict-confirmed
    /// fallback, never a positive.
    pub spender_seen: Option<bool>,
    pub spender_final: Option<bool>,
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
            spender_seen: None,
            spender_final: None,
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
    /// confirmation flag. No #371 witness fields — the `/spent-any`
    /// (WoC-backed) producer and legacy callers have none, and their absence
    /// keeps those rows on the strict confirmed bar.
    pub fn known(
        op: &Outpoint,
        spent: bool,
        spending_txid: Option<String>,
        spent_confirmed: bool,
    ) -> Self {
        Self::known_with_witness(op, spent, spending_txid, spent_confirmed, None, None)
    }

    /// [`Self::known`] carrying the #371 witness pair (the D1 batch read's
    /// shape): the overlay's own `network_seen` witness for the recorded
    /// spender + the spender's bytes-finality latch.
    pub fn known_with_witness(
        op: &Outpoint,
        spent: bool,
        spending_txid: Option<String>,
        spent_confirmed: bool,
        spender_seen: Option<bool>,
        spender_final: Option<bool>,
    ) -> Self {
        Self {
            txid: op.txid.clone(),
            vout: op.vout,
            known: true,
            spent: Some(spent),
            spending_txid,
            spent_confirmed: Some(spent_confirmed),
            spender_seen,
            spender_final,
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
                // #371 witness pair (2026-08-15): `spenderSeen` = ARC
                // SEEN_ON_NETWORK (mempool-accepted, double-spends excluded);
                // `spenderFinal` = the spender's bytes-finality latch. Present
                // on the D1 batch path (null elsewhere = strict-confirmed
                // fallback). low-monitor trusts a spender pointer only when
                // `spenderSeen` OR `spentConfirmed` — a bare caller plant earns
                // neither. Additive: existing consumers ignore unknown keys.
                "spenderSeen": e.spender_seen,
                "spenderFinal": e.spender_final,
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
    let clause = vec!["(p.txid = ? AND p.outputIndex = ?)"; n].join(" OR ");
    // #371 owner ruling (2026-08-06, "we shouldn't have to wait for confirm"):
    // the batch status read carries the spender's bytes-finality latch and the
    // overlay's OWN network witness, so the leaderboard's counting bar can
    // accept a SEEN covenant settle — the network already validated the
    // covenant spend; an invalid one would never be relayed.
    format!(
        "SELECT p.txid, p.outputIndex, p.spent, p.spendingTxid, p.spentConfirmed, \
                p.spenderFinal, ns.txid IS NOT NULL AS spenderSeen \
         FROM pot_records p \
         LEFT JOIN network_seen ns ON p.spendingTxid IS NOT NULL \
              AND ns.txid = lower(p.spendingTxid) \
         WHERE {clause}"
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
    /// #371: the spender's bytes-finality latch (`None` = pre-migration row)
    /// and the overlay's own `network_seen` witness for the recorded spender.
    pub spender_final: Option<bool>,
    pub spender_seen: Option<bool>,
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
                Some(r) => OutpointStatus::known_with_witness(
                    op,
                    r.spent,
                    r.spending_txid.clone(),
                    r.spent_confirmed,
                    r.spender_seen,
                    r.spender_final,
                ),
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
/// #375: `written_off_before_ms` set ⇒ a pot whose ADMISSION stamp
/// (`pot_records.createdAt`, unix seconds, server-written) pre-dates the
/// cutoff is excluded — the requested outpoint then assembles as the
/// fail-safe `known:false` shape, never a Collect-able status. ONE extra
/// bind (the cutoff, LAST). `None` ⇒ byte-identical to the pre-#375 query.
pub fn pots_view_join_sql(n: usize, written_off_before_ms: Option<i64>) -> String {
    debug_assert!((1..=MAX_OUTPOINTS).contains(&n), "parse_outpoints bounds n");
    let clause = vec!["(p.txid = ? AND p.outputIndex = ?)"; n].join(" OR ");
    // The OR list needs its own parens once a conjunct follows it.
    let where_clause = match written_off_before_ms {
        None => format!("WHERE {clause}"),
        Some(_) => format!(
            "WHERE ({clause}){era}",
            era = era_filter_sql("p.createdAt", "?", written_off_before_ms)
        ),
    };
    format!(
        "SELECT p.txid, p.outputIndex, p.spent, p.spendingTxid, p.spentConfirmed, \
                hex(b.beef) AS spenderBeef \
         FROM pot_records p \
         LEFT JOIN pot_beefs b ON b.txid = lower(p.spendingTxid) \
         {where_clause}"
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
/// #375: `written_off_before_ms` set ⇒ the innermost scan drops rows whose
/// era anchor pre-dates the cutoff BEFORE the dedupe/quota windows run (a
/// written-off ghost must not consume an unknown-pot quota slot). The anchor
/// is `COALESCE(r.createdAt, pp.createdAt)` — the pot's OWN admission stamp
/// when a `pot_records` row survives (a marker republish alone cannot
/// resurface such a pot), else the marker's admission stamp (the fresh
/// in-flight pot a recovering client most needs stays visible; the
/// re-admission residual this fallback carries is stated ONCE, at
/// `era_filter_sql`). ONE extra bind (the
/// cutoff, after the identity). `None` ⇒ byte-identical to the pre-#375
/// query.
/// PARTY CANDIDATES (bsv-low 2026-08-29, the run-A "invisible own refund"):
/// the candidate set every IDENTITY-KEYED view (`/results`, `/recovery-view`,
/// `/refund-view`) enumerates, as ONE subquery aliased `pp` with the
/// `potparty_records` column shape the three windows already consume
/// (`identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight,
/// seatSettlePubkey, seatSigHex, sigHex, createdAt, markerRowid, sigValid`).
///
/// THE GAP. The views keyed pot ownership on the seat's `LOW/potparty/v2`
/// marker alone. That marker is published at the END of a hand; a seat
/// killed right after funding (towerDeadManArm / refundLandedArm: both
/// clients die post-fund by design) never publishes it, so its OWN refunded
/// pot was invisible to its own views — `/hops-view` served the hop with
/// `spendingTxid` = the JOIN, `/utxo-status` served the pot spent by the
/// refund with 8 confirmations, and `/results?identity` carried no row. The
/// refund never became a Collect card (the credit-on-return cells:
/// refundLandedVerify, stageFByteDropDrill, homeKeyRecovery).
///
/// THE PROOF ALREADY IN HAND. The seat's `LOW/hopparty` marker is published
/// at FUND time, admission-latched (`markerValid`), carries the seat's
/// committed settle key, and its hop output is spent by exactly one tx —
/// the JOIN, which IS the pot funding tx (the covenant pot sits at vout
/// `LEADERBOARD_POT_VOUT`). A pot whose decoded lock commits that seat key
/// is therefore this identity's pot by chain construction: the same
/// decoded-owner spine the leaderboard's #403 fold and
/// `fill_seats_from_hop_markers` already trust. No party marker is needed
/// to LIST the pot; the party marker, when it exists, still wins (the hop
/// arm is suppressed for any pot the identity already has a party row
/// for — one pot, one candidate source, no double rows).
///
/// Bars kept: `markerValid = 1` only (the latched verdict; an unlatched or
/// failed hop marker attributes nothing — the `attribute_seats` rule), the
/// hop must be SPENT with a recorded spender, the pot must be DECODED
/// (`paramsDecoded`) with a `recoveryHeight`, and the marker's seat key must
/// be one of the pot's committed keys. Everything is read from columns the
/// overlay wrote at admission; the read path runs no ECDSA.
///
/// BINDS: `?1` = the identity (lowercase), used by BOTH arms — the routes
/// still bind exactly [identity, era?]; the views number their era slot
/// `?2` for the same reason. `rowid` is exposed as `markerRowid` (a
/// compound subquery has no implicit rowid).
pub fn party_candidates_sql() -> String {
    format!(
        "(SELECT identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
                 seatSettlePubkey, seatSigHex, sigHex, createdAt, rowid AS markerRowid, sigValid \
            FROM potparty_records WHERE identity = ?1 \
          UNION ALL \
          SELECT hp.identity AS identity, hp.opponentIdentity AS opponentIdentity, \
                 hp.gameId AS gameId, pot.txid AS potTxid, pot.outputIndex AS potVout, \
                 pot.recoveryHeight AS recoveryHeight, \
                 hp.seatSettlePubkey AS seatSettlePubkey, hp.seatSigHex AS seatSigHex, \
                 hp.identitySigHex AS sigHex, hp.createdAt AS createdAt, \
                 hp.rowid AS markerRowid, hp.markerValid AS sigValid \
            FROM hopparty_records hp \
            JOIN pot_records hop ON hop.txid = hp.txid AND hop.outputIndex = hp.hopVout \
            JOIN pot_records pot ON pot.txid = lower(hop.spendingTxid) \
                 AND pot.outputIndex = {vout} \
           WHERE hp.identity = ?1 AND hp.markerValid = 1 \
             AND hop.spent = 1 AND hop.spendingTxid IS NOT NULL \
             AND pot.paramsDecoded = 1 AND pot.recoveryHeight IS NOT NULL \
             AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL \
             AND lower(hp.seatSettlePubkey) IN (lower(pot.pubA), lower(pot.pubB)) \
             AND NOT EXISTS (SELECT 1 FROM potparty_records x \
                              WHERE x.identity = hp.identity \
                                AND x.potTxid = pot.txid AND x.potVout = pot.outputIndex))",
        vout = LEADERBOARD_POT_VOUT,
    )
}

pub fn recovery_view_sql(written_off_before_ms: Option<i64>, after: usize) -> String {
    // NOTE: any change here must keep the `w.`-qualified outer ORDER BY —
    // SQLite does not guarantee ordering survives a join otherwise.
    //
    // The window takes MAX_ROWS + 1 so the caller can detect truncation
    // without a second COUNT query (see `assemble_recovery_view`).
    format!(
        "SELECT w.gameId AS gameId, w.potTxid AS potTxid, w.potVout AS potVout, \
            w.recoveryHeight AS recoveryHeight, \
            w.covRecoveryHeight AS covRecoveryHeight, \
            w.covPubA AS covPubA, w.covPubB AS covPubB, \
            w.covPayPkhA AS covPayPkhA, w.covPayPkhB AS covPayPkhB, \
            w.opponentIdentity AS opponentIdentity, \
            w.spent AS spent, w.spendingTxid AS spendingTxid, \
            w.spentConfirmed AS spentConfirmed, \
            hex(b.beef) AS spenderBeef \
     FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
              covPubA, covPubB, covPayPkhA, covPayPkhB, \
              opponentIdentity, spent, spendingTxid, spentConfirmed, \
              markerCreatedAt, markerRowid, potCreatedAt, potBestSigRank, tier \
       FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
                covPubA, covPubB, covPayPkhA, covPayPkhB, \
                opponentIdentity, spent, spendingTxid, spentConfirmed, \
                markerCreatedAt, markerRowid, potCreatedAt, potBestSigRank, tier, \
                DENSE_RANK() OVER (ORDER BY potBestSigRank DESC, tier ASC, \
                                            COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                                            markerCreatedAt DESC, markerRowid DESC) \
                    AS finalRank \
       FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
              covPubA, covPubB, covPayPkhA, covPayPkhB, \
              opponentIdentity, spent, spendingTxid, spentConfirmed, \
              markerCreatedAt, markerRowid, potCreatedAt, potBestSigRank, \
              CASE WHEN unknownPot = 0 OR potRank <= {quota} THEN 0 ELSE 1 END AS tier \
       FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
                covPubA, covPubB, covPayPkhA, covPayPkhB, \
                opponentIdentity, spent, spendingTxid, spentConfirmed, \
                markerCreatedAt, markerRowid, potCreatedAt, unknownPot, \
                potBestSigRank, \
                ROW_NUMBER() OVER (PARTITION BY unknownPot \
                                   ORDER BY potBestSigRank DESC, \
                                            COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                                            markerCreatedAt DESC, markerRowid DESC) AS potRank \
         FROM (SELECT pp.gameId AS gameId, pp.potTxid AS potTxid, \
                  pp.potVout AS potVout, pp.recoveryHeight AS recoveryHeight, \
                  r.recoveryHeight AS covRecoveryHeight, \
                  r.pubA AS covPubA, r.pubB AS covPubB, \
                  r.payPkhA AS covPayPkhA, r.payPkhB AS covPayPkhB, \
                  pp.opponentIdentity AS opponentIdentity, \
                  r.spent AS spent, r.spendingTxid AS spendingTxid, \
                  r.spentConfirmed AS spentConfirmed, \
                  pp.createdAt AS markerCreatedAt, pp.markerRowid AS markerRowid, \
                  r.createdAt AS potCreatedAt, \
                  CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                  MAX({rank}) OVER (PARTITION BY pp.potTxid, pp.potVout) \
                      AS potBestSigRank, \
                  ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout \
                                     ORDER BY {rank} DESC, \
                                              pp.createdAt ASC, pp.markerRowid ASC) AS rn \
           FROM {party} pp \
           LEFT JOIN pot_records r \
                  ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
           WHERE pp.identity = ?1{era}) \
         WHERE rn = 1))) \
       WHERE finalRank > {after} AND finalRank <= {after} + {probe} \
       ORDER BY potBestSigRank DESC, tier ASC, \
                COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                markerCreatedAt DESC, markerRowid DESC \
       LIMIT {probe}) w \
     LEFT JOIN pot_beefs b ON b.txid = lower(w.spendingTxid) \
     ORDER BY w.potBestSigRank DESC, w.tier ASC, \
              COALESCE(w.potCreatedAt, w.markerCreatedAt) DESC, \
              w.markerCreatedAt DESC, w.markerRowid DESC",
        quota = RECOVERY_VIEW_UNKNOWN_QUOTA,
        probe = RECOVERY_VIEW_MAX_ROWS + 1,
        after = after,
        rank = overlay_discovery::potparty::validity::sig_rank_expr("pp."),
        party = party_candidates_sql(),
        era = era_filter_sql(
            "COALESCE(r.createdAt, pp.createdAt)",
            "?2",
            written_off_before_ms
        ),
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

/// The `after` cursor's ceiling (brain-cutover M2c) — same bound + rationale
/// as [`crate::hops_view::HOPS_VIEW_AFTER_MAX`]: the route clamps the parsed
/// value here and [`recovery_view_body`] stops emitting `nextAfter` at the
/// ceiling, because a walker whose next step re-clamps to the same page loops
/// forever rather than walking.
pub const RECOVERY_VIEW_AFTER_MAX: usize = 1_000_000;

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
    /// The pot's COMMITTED covenant keys (#343), decoded at admission from
    /// its own funding lock (`pot_records`' #284 columns). `None` when the
    /// pot has no row, is not a covenant lock, or its stored params are not
    /// a complete well-formed set — see [`crate::results::CommittedKeys`],
    /// including why a MISMATCH is not a not-yours claim.
    pub committed_keys: Option<crate::results::CommittedKeys>,
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
    /// The pot's COMMITTED covenant keys (#343), or `None` = cannot say. See
    /// [`crate::results::CommittedKeys`].
    pub committed_keys: Option<crate::results::CommittedKeys>,
    /// #252 stage A — a `collected_markers_v2` row EXISTS for
    /// (caller identity, gameId). PRESENCE ONLY, and presence proves nothing
    /// (admission is byte-format with no server-side sig verify — see the
    /// overlay's collected storage docs): the client's Collect surface still
    /// verifies marker signatures under its own wallet before treating a game
    /// as collected. `None` = could not ask (query fault / racing migration)
    /// — never conflated with `Some(false)` ("asked, no row"). Display /
    /// dedup hint only; NEVER a money gate.
    pub collected: Option<bool>,
    /// #252 stage A — the caller's trust-gated outcome word for this pot,
    /// REUSED verbatim from the `/results` derivation
    /// (`derive_outcome_with_seat` — Rule 15: derived once, never
    /// re-implemented here). `None` = the results derivation was unavailable
    /// or did not cover this pot; the honesty pair below says how a present
    /// word was derived.
    pub outcome: Option<crate::results::Outcome>,
    /// How `outcome` was derived (`"chain"` / `"chain+seatkey"` /
    /// `"chain+claim"`), `None` for absent/unresolved — the same wire
    /// contract as `/results`' `outcomeSource`.
    pub outcome_source: Option<&'static str>,
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
                // #343 — carried straight through: the row already holds the
                // pot's committed keys, and this view never re-derives them.
                committed_keys: r.committed_keys,
                // #252 stage A — filled by `apply_recovery_extras` AFTER
                // assembly (the route's best-effort side reads); `None` here
                // is the honest "not asked yet".
                collected: None,
                outcome: None,
                outcome_source: None,
            }
        })
        .collect();
    (entries, truncated)
}

// ── #252 stage A — the /recovery-view read-behind extras ────────────────────

/// The `collected_markers_v2` PRESENCE query for one identity over a chunk of
/// gameIds: which of these games carry at least one admitted collected
/// marker naming this identity? `DISTINCT gameId` — presence, never rows
/// (the marker sig is verified CLIENT-side only; see [`RecoveryEntry::collected`]).
/// Binds: identity, then `n` gameIds (all lowercase hex by write convention).
pub fn collected_presence_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let marks = vec!["?"; n].join(", ");
    format!(
        "SELECT DISTINCT gameId FROM collected_markers_v2 \
         WHERE identity = ? AND gameId IN ({marks})"
    )
}

/// Proof that the #252 extras fold RAN over these entries — the ONLY way to
/// obtain the argument [`recovery_view_body`] demands (private field, one
/// constructor: [`apply_recovery_extras`]).
///
/// Why a proof type rather than a convention (Rule 22, the schema.rs
/// `LatchColumnsEnsured` pattern): the extras' OFF state (`collected: null`,
/// `outcome: null`) is a VALID tri-state wire answer, so if the route's fold
/// call were deleted (or `if false`'d), every test would stay green while
/// the feature died silently in production — the exact D1-latch-NULL class
/// Rule 22 documents. With this type, deleting the fold is a BUILD failure.
pub struct RecoveryEntriesReady(Vec<RecoveryEntry>);

impl RecoveryEntriesReady {
    /// The folded entries (read-only — the fold is the sole writer).
    pub fn entries(&self) -> &[RecoveryEntry] {
        &self.0
    }
}

/// One pot outpoint's folded outcome pair: `(outcome, outcomeSource)`.
type OutcomePair = (crate::results::Outcome, Option<&'static str>);
/// The `(gameId, potTxid, potVout)` → [`OutcomePair`] fold index.
type OutcomeIndex = std::collections::HashMap<(String, String, u32), OutcomePair>;

/// Fold the route's two BEST-EFFORT side reads into the assembled entries —
/// PURE, so the fold is natively testable (the D1 fetches themselves are the
/// route's, like every other view).
///
/// * `outcomes`: the `/results` entries for the SAME identity (Rule 15 — the
///   one derivation), matched on `(gameId, potTxid, potVout)` lowercase.
///   `None` = the derivation was unavailable; every entry keeps
///   `outcome: None`. An entry the results page does not cover (e.g. its own
///   truncation window differs) also stays `None` — absence, never a guess.
/// * `collected`: the set of gameIds (lowercase) with a marker row, or
///   `None` = the presence query could not run — every entry keeps
///   `collected: None` (cannot-say), NEVER `Some(false)`.
pub fn apply_recovery_extras(
    mut entries: Vec<RecoveryEntry>,
    outcomes: Option<&[crate::results::ResultEntry]>,
    collected: Option<&std::collections::HashSet<String>>,
) -> RecoveryEntriesReady {
    let outcome_map: Option<OutcomeIndex> = outcomes.map(|list| {
        list.iter()
            .map(|e| {
                (
                    (
                        e.game_id.to_ascii_lowercase(),
                        e.pot_txid.to_ascii_lowercase(),
                        e.pot_vout,
                    ),
                    (e.outcome, e.outcome_source),
                )
            })
            .collect()
    });
    for entry in &mut entries {
        if let Some(set) = collected {
            entry.collected = Some(set.contains(&entry.game_id.to_ascii_lowercase()));
        }
        if let Some(map) = &outcome_map {
            if let Some((outcome, source)) = map.get(&(
                entry.game_id.to_ascii_lowercase(),
                entry.pot_txid.to_ascii_lowercase(),
                entry.pot_vout,
            )) {
                entry.outcome = Some(*outcome);
                entry.outcome_source = *source;
            }
        }
    }
    RecoveryEntriesReady(entries)
}

/// Assemble the `/recovery-view` wire body:
/// `{"tip":<height|null>,"entries":[{gameId,potTxid,potVout,recoveryHeight,
/// opponentIdentity,spent,spendingTxid,spentConfirmed,spenderRawHex}]}`.
/// `tip` mirrors `/pots-view` (the recovery-height gate needs it) and is
/// `null` on a chaintracks fault — the D1 facts still serve, and the client
/// falls back to its own `/tip`.
pub fn recovery_view_body(
    entries: &RecoveryEntriesReady,
    tip: Option<u64>,
    truncated: bool,
    // The cursor position this page was served from (brain-cutover M2c).
    after: usize,
) -> String {
    let arr: Vec<serde_json::Value> = entries
        .entries()
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
                // NEW (#343): the pot's COMMITTED covenant keys from its own
                // funding lock, or null. This is the WIPED-DEVICE case the
                // field exists for — a device with no local money records
                // still derives its own `[2,'low settle']` key and its own
                // `counterparty:'self'` pay-home PKH and tests membership,
                // instead of taking this server's shaping on trust.
                //
                // A MISMATCH IS NOT "NOT YOURS". The row's potTxid is
                // attacker-chosen and these values are only as good as this
                // server; a consumer treats absence and mismatch alike as
                // CANNOT-SAY and re-derives from hash-verified bytes before
                // acting on a negative. A match on `payPkhA`/`payPkhB` is the
                // unforgeable half.
                "committedKeys": crate::results::CommittedKeys::to_json(
                    e.committed_keys.as_ref()
                ),
                // NEW (#252 stage A) — both ADDITIVE and read-behind (the
                // deployed client's strict parser ignores unknown fields;
                // stage B consumes them). Tri-state honesty: `null` =
                // could-not-ask / not-derived, never conflated with a
                // negative fact.
                //
                // `collected`: a collected_markers_v2 row EXISTS for this
                // (identity, gameId) — PRESENCE ONLY; the client verifies
                // marker sigs itself (a row's existence proves nothing).
                "collected": e.collected,
                // `outcome`/`outcomeSource`: the /results honesty pair,
                // REUSED from the one derivation (Rule 15) — trust-gated
                // exactly as /results serves it.
                "outcome": e.outcome.map(crate::results::Outcome::as_str),
                "outcomeSource": e.outcome_source,
            })
        })
        .collect();
    // #323 HIGH-1 — `truncated` is what makes a marker FLOOD detectable
    // instead of a silently-short page that looks complete. `potparty_records`
    // is attacker-writable (byte-format admission, no signature), so a caller
    // seeing `truncated: true` must treat the page as INCOMPLETE rather than
    // as "these are all my pots".
    // #398's cursor, applied to the recovery surface (brain-cutover M2c).
    // `nextAfter` is present IFF this page trimmed AND a further step is
    // representable — so a walker knows the set is REACHABLE rather than
    // merely knowing it is incomplete. `truncated` keeps its exact meaning
    // (this page is not the whole set), so a deployed client that ignores
    // `nextAfter` behaves byte-identically to before.
    //
    // Why this matters beyond paging: the CLIENT's `creditSweep.seedFromServer`
    // unions this view with an `ls_potparty partyFor` read precisely BECAUSE a
    // truncated page could hide a pot (bsv-low #347's denial-of-recovery). A
    // walkable view removes that justification — the union's remaining value
    // is rank-cap divergence, which a complete walk subsumes.
    let next_after = if truncated && after < RECOVERY_VIEW_AFTER_MAX {
        Some(after + entries.entries().len())
    } else {
        None
    };
    json!({
        "tip": tip,
        "entries": arr,
        "truncated": truncated,
        "nextAfter": next_after,
    })
    .to_string()
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

// ── /epoch — the storage-epoch directive (bsv-low THE ORDER item 2,
//    owner-ruled 2026-08-06) ────────────────────────────────────────────────
//
// A public, static answer read straight from the `STORAGE_EPOCH` var: bumping
// that value in wrangler.toml orders every client to clear its local `low_*`
// state at its next idle home visit (the client half lives in bsv-low
// `app/src/lib/storageEpoch.ts`). No D1, no auth, `no-store` like every
// other route. `null` is the FAIL-SAFE shape — the client treats it as "no
// wipe directive", so an unset/empty var can never trigger a wipe.

/// Normalize the raw `STORAGE_EPOCH` var: unset, empty, or whitespace-only →
/// `None` (serve `null` — no directive), anything else → the trimmed value.
pub fn normalize_storage_epoch(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

// ── #375 — the one-shot pre-launch era write-off (owner-ruled) ──────────────
//
// A cutoff INSTANT (`WRITTEN_OFF_BEFORE_MS`, ms since epoch) is fixed before
// launch; rows whose SERVER-OBSERVED admission stamp pre-dates it stop being
// served by the money-listing views, so written-off test-era games never
// render Collect affordances anywhere and a wiped device's recovery
// enumeration is bounded. Because the cutoff pre-dates launch, no real
// player can ever be behind it — the blunt DROP is the owner ruling (display
// policy in this read-only app-layer; the overlay's admitted truth is
// untouched). The client learns the same instant from `GET /epoch`.

/// Normalize the raw `WRITTEN_OFF_BEFORE_MS` var: unset / empty / whitespace
/// / non-numeric / zero / negative → `None` (INERT — every query is
/// byte-identical to the un-configured build). A positive integer is the
/// cutoff in MILLISECONDS since epoch. Fail-safe direction: a malformed var
/// can only ever serve MORE history, never silently widen the write-off.
pub fn normalize_written_off_before_ms(v: Option<String>) -> Option<i64> {
    v.as_deref()
        .map(str::trim)
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&ms| ms > 0)
}

/// Normalize the raw `WRITTEN_OFF_BEFORE_HEIGHT` var (#375 review H1 — the
/// client's chain-anchored rescue): unset/empty/junk/zero/negative → `None`.
/// The height is CLIENT-PROTECTIVE ONLY — served verbatim on `/epoch`, never
/// used in any SQL here: the client rescues a record from the write-off when
/// the record's own recovery/lock height reaches it (a backdated writing
/// clock can fake a stamp, never a chain height), and tip-sanes the seeding
/// gate against it. A wrong value therefore cannot blank any server view;
/// too high fails to rescue (stamp judgment stands), too low rescues extra
/// (cards stay — the safe direction). Same parse as the ms var.
pub fn normalize_written_off_before_height(v: Option<String>) -> Option<i64> {
    normalize_written_off_before_ms(v)
}

/// #375 review MED-2 — the FUTURE-CUTOFF BELT. `WRITTEN_OFF_BEFORE_MS` is a
/// hand-set one-shot, and the two adjacent one-character mistakes (an extra
/// digit ⇒ effectively microseconds; a pasted FUTURE instant) both produce a
/// well-formed value that would silently blank EVERY money view for EVERY
/// current player — the one direction the write-off must never fail. A
/// cutoff at/after `now` contradicts the ruling's own premise (the cutoff
/// pre-dates launch, i.e. is in the past), so it reads as ABSENT — the same
/// belt the client half carries (`writtenOffEra.ts`). Pure so it is
/// unit-testable; the route reader supplies the worker clock and WARNS on a
/// refusal (a silent clamp would hide the misconfiguration — Rule 13), and
/// `/epoch` + `/health` serve the CLAMPED value so the wire never carries a
/// number the server itself is ignoring.
pub fn clamp_future_cutoff(cutoff_ms: Option<i64>, now_ms: i64) -> Option<i64> {
    cutoff_ms.filter(|&ms| ms < now_ms)
}

pub fn era_filter_sql(anchor_expr_secs: &str, placeholder: &str, cutoff_ms: Option<i64>) -> String {
    match cutoff_ms {
        Some(_) => format!(" AND ({anchor_expr_secs} * 1000 >= {placeholder})"),
        None => String::new(),
    }
}

/// Assemble the `/epoch` wire body:
/// `{"storageEpoch":<string|null>,"writtenOffBeforeMs":<number|null>}`.
/// ADDITIVE (#375): the deployed client reads only `storageEpoch` and
/// ignores unknown fields; `writtenOffBeforeMs: null` is the fail-safe "no
/// write-off" shape, mirroring `storageEpoch`'s.
pub fn epoch_body(
    epoch: Option<&str>,
    written_off_before_ms: Option<i64>,
    written_off_before_height: Option<i64>,
) -> String {
    json!({
        "storageEpoch": epoch,
        "writtenOffBeforeMs": written_off_before_ms,
        "writtenOffBeforeHeight": written_off_before_height,
    })
    .to_string()
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
// TRUST MODEL (#332 v3 — this REPLACES the presence gate the #317 audit ranked
// DEFECTIVE, the interim "verify-the-marker" gate that was a marker-driven
// erasure vector, AND the v2 spine, whose delta re-gate found it minted the win
// from the SEAT ATTRIBUTION and so moved the same oldest-N-over-an-attacker-
// writable-table class one layer down — from the result markers to the
// potparty markers — and ELEVATED it to a public erasure. The win is now a
// pure CHAIN FACT that no marker of any kind can move:
//
//   a win = an admitted pot, CONFIRMED spent (`pot_records` +
//           `is_confirmed_landing`), with a covenant `PotVerdict` naming a
//           winning SEAT (A or B), counted for that seat's COMMITTED SETTLE
//           KEY — `pubA`/`pubB`, read straight out of the pot's own funding
//           lock (the covenant `CovenantParams`), exactly as the #316 validity
//           work reads a lock. Verdict and committed key are both on-chain and
//           UNEVICTABLE; the win depends on NO marker, potparty or result.
//
// Two things ONLY DECORATE a chain-counted win, and neither can erase it:
//  - the WINNER'S IDENTITY is a display mapping settle-key → identity, taken
//    from a SIGNATURE-VERIFIED `LOW/potparty/v2` marker (`attribute_seats`
//    validity-filters: a row counts only if its `seatSig` verifies under the
//    committed key AND its identity sig verifies — byte-format junk under the
//    committed key is FILTERED OUT, never ranked ahead). Where no valid marker
//    is found (an unresolved race, or a junk flood beyond the candidate cap),
//    the identity is UNKNOWN and the win is attributed to the committed settle
//    KEY itself — a stable on-chain-derived id. Never no-win, never a wrong
//    winner: the winner's own client always holds its own key↔identity mapping,
//    so from the winner's view the win always attributes; a third party at
//    worst sees it under the settle key (`identityIsKey` flags this on the
//    wire). This is why the v2 erasure is closed at the root rather than moved
//    again (Rule 3: an index is a set, not a slot; oldest-wins IS the bug — the
//    identity is chosen by VALIDITY, not by winning a first-writer race).
//  - the `result_marker` (also byte-format-admitted) attaches the loser's
//    countersignature (`proven`), the winner's revealed hand (the hands board),
//    and the drill-down evidence; its eviction costs only that decoration.
//
// Where a pot has NO covenant verdict — bare/legacy, tie, or refund — it is
// UNRANKED: never counted. UNDER-count is the honest fail direction. The
// consequence is deliberate: a pre-covenant / bare pot shows NO win however it
// was signed; covenant pots (the only funding path since 2026-07-05) always
// count for the committed winning key, whether or not any marker survives.
//
// `verified_claim` runs only on result markers for an already-counted pot
// whose claimed winner matches the resolved identity (bounded verify budget —
// MEDIUM-3). `chainProven ⇔ wins > 0`; `proven` stays the stricter, distinct
// fact "the loser countersigned".
//
// NO WRONG-WINNER, NO ERASURE RESIDUAL. A covenant pot's win is unconditional
// on the winning key. A potparty-marker flood under the committed key can only
// degrade the DISPLAY from the identity to the key (under-count of the display,
// self-recovering for the winner's own client); it can never drop the win or
// award it elsewhere. The only remaining under-count is unattributed
// (non-covenant) pots, by construction.

/// Default `?limit` for `/leaderboard` (contract default). Since #332 the
/// limit counts DISTINCT POTS in the marker window (a settled hand ≙ one
/// pot — the #282 `ls_result` semantics), not raw marker rows; each pot
/// yields at most [`overlay_discovery::result::storage::RESULT_ROWS_PER_POT`]
/// rows, so `resultCount` can exceed `limit` by that factor.
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

/// Freshness window for the `/leaderboard` unknown-pot quota promotion —
/// MUST equal the overlay's
/// `d1_discovery::UNKNOWN_POT_PROMOTION_MAX_AGE_SECS` (#283a; same table,
/// same attack, same honest race). The value is DUPLICATED here because
/// `bsv-overlay-cloudflare` is a Worker-binary crate this pure read layer
/// cannot link at runtime; the agreement is PINNED by an executing test in
/// `tests/leaderboard_window_sqlite.rs` (Rule 16: share the constant or pin
/// the boundary — a duplicated value with no pin is a boundary with no pin).
pub const LEADERBOARD_UNKNOWN_POT_MAX_AGE_SECS: u64 = 3600;

/// The per-pot superset size of the `/leaderboard` marker window — re-export
/// of `overlay_discovery::result::storage::RESULT_ROWS_PER_POT`, the SAME
/// constant the overlay's own `result_window_sql` uses (#332 LOW-1: so the
/// SQL builder and every harness read one shared value, never a hand-typed
/// copy of it).
pub const LEADERBOARD_RESULT_ROWS_PER_POT: usize =
    overlay_discovery::result::storage::RESULT_ROWS_PER_POT;

/// The `/leaderboard` unknown-pot promotion quota for a pot-window `limit` —
/// MUST agree with the overlay's `d1_discovery::unknown_pot_quota` (#283b;
/// pinned by the same executing agreement test as the freshness constant).
pub fn leaderboard_unknown_pot_quota(limit: usize) -> usize {
    (limit / 10).max(1)
}

/// Cap on DISTINCT `(gameId, winner)` pairs the route fetches `proof_markers`
/// pointers for (#332). Pairs come from the marker window in rank order, so
/// the honest population is ~1–2 per pot (≤ the pot limit); the cap only
/// bounds the D1 chunk fan-out when an attacker stuffs a pot's
/// [`overlay_discovery::result::storage::RESULT_ROWS_PER_POT`]-row superset
/// with distinct invented gameIds. Exceeding it degrades ONLY the
/// `proofTxid` display hint for the overflow pairs — never a count.
pub const LEADERBOARD_PROOF_PAIRS_CAP: usize = 512;

/// How many `proof_markers` pointers the fetch returns PER `(gameId, winner)`
/// key (#332 HIGH-1). `gameId` and `winner` are BOTH public, claimable names,
/// and `tm_proof` admission is byte-format-only — so any single-winner slot
/// (oldest-wins OR newest-wins) is a squattable drill-down: one pre-filed junk
/// pointer would OWN a victim's proof link for the life of the object (the
/// #316 negative result — anchoring a name-keyed slot to a first claimant made
/// the squat WORSE). Per network-enforcement rule 3, this surface STOPS
/// PICKING: it returns a bounded SUPERSET per key and the CLIENT filters by
/// transcript validity (it fetches each bundle and verifies the proof anyway).
/// The bound is the cost cap; no order within it decides truth.
pub const PROOF_POINTERS_PER_KEY: usize = 4;

/// The `proof_markers` pointer fetch for a chunk of `n` `(gameId, winner)`
/// pairs — 2 binds each, chunked at [`D1_CHUNK_OUTPOINTS`] (#332).
///
/// Returns up to [`PROOF_POINTERS_PER_KEY`] pointers per key (superset —
/// HIGH-1: no exclusivity, so nothing to squat), keyed to the window's OWN
/// pairs so an unrelated flood is irrelevant (this replaces the flat
/// `ORDER BY createdAt DESC LIMIT 2000` scan, which was both floodable and a
/// repoint primitive). Newest-first inside the key so a fresh honest
/// republish can always enter the superset; the client transcript-verifies
/// each candidate and keeps the valid one, so order never decides truth.
/// Residual, stated: >`PROOF_POINTERS_PER_KEY` junk pointers per key can push
/// the honest one out of the superset — a broken display hint (never a false
/// proof, the client verifies), the same bounded-eviction residual as every
/// other superset-then-verify surface here (`seat_markers_sql`,
/// `result_window_sql`).
pub fn proof_pointers_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let pairs = vec!["(gameId = ? AND winner = ?)"; n].join(" OR ");
    format!(
        "SELECT gameId, winner, txid \
         FROM (SELECT gameId, winner, txid, \
                      ROW_NUMBER() OVER (PARTITION BY gameId, winner \
                                         ORDER BY createdAt DESC, rowid DESC) AS rn \
               FROM proof_markers WHERE {pairs}) \
         WHERE rn <= {cap}",
        cap = PROOF_POINTERS_PER_KEY
    )
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
    /// The admission-latched claim TIER (brain-cutover M1): `None` = admitted
    /// before the latch (the compute-at-serve arm), `Some(0)` invalid,
    /// `Some(1)` winner-valid, `Some(2)` countersigned. `verified_claim`
    /// consults this FIRST and only computes on `None`.
    pub claim_valid: Option<i64>,
}

/// The unknown-tier page bound. Stage-1 ranks fresh-unknown pots over the
/// WHOLE history; this page is bounded, ordered `orderAt DESC` — fresh
/// unknowns (potFirstMarkerAt within the hour) are by construction the
/// newest, so they front-load the page. A flood beyond this bound narrows
/// the quota's candidate set (the honest direction: fewer unknowns listed,
/// never a minted row), and any board this page cannot PROVE over-full is
/// served by the fallback path anyway.
pub const LB_UNKNOWN_PAGE_ROWS: usize = 400;

/// One `lb_marker_rows` page row: the marker plus the write-time stamps the
/// tier/quota combiner needs.
pub struct LbPageRow {
    pub marker: ResultMarkerRow,
    pub marker_rowid: i64,
    pub pot_first_marker_at: Option<i64>,
    pub order_at: Option<i64>,
    pub unknown_pot: bool,
}

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
    /// The `proof_markers` (ls_proof) pointer SUPERSET for (gameId, winner) —
    /// up to [`PROOF_POINTERS_PER_KEY`] candidate marker txids (#332 HIGH-1:
    /// a single-pointer slot on a claimable name is squattable, so this is a
    /// set the CLIENT filters by transcript validity, never a server pick).
    /// Empty when none indexed.
    pub proof_txids: Vec<String>,
    /// A VALID `LOW/proof/v1` bundle is POSTED for this (game, winner) —
    /// proof-in-DB (2026-09-02): the winner files the bundle with the
    /// app-layer instead of buying an on-chain marker. Stamped by the route
    /// from `proof_posts` (never by the aggregator: no marker carries it).
    /// Display hint only — the client fetches + replays the bundle itself.
    pub proof_posted: bool,
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
    /// bsv-low #406: WHO SIGNED this pot's recorded spend, from the overlay's
    /// admission-latched classification (`'coop'` = both seats signed the
    /// settlement itself; `'tower-a'`/`'tower-b'` = the tower co-signed with
    /// that seat — the enforced family). `None` = not established (pre-#406
    /// row awaiting backfill, no verifying pair, non-covenant). Served under
    /// the SAME freshness guard as `server_verdict` (the verdict group
    /// shares `verdictTxid`'s lineage). DISPLAY-TIER by contract: the client
    /// picks its ending narration from it; nothing counts or ranks on it.
    pub settle_signers: Option<String>,
    /// The ADMISSION-LATCHED claim tier for this row (brain-cutover M2b):
    /// `Some(2)` countersigned, `Some(1)` winner-sig-valid, `Some(0)`
    /// invalid, `None` = a row the relatch sweep has not reached (the client
    /// computes that one itself, exactly as it computed every row before).
    ///
    /// This is the same verdict `verified_claim` serves `/results` from, and
    /// the same recipe the client's `verifyBoardEvidence` runs — carried onto
    /// the wire so the Leaderboard stops re-running an ECDSA per counted row
    /// per render (#401: 721 ms of blocked main thread on a lived-in
    /// identity). The signature fields stay on the row, so a client that
    /// wants to falsify the server still can (the "verify on chain" escape
    /// hatch is unchanged) — it simply no longer does it by default.
    pub claim_tier: Option<i64>,
    /// When the overlay first admitted this result marker (unix seconds), from
    /// `result_markers_v2.createdAt`. `None` when the index never recorded one.
    ///
    /// DISPLAY-ONLY, and the client treats it that way: it is the app-layer's
    /// own admission stamp, not a chain fact and not a claim either seat
    /// signed, so nothing counts or ranks on it. It exists because a played
    /// hand without a date is a record nobody can place in time — the slow
    /// `gatherBoard` path has always carried it (`HandRow.createdAt`), and the
    /// fast path silently dropped it, so the same board rendered dated or
    /// undated depending on which path served it.
    pub created_at: Option<i64>,
}

/// One chain-counted win's on-chain ANCHOR (bsv-low #336/#337): the pot
/// funding txid (vout 0 is the pot) and the settle txid that spent it. The
/// client re-derives the win from these via `/beef` (the covenant lock + the
/// settle's output shape + the winning committed key) INDEPENDENT of any
/// result marker — so a marker flood that evicts the honest countersigned
/// marker (leaving `evidence` empty) can no longer erase the win client-side,
/// and a KEY-ATTRIBUTED win (no result marker at all) still counts for a
/// third-party viewer. It is a POINTER, never an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainWinAnchor {
    pub pot_txid: String,
    pub settle_txid: String,
    /// bsv-low #406: who signed this settle (see
    /// `LeaderboardEvidence::settle_signers` — same source, same guard).
    pub settle_signers: Option<String>,
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
    /// #332 v3: `identity` above is the committed WINNING SETTLE KEY, not a
    /// resolved identity key — the potparty identity mapping was unavailable
    /// (a still-in-flight marker, or a junk flood beyond the candidate cap
    /// evicting the verified one). The WIN is real and chain-derived
    /// regardless; this only tells a viewer the row is keyed by an on-chain
    /// key rather than a player identity. The winning player's own client
    /// holds its key↔identity mapping, so it always renders under its
    /// identity; a third party sees the key. Never a wrong winner, never a
    /// dropped win.
    pub identity_is_key: bool,
    /// #336/#337: the on-chain anchors of THIS row's chain-counted wins — one
    /// per counted pot, emitted for EVERY counted win (not only the
    /// marker-less ones), so the client counts from chain facts rather than the
    /// evictable result marker. Empty only when the row genuinely has no
    /// chain-counted win. Sorted by `pot_txid` so the wire body is a pure
    /// function of the data.
    pub chain_wins: Vec<ChainWinAnchor>,
    pub evidence: Vec<LeaderboardEvidence>,
}

/// One `hands[i]` row — a lowest-winning-hand entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardHandRow {
    pub game_id: String,
    pub score: u32,
    pub cards_hex: String,
    pub winner: String,
    /// The identity this hand was won AGAINST — the marker's `loser`. A hand
    /// is a game between two people; the board showed only one of them.
    /// Carried verbatim (lowercase hex) and, like `winner`, re-verifiable by
    /// the client against the marker's own signatures.
    pub loser: String,
    /// Always `true` for a hand row (only anchored + confirmed hands qualify).
    pub anchored: bool,
    /// The marker's overlay admission stamp — see [`LeaderboardEvidence::created_at`].
    /// Already computed here before this field existed: it was the hand list's
    /// score-tie break, carried in a side tuple and then thrown away.
    pub created_at: Option<i64>,
}

/// The assembled leaderboard, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leaderboard {
    pub board: Vec<LeaderboardBoardRow>,
    pub hands: Vec<LeaderboardHandRow>,
}

/// Is this outpoint status a COUNTED landing — the one basis on which a
/// leaderboard verdict may be derived (#323, widened by the 2026-08-06 owner
/// ruling)?
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
///
/// # The #371 arm (owner ruling, 2026-08-06: "we shouldn't have to wait for
/// confirm to see it on the leaderboard")
///
/// A spend the overlay ITSELF witnessed the network accept (`spender_seen`,
/// the `network_seen` latch — never a caller claim) whose bytes parse FINAL
/// (`spender_final`) is a miner-validated covenant spend: an invalid one
/// would never be relayed, so SEEN ∧ FINAL already proves a finished game.
/// The #332 lesson ("nothing evictable or unconfirmed in a counting path")
/// is preserved because the arm's inputs are the overlay's own broadcast
/// verdicts and a write-latched parse — an attacker-planted unconfirmed
/// pointer from the ungated `/submit` carries NEITHER. Accepted residual
/// (same as the display half's): a SEEN-never-mined settle counts until a
/// competing CONFIRMED spend displaces the pointer, at which point the count
/// self-corrects — and a different-verdict competitor requires a second
/// valid quorum signature set, inside the accountable trust model.
pub fn is_confirmed_landing(status: &OutpointStatus) -> bool {
    status.spent == Some(true)
        && (status.spent_confirmed == Some(true)
            || (status.spender_seen == Some(true) && status.spender_final == Some(true)))
}

/// Is a recorded spend a LANDING the money views (`/results`,
/// `/refund-view`, `/hops-view`) may derive from?
///
/// Three arms, each a different provenance of the same fact:
///
/// 1. the `spentConfirmed` flag (the overlay SPV-verified the spend's bump);
/// 2. a chaintracks-VERIFIED spender proof (`pot_beefs.proof_verified`) —
///    exists because the flag column was added by migration with default 0,
///    so a pre-existing row whose spend genuinely MINED can carry
///    `spentConfirmed = 0`; a parked tx that never mined can never acquire a
///    verified proof, so this widens toward CHAIN TRUTH, never away from it;
/// 3. **bsv-low #371, owner ruling 3 ("SEEN_ON_NETWORK is the finality
///    bar")**: the overlay ITSELF witnessed the network accept this spender
///    (`network_seen` — written only by the broadcast-gated arm's own
///    Arcade verdict or the ungated arm's Arcade corroboration, never from
///    a caller's claim) AND the spender's own bytes parse as FINAL
///    (`pot_records.spenderFinal`, latched at spend-record time). Both
///    conjuncts are load-bearing: without `spender_final`, a tower-parked
///    NON-FINAL refund that got network-witnessed would publish before its
///    height gate (#323's exact defect); without `spender_seen`, an
///    attacker-planted never-broadcast spend pointer from the ungated
///    public `/submit` would publish a fabricated verdict (epoch Rule 21's
///    worked-example defeat). `None` on either (pre-#371 rows) degrades to
///    the merkle arms — honest, self-draining.
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
/// The sibling [`is_confirmed_landing`] serves the leaderboard/classifier
/// pair from the batch-status read (no spender BEEF join, so no
/// `proof_verified` arm) — since the 2026-08-06 owner ruling it carries the
/// SAME seen∧final third arm, sourced from the batch read's own
/// `network_seen` join (see its doc for why #332 survives the widening).
pub fn is_confirmed_landing_with_proof(
    spent_confirmed: Option<bool>,
    spender_proof_verified: Option<bool>,
    spender_seen: Option<bool>,
    spender_final: Option<bool>,
) -> bool {
    spent_confirmed == Some(true)
        || spender_proof_verified == Some(true)
        || (spender_seen == Some(true) && spender_final == Some(true))
}

/// True iff the marker is anchored: its `potTxid:0` is recorded spent by the
/// named `settleTxid` in `pot_records` — the SAME anchor `/utxo-status`
/// reports. An unknown/unspent/differently-spent pot is NOT anchored
/// (fail-safe: never surface a marker as evidence against a spend the chain
/// doesn't back). Since #332 v2 this gates a marker's use as EVIDENCE/CARDS
/// only — the WIN is chain-derived and never depends on it — but the
/// confirmed-landing bar stays: a coop settle that never mined (displaced by
/// the tower-enforced settle that paid the OPPONENT) must not decorate a win
/// with a wrong-settle hand.
fn marker_anchored(
    m: &ResultMarkerRow,
    status_by_pot: &std::collections::HashMap<String, &OutpointStatus>,
) -> bool {
    match status_by_pot.get(&m.pot_txid.to_ascii_lowercase()) {
        Some(st) => {
            is_confirmed_landing(st)
                && st
                    .spending_txid
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(&m.settle_txid))
        }
        None => false,
    }
}

/// The CHAIN-SPINE leaderboard fold (#332 v3 — see the module note for the
/// full trust argument). `verdict_by_pot` maps a lowercase pot txid to the
/// classified covenant template its confirmed spend paid; `params_by_pot` maps
/// it to the pot's COMMITTED covenant params (the `pubA`/`pubB` settle keys
/// read from its funding lock); `attr_by_pot` maps it to the validity-filtered
/// `LOW/potparty/v2` seat → identity attribution.
///
/// A win is minted from CHAIN FACTS ONLY — verdict + committed winning key +
/// confirmed spend — never from any marker:
/// - a pot with no covenant verdict (bare/legacy), or `tie`/`refund`, is
///   UNRANKED — it counts for nobody;
/// - a `winner-A`/`winner-B` pot counts +1 for the committed WINNING SETTLE
///   KEY (`pubA`/`pubB`), whether or not any marker exists — UNEVICTABLE;
/// - the winner's IDENTITY is a display mapping from the validity-filtered
///   attribution; when present the win is keyed by the identity, else by the
///   settle key (`identity_is_key`) — never dropped, never mis-awarded;
/// - the winner's OWN verified, anchored result marker DECORATES the win:
///   `proven` iff it carries a verified loser countersig, the hands board from
///   its revealed cards, and the evidence row. Eviction costs only that.
// Eight inputs = the eight independent fact sources the fold joins (markers,
// statuses, proof pointers, hands cap, verdicts, attributions, params,
// signers); bundling them would only hide the join.
#[allow(clippy::too_many_arguments)]
pub fn aggregate_leaderboard_attributed(
    markers: &[ResultMarkerRow],
    statuses: &[OutpointStatus],
    proof_by_game_winner: &std::collections::HashMap<(String, String), Vec<String>>,
    hands_limit: usize,
    verdict_by_pot: &std::collections::HashMap<String, crate::results::PotVerdict>,
    attr_by_pot: &std::collections::HashMap<String, crate::results::SeatAttribution>,
    params_by_pot: &std::collections::HashMap<String, crate::results::CovenantParams>,
    signers_by_pot: &std::collections::HashMap<String, String>,
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
    // ── the CHAIN-COUNTING spine (#332 v3) ─────────────────────────────────
    // A win is minted from the VERDICT + the COMMITTED WINNING SETTLE KEY +
    // a confirmed landing — never from any marker. `counted` maps a pot to
    // its (owner_lc, identity_is_key): the owner is the winner's IDENTITY when
    // a validity-filtered attribution resolves it, else the committed settle
    // KEY itself (a stable on-chain id). Never dropped, never mis-awarded.
    use crate::results::PotVerdict;
    let mut counted: HashMap<String, (String, bool)> = HashMap::new();
    for (pot_lc, &verdict) in verdict_by_pot {
        // Confirmed landing WITH a recorded spender — re-checked here so the
        // spine never depends on the route's filter. The `spending_txid.is_some()`
        // limb makes the chain-win anchor invariant EXECUTABLE rather than a
        // comment (#336/#337 delta LOW-2): a counted win is emitted with a
        // `chainWins` anchor whose `settleTxid` is this spender, so a confirmed
        // landing that somehow carried no spender (never observed — `mark_spent`
        // always records one) must not be counted as a win the client then
        // cannot re-derive. `is_confirmed_landing` stays deliberately "flag only"
        // (its documented contract, shared with the classifier); the stricter
        // bar lives HERE, where the anchor is built. Pinned by
        // `confirmed_landing_without_spender_is_not_counted`.
        if !status_by_pot
            .get(pot_lc)
            .is_some_and(|s| is_confirmed_landing(s) && s.spending_txid.is_some())
        {
            continue;
        }
        // The winner's IDENTITY, from the VALIDITY-FILTERED attribution
        // (`winner_for` returns None for tie/refund AND for a junk potparty
        // flood that evicts the verified honest marker — the sigs don't
        // verify). When it resolves, the win is keyed by the identity.
        let identity = attr_by_pot
            .get(pot_lc)
            .and_then(|a| a.winner_for(verdict))
            .map(|s| s.to_ascii_lowercase());
        let owner = match identity {
            Some(id) => (id, false),
            None => {
                // FALLBACK: no identity resolved. The win still counts, under
                // the COMMITTED WINNING SETTLE KEY read from the pot's own
                // funding lock (chain truth, UNEVICTABLE). No params ⇒ no
                // covenant lock (a bare pot — never reaches here, it has no
                // verdict), and tie/refund attribute nobody.
                let Some(params) = params_by_pot.get(pot_lc) else {
                    continue;
                };
                match verdict {
                    PotVerdict::WinnerA => (hex::encode(params.pub_a), true),
                    PotVerdict::WinnerB => (hex::encode(params.pub_b), true),
                    PotVerdict::Tie | PotVerdict::Refund => continue,
                }
            }
        };
        counted.insert(pot_lc.clone(), owner);
    }

    // ── the chain-win ANCHORS (#336/#337), grouped by owner ─────────────────
    // Every chain-counted win gets its (potTxid, settleTxid) on its owner's
    // board row, so the client re-derives the win from `/beef` INDEPENDENT of
    // the evictable result marker. The settle txid is the pot's own confirmed
    // spender, which the `counted` gate above now REQUIRES to be present
    // (`spending_txid.is_some()`, LOW-2) — so this `let else` never `continue`s
    // in practice; it stays as a total-function fail-safe. Sorted per owner by
    // pot txid so the wire body is a pure function of the data.
    let mut chain_wins_by_owner: HashMap<String, Vec<ChainWinAnchor>> = HashMap::new();
    for (pot_lc, (owner_lc, _)) in &counted {
        let Some(settle) = status_by_pot
            .get(pot_lc)
            .and_then(|s| s.spending_txid.as_ref())
        else {
            continue;
        };
        chain_wins_by_owner
            .entry(owner_lc.clone())
            .or_default()
            .push(ChainWinAnchor {
                pot_txid: pot_lc.clone(),
                settle_txid: settle.to_ascii_lowercase(),
                settle_signers: signers_by_pot.get(pot_lc).cloned(),
            });
    }
    for anchors in chain_wins_by_owner.values_mut() {
        anchors.sort_by(|a, b| a.pot_txid.cmp(&b.pot_txid));
    }

    // ── marker DECORATION, bounded to the chain-counted set (MEDIUM-3) ──────
    // `verified_claim` (real ECDSA) runs ONLY on markers whose pot is
    // chain-counted for the claimed winner — at most (counted pots ×
    // rows-per-pot) verifications, not the whole hostile window. A marker
    // that verifies was AUTHORED by the winner (only the winner's key
    // signs), so it is a truthful decoration: the loser's countersignature
    // (`proven`), the winner's revealed cards (the hands board), and the
    // evidence row the client re-verifies. Its ABSENCE (eviction from the
    // window, or never published) costs only the decoration — never the win.
    let mut ev_by_identity: HashMap<String, Vec<usize>> = HashMap::new();
    // winner_lc → set of pots whose win carries a VERIFIED loser countersig.
    let mut proven_pots: HashMap<String, HashSet<String>> = HashMap::new();
    // pot_lc → (marker idx, createdAt) — the oldest verified carded winner
    // marker for the pot (its settle-time hand; deterministic).
    let mut hand_marker: HashMap<String, (usize, i64)> = HashMap::new();
    for (i, m) in markers.iter().enumerate() {
        let pot_lc = m.pot_txid.to_ascii_lowercase();
        let Some((owner_lc, is_key)) = counted.get(&pot_lc) else {
            continue; // pot is not a chain-counted win — no decoration
        };
        // A result marker names the winner IDENTITY. When the win is keyed by
        // the settle KEY (identity unknown), no result marker can match it —
        // so a key-keyed win carries no evidence, which is correct: we could
        // not prove the identity, so we never assert one via a marker.
        if *is_key || !m.winner.eq_ignore_ascii_case(owner_lc) {
            continue;
        }
        if !anchored[i] {
            continue; // its settleTxid must match the pot's recorded spend
        }
        let Some(fact) = crate::results::verified_claim(m) else {
            continue; // unverifiable winner sig ⇒ not the winner's own claim
        };
        ev_by_identity.entry(owner_lc.clone()).or_default().push(i);
        if fact.loser_sig_verified {
            proven_pots
                .entry(owner_lc.clone())
                .or_default()
                .insert(pot_lc.clone());
        }
        // The winner's own signed cards (bound by the verified sig) → hands.
        if fact.cards_hex.is_some()
            && leaderboard_cards_from_hex(&fact.cards_hex.unwrap()).is_some()
        {
            let at = m.created_at.unwrap_or(i64::MAX);
            hand_marker
                .entry(pot_lc.clone())
                .and_modify(|(bi, bat)| {
                    if at < *bat {
                        *bi = i;
                        *bat = at;
                    }
                })
                .or_insert((i, at));
        }
    }

    // Wins per owner (identity or settle key) — one per chain-counted pot.
    let mut wins_by_id: HashMap<String, u32> = HashMap::new();
    let mut is_key_by_owner: HashMap<String, bool> = HashMap::new();
    for (owner, is_key) in counted.values() {
        *wins_by_id.entry(owner.clone()).or_default() += 1;
        is_key_by_owner.insert(owner.clone(), *is_key);
    }

    let mut rows: Vec<LeaderboardBoardRow> = wins_by_id
        .iter()
        .map(|(id, &wins)| {
            let mut ev_idx = ev_by_identity.get(id).cloned().unwrap_or_default();
            // Confirmed (countersigned) first, then the rest; newest first
            // within a tier — a display-friendly drill-down, not a ranking.
            let confirmed_ev = |i: usize| {
                markers[i]
                    .loser_sig_hex
                    .as_deref()
                    .is_some_and(|s| !s.is_empty())
            };
            ev_idx.sort_by(|&a, &b| {
                confirmed_ev(b)
                    .cmp(&confirmed_ev(a))
                    .then(markers[b].created_at.cmp(&markers[a].created_at))
            });
            let evidence = ev_idx
                .iter()
                .map(|&i| {
                    let m = &markers[i];
                    let g = m.game_id.to_ascii_lowercase();
                    let w = m.winner.to_ascii_lowercase();
                    let pot = m.pot_txid.to_ascii_lowercase();
                    let proof_txids = proof_by_game_winner
                        .get(&(g.clone(), w.clone()))
                        .cloned()
                        .unwrap_or_default();
                    let verdict = verdict_by_pot.get(&pot).copied();
                    LeaderboardEvidence {
                        game_id: g,
                        winner: w,
                        loser: m.loser.to_ascii_lowercase(),
                        pot_txid: pot.clone(),
                        settle_txid: m.settle_txid.to_ascii_lowercase(),
                        winner_sig_hex: m.winner_sig_hex.to_ascii_lowercase(),
                        loser_sig_hex: m.loser_sig_hex.as_ref().map(|s| s.to_ascii_lowercase()),
                        cards_hex: m.cards_hex.as_ref().map(|s| s.to_ascii_lowercase()),
                        anchored: anchored[i],
                        proof_txids,
                        proof_posted: false,
                        server_verdict: verdict,
                        chain_attributed_winner: counted.get(&pot).map(|(o, _)| o.clone()),
                        settle_signers: signers_by_pot.get(&pot).cloned(),
                        claim_tier: m.claim_valid,
                        created_at: m.created_at,
                    }
                })
                .collect();
            let proven = proven_pots.get(id).is_some_and(|p| !p.is_empty());
            LeaderboardBoardRow {
                identity: id.clone(),
                wins,
                proven,
                // Every counted win is chain-attributed now, so chainProven
                // ⇔ wins > 0. `proven` (the loser's own countersignature)
                // stays a distinct, stricter fact.
                chain_proven: wins > 0,
                // #332 v3: true when `identity` is the committed SETTLE KEY
                // (the potparty identity mapping was unavailable) rather than a
                // resolved identity key — a display honesty bit, never a win
                // change. The win is real regardless.
                identity_is_key: *is_key_by_owner.get(id).unwrap_or(&false),
                // #336/#337: the chain-win anchors for this owner (sorted by
                // pot txid above) — the client's eviction-immune counting input.
                chain_wins: chain_wins_by_owner.get(id).cloned().unwrap_or_default(),
                evidence,
            }
        })
        .collect();
    // Rank: wins desc, then identity asc (lowercase hex byte order).
    rows.sort_by(|a, b| {
        b.wins
            .cmp(&a.wins)
            .then_with(|| a.identity.cmp(&b.identity))
    });
    let board = rows;

    // ── hands: the lowest-score winner hand of a CHAIN-COUNTED pot ──────────
    // One per counted pot (the winner's own signed cards), never marker-
    // grouped — so a marker flood can only DENY a hand (undercount), never
    // fabricate or steal one.
    // The score-tie break is the earliest claim — read off the row's own
    // `created_at` now that it carries one (it used to ride in a side tuple).
    let mut hands: Vec<LeaderboardHandRow> = Vec::new();
    for (pot_lc, &(i, _)) in &hand_marker {
        let m = &markers[i];
        // hand_marker is only populated for identity-keyed wins (the
        // decoration loop skips key-keyed pots), so the owner is the winner's
        // identity — the hand names it.
        let winner_lc = &counted[pot_lc].0;
        let cards = leaderboard_cards_from_hex(m.cards_hex.as_ref().unwrap()).unwrap();
        hands.push(LeaderboardHandRow {
            game_id: m.game_id.to_ascii_lowercase(),
            score: hand_score(&cards),
            cards_hex: m.cards_hex.as_ref().unwrap().to_ascii_lowercase(),
            winner: winner_lc.clone(),
            loser: m.loser.to_ascii_lowercase(),
            anchored: true,
            created_at: m.created_at,
        });
    }
    // Score ascending; tie → earliest createdAt (None sorts LAST, == the
    // client's `?? Infinity`); final tie → gameId asc. The gameId tiebreak
    // (LOW-3) makes the order a TOTAL function of the data, so it depends on
    // NOTHING outside this function — no coupling to any window ORDER BY.
    hands.sort_by(|a, b| {
        a.score
            .cmp(&b.score)
            .then_with(|| {
                a.created_at
                    .unwrap_or(i64::MAX)
                    .cmp(&b.created_at.unwrap_or(i64::MAX))
            })
            .then_with(|| a.game_id.cmp(&b.game_id))
    });
    let hands = hands.into_iter().take(hands_limit).collect();

    Leaderboard { board, hands }
}

// ═══════════════════════════════════════════════════════════════════════════
// #403 BOARD PAGING (2026-08-29) — the whole-era CHAIN-WINS SPINE
// ═══════════════════════════════════════════════════════════════════════════
//
// THE CLIFF. `/leaderboard` counted wins over a MARKER WINDOW of at most
// `LEADERBOARD_MAX_LIMIT` (500) distinct pots. The moment an era held more
// pots than the window, every page came back `truncated:true` and the
// client — correctly refusing a complete-looking partial board — fell back
// to the slow whole-history gather (layer 10 of the 2026-08-27 onion). The
// stopgap was the #375 era cutoff bumped forward on beta every time its
// window re-filled. The window was the wrong unit: it bounded the COUNTING
// SPINE by a display cap.
//
// THE FIX. Counting no longer walks markers at all. Every fact a win needs
// is LATCHED AT WRITE TIME by the overlay and sits in indexed columns:
//   - the verdict + its freshness (`pot_records.verdict`, `verdictTxid =
//     spendingTxid`, written by the spend classifier's CAS — no caller path),
//   - the committed winning settle key (`pubA`/`pubB`, `paramsDecoded`),
//   - the confirmed landing (`spent`/`spentConfirmed`, or the overlay's own
//     `network_seen` witness + `spenderFinal` — exactly `is_confirmed_landing`),
//   - the identity holding that key (`potparty_records.seatSettlePubkey` +
//     the admission-latched `sigValid = 1`, the brain-cutover M1 latch that
//     `attribute_seats` consults; a conflicting pair of verified identities
//     for one slot poisons it to the key, as `attribute_seats` does).
// So the spine is ONE aggregate over `pot_records` for the whole era,
// grouped by owner, ranked `wins DESC, owner ASC`, and PAGED by rank
// (`?limit` owners per page, `?after` rank offset, a `+1` probe for the
// honest `truncated` bit + `nextAfter`). The marker tables only DECORATE
// the owners on the page (evidence rows, the countersigned `proven` badge,
// the hands board) — bounded by the page, never by the history. Rows that
// predate the M1 latch (`sigValid IS NULL`) count under the settle key
// until the relatch sweep latches them (`identityIsKey`, the honest
// direction: a win is never dropped, never mis-awarded — see
// `attribute_seats`); the read path runs no ECDSA.
//
// The existing fold (`aggregate_leaderboard_attributed`) is UNCHANGED and
// still runs over the page's pots: verdict + committed key + confirmed
// landing mint the win, the winner's own verified marker decorates it. The
// SQL below is pinned against the fold's own predicates by the executing
// harness (`tests/leaderboard_paging_sqlite.rs`, production migrations).

/// Default owners per `/leaderboard` page.
pub const LEADERBOARD_PAGE_DEFAULT: usize = 50;
/// Hard cap on owners per page — bounds the per-request decoration joins
/// (each owner's counted pots → statuses / classification / markers /
/// proof pointers, all chunked at `D1_CHUNK_OUTPOINTS`).
pub const LEADERBOARD_PAGE_MAX: usize = 200;
/// The `?after` ceiling (same rationale as [`RECOVERY_VIEW_AFTER_MAX`]: a
/// walker whose next step re-clamps to the same page loops forever, so
/// `nextAfter` stops being emitted at the ceiling).
pub const LEADERBOARD_AFTER_MAX: usize = 1_000_000;

/// Clamp `?limit` to `1..=LEADERBOARD_PAGE_MAX` owners; absent ⇒ default.
/// NOTE: the pre-#403 client sends `limit=500` (the old distinct-POT
/// cap) — it clamps to `LEADERBOARD_PAGE_MAX` owners, a strictly larger
/// board than the old window could ever hold.
pub fn clamp_leaderboard_page(raw: Option<u32>) -> usize {
    match raw {
        Some(n) => (n as usize).clamp(1, LEADERBOARD_PAGE_MAX),
        None => LEADERBOARD_PAGE_DEFAULT,
    }
}

/// Clamp `?after` (rank offset) to `0..=LEADERBOARD_AFTER_MAX`.
pub fn clamp_leaderboard_after(raw: Option<u32>) -> usize {
    (raw.unwrap_or(0) as usize).min(LEADERBOARD_AFTER_MAX)
}

/// The cursor for the NEXT page, or `None` when this page is the last one
/// (not truncated) or the walk has hit the ceiling.
pub fn leaderboard_next_after(after: usize, page: usize, truncated: bool) -> Option<usize> {
    if truncated && after + page < LEADERBOARD_AFTER_MAX {
        Some(after + page)
    } else {
        None
    }
}

/// The shared CTE every paging query is built on — ONE derivation of "a
/// counted win and who owns it", so the owner page, the owners' pots and
/// the hands board can never disagree on the population.
///
/// `wins`: every era pot whose CONFIRMED-LANDED spend paid a winner
/// template, the verdict FRESH for the recorded spender, with decoded
/// committed keys; plus the latched identity holding each seat key
/// (`identityA`/`identityB`: exactly one distinct `sigValid = 1` identity
/// for that key on that pot, else NULL — the `attribute_seats` slot rule).
/// `owned`: the winning side resolved — `winnerIdentity` (NULL when the
/// lock is degenerate `pubA == pubB`, the slot is unlatched, or poisoned)
/// and `winKey` (the committed winning settle key); `identityA`/`identityB`
/// are the per-slot resolutions with the SAME degenerate-lock rule applied
/// (a `pubA == pubB` lock attributes nobody — `attribute_seats`), so the
/// fold's attribution input and the page's owner can never disagree.
///
/// `era_placeholder` is the bind slot for the #375 cutoff when set (the
/// caller numbers it; see each query). Filtered on the pot's own admission
/// stamp (`p.createdAt`), the server-written anchor.
pub fn chain_wins_cte(era_placeholder: &str, written_off_before_ms: Option<i64>) -> String {
    let era = era_filter_sql("p.createdAt", era_placeholder, written_off_before_ms);
    format!(
        "WITH wins AS ( \
           SELECT p.txid AS potTxid, lower(p.spendingTxid) AS settleTxid, \
                  p.verdict AS verdict, lower(p.pubA) AS pubA, lower(p.pubB) AS pubB, \
                  p.settleSigners AS settleSigners, p.createdAt AS potCreatedAt, \
                  (SELECT CASE WHEN COUNT(DISTINCT lower(q.identity)) = 1 \
                               THEN MIN(lower(q.identity)) END \
                     FROM potparty_records q \
                    WHERE q.potTxid = p.txid AND q.potVout = p.outputIndex \
                      AND q.sigValid = 1 \
                      AND lower(q.seatSettlePubkey) = lower(p.pubA)) AS slotA, \
                  (SELECT CASE WHEN COUNT(DISTINCT lower(q.identity)) = 1 \
                               THEN MIN(lower(q.identity)) END \
                     FROM potparty_records q \
                    WHERE q.potTxid = p.txid AND q.potVout = p.outputIndex \
                      AND q.sigValid = 1 \
                      AND lower(q.seatSettlePubkey) = lower(p.pubB)) AS slotB \
             FROM pot_records p \
             LEFT JOIN network_seen ns ON p.spendingTxid IS NOT NULL \
                  AND ns.txid = lower(p.spendingTxid) \
            WHERE p.outputIndex = {vout} \
              AND p.paramsDecoded = 1 \
              AND p.pubA IS NOT NULL AND p.pubB IS NOT NULL \
              AND p.verdict IN ('winner-a', 'winner-b') \
              AND p.spendingTxid IS NOT NULL \
              AND p.verdictTxid = p.spendingTxid \
              AND p.spent = 1 \
              AND (p.spentConfirmed = 1 OR (ns.txid IS NOT NULL AND p.spenderFinal = 1)){era} \
         ), \
         owned AS ( \
           SELECT w.potTxid, w.settleTxid, w.verdict, w.pubA, w.pubB, w.settleSigners, \
                  w.potCreatedAt, \
                  CASE WHEN w.pubA = w.pubB THEN NULL ELSE w.slotA END AS identityA, \
                  CASE WHEN w.pubA = w.pubB THEN NULL ELSE w.slotB END AS identityB, \
                  CASE WHEN w.pubA = w.pubB THEN NULL \
                       WHEN w.verdict = 'winner-a' THEN w.slotA \
                       ELSE w.slotB END AS winnerIdentity, \
                  CASE WHEN w.verdict = 'winner-a' THEN w.pubA ELSE w.pubB END AS winKey \
             FROM wins w \
         ) ",
        vout = LEADERBOARD_POT_VOUT,
    )
}

/// The OWNER PAGE: `(owner, identityIsKey, wins)` ranked `wins DESC, owner
/// ASC`. BINDS: `?1` = page + 1 (the truncation probe), `?2` = `after`
/// (rank offset), `?3` = the era cutoff (ms) iff configured.
pub fn chain_wins_spine_sql(written_off_before_ms: Option<i64>) -> String {
    format!(
        "{cte} \
         SELECT COALESCE(winnerIdentity, winKey) AS owner, \
                MAX(CASE WHEN winnerIdentity IS NULL THEN 1 ELSE 0 END) AS identityIsKey, \
                COUNT(*) AS wins \
           FROM owned \
          GROUP BY COALESCE(winnerIdentity, winKey) \
          ORDER BY wins DESC, owner ASC \
          LIMIT ?1 OFFSET ?2",
        cte = chain_wins_cte("?3", written_off_before_ms),
    )
}

/// The counted pots of `n` owners (the page): one row per win with the
/// facts the fold's inputs are built from. BINDS: `?1` = the era cutoff
/// iff configured (a no-op slot otherwise — bind it anyway, see
/// [`chain_wins_owner_binds_era_first`]), then the `n` owners as `?2..`.
pub fn chain_wins_owners_sql(n: usize, written_off_before_ms: Option<i64>) -> String {
    debug_assert!((1..=D1_CHUNK_OUTPOINTS).contains(&n));
    let owners = (0..n)
        .map(|i| format!("?{}", i + 2))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{cte} \
         SELECT COALESCE(winnerIdentity, winKey) AS owner, \
                CASE WHEN winnerIdentity IS NULL THEN 1 ELSE 0 END AS identityIsKey, \
                potTxid, settleTxid, verdict, pubA, pubB, settleSigners, \
                identityA, identityB \
           FROM owned \
          WHERE COALESCE(winnerIdentity, winKey) IN ({owners}) \
          ORDER BY owner ASC, potCreatedAt DESC, potTxid ASC",
        cte = chain_wins_cte("?1", written_off_before_ms),
    )
}

/// The owners query numbers the era slot `?1` unconditionally so the owner
/// binds are stable at `?2..`; when no cutoff is configured the slot is
/// unreferenced by the SQL and the bound value is ignored. `true` always —
/// exists so the route's bind order is a pinned fact, not a convention.
pub const fn chain_wins_owner_binds_era_first() -> bool {
    true
}

/// The DECORATION markers for `n` pots: each pot's oldest
/// `RESULT_ROWS_PER_POT` `result_markers_v2` rows (the same per-pot
/// superset rule the old window used — oldest-first because oldest is the
/// one order later spam cannot improve on; verification happens in the
/// fold, never in SQL). BINDS: the `n` pot txids as `?1..`.
pub fn pot_markers_sql(n: usize) -> String {
    debug_assert!((1..=D1_CHUNK_OUTPOINTS).contains(&n));
    let pots = (0..n)
        .map(|i| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, \
                cardsHex, txid, createdAt, claimValid \
           FROM (SELECT gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
                        loserSigHex, cardsHex, txid, createdAt, claimValid, \
                        ROW_NUMBER() OVER (PARTITION BY potTxid \
                                           ORDER BY createdAt ASC, rowid ASC) AS rn \
                   FROM result_markers_v2 \
                  WHERE potTxid IN ({pots})) \
          WHERE rn <= {per_pot} \
          ORDER BY potTxid ASC, rn ASC",
        per_pot = overlay_discovery::result::storage::RESULT_ROWS_PER_POT,
    )
}

/// The LOW hand score of a 10-hex-char `cardsHex` column, in pure SQL —
/// byte-identical to [`hand_score`] (pinned by the harness over every
/// card): rank = card % 13 (0='2' … 12='A'); A=1, J/Q/K=10, else rank+2.
/// Each card is also bounded `<= 51` (a malformed byte scores 0 here and
/// the fold, which re-decodes, drops the marker — a bad row can only lose
/// its own hand, never rank above an honest one: it would need a LOWER
/// score than honest cards, and a 0 contribution is exactly what a
/// too-high byte gets, so the `<= 51` guard keeps the order honest).
pub fn sql_hand_score_expr(col: &str) -> String {
    let nib =
        |pos: usize| format!("(instr('0123456789abcdef', substr(lower({col}), {pos}, 1)) - 1)");
    (1..=5)
        .map(|i| {
            let card = format!("({} * 16 + {})", nib(2 * i - 1), nib(2 * i));
            format!(
                "(CASE WHEN {card} > 51 THEN 0 \
                       WHEN ({card} % 13) = 12 THEN 1 \
                       WHEN ({card} % 13) >= 9 THEN 10 \
                       ELSE ({card} % 13) + 2 END)"
            )
        })
        .collect::<Vec<_>>()
        .join(" + ")
}

/// The ERA-WIDE LOWEST HANDS: the winner's own earliest card-bearing
/// verified claim (`claimValid IN (1, 2)`, anchored on the recorded settle)
/// of every counted pot whose winner resolved to an IDENTITY, ordered by
/// the SQL hand score (`sql_hand_score_expr`), then earliest claim, then
/// gameId — the fold's own total order. BINDS: `?1` = hands limit, `?2` =
/// the era cutoff iff configured.
pub fn era_hands_sql(written_off_before_ms: Option<i64>) -> String {
    format!(
        "{cte} \
         SELECT gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, \
                cardsHex, txid, createdAt, claimValid \
           FROM (SELECT m.gameId, m.winner, m.loser, m.potTxid, m.settleTxid, \
                        m.winnerSigHex, m.loserSigHex, m.cardsHex, m.txid, \
                        m.createdAt, m.claimValid, \
                        ROW_NUMBER() OVER (PARTITION BY m.potTxid \
                                           ORDER BY m.createdAt ASC, m.rowid ASC) AS rn \
                   FROM owned o \
                   JOIN result_markers_v2 m ON m.potTxid = o.potTxid \
                  WHERE o.winnerIdentity IS NOT NULL \
                    AND lower(m.winner) = o.winnerIdentity \
                    AND lower(m.settleTxid) = o.settleTxid \
                    AND m.claimValid IN (1, 2) \
                    AND m.cardsHex IS NOT NULL AND length(m.cardsHex) = 10) \
          WHERE rn = 1 \
          ORDER BY ({score}) ASC, createdAt ASC, gameId ASC \
          LIMIT ?1",
        cte = chain_wins_cte("?2", written_off_before_ms),
        score = sql_hand_score_expr("cardsHex"),
    )
}

/// One row of [`chain_wins_owners_sql`] — a counted win with its owner and
/// the latched seat identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainWinPotRow {
    pub owner: String,
    pub identity_is_key: bool,
    pub pot_txid: String,
    pub settle_txid: String,
    pub verdict: String,
    pub pub_a: String,
    pub pub_b: String,
    pub settle_signers: Option<String>,
    pub identity_a: Option<String>,
    pub identity_b: Option<String>,
}

/// The fold's `attr_by_pot` input, built from the SAME latched resolution
/// the spine grouped by — so the fold can never attribute a pot to an
/// identity the page ranked under a key (or vice versa).
pub fn attributions_from_pot_rows(
    rows: &[ChainWinPotRow],
) -> std::collections::HashMap<String, crate::results::SeatAttribution> {
    rows.iter()
        .map(|r| {
            (
                r.pot_txid.to_ascii_lowercase(),
                crate::results::SeatAttribution {
                    identity_a: r.identity_a.as_ref().map(|s| s.to_ascii_lowercase()),
                    identity_b: r.identity_b.as_ref().map(|s| s.to_ascii_lowercase()),
                },
            )
        })
        .collect()
}

/// Keep only the page's owners' rows (the fold also sees the hands-board
/// pots, whose owners may lie beyond the page) and order them by the
/// spine's rank: `wins DESC, owner ASC`.
pub fn retain_page_owners(lb: &mut Leaderboard, owners: &[String]) {
    let set: std::collections::HashSet<String> =
        owners.iter().map(|o| o.to_ascii_lowercase()).collect();
    lb.board.retain(|r| set.contains(&r.identity));
    lb.board.sort_by(|a, b| {
        b.wins
            .cmp(&a.wins)
            .then_with(|| a.identity.cmp(&b.identity))
    });
}

/// Assemble the `/leaderboard` wire body (the endpoint CONTRACT):
/// `{"board":[…],"hands":[…],"computedAt":<unix>,"resultCount":<int>,
/// "truncated":<bool>}`.
///
/// `truncated` (#332 / #335 item 2) is the honest incompleteness bit the
/// board previously lacked: `true` means the marker window held MORE
/// distinct pots than the request's `limit`, so the answer is a PAGE, not
/// the whole record — under a marker flood the pre-#332 body reported
/// `resultCount == limit` and the wrong answer looked complete. Same
/// contract as `/recovery-view`'s bit: additive, and a caller that ignores
/// it sees exactly the old shape.
///
/// `nextAfter` (#403, 2026-08-29): the rank cursor for the next page of
/// OWNERS, `null` when this page is the last (or the walk hit
/// `LEADERBOARD_AFTER_MAX`). Since #403 `truncated` means "more owners
/// beyond this page" — a walkable page, never a reason to gather the whole
/// history client-side.
pub fn leaderboard_body(
    lb: &Leaderboard,
    computed_at: i64,
    result_count: usize,
    truncated: bool,
    next_after: Option<usize>,
) -> String {
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
                        // #332 HIGH-1: the pointer SUPERSET (client filters by
                        // transcript validity). `proofTxid` (first, or null)
                        // stays for back-compat with a client that reads the
                        // singular field; `proofTxids` is the authoritative set.
                        "proofTxid": e.proof_txids.first(),
                        "proofTxids": e.proof_txids,
                        "proofPosted": e.proof_posted,
                        "serverVerdict": e.server_verdict.map(crate::results::PotVerdict::as_str),
                        // bsv-low #230: the identity attributed as this pot's
                        // winner via the verified seat-binding marker + chain
                        // verdict (null when unattributed) — the falsifiable
                        // fact behind the row's `chainProven` tier.
                        "chainAttributedWinner": e.chain_attributed_winner,
                        // bsv-low #406 (ADDITIVE): who signed the settle —
                        // 'coop' (both seats signed the payout itself) or
                        // 'tower-a'/'tower-b' (the enforced family), null =
                        // not established. Picks the client's ending
                        // narration; display-tier, nothing counts on it.
                        "settleSigners": e.settle_signers,
                        // Brain-cutover M2b (ADDITIVE): the admission-latched
                        // claim tier — 2 countersigned, 1 winner-sig-valid,
                        // 0 invalid, null = not yet swept (the client
                        // computes that row itself). Lets the client skip the
                        // per-row ECDSA it used to run on every render; the
                        // signature fields remain so falsification stays
                        // possible. An older client ignores the key.
                        "claimTier": e.claim_tier,
                        // The marker's overlay admission stamp (unix seconds,
                        // null when unrecorded) — DISPLAY ONLY. Nothing counts,
                        // ranks or verifies on it, so a garbled value costs a
                        // date and nothing else; the client parses it as
                        // optional and simply omits the date when it is absent.
                        "createdAt": e.created_at,
                    })
                })
                .collect();
            // #336/#337: the chain-win anchors — one `{potTxid, settleTxid}`
            // per counted pot, the client's eviction-immune counting input.
            let chain_wins: Vec<serde_json::Value> = r
                .chain_wins
                .iter()
                .map(|c| {
                    json!({
                        "potTxid": c.pot_txid,
                        "settleTxid": c.settle_txid,
                        // #406 (ADDITIVE): see the evidence row's field.
                        "settleSigners": c.settle_signers,
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
                // #332 v3: `identity` is the committed WINNING SETTLE KEY, not
                // a resolved player identity (the potparty mapping was
                // unavailable). The win is real; a viewer should render it
                // under the key, not attribute it to a player. Absent/false on
                // a normally-attributed row.
                "identityIsKey": r.identity_is_key,
                // #336/#337: the chain-win anchors (absent-safe: an older
                // client ignores the field; a newer one counts from it,
                // independent of the evictable result marker).
                "chainWins": chain_wins,
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
                // Who the hand was won against, and when — both display-only
                // (see the evidence row's `createdAt`). A hand row that names
                // only its winner cannot say who was across the table.
                "loser": h.loser,
                "anchored": h.anchored,
                "createdAt": h.created_at,
            })
        })
        .collect();
    json!({
        "board": board,
        "hands": hands,
        "computedAt": computed_at,
        "resultCount": result_count,
        "truncated": truncated,
        "nextAfter": next_after,
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
            // A network-SEEN spender (the #371 witness), via the D1 producer.
            OutpointStatus::known_with_witness(
                &op_a,
                true,
                Some("f0".repeat(32)),
                false,
                Some(true),  // spenderSeen — ARC SEEN_ON_NETWORK
                Some(false), // spenderFinal
            ),
            OutpointStatus::known(&op_a, false, None, false),
            OutpointStatus::unknown(&op_b),
        ];
        let v: serde_json::Value = serde_json::from_str(&utxo_status_body(&entries)).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        // Spent row: a network-SEEN spender that is not yet confirmed — the
        // monitor trusts it on `spenderSeen` alone (no confirmation wait).
        assert_eq!(arr[0]["txid"], txid_a());
        assert_eq!(arr[0]["vout"], 0);
        assert_eq!(arr[0]["known"], true);
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], "f0".repeat(32));
        assert_eq!(arr[0]["spentConfirmed"], false);
        assert_eq!(
            arr[0]["spenderSeen"], true,
            "the #371 network-seen witness reaches the wire"
        );
        assert_eq!(arr[0]["spenderFinal"], false);
        // Known-unspent row: no witness (produced by `known`, not the D1 path).
        assert_eq!(arr[1]["known"], true);
        assert_eq!(arr[1]["spent"], false);
        assert!(arr[1]["spendingTxid"].is_null());
        assert_eq!(arr[1]["spentConfirmed"], false);
        assert!(
            arr[1]["spenderSeen"].is_null(),
            "a non-D1 producer leaves the witness null — the strict-confirmed fallback, never a positive"
        );
        // Unknown row: fail-safe nulls, never asserted unspent.
        assert_eq!(arr[2]["txid"], txid_b());
        assert_eq!(arr[2]["vout"], 1);
        assert_eq!(arr[2]["known"], false);
        assert!(arr[2]["spent"].is_null());
        assert!(arr[2]["spendingTxid"].is_null());
        assert!(arr[2]["spentConfirmed"].is_null());
        assert!(arr[2]["spenderSeen"].is_null());
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
            "SELECT p.txid, p.outputIndex, p.spent, p.spendingTxid, p.spentConfirmed, \
                p.spenderFinal, ns.txid IS NOT NULL AS spenderSeen \
         FROM pot_records p \
         LEFT JOIN network_seen ns ON p.spendingTxid IS NOT NULL \
              AND ns.txid = lower(p.spendingTxid) \
         WHERE (p.txid = ? AND p.outputIndex = ?)"
        );
        let three = batch_where_sql(3);
        assert_eq!(
            three.matches("(p.txid = ? AND p.outputIndex = ?)").count(),
            3
        );
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
                spender_final: None,
                spender_seen: None,
                txid: txid_a(),
                vout: 2,
                spent: false,
                spending_txid: None,
                spent_confirmed: false,
            },
            PotRecordRow {
                spender_final: None,
                spender_seen: None,
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
            spender_final: None,
            spender_seen: None,
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
        let one = pots_view_join_sql(1, None);
        assert!(one.contains("LEFT JOIN pot_beefs b ON b.txid = lower(p.spendingTxid)"));
        assert!(one.contains("hex(b.beef) AS spenderBeef"));
        assert_eq!(one.matches("(p.txid = ? AND p.outputIndex = ?)").count(), 1);
        let three = pots_view_join_sql(3, None);
        assert_eq!(
            three.matches("(p.txid = ? AND p.outputIndex = ?)").count(),
            3
        );
        assert_eq!(three.matches(" OR ").count(), 2);
    }

    /// #375 — the era filter on `/pots-view`: the shared fragment appears
    /// EXACTLY ONCE (anchored on `pot_records.createdAt`, the pot's own
    /// admission stamp), the OR list is parenthesized so the conjunct binds
    /// the whole batch, and STRIPPING the fragment (plus the added parens)
    /// yields the `None` arm byte-for-byte — the None-is-byte-identical
    /// property, pinned positively rather than by a bare negative substring.
    #[test]
    fn pots_view_sql_era_filter_shape_and_none_identity() {
        let cutoff = Some(1_754_500_000_000i64);
        let frag = era_filter_sql("p.createdAt", "?", cutoff);
        for n in [1usize, 3] {
            let with = pots_view_join_sql(n, cutoff);
            let without = pots_view_join_sql(n, None);
            assert_eq!(with.matches(&frag).count(), 1, "exactly one era fragment");
            // The fragment sits AFTER the parenthesized outpoint OR-list.
            let clause = vec!["(p.txid = ? AND p.outputIndex = ?)"; n].join(" OR ");
            assert_eq!(
                with.matches(&format!("WHERE ({clause}){frag}")).count(),
                1,
                "the era conjunct applies to the WHOLE outpoint batch"
            );
            // Removing the era decoration restores the None arm exactly.
            assert_eq!(
                with.replace(&format!("({clause}){frag}"), &clause),
                without,
                "None must stay byte-identical to the pre-#375 query"
            );
        }
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
                    spender_final: None,
                    spender_seen: None,
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
                    spender_final: None,
                    spender_seen: None,
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
                    spender_final: None,
                    spender_seen: None,
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
                spender_final: None,
                spender_seen: None,
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

    /// `/epoch` (bsv-low THE ORDER item 2, + #375): the wire shape is
    /// EXACTLY `{"storageEpoch": <string|null>,
    /// "writtenOffBeforeMs": <number|null>,
    /// "writtenOffBeforeHeight": <number|null>}`, and the fail-safe
    /// normalization — unset/empty/whitespace-only var → `null` (the
    /// client's "no wipe directive") — never invents a directive.
    #[test]
    fn epoch_body_shape_and_failsafe_normalization() {
        // A set var serves the trimmed string.
        let set = normalize_storage_epoch(Some("  2026-08-06-zero-world-1 ".into()));
        assert_eq!(set.as_deref(), Some("2026-08-06-zero-world-1"));
        let v: serde_json::Value = serde_json::from_str(&epoch_body(
            set.as_deref(),
            Some(1_754_500_000_000),
            Some(961_000),
        ))
        .unwrap();
        assert_eq!(v["storageEpoch"], "2026-08-06-zero-world-1");
        assert_eq!(v["writtenOffBeforeMs"], 1_754_500_000_000i64);
        assert_eq!(v["writtenOffBeforeHeight"], 961_000i64);
        assert_eq!(v.as_object().unwrap().len(), 3); // exactly the three fields

        // Unset / empty / whitespace-only all serve LITERAL null.
        for raw in [None, Some(String::new()), Some("   ".to_string())] {
            let none = normalize_storage_epoch(raw);
            assert_eq!(none, None);
            let n: serde_json::Value =
                serde_json::from_str(&epoch_body(none.as_deref(), None, None)).unwrap();
            assert!(n["storageEpoch"].is_null());
            assert!(n.as_object().unwrap().contains_key("storageEpoch"));
            // #375: the write-off field is PRESENT and literal null when
            // unset — the client's fail-safe "no write-off" shape.
            assert!(n["writtenOffBeforeMs"].is_null());
            assert!(n.as_object().unwrap().contains_key("writtenOffBeforeMs"));
        }
    }

    /// #375 — `WRITTEN_OFF_BEFORE_MS` normalization: only a positive
    /// integer becomes a cutoff; every malformed shape is INERT (`None`),
    /// so a bad var can only serve MORE history, never widen the write-off.
    #[test]
    fn written_off_before_ms_normalization_edge_cases() {
        // Valid: a positive ms instant, whitespace tolerated.
        assert_eq!(
            normalize_written_off_before_ms(Some("1754500000000".into())),
            Some(1_754_500_000_000)
        );
        assert_eq!(
            normalize_written_off_before_ms(Some("  1754500000000  ".into())),
            Some(1_754_500_000_000)
        );
        // Inert: unset, empty, whitespace, junk, non-integer, zero, negative,
        // overflow.
        for raw in [
            None,
            Some(String::new()),
            Some("   ".to_string()),
            Some("not-a-number".to_string()),
            Some("1754500000000junk".to_string()),
            Some("17545.5".to_string()),
            Some("0".to_string()),
            Some("-1".to_string()),
            Some("-1754500000000".to_string()),
            Some("99999999999999999999999999".to_string()), // > i64::MAX
        ] {
            assert_eq!(
                normalize_written_off_before_ms(raw.clone()),
                None,
                "{raw:?} must normalize to the inert None"
            );
        }
    }

    /// #375 review MED-2 — the future-cutoff belt: a cutoff at/after `now`
    /// is a misconfiguration and reads as ABSENT; strictly-past passes
    /// through untouched; `None` stays `None`. The boundary is `<` (a
    /// cutoff EQUAL to now is refused — "pre-dates launch" means the past).
    #[test]
    fn clamp_future_cutoff_refuses_at_or_after_now() {
        const NOW: i64 = 1_754_500_000_000;
        assert_eq!(clamp_future_cutoff(Some(NOW - 1), NOW), Some(NOW - 1));
        assert_eq!(clamp_future_cutoff(Some(NOW), NOW), None);
        assert_eq!(clamp_future_cutoff(Some(NOW + 1), NOW), None);
        // The extra-digit (microseconds-looking) paste is a FUTURE instant.
        assert_eq!(clamp_future_cutoff(Some(NOW * 10), NOW), None);
        assert_eq!(clamp_future_cutoff(None, NOW), None);
    }

    /// #375 — the ONE era predicate: `None` emits NOTHING (the byte-identity
    /// arm every view pins), `Some` emits exactly one fragment with exactly
    /// one bind placeholder and the seconds→ms conversion (`* 1000`) spelled
    /// exactly once.
    #[test]
    fn era_filter_sql_fragment_shape() {
        assert_eq!(era_filter_sql("p.createdAt", "?", None), "");
        let frag = era_filter_sql("p.createdAt", "?", Some(1_754_500_000_000));
        assert_eq!(frag, " AND (p.createdAt * 1000 >= ?)");
        // Exactly one placeholder / one conversion (positive counts).
        assert_eq!(frag.matches('?').count(), 1);
        assert_eq!(frag.matches("* 1000").count(), 1);
        // Numbered-bind spelling for the ?N queries.
        let numbered = era_filter_sql("COALESCE(a, b)", "?4", Some(1));
        assert_eq!(numbered, " AND (COALESCE(a, b) * 1000 >= ?4)");
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
            committed_keys: None,
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
        let body: serde_json::Value = serde_json::from_str(&recovery_view_body(
            &apply_recovery_extras(out, None, None),
            Some(958_800),
            false,
            0,
        ))
        .unwrap();
        assert!(
            body["entries"][0]["recoveryHeight"].is_number(),
            "recoveryHeight must be a NUMBER on the wire: {body}"
        );
    }

    /// The SQL must FETCH the covenant height, else the preference above is
    /// inoperative in production (producer-level check).
    #[test]
    fn recovery_view_sql_fetches_the_covenant_height() {
        let sql = recovery_view_sql(None, 0);
        assert!(
            sql.contains("r.recoveryHeight AS covRecoveryHeight"),
            "covenant height must be SELECTed from pot_records: {sql}"
        );
        assert!(sql.contains("w.covRecoveryHeight AS covRecoveryHeight"));
    }

    /// #375 — the era filter on `/recovery-view`: exactly one shared
    /// fragment, at the INNERMOST scan (beside the identity bind, before
    /// the dedupe/quota windows), anchored `COALESCE(r.createdAt,
    /// pp.createdAt)`; stripping it restores the `None` arm byte-for-byte.
    #[test]
    fn recovery_view_sql_era_filter_shape_and_none_identity() {
        let cutoff = Some(1_754_500_000_000i64);
        // 2026-08-29 party-candidates: identity is `?1` (reused by the
        // subquery's two arms), the cutoff `?2` — still [identity, era].
        let frag = era_filter_sql("COALESCE(r.createdAt, pp.createdAt)", "?2", cutoff);
        let with = recovery_view_sql(cutoff, 0);
        let without = recovery_view_sql(None, 0);
        assert_eq!(with.matches(&frag).count(), 1, "exactly one era fragment");
        assert_eq!(
            with.matches(&format!("WHERE pp.identity = ?1{frag})"))
                .count(),
            1,
            "the era filter rides the innermost identity scan"
        );
        assert_eq!(
            with.replace(&frag, ""),
            without,
            "None must stay byte-identical to the pre-#375 query"
        );
    }

    /// #375 — the era filter on the `/leaderboard` marker window (the spine
    /// the whole board counts from): exactly one shared fragment, as bind
    /// `?4`, at the `rn <=` level where the per-pot anchor columns are
    /// constant per pot (a written-off pot drops atomically, before the
    /// tier/quota/DENSE_RANK windows); stripping it restores the `None`
    /// arm byte-for-byte.
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
        assert!(is_confirmed_landing_with_proof(
            Some(true),
            None,
            None,
            None
        ));
        // A chaintracks-VERIFIED spender proof alone (the migrated-row case).
        assert!(is_confirmed_landing_with_proof(
            Some(false),
            Some(true),
            None,
            None
        ));
        assert!(is_confirmed_landing_with_proof(
            None,
            Some(true),
            None,
            None
        ));
        // A PARKED intent: no signal at all.
        assert!(!is_confirmed_landing_with_proof(
            Some(false),
            None,
            None,
            None
        ));
        assert!(!is_confirmed_landing_with_proof(None, None, None, None));
        // An UNVERIFIED latch is not a signal (never a guess).
        assert!(!is_confirmed_landing_with_proof(
            Some(false),
            Some(false),
            None,
            None
        ));
        // ── The #371 third arm: SEEN && FINAL, and nothing weaker. ──
        // The overlay's own witness + final bytes: the ruling-3 bar.
        assert!(is_confirmed_landing_with_proof(
            Some(false),
            None,
            Some(true),
            Some(true)
        ));
        // SEEN alone is NOT enough: a network-witnessed but NON-FINAL
        // spender is a tower-parked refund before its height — publishing
        // it is #323's exact defect.
        assert!(!is_confirmed_landing_with_proof(
            None,
            None,
            Some(true),
            Some(false)
        ));
        assert!(!is_confirmed_landing_with_proof(
            None,
            None,
            Some(true),
            None
        ));
        // FINAL alone is NOT enough: a final-looking spend pointer with no
        // network witness is exactly what a stranger can plant through the
        // ungated public /submit (epoch Rule 21) — it stays behind the
        // merkle bar.
        assert!(!is_confirmed_landing_with_proof(
            None,
            None,
            None,
            Some(true)
        ));
        assert!(!is_confirmed_landing_with_proof(
            None,
            None,
            Some(false),
            Some(true)
        ));
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
            Some(true),
            None,
            None
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

        // ── The #371 arm (owner ruling 2026-08-06): SEEN ∧ FINAL counts an
        // UNCONFIRMED spend; neither half alone does; an unspent row never
        // does regardless of witness.
        let with = |spent: bool, seen: Option<bool>, fin: Option<bool>| {
            let mut s = mk(spent, false);
            s.spender_seen = seen;
            s.spender_final = fin;
            s
        };
        assert!(
            is_confirmed_landing(&with(true, Some(true), Some(true))),
            "a network-witnessed FINAL settle counts at the SEEN bar"
        );
        assert!(
            !is_confirmed_landing(&with(true, None, Some(true))),
            "final bytes with no overlay witness: the Rule-21 plant stays out"
        );
        assert!(
            !is_confirmed_landing(&with(true, Some(false), Some(true))),
            "an explicit no-witness is not a witness"
        );
        assert!(
            !is_confirmed_landing(&with(true, Some(true), Some(false))),
            "a witnessed NON-FINAL spender is a parked refund — #323 verbatim"
        );
        assert!(
            !is_confirmed_landing(&with(true, Some(true), None)),
            "an unlatched finality is not finality"
        );
        assert!(!is_confirmed_landing(&with(false, Some(true), Some(true))));
    }

    #[test]
    fn recovery_view_sql_shape() {
        let sql = recovery_view_sql(None, 0);
        // JOINs the pot outpoint for spend status; the BEEF join now sits
        // OUTSIDE the window, on survivors only, so a marker flood can never
        // drag real BLOBs along with it (#323 HIGH-1).
        assert!(sql.contains(
            "LEFT JOIN pot_records r \
                  ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout"
        ));
        assert!(sql.contains("LEFT JOIN pot_beefs b ON b.txid = lower(w.spendingTxid)"));
        assert!(sql.contains("hex(b.beef) AS spenderBeef"));
        // Keyed by ONE identity — since 2026-08-29 through the PARTY-CANDIDATES
        // subquery (party rows UNION hop-proven rows), whose two arms reuse
        // the numbered identity bind: one BIND, three `?1` placeholders.
        assert!(sql.contains("WHERE pp.identity = ?1"));
        assert!(
            sql.contains("FROM hopparty_records hp"),
            "the hop-proven arm is present"
        );
        assert_eq!(sql.matches("?1").count(), 3, "identity bind reused thrice");
        assert!(
            !sql.contains("?2"),
            "no cutoff placeholder without a cutoff"
        );

        // #323 HIGH-1 — the three anti-flood properties, asserted
        // individually so losing any ONE of them fails loudly. A marker
        // flood on an attacker-writable identity must not be able to evict
        // the caller's real pots.
        //
        // 1. dedupe in SQL, VERIFIED-then-oldest marker per pot (bsv-low
        //    #283: `createdAt ASC` alone was an out-stampable ordering — one
        //    earlier junk row owned the pot's row. The admission-time
        //    `sigValid` latch leads it now, and one pot still takes one slot).
        assert!(
            sql.contains(&format!(
                "ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout \
                                     ORDER BY {rank} DESC, \
                                              pp.createdAt ASC, pp.markerRowid ASC) AS rn",
                rank = overlay_discovery::potparty::validity::sig_rank_expr("pp.")
            )),
            "per-pot dedupe window missing: {sql}"
        );
        assert!(sql.contains("WHERE rn = 1"), "dedupe filter missing: {sql}");
        // 2. rank by the POT's own admission stamp — an attacker cannot
        //    backdate or advance `pot_records.createdAt` by filing markers.
        //    (A BACKSTOP since #283: a `pot_records` row is free to fabricate,
        //    bsv-low#347, so the latch rank leads this too.)
        assert!(
            sql.contains(
                "ORDER BY potBestSigRank DESC, \
                 COALESCE(potCreatedAt, markerCreatedAt) DESC"
            ),
            "pot-stamp ranking missing (behind the #283 latch rank): {sql}"
        );
        assert!(
            sql.contains("ORDER BY potBestSigRank DESC, tier ASC"),
            "#283: the latch rank must lead the tier ordering: {sql}"
        );
        // 3. a reserved quota for unknown pots, so ghost rows occupy a
        //    bounded slice instead of the whole page — and since #283 a
        //    provably-forged marker cannot occupy one at all.
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
                committed_keys: None,
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
                committed_keys: None,
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
                committed_keys: None,
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
            committed_keys: None,
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
            committed_keys: None,
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
                committed_keys: None,
                collected: None,
                outcome: None,
                outcome_source: None,
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
                committed_keys: None,
                collected: None,
                outcome: None,
                outcome_source: None,
            },
        ];
        let v: serde_json::Value = serde_json::from_str(&recovery_view_body(
            &apply_recovery_extras(entries.clone(), None, None),
            Some(958_800),
            false,
            0,
        ))
        .unwrap();
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
        let v2: serde_json::Value = serde_json::from_str(&recovery_view_body(
            &apply_recovery_extras(entries, None, None),
            None,
            false,
            0,
        ))
        .unwrap();
        assert!(v2["tip"].is_null());
        assert_eq!(v2["entries"].as_array().unwrap().len(), 2);
        // An empty result (invalid/empty identity) is a well-formed body.
        let v3: serde_json::Value = serde_json::from_str(&recovery_view_body(
            &apply_recovery_extras(Vec::new(), None, None),
            None,
            false,
            0,
        ))
        .unwrap();
        assert!(v3["tip"].is_null());
        assert_eq!(v3["entries"].as_array().unwrap().len(), 0);
    }

    // ── /leaderboard aggregation (bsv-low #38) ─────────────────────────────

    use std::collections::HashMap;

    /// 64-hex txid / gameId from a byte.
    fn tx(b: u8) -> String {
        format!("{b:02x}").repeat(32)
    }

    /// Deterministic test wallet per seed (the same test-key crypto the
    /// `results` tests use — a pinned root private key).
    fn seed_wallet(seed: u8) -> bsv_rs::wallet::ProtoWallet {
        let key = bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{seed:064x}")).unwrap();
        bsv_rs::wallet::ProtoWallet::new(Some(key))
    }

    /// seed → identity and identity → seed maps for every nonzero byte seed,
    /// computed once. #332: `ident()` used to mint arbitrary pubkey-SHAPED
    /// strings; counting now VERIFIES marker signatures, so fixture
    /// identities must be REAL keys the fixture can sign under, and `mk`
    /// must be able to find the wallet behind an identity it is handed.
    fn test_identities() -> &'static (HashMap<u8, String>, HashMap<String, u8>) {
        static CACHE: std::sync::OnceLock<(HashMap<u8, String>, HashMap<String, u8>)> =
            std::sync::OnceLock::new();
        CACHE.get_or_init(|| {
            let mut fwd = HashMap::new();
            let mut rev = HashMap::new();
            for b in 1u8..=255 {
                let id = seed_wallet(b).identity_key_hex().to_ascii_lowercase();
                rev.insert(id.clone(), b);
                fwd.insert(b, id);
            }
            (fwd, rev)
        })
    }

    /// 66-hex compressed identity pubkey of the seed wallet (REAL key —
    /// see [`test_identities`]).
    fn ident(b: u8) -> String {
        test_identities().0[&b].clone()
    }

    /// Sign the canonical result challenge as the CLIENT does (`result.ts`:
    /// counterparty 'anyone', protocol `[1,'low result']`, keyID = gameId),
    /// returning DER hex — the real producer recipe, via the exported
    /// `results::result_challenge_bytes` / `results::result_protocol` so the
    /// fixture cannot drift from the verifier by convention.
    fn sign_result_as(seed: u8, game_id_lc: &str, challenge: &[u8]) -> String {
        let sig = seed_wallet(seed)
            .create_signature(bsv_rs::wallet::CreateSignatureArgs {
                data: Some(challenge.to_vec()),
                hash_to_directly_sign: None,
                protocol_id: crate::results::result_protocol(),
                key_id: game_id_lc.to_string(),
                counterparty: Some(bsv_rs::wallet::Counterparty::Anyone),
            })
            .unwrap();
        hex::encode(sig.signature)
    }

    /// A REAL-SIGNED result marker (#332): the winner signature always
    /// verifies under `winner`; `confirmed` ⇒ a VERIFYING loser countersig is
    /// present. `winner`/`loser` must be [`ident`] identities (the fixture
    /// signs with the wallets behind them). `cards` is a 10-hex v2 cards push
    /// or None (v1). The marker txid is derived from game+seq so distinct
    /// markers for the same (game, winner) are distinct outpoints (the
    /// censorship-fix shape).
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
        let rev = &test_identities().1;
        let (w_lc, l_lc) = (winner.to_ascii_lowercase(), loser.to_ascii_lowercase());
        let game_lc = tx(game);
        let challenge = crate::results::result_challenge_bytes(
            &game_lc,
            &w_lc,
            &l_lc,
            &tx(pot),
            &tx(settle),
            cards,
        )
        .expect("fixture challenge must build");
        ResultMarkerRow {
            game_id: game_lc.clone(),
            winner: winner.to_string(),
            loser: loser.to_string(),
            pot_txid: tx(pot),
            settle_txid: tx(settle),
            winner_sig_hex: sign_result_as(rev[&w_lc], &game_lc, &challenge),
            loser_sig_hex: confirmed.then(|| sign_result_as(rev[&l_lc], &game_lc, &challenge)),
            cards_hex: cards.map(str::to_string),
            txid: format!("{game:02x}{seq:02x}").repeat(16),
            created_at: Some(created),
            claim_valid: None, // legacy tier — exercises the compute arm
        }
    }

    /// A FORGED marker (#332 attack fixture): same shape as [`mk`] but both
    /// signature pushes are plausibly-DER-shaped JUNK — exactly the bytes the
    /// pre-#332 presence gate counted as "confirmed". `winner`/`loser` can be
    /// ANY strings (no wallet needed — that is the attack's whole point).
    #[allow(clippy::too_many_arguments)]
    fn mk_forged(
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
            claim_valid: None, // legacy tier — the compute arm must refute it
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
                        spender_final: None,
                        spender_seen: None,
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
    /// public leaderboard: `is_confirmed_landing` gates the chain spine, so a
    /// coop settle that never mined (displaced by the tower-enforced settle
    /// that paid the OPPONENT) publishes no win even with a full attribution.
    #[test]
    fn an_unconfirmed_settle_never_counts_on_the_leaderboard() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0)];
        let spent = HashMap::from([(1u8, 2u8)]);
        let world = win_world(&[(1, &a, &b)]);

        // CONTROL: confirmed ⇒ it counts (so the test discriminates).
        let lb = agg(
            &markers,
            &statuses_for_confirmed(&markers, &spent, true),
            &no_proofs(),
            &world,
        );
        assert_eq!(lb.hands.len(), 1, "a CONFIRMED settle counts");
        assert!(lb.hands[0].anchored);
        assert!(
            lb.board.iter().any(|r| r.wins > 0),
            "the confirmed win is on the board"
        );

        // THE DEFECT: same rows + attribution, spend recorded but NOT
        // confirmed. The spine requires a confirmed landing, so nothing
        // counts however strong the attribution.
        let lb = agg(
            &markers,
            &statuses_for_confirmed(&markers, &spent, false),
            &no_proofs(),
            &world,
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
                        spender_final: None,
                        spender_seen: None,
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

    fn no_proofs() -> HashMap<(String, String), Vec<String>> {
        HashMap::new()
    }

    /// Empty committed-params map — for cells whose win counts under the
    /// resolved IDENTITY (the #332 v3 spine reads params only for the
    /// settle-key fallback when no attribution resolves).
    fn no_params() -> HashMap<String, crate::results::CovenantParams> {
        HashMap::new()
    }

    /// A committed CovenantParams with the two seat settle keys set to the
    /// given pubkey hexes (66-hex); everything else is filler. #332 v3 counts
    /// the win from these committed keys, so a behavioural test must supply
    /// them.
    fn params_with(pub_a_hex: &str, pub_b_hex: &str) -> crate::results::CovenantParams {
        let mut a = [0u8; 33];
        let mut b = [0u8; 33];
        a.copy_from_slice(&hex::decode(pub_a_hex).unwrap());
        b.copy_from_slice(&hex::decode(pub_b_hex).unwrap());
        crate::results::CovenantParams {
            pub_a: a,
            pub_b: b,
            pub_tower: [2u8; 33],
            pay_pkh_a: [0u8; 20],
            pay_pkh_b: [0u8; 20],
            rake_pkh: [0u8; 20],
            stake_a: 500,
            stake_b: 500,
            fee_sats: 8,
            recovery_height: 1,
        }
    }

    type World = (
        HashMap<String, crate::results::PotVerdict>,
        HashMap<String, crate::results::SeatAttribution>,
        HashMap<String, crate::results::CovenantParams>,
    );

    /// The chain facts that make each `(pot, winner, loser)` a COUNTED win
    /// under the #332 v3 spine: a `WinnerA` covenant verdict, committed params
    /// whose seat-A key is a real key the `winner` identity holds (so the win
    /// counts under the identity, not the settle key), and a verified
    /// attribution naming `winner` in seat A. The seat-A committed key is
    /// derived from the winner seed so it is a real pubkey.
    fn win_world(entries: &[(u8, &str, &str)]) -> World {
        let mut v = HashMap::new();
        let mut a = HashMap::new();
        let mut p = HashMap::new();
        let rev = &test_identities().1;
        for (pot, winner, loser) in entries {
            v.insert(tx(*pot), crate::results::PotVerdict::WinnerA);
            a.insert(
                tx(*pot),
                crate::results::SeatAttribution {
                    identity_a: Some(winner.to_ascii_lowercase()),
                    identity_b: Some(loser.to_ascii_lowercase()),
                },
            );
            // The committed seat-A key: a distinct real key (the winner's
            // SETTLE key would be a BRC-42 derivation in production; here any
            // real pubkey suffices — the identity path doesn't read it).
            let seed_a = *rev.get(&winner.to_ascii_lowercase()).unwrap_or(&0xa0);
            let seed_b = *rev.get(&loser.to_ascii_lowercase()).unwrap_or(&0xb0);
            p.insert(tx(*pot), params_with(&ident(seed_a), &ident(seed_b)));
        }
        (v, a, p)
    }

    /// [`aggregate_leaderboard_attributed`] with an explicit chain world — the
    /// behavioural-test entry point since the spine became chain-driven.
    fn agg(
        markers: &[ResultMarkerRow],
        statuses: &[OutpointStatus],
        proofs: &HashMap<(String, String), Vec<String>>,
        world: &World,
    ) -> Leaderboard {
        aggregate_leaderboard_attributed(
            markers,
            statuses,
            proofs,
            200,
            &world.0,
            &world.1,
            &world.2,
            &std::collections::HashMap::new(),
        )
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
    fn counts_a_chain_attributed_win_and_decorates_it() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &a, &b)]),
        );
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].identity, a);
        assert_eq!(lb.board[0].wins, 1);
        assert!(lb.board[0].chain_proven);
        assert!(lb.board[0].proven, "the countersigned marker decorates it");
        assert_eq!(lb.board[0].evidence.len(), 1);
        let ev = &lb.board[0].evidence[0];
        assert!(ev.anchored);
        assert_eq!(ev.winner, a);
        assert_eq!(ev.loser, b);
        assert!(ev.loser_sig_hex.is_some());
    }

    #[test]
    fn unattributed_pot_is_unranked_however_it_is_signed() {
        // A fully countersigned, anchored, confirmed marker on a pot with NO
        // chain attribution counts NOTHING (#332 v2 — signatures prove
        // authorship, not authority over the pot). This is the property the
        // from-scratch gate showed the interim design lacked.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        // E7b: the non-attributed aggregator is gone — an EMPTY attribution
        // world through the live spine says the same thing.
        let lb = agg(&markers, &statuses, &no_proofs(), &World::default());
        assert!(
            lb.board.is_empty(),
            "no attribution ⇒ unranked, never a marker-only win"
        );
        assert!(lb.hands.is_empty());
    }

    #[test]
    fn an_attributed_win_without_a_countersig_is_chain_proven_not_proven() {
        // The #276 tower-enforced winner: attributed, its own marker present
        // but singly-signed (the loser quit). It counts (chainProven) but
        // `proven` stays false.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &a, &b)]),
        );
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].wins, 1);
        assert!(lb.board[0].chain_proven);
        assert!(!lb.board[0].proven, "no verified countersig ⇒ not proven");
        assert_eq!(lb.board[0].evidence.len(), 1);
        assert!(lb.board[0].evidence[0].anchored);
        assert_eq!(lb.board[0].evidence[0].loser_sig_hex, None);
    }

    #[test]
    fn an_unconfirmed_or_unknown_pot_is_never_counted() {
        // The win is chain-derived, but only from a CONFIRMED landing: a pot
        // with no `pot_records` row is unknown → never counted, even with a
        // full attribution.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, None, 100, 0)];
        let world = win_world(&[(1, &a, &b)]);
        // Pot 1 has NO pot_records row → unknown.
        let statuses = statuses_for(&markers, &HashMap::new());
        let lb = agg(&markers, &statuses, &no_proofs(), &world);
        assert!(
            lb.board.is_empty(),
            "an unknown (unconfirmed) pot is never counted"
        );
        assert!(lb.hands.is_empty());
    }

    #[test]
    fn a_settle_mismatched_marker_decorates_nothing_but_the_chain_win_stands() {
        // The pot IS confirmed-spent (by txid 9) and attributed to A, so A's
        // win counts from the CHAIN. A marker naming a DIFFERENT settle (2)
        // does not anchor, so it never becomes evidence/cards — decoration
        // only, never the win.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 9u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &a, &b)]),
        );
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].wins, 1, "the chain win stands");
        assert!(
            lb.board[0].evidence.is_empty(),
            "a settle-mismatched marker never anchors ⇒ no evidence"
        );
        assert!(lb.hands.is_empty(), "and no hand");
        assert!(!lb.board[0].proven, "no anchored countersig ⇒ not proven");
    }

    #[test]
    fn one_pot_counts_once_however_many_markers() {
        // Two markers for the SAME pot + winner: one chain win, both markers
        // decorate.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(1, &a, &b, 1, 2, true, None, 100, 0),
            mk(1, &a, &b, 1, 2, true, None, 101, 1),
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &a, &b)]),
        );
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].wins, 1, "one pot, one win");
        assert_eq!(lb.board[0].evidence.len(), 2, "both markers decorate");
    }

    #[test]
    fn a_marker_naming_a_non_winner_neither_steals_nor_blocks_the_chain_win() {
        // The chain attributes pot 1 to A. A second confirmed marker names B
        // as winner of the same pot (collusion garbage). Under the spine the
        // chain decides: A counts, B counts nothing, and B's marker decorates
        // nothing (it names a non-winner).
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(1, &a, &b, 1, 2, true, None, 100, 0),
            mk(1, &b, &a, 1, 2, true, None, 101, 1),
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &a, &b)]),
        );
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].identity, a);
        assert_eq!(lb.board[0].wins, 1);
        assert!(
            lb.board.iter().all(|r| r.identity != b),
            "the non-winner marker never mints a row"
        );
    }

    #[test]
    fn board_ranks_by_wins_desc_then_identity() {
        // A: 2 wins; B: 1; C: 1. Ordered A, then B/C by identity asc.
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
        // Pots are 1,3,5,7 (A won 1&3, B 5, C 7).
        let world = win_world(&[(1, &a, &z), (3, &a, &z), (5, &b, &z), (7, &c, &z)]);
        let lb = agg(&markers, &statuses, &no_proofs(), &world);
        assert_eq!(lb.board.len(), 3);
        assert_eq!(lb.board[0].identity, a);
        assert_eq!(lb.board[0].wins, 2);
        // At equal wins the LOWER identity (lowercase hex byte order) ranks
        // first — assert against the computed order so the rule, not the
        // seed choice, is what is pinned.
        let (first, second) = if b < c { (&b, &c) } else { (&c, &b) };
        assert_eq!(&lb.board[1].identity, first);
        assert_eq!(&lb.board[2].identity, second);
    }

    #[test]
    fn lowest_hands_ordering() {
        // Two chain-counted v2 hands: pot 1 scores 15, pot 2 scores 20.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(2, &a, &b, 3, 4, true, Some("0001020304"), 200, 0), // score 20
            mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0), // score 15
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8), (3, 4)]));
        // Pots are 1 and 3.
        let world = win_world(&[(1, &a, &b), (3, &a, &b)]);
        let lb = agg(&markers, &statuses, &no_proofs(), &world);
        assert_eq!(lb.hands.len(), 2);
        assert_eq!(lb.hands[0].score, 15);
        assert_eq!(lb.hands[0].game_id, tx(1));
        assert!(lb.hands[0].anchored);
        assert_eq!(lb.hands[1].score, 20);

        // A v1 (no-cards) counted pot yields NO hand; an un-anchored pot is
        // not counted at all.
        let markers2 = vec![
            mk(1, &a, &b, 1, 2, true, None, 100, 0), // no cards
            mk(2, &a, &b, 3, 4, true, Some("0001020304"), 200, 0), // pot 3 unspent below
        ];
        let statuses2 = statuses_for(&markers2, &HashMap::from([(1u8, 2u8)])); // pot 3 unspent
        let lb2 = agg(&markers2, &statuses2, &no_proofs(), &world);
        assert!(lb2.hands.is_empty(), "no-cards + uncounted pot ⇒ no hands");
    }

    #[test]
    fn score_tie_breaks_on_earliest_created_at() {
        // Same score, different pots — earlier createdAt ranks first.
        let a = ident(0xaa);
        let b = ident(0xbb);
        let markers = vec![
            mk(2, &a, &b, 3, 4, true, Some("0001020304"), 500, 0), // score 20, later
            mk(1, &a, &b, 1, 2, true, Some("0001020304"), 100, 0), // score 20, earlier
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8), (3, 4)]));
        // Pots are 1 and 3; games are 1 and 2.
        let world = win_world(&[(1, &a, &b), (3, &a, &b)]);
        let lb = agg(&markers, &statuses, &no_proofs(), &world);
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
        let world = win_world(&[(1, &a, &b)]);
        // A pointer SUPERSET (client filters by validity); two candidates.
        let proofs = HashMap::from([(
            (tx(1), a.clone()),
            vec!["proof-a".to_string(), "proof-b".to_string()],
        )]);
        let lb = agg(&markers, &statuses, &proofs, &world);
        assert_eq!(
            lb.board[0].evidence[0].proof_txids,
            vec!["proof-a".to_string(), "proof-b".to_string()]
        );
        // Absent proof → empty set, count unchanged.
        let lb2 = agg(&markers, &statuses, &no_proofs(), &world);
        assert!(lb2.board[0].evidence[0].proof_txids.is_empty());
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
        // A doubly-signed, ANCHORED v2 marker.
        let markers = vec![mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let attrs = win_world(&[(1, &a, &b)]).1;
        let params = params_of(&[(1, &a, &b)]);

        // No verdict at all → unranked (a marker alone counts nothing).
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &HashMap::new(),
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
        assert!(lb.board.is_empty(), "no verdict ⇒ unranked");

        // WinnerA verdict + attribution → counts, verdict carried in evidence.
        let verdicts = HashMap::from([(tx(1), PotVerdict::WinnerA)]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
        assert_eq!(lb.board[0].wins, 1);
        assert_eq!(lb.hands.len(), 1);
        assert_eq!(
            lb.board[0].evidence[0].server_verdict,
            Some(PotVerdict::WinnerA)
        );

        // REFUND verdict → nobody won: no wins, no hands, no board.
        let verdicts = HashMap::from([(tx(1), PotVerdict::Refund)]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
        assert!(lb.board.is_empty(), "a refund is never a win");
        assert!(lb.hands.is_empty());

        // TIE verdict → same exclusion.
        let verdicts = HashMap::from([(tx(1), PotVerdict::Tie)]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
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
        let world = win_world(&[(1, &a, &b)]);
        let _ = PotVerdict::WinnerA;
        let lb = agg(&markers, &statuses, &no_proofs(), &world);
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1, false, None)).unwrap();
        assert_eq!(v["board"][0]["evidence"][0]["serverVerdict"], "winner-a");
    }

    /// #336/#337 CROSS-REPO WIRE PIN (Rule 16 — share the ARTIFACT, not the
    /// convention). The REAL serializer (`leaderboard_body`) drives a scenario
    /// covering BOTH new shapes — an identity win with evidence + `chainWins`
    /// (#336 coexistence) and a KEY-ATTRIBUTED win with EMPTY evidence +
    /// `chainWins` + `identityIsKey` (#337) — and its output, normalized to the
    /// canonical pretty form, must EQUAL the committed fixture BYTE-FOR-BYTE.
    ///
    /// The IDENTICAL bytes are checked into the bsv-low client
    /// (`app/src/lib/fixtures/leaderboard_chain_wins.fixture.json`) and read
    /// back by the real client PARSER + `gatherBoardFast`, so the producer's
    /// output is proven acceptable to the consumer across the language boundary.
    /// bsv-low #406: the served settle-signer classification rides both the
    /// evidence row and the chain-win anchor when the route resolved it, and
    /// stays null when it did not — the client's ending narration must never
    /// see a value the freshness-guarded column read did not produce.
    #[test]
    fn evidence_and_anchors_carry_settle_signers_when_resolved() {
        use crate::results::PotVerdict;
        let w = ident(0xaa);
        let l = ident(0xbb);
        let key_a = ident(0x5a);
        let key_b = ident(0x5b);
        let markers = vec![
            mk(1, &w, &l, 1, 2, true, None, 100, 0),
            mk(3, &w, &l, 3, 4, true, None, 100, 0),
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8), (3u8, 4u8)]));
        let verdicts = verdicts_of(&[(1, PotVerdict::WinnerA), (3, PotVerdict::WinnerA)]);
        let attrs = attrs_of(&[(3, Some(&w), Some(&l))]);
        let params = params_of(&[(1, &key_a, &key_b)]);
        // Only pot 3 has a resolved signer classification.
        let pot3 = hex::encode([3u8; 32]);
        let signers = HashMap::from([(pot3, "coop".to_string())]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &signers,
        );
        let with_ev = lb
            .board
            .iter()
            .find(|r| !r.evidence.is_empty())
            .expect("the identity row");
        assert_eq!(with_ev.evidence[0].settle_signers.as_deref(), Some("coop"));
        assert_eq!(
            with_ev.chain_wins[0].settle_signers.as_deref(),
            Some("coop")
        );
        let key_row = lb
            .board
            .iter()
            .find(|r| r.identity_is_key)
            .expect("the key row (pot 1 — unresolved)");
        assert_eq!(
            key_row.chain_wins[0].settle_signers, None,
            "unresolved stays null"
        );
        // …and the body emits exactly that.
        let body = leaderboard_body(&lb, 1, 2, false, None);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let rows = v["board"].as_array().unwrap();
        let ev_row = rows
            .iter()
            .find(|r| !r["evidence"].as_array().unwrap().is_empty())
            .unwrap();
        assert_eq!(ev_row["evidence"][0]["settleSigners"], "coop");
        assert_eq!(ev_row["chainWins"][0]["settleSigners"], "coop");
        let k_row = rows.iter().find(|r| r["identityIsKey"] == true).unwrap();
        assert_eq!(
            k_row["chainWins"][0]["settleSigners"],
            serde_json::Value::Null
        );
    }

    /// A serializer field-name/shape drift on this side goes RED here; a
    /// parser drift on the client side goes red there; and the two copies are
    /// byte-compared on the client so they can never diverge silently.
    #[test]
    fn chain_wins_body_matches_cross_repo_fixture() {
        use crate::results::PotVerdict;
        let w = ident(0xaa);
        let l = ident(0xbb);
        let key_a = ident(0x5a);
        let key_b = ident(0x5b);
        let markers = vec![
            mk(1, &w, &l, 1, 2, true, None, 100, 0), // pot 1 — key-attributed (no attribution)
            mk(3, &w, &l, 3, 4, true, None, 100, 0), // pot 3 — identity-attributed, countersigned
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8), (3u8, 4u8)]));
        let verdicts = verdicts_of(&[(1, PotVerdict::WinnerA), (3, PotVerdict::WinnerA)]);
        let attrs = attrs_of(&[(3, Some(&w), Some(&l))]); // only pot 3 resolves an identity
        let params = params_of(&[(1, &key_a, &key_b)]); // pot 1 falls back to the committed key
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
        // Loud-count guard: exactly two board rows, one of each new shape.
        assert_eq!(
            lb.board.len(),
            2,
            "the scenario must produce two board rows"
        );
        assert!(
            lb.board
                .iter()
                .any(|r| r.identity_is_key && r.evidence.is_empty() && r.chain_wins.len() == 1),
            "a key-attributed row with empty evidence + a chain-win anchor (#337)"
        );
        assert!(
            lb.board
                .iter()
                .any(|r| !r.identity_is_key && !r.evidence.is_empty() && r.chain_wins.len() == 1),
            "an identity row with evidence + a chain-win anchor (#336 coexistence)"
        );
        let body = leaderboard_body(&lb, 1_700_000_000, 2, false, None);
        let pretty: serde_json::Value = serde_json::from_str(&body).unwrap();
        let mut got = serde_json::to_string_pretty(&pretty).unwrap();
        got.push('\n');
        let fixture = include_str!("fixtures/leaderboard_chain_wins.fixture.json");
        assert_eq!(
            got, fixture,
            "the /leaderboard body must match the cross-repo fixture BYTE-FOR-BYTE — if this \
             changed intentionally, regenerate the fixture and copy it byte-identically to \
             app/src/lib/fixtures/leaderboard_chain_wins.fixture.json in bsv-low"
        );
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
                            spender_final: None,
                            spender_seen: None,
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
        // Attribute every EVEN (anchored/confirmed) pot to A — the chain
        // world the route would derive for them. Odd pots are unspent, so
        // even with an attribution they are unconfirmed → uncounted.
        let mut world: World = (HashMap::new(), HashMap::new(), HashMap::new());
        for i in (0u8..50).step_by(2) {
            let (v, at, pr) = win_world(&[(100 + i, &a, &b)]);
            world.0.extend(v);
            world.1.extend(at);
            world.2.extend(pr);
        }
        let lb = agg(&markers, &statuses, &no_proofs(), &world);

        // 25 even pots confirmed + attributed ⇒ 25 wins for identity a.
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].identity, a);
        assert_eq!(lb.board[0].wins, 25, "only the 25 confirmed pots count");
        // A pot from the SECOND chunk (index 46, even ⇒ anchored) must count —
        // proves the merge crosses the chunk boundary. Its evidence entry is
        // anchored; an odd (un-anchored) one never counted, so is absent.
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
        let proofs = HashMap::from([((tx(1), a.clone()), vec!["px".to_string()])]);
        let lb = agg(&markers, &statuses, &proofs, &win_world(&[(1, &a, &b)]));
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1, false, None)).unwrap();
        assert_eq!(v["computedAt"], 1_700_000_000_i64);
        assert_eq!(v["resultCount"], 1);
        let board = v["board"].as_array().unwrap();
        assert_eq!(board.len(), 1);
        assert_eq!(board[0]["identity"], a);
        assert_eq!(board[0]["wins"], 1);
        assert_eq!(board[0]["proven"], true);
        assert_eq!(board[0]["identityIsKey"], false); // resolved identity, not a key
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
        assert_eq!(ev[0]["proofTxids"][0], "px");
        assert_eq!(
            ev[0]["proofPosted"], false,
            "the aggregator never stamps a posted proof; the route does"
        );
        assert_eq!(ev[0]["cardsHex"], "000102030c");
        assert_eq!(ev[0]["createdAt"], 100);
        let hands = v["hands"].as_array().unwrap();
        assert_eq!(hands.len(), 1);
        assert_eq!(hands[0]["gameId"], tx(1));
        assert_eq!(hands[0]["score"], 15);
        assert_eq!(hands[0]["cardsHex"], "000102030c");
        assert_eq!(hands[0]["winner"], a);
        assert_eq!(hands[0]["loser"], b);
        assert_eq!(hands[0]["anchored"], true);
        assert_eq!(hands[0]["createdAt"], 100);
    }

    /// The date and the opponent must SURVIVE to the wire — the fast path's
    /// whole job is to serve the same facts the slow client-side `gatherBoard`
    /// derives, and it silently dropped both: `createdAt` was computed here (it
    /// is the hand-list tie-break) and thrown away, and `loser` never left the
    /// evidence row, so a hand row could not name who was across the table.
    ///
    /// Both are DISPLAY-ONLY and therefore OPTIONAL on the wire: an unstamped
    /// marker must serialize as an explicit `null`, not vanish, so the client
    /// can tell "no date recorded" from "this server is too old to send one".
    #[test]
    fn an_unstamped_marker_serializes_a_null_date_not_an_absent_field() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let mut m = mk(1, &a, &b, 1, 2, true, Some("000102030c"), 100, 0);
        m.created_at = None; // the index never recorded an admission time
        let markers = vec![m];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &a, &b)]),
        );
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1, false, None)).unwrap();
        let ev = &v["board"][0]["evidence"][0];
        assert!(ev["createdAt"].is_null(), "absent stamp must be null");
        assert!(
            ev.as_object().unwrap().contains_key("createdAt"),
            "the field must be EMITTED even when null — absent means 'old server'"
        );
        let hand = &v["hands"][0];
        assert!(hand["createdAt"].is_null());
        assert!(hand.as_object().unwrap().contains_key("createdAt"));
        // The opponent is not optional: every marker names one.
        assert_eq!(hand["loser"], b);
    }

    /// The hand list's score-tie break is the EARLIEST claim. That ordering
    /// used to ride in a side tuple; it now reads off the row's own
    /// `created_at`, so pin that the move did not invert it.
    #[test]
    fn tied_hand_scores_still_break_to_the_earliest_claim() {
        let a = ident(0xaa);
        let b = ident(0xbb);
        let c = ident(0xcc);
        // Two different games, same five cards (same score), different stamps.
        let markers = vec![
            mk(1, &a, &b, 1, 2, true, Some("000102030c"), 900, 0), // later
            mk(3, &c, &b, 3, 4, true, Some("000102030c"), 100, 0), // earlier
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8), (3u8, 4u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &a, &b), (3, &c, &b)]),
        );
        assert_eq!(lb.hands.len(), 2);
        assert_eq!(lb.hands[0].score, lb.hands[1].score);
        assert_eq!(
            lb.hands[0].created_at,
            Some(100),
            "earliest claim ranks first"
        );
        assert_eq!(lb.hands[0].winner, c);
        assert_eq!(lb.hands[1].created_at, Some(900));
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
        let world = win_world(&[(1, &a, &b)]);
        // confirmed=false ⇒ no loserSigHex: exactly the tower-enforced shape.
        // The win is CHAIN-attributed (loser gone, no countersig).
        let markers = vec![mk(1, &a, &b, 1, 2, false, Some("000102030c"), 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(&markers, &statuses, &no_proofs(), &world);
        assert_eq!(
            lb.board[0].evidence[0].cards_hex.as_deref(),
            Some("000102030c"),
            "the v2 cards the winner's signature binds must survive aggregation"
        );
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1, false, None)).unwrap();
        let ev = &v["board"][0]["evidence"][0];
        assert_eq!(
            ev["loserSigHex"],
            serde_json::Value::Null,
            "unconfirmed row"
        );
        assert_eq!(ev["anchored"], true);
        assert_eq!(ev["cardsHex"], "000102030c");
        // The chain-attributed win counts (chainProven), but proven is false.
        assert_eq!(lb.board[0].wins, 1);
        assert!(lb.board[0].chain_proven);
        assert!(!lb.board[0].proven);

        // A genuine v1 claim emits an explicit null — never an absent key
        // (absent and null must stay distinguishable on the wire).
        let markers = vec![mk(1, &a, &b, 1, 2, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(&markers, &statuses, &no_proofs(), &world);
        let v: serde_json::Value =
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1, false, None)).unwrap();
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

    /// Committed params per pot — the seat keys are the given identity hexes
    /// (real 66-hex pubkeys), which is all the identity-display path needs
    /// (#332 v3 reads the committed key only for the settle-key FALLBACK when
    /// no attribution resolves). `key_ab` names the committed seat-A/B keys
    /// explicitly for the KEY-fallback cells.
    fn params_of(entries: &[(u8, &str, &str)]) -> HashMap<String, crate::results::CovenantParams> {
        entries
            .iter()
            .map(|(pot, a, b)| (tx(*pot), params_with(a, b)))
            .collect()
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

        // WITHOUT the attribution: a verdict alone is UNRANKED (no row).
        // E7b: the non-attributed aggregator is gone — the live spine with an
        // EMPTY attribution map says the same thing.
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &HashMap::new(),
            &no_params(),
            &std::collections::HashMap::new(),
        );
        assert!(
            lb.board.is_empty(),
            "verdict without attribution ⇒ unranked"
        );

        // WITH it: the win counts, on the honest new tier.
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &no_params(),
            &std::collections::HashMap::new(),
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
            serde_json::from_str(&leaderboard_body(&lb, 1_700_000_000, 1, false, None)).unwrap();
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
            &no_params(),
            &std::collections::HashMap::new(),
        );
        assert_eq!(lb.board[0].wins, 1);
        assert!(lb.board[0].proven);
        assert!(lb.board[0].chain_proven);
    }

    /// #332 v3 — the ATTRIBUTION-EVICTION close (the delta re-gate's HIGH). A
    /// covenant pot with a WinnerA verdict + committed keys, but NO resolved
    /// identity (the potparty seat marker was evicted / never landed), STILL
    /// counts a win — under the committed winning SETTLE KEY (`pubA`), flagged
    /// `identity_is_key`. Never erased, never a wrong winner. The v2 spine
    /// (win minted from `attr.winner_for`) returned NO win here — a public
    /// erasure — which is exactly what this closes.
    #[test]
    fn an_attributed_win_survives_with_no_identity_under_the_settle_key() {
        let w = ident(0xaa);
        let l = ident(0xbb);
        let key_a = ident(0x5a); // the committed seat-A settle key
        let key_b = ident(0x5b);
        let markers = vec![mk(1, &w, &l, 1, 2, true, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let verdicts = verdicts_of(&[(1, crate::results::PotVerdict::WinnerA)]);
        // NO attribution resolved (empty attr) — the eviction case.
        let params = params_of(&[(1, &key_a, &key_b)]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &HashMap::new(),
            &params,
            &std::collections::HashMap::new(),
        );
        assert_eq!(lb.board.len(), 1, "the win is NOT erased");
        assert_eq!(
            lb.board[0].identity, key_a,
            "it counts under the committed winning settle key"
        );
        assert_eq!(lb.board[0].wins, 1);
        assert!(lb.board[0].chain_proven);
        assert!(
            lb.board[0].identity_is_key,
            "flagged as key-attributed (identity unknown), never a player id"
        );
        // WinnerB would count under pubB instead.
        let verdicts_b = verdicts_of(&[(1, crate::results::PotVerdict::WinnerB)]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts_b,
            &HashMap::new(),
            &params,
            &std::collections::HashMap::new(),
        );
        assert_eq!(lb.board[0].identity, key_b);
        // With the attribution PRESENT, the same pot counts under the identity.
        let attrs = attrs_of(&[(1, Some(&w), Some(&l))]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
        assert_eq!(lb.board[0].identity, w);
        assert!(!lb.board[0].identity_is_key);
    }

    /// #336/#337 LOW-2 (Rule 10 — the invariant is a CHECK, not a comment): the
    /// chain-win anchor's `settleTxid` is the pot's confirmed spender, and the
    /// `counted` gate now requires `spending_txid.is_some()`. A confirmed
    /// landing that carried NO spender (never observed in production, but not
    /// type-enforced) must therefore NOT be counted — a win the client could
    /// never re-derive (no settle txid to feed `/beef`) is not minted.
    #[test]
    fn confirmed_landing_without_spender_is_not_counted() {
        use crate::results::PotVerdict;
        let w = ident(0xaa);
        let l = ident(0xbb);
        let key_a = ident(0x5a);
        let key_b = ident(0x5b);
        let markers = vec![mk(1, &w, &l, 1, 2, true, None, 100, 0)];
        let verdicts = verdicts_of(&[(1, PotVerdict::WinnerA)]);
        let attrs = attrs_of(&[(1, Some(&w), Some(&l))]);
        let params = params_of(&[(1, &key_a, &key_b)]);

        // CONTROL: a normal confirmed landing WITH a spender counts, and carries
        // exactly one chain-win anchor whose settleTxid is that spender.
        let ok = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &ok,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
        assert_eq!(lb.board.len(), 1);
        assert_eq!(lb.board[0].chain_wins.len(), 1);
        assert_eq!(lb.board[0].chain_wins[0].settle_txid, tx(2));

        // DEFECT SHAPE: same confirmed landing, but spending_txid stripped. Its
        // `is_confirmed_landing` is still true (flag-only), so ONLY the new
        // `spending_txid.is_some()` limb keeps it out of `counted`.
        let mut no_spender = ok.clone();
        assert!(
            is_confirmed_landing(&no_spender[0]),
            "flags still say confirmed"
        );
        no_spender[0].spending_txid = None;
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &no_spender,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &params,
            &std::collections::HashMap::new(),
        );
        assert!(
            lb.board.is_empty(),
            "a spender-less confirmed landing is not counted"
        );
    }

    /// Adversarial (risk register B1/B5): the CHAIN decides the winner, and a
    /// fabricated marker naming a THIEF as winner of a chain-attributed pot
    /// decorates nothing — the honest attributed winner counts. And, the #332
    /// v2 anti-erasure property: the win counts from the chain even when the
    /// winner has NO marker at all (a rowless-but-real win), so eviction of
    /// the honest marker can never drop it.
    #[test]
    fn the_chain_decides_the_winner_regardless_of_markers() {
        let honest = ident(0xaa);
        let loser = ident(0xbb);
        let thief = ident(0xcc);
        let verdicts = verdicts_of(&[(1, crate::results::PotVerdict::WinnerA)]);
        let attrs = attrs_of(&[(1, Some(&honest), Some(&loser))]);

        // Thief's countersigned fabrication vs the honest unconfirmed claim,
        // pot attributed to honest: the chain awards the pot to honest, the
        // thief marker decorates nothing.
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
            &no_params(),
            &std::collections::HashMap::new(),
        );
        let row_of = |id: &str| lb.board.iter().find(|r| r.identity == id);
        assert!(
            row_of(&thief).is_none(),
            "the thief marker never mints a row"
        );
        let h = row_of(&honest).expect("the attributed winner counts");
        assert_eq!(h.wins, 1);
        assert!(h.chain_proven);
        assert!(!h.proven);

        // ANTI-ERASURE (#332 v2): the win counts from the chain even with NO
        // honest marker at all — only the thief's fabrication is present. The
        // honest winner still gets the row (rowless evidence), and the thief
        // gets nothing. This is the property the interim design LACKED.
        let markers = vec![mk(1, &thief, &loser, 1, 2, true, None, 200, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &no_params(),
            &std::collections::HashMap::new(),
        );
        let h = row_of2(&lb, &honest).expect("the chain win stands without a marker");
        assert_eq!(h.wins, 1);
        assert!(h.chain_proven);
        assert!(h.evidence.is_empty(), "no honest marker ⇒ no evidence");
        assert!(
            lb.board.iter().all(|r| r.identity != thief),
            "the thief marker mints nothing"
        );

        // An UNKNOWN (unconfirmed) pot never counts even when attributed.
        let markers = vec![mk(1, &honest, &loser, 1, 2, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::new()); // pot unknown
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &no_params(),
            &std::collections::HashMap::new(),
        );
        assert!(lb.board.is_empty());

        // A pot with NO verdict (unclassified) contributes nothing even with
        // an attribution — unranked.
        let markers = vec![mk(3, &honest, &loser, 3, 4, false, None, 100, 0)];
        let statuses = statuses_for(&markers, &HashMap::from([(3u8, 4u8)]));
        let attrs3 = attrs_of(&[(3, Some(&honest), Some(&loser))]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &HashMap::new(),
            &attrs3,
            &no_params(),
            &std::collections::HashMap::new(),
        );
        assert!(lb.board.is_empty(), "no verdict ⇒ unranked");
    }

    /// Helper: find a board row by identity (borrowed).
    fn row_of2<'a>(lb: &'a Leaderboard, id: &str) -> Option<&'a LeaderboardBoardRow> {
        lb.board.iter().find(|r| r.identity == id)
    }

    // ── #332 attack cells (RED-verified against the pre-#332 gate) ──────────

    /// #332 attack (a) — INFLATION. A real, confirmed `(potTxid, settleTxid)`
    /// is PUBLIC. The attacker copies it into N invented gameIds, each marker
    /// naming ITSELF winner with junk bytes. Under the spine those markers
    /// decorate nothing (junk sig, and the attacker is not the attributed
    /// winner of any pot); the only counted win is pot 1's, attributed to the
    /// honest winner.
    #[test]
    fn inflation_junk_sig_copies_of_a_public_settle_mint_no_wins() {
        let w = ident(0x11);
        let l = ident(0x22);
        let attacker = ident(0xcc);
        let mut markers = vec![mk(1, &w, &l, 1, 2, true, None, 100, 0)];
        for g in 0..10u8 {
            markers.push(mk_forged(
                0x30 + g,
                &attacker,
                &w,
                1,
                2,
                true,
                None,
                200 + i64::from(g),
                g,
            ));
        }
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &w, &l)]),
        );
        let wins_of = |id: &str| {
            lb.board
                .iter()
                .find(|r| r.identity == *id)
                .map_or(0, |r| r.wins)
        };
        assert_eq!(
            wins_of(&attacker),
            0,
            "junk-sig copies of a public (pot, settle) must mint NO wins"
        );
        assert_eq!(wins_of(&w), 1, "the honest attributed win still scores");
        assert!(
            lb.board.iter().find(|r| r.identity == w).unwrap().proven,
            "the honest win keeps its VERIFIED-countersigned decoration"
        );
    }

    /// #332 attack (b) — ERASURE (the reviewed CRITICAL-1). The opponent knows
    /// `potTxid` at funding and, DURING the hand, files the RESULT_ROWS_PER_POT
    /// oldest rows for the pot (junk sigs, garbage settle) — so the honest
    /// winner's real countersigned marker, published LATER, is EVICTED from the
    /// per-pot window. Under the interim (marker-driven) count that permanently
    /// dropped the win even under a full attribution. Under the spine the win
    /// is a CHAIN fact: the eviction costs only the decoration, and the win —
    /// attributed to the victim — stands.
    #[test]
    fn earlier_spam_evicts_the_honest_marker_but_never_the_chain_win() {
        let w = ident(0x11);
        let l = ident(0x22);
        let opponent = l.clone();
        // The window delivered ONLY the 4 attacker rows (the honest marker was
        // evicted — it is simply not in the slice the aggregate receives).
        let markers: Vec<ResultMarkerRow> = (0..4u8)
            .map(|i| {
                mk_forged(
                    0x50 + i,
                    &opponent,
                    &w,
                    1,
                    9, // garbage settle
                    true,
                    None,
                    10 + i64::from(i), // stamped EARLIER than the honest marker
                    i,
                )
            })
            .collect();
        // The pot IS confirmed-spent (by its real settle 2) and attributed to
        // the victim — the chain facts the route derives independently of the
        // result markers.
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &w, &l)]),
        );
        let honest = lb
            .board
            .iter()
            .find(|r| r.identity == w)
            .expect("the chain win survives eviction of the honest marker");
        assert_eq!(honest.wins, 1, "eviction costs cards, never the win");
        assert!(honest.chain_proven);
        assert!(
            honest.evidence.is_empty(),
            "the honest marker was evicted ⇒ no evidence, but the win stands"
        );
        assert!(
            lb.board.iter().all(|r| r.identity != opponent),
            "the opponent's junk rows mint nothing"
        );
    }

    /// #332 (the reviewed CRITICAL-2) — EVICT-THEN-CLAIM credits nobody on an
    /// UNATTRIBUTED pot. An attacker with two keys files real-signed,
    /// countersigned claims naming ITSELF winner of the victim's real
    /// `(potTxid, settleTxid)`, stamped before the victim's marker; the pot is
    /// bare/legacy (no attribution). Under the interim count the attacker's
    /// verified claim WON (a public wrong-winner the client re-verify agreed
    /// with). Under the spine an unattributed pot is UNRANKED — the attacker
    /// gets nothing, and neither does anyone else.
    #[test]
    fn evict_then_claim_on_an_unattributed_pot_credits_nobody() {
        let w = ident(0x11);
        let attacker = ident(0xcc);
        let sock = ident(0xdd);
        // Attacker's 4 REAL-signed countersigned claims naming itself, over
        // the victim's real pot/settle; the honest marker is evicted (absent).
        let markers: Vec<ResultMarkerRow> = (0..4u8)
            .map(|i| {
                mk(
                    0x60 + i,
                    &attacker,
                    &sock,
                    1,
                    2,
                    true,
                    None,
                    10 + i64::from(i),
                    i,
                )
            })
            .collect();
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        // NO verdict, NO attribution — a bare/legacy pot.
        let lb = agg(&markers, &statuses, &no_proofs(), &World::default());
        assert!(
            lb.board.is_empty(),
            "an unattributed pot is UNRANKED — no wrong-winner, no win at all"
        );
        assert!(
            !lb.board
                .iter()
                .any(|r| r.identity == attacker || r.identity == w),
            "neither the forger nor the victim is credited on a bare pot"
        );
    }

    /// The verification-agnostic INFLATION variant, kept for completeness:
    /// even REAL two-key sock claims over a copied pot mint nothing, because
    /// the attacker is never the ATTRIBUTED winner. On the victim's covenant
    /// pot the honest win stands.
    #[test]
    fn sock_countersigned_copies_never_beat_the_attribution() {
        let w = ident(0x11);
        let l = ident(0x22);
        let attacker = ident(0xcc);
        let sock = ident(0xdd);
        let mut markers = vec![mk(1, &w, &l, 1, 2, true, None, 100, 0)];
        for g in 0..5u8 {
            markers.push(mk(
                0x40 + g,
                &attacker,
                &sock,
                1,
                2,
                true,
                None,
                200 + i64::from(g),
                g,
            ));
        }
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let lb = agg(
            &markers,
            &statuses,
            &no_proofs(),
            &win_world(&[(1, &w, &l)]),
        );
        let wins_of = |id: &str| {
            lb.board
                .iter()
                .find(|r| r.identity == *id)
                .map_or(0, |r| r.wins)
        };
        assert_eq!(wins_of(&attacker), 0, "sock copies mint nothing");
        assert_eq!(wins_of(&w), 1, "the attributed honest win stands");
    }

    /// Standing product rule (leaderboard-tower-wins-count): a quitting
    /// opponent must not erase the winner's record. The tower-enforced winner
    /// is UNCOUNTERSIGNED (the loser is gone); the quitter files a junk-sig
    /// marker naming ITSELF. The chain attribution counts the honest win and
    /// the forgery decorates nothing.
    #[test]
    fn a_quitting_opponents_junk_marker_cannot_erase_a_tower_enforced_win() {
        let w = ident(0x11);
        let l = ident(0x22);
        let markers = vec![
            mk(1, &w, &l, 1, 2, false, None, 100, 0),
            mk_forged(1, &l, &w, 1, 2, true, None, 200, 1),
        ];
        let statuses = statuses_for(&markers, &HashMap::from([(1u8, 2u8)]));
        let verdicts = verdicts_of(&[(1, crate::results::PotVerdict::WinnerA)]);
        let attrs = attrs_of(&[(1, Some(&w), Some(&l))]);
        let lb = aggregate_leaderboard_attributed(
            &markers,
            &statuses,
            &no_proofs(),
            200,
            &verdicts,
            &attrs,
            &no_params(),
            &std::collections::HashMap::new(),
        );
        let honest = lb
            .board
            .iter()
            .find(|r| r.identity == w)
            .expect("the tower-enforced winner keeps its record");
        assert_eq!(honest.wins, 1);
        assert!(honest.chain_proven);
        assert!(!honest.proven, "no countersignature ⇒ proven stays false");
        assert!(
            lb.board
                .iter()
                .find(|r| r.identity == l)
                .is_none_or(|r| r.wins == 0),
            "the quitter's forgery counts nothing"
        );
    }

    /// #335 item 2 — the truncation cut + the honest wire bit.
    #[test]
    fn clamp_limit_defaults_and_bounds() {
        assert_eq!(clamp_leaderboard_limit(None), LEADERBOARD_DEFAULT_LIMIT);
        assert_eq!(clamp_leaderboard_limit(Some(50)), 50);
        assert_eq!(clamp_leaderboard_limit(Some(0)), 1);
        assert_eq!(clamp_leaderboard_limit(Some(99_999)), LEADERBOARD_MAX_LIMIT);
    }
}
