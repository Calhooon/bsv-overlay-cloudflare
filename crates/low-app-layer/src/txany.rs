//! `GET /tx-any/:txid` — tx-level presence / confirmation / raw bytes for
//! ARBITRARY txids, honoring the READ HIERARCHY (owner doctrine, bsv-low
//! #229, 2026-07-22):
//!
//!   1. **Index-native leg (system of record for BYTES, not for network
//!      presence).** Every tx LOW ever broadcast is admitted to the overlay
//!      and its BEEF is stored durably (`pot_beefs` / `transactions`). If the
//!      stored BEEF carries a chaintracks-verified BUMP (stitched by the
//!      completion pass / arc-ingest merkle push), the tx is PROVEN mined —
//!      presence, raw bytes, confirmation, and height all answer from the
//!      index, zero external reads. A stored BEEF WITHOUT a BUMP proves only
//!      that we HOLD the bytes (bsv-low #247): the broadcast gate has had
//!      holes (#267/#268) and the sibling admission modes
//!      (historical-tx / GASP sync / peer crawl) are ungated by design, so
//!      the PRESENCE question falls through to the external leg alongside
//!      the confirmation question — the raw is still served either way.
//!   2. **Break-glass external leg (WoC + Bitails, SERVER-SIDE).** Only for
//!      txids the index has never seen: legacy pre-overlay-era txs (the
//!      2026-07-21 incident class) and foreign txs. The trust bars of the
//!      client code this replaces are preserved server-side:
//!        - POSITIVE presence requires the raw bytes fetched AND hash-verified
//!          against the txid (never a bare pointer/claim — the raw is also
//!          returned so the caller gets verified bytes for free);
//!        - `confirmed` carries WoC's `confirmations >= 1` claim — the exact
//!          trust the client's `wocTxConfirmed` placed in a direct WoC read;
//!        - NEGATIVE (provably absent) requires BOTH indexers to answer a
//!          definitive 404 AND the Bitails tx route to prove itself healthy
//!          against a known-mined anchor (the client's
//!          `bitailsConclusively404` route-rot guard, ported verbatim) — one
//!          provider's 404, or a 404 on a rotten route, is never absence;
//!        - anything else is the honest unknown (`present: null`) — the
//!          callers' fail-safe "unknown ⇒ retry, never a conclusion".
//!
//! Wire body: `{"txid","present","confirmed","height","rawHex","source"}`
//! where `source` is `"index"` / `"index+external"` / `"external"` / `null`
//! (unknown). All-null fields = nothing could be established.

use serde_json::json;

/// A tx that is unquestionably mined (mainnet, height 958886 — the same
/// route-sanity anchor the client used: bsv-low `homeCards.ts
/// KNOWN_MINED_TXID`). If Bitails 404s THIS txid, its tx route is
/// broken/moved and its 404s prove nothing.
pub const KNOWN_MINED_TXID: &str =
    "f358a4dd67c9d7b3a295d05d7a23abc0b85ba1f95c8afa756f1f466419be5e1c";

/// Hard TTL for the in-isolate `/tx-any` cache, milliseconds (same figure as
/// `/spent-any` — bounds upstream pressure; isolate recycling empties it).
pub const TX_ANY_CACHE_TTL_MS: f64 = 15_000.0;

/// bsv-low #451 slice B (2026-09-17): an `unconfirmable` verdict (an input spent by a DIFFERENT confirmed tx —
/// chain truth short of a reorg) lives in the in-isolate cache this long. The seeded home of a lived-in identity asks
/// the same dead stories on every boot, and each ask was a WoC read, a Bitails read and up to three courier ladders
/// (1.3–2.0 s per ask, measured on the beta budget hand). The ISOLATE cache, never the edge: the 2026-09-10 gate's
/// HIGH-1/2 rule (a courier's word never outlives the isolate) stands.
pub const TX_ANY_UNCONFIRMABLE_TTL_MS: f64 = 10.0 * 60_000.0;

/// PURE: how long one `/tx-any` answer lives in the isolate cache.
pub fn tx_any_cache_ttl_ms(answer: &TxAnyAnswer) -> f64 {
    if answer.unconfirmable {
        TX_ANY_UNCONFIRMABLE_TTL_MS
    } else {
        TX_ANY_CACHE_TTL_MS
    }
}

/// bsv-low #451 slice B: Arcade's `GET /tx/{txid}` as the FIRST external witness for a tx the index holds WITHOUT a
/// verified bump (the JOIN's first minutes; a MINED push not yet landed). Arcade is the broadcaster whose SEEN is the
/// index's own admission witness (CLAUDE.md D1), so its live word is exactly the network presence the external leg
/// exists to establish. THE GATE'S RULES (2026-09-17, HIGH-1 / HIGH-2 / MEDIUM-1): an ALLOW-LIST, never a deny-list
/// — only `SEEN_ON_NETWORK` / `SEEN_MULTIPLE_NODES` are the network holding it (the overlay's own `SEEN_OR_BETTER`
/// bar; the orphan view `SEEN_IN_ORPHAN_MEMPOOL` and the pre-network ranks RECEIVED / STORED / ANNOUNCED /
/// REQUESTED / SENT are NOT — the #267 hole was admitting on exactly that view, and a STORED tx can have mined
/// through the client's direct-ARC fallback while Arcade never sends it); a SEEN word counts only while its status
/// stamp is younger than [`ARCADE_WORD_FRESH_MS`] (a wedged or lagging Arcade must not hold the confirmation
/// question forever: the couriers are each other's fallbacks on every chain question, the 2026-09-04 ruling); a
/// MINED / IMMUTABLE word is a CLAIM — its `merklePath` + `blockHeight` come back for the caller to verify against
/// chaintracks before `confirmed` leaves the server (the index refused Arcade's pushed bump once on the D7 class,
/// and the client latches `confirmed` durably). Everything else is `Unknown`: the couriers decide, as before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArcadeWord {
    /// The network holds it unmined, said within the freshness window.
    SeenFresh,
    /// Mined, per Arcade: the bump hex and the height, to be VERIFIED by the caller.
    Mined { bump_hex: String, height: u64 },
    /// No usable word: ask the couriers.
    Unknown,
}

/// How old Arcade's status stamp may be for its live word to stand alone (the push-backstop reasoning:
/// `PUSH_BACKSTOP_MIN_AGE_SECS` on the overlay — after this long the push has had its chance).
pub const ARCADE_WORD_FRESH_MS: i64 = 30 * 60 * 1_000;

/// The two statuses that mean "the network holds it" (mirrors the overlay's `SEEN_OR_BETTER` minus the mined pair).
pub const ARCADE_SEEN_STATUSES: &[&str] = &["SEEN_ON_NETWORK", "SEEN_MULTIPLE_NODES"];

/// PURE: the three-way read of one Arcade body at `now_ms`.
pub fn parse_arcade_word(status: u16, body: &str, now_ms: i64) -> ArcadeWord {
    if !(200..300).contains(&status) {
        return ArcadeWord::Unknown;
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return ArcadeWord::Unknown;
    };
    let tx_status = v
        .get("txStatus")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    if tx_status == "MINED" || tx_status == "IMMUTABLE" {
        let path = v
            .get("merklePath")
            .and_then(|m| m.as_str())
            .map(str::trim)
            .filter(|m| !m.is_empty() && m.bytes().all(|b| b.is_ascii_hexdigit()));
        let height = v.get("blockHeight").and_then(|h| h.as_u64()).filter(|h| *h > 0);
        return match (path, height) {
            (Some(p), Some(h)) => ArcadeWord::Mined {
                bump_hex: p.to_ascii_lowercase(),
                height: h,
            },
            _ => ArcadeWord::Unknown,
        };
    }
    if !ARCADE_SEEN_STATUSES.contains(&tx_status.as_str()) {
        return ArcadeWord::Unknown;
    }
    let fresh = v
        .get("timestamp")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_utc_ms)
        .map(|ts| now_ms - ts < ARCADE_WORD_FRESH_MS && ts <= now_ms + 60_000)
        .unwrap_or(false);
    if fresh {
        ArcadeWord::SeenFresh
    } else {
        ArcadeWord::Unknown
    }
}

/// PURE: an RFC 3339 UTC instant (`YYYY-MM-DDTHH:MM:SS[.frac](Z|+00:00)`, the shape Arcade stamps) as ms since the
/// epoch; anything else `None`. No calendar crate in this crate — the civil-days arithmetic is Howard Hinnant's.
pub fn rfc3339_utc_ms(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, rest) = s.split_once('T')?;
    let mut d = date.split('-');
    let y = i64::from(digits(d.next()?)?);
    let m = digits(d.next()?)?;
    let day = digits(d.next()?)?;
    if d.next().is_some() || !(1..=12).contains(&m) || day == 0 || day > days_in_month(y, m) {
        return None;
    }
    let time = rest
        .strip_suffix('Z')
        .or_else(|| rest.strip_suffix("+00:00"))
        .or_else(|| rest.strip_suffix("+0000"))?;
    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (time, None),
    };
    let mut t = hms.split(':');
    // digit-only fields: a `-1` or `+5` never parses (the delta-verify's LOW-C; `u32::from_str` accepts a `+`)
    let hh = i64::from(digits(t.next()?)?);
    let mm = i64::from(digits(t.next()?)?);
    let ss = i64::from(digits(t.next()?)?);
    if t.next().is_some() || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    let millis: i64 = match frac {
        Some(f) if !f.is_empty() && f.bytes().all(|b| b.is_ascii_digit()) => {
            let digits: String = f.chars().take(3).collect();
            let v: i64 = digits.parse().ok()?;
            v * 10_i64.pow(3 - digits.len() as u32)
        }
        Some(_) => return None,
        None => 0,
    };
    // days from civil (proleptic Gregorian), 1970-01-01 = 0
    let (y2, m2) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let doy = (153 * i64::from(m2) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + hh * 3_600 + mm * 60 + ss) * 1_000) + millis)
}

/// PURE: a non-empty all-ASCII-digit field, as a number (no sign, no whitespace).
fn digits(s: &str) -> Option<u32> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// PURE: the days of `month` in `year` (proleptic Gregorian; a leap year every 4, not every 100, but every 400).
pub fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// bsv-low #451 slice C (iii) (2026-09-18): one memoised UNCONFIRMABLE verdict (`tx_any_verdicts`, overlay
/// migration 151) — the input a different confirmed tx spent, and that spender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictMemo {
    pub txid: String,
    pub verdict_at_ms: i64,
    /// `<txid>:<vout>` of the input the conflicting spend consumed (empty for an absence verdict).
    pub input_outpoint: String,
    /// The confirmed spender of that input (empty for an absence verdict).
    pub spender_txid: String,
    /// bsv-low #451 slice C (iv), migration 152: what kind of negative this is.
    pub kind: VerdictKind,
    /// The evidence, as words (the janitor's retire reason; the input and its spender).
    pub evidence: String,
}

/// The kinds of negative a memo can hold. `Unconfirmable` = an input a DIFFERENT confirmed tx spent (the subject can
/// never confirm); `Absent` = corroborated network absence (Arcade 404 + both indexers 404, past the janitor's age
/// bar); `Refused` = Arcade's terminal refusal corroborated by both indexers' absence. Every kind answers
/// `present: false`; only `Unconfirmable` sets the client's `unconfirmable` flag (the refund rebroadcast retirement
/// reads it, and only an input conflict earns it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictKind {
    Unconfirmable,
    Absent,
    Refused,
}

impl VerdictKind {
    pub fn as_str(self) -> &'static str {
        match self {
            VerdictKind::Unconfirmable => "unconfirmable",
            VerdictKind::Absent => "absent",
            VerdictKind::Refused => "refused",
        }
    }
    /// A row written before migration 152 carries no kind: it was an unconfirmable verdict (the only kind then).
    pub fn parse(s: Option<&str>) -> Option<VerdictKind> {
        match s.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None | Some("") | Some("unconfirmable") => Some(VerdictKind::Unconfirmable),
            Some("absent") => Some(VerdictKind::Absent),
            Some("refused") => Some(VerdictKind::Refused),
            _ => None,
        }
    }
}

/// PURE: the `/tx-any` answer a fresh memo yields for a row whose index bytes are `index_raw` (served beside the
/// negative when held): every kind is a corroborated `present: false`; only an input conflict is `unconfirmable`.
pub fn answer_from_verdict_memo(memo: &VerdictMemo, index_raw: Option<String>) -> TxAnyAnswer {
    let mut a = decide_tx_any(
        index_raw,
        None,
        Some(&TxObservation::Absent),
        AbsenceCorroboration::CorroboratedAbsent,
    );
    a.unconfirmable = memo.kind == VerdictKind::Unconfirmable;
    a.source = Some("memo");
    a
}

/// How old an ABSENCE memo may be to answer without the couriers: an hour — an absence can end (a late broadcast of
/// the same bytes), so it is re-asked, and for an index-unknown row the request-time break-glass re-asks once an
/// hour per txid at most. An `Unconfirmable` or `Refused` memo is TERMINAL and answers at any age (slice C (iv),
/// measured 2026-09-18 03:47Z: when the hour lapsed and the request path no longer re-derived the verdict, every
/// dead story turned into a recurring `null` — 109 client reads on the felt, 44 `/utxo-status` in one second):
/// an input a different CONFIRMED tx spent, or Arcade's terminal word corroborated by both indexers' absence, is
/// chain truth short of a reorg deeper than that spender, a headline event; the reorg sweep is where such a memo
/// would be revisited, never the request path.
pub const VERDICT_MEMO_MAX_AGE_MS: i64 = 60 * 60_000;

/// PURE: does a memo answer for `txid` at `now_ms`? A terminal kind at any age (never from the future); an absence
/// inside `max_age_ms`.
pub fn verdict_memo_answers(memo: &VerdictMemo, txid: &str, now_ms: i64, max_age_ms: i64) -> bool {
    let age = now_ms - memo.verdict_at_ms;
    if memo.txid != txid.to_ascii_lowercase() || age < 0 {
        return false;
    }
    match memo.kind {
        VerdictKind::Unconfirmable | VerdictKind::Refused => true,
        VerdictKind::Absent => age < max_age_ms,
    }
}

/// The memo read for one txid, column order = the row's fields.
pub const VERDICT_MEMO_READ_SQL: &str =
    "SELECT txid, verdictAtMs, inputOutpoint, spenderTxid, kind, evidence FROM tx_any_verdicts WHERE txid = ?";
/// The memo read for `n` txids (`IN (?, …)`), the same columns — the batched `/tx-any` and `/spent-any` legs.
pub fn verdict_memo_read_many_sql(n: usize) -> String {
    let marks = std::iter::repeat_n("?", n).collect::<Vec<_>>().join(", ");
    format!("SELECT txid, verdictAtMs, inputOutpoint, spenderTxid, kind, evidence FROM tx_any_verdicts WHERE txid IN ({marks})")
}
/// bsv-low #451 slice C (v): the `/spent-any` reason for an outpoint of a tx with a terminal negative verdict — the
/// tx never confirmed, so its outputs are moot; no courier is asked. `known:false` keeps the honest unknown for every
/// consumer (a landing proof never rests on it), the reason says why.
pub const SPENT_ANY_REASON_TX_ABSENT: &str = "tx-absent";
/// The memo upsert (the same statement the overlay's dead-letter pass runs, by value).
pub const VERDICT_MEMO_UPSERT_SQL: &str = "INSERT INTO tx_any_verdicts (txid, verdictAtMs, inputOutpoint, spenderTxid, kind, evidence) VALUES (?, ?, ?, ?, ?, ?) \
     ON CONFLICT(txid) DO UPDATE SET verdictAtMs = excluded.verdictAtMs, inputOutpoint = excluded.inputOutpoint, spenderTxid = excluded.spenderTxid, kind = excluded.kind, evidence = excluded.evidence";

/// The external (WoC) observation of a txid, already shape-validated by the
/// route glue. `Present.raw_hex` is `Some` ONLY when the fetched raw bytes
/// HASHED to the txid (the route verifies before constructing this).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxObservation {
    /// WoC 200 on `/tx/hash/{txid}`: `confirmed` = `confirmations >= 1`;
    /// `raw_hex` = the hash-VERIFIED raw (None when the raw fetch failed or
    /// the bytes didn't hash to the txid).
    Present {
        confirmed: bool,
        raw_hex: Option<String>,
    },
    /// WoC definitive 404 — "this txid is not in my index".
    Absent,
    /// Transport / 5xx / rate-limit / malformed body.
    Fault,
}

/// Bitails' corroboration of an ABSENT claim (negatives are never one
/// provider's word — the #212/#213/#214 lesson).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsenceCorroboration {
    /// Bitails 404 for the txid AND its tx route proved healthy against the
    /// known-mined anchor.
    CorroboratedAbsent,
    /// Anything else — fault, 200 (contradiction), rotten route.
    Unknown,
}

/// The assembled `/tx-any` answer, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TxAnyAnswer {
    /// `Some(true)` = provably NETWORK-present (index BEEF with a verified
    /// BUMP, or an external indexer's hash-verified positive);
    /// `Some(false)` = provably absent (corroborated double-404);
    /// `None` = unknown.
    ///
    /// bsv-low #247: own-store bytes WITHOUT a BUMP no longer assert
    /// presence on their own. Admission is broadcast-gated on the money
    /// path, but (a) the gate had holes (#267 degraded-Arcade false-SEEN,
    /// #268 fake-bump efs==0 — both since closed) and (b) the sibling
    /// admission modes (historical-tx / GASP / peer-crawl) are ungated by
    /// design — so "we hold the bytes" is NOT "the network saw it", and the
    /// client treats `present` as network truth (a zombie orphan JOIN served
    /// present:true kept its bounded rebroadcasts alive forever).
    pub present: Option<bool>,
    /// `Some(true)` = proven/claimed mined (index BUMP, or WoC
    /// confirmations≥1); `Some(false)` = present but not yet confirmed per
    /// the external leg; `None` = unknown.
    pub confirmed: Option<bool>,
    /// The mined block height per a chaintracks-VERIFIED bump: the stored
    /// BEEF's (index leg), or — bsv-low #451 slice B — Arcade's MINED claim
    /// whose path computed, from this txid, the root chaintracks holds at
    /// that height (`routes::arcade_confirmation_look`). The courier leg
    /// never claims a height (a `confirmations` count is a claim, not a proof).
    pub height: Option<u64>,
    /// The raw tx bytes as lowercase hex — index-extracted or externally
    /// hash-verified. Never an unverified byte. Still served when the
    /// network verdict is absent/unknown (they are the caller's own admitted
    /// bytes — e.g. for a rebroadcast).
    pub raw_hex: Option<String>,
    /// Which leg answered: `"index"` / `"index+external"` / `"external"`.
    pub source: Option<&'static str>,
    /// bsv-low #247: `true` = PROVABLY UNCONFIRMABLE — an input of this tx
    /// is spent by a DIFFERENT, CONFIRMED tx, so this tx can never land.
    /// A terminal skip signal the client may consume to stop bounded
    /// rebroadcasts. Only ever set alongside `present: Some(false)` (the
    /// route probes inputs only for a corroborated-absent index-held tx);
    /// `false` means "not proven unconfirmable", never "confirmable".
    pub unconfirmable: bool,
}

/// The pure `/tx-any` decision table (unit-tested; the route feeds it real
/// observations). `index_raw_hex` is the raw extracted from a STORED BEEF
/// (already txid-bound by `extract_raw_tx_hex`); `index_height` is the BUMP
/// height when the completion pass has stitched one.
pub fn decide_tx_any(
    index_raw_hex: Option<String>,
    index_height: Option<u64>,
    external: Option<&TxObservation>,
    absence: AbsenceCorroboration,
) -> TxAnyAnswer {
    if let Some(raw) = index_raw_hex {
        // Index-native ONLY with a verified BUMP: a chaintracks-verified
        // merkle path is the strongest network truth there is.
        if let Some(h) = index_height {
            return TxAnyAnswer {
                present: Some(true),
                confirmed: Some(true),
                height: Some(h),
                raw_hex: Some(raw),
                source: Some("index"),
                unconfirmable: false,
            };
        }
        // Stored bytes WITHOUT a BUMP (#247): the store proves we HOLD the
        // bytes, not that the network ever saw them (see the `present` doc
        // — gate holes + deliberately ungated sibling admission modes), so
        // the PRESENCE question falls through to the external leg:
        //  - an external positive corroborates network presence
        //    (mempool `confirmed:false` or mined `confirmed:true`);
        //  - a CORROBORATED double-404 is an honest network-absent — the
        //    raw is still served (the caller's own bytes, rebroadcastable);
        //  - anything else is the honest unknown (`present: null`), raw
        //    still served. Fail-safe either way: the client's
        //    positive-anywhere-outranks-negatives read stays intact.
        return match external {
            Some(TxObservation::Present { confirmed, .. }) => TxAnyAnswer {
                present: Some(true),
                confirmed: Some(*confirmed),
                height: None,
                raw_hex: Some(raw),
                source: Some("index+external"),
                unconfirmable: false,
            },
            Some(TxObservation::Absent) if absence == AbsenceCorroboration::CorroboratedAbsent => {
                TxAnyAnswer {
                    present: Some(false),
                    confirmed: None,
                    height: None,
                    raw_hex: Some(raw),
                    source: Some("index+external"),
                    unconfirmable: false,
                }
            }
            _ => TxAnyAnswer {
                present: None,
                confirmed: None,
                height: None,
                raw_hex: Some(raw),
                source: Some("index"),
                unconfirmable: false,
            },
        };
    }
    // Break-glass external leg (legacy / foreign txids only).
    match external {
        Some(TxObservation::Present { confirmed, raw_hex }) => match raw_hex {
            // Positive presence ONLY with hash-verified bytes in hand — a
            // bare WoC pointer whose raw could not be fetched/verified is an
            // honest unknown, never a positive.
            Some(raw) => TxAnyAnswer {
                present: Some(true),
                confirmed: Some(*confirmed),
                height: None,
                raw_hex: Some(raw.clone()),
                source: Some("external"),
                unconfirmable: false,
            },
            None => TxAnyAnswer::default(),
        },
        Some(TxObservation::Absent) => match absence {
            AbsenceCorroboration::CorroboratedAbsent => TxAnyAnswer {
                present: Some(false),
                confirmed: None,
                height: None,
                raw_hex: None,
                source: Some("external"),
                unconfirmable: false,
            },
            AbsenceCorroboration::Unknown => TxAnyAnswer::default(),
        },
        Some(TxObservation::Fault) | None => TxAnyAnswer::default(),
    }
}

/// PURE (#247): does ONE input's spend observation prove the subject can
/// never confirm? True iff the input's outpoint is VERIFIED spent by a
/// DIFFERENT tx that is CONFIRMED — a confirmed conflicting spend is
/// permanent (absent a reorg), so the subject is provably unconfirmable.
/// Everything else (unknown, unspent, spent by the subject itself, spent
/// unconfirmed) proves nothing — fail toward `false` (keep retrying),
/// never toward a fabricated terminal verdict.
pub fn input_proves_unconfirmable(
    subject_txid: &str,
    known: bool,
    spent: Option<bool>,
    spending_txid: Option<&str>,
    spent_confirmed: Option<bool>,
) -> bool {
    known
        && spent == Some(true)
        && spent_confirmed == Some(true)
        && spending_txid.is_some_and(|s| !s.eq_ignore_ascii_case(subject_txid))
}

/// Parse a WoC `GET /tx/hash/{txid}` 200 body into the confirmation claim:
/// `confirmations >= 1`. A malformed body is simply "present, unconfirmed
/// claim unknown" → treated as `confirmed: false` (the caller's
/// `wocTxConfirmed` parity: anything unsure is false, never a landing).
pub fn parse_woc_confirmations(v: &serde_json::Value) -> bool {
    v.get("confirmations")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|c| c >= 1)
}

/// Verify externally-fetched raw bytes: they must parse AND hash to `txid`.
/// Returns the lowercase hex, or `None` (a lying/garbled provider byte never
/// leaves the server).
pub fn verify_raw_bytes(raw: &[u8], txid: &str) -> Option<String> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    if !tx.id().eq_ignore_ascii_case(txid) {
        return None;
    }
    Some(hex::encode(raw))
}

/// The `/tx-any` wire body. `unconfirmable` is additive (#247) — pre-#247
/// clients ignore it; a client that consumes it gets the terminal-skip
/// signal for a provably-dead tx.
/// bsv-low W-C.3 — the batched route's bound: one `/tx-any?txids=` answers up
/// to this many txids (the client chunks). Matches the tower's `/cases` shape.
pub const TX_ANY_BATCH_MAX: usize = 50;

/// bsv-low W-C.3 (gate MED-2) — how many txids one batched `IN (…)` query
/// asks for: with the per-row byte bound below, one query materializes at
/// most `TX_ANY_BATCH_D1_CHUNK × TX_ANY_BATCH_BEEF_MAX_BYTES` of BEEF (4 MiB)
/// before the hex doubles it, on a set an unauthenticated caller chose.
pub const TX_ANY_BATCH_D1_CHUNK: usize = 16;

/// bsv-low W-C.3 (gate MED-2) — the largest stored BEEF the batched index leg
/// serves (bytes). A bigger row is not served by the batch (the txid lands in
/// `unknown`, `index-miss`) and the per-txid route reads it on its own.
pub const TX_ANY_BATCH_BEEF_MAX_BYTES: u64 = 262_144;

/// `txids=<txid>,…` → lowercase txids, duplicates collapsed (first occurrence
/// kept, order preserved), empty items between commas ignored; `Err` names
/// the refusal (400). One malformed item refuses the whole list: a client
/// must never read a half-answered map as complete. The bound is checked
/// BEFORE the push (a 51st distinct txid is refused, not admitted).
pub fn parse_txids(param: &str) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for item in param.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if !crate::logic::valid_txid(item) {
            return Err(format!("malformed txid (expect 64 hex chars): {item:?}"));
        }
        let lc = item.to_ascii_lowercase();
        if !seen.insert(lc.clone()) {
            continue;
        }
        if out.len() >= TX_ANY_BATCH_MAX {
            return Err(format!("too many txids (max {TX_ANY_BATCH_MAX})"));
        }
        out.push(lc);
    }
    if out.is_empty() {
        return Err("empty txids parameter".to_string());
    }
    Ok(out)
}

/// bsv-low W-C.3 (gate MED-3) — the batched index-leg query for one table and
/// `n` txids: `txid IN (?,…)` bounded by the per-row byte budget. Pure, so it
/// is PREPARED against the production schema in `tests/sql_prepares_sqlite.rs`
/// like every other builder in this crate.
pub fn tx_any_index_leg_batch_sql(table: &str, proof_col: &str, n: usize) -> String {
    let marks = vec!["?"; n.max(1)].join(",");
    format!(
        "SELECT txid, hex(beef) AS beef, {proof_col} AS proofVerified FROM {table} \
         WHERE txid IN ({marks}) AND length(beef) <= {TX_ANY_BATCH_BEEF_MAX_BYTES}"
    )
}

/// bsv-low W-C.3 — the batched route's answer for ONE index read: exactly the
/// single route's decision when its index leg DECIDES (raw AND a verified-BUMP
/// height → `decide_tx_any` on the index alone), and `None` for anything the
/// index did not decide (the batch never runs the external leg — gate MED-1).
pub fn batch_index_answer(
    index_raw: Option<String>,
    index_height: Option<u64>,
) -> Option<TxAnyAnswer> {
    if index_raw.is_some() && index_height.is_some() {
        Some(decide_tx_any(
            index_raw,
            index_height,
            None,
            AbsenceCorroboration::Unknown,
        ))
    } else {
        None
    }
}

/// bsv-low W-C.3 — the fold of one chunk's served rows into `(resolved,
/// unresolved)`: a served row whose BEEF the extractor decodes for its txid is
/// resolved `(txid, raw, height)` — final at this table; a txid the table did
/// not serve, or served with bytes the extractor could not decode/extract for
/// it, is unresolved and falls through to the next table (then to `unknown`).
/// The extractor is injected so the fall-through semantics are pinned without
/// a BEEF fixture. Pure.
pub type IndexExtract<'a> = &'a dyn Fn(&str, &str, bool) -> Option<(String, Option<u64>)>;
/// A resolved index row: `(txid, raw hex, verified-BUMP height)`.
pub type IndexResolved = Vec<(String, String, Option<u64>)>;

pub fn fold_index_rows(
    served: &[(String, Option<String>, bool)],
    chunk: &[String],
    extract: IndexExtract<'_>,
) -> (IndexResolved, Vec<String>) {
    let mut resolved: IndexResolved = Vec::new();
    let mut hit: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (txid, beef, proof_verified) in served {
        if hit.contains(txid) {
            continue;
        }
        let Some(beef) = beef else { continue };
        if let Some((raw, height)) = extract(beef, txid, *proof_verified) {
            resolved.push((txid.clone(), raw, height));
            hit.insert(txid.clone());
        }
    }
    let unresolved = chunk
        .iter()
        .filter(|k| !hit.contains(*k))
        .cloned()
        .collect();
    (resolved, unresolved)
}

/// The one `/tx-any` answer as a JSON value (the single route's body, the
/// batched route's per-txid entry — one writer, so the two cannot drift).
pub fn tx_any_value(txid: &str, a: &TxAnyAnswer) -> serde_json::Value {
    json!({
        "txid": txid,
        "present": a.present,
        "confirmed": a.confirmed,
        "height": a.height,
        "rawHex": a.raw_hex,
        "source": a.source,
        "unconfirmable": a.unconfirmable,
    })
}

/// bsv-low W-C.3 — the batched body: `answers[txid]` = the single route's
/// body for that txid; `unknown` = the txids this batch did not answer, each
/// with its reason under `reasons` (`index-miss` | `index-unproven` |
/// `index-fault`, gate LOW-6) — listed apart, never a null.
pub fn tx_any_batch_body(answers: &[(String, TxAnyAnswer)], unknown: &[(String, &str)]) -> String {
    let mut map = serde_json::Map::new();
    for (txid, a) in answers {
        map.insert(txid.clone(), tx_any_value(txid, a));
    }
    let mut reasons = serde_json::Map::new();
    for (txid, why) in unknown {
        reasons.insert(txid.clone(), json!(why));
    }
    let unknown_ids: Vec<&str> = unknown.iter().map(|(t, _)| t.as_str()).collect();
    json!({
        "answers": serde_json::Value::Object(map),
        "unknown": unknown_ids,
        "reasons": serde_json::Value::Object(reasons),
    })
    .to_string()
}

pub fn tx_any_body(txid: &str, a: &TxAnyAnswer) -> String {
    tx_any_value(txid, a).to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// bsv-low #451 slice C (iii): a verdict memo answers for its own txid inside the hour, never older, never
    /// from the future, never for another txid.
    #[test]
    fn a_verdict_memo_answers_inside_the_hour_for_its_own_txid() {
        let now = 1_789_683_775_142_i64;
        let m = |age: i64| VerdictMemo {
            txid: "ab".repeat(32),
            verdict_at_ms: now - age,
            input_outpoint: format!("{}:0", "cd".repeat(32)),
            spender_txid: "ef".repeat(32),
            kind: VerdictKind::Unconfirmable,
            evidence: String::new(),
        };
        assert!(verdict_memo_answers(&m(1_000), &"AB".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "a minute old, case-insensitive");
        assert!(verdict_memo_answers(&m(VERDICT_MEMO_MAX_AGE_MS - 1), &"ab".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS));
        // slice C (iv), measured: a proven input conflict is TERMINAL — it answers at any age (a lapsed memo turned
        // every dead story into a recurring null on the felt)
        assert!(verdict_memo_answers(&m(VERDICT_MEMO_MAX_AGE_MS), &"ab".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "an hour old: an input conflict still answers");
        assert!(verdict_memo_answers(&m(30 * 24 * 60 * 60_000), &"ab".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "a month old: still terminal");
        let refused = |age: i64| VerdictMemo { kind: VerdictKind::Refused, ..m(age) };
        assert!(verdict_memo_answers(&refused(2 * VERDICT_MEMO_MAX_AGE_MS), &"ab".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "a corroborated refusal is terminal too");
        let absent = |age: i64| VerdictMemo { kind: VerdictKind::Absent, ..m(age) };
        assert!(verdict_memo_answers(&absent(VERDICT_MEMO_MAX_AGE_MS - 1), &"ab".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "a fresh absence answers");
        assert!(!verdict_memo_answers(&absent(VERDICT_MEMO_MAX_AGE_MS), &"ab".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "an hour-old absence is re-asked (an absence can end)");
        assert!(!verdict_memo_answers(&m(-5_000), &"ab".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "from the future: no");
        assert!(!verdict_memo_answers(&m(1_000), &"ff".repeat(32), now, VERDICT_MEMO_MAX_AGE_MS), "another txid: no");
    }

    /// bsv-low #451 slice C (iv): every memo kind is a corroborated `present: false`; only an input conflict sets
    /// `unconfirmable`; the index bytes ride beside the negative when held; a pre-152 row (no kind) is unconfirmable.
    #[test]
    fn a_memo_answers_present_false_and_only_a_conflict_is_unconfirmable() {
        let base = VerdictMemo { txid: "ab".repeat(32), verdict_at_ms: 1, input_outpoint: String::new(), spender_txid: String::new(), kind: VerdictKind::Absent, evidence: "network-absent".into() };
        let a = answer_from_verdict_memo(&base, Some("0100".into()));
        assert_eq!((a.present, a.confirmed, a.unconfirmable, a.raw_hex.as_deref(), a.source), (Some(false), None, false, Some("0100"), Some("memo")));
        let a = answer_from_verdict_memo(&VerdictMemo { kind: VerdictKind::Refused, ..base.clone() }, None);
        assert_eq!((a.present, a.unconfirmable, a.raw_hex), (Some(false), false, None));
        let a = answer_from_verdict_memo(&VerdictMemo { kind: VerdictKind::Unconfirmable, ..base }, Some("0100".into()));
        assert_eq!((a.present, a.unconfirmable), (Some(false), true), "an input conflict is the one unconfirmable kind");
        assert_eq!(VerdictKind::parse(None), Some(VerdictKind::Unconfirmable), "a pre-152 row");
        assert_eq!(VerdictKind::parse(Some("")), Some(VerdictKind::Unconfirmable));
        assert_eq!(VerdictKind::parse(Some("Absent")), Some(VerdictKind::Absent));
        assert_eq!(VerdictKind::parse(Some("refused")), Some(VerdictKind::Refused));
        assert_eq!(VerdictKind::parse(Some("nope")), None, "an unknown kind is no memo");
        assert_eq!(verdict_memo_read_many_sql(2), "SELECT txid, verdictAtMs, inputOutpoint, spenderTxid, kind, evidence FROM tx_any_verdicts WHERE txid IN (?, ?)");
        assert_eq!(verdict_memo_read_many_sql(1).replace("IN (?)", "= ?"), VERDICT_MEMO_READ_SQL, "the same columns as the single read");
    }

    /// bsv-low #451 slice B: an unconfirmable verdict lives 10 minutes in the isolate; everything else 15 s.
    #[test]
    fn unconfirmable_answers_live_longer_in_the_isolate() {
        let mut a = TxAnyAnswer::default();
        assert_eq!(tx_any_cache_ttl_ms(&a), TX_ANY_CACHE_TTL_MS);
        a.present = Some(false);
        a.unconfirmable = true;
        assert_eq!(tx_any_cache_ttl_ms(&a), TX_ANY_UNCONFIRMABLE_TTL_MS);
        assert!(tx_any_cache_ttl_ms(&a) > tx_any_cache_ttl_ms(&TxAnyAnswer::default()), "the negative outlives the ordinary answer");
    }

    /// bsv-low #451 slice B (the gate's HIGH-1 / HIGH-2 / MEDIUM-1): an allow-list with a freshness bound; MINED is
    /// a claim carrying its path and height; the orphan view, the pre-network ranks, a stale SEEN, a refusal, an
    /// empty status, a 404 and a fault all send the question to the couriers (Unknown).
    #[test]
    fn arcade_word_allow_list_and_freshness() {
        let now = 1_789_683_775_142_i64; // 2026-09-17T22:22:55.142Z
        let fresh = r#"{"txStatus":"SEEN_ON_NETWORK","timestamp":"2026-09-17T22:10:00Z"}"#;
        assert_eq!(parse_arcade_word(200, fresh, now), ArcadeWord::SeenFresh);
        let fresh2 = r#"{"txStatus":"seen_multiple_nodes","timestamp":"2026-09-17T22:22:50.5Z"}"#;
        assert_eq!(parse_arcade_word(200, fresh2, now), ArcadeWord::SeenFresh);
        let stale = r#"{"txStatus":"SEEN_ON_NETWORK","timestamp":"2026-09-17T21:52:00Z"}"#;
        assert_eq!(parse_arcade_word(200, stale, now), ArcadeWord::Unknown, "31 minutes old: the couriers decide");
        let future = r#"{"txStatus":"SEEN_ON_NETWORK","timestamp":"2026-09-17T23:30:00Z"}"#;
        assert_eq!(parse_arcade_word(200, future, now), ArcadeWord::Unknown, "a stamp from the future is no word");
        let unstamped = r#"{"txStatus":"SEEN_ON_NETWORK"}"#;
        assert_eq!(parse_arcade_word(200, unstamped, now), ArcadeWord::Unknown, "no stamp: not fresh");
        let zero = r#"{"txStatus":"SEEN_ON_NETWORK","timestamp":"0001-01-01T00:00:00Z"}"#;
        assert_eq!(parse_arcade_word(200, zero, now), ArcadeWord::Unknown);
        for st in ["SEEN_IN_ORPHAN_MEMPOOL", "MINED_IN_STALE_BLOCK", "RECEIVED", "STORED", "ANNOUNCED_TO_NETWORK", "REQUESTED_BY_NETWORK", "SENT_TO_NETWORK", "ACCEPTED_BY_NETWORK", "QUEUED", "UNKNOWN", "REJECTED", "DOUBLE_SPEND_ATTEMPTED", ""] {
            let body = format!(r#"{{"txStatus":"{st}","timestamp":"2026-09-17T22:22:00Z"}}"#);
            assert_eq!(parse_arcade_word(200, &body, now), ArcadeWord::Unknown, "not on the allow-list: the couriers decide — judged {body}");
        }
        let mined = r#"{"txStatus":"MINED","blockHeight":965000,"merklePath":"FE0A0B0C","timestamp":"2026-01-01T00:00:00Z"}"#;
        assert_eq!(parse_arcade_word(200, mined, now), ArcadeWord::Mined { bump_hex: "fe0a0b0c".into(), height: 965000 }, "a MINED word is a claim, age-free, carried for verification");
        assert_eq!(parse_arcade_word(200, r#"{"txStatus":"IMMUTABLE","blockHeight":1,"merklePath":"aa"}"#, now), ArcadeWord::Mined { bump_hex: "aa".into(), height: 1 });
        assert_eq!(parse_arcade_word(200, r#"{"txStatus":"MINED","blockHeight":965000}"#, now), ArcadeWord::Unknown, "MINED without a path: nothing to verify");
        assert_eq!(parse_arcade_word(200, r#"{"txStatus":"MINED","merklePath":"aa"}"#, now), ArcadeWord::Unknown, "MINED without a height: nothing to verify against");
        assert_eq!(parse_arcade_word(200, r#"{"txStatus":"MINED","blockHeight":0,"merklePath":"aa"}"#, now), ArcadeWord::Unknown);
        assert_eq!(parse_arcade_word(200, r#"{"txStatus":"MINED","blockHeight":5,"merklePath":"zz"}"#, now), ArcadeWord::Unknown, "a non-hex path is no bump");
        assert_eq!(parse_arcade_word(404, fresh, now), ArcadeWord::Unknown, "a 404 body is not a look");
        assert_eq!(parse_arcade_word(200, "not json", now), ArcadeWord::Unknown);
        assert_eq!(parse_arcade_word(503, "", now), ArcadeWord::Unknown);
    }

    /// bsv-low #451 slice B: Arcade's status stamp read as an instant (the freshness bound rests on it).
    #[test]
    fn rfc3339_utc_ms_reads_arcade_stamps() {
        assert_eq!(rfc3339_utc_ms("2026-09-17T22:22:55.142485Z"), Some(1_789_683_775_142));
        assert_eq!(rfc3339_utc_ms("2026-09-17T22:22:55Z"), Some(1_789_683_775_000));
        assert_eq!(rfc3339_utc_ms("2026-01-01T00:00:00Z"), Some(1_767_225_600_000));
        assert_eq!(rfc3339_utc_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339_utc_ms("2026-09-17T22:22:55+00:00"), Some(1_789_683_775_000));
        assert_eq!(rfc3339_utc_ms("0001-01-01T00:00:00Z").map(|v| v < 0), Some(true), "Arcade's zero stamp is far in the past, never fresh");
        for bad in ["", "2026-09-17", "2026-09-17T22:22:55", "2026-13-01T00:00:00Z", "2026-09-17T25:00:00Z", "not a time", "2026-09-17T22:22:55.abcZ",
                    "2026-09-31T00:00:00Z", "2026-02-29T00:00:00Z", "2026-04-31T00:00:00Z", "2026-09-00T00:00:00Z", "2026-09-17T-1:00:00Z", "2026-09-17T22:+5:00Z", "2026-09-17T22:22:-0Z"] {
            assert_eq!(rfc3339_utc_ms(bad), None, "judged {bad:?}");
        }
        // the delta-verify's LOW-C: the day is bound by its month, leap years included
        assert_eq!(rfc3339_utc_ms("2024-02-29T00:00:00Z"), Some(1_709_164_800_000));
        assert!(rfc3339_utc_ms("2000-02-29T00:00:00Z").is_some(), "400-year leap");
        assert_eq!(rfc3339_utc_ms("1900-02-29T00:00:00Z"), None, "100-year non-leap");
        assert_eq!(days_in_month(2026, 2), 28);
        assert_eq!(days_in_month(2028, 2), 29);
        assert_eq!(days_in_month(2026, 13), 0);
    }

    /// bsv-low #451 slice B: a bumpless index row + Arcade's live word = present (unconfirmed / confirmed), the raw
    /// served from the index, source `index+external` — the same shape a WoC witness produced.
    #[test]
    fn arcade_witness_beside_index_bytes_decides_like_woc() {
        let raw = "0100".to_string();
        // the route builds the observation from the word: SeenFresh → unconfirmed presence; a VERIFIED Mined → confirmed
        let seen = TxObservation::Present { confirmed: false, raw_hex: None };
        let a = decide_tx_any(Some(raw.clone()), None, Some(&seen), AbsenceCorroboration::Unknown);
        assert_eq!((a.present, a.confirmed, a.raw_hex.as_deref(), a.source), (Some(true), Some(false), Some("0100"), Some("index+external")));
        let mined = TxObservation::Present { confirmed: true, raw_hex: None };
        let a = decide_tx_any(Some(raw), None, Some(&mined), AbsenceCorroboration::Unknown);
        assert_eq!((a.present, a.confirmed, a.source), (Some(true), Some(true), Some("index+external")));
    }

    fn raw() -> String {
        "aabbccdd00".into() // opaque placeholder bytes — the decision table never parses them
    }

    #[test]
    fn index_leg_with_bump_is_fully_native() {
        // Even a contradicting external observation is irrelevant — the index
        // never consults it once the BUMP proves the mine.
        let a = decide_tx_any(
            Some(raw()),
            Some(958_886),
            Some(&TxObservation::Absent),
            AbsenceCorroboration::CorroboratedAbsent,
        );
        assert_eq!(a.present, Some(true));
        assert_eq!(a.confirmed, Some(true));
        assert_eq!(a.height, Some(958_886));
        assert_eq!(a.raw_hex, Some(raw()));
        assert_eq!(a.source, Some("index"));
    }

    #[test]
    fn index_leg_without_bump_defers_presence_to_the_external_leg() {
        // bsv-low #247: own-store bytes with no BUMP are not network truth.
        // An external positive corroborates presence (and confirmation).
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Present {
                confirmed: true,
                raw_hex: None,
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), Some(true)));
        assert_eq!(a.source, Some("index+external"));

        // External present-but-unconfirmed still corroborates PRESENCE
        // (mempool) — confirmed honestly false.
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Present {
                confirmed: false,
                raw_hex: None,
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), Some(false)));
        assert_eq!(a.source, Some("index+external"));

        // THE #247 fix: a CORROBORATED double-404 for an index-held,
        // bump-less tx is an honest network-absent (the zombie orphan JOIN
        // class) — present:false, raw still served (the caller's own bytes).
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Absent),
            AbsenceCorroboration::CorroboratedAbsent,
        );
        assert_eq!((a.present, a.confirmed), (Some(false), None));
        assert_eq!(a.raw_hex, Some(raw()));
        assert_eq!(a.source, Some("index+external"));

        // An UNCORROBORATED 404 / a fault is the honest unknown — never a
        // negative on one provider's word, and no longer a fabricated
        // positive from our own store either.
        for (external, absence) in [
            (TxObservation::Absent, AbsenceCorroboration::Unknown),
            (TxObservation::Fault, AbsenceCorroboration::Unknown),
            (
                TxObservation::Fault,
                AbsenceCorroboration::CorroboratedAbsent,
            ),
        ] {
            let a = decide_tx_any(Some(raw()), None, Some(&external), absence);
            assert_eq!((a.present, a.confirmed), (None, None), "{external:?}");
            assert_eq!(a.raw_hex, Some(raw()), "raw is still served");
            assert_eq!(a.source, Some("index"));
        }
    }

    #[test]
    fn unconfirmable_requires_a_confirmed_conflicting_spender() {
        // Provably unconfirmable: an input spent by a DIFFERENT confirmed tx.
        let subject = "aa".repeat(32);
        let other = "bb".repeat(32);
        assert!(input_proves_unconfirmable(
            &subject,
            true,
            Some(true),
            Some(&other),
            Some(true)
        ));
        // Everything weaker proves NOTHING (fail toward retry):
        // spent by the subject itself (i.e. the subject IS the spender),
        for (known, spent, spender, conf) in [
            (true, Some(true), Some(subject.as_str()), Some(true)), // self-spend
            (true, Some(true), Some(other.as_str()), Some(false)),  // unconfirmed conflict
            (true, Some(true), Some(other.as_str()), None),         // confirmation unknown
            (true, Some(false), None, None),                        // unspent
            (false, Some(true), Some(other.as_str()), Some(true)),  // unverified read
            (true, None, Some(other.as_str()), Some(true)),         // spend unknown
        ] {
            assert!(
                !input_proves_unconfirmable(&subject, known, spent, spender, conf),
                "({known},{spent:?},{spender:?},{conf:?}) must not prove unconfirmable"
            );
        }
    }

    #[test]
    fn external_positive_requires_verified_raw() {
        // Verified raw in hand → positive with the raw served.
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Present {
                confirmed: true,
                raw_hex: Some(raw()),
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), Some(true)));
        assert_eq!(a.raw_hex, Some(raw()));
        assert_eq!(a.source, Some("external"));

        // A bare pointer whose raw could not be verified is an honest
        // unknown — never a positive off an unverified claim.
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Present {
                confirmed: true,
                raw_hex: None,
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!(a, TxAnyAnswer::default());
    }

    #[test]
    fn absence_requires_corroboration() {
        // WoC 404 alone → unknown (one provider's negative is never the
        // network verdict).
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Absent),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!(a, TxAnyAnswer::default());

        // Both 404 + healthy route → provably absent.
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Absent),
            AbsenceCorroboration::CorroboratedAbsent,
        );
        assert_eq!(a.present, Some(false));
        assert_eq!(a.source, Some("external"));
    }

    #[test]
    fn faults_are_unknown() {
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Fault),
            AbsenceCorroboration::CorroboratedAbsent, // even a "corroborated" absence can't rescue a WoC fault
        );
        assert_eq!(a, TxAnyAnswer::default());
        let a = decide_tx_any(None, None, None, AbsenceCorroboration::Unknown);
        assert_eq!(a, TxAnyAnswer::default());
    }

    #[test]
    fn woc_confirmations_parse() {
        assert!(parse_woc_confirmations(&json!({"confirmations": 3})));
        assert!(!parse_woc_confirmations(&json!({"confirmations": 0})));
        assert!(!parse_woc_confirmations(&json!({})));
        assert!(!parse_woc_confirmations(&json!({"confirmations": "3"})));
    }

    #[test]
    fn raw_verification_binds_the_hash() {
        // A real minimal tx: version|0 inputs|0 outputs|locktime.
        let bytes = hex::decode("01000000000000000000").unwrap();
        let txid = bsv_rs::transaction::Transaction::from_binary(&bytes)
            .unwrap()
            .id();
        assert_eq!(verify_raw_bytes(&bytes, &txid), Some(hex::encode(&bytes)));
        // Wrong txid → refused.
        assert_eq!(verify_raw_bytes(&bytes, &"0".repeat(64)), None);
        // Garbage bytes → refused.
        assert_eq!(verify_raw_bytes(&[0x00, 0x01], &txid), None);
    }

    #[test]
    fn wire_body_shape() {
        let a = TxAnyAnswer {
            present: Some(true),
            confirmed: Some(true),
            height: Some(1),
            raw_hex: Some("aa".into()),
            source: Some("index"),
            unconfirmable: false,
        };
        let v: serde_json::Value = serde_json::from_str(&tx_any_body("ab", &a)).unwrap();
        assert_eq!(v["txid"], "ab");
        assert_eq!(v["present"], true);
        assert_eq!(v["confirmed"], true);
        assert_eq!(v["height"], 1);
        assert_eq!(v["rawHex"], "aa");
        assert_eq!(v["source"], "index");
        assert_eq!(v["unconfirmable"], false);
        let empty: serde_json::Value =
            serde_json::from_str(&tx_any_body("ab", &TxAnyAnswer::default())).unwrap();
        assert!(empty["present"].is_null());
        assert!(empty["source"].is_null());
    }

    // ── bsv-low W-C.3 (2026-09-09/10): the batched, index-only route ──

    fn mined() -> TxAnyAnswer {
        TxAnyAnswer {
            present: Some(true),
            confirmed: Some(true),
            height: Some(965_000),
            raw_hex: Some(raw()),
            source: Some("index"),
            unconfirmable: false,
        }
    }

    #[test]
    fn parse_txids_lowercases_dedupes_bounds_and_refuses_any_malformed_item() {
        let a = "ab".repeat(32);
        let b = "cd".repeat(32);
        assert_eq!(
            parse_txids(&format!("{},{a},{b},,", a.to_uppercase())).unwrap(),
            vec![a.clone(), b.clone()]
        );
        assert!(parse_txids("").unwrap_err().contains("empty"));
        assert!(parse_txids(",,").unwrap_err().contains("empty"));
        assert!(parse_txids("abcd").unwrap_err().contains("malformed txid"));
        assert!(parse_txids(&format!("{a},zz"))
            .unwrap_err()
            .contains("malformed txid"));
        let fifty: Vec<String> = (0..50).map(|i| format!("{:064x}", i + 1)).collect();
        assert_eq!(parse_txids(&fifty.join(",")).unwrap().len(), 50);
        let mut dup = fifty.clone();
        dup.push(fifty[0].clone());
        assert_eq!(parse_txids(&dup.join(",")).unwrap().len(), 50);
        let mut over = fifty;
        over.push(format!("{:064x}", 99));
        assert!(parse_txids(&over.join(","))
            .unwrap_err()
            .contains("too many txids"));
    }

    #[test]
    fn the_batched_entry_is_the_single_body_and_unknowns_stay_apart_with_reasons() {
        let a = "ab".repeat(32);
        let u = "ee".repeat(32);
        let f = "ff".repeat(32);
        let body: serde_json::Value = serde_json::from_str(&tx_any_batch_body(
            &[(a.clone(), mined())],
            &[(u.clone(), "index-miss"), (f.clone(), "index-fault")],
        ))
        .unwrap();
        let single: serde_json::Value = serde_json::from_str(&tx_any_body(&a, &mined())).unwrap();
        assert_eq!(body["answers"][&a], single);
        assert!(
            body["answers"].get(&u).is_none(),
            "an unknown txid is NOT in answers"
        );
        assert_eq!(body["unknown"], json!([u, f]));
        assert_eq!(body["reasons"][&u], json!("index-miss"));
        assert_eq!(body["reasons"][&f], json!("index-fault"));
    }

    /// The equivalence the batch rests on: an index HIT is answered by the very
    /// call the single route makes for an index hit; anything else is not
    /// answered at all (never a guess, never the external leg).
    #[test]
    fn the_batch_answers_an_index_hit_exactly_as_the_single_route_and_nothing_else() {
        let single = decide_tx_any(Some(raw()), Some(7), None, AbsenceCorroboration::Unknown);
        assert_eq!(
            batch_index_answer(Some(raw()), Some(7)),
            Some(single.clone())
        );
        assert_eq!(single.present, Some(true));
        assert_eq!(single.confirmed, Some(true));
        assert_eq!(
            batch_index_answer(Some(raw()), None),
            None,
            "a proofless row is not decided"
        );
        assert_eq!(batch_index_answer(None, Some(7)), None);
        assert_eq!(batch_index_answer(None, None), None);
    }

    /// The batched index leg's only real logic: a served, decodable row is a
    /// hit (final at its table); an undecodable row and an unserved txid fall
    /// through; a duplicate row never double-counts.
    #[test]
    fn fold_index_rows_hits_are_final_and_everything_else_falls_through() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        let extract = |beef: &str, txid: &str, pv: bool| -> Option<(String, Option<u64>)> {
            if beef == "bad" {
                return None;
            }
            Some((format!("raw-{txid}"), if pv { Some(1) } else { None }))
        };
        let served = vec![
            (a.clone(), Some("good".to_string()), true),
            (b.clone(), Some("bad".to_string()), true),
            (a.clone(), Some("good".to_string()), false), // a duplicate row
            (c.clone(), None, true),                      // served without bytes
        ];
        let chunk = vec![a.clone(), b.clone(), c.clone(), "dd".repeat(32)];
        let (resolved, unresolved) = fold_index_rows(&served, &chunk, &extract);
        assert_eq!(resolved, vec![(a.clone(), format!("raw-{a}"), Some(1))]);
        assert_eq!(unresolved, vec![b, c, "dd".repeat(32)]);
    }

    #[test]
    fn the_batched_index_sql_carries_one_mark_per_txid_and_the_byte_bound() {
        let sql = tx_any_index_leg_batch_sql("pot_beefs", "proof_verified", 3);
        assert_eq!(sql.matches('?').count(), 3);
        assert!(sql.contains("length(beef) <= 262144"));
        assert!(sql.contains("FROM pot_beefs"));
        assert!(tx_any_index_leg_batch_sql("transactions", "has_proof", 1)
            .contains("has_proof AS proofVerified"));
    }
}
