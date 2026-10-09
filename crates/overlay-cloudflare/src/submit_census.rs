//! `/submit` broadcast-gated READINESS CENSUS (bsv-low #366) — measurement
//! only, never a gate.
//!
//! ## The number nothing was measuring
//!
//! bsv-low #347 will eventually flip the `/submit` default and DELETE the
//! `HistoricalUngated` mode. Its first flip criterion is "every honest CLIENT
//! submit would survive `broadcast-gated`". The tower's overlay legs already
//! have a counted, paged receipt (bsv-low #351); the CLIENT's ten overlay
//! surfaces have only a `console.warn` (`overlay.ts submitBeef` computes
//! `beefGatedReadiness(...)` browser-side) — and nothing reads a browser
//! console. This module derives the answer SERVER-SIDE (#366 Option 3): the
//! overlay already sees every submitted body, so for every submit served on an
//! UNGATED path it classifies, without broadcasting, whether that same body
//! would have survived the gated path's pre-network structural checks.
//!
//! ## What this is NOT (the hard boundary)
//!
//! The census **never broadcasts, never refuses, never alters the admission
//! outcome of any request**. It runs strictly AFTER the #347 admission
//! decision (`submit_gate::action_for`) and only on submits that PROCEED
//! WITHOUT the gate; its output is a log line plus durable counters. Deleting
//! the whole census leaves every request's status code and admission byte-
//! identical — that property is what makes it shippable without a redeploy
//! gate on the admission design.
//!
//! ## The predicate, and why it is not a re-derivation (Rule 10 posture)
//!
//! "Would this body survive `broadcast-gated`?" is answered by calling the
//! SAME functions the gated arm calls, in the same order, on the same bytes:
//!
//! 1. [`crate::ef::beef_to_ef_batch`] — the gated arm 400s on `Err` before any
//!    network call (`Parse` / subject `EfConversion`).
//! 2. (none since NL-6c) the gated arm weighs the EF work against one
//!    request's budget and DEFERS the work past it to its queue
//!    (`crate::ef_deferred`); until NL-6c it answered 429, and the census
//!    counted that `EfOverCap`. A deferral is not a refusal, so a body past the
//!    budget is classified as every other body is, by the checks below.
//! 3. empty `efs` (an all-proven "already mined" claim):
//!    [`crate::ef::proven_subject_raw`] — `None` means the gated arm's
//!    mined-claim corroboration has no body and refuses fail-closed (502,
//!    deterministic for these bytes).
//!
//! There is no second spelling of any of those checks here — the census would
//! drift only if the gated arm grew a NEW pre-network structural check without
//! this sequence learning it. That residual is covered empirically at the
//! route tier (`tools/lane-366/census_route_ci.mjs` posts the same bytes to
//! the REAL `broadcast-gated` route and asserts the census class agrees with
//! the route's own structural verdict), not by a second derivation.
//!
//! ## The three states (Rule 13 — the third is never collapsed)
//!
//! * **`GatedReady`** — the body passes every pre-network structural check the
//!   gated arm runs. (Whether the NETWORK would then accept the tx is the
//!   gate's own question and is unknowable without broadcasting; for the
//!   honest population the tx was already broadcast by the wallet, and the
//!   gated path admits an already-known tx idempotently.)
//! * **`WouldHaveFailed`** — the gated arm would have refused these bytes
//!   BEFORE any network call (flat 400 / fail-closed mined-claim 502).
//!   This is THE number the #347 flip reads: every count here is an honest
//!   submit the flip would strand.
//! * **`CouldNotEvaluate`** — the census could not honestly decide. Two
//!   named classes: an all-proven mined-claim whose fate is the network
//!   corroboration this census must not perform; and a body whose
//!   sorted-last "subject" does not cover the BEEF with its ancestry (subject
//!   identification unreliable). Never folded into either other state — a
//!   flip decision that reads 0 fails must ALSO read (and reason about) this
//!   bucket.
//!
//! ## No stop by size (bsv-low #585, door 2)
//!
//! Until #585 a body over 2 MiB was a third class of the third state
//! (`body-over-eval-bound`: not evaluated at all). A valid BEEF is never left
//! unclassified for its size: that stop is gone, and so is the census's own
//! bounded parse (`CENSUS_BEEF_LIMITS`). What the census adds to the gated
//! arm's functions, the ancestry check below, reads the STREAM (bsv-rs
//! 0.4.0's `BeefStream`, one element at a time) into an index of one number
//! per raw transaction and one pair per in-BEEF spend; it hydrates nothing.
//! The durable row `submit_census_reason_body_over_eval_bound_total` stays in
//! the read table (a total already counted is not un-counted) and is never
//! bumped again.
//!
//! The gated arm's own functions (step 1 and step 3 above) still parse with
//! `beef_limits::parse_beef` under `EF_BEEF_LIMITS`: a body past 10,000,036
//! bytes, 512 transactions or 512 BUMPs is `would-fail(parse)` here because
//! the gated arm refuses those bytes at that same call. That refusal is the
//! arm's (NL-6's module), mirrored, not the census's.
//!
//! ## Divergence from the client predicate, stated rather than hidden
//!
//! `overlay.ts beefGatedReadiness` (bsv-low) answers a slightly different
//! question — "can I prove this body is ready?" — and deliberately fails safe
//! where it cannot identify the subject. This census answers "what would the
//! gated ROUTE actually have done?", so it uses the route's own subject
//! choice (`sort_txs(); last()`, inside `beef_to_ef_batch`). Two mapped
//! divergences, pinned by the cross-language fixture test
//! (`census_parity_fixture_agrees`):
//! * client `ready:true` on a proven (mined-claim) subject ⇒ census
//!   `CouldNotEvaluate(MinedClaimUnverified)` — the client trusts its own
//!   bump-commitment check; the route trusts only a network corroboration.
//! * client `ready:false` "subject was not named" ⇒ census still classifies
//!   (the route has no notion of a caller-named subject).
//!
//! ## RESIDUAL (named, not hidden): the census counts only submits that ARRIVE
//!
//! An overlay outage is exactly when the client is least ready, and nothing
//! here can see a submit that never reached `/submit`. That residual stays
//! with the client-side `console.warn` and with bsv-low #351's producer
//! receipts; a flip decision must treat this census as "of the traffic the
//! overlay served", never "of the traffic the client attempted".
//!
//! ## Where the count lives, and its trust posture
//!
//! Durable name-keyed rows in the existing `ops_counters` table (additive
//! upsert via [`crate::ops::bump_counter`] — no schema migration). Cost
//! split, stated precisely (gate LOW-1): the CLASSIFICATION
//! ([`census_verdict`]) runs SYNCHRONOUSLY on every ungated submit — the
//! gated arm's own parse and the subject's `to_ef` conversions, then two
//! reads of the stream and an ancestor-closure walk over its index, with no
//! bound of their own (the route's request cap is the body's) — while
//! only the durable D1 WRITE is backgrounded (`ctx.wait_until`), so the
//! write adds no caller latency. Read on `GET /health/invariants` as
//! `submitReadinessCensus`. Counters
//! are MONOTONIC TOTALS: "the last N" is a delta between two reads, and an
//! observation window whose `observed` delta is 0 is NO EVIDENCE — it carries
//! a streak, it never credits one (the bsv-low #341
//! `unvalidated_share_bps() -> None` posture). `/health/invariants` is
//! unauthenticated and `/submit` is public, so the counters are
//! attacker-inflatable upward — a soak/flip SIGNAL to corroborate against
//! producer-side facts, never an audit log (same posture as
//! `submit_gate::counters_json`).

use std::collections::HashMap;

use bsv_rs::transaction::beef_stream::{BeefStream, Element, Hash32};

use crate::ef::{beef_to_ef_batch, proven_subject_raw, EfError};
use crate::submit_gate::AdmissionPath;

/// Why the gated arm would have refused these bytes before any network call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WouldFailWhy {
    /// `Beef::from_binary` failed — the gated arm 400s.
    Parse,
    /// The SUBJECT could not be converted to Extended Format (txid-only
    /// subject, missing input source txs — THE bsv-low #351 class) — the
    /// gated arm 400s.
    SubjectEf,
    /// NOT PRODUCED since NL-6c: the gated arm answered 429 past its EF work
    /// bound until then, and now defers that work to its queue
    /// (`crate::ef_deferred`). Kept so the counter's name
    /// (`submit_census_reason_ef_over_cap_total`) and its served key stay
    /// readable with the counts taken before.
    EfOverCap,
    /// All-proven mined-claim shape with no extractable subject raw — the
    /// gated arm's corroboration has no body and fails closed (deterministic
    /// 502 for these bytes).
    MinedClaimUnextractable,
}

/// Why the census could not decide. NEVER collapsed into ready or fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnevalWhy {
    /// All-proven mined-claim shape: the gated outcome is the network
    /// corroboration (bsv-low #268) that a census must not perform.
    MinedClaimUnverified,
    /// A data-carrying BEEF entry sits OUTSIDE the sorted-last "subject"'s
    /// in-BEEF ancestor closure — the route's subject identification
    /// (`sort_txs(); last()`) is unreliable for this body, so a structural
    /// green would be a false one.
    ///
    /// FOUND BY THE CROSS-LANGUAGE FIXTURE, not by design: a partial-ancestry
    /// body (subject spending one in-BEEF and one absent parent) sorts the
    /// REAL subject into bsv-rs's `with_missing_inputs` front group, leaving a
    /// complete ANCESTOR last. The first census read that body `GatedReady`
    /// while the client predicate (whose gate A HIGH-2 documents exactly this
    /// sort) said not-ready. The gated route itself would broadcast the wrong
    /// tx and land on the NETWORK's verdict for it — unknowable here, so the
    /// honest state is the third one. An honest submit never trips this: its
    /// subject is the tip of its own ancestry, so the closure covers every
    /// entry (see [`beef_has_entries_outside_subject_ancestry`] for the
    /// probe-earned generalisation from "childless" to "closure-covering").
    ///
    /// Also the answer when the ancestry cannot be READ (bsv-low #585): the
    /// gated arm's parser took the bytes and the streaming reader refuses
    /// them (a byte after the frame's end, a BUMP whose nodes disagree). The
    /// green cannot be vouched for, so it is not given.
    SubjectAmbiguous,
}

/// The census classification of one arriving body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CensusVerdict {
    /// Passes every pre-network structural check of the gated arm.
    GatedReady,
    /// The gated arm would have refused before any network call.
    WouldHaveFailed(WouldFailWhy),
    /// The census cannot honestly decide (named reason).
    CouldNotEvaluate(UnevalWhy),
}

impl CensusVerdict {
    /// Stable name for logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GatedReady => "gated-ready",
            Self::WouldHaveFailed(WouldFailWhy::Parse) => "would-fail(parse)",
            Self::WouldHaveFailed(WouldFailWhy::SubjectEf) => "would-fail(subject-ef)",
            Self::WouldHaveFailed(WouldFailWhy::EfOverCap) => "would-fail(ef-over-cap)",
            Self::WouldHaveFailed(WouldFailWhy::MinedClaimUnextractable) => {
                "would-fail(mined-claim-unextractable)"
            }
            Self::CouldNotEvaluate(UnevalWhy::MinedClaimUnverified) => {
                "could-not-evaluate(mined-claim-unverified)"
            }
            Self::CouldNotEvaluate(UnevalWhy::SubjectAmbiguous) => {
                "could-not-evaluate(subject-ambiguous)"
            }
        }
    }
}

/// Classify one arriving `/submit` body: would it have survived the
/// `broadcast-gated` path's pre-network structural checks?
///
/// PURE and network-free. Mirrors the gated arm by CALLING its functions —
/// see the module doc for the exact 1:1 mapping and the route-tier cell that
/// pins the agreement empirically.
///
/// A body of any size is classified (bsv-low #585): there is no stop by size
/// here.
pub fn census_verdict(beef_bytes: &[u8]) -> CensusVerdict {
    let (efs, subject_txid) = match beef_to_ef_batch(beef_bytes) {
        Ok(v) => v,
        Err(EfError::Parse(_)) => return CensusVerdict::WouldHaveFailed(WouldFailWhy::Parse),
        Err(EfError::EfConversion(_)) => {
            return CensusVerdict::WouldHaveFailed(WouldFailWhy::SubjectEf)
        }
    };
    // NL-6c: no EF size check here. The gated arm defers work past one
    // request's budget to its queue (`crate::ef_deferred`) and refuses
    // nothing for it, so a body past the budget is not one it would refuse.
    if efs.is_empty() {
        // All-proven "already mined" claim. The gated arm corroborates the
        // claim against a real provider (bsv-low #268) — a network call this
        // census must not make — UNLESS the subject raw cannot even be
        // extracted, in which case the arm fails closed deterministically.
        return match proven_subject_raw(beef_bytes) {
            Some(_) => CensusVerdict::CouldNotEvaluate(UnevalWhy::MinedClaimUnverified),
            None => CensusVerdict::WouldHaveFailed(WouldFailWhy::MinedClaimUnextractable),
        };
    }
    // A structural GREEN is only honest if the sorted-last entry actually IS
    // the subject. In an honest submit the subject is the TIP of its own
    // ancestry: every data-carrying entry in the BEEF is reachable from it by
    // walking input source references. Any stray entry outside that closure
    // means the sort was poisoned (the real subject sits in a front group)
    // and the gated route would broadcast the wrong tx — see
    // `UnevalWhy::SubjectAmbiguous`. Only the green needs this guard: a
    // structural refusal (above) stands regardless of which tx the caller
    // meant.
    match beef_has_entries_outside_subject_ancestry(beef_bytes, &subject_txid) {
        Some(false) => CensusVerdict::GatedReady,
        // A stray entry, or an ancestry the stream cannot read: no green.
        Some(true) | None => CensusVerdict::CouldNotEvaluate(UnevalWhy::SubjectAmbiguous),
    }
}

/// Is any data-carrying BEEF entry OUTSIDE the sorted-last subject's in-BEEF
/// ancestor closure?
///
/// An honest submit's subject is the tip of its own ancestry, so the closure
/// covers the whole body — including the recovery shape whose unconvertible
/// ancestor is still REFERENCED by the subject (in-closure, not a stray).
/// Probe history, because each spelling was earned by a NOT-RED:
/// * v1 asked "does anything spend the subject?" — caught the fixture's
///   partial-ancestry mis-sort, but went blind the moment the stray's
///   dangling reference pointed outside the BEEF entirely (RED-probe F2: one
///   byte flipped in a child's prevout txid left the stray unlinked to the
///   sorted-last parent, and the census read a false green again).
/// * the closure test subsumes v1: a stray that spends the subject and a
///   stray that spends nothing in the BEEF are both simply NOT ANCESTORS.
///
/// Read from the STREAM (bsv-low #585), never from a hydrated BEEF: the
/// answer needs only which transactions carry data and which of them spend
/// which ([`AncestryShape`]). `None` when the streaming reader refuses the
/// bytes: the caller gives no green on a non-answer.
fn beef_has_entries_outside_subject_ancestry(
    beef_bytes: &[u8],
    subject_txid: &str,
) -> Option<bool> {
    let shape = AncestryShape::read(beef_bytes)?;
    let subject = bsv_rs::primitives::from_hex(subject_txid)
        .ok()
        .and_then(|bytes| Hash32::try_from(bytes).ok())
        .map(|mut wire| {
            wire.reverse();
            wire
        });
    Some(shape.has_entry_outside(subject.as_ref()))
}

/// The shape of a BEEF's ancestry, as the census needs it: a number per
/// data-carrying transaction (mirrors the gated arm's source map: `if let
/// Some(tx) = btx.tx()`; a txid-only stub carries nothing that could be
/// mis-broadcast, so it is neither a closure member nor a stray) and a pair
/// per input that spends another data-carrying transaction of the same BEEF:
/// an entry of 36 bytes a transaction and a pair of 8 a spend, whatever the
/// transactions weigh (74 bytes per element measured, the tables' slack
/// included).
struct AncestryShape {
    ids: HashMap<Hash32, u32>,
    /// (spender, source), sorted.
    spends: Vec<(u32, u32)>,
}

impl AncestryShape {
    /// Two reads of the stream, one element in hand at a time: the first
    /// numbers the transactions, the second keeps each input whose source has
    /// a number (a source may lie later in the stream than its spender, and
    /// an input that names a transaction outside the BEEF is not kept).
    fn read(beef_bytes: &[u8]) -> Option<Self> {
        let mut ids: HashMap<Hash32, u32> = HashMap::new();
        let mut stream = BeefStream::new(beef_bytes);
        while let Some(element) = stream.next_element().ok()? {
            if let Element::Tx { txid, .. } = element {
                let next = u32::try_from(ids.len()).ok()?;
                ids.entry(txid).or_insert(next);
            }
        }
        let mut spends: Vec<(u32, u32)> = Vec::new();
        let mut stream = BeefStream::new(beef_bytes);
        while let Some(element) = stream.next_element().ok()? {
            if let Element::Tx { txid, body, .. } = element {
                let spender = *ids.get(&txid)?;
                spends.extend(
                    body.inputs
                        .iter()
                        .filter_map(|input| ids.get(&input.prev))
                        .map(|source| (spender, *source)),
                );
            }
        }
        spends.sort_unstable();
        spends.dedup();
        Some(Self { ids, spends })
    }

    /// Is any transaction outside the closure of `subject` over the spends?
    fn has_entry_outside(&self, subject: Option<&Hash32>) -> bool {
        let mut inside = vec![false; self.ids.len()];
        let mut frontier: Vec<u32> = subject
            .and_then(|txid| self.ids.get(txid))
            .copied()
            .into_iter()
            .collect();
        while let Some(tx) = frontier.pop() {
            if std::mem::replace(&mut inside[tx as usize], true) {
                continue;
            }
            let from = self.spends.partition_point(|(spender, _)| *spender < tx);
            frontier.extend(
                self.spends[from..]
                    .iter()
                    .take_while(|(spender, _)| *spender == tx)
                    .map(|(_, source)| *source)
                    .filter(|source| !inside[*source as usize]),
            );
        }
        inside.contains(&false)
    }

    /// The heap the index holds, by its capacity.
    #[cfg(test)]
    fn heap_bytes(&self) -> usize {
        self.ids.capacity() * (std::mem::size_of::<(Hash32, u32)>() + 1)
            + self.spends.capacity() * std::mem::size_of::<(u32, u32)>()
    }
}

// ── durable counter names (rows in `ops_counters`; read by `ops::census_json`) ─

/// Every state counter, `(name, mode, population, state)` — the read side and
/// the tests iterate THIS table, so a name cannot drift between write and read.
///
/// `population`: `client` = the unauthenticated lenient-window population (the
/// exact population the #347 flip criterion is about); `operator` = submits
/// carrying the `SUBMIT_OPERATOR_TOKEN` (the tower/peer/migration population,
/// which #351 receipts also cover from the producer side).
pub const CENSUS_STATE_COUNTERS: [(&str, &str, &str, &str); 18] = [
    (
        "submit_census_current_tx_client_ready_total",
        "current-tx",
        "client",
        "ready",
    ),
    (
        "submit_census_current_tx_client_would_fail_total",
        "current-tx",
        "client",
        "would_fail",
    ),
    (
        "submit_census_current_tx_client_uneval_total",
        "current-tx",
        "client",
        "uneval",
    ),
    (
        "submit_census_current_tx_operator_ready_total",
        "current-tx",
        "operator",
        "ready",
    ),
    (
        "submit_census_current_tx_operator_would_fail_total",
        "current-tx",
        "operator",
        "would_fail",
    ),
    (
        "submit_census_current_tx_operator_uneval_total",
        "current-tx",
        "operator",
        "uneval",
    ),
    (
        "submit_census_historical_tx_client_ready_total",
        "historical-tx",
        "client",
        "ready",
    ),
    (
        "submit_census_historical_tx_client_would_fail_total",
        "historical-tx",
        "client",
        "would_fail",
    ),
    (
        "submit_census_historical_tx_client_uneval_total",
        "historical-tx",
        "client",
        "uneval",
    ),
    (
        "submit_census_historical_tx_operator_ready_total",
        "historical-tx",
        "operator",
        "ready",
    ),
    (
        "submit_census_historical_tx_operator_would_fail_total",
        "historical-tx",
        "operator",
        "would_fail",
    ),
    (
        "submit_census_historical_tx_operator_uneval_total",
        "historical-tx",
        "operator",
        "uneval",
    ),
    (
        "submit_census_historical_tx_no_spv_client_ready_total",
        "historical-tx-no-spv",
        "client",
        "ready",
    ),
    (
        "submit_census_historical_tx_no_spv_client_would_fail_total",
        "historical-tx-no-spv",
        "client",
        "would_fail",
    ),
    (
        "submit_census_historical_tx_no_spv_client_uneval_total",
        "historical-tx-no-spv",
        "client",
        "uneval",
    ),
    (
        "submit_census_historical_tx_no_spv_operator_ready_total",
        "historical-tx-no-spv",
        "operator",
        "ready",
    ),
    (
        "submit_census_historical_tx_no_spv_operator_would_fail_total",
        "historical-tx-no-spv",
        "operator",
        "would_fail",
    ),
    (
        "submit_census_historical_tx_no_spv_operator_uneval_total",
        "historical-tx-no-spv",
        "operator",
        "uneval",
    ),
];

/// Reason counters, `(name, reason-key)` — global (not per-mode) to keep the
/// row cardinality bounded; the per-mode state counters carry the split that
/// the flip decision reads.
///
/// `bodyOverEvalBound` is HISTORY since bsv-low #585: no verdict maps to it
/// (the census stops for no size), and its row stays here so that the total
/// counted before is still served.
pub const CENSUS_REASON_COUNTERS: [(&str, &str); 7] = [
    ("submit_census_reason_parse_total", "parse"),
    ("submit_census_reason_subject_ef_total", "subjectEf"),
    ("submit_census_reason_ef_over_cap_total", "efOverCap"),
    (
        "submit_census_reason_mined_claim_unextractable_total",
        "minedClaimUnextractable",
    ),
    (
        "submit_census_reason_mined_claim_unverified_total",
        "minedClaimUnverified",
    ),
    (
        "submit_census_reason_body_over_eval_bound_total",
        "bodyOverEvalBound",
    ),
    (
        "submit_census_reason_subject_ambiguous_total",
        "subjectAmbiguous",
    ),
];

/// The counters one classified submit bumps: `(state_counter,
/// Some(reason_counter))` for fail/uneval, `(state_counter, None)` for ready.
///
/// TOTAL over every input (no panic path on a Worker): a `NetworkGated` path
/// never reaches the census in production (the gated arm gets the REAL
/// verdict), but if one ever did, it is counted under the `current-tx`
/// mode-of-record rather than dropped — a misrouted count is visible, a
/// dropped one is not. `client` == the lenient unauthenticated population
/// (`lenient_unbarred` from the ONE `submit_gate` derivation, never a second
/// reading of the credential).
pub fn census_counters(
    path: AdmissionPath,
    client_population: bool,
    verdict: CensusVerdict,
) -> (&'static str, Option<&'static str>) {
    let mode = match path {
        AdmissionPath::CurrentTx | AdmissionPath::NetworkGated => "current-tx",
        AdmissionPath::HistoricalSpv => "historical-tx",
        AdmissionPath::HistoricalUngated => "historical-tx-no-spv",
    };
    let population = if client_population {
        "client"
    } else {
        "operator"
    };
    let state = match verdict {
        CensusVerdict::GatedReady => "ready",
        CensusVerdict::WouldHaveFailed(_) => "would_fail",
        CensusVerdict::CouldNotEvaluate(_) => "uneval",
    };
    let state_counter = CENSUS_STATE_COUNTERS
        .iter()
        .find(|(_, m, p, s)| *m == mode && *p == population && *s == state)
        .map(|(name, _, _, _)| *name)
        // Unreachable by construction (the table is total over the three
        // dimensions); expect() would be a panic path on a Worker, so fall
        // back to the first counter — which the exhaustiveness test below
        // proves can never happen.
        .unwrap_or(CENSUS_STATE_COUNTERS[0].0);
    let reason_key = match verdict {
        CensusVerdict::GatedReady => None,
        CensusVerdict::WouldHaveFailed(WouldFailWhy::Parse) => Some("parse"),
        CensusVerdict::WouldHaveFailed(WouldFailWhy::SubjectEf) => Some("subjectEf"),
        CensusVerdict::WouldHaveFailed(WouldFailWhy::EfOverCap) => Some("efOverCap"),
        CensusVerdict::WouldHaveFailed(WouldFailWhy::MinedClaimUnextractable) => {
            Some("minedClaimUnextractable")
        }
        CensusVerdict::CouldNotEvaluate(UnevalWhy::MinedClaimUnverified) => {
            Some("minedClaimUnverified")
        }
        CensusVerdict::CouldNotEvaluate(UnevalWhy::SubjectAmbiguous) => Some("subjectAmbiguous"),
    };
    let reason_counter = reason_key.and_then(|k| {
        CENSUS_REASON_COUNTERS
            .iter()
            .find(|(_, key)| *key == k)
            .map(|(name, _)| *name)
    });
    (state_counter, reason_counter)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use bsv_rs::transaction::{Beef, Transaction};

    // The SAME committed real-mainnet fixtures the ef.rs suite uses — the
    // census must be exercised through the shapes the gated arm actually
    // sees, not hand-typed approximations.
    const SUBJECT_RAW_HEX: &str = include_str!("../tests/fixtures/ef/subject_e98cdd1f.rawhex");
    const PARENT_BEEF_HEX: &str = include_str!("../tests/fixtures/ef/parent_a7d76588_beef.hex");

    fn ancestry_carrying_beef() -> Vec<u8> {
        let mut beef = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap();
        let subject = Transaction::from_hex(SUBJECT_RAW_HEX.trim()).unwrap();
        beef.merge_transaction(subject);
        beef.to_binary()
    }

    fn no_ancestry_beef() -> Vec<u8> {
        // THE bsv-low #351 client shape: `new Beef()` + `mergeRawTx` with no
        // ancestry — the subject spends a parent that is not in the BEEF.
        let mut beef = Beef::new();
        let subject = Transaction::from_hex(SUBJECT_RAW_HEX.trim()).unwrap();
        beef.merge_transaction(subject);
        beef.to_binary()
    }

    /// The four production shapes classify exactly as the gated arm treats
    /// them (see the route-tier cell for the same agreement over HTTP).
    #[test]
    fn census_classifies_the_production_shapes() {
        // Garbage bytes → the gated arm's 400 (Parse).
        assert_eq!(
            census_verdict(&[0xde, 0xad]),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::Parse)
        );
        // No-ancestry-no-proof (the TEN client marker surfaces today) → 400.
        assert_eq!(
            census_verdict(&no_ancestry_beef()),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::SubjectEf)
        );
        // Ancestry-carrying (proven parent + unmined subject — the honest
        // wallet AtomicBEEF shape) → structurally ready.
        assert_eq!(
            census_verdict(&ancestry_carrying_beef()),
            CensusVerdict::GatedReady
        );
        // The honest RECOVERY shape (adversarial review 2026-07-17 finding 5,
        // same construction as ef.rs's skips-unconvertible-ancestor cell): the
        // parent rides UNPROVEN and WITHOUT its own sources. The gated arm
        // skips it and broadcasts the subject, so the census must stay GREEN —
        // the ancestor is IN the subject's closure, never a stray (the guard
        // that flags mis-sorted bodies must not flag this one).
        let recovery = {
            let mut beef = Beef::new();
            let parent_raw = {
                let pb = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap();
                pb.txs.last().unwrap().tx().unwrap().to_hex()
            };
            beef.merge_transaction(Transaction::from_hex(&parent_raw).unwrap());
            beef.merge_transaction(Transaction::from_hex(SUBJECT_RAW_HEX.trim()).unwrap());
            beef.to_binary()
        };
        assert_eq!(census_verdict(&recovery), CensusVerdict::GatedReady);
        // All-proven mined claim → the network corroboration this census must
        // not perform: the THIRD state, never collapsed (Rule 13).
        let mined_claim = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap().to_binary();
        assert_eq!(
            census_verdict(&mined_claim),
            CensusVerdict::CouldNotEvaluate(UnevalWhy::MinedClaimUnverified)
        );
    }

    /// Fail-closed corners: an empty BEEF has no extractable subject, and the
    /// gated arm refuses it deterministically without a network call.
    #[test]
    fn census_counts_the_unextractable_mined_claim_as_would_fail() {
        let empty = Beef::new().to_binary();
        assert_eq!(
            census_verdict(&empty),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::MinedClaimUnextractable)
        );
    }

    /// The fixture-found false green, reproduced natively: a body whose REAL
    /// subject has a missing input sorts that subject into the front group,
    /// leaving a complete ANCESTOR as `last()`. Pre-loop-2 the census answered
    /// the THIRD state here (subject ambiguous); since the tip rule the gate
    /// names the real subject and its structural refusal is the answer.
    #[test]
    fn a_poisoned_subject_sort_is_uneval_never_a_green() {
        // A COMPLETE in-BEEF parent (no inputs of its own to be missing;
        // a parent with its own absent ancestry would join the subject in
        // the front group and the trap would not spring). Since bsv-rs 0.4.1
        // a transaction has an input (NL-8 W6), so the parent spends a
        // proven grandparent, itself a coinbase's null outpoint spender.
        let mut grandparent = Transaction::new();
        grandparent
            .add_input(bsv_rs::transaction::TransactionInput::new(
                "00".repeat(32),
                u32::MAX,
            ))
            .unwrap();
        grandparent
            .add_output(bsv_rs::transaction::TransactionOutput {
                satoshis: Some(6_000),
                locking_script: bsv_rs::script::LockingScript::from_binary(&[0x51]).unwrap(),
                change: false,
            })
            .unwrap();
        grandparent.merkle_path = Some(bsv_rs::transaction::MerklePath::from_coinbase_txid(
            &grandparent.id(),
            900_000,
        ));
        let mut parent = Transaction::new();
        let mut spend = bsv_rs::transaction::TransactionInput::new(grandparent.id(), 0);
        spend.source_transaction = Some(Box::new(grandparent.clone()));
        parent.add_input(spend).unwrap();
        parent
            .add_output(bsv_rs::transaction::TransactionOutput {
                satoshis: Some(5_000),
                locking_script: bsv_rs::script::LockingScript::from_binary(&[0x51]).unwrap(),
                change: false,
            })
            .unwrap();
        let parent_txid = parent.id();
        // A "real subject" spending BOTH the in-BEEF parent and an absent tx.
        let mut real_subject = Transaction::new();
        real_subject
            .add_input(bsv_rs::transaction::TransactionInput::new(
                parent_txid.clone(),
                0,
            ))
            .unwrap();
        real_subject
            .add_input(bsv_rs::transaction::TransactionInput::new(
                "11".repeat(32),
                0,
            ))
            .unwrap();
        // Since bsv-rs 0.4.3 a transaction has an output (bsv-rs #59): a
        // subject with none is refused at the parse (`NoOutputs` at its first
        // byte), before the EF step this cell is about. It is meant well
        // formed, its absent source the only fault, so it pays one output.
        real_subject
            .add_output(bsv_rs::transaction::TransactionOutput {
                satoshis: Some(4_000),
                locking_script: bsv_rs::script::LockingScript::from_binary(&[0x51]).unwrap(),
                change: false,
            })
            .unwrap();
        let mut beef = Beef::new();
        beef.merge_transaction(grandparent);
        beef.merge_transaction(parent);
        beef.merge_transaction(real_subject);
        // Precondition of the trap: the sorted-last entry is the ANCESTOR.
        let mut sorted = Beef::from_binary(&beef.to_binary()).unwrap();
        sorted.sort_txs();
        assert_eq!(
            sorted.txs.last().unwrap().txid(),
            parent_txid,
            "fixture drift: the mis-sort this cell exists for is not happening"
        );
        // LOOP-2 HARDENING (2026-09-05): the route no longer takes the
        // sorted-last as the subject — `ef::subject_txid_of` names the UNIQUE
        // TIP (the real subject: nothing spends it), so the trap can no longer
        // spring the census into the third state. The real subject's absent
        // source makes the EF conversion fail, and THAT is the honest, mapped
        // answer: the route would refuse 400 (`SubjectEf`), never broadcast
        // the ancestor. (The fleet found this exact shape live on pair-17 —
        // a JOIN admitted as a HOP for 200 — and the fix moved from
        // "uneval" here to "right subject" in the gate.)
        assert_eq!(
            census_verdict(&beef.to_binary()),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::SubjectEf)
        );
    }

    // ── bsv-low #585, door 2: no stop by size ───────────────────────────

    const FIVE_MB: usize = 5 * 1024 * 1024;

    fn out(satoshis: u64, script: &[u8]) -> bsv_rs::transaction::TransactionOutput {
        bsv_rs::transaction::TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: bsv_rs::script::LockingScript::from_binary(script).unwrap(),
            change: false,
        }
    }

    /// `OP_FALSE OP_RETURN <len bytes>`.
    fn data_script(len: usize) -> Vec<u8> {
        let mut script = vec![0x00, 0x6a, 0x4e];
        script.extend_from_slice(&(len as u32).to_le_bytes());
        script.resize(script.len() + len, 0x5a);
        script
    }

    /// A transaction with one input spending `source:vout` (an empty
    /// unlocking script: the census reads structure, it executes nothing).
    fn spending(source: &Transaction, vout: u32) -> Transaction {
        let mut tx = Transaction::new();
        let mut input = bsv_rs::transaction::TransactionInput::new(source.id(), vout);
        input.unlocking_script = Some(bsv_rs::script::UnlockingScript::from_binary(&[]).unwrap());
        tx.add_input(input).unwrap();
        tx
    }

    /// A "mined" transaction: a one-leaf BUMP whose root is its txid.
    fn proven(mut tx: Transaction) -> Transaction {
        let txid = tx.id();
        tx.merkle_path = Some(
            bsv_rs::transaction::MerklePath::new(
                800_000,
                vec![vec![bsv_rs::transaction::MerklePathLeaf::new_txid(0, txid)]],
            )
            .unwrap(),
        );
        tx
    }

    fn funding(outputs: &[(u64, Vec<u8>)]) -> Transaction {
        let mut tx = Transaction::new();
        let mut input = bsv_rs::transaction::TransactionInput::new("aa".repeat(32), 0);
        input.unlocking_script = Some(bsv_rs::script::UnlockingScript::from_binary(&[]).unwrap());
        tx.add_input(input).unwrap();
        for (satoshis, script) in outputs {
            tx.add_output(out(*satoshis, script)).unwrap();
        }
        tx
    }

    /// The honest shape at 5 MB: a PROVEN parent that carries 5 MB of data
    /// beside the output the unmined subject spends.
    fn five_mb_ready_beef() -> Vec<u8> {
        let parent = proven(funding(&[(5_000, vec![0x51]), (0, data_script(FIVE_MB))]));
        let mut subject = spending(&parent, 0);
        subject.add_output(out(4_000, &[0x51])).unwrap();
        let mut beef = Beef::new();
        beef.merge_transaction(parent);
        beef.merge_transaction(subject);
        beef.to_binary()
    }

    /// THE PIN (bsv-low #585, door 2): a 5 MB valid BEEF is CLASSIFIED. RED
    /// on `d6d2774`: `could-not-evaluate(body-over-eval-bound)`, all three.
    #[test]
    fn e585_d2_a_5mb_valid_beef_is_classified_never_unevaluated_by_size() {
        // Gated-ready: the gated arm would convert and broadcast the subject.
        let ready = five_mb_ready_beef();
        assert!(ready.len() > FIVE_MB, "{} bytes", ready.len());
        assert_eq!(census_verdict(&ready), CensusVerdict::GatedReady);

        // Would-fail, by the gated arm's own reason: the SUBJECT is the 5 MB
        // transaction, and the arm answers 429 at its EF work bound before
        // any broadcast.
        let parent = proven(funding(&[(5_000, vec![0x51])]));
        let mut subject = spending(&parent, 0);
        subject.add_output(out(4_000, &[0x51])).unwrap();
        subject.add_output(out(0, &data_script(FIVE_MB))).unwrap();
        let mut beef = Beef::new();
        beef.merge_transaction(parent);
        beef.merge_transaction(subject);
        let heavy_subject = beef.to_binary();
        assert!(heavy_subject.len() > FIVE_MB);
        assert_eq!(
            census_verdict(&heavy_subject),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::EfOverCap)
        );

        // The third state, by its own reason and not by size: the 5 MB ready
        // body with a second, unrelated tip. Which tip the route would
        // broadcast is not the caller's to know.
        let parent = proven(funding(&[
            (5_000, vec![0x51]),
            (5_000, vec![0x51]),
            (0, data_script(FIVE_MB)),
        ]));
        let mut one = spending(&parent, 0);
        one.add_output(out(4_000, &[0x51])).unwrap();
        let mut other = spending(&parent, 1);
        other.add_output(out(3_000, &[0x51])).unwrap();
        let mut beef = Beef::new();
        beef.merge_transaction(parent);
        beef.merge_transaction(one);
        beef.merge_transaction(other);
        let two_tips = beef.to_binary();
        assert!(two_tips.len() > FIVE_MB);
        assert_eq!(
            census_verdict(&two_tips),
            CensusVerdict::CouldNotEvaluate(UnevalWhy::SubjectAmbiguous)
        );
    }

    /// The ancestry check before #585, kept verbatim over a hydrated BEEF:
    /// what the stream's answer is held against.
    fn outside_ancestry_by_the_hydrated_parse(beef_bytes: &[u8], subject_txid: &str) -> bool {
        use std::collections::HashSet;
        let beef = Beef::from_binary(beef_bytes).unwrap();
        let mut txs: HashMap<String, Transaction> = HashMap::new();
        for btx in &beef.txs {
            if let Some(tx) = btx.tx() {
                txs.insert(btx.txid(), tx.clone());
            }
        }
        let mut closure: HashSet<String> = HashSet::new();
        let mut frontier = vec![subject_txid.to_string()];
        while let Some(txid) = frontier.pop() {
            if !closure.insert(txid.clone()) {
                continue;
            }
            if let Some(tx) = txs.get(&txid) {
                for input in &tx.inputs {
                    if let Some(src) = &input.source_txid {
                        if txs.contains_key(src) && !closure.contains(src) {
                            frontier.push(src.clone());
                        }
                    }
                }
            }
        }
        txs.keys().any(|txid| !closure.contains(txid))
    }

    /// The stream's ancestry answer is the hydrated parse's, for EVERY
    /// transaction of each body named as the subject, and for a txid the body
    /// does not carry: the production shapes, the client fixture's bodies,
    /// and a source that lies AFTER its spender in the stream.
    #[test]
    fn e585_d2_the_streams_ancestry_is_the_hydrated_parses() {
        let mut bodies: Vec<Vec<u8>> = vec![
            ancestry_carrying_beef(),
            no_ancestry_beef(),
            Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap().to_binary(),
            five_mb_ready_beef(),
        ];
        let raw = include_str!("../tests/fixtures/census/client_parity.fixture.json");
        let fixture: serde_json::Value = serde_json::from_str(raw).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let body = hex::decode(case["beefHex"].as_str().unwrap()).unwrap();
            // A case the gated arm's parser refuses never reaches the check.
            if Beef::from_binary(&body).is_ok() {
                bodies.push(body);
            }
        }
        // A chain a <- b <- c with a stray d, written spender FIRST (a V1
        // frame by hand: `to_binary` would sort it).
        let a = funding(&[(9_000, vec![0x51])]);
        let mut b = spending(&a, 0);
        b.add_output(out(8_000, &[0x51])).unwrap();
        let mut c = spending(&b, 0);
        c.add_output(out(7_000, &[0x51])).unwrap();
        let d = funding(&[(1, vec![0x52])]);
        let mut backwards = vec![0x01, 0x00, 0xbe, 0xef, 0x00, 0x04];
        for tx in [&c, &d, &b, &a] {
            backwards.extend_from_slice(&tx.to_binary());
            backwards.push(0x00);
        }
        bodies.push(backwards);

        let mut asked = 0usize;
        let (mut strays, mut covered) = (0usize, 0usize);
        for body in &bodies {
            let parsed = Beef::from_binary(body).unwrap();
            let mut subjects: Vec<String> = parsed.txs.iter().map(|btx| btx.txid()).collect();
            subjects.push("cd".repeat(32));
            for subject in &subjects {
                let by_stream = beef_has_entries_outside_subject_ancestry(body, subject)
                    .expect("the stream reads a body the SDK parses");
                let by_parse = outside_ancestry_by_the_hydrated_parse(body, subject);
                assert_eq!(by_stream, by_parse, "subject {subject}");
                asked += 1;
                if by_stream {
                    strays += 1;
                } else {
                    covered += 1;
                }
            }
        }
        assert!(
            asked >= 20 && strays >= 5 && covered >= 5,
            "{asked} asked, {strays} strays, {covered} covered"
        );
        // Bytes the stream refuses are a non-answer, and a non-answer is no green.
        let mut trailing = ancestry_carrying_beef();
        trailing.push(0x00);
        assert_eq!(
            beef_has_entries_outside_subject_ancestry(&trailing, &"cd".repeat(32)),
            None
        );
    }

    /// What the census keeps of a body: an entry a transaction and a pair a
    /// spend (measured by capacity), not the body.
    #[test]
    fn e585_d2_the_ancestry_index_is_small_beside_the_body() {
        let body = five_mb_ready_beef();
        let shape = AncestryShape::read(&body).unwrap();
        assert_eq!((shape.ids.len(), shape.spends.len()), (2, 1));
        let held = shape.heap_bytes();
        println!(
            "e585_d2: body {} bytes, 3 elements (2 transactions, 1 BUMP), the ancestry index \
             {held} bytes",
            body.len()
        );
        assert!(held < 1024, "{held} bytes");

        // 1000 transactions in a chain: an entry and a spend each.
        let mut beef = Beef::new();
        let mut prev = funding(&[(1_000_000, vec![0x51])]);
        beef.merge_transaction(prev.clone());
        for depth in 1..1000u64 {
            let mut next = spending(&prev, 0);
            next.add_output(out(1_000_000 - depth, &[0x51])).unwrap();
            beef.merge_transaction(next.clone());
            prev = next;
        }
        let chain = beef.to_binary();
        let shape = AncestryShape::read(&chain).unwrap();
        assert_eq!((shape.ids.len(), shape.spends.len()), (1000, 999));
        assert!(!shape.has_entry_outside(shape.ids.keys().find(|txid| {
            let mut display = **txid;
            display.reverse();
            hex::encode(display) == prev.id()
        })));
        let held = shape.heap_bytes();
        println!(
            "e585_d2: body {} bytes, 1000 elements, the ancestry index {held} bytes = {} per \
             element",
            chain.len(),
            held / 1000
        );
        assert!(held <= 1000 * 128, "{held} bytes");
    }

    /// Rule 13 pinned POSITIVELY: the three states map to three DISTINCT
    /// counters for every (path, population), so `CouldNotEvaluate` can never
    /// be silently absorbed by ready or fail at the recording seam.
    #[test]
    fn the_three_states_never_share_a_counter() {
        let verdicts = [
            CensusVerdict::GatedReady,
            CensusVerdict::WouldHaveFailed(WouldFailWhy::SubjectEf),
            CensusVerdict::CouldNotEvaluate(UnevalWhy::MinedClaimUnverified),
        ];
        for path in crate::submit_gate::ALL_ADMISSION_PATHS {
            for client in [true, false] {
                let names: Vec<&str> = verdicts
                    .iter()
                    .map(|v| census_counters(path, client, *v).0)
                    .collect();
                let mut dedup = names.clone();
                dedup.dedup();
                assert_eq!(
                    names.len(),
                    dedup.len(),
                    "{path:?} client={client}: two states share a counter — \
                     the third state has been collapsed"
                );
            }
        }
    }

    /// Every (mode, population, state) cell resolves to a REAL table entry —
    /// the `.unwrap_or` fallback in `census_counters` is proven unreachable,
    /// and the client/operator populations never share a row (the flip reads
    /// the CLIENT rows; an operator count leaking into them would fake a
    /// blocker or hide one).
    #[test]
    fn census_counters_is_total_and_populations_are_disjoint() {
        let verdicts = [
            CensusVerdict::GatedReady,
            CensusVerdict::WouldHaveFailed(WouldFailWhy::Parse),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::SubjectEf),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::EfOverCap),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::MinedClaimUnextractable),
            CensusVerdict::CouldNotEvaluate(UnevalWhy::MinedClaimUnverified),
            CensusVerdict::CouldNotEvaluate(UnevalWhy::SubjectAmbiguous),
        ];
        for path in crate::submit_gate::ALL_ADMISSION_PATHS {
            for client in [true, false] {
                for v in verdicts {
                    let (state, reason) = census_counters(path, client, v);
                    let entry = CENSUS_STATE_COUNTERS
                        .iter()
                        .find(|(name, _, _, _)| *name == state)
                        .expect("state counter must be a table entry");
                    // Population is faithful — never crossed.
                    assert_eq!(entry.2, if client { "client" } else { "operator" });
                    // Ready has no reason; fail/uneval always carry one.
                    match v {
                        CensusVerdict::GatedReady => assert!(reason.is_none()),
                        _ => {
                            let r = reason.expect("non-ready verdicts carry a reason counter");
                            assert!(
                                CENSUS_REASON_COUNTERS.iter().any(|(name, _)| *name == r),
                                "{r} not in the reason table"
                            );
                        }
                    }
                }
            }
        }
        // No duplicate names padding either table.
        for names in [
            CENSUS_STATE_COUNTERS
                .iter()
                .map(|(n, _, _, _)| *n)
                .collect::<Vec<_>>(),
            CENSUS_REASON_COUNTERS
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>(),
        ] {
            let mut sorted = names.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), names.len(), "duplicate counter names");
        }
    }

    /// CROSS-LANGUAGE parity (Rule 16): bodies BUILT BY THE REAL CLIENT-SIDE
    /// SERIALIZER (`@bsv/sdk`, resolved from the bsv-low checkout — the same
    /// library every client `/submit` body rides through), frozen as a
    /// committed fixture with the client predicate's own verdicts, and driven
    /// through the REAL Rust census.
    ///
    /// The fixture is an ARTIFACT, not a convention: it was emitted by
    /// `tools/lane-366/emit_census_parity_fixture.mjs` running the TS
    /// producer, then committed byte-identically (the bsv-low #343 pattern —
    /// a hand-retyped fixture would encode THIS side's understanding on both
    /// sides of the comparison and be green for the same wrong reason).
    /// Regenerate by re-running the emitter and committing the diff.
    ///
    /// The asserted MAPPING (not identity — the two predicates answer
    /// different questions, see the module doc):
    /// * client `ready:true`, subject unmined  ⇒ census `GatedReady`
    /// * client `ready:true`, subject proven   ⇒ census `CouldNotEvaluate`
    /// * client `ready:false`                  ⇒ census is NEVER `GatedReady`
    ///   (the safety row: a body the client itself says cannot migrate must
    ///   never be censused as ready) — and for at least two of those cases
    ///   (no-ancestry, txid-only stub) it must be a hard `WouldHaveFailed`,
    ///   so a census that hid behind `CouldNotEvaluate` everywhere cannot
    ///   pass either.
    ///
    /// This cell EARNED its keep before it ever went green: the fixture's
    /// partial-ancestry case exposed the mis-sorted-subject false green that
    /// became `UnevalWhy::SubjectAmbiguous`.
    #[test]
    fn census_parity_fixture_agrees_with_the_client_predicate() {
        let raw = include_str!("../tests/fixtures/census/client_parity.fixture.json");
        let fixture: serde_json::Value = serde_json::from_str(raw).expect("fixture parses");
        let cases = fixture["cases"].as_array().expect("cases array");
        // LOUD-COUNT guard (the #343 lesson): a regenerated fixture that
        // covers fewer states than this wire can carry must not pass quietly.
        assert!(
            cases.len() >= 4,
            "fixture shrank to {} case(s) — it no longer covers the verdict space",
            cases.len()
        );
        let mut saw = (false, false, false); // (ready-unmined, ready-proven, not-ready)
        let mut not_ready_hard_fails = 0usize;
        // Bodies the door refuses for a transaction with no input (bsv-rs
        // 0.4.1 `NoInputs`, bsv-stack-lean NL-8 W6). The committed fixture's
        // ready-unmined body carries a funded parent with no input; the census
        // refuses it at the parse, never a green. The emitter now gives every
        // source transaction an input; the fixture is regenerated from the
        // bsv-low checkout (its header), after which this set is empty.
        let mut door_refused_ready: Vec<String> = Vec::new();
        for case in cases {
            let label = case["label"].as_str().unwrap();
            let body = hex::decode(case["beefHex"].as_str().unwrap()).unwrap();
            let client_ready = case["client"]["ready"].as_bool().unwrap();
            let proven = case["subjectProven"].as_bool().unwrap();
            let got = census_verdict(&body);
            let no_input = beef_limits::read_beef(&body)
                .err()
                .is_some_and(|r| r.kind() == beef_limits::Kind::NoInputs);
            if client_ready && no_input {
                assert_eq!(
                    got,
                    CensusVerdict::WouldHaveFailed(WouldFailWhy::Parse),
                    "{label}"
                );
                door_refused_ready.push(label.to_string());
                continue;
            }
            match (client_ready, proven) {
                (true, false) => {
                    saw.0 = true;
                    assert_eq!(got, CensusVerdict::GatedReady, "{label}");
                }
                (true, true) => {
                    saw.1 = true;
                    assert!(
                        matches!(got, CensusVerdict::CouldNotEvaluate(_)),
                        "{label}: proven-subject divergence must map to the THIRD state, got {got:?}"
                    );
                }
                (false, _) => {
                    saw.2 = true;
                    assert!(
                        got != CensusVerdict::GatedReady,
                        "{label}: client says not-ready — a census green here is a FALSE green"
                    );
                    if matches!(got, CensusVerdict::WouldHaveFailed(_)) {
                        not_ready_hard_fails += 1;
                    }
                }
            }
        }
        assert_eq!(
            door_refused_ready,
            ["ready-unmined: funded parent + spending subject"],
            "the door's refusals among the ready cases"
        );
        // Until the regeneration the ready-unmined row is not proven against
        // the client here; the census's own ready path stays proven natively
        // (`ancestry_carrying_beef` answers `GatedReady` above).
        saw.0 |= !door_refused_ready.is_empty();
        assert!(
            saw.0 && saw.1 && saw.2,
            "fixture must cover all three mapping rows (got ready-unmined={}, ready-proven={}, not-ready={})",
            saw.0, saw.1, saw.2
        );
        // A census answering `CouldNotEvaluate` for everything would satisfy
        // the not-ready row vacuously — require the hard-fail classes too.
        assert!(
            not_ready_hard_fails >= 2,
            "expected ≥2 hard WouldHaveFailed among the not-ready cases \
             (no-ancestry, txid-only), got {not_ready_hard_fails}"
        );
    }
}
