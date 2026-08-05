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
//! 2. [`crate::routes::subject_ef_over_cap`] — the gated arm 429s when either
//!    EF work bound is exceeded, before any network call.
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
//!   BEFORE any network call (flat 400 / 429 / fail-closed mined-claim 502).
//!   This is THE number the #347 flip reads: every count here is an honest
//!   submit the flip would strand.
//! * **`CouldNotEvaluate`** — the census could not honestly decide. Three
//!   named classes: an all-proven mined-claim whose fate is the network
//!   corroboration this census must not perform; a body over the census work
//!   bound; and a body whose sorted-last "subject" does not cover the BEEF
//!   with its ancestry (subject identification unreliable). Never folded into
//!   either other state — a flip decision that reads 0 fails must ALSO read
//!   (and reason about) this bucket.
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
//! upsert via [`crate::ops::bump_counter`] — no schema migration), written in
//! the BACKGROUND (`ctx.wait_until`) so measurement adds no caller latency,
//! and read on `GET /health/invariants` as `submitReadinessCensus`. Counters
//! are MONOTONIC TOTALS: "the last N" is a delta between two reads, and an
//! observation window whose `observed` delta is 0 is NO EVIDENCE — it carries
//! a streak, it never credits one (the bsv-low #341
//! `unvalidated_share_bps() -> None` posture). `/health/invariants` is
//! unauthenticated and `/submit` is public, so the counters are
//! attacker-inflatable upward — a soak/flip SIGNAL to corroborate against
//! producer-side facts, never an audit log (same posture as
//! `submit_gate::counters_json`).

use crate::ef::{beef_to_ef_batch, proven_subject_raw, EfError};
use crate::submit_gate::AdmissionPath;

/// Census work bound: bodies larger than this are counted
/// [`UnevalWhy::BodyOverEvalBound`] instead of being converted. The route's own
/// body cap is 10 MB and the gated arm converts anything under it, but the
/// census runs on the UNGATED path where this work is additive — 2 MiB matches
/// the gated arm's total-batch EF bound scale (`MAX_BATCH_EF_BYTES`) and is
/// orders of magnitude above any honest LOW BEEF (a few KB; deep-ancestry
/// recovery shapes are tens of KB).
pub const MAX_CENSUS_EVAL_BYTES: usize = 2 * 1024 * 1024;

/// Why the gated arm would have refused these bytes before any network call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WouldFailWhy {
    /// `Beef::from_binary` failed — the gated arm 400s.
    Parse,
    /// The SUBJECT could not be converted to Extended Format (txid-only
    /// subject, missing input source txs — THE bsv-low #351 class) — the
    /// gated arm 400s.
    SubjectEf,
    /// An EF work bound would trip ([`crate::routes::subject_ef_over_cap`]) —
    /// the gated arm 429s before any broadcast.
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
    /// Body over [`MAX_CENSUS_EVAL_BYTES`] — evaluation skipped, not attempted
    /// (write-time-scale work is not owed to a measurement).
    BodyOverEvalBound,
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
            Self::CouldNotEvaluate(UnevalWhy::BodyOverEvalBound) => {
                "could-not-evaluate(body-over-eval-bound)"
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
pub fn census_verdict(beef_bytes: &[u8]) -> CensusVerdict {
    if beef_bytes.len() > MAX_CENSUS_EVAL_BYTES {
        return CensusVerdict::CouldNotEvaluate(UnevalWhy::BodyOverEvalBound);
    }
    let (efs, subject_txid) = match beef_to_ef_batch(beef_bytes) {
        Ok(v) => v,
        Err(EfError::Parse(_)) => return CensusVerdict::WouldHaveFailed(WouldFailWhy::Parse),
        Err(EfError::EfConversion(_)) => {
            return CensusVerdict::WouldHaveFailed(WouldFailWhy::SubjectEf)
        }
    };
    // Same bound, same function, same order as the gated arm (429 before any
    // broadcast). With `efs` empty both sizes are 0, so this never fires on
    // the mined-claim shape — matching the route, where the cap check runs
    // before the mined-claim raw is even extracted.
    if crate::routes::subject_ef_over_cap(&efs, &subject_txid).is_some() {
        return CensusVerdict::WouldHaveFailed(WouldFailWhy::EfOverCap);
    }
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
    if beef_has_entries_outside_subject_ancestry(beef_bytes, &subject_txid) {
        return CensusVerdict::CouldNotEvaluate(UnevalWhy::SubjectAmbiguous);
    }
    CensusVerdict::GatedReady
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
/// A parse failure returns `false`: unreachable in practice (the caller only
/// asks after `beef_to_ef_batch` parsed the same bytes), and a non-answer
/// must not manufacture a verdict.
fn beef_has_entries_outside_subject_ancestry(beef_bytes: &[u8], subject_txid: &str) -> bool {
    use std::collections::{HashMap, HashSet};
    let Ok(beef) = bsv_rs::transaction::Beef::from_binary(beef_bytes) else {
        return false;
    };
    // Entries with transaction data (mirrors the gated arm's source map:
    // `if let Some(tx) = btx.tx()`). Txid-only stubs carry nothing that could
    // be mis-broadcast, so they are neither closure members nor strays.
    let mut txs: HashMap<String, bsv_rs::transaction::Transaction> = HashMap::new();
    for btx in &beef.txs {
        if let Some(tx) = btx.tx() {
            txs.insert(btx.txid(), tx.clone());
        }
    }
    // BFS the subject's ancestor closure over in-BEEF edges.
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

// ── durable counter names (rows in `ops_counters`; read by `ops::census_json`) ─

/// Every state counter, `(name, mode, population, state)` — the read side and
/// the tests iterate THIS table, so a name cannot drift between write and read.
///
/// `population`: `client` = the unauthenticated lenient-window population (the
/// exact population the #347 flip criterion is about); `operator` = submits
/// carrying the `SUBMIT_OPERATOR_TOKEN` (the tower/peer/migration population,
/// which #351 receipts also cover from the producer side).
pub const CENSUS_STATE_COUNTERS: [(&str, &str, &str, &str); 18] = [
    ("submit_census_current_tx_client_ready_total", "current-tx", "client", "ready"),
    ("submit_census_current_tx_client_would_fail_total", "current-tx", "client", "would_fail"),
    ("submit_census_current_tx_client_uneval_total", "current-tx", "client", "uneval"),
    ("submit_census_current_tx_operator_ready_total", "current-tx", "operator", "ready"),
    ("submit_census_current_tx_operator_would_fail_total", "current-tx", "operator", "would_fail"),
    ("submit_census_current_tx_operator_uneval_total", "current-tx", "operator", "uneval"),
    ("submit_census_historical_tx_client_ready_total", "historical-tx", "client", "ready"),
    ("submit_census_historical_tx_client_would_fail_total", "historical-tx", "client", "would_fail"),
    ("submit_census_historical_tx_client_uneval_total", "historical-tx", "client", "uneval"),
    ("submit_census_historical_tx_operator_ready_total", "historical-tx", "operator", "ready"),
    (
        "submit_census_historical_tx_operator_would_fail_total",
        "historical-tx",
        "operator",
        "would_fail",
    ),
    ("submit_census_historical_tx_operator_uneval_total", "historical-tx", "operator", "uneval"),
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
    let population = if client_population { "client" } else { "operator" };
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
        CensusVerdict::CouldNotEvaluate(UnevalWhy::BodyOverEvalBound) => Some("bodyOverEvalBound"),
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
        assert_eq!(census_verdict(&ancestry_carrying_beef()), CensusVerdict::GatedReady);
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
    /// leaving a complete ANCESTOR as `last()`. The census must answer with
    /// the THIRD state — never a green (the first implementation said
    /// `GatedReady` here), and never a fail it cannot establish (the gated
    /// fate is the network's verdict on the wrong tx).
    #[test]
    fn a_poisoned_subject_sort_is_uneval_never_a_green() {
        // A COMPLETE in-BEEF parent (no inputs of its own to be missing —
        // the same shape as the fixture's funded test parent; a parent with
        // its own absent ancestry would join the subject in the front group
        // and the trap would not spring).
        let mut parent = Transaction::new();
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
        let mut beef = Beef::new();
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
        assert_eq!(
            census_verdict(&beef.to_binary()),
            CensusVerdict::CouldNotEvaluate(UnevalWhy::SubjectAmbiguous)
        );
    }

    /// The census work bound is honest: over-bound bodies are UNEVALUATED,
    /// never guessed into either decided state.
    #[test]
    fn census_over_eval_bound_is_uneval_not_a_guess() {
        // A body one byte over the bound. Content is irrelevant — the bound
        // check must run BEFORE any parse attempt.
        let big = vec![0u8; MAX_CENSUS_EVAL_BYTES + 1];
        assert_eq!(
            census_verdict(&big),
            CensusVerdict::CouldNotEvaluate(UnevalWhy::BodyOverEvalBound)
        );
        // At the bound: evaluated (garbage → Parse), proving the bound is
        // exclusive and the parse still runs at the boundary.
        let at = vec![0u8; MAX_CENSUS_EVAL_BYTES];
        assert_eq!(
            census_verdict(&at),
            CensusVerdict::WouldHaveFailed(WouldFailWhy::Parse)
        );
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
            CensusVerdict::CouldNotEvaluate(UnevalWhy::BodyOverEvalBound),
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
            CENSUS_STATE_COUNTERS.iter().map(|(n, _, _, _)| *n).collect::<Vec<_>>(),
            CENSUS_REASON_COUNTERS.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
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
        for case in cases {
            let label = case["label"].as_str().unwrap();
            let body = hex::decode(case["beefHex"].as_str().unwrap()).unwrap();
            let client_ready = case["client"]["ready"].as_bool().unwrap();
            let proven = case["subjectProven"].as_bool().unwrap();
            let got = census_verdict(&body);
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
