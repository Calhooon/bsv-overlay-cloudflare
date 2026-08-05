//! `POST /submit` admission gating — the gate is the ENDPOINT's property,
//! never the request's (bsv-low #347).
//!
//! ## The defect this closes
//!
//! `/submit` is dispatched with no auth and no rate limit. Before this module,
//! the network gate was conditioned on a **caller-supplied header**:
//!
//! ```text
//! if mode_header.as_deref() == Some("broadcast-gated") { /* …broadcast + SEEN gate… */ }
//! ```
//!
//! and `historical-tx-no-spv` mapped to the *same* engine mode as
//! `broadcast-gated` while skipping that block entirely. So only the mode our
//! own client happens to send was gated. An adversary sent
//! `x-submit-mode: historical-tx-no-spv` and was admitted with **no broadcast,
//! no SPV, no fee, and no on-chain transaction at all** — proven through the
//! real route against a local worker: a fabricated single-tx BEEF (junk
//! empty-script input spending a random prevout, one well-formed
//! `LOW/collected/v1` OP_RETURN naming an arbitrary victim) returned
//! `200 {"tm_collected":{"outputsToAdmit":[0]}}` and was then served back by the
//! public `/lookup` as `present:true`.
//!
//! **The generalisable trap (epoch Rule 8b, applied to a MODE rather than a
//! value): a gate selected by a caller-supplied discriminator is not a gate.**
//! If the request chooses which validation path it takes, an adversary chooses
//! the weakest one.
//!
//! ## Why this is a money issue, not a display issue
//!
//! The claim "every money exit verifies independently of the index" is true of
//! the **gates** (credits require an on-chain landing proof from independent
//! providers) and false of **which objects those gates are ever run against**.
//! A wiped device enumerates its pots exclusively from index-backed windows
//! (`bsv-low app/src/lib/creditSweep.ts::seedFromServer`), so an attacker who
//! can file free rows can starve an honest pot out of that enumeration — the
//! landing-proof gate is never invoked for it, and recoverable funds go
//! unswept. **A gate that is never invoked is not a defence.** Free admission
//! is what turns a costed, chain-traceable eviction into a free traceless one.
//!
//! ## The four admission paths, and the invariant that orders them
//!
//! [`AdmissionPath`] is derived by the endpoint from the header; the header
//! never reaches a gate decision directly. Each path carries a **bar** — the
//! thing that makes admission cost something an adversary cannot fake:
//!
//! | Path | Bar | Public? |
//! |---|---|---|
//! | [`CurrentTx`](AdmissionPath::CurrentTx) (no header) | SPV: BEEF merkle proof validated | yes |
//! | [`HistoricalSpv`](AdmissionPath::HistoricalSpv) (`historical-tx`) | SPV: same | yes |
//! | [`NetworkGated`](AdmissionPath::NetworkGated) (`broadcast-gated`) | overlay broadcasts, admits only on `SEEN_ON_NETWORK` | yes |
//! | [`HistoricalUngated`](AdmissionPath::HistoricalUngated) (`historical-tx-no-spv`) | **none** | **operator only** |
//!
//! The load-bearing invariant, pinned exhaustively by
//! `every_path_has_a_bar_or_requires_operator_auth`: **every path either has a
//! bar or requires operator auth.** That is a positive assertion over an
//! exhaustive match, so adding a fifth path without deciding its bar fails the
//! suite rather than silently opening a hole (Rule 9: never a bare
//! `assert!(!contains(..))`; Rule 15: widen the type, don't carve an exception).
//!
//! Note the shape of the fix: the two ungated-in-practice modes previously
//! collapsed onto one engine mode, so `SubmitMode` alone **cannot** express the
//! distinction — gating on it directly would have been a no-op. The type had to
//! be widened first. That is why [`AdmissionPath`] exists rather than a boolean.
//!
//! ## Rule 20 — the honest player's path is never gated, priced, or throttled
//!
//! This adds no quota, no price, and no credential to any path an honest
//! client takes. The honest client sends `broadcast-gated` and is untouched.
//! The ungated mode becomes operator-only, and the operator credential is the
//! existing `ADMIN_TOKEN` bearer — deliberately NOT BRC-103: that middleware
//! exists for identity-scoped *reads* (`low-app-layer`, bsv-low #318), and a
//! peer/operator sync path wants a shared operator secret, not a per-identity
//! handshake. Bitcoin remains the rate limiter on every public path: the bar is
//! always "the network already accepted this", never "you have paid us".
//!
//! ## Rule 6c — the lenient window, and how it closes
//!
//! The deployed bsv-low client and watchtower both send `historical-tx-no-spv`
//! for marker submits today, so enforcing immediately is an outage. Therefore:
//!
//! * **Lenient ([`GateMode::Lenient`], the default — `SUBMIT_ENFORCE` unset):**
//!   an unauthenticated ungated submit is SERVED exactly as before, but
//!   **counted** and logged (Rule 13: surface the signal, don't consume it).
//!   Nothing changes for any caller.
//! * **Strict (`SUBMIT_ENFORCE=true`):** an unauthenticated `historical-tx-no-spv`
//!   is refused `401` with an honest body naming the credential. Every other
//!   path is untouched in both modes.
//!
//! **Closure criteria, written when the window is opened:**
//!   1. the bsv-low client migrates its marker submits from
//!      `historical-tx-no-spv` to `broadcast-gated` (verified viable: an
//!      already-broadcast tx satisfies the SEEN gate idempotently — see
//!      "already-known" below — and the client already builds ancestry-carrying
//!      BEEFs on its gated path, `app/src/lib/overlay.ts`);
//!   2. the watchtower either presents `ADMIN_TOKEN` on its `submit_tm_pot`
//!      call or migrates likewise (it is an operator and holds secrets, so the
//!      token is legitimate there in a way it never is in a browser bundle);
//!   3. soak until `unauthenticatedUngated` on `/health/invariants` reaches ~0;
//!   4. then flip `SUBMIT_ENFORCE=true`. Flip it back for instant rollback; no
//!      code change either way.
//!
//! **What a stale caller experiences at closure:** an honest `401` naming the
//! credential. bsv-low's producers treat a non-2xx submit as best-effort and
//! retry on later ticks (`readmitSweep.ts`), and the tower's reveal lookup
//! falls back to the WoC+Bitails chain scan — so a refusal degrades to "not
//! indexed yet", never to a silent wrong answer or a lost credit.
//!
//! ## The already-known idempotency that makes migration free
//!
//! Migrating an honest submit to `broadcast-gated` does **not** cost a second
//! real broadcast: `broadcaster::already_known` treats "already"/"known"/
//! "mined"/"seen"/node code 257 in any dress as `Accepted`, on both the primary
//! (`arc_verdict`) and corroborating (`corroborator_verdict`) legs and on both
//! 2xx-with-error-status and non-2xx bodies; and `MINED` (rank 9) outranks the
//! `SEEN_ON_NETWORK` gate (rank 7), so a mined tx reads as `Reached`. A
//! re-submit of an already-broadcast tx therefore passes the gate idempotently.
//!
//! **The prerequisite that is NOT free, and it splits by producer:** the gated
//! path first converts the BEEF to Extended Format, and
//! `ef::beef_to_ef_batch` hard-errors when the SUBJECT's source transactions
//! are absent from the BEEF. A single-tx proofless BEEF therefore returns 400
//! *before any network call*. The browser client is fine (it merges ancestry on
//! its gated path); the watchtower's `wrap_raw_tx_beef_v1` single-tx wrapper is
//! not, which is why step 2 above offers it the token as well as migration.

use std::sync::atomic::{AtomicU64, Ordering};

use overlay_engine::types::SubmitMode;

/// The admission path a `/submit` takes. DERIVED from the request by the
/// endpoint — a caller can request a path, but never one weaker than the
/// endpoint allows it (Rule 15: derive the decision, don't accept it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionPath {
    /// No mode header — engine `CurrentTx`. Barred by SPV.
    CurrentTx,
    /// `historical-tx` — engine `HistoricalTx`. Barred by SPV.
    HistoricalSpv,
    /// `broadcast-gated` — the overlay broadcasts and admits only on
    /// `SEEN_ON_NETWORK`. Barred by the network.
    NetworkGated,
    /// `historical-tx-no-spv` — no SPV, no broadcast, NO BAR. Operator only.
    HistoricalUngated,
}

/// Every variant, for exhaustive property pins. Adding a variant without
/// adding it here fails [`AdmissionPath::as_str`]'s exhaustive match first.
pub const ALL_ADMISSION_PATHS: [AdmissionPath; 4] = [
    AdmissionPath::CurrentTx,
    AdmissionPath::HistoricalSpv,
    AdmissionPath::NetworkGated,
    AdmissionPath::HistoricalUngated,
];

impl AdmissionPath {
    /// Map the `x-submit-mode` header to a path.
    ///
    /// `extensions_enabled == false` is the KILL SWITCH: the header is ignored
    /// entirely and every submit takes the SPV-barred default. Before #347 the
    /// `ENABLE_EXTENSIONS` var was dead config — set in both wrangler configs,
    /// read nowhere in Rust — while the Makefile claimed it disabled
    /// `X-Submit-Mode`. That claim is now true.
    pub fn from_header(header: Option<&str>, extensions_enabled: bool) -> Self {
        if !extensions_enabled {
            return Self::CurrentTx;
        }
        match header {
            Some("historical-tx") => Self::HistoricalSpv,
            Some("historical-tx-no-spv") => Self::HistoricalUngated,
            Some("broadcast-gated") => Self::NetworkGated,
            _ => Self::CurrentTx,
        }
    }

    /// The engine mode this path submits under.
    ///
    /// `NetworkGated` and `HistoricalUngated` share `HistoricalTxNoSpv` — the
    /// network gate, not the engine mode, is what makes the former stronger.
    /// This collapse is exactly why the gate could not be keyed on
    /// `SubmitMode`.
    pub fn engine_mode(self) -> SubmitMode {
        match self {
            Self::CurrentTx => SubmitMode::CurrentTx,
            Self::HistoricalSpv => SubmitMode::HistoricalTx,
            Self::NetworkGated | Self::HistoricalUngated => SubmitMode::HistoricalTxNoSpv,
        }
    }

    /// Does this path run the broadcast + `SEEN_ON_NETWORK` gate?
    ///
    /// THE #347 fix: the route asks this, never `header == "broadcast-gated"`.
    pub fn network_gate_required(self) -> bool {
        matches!(self, Self::NetworkGated)
    }

    /// Does the engine apply the SPV (BEEF merkle proof) bar on this path?
    pub fn spv_barred(self) -> bool {
        !matches!(self.engine_mode(), SubmitMode::HistoricalTxNoSpv)
    }

    /// Does this path carry ANY bar an adversary cannot fake for free?
    pub fn has_bar(self) -> bool {
        self.network_gate_required() || self.spv_barred()
    }

    /// Paths with no bar are operator/peer-only (GASP sync, migration).
    pub fn requires_operator_auth(self) -> bool {
        !self.has_bar()
    }

    /// Stable name for logs, counters and refusal bodies.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CurrentTx => "current-tx",
            Self::HistoricalSpv => "historical-tx",
            Self::NetworkGated => "broadcast-gated",
            Self::HistoricalUngated => "historical-tx-no-spv",
        }
    }
}

/// Enforcement mode, from `SUBMIT_ENFORCE`. Only an explicit opt-in enforces,
/// so a missing var can never cause a surprise outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateMode {
    /// Serve unauthenticated ungated submits, but count them.
    Lenient,
    /// Refuse unauthenticated ungated submits with an honest 401.
    Strict,
}

impl GateMode {
    /// Parse the var: `"true"` (any case) → strict; everything else lenient.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some(v) if v.trim().eq_ignore_ascii_case("true") => Self::Strict,
            _ => Self::Lenient,
        }
    }
}

/// What the route should do with this submit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateDecision {
    /// Run the submit (the gate, if any, still applies).
    Proceed,
    /// Strict mode, an unbarred path, and no operator credential → 401.
    RefuseUnauthenticated,
}

/// THE decision seam — one place answers "may this submit proceed?".
///
/// Derived from the path and the credential, never from the raw header
/// (Rule 15). A barred path is ALWAYS `Proceed`: the bar is the defence, and
/// gating it on a credential too would price an honest player's path (Rule 20).
pub fn decide(path: AdmissionPath, operator_authed: bool, mode: GateMode) -> GateDecision {
    if !path.requires_operator_auth() || operator_authed {
        return GateDecision::Proceed;
    }
    match mode {
        GateMode::Lenient => GateDecision::Proceed,
        GateMode::Strict => GateDecision::RefuseUnauthenticated,
    }
}

// ── Counters (Rule 13: surface the signal; the soak metric that gates the flip)

/// Unauthenticated submits on an unbarred path — the number that must reach ~0
/// before `SUBMIT_ENFORCE=true`.
static UNAUTHENTICATED_UNGATED: AtomicU64 = AtomicU64::new(0);
/// Operator-authenticated submits on an unbarred path (legitimate peer/migration).
static OPERATOR_UNGATED: AtomicU64 = AtomicU64::new(0);
/// Submits refused by strict mode.
static STRICT_REFUSED: AtomicU64 = AtomicU64::new(0);
/// Submits that took a barred path (the honest majority).
static BARRED: AtomicU64 = AtomicU64::new(0);

/// Record one submit's classification. Called once per `/submit`, before the
/// engine runs, so a later engine error still leaves the admission-path signal
/// visible.
pub fn note(path: AdmissionPath, operator_authed: bool, decision: GateDecision) {
    if decision == GateDecision::RefuseUnauthenticated {
        STRICT_REFUSED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    if !path.requires_operator_auth() {
        BARRED.fetch_add(1, Ordering::Relaxed);
    } else if operator_authed {
        OPERATOR_UNGATED.fetch_add(1, Ordering::Relaxed);
    } else {
        UNAUTHENTICATED_UNGATED.fetch_add(1, Ordering::Relaxed);
    }
}

/// The counters, for `/health/invariants`.
///
/// **Modelling boundary (stated per Rule 17):** these are per-isolate
/// `AtomicU64`s, so they are lossy across Cloudflare isolate recycling and are
/// a SOAK SIGNAL, not an audit log. Same posture as the `low-app-layer` #318
/// auth counters. A non-zero `unauthenticatedUngated` is trustworthy evidence
/// that ungated traffic exists; a zero is evidence only across a sustained
/// observation window.
pub fn counters_json() -> serde_json::Value {
    serde_json::json!({
        "unauthenticatedUngated": UNAUTHENTICATED_UNGATED.load(Ordering::Relaxed),
        "operatorUngated": OPERATOR_UNGATED.load(Ordering::Relaxed),
        "strictRefused": STRICT_REFUSED.load(Ordering::Relaxed),
        "barred": BARRED.load(Ordering::Relaxed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE load-bearing invariant, as a POSITIVE exhaustive assertion (Rule 9):
    /// every path either carries a bar or demands an operator credential.
    /// A fifth path added without deciding its bar fails here.
    #[test]
    fn every_path_has_a_bar_or_requires_operator_auth() {
        for path in ALL_ADMISSION_PATHS {
            assert!(
                path.has_bar() || path.requires_operator_auth(),
                "{} is public AND unbarred — the #347 hole",
                path.as_str()
            );
        }
        // Exactly one path is unbarred, and it is the one the CVE was about.
        let unbarred: Vec<_> = ALL_ADMISSION_PATHS
            .iter()
            .filter(|p| !p.has_bar())
            .collect();
        assert_eq!(unbarred, vec![&AdmissionPath::HistoricalUngated]);
    }

    /// The header→path mapping, including the two that share an engine mode.
    #[test]
    fn header_maps_to_the_intended_path() {
        let cases = [
            (None, AdmissionPath::CurrentTx),
            (Some("historical-tx"), AdmissionPath::HistoricalSpv),
            (Some("broadcast-gated"), AdmissionPath::NetworkGated),
            (
                Some("historical-tx-no-spv"),
                AdmissionPath::HistoricalUngated,
            ),
            // An unknown mode must fall to the SPV-barred default, never to
            // the unbarred path.
            (Some("wat"), AdmissionPath::CurrentTx),
            (Some(""), AdmissionPath::CurrentTx),
        ];
        for (header, want) in cases {
            assert_eq!(
                AdmissionPath::from_header(header, true),
                want,
                "header {header:?}"
            );
        }
    }

    /// #347 REGRESSION PIN: the two modes that share `HistoricalTxNoSpv` must
    /// NOT share a gate decision. This is the exact confusion the defect was.
    #[test]
    fn the_two_paths_sharing_an_engine_mode_do_not_share_a_gate() {
        let gated = AdmissionPath::NetworkGated;
        let ungated = AdmissionPath::HistoricalUngated;
        assert_eq!(gated.engine_mode(), ungated.engine_mode());
        assert!(gated.network_gate_required());
        assert!(!ungated.network_gate_required());
        assert!(gated.has_bar());
        assert!(!ungated.has_bar());
    }

    /// The kill switch: with extensions off, NO header can select an unbarred
    /// path — including the attacker's.
    #[test]
    fn extensions_disabled_forces_every_header_onto_a_barred_path() {
        for header in [
            None,
            Some("historical-tx"),
            Some("historical-tx-no-spv"),
            Some("broadcast-gated"),
            Some("wat"),
        ] {
            let path = AdmissionPath::from_header(header, false);
            assert_eq!(path, AdmissionPath::CurrentTx, "header {header:?}");
            assert!(path.has_bar());
            assert!(!path.requires_operator_auth());
        }
    }

    /// The full decision matrix — asserted as INTENDED behaviour (Rule 11),
    /// not read off the implementation.
    #[test]
    fn decision_matrix_is_exhaustive_over_path_auth_and_mode() {
        for path in ALL_ADMISSION_PATHS {
            for authed in [false, true] {
                for mode in [GateMode::Lenient, GateMode::Strict] {
                    let got = decide(path, authed, mode);
                    let want = if path.has_bar() || authed || mode == GateMode::Lenient {
                        GateDecision::Proceed
                    } else {
                        GateDecision::RefuseUnauthenticated
                    };
                    assert_eq!(got, want, "{} authed={authed} {mode:?}", path.as_str());
                }
            }
        }
        // Spelled out for the one cell that is the whole point of the lane:
        assert_eq!(
            decide(AdmissionPath::HistoricalUngated, false, GateMode::Strict),
            GateDecision::RefuseUnauthenticated
        );
        // …and the one that must NEVER refuse (Rule 20 — the honest path).
        assert_eq!(
            decide(AdmissionPath::NetworkGated, false, GateMode::Strict),
            GateDecision::Proceed
        );
    }

    #[test]
    fn gate_mode_defaults_to_lenient_and_only_true_enforces() {
        assert_eq!(GateMode::parse(None), GateMode::Lenient);
        assert_eq!(GateMode::parse(Some("")), GateMode::Lenient);
        assert_eq!(GateMode::parse(Some("false")), GateMode::Lenient);
        assert_eq!(GateMode::parse(Some("1")), GateMode::Lenient);
        assert_eq!(GateMode::parse(Some("true")), GateMode::Strict);
        assert_eq!(GateMode::parse(Some("TRUE")), GateMode::Strict);
        assert_eq!(GateMode::parse(Some("  true  ")), GateMode::Strict);
    }

    /// A barred path is never refused and never counted as ungated, whatever
    /// the credential — the Rule 20 property, stated executably.
    #[test]
    fn barred_paths_never_require_a_credential() {
        for path in ALL_ADMISSION_PATHS.iter().filter(|p| p.has_bar()) {
            assert!(!path.requires_operator_auth(), "{}", path.as_str());
            for mode in [GateMode::Lenient, GateMode::Strict] {
                assert_eq!(decide(*path, false, mode), GateDecision::Proceed);
            }
        }
    }
}
