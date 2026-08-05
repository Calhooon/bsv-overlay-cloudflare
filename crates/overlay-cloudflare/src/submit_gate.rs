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
//! | [`NetworkGated`](AdmissionPath::NetworkGated) (`broadcast-gated`) | overlay broadcasts, admits only on `SEEN_ON_NETWORK` | **yes — the ONLY public path** |
//! | [`CurrentTx`](AdmissionPath::CurrentTx) (no header) | none (SPV is not a bar — see [`AdmissionPath::has_bar`]) | operator only |
//! | [`HistoricalSpv`](AdmissionPath::HistoricalSpv) (`historical-tx`) | none (same) | operator only |
//! | [`HistoricalUngated`](AdmissionPath::HistoricalUngated) (`historical-tx-no-spv`) | none | operator only |
//!
//! The load-bearing invariant, pinned exhaustively by
//! `every_path_has_a_bar_or_requires_operator_auth`: **every path either has a
//! bar or requires operator auth.** That is a positive assertion over an
//! exhaustive match, so adding a fifth path without deciding its bar fails the
//! suite rather than silently opening a hole (Rule 9: never a bare
//! `assert!(!contains(..))`; Rule 15: widen the type, don't carve an exception).
//!
//! **The invariant survived; its PREMISE did not.** The first version of this
//! module counted SPV as a bar, so the same exhaustive pin passed while three
//! of the four paths were freely admitting fabrications. A pin is only ever as
//! true as the predicate it quantifies over — see [`AdmissionPath::has_bar`]
//! for the two shapes that defeat SPV and the live evidence for each.
//!
//! Rule 5 — what is irreducibly a service here: **nothing.** Once network
//! acceptance is the only public bar, the endpoint exercises no discretion
//! that Bitcoin has not already exercised for it.
//!
//! ## RESIDUAL — this module does NOT close the escalation class (Rule 19)
//!
//! Stated here, in code, rather than only in a commit message. Making
//! admission network-gated converts free fabrication into a *fee-bearing*
//! attack; it does not make the CONTENT true. A funded adversary can still buy
//! real transactions carrying victim-named markers. The enumeration-starvation
//! money path therefore stays reachable until the promotion predicate binds
//! `unknownPot` to network reality (bsv-low #283) and the client stops
//! discarding the `truncated` bit that documents a cut enumeration. "The gate
//! landed" is not "the class is closed" — the pressure moves to whichever
//! input is still evictable.
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
//! The unbarred modes become operator-only, behind a DEDICATED
//! `SUBMIT_OPERATOR_TOKEN` — deliberately **not** `ADMIN_TOKEN`, which also
//! gates `/admin/evictOutpoint`, `/admin/ban` and `/admin/startGASPSync`:
//! issuing that to every submit operator (the watchtower included) would grant
//! eviction of any outpoint from the index, which is exactly the primitive the
//! enumeration-starvation money path needs. And deliberately not BRC-103: that
//! middleware exists for identity-scoped *reads* (`low-app-layer`, bsv-low
//! #318), whereas a peer/operator sync path wants a shared operator secret,
//! not a per-identity handshake. Bitcoin remains the rate limiter on every
//! public path: the bar is always "the network already accepted this", never
//! "you have paid us".
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
//! * **Strict (`SUBMIT_ENFORCE=true`):** an unauthenticated submit on ANY
//!   unbarred path — which is every path except `broadcast-gated`, including
//!   the no-header default — is refused `401` with an honest body naming both
//!   remedies. `broadcast-gated` is never refused, in any configuration.
//!
//! **Closure criteria — gated on PRODUCER-SIDE facts we control, NOT on the
//! counter.** The soak metric is attacker-writable (`/health/invariants` is
//! unauthenticated, and an adversary can hold the number wherever it likes),
//! so it is corroboration only, never the trigger:
//!
//!   1. **bsv-low #351 lands and is RELEASE-PINNED: every marker producer emits
//!      an ancestry-carrying BEEF and submits `broadcast-gated`.** This is the
//!      ENABLER, not a follow-up. Two producers are blocked today and both are
//!      BEEF-shape problems with the parents available at the call site:
//!      `overlay.ts::submitBeef` builds `new Beef()` + `mergeRawTx` with NO
//!      ancestry and **ten** marker surfaces route through it, and the
//!      watchtower's `wrap_raw_tx_beef_v1` wraps a bare raw tx. (An earlier
//!      version of this note claimed the client "already builds
//!      ancestry-carrying BEEFs" — that cites `broadcastViaOverlay`, a
//!      DIFFERENT producer, and is false for the callers that matter.)
//!   2. the watchtower likewise migrates, or is issued
//!      `SUBMIT_OPERATOR_TOKEN` (never `ADMIN_TOKEN`).
//!   3. both releases are pinned and SOAKED for a full recovery cycle.
//!   4. only then flip `SUBMIT_ENFORCE=true`. Flip back for instant rollback.
//!
//! **DO NOT FLIP BEFORE (1).** `submitPotPartyMarker` and `submitPotRefundBeef`
//! ride the ungated `submitBeef`, and THOSE ROWS ARE THE RECOVERY ENUMERATION
//! a wiped device reads. A stale client refused at fund time never files the
//! row; every caller is fire-and-forget, so the 401 is swallowed silently; a
//! later wiped device then has nothing to enumerate. Flipping early would
//! re-create, with our own hands, the exact money path #347 exists to close
//! (Rule 14: the victims of this failure are precisely the population least
//! able to report it).
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
//! **The prerequisite that is NOT free:** the gated path first converts the
//! BEEF to Extended Format, and `ef::beef_to_ef_batch` hard-errors when the
//! SUBJECT's source transactions are absent from the BEEF. A single-tx
//! proofless BEEF therefore returns 400 *before any network call*.
//!
//! **BOTH honest producers are blocked on this today** — that is what makes
//! bsv-low #351 the enabler rather than a follow-up. `overlay.ts::submitBeef`
//! builds `new Beef()` + `mergeRawTx` with NO ancestry and ten marker surfaces
//! route through it; the watchtower's `wrap_raw_tx_beef_v1` wraps a bare raw
//! tx. (An earlier version of this paragraph said "the browser client is fine
//! (it merges ancestry on its gated path)" — that describes
//! `broadcastViaOverlay`, a DIFFERENT producer, and is false for the callers
//! that matter. It contradicted the closure criteria thirty lines above it.)

use std::sync::atomic::{AtomicU64, Ordering};

use overlay_engine::types::SubmitMode;

/// The admission path a `/submit` takes. DERIVED from the request by the
/// endpoint — a caller can request a path, but never one weaker than the
/// endpoint allows it (Rule 15: derive the decision, don't accept it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionPath {
    /// No mode header — engine `CurrentTx`. **NOT barred**: the engine's SPV
    /// pass is not an admission bar (see [`AdmissionPath::has_bar`]). Operator only.
    CurrentTx,
    /// `historical-tx` — engine `HistoricalTx`. **NOT barred** for the same
    /// reason as [`CurrentTx`](Self::CurrentTx); the name is the wire mode's,
    /// not a claim about admission. Operator only.
    HistoricalSpv,
    /// `broadcast-gated` — the overlay broadcasts and admits only on
    /// `SEEN_ON_NETWORK`. Barred by the network.
    NetworkGated,
    /// `historical-tx-no-spv` — no SPV, no broadcast, NO BAR. Operator only.
    HistoricalUngated,
}

/// Every variant, for exhaustive property pins.
///
/// **This array can silently shrink, and four pins quantify over it** — the
/// same "subject leaves its field of view" mode that produced the #347
/// CRITICAL, in this same file. A re-gate proved it: a fifth variant reachable
/// from a public header, with arms in `engine_mode`/`as_str`/`from_header` but
/// NOT here, compiled and left every "exhaustive" pin green.
///
/// Note the trap in the OBVIOUS fix: a "variant count" computed by iterating
/// this array always equals its length, so it is `f(x) == f(x)` in a costume
/// (Rule 9 — "is there an edit that changes both sides at once?"). It was
/// tried here first and passed probe G unchanged.
///
/// What actually closes it is
/// `every_header_reachable_path_is_covered_by_the_pinned_set`, which pins the
/// property that matters rather than a count: it reads the header literals out
/// of [`AdmissionPath::from_header`]'s own source, drives each one through it,
/// and asserts every reachable result is in this array. A variant reachable
/// from a new public header is then RED whether or not anyone remembers to
/// update a number.
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
    /// and every submit takes the default path — which is itself UNBARRED and
    /// therefore operator-only. `broadcast-gated` is exempt and honoured
    /// unconditionally, so the switch can never route a caller off the gate. Before #347 the
    /// `ENABLE_EXTENSIONS` var was dead config — set in both wrangler configs,
    /// read nowhere in Rust — while the Makefile claimed it disabled
    /// `X-Submit-Mode`. That claim is now true.
    pub fn from_header(header: Option<&str>, extensions_enabled: bool) -> Self {
        let normalized = header.map(|h| h.trim().to_ascii_lowercase());
        // `broadcast-gated` is honoured UNCONDITIONALLY — it is the only
        // barred path, so the kill switch must never be able to route a
        // caller OFF it. (Gate finding: the previous collapse-everything
        // switch turned the network gate OFF, admitting a fabrication that
        // even origin/main refused.)
        if normalized.as_deref() == Some("broadcast-gated") {
            return Self::NetworkGated;
        }
        if !extensions_enabled {
            return Self::CurrentTx;
        }
        match normalized.as_deref() {
            Some("historical-tx") => Self::HistoricalSpv,
            Some("historical-tx-no-spv") => Self::HistoricalUngated,
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

    /// Does this path carry a bar an adversary cannot fake for free?
    ///
    /// **Network acceptance is the ONLY honest public bar, and SPV is NOT
    /// one.** An earlier version of this module counted "the engine runs SPV"
    /// as a bar. That premise was false and the exhaustive pin above it was
    /// therefore a confident proof of nothing (Rule 11 — the seam blessed the
    /// behaviour it modelled; the evidence behind it had only ever tested one
    /// input shape).
    ///
    /// Two shapes defeat SPV, both verified live through the real route:
    ///
    /// 1. **A zero-input transaction has nothing to prove.**
    ///    `Beef::verify_valid(false)` returns `valid = true` with an EMPTY
    ///    root set, so the chain-tracker loop iterates zero times and the
    ///    topic managers admit on byte format alone.
    /// 2. **A real parent plus a fabricated child.** A BUMP-proven ancestor is
    ///    free and public (WoC, our own `/beef/:txid`, ARC callbacks); pairing
    ///    one with an unsigned fabricated child still verifies, because SPV
    ///    proves the ANCESTRY is real and says NOTHING about the subject being
    ///    signed, funded, or broadcast.
    ///
    /// Only `broadcast-gated` asks the question that matters — *did the
    /// network accept THIS transaction* — and it refuses both shapes
    /// (`bad-txns-vin-empty` for the first). So it is the one public path;
    /// everything else is operator-only.
    /// **Scope:** this classification governs the `/submit` ROUTE only. Other
    /// admission entry points (`engine.submit` for GASP sync, `peer_crawler`)
    /// admit on paths classified unbarred here; they are not route-reachable
    /// (GASP is `Disabled` for every LOW topic and the peer list is pinned),
    /// but the invariant is about the public HTTP surface, not the engine.
    ///
    /// Kept as a distinct name from [`network_gate_required`](Self::network_gate_required)
    /// even though the two are now the same predicate: they answer different
    /// questions ("is this path defensible?" vs "does the route run the
    /// broadcast block?"), and collapsing them would make a future divergence
    /// silent. If a second genuine bar ever exists, only this one changes.
    pub fn has_bar(self) -> bool {
        self.network_gate_required()
    }

    /// Paths with no bar are operator/peer-only (GASP sync, migration).
    ///
    /// That is now every path except [`NetworkGated`](Self::NetworkGated).
    /// **Endgame (bsv-low #351):** once the two blocked producers emit
    /// ancestry-carrying BEEFs, `HistoricalUngated` has no honest caller left
    /// and should be DELETED rather than credentialed — at which point every
    /// public path is barred by construction and this credential has no
    /// remaining job. It cannot be deleted before then: ten client marker
    /// surfaces still route through it, and dropping the mapping today would
    /// send them to `CurrentTx`, whose SPV check their proofless BEEFs fail.
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
///
/// Precisely: the credential is *evaluated* unconditionally by the caller, but
/// it cannot change the outcome for a barred path. (An earlier comment claimed
/// a barred path "never consults the credential", which was false as written —
/// the behaviour was always correct, the sentence was not.)
pub fn decide(path: AdmissionPath, operator_authed: bool, mode: GateMode) -> GateDecision {
    if !path.requires_operator_auth() || operator_authed {
        return GateDecision::Proceed;
    }
    match mode {
        GateMode::Lenient => GateDecision::Proceed,
        GateMode::Strict => GateDecision::RefuseUnauthenticated,
    }
}

/// The COMPLETE wiring decision for one `/submit`, derived in one place.
///
/// Gate finding (H1/M-5): the route's header→path→decision→branch wiring had
/// ZERO automated coverage — deleting the entire refusal from `routes.rs`
/// compiled and left CI fully green. **The wiring IS the defect class** (#347
/// was a wiring bug, not a logic bug), so it cannot live only as inline route
/// code.
///
/// **An earlier version of this doc claimed "there is no second way for it to
/// express the decision". That was FALSE and is exactly the failure it warns
/// about** (Rule 22): a re-gate deleted the whole 401 block and disabled the
/// gate branch, and BOTH left 1826/1826 green. The struct made the route
/// *tidier*, not its use *observable*, and the claim then sat in the source
/// where it would stop the next auditor from running the check (Rule 10).
///
/// What actually constrains the route now is [`SubmitAction`]: the route
/// consumes it through an EXHAUSTIVE match, so deleting the refusal arm is a
/// COMPILE ERROR rather than a silent green. See
/// [`crate::routes::tests`]' source pins for the branch that a match cannot
/// cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitPlan {
    /// The derived admission path.
    pub path: AdmissionPath,
    /// Proceed, or refuse with an honest 401.
    pub decision: GateDecision,
    /// The engine mode to submit under.
    pub engine_mode: SubmitMode,
    /// Whether the route must run the broadcast + SEEN gate block.
    pub run_network_gate: bool,
    /// Whether this submit is an unauthenticated caller on an unbarred path
    /// (the lenient-window signal the operator logs and counts).
    pub unauthenticated_unbarred: bool,
}

/// What the route must DO — consumed through an exhaustive `match`, so an arm
/// cannot be deleted without breaking the build (Rule 22: the test is whether
/// anything can observe the seam being IGNORED).
///
/// A bare `SubmitPlan` could not deliver that: reading a struct field is
/// optional, and a probe that simply removed the `if plan.decision == …`
/// block compiled and stayed green. An enum arm is not optional.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitAction {
    /// Unbarred path + unauthenticated + strict → honest 401.
    RefuseUnauthenticated(AdmissionPath),
    /// The one public path: run the broadcast + SEEN gate, then admit.
    ProceedWithNetworkGate(AdmissionPath),
    /// An operator on an unbarred path, or the lenient window → admit
    /// without the gate. `lenient_unbarred` marks the counted soak case.
    ProceedWithoutGate {
        path: AdmissionPath,
        lenient_unbarred: bool,
    },
}

impl SubmitAction {
    /// The engine mode this action submits under.
    pub fn engine_mode(self) -> SubmitMode {
        self.path().engine_mode()
    }

    /// The admission path this action was derived from.
    pub fn path(self) -> AdmissionPath {
        match self {
            Self::RefuseUnauthenticated(p) => p,
            Self::ProceedWithNetworkGate(p) => p,
            Self::ProceedWithoutGate { path, .. } => path,
        }
    }
}

/// Derive the ACTION — the form the route is obliged to consume exhaustively.
///
/// **This also RECORDS the classification** ([`note`]), because the caller must
/// not be able to choose what gets counted. `note` used to be `pub` and to take
/// a `SubmitAction` by value, so the route could hand it a fabricated one: a
/// re-gate passed `note(SubmitAction::ProceedWithNetworkGate(action.path()))`
/// and every submit reported as `barred` while behaviour was unchanged — 0
/// compile errors, native suite green, every source pin matching, and nothing
/// in the route tier reads `/health/invariants`. Fixed the same way as probes H
/// and F′: remove the class rather than document it (Rule 15 — don't hand a
/// call site a decision it can get wrong; derive it). The counter is now fed by
/// the derivation itself, so there is no second value for anyone to supply.
///
/// Consequence, stated because it is the point of the shape: one derivation
/// feeds both behaviour and the metric, so a future single-derivation bug
/// corrupts the metric instead of hiding behind an honest second copy.
///
/// **Boundary:** this makes the function impure. It is called exactly once per
/// `/submit` in production (pinned by the `action_for(` count in `routes.rs`'s
/// source pins), so there is no double-count; the native cells that call it do
/// increment the process-local counters, which nothing asserts on.
pub fn action_for(
    header: Option<&str>,
    extensions_enabled: bool,
    operator_authed: bool,
    mode: GateMode,
) -> SubmitAction {
    let plan = plan_submit(header, extensions_enabled, operator_authed, mode);
    let action = match plan.decision {
        GateDecision::RefuseUnauthenticated => SubmitAction::RefuseUnauthenticated(plan.path),
        GateDecision::Proceed if plan.run_network_gate => {
            SubmitAction::ProceedWithNetworkGate(plan.path)
        }
        GateDecision::Proceed => SubmitAction::ProceedWithoutGate {
            path: plan.path,
            lenient_unbarred: plan.unauthenticated_unbarred,
        },
    };
    note(action);
    action
}

/// Derive the whole plan. The ONLY entry point the route uses.
pub fn plan_submit(
    header: Option<&str>,
    extensions_enabled: bool,
    operator_authed: bool,
    mode: GateMode,
) -> SubmitPlan {
    let path = AdmissionPath::from_header(header, extensions_enabled);
    let decision = decide(path, operator_authed, mode);
    SubmitPlan {
        path,
        decision,
        engine_mode: path.engine_mode(),
        // A refused submit never runs the gate; a barred one always does.
        run_network_gate: decision == GateDecision::Proceed && path.network_gate_required(),
        unauthenticated_unbarred: path.requires_operator_auth() && !operator_authed,
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

/// Record one submit's classification, derived ENTIRELY from the action the
/// route is about to act on.
///
/// **PRIVATE, and called only from [`action_for`]** — no caller can supply the
/// argument, which is the whole point (see `action_for`'s doc for the re-gate
/// that fabricated one while this was `pub`).
///
/// It used to take `(path, operator_authed, decision)` — a second argument
/// list, built beside a second `plan_submit(...)` call. A re-gate flipped
/// `operator_authed` to `true` on ONLY the behavioural call: it compiled, the
/// whole native suite stayed green, every caller was treated as an
/// authenticated operator (the CRITICAL restored), and the counter went on
/// reporting honestly because it was fed from the other derivation.
///
/// Two derivations of one decision is the defect; the fix is to DELETE one of
/// them (Rule 10), not to test that they agree. There is now exactly one
/// derivation, it does the counting itself, and no argument list is left to
/// diverge.
fn note(action: SubmitAction) {
    match action {
        SubmitAction::RefuseUnauthenticated(_) => {
            STRICT_REFUSED.fetch_add(1, Ordering::Relaxed);
        }
        SubmitAction::ProceedWithNetworkGate(_) => {
            BARRED.fetch_add(1, Ordering::Relaxed);
        }
        SubmitAction::ProceedWithoutGate {
            lenient_unbarred: true,
            ..
        } => {
            UNAUTHENTICATED_UNGATED.fetch_add(1, Ordering::Relaxed);
        }
        SubmitAction::ProceedWithoutGate {
            lenient_unbarred: false,
            ..
        } => {
            OPERATOR_UNGATED.fetch_add(1, Ordering::Relaxed);
        }
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
        // Exactly ONE path carries a bar — network acceptance. Every other
        // path is unbarred and therefore operator-only. (This previously
        // asserted only `HistoricalUngated` was unbarred, which was the false
        // premise: SPV was being counted as a bar.)
        let barred: Vec<_> = ALL_ADMISSION_PATHS
            .iter()
            .filter(|p| p.has_bar())
            .collect();
        assert_eq!(barred, vec![&AdmissionPath::NetworkGated]);
    }

    /// MEDIUM-B: no path reachable from a PUBLIC HEADER may escape the pinned
    /// set, or every "exhaustive" pin in this module silently stops covering it.
    ///
    /// A re-gate (probe G) added a fifth variant with arms in `engine_mode`,
    /// `as_str` and `from_header` but NOT in `ALL_ADMISSION_PATHS`: it compiled
    /// and all 1826 tests stayed green — the same "subject leaves its field of
    /// view" mode that produced the original CRITICAL, in this same file.
    ///
    /// This pins the PROPERTY, not a count: the header literals are read out of
    /// `from_header`'s own source (so a new header cannot be added without this
    /// cell seeing it), driven through the real function, and every result must
    /// be a member of the pinned set. Built per Rule 9 — a POSITIVE count of
    /// the literals found, a needle split so it cannot match itself, and
    /// comments stripped so prose is never counted.
    #[test]
    fn every_header_reachable_path_is_covered_by_the_pinned_set() {
        let src = include_str!("submit_gate.rs");
        // Isolate `from_header`'s body (the construct, not a region — Rule 9).
        let sig = ["pub fn from_", "header("].concat();
        let start = src.find(&sig).expect("from_header must exist");
        let body = &src[start..];
        let end = body.find("\n    }").expect("from_header body must terminate");
        let body = &body[..end];
        // Strip line comments so a doc example can never inflate the corpus.
        let code: String = body
            .lines()
            .map(|l| match l.find("//") {
                Some(i) => &l[..i],
                None => l,
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Every `Some("…")` arm literal in the real matcher.
        let mut headers: Vec<String> = Vec::new();
        let open = ["Some(", "\""].concat();
        let mut rest = code.as_str();
        while let Some(i) = rest.find(&open) {
            rest = &rest[i + open.len()..];
            if let Some(j) = rest.find('"') {
                headers.push(rest[..j].to_string());
                rest = &rest[j + 1..];
            }
        }
        assert!(
            headers.len() >= 3,
            "expected at least the three known mode literals, found {headers:?} \
             — the scan drifted and this cell is asserting nothing"
        );

        // The property: every header-reachable path is in the pinned set.
        for h in &headers {
            for extensions in [true, false] {
                let path = AdmissionPath::from_header(Some(h), extensions);
                assert!(
                    ALL_ADMISSION_PATHS.contains(&path),
                    "header {h:?} (extensions={extensions}) reaches {} which is \
                     NOT in ALL_ADMISSION_PATHS — every exhaustive pin in this \
                     module is silently not covering it",
                    path.as_str()
                );
            }
        }
        // The default and an unknown header too.
        for h in [None, Some("wat")] {
            assert!(ALL_ADMISSION_PATHS.contains(&AdmissionPath::from_header(h, true)));
        }
        // No duplicates padding the set.
        let mut seen = ALL_ADMISSION_PATHS.to_vec();
        seen.dedup();
        assert_eq!(seen.len(), ALL_ADMISSION_PATHS.len(), "duplicate variants");
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
            // An unknown mode must fall to the DEFAULT path (itself unbarred
            // and therefore operator-only), never be treated as a public one.
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

    /// The kill switch must collapse only UNBARRED paths — it must NEVER be
    /// able to route a caller OFF the network gate.
    ///
    /// This REPLACES a cell that asserted the opposite ("every header lands on
    /// CurrentTx") as if it were correct. That cell was Rule 11 in its purest
    /// form: it blessed a switch which, when thrown, turned the network gate
    /// OFF and admitted a fabrication that even origin/main had refused —
    /// a lever documented as the INCIDENT response.
    #[test]
    fn the_kill_switch_can_never_disable_the_network_gate() {
        for header in [
            Some("broadcast-gated"),
            Some("BROADCAST-GATED"),
            Some("  broadcast-gated  "),
        ] {
            for extensions in [true, false] {
                let path = AdmissionPath::from_header(header, extensions);
                assert_eq!(
                    path,
                    AdmissionPath::NetworkGated,
                    "header {header:?} extensions={extensions}"
                );
                assert!(path.has_bar());
                assert!(path.network_gate_required());
            }
        }
        // The UNBARRED headers DO collapse when extensions are off…
        for header in [Some("historical-tx"), Some("historical-tx-no-spv"), Some("wat"), None] {
            assert_eq!(
                AdmissionPath::from_header(header, false),
                AdmissionPath::CurrentTx,
                "header {header:?}"
            );
        }
        // …and collapsing them never manufactures a bar out of nothing.
        assert!(!AdmissionPath::CurrentTx.has_bar());
    }

    /// CRITICAL REGRESSION PIN (gate-found, 2026-08-05).
    ///
    /// SPV IS NOT A BAR. Three of four paths were public-and-unbarred while an
    /// exhaustive invariant pin over them was green, because the predicate it
    /// quantified over was false. The live probes that proved it: a zero-input
    /// transaction (nothing to prove — `verify_valid` returns true with an
    /// empty root set) and a real BUMP-proven parent carrying a fabricated
    /// unsigned child. Both were admitted `200 present:true` with NO header
    /// and NO credential, including through the `tm_potparty` money topic.
    ///
    /// If this cell ever goes green with SPV counted as a bar again, the free
    /// admission class is re-opened and the five-link enumeration-starvation
    /// money path is reachable.
    #[test]
    fn spv_is_not_a_bar_only_network_acceptance_is() {
        // Exactly ONE path is public; it is the network-gated one.
        let public: Vec<_> = ALL_ADMISSION_PATHS
            .iter()
            .filter(|p| !p.requires_operator_auth())
            .copied()
            .collect();
        assert_eq!(
            public,
            vec![AdmissionPath::NetworkGated],
            "network acceptance is the ONLY honest public bar"
        );
        // The three SPV-adjacent paths are barred by NOTHING.
        for path in [
            AdmissionPath::CurrentTx,
            AdmissionPath::HistoricalSpv,
            AdmissionPath::HistoricalUngated,
        ] {
            assert!(!path.has_bar(), "{} must not count SPV as a bar", path.as_str());
            assert!(path.requires_operator_auth(), "{}", path.as_str());
        }
        // The exact live probe that reproduced the CRITICAL: no header at all,
        // unauthenticated, strict mode → REFUSED.
        let plan = plan_submit(None, true, false, GateMode::Strict);
        assert_eq!(plan.decision, GateDecision::RefuseUnauthenticated);
        assert!(!plan.run_network_gate);
        // …and the same request WITH the honest public mode proceeds, gated.
        let plan = plan_submit(Some("broadcast-gated"), true, false, GateMode::Strict);
        assert_eq!(plan.decision, GateDecision::Proceed);
        assert!(plan.run_network_gate, "the honest public path must be GATED");
    }

    /// `action_for` is what the ROUTE consumes, so it needs its own cells —
    /// pinning `plan_submit` alone leaves the plan→action mapping uncovered.
    ///
    /// Found by RED-verifying the route harness: neutering the Refuse mapping
    /// HERE (returning `ProceedWithoutGate` instead) left the route's
    /// exhaustive match intact, the source pins matching, and the ENTIRE
    /// native suite green — only the HTTP harness caught it. That is Rule 16
    /// in miniature: both sides of the boundary were pinned and the boundary
    /// itself was not.
    #[test]
    fn action_for_maps_every_decision_to_the_action_the_route_consumes() {
        // The refusal must become the Refuse action, not a silent proceed.
        assert_eq!(
            action_for(None, true, false, GateMode::Strict),
            SubmitAction::RefuseUnauthenticated(AdmissionPath::CurrentTx),
        );
        assert_eq!(
            action_for(Some("historical-tx-no-spv"), true, false, GateMode::Strict),
            SubmitAction::RefuseUnauthenticated(AdmissionPath::HistoricalUngated),
        );
        // The one public path always gates, in every mode, authed or not.
        for mode in [GateMode::Lenient, GateMode::Strict] {
            for authed in [false, true] {
                assert_eq!(
                    action_for(Some("broadcast-gated"), true, authed, mode),
                    SubmitAction::ProceedWithNetworkGate(AdmissionPath::NetworkGated),
                );
            }
        }
        // An operator on an unbarred path proceeds ungated and is NOT counted
        // as the lenient soak case.
        assert_eq!(
            action_for(Some("historical-tx-no-spv"), true, true, GateMode::Strict),
            SubmitAction::ProceedWithoutGate {
                path: AdmissionPath::HistoricalUngated,
                lenient_unbarred: false,
            },
        );
        // The lenient window proceeds ungated and IS counted.
        assert_eq!(
            action_for(Some("historical-tx-no-spv"), true, false, GateMode::Lenient),
            SubmitAction::ProceedWithoutGate {
                path: AdmissionPath::HistoricalUngated,
                lenient_unbarred: true,
            },
        );
    }

    /// Exhaustively: `action_for` must agree with `plan_submit` for EVERY
    /// input, so the two cannot drift (the plan is the seam, the action is
    /// what the route sees).
    #[test]
    fn action_for_agrees_with_plan_submit_everywhere() {
        for header in [
            None,
            Some("historical-tx"),
            Some("historical-tx-no-spv"),
            Some("broadcast-gated"),
            Some("wat"),
        ] {
            for extensions in [true, false] {
                for authed in [false, true] {
                    for mode in [GateMode::Lenient, GateMode::Strict] {
                        let plan = plan_submit(header, extensions, authed, mode);
                        let action = action_for(header, extensions, authed, mode);
                        assert_eq!(action.path(), plan.path);
                        assert_eq!(action.engine_mode(), plan.engine_mode);
                        match action {
                            SubmitAction::RefuseUnauthenticated(_) => assert_eq!(
                                plan.decision,
                                GateDecision::RefuseUnauthenticated,
                                "a Refuse action must come from a Refuse decision"
                            ),
                            SubmitAction::ProceedWithNetworkGate(_) => {
                                assert_eq!(plan.decision, GateDecision::Proceed);
                                assert!(plan.run_network_gate);
                            }
                            SubmitAction::ProceedWithoutGate { .. } => {
                                assert_eq!(plan.decision, GateDecision::Proceed);
                                assert!(!plan.run_network_gate);
                            }
                        }
                    }
                }
            }
        }
    }

    /// The wiring seam: a refused submit never runs the gate, a barred one
    /// always does, and the engine mode always matches the path.
    #[test]
    fn plan_submit_wires_every_combination_consistently() {
        for header in [
            None,
            Some("historical-tx"),
            Some("historical-tx-no-spv"),
            Some("broadcast-gated"),
            Some("wat"),
        ] {
            for extensions in [true, false] {
                for authed in [false, true] {
                    for mode in [GateMode::Lenient, GateMode::Strict] {
                        let plan = plan_submit(header, extensions, authed, mode);
                        assert_eq!(plan.engine_mode, plan.path.engine_mode());
                        assert_eq!(
                            plan.run_network_gate,
                            plan.decision == GateDecision::Proceed
                                && plan.path.network_gate_required()
                        );
                        if plan.decision == GateDecision::RefuseUnauthenticated {
                            assert!(!plan.run_network_gate, "a refusal never broadcasts");
                            assert!(!authed && plan.path.requires_operator_auth());
                            assert_eq!(mode, GateMode::Strict);
                        }
                        // The honest public path is NEVER refused, in any
                        // configuration whatsoever (Rule 20).
                        if plan.path == AdmissionPath::NetworkGated {
                            assert_eq!(plan.decision, GateDecision::Proceed);
                            assert!(plan.run_network_gate);
                        }
                    }
                }
            }
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
