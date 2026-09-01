//! CROSS-REPO PIN (bsv-low A2, gate round 4 HIGH-1) — the watchtower
//! (`~/bsv/bsv-low` `workers/low-watchtower/src/relay_lib.rs`,
//! `overlay_message_fresh_verdict`) parses the gated `/submit` 422 MESSAGE to
//! tell a FRESH per-tx verdict (a synchronous Arcade validation refusal, or a
//! corroborated wrapper whose BOTH halves are a synchronous `ARC HTTP 461–464`)
//! from a wrapper around two STORED echoes (`REJECTED …` halves — what the
//! corroborators answer on a re-present of a txid they already hold). These
//! are the literal producer strings it parses. Changing any of them is a
//! co-evolution change: update the tower's parser + its cells in the same
//! campaign, or a give-up belt on real money goes silently dead.
//!
//! Source-text pins (the strings, not behavior — the behavior is pinned by the
//! unit cells beside each producer).

const ROUTES: &str = include_str!("../src/routes.rs");
const BROADCASTER: &str = include_str!("../src/broadcaster.rs");

#[test]
fn the_422_reason_shapes_the_tower_parses_are_stable() {
    // routes.rs: every gated refusal is served as `network rejected: {reason}`.
    assert!(
        ROUTES.contains(r#"json_error(&format!("network rejected: {reason}"), 422)"#),
        "routes.rs must serve the gated refusal as `network rejected: {{reason}}` (422)"
    );
    // (C) the corroborated exhaustion wrapper and the two-host fold.
    assert!(
        BROADCASTER.contains(
            r#""network did not accept {subject_txid}; retried; corroborated by second broadcaster: {r}""#
        ),
        "corroborated_exhaustion's wrapper text"
    );
    assert!(
        BROADCASTER.contains(r#""both corroborators rejected — taal: {a}; gorillapool: {b}""#),
        "fold_refuse_bar's two-host text — the tower splits on `taal: ` / `; gorillapool: `"
    );
    // Each corroborator half: a non-2xx SYNCHRONOUS answer is `ARC HTTP {status}: {body}`;
    // a 2xx error txStatus is `{txStatus} {extraInfo}` (a stored echo on a re-present).
    assert!(
        BROADCASTER.contains(r#"Ok(ArcOutcome::Rejected(format!("ARC HTTP {status}: {body}")))"#),
        "corroborator_verdict's non-2xx rejection text"
    );
    assert!(
        BROADCASTER.contains(r#"format!("{} {}", arc_resp.tx_status, arc_resp.extra_info)"#),
        "corroborator_verdict's 2xx echo text"
    );
    // (S) Arcade's synchronous validation refusal shapes.
    assert!(
        BROADCASTER.contains(r#"SubmitOutcome::SyncRejected(format!("(status {s}) {reason}"))"#),
        "the #228 additive-status sync refusal text"
    );
    assert!(
        BROADCASTER.contains(
            r#"const ARCADE_VALIDATION_FAILED_ERROR: &str = "transaction failed validation";"#
        ),
        "the structured 400 body's error literal"
    );
    // The async stale-validator dress must keep NOT reaching a 422: an
    // AsyncRejected step retries and its exhaustion ends at the corroborator.
    assert!(
        BROADCASTER.contains("GateStep::AsyncRejected(_) => Ladder::Retry"),
        "an async REJECTED never returns a 422 on its own"
    );
}
