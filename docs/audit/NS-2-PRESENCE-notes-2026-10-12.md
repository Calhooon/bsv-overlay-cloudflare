# NS-2-PRESENCE readiness, 2026-10-12

Base: origin/main 4634090 (engine 0.1.1). No deploy or spend.
Read bsv-low #612, #614, #519, CLAUDE.md D1 (#519/#602), the
couriers convention, and stack tracker charter sections 1 and 2.
Every broadcaster word remains a hint; chain confirmation needs evidence.

## #612, admission

`crates/overlay-cloudflare/src/broadcaster.rs`: `submit_once_and_gate`
binds the POST echo to the subject before classification. A reached echo
uses `reached_submit_echo`: fast mode returns `SyncAccepted`, otherwise
the existing witnessed gate stands. Fatal and orphan echoes keep their
own arms; the pre-flight terminal branch still asks the #519/#602 ladder.
`broadcast_efs_gated_with` passes `AcceptedPending` without corroborating:
STORED at TAAL/GorillaPool cannot refuse a first-party stored acceptance.
`routes.rs`'s `AcceptedPending` arm admits with network_seen unlatched,
starts the pending watch and counts `submit_fast_admitted_total` (existing).
An already applied topic remains the engine's idempotent submit.
Pins `ns2_612_*`: two concurrent ladder futures for one subject and a
never-admitted known-SEEN subject, with STORED corroborators: RED 101,
then GREEN. All 86 broadcaster tests GREEN, including terminal #519 pins.
Native futures verify admission outcomes, not concurrent D1 route writes.
Captain's lens should read the pending arm, watch and engine deduplication.

## #614, tx-any presence

`crates/low-app-layer/src/routes.rs`: `resolve_tx_any` preserves proven
index hits and terminal memos; unproven held bytes use the existing
`arcade_confirmation_look`. Index misses use `tx_any_external_leg`, now
Arcade-first through the injected `tx_any_presence_leg_via` port.
`txany.rs`: `parse_arcade_subject_word` refuses mismatched/missing txids,
faults, orphan/pre-network/terminal words and stale or unstamped hints.
SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES and ACCEPTED give present/unconfirmed.
`FirstPartyPresent` permits a positive hint with no courier bytes; held
bytes remain preferred, other raw goes through `tx_any_courier_raw_via`
and must hash to the subject. Bad bytes are withheld, never substituted.
MINED/IMMUTABLE require a bounded BUMP whose subject root matches
`beef_guard::canonical_root`; only then is confirmed/height served.
Courier-mined answers still carry claimedHeight, not a proven height.
`tx_any_courier_leg_via` refuses Bitails 404 as absence corroboration.
A real request-time absence needs Arcade 404 (the same endpoint must
serve a known subject's usable word) and WoC 404 behind its healthy route.
Unknowns never satisfy the Absent memo writer's existing two-source guard.
Five-minute memo TTL remains; old WoC+Bitails Absent evidence is ignored.
Counters: existing courier tally records Arcade calls; no new counter.
Endpoint follows the existing client: `${ARCADE_URL}/tx/{txid}`; no live
request was made to verify a different `/v1/tx` alias.
Pins `ns2_614_*`: RED 101, then GREEN; 453 app-layer native tests passed,
including existing courier-mined and memo lifetime tests.
Captain's lens should read both Arcade consumers, proof verification,
raw verification, absence health probes and the memo read/write guards.

## Verification limits

Route tier and actual concurrent D1 writes remain unverified; that tier's
existing harness starts background Bash processes, forbidden in this lane.
Live Arcade responses, freshness stamps and health anchor retention are
unverified. A missing anchor fails safe to unknown, never absence.
Changed hunks are rustfmt formatted; full worker-workspace fmt check exits
1 on pre-existing drift (also reproduced on the base's d1_budget_seed.rs).
Both #614 pins were also grafted onto 4634090 with only the native port:
RED 101, each answered present:false. Restored branch: six pins GREEN 0.
Final package-scoped clippy (both workers, all targets, -D warnings): 0.
Release wasm32 builds (both workers): 0, launched at 1-minute load 11.18.
Broadcaster tests: 0 (86); app-layer tests: 0 (453); diff whitespace: 0.
No deploy or spend. Captain's from-scratch lens is the next gate.
