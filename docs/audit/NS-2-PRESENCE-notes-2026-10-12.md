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
starts the pending watch; the fold counts echoes separately and labels fast-echo.
An already applied topic remains the engine's idempotent submit.
Original pins `ns2_612_*`: independent ladder futures (no shared engine state),
with STORED corroborator closures that are never called. Original RED 101
was a missing-helper compile RED; it did not prove the production change.
The fold adds a pure decision pin and a Reached-arm source pin. All 86 broadcaster tests GREEN, including terminal #519 pins.
Native futures verify admission outcomes, not concurrent D1 route writes.
Captain's lens should read the pending arm, watch and engine deduplication.

## #614, tx-any presence

`crates/low-app-layer/src/routes.rs`: `resolve_tx_any` preserves proven
index hits and terminal memos; unproven held bytes use the existing
`arcade_confirmation_look`. Index misses use `tx_any_external_leg`: the fold asks Arcade only for held
bytes or a locally named submit. Foreign subjects go straight to couriers.
`txany.rs`: `parse_arcade_subject_word` refuses mismatched/missing txids,
faults, orphan/pre-network/terminal words and stale or unstamped hints.
SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES give present/unconfirmed.
`FirstPartyPresent` permits a positive hint with no courier bytes; held
bytes remain preferred, other raw goes through `tx_any_courier_raw_via`
and must hash to the subject. Bad bytes are withheld, never substituted.
MINED/IMMUTABLE require a bounded BUMP whose subject root matches
`beef_guard::canonical_root`; only then is confirmed/height served.
The fold requires a courier-mined proof bound to subject, height and our
header root. Unmined foreign answers are timed courier hints, present:null.
Their raw remains served for display/retry, while the shared fact-reader
guard excludes it from both owed read paths and the cached spender path.
`tx_any_courier_leg_via` refuses Bitails 404 as absence corroboration.
The fold removes the request-time Absent writer: only WoC is established
as a general mempool-capable source; BananaBlocks is unprobed. Arcade
404 is never-carried, Bitails pruned. One capable source is counted unknown.
Arcade health uses a HELD verified anchor, with success memoised per isolate.
Five-minute memo TTL remains; old WoC+Bitails Absent evidence is ignored.
Counters: courier tally plus held-anchor outcomes and one-capable-source unknown.
Endpoint follows the existing client: `${ARCADE_URL}/tx/{txid}`; no live
request was made to verify a different `/v1/tx` alias.
Pins `ns2_614_*`: RED 101, then GREEN; 453 app-layer native tests passed,
including existing courier-mined and memo lifetime tests.
Captain's lens should read both Arcade consumers, proof verification,
raw verification, absence health probes and the memo read/write guards.

## Original verification limits (fold results are in the fold REPORT)

Route tier and actual concurrent D1 writes remain unverified; that tier's
fold runs the make targets in the foreground under the host load gate.
Live Arcade responses, freshness stamps and health anchor retention are
unverified. A missing anchor fails safe to unknown, never absence.
Changed hunks are rustfmt formatted; full worker-workspace fmt check exits
1 on pre-existing drift (also reproduced on the base's d1_budget_seed.rs).
The original #614 RED claim is superseded by NS2P-L4: its fixtures did
not serve the Bitails anchor, so base answered unknown. The fold serves
the anchor and checks the base courier function in a neutered copy.
Final package-scoped clippy (both workers, all targets, -D warnings): 0.
Release wasm32 builds (both workers): 0, launched at 1-minute load 11.18.
Broadcaster tests: 0 (86); app-layer tests: 0 (453); diff whitespace: 0.
No deploy or spend. Captain's from-scratch lens is the next gate.
