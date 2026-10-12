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
