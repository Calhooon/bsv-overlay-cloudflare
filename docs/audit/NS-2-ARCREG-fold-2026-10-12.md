# NS-2-ARCREG review fold (#640)

Review: `bsv-low/docs/audit/NS-2-ARCREG-review-2026-10-12.md` (read-only).
This is the overlay's register text for the captain to carry to LOW D1.

## H1: registration window

Registration shares NL-6d's sizing constants: 32 ancestors in a request,
256 in a consumer run. It is a cap, not a paused corroboration: the subject
is always posted, then only its answer decides admission. Ancestors are
selected breadth-first from the subject's EF input references, nearest first,
with duplicates excluded; scripts are skipped as borrowed bytes, not parsed
or hydrated. The cursor remains the corroborator's; registration uses the
nearest ancestors on each invocation. Unselected ancestors are not posted or
registered by this rung. Their proofs remain the poll backstop's and the
couriers', as before #640. `arc_registration_legs_skipped_total` counts them.

Arcade's `services/api_server/routes.go` exposes only `GET /tx/:txid` for
transaction status: there is no batched status-read endpoint to use.
`POST /txs` shares one callback header pair for the body, so registration
under each txid requires a separate `POST /tx`. Both ancestor operations
have a two-second deadline, including response-body reads. Faults, refusals
and timeouts are best effort and cannot prevent the subject's own POST.

Rung-3 calls before this fold: `2N + 1`, unbounded. After: at most
`2 min(N, 32) + 1 = 65` in requests, `513` in consumer runs, plus unchanged
subject polling. For 100 ancestors: 32 registered, subject registered, 68
skipped, 65 calls (formerly 201). For 80: 32 plus subject, 48 skipped,
65 calls (formerly 161). Known ancestors omit callback headers; terminal
skips are described below when folded. The pre-#640 batch was one POST.

Pins use an injectable transport seam plus source checks connecting the seam,
timeout adapters, body/token identity and subject gate to production.
Arcade's live behaviour on this windowed registration remains unverified.

## L1: rung-3 echo and D1 register text

**Since #640 (2026-10-12, overlay `lane/ns-2-arcreg`): a callback's token
is the txid of the ONE transaction its POST carries.** The ladder's
full-batch rung (attempt 3, reached on an orphan view or an inconclusive
pre-batch corroboration) posts at most 32 unproven ancestors per request,
256 per consumer run, NEAREST the subject first, each alone to Arcade `/tx`
under its own txid and registered only when Arcade's own status read does
not hold it; their answers are discarded. Known terminal ancestors (REJECTED /
DOUBLE_SPEND_ATTEMPTED) are neither registered nor re-fed, and are counted
in the same skipped counter. Then it posts the subject alone
under its own txid with the register-once rule unchanged. The `/txs` body
that registered every ancestor under the SUBJECT's token is gone. The rest
remain unregistered and counted; their proofs remain the backstop poll's
and the couriers'. An ancestor's MINED push now arrives as its own: D6's
`unknown_txid` ack for a hop, the stitch and `readmit_if_evicted` for a held
pot, never a 401. The subject's rung-3 POST is a one-transaction body, so
the echoed-status short-circuit applies there as on rungs 1 and 2 (under
admit-fast an echoed Pending answers `AcceptedPending`, with the pending
watch armed by the route). A witnessed acceptance keeps the two-witness bar
for unproven ancestry. This confirms review §5 with the owner's 32/256 cap
and nearest-first choice in place of its proposed pause or 128-leg tail.

The L1 wiring pin drives rung 1 orphan → rung 3 echoed Pending →
`AcceptedPending`, with zero corroborations. Source pins tie the real rung's
one-transaction body to the echo fast return and the route's pending watch;
the Worker Fetch runtime cannot run in native unit tests.

## L2: bounded refusal line (D1 register continuation)

`/arc-ingest`'s refusal line names the pushed txid (64 hex or `other`),
`txStatus` (at most 80 characters), User-Agent (at most 64 characters), and
bearer shape (`64-hex` or `other`) plus its byte length, never its value:
`POST /arc-ingest txid=".." txStatus=".." ua=".." bearer=64-hex bearerLen=64 -> 401 (<why>)`.
Fields are debug-quoted on one line; the formatter receives only bearer
shape metadata. Both no-token and bad-token arms use it; their counters are
unchanged. With two mismatching credentials, shape describes Authorization's
Bearer candidate first, then X-CallbackToken; absent/malformed schemes have
shape `other`, length 0. The auth classifier still compares both candidates.
The pin covers a 1-MiB body, huge status/UA, malformed headers, both
shape words and absence of the credential's value.

## L3: terminal ancestor skip

A status with matching identity and REJECTED / DOUBLE_SPEND_ATTEMPTED skips
both registration and re-feed, counted in `arc_registration_legs_skipped_total`.
The window bounds status reads even if all selected ancestors are terminal;
terminal legs do not cause the walk to search farther. Known nonterminal
ancestors are primed without registration. A mismatched terminal response
is an unknown look, not permission to suppress a leg. The seam pin covers
both terminal statuses, a known pending leg, a mismatched response and the
subject still posted last.

## NOTEs and register continuation

N1: `register_full_batch_with` is the rung's transport seam. Behavioural
pins exercise window counts, body/token dispatch selection, known/terminal
statuses and reaching the subject. Source pins tie the seam to Worker Fetch,
two-second deadlines, the actual per-leg token/body and the subject gate.
The L1 echo/watch linkage remains a wiring plus source pin, the module's
native-test shape; no native test claims to execute Worker Fetch.
N2: the subject's existing per-ladder register-once guard is unchanged;
probe faults can still re-register known txids, as before, within the cap.
N3: the ci-route cell and Makefile are unchanged by this fold. The cell still
reads the existing P4 worker log and pins ancestor auth, not live proof delivery.
N4: both refusal arms use the bounded formatter; auth and refusal counters are unchanged.
N5: `arc_ingest_unknown_txid_total` now also counts ancestors' registered
MINED proof pushes for txids no topic holds, stated in its counter doc.
The six new pins and the three existing arcreg pins are retained.

## RED / GREEN evidence

All mutations are in `~/bsv/ns2-arcreg-fold-neuter`, never the lane tree.
The unmodified lane follows each RED run with GREEN.

- H1: three seam pins RED with unlimited/wrong-order selection and subject
  dispatch neutered; GREEN (100 and 80 ancestors, 300 under the run window,
  both JOIN seats' nearest parents, ancestor faults/timeouts).
- L1: rung-3 Pending/watch pin RED with subject body length changed to 2;
  GREEN with length 1, `AcceptedPending`, no corroborations and watch armed.
- L2: bounded-body/header pin and existing refusal-log pin RED with field
  bounds and shape words neutered; GREEN with 1-MiB body and malformed header.
- L3: terminal-ancestor pin RED with terminal skip neutered; GREEN for both
  terminal statuses, a known pending ancestor and a mismatched identity.
- Existing per-leg source and own-token pins: refreshed RED with dispatch
  token and subject-last ordering neutered, then GREEN on the lane.

D1 register text above is review §5 confirmed with the owner's window
choice, terminal skip, bounded log/shape wording and pending-watch sentence.
The captain owns LOW's register/design pickup and the light delta. No deploy
or spend is part of this lane. Arcade's live windowed behaviour is unverified.

## Final verification and owed work

Changed Rust hunks formatted with rustfmt; package clippy `--all-targets -D warnings`
GREEN. `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare`
GREEN: 713 passed, 1 pre-existing ignored stored-row export test (699 unit tests,
90 broadcaster tests including the fold's pins, all nine arcreg pins).
No Cargo.lock change. The wasm-bindgen belt CLI is 0.2.126, matching the lock.

Route tier OWED: `uptime` at its actual gate was 24.46 (16:50 local), above
24; neither `ci-d1-budget` nor `ci-route` started a worker. A prior 19.27
reading was not used to bypass the fresh gate.

Known constraint conflict: the unchanged ci-route cell creates a 77-character
User-Agent and requires the entire value in the refusal log; L2 requires the
log to cut it to 64. The existing assertion would therefore fail. The owner's
instruction to leave the cell unchanged is respected; clarification was
requested. A one-line fixture-only patch is prepared outside the tree at
`/tmp/ns2-arcreg-ua64.patch` (13-character prefix + 51 random hex characters).
Its JavaScript syntax check passed. The Makefile, tier, ports and cell remain
unchanged. Approving that fixture change is needed before an all-GREEN route
run; no native test substitutes for the owed route tier.

Unverified: the captain's light delta and LOW register/design pickup;
Arcade's live windowed registration; route behaviour (owed). No deploy or spend.
