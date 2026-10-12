# NS-2-ARCREG review fold (#640)

Review: `bsv-low/docs/audit/NS-2-ARCREG-review-2026-10-12.md` (read-only).
This is the overlay's register text for the captain to carry to LOW D1.

## H1: registration window

Registration shares NL-6d's sizing constants: 32 ancestors in a request,
256 in a consumer run. It is a cap, not a paused corroboration: the subject
is always posted, then only its answer decides admission. Ancestors are
selected breadth-first from the subject's EF input references, nearest first,
with duplicates excluded; scripts are skipped as borrowed bytes, not parsed
or hydrated. The cursor remains the corroborator's; registration selects the nearest
ancestors on each invocation. A consumer reserves its one walk first;
registration spends only the unused portion of the same 256-leg allowance.
A paused walk ends the pass before rung 3. ARCREGD-H1 separates the acts: the
FEED is every unproven leg in one non-registering `/txs` body, parents first,
as in the base. Matching terminal reads suppress those legs in both acts.
REGISTRATION alone is windowed; the subject is always posted and registered
under its register-once rule, then the nearest 32 ancestors (the unused portion of 256 in a run)
are the selection. Unselected ancestors are fed but remain unregistered and
counted by `arc_registration_legs_skipped_total`; their proofs retain the
poll/courier backstop.

Arcade's `services/api_server/routes.go` exposes only `GET /tx/:txid` for
transaction status: there is no batched status-read endpoint to use.
`POST /txs` shares one callback header pair for the body, so registration
under each txid requires a separate `POST /tx`. Both ancestor operations
have a two-second deadline, including response-body reads. Faults, refusals
and timeouts are best effort and cannot prevent the subject's own POST.

Rung-3 calls before this fold: `2N + 1`, unbounded. After: at most
`2 min(N, 32) + 2 = 66` in requests. Consumer registrations use
only the remainder of the 256 shared legs: walk plus registration is at most
512 ancestor calls, plus at most 40 subject/probe/feed/poll calls (552).
This includes two fallback hosts per corroborated leg and initial polls at
time zero. The request's background dual push remains its existing path;
every consumer run suppresses it, including a byte-deferred run at cursor 0. For 100 ancestors: 32 registered, subject registered, 68
skipped, 66 calls (formerly 201). For 80: 32 plus subject, 48 skipped,
66 calls (formerly 161). Known ancestors omit callback headers; terminal
skips are described below when folded. The pre-#640 batch was one POST.

Pins use an injectable transport seam plus source checks connecting the seam,
timeout adapters, body/token identity and subject gate to production.
Arcade's live behaviour on this windowed registration remains unverified.

## L1: rung-3 echo and D1 register text

**Since #640 (2026-10-12, overlay `lane/ns-2-arcreg`): a callback's token
is the txid of the ONE transaction its POST carries.** The ladder's
full-batch rung (attempt 3, reached on an orphan view or an inconclusive
pre-batch corroboration) feeds every unproven leg whole and parents-first
in ONE non-registering Arcade `/txs` body; the registration window applies
only to registrations: the subject always, then the nearest 32 ancestors
per request (at most 256 per consumer run), each alone to Arcade `/tx`
under its own txid and registered only when Arcade's own status read does
not hold it; their answers are discarded. Known terminal ancestors (REJECTED /
DOUBLE_SPEND_ATTEMPTED) are neither registered nor re-fed, and are counted
in the same skipped counter. Then it posts the subject alone
under its own txid with the register-once rule unchanged. The `/txs` body
that registered every ancestor under the SUBJECT's token is gone; the whole
parents-first feed has no callback headers. The rest
remain unregistered and counted; their proofs remain the backstop poll's
and the couriers'. An ancestor's MINED push now arrives as its own: D6's
`unknown_txid` ack for a hop, the stitch and `readmit_if_evicted` for a held
pot, never a 401. The subject's rung-3 POST is a one-transaction body, so
the echoed-status short-circuit applies there as on rungs 1 and 2 (under
admit-fast an echoed Pending answers `AcceptedPending`, with the pending
watch armed by the route). A witnessed acceptance keeps the two-witness bar
for unproven ancestry. This confirms review §5 with the owner's 32/256 cap
and nearest-first registration choice, with the base's complete parents-first
feed restored by ARCREGD-H1. ARCREGD-H2: a paused walk stops the ladder for
that pass; each consumer run shares 256 ancestor legs between one walk and
registrations, reuses the exhaustion word, and spends at most 552 broadcaster
subrequests under the assumed 1,000 default (448 left for binding calls,
whose size-dependent tail remains unverified under the house's #571 bound).

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

## Earlier fold verification (historical, superseded below)

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

## ARCREGD-H1 fold (NS-2-ARCREG-FOLD-4)

The 40th-ancestor orphan resolves at rung 3 even though only 32 ancestors
plus the subject register. The independent body pin checks all 41 legs in
parents-first order and exactly one feed call. Both pins are RED on a
neutered copy restoring the reviewed head's capped, nearest-first feed;
GREEN on the fold. The nine earlier ARCREG pins remain GREEN, with the
source pin now requiring the non-registering whole `/txs` body.

## ARCREGD-H2 fold (NS-2-ARCREG-FOLD-4)

The typed transport seam preserves pause as a signal. In a consumer it
presents the subject once, then walks before the orphan shortcut; on pause
no later rung or registration runs.
After a completed walk, only the unused portion of 256 is available for
registration, even on an inconclusive word. Exhaustion reuses that word;
a request's paused pre-batch walk likewise blocks every later rung.
Consumer runs suppress the unbounded #413 duplicate push in `wait_until`,
including the first run of a byte-deferred job. The foreground walk supplies
the witness instead. The request's direct orphan feed is unchanged.

The 300-ancestor model includes two hosts per walked leg, the nearest
registrations and a conservative allowance for polls/probes/subject calls.
The GREEN runs spend 548 and 552 modeled calls (32 to 288 to 300).
On the reviewed control-flow mutant it spends over 1,000 each run, cannot
persist its cursor and ends failed after five runs. On the fold the cursor
advances 32 to 288 to 300 in two runs, at most 552 calls each. The independent
pre-batch-pause and shared-budget/cache pins are also RED then GREEN.
This native budget model is not a platform-limit or real queue execution.

**D1 delta register addition (verbatim):**

Rung 3 feeds every unproven leg whole and parents-first in one non-registering `/txs` body, except legs the registration window found terminal; only registration is windowed: the subject always, then the nearest 32 ancestors per request. A paused walk stops the pass; on a consumer run the walk precedes the subject-only resubmit and pre-batch corroboration. Each run shares 256 ancestor legs between one walk and registrations and reuses the exhaustion word: at most 552 broadcaster subrequests under the assumed 1,000 default, before binding calls, the engine’s 256 predecessor reads and the pending watch. A run’s cascade-retryable refusal is retried as in a request; Arcade-terminal refusal still requires #519’s two-source corroboration. A first byte-deferred run dual-pushes only the subject (at most two fetches); later runs never dual-push, and the request’s push is unchanged. The skipped-ancestor counter is per rung-3 pass.

The broadcaster figure bounds fetches, not arbitrary storage: D1 chunks,
admission topics and background binding work still depend on the body.
Cloudflare's actual platform limit and live Arcade delivery remain unverified.
The second delta and LOW's register pickup belong to the captain.

## FOLD-4 final verification

Changed Rust hunks formatted; no Cargo.lock change. Rust 1.95.0 (the
repository's declared toolchain floor): package clippy `--all-targets -- -D warnings`
exit 0; package test exit 0, 718 passed and one pre-existing ignored test
(704 unit tests, including all nine earlier ARCREG pins plus five delta pins).
The machine's default Rust 1.98 clippy found three pre-existing warnings
(two formatting borrows, `change_flush::post_body`'s unit error); Rust 1.95
passed without lint suppressions or unrelated code changes.

RED evidence outside the tree: H1's capped/nearest feed mutant fails both
40-hop pins (exit 101); its independent reversed complete feed fails the
order pin (101). H2's reviewed-control-flow mutant fails all three pins
(101): 1,062 modeled calls a run, cursor stuck at 32 for five runs, failed.
Followed by GREEN on the lane's fourteen ARCREG pins (exit 0); H2's job
spends 548 then 552 modeled calls, cursor 32 to 288 to 300.

The exact `make ci-route` cells were run under a foreground Python supervisor
with owned process groups and teardown, not background Bash: ARC-auth exit 0,
NL-6c EF-work exit 0, NL-6d corroboration exit 0. Node 22.23.3 via nvm;
`WASM_BINDGEN_BIN=~/bsv/targets/wbg-0.2.126/bin/wasm-bindgen`;
`LANE_BASE=14860` (free through +12). Fresh load gates: 4.79 for ARC-auth,
3.18 for corroboration, both below 24. Isolated local persistence, no deploy
or spend. Logs: `/tmp/ns2-arcreg-fold-4/{arc-auth,ef-work,corroboration}-cell.log`.
The actual 1,000-leg local job reached done; every ancestor was corroborated
once across five subject presentations. Native budget pins do not claim to
exercise Cloudflare's production API ceiling.

Unverified: live Arcade orphan/proof delivery; the production platform default
(the code assumes 1,000), the variable storage/binding tail; the full CI tiers
outside the requested cells; the captain's second delta and LOW register pickup.
Cleanup removes only this lane's target and neutered copy; logs remain in /tmp.

## ARCREGD2-H1 fold (NS-2-ARCREG-FOLD-5)

The consumer wrapper routes synchronous refusals through `ladder_step`'s
request arms. A cascade-retryable ancestor word retries and is never a
definitive refusal; body-only script/fee failures retain the ladder's
validation arm. Arcade-terminal network status still uses #519's fresh
indexer judgement; one broadcaster's stored word evicts nothing.

Two native pins RED on the reviewed wrapper (exit 101): the first byte-deferred
run's stale ancestor word answered Rejected / 422 / Done, and the corroborator
was never asked. GREEN (exit 0): the paused walk leaves the job retryable with
no Rejected outcome to enter the route's durable-refusal arm; a completing
walk corroborates acceptance, and a non-cascade validation failure uses the
same terminal arm as a request. All 14 existing pins plus these two pass.
Evidence: `/tmp/ns2-arcreg-fold-5/h1-{red,green}.log`. These are native seams,
not an end-to-end queue / D1 refusal-record trace.


## ARCREGD2-M1 fold (NS-2-ARCREG-FOLD-5)

`WorkBudget::dual_push_legs` is the real route's selector: requests retain
all ancestry legs, a first byte-deferred run selects its subject alone, and
later runs select none. At most two extra broadcaster fetches fit the
existing 40-call allowance; no duplicate unbounded consumer push returns.
The proven-ancestry subject pin RED on the neutered selector (no first-run
legs, exit 101), then GREEN with the first-run arm restored (exit 0). It
also checks request ancestry and empty / later-run selections and pins the
route's use of that selector. Evidence: `/tmp/ns2-arcreg-fold-5/m1-{red,green}.log`.
The D1 addition above incorporates all six second-delta register corrections.
