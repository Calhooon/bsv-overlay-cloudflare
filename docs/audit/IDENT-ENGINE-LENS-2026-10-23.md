# IDENT-ENGINE-LENS: the lens on overlay PR 156 (bsv-low #558, #590)

Lane IDENT-ENGINE-LENS, branch `lane/IDENT-ENGINE-LENS` over PR 156's head `lane/IDENT-ENGINE` at 0124e357 (two commits: 96d2c886 #590, 0124e357 #558), base 65027163. Written 2026-10-23T20:57:41Z. One lens, from scratch (the code before the PR's text), then the diff, the PR, the issues, the reference (`Engine.ts` at ts-stack f999e0c1a, `abortProvisionalAdmissions` 1192 to 1210, the broadcast 1163 to 1189, `throwOnBroadcastFailure` 405) and LOW's register rows D1 and D13 to D18.

## Verdict: NOT SOUND, one MEDIUM (not money), four LOW, five NOTEs

Nothing in the PR admits, keeps or loses money. The eviction is unchanged in every reachable state: the abort's reads are gated on `aborts_admissions()` (no manager answers it today), a read that faults aborts nothing and the shadow move runs as before, the OPEN marker still stands first. The GASP walk's bounds hold: the per-peer deadline drops a node in flight and keeps the nodes before it (D16), a hung fetch is still a `time` cut, the yieldless bound still ends a peer that never finalizes.

The MEDIUM is that #590's part (2), the time limb charged per resume, is INERT on every host that sets a per-peer budget (LOW's worker and Zanaadu's node both do): the resumes' share is by construction at most one slice and is read before every step, so the resumes of one (peer, topic) still get one slice of time a tick, plus one node. On Zanaadu's measured shape the fan of resumes is "two resumed, one converged, one held back" a tick: the issue's "one or two a tick", unchanged. What #590 delivers on a configured host is the finish-the-node rule, which is real and correct. The fold is small (the share read at a resume's START only, and the owner's line on the 15 s clamp), stated below with two measured counterfactuals.

## Findings

### MEDIUM-1 (#590): the per-resume slice never binds under a per-peer budget; the fan of resumes is as measured in #377

- Where: `crates/overlay-engine/src/gasp.rs` 3317 to 3391 (the resume arm of `ingest`: `earlier_of(latched((budget.deadline)()), share)`), 602 to 609 (`resume_share_ms` = `min(33 % of the peer slice, the graph slice)`), 4432 to 4436 (`walk_graph`'s `due(deadline)` before every step); `engine.rs` 5491 to 5507 (the share set whenever a per-peer budget is).
- The input: any host with `set_peer_sync_budget` (the worker: 30 s peer, 15 s graph, share 9.9 s; Zanaadu: 120 s topic slice, the engine's 60 s graph default clamped to 15 s, share 15 s) and more than one deferred graph of one (peer, topic).
- Why the own slice is dead: the share's deadline is made at the held loop's start and lasts at most one slice; a resume's own slice is made later and lasts one slice; so the share always falls due first, `earlier_of(own, share)` is the share, and it is read before every step. The resumes of a sync therefore run inside the share plus one node, exactly the bound before #590 plus the finished node.
- Measured on the branch (case 3a, the issue's pace, 3 s a node, three 7-node graphs, 120 s peer, 15 s graph): pass 1 defers all three; pass 2: `resumed=2 held_back=1 converged=1` in 15,000 ms (the records then hold 6 and 5 nodes). Case 3b (the worker's shape, 30 s / 15 s / 9.9 s): pass 2 `resumed=2 converged=1`. Pin `i590_c` ("each resume walks its own slice") passes only because it sets NO per-peer budget, a configuration no host runs; `obligations_0111` (with a per-peer budget) shows the real rule: one resume deferred by the share, ten held back.
- Counterfactual A (the share decoupled from the slice: 33 % of the peer slice alone, 39.6 s): pass 2 `resumed=3 converged=3` in 27,000 ms. Counterfactual B (the 15 s clamp lifted, the configured 60 s slice in force, share `min(39.6, 60)` = 39.6 s): all three converge in pass 1 (pass 2 resumes nothing).
- Money consequence: none (discovered graphs converge at the measured rate; the cursor is held below them meanwhile; nothing is admitted unchecked). The consequence is that the PR's and CLAUDE.md's sentence "each resume walks under a fresh per-graph slice of its own" is true of the code and false of every configured host, and Zanaadu's #377 number does not move.
- Fix shape: read the share before a resume STARTS only (held back past it, as now) and let a started resume run under its own slice (the walk's deadline the own slice alone; the per-peer deadline the outer bound). The worst case held loop is unchanged (share + one slice: a node slice already equals a slice), the common case becomes "every resume started inside the share runs whole". Pair it with the clamp line below. Re-state CLAUDE.md's "per resume" paragraph either way.

### LOW-1 (#558): the tell sits between the passes and the ledger's close, unbounded

- Where: `admit_fast.rs` 1346 to 1352 (`tell` after `evict_core`, before `release_spends_of`, the released list's merge and `CLOSE_EVICTION_MARKER_SQL`).
- The input: a future aborting manager whose `abort_admissible_outputs` awaits I/O (a stateful manager releasing a row) under the pending watch's or the callback's `wait_until` ceiling.
- Consequence (PLAUSIBLE, not reproducible today): the invocation ends inside the tell; the passes have run, the OPEN marker stands, the spends are NOT released and the ledger's `releasedSpends` is not written, so the hops keep their pointer to the evicted pot until the next accept, MINED push or readmission runs the row again (the OPEN marker's safe direction: the pot is out of the index). No manager aborts today, so the tell is a no-op and the wrapper's order is as before.
- Fix shape: tell after the close (the managers have no stake in the ledger's order), or race the tell against a short deadline and log a drop.

### LOW-2 (#558): the abort's read precedes the OPEN marker, and a BEEF the twin holds is "none"

- Where: `admit_fast.rs` 1339 to 1345 (the read before `evict_core`, so before `OPEN_EVICTION_MARKER_SQL`); `admission_abort.rs` 117 to 122 (the stored BEEF from `transactions` only, `unwrap_or_default`).
- (a) Outputs that land between the read and the marker (loop 18's class: the subject's Phase-3 write still landing) are moved by the verification pass and never told. (b) The signature carries no txid (the reference's shape), so a tell with an EMPTY BEEF names no admission: measured (case 1 and 2b) on a faulted BEEF read ("told with none") and on a re-run after an eviction that moved `transactions` but not `outputs`, the very case the PR states as "told twice, idempotent": the second tell is `(beef=[], outputs=[0, 1])`, which a manager cannot act on.
- Consequence: a stateful manager's provisional state is not released in those two states; nothing of the eviction changes. Today unreachable.
- Fix shape: read the BEEF from `transactions` and then `transactions_evicted` (one more key read, only when the first is empty), and skip the tell with a log when no BEEF is held.

### LOW-3 (#558): the library consumer the issue names has no trigger

- Where: `engine.rs` 1377 to 1396 (`Engine::abort_provisional_admissions`, called by nothing in the engine), 3683 to 3688 (the non-fatal broadcast branch).
- The engine has no `throwOnBroadcastFailure` equivalent, so parity with the reference's DEFAULT is parity with the branch that never aborts; the only caller is the worker's eviction. Zanaadu's pf managers (the issue's "found by") get the member and no trigger unless their door calls `Engine::abort_provisional_admissions` after a refusal it learns itself. The issue asked "say which, with the register row": the PR says D1 and the worker; the day-of-landing note to Zanaadu must say the call is theirs to make.

### LOW-4 (#590): with no per-peer budget nothing shares the resumes' time; the meter's root bound is stale

- Where: `gasp.rs` 3338 (share `None`: each resume its own slice); `meter.rs` 245 to 252 ("The most one root's graph walk may take" and "the page retains 20,100 ms").
- Measured (case 3c, no per-peer budget, eight deferred graphs): pass 2 resumes all eight, held loop 48,000 ms, where the base charged one slice for all resumes. Per (peer, topic) the bound is now 16 records x one slice x one node. No known host runs a graph budget without a per-peer budget (the worker and Zanaadu set one); the PR's "With no share no time bound is shared" is the statement, without its size.
- The meter: a step started inside the slice finishes under a fresh slice of its own, so one root's walk is now up to two slices (the amended `load_0112` pin documents it: a courier step at 8 s runs to 23 s), and the page's "20,100 ms" is 5,100 ms in the worst case (the PR states the latter in CLAUDE.md; `ROOT_MAX_MS`'s comment does not).

### NOTEs

- N1 (#558, the gate): `read_before_eviction` returns before any read when `managers_that_abort()` is empty (`admission_abort.rs` 131 to 133), and that list is filtered on `aborts_admissions()` (104 to 110), not on a result left unused; the i558 pin's `Unreadable` port panics on a read and passes. A faulting outputs read (case 1): nothing told, the rows moved all the same, `core.faults` empty. The refused pot never stays in the index for the abort.
- N2 (#558, idempotency and placement): the tell is gated on `yielded_to_readmission.is_none()`, which covers a readmission found before the passes (nothing moved) and one found after them (the pass's moves restored). A readmission landing after the second check restores the rows itself with no tell (the PR's stated divergence; the reference has no readmission, so the gap is ours). Case 2: told once, the restore brings the rows back (4 rows, no fault), the manager is not told the admission stands, a fresh corroborated refusal later re-opens and tells again with the BEEF present. For Zanaadu's pf managers a readmission after an abort would leave the head advanced with its provisional admit row released; their reconcile (`pf_reconcile`, re-admit under `HistoricalTx`) is the road back, not this port.
- N3 (#558, the statement budget): with an aborting manager the eviction adds exactly two key reads (`ADMITTED_OUTPUTS_SQL` by txid over `idx_outputs`, `STORED_BEEF_SQL` by primary key), both through `d1::Query` (costed by the ledger); zero today. The retire pass's 96-statement reserve per candidate and the budget tier's ceilings are untouched on this tip; the tier was not run (the captain's at the land).
- N4 (#590, D16 and the belt): case 4b (30 s peer, 15 s graph, two deep graphs, 2 s a node): A walks 0 to 16 s (8 nodes, the node at 14 s finished) and defers `time`; B's step at 28 s runs past 30 s and the per-peer race drops the sync: `deadline_dropped_graphs=1`, B's record `peer_deadline` with 6 nodes (the seventh, in flight, lost and asked again), 15 requests in all, the cursor passes both records (`cursor_passable`: the held keys resume them, not the listing). A node that finishes after the drop is never written: the step's commit is after the await, inside the dropped future (D16 holds; a finalize submit's write section is the only guarded wait). What bounds the request itself: nothing in `gasp_remote::post_text` (no fetch timeout); the node slice (15 s from the step's start, `node_slice`) and the per-peer deadline do. Case 4a: a 20 s request started at 4 s under a 5 s slice is cut `time` at 26 s with the two nodes before it kept and the link asked again next pass (the stall rule's case, as stated). The worker's 240 s belt bounds the tick through the same guarded race.
- N5 (#590, the yieldless bound): a resumed graph that appends a node is PROGRESS (not stalled), never a YIELD. A peer that answers each node in 10 to 15 s now progresses one node a resume where it stalled before, so its quarantine moves from H1's eight failed ticks to the yieldless bound (more than 12 syncs and 3 h with nothing finalized and no cursor move); a hung peer is still a `time` cut with nothing appended, stalled, as before. Bounded either way; stated here because the H1 rule's reach narrows by one shape.
- N6 (the pins): `fold_high1`, `i552`, `e555` (all deltas), `e586`, `e585_d4`, `i551`, `e1d`, `dryrun`, `fix791`, `i558` run by name (84 in `gasp_topic_manager`, 169 in the package), and the engine lib's `obligations_0111`, `load_0112`, `cursor_018`, `proofs_deadline_stalls`, `e555d` (29): green, none weakened. `obligations_0111`'s amended `(stalled, idle) = (0, 0)` and `deferred 1` is the finish-the-node rule, not a loosening (the fourth resume now defers with its root). The i590 pins drive `Engine::start_gasp_sync` over the real remote factory and storage (`i590_record_nodes` reads the record back); their virtual clock charges a request at its start and yields once, so a race sees its deadline only after the cost, which is why the inner node slice is seen before the per-peer deadline in that model (case 4b needed a step started late in the peer slice).

## The 15 s cap, one paragraph for the owner's line

Today `Engine::start_gasp_sync` (engine.rs 5491) holds any configured per-graph slice at `meter::ROOT_MAX_MS` (15 s), the figure sized for twelve 1 s courier rungs; the clamp also sizes the half-deadline, the node slice and, through `resume_share_ms`, the resumes' share. On LOW's worker lifting it changes nothing (15 s configured, 30 s peer). On Zanaadu's node (120 s topic slice, the engine's 60 s default) lifting it puts 60 s in force: a hung fetch is cut at 60 s instead of 15 s inside their 120 s, the share becomes 39.6 s, and their 7-node graphs at 3 s a node converge in their FIRST pass (counterfactual B) instead of over two or three ticks; the worst case held loop becomes 39.6 s plus one 60 s node slice, leaving the page 20 s of the 120 s, which is the cost to weigh. Recommendation: lift the clamp to the per-peer budget (`min(graph_ms, peer_ms)`, the per-graph budget's own doc already asks the caller to keep it below the per-peer slice), keep `ROOT_MAX_MS` as the courier-rung sizing only (the rungs are a count, not a time), fold MEDIUM-1's start-only share gate with it so the lifted slice reaches more than the first resume, and leave LOW's worker at 15 s.

## The cases (a patch beside the audit, not pushed to the lane's branch)

Kept at `/Volumes/Lanes/out/wt-overlay-IDENT-ENGINE-LENS/cases/`: `lens_ident_cases.rs` (included at the end of `engine::tests` by `engine_include.patch`) and `lens_ident_cases_worker.rs` (included at the end of `admit_fast::tests`). Run against 0124e357:

| case | shape | result on the branch |
|---|---|---|
| 1 (#558) faulting outputs read, then faulting BEEF read, an aborting manager | shadow over the shipped schema | told 0, rows evicted (3 in the twin), no fault; then told 1 with an EMPTY BEEF, rows evicted |
| 2 (#558) a readmission after a tell | `restore_twins` (what `readmit_if_evicted` runs), then a fresh refusal at a later stamp | told once; 4 rows back, no tell; re-opened and told a second time with the BEEF |
| 2b (#558) a re-run after a half-faulted eviction (`transactions` already moved) | the row deleted by hand | told `(beef=[], outputs=[0, 1])` |
| 3a (#590) Zanaadu's shape: 120 s peer, 15 s graph, three 7-node graphs, 3 s a node | pass 2 | resumed 2, converged 1, held back 1, 15,000 ms |
| 3a-A counterfactual: share = 33 % of the peer slice alone | pass 2 | resumed 3, converged 3, 27,000 ms |
| 3a-B counterfactual: the 15 s clamp lifted (60 s) | pass 1 | all three converged, pass 2 idle |
| 3b (#590) the worker's shape: 30 s / 15 s / 9.9 s | pass 1 and 2 | pass 1 deferred 2, dropped 1 at 30 s; pass 2 resumed 2, converged 1 |
| 3c (#590) no per-peer budget, eight deferred graphs | pass 2 | resumed 8, held loop 48,000 ms |
| 4a (#590) a 20 s request inside a 5 s slice | one pass, then a resume | cut `time` at 26 s, 2 nodes kept, the link asked again |
| 4b (#590) a step started at 28 s of a 30 s peer slice | one pass | `deadline_dropped_graphs` 1, records `("time", 8)` and `("peer_deadline", 6)`, 15 requests, the cursor passes both |

## Commands and exits (all foreground, `CARGO_TARGET_DIR` private per workspace under `~/bsv/targets`)

| command | result |
|---|---|
| `cargo test -p bsv-overlay-engine --features memory-storage --lib i590` | 3 passed, exit 0 |
| `cargo test -p bsv-overlay-engine --features memory-storage --lib` (with the 5 cases) | 303 passed, exit 0 (298 + 5) |
| `cargo test -p bsv-overlay-engine --features memory-storage --lib -- obligations_0111 load_0112 i590 i558 cursor_018 proofs_deadline_stalls e555d` | 29 passed, exit 0 |
| `cargo test -p bsv-overlay-engine --features memory-storage --tests` | 244 passed over 18 binaries (script_door_fold 131 s), exit 0 |
| `cargo test -p bsv-overlay-discovery -p parity-harness` | 1,011 passed, exit 0 |
| `cargo test -p overlay-integration-tests --features bsv-overlay-engine/memory-storage --test gasp_topic_manager -- fold_high1 i552 e555 e586 e585_d4 i558 e1d i551 dryrun fix791` | 84 passed, exit 0 |
| `cargo test -p overlay-integration-tests --features bsv-overlay-engine/memory-storage` | 169 passed, exit 0 |
| `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib` (with the 4 worker cases) | 976 passed, 1 ignored, exit 0 |
| `cargo clippy --workspace --all-targets --features bsv-overlay-engine/memory-storage -- -D warnings` (scratch removed) | exit 0 |
| `cargo clippy --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --all-targets -- -D warnings` | exit 0 |
| `cargo fmt --all -- --check` (root) | exit 0 |
| `rustfmt --check` on `admission_abort.rs`, `lib.rs`, `admit_fast.rs` | the first two clean; `admit_fast.rs` has 22 diffs on the BASE too (none in the PR's hunks): pre-existing |
| `python3 scripts/check-storage-ownership.py` | 1,274 references match, exit 0 |

Not run: wasm32, the tiers (`make ci-d1-budget`, `ci-route`), the eviction's reads in the budget tier (zero statements today). The counterfactual patches (`resume_share_ms` without `.min(graph_budget_ms)`; engine.rs 5492 without `.min(ROOT_MAX_MS)`) were applied, run on the Zanaadu case, and reverted; the tree is 0124e357 plus this file.
