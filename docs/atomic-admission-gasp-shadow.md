# GASP's shadow under `admit_graph` (bsv-low #657, R7.7 item 5)

The engine's later PR of Zanaadu's atomic admission round (the owner's D31),
on A's first PR (draft #43, `atomic-admission-a1` at `9d5a75a`). The design:
zanaadu-v2 `docs/audit/2026-10-13-eve/atomic-admission-design.md`, "The next
slice, re-cut against what was built (R7.7)" item 5, 3.3a "The GASP gap"
(R7.3) and test 29's "not shadowed" rows. The lens's corrections
(`docs/atomic-admission-gasp-shadow-lens.md` on `lane/atomic-gasp-shadow-lens`,
`5536f32`) are taken on `lane/atomic-gasp-shadow-2`; see "Lens corrections
taken" below.

Until this PR the engine's own GASP finalize submits (`submit_finalized_graphs`
calling `submit_bounded` under `historical-tx-no-spv`, one graph at a time) had
no observer, so Zanaadu's census marks the HTTP corpus's four GASP chaos
records "not shadowed". This PR is their shadow, the engine's.

## What it does

- **The feature.** The shadow is the cargo feature `gasp-shadow` of
  `bsv-overlay-engine`, default OFF: the `gasp_shadow` modules, the engine's
  shadow fields, `Engine::set_gasp_shadow`, the drain and every shadow arm of
  the finalize loop exist only with it. An `off` build links none of A's GASP
  path (the lens's MEDIUM 1). `bsv-overlay-cloudflare` forwards it (default
  off; no Worker hosts the shadow today); `overlay-integration-tests` turns it
  on by default.
- **The switch.** `Engine::set_gasp_shadow(AdmissionMode, Rc<dyn GaspShadow>)`
  (`gasp_shadow.rs`). `AdmissionMode::parse` reads the Worker's words: `off`
  (the default), `shadow`, and `on` refused ("ADMISSION_MODE=on is not
  enabled: the admission log runner is a later slice"). The shadow runs only
  under `Shadow` with a host set.
- **The host** (`GaspShadow`): `snapshot(scope, budget)` (a disposable engine
  over an isolated copy of the served store, its unit runner, and the rows and
  bytes it read), `now_ms()` and `record(row)`. Its contract, in the trait's
  doc: `snapshot` honours the budget it is handed (it reads no further than
  the rows and bytes limbs and answers the engine's `not shadowed: budget
  (reads: ...)` words), the snapshot's engine reaches no network, and
  `snapshot` is the one call made inside the served race. The reference host,
  `MemoryGaspShadow` (with `memory-storage`), refuses before copying a store
  past the reads limb, else copies the whole `MemoryStorage`
  (`MemoryStorage::shadow_copy`, `shadow_size`) and builds the shadow engine
  with the caller's managers and no tracker, broadcaster, fetcher or peer
  factory. The Worker's D1 host (its scoped SQLite, the slice's `ShadowEngine`)
  is the Worker's to add; every Worker stays `off`.
- **Per graph and body** (`engine/gasp_shadow.rs`). In the served path, before
  a graph's first served submit: one snapshot, which serves the graph AND its
  first body (one state, one copy: the lens's NOTE 6), and the graph's job
  queued in the graph's place. Before each later body's served submit: one
  snapshot. After each served submit: the served word (its STEAK in topic
  order, beside what landed, the finalize's not-landed reason and the topic
  managers' errors) and the served judgement. After the graph's last served
  body (or the one that did not land): the rows the graph reaches, read from
  the served store.
- **After the sync** (once, at the end of `start_gasp_sync`, after every
  served write of it: each peer's race, cursor and health rows; the lens's
  LOW 4): each body walked by A's `walk_with_mode` on its snapshot
  (`historical-tx-no-spv`, `NotNow`), handed the evidence the served path
  gathered (none: the finalize does not walk and asks no network, the anchor
  check already ran; `ServedEvidence` returns that record and nothing else),
  `judge_dry` on the untouched snapshot beside the served judgement, and the
  body's own unit through `admit_graph_observed`. For a graph: every body
  walked on the graph's snapshot, the first body's `judge_dry`, then
  `admit_graph` over the whole graph (each body its own unit, stopping at the
  first that does not land), the first unit's answer the first body's, and
  the same rows read from the shadow's store. One census row per body and
  one per graph, the graph's after its bodies'.
- **The delta classes** are the slice's (`same`, `not_shadowed`,
  `unit_refusal`, `evidence`, `words`) plus two for a graph row:
  `materialization` (the words differ, the rows agree: a stop point or a
  status apart, D1 below) and `rows_differ` (the rows differ: D2 below; the
  lens's MEDIUM 3). `unit_refusal` is the owner's `unit-wide` first delta
  (D34, given 2026-10-14): A refused the unit where the served path landed a
  topic, the intended change, counted. A body row is `words` when the words
  differ or `judge_dry`'s judgement differs from the served one.
- **The budget** is the slice's per comparison (4,096 rows, 16 MiB and
  2,000 ms) plus one limb per sync: at most `SHADOW_SNAPSHOTS_HELD` (64)
  snapshots held at once (taken and not yet compared), each counted by a guard
  it carries; past it no snapshot is taken (`ShadowBudget`,
  `set_gasp_shadow_budget`). Past a limb the row says `not shadowed: budget
  (reads: ...)`, `(time: ...)` or `(snapshots: ...)`. A failed snapshot is
  `not shadowed: shadow setup: ...`; a failed `record` is logged; neither
  changes anything served.
- **`off`** runs none of it: no snapshot, no read, no extra await. Each new
  step in the finalize loop is a `match` on `None` with no await on that arm,
  and the drain is guarded by the same check; without the feature none of it
  is compiled.
- `admit_graph` is now `admit_graph_observed` with a no-op observer (the shadow
  words each body's unit answer as it is given).

## The census row

`{kind: body|materialization, peer, topic, graph (root txid), txid, position,
served: {status, bytes, landed_topics}, shadow: {status, bytes, landed_topics},
delta, served_judgement, shadow_judgement, served_evidence, shadow_evidence,
shadow_seqs, snapshot_rows, snapshot_bytes, shadow_ms}`.

A body word's bytes are `{"steak", "refused", "faults", "applied_topics",
"deduped_topics", "not_landed", "manager_errors"}` (served, and A's unit as
the `on` runner's door will word it: `Lookup`/`Deterministic` 422, `NotNow`
503, `Transient` 500). `not_landed` is the served finalize loop's own reason
for counting the body not landed ("the topic manager failed on it", "not
durable: ...", or the engine error), `null` when it landed and always `null`
for A's unit, which commits and goes on. `manager_errors` names each topic
manager that answered `Err` and its error (`topic_manager.<topic>: <error>`),
captured from the engine's `judge` only while a comparison captures (the
served submit between its snapshot and its word; A's unit on the snapshot),
at most 64. A manager that failed and one that admitted nothing now read
apart (the lens's MEDIUM 2).

A materialization word's bytes are `{"landed", "stopped", "manager_errors",
"rows"}`, `stopped` the stopping body's whole word (`{txid, status, bytes}`).
Its status is 200 only when nothing stopped; else the stopping word's, and
503 when that word is 200 (a durable report whose topic did not land: the
finalize's "not now", the graph held below the cursor). Status 0 is "not
shadowed".

## Witnesses

`crates/overlay-integration-tests/tests/gasp_shadow.rs` and
`gasp_shadow_off.rs`, over `tests/support/gasp_shadow_world.rs` (proven
transactions served by a scripted peer that can be down, partitioned or
blipping; a stateless manager that admits every output; a recording storage
that yields once per call; a counting executor). Every shadow run is beside an
`off` run of the same scenario, and the peer's calls, the sync results, the
rows of every transaction and the cursor must be equal ("never a served
change", and "never a second outbound call": the peer is asked exactly what
`off` asks). A fan-in's sibling order is a `HashMap`'s, so its shadow run is
compared with an `off` run that took the same order (the same rows), the
peer's calls as a multiset.

RED on `380bd6c` (the witnesses with the API inert), green on `fabeb44`:

| witness | RED (380bd6c) | green |
| --- | --- | --- |
| w1 one census row per finalized body, `judge_dry` beside the served judgement, the duplicate body | `left: [] right: [("28f15e0d…", 0), ("bd15fb78…", 1), ("28f15e0d…", 0), …]` | 8 body rows (7 under the per-peer budget's hook path, where the shared parent is held before the second walk and stripped), all `same`, judgements equal, the shadow's evidence its walk alone |
| w2 the materialization against the served one | `left: [] right: [("bd15fb78…", 2), ("eb5ded05…", 2), ("a45e4ca6…", 1), …]` | 5 graph rows `same`; a manager erring at the finalize on the middle link: the graph `materialization` (served landed `[g]` stopped at `c`; A landed `[g, c]` stopped at `d` waiting for its predecessor, F5/C7; the rows equal) |
| w3 01_downtime_recovery (the re-sent range) | `left: (0, 0) right: (6, 4)` | 6 body rows, 4 graph rows, no served change |
| w3 02_partition_heal (the partial listing) | `left: (0, 0) right: (6, 6)` | 6 and 6, the visible half on the split tick, the rest on the heal |
| w3 03_chain_fork | `left: (0, 0) right: (3, 3)` | 3 and 3; the orphan, discarded by the anchor check, never finalized and never shadowed |
| w3 04_cursor_no_skip_on_transient (the cursor skip) | `left: (0, 0) right: (3, 3)` | 3 and 3, the blipped UTXO shadowed on the tick that admits it |
| w5 `not shadowed: budget` | `left: 0 right: 13` | rows past 0: every snapshot of a store holding anything is `not shadowed: budget (reads …)`; a clock at 10 s a read: all 13 `not shadowed: budget (time …)`; nothing served moves |
| w4 `off` with a host set (one that panics if asked) | green (an `off` pin) | the recording equals A1's |
| `gasp_shadow_off` the `off` recording | green (an `off` pin) | `431baab4e305582981b14370ed9ee36007a0a922e25341e9ca7c772192d4d112`, polls `[57, 11, 54, 11]`, with the feature on and off |
| w6 the mode's words | green | `off`, `shadow`, `on` refused |

The lens corrections' witnesses, each RED on the commit before its fix:

| witness | RED | green |
| --- | --- | --- |
| `make gasp-shadow-size` (`scripts/check-gasp-shadow-size.sh`), MEDIUM 1 | on `8b9e6ca`: `A1 9d5a75a 8963976 B; this tree, feature off, 9401531 B (delta 437555 B)`; the off Worker links `gasp_shadow` (69 names), `admit_graph` (3), `judge_dry` (1), `walk_with_mode` (1) | `A1 9d5a75a 8963951 B; this tree, feature off, 8963657 B (delta -294 B)`; none of the four names; feature on 9,436,171 B (+472,220), 76 `gasp_shadow` names |
| w2b a manager error at the finalize, MEDIUM 2 | on `12ad069`: the erring body `served=200 shadow=200 delta=Same`, its word `{"faults":[],"refused":[],"steak":{…}}`: `left: Null right: Array []` | the row `words`: served `not_landed` "the topic manager failed on it", both sides `manager_errors` ["topic_manager.tm_shadow: invalid script: the finalize refuses this body"], `applied_topics` and `deduped_topics` `[]`; the served graph 503 carrying `c`'s whole word |
| w2c D1 and D2 apart, MEDIUM 3 | on `ec192a9`: `D2 erring=0: served {"landed":[],…} left: String("materialization") right: String("rows_differ")` | D1 `materialization` (the rows agree); D2 `rows_differ` with the manager erring on either parent (20 runs, both sibling orders): A lands `a`, `b` and `r`, the served finalize never lands `r` |
| w5c the drain after every served write of the sync, LOW 4 | on `05cb0b3`: `tick 0 budgeted=false: a served call after the census began (line 90): store.peer_outcome shadow-peer tm_shadow true` | every census line after the sync's last served call |
| w5d the snapshots a sync holds, LOW 4 | on `05cb0b3`: does not compile (`ShadowBudget` has no field `snapshots`; no `SHADOW_SNAPSHOTS_HELD`) | under an allowance of 2: at most 2 held, the rest `not shadowed: budget (snapshots …)`, nothing served moves; the peaks below |
| w1 one snapshot per body, NOTE 6 | on `f5c71be`: `one snapshot per body left: 13 right: 8` | 8 (7 on the hook path) |
| w7 no outbound call, NOTE 7 | a pin, green on `f5c71be` and after | peer calls 29 under `off` and 29 under `shadow`, the same lines in order; store calls 129 and 177: +48, 33 `find_output` and 15 `applied?`, all reads; `off`'s transcript is the shadow's with exactly those reads and the census taken out |

**The snapshots held at the peak** (the lens's LOW 4, measured by a host that
counts the live snapshots, the default allowance 64): the recorded world 7 held
of 8 taken (13 before NOTE 6 and the shared snapshot, 11 held then); chaos 01
6 (of 6), 02 3 (of 6), 03 3 (of 3), 04 2 (of 3).

The chaos records are Zanaadu's (`parity-harness/corpus/xanaverse/chaos/01` to
`04`, `kind: gasp-chaos`); each witness models its record's setup and pass
condition at the engine (a smaller set of UTXOs). The brief's four words map
to them as: the cursor skip is 04, the partial graph is 02's partition, the
re-sent body is 01's re-pulled range, and the duplicate body is the parent
shared by two listed children (01, and the recorded world of w1, where the
served finalize submits it twice and both sides answer the dedup in one word).

**The `off` recording** is every storage call in order (each yielding once,
so the executor's poll count is the engine's awaits), every peer call, each
tick's sync result and the rows left, over two ticks (a shared parent, a lone
genesis, a blip held then healed, a two-link chain), without a budget and under
a per-peer budget whose deadline never falls due (the finalize hook's path).
Its digest and polls were computed by running `gasp_shadow_off.rs` on a
`git archive` of `9d5a75a` (before any of this PR) and frozen; on this branch,
with the feature off (`cargo test -p overlay-integration-tests
--no-default-features --test gasp_shadow_off`), with it on, and with a host
set under `Off`, they are equal.

## Counts, gates and size

Re-run on `lane/atomic-gasp-shadow-2` after the lens's corrections:

- `cargo test --workspace --features bsv-overlay-engine/memory-storage
  --no-fail-fast`: **1,686 passed, 0 failed, 66 ignored** (102 binaries; the
  PR at `8b9e6ca` recorded 1,681, A1 1,671; the five are the corrections'
  w2b, w2c, w5c, w5d, w7). With the feature off, `cargo test -p
  overlay-integration-tests --no-default-features --test gasp_shadow_off`:
  the `off` recording green.
- `cargo test --manifest-path workers/Cargo.toml --workspace --no-fail-fast`:
  **1,565 passed, 0 failed, 1 ignored** (A1's 1,565).
- `cargo clippy --workspace --all-targets --features
  bsv-overlay-engine/memory-storage -- -D warnings`, the engine's clippy with
  no feature and with `gasp-shadow`, the Workers' clippy, and the overlay
  Worker's with `--features gasp-shadow`: clean. `cargo fmt --all -- --check`:
  clean.
- `cargo build -p bsv-overlay-engine -p bsv-overlay-discovery --target
  wasm32-unknown-unknown`, and the engine with `--features gasp-shadow`:
  green.
- `make ci-deploy`: all three deployable configs built through the real
  worker-build (dry runs, not deployments), and a fourth, `low-overlay` with
  `--features gasp-shadow`.
- **The wasm size.** The raw release wasm of `bsv-overlay-cloudflare` (before
  wasm-bindgen and wasm-opt; `make gasp-shadow-size`, both trees under one
  `--remap-path-prefix`) and the deployed bundle of `wrangler deploy --config
  wrangler.low.toml --dry-run` (`low-overlay`, the LIVE config; A1 a `git
  archive` of `9d5a75a` at a path of the same length, built the same way):

  | | raw release wasm | against A1 | `low-overlay` dry-run wasm | against A1 |
  | --- | --- | --- | --- | --- |
  | A1 `9d5a75a` | 8,963,951 B | | 6,675,687 B | |
  | the PR at `8b9e6ca` | 9,401,531 B | +437,555 | | |
  | feature off (this branch) | 8,963,657 B | **-294** | 6,675,401 B | **-286** |
  | feature on (this branch) | 9,436,171 B | +472,220 | 7,015,838 B | +340,151 |

  The lens measured A1 8,978,547 B and the PR 9,458,459 B raw without the
  remap, on other paths (+479,912); the PR's own record measured the deployed
  `low-overlay` at 6,660,132 B (A1) and 7,045,776 B (+385,644) on another
  checkout. The feature-off build carries none of `gasp_shadow`,
  `admit_graph`, `judge_dry` or `walk_with_mode` (A1's carries none either);
  the feature-on build carries 76 `gasp_shadow` names. Without the feature
  the engine source differs from A1's in `admission.rs`'s observer
  (`admit_graph_observed`, unreachable from a Worker), one `match &outcome`
  in the finalize loop and `judge` binding its answer before returning it.

## Limits, stated

- The snapshots and the served rows are read INSIDE the served path (before
  each served write, and after each graph), so under `shadow` a budgeted
  peer's race spends their time. The time limb counts the shadow's own time
  (its snapshot and its work), never the served SUBMIT's; but the snapshot is
  taken inside the served race (corrected: the record said "never the served
  submit's" without the second half, the lens's NOTE 8). The walks,
  judgements, units and census writes run once, after every served write of
  the sync; inside a caller's race of the whole sync (the Worker's outer
  240 s belt) they are the last thing cut. Under `shadow` the served store is
  read 48 times more on the recorded world (w7), all in the served path.
- A graph's row is the likeliest to be `not shadowed: budget`: one comparison
  walks all N bodies and runs N units, and its snapshot's scope is the whole
  graph, where each body row has its own snapshot and clock. The body rows do
  not run the graph's stop rule, so on a graph that lost its row the census
  says nothing about the graph's admission. The census counts the two kinds
  apart (`kind`). (The lens's LOW 5.)
- The memory host copies the whole store, so its rows are the store's: once the
  served store holds more than 4,096 rows (or 16 MiB) every row is `not
  shadowed: budget (reads: ...)` and its census goes dark (the host now
  refuses before copying, so it costs nothing). The Worker's host scopes its
  read (the slice's scoped SQLite) and reports what it read.
- A graph the per-peer deadline drops mid-graph (the hook path) leaves the body
  rows already queued (its first body's included, from the graph's snapshot)
  and no graph row. The comparisons queued by a sync a caller drops (the
  Worker's outer race) are dropped at the next sync's start, their snapshots
  with them, and never recorded.
- The served finalize gathers no evidence, so the `evidence` class cannot fire
  on a GASP body; the row carries the shadow's walk verdicts for the census.
- A fan-in's sibling order is a `HashMap`'s (before and after this PR), so
  which parent the served finalize submits first, and therefore its D2 rows,
  vary run to run; the class does not.
- The manager errors are captured through the engine's own `judge` (the
  served `validate_topic` and A's `admit` both call it); a manager asked
  elsewhere (the predecessor question's dry run, not asked at a finalize) is
  not captured.
- Not run here: a Worker host over D1, the HTTP corpus's runtime pin with the
  GASP rows shadowed (Zanaadu's `atomic-shadow.mjs` still prints them "not
  shadowed" until its engine re-pin and a Worker host), hosted D1, the wasm32
  time and memory of the shadow.

## Lens corrections taken

The lens (`5536f32`, on `8b9e6ca`): ACCEPT WITH CORRECTIONS. Each taken as a
RED witness, then the fix, one commit each, on `lane/atomic-gasp-shadow-2`.

| # | severity | finding | taken | commits (RED, fix) |
| --- | --- | --- | --- | --- |
| 1 | MEDIUM | the `off` Worker links A's GASP path (+479,912 B raw) | the cargo feature `gasp-shadow`, default off; `make gasp-shadow-size`; `make ci-deploy` dry-runs `low-overlay` with it on too | `e054f4b`, `12ad069` |
| 2 | MEDIUM | a manager error at the finalize is invisible in the census | `applied_topics`, `deduped_topics`, the served `not_landed` and `manager_errors` in a body word; the stopping body's whole word in the graph word, its status 200 only when nothing stopped | `6f6ef91`, `ec192a9` |
| 3 | MEDIUM | `materialization` covers a stop delta and a rows delta (D2 unwitnessed) | a fan-in witness; `rows_differ` for a graph whose rows differ, `materialization` kept for rows that agree; A's behaviour on D2 unchanged | `ac0f35d`, `05cb0b3` |
| 4 | LOW | the budget labels a comparison but never bounds it; N+1 snapshots held until a drain after each peer | a per-sync limb of snapshots held (64); the drain once, after every served write of the sync; the host's contract to honour its budget, the memory host refusing before the copy; the peaks measured | `9c3f2b6`, `f5c71be` |
| 5 | LOW | the record omits the graph row's exposure and the memory host's 4,096-row darkness | stated above (Limits) | this record |
| 6 | NOTE | position 0 takes two snapshots of one state | one snapshot for the graph and its first body | `3343887`, `4cca7cf` |
| 7 | NOTE | "nothing reaches the network" is a doc comment | the `GaspShadow` contract, and w7 a pin of it (peer calls 29 and 29; the store reads +48 stated) | `3343887` |
| 8 | NOTE | "never the served submit's" contradicts the Limits | reworded: never the served SUBMIT's, but inside the served race | this record |

## The owner's questions: the two materialization deltas

Both arise only from a topic manager that answers `Err` at the finalize (the
served finalize treats it as "did not land", D15's "Err is not now"; A's unit
commits F5's empty judgement and `admit_graph` goes on, `admission.rs`: only a
refused unit stops it). The census now names them apart; A's behaviour is
unchanged.

- **D1, `materialization`, rows equal** (w2's chain `g <- c <- d`, the manager
  erring on `c`). Served: lands `g`, stops at `c`, the graph held below the
  cursor (503 in the census). A: commits `c`'s empty judgement, goes on, and
  refuses `d` `NotNow` (503) for its unlanded predecessor. The rows agree; the
  stop point differs. It recurs on every tick while the manager errs.
  **Question:** is A's stop one body later (at the spender, not the erring
  body) the intended reading of F5/C7 at the finalize?
- **D2, `rows_differ`** (w2c: `r` spends `a.0` and `b.0`, the manager erring
  on either parent). Served: stops at the erring parent, so it lands neither
  `r` nor (when the erring parent comes first) the sibling. A: lands `a`, `b`
  and `r` (`r` applied and `r.0` inserted on A's side only); its graph counts
  as landed and, under `on`, the cursor would pass a graph whose parent was
  never admitted. **Question:** should `admit_graph` stop at a unit that
  committed F5's empty judgement after a manager `Err`, as the served finalize
  does (D15), or is going on F5's intent at the finalize too, as the
  reference's per-topic catch would carry on?
