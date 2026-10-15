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
  under `Shadow` with a host set. (Since the host seam, below: `on` parses,
  and the engine takes it only where a `GaspFinalizeHost` is set.)
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
  `admit_graph` over the whole graph (each body its own unit under the
  owner's D44 dependency gate, "D44 as built" below), the first unit's answer
  the first body's, and
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
  words each body's unit answer as it is given, with the gate's reason when
  the gate held it).

## The census row

`{kind: body|materialization, peer, topic, graph (root txid), txid, position,
served: {status, bytes, landed_topics}, shadow: {status, bytes, landed_topics},
delta, served_judgement, shadow_judgement, served_evidence, shadow_evidence,
shadow_seqs, gate, snapshot_rows, snapshot_bytes, shadow_ms}`. `gate` (D44,
below) lists each body A's gate held, in graph order, `{txid, reason}`, the
reason `"parent_error"` or `{"waiting_on": [txid, ...]}`: a body row its own
(empty when it landed or met its own refusal), a graph row every body held.

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
| `make gasp-shadow-size` (`scripts/check-gasp-shadow-size.sh`), MEDIUM 1 | on `8b9e6ca`: `A1 9d5a75a 8963976 B; this tree, feature off, 9401531 B (delta 437555 B)`; the off Worker links `gasp_shadow` (69 names), `admit_graph` (3), `judge_dry` (1), `walk_with_mode` (1) | on the tip: `A1 9d5a75a 8963951 B; this tree, feature off, 8963375 B (delta -576 B)`; none of the four names; feature on 9,405,638 B (+441,687), 100 `gasp_shadow` names (on `12ad069`, the fix alone: off -294 B, on +472,220) |
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
  | feature off (this branch) | 8,963,375 B | **-576** | 6,675,401 B | **-286** |
  | feature on (this branch) | 9,405,638 B | +441,687 | 7,015,838 B | +340,151 |

  The lens measured A1 8,978,547 B and the PR 9,458,459 B raw without the
  remap, on other paths (+479,912); the PR's own record measured the deployed
  `low-overlay` at 6,660,132 B (A1) and 7,045,776 B (+385,644) on another
  checkout. The feature-off build carries none of `gasp_shadow`,
  `admit_graph`, `judge_dry` or `walk_with_mode` (A1's carries none either);
  the feature-on build carries 100 `gasp_shadow` names. Without the feature
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
unchanged. (Ruled since: D44, `dependency gate`, the next section. On D1 A
now holds the erring body itself, `not now`, and its spender waits on it; on
D2 A lands the sibling only.)

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

## D44 as built (the dependency gate, `lane/atomic-gasp-dependency-gate`)

The owner's D44 (zanaadu-v2 `progress/decisions/2026-10-14-owner-rulings.md`;
the memo's item 4, revision 3) ruled the fan-in delta D2: `dependency gate`.
It lives in A's `admit_graph` (`admission.rs`, `admit_graph_observed`), which
the shadow's judgement and the `on` path share. The served finalize (legacy)
is unchanged: it still stops at the erring body, so the census keeps naming
the class between them until `on`.

**The rule, in five lines** (each body of a GASP graph, in graph order):

1. A parent it spends that is a body of this graph and did not land in this
   graph holds it without a unit: `WaitingOn([parent])`, refused with the
   parent's typed word when that is terminal (`Lookup`, `Deterministic`:
   D34's unit-wide refusal reaches its descendants), else `not now`.
2. A parent of the graph not yet met (a graph out of order) must hold an
   applied row in every topic of the body, read inside the body's unit; else
   the unit is refused `not now`, `WaitingOn`.
3. A topic manager that answers `Err` at the finalize (a topic of the unit
   neither applied nor deduped: F5's empty judgement) refuses the whole unit
   `not now` (`gasp_finalize.topic_manager`), `ParentError`: it rolls back,
   nothing of it is written, never terminal.
4. Any other refusal is the body's own (no gate reason); its descendants wait
   on it by line 1.
5. An independent sibling is judged and lands as its own unit; a body that
   waited lands on the peer's next sync with the same judgement as any
   first-time body (nothing of it was written).

The API: `GraphOutcome::StoppedAt` is now `Held { landed, held }`, each
`HeldBody { txid, refusal, gate: Option<GateReason> }`; `GateReason` is
`ParentError` or `WaitingOn(Vec<txid>)`; the observer is handed the reason
(`GraphObserver`). A1's C4 pin (`a1_c4_graph_stops_at_first_refusal`) is
restated as `a1_c4_graph_holds_a_refused_body_and_judges_the_rest` (the refused
body held, the body after it judged: a dedup, `landed [1, 1]`). The shadow's
words follow: a unit refused while its managers erred keeps their errors in
its word (`{"refusal", "manager_errors"}`, the lens's MEDIUM 2 kept), and a
graph word's `stopped` is the FIRST body held (the gate judges the rest).

**The census row** carries the gate's reason per body: `gate: [{txid, reason}]`,
`reason` `"parent_error"` or `{"waiting_on": [txid, ...]}`, in graph order; a
body row its own (empty when it landed or met its own refusal), a graph row
every body held. On the fan-in with the erring parent first:

```
"delta": "rows_differ",
"gate": [{"txid": a, "reason": "parent_error"},
         {"txid": r, "reason": {"waiting_on": [a]}}]
```

The class against today's path: on the fan-in, `rows_differ` when the erring
parent comes first (today lands nothing after the error; A lands the
sibling), `materialization` when the sibling comes first (both land the
sibling alone: the rows agree); on the chain (D1) `materialization` (both land
`g` alone; A's first held body is the erring one, 503). The served order is a
`HashMap`'s, so w2c takes either and asserts the class that order gives; w8
takes the erring-first order.

**Witnesses**, `crates/overlay-integration-tests/tests/gasp_dependency_gate.rs`
(A's `admit_graph` run directly over a `MemoryStorage` runner and the shadow
world's proven transactions; no feature needed), and `gasp_shadow.rs` w8.
RED on `24df313` (the witnesses on `75e2d7b`'s engine) and `590c503`:

| witness | RED | green |
| --- | --- | --- |
| the fan-in (`r` spends `a.0`, `b.0`; the manager erring on `a` at the finalize), both batch orders | `the erring parent waits, not now (never terminal)`: `left: Some(Landed) right: Some(NotNow)` (all three landed, `a` on F5's empty judgement) | `b` lands; `a` `not now`, `parent_error`; `r` `not now`, `waiting_on [a]`; nothing of `a` or `r` written; the rows equal in both orders |
| the chain (`g <- c <- d`, the error at `c`) | `left: Some(Landed) right: Some(NotNow)` (`c` landed its empty judgement; `d` refused `predecessor_not_landed`) | `g` lands; `c` `not now`, `parent_error`; `d` `not now`, `waiting_on [c]`; `g.0` unspent |
| the re-sync (the erring tick, then the healed manager handed the waiting part `[a, r]`) | `left: Some(Landed) right: Some(NotNow)` (the erring tick landed everything) | the erring tick lands `b`; the re-sync lands `a` and `r`; the digest of the rows and the STEAKs `3e9537070c81b8bd4972f2afa1f3dc981bb8fd886ddb20a4dfc45da2ce5fd12b`, equal to the first-time graph's |
| a lookup refusal (`check_admitted` refusing `c`'s outputs) | `the descendant is refused with the same typed word, not not now`: `left: None right: Some(Lookup)` (the graph stopped at `c`; `d` never judged) | `g` lands; `c` `Lookup` (its own, no gate reason), nothing of it written; `d` refused with the same `Lookup` word, `waiting_on [c]` |
| w8 the census row's reason per body | on `cd53832`: `left: Null right: Array [{"reason": "parent_error", ..}, {"reason": {"waiting_on": [..]}, ..}]` | the graph row `rows_differ` with the two entries above; `a`'s body row `[{"txid": a, "reason": "parent_error"}]` |

Restated with the gate (green on `cd53832`): w2 (the erring chain: A lands
`[g]`, its first held body `c` at 503, the rows agree), w2b (`c`'s shadow word
503, the gate's site, the manager and its error named), w2c (the class by the
served order, above).

**Not built, stated.** `on` is still refused, so nothing serves the gate: the
cursor's rule under `on` (a graph with a held body held below the cursor so
the peer's next sync re-sends the waiting part) is the `on` runner's, a later
slice; the re-sync witness hands the waiting part in by hand. The gate's
parents are the inputs of each body that name another body of the graph; a
parent outside the graph is the walk's and A's predecessor question's, as
before. A body held by line 1 opens no unit, so it reads nothing and costs no
seq.

**Gates, on the tip of `lane/atomic-gasp-dependency-gate`.**

- `cargo test --workspace --features bsv-overlay-engine/memory-storage
  --no-fail-fast`: **1,691 passed, 0 failed, 66 ignored** (103 binaries;
  1,686 before, plus the gate's four witnesses and w8). With the feature off
  (`-p overlay-integration-tests --no-default-features`): `gasp_shadow_off` and
  the four gate witnesses green.
- `cargo test --manifest-path workers/Cargo.toml --workspace --no-fail-fast`:
  **1,565 passed, 0 failed, 1 ignored** (unchanged).
- clippy with warnings denied (the workspace with `memory-storage`, the engine
  with no feature and with `gasp-shadow,memory-storage`) and `cargo fmt`: clean.
- **The `off` recording** `431baab4e305582981b14370ed9ee36007a0a922e25341e9ca7c772192d4d112`,
  polls `[57, 11, 54, 11]`: unchanged, with the feature on and off. **The peer
  calls** (w7): 29 under `off`, 29 under `shadow`; the served store 129 and 177.
- **The wasm, raw release** (`bsv-overlay-cloudflare`, both trees built AT
  THIS WORKTREE'S PATH under the same `--remap-path-prefix`, `75e2d7b` checked
  out in place): `75e2d7b` feature off 8,963,479 B; this tip feature off
  8,963,133 B (**-346 B**), none of `gasp_shadow`, `admit_graph`, `judge_dry`,
  `walk_with_mode`; feature on 9,407,485 B, 100 `gasp_shadow` names.
  `make gasp-shadow-size` itself FAILS its 4,096 B tolerance here, on the
  base's side: its `git archive` tree builds about 5 KB larger at its scratch
  path than the same code at the worktree's (A1 `9d5a75a` 8,968,284 B against
  the record's 8,963,951 B; `75e2d7b` 8,968,002 B there against 8,963,479 B
  in place), while this tree's off build is within 4 B of the lens's tip
  figure. The script's remap does not equalize the two paths; not chased.
- `make ci-deploy`: green, four dry runs through the real worker-build:
  `bsv-overlay-cloudflare` and `low-overlay` 6,680,563 B, `low-app-layer`
  4,795,299 B, `low-overlay` with `gasp-shadow` 7,016,450 B (the record's
  6,675,401 B and 7,015,838 B were built at another path).

## The lens's corrections taken (the gate lens, `lane/atomic-gasp-dependency-gate-2`)

The gate lens (`docs/atomic-admission-gasp-dependency-gate-lens.md`,
`lane/atomic-gasp-dependency-gate-lens` at `96ecc78`, on `8ed47d8`): ACCEPT
WITH CORRECTIONS, three LOW and six NOTE. Each taken as a RED witness, then
the fix, one commit each, on `lane/atomic-gasp-dependency-gate-2` (base
`8ed47d8`).

| # | severity | finding | taken | commits (RED, fix) |
| --- | --- | --- | --- | --- |
| 1 | LOW | the gate depends on batch order: a child handed before its parent waits `not now` and is not re-visited | the bodies are judged ancestors first (`admission::ancestors_first`: Kahn's order over the in-graph parents, the graph's order the tie-break); the out-of-order store read (line 2 of "D44 as built") can no longer fire and is gone | `943231a`, `0ee7bac` |
| 2 | LOW | the shadow stops a graph by position at a walk refusal; siblings after it are never judged | `admit_graph_observed` takes the bodies its caller could not walk (`admission::Unwalked`, the walk's refusal), held before any unit, parents like any other; the shadow walks on past a refusal (`walk_refusal`: a 400 terminal, a 503 `not now`, else transient) and the graph's stop is the first held body in graph order | `8d539ca`, `8e8c4c7` |
| 3 | LOW | draft #55's description is stale and does not say why the engine | rewritten (below, and the PR) | this record |
| N1 | NOTE | a terminal refusal cloned onto a descendant names no parent | `AdmitRefusal::Lookup` and `::Deterministic` carry `refused_parent` (`Option<Box<str>>`: the enum keeps its size), set by the gate to the body whose own refusal it is, through every generation; the word reads `parent <txid> was refused: <the parent's word>` | `2c5f700`, `506553e` |
| N2 | NOTE | the re-sync digest compares stored BEEFs by length | the gate witnesses' rows name each stored BEEF by its bytes (SHA-256); a waiting parent re-sent under another proof is a pin | `2c5f700` |
| N3 | NOTE | the gate parses each body's BEEF once more | `Walked::spends` (serde-skipped): the subject's spent txids from `walk_with_mode`'s own parse; the gate parses only a body built without it | `506553e` |
| N4, N5 | NOTE | `stopped.bytes` is the bare refusal; a funding parent whose manager admits nothing passes | no change asked; N5 stays read from the code (the world's manager admits every output) | none |
| N6 | NOTE | the scratch witnesses were not committed | committed as pins (below) | `2c5f700` |

**The rule now** (replacing lines 1 and 2 of "D44 as built"): the bodies of
a graph are judged ancestors first, so every in-graph parent a body spends was
judged before it; a parent that did not land in this graph (refused, held, or
handed in unwalked) holds the body without a unit, with the parent's word
(naming that parent) when it is terminal, else `not now`, `waiting_on`. A
parent outside the graph is not the gate's (the walk's and the unit's own
predecessor question's); a parent of the graph the store already holds is
judged as its own unit and dedups. Lines 3 to 5 are unchanged. A body a caller
DROPS (neither walks nor names as unwalked) is a parent the gate cannot see:
its descendants are judged as if it were outside the graph. That is the
stated limit; the shadow drops none.

**The sort moves nothing recorded.** A GASP batch is `compute_ordered_beefs`,
ancestors first, and Kahn's order with the graph's order as the tie-break is
the identity on an ancestors-first batch (the smallest unjudged index always
has every parent judged). Proven by a pin frozen on `8ed47d8` BEFORE the sort:
w9 digests every census row (the wall clock `shadow_ms` left out) of every
world of `gasp_shadow.rs` whose finalize order is our node's (the recorded
world, budgeted and not; the erring chain; the four chaos records) and the
fan-in in w8's order: **64 rows,
`38044a236d2293d8f93942871c3dcaafd4e53cf4c6d13258802c19fbe6c29f2d`**, green on
`943231a` (the base's engine) and unchanged on every commit after it. The `off`
recording and w7's store calls are unchanged too (gates below).

**Witnesses**, each RED on its commit, green on the next:

| witness | RED | green |
| --- | --- | --- |
| `d44_l1_a_child_handed_before_its_parent_gets_the_in_order_answer` (`gasp_dependency_gate.rs`: the chain `g <- c <- d` reversed; the fan-in root first, healthy and with `a`'s manager erring) | on `943231a`: `a reversed chain: the same words and rows`, left: `g` Landed, `c` and `d` NotNow (`waiting_on`), applied `c` and `d` false; right: all three Landed | the reversed chain and the root-first fan-in give the in-order words, gates, STEAKs and rows |
| `w10_d44_a_refused_walk_holds_its_descendants_and_its_siblings_land` (`gasp_shadow.rs`: `a` carries a 1.9 MB output so its BEEF rides R2, the host's R2 read faults, run until `a` comes first) | on `8d539ca`: `left: (Array [], String(a), Array []) right: (Array [b], String(a), Array [{"txid": r, "reason": {"waiting_on": [a]}}])` | `b` lands, the stop is `a`, `r` `waiting_on [a]`, the rows `rows_differ` against the served path (which lands all three) |
| `d44_n1_a_terminal_refusals_descendants_name_the_refused_parent` (`g <- c <- d <- e`, the index refusing `c`) | on `2c5f700`: `the descendant's word names the refused parent: lookup preflight refused: [AdmissionRefusal { .. }]` | `d` and `e` `Lookup`, each `parent <c> was refused: <c's word>`; `c`'s own word unprefixed |
| `d44_n6_a_fan_in_whose_parents_meet_a_manager_error_and_a_lookup_refusal` | on `2c5f700`, on its refused-parent line alone | `a` NotNow `parent_error`; `b` Lookup, its own; `r` Lookup (terminal wins), `waiting_on [a, b]`, naming `b`; nothing written |
| `d44_n3_the_gate_reads_the_walks_spends` (pin) | does not compile before `506553e` (no `Walked::spends`) | the walk carries `r`'s `[a, b]`; a sibling whose carried spends name the erring parent is held `waiting_on [a]` |
| `d44_n6_a_graph_whose_every_body_waits_lands_nothing_and_says_why`, `w11_d44_a_graph_whose_every_body_waits_says_so_in_the_census` (pins) | green on `2c5f700` | nothing lands; `g` `parent_error`, `c` `waiting_on [g]`; the graph row `materialization`, both `landed: []`, A's stop `g` at 503 with both gate entries; `g`'s body row `words` (200 against 503 naming the manager's error) |
| `d44_n2_a_waiting_parent_re_sent_under_another_proof_lands_as_a_first_time_body` (pin) | green on `2c5f700` | `a` re-sent at height 105 (100 on the erring tick): `a` and `r` land with a first-time graph's STEAKs and rows, the stored BEEFs byte for byte equal to a first-time graph handed those bytes |

Amended in `506553e`, stated: the fan-in witness now gives each body a fixed
height whatever its batch order (its rows compare stored BEEFs by bytes, and
the old witness handed `b` at 100 or 101 by its position); the lookup witness's
"the same word" now names the refused parent. The re-sync digest over the rows
with the BEEFs' bytes: `c1663764e38c18577a10f2bee2a1b80bd5774d4097d9714ef9a02d7d49ddaee0`,
equal on both sides.

**Why the gate lives in the engine** (the keeper's question, the lens's item
5). The rule is the graph's: a body lands when the bodies of its graph it
spends landed. Only `admit_graph` sees the graph; Zanaadu's executor
(`UnitRunner::run_unit`) is handed one unit and its evidence and knows no
graph, so the gate there would need a new seam carrying the graph's
structure. The shadow's judgement and `on` both run `admit_graph`, so one rule
serves both and the census measures what `on` will do. And it costs the
executor nothing: the gate reaches it only as ordinary refusals (`NotNow` at
`gasp_finalize.topic_manager` or `gasp_finalize.dependency_gate`, or a cloned
terminal refusal), and since the sort it reads nothing of the store itself.

**Gates, on the tip of `lane/atomic-gasp-dependency-gate-2`.**

- `cargo test --workspace --features bsv-overlay-engine/memory-storage
  --no-fail-fast`: **1,700 passed, 0 failed, 66 ignored** (103 binaries;
  1,691 before, plus nine: `d44_l1`, `d44_n1`, `d44_n2`, `d44_n3`, two
  `d44_n6`, w9, w10, w11). With the feature off (`-p overlay-integration-tests
  --no-default-features --test gasp_shadow_off --test gasp_dependency_gate`):
  the ten gate witnesses and `gasp_shadow_off` green.
- `cargo test --manifest-path workers/Cargo.toml --workspace --no-fail-fast`:
  **1,565 passed, 0 failed, 1 ignored** (93 binaries; unchanged).
- clippy with warnings denied (the root workspace with `memory-storage`, the
  engine with no feature and with `gasp-shadow,memory-storage`, the Workers'
  workspace) and `cargo fmt --all -- --check`: clean.
- **The `off` recording** `431baab4e305582981b14370ed9ee36007a0a922e25341e9ca7c772192d4d112`,
  polls `[57, 11, 54, 11]`: unchanged, with the feature on and off. **The peer
  calls** (w7): 29 under `off`, 29 under `shadow`; the served store 129 and
  177 (unchanged). **The ordered census** (w9): 64 rows, `38044a23…`,
  unchanged.
- **The wasm, raw release** (`bsv-overlay-cloudflare`, both trees built at
  this worktree's path under the same `--remap-path-prefix`, `8ed47d8` checked
  out in place): feature OFF `8ed47d8` 8,963,085 B, this tip **8,964,054 B
  (+969 B; +367, +344 and +256 at the three fixes)**, none of `gasp_shadow`,
  `admit_graph`, `judge_dry`, `walk_with_mode`; within the 4,096 B tolerance
  of `make gasp-shadow-size`. Feature ON: `8ed47d8` 9,407,662 B, this tip
  9,527,096 B (**+119,436 B**: the sort +25,733, LOW 2 +92,290, the NOTEs
  +1,411); 103 `gasp_shadow` names.
- `make ci-deploy`: green, four dry runs through the real worker-build:
  `bsv-overlay-cloudflare` and `low-overlay` **6,680,557 B** (the record's
  6,680,563 B at the gate's tip), `low-app-layer` 4,795,674 B, `low-overlay`
  with `gasp-shadow` **7,122,562 B (+106,112 B** against the record's
  7,016,450 B).

**The feature-on growth, stated (not chased to its end).** It is not the
gate's code: `twiggy diff` of the two raw builds puts 28,175 B in 53 new
functions (the largest the shadow's drain closure, 17,340 B, which loses
15,444 B from `start_gasp_sync`'s closure: a move) and the rest in functions
this lane does not touch growing, almost all the Worker's own JSON builders
(`routes::build_health_response` +5,931 B, `ops::read_counters` +5,162 B,
`beef_blob_sweep::queue_json` +4,941 B, `dead_letters::internal_discard`
+2,771 B, `dead_letters::health_json` +2,474 B, ...), while the base's one
outlined `BTreeMap::insert` is gone: LLVM's inlining over the whole
`gasp-shadow` Worker moved. Measured, then not kept: replacing the sort's
`BTreeSet` and the shadow's `BTreeMap` of positions with plain vectors gave
9,506,911 B (-20 KB); over that build, five builds each undoing one part of
LOW 2 (the shadow's walk loop breaking again, `walk_refusal` a constant, `&[]`
for the unwalked bodies, the stop rule as before, `shadow_walk`'s error a bare
word) stayed within 3.4 KB of it; only the shadow file as it stood at
`0ee7bac` came back to 9,420,123 B (+12.5 KB). So the growth follows the shape of the drain's code, not any one
construct, and was left as built. It weighs only a `gasp-shadow` build: every
Worker builds `off` (the dry runs above), whose size moved +969 B raw and
-6 B deployed.

## The host seam as built (`lane/atomic-gasp-seam-host`, the a17 lane)

Zanaadu's a17 lane (zanaadu-v2 `lane/atomic-admission-a17-gasp-seam` at
`bffd6ccd`, its record `docs/audit/2026-10-13-eve/atomic-admission-a17-gasp-seam.md`)
pinned the engine to `6389501` and found the tick could not run the GASP
finalize through the log's `admit_graph` under `on`: the engine handed the host
no graph (the sink is drained inside `submit_finalized_graphs` through the
served `submit_bounded`), `AdmissionMode::parse("on")` was refused, and the
gate was one synchronous call, so a host whose units are awaited (an RPC to
its log) could not run under it, and every R2-carried body had to be resolved
before the call (the graph's bytes at once). This lane builds that seam in the
engine, on draft #55's branch (base `6389501`), under the same feature
(`gasp-shadow`, default off).

**The host API** (the exact signatures; `admission` is ungated, the rest is
`gasp-shadow`):

```rust
// bsv_overlay_engine::admission (no feature)
#[async_trait(?Send)]
pub trait GraphHost {
    async fn resolve(&self, body: &Walked) -> Result<Vec<u8>, AdmitRefusal>;
    async fn run(&self, body: &Walked, resolved: Option<Vec<u8>>)
        -> Result<Committed, AdmitRefusal>;
    fn answered(&self, _body: &Walked, _answer: &Result<Committed, AdmitRefusal>,
        _gate: Option<&GateReason>) {}
}
pub async fn gasp_unit(engine: &Engine, walked: &Walked)
    -> Result<UnitOutput, AdmitRefusal>;
pub async fn admit_graph_hosted(host: &dyn GraphHost, graph: &[Walked],
    unwalked: &[Unwalked]) -> GraphOutcome;

// bsv_overlay_engine::gasp_shadow (feature `gasp-shadow`)
pub enum AdmissionMode { Off, Shadow, On }          // parse("on") == Ok(On)
pub enum AdmissionModeRefused { NoFinalizeHost }
pub struct FinalizeGraph<'a> { pub peer: &'a str, pub topic: &'a str,
    pub root: &'a str, pub beefs: &'a [Vec<u8>] }
pub struct FinalizeBody<'a> { pub graph: &'a FinalizeGraph<'a>,
    pub position: usize, pub walked: &'a Walked }  // .beef() -> &[u8]
#[async_trait(?Send)]
pub trait GaspFinalizeHost {
    async fn resolve(&self, body: &FinalizeBody<'_>) -> Result<Vec<u8>, AdmitRefusal>
        { Ok(body.beef().to_vec()) }
    async fn admit(&self, body: &FinalizeBody<'_>, resolved: Option<Vec<u8>>)
        -> Result<Committed, AdmitRefusal>;
    fn answered(&self, _body: &FinalizeBody<'_>,
        _answer: &Result<Committed, AdmitRefusal>, _gate: Option<&GateReason>) {}
    async fn graph_done(&self, _graph: &FinalizeGraph<'_>, outcome: &GraphOutcome)
        -> bool { matches!(outcome, GraphOutcome::AllLanded { .. }) }
}

// bsv_overlay_engine::engine::Engine (feature `gasp-shadow`)
pub fn set_gasp_finalize_host(&mut self, host: Rc<dyn GaspFinalizeHost>);
pub fn set_admission_mode(&mut self, mode: AdmissionMode)
    -> Result<(), AdmissionModeRefused>;
pub fn set_gasp_shadow(&mut self, mode: AdmissionMode, host: Rc<dyn GaspShadow>)
    -> Result<(), AdmissionModeRefused>;            // was `()`
// memory-storage: MemoryStorage::release_admission_beef(&self, key: &str)
```

**What runs under `on`.** `Engine::submit_finalized_graphs`, with the mode
`On` and a finalize host set (`engine/gasp_on.rs`, `finalize_on`), drains the
sink and, for each graph: walks every body on the engine
(`walk_with_mode`, `historical-tx-no-spv`, `NotNow`, `NoEvidence`: no network,
the anchor check already ran; a refused walk is an `Unwalked` parent, the
shadow's `walk_refusal`), then runs `admit_graph_hosted` over the walked
bodies. The gate (the one rule of D44 as the gate lens left it, now a stepper
`admission::Gate` that `admit_graph_observed` and `admit_graph_hosted` share)
takes the bodies ancestors first; a body whose in-graph parent did not land is
held without a unit (`WaitingOn`, the parent's word when terminal); any other
body is handed to `GaspFinalizeHost::admit`, ONE AT A TIME, its R2 bytes
resolved through `resolve` just before it and moved into it, each in its own
write section of the finalize gate (a due per-peer deadline stops the graph
between two bodies, never inside one's unit, the served rule). The host runs
the body as one unit of its own log inside its FIFO turn, the unit's body
`gasp_unit` (rule 3: a topic manager's `Err` refuses the unit `not now` at
`gasp_finalize.topic_manager`, which the gate reads as `ParentError`). Every
body's word, held or run, reaches `answered` in the gate's order with the
gate's reason. The engine writes nothing of a graph: the finalize's one
materialization is the host's. **The cursor follows the host's word:**
`graph_done` (by default: every body landed) says whether the cursor may pass
the graph; `false` is a graph that did not land (without a per-peer budget
the peer's whole cursor stays for the tick; under one, the hook fails that
UTXO and the gap guard holds the cursor below it), so the peer's next sync
re-sends the waiting part. `off` and `shadow` are untouched: one `if let` on
`None` with no await, before the served loop.

**`on` is taken only where the seam is set.** `AdmissionMode::parse("on")` is
`Ok(On)`; `Engine::set_admission_mode(On)` (and `set_gasp_shadow(On, _)`, which
now goes through it) with no `GaspFinalizeHost` answers
`AdmissionModeRefused::NoFinalizeHost`, "ADMISSION_MODE=on is refused: no GASP
finalize host is set (Engine::set_gasp_finalize_host)", and leaves the mode as
it was.

**The per-body resolver.** `admit_graph_hosted` asks `GraphHost::resolve` for
an R2-carried body only when the gate runs it, just before its `run`, and
moves the bytes into that run (the host releases them when it returns): the
host holds one body's bytes at a time, and a body the gate holds is never read.
A refused resolve is the body's own refusal (no unit; its descendants wait).
The same function serves the log's own graph route (Zanaadu's
`OnLog::admit_graph_in_turn`: its R2 resolver as `resolve`, `admit_in_turn` as
`run`, inside its one FIFO turn), so the one-body bound (the a4 lens's
correction 4, a9's `peak`) and its carried-predecessor loop stay the host's.

**Witnesses**, each RED on its commit, green on the next
(`crates/overlay-integration-tests/tests/gasp_on_seam.rs`, a sync engine over
the shadow world's scripted peer and a `SinkHost` running each body through
`gasp_unit` in the memory log over the served store; `gasp_dependency_gate.rs`
for item 3, no feature):

| witness | RED | green |
| --- | --- | --- |
| s1 the fan-in under `on` (`r` spends `a.0`, `b.0`; the log's manager erring at the finalize on `a`), unbudgeted and under a per-peer budget (the hook's path) | on `def6f52` (the API inert): `the engine's own finalize did not submit`, `left: [the three txids] right: []` | the sink hears `a` `not now at gasp_finalize.topic_manager`, `parent_error`; `b` `landed`; `r` (last) `not now at gasp_finalize.dependency_gate`, `{"waiting_on": [a]}`; the graph `landed 1, held [[a, "parent_error"], [r, {"waiting_on": [a]}]]`; the root never run; the engine submits nothing; the re-sync (the manager healed) hears `a` `landed`, `r` `landed`, the graph `landed 2`, and the rows and cursor equal the `off` run's healthy fan-in |
| s1b a healthy fan-in: `on` against `off`; `off` with a host set | on `def6f52`: `left: [the three txids] right: []` | `on`: the host runs 3 bodies, the engine none; the peer's calls (as a multiset), the sync results, the rows and the cursor equal `off`'s; `off` with a host set never asks it and equals `off` |
| s2 `on` where the seam is not set | on `44e714b` (set_admission_mode inert): `on without the seam is refused`, `left: Ok(()) right: Err(NoFinalizeHost)` | refused with the typed word through both setters, the mode left `Off`; `shadow`/`off` need no seam; `parse("on") == Ok(On)`; with the seam set, `On` taken |
| `d44_s3` the chain `g <- c <- d`, three 4 MiB BEEFs riding R2, through `admit_graph_hosted` over a host resolving from its object store | on `f1b7e89` (every carried body resolved before the first unit, as item 1 built it): `at most one body's bytes held at once: peak 12583291 B, one body 4194431 B, the graph 12583291 B`, steps `resolve, resolve, resolve, run, run, run` | peak **4,194,431 B** (one body; the graph 12,583,291 B), steps `resolve g, run g, resolve c, run c, resolve d, run d`; the erring genesis: only `g` read, `c` and `d` held `waiting_on` and never resolved |

w6 restated (`parse("on") == Ok(On)`; an unknown word still refused).

**Gates, on the tip of `lane/atomic-gasp-seam-host`.**

- `cargo test --workspace --features bsv-overlay-engine/memory-storage
  --no-fail-fast`: **1,704 passed, 0 failed, 66 ignored** (104 binaries;
  1,700 before, plus s1, s1b, s2, `d44_s3`). With the feature off (`-p
  overlay-integration-tests --no-default-features --test gasp_shadow_off
  --test gasp_dependency_gate`): the eleven gate witnesses (`d44_s3` among
  them) and `gasp_shadow_off` green.
- `cargo test --manifest-path workers/Cargo.toml --workspace --no-fail-fast`:
  **1,565 passed, 0 failed, 1 ignored** (93 binaries; unchanged).
- **The `off` recording** `431baab4e305582981b14370ed9ee36007a0a922e25341e9ca7c772192d4d112`,
  polls `[57, 11, 54, 11]`: unchanged, with the feature on and off. **The peer
  calls** (w7): 29 under `off`, 29 under `shadow`; the served store 129 and 177
  (unchanged). **The ordered census** (w9): 64 rows, `38044a23…`, unchanged
  (the gate's refactor into a stepper moves no word).
- clippy with warnings denied (the root workspace all targets with
  `memory-storage`; the engine all targets with no feature and with
  `gasp-shadow,memory-storage`; the Workers' workspace all targets; the
  overlay Worker with `--features gasp-shadow`) and `cargo fmt --all -- --check`:
  clean. `cargo build --target wasm32-unknown-unknown` of the engine and
  discovery, and of the engine with `gasp-shadow`: green.
- **The wasm, raw release** (`bsv-overlay-cloudflare`, both trees built at
  this worktree's path under the same `--remap-path-prefix`, `6389501` checked
  out in place): feature OFF `6389501` 8,964,028 B, this tip **8,964,066 B
  (+38 B)**, none of `gasp_shadow`, `gasp_on`, `admit_graph`, `judge_dry`,
  `walk_with_mode`, `finalize_on`. Feature ON: `6389501` 9,525,805 B, this tip
  9,579,116 B (**+53,311 B**: the hosted gate, `finalize_on` and the host
  adapter; 101 `gasp_shadow` names, 16 `gasp_on`).
- Not run in this lane: `make ci-deploy` (the dry runs through the real
  worker-build), `make gasp-shadow-size` (its `git archive` path bias, stated
  above, is unchanged).

**Limits, stated.**

- Rule 3 lives in the unit: a host whose `run`/`admit` does not run
  `gasp_unit` commits F5's empty judgement after a manager `Err` and the gate
  sees a landed body (the gate reads `ParentError` from the refusal's site,
  `gasp_finalize.topic_manager`; the synchronous gate reads it the same way
  now, equivalent to the old flag since nothing else answers that site).
- The engine bounds no host call: the served finalize bounds each storage
  call inside a submit (`set_finalize_submit_budget`); `GaspFinalizeHost::admit`
  is one await the per-peer race waits for (a write section, never dropped
  inside). A host whose call can hang must bound it itself, and a lost answer
  (a unit that committed behind a dropped RPC) is the host's replay to settle
  (`LogError::AnswerLost`'s rule).
- A graph's bodies are handed one call each; whether they share one FIFO turn
  is the host's (a host that wants the graph in one turn runs
  `admit_graph_hosted` itself inside it, as the log's own graph route can).
  A carried predecessor (`NotNow { predecessor }`) is the host's to land inside
  its `run`, as `OnLog::admit_in_turn` does; the gate passes the word on.
- The walk is the engine's, on the served store, before the host's units (as
  the shadow walks on its snapshot); under `on` the engine still writes the
  cursor and the peer-health rows (the sync's own bookkeeping), never a graph's
  rows.
- On the engine's finalize the graph is already whole in memory (the sink, as
  before); `GaspFinalizeHost::resolve`'s default copies that one body. The
  one-body bound is the log's (bodies by pointer), measured by `d44_s3`.
- Under `on` the shadow does not run (it is `Shadow`'s alone): no census row
  compares the host's finalize with anything.
- Not run: Zanaadu's a17 re-run against this tip (the tick's GASP intake
  through the seam, the fan-in under workerd, the chaos rows with the gate's
  words), a Worker host over D1 and R2, wasm32 time and memory of the hosted
  gate.

## The seam lens's corrections taken (`lane/atomic-gasp-seam-host-2`)

The lens of record on the host seam (`docs/atomic-admission-gasp-seam-host-lens.md`,
`lane/atomic-gasp-seam-host-lens` at `602db56`, on `2ea647a`): ACCEPT WITH
CORRECTIONS. The engine-side findings (M1, L1, L3, L4) are taken here, each a
RED witness, then the fix, one commit each, on `lane/atomic-gasp-seam-host-2`
(base `2ea647a`). H1, M2, M3, L2 and L5 are Zanaadu's (a20) and are not
touched.

| # | finding | taken | commits (RED, fix) |
| --- | --- | --- | --- |
| M1 | under `on` the engine holds the graph's bytes twice (the sink and the walked list); `d44_s3`'s peak is a counter | the gate is handed the walked bodies BARE; the body it is on is made whole from the sink for the host's calls and released after its answer; `d44_s3` renamed for what it counts | `5fae541`, `971a8ea` |
| L1 | s1 asserts nothing of the cursor between ticks, and its peer's `score >= since` makes the re-send prove nothing | s1 lists an independent `x` above the root and reads the cursor after each tick | `bdb50ce` (a pin; RED against a mutant) |
| L3 | the default `graph_done` holds the cursor below a graph with a terminal refusal for good | the default is `cursor_may_pass`: every body landed or held by a terminal word | `28a7323`, `abe8c52` |
| L4 | `position` collapses a txid that appears twice in one graph | the position is the occurrence's (a vector parallel to the walked list, a body found by identity) | `0a81157`, `1dc31a6` |

**M1, the one copy.** `finalize_on` still walks every body before the gate
(the gate needs every body's txid and spends up front, and the walk's reads
and words are unchanged), but keeps each `Walked` bare (`bare`: an inline
body's BEEF and an R2 body's raw transactions dropped; key, hash, length,
spends, evidence and every other field kept). `OnGraph` makes the body the
gate is on whole again from the sink (`whole`: the carry `BeefCarry::from_beef`
makes of those bytes, the walk's own rule) for `resolve`, `admit` and
`answered`, keeps it between those calls and releases it after `answered`. The
host is handed the same `Walked` as before (the a17-2 host's hop encodes it).
The engine holds the sink and one body; an R2 body's `admit` also holds its
resolved bytes, as before.

The witness, `crates/overlay-integration-tests/tests/gasp_on_memory.rs` (its
own test binary, a counting global allocator; the lens's scratch witness,
kept): a proven genesis and 12 proven 256 KiB links (13 bodies, the graph's
BEEFs about 3.15 MB), the peer listing the tip, the live heap sampled at every
finalize judgement (the manager's non-dry call, the same manager in both
modes):

```
RED (5fae541, the engine of 2ea647a):
M1 off: [28438135, 29227808, 29752469, ... 35004805]
M1 on:  [31623006, 33463736, 34513034, ... 45019526]
M1 at the first judgement: on 31623006 B, off 28438135 B, on over off 3184871 B (one body 262225 B)
green (971a8ea; 1dc31a6 the same within 1,240 B):
M1 on:  [28477807, 30580700, 31629998, ... 42136490]
M1 at the first judgement: on 28477807 B, off 28438135 B, on over off 39672 B (one body 262225 B)
```

The pin holds `on` within one body of `off` at the first judgement (the lens's
+3,183,983 B, here +3,184,871 B, is now +39,672 B; at the L4 tip +38,432 B).
The per-body slope under `on` (about 1.05 MB a body against `off`'s 0.52 MB) is
unchanged by the fix: it is the test host's memory log keeping its units'
writes (`after_commit` intents and statements), the host's and not the
engine's, as the lens read it; the pin asserts the first judgement for that
reason.

`d44_s3` is renamed `d44_s3_the_gate_resolves_each_carried_body_just_before_its_own_unit`
and its words say what it counts: the RESOLVED bytes the host holds, not the
heap. A caller of `admit_graph_hosted` (Zanaadu's log graph route) hands the
gate its `Walked` list, whose R2 carries hold every body's raw transactions
for the whole gate: that list is the caller's to hold, and the engine's own
caller now hands it bare.

**L1, the cursor between ticks.** s1 now runs the fan-in with an independent
`x` listed at score 2 above the root `r` at 1, and reads the cursor after each
tick: 0 after the erring tick (`x` landed, `r` held), 2 after the re-sync,
`off` 2; `x`'s word and graph are asserted. A cursor moved past `r` no longer
lists it, so the re-send now proves the hold. RED against the mutant that
lets the cursor pass a held graph (`finalize_on` ignoring `graph_done`'s
`false`), unbudgeted and on the hook's path: `after the erring tick the cursor
stays below the held root (score 1), though x (score 2) landed, left: 2 right:
0`; with that assertion taken out, `the re-sync lands the waiting part, left:
[]`. s1 as it stood on `2ea647a` is green against the same mutant (run here).

**L3, the cursor's rule under `on`.** `gasp_shadow::cursor_may_pass(outcome)`,
the default of `GaspFinalizeHost::graph_done`: the cursor may pass a graph
once every body of it landed or is held by a TERMINAL word (`Lookup`,
`Deterministic`: its own, a walk refused 400, or a parent's the gate passed
on); a body held `not now` or transient holds it, so the peer's next sync
re-sends the graph. As the reference moves past a refused graph and the anchor
check's refusal moves it. Witness s3 (`gasp_on_seam.rs`, the default
`graph_done`, `x` beside, both paths): the host refuses `a` for good, `b`
lands, `r` is refused `parent a was refused: ...`, `x` lands; RED on `971a8ea`:
`a graph held by terminal words alone lets the cursor pass it, left: [0, 0]
right: [2, 2]` (the graph re-sent and re-judged on the next tick); green
`[2, 2]`, the next tick re-sends nothing. Its second half: `b` refused `not
now` beside `a`'s terminal word holds the cursor at 0; the next tick re-sends
the graph, `b` lands, the cursor 2. A host that overrides `graph_done`
(a17-2's `GaspHost`: `matches!(outcome, AllLanded { .. })`) keeps the old rule
until it calls `cursor_may_pass`: that is a20's.

**L4, one place per occurrence.** A GASP graph carries a txid once per NODE,
and a transaction is a node once per output the graph spends of it; the
finalize hands on one BEEF per node and drops no repeat (as the reference,
"The assembly and the two limbs" in `CLAUDE.md`). So a graph does repeat a
txid, and the fix is the position, not a refusal. `OnGraph` keeps each walked
body's place in the sink in a vector parallel to the list the gate is handed
and finds the body by identity (the gate hands each body by reference into
that list). Witness s4: `c` spends `p.0` and `p.1`, so `p` is two bodies of a
three-body graph. RED on `abe8c52`: `every place of the graph handed once,
left: [0, 0, 2] right: [0, 1, 2]`; green: answered at `[0, 1, 2]`, each
`FinalizeBody::beef()` the BEEF at its own place and its body's, `p` landing
once and its second place `landed (dedup)`, on both paths.

**Gates, on the tip of `lane/atomic-gasp-seam-host-2` (`1dc31a6`).**

- `cargo test --workspace --features bsv-overlay-engine/memory-storage
  --no-fail-fast`: **1,707 passed, 0 failed, 66 ignored** (105 result lines;
  1,704 before, plus M1, s3, s4; s1 amended). With the feature off (`-p
  overlay-integration-tests --no-default-features --test gasp_shadow_off
  --test gasp_dependency_gate`): the eleven gate witnesses and
  `gasp_shadow_off` green; the `off` recording
  `431baab4e305582981b14370ed9ee36007a0a922e25341e9ca7c772192d4d112`, polls
  `[57, 11, 54, 11]`, unchanged; the D44 re-sync digest `c1663764…` unchanged.
- clippy with warnings denied (the root workspace all targets with
  `memory-storage`; the engine all targets with no feature and with
  `gasp-shadow,memory-storage`) and `cargo fmt --all -- --check`: clean.
- **The wasm, raw release** (`bsv-overlay-cloudflare`, no path remap, as the
  lens built it, both trees at THIS worktree's path, `2ea647a` checked out in
  place): feature OFF `2ea647a` 8,989,118 B, this tip **8,989,118 B (+0 B**;
  144 bytes differ, the same length: the engine crate's hash in mangled
  names), none of `gasp_shadow`, `gasp_on`, `finalize_on`,
  `admit_graph_hosted`, `walk_with_mode`. Feature ON: `2ea647a` 9,599,378 B,
  this tip **9,539,411 B (-59,967 B)**; 101 `gasp_shadow` names, 18 `gasp_on`
  (16). The lens's 8,986,373 B and 9,601,335 B were built at its own
  worktree's path (a different length in every embedded path), so its figures
  stand beside these, not against them. The feature-on shrink was not chased
  (the txid map's `HashMap<String, usize>` is gone from `finalize_on`; the
  gate-lens record shows how far LLVM's inlining moves a `gasp-shadow` build).
- Not run in this lane: the Workers' workspace tests (no Worker crate
  changed), `make ci-deploy`, `make gasp-shadow-size` (its `git archive` path
  bias, stated above), any measure under workerd or wasm32 of the hosted
  gate's heap, Zanaadu's a17-2 host against this tip (its `graph_done`
  override, above).
