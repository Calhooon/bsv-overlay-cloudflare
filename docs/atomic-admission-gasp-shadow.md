# GASP's shadow under `admit_graph` (bsv-low #657, R7.7 item 5)

The engine's later PR of Zanaadu's atomic admission round (the owner's D31),
on A's first PR (draft #43, `atomic-admission-a1` at `9d5a75a`). The design:
zanaadu-v2 `docs/audit/2026-10-13-eve/atomic-admission-design.md`, "The next
slice, re-cut against what was built (R7.7)" item 5, 3.3a "The GASP gap"
(R7.3) and test 29's "not shadowed" rows.

Until this PR the engine's own GASP finalize submits (`submit_finalized_graphs`
calling `submit_bounded` under `historical-tx-no-spv`, one graph at a time) had
no observer, so Zanaadu's census marks the HTTP corpus's four GASP chaos
records "not shadowed". This PR is their shadow, the engine's.

## What it does

- **The switch.** `Engine::set_gasp_shadow(AdmissionMode, Rc<dyn GaspShadow>)`
  (`gasp_shadow.rs`). `AdmissionMode::parse` reads the Worker's words: `off`
  (the default), `shadow`, and `on` refused ("ADMISSION_MODE=on is not
  enabled: the admission log runner is a later slice"). The shadow runs only
  under `Shadow` with a host set.
- **The host** (`GaspShadow`): `snapshot(scope, budget)` (a disposable engine
  over an isolated copy of the served store, its unit runner, and the rows and
  bytes it read), `now_ms()` and `record(row)`. The reference host,
  `MemoryGaspShadow` (feature `memory-storage`), copies the whole
  `MemoryStorage` (`MemoryStorage::shadow_copy`, `shadow_size`) and builds the
  shadow engine with the caller's managers and no tracker, broadcaster,
  fetcher or peer factory. The Worker's D1 host (its scoped SQLite, the slice's
  `ShadowEngine`) is the Worker's to add; nothing in `overlay-cloudflare`
  changes, so every Worker stays `off`.
- **Per body** (`engine/gasp_shadow.rs`). In the served path, before each
  served finalize submit: one snapshot. After it: the served word (the slice's
  `Word::legacy`, its STEAK in topic order) and the served judgement queued.
  After the peer's sync, outside its per-peer race: A's `walk_with_mode` of the
  body on the snapshot (`historical-tx-no-spv`, `NotNow`), handed the evidence
  the served path gathered (none: the finalize does not walk and asks no
  network, the anchor check already ran; `ServedEvidence` returns that record
  and nothing else), `judge_dry` beside the served judgement, and the body's own
  unit through `admit_graph_observed`. One census row.
- **Per graph** (the finalize hop's one materialization). In the served path,
  before the graph's first body: one snapshot; after its last served body (or
  the one that did not land): the rows the graph reaches read from the served
  store. After the sync: every body walked on the graph's snapshot, then
  `admit_graph` over the whole graph (each body its own unit, stopping at the
  first that does not land), and the same rows read from the shadow's store.
  One census row comparing what landed, where it stopped and the rows.
- **The delta classes** are the slice's (`same`, `not_shadowed`,
  `unit_refusal`, `evidence`, `words`) plus `materialization` for a graph row.
  `unit_refusal` is the owner's `unit-wide` first delta (D34, given
  2026-10-14): A refused the unit where the served path landed a topic, the
  intended change, counted. A body row is `words` when the words differ or
  `judge_dry`'s judgement differs from the served one.
- **The budget** is the slice's loop's: 4,096 rows, 16 MiB and 2,000 ms per
  comparison (`ShadowBudget`, `set_gasp_shadow_budget`); past a limb the row
  says `not shadowed: budget (reads: ...)` or `not shadowed: budget (time:
  ...)`. The time is the shadow's own (its snapshot and its work), never the
  served submit's. A failed snapshot is `not shadowed: shadow setup: ...`; a
  failed `record` is logged; neither changes anything served.
- **`off`** runs none of it: no snapshot, no read, no extra await. Each new
  step in the finalize loop is a `match` on `None` with no await on that arm,
  and the drain after a peer's sync is guarded by the same check.
- `admit_graph` is now `admit_graph_observed` with a no-op observer (the shadow
  words each body's unit answer as it is given).

## The census row

`{kind: body|materialization, peer, topic, graph (root txid), txid, position,
served: {status, bytes, landed_topics}, shadow: {status, bytes, landed_topics},
delta, served_judgement, shadow_judgement, served_evidence, shadow_evidence,
shadow_seqs, snapshot_rows, snapshot_bytes, shadow_ms}`. A body word's bytes
are `{"steak", "refused", "faults"}` (served, and A's unit as the `on` runner's
door will word it: `Lookup`/`Deterministic` 422, `NotNow` 503, `Transient`
500); a materialization word's are `{"landed", "stopped", "rows"}` with the
stopping body's status (200 when all landed). Status 0 is "not shadowed".

## Witnesses

`crates/overlay-integration-tests/tests/gasp_shadow.rs` and
`gasp_shadow_off.rs`, over `tests/support/gasp_shadow_world.rs` (proven
transactions served by a scripted peer that can be down, partitioned or
blipping; a stateless manager that admits every output; a recording storage
that yields once per call; a counting executor). Every shadow run is beside an
`off` run of the same scenario, and the peer's calls, the sync results, the
rows of every transaction and the cursor must be equal ("never a served
change", and "never a second outbound call": the peer is asked exactly what
`off` asks).

RED on `380bd6c` (the witnesses with the API inert), green on `fabeb44`:

| witness | RED (380bd6c) | green |
| --- | --- | --- |
| w1 one census row per finalized body, `judge_dry` beside the served judgement, the duplicate body | `left: [] right: [("28f15e0d…", 0), ("bd15fb78…", 1), ("28f15e0d…", 0), …]` | 8 body rows (7 under the per-peer budget's hook path, where the shared parent is held before the second walk and stripped), all `same`, judgements equal, the shadow's evidence its walk alone |
| w2 the materialization against the served one | `left: [] right: [("bd15fb78…", 2), ("eb5ded05…", 2), ("a45e4ca6…", 1), …]` | 5 graph rows `same`; a manager erring at the finalize on the middle link: both bodies `same`, the graph `materialization` (served landed `[g]` stopped at `c`; A landed `[g, c]` stopped at `d` waiting for its predecessor, F5/C7; the rows equal) |
| w3 01_downtime_recovery (the re-sent range) | `left: (0, 0) right: (6, 4)` | 6 body rows, 4 graph rows, no served change |
| w3 02_partition_heal (the partial listing) | `left: (0, 0) right: (6, 6)` | 6 and 6, the visible half on the split tick, the rest on the heal |
| w3 03_chain_fork | `left: (0, 0) right: (3, 3)` | 3 and 3; the orphan, discarded by the anchor check, never finalized and never shadowed |
| w3 04_cursor_no_skip_on_transient (the cursor skip) | `left: (0, 0) right: (3, 3)` | 3 and 3, the blipped UTXO shadowed on the tick that admits it |
| w5 `not shadowed: budget` | `left: 0 right: 13` | rows past 0: every snapshot holding anything is `not shadowed: budget (reads …)`; a clock at 10 s a read: all 13 `not shadowed: budget (time …)`; nothing served moves |
| w4 `off` with a host set (one that panics if asked) | green (an `off` pin) | the recording equals A1's |
| `gasp_shadow_off` the `off` recording | green (an `off` pin) | `431baab4e305582981b14370ed9ee36007a0a922e25341e9ca7c772192d4d112`, polls `[57, 11, 54, 11]` |
| w6 the mode's words | green | `off`, `shadow`, `on` refused |

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
`git archive` of `9d5a75a` (before any of this PR) and frozen; on `fabeb44`,
with and without a host set under `Off`, they are equal.

## Counts, gates and size

- `cargo test --workspace --features bsv-overlay-engine/memory-storage
  --no-fail-fast`: **1,681 passed, 0 failed, 66 ignored** (A1 recorded 1,671;
  the ten are this PR's: nine in `gasp_shadow`, one in `gasp_shadow_off`).
- `cargo test --manifest-path workers/Cargo.toml --workspace --no-fail-fast`:
  **1,565 passed, 0 failed, 1 ignored** (A1's 1,565).
- `cargo clippy --workspace --all-targets --features
  bsv-overlay-engine/memory-storage -- -D warnings`, the Workers' clippy with
  the same flags, and the engine's clippy without features: clean.
  `cargo fmt --all -- --check`: clean.
- `cargo build -p bsv-overlay-engine -p bsv-overlay-discovery --target
  wasm32-unknown-unknown`: green. `make ci-deploy`: all three deployable
  configs built through the real worker-build (dry runs, not deployments).
- **The wasm size**, `wrangler deploy --config wrangler.low.toml --dry-run`
  (`low-overlay`, the LIVE config), this branch against a `git archive` of
  A1's `9d5a75a` built the same way:

  | | wasm | Total Upload | gzip |
  | --- | --- | --- | --- |
  | A1 `9d5a75a` | 6,660,132 B | 6,563.08 KiB | 2,241.40 KiB |
  | this PR | 7,045,776 B | 6,939.69 KiB | 2,353.68 KiB |
  | delta | **+385,644 B** | +376.61 KiB | +112.28 KiB |

  The Worker sets no host, so the shadow never runs there, but the finalize
  now reaches A's path (`walk_with_mode`, `judge_dry`, `admit`,
  `admit_graph_observed`, the comparison), which the linker used to strip as
  unreachable from the Worker. That is the reasoned cause; no symbol-level
  diff was run. If the keeper wants `off` builds lean until a Worker host
  exists, a cargo feature around the shadow's call sites would take the
  delta back; not done here, because the Worker's host will need it linked.
- The final witness text was re-run against `380bd6c`'s engine (a `git
  archive`, the witness files copied in): the same seven RED, w4 and w6 green.

## Limits, stated

- The snapshots and the served rows are read INSIDE the served path (before
  each served write, and after each graph), so under `shadow` a budgeted peer's
  race spends their time; the walks, judgements, units and census writes run
  after the race and spend none of it. `off` spends nothing.
- A graph the per-peer deadline drops mid-graph (the hook path) leaves the
  body rows already queued and no graph row; the jobs queued by a sync the
  Worker's outer race drops are dropped with it.
- The memory host copies the whole store, so its `snapshot_rows` are the
  store's; the Worker's host scopes its read (the slice's scoped SQLite) and
  reports what it read.
- The served finalize gathers no evidence, so the `evidence` class cannot fire
  on a GASP body; the row carries the shadow's walk verdicts for the census.
- Not run here: a Worker host over D1, the HTTP corpus's runtime pin with the
  GASP rows shadowed (Zanaadu's `atomic-shadow.mjs` still prints them "not
  shadowed" until its engine re-pin and a Worker host), hosted D1.
