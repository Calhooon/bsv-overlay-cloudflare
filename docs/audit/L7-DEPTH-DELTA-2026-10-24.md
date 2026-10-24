# L7-DEPTH-DELTA: the delta lens on overlay PR 167's fold (6546ba67..89543e91)

Stamp: 2026-10-24 18:33 UTC (`date -u`). Lane `lane/L7-DEPTH-DELTA`, cut from 89543e91. The last lens on this seam.

Against: bsv-low `docs/audit/L7-DEPTH-LENS-2026-10-24.md` (NOT SOUND on MEDIUM-1, LOW-1 PLAUSIBLE), the fold
brief `docs/lanes/L7-DEPTH-FOLD-brief-2026-10-24.md`, this lane's brief
`docs/lanes/L7-DEPTH-DELTA-brief-2026-10-24.md`.

## Verdict: LAND

MEDIUM-1 is closed by one shared bound (`SPEND_DEPTH_MAX` 144), with the floor served and the counter bumped. Both
renders set `"1"`. LOW-1's judgement holds. Nothing else moved. There are no findings above NOTE.

## 1. MEDIUM-1: closed

The rule, at `crates/low-app-layer/src/tracker_host/runtime.rs:324` (the constant) and `:340-354`
(`spend_depth_of`): the var is trimmed, must be ASCII digits, must parse as `u32`, and must lie in
`1..=SPEND_DEPTH_MAX`. Any other value gives `{depth: 1, from_var: false}`. `spend_depth_noted` (`:362-368`) counts
`spend_depth_var_malformed_total` once per isolate whenever `from_var` is false. The health read
(`spend_depth_health_json`, `:405-413`) derives `source` from the same `from_var`.

One function at every read site. The four stamp sites and the health read all go through `spend_depth_of` on the
same var with the same constant:

- `/tx-any/:txid` and `/tx-any?txids=`: `body_loaded`, `runtime.rs:198`, through `spend_depth(env)`.
- `/utxo-status`: `outpoint_body`, `runtime.rs:312`.
- `/spent-any`: `routes.rs:9933-9936`, through `spend_depth(&ctx.env)`.
- `/health.spendDepth`: `routes.rs:11068`, through `spend_depth_health_json`, which reads `spend_depth_of`.

There is no second parse and no second bound. The var is parsed again on each call rather than cached. That is
harmless: a var is fixed within an isolate and the function is pure. The once-per-isolate rule applies to the
counter latch only.

Edge values. I checked each through `spend_depth_of`, `spend_depth_health_json` and `stamp_spend_depth` on a
`/tx-any` subject, a batch `answers[id]` and an outpoint row. The test file was temporary and was not committed.

| var | served at every site | `/health` source | counted |
|---|---|---|---|
| `"145"`, `"0"`, `"-1"`, `"1.5"`, `"4294967295"`, `""`, unset | 1 | `floor` | yes, once per isolate |
| `"1"` | 1 | `var` | no |
| `"144"` | 144 | `var` | no |
| `" 7"`, `"07"` | 7 | `var` | no |

On `" 7"` and `"07"` I disagree with this lane's brief, which listed both as the floor. The PR documents the parse
as "surrounding whitespace allowed" (`runtime.rs:338`), and the lens's N3 already judged leading zeros and
whitespace harmless. The fold kept them, pinned by `lens_the_vars_edge_values` (`"01"`, `"006"`, `"\t2\n"`). No money
consequence follows. The host serves the integer 7 on every word, the client's `parseServedSpendDepth` honours 7,
and `/health` reports `depth: 7, source: "var"` and echoes the raw var. Both sides therefore agree on the number the
operator wrote. Making these values the floor would only turn an unambiguous value into a counted floor. NOTE,
no fold asked.

### The unset var counts: judged correct

The fold brief asked for an unset var to serve the floor uncounted. The fold kept the PR's rule instead: unset is
counted (`l7_a_missing_var_is_the_floor_counted_once`; my probe gave `Some(("spend_depth_var_malformed_total", 1))`
after three reads). I agree with the fold.

- Neither deployed render leaves the var unset. Prod and beta both set `SPEND_DEPTH = "1"`
  (`crates/low-app-layer/wrangler.toml:115`, `:292`), and `l7_both_renders_set_spend_depth_1` pins it. On a
  deployed render an unset var is never "the default configuration of a render that chose not to set it". It means
  the `[vars]` line was lost in an edit, which is exactly the misconfiguration the counter exists to show.
- The cost is one increment per isolate, written through the existing flush. That is not noise on a render that
  sets the var. A render that wanted no counts would set `"1"`, as both do.
- The floor is served either way, so the count changes no credit.
- `/health.spendDepth.var` is `null` for unset and echoes the string for a malformed value, so an operator can tell
  the two apart without a second counter.

The counter's name says "malformed" while it also counts a missing var. That is a naming NOTE only. The CLAUDE.md
paragraph and the doc comment (`runtime.rs:327-328`) both say "missing or malformed".

NOTE: the `/health` read calls `spend_depth_of`, not `spend_depth_noted`. An isolate that only ever answers
`/health` therefore shows `source: "floor"` without counting. The count comes from the first served word. This is
coherent, because `/health` is itself the loud signal.

### The fold's RED, reproduced on 6546ba67

Method: a detached worktree at 6546ba67 (removed afterwards). I grafted the fold's `tests/spend_depth_l7.rs` and the
constant alone (`pub const SPEND_DEPTH_MAX: u64 = 144;`, with no rule change), then ran
`cargo test --manifest-path workers/Cargo.toml -p low-app-layer --test spend_depth_l7`. Exit 101, 9 passed, 1
failed:

```
test l7f_m1_a_var_past_the_shared_bound_is_malformed_the_floor_counted ... FAILED
assertion `left == right` failed: SPEND_DEPTH="145"
  left: (145, true)
 right: (1, false)
```

This is the commit message's line, verbatim. On 89543e91 it is green.

## 2. The two renders (fold tip 89543e91)

| render | `SPEND_DEPTH` | judgement |
|---|---|---|
| prod `[vars]` (`crates/low-app-layer/wrangler.toml:115`) | `"1"` | the floor, today's behaviour, served `source: "var"` and uncounted |
| beta `[env.beta.vars]` (`wrangler.toml:292`) | `"1"` | the same |

The owner's R3 set no number, and 1 is today's proven behaviour. No value above 1 is set, so nothing waits on the
owner's word. Any depth above 1 is the owner's decision by name.

## 3. LOW-1: I agree with the fold's judgement

Source: bsv-low `origin/lane/L7-TOWER` at fc8dd09a, `app/src/lib/creditWord.ts:244-254`.

`servedDepthOf` takes the largest `spendDepth` from both the outpoint row and the `/tx-any` subject. It keeps the
session's largest ever seen in `servedDepthSeen`. When a read serves none after the session has seen one, it
answers `null`, which becomes `DEPTH_UNREAD`, i.e. `pending`. The client bound is `SERVED_SPEND_DEPTH_MAX` 144
(`chainReads.ts:2390`), equal to the server's.

So a faulted `/tx-any` (the legacy body, no depth) no longer drops a credit to the floor. The outpoint row read in
the same credit carries the depth, and if it did not, the session memo makes the read pending.

The one residual is already stated in the client's own comment (`creditWord.ts:40`): a session whose first reads all
fault before any answer has served a depth judges at the floor. That is unreachable at today's `"1"`. Changing it
would mean stamping the legacy fault body, which breaks the byte-identity the PR pins. The fold rightly made no
server change.

## 4. Nothing else moved

`git diff --stat 6546ba67..89543e91` lists 4 files: `CLAUDE.md` (the `spendDepth` paragraph only), `runtime.rs` (the
constant, two doc comments and one `filter`), `tests/spend_depth_l7.rs` (the module doc and the fold's two pins),
and `tests/spend_depth_l7_lens.rs` (new: the lens's seven, c20ff61f as the audit's patch wrote them).

The fold adjusted two of the lens's cases (`git diff c20ff61f..89543e91`). Neither change weakened a case.

- `lens_the_vars_edge_values`: the `4294967295` row moved from `(4294967295, true)` to `(1, false)`, and a `145` row
  was added. It now covers more of the bound.
- `lens_m1_a_depth_past_the_clients_bound_is_served_unclamped` became `..._is_the_floor`. The old `assert!(d.from_var
  && d.depth > 144)` became an exact `(1, false)`, `source` went from `"var"` to `"floor"`, and the stamped
  `spendDepth` went from `d.depth` to the literal `1`. It also adds `SPEND_DEPTH_MAX == 144`. Every assertion is
  exact, and the case is stronger than before.

The CLAUDE.md paragraph is accurate. It names the bound, the client's constant, the fold's pins and their RED line,
the lens's adjusted cases, and LOW-1's judgement. It says "decimal digits" and omits the trim, which the code's doc
comment states. NOTE only.

## Commands and exits

All runs used `CARGO_TARGET_DIR=/Volumes/Lanes/targets/L7-DEPTH-DELTA` and `--manifest-path workers/Cargo.toml`. Logs
are under `/Volumes/Lanes/out/L7-DEPTH-DELTA/`.

| command | result |
|---|---|
| `cargo test -p low-app-layer --test spend_depth_l7 --test spend_depth_l7_lens` | exit 0; 10 + 7 passed |
| the lib binary, run directly from the crate dir in six parts (`owed::`, `logic::`, `routes::`, `results::`, `identity::`, then the rest via `--skip`) | each exit 0; 142 + 79 + 62 (2 ignored) + 54 + 40 + 323 = 700 passed, 2 ignored, out of the 702 listed; `routes::` alone took 478 s |
| `cargo test -p low-app-layer --tests --no-run`, then each of the 40 integration binaries from the crate dir | every one exit 0, `test result: ok` (`int-1.log` to `int-40.log`) |
| `cargo clippy -p low-app-layer --all-targets -- -D warnings` | exit 0 |
| `rustfmt --edition 2021 --check` on the two test files; `rustfmt --emit stdout` of `runtime.rs` diffed against the file | exit 0; no diff |
| `cargo test -p bsv-overlay-cloudflare --lib applayer_counters` | exit 0; 1 passed |
| the RED on 6546ba67 (section 1) | exit 101; the line reproduced |
| the edge-value probe (a temporary test, deleted, never committed) | exit 0; the table in section 1 |

## Not verified

- The route tier and the NS3D-M1 fault cell (the captain's).
- The overlay crate's clippy: this brief asked only for `-p low-app-layer`, and the fold changed no overlay source.
- `cargo fmt --all --check` (the 28 files of known drift are not touched).
- wasm32 and the deployed renders' live `/health`.
- The client half was read, not run (bsv-low `lane/L7-TOWER` at fc8dd09a).
