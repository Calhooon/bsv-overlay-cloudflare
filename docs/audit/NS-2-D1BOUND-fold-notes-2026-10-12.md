# NS-2-D1BOUND fold, 2026-10-12

Lane NS-2-D1BOUND-FOLD on `box`, isolated worktree
`~/bsv/wt-ns2-d1bound-fold`, private target
`/Volumes/Lanes/targets/ns-2-d1bound-fold`. The shared overlay checkout's HEAD,
index and files were untouched. No deploy, spend or merge.

The from-scratch `NS-2-D1BOUND-lens-2026-10-12.md` judged the original
`129ed6a` SOUND for LOW's beta, with M1–M3 required for the land. This fold is
for the captain's delta lens; the PR is a draft, and merge belongs to a later lane.

## Findings and controls

| Finding | Fold commit | RED | GREEN |
|---|---|---|---|
| M3 | `d9f8bd9` | original constructor, wrangler 4.149.0: ERR_VALIDATION, unrecognized modules/script/d1Databases/d1Persist (1) | both 4.148.0 (deploy) and 4.149.0 (current): 177 identical pages, max 160/287 (0 each) |
| M2 | `f0c3f7f` | populated original SQL: FullscanStep [7548,22577] != [0,0] (101); local D1 7550+22579=30129 rows read, zero writes (measurement exit 0) | guard before CROSS JOIN: FullscanStep [0,0]; D1 1+1=2 rows read, zero writes (0); Leg B 533 links and 7549 seats byte-identical to triggers (0) |
| M1 | `cc58aea` | rebase conflict (1), missing terminal E0063 (101), exact-body missing main's three fields (101) | cargo check (0), terminal pins (0), fields tier/hintSource/observedAtMs retained by name |
| L1 | `b30bb59` | neutered last-row initialization: (10,3) != (10,9) (101) | maximum cursor key, reordered/NULL/empty pages (0) |
| L2 | `67a9641` | neutered marker-first stamp: [true,false,true,false] != [false,true,true,false] (101) | pot COALESCE precedence, NULL fallback and byte identity across four eras (0) |
| L4 | `08985c7` | documentation finding, no pin | budgets doc states six derived statements per pot UPDATE, about ten reads and two to four writes at beta shape |

M3 adapts constructor and persistence options, retaining the ordered page and
cursor-row contract and the real rows-read ceiling. The explicit bundle knob is
`CURSOR_PIN_WRANGLER=wrangler@4.148`. One early 4.148 pin invocation raced its
fixture load and exited 1 (`no such table`); the final completed-fixture run
passed. Both fixture-load commands exited 0. L3 was resolved by the lens's
measurement and requires no fold.

Rebased onto `origin/main` `13f0e66`. Only conflict:
`crates/low-app-layer/src/txany.rs`, in `tx_any_value`. Main's `tier`,
`hintSource`, `observedAtMs` stay inside the body and the branch's conditional
`terminal` append stays afterward. The new CourierHint literal uses Default
(terminal false); FirstPartyPresent explicitly sets false. The owed cursor shape
is retained. Both locks exactly match main, with one bsv-rs 0.5.0 in each;
no separate lock edits.

## Foreground gates

Every heavy command was capped below ten minutes. Route tiers used the
worktree above with distinct `LANE_BASE=31000` (D1) and `31200` (route),
verified free before each launch; D1 ports 31009/31010, route ports
31200–31208 and 31211/31212.

CI pins Rust 1.95.0, which was used for the final clippy, test and route gates.
The box's default is 1.98.1; exploratory root and workers clippy each exited 101
on inherited newer-toolchain lints (`unused_async_trait_impl`,
`useless_borrows_in_formatting`, `result_unit_err`). No lint was suppressed.
The extra workers `cargo fmt --all -- --check` exited 1 on broad existing
format drift, already named in the branch's readiness notes. The workflow
explicitly reserves workers formatting for its separate K1 hygiene commit;
its required root format gate passes on both toolchains (0 each).

Diagnostic corrections: the first Leg B pin attempt exited 101 because its
new dump ordered four columns on the three-column seat table; using each
table's primary-key order fixed the harness, and the final four-pin run exited
0. The first L2 neutered run exited 101 at the ordered-byte assertion with era
None; moving the mixed era first reproduced the explicit membership failure
above (101), and the restored production code passed (0). No neutered source
was committed. Native seed generation and both wrangler fixture loads exited 0.

| Gate | Exit / result |
|---|---|
| cargo +1.95.0 fmt --all -- --check (root) | 0 |
| cargo +1.95.0 clippy --workspace --all-targets --features bsv-overlay-engine/memory-storage -- -D warnings | 0 |
| cargo +1.95.0 clippy --manifest-path workers/Cargo.toml --workspace --all-targets -- -D warnings | 0 |
| cargo +1.95.0 test --manifest-path workers/Cargo.toml -p low-app-layer | 0; 674 passed, zero failures |
| RUSTUP_TOOLCHAIN=1.95.0 LANE_BASE=31000 make ci-d1-budget | 0; all ceilings and all sibling scripts passed |
| RUSTUP_TOOLCHAIN=1.95.0 LANE_BASE=31200 make -o ci-d1-budget ci-route | 0; 76 PASS, zero FAIL, 142.4 s (lens: 75 PASS) |
| git merge-tree --write-tree origin/main HEAD | 0 |
| storage ownership / workspace preflight / JS syntax / diff check | 0 each |

| D1 scenario | Before (original control, lens) | Fold gate | Ceiling |
|---|---:|---:|---:|
| /results:view | 7805 | 256 reads, 8 statements, 0 writes | 287 reads / 8 statements / 0 writes |
| /owed first read | 15323 | 311 reads, 28 statements, 18 writes | 528 / 28 / 20 |
| /owed served | 17 | 17 reads, 2 statements | 26 / 4 |
| owed-recompute maximum | 15041 | 203 over 24 detached walks | measured |
| /recovery-view | 7950 | 401 | measured sibling |
| /refund-view | 143 | 143 | measured sibling |
| /refund-backups | 10 | 10 | measured sibling |
| cursor/hydration pages | tuple control 288, exceeded 287 | 177 identical pages, max 160 | 287 per statement |
| /utxo-status, /pots-view | 8 | 8 each | 12 each |
| /results forwarding | 0 | 0 | 4 |
| /submit | 0 reads, 13 writes | 0 reads, 13 writes, 8 statements | 4 / 15 / 9 |

The budget census self-test, landing guard, dead letters, deferred graphs and
BEEF blobs pass. The route gate covers all eight overlay legs and ends with
“NL-6d corroboration legs route witness: every expectation held.” These are
regressions of the overlay worker, not additional money proofs of #638.

Unverified: full-whale HTTP /owed totals, the live #634 census after the next
loop, and completion of PR checks. No request ceiling is invented for the
whale's summed history or the measured siblings.

Next: force-with-lease push, draft PR, one read of its four checks, and remove
this worktree and its private target. The captain's delta lens judges before
any merge; merge is a later lane's. Logs are retained in
/tmp/ns2-d1bound-fold, and the per-tier Makefile logs use bases 31000/31200.

## Merge addendum: D1BD-L1

The delta lens is LAND. On main `17418b1`, keep its seven migration statements first, followed by the branch's twenty-two; total 218. Register names are `derived_hops::HOP_INSERT` through `PARTY_DELETE`, `LINKS_BACKFILL`, `SEATS_BACKFILL`, and `STATE_DONE` (positions at this merge: 207–215, 216–217, 218). A stamped backfill puts its guard in the OUTER derived table before CROSS JOIN; the populated pin proves zero fleet-scan steps and D1 measures 1 + 1 = 2 rows. The conditional tx-any `terminal` follows main's `tier`, `hintSource`, and `observedAtMs`.

D1BD-L2 remains owed: the route's initial combined page followed by `Cursor::start` is not replayed against the unpaged view by the fixture identity pin. No fold is requested here.

## Merge gate record

Rebase onto `17418b1` retained only the lens's named keep-both hunks. Main's appended fixture rows need SQLite replay rather than the branch's literal parser: seed RED (101, make 2), complete fixture/cursor GREEN (177 identical pages, max 160/287). The legacy marker-carry pin also counted the three derived INSERTs; excluding only `LINKS_BACKFILL`, `SEATS_BACKFILL`, `STATE_DONE` preserves its unexpected-carry guard (101→0; overlay suite 730 passed, one ignored).

Merged `/owed` measures 331 reads, 18 writes, 30 statements; the 28-statement budget reds, then the measured 30 pin greens. Original #638 trade: 15,323→311 branch rows read, 331 merged. Results 256/287, served owed 18/26, recompute max 216, refund/recovery/backups 143/401/10. The complete settled-source budget exits 0 (306.6 s), route tier exits 0 (76 PASS, 152.9 s); one intermediate budget attempt exited 2 on a dead-letter timing assertion during a watched-worker rebuild, and the settled-source run passed that assertion.

The first pushed four-check run at `98b3d3d` passed fmt/clippy/wasm32 but failed main's separate `ns2pd2_l1_owed_external_spender_skips_scope_statement` ceiling assertion (30 != 28). That assertion is aligned to the measured 30; its owed-scope behavior assertions remain unchanged. This companion ceiling pin reproduces RED locally (101) and is verified by the complete app suite after the correction. No merge on the red run.
