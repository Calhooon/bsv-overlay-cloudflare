# W15E-L4-DELTA-LENS: the delta lens on overlay PR 154 after its fold (bsv-low #620 the words map's L4, #777, #778)

Stamp: 2026-10-23T22:09Z. Lensed: `lane/W15E-L4` at `8040093b` (the fold's head), on the lens branch
`lane/W15E-L4-DELTA-LENS`. The delta: the fold's six commits against the from-scratch lens's findings
(`docs/audit/W15E-L4-LENS-2026-10-23.md`, NOT SOUND: MEDIUM-1 to 3, LOW-1 to 3), the merge of main (`f3039f1a`, the
feed's table at migration 347) and the e607 pin's new shape (`8040093b`). Read: the first lens whole, each fold
commit's diff and message, `chain_feed.rs` (the predicate, the gate function, every arm of `consume_chain_feed`, the
`write_now!` macro, `run_chain_feed_pass`, `health_json`), `tick.rs` and `tip_pass.rs` at the gates,
`arcade_reorg.rs` (`judge_window_legs`, D7's consumer), `chain_feed_tests.rs` whole (the lane's, the lens's and the
fold's pins), `d1/mod.rs` (the migration list, the count, the boot gate), the app layer's `chain_events.rs`
(`health_read`, `health_served`, the amended `i771`, `i777f_l2`), `lib.rs` (the scheduled wiring), `routes.rs`
(`health`), `tests/e602_convergence.rs` (`e607`), this repository's CLAUDE.md section, LOW's row D7 and enforcement
test 6, `gh pr view 154` and its two comments. This is the second and LAST lens on this seam.

## Verdict: LAND

Nothing above LOW. The fold answers every finding of the first lens: the design finding (MEDIUM-3, Rule 6) is answered
by the liveness predicate and ONE gate function both passes read, pinned per stop class with D7's consumer run on the
same store; the two gate reds (MEDIUM-1, MEDIUM-2) are green on the house toolchain; LOW-1 is pinned by behaviour
(`lens_l7`, RED under the blind-demotion mutant by its counts), LOW-2 by the 10 s memo (`i777f_l2`), LOW-3 by the
once-counted contention (`lens_l8`). The merge is idempotent and append-only, its count and fingerprint pinned, and the
e607 pin's new shape still catches a dropped or reordered migration together with `e620_f`. Every gate is green:
the workers' lib suites (987 and 677), their integration targets, clippy with warnings denied in both workspaces, the
root workspace (1729), the ownership checker.

One LOW remains, stated for the record (the land proceeds): the liveness predicate reads the row's LANDED document, so
a stop SET this pass whose write did not land (a lost compare-and-set, a faulted write) is read as "the word" for that
pass and D7 is skipped for it. One pass, self-healing, nil in practice (below); a two-line fix and a case are in this
file for the captain or a follow-up.

## The delta, finding by finding

### MEDIUM-3 (the design finding, Rule 6): answered

`FeedPassSummary::is_reorg_word` (`chain_feed.rs:417-421`) is now the lens's liveness predicate:
`!unconfigured && faults == 0 && state.as_ref().is_some_and(|st| st.stop.is_none())`. `faults` is bumped by every
read fault of the pass (`:743` a leg's read fault, `:782` the feed's transport, `:808` a page that did not parse,
`:834` an HTTP status other than 200 and 409, `:906` the tip read for an `invalidated`); `state.stop` is the recorded
stop (`:818` the 409, `:869` an envelope the crate refuses), cleared only when an event past it is consumed (`:886`,
`:918`) or by the lever (`:1046`). The gate is ONE function, `arcade_not_an_input(Option<&FeedPassSummary>)`
(`:521-524`): `None` means D7's consumer runs. Both sites call it and neither names the predicate: the cron's
`tick.rs:600` (`if let Some(why) = crate::chain_feed::arcade_not_an_input(Some(&feed))`, D7 in the else arm) and the
block-event pass's `tip_pass.rs:245` (`arcade_not_an_input(feed.as_ref())`, `None` when there is no D1 handle, so D7
runs with no feed pass at all). `e620_g` pins both sites by the gate's exact needle and asserts
`!tick.contains("is_reorg_word")` and the same for `tip_pass.rs`.

The transport fault: the fold DECIDED it counts as a stop for its pass (the lens's reading, taken): the feed judged
nothing that pass and D7 may; the next healthy pass re-takes the word with no lever. Stated in the commit message, the
predicate's doc comment (`:404-416`), CLAUDE.md's section ("a transport fault counts for its pass only"),
`docs/STORAGE-OWNERSHIP.md` and `storage-ownership.json`'s row. The row's own write fault (`errors`) and a lost
compare-and-set (`contended`) are NOT the feed's by the fold's reasoning (the rows judged were judged; another isolate
owns the row); the one gap in that reasoning is DELTA-LOW-1 below.

The pins. `e620_d` is inverted (`s.cursor_ahead && !s.is_reorg_word()`, "a recorded stop hands D7 back",
`chain_feed_tests.rs:554`). `lens_l1` is re-aimed: the lens's `feed_is_live` kept and asserted EQUAL to the shipped
word in every shape (three passes of an unreadable envelope with the stop recorded each time, a 409 and the pass after
it, a transport fault with the row present, the healthy journal). `e620f_m3` is the per-class pin the lane asked for:
for each stop class the feed's pass stops, `arcade_not_an_input(Some(&s))` answers `None`, and D7's consumer
(`consume_arcade_reorg_events`, the function `run_arcade_reorg_pass` runs, through the module's `d7_pass`) RUNS over
Arcade's orphan row at 101 on the SAME store and DEMOTES the refuted row the feed did not reach: (1) an unreadable
envelope, on the pass of the stop and the pass after it, then the lever's clear and the next pass consuming the reorg
with the feed the word again; (2) a 409; (3) a transport fault, an unparseable page and an HTTP 503, each followed by
a healthy pass that re-takes the word with no lever; (4) a leg's read fault (`FaultyTracker`); (5) no feed pass
(`arcade_not_an_input(None) == None`); (6) the healthy journal is the word and an unconfigured feed is not. It runs
the gate FUNCTION the sites call and D7's consumer FUNCTION the sites call; the sites themselves need a Worker `Env`
and are pinned by text (`e620_g`). That is the house's shape ("one function the handler calls as is"), and it is as
far as a native pin reaches here; stated.

Re-run here, the fold's mutant: `is_reorg_word` put back to `!unconfigured && state.is_some()` (the `40fec4ce` word)
reds `e620_d`, `e620f_m3` and `lens_l1` (and the delta case below), 4 of 37 in `chain_feed arcade_reorg`, reverted.

The double-demotion question, re-checked under the new predicate. The two consumers now DO run on one tick whenever
the feed's pass stops or faults, and the feed may have judged part of a window before a leg fault (its `pending`
kept with each leg's cursor, `:739-746`). Each demotion is a state transition the window read cannot see again: the
spenders leg reads `spentConfirmed = 1` rows only (`d1_discovery.rs:2319`, `confirmed_window_same_height_sql` and its
sibling), a demotion clears it, a re-anchor in place leaves a proof that stands against the active root. So D7 after a
feed pass scans what the feed left and demotes nothing twice (`lens_l4`: scanned 1, stale 0, standing 1, the ladder
asked once in all), and the feed's resumed window after D7's demotions scans fewer rows (reasoned from the predicate;
not run in that order). Two cursors on one EVENT cannot happen: the feed's row (`overlay_chain_events`) holds a journal
cursor, D7's (`arcade_reorg_state`) an `(orphanedAt, height, hash)` cursor over Arcade's listing; the R2 lifetime totals
and `hop_probe_memos::clear_on_reorg` are bumped by whichever pass demoted, once per row, and the memo clear is
idempotent. The cost of the overlap is reads: D7's corroboration header read and one header read per distinct
`(height, root)` on the feed's side. Stated, N1 below: a D7 handed back after a long live stretch resumes from its own
old cursor and judges Arcade's recent orphan events it skipped, two a pass, each idempotent, bounded by the listing's
two pages.

### MEDIUM-1 (the un-amended `i771` pin): answered

`chain_events.rs:2122-2150`, amended in `f775035a` and again in `730fbc8a`. It still pins the intake running over the
binding on the scheduled pass ALONE: `tracker_host::chain_events::run(&env_intake)` sits inside `async fn scheduled(`
and before `pub async fn fetch(`, before the tracker task's `backfill` and `tick`; exactly one `chain_events::run(` in
`lib.rs`; no route calls `run`, `pull` or `drain`; `BindingFeed` reads `.service("CHAINTRACKS")` and never
`Fetch::Url` or `Fetch::Request`; `/health` serves `chainEvents` through `health_served` and never `health_json()` or
`health_read(` directly. `i778_b` pins the task order. Measured: `-p low-app-layer --lib` 677 passed, 0 failed,
2 ignored (the first lens: 671 passed, 1 failed).

### MEDIUM-2 (the fixture's two clippy lines): answered

`chain_feed_tests.rs:70-78` `type PageAnswer = Result<(u16, Vec<u8>), String>;` and `:115` `.next_back()`.
`cargo +1.95.0 clippy --manifest-path workers/Cargo.toml --workspace --all-targets -- -D warnings` exit 0; the root
workspace's clippy exit 0.

### LOW-1 ("never a blind demotion" by behaviour): answered

`lens_l7_a_proofless_confirmed_row_is_never_demoted_blind` landed in `f775035a`: a confirmed spender with NO stored
bump at the reorg's height, no courier proof: left alone, `no_stored_proof` 1, `demoted_blind` 0, still confirmed; a
courier proof naming another block: demoted on that evidence. Re-run here under the mutant `demote_proofless: true` in
`judge_window_legs`: `lens_l7` RED `left: (0, 1, 0) right: (1, 0, 0)` and `e620_g` RED (the text belt), 3 of 37
(the third the delta case, RED on the base), reverted.

### LOW-2 (the public `/health` reading D1 on every call): answered

`chain_events.rs:1275-1312` `HEALTH_MEMO_TTL_MS` 10 s, `health_served` the route's one call (`routes.rs` `health`).
Checked: a fresh read serves `health_read`'s object with `source: "d1"` and `memoAgeMs` 0 and memoizes it; a read
inside the TTL serves the memo with `source` still `"d1"` (it is the durable read's answer), `memoAgeMs` its age and
`isolateCounters` re-stamped from the live tally; a read past the TTL asks D1 again; a FAULTED read is served as the
isolate memo (`source: "isolate"`, `readFault`) and never memoized, and a stale memo is never served over a fault
(the memo is consulted only inside the TTL; past it the read runs and a fault is the answer). The clock is the port's.
Statement count: one batch of two statements (`STATE_READ`, `HEALTH_COUNTERS_READ`) per isolate per 10 s whatever the
read rate. PR 149's ceiling is `/health/invariants` (`tools/lane-499/ceilings.json` `health-invariants.stmts` 34); the
app layer's `/health` has no ceiling in the tier (no `"/health"` entry in any `ceilings.json`), so "under the ceiling"
is the overlay's read: `health_invariants` (`ops.rs:1925`) issues `chain_feed::health_json(db)` (one statement,
`STATE_READ`, `lens_l5`) inside its one join (`fix838_the_invariants_reads_are_issued_together...` lists it); main's
`65027163` (#607) added counters served from the existing `ops_counters` read and no statement; so 32 + 1 = 33 of 34,
the first lens's arithmetic, unchanged by the fold. Pin `i777f_l2` green (two reads inside the TTL one batch, the age
served, D1 again past it, a fault served `isolate` and not memoized, D1 again after it, the memo after that).

### LOW-3 (the contention counted twice): answered

`chain_feed.rs:668-693` `write_now!`: `landed = state == written || (out.contended == 0 && persist(..) == Written)`.
After a lost compare-and-set nothing is tried again (the version held is stale for good) and the contention is counted
once; a write that FAULTED is retried once by the trailing write (it may land). Every in-loop site either breaks on a
false answer or breaks right after the call, and the trailing call is the last; so at most one more persist after a
fault and none after a contention. `lens_l8` re-aimed: `(applied, contended) == (0, 1)`, the summary's state the read
one, the `stopped` text "another isolate owns this pass". Green.

## The merge of main (`f3039f1a`) and migration 347

`chain_feed::CREATE` is `CREATE TABLE IF NOT EXISTS overlay_chain_events (id INTEGER PRIMARY KEY CHECK (id = 1),
state TEXT NOT NULL, version INTEGER NOT NULL, updated_at INTEGER NOT NULL)`: idempotent, touches no row. It is the
LAST entry of `OVERLAY_MIGRATIONS` (`d1/mod.rs:2288`, after `conflict_repair::REPAIR_CREATE` at 346,
`:2285`); `OVERLAY_MIGRATION_COUNT` is 347 (`:619`, its comment naming the renumber). The pins: `e620_f` (the whole
list run under real SQLite, `last() == CREATE`, `len() == OVERLAY_MIGRATION_COUNT`), `d1::tests::migrations_are_valid_sql`
(`len() == COUNT`), `migration_count_gate_is_exact_equality` (the boot gate compares the persisted count to the
constant exactly, so a deployed database at 346 re-runs the idempotent list once and is certified at 347 with the
list's fingerprint, `ensure_overlay_migrations`, `:345-372`), `migration_fingerprint_moves_on_content_and_certify_sql_is_two_row`,
`migrations_cover_all_tables` and `..._indexes`: all green in the lib run. The e607 pin's new shape (`8040093b`):
`len() >= 346` and `OVERLAY_MIGRATIONS[345] == REPAIR_CREATE`. What it still catches: the repair's CREATE moved or a
migration dropped before it. What `e620_f` catches beside it: the feed's CREATE dropped (the last entry would be the
repair's) or the two swapped (both `[345]` and `last()` move). A dropped or reordered migration anywhere in the list
is caught by one of the two together with the count pin. `e602_convergence` (10 passed, `e607_append_only_migration_and_the_page_uses_the_rowid_range`
among them) green. The merge's other hunks (`courier.rs`, `pot_hook.rs`, `conflict_repair.rs`, `tick.rs`, `ops.rs`,
the ownership rows) are main's #607 and #658 carried; the ownership checker is green over the merged tree (1278
references). The PR's comments: PR 151 (FAST-C) takes two migrations at its land, so whichever lands second renumbers
(the captain's); not a finding.

## Findings

### DELTA-LOW-1: a stop set this pass whose write did not land is read as "the word" for that pass

`chain_feed.rs:417-421` (`is_reorg_word`) with `:921-922` (`write_now!(); out.state = Some(written);`). `written` is
the last LANDED document. The 409 arm (`:815-825`) and the unreadable-envelope arm (`:865-876`) set `state.stop` and
bump no fault; if the trailing write then loses its compare-and-set (`contended`, another isolate wrote the row) or
faults (`errors`), `written` stays the read document, without the stop, and the predicate answers true: the gate says
"not an input" and D7 is skipped for that pass though the feed judged nothing and recorded nothing. Reproduced by the
delta case below: on the fold's head it is RED on the 409 under a lost compare-and-set,
`left: Some("not an input: the chain-event feed is the reorg word this pass (bsv-low #620)") right: None`, with the
summary showing `contended: 1, faults: 0, cursor_ahead: true, stop: None`.

Reach and money: one pass, self-healing. Under a lost compare-and-set the other writer is the cron and the block-event
pass racing on the row; the winner judged the same feed, recorded the stop and ran D7 for itself, so a refuted row is
judged that tick by the other pass. Under a faulted write D7 is skipped that pass and every pass the fault lasts; a D1
write fault that spares D7's own cursor row and its demotion writes in the same database has no known cause, and
under a general D1 write fault D7 writes nothing either (`a_lost_state_write_stops_the_pass_and_writes_nothing`). The
first lens's money consequence (a `claimable` served on a refuted index latch) is bounded here to the one pass in
which no consumer judged, at most the stop's recurrence under a persistent row write fault; nil in practice. LOW, for
the record.

Fix shape (two lines; applied here, measured, reverted): the predicate also reads the pass's OWN stop marks, which are
fresh on every pass whose stop never landed: `&& !self.unreadable && !self.cursor_ahead`. With the stop landed the
row's `stop` covers the later passes (`unreadable` and `cursor_ahead` are false there, counted once); with it never
landed the marks are true on every pass. Under the fix the delta case is green and `chain_feed arcade_reorg tick::
tip_pass` is 49 passed. The patch and the case are at the end of this file.

## NOTEs, stated

- N1: when the feed hands D7 back after a stretch of live passes, D7 resumes from its own cursor and judges the orphan
  events of Arcade's listing newer than it (two pages a pass, two events a pass on the cron, one on the block event),
  each over heights the feed already judged: idempotent (the window reads confirmed rows only), reads only, bounded
  by the listing's reach. Not pinned in that order (the feed's resumed window after D7's demotions); reasoned from
  the `spentConfirmed = 1` predicate.
- N2: `overlay_chain_events_faults_total` counts `faults + errors` together (`:494-495`), so a dashboard cannot tell
  the feed's read faults from the row's write faults; the log line carries both apart. Cosmetic.
- N3: the first lens's N1 to N5 stand (the kindless `reorgFrom` arm dead on LOW; the header read per distinct
  `(height, root)` with no per-read timeout; the lever below the journal's head; no config sets
  `CHAIN_EVENTS_START_CURSOR`, so on deploy the feed is unconfigured and D7 runs as before). The fold changed none of
  them and none is a finding of this lens.
- N4: environment, as the first lens's N6: every cargo command here ran on the house's 1.95.0 with
  `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk` and a private `CARGO_TARGET_DIR`; the app layer's
  integration binaries were run directly from the target dir to keep the cargo lock free (40 binaries present, 341
  tests, every binary green).

## The case and the mutants

| case | what | on the fold's head | under the fix |
|---|---|---|---|
| `delta_d1_a_stop_whose_write_did_not_land_hands_arcades_listing_back` | the 409 and the unreadable envelope, each over a row whose write lands (the control, `None`), loses its compare-and-set (`Contend`), or faults (`FaultWrite`) | RED at the 409 under `Contend`: `left: Some("not an input ..") right: None`, `contended: 1, faults: 0, stop: None` | green |

| mutant (applied to the fold's head, reverted) | RED in `chain_feed arcade_reorg` (37) |
|---|---|
| `is_reorg_word` = `!unconfigured && state.is_some()` (the `40fec4ce` word) | `e620_d`, `e620f_m3`, `lens_l1`, `delta_d1` (4) |
| `demote_proofless: true` in `judge_window_legs` | `lens_l7` (`left: (0, 1, 0) right: (1, 0, 0)`), `e620_g`, `delta_d1` (base RED) (3) |

## Commands and exits

Every command in this worktree, toolchain 1.95.0, `SDKROOT` as N4, `CARGO_TARGET_DIR` private (the root workspace in a
second private dir). Logs at `/Volumes/Lanes/out/wt-overlay-W15E-L4-DELTA-LENS/`.

| command | exit | note |
|---|---|---|
| `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib` (the compiled binary run directly) | 0 | 987 passed, 0 failed, 1 ignored, 228.75 s; `e620_a..g`, `e620f_m3`, `lens_l1..l8`, the 15 `arcade_reorg::tests`, `tick::tests`, `tip_pass::tests`, the `d1::tests` migration pins all `ok` |
| `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --test '*'` | 0 | 4 targets, 21 passed (`e602_convergence` 10, `e607_append_only_migration...` among them) |
| `cargo test --manifest-path workers/Cargo.toml -p bsv-low-proof-replay` | 0 | 4 passed |
| `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib` (the compiled binary) | 0 | 677 passed, 0 failed, 2 ignored, 502 s; `i771*`, `i771b*`, `i777`, `i777f_l2`, `i778_a`, `i778_b`, `b_fleet_4*`, `e649f2_n4`, `fix800_a..l`, `lb674_cut*` all `ok` |
| `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --test '*'` (the 38 compiled binaries run directly; 40 present) | 0 | 341 passed, 0 failed |
| `cargo clippy --manifest-path workers/Cargo.toml --workspace --all-targets -- -D warnings` | 0 | |
| `cargo test --workspace --features bsv-overlay-engine/memory-storage` (root) | 0 | 1729 passed, 0 failed, 66 ignored (the live tests) |
| `cargo clippy --workspace --all-targets --features bsv-overlay-engine/memory-storage -- -D warnings` (root) | 0 | |
| `python3 scripts/check-storage-ownership.py` | 0 | 1278 SQL references in 3 crates match |
| `python3 scripts/check-storage-ownership.py --self-test` | 0 | 39 ok, 0 failed |
| the delta case on the fold's head (`--lib -- delta_d1`) | 101 | RED as DELTA-LOW-1 states |
| the fix applied, `--lib -- chain_feed arcade_reorg tick:: tip_pass` | 0 | 49 passed (the delta case among them); the fix reverted |
| mutant A (`--lib -- chain_feed arcade_reorg`) | 101 | 4 failed (the table above); reverted |
| mutant B (`--lib -- chain_feed arcade_reorg`) | 101 | 3 failed (the table above); reverted |
| `gh pr view 154 --json ...` | 0 | read only |

Nothing deployed, nothing spent, no network beyond the `gh` read and the push of this branch. Not run (the
captain's at the land): the route tier (`make ci-d1-budget`, the invariants ceiling re-measure at 33 of 34), a tier
with a real feed page over a local chaintracks, and a deployment case of a stopped feed with a live reorg behind it.

## The patch (the fix shape and the case; applied in this worktree against `8040093b`, measured, reverted; not committed)

sha256 of the two diffs as files: fix `1bd1772b23d9b134f63ea8dcd75e96f25eed0fb106a6ba56572749fb36fb9632`, case
`1ca605810f34ad6b8d09f5b00d65e3792e194e50de510c3a3b080ee3f62e6c96` (also at
`/Volumes/Lanes/out/wt-overlay-W15E-L4-DELTA-LENS/w15e-delta-{fix,case}.diff`).

```diff
diff --git a/crates/overlay-cloudflare/src/chain_feed.rs b/crates/overlay-cloudflare/src/chain_feed.rs
index b89d53b0..81ba6d0e 100644
--- a/crates/overlay-cloudflare/src/chain_feed.rs
+++ b/crates/overlay-cloudflare/src/chain_feed.rs
@@ -416,6 +416,8 @@ impl FeedPassSummary {
     pub fn is_reorg_word(&self) -> bool {
         !self.unconfigured
             && self.faults == 0
+            && !self.unreadable
+            && !self.cursor_ahead
             && self.state.as_ref().is_some_and(|st| st.stop.is_none())
     }
 
diff --git a/crates/overlay-cloudflare/src/chain_feed_tests.rs b/crates/overlay-cloudflare/src/chain_feed_tests.rs
index abb4a6d5..fdb1d789 100644
--- a/crates/overlay-cloudflare/src/chain_feed_tests.rs
+++ b/crates/overlay-cloudflare/src/chain_feed_tests.rs
@@ -1316,3 +1316,127 @@ async fn e620f_m3_a_stopped_or_faulting_feed_hands_arcades_listing_back_this_pas
     let s = pass(&feed, &Row::default(), &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::Unset).await;
     assert!(s.unconfigured && arcade_not_an_input(Some(&s)).is_none());
 }
+
+// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
+// W15E-L4-DELTA-LENS (2026-10-23): the delta lens's own case, built in the lens worktree against the fold's head
+// (8040093b). Not a pin of the lane; it documents the one gap the delta lens found in the fold's liveness predicate.
+// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
+
+/// A row whose every write FAULTS (D1 refused the statement: the row's own write fault, `errors`).
+struct FaultWrite(FeedState);
+impl FeedStateStore for FaultWrite {
+    async fn read(&self) -> Result<Option<(FeedState, u64)>, String> {
+        Ok(Some((self.0.clone(), 1)))
+    }
+    async fn write(
+        &self,
+        _state: &FeedState,
+        _expected_version: Option<u64>,
+        _now_ms: u64,
+    ) -> Result<Option<u64>, String> {
+        Err("D1_ERROR: the statement was refused".into())
+    }
+}
+
+/// DELTA D1: a stop SET this pass (a 409 `cursorAhead`, an envelope the crate refuses) whose write did NOT land (a
+/// lost compare-and-set, a faulted write) must hand Arcade's listing back exactly as a stop that landed does: the
+/// feed judged nothing this pass and recorded nothing. `is_reorg_word` reads `state`, the last LANDED document
+/// (without this pass's stop), and `faults`, which the two stop arms do not bump, so the gate reads such a pass as
+/// "live" and D7 is skipped for it. The control first: the same two stops over a row whose write lands (the fold's
+/// `e620f_m3`).
+#[tokio::test]
+async fn delta_d1_a_stop_whose_write_did_not_land_hands_arcades_listing_back() {
+    async fn run<C: FeedStateStore>(
+        feed: &Journal,
+        row: &C,
+        store: &MemoryPotStorage,
+        txs: &MemoryTxStore,
+        tracker: &dyn ChainTracker,
+        ladder: &CourierStub,
+    ) -> FeedPassSummary {
+        consume_chain_feed(
+            feed,
+            row,
+            store,
+            Some(txs),
+            Some(tracker),
+            Some(ladder),
+            limits(2, 50, 12),
+            &mut RootMemo::default(),
+            &Start::Unset,
+            1,
+        )
+        .await
+    }
+    let store = MemoryPotStorage::new();
+    let tracker = Roots::at(102);
+    let txs = MemoryTxStore::default();
+    let ladder = CourierStub::none();
+    for (what, feed, seed) in [
+        ("409", Journal::of(&[(41, TIP)]), "999"),
+        (
+            "unreadable envelope",
+            Journal::of(&[(41, TIP), (42, UNKNOWN_KIND), (43, REORG)]),
+            "40",
+        ),
+    ] {
+        let control = pass(
+            &feed,
+            &Row::default(),
+            &store,
+            &txs,
+            &tracker,
+            &ladder,
+            limits(2, 50, 12),
+            &Start::At(seed.into()),
+        )
+        .await;
+        assert_eq!(
+            arcade_not_an_input(Some(&control)),
+            None,
+            "{what}, the write landed: D7 is the input (e620f_m3): {control:?}"
+        );
+        let contended = run(
+            &feed,
+            &Contend(FeedState::seeded(seed, 1)),
+            &store,
+            &txs,
+            &tracker,
+            &ladder,
+        )
+        .await;
+        assert_eq!(
+            (
+                contended.contended,
+                contended.faults,
+                contended.state.as_ref().and_then(|st| st.stop.clone())
+            ),
+            (1, 0, None),
+            "{what}: the stop set this pass is not on the summary's (landed) state: {contended:?}"
+        );
+        assert_eq!(
+            arcade_not_an_input(Some(&contended)),
+            None,
+            "{what}, the write lost its compare-and-set: the feed judged nothing this pass, D7 is the input: {contended:?}"
+        );
+        let faulted = run(
+            &feed,
+            &FaultWrite(FeedState::seeded(seed, 1)),
+            &store,
+            &txs,
+            &tracker,
+            &ladder,
+        )
+        .await;
+        assert_eq!(
+            (faulted.errors, faulted.faults),
+            (1, 0),
+            "{what}: the row's write fault is counted apart from the feed's faults: {faulted:?}"
+        );
+        assert_eq!(
+            arcade_not_an_input(Some(&faulted)),
+            None,
+            "{what}, the write faulted: nothing recorded, D7 is the input: {faulted:?}"
+        );
+    }
+}
```
