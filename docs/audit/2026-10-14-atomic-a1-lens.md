# Lens on atomic admission A1 (`atomic-admission-a1` at `e5f5217`)

The lens of record on A's first PR in the engine, against the design at revision 6
(zanaadu-v2 main, `docs/audit/2026-10-13-eve/atomic-admission-design.md`, last
touched at `3950ddba`): "The API as frozen (revision 5; the binding's contract,
revision 6)", the revision 6 corrections (G1 to G7), section 3.6 and "The first PR
of A, in three lines". The lane was briefed on revision 4's three lines
(`progress/briefs/2026-10-13/eve/atomic-admission-a-pr1.md`) and built exactly
that, so most of the API differences below are the list the design itself gives
under "What A's first PR must change against revision 4's three lines". They are
not the lane's errors; they are what this PR still owes before later PRs build on
it.

Worktree `wt-overlay-atomic-a1-lens`, branch `atomic-admission-a1-lens` off
`e5f5217`. Nothing in the PR changed. REDs and mutations ran in a separate
detached scratch worktree (`wt-overlay-atomic-a1-lens-red`), removed afterwards.

## VERDICT: ACCEPT WITH CORRECTIONS

The foundation is sound: the ordinary `Engine::submit` is a mechanical refactor
(`run_validation` split into `walk_transaction` and `broadcast_validations`, no
change to its words, proven against keeper main below); the memory log's snapshot
and restore, one poll, statement latch, staged outbox and crash seams work and are
pinned; every recorded RED reproduces; the full gate is green. It is NOT mergeable
as the foundation of D31 as it stands: the frozen API differs in shape at the
walk, the admit, the runner and the SQL surface (C1 to C6), and the log path's
judgement silently diverges from today's path in two places (C7, C8). Every later
PR stands on these types, so the corrections land on this branch, not after it.

## Corrections

Severity is for this PR as the foundation of the round.

| # | sev | where | the defect | the fix |
| --- | --- | --- | --- | --- |
| C1 | HIGH | `admission.rs:88-104`, `engine/atomic.rs:16,68,80,116,131,152` | E2 missed: `walk` returns `Result<Walked, WalkAnswer>` with `WalkAnswer::EvidenceUnobtainable`, and `EvidenceSource::gather` is fallible. A host whose ARC or chaintracks is down answers 503 from the walk before the log judges: exactly test 31's named RED ("a walk that answers before the log on an ARC outage", `head_contested` turning into a 503). The walk also answers for a predecessor-read fault and for the dry judgement. | `walk(&TaggedBEEF, WalkBreachPolicy, &dyn EvidenceSource) -> Result<Walked, EngineError>`, `gather(..) -> AdmitEvidence` (infallible), delete `WalkAnswer`; read faults stay today's `EngineError` or ride as `Unavailable`; the predecessor allowance answers as today's door does (`predecessor_not_landed` from the log). |
| C2 | HIGH | `admission.rs:32-36` | `ProofVerdict` has no `Unavailable`; a header source that cannot answer has no verdict to carry, which forces C1's early answer. | Add `ProofVerdict::Unavailable { reason }`. |
| C3 | HIGH | `engine/atomic.rs:217-226`, `storage.rs:35` | E10 missed: `admit(&self, walked, log: &dyn LogTxn)` takes the log, and the engine checks identity through a new `Storage::admission_log_id()` (pointer as `usize`). | `admit(&self, &Walked)`; `Storage` gains the defaulted `fn admission_log(&self) -> Option<&dyn LogTxn> { None }`; without it, `Transient { site: "admit_without_log" }`. Drop `admission_log_id`. |
| C4 | HIGH | absent; `memory_log.rs:136-213`, `:217-304` | No `UnitRunner`, `UnitKind`, `UnitOutput`, `Committed`, `admit_graph` or `GraphOutcome`. The runner is a concrete `MemoryStorage::transaction(txid, evidence, FnOnce)`; the carried-predecessor driver is `MemoryStorage::admit`; there is no GASP finalize over a runner (test 4's shape). The executor seam PR 3 implements on the object has nothing to implement. | Add the trait and the types as frozen; implement them for the memory log; `admit_graph(engine, runner, graph)` off `Engine`, stopping at the first body that does not land. |
| C5 | HIGH | absent; `admission.rs:209-224` | No `SqlExecutor`, `SqlValue`, `SqlStmt`, `SqlRun`, `SqlRow`; `LogTxn` lacks `sql()`, `after_commit()` and `defer()` (F5); no `AfterCommit`. Managers' rows exist only as the memory store's key/value `put_admission_row`, which no D1 or object backend can implement. PR 2 cannot move Zanaadu's six managers onto "statements of the unit" without `LogTxn::sql`, and test 34 needs `batch` (E3). | Add the SQL surface and the three `LogTxn` methods as frozen; the memory log implements `sql()` over an in-memory SQLite (or a recording executor) inside the snapshot. |
| C6 | HIGH | `engine/atomic.rs:129-184`, `storage.rs` default `admission_dry_run` | No public `judge_dry`: the dry judgement and the ARC broadcast are inside `walk` for every `CurrentTx`, and `Storage::admission_dry_run` defaults to `Err`, so on any storage that does not override it (D1, Zanaadu) every `CurrentTx` walk answers `dry_judgement` not now. The design says Zanaadu never runs it. | Lift it to `Engine::judge_dry(&Walked)` for the generic host; `walk` does no judgement. |
| C7 | HIGH | `engine/atomic.rs:303-314`, `:620-633` against `engine.rs:3346-3365` | F5 missed, and a word divergence: today a manager's `Err` fails its topic (empty admittance, other topics proceed, 200); the log maps it to a whole-unit `Deterministic` (422) or `Transient` (500). The corpus pin cannot see it (no manager errs on it). | A manager `Err` fails that topic as `validate_topic` does unless the unit holds a deferral (`LogTxn::defer`), then `NotNow`. Add a two-topic pin (one manager errs) comparing the log's word with `submit`'s. |
| C8 | HIGH | `engine/atomic.rs:290` against `engine.rs:3305` | The log reads previous coins with `spent: Some(false)`; today's `validate_topic` reads `None`. A second spender of a retained coin is judged without it in the log and with it today: the two paths give different words and shadow (test 29) would count it as a true divergence. Test 5's GREEN comes from this filter, not from serialization: with `None` restored, `a1_05` fails `left: 2` (mutation run below). | Use today's `None`; rewrite test 5 to assert the second unit is judged against the first's committed rows and its word is the manager's own. If excluding spent coins is wanted, it is a separate, recorded engine change on both paths. |
| C9 | MED | `memory_log.rs:435-446`; no test | The trap arm (E6) is a rollback-and-continue on unwind, not an abort of the instance with a lost answer; nothing tests a panicking unit. | A unit that panics after one write: 0 rows, the instance poisoned (every later unit refused) and the caller sees `AnswerLost`; pin it. |
| C10 | MED | `engine/atomic.rs:327`, `memory_log.rs:225` | In the log, `unlanded_predecessor` restarts `reads` at 0 per topic and per landing attempt, and the driver allows 257 attempts: the in-log bound is not per submit (today's is, `engine.rs:2297`). | One read counter per `Walked`, carried across topics and attempts, against `PREDECESSOR_READS_PER_SUBMIT`. |
| C11 | MED | `admission.rs:76-86` | `Walked` carries the whole `TaggedBEEF` inline (no `BeefCarry`, no R2 arm above 1,900,000 bytes) plus `mode` and `could_not_run`; `carried` is `Vec<Carried>` not `Vec<String>`. | Add `BeefCarry` and `topics`; the extra fields are acceptable if the keeper keeps them. |
| C12 | LOW | `topic_manager.rs:105-107`, `lookup_service.rs:131-133` | New variants on two exhaustive public enums are a breaking change for an exhaustive matcher. None found in LOW's workers or Zanaadu's `overlay-lib` (all wildcard), and Zanaadu pins crates.io `=0.1.0`. | Name it in the keeper's minor; or `#[non_exhaustive]` both. |
| C13 | LOW | `scripts/check-admission-managers.py` | The scan's realism: it reads only files named `*topic_manager.rs` (16 production plus one integration-test file counted in its 17), drops everything after the FIRST `#[cfg(test)]`, misses imported aliases (`use worker::{Fetch}`, `Date::now()`), helper modules a manager calls, and lookup services and their storages, which DO read the platform clock inside the unit (`ship/storage.rs:166`, `slap/storage.rs:143`, `uhrp/storage.rs:164`, `agent/storage.rs:168`, `dm_delegation/storage.rs:148`, `SystemTime::now`/`js_sys::Date::now` for `created_at`). There is no runtime half. | Extend to services and their storages (time from `LogTxn::now_ms`, R6); item-level `cfg(test)`; record it as test 32's static half, not test 28. |
| C14 | LOW | sites | Site spellings differ from the frozen list: `admit_evidence_missing` (frozen `evidence_missing`; 3.6 test 31 uses the PR's), `admit_statement_failed` (`statement_failed`), `admission_scope` (`admit_without_log`), `admission_busy` (`overloaded`). | Keeper's names; pick one list. |
| C15 | LOW | record | The record should say the memory runner holds only the executor's half of the await guard (the one poll with the statement latch); the thenable check is the binding's (G3) and lands with PR 3. The engine workspace locks `worker` 0.8.5 and `wasm-bindgen` 0.2.126 against revision 6's measured 0.8.3, 0.2.127 and `worker-build` 0.8.7: test 21 must be re-run on the lock's versions when the binding lands here. | Note both. |

## The API differences

| frozen | the PR | rename or shape |
| --- | --- | --- |
| `walk(beef, policy, evidence) -> Result<Walked, EngineError>` | `walk(beef, mode, policy, source) -> Result<Walked, WalkAnswer>` | shape (E2), C1 |
| `EvidenceSource::gather -> AdmitEvidence` | `-> Result<AdmitEvidence, WalkAnswer>` | shape (E2), C1 |
| `Walked { txid, topics, beef: BeefCarry, carried: Vec<String>, walk, policy, evidence, charge }` | `{ txid, tagged_beef, mode, policy, walk, carried: Vec<Carried>, evidence, charge, could_not_run }` | shape, C11 |
| `BeefCarry` | absent | shape, C11 |
| `AdmitEvidence`, `TxEvidence`, `WalkOutcome`, `NetworkVerdict`, `NetworkSource`, `SidecarVerdict` | same | match |
| `ProofVerdict { Holds, Fails, Unavailable }` | `{ Holds, Fails }` | shape, C2 |
| `admit(&self, &Walked)` | `admit(&self, &Walked, &dyn LogTxn)` | shape (E10), C3 |
| `Storage::admission_log()` | `admission_log_id()`, `admission_dry_run()` | shape, C3, C6 |
| `judge_dry(&Walked)` | inside `walk` for `CurrentTx` | shape, C6 |
| `admit_graph(engine, runner, graph) -> GraphOutcome` | absent (`MemoryStorage::admit` lands carried bodies only) | shape, C4 |
| `UnitRunner::run_unit`, `UnitKind`, `UnitOutput`, `Committed` | `MemoryStorage::transaction(txid, evidence, run) -> Result<T, LogError>` | shape, C4 |
| `AdmitRefusal { Deterministic, Transient, NotNow }` | same three, `Transient` with notes, `NotNow` with `predecessor` | rename (superset) |
| `Admitted { steak, dedup, notes }` | `{ steak, report, seq, notes }` | rename (dedup in `report.deduped_topics`) |
| `LogTxn { sql, emit, after_commit, seq, now_ms, evidence, note, defer }` | `{ seq, now_ms, evidence -> Arc, note, notes, emit -> Result, at, fail, read_only }` | shape: `sql`, `after_commit`, `defer` absent, C5 |
| `SqlExecutor`, `SqlValue`, `SqlStmt`, `SqlRun`, `SqlRow` | absent | shape, C5 |
| `OutboxEvent { kind, envelope }` | `{ txid, topic, output_index, event_type, envelope }` | rename (superset) |
| `AfterCommit` | absent | shape, C5 |
| `TopicAdmittanceContext.unit_seq` | `admission_seq` | rename |
| `typed_unit` (the executor's entry, the binding) | not in this PR (Zanaadu/PR 3) | correctly absent, C15 |

F1 and F3: notes are a field of the unit scope, returned in `Admitted.notes` and in every refusal; the generic path's broadcaster accept rides `NetworkVerdict::Accepted` in the evidence. `grep thread_local` over the engine: none. Held.

## 1. Against the frozen API

Ran: read `admission.rs`, `engine/atomic.rs`, `memory_log.rs`, the `storage.rs`, `types.rs`, `topic_manager.rs`, `lookup_service.rs` and `engine.rs` diffs against `origin/main` (`de5cda7`). Findings C1 to C6, C11, C14 and the table above. E10 (`admit` takes no log): not met, C3.

## 2. The walk's bound and its evidence pass

Ran: read `admit` (`engine/atomic.rs:217-603`) and every callee it reaches (`judge`, `unlanded_predecessor`, `named_history`, `bounded`). The admit path awaits only storage reads and writes, manager and service hooks; no `fetch`, chain tracker, ARC or broadcaster; the clock is `log.now_ms()`. The walk does the SPV walk per carried body, the gather, the dry judgement and the ARC broadcast. Predecessor reads in the walk are bounded at 256 per submit; in the log the bound resets (C10). Missing evidence: the subject's or a carried body's absent entry gives `NotNow admit_evidence_missing` before any write of that unit, after the dedup read (a dedup answers 200). But the walk answers before the log for any gather fault (C1), which is E2's precedence inverted.

## 3. The memory log

Ran: read, plus a mutation. Snapshot and restore of every engine table, manager rows, units and outbox per unit (`MemorySnapshot`), restored on `Err`, `Pending` and unwind. One poll with `Waker::noop()`; `Pending` gives `Transient admit_awaited_io` naming the active hook, the future dropped before the restore. The statement latch: a write fault or a dry write is recorded in the scope and wins over a later verdict. Revision 6's two guards: the memory runner holds the executor's half (the poll and the latch); the thenable check is the binding's and is not here (C15). REDs: the poll guard by `a1_27` on the pre-change engine (reproduced below); the statement guard had none, so I made one: replacing the latch arm (`memory_log.rs:190`) with `(result, Some(_)) => result` fails `a1_statement_guard_catches_swallowed_manager_write` (`assertion failed: matches!(result, Err(LogError::Refused(AdmitRefusal::Transient { site, .. })) if site == "admit_statement_failed")`), 15 others pass. Outbox: rows staged in the unit (part of the snapshot), `drain_outbox` refuses inside a unit, a failed send keeps the row and counts the attempt. Lost answer: `CrashPoint::BeforeUnit` and `AfterCommit`; the replay deduplicates on the applied row (txid, topic), one unit, one head (`a1_lost_answer_is_ambiguous_and_replay_deduplicates`); (txid, topic, vout) is implied because a topic's outputs land in one unit. Fault switch: `fault_write(Some(n))` on every engine, manager and outbox write. Trap arm: C9.

## 4. Submit parity

Ran: `cargo test -p overlay-integration-tests --test atomic_legacy_parity`: 2 passed. Then independently on keeper main `de5cda7` (the first test copied, `ATOMIC_PARITY_CAPTURE` set): the 66 words main produces are byte-identical (`cmp`) to the PR's frozen `atomic_legacy_words.json`. Shape: 11 inputs (5 parity-harness submit bodies, 6 production SHIP/SLAP wire bodies), 3 modes, first and replay = 66 words; it compares the STEAK or the error string, nothing of the store. Its realism: 54 words are `ok`, and every one of them admits ZERO outputs (the SHIP and SLAP managers admit nothing on these bodies); there are no lookup services, no chain tracker and no broadcaster. So the pin proves the walk's errors and the empty judgement are unchanged, and `log_corpus_completes_in_one_poll_with_the_same_words` proves nothing about the log's mutation half (C7 and C8 pass it). By inspection of the diff, `Engine::submit`'s path is a pure extraction (`walk_transaction`, `broadcast_validations` returning the ARC txid, `named_history(.., None)`, `unlanded_predecessor(.., None)`): no path bypasses it and none of its writes changed. Held for `submit`; not evidence for `admit`.

## 5. The tests

REDs reproduced by hand (scratch worktree, the pin commits):

- `b933f59`, `cargo test -p bsv-overlay-engine --features memory-storage --lib e657`: 0 passed, 2 failed. `bsv-low #657: refused submit changed stored bytes (before 7495, after 18952)`; `assertion left == right failed: bsv-low #657: identical bytes changed the word`.
- `0cd2e22`, `--lib a1_`: 0 passed, 6 failed: `a1_03 write 1 left rows`, `a1_05 both spends admitted against one retained coin`, `a1_07 refused successor changed predecessor unit`, `a1_11 applied fault left output and index`, `a1_13 event escaped before outbox drain`, `a1_27 awaited promise committed rows: Ok(...)`.
- Mutations at `e5f5217`: the statement latch removed fails the statement-guard pin (section 3); `Some(false)` at `engine/atomic.rs:290` set back to today's `None` fails `a1_05` with `left: 2` (C8).

Against 3.6: 1, 2, 3, 5, 7, 11, 13 and 27 are present (5 green for the wrong reason, C8; 27 over SHIP/SLAP only, Zanaadu's shapes are its twin's). 28 is not here and cannot be: it is the writer-seam scan over `overlay-lib/src`; the engine's scan is test 32's static half over this repository's managers (C13). The first PR's list at revision 5 adds 31, 32, 33 and 34: none here (31 to 33 run in Zanaadu with the evidence readers; 34 needs the `SqlExecutor`, C5). 12's shape is present (`a1_lost_answer_..`); 4 and 6 are not (no `admit_graph`, no `AfterCommit`).

## 6. The release

Ran, `CARGO_TARGET_DIR=/Volumes/Lanes/targets/engine-atomic-a1-lens`, `RUST_TEST_THREADS=1`, `LANE_BASE=27431`, `make ci`'s steps in order (split under the foreground cap):

- config-ids and storage-ownership checks with self-tests: ok; admission manager scan: 17 managers, 0 violations.
- engine alone: 485 passed, 0 failed, 63 ignored (plus 2 ignored doctests).
- root workspace: 1,644 passed, 0 failed, 66 ignored, exit 0.
- workers workspace: 1,565 passed, 0 failed, 1 ignored, exit 0.
- clippy root and workers, `--all-targets -D warnings`: exit 0 both.
- `cargo fmt --all --check`: exit 0; `rustfmt --check` on each of the 12 changed `.rs` files: clean.
- wasm32: engine plus discovery, overlay release, app layer release: exit 0 all three.
- `make ci-deploy`: preflight ok (`worker` 0.8.5), the three dry runs ok.
- `make ci-d1-budget` then `make -o ci-d1-budget ci-route`: exit 0 both.

No `Cargo.toml`, `Cargo.lock` or `wrangler*.toml` changed; crate versions unchanged; `script_door.rs`, `stream_sizing.rs` and `beef_limits.rs` (e592) unchanged. Additive for consumers: `Storage` gains two defaulted methods; `TopicAdmittanceContext` is `#[non_exhaustive]`, so its field is not breaking; the UHRP edit is behaviour-neutral (on main `validate_uhrp_output` already ignored `now`, `let _ = now;`), so only an unused clock read went; the two enum variants are C12.

## 7. What the second PR needs from this one

- Zanaadu's managers onto the evidence: `LogTxn::evidence()` and the evidence types are present; `ProofVerdict::Unavailable` (C2), `LogTxn::defer` (C5, F5) and the manager-`Err` rule (C7) are missing, so `EvidenceProofVerifier` and `EvidenceTxStatusChecker` cannot answer "not now" as frozen.
- The managers' staged rows: missing. No `LogTxn::sql`, no `SqlExecutor` with `batch` (C5); `put_admission_row` is memory-only.
- The emitter: `LogTxn::emit` present (returns `Result`, frozen `()`); `OutboxEvent` carries more keys than frozen.
- The clock: `LogTxn::now_ms` present.
- Dry judgements on a read-only executor: `read_only()` present for nested questions; the generic `judge_dry` is buried in `walk` and breaks every non-memory storage (C6).
- The D1 materializer: needs the SQL surface (C5) and `after_commit` (C5); the memory log's dirty-key and cursor model is not here (PR 2's by the design, fine).
- The executor seam: no `UnitRunner` to implement (C4); `typed_unit` maps onto `run_unit`, which does not exist yet.
- The walk's evidence pass: `EvidenceSource` present but fallible (C1).
