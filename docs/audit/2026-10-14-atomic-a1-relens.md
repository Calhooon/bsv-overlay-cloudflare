# Re-lens on atomic admission A1 after its eleven corrections (`atomic-admission-a1` at `80e47c8`)

The scoped re-lens of draft PR #43 after the fix lane (`e5cd32a` to `80e47c8`, C1 to C11), against
the first lens (`docs/audit/2026-10-14-atomic-a1-lens.md`, origin `atomic-admission-a1-lens` at
`89bcf8c`), the fix brief (zanaadu-v2 main `progress/briefs/2026-10-13/eve/atomic-admission-a1-fix.md`)
and the design at revision 6 (zanaadu-v2 main `docs/audit/2026-10-13-eve/atomic-admission-design.md`,
"The API as frozen", the trap rule in 3.4 and 3.5).

Worktree `wt-overlay-atomic-a1-relens`, branch `atomic-admission-a1-relens` off `80e47c8`.
`CARGO_TARGET_DIR=/Volumes/Lanes/targets/engine-atomic-a1-lens`, `RUST_TEST_THREADS=1`,
`LANE_BASE=31847`. Nothing in the PR changed. The RED reversions and two probe tests were made by
hand in this worktree and reverted with `git checkout --` (tree clean after each); none is committed.

## VERDICT: ACCEPT WITH CORRECTIONS

C1 to C11 are each taken in the shape the lens asked; the C7 and C8 REDs reproduce exactly as
recorded; submit parity holds (no change to `Engine::submit`'s path since the first lens proved it
byte-identical to keeper main); every gate is green at the PR's counts. Two new HIGH defects sit in the
corrected code, both found by a probe test that fails at `80e47c8`: the C7 correction covers one manager
hook and misses the other (N1), and the C11 R2 arm silently persists a proof-stripped BEEF (N2). Both
land on this branch before later PRs build on the walk and the unit.

## Corrections still owed

| # | sev | where | the defect | the fix |
| --- | --- | --- | --- | --- |
| N1 | HIGH | `engine/atomic.rs:310-319`; `engine.rs:2994-3004` (reached from the log via `unlanded_predecessor(.., Some(log))`, `engine.rs:2905`, `:2917`) | F5 on the second manager hook. A manager whose `identify_needed_inputs` returns `Err` FAILS its topic in the log (empty admittance, no write); on `submit` the same `Err` names nothing (`named_history`, "an `Err` or no answer names nothing") and the topic admits. Probe (two topics, the erring manager admits output 1, no stored coin): submit `{"tm_needs_err":{"outputsToAdmit":[1],...},"tm_proof_657":{"outputsToAdmit":[2],...}}`, log `{"tm_needs_err":{"outputsToAdmit":[],"coinsToRetain":[]},"tm_proof_657":{"outputsToAdmit":[2],...}}`. By inspection, a second arm: for a carried predecessor's history `named_history(.., Some(log))` calls `log.fail`, so the same manager `Err` turns the whole unit into Transient `admit_statement_failed` where submit warns and names nothing. Shadow (test 29) would count both as true divergences. | On `identify_needed_inputs` `Err`: honor a deferral (`NotNow`) if the unit holds one, otherwise name nothing and go on exactly as `named_history` does; do not `log.fail` a manager's `Err` in `named_history` (keep `fail` for storage faults). Pin: the N1 probe (word equal to submit's), plus a carried-predecessor variant. |
| N2 | HIGH | `admission.rs:401-417` (`Walked::tagged_beef`, `merge_raw_tx(raw, None)`); `engine/atomic.rs:447` (`beef: Some(tagged_beef.beef.clone())`) | For `BeefCarry::R2` the log judges AND PERSISTS a BEEF rebuilt from `raw_txs` with no BUMPs: every output row's `beef` loses the subject's (and every ancestor's) merkle proof. Probe (the C11 shape, 1,900,130 bytes, topic admits output 2): stored 1,900,128 bytes, `stored subject has proof: false`, not equal to the original; `submit` of the same bytes stores the original (`equal: true`). The same stripped bytes reach `identify_needed_inputs` and `WholeTx` lookup payloads. The struct's own comment says "resolving that pointer must precede the unit"; the code reconstructs instead. Silent, durable, and only above the 1,900,000 cap, so no existing test reaches it. | Either the host resolves the R2 original before the unit (verified against `sha256` and `len`) and `admit` judges and persists those bytes, or the output row records the R2 descriptor rather than rebuilt bytes; until one lands, `admit` refuses an R2 carry (Transient, named site) rather than strip proofs. Pin: the N2 probe (stored bytes equal to submit's). |
| N3 | LOW | `engine/atomic.rs:126-140` | `charge` is the evidence's serialized size only; the frozen comment is "the walk's ledger, the evidence included". A host charging the object's ledger from `Walked.charge` undercounts the walk. | Add the walk's e592 ledger total, or record the narrower meaning as the keeper's. |
| N4 | LOW | `memory_log.rs:639-647` | `run_unit` opens every unit with txid `""` (so `MemoryUnit.txid` is empty for every runner unit, `admit_graph` included), and `Committed.seq` is `log_head()`, which for a unit that wrote nothing (a dedup) is the PREVIOUS unit's seq while that unit's `Admitted.seq` is head + 1. | Pass the walked txid through (or drop the field for runner units); answer the unit's own seq, or `seq` only when the unit wrote. |
| N5 | LOW | `admission.rs:71-83` | `Walked` carries three fields beyond the frozen eight (`off_chain_values`, `mode`, `could_not_run`) and a non-wire `predecessor_reads: Rc<Cell<usize>>` (`serde(skip)`): a `clone()` SHARES the allowance (`a1_c4` builds a graph from clones), a hop resets it. `off_chain_values` is needed (the frozen struct has no home for them; a design gap, not the PR's). | Keeper keeps or drops the three (the lens allowed it); document that clones share the allowance, or reset it in `Clone`. |

C12 (the two exhaustive enums, `TopicManagerError::Admission`, `LookupServiceError::Admission`, still
not `#[non_exhaustive]`) and C14 (site spellings) remain the keeper's two decisions; the table below is
for both. C13 is not part of this fix set and the record says so (the scan is test 32's limited static
half). C15 is taken in the record (the executor's half of the await guard; the lock's `worker` 0.8.5
and `wasm-bindgen` 0.2.126; test 21 to be re-run on them).

A design observation, not a correction of the PR: on the C11 shape the R2 arm's `raw_txs` are
1,900,128 bytes for a 1,900,130-byte BEEF, so for one large transaction the frozen R2 arm bounds the hop
by nothing (proofs are the only bytes it drops); and `serde_json` writes each `Vec<u8>` as a number
array (about 3.6 times the bytes). The design's hop cap needs the binary encoding and the R2 arm's
purpose stated.

## 1. C1 to C11 against the frozen API

Ran: read `admission.rs`, `engine/atomic.rs`, `memory_log.rs`, `storage.rs` and the C1 to C11 tests
at `80e47c8`; `grep` for `WalkAnswer`, `EvidenceUnobtainable`, `admission_log_id` over `crates/`: none.

| # | the lens asked | at `80e47c8` | held |
| --- | --- | --- | --- |
| C1 | `walk(beef, policy, evidence) -> Result<Walked, EngineError>`, infallible `gather`, no `WalkAnswer` | exactly; the walk's errors are `walk_transaction`'s and storage reads' `EngineError`; `a1_c1` shows an `Unavailable` ARC verdict reaching the log, whose deterministic refusal wins | yes |
| C2 | `ProofVerdict::Unavailable { reason }` | present, serde roundtrip pinned | yes |
| C3 | `admit(&self, &Walked)`, defaulted `Storage::admission_log()`, `admit_without_log` | exactly; memory answers `Some` only while a unit is open | yes |
| C4 | `UnitRunner`, `UnitKind`, `UnitOutput`, `Committed`, free `admit_graph`, `GraphOutcome` | exactly as frozen; memory implements `run_unit`; `admit_graph` stops at the first refusal (`landed == [1]`, third body untouched) | yes (N4) |
| C5 | `SqlExecutor` with `batch`, the SQL types, `LogTxn::{sql, after_commit, defer}`, `AfterCommit` | all present; memory is the recording executor the lens allowed (records in the snapshot, empty reads, `batch` truncates its records on a fault and the latch refuses the unit) | yes |
| C6 | `Engine::judge_dry(&Walked)`; `walk` judges nothing | exactly; `a1_current_walk_and_admit_never_broadcast` (0 broadcaster calls in walk and admit); `judge_dry` takes its read-only scope from the extra `Storage::admission_dry_run` (defaults to `Err`, so a host must supply it) | yes |
| C7 | a manager `Err` fails its topic unless deferred; a two-topic pin vs submit | done for `identify_admissible_outputs`, pinned; NOT for `identify_needed_inputs` | partly (N1) |
| C8 | `spent: None` as today; test 5 rewritten | done; `a1_05` rewritten, `a1_c8` new | yes |
| C9 | a panicking unit: 0 rows, instance poisoned, `AnswerLost` | done; see below | yes |
| C10 | one read counter per `Walked` across topics and attempts | `Walked.predecessor_reads`, shared by carried units; `a1_c10` exhausts 256 over 130 topics and two attempts | yes (N5) |
| C11 | `BeefCarry`, `topics`, `carried: Vec<String>` | present; inline at or below 1,900,000, R2 above with hex sha256 key, hash, length, raw txs | shape yes; persistence N2 |

**C7 RED, reproduced.** Reverted the hunk by hand (the `let Ok(admittance) = .. else { deferral ..;
continue }` arm back to `.map_err(|e| manager_refusal(topic, e))?` with the deleted `manager_refusal`
restored), `cargo test -p bsv-overlay-engine --features memory-storage --lib a1_c7`:
`called Result::unwrap() on an Err value: Refused(Transient { site: "topic_manager.identify_admissible_outputs", reason: "manager error", notes: [] })`,
0 passed, 1 failed. Restored: 1 passed. Matches the record's line.

**C8 RED, reproduced.** Reverted `find_output(.., Some(topic), None, false)` to `Some(false)` by hand,
`--lib -- a1_c8 a1_05`: `a1_c8`: `assertion left == right failed: spent-coin judgement must match submit`
(left, submit: the second spender admits `[0]` retaining `[0]`; right, log: `outputsToAdmit: []`,
`coinsToRetain: []`); `a1_05`: `a1_05 retained spent coins remain visible to the manager`, `left: 1`,
`right: 2`. 0 passed, 2 failed. Restored: green. Matches the record.

**C9, the trap rule.** Revision 6: a panic in the unit is a trap; the transaction rolls back; the
instance is poisoned and aborted; the caller sees a lost answer; `not_durable` is the Worker's word for
the failed hop and the JavaScript shim's concern (`guardedTransaction`, `ctx.abort`). The memory log:
`transaction` catches the unwind around the one poll, sets `poisoned`, and returns
`LogError::AnswerLost`; the `MemoryRollback` guard restores the snapshot and clears the scope (0 rows,
head 0); every later `transaction` answers `AnswerLost` before opening a unit (a memory instance cannot
restart, so it stays aborted); `run_unit`, whose frozen signature has no lost-answer arm, re-raises it as
an unwind carrying `LogError::AnswerLost`, never as an `AdmitRefusal`. `grep not_durable|admit_trapped`
over the engine crate outside `engine.rs`: none. The contract matches: no HTTP word, no `not_durable`, no
refusal in the memory log; the lost answer is at the transport boundary.

## 2. Submit parity

Ran: `cargo test -p overlay-integration-tests --test atomic_legacy_parity`: 2 passed
(`ordinary_submit_words_are_byte_identical_to_keeper_main`, `log_corpus_completes_in_one_poll_with_the_same_words`).
`git diff e5f5217 80e47c8` touches no file of `Engine::submit`'s path: `engine.rs` unchanged, the frozen
`atomic_legacy_words.json` and inputs unchanged (the parity test's only change is `walk` to
`walk_with_mode`, the log half); `storage.rs` swaps `admission_log_id` for `admission_log`, which
`engine.rs` never calls. C8 changed only the log's read (`engine/atomic.rs`). So the first lens's proof
(66 words from keeper main `de5cda7` `cmp`-identical to the frozen file) still covers this tip: submit is
byte-identical. The corpus still proves nothing about the log's mutation half (its 54 `ok` words admit
zero outputs), which is why N1 and N2 needed probes.

## 3. The wire (C11)

`Walked` serializes `txid, topics, beef, off_chain_values, mode, policy, walk, carried, evidence,
charge, could_not_run`; the frozen eight are all present with the frozen types (`beef: BeefCarry`,
`carried: Vec<String>`); three extras and the skipped counter (N5); no `tagged_beef` key (pinned).
`INLINE_BEEF_MAX_BYTES = 1_900_000`, inline when `len <= cap`. `R2 { key: hex(sha256), sha256, len,
raw_txs }` matches the frozen arm; `a1_c11` pins key, hash, length, that the subject is among `raw_txs`,
and the JSON roundtrip. The predecessor ids: `carried` is the walk's postorder (each predecessor before
its spender), proven predecessors not descended, read-bounded at 256 in the walk. What the R2 arm does
to history is N2.

## 4. Nothing regressed

Ran `make ci`'s steps in order, split under the foreground cap (not as one invocation):

- config-ids, storage-ownership (38 self-tests, 933 references) and the admission manager scan (17 managers, 0 violations): ok.
- engine alone, `cargo test -p bsv-overlay-engine --features memory-storage`: 496 passed, 0 failed, 63 ignored, plus 2 ignored doctests (the PR: 496).
- root workspace, `--no-fail-fast`: 1,655 passed, 0 failed, 66 ignored, exit 0 (the PR: 1,655).
- workers workspace: 1,565 passed, 0 failed, 1 ignored, exit 0 (the PR: 1,565).
- clippy root and workers, `--all-targets -D warnings`: exit 0 both.
- `cargo fmt --all --check`: exit 0; each of the 12 `.rs` files the PR changes against `de5cda7`, `rustfmt --edition 2021 --check` over stdin: 0 `Diff in` each.
- wasm32: engine plus discovery, overlay release, app layer release: exit 0 all three.
- `make ci-deploy`: preflight ok (`worker` 0.8.5, 3 configs), the three dry runs ok.
- `make ci-d1-budget`, then `make -o ci-d1-budget ci-route`: exit 0 both, 0 `FAIL` lines.
- e592: `script_door.rs`, `stream_sizing.rs`, `beef_limits.rs` identical to `e5f5217` and to main `de5cda7`; no `Cargo.toml`, `Cargo.lock`, `workers/Cargo.lock` or `wrangler*.toml` changed; crate versions unchanged.

## 5. The names table (C12 and C14, the keeper's one pass)

Every public name the PR introduces, against the frozen name. "same" means the frozen name and shape;
the keeper renames in this table only.

| # | frozen | the PR | note |
| --- | --- | --- | --- |
| 1 | (module) | `bsv_overlay_engine::admission` | new public module |
| 2 | `Engine::walk` | `Engine::walk` | same |
| 3 | (none) | `Engine::walk_with_mode` | extra: explicit `SubmitMode` for the corpus and verified GASP bodies |
| 4 | `Engine::admit` | `Engine::admit` | same |
| 5 | `Engine::judge_dry` | `Engine::judge_dry` | same |
| 6 | `admit_graph` | `admission::admit_graph` | same |
| 7 | `Walked` | `Walked` | + `off_chain_values`, `mode`, `could_not_run`, `predecessor_reads` (N5) |
| 8 | `BeefCarry::{Inline, R2}` | same | same |
| 9 | `INLINE_BEEF_MAX_BYTES` | same | same |
| 10 | `WalkOutcome::{Spv, HistoricalTxNoSpv}` | same | same |
| 11 | `AdmitEvidence`, `TxEvidence` | same | same |
| 12 | `NetworkVerdict::{Accepted, Refused, Unavailable}` | same | same |
| 13 | `NetworkSource::{Arc, Broadcaster}` | same | same |
| 14 | `ProofVerdict::{Holds, Fails, Unavailable}` | same | same |
| 15 | `SidecarVerdict::{Stored, NotStored}` | same | same |
| 16 | `EvidenceSource::gather` | same | same |
| 17 | (none) | `NoEvidence` | extra: the empty source |
| 18 | `LogTxn::{sql, emit, after_commit, seq, now_ms, evidence, note, defer}` | same eight | `evidence` returns `Arc<AdmitEvidence>`, not `&` |
| 19 | (none) | `LogTxn::{deferral, notes, at, fail, read_only}` | extras |
| 20 | `Storage::admission_log` | same | same |
| 21 | (a read-only executor) | `Storage::admission_dry_run`, `DryRunScope::failure` | extra: how `judge_dry` gets its read-only scope |
| 22 | `UnitRunner::run_unit` | same | same |
| 23 | `UnitKind::{Admit, Proof, Evict, Unevict, Readmit, Gc, Renotify}` | same | same |
| 24 | `UnitOutput::{Admitted, Done}` | same | same |
| 25 | `Committed { seq, output, notes }` | same | same (N4) |
| 26 | `Admitted { steak, dedup, notes }` | + `report`, `seq` | superset |
| 27 | `GraphOutcome::{AllLanded, StoppedAt}` | same | same |
| 28 | `AdmitRefusal::{Deterministic, Transient, NotNow}` | same three | `Transient` + `notes`; `NotNow` + `predecessor` |
| 29 | (none) | `AdmitRefusal::{transient, not_now, with_notes, notes}` | helper fns |
| 30 | `LookupRefusal` (named in `admit`'s contract) | `LookupRefusal::{Deterministic, Transient}` | same |
| 31 | `RefusalNote` | `RefusalNote { kind, reason }`, `RefusalNotes` | alias extra |
| 32 | `SqlValue`, `SqlStmt`, `SqlRun`, `SqlRow`, `SqlExecutor::{run, all, batch}` | same | same |
| 33 | `OutboxEvent { kind, envelope }` | `{ txid, topic, output_index, event_type, envelope }` | `kind` is `event_type`; superset |
| 34 | `AfterCommit::R2Put` | same | same |
| 35 | `TopicAdmittanceContext.unit_seq` | `admission_seq` | rename |
| 36 | (none) | `LogError::{Refused, AnswerLost}` | extra: the memory transport's lost answer |
| 37 | (none) | `LocalBoxFuture` | extra alias |
| 38 | (C12) | `TopicManagerError::Admission`, `LookupServiceError::Admission` | new variants on exhaustive enums: the minor, or `#[non_exhaustive]` |
| 39 | site `evidence_missing` | `admit_evidence_missing` | C14 |
| 40 | site `statement_failed` | `admit_statement_failed` | C14 |
| 41 | site `overloaded` | `admission_busy` | C14 |
| 42 | sites `predecessor_not_landed`, `admit_awaited_io`, `admit_without_log` | same | same |
| 43 | sites `evidence_unavailable`, `log_full`, `pf_cas_in_log` | not minted by the engine | the first is a manager's `defer` site; the others are the object's and Zanaadu's |
| 44 | (none) | Transient sites `walked_payload`, `carried_payload`, `dry_judgement`, `topic`, the storage call names, `lookup_service.<hook>` | extras: name them or fold into `statement_failed` |
| 45 | (none) | memory surface: `CrashPoint`, `OutboxRow`, `MemoryUnit`, `MemoryStorage::{transaction, admit, crash_at, log_head, log_units, outbox, awaited_io, put_admission_row, admission_row, drain_outbox, sql_statements, after_commit_intents, fault_write}` | test and memory-host surface |

45 rows: 23 the same, 1 rename (35), 4 C14 site rows (39 to 41, 43), 1 C12 row (38), 16 extras or
supersets.

## What was not verified

Hosted behaviour (test 21's arms, the binding, the object) is not in this PR and was not run. N1's
second arm (the carried predecessor's `named_history` `log.fail`) is by inspection, not by a test. `make
ci` was run step by step, not as one invocation.
