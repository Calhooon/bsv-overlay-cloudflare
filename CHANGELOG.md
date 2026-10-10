# Changelog

## Unreleased: a corroboration of any number of legs is read one leg at a time, never refused for its count (NL-6d, 2026-10-10)

The posture, the charter's (bsv-stack-lean `docs/charters/beef-of-any-size.md`,
issue #60): **never a count refusal of what the network accepts; where one
request cannot finish the work, it is deferred and resumed, never restarted.**

- **The leg cap is gone.** The #267 ancestry-primed corroboration refused a
  batch of more than 32 EF legs before any submit (`MAX_CORROBORATION_LEGS`,
  `broadcaster.rs`): inconclusive, 502, nothing admitted; a job the NL-6c
  deferral had queued answered the same 502 on each run and ended `failed`
  after five. The constant and its refusal are gone.
- **One leg at a time, in windows.** The corroboration posts each ancestor to
  the corroborating host in ancestry order from a cursor, at most
  `IN_REQUEST_CORROBORATION_LEGS` (32, the #267 sizing of a request's serial
  POSTs, now a routing number) in the request and `RUN_CORROBORATION_LEGS`
  (256) in each consumer run. Only the subject's verdict after every ancestor
  was primed decides; a walk that pauses never posts the deciding attempt, so a
  partial corroboration cannot admit.
- **Deferred past the window, resumed from the leg reached.** A walk with legs
  left in the request is deferred as NL-6c defers: 202 with a reference and the
  poll path, the body naming `legsFrom` and the leg budget. In a run it answers
  202 `{resumeAt}`; the consumer records the cursor (`ef_deferred_jobs.legs_from`,
  migration 179, forward only), clears the run's attempt (a run that advanced
  is no failed run) and hands the job straight back to the queue. The poll
  serves `legsFrom`. A run resumed mid-walk does not push the #413
  dual-broadcast legs again.
- **A switch for the route tier:** `CORROBORATOR_URL` routes both corroborating
  hosts (TAAL, then GorillaPool) to one base, so the CI witness posts nothing
  to a real host. Unset in production: the hosts are as before.
- **The witness** (`tools/lane-nl6d`, a leg of `make ci-route` on the NL-6c
  worker): 33 legs answered 200 in the request; 1,000 legs answered 202, done
  200 after the request and four runs, each of the 999 ancestors posted once;
  40 legs past the byte budget: every ancestor once and the subject accepted.
  Red at the witness commit: 502 for 33 and 1,000 legs, the deferred job's
  run 502 again.
- **The ceilings not routed past, named.** One leg is one POST: an ancestor the
  corroborating host cannot answer inside one invocation is the grain this does
  not divide. The EF batch is still held whole in memory (NL-6c's ceiling: one
  isolate, 128 MB). After the corroboration, an admission the engine's landing
  guard calls "not now" (an unproven chain deeper than its 16 predecessor
  reads) is replayed through the mutation queue with its bytes in the message;
  a BEEF over `QUEUE_BEEF_SIZE_LIMIT` (90,000 B) cannot be queued and the arm
  answers 502 "admission not durable", in the request and on every run of a
  deferred job. That bound is the S2 replay's, not the count's; it is left as
  found. So is the rebroadcast backstop's own leg cap (`REBROADCAST_MAX_LEGS`,
  32), a skip, not an answer to a caller.
- **The cost, stated:** the corroborating work is linear in the legs handed in,
  and past the request's window it runs on the queue: a body of N unproven legs
  costs about N corroborator POSTs and N / 256 consumer runs, each re-running
  the ladder up to the corroboration. #267 bounded that work by refusing; the
  posture bounds it by the bytes the caller sent.

## Unreleased: bsv-rs 0.4.3; a transaction with no output is invalid bytes (NL-6f, 2026-10-10)

- **bsv-rs #59.** Both workspaces pin bsv-rs **0.4.3** (the workers' lock keeps
  0.3.35 for the middleware bridge). The reader, and with it every door of this
  workspace that reads a BEEF (`read_beef`, `fold_beef`, `parse_beef`,
  `transaction_from_beef`, the census), refuses a transaction with no output as
  `invalid BEEF at byte <offset>: NoOutputs`, at the transaction's first byte,
  as it refuses one with no input since 0.4.2. `Beef::verify_valid` and
  `Transaction::verify` refuse both shapes too, as the TypeScript reference's
  `Transaction.verify` does.
- **The one fixture it moved:** the census cell
  `a_poisoned_subject_sort_is_uneval_never_a_green` built its real subject with
  two inputs and no output, so the census answered `Parse` before the EF step
  the cell is about. The subject now pays one output and the verdict is
  `SubjectEf` again. No other test moved.

## Unreleased: bsv-rs 0.4.2; the SPV walk is linear in a BUMP's leaves (NL-6e, 2026-10-10)

The no-limits program's replay (bsv-stack-lean NL-8) named two findings at the
engine; both close here.

- **W6, a transaction with no input.** Both workspaces pin bsv-rs **0.4.2** (the
  workers' lock keeps 0.3.35 for the middleware bridge). Every door
  (`read_beef`, `fold_beef`, `parse_beef` at each of the ten doors,
  `transaction_from_beef`) refuses a transaction with no input as
  `invalid BEEF at byte <offset>: NoInputs`, at the transaction's first byte;
  discovery's `pot_beef_has_proof` no longer reads a proven transaction with no
  input as proven. At 0.4.0 all of them read the three no-input rows.
- **W1, the walk's time.** The escape hatch's structural walk
  (`verify_scripts` false) is the streaming reader's structure check,
  `beef_limits::structure_roots` (bsv-rs 0.4.2 `verify_stream_structure`): one
  element in hand, each BUMP walked once, each distinct height and root asked of
  the tracker after. It was `Beef::verify_valid`, whose root check at 0.4.0
  walked a BUMP once per leaf. The script walk (the default, and the script
  door's budgeted walk) computes and asks each BUMP's root once; it did so per
  proven transaction. Measured at the entry (`verify_spv_like_the_reference`,
  release): one BUMP of 4,096 and 8,192 leaves 45.9 s and 157.5 s before,
  0.006 s and 0.012 s now; 4,096 and 8,192 proven parents in one BUMP under the
  script walk 41.0 s and 127.6 s before, 0.088 s and 0.174 s now.
- **What the structural walk now answers otherwise than at 0.4.0:** it refuses
  an input naming a transaction written after it (the reader reads the wire's
  order; the in-memory check sorted first), and an atomic BEEF whose subject
  is not its last transaction or that carries a transaction its subject does
  not descend from; it accepts a txid-only entry a BUMP of the BEEF proves,
  where it refused every txid-only entry. Each refusal names the offset and
  the kind.
- **For callers of bsv-rs through this workspace:** 0.4.2's `Beef::from_binary`
  refuses a byte after the frame, and `verify_valid`'s `allow_txid_only`
  decides nothing (bsv-rs's CHANGELOG). Test fixtures that built a
  transaction with no input now spend an outpoint of the all-zero txid; the
  pinned digests they feed were re-frozen on main and on the bump alike. The
  census's parity fixture (the TS client's, regenerated from the bsv-low
  checkout) still carries a funded parent with no input, which the census now
  refuses at the parse; its emitter gives every source an input for the next
  regeneration.

## Unreleased: the EF work bound is a deferral, not a refusal (NL-6c, 2026-10-09)

The posture, the same charter's: **a valid submission is never refused for its
size; where the platform has a bound (here one request's CPU slice) the design
routes around it, with the bytes at rest and the remaining work resumable.**

- **The 429 is gone.** The broadcast-gated arm of `/submit` answered 429 "EF
  too large ... retry via fallback" when the subject's Extended Format passed
  256 KiB or the batch's passed 2 MiB (`MAX_SUBJECT_EF_BYTES`,
  `MAX_BATCH_EF_BYTES`, `subject_ef_over_cap` in `routes.rs`, #211/#209). All
  three are gone.
- **A work budget, routed past.** The request keeps a budget of the same two
  numbers (`ef_deferred::IN_REQUEST_SUBJECT_EF_BYTES`,
  `IN_REQUEST_BATCH_EF_BYTES`). Work within it is done in the request and
  answered as before. Past it the submission's bytes go to rest (R2 under
  `ef-deferred/` when the `BEEF_BLOBS` binding exists, else D1 in chunks of
  1,000,000 bytes, under D1's 2,000,000-byte row), the mutation queue carries
  the job's reference (never the bytes, so the queue's 128 KB message is no
  bound), and the caller is answered **202** with
  `{"accepted": true, "deferred": true, "reference", "poll", "subjectTxid",
  "work", "budget"}` and a `Location` header.
- **The queue consumer finishes the work** by running the same arm (the
  route's body is now `submit_parts`, under `WorkBudget::Resumed`), and the
  job keeps the arm's answer: **`GET /submit-deferred/<reference>`** serves
  `{state: queued | running | done | failed, attempts, answer: {status,
  body}}`, where `body` is the STEAK or the refusal the request would have
  answered. A run that does not settle (the invocation ended, a 5xx or a 429
  answer) is handed back to the queue by the cron after 10 quiet minutes, up
  to 5 runs; a settled job and its bytes are swept after 7 days. The same
  submission sent again while its job is open names the same reference.
- **The ceiling not routed past, named:** the request still reads its body
  whole and converts it to EF before it knows the work's size, and the
  consumer holds the bytes whole to run the arm; one submission is bounded by
  one isolate's memory (128 MB) and the plan's request-body limit (100 MB on
  the Free and Pro plans). Resumption is at the job's grain: a step that
  cannot finish in one consumer invocation fails its 5 runs and the job ends
  `failed` with its last answer.
- **For clients:** a broadcast-gated submit can now answer 202 instead of 200.
  A 202 body is not a STEAK; poll the reference. Nothing that answered 200
  before answers otherwise.
- **For operators:** migrations 175 to 178 (`ef_deferred_jobs`, its state
  index, `ef_deferred_chunks`), applied on boot. The cron (`*/15`) is the
  hand-back; with no cron a job whose first run does not settle waits.
  `DUAL_BROADCAST=off` turns off the post-response TAAL/GorillaPool push (for
  a worker whose broadcaster is a fixture; unset, it is on).
- **The census** no longer counts a body past the budget as `would-fail`; its
  `efOverCap` reason is no longer produced and its counter stays served.
- **The witness:** `tools/lane-nl6c/ef_work_bound_ci.mjs` in `make ci-route`
  (its own worker at `LANE_BASE+11`, a fixture Arcade at `+12`, nothing sent
  to a real host): a 300,140-byte body over the subject budget and a
  2,200,282-byte body over the batch budget, each 202 with a reference that
  reaches `done` with the arm's 200 after the consumer asked the broadcaster.

## Unreleased: a BEEF of any size (NL-6, 2026-10-09)

The posture, from the charter "a BEEF of any size" (bsv-stack-lean
`docs/charters/beef-of-any-size.md`; the Lean definition is
`lean/BeefOfAnySize.lean` there): **a valid BEEF is never refused for its size
or its counts. A refusal is for invalid bytes only, and names them: the offset
of the byte and the kind.** This reverses the verdict by size that P0-5f put
on the engine's doors; what P0-5f fixed underneath (the linear BUMP walk, the
linear parse) stays, in bsv-rs.

- **bsv-rs 0.4.0** in both workspaces. The workers' lock also holds bsv-rs
  0.3.35, because bsv-middleware-cloudflare 0.4.1 and bsv-middleware-core
  0.1.0 are built on it; the `bsv-rs-03` dependency of `overlay-cloudflare`
  bridges the one value that crosses (the wallet handed to
  `WorkerStorageClient` in `wallet/client.rs`, built from the admin key's 32
  bytes). It leaves when the middleware moves to 0.4.
- **`overlay_engine::beef_limits` is the streaming door.** It keeps its name
  and its constants as names; no function in it refuses a BEEF by size or
  count. `fold_beef` reads any `std::io::Read` through bsv-rs 0.4.0's
  `BeefStream`, one element in hand; `read_beef`, `has_proof`, `own_proof`
  and `own_bump` fold over held bytes; `parse_beef` and
  `transaction_from_beef` run the same door and then build the in-memory
  `Beef` for a caller that merges, links, sorts or re-serializes one. A
  refusal reads `invalid BEEF at byte <offset>: <kind and its data>`.
  `is_limit_breach` answers `false` and stays exported for its callers.
- **Refusals that are gone:** `/submit`'s 413 over 10,000,000 bytes; the 512
  transaction and 512 BUMP counts at every door; the byte budget of every
  `*_BEEF_LIMITS` (the 10 MB doors, the census's 2 MiB and the queue's 90 KB
  as the module applied them); the base64 length bound of the queue's and
  the dead letter's reader; the byte bound on a stored funding body; the
  4,095-byte bound on a proof read back from a store or handed by a GASP
  peer (`proof_from_hex`).
- **What is stricter, by the same rule:** the door is bsv-rs 0.4.0's
  streaming decoder, which checks the whole frame and each BUMP's own
  agreement (a byte after the frame, a tree height of 0 or over 64, nodes
  that disagree, a missing sibling), each refused as invalid bytes with an
  offset. The in-memory parser read a body with a byte after its frame (the
  witness's `TrailingBytes` shape); such a body is refused now, at every
  door, stored rows included. No stored corpus was measured against the
  decoder by this change.
- **What stays, and why:** `COURIER_PROOF_MAX_BYTES` and
  `PUSH_PROOF_MAX_BYTES` (an 8 KiB JSON hex field of a courier's wire, not a
  BEEF); the ARC callback's 1 MiB JSON body (not a BEEF). bsv-low #585 owns
  and re-wires four doors after this change: the script door's byte budget,
  the census's `MAX_CENSUS_EVAL_BYTES`, the queue producer's
  `QUEUE_BEEF_SIZE_LIMIT` and the deferred-graph record.
- **What does not stream yet, said plainly:** the Worker reads a request
  body whole and D1 holds a BEEF in a row; the doors stream over the held
  value. The Worker has no R2 binding. The sites that take one are named in
  the code (`routes.rs` `/submit`, `d1_storage.rs` `insert_output` and
  `update_transaction_beef`, `queue.rs` `decode_beef_b64`); the binding and
  the buckets `low-overlay-beefs-beta` and `low-overlay-beefs` arrive with
  bsv-low #585 door 3.
- **Witnesses:** `crates/overlay-engine/tests/beef_doors.rs` and its
  siblings in `overlay-discovery`, `overlay-cloudflare` and `low-app-layer`
  (P0-5f's, inverted); `tools/lane-nl6/submit_any_size_ci.mjs` in
  `make ci-route`.

Nothing is deployed by this change.
