# Changelog

## 0.3.1 (2026-10-19)

- The engine crate's dev-dependency on itself is path-only again (as at 0.2.2): 0.3.0's `version = "0.3.0"` on it made `cargo publish --dry-run` resolve `bsv-overlay-engine ^0.3.0` from the registry before the crate was there, and the v0.3.0 Release run published nothing. No code change; every crate to 0.3.1 so the tag matches the workspace.

## Unreleased

- GASP (bsv-low #590): the per-graph time limb is charged per RESUME (each deferred graph a slice of its own each pass; the resumes' share gates when a resume may start; the per-peer deadline bounds them all), and it is read before each step: a node in flight finishes, cut only past a whole slice of its own. The counted limbs stay shared by the resumes of a sync. See CLAUDE.md "The time limb per resume".
- `ls_uhrp`'s lookup takes the reference's door (bsv-low #819, ts-stack@9262cac66 `UHRPLookupService.ts:81-136`, `shared/queryValidation.ts`): the allow-list `outpoint`, `uhrpUrl`, `expiryTime`, `hostIdentityKey`, `limit`, `offset`; a filter required; `limit` 1 to 200 (default 50, 201 refused), `offset` 0 to 100,000; invalid values refused, never an empty filter; no public find-all (the string `"findAll"`, `findAll`, `skip`, `sortOrder`, `includeExpired`, `nowUnixSeconds` are refused by name). `UHRPQuery` denies unknown fields. A refused lookup query answers 400 `{status:"error", code:"ERR_INVALID_LOOKUP_QUERY", description, message}`. The stack's thirty vectors replayed as a pin (`tests/uhrp_lookup_door_819.rs`).

## [0.3.0] - 2026-10-19

- bsv-rs `=0.7.0` in both workspaces (the line's exact pin of record; 0.7.1
  is in range and moves at the recon), bsv-middleware-cloudflare `=0.7.0` in
  both workers crates (its release on bsv-rs 0.7.0, whose core the lock holds
  at exactly 0.4.0), and bsv-tracker `=0.6.0` (its release on bsv-rs 0.7.0)
  in the door and the app layer: one bsv-rs in each lock. A bsv-rs type sits
  in the public API (`types` re-exports bsv-rs's overlay types), hence 0.3.0.
  No compile break and no source change was needed for the new APIs: no
  `SigningContext` struct literal exists here.
- The signing digest. bsv-rs 0.7.0 moved its default signing digest to the
  node's dispatch (BIP-143 only for a FORKID type without CHRONICLE). The
  admin signer (`wallet/signer.rs`, both paths) signs `ALL | FORKID` over the
  P2PKH or PushDrop lock, and now calls the SDK's dispatcher
  (`compute_sighash_dispatched_for_signing`), the one rule its interpreter
  verifies with; for that scope it is the BIP-143 digest 0.6.0 signed, byte
  for byte. Pinned by `w8e_the_digest_the_admin_signer_signs_is_the_nodes`
  (`--manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib
  w8e_`): the digest `85f5575e...b881f824` and the txid `d6d6879a...21a890e1`
  of the sample template, both computed again on bsv-rs 0.6.0 and equal; the
  digest equal to the SDK's BIP-143 function; the signed transaction run by
  `Spend` under `ScriptFlags::block(ProtocolEra::PostChronicle)` and valid.
- The tracker's vocabulary pin (`door_words`,
  `tracker_vocabulary_differs_from_the_door_exactly_here`) moves with
  bsv-tracker 0.4.1: the padded word (`" SEEN_ON_NETWORK "`) is trimmed and an
  `ORPHAN` in `extraInfo` no longer makes an orphan, so those two no longer
  differ from the door; seven differences stand for #624. The app layer's
  hosted tracker reads Arcade through the same `Verdict::from_arc_body`, so
  its hints take 0.4.1's vocabulary (`PENDING_RETRY` and `STUMP_PROCESSING`
  are `Accepted`, the word is trimmed, `extraInfo` is text); every hint is
  still a hint, never a chain word, and no app-layer pin moved.
- The middleware's 0.7.0 payment-prefix claim (`PaymentPrefixStore`,
  `PAYMENT_CLAIMS_MIGRATION`) is on its payment path, which neither workers
  crate links: no claims table, no migration applies here. The classifier
  of `AuthCloudflareError` was re-read (the enum is 0.6.2's; every new
  `KvError` site is a store fault) and its pin moved to 0.7.0.

- The seen witness names its source (bsv-low #621, LOW's words map L5).
  `network_seen` gains `seenSource` and `sourceAtMs` (migrations 335 and
  336, additive; the app layer's catch-up carries the same two ALTERs):
  every latch names the broadcaster whose answer said SEEN (Arcade, TAAL or
  GorillaPool; a courier never writes it) and that broadcaster's stamp when
  its answer carried one. `ls_pot spentStatus` and `/utxo-status` keep
  `networkSeen` byte for byte and add `networkSeenBy` (`{word: "seen",
  tier: "hint", source, at}`). The served eviction word is the chain word
  the eviction rests on: `rejected` (Arcade's own terminal word),
  `conflicted` (the conflict pass), `null` for an eviction on absence alone
  (`eviction_chain_word`). In `bsv-overlay-discovery`:
  `EvictionLedgerEntry` gains `chain_word`, `served_eviction_value` takes
  it, and `PotStorage` gains `network_seen_witness_for` (defaulted).

- The lint bar (bsv-stack-lean #134, #135, #137, #139, #140, #141, #144,
  #145); no version move. Both workspaces carry a `[workspace.lints]`
  table every member takes: `clippy::all` and unsafe code denied in both;
  in the root workspace `missing_docs`, `unwrap_used`, `expect_used` and
  `panic` denied too, which `overlay-cloudflare` and `worker-build-info`
  deny by crate attribute (the LOW crates beside them keep their code). A
  crate with no unsafe code forbids it at its root; the Worker crate's four
  unsafe items each carry their `// SAFETY:` reason. Every public item of
  the five published crates is documented, and the doc links resolve.
  `rust-version` is 1.88 in both workspaces, the lowest toolchain they
  build on (1.87 stops in bsv-rs 0.6.0). The gate gains `msrv`, `docs`
  (`RUSTDOCFLAGS=-D warnings`), `secrets` (gitleaks), `actionlint` and a
  non-blocking `machete`; the tools build from release sources pinned by
  sha256, and every action is pinned by commit sha.

## [0.2.2] - 2026-10-15

- The sync's storage per page (#58's finding; the owner's word of
  2026-10-15). Under the finalize hook (a peer budget, as both Workers run),
  a GASP page's admissions are queued and written in ONE storage batch at
  the page's end: each output with its transaction, each applied row, and
  the Worker's UHRP index row. The page's reads are made for the page: one
  read of the applied state of its fresh roots as it opens, one read of the
  held inputs of a batch answer's nodes, each later question answered from
  the page. Any other write, and any read of the queued tables, writes the
  queue before it, so the store sees the writes in the order one at a time
  wrote them; the absence rule, the fault memo, the proof window, the route
  fault rule and the accounting read the same events. On D1 the batch is one
  `batch()` (one transaction, one subrequest, metered as one D1 query) under
  the finalize submit's bound. A batch that fails admits nothing of the page:
  the sync fails, the cursor stays where it was, and the next run is served
  the page again. A page of 100 costs its two reads and one batch where
  0.2.1 made 500 storage calls (five an output), so the backfill allowance of
  500 (unchanged) walks pages: the 1,339-output walk of 2026-10-14 is one
  call in the witness, fourteen at 0.2.1. `SyncCost` gains `storage_batches`
  and `storage_statements`; the tick lines name them. `Storage` gains the
  defaulted `write_batch`, `find_topic_outputs` and
  `find_applied_transactions`; `GASPStorage` the defaulted `open_page`,
  `close_page` and `prefetch_page`; `UHRPStorage` the defaulted
  `store_record_unless_held`, which the Worker answers in one
  `INSERT OR IGNORE` on the record's primary key (two statements before).
  Without a peer budget the graphs are submitted after the sync one at a
  time, as before.

DEPLOYMENT NOTE: no migration, no var, no table. Each Worker gains on its
own; no peer needs 0.2.2.

## [0.2.1] - 2026-10-15

- A batched node request between our own peers (#58).
  `POST /requestForeignGASPNodes` takes `{"outpoints":[{"txid","vout"}...]}`,
  at most `NODE_BATCH_MAX_OUTPOINTS` (100, the engine's page; a request over
  it is answered 400 with `maxOutpoints` in the body), and answers
  `{"nodes":[...]}`: each entry names its outpoint and carries the node as
  `/requestForeignGASPNode` serves it (its proof included) or a refusal
  (`not_found`, `over_budget`, `error`), in an answer of at most
  `NODE_BATCH_MAX_BYTES` (4 MiB); the outpoints the budget leaves out have
  no entry and are named again in the next body. The same auth as the single
  route (none). The sync asks a peer that answers it for each page's fresh
  roots in as few bodies as the budget allows; every entry feeds the code a
  single answer feeds (`not_found` is the peer's `NodeNotFound`, counted by
  the absence rule as before), and any other refusal is asked on the single
  route. A batch that fails (a 404 from a peer without the route, a route
  fault, any error) or an answer over the byte budget counts nothing against
  the peer: the peer is asked one node at a time for the rest of the run,
  remembered per (peer, run) and never persisted. A reference peer is asked
  exactly as before, after one 404 a run. `SyncCost` gains `batch_requests`
  and `batch_nodes`; the tick lines name them. `GASPRemote` gains the
  defaulted `batches` and `request_nodes`.

DEPLOYMENT NOTE: no migration, no var, no table. Both of our Workers need
0.2.1 for the batch to be used between them; until the peer has it, the
asker reads one 404 a run and walks as 0.2.0 does.

## [0.2.0] - 2026-10-15

- bsv-rs 0.6.0 in both workspaces (caret `0.6.0`), and the home of the
  overlay's varint: the route's `write_varint` and `read_varint_prefix`, the
  script door's reader, and the test copies (`wire_compat.rs`,
  `live_worker.rs`, the script door's and the walk witness's helpers, the
  UHRP tests) are deleted for bsv-rs's public `to_var_int` and canonical
  `from_var_int`. A length written wider than it needs (`fd 01 00` for 1) is
  now refused by the script door and by bsv-rs's transaction and BEEF
  parsers, as the node refuses it; the `/submit` off-chain prefix is read as
  the reference's `readVarIntNumStrict` reads it. bsv-middleware-cloudflare
  `=0.6.2` (its release on bsv-rs 0.6.0), so the lock holds one bsv-rs. A
  bsv-rs type sits in the public API (`types` re-exports bsv-rs's overlay
  types; `beef_limits` returns its `Beef` and `MerklePath`), hence 0.2.0.
- The stack's `deny.toml`, byte for byte (sha256 `ba065ed0...8ed5083`); the
  root workspace passes it: parity-harness's lock moves anyhow, h2, rustls
  and rustls-webpki past six RustSec advisories, and its reqwest takes the
  system's roots (`rustls-tls-native-roots`) for the bundled webpki-roots,
  whose license the template does not allow.
- A route fault counts nothing against the peer (#57). A failure the
  platform answers before the peer is reached (an HTTP 404 carrying the
  platform's `error code: 1042`; a fetch with no answer whose error names DNS,
  TLS or a connect timeout) is `GASPError::RouteFault`. A sync it ends with
  no work done writes none of the peer's health rows: not its failures or
  attempt stamp (the quarantine), not its yieldless streak, not its quiet
  cadence. Only an answer from the peer counts (a 5xx, a malformed body, a
  verification failure, a GASP error); a failure the classifier does not
  name counts as before. The sync result carries `route_faults` (peer,
  class), the error and tick lines read `route_fault:<class>`, a node-level
  route fault is the walk's class `route` (never a strike), and invariants
  carry `gaspRouteFaults24h` (`total`, `byPeerClass`).

DEPLOYMENT NOTE: one transient table, `gasp_route_faults`, and its age index
join the runtime schema list (254 to 256), created on the next cold start;
no var. The workers lock needs a bsv-tracker release on bsv-rs 0.6 before it
builds: until then bsv-tracker 0.2.2 holds bsv-rs 0.5.0 beside 0.6.0 and the
build stamp refuses the lock. Constructors of `TopicSyncResult` gain
`route_faults`; a match on `GASPError` gains `RouteFault`.

## [0.1.15] - 2026-10-14

- An absence is the peers' word, never an explorer's (#56). A peer's
  `NodeNotFound` for a root it lists, or for a transaction an unproven node
  of the root's graph needs, is the `not_found` class (it was `not_served`).
  Three such runs at least `ABSENCE_GAP_SECS` (60 s) apart strike the root
  `unanchorable:not_found`: passable, on the six-hour backoff, the four
  bounded retries, dead and swept at the advert's expiry, as every strike.
  A run inside the gap asks nothing and counts nothing; a transport fault, a
  timeout, a 5xx or a disabled remote counts nothing.
- The raw rungs fetch, never decide. All configured rungs answering absent
  (`DefinitiveNotFound`) counts the same one run, never a strike by itself
  (0.1.12 to 0.1.14 struck at once). An input of an unproven node the courier
  cannot serve is asked of the peer, one request; with no rung configured
  (`PROOF_SOURCES` unset) the peer was never asked before and such a root was
  named `courier`. The D8 prune of a proven node's named inputs and a spent
  courier budget are unchanged.
- A peer's absence is remembered: an input it answered it does not hold is
  not asked again in the sync, and a root inside its gap or backoff costs no
  request. `SyncCost.peer_absent_memo_hits` counts both; topic results carry
  `absent_struck`; invariants expose `gaspPeerAbsent24h` (`recorded`,
  `struck`) from the fault memo; the tick lines name `absences_struck` and
  `peer_absent_memo_hits`.

DEPLOYMENT NOTE: no migration, var or pin. Fault memo rows written by 0.1.14
read as they are: a `not_served` record keeps its class and count, a struck
`unanchorable:not_found` keeps its verdict. A `retry:fault:not_found` row
written by 0.1.15 carries its gap in `next_at`; a rollback to 0.1.14 leaves it
out of the due read until then but walks it when its page lists it. Production with no rung configured reaches
`unanchorable:not_found` on the advertising peer's word alone. Each walk of a
graph whose unproven input the courier cannot serve now costs one more peer
request (the 0.1.11 resume share holds fewer such resumes per pass).
Constructors of `SyncCost` and `TopicSyncResult` gain one field each.

## [0.1.14] - 2026-10-14

- Store GASP fault facts in `gasp_fault_memo`, separately from partial graphs.
  The memo holds 20,000 rows per topic and evicts the oldest observations.
  Faults no longer compete for 32 fault slots or 16 partial-graph slots.
  Batched page lookups preserve cursor passability without node requests;
  bounded due reads retain the six-hour backoff, four retry reservations,
  terminal verdicts, score changes and expiry sweep. Storage failures still
  count as refused strikes; no admission or proof rule is relaxed.
  Known expiry is swept by index every tick; unknown legacy expiry is
  reconciled against the shared expiry memo on folding and hourly maintenance.
- Fold legacy fault records into the memo on reading a peer/topic, atomically
  freeing their old rows and counting each fold once. Invariants expose
  `gaspFaultMemoRows` and `gaspFaultMemoFolded`; scheduled logs name memo
  expiry removals and folds.
- Mark GASP admissions in `transactions.admission_origin`. Proof completion
  can fetch their missing root proof immediately, while submitted rows retain
  the 30-minute callback window. Completion logs report `early_gasp`, and
  the admin response exposes `tx_early_gasp`.

DEPLOYMENT NOTE: eight additive schema statements (245 to 253) create
`gasp_fault_memo`, its due/age/expiry indexes, `gasp_fault_memo_size` and two
size-maintenance triggers, and add `transactions.admission_origin` with a
`submit` default. Existing transactions retain that default; historic origin
is not inferred. Legacy zero-node fault records are folded in a counted D1
batch when their peer/topic is read. Other partial graphs stay in place.
Rollback to 0.1.13 ignores these additions and reads the old store as it then
stands; folded fault records are not copied back, so their below-cursor retry
obligations are unavailable to the older worker. No schema reversal is needed.
Custom `Storage` implementations must implement `read_fault_memos` and
`insert_output_with_origin`, keeping fault storage independent of graph slots.
No version, dependency pin or runtime var changes are included.

## [0.1.13] - 2026-10-14

- Accept `until: "tip"` with a positive `budget_ms` for one selected topic and
  peer on `POST /admin/startGASPSync`. Page slices retain the existing peer and
  graph deadlines and share the request's expiry and courier-absence memos.
  Report summed accounting, data slices and a continuation from the durable
  cursor when wall, request, slice or progress limits stop the walk. No tick
  selection or deadline changes. A repeated boundary is checked above its
  score before reporting tip; the probe never advances the stored cursor.
- Reserve at most four six-hour retries after an unanchorable verdict, including
  interrupted walks. Claims compare the stored count, score and backoff in one
  atomic operation, so overlapping syncs cannot spend the same retry slot.
  Exhausted roots become passable `dead:<class>` records,
  never resumed on the same score. A changed peer score opens a new walk.
- Sweep dead and unanchorable records at the advert's own expiry. Count terminal
  records and expiry removals as `gaspDead` and `gaspFaultExpired`. The authenticated
  proof probe accepts `walk=1&readmit=1` to clear the selected topic/outpoint's
  fault records before its diagnostic walk; the response counts the removals.
- Report `outputs_walked`, `ms` and `ms_per_1000_outputs` per peer, per topic and
  in total. Invariants expose `gaspLastSync`, `gaspWalk24h` and
  `gaspWalkByTopicPeer24h`; rates use summed time divided by summed outputs,
  with `null` for zero outputs. `d1_queries` counts attempted SQL statements.

DEPLOYMENT NOTE: existing JSON records default to zero retry reservations and
unknown expiry. Two additive migrations create `gasp_sync_speed` and its
`gasp_sync_speed_age` index for rolling speed readings and the latest sync.
No lifecycle table migration, var or dependency pin changes.
Older records can be swept through the existing expired-outpoint memo; a future
walk also captures the advert expiry. No version bump is included here.
Backfills checkpoint at 180,000 ms, 500 trait/network operations, 600 measured
SQL statements or 64 slice attempts, with room for guarded finalization and
request bookkeeping. CPU limits still require measurement at the deployment.
Custom `Storage` implementations and wrappers must implement or forward
`reserve_unanchorable_retry` atomically. Standalone `GASPStorage` adapters that
do not support it decline retries safely; the shipped adapter forwards it.

## [0.1.12] - 2026-10-14

- Accept optional `topics` and `peers` arrays on `POST /admin/startGASPSync`.
  An empty body selects the full configured sync; an empty array selects
  nothing. Named peers bypass network discovery. Results name the filter
  and excluded topics and peers. Scheduled sync selection is unchanged.
- Back off quiet successful peers after three empty syncs of a topic: 60
  seconds, doubling to one hour. Productive work resets the streak; an
  explicit filtered sync resets it, and a changed locally admitted SLAP
  advertisement (or topic-specific SHIP advertisement) wakes the peer.
  Results name `peers_skipped` with `no_yield` or `quarantine`. Quiet
  polling and the existing failed-peer quarantine use separate counters.
- Treat three definite raw-rung absences as `unanchorable:not_found` on
  that run, for fresh roots and resumed obligations. The six-hour retry
  and fault-record ceilings remain. The per-run memo is shared across
  topics and peers and counts `courier_not_found_memo_hits`. Disabled
  rungs, malformed responses, 429s, 5xx responses, transport errors and
  spent budgets cannot establish absence. Other fault classes retain
  their three-run rule. No unanchored graph is admitted.
- Bound a budgeted root pass to twelve courier HTTP attempts and at most
  15,000 ms of walking, with a 1,000 ms timeout per courier request.
  `root_bounds` names the root and limiting reason; the existing durable
  deferral path retains its work. A refused save cannot extend a time or
  courier limit; the call/size fallback keeps the same root deadline.
  Storage/finalize completion retains its
  existing guarded semantics. Library users still opt into graph budgets.
- Report `calls_out`, `peer_requests`, `courier_rungs`, `storage_reads`,
  `storage_writes` and `ms` per topic and for the whole sync. Storage
  figures count attempted trait operations, including errors, rather than
  SQL rows. The tick logs totals even when its outer budget cancels the
  sync. `/health/invariants` serves `gaspLastTick`, `gaspCallsOut24h`,
  `gaspStorageReads24h`, `gaspStorageWrites24h`, `gaspSyncMs24h` and a read
  fault flag. One-day totals include admin and scheduled syncs.
- **DEPLOYMENT NOTE:** Five additive schema statements (238 to 243):
  `gasp_peer_health.quiet_syncs`, `quiet_retry_at`, `quiet_advert`, and
  `gasp_sync_cost` with its age index. Existing rows receive zero/empty
  cadence defaults; no deferred-record rewrite is required. No new var,
  dependency pin or version move. Older workers ignore the additions;
  rollback restores their polling cadence. Consumers constructing sync
  result literals must supply the added fields, and exhaustive GASP error
  matches must handle the two added variants. The keeper sets workspace
  versions and internal floors for release and calibrates the counters
  against production analytics before claiming reduced load.

## [0.1.11] - 2026-10-14

- Strike a held record whose resume faults. A RESUMED pass dropped by a fault
  of a striking class (`courier`, `not_served`, `verification`) at the
  `not_held` or `idle_faults` drop counts a faulted run on the record's own
  slot, as for a fresh root: `faults`, the reason `retry:fault:<class>`, its
  score kept. At `UNANCHORABLE_AFTER_FAULTED_RUNS` (3) it is
  `unanchorable:<class>` and waits `UNANCHORABLE_RETRY_SECS` (6 h); a later
  success clears it. On 0.1.10 such a resume was dropped and saved again as
  it was on every run, named `courier` with `faults 0`, never struck.
  - A record the cursor had passed stays passable while it counts its runs.
  - The struck record frees its deferred slot and is held under
    `FAULT_RECORDS_PER_PEER_TOPIC`; in the held loop the keys not yet read
    count as fault records. A strike refused there is counted and named as
    in 0.1.10, and the record is dropped as before.
  - A pass that never got the root (the peer no longer serves it) is left to
    0.1.10's idle rule. A peer's fault of the moment is never a strike.
- Make the idle count fire on every ending. A resumed pass that asked the
  peer and kept no node counts one idle resume whether it ended in its own
  save, a drop or an error. On 0.1.10 the count after a drop read the
  sync's appended nodes, which the root appended and then thrown away with
  the drop had raised, so it never fired. A strike takes precedence. The
  sync's accounting carries `idle_counted` (records counted this run), and
  the tick's summary line carries `idle_counted=`.
- Give the page its share of the slice. The resumes of one pass share one
  deadline of `resume_share_ms` = min(the per-peer budget times
  `RESUMES_SHARE_OF_PEER_BUDGET_PERCENT` (33) / 100, the graph budget):
  9,900 ms on the worker's 30,000 ms and 15,000 ms. It starts with the held
  loop, so the storage's calls between resumes are inside it. The page keeps
  the rest. A record reached after the share is held back for the next sync:
  not resumed, not a failed UTXO, not stalled, not an idle resume. An
  unanchorable record inside its backoff is not counted held back.
- **DEPLOYMENT NOTE:** No migration, new variable or dependency pin. The
  records written by 0.1.8 to 0.1.10 are read as they are (`reason "retry"`,
  no `idleResumes`, no `faults`). A struck obligation's JSON carries `faults`
  and, at three, `retryAt`, as 0.1.9's fault records do. Rolling back to
  0.1.10 is safe for the store; 0.1.10 reads a struck obligation as a fault
  record, and its next strike of one still under three runs clears the
  passable flag, so that root holds the cursor until its third run. The
  keeper sets both workspace versions and internal floors for release.

## [0.1.10] - 2026-10-14

- Give the strike room of its own. A fault record (`retry:fault:<class>` or
  `unanchorable:<class>`: a root, a class, a count, no nodes) is bounded by
  `FAULT_RECORDS_PER_PEER_TOPIC` (32), not under
  `DEFERRED_GRAPHS_PER_PEER_TOPIC` (16). On 0.1.9 a (peer, topic) whose
  sixteen slots held root retry obligations refused the strike of the root
  that held its cursor, and the rule was inert.
  - The strike of the run's lowest faulted root is written whatever the
    deferred store holds. The storage's own ceiling stays the only one above
    the new bound.
  - A struck root's walk replaces its fault record with the hold it earned
    only with room among the deferred slots; else it goes on as any walk
    past the bound, and the fault record stays.
- Bound the churn of held records that never progress. A held (passable)
  record resumed `IDLE_RESUMES_BEFORE_BACKOFF` (3) passes in a row with no
  node appended waits `IDLE_RESUME_RETRY_SECS` (6 h) before its next resume:
  no request, the cursor passes it, not an attempt for the yieldless streak.
  A resume that appends a node resets the count. The records are kept, and
  an expired root a record holds is still refused ahead of the backoff, with
  no request.
  - A pass counts only when it asked the peer. A resume cut in flight by the
    per-peer deadline with nothing appended counts.
  - A hung peer whose only record waits is asked only its listing in those
    ticks: a quiet sync, recorded as any. Each pass after the backoff runs
    out is a failed attempt again, and `PEER_QUARANTINE_THRESHOLD` of them in
    a row still quarantine it.
- Make a refused strike readable. A strike refused at the fault bound, at the
  storage's ceiling, with the keys unread or on a storage fault is counted
  (`strikes_refused` in the sync's accounting) and named on the root
  (`refused`: `no_room` or `store_fault`). The tick's per-topic `faulted`
  items carry the reason; its summary line carries `strikes_refused=`,
  `backed_off_graphs=` and `waiting_graphs=`. The counter
  `gasp_strikes_refused_total` is served on invariants as
  `gaspStrikesRefused`.
- **DEPLOYMENT NOTE:** No migration, new variable or dependency pin. The
  deferred store's columns and keys are unchanged: the sync knows a fault
  record when it reads it. The idle count lives in the record's JSON
  (`idle_resumes`, absent when 0) beside 0.1.9's `retry_at`. Rolling back to
  0.1.9 is safe: 0.1.9 ignores `idle_resumes` and the `retry_at` of a record
  that is not a fault record, so it resumes such records every run again,
  and counts fault records among its sixteen slots again. The keeper sets
  both workspace versions and internal floors for release.

## [0.1.9] - 2026-10-14

- Record a GASP root whose walk is dropped by a fault in three consecutive
  runs as unanchorable, and move the cursor past it. The faults that count are
  a courier fault, an input nobody served, and a verification refusal. A
  deadline, a bound and a peer's fault of the moment do not count.
  - The run's lowest faulted root starts a record `retry:fault:<class>` in
    `gasp_deferred_graphs`. Its next runs walk it again ahead of the pages.
  - At `UNANCHORABLE_AFTER_FAULTED_RUNS` (3) the record becomes
    `unanchorable:<class>` and passable, and is walked again only after
    `UNANCHORABLE_RETRY_SECS` (6 h).
  - A later success admits the root as any root and clears the record. Nothing
    is admitted unanchored.
  - At most one new record per (peer, topic) per run.
- Name every dropped root with its fault class, once per run. The sync's
  accounting carries `faulted_roots`, `unanchorable_graphs` and
  `unanchorable_cleared`. The tick's per-topic line carries `faulted=[...]`
  beside `cursor_held_by`, and the sync line sums both counts. The counter
  `gasp_unanchorable_total` is served on invariants as `gaspUnanchorable`.
- `GET /admin/proof-probe?txid=<hex>&walk=1[&vout=<n>][&topic=<tm>]` (admin
  bearer) reports what a walk of an unadmitted outpoint is served. Each
  configured peer is asked for the root's node: present, proof, inputs. For an
  unproven root, the courier is asked for each ancestor, to depth 3 and at
  most 24 asks. Nothing is written.
- **DEPLOYMENT NOTE:** No migration, new variable or dependency pin. The
  deferred store carries the reason in its existing `reason` column. The
  count and backoff live in the record's JSON (`faults`, `retry_at`), and
  are absent from every other record. Rolling back to 0.1.8 is safe: 0.1.8
  ignores the two new fields. It resumes an `unanchorable:` record as a
  passable hold walked every run, and deletes a `retry:fault:` record at its
  next fault, so that root is a gap again, as on 0.1.8. The keeper sets
  both workspace versions and internal floors for release.

## [0.1.8] - 2026-10-14

- Hold the GASP graph in flight when the per-peer deadline falls due, whatever
  it fetched (`gasp_deferred_graphs`, reason `peer_deadline`), and move the
  cursor past it to the page's last finished score; the next sync resumes it
  ahead of its pages under the per-graph budget. Three consecutive cuts are
  now needed only for the graph's own deadline. `cursor_held_by` names only
  outpoints neither finished nor held.
- Remember a root refused as expired across runs (`gasp_expired_outpoints`,
  keyed by topic and outpoint), so a later sync skips it with no node fetch.
  A row is kept until `max(expiry, refusal) + 7 days` and at most 20,000 a
  topic, both swept by the cron. `gaspExpiredRefused` counts new refusals only.
- A sync that finalized, moved the cursor, refused an expired root, retained a
  graph holding a served node or resumed one with progress is never a failed
  attempt for the quarantine. Only a finalize, a cursor move or a refusal ends
  the yieldless streak; held and resumed walks stay inside its allowance.
- New `Storage` methods `remember_expired_outpoint` and
  `find_expired_outpoints` have defaults (fail-safe: the dead are fetched
  again); a wrapper forwards both.
- **DEPLOYMENT NOTE:** One additive migration (236 to 238): the table
  `gasp_expired_outpoints` and its index `idx_gasp_expired_outpoints_keep`,
  applied by the overlay's migration runner on its next cold start. No new
  variable or dependency pin. The keeper sets both workspace versions and
  internal floors for release.

## [0.1.7] - 2026-10-14

- Give scheduled and admin transaction proof completion a dedicated 60-attempt
  courier and candidate allowance inside its 30-second deadline. Other passes
  retain their own budgets and every accepted proof still requires headers.
- Parse both WoC's single-object array and Bitails' object TSC response. Count
  proof-rung HTTP, parse and judge faults, logging one detail per rung per run.
- Preserve the root proof request before ancestry and report validated roots
  through `gaspRootsProven` and `gaspRootsUnproven`.
- Advance pagination past durably held graphs and resume their records without
  relying on a peer to list them again. Three consecutive empty deadline cuts
  turn a retry record into a deliberate hold. Held records survive age cleanup;
  failed partial walks retain a root to retry. Report `cursor_held_by`.
- Reject expired UHRP crawl answers before submission and count
  `crawlExpiredRefused`. Direct submission retains its admission policy.
- Add `GET /admin/proof-probe?txid=<hex>` with rung requests, HTTP status,
  parsing and header verdicts, plus the stored BEEF's own-proof presence.
- **DEPLOYMENT NOTE:** No new variable, dependency pin or migration. The probe
  requires the admin bearer and writes no data. Explorer sources remain opt-in.
  The keeper sets both workspace versions and internal floors for release.

## [0.1.6] - 2026-10-14

- GASP skips expired UHRP roots before proof hydration, advances their cursor,
  and counts `gaspExpiredRefused`. Resumed roots and expired siblings follow
  the same sync rule; direct submission retains the reference admission rule.
- Courier attempts share a per-run transaction memo across output, ancestry
  and exhaustive paths. Faults and interrupted attempts remain retryable;
  a partial unmined hint never becomes an exhaustive negative.
- GASP has a separate 60-attempt proof allowance. Completion keeps its default
  courier budget. Every proof still requires header verification.
- Peer crawls query bare UHRP hashes from at most 16 local live content keys.
  GASP enumerates unfamiliar content. A peer's HTTP 400 includes the request
  shape and is logged once per peer, service and shape version, using D1.
- **DEPLOYMENT NOTE:** Nothing changes for operators. No new configuration,
  dependency pin or schema migration. Explorer sources stay opt-in. The keeper
  sets both workspace versions for the release.

## [0.1.5] - 2026-10-13

- **DEPLOYMENT CHANGE: public explorer rungs are OFF unless named in `PROOF_SOURCES=arcade,bananablocks,woc,bitails`. LOW and Zanaadu must set this var to retain the previous explorer behavior. The stack leaves it unset.** `ARCADE_URL` and `WOC_API_KEY` only configure enabled rungs. Proof, raw-ancestor, spend-hint, retirement, conflict and rebroadcast probes follow this setting.
- Hold admitted outputs without their own verified proof after the shared 24-hour observation window. Lookup and UHRP SQL omit held rows, keep the BEEF as stored, and bound each result read to 1,000 outpoints. Admission verification is unchanged.
- Retry up to 20 held outputs against their configured or SHIP-discovered topic peers in a 30-second tick slot, with five seconds per node request. Verify each BUMP against headers, stitch it into stored BEEF, and restore lookup visibility. Report `heldByWindow`, `peerProofRequests` and `peerProofsLanded` on invariants.
- Retire a held transaction as `expired-unproven` when every indexed output is an expired UHRP advert. Keep live, permanent and non-UHRP siblings. Report the live `retiredExpiredUnprovenTotal` separately from structurally unprovable LOW rows.
- Use the existing watch observation stamp, falling back to transaction creation time; watch enrollment retains that creation time. No schema, SDK pin or package version change in this lane.
- The Arcade reorg feed also requires the `arcade` source. LOW advert spend probes require both `woc` and `bitails` for independent corroboration; their D1 expiry reaper still runs. A peer BUMP with an unmarked subject hash is attached to that verified subject before the proven flag moves.
- Share the maintenance tick between cron and the `TickAlarm` Durable Object, with a D1 lease, bounded execution and source activity on `/health/invariants.tick`. The alarm re-arms after failed ticks; the cron revives a dormant alarm. Authenticated operators can use `POST /admin/tick/arm` and `GET /admin/tick/status`.
- Deployment configs must add the `TICK_ALARM` binding for class `TickAlarm` and a migration with `new_sqlite_classes = ["TickAlarm"]` (choose an unused migration tag). The engine's example uses tag `v1`; the stack keeper, LOW and Zanaadu own their deploy configs. Without the binding, cron still runs and logs the skipped arm. Arm after deploying, then verify both source counters and the next alarm.
- D1 adds a transient lease, a one-day activity history and two nullable heartbeat fields. Existing proof-completion counters keep their meaning. Rollback may leave these additive tables and columns in place. Workspace versions remain for the keeper's release commit.
- Carries the E649 work landed on `main` after 0.1.4 (bsv-low #649, LOW's captain): a background flush's withheld pot and hop notes parked in D1 and drained by the shared tick and by `/admin/complete-proofs` (migrations 222 to 231; the tick lane's are 232 to 236), the conflict pass persisting each candidate's counts, the eviction ledger served on the app layer's `/utxo-status`.

## [0.1.4] - 2026-10-13

- Request GASP pages of 100 outputs under the 30-second peer budget and persist only completed scores below every failed, deferred or unfinished output. Unordered pages, score ties, responder clamping and deadline resumes retain the existing gap guards. Reference overlay-services@480d77a uses 10,000 at `src/Engine.ts:29,895` through `src/GASP/OverlayGASPRemote.ts`; the smaller engine page bounds reply work on Workers.
- Filter GASP targets to registered deployment managers after topic suffix rekeying, with one warning per skipped topic per isolate. Unregistered topics spend no sync slots.
- Hydrate proofless GASP roots through the existing proof-only courier API without a redundant raw fetch. Keep graph anchor verification before admission, including resumed graphs, and enable the engine's header tracker from the CHAINTRACKS binding even without a URL variable. Give transaction proof completion an early 30-second slot and a 20-candidate page, preserving half the courier budget for GASP roots; record its heartbeat before later network maintenance.
- Discover SHIP peers from the local index, the four configured SLAP trackers, and bounded SLAP-advertised `ls_ship` providers. An injected platform lookup client keeps HTTP out of the engine; the Worker bounds each lookup and response read to five seconds. Provider discovery is shared across topics in each tick, with four additional lookup hosts allowed; advertised sync peers are not capped.
- Run `tm_uhrp` in SHIP mode while retaining its bootstrap peer in the discovered union, ahead of tracker imports that could consume the tick. Normalize and deduplicate origins across local, remote and bootstrap answers, exclude our own origin, and preserve independent answers when a tracker fails. Topic suffixes retain every configured target.
- Reference: overlay-services@480d77a `src/Engine.ts:105-108,839-876`; ts-stack@edf6e03 `packages/sdk/src/overlay-tools/LookupResolver.ts:216-223,248,1976`. The reference defaults topics to SHIP and resolves `ls_ship` through SLAP advertisements before synchronizing with the returned domains.
- Align both workspace versions and compatible internal dependency floors to 0.1.4 under RELEASING.md rule 4. Both bsv-rs lock resolutions remain 0.5.0; no SDK pin or stored-data migration changes.

## [0.1.3] - 2026-10-12

- Index admitted UHRP adverts with the reference's ce00 prefix and bare Base58Check URL. Normalize optional UHRP query schemes in memory and D1. Five storage golden vectors prove admission and lookup parity.
- Remove the obsolete 0x01-to-hex lookup fallback. Storage accepts legacy input but emits canonical adverts; no current producer requires that fallback. Historical 0x01/hex rows need bounded migration or re-advertisement before canonical lookup; this patch does not rewrite persisted rows.
- Align both workspaces and internal package dependencies to 0.1.3 per RELEASING.md rule 4. The keeper lands this after the 0.1.2 release.

## [0.1.2] - 2026-10-12

- Give hand-made coinbase fixtures lone-leaf proofs and ordinary funding fixtures two-leaf proofs with real synthetic parents under bsv-rs 0.5.0's mining checks; no door rule changed.

- Charge the stream's 32-byte SDK `OutputRef` at 96 bytes per output including growing-buffer slack (was 72); the size-derived guard stays intact. Repair synthetic unproven-coinbase fixtures and refresh the exact lock-version snapshot without changing the Worker health shape or test assertions.

- Extend the structure-only verdict match for SDK 0.5.0's contextual decline; this mode disables spend/context checks, so that variant is unreachable here. Byte-level mining checks remain enforced.

- Align the two Workers on exact `bsv-middleware-cloudflare =0.6.0` (core 0.3.0), whose SDK requirement is 0.5.0; 0.5.1 requires SDK 0.4 and would leave two SDK types in the Workers lock.

- Declare `bsv-rs = "0.5.0"` by caret in the published libraries. The update loop can deliver compatible SDK patches to consumers without another library release; deployables keep their exact pins and committed locks.
- Resolve bsv-rs 0.5.0, including its streaming mining checks. Stored BEEF and old BSC1 cursors require the SDK's revalidation or restart rules; no stored-data migration is performed here.
- Both shared workspace tables carry the caret and both locks pin the exact resolution: caret in the manifests, the lock the pin. The five published crates and their internal dependency floors move to 0.1.2.

## 0.1.1

- [#17](https://github.com/Calhooon/bsv-overlay-cloudflare/pull/17): Replace four-crate parity claims with the published engine line and refresh the README.
- [#18](https://github.com/Calhooon/bsv-overlay-cloudflare/pull/18): Distinguish terminal mempool races, reserve network_seen for broadcaster witnesses, and converge readmitted pots on confirmed competitors (LOW #602).
- [#19](https://github.com/Calhooon/bsv-overlay-cloudflare/pull/19): Record the confirmed competitor before conflict eviction, recognize unconfirmed mempool bodies, reuse verified spender bytes, preserve the released hop, and bound the spender memo (LOW #605 and #606).

## Unreleased: health build metadata (L2-74)

- Both deployable Workers serve Zanaadu-shaped `build` metadata on every health route: engine and deployer revisions, dirty status, UTC build time, and exact workers-lock crate versions. Duplicate package entries fail the build; `BUILD_LIB_REV` overrides the deployer revision. Existing health checks and the dashboard version are unchanged.

## 0.1.0 (prepared, unpublished)

- bsv-rs 0.4.5 in both workspaces: the lone leaf at offset 0 only; the LOCAL work bound; no behavior change under the defaults.

- Registry packages for `bsv-overlay-engine`, `bsv-overlay-discovery`,
  `bsv-low-proof-replay`, `bsv-worker-build-info` and `bsv-overlay-cloudflare`, in that dependency order.
- Engine behavior is the e592 release at `engine-e592-2026-10-10`.
- Includes #74's Worker build object; its shared capture crate is packaged once
  for both Workers. LOW's deployable app layer remains unpublished.
- Crate metadata, scoped package contents and a tag-driven trusted publishing
  workflow. Cross-crate engine tests now live in an unpublished test workspace
  member to remove a publication cycle without changing their assertions.

## Unreleased: the workers on bsv-middleware-cloudflare 0.5.1, the bsv-rs 0.3 bridge gone, the Arcade vector at the toolbox 0.7.4's bytes, LOW's N3, N4, N5 and L1 (align-engine-050, 2026-10-10)

The four findings are LOW's, in their own words at bsv-low's `docs/audit/E585-land2-lens-2026-10-10.md`
(the E585 land2 lens over `54dbb16`).

- **bsv-middleware-cloudflare 0.5.1** (`=0.5.1` in `overlay-cloudflare` and `low-app-layer`, was `=0.4.1`;
  with `bsv-middleware-core` 0.2.1, both on bsv-rs 0.4; the lane pinned 0.5.0 and the captain took 0.5.1 the
  hour it published: bsv-rs 0.4.1 as the floor, a transaction with no input the reader's `NoInputs` word through
  `PaymentVerifyError::Unverifiable`, the refusal class unchanged, the 22 vectors passing unchanged). The API moves of its CHANGELOG this workspace meets:
  `WorkerStorageClient::new` takes bsv-rs 0.4's `ProtoWallet`. The payment verdict's move
  (`PaymentVerifyError::Unverifiable { reason }`; `BadTransaction`, `MissingOutput`, `BadBeef` gone) touches no
  call site here; `low-app-layer` compiled unchanged.
- **The bridge gone.** `bsv-rs-03` (the bsv-rs 0.3.35 dependency that built the one wallet 0.4.1 took) leaves
  `overlay-cloudflare`; `make_storage_client` hands 0.5.1's `WorkerStorageClient::new` a `ProtoWallet` built from
  the admin key as held (the workspace bsv-rs 0.4.3): no conversion, no refusal. The workers' lock holds one
  bsv-rs (`cargo tree -d`: no duplicate `bsv-*` crate in either workspace). New test: the storage client's wallet
  is the admin key, never the anonymous wallet.
- **The Arcade status vector** (`crates/overlay-discovery/vectors/arcade_status_verdicts.json`) is
  bsv-wallet-toolbox-rs 0.7.4's file byte for byte (tag `v0.7.4`, main `623f890`; the same bytes at
  bsv-wallet-cli main `636c6dd`; sha256 `80d2ac72112d…`, pinned whole by
  `the_vector_is_the_toolbox_0_7_4_bytes`; the rule is `cmp`). What moved from the engine's copy at `54dbb16`
  (`7bf85a94cd0b…`): the `toolbox_push` expects of four cases and the `outcomes` block; no `engine_*` field.
- **N3, the settled-jobs sweep.** `JOB_DELETE_SQL` was unconditional and the sweep released the bytes before the
  delete: a re-presentation of the same submission landing between the sweep's select and its delete (bytes
  re-put, row upserted to `queued`) lost both, the job's message found no row and the 202's poll answered 404.
  Now `DELETE ... WHERE reference = ?1 AND state IN ('done','failed') AND updated_at < ?2 RETURNING at_rest`,
  the row first, the bytes released only for a row that came back.
- **N4, the deferred job's ack, as a bound.** The pin's sentence that a deferred EF job's message never rides the
  platform's retries or the dead letters held only when `run_job` returns. `run_job`'s doc states the bound: a
  run that kills the isolate never reaches the ack; the platform redelivers (`max_retries` 3, no `retry_delay`); a
  redelivery inside `JOB_RESUME_AFTER_MS` of the take is acked untaken (the live-run guard), a later one is a
  `JOB_TAKE` of the five `JOB_ATTEMPTS`; after `max_retries` the message dead-letters as an `unparsed:` fault
  letter counted against the 2,000, whose re-drive takes the `ef_job` branch again. No code.
- **N5, the deferred bytes on the health figure.** A pass over `ef-deferred/` on the tick (`ef_sweep_pass`, its
  own 30 s race, 200 objects a pass from its own key at rest in `ef_deferred_sweep`, migration 189) counts the
  objects and bytes as the door's pass counts `atRest`; `/health/invariants.queue.r2.efDeferred` renders that
  count (`atRest`, `round`, `lastPassAt`) beside `atRest`, never from a listing at the health call.
- **L1 (a), the deferred put's digest.** The R2 put of `ef-deferred/<reference>` hands R2 the sha256 of the
  stored stream through the put's checksum option, as door 3's put does, and the `touched` stamp: a corrupted
  upload is refused at the put. (The reference hashes the framed submission, topics included, so it is not the
  object's digest.)
- **L1 (b), the rowless bytes.** The pass over `ef-deferred/` deletes an object no `ef_deferred_jobs` row names
  once it is older than `JOB_KEEP_MS` (seven days) by its age stamp, each read again before its delete, at most
  50 a pass: bytes whose row was never written, or whose release faulted after N3's delete. Limit, stated: a
  re-presentation whose put lands between that read and the delete loses its bytes (door 3's window); its run
  fails and the caller re-presents.
- **The engine's version** is unchanged.

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
