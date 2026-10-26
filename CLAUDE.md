# bsv-overlay-cloudflare

Rust port of [`@bsv/overlay-express`][ts] deployed on Cloudflare Workers.
Two cargo workspaces in one repository (see "Two workspaces"), compiled to WebAssembly. 1:1 protocol parity with
mainline 2.2.0, proven by a differential harness.

[ts]: https://github.com/bsv-blockchain/overlay-express

## Architecture

```
bsv-overlay-cloudflare/
├── crates/
│   ├── overlay-engine/         # Core library — Engine, Storage trait, GASP,
│   │   ├── src/                #   topic-manager + lookup-service traits,
│   │   │   ├── engine.rs       #   merkle proofs. Platform-agnostic.
│   │   │   ├── storage.rs      #   Storage trait (14 methods) + MemoryStorage
│   │   │   ├── topic_manager.rs
│   │   │   ├── lookup_service.rs
│   │   │   ├── advertiser.rs   #   Advertiser trait
│   │   │   ├── builder.rs      #   EngineBuilder for composable config
│   │   │   ├── gasp.rs         #   GASP sync protocol
│   │   │   └── types.rs        #   Shared types; re-exports bsv-rs overlay types
│   │   └── tests/              # Unit + integration + cross-SDK + property + live
│   │
│   ├── overlay-discovery/      # SHIP/SLAP/UHRP/Agent/DmDelegation plugins
│   │   └── src/
│   │       ├── ship/           # SHIPTopicManager, SHIPLookupService, storage trait
│   │       ├── slap/           # SLAP equivalents
│   │       ├── uhrp/           # UHRP advert topic manager + lookup
│   │       ├── agent/          # Agent registry topic manager + lookup
│   │       ├── dm_delegation/  # Delegation-revocation topic manager + lookup
│   │       ├── advertiser.rs   # WalletAdvertiser (PushDrop create/parse)
│   │       └── validation.rs   # BRC-87 names, URI validation
│   │
│   └── overlay-cloudflare/     # Cloudflare Workers deployment
│       ├── src/
│       │   ├── lib.rs          # #[event(fetch)] + #[event(scheduled)] + #[event(queue)]
│       │   ├── routes.rs       # All 27 mainline-parity HTTP handlers
│       │   ├── d1_storage.rs   # D1-backed Storage impl
│       │   ├── d1_discovery.rs # D1-backed SHIP/SLAP/UHRP/Agent/DmDelegation storage
│       │   ├── advertiser.rs   # CloudflareAdvertiser (SHIP/SLAP issuance)
│       │   ├── ban_storage.rs  # D1BanStorage (BanService equivalent)
│       │   ├── janitor.rs      # Background health-check sweep
│       │   ├── peer_crawler.rs # Non-GASP peer bridge (/lookup + /submit)
│       │   └── wallet/client.rs# BRC-31-authed wallet-storage HTTP client
│       └── wrangler.toml
│
├── workers/Cargo.toml          # The LOW workers' workspace (own Cargo.lock):
│                               #   overlay-cloudflare, low-app-layer,
│                               #   low-proof-replay (crates/, see below)
│
└── parity-harness/             # Rust CLI that diffs our Worker vs the
    ├── src/                    #   reference @bsv/overlay-express@2.2.0 docker
    └── corpus/                 #   43 JSON request/response scenarios
```

## Two workspaces (bsv-low #553, 2026-10-06)

The ROOT workspace (`Cargo.toml`: `overlay-engine`, `overlay-discovery`,
`parity-harness`) is what a consumer pins by git rev. It must build from a
fresh clone of this repository ALONE, so no member may carry a path
dependency that leaves the repository. Cargo reads a path dependency's
manifest even when it is `optional`, so a feature gate does NOT satisfy this
(measured: `failed to load manifest for dependency low-core`).

`workers/Cargo.toml` is a second, virtual workspace with its own
`workers/Cargo.lock`: `crates/overlay-cloudflare`, `crates/low-app-layer`
and `crates/low-proof-replay` (each names it with `package.workspace`).
These link LOW's private `low-core` / `low-wire` by PATH from a `bsv-low`
checkout that must sit beside this repository. `low-proof-replay` is the
`LOW/proof/v1` replay (it was `overlay_discovery::proof::replay`);
`ProofLookupService` takes it as a hook (`with_prover`, a
`proof::BundleProver`), and with no prover it records `bundleValid = NULL`.
Both workers pass `low_proof_replay::prove_bundle`.

Cargo commands for a worker need `--manifest-path workers/Cargo.toml` (or
run inside the crate directory). `make ci` runs both workspaces.
`make ci-deploy` runs `scripts/check-workspaces.sh` first: it refuses any
`path` in a root-workspace manifest that resolves outside the repository, and
any entry of `[workspace.package]` / `[workspace.dependencies]` that differs
between `Cargo.toml` and its hand-kept mirror in `workers/Cargo.toml`.

## Dependencies

- `bsv-rs` — BSV SDK for Rust (crates.io, `overlay` feature)
- `bsv-middleware-cloudflare`: BRC-103/104 auth middleware for CF Workers (crates.io), pinned exactly `=0.5.0` in both workers crates since 2026-10-10 (align-engine-050), so the workers' lock holds ONE middleware (0.5.0) beside `bsv-middleware-core` 0.2.0 and ONE bsv-rs (0.4.3): 0.5.0 is on bsv-rs 0.4, so `overlay-cloudflare`'s `WorkerStorageClient` takes the workspace's own `ProtoWallet` and the bsv-rs 0.3 bridge (`bsv-rs-03`) is gone; 0.5.0's breaking move is the payment verdict (`PaymentVerifyError::Unverifiable { reason }`), which no call site here uses. Before it, `=0.4.1` from 2026-10-09 (bsv-low #588: `low-app-layer` at `b7777bd`, `overlay-cloudflare` at `2749841`). 0.4.0 was a major bump: it moved the rules to `bsv-middleware-core` 0.1.0, re-exported at the 0.3 paths, the `AuthSessionStore` DO and the session lane (D12) unchanged; 0.4.1 answers every authentication refusal as its own 401 `{"status":"error","code":"ERR_INVALID_AUTH"|...}` where 0.3.8 raised four as `Err`, which the app layer's front door rendered 400 `authentication handshake failed: ...`; an `Err` is now a fault only and is still that 400. Before it, `=0.3.8` from 2026-10-08 (bsv-low #577: the registry 0.3.8, main `c39642f`, the git `[patch]` to the session-lane branch gone; 0.3.7 had folded the session lane into main). Never 0.3.7.
- `worker`: Cloudflare Workers Rust SDK, pinned `0.8` (the workers' lock holds 0.8.7 since REPIN-080-OVERLAY, 2026-10-21; it held 0.8.5. 0.8.6's one behaviour change: `D1Result::results()` answers `Err` on a row that does not deserialize where 0.8.5 panicked, a wasm trap; `D1PreparedStatement` gains `IntoFuture` and `#[must_use]`. 0.8.7: MSRV 1.91. The queue's hidden `attempts`, R2's `custom_metadata` through `Object::keys` and `delete_all`'s delegation, stated below at 0.8.5, are unchanged at 0.8.7)

**THE WORKSPACE MAY HOLD EXACTLY ONE `worker` VERSION** (bsv-low #348). `worker-build`, which runs only at DEPLOY time, resolves `worker` from the *workspace* `Cargo.lock` (the workers' one, `workers/Cargo.lock`; the root lock holds no `worker`) and takes the LOWEST version it finds there, for every crate, regardless of which crate directory it was invoked from; its per-crate disambiguation is dead code (off-by-one in `Lockfile::get_package_version`, verified in worker-build 0.7.5 / 0.8.4 / 0.8.5). Plain `cargo build` is perfectly happy with two majors, so a split is invisible to every native gate: `low-app-layer` sat undeployable for a month with `make ci` green throughout. If a crate ever needs a different `worker`, it must leave the workspace *and* stop depending on anything in it: a path dev-dep drags the other version straight back into the lock. `make ci-deploy` (part of `make ci`) is what enforces this: a lock/pin preflight plus a real `wrangler deploy --dry-run` of all three deployable configs.

## HTTP route set

27 routes total, matching `@bsv/overlay-express@2.2.0` exactly:

```
GET  /, /health, /health/live, /health/ready,
     /listTopicManagers, /listLookupServiceProviders,
     /getDocumentationForTopicManager,
     /getDocumentationForLookupServiceProvider
POST /submit, /lookup, /arc-ingest (gated on TAAL_API_KEY),
     /requestSyncResponse, /requestForeignGASPNode,
     /requestForeignGASPNodes (engine 0.2.1)
GET  /admin/config (unauth)
GET  /admin/stats, /admin/ship-records, /admin/slap-records,
     /admin/bans
POST /admin/health-check, /admin/ban, /admin/unban,
     /admin/remove-token, /admin/syncAdvertisements,
     /admin/startGASPSync, /admin/evictOutpoint, /admin/janitor
```

Admin routes except `/admin/config` require `Authorization: Bearer <ADMIN_TOKEN>`.

The observability counters (#627, #625, #640/#632) are durable `ops_counters` rows served by `/health/invariants`: `submit_answer_{status}_total`, `submit_answer_reason_{slug}_total` (35 fixed reason classes, `terminal_judged_not_final` the 34th since #836, `stored_echo_under_eviction` the 35th since B-DOOR-3: #612's POST echo and #636's probe refused under an OPEN eviction, the probe's 30 minute arm included; an answer whose arm names none counts `unclassified`, and `admit` is named only where the admission went durable), and `courier_{door|janitor}_{arcade|bitails|woc|bananablocks|other}_{ok|notfound|fault|shape}_total`. The door reader covers submit source, status and terminal-presence reads; the janitor reader covers completion, spender and history ladders, including admin completion. Body-shape failures are `shape`, transport and HTTP failures are `fault`, and every provider's 404/410 (Arcade, Bitails, WoC, BananaBlocks; the code does not know whether the subject is mined) and pruned Bitails' no-spend-data answer (`spent: ""`) are `notfound` (the ladder still reads the latter as "could not look"). Since bsv-low #868 BananaBlocks' native `/tx/{txid}` presence rung (`bananablocks_presence`) counts under its provider's rows, and the presence fold reads Bitails' 404 as "could not look" while its row stays `notfound`. The LEGACY rows `courier_{arcade,bitails,woc}_fault_total`, the `[proof]` log line and the admin probe's `parseResult` keep a proof rung's 404/410 a fault (`http-error`), exactly as before (the design's baseline rows); only the new family reads it `notfound`. The S2 queued 200 is its own reason, `admit_queued`. Each HTTP answer flushes its status, reason and existing operation deltas in one post-answer D1 batch (`/admin/complete-proofs` flushes its courier deltas in `ctx.wait_until` too); the deferred EF job's run (`submit_resumed`) contributes operation and courier deltas only, with no answer counted, and the mutation queue's own replay (`queue.rs`) contributes neither family. Every registering post carries one transaction under its own txid; the full ancestry feed to `/txs` disables callbacks, bounded ancestor registrations precede the subject, and known-txid registrations remain suppressed. `arc_ingest_unauthorized_{bad_token,no_token}_total` stays distinct, and refusal logs name the push with bearer shape and length only. Alerts remain OFF.

## Submit verification (reference parity)

`Engine::submit` verifies the subject transaction the way the reference does
(`overlay-express` `Engine.submit`: `await tx.verify(this.chainTracker)`,
ts-sdk `Transaction.verify`) in every mode except `historical-tx-no-spv`
(that mode exists for GASP `finalizeGraph`, whose graphs `validateGraphAnchor`
already verified). Since 2026-09-08 the walk is
`bsv_rs::transaction::Transaction::verify` (`engine.rs`
`verify_spv_like_the_reference`): a transaction WITH a merkle path has its
root checked against the engine's `ChainTracker` and is then trusted; one
WITHOUT has EVERY input's unlocking script EXECUTED against its source output
(`bsv_rs::script::Spend`, OP_PUSH_TX-aware, ts-sdk `Spend` flags) and its
sources walked in turn. With NO chain tracker the walk is the reference's
`tx.verify('scripts only')`: roots are accepted unchecked, scripts still run.
The engine adds the one rule of the reference's walk that bsv-rs omits: an
unproven transaction may not create satoshis (`outputTotal > inputTotal`).

Before 2026-09-08 this was a divergence: a tracker-less engine checked
nothing, a tracker-ful one checked BEEF structure plus roots only
(`Beef::verify_valid`), and no script was ever executed, so an invalid spend
that no broadcaster had yet refused (or one arriving as `historical-tx`)
was admitted on structure alone.

Failures are classified so an operator can tell a bad SPEND from a bad PROOF:
`EngineError::ScriptVerificationFailed { subject_txid, input_index, reason }`
(the interpreter refused that input; `reason` is its own message) versus
`EngineError::SpvError` (bad or unverifiable proof, missing source, chain
tracker fault, the value rule). Both answer 400 on `/submit`.

**Switch:** `EngineBuilder::with_script_verification(bool)` /
`Engine::set_script_verification(bool)`, DEFAULT ON. `false` is an ESCAPE
HATCH, not a mode: it restores the pre-2026-09-08 structural check (for an
operator who must admit a body the interpreter wrongly refuses while the
defect is fixed). Executable proof, incl. two REAL mainnet OP_PUSH_TX
covenant legs and a deliberately expensive spend whose verification time is
printed: `cargo test -p bsv-overlay-engine --features memory-storage --test
script_verification -- --nocapture`. Measured natively, release profile
(2026-09-08): the real 3150-byte Poc5 covenant settle is ~1 ms end to end; a
~7 KB lock / ~20 KB unlock / ~7000-opcode spend that hashes 46 MB is 137 ms
in `Transaction::verify` (2.1 s in a debug build). Workers wasm is slower
than native; budget CPU accordingly for big covenant legs. (Since #592 that
spend is past the submit's work limb at submit: below.)

### The submit's walk takes the door's two limbs (bsv-low #592; the owner's ruling 3a of 2026-10-10)

Until #592 the walk above ran with no memory charge and no work bound:
219.7 MB natively (217,765,812 measured by the pin) for a valid BEEF whose
unproven parent carries 1.9 MB of push-only unlocking script, an isolate kill
on Workers (128 MB). The door below bounded the gated arm's pre-broadcast
walk only. The reach was every mode that walks: the ungated modes
(operator-only under `SUBMIT_ENFORCE`), `/admin/readmit`, the peer crawler,
the GASP anchor check (#557), and above all the QUEUE REPLAY: every gated
submit enqueues its replay (S2) and the consumer re-submits it under
`historical-tx`, which walks (the gated arm's own synchronous admission is
no-spv and does not). A stranger's valid 1.9 MB parent passed the door,
broadcast, admitted, then killed the consumer's isolate on each redelivery
until it dead-lettered.

**One walker.** `Engine::submit`'s walk (`verify_beef_linear`, every mode but
`HistoricalTxNoSpv`) and the GASP anchor check (`verify_spv_like_the_reference`)
now run the DOOR's stream walk (`script_door::walk_trusting`, the function
`verify_scripts_only` runs) under the engine's `DoorBudget`: the same two limbs
(64 MiB of work, 48 MiB of memory), the same estimates charged from the bytes
before anything is parsed or run, the same 128 KB interpreter element limit.
The walk and `DoorBudget` were already in `overlay-engine` (since #585), so
nothing moved; the worker's door is unchanged byte for byte. What the submit
adds after the walk: the roots of the proven transactions it reached, in its
order, asked of the chain tracker once per BUMP (computed and accepted with
none). A body with BOTH a refused script and a bad root now names the script
(the old walk checked them in one order; stated, not pinned).

**The budget is configuration.** `EngineBuilder::with_walk_budget` /
`Engine::set_walk_budget`, `DoorBudget::DEFAULT` unless set (a library
consumer, Zanaadu, gets 64 MiB / 48 MiB). The worker sets
`WORKER_WALK_BUDGET` (`lib.rs`, equal to `DoorBudget::DEFAULT`, under which
the gated door walks; pinned equal) and the engine hands it to the GASP
anchor check (`OverlayGASPStorage::with_walk_budget`).

**A breach is "the walk could not run"**, never a refusal, and (the E592 lens
fold, 2026-10-10) NEVER AN ADMISSION WITHOUT THE NETWORK'S WORD. As built at
`4060ffd` the submit went on as `HistoricalTxNoSpv` on every walking path, and
the lens found that NOT SOUND (E592-H1): the work limb is a static estimate
from the bytes, a one-byte unlocking script `OP_CHECKMULTISIG` was charged
3,971 keys (about 260 MB) and passed it before anything ran, and the roots
were skipped with the scripts, so a stranger could spend a held coin with no
signature on the ungated modes (public wherever `SUBMIT_ENFORCE` is unset, as
in `wrangler.toml`), the peer crawler and the GASP anchor. The rule now is the
caller's `WalkBreachPolicy`, an explicit engine argument, never inferred from
the mode:

- `NotNow` (every door's default: `Engine::submit`, `submit_with_report`):
  `EngineError::WalkCouldNotRun(WalkCouldNotRun)` before any manager is asked.
  Nothing judged, nothing written, never a final refusal. The way out is the
  subject PROVEN: a proven transaction is not walked, so an honest body too
  big to walk unmined lands after its block.
  - The ungated `/submit` (`current-tx`, `historical-tx`) and `/admin/readmit`
    answer 503 `{"code":"walk-could-not-run","limb":..,"retryable":true,
    "retryAfterSecs":600}` with `Retry-After: 600` (`routes::walk_not_now`):
    nothing written and NOTHING QUEUED (the same unproven bytes would breach
    again). A client retries after the block, not at once.
  - The peer crawler keeps the report (`submit_with_report`), counts the stop
    (`CrawlResult::walks_could_not_run`), admits nothing, and crawls it again
    next tick.
  - The queue replay of a message WITHOUT the gated mark: the error is classed
    `not_now` (`queue::replay_error_class`), so after its retries it rests as
    a parked `not_now` letter under #576's bounds (one per txid, 200 a day),
    never a `fault` letter and never an admission.
  - The GASP anchor HOLDS (`GASPError::AnchorWalkHeld`): below.
- `NetworkAccepted` (`Engine::submit_with_report_under`): the network's accept
  of these very bytes is on record. Used by ONE caller: the queue consumer, for
  a message the broadcast-gated arm's producer marked (`MutationMessage::gated`,
  `"gated":true` on the wire only when set; `queue::replay_breach_policy`). The
  submit goes on exactly as under `HistoricalTxNoSpv`, reported on
  `MutationReport::walk_could_not_run` (`subject_txid`, `at_txid`,
  `subject_judged`, `limb`: `OverWork`, `OverMemory` or `InterpreterMemory`;
  `what`), durable, acked, never dead-lettered for the walk's size.

Stated, found by the fold: the gated arm submits, and enqueues its S2 replay,
under its engine mode `HistoricalTxNoSpv` (`SubmitAction::engine_mode`, `AdmissionPath::NetworkGated`),
so its replay message carries `historical-tx-no-spv` and does NOT walk. #592's
premise that "the consumer re-submits it under `historical-tx`" does not hold
on this tip; the `NetworkAccepted` arm is the rule for a gated message that
walks, and no production message reaches it today. What walks in the queue is
an UNGATED submit's fault replay (its own mode), which is now "not now".

**The roots of a breached walk are checked** (both policies): every proven
transaction the walk reached and every proven SOURCE an input read before the
breach (`WalkTrace::proven_sources`, recorded when the input reads its lock),
once per BUMP; a bad root is the `SpvError` it is on a walk that ran.

**A static refusal is a refusal, never a breach.** An unlocking script that is
not push-only is refused BEFORE any charge, in the interpreter's own words
(`ScriptVerificationFailed`, "Unlocking scripts can only contain push
operations, and no other opcodes."; `script_door::push_only`, no allocation,
pinned equal to bsv-rs's `Script::is_push_only` by `e592f_h1_push_only_is_the_sdks`).
bsv-rs 0.4.3 enforces it at EVERY transaction version (`REQUIRE_PUSH_ONLY_UNLOCKING`,
`Spend::new`, not version-conditional, unlike MINIMALDATA/LOW_S/CLEANSTACK/NULLDUMMY),
so the check is applied to v1 and v2 alike. It holds across the WHOLE walk,
not per input (the E592 delta lens, D1-L1), once the frame's memory limb
admits the stream (a body over that limb at the frame answers
`WalkCouldNotRun` before any script is read; the delta-2 lens, NOTE-3): a PRE-PASS reads the unlocking
script of every input of every transaction the walk can reach, in the walk's
order, before any charge (`script_door::first_not_push_only`, O(script
bytes), no parse), so a non-push unlocking script on input 1, or on an
unproven ancestor, is refused even when input 0's lock would breach the
census first (it was `WalkCouldNotRun`); the per-input check stays as the
belt, unreachable while the pre-pass visits a superset of what the walk
executes, so no pin guards it (the delta-2 lens, NOTE-2, a mutant run). The pre-pass reads only the unlocking scripts, so it can name a
refusal where the walk would have stopped earlier at a structural fault (a
source output out of bounds, the value rule): both are bodies the network
refuses. At the gated door the pre-pass therefore refuses (400
`script-refused`, `submit_script_refused_total`, nothing broadcast) a body
that before it was left to the network for a breach or a structural fault;
reference parity (the SDK's `Spend` refuses a non-push unlock at every
version); LOW's own spends never take this path (the delta-2 lens, NOTE-1). Pin `e592_d1_l1_a_non_push_unlock_anywhere_is_refused_before_a_breach`
(`--test engine_walk_budget`, under both policies; RED with the pre-pass
inert). The census may still over-estimate
a hostile LOCK (a bare `OP_CHECKMULTISIG`, 513 hash opcodes): that is the
policy's case, "not now", not a refusal. What a breach costs now: an unlocking
script can no longer trip the work limb (it is push-only, so no hash or
signature opcode is charged in it); a breach needs a LOCK, i.e. a source
output in the walk, at least one byte of lock in a fabricated parent
(`OP_CHECKMULTISIG`, charged 3,971 keys at the 64 KiB floor). Its effect is
"not now" everywhere but a message the network accepted. Not measured in this
lane: the smallest such body in bytes.

**The landing trusts only a walk that ran** (E592-M1): the carried
predecessors' trust set is `walk_cover` of the subject only when the subject's
walk ran (`Landing::subject_walk_ran`); after a breach each carried predecessor
is walked by its own submit, and a landing's own breach is reported on
`MutationReport::landed_walks_could_not_run` (under `NotNow` that landing does
not happen and the successor waits) and counted by the worker. A body a
breached landing went past (`NetworkAccepted`) is not added to the trust set.

**Every walking door counts** (E592-L3): `ops::note_engine_walk` (a breach
that went on, and every landing's) and `ops::note_engine_walk_not_now` (a "not
now") at the queue, `/submit`, `/admin/readmit` and the peer crawl (counted INSIDE the crawl as each
submit returns, the cron's and `/admin/crawlPeers`' alike, so a crawl the
cron drops at `PEER_CRAWL_BUDGET_MS` keeps what it finished; they were
counted after the race and lost with it: the E592 delta lens, NOTE-3), into
`submit_engine_walk_over_budget_total` (work, interpreter memory) or
`submit_engine_walk_over_memory_total`; the anchor's hold into
`gasp_anchor_walk_held_total` and `anchor_walk_held_graphs` on both GASP sync
lines (`TopicSyncResult::anchor_walk_held_graphs`). A clean FALSE from the
interpreter is the refusal it was; a structural fault is the `SpvError` it
was, in the same words. The reference (`Engine.submit`, `tx.verify`) has no
budget: this is the platform's addition, on D8's lineage.

**One budget per engine** (the lens's NOTE-2): `Engine::verify_scripts_only`
(the gated door) now walks under the engine's `walk_budget`, not a hard-coded
`DoorBudget::DEFAULT`; the worker builds its engine under
`WORKER_WALK_BUDGET` (= `DoorBudget::DEFAULT`, pinned).

What it costs: the cost of the budget falls on bodies the walk cannot hold,
and none of them is now admitted unrun except under the network's accept. A
VALID body past the limbs (the hash-heavy and `OP_CAT` shapes, a 1 MB `OP_NOP`
lock, more than 1,024 signature checks in one walk, the ~7000-opcode spend
above, an element past 128 KB) is "not now" on the ungated doors until it is
presented proven; an INVALID one is never admitted (it was, as built at
`4060ffd`). Measured natively (debug), the witness under `historical-tx`:
217,765,812 bytes on `54dbb16`; 13,312,867 under `NotNow` and 15,211,772
under `NetworkAccepted` on this tip. The bulk is the ENGINE's own submit, not
any route (the lens's L1; pin (a) calls `Engine::submit_with_report_under`
directly): `run_validation`'s `parse_beef` and `transaction_from_beef` of the
1.9 MB body, and, when it goes on, the atomic BEEF built for the lookup
services and the stored copy; the walk itself adds hundreds of bytes (the lens
measured 764 against no-spv). Not measured on wasm32.

Pins: `cargo test -p bsv-overlay-engine --features memory-storage --test
engine_walk_budget -- --nocapture` (`e592_a_the_witness_*`: not now under
`NotNow`, goes on under `NetworkAccepted`, both peaks under the limb plus the
body; RED on `54dbb16` with the report's field unread: "the submit held
217765812 bytes (the memory limb plus the body is 52231890): Ok(MutationReport
{ faults: [], applied_topics: [\"tm_test\"], .. })";
`e592_a_each_limb_is_named_not_now_or_goes_on_by_the_policy`,
`e592_the_budget_*`; `e592_c_*`, parity); `--lib e592f` (`push_only` against
the SDK); `--test script_verification` (`door_over_budget_*_and_the_submit_is_not_now`,
`large_spend_verification_cost`: not now, then admitted under
`NetworkAccepted`). The worker's `--lib e592` (`beef_door_replay::e592`:
`e592_b_the_gated_replay_goes_on_and_an_unmarked_one_is_not_now`, the
witness's message by key with and without the mark, the mark on the wire only
when set, the policy read off the message, a REAL engine under the Worker's
budget: marked, applied, acked, its object deleted; unmarked, `WalkCouldNotRun`,
classed `not_now`, nothing written; `e592f_the_route_answers_*`, the 503 body;
`e592_d` the wiring of every door's count, the gated mark set by the gated arm
alone). Each new pin uses API this fold adds and does not compile on `4060ffd`.
Not committed, stated: the lens's four reproduction bodies (a forged spend of a
held coin) and a GASP-level pin over a test peer serving such a root; the
classes they reach are pinned by the policy pins above and the anchor's hold
by its code path, not by a forged body. The route tier (an ungated
`historical-tx` of the witness answering 503 and bumping the counter) is not
run in the lane.

## The script door reads the stream (D8, bsv-low #585 door 1; the doors lens fold of 2026-10-09)

`Engine::verify_scripts_only` is the walk the gated `/submit` runs before a
broadcast (register row D8): the reference's `tx.verify('scripts only')`, every
unproven transaction the subject reaches executed, a proven one trusted as it
stands, no tracker asked. Its verdicts are unchanged: only the INTERPRETER
refuses (`ScriptVerificationFailed`, the route's 400 `script-refused`); a
structural fault is `ScriptWalkInconclusive` and the door's own bound
`ScriptWalkOverBudget`, and on both the request goes on and the network judges
(counted, `submit_script_walk_*_total`).

**The rule.** A valid BEEF is never REFUSED for its size or its counts, and
never left unjudged for a count of the body. Until #585 the door had four
bounds on the BODY: 64 unproven transactions, 256 inputs per transaction, 512
KB per transaction (`DoorBudget`), and the parse's own (`beef_limits::parse_beef`,
`ENGINE_BEEF_LIMITS`). All four are gone from the door. What it has is a
BUDGET of two limbs, each estimated from the bytes before it is spent, each a
breach that is "the network judges" and never a refusal. `script_door.rs`
reads the bytes the caller holds three times:

0. THE SIZING (`stream_sizing::estimate`): the frame's lengths and counts,
   with no allocation and no hash, give what the next two reads will hold.
   Past the memory limb the stream is not opened.
1. THE INDEX: bsv-rs 0.4.0's `BeefStream` cuts the body one element at a time;
   the door keeps, per raw transaction, its txid and the place of its bytes
   (offset, length), and per BUMP the txids its level 0 carries. Measured: 78
   bytes per element (1,500 elements, 117,376 bytes).
2. THE WALK, from the subject, in the order of the walk before #585. A
   transaction is read IN PLACE at its offset (its fields' places, no script
   copied), a source's output by the offset of that output (one word per
   output of a source the walk spent from). No whole-body copy, no hydrated
   `Beef`.

The door does not use `BeefIndex`: it serves no lookup and no offset, and its
fold refuses bodies the door has always walked (an input naming no EARLIER
element, a transaction nothing spends; pin (h)'s parent-and-child BEEF with no
funding).

**The work limb** (`DoorBudget::max_work_bytes` 64 MiB; `memory_limit` 128 KB
is the interpreter's element limit, the unit of the estimate and not a bound on
the BEEF). Charged per input from the bytes before that input runs: script
bytes + hash opcodes x 128 KB + signature checks x `max(the transaction's
bytes, sig_check_floor)`. A breach, and the interpreter's own memory limit
tripping, are counted `submit_script_walk_over_budget_total`. What the count
bounds held down is otherwise linear in the body: each element is cut once,
each judged transaction laid out once, each source's outputs located once; and
an input whose scripts hold no signature check is run with NO copy of its
transaction (the interpreter reads the other inputs and the outputs only for a
signature's digest; that copy per input was the O(n^2) the 256-input bound was
for).

**The signature floor** (`DoorBudget::sig_check_floor`, 64 KiB; the lens's
E585-D12-M1). Until the fold a check was charged its transaction's bytes
alone. The digest follows the transaction; the EC verification behind it does
not (about 90 us native, whatever the transaction weighs), and the 64
transactions and 256 inputs #585 removed were what bounded the verifications
of SMALL transactions: a chain of 106 byte `OP_CHECKSIG OP_NOT` transactions
bought a verification for 152 bytes of budget, and 17,000 of them (1.8 MB)
walked `Ok` at 3.9 % of it (90,000 at 20 %, 9.6 s native, by the lens). With
the floor the budget bounds the EC verifications of one walk at 64 MiB / 64
KiB = 1,024. Measured natively: 1,000 walk in 87 ms in the release profile
(1.4 to 1.8 s in a debug build), and the 17,000 stop at the 1,024th in 108 ms.

F was chosen against LOW's shapes, as the pin
`door_low_shapes_sit_under_a_quarter_of_the_work_budget` measures them (before
the floor in brackets): the real covenant settle and refund (5 hash opcodes, 4
signature checks: a CHECKSIG and the stated `OP_3` CHECKMULTISIG) 924,112
bytes each, 1.4 % of the budget (676,284); a coin 30 P2PKH hops deep
5,902,188, 8.8 % (3,941,856). LOW's JOIN is TWO seats (bsv-low
`template_tx_sizes.rs:76`; the real funding fixture of the covenant legs is
one: two P2PKH inputs, the 3,150 byte pot lock, no change). The pin builds it
over the real pot lock with each seat's coin 40 unproven P2PKH hops deep, the
deepest unproven ancestry the fleet's record names (bsv-low
`DECISION-LOG-spite-relay-2026-07.md:1327`, a JOIN-funding hop's "~40-tx"
ancestry, before the harness fetched proofs): 82 inputs, 16,132,638, 24.0 %
(the delta lens's E585-D12-DELTA-N5; at 42 hops a seat it is past a quarter,
and it walks to its end up to 169 hops a seat). The nine-seat JOIN three
hops deep (36 inputs, 7,082,619, 10.6 %; 4,740,838 before the floor) is NOT
LOW's shape: it is kept as a STRESS shape.
A P2PKH input is charged 196,739 bytes (it was 131,394), so 341 of them in one
walk are the budget (it was about 490) and 85 a quarter of it. At 128 KiB the
same shapes would be 14.1 % and 11.7 %, and 256 P2PKH inputs the budget: the
lower floor keeps more of a valid BEEF judged for a bound of 0.1 s.

What the floor changes for a body under the old bounds (the frozen table, 13
of its 39 rows): the `work` figure of every walked input with a signature
check; and a CHECKMULTISIG whose key count is a COMPUTED value, which was
charged the most keys the element limit admits (3,971) at its transaction's
bytes and run, is now 260 MB by the estimate and never run at the door (the
network judges). That closes one of `DoorBudget`'s three open residuals.

**The memory limb** (`DoorBudget::max_memory_bytes`, 48 MiB; the lens's
E585-D12-L3). What the door holds beside the body follows the body's element
COUNT and a BUMP's LEAVES, not its bytes. Measured natively by the lens and
again by the fold's pin: 77,086,752 bytes of heap for a 9,900,010 byte body of
900,000 minimal transactions (the index, 49 bytes a bucket, its tables
doubling), and 108,544,792 for a 9,830,056 byte body holding ONE BUMP of 2^18
leaves (the SDK's `Leaf`s, `bump_root`'s tables, the proven set), each beside
the body in a 128 MB isolate. The sizing read charges, before anything is
allocated (`script_door::DOOR_CHARGES`, each derived from a size and pinned
against it by `e585f_l3_the_charges_are_derived_from_the_sizes`): KEPT, 450
bytes a raw transaction, 96 an input, 24 an output, 114 a txid a BUMP proves;
IN HAND, the heaviest element, a BUMP at 576 bytes a leaf or a raw transaction
at 4 bytes a byte, 360 an input and 168 an output (the stream's element, then
the walk's layout and a signed input's digest view). Each is an UPPER bound
(a growing buffer three times its contents, a hash table three times its
buckets at the fullest load, native sizes): the pin holds the measured peak
under the estimate for six shapes, 1.3 to 5.3 times over. A breach is
`ScriptWalkOverBudget { limb: DoorLimb::Memory }`, counted
`submit_script_walk_over_memory_total`, the stream never opened (the call
holds 250 bytes on both lens bodies), the network judges.

**The scripts' parse is the limb's too** (the delta lens's E585-D12-DELTA-M1).
bsv-rs cuts a script into one 32 byte `ScriptChunk` per chunk (natively; 16
on wasm32), and the door's census and `Spend` each parse and clone both
scripts, a reached signature check clones them again for its subscript, and
the interpreter pushes up to three EMPTY stack entries per opcode (`OP_3DUP`),
which its 128 KB memory limit does not count. None of it is a frame charge:
until the fold a 1 MB `OP_NOP` lock was estimated 4.0 MB and peaked at 67.5
MB, walked `Ok`, and an 8 MB one 540 MB (on a PROVEN source too: never bounded
before #585; on an unproven one #585 had lifted the 512 KB stop). Each input's
two scripts (the locking script of the source it spends, proven or not, and
its own unlocking script) are now charged, beside the frame's estimate and
BEFORE either is parsed (`script_door::script_parse_charge`, from a chunk count
that allocates nothing and is pinned equal to bsv-rs's parse,
`e585f2_m1_the_chunk_count_is_the_sdks`), 512 bytes a chunk and 8 a byte. A
breach is the memory limb, and the input is never parsed. Measured, the peak
of the live heap a script byte (native, a lock of 262,145 chunks so every
growing buffer is at its slackest, the frame's own 4 included): `OP_NOP` 98,
`OP_0` 169, `OP_NOP`s then a reached `OP_CHECKSIG` 162, `OP_3DUP` 241 and with
a reached `OP_CHECKSIG` 258, `0x01 xx OP_DROP` 66.7, `0x01 xx` unlocking 62.5,
ten 100,000 byte pushes then `OP_CHECKSIG` 6.7. THE WORST known is an ERROR
path (the doors delta-2 lens E585-D12-DELTA2-L1): bsv-rs's `Spend::error`
clones the stack into the error, so `OP_3DUP`s over empty entries (uncounted
by the interpreter's 128 KB limit) then a refused `OP_VERIFY` is 265 a byte at
262,145 `OP_3DUP`s and 297 at 349,526 (a stack of 1,048,581 entries, just past
a power of two: the doubled buffer and the clone at once). So 520 a one-byte
opcode (524 with the frame's 4) is 1.76 times the worst natively, and about
3.5 times on wasm32 by arithmetic (a chunk 16 bytes and a `Vec` 12 there,
about 150 a byte; not run); 8 a byte is 3 times the pushes' 2.7 past the
frame. The interpreter's own limit covers the element BYTES (a trip is `limb:
Work`); this charge covers what it does not count: the chunk records and
their clones, the subscript, the stack ENTRIES and the error's clone of them. How it composes: the frame's estimate is the most the walk
holds apart from the input being run (the heaviest transaction in hand counted
once, the interpreter's raw copies of the unlocking script among its 4 bytes a
byte); the scripts' charge is the EXPANSION of one input past those bytes,
held while it runs. `WalkStats::memory_bytes` is the frame's plus the largest
input's charge. What it costs a valid BEEF: an input whose two scripts are
about 96,000 opcodes (or a 1 MB `OP_NOP` lock) is left to the network; data
pushes are 8 bytes a byte (a 1 MB push is 8 MB); LOW's covenant leg (6,608
script bytes) is charged 1,282,178 in all, 2.55 % of the limb.

The default's arithmetic: a 128 MB isolate, less, for a 10 MB request (the
figure the arithmetic is made at: since NL-6 the route caps no body, so a
heavier one is the isolate's), the request's body, the completed BEEF the
route hands the door (a second copy of up to as much) and the EF batch (the
route's 2 MB bound, twice while it is serialized), 24 MB, less 56 MB left for the module, the runtime, the
allocator's fragmentation and what a native estimate does not see of wasm32:
48 MiB, three eighths of the isolate. It lets through a BUMP of up to 72,944
leaves (690 bytes a level-0 leaf), a single transaction of up to about 12 MB
whose walked inputs' scripts are few chunks (above), and a 10 MB body of one-input, one-output P2PKH transactions (191 bytes each,
charged 570: 30 MB). It stops about 89,770 one-input, one-output transactions of 60 bytes (5.4 MB; the L3 pin); a transaction with no input or no output is invalid bytes since bsv-rs 0.4.2 / 0.4.3 and refused by every parse first.

Measured (native, debug profile, `tests/script_door_stream.rs`): a 2,122,796
byte BEEF whose subject is 2,112,195 bytes with 300 inputs over 100 unproven
parents (101 unproven transactions, 400 inputs executed) is walked in about
0.2 s, peak heap of the walk 3,889,524 bytes beside the caller's body (8.6 MB
by the estimate): 1.84x its largest element, which is the stream's one element
in hand (the SDK's copy of the transaction being cut, with its buffer's
slack). That ratio is of a transaction that is mostly one script: a
transaction of 200,000 empty outputs is 5.9x its bytes (10.7 MB for 1.8 MB),
which is what the per-output charge is for.

Parity for bodies under the old bounds: the 31 pins of `script_verification`
unchanged, and the table `tests/script_door_fold.rs` `e585f_n2` (the lens's
N2: 39 deterministic bodies, the door's answer frozen per body, statistics and
error text, and the SHA-256 of the whole). It holds the real covenant legs
intact and tampered, P2PKH in its V1, V2 and atomic frames, a subject that is
proven, absent, not a txid or in upper case, a diamond chain, the value rule, a
funding that is txid-only or absent, a mixed transaction tampered four ways,
the budget's edge on both limbs, the memory trip, `OP_NUM2BIN`, a false script,
and a BEEF cut short, with a trailing byte, with a BUMP flag of unknown bits.
Run on `8c92671` it differs on 13 rows, the fold and nothing else (above).
The lane's 27 bodies run on `d6d2774` were a one-off and are not in the tree;
what `d6d2774` answered that the stream does not: a BEEF cut short is
`BeefParseError` on both, in the stream's words (`invalid BEEF at byte 297:
Truncated { needed: 25 }`), and limit (3) below.

Limits, stated. (1) The door is reached through the gated route, whose own
steps stand BEFORE it and are not this door's. Since NL-6 the route caps no
request and its parses (`ef::missing_source_txids`, the subject's log line,
`ef::beef_to_ef_batch`: `beef_limits::parse_beef`, the streaming door, then a
HYDRATED `Beef`; `EF_BEEF_LIMITS` is a name only, no count or size is
compared) refuse invalid bytes only (since bsv-rs 0.4.2 / 0.4.3 a transaction
with no input or no output among them); then the route's EF work budget
(`ef_deferred.rs` `IN_REQUEST_SUBJECT_EF_BYTES` 256 KiB,
`IN_REQUEST_BATCH_EF_BYTES` 2 MiB): past it NL-6c DEFERS the work (202 and a
poll, the job run by the queue consumer through the same arm, the door
included), never a 429. The pin's 2 MB subject passes the DOOR; through the
route it is deferred, and the job's run meets the door. Those parses hydrate the body before the door's limb is read, and
nothing but the isolate bounds them (the land lens E585-LAND-M1, measured
natively, release, on `7730e20`: a 9,900,010 byte body of 900,000 minimal
transactions peaked at 1,427,616,056 bytes in the route's parse and EF
conversion; since bsv-rs 0.4.2 the route's parse refuses that body at its
first element, `NoInputs`; a body of as many VALID transactions was not
measured through the route; the wide BUMP, 2^18 leaves in 9,830,056 bytes, 190,320,572
in each of its three parses, then the door stops at its memory limb in 250
bytes; on wasm32 both die in the route, the platform's error; identical on
`7730e20`, NL-6's, the stack-lean captain's NL-6c): the door's memory limb
bounds the door, not the route. (2) The body is the caller's
`&[u8]`: the door adds one element and the index beside it, it does not make
the route stream its request. (3) A body the stream refuses for its bytes
(trailing bytes, a BUMP whose nodes disagree, a BUMP leaf flag or a V2 format
byte with unknown bits) that `Beef::from_binary` parsed is `BeefParseError` at
the door (the network judges), where it was walked. Through the route no such
body reaches the door since NL-6: the route's own parse is the same decoder
and refuses those bytes first. A subject txid in upper
case is walked (it was "not in the BEEF"), and so is a transaction with a
non-canonical varint (the SDK's txid was of its re-serialization). (4) A
transaction's length is its raw bytes (it was the SDK's re-serialization: the
same for canonical varints), and a script is the raw bytes (it was the SDK's
parse written back). (5) wasm32 was not measured: neither the time of 1,024
verifications nor the heap under the 48 MiB. (6) The open residuals of the
work estimate are as before and bounded by the body alone: bignum rounds,
`OP_NUM2BIN`'s allocation, and an opcode that copies an element (`OP_DUP`,
`OP_CAT`, `OP_PICK`: charged its one script byte, up to 128 KB copied; the
lens's N7). (7) The memory estimate is conservative, so a valid BEEF can be
left to the network that would have fitted: the walk's own tables are charged
to every transaction though only a walked one pays them, so 10 MB of
one-input, one-output transactions under about 113 bytes each is past the
limb. (8) The sizing read
is a second reader of the frame: a frame the stream reads that it does not
follow would be walked unestimated. The pin
`e585f_l3_the_sizing_follows_every_frame_the_stream_reads` holds it over three
frames, every cut and three changes of every byte (5,358 bodies the stream
reads), and if a later SDK reads a frame it does not, the walk answers "over
budget" rather than run unestimated. Re-run it at every bsv-rs bump. The
"over budget" is answered AFTER that read (the delta lens's E585-D12-DELTA-N1):
the index is built, then the frame is found unfollowed. The door and the census
share the rule (`stream_sizing::StreamEstimate::admission`: `Open`, `Over`,
`Unfollowed`; a frame the sizing does not follow is given to the stream, which
refuses every such frame today, and a read it DOES make is over the limb).
Checking before the stream would turn every malformed body (a BEEF cut short)
from the stream's parse fault into a memory breach, the table's rows with it.
The bound of that unestimated read, if a later SDK makes one: one pass of the
stream over the caller's body, one element in hand and the index (the census:
its two passes), which follows the element count and the BUMP leaves (the
lens's 108 MB for a 9.8 MB body), made once before the answer. (9) The
scripts' charge rests on bsv-rs 0.4.0's parser and interpreter (a chunk of 32
bytes, the clones of `Spend::new` and the subscript, empty stack entries
uncounted): `e585f2_m1_the_scripts_charge_is_above_the_measured_heap` and the
chunk-count pin are re-run at every bsv-rs bump.

Pins. `cargo test -p bsv-overlay-engine --features memory-storage --test
script_door_stream -- --nocapture`: `e585_d1_a` the 2 MB / 300-input body
walked, `e585_d1_b` 260 REAL signatures over 66 unproven transactions walked
at 76 % of the work budget (the pin asks for a fifth of it left; the lens's N5:
its 400 inputs sat at 98 %) and a corrupted one refused at the last input,
`e585_d1_c` the work budget stopping 300 digests of 2 MB from the bytes,
`e585_d1_d` the index pass. On `d6d2774` a, b and c are RED for the bounds
(`ScriptWalkOverBudget ... has 300 inputs (limit 256)`; b, resized, for its
65th unproven transaction, run by the delta lens: "more than 64 unproven
transactions to execute") and d for its
heap bound (355,684 bytes beside the chunk against 133,368 allowed), not for
that word (the lens's N3). `--lib e585` (the door's layout of a transaction
against the SDK's parse; the index per element; the fold's two above).
`--test script_door_fold -- --nocapture`, each run on `8c92671` (the file cut
at its marked line): `e585f_m1_a_chain_of_17000_*` (RED: walked, `Ok(WalkStats
{ unproven_txs: 17000, inputs_executed: 17000, .., work_bytes: 2584000 })`)
and `e585f_m1_the_work_budget_bounds_*` (RED: 152 bytes a check);
`e585f_l3_900000_minimal_transactions_*` and `e585f_l3_one_bump_of_2_18_*`
(RED: walked, the call's heap at 77,086,752 and 108,544,792 bytes);
`e585f_n2_*` the table (RED on its 13 rows); `e585f_n4_*` the lens's N4, the
`sig_ops == 0` shortcut: a mixed transaction whose sig-less input's sequence
(or the lock time, or an output) is changed after signing is refused AT THE
SIGNED INPUT, and seven scripts that read the lock time, the sequence, the
version (`OP_VER`), a hash or `OP_CODESEPARATOR` are judged the same with and
without the copy of their transaction at versions 1 and 2 (green on `8c92671`,
where the shortcut already was). Against the shortcut taken for EVERY input
both are RED by their own assertion (the delta lens's E585-D12-DELTA-N3): the
first walks the mixed transaction (a P2PKH signed SIGHASH_ALL | FORKID beside
two sig-less `0x01 0x42` over `OP_DROP OP_1`) with the signed input FIRST,
then at 1 ("mixed, signed first: intact: Err(ScriptVerificationFailed { ..,
input_index: 0, .. })"; with it at 1 alone the mutant panicked inside bsv-rs);
the second's pairs (the seven reader locks spent by `0x01 0x42`, bare and
behind `OP_0 OP_IF OP_CHECKSIG OP_ENDIF`) guard the SDK, not the shortcut, so
it holds a CONTROL first, that same signed-first transaction walked `OK
inputs=3` ("the control needs its transaction: REFUSED input 0: .."). Below
the marked line: `e585f_a_breach_names_its_limb` and
`e585f_l3_the_memory_estimate_is_above_the_measured_heap`.
`--test script_door_parse -- --nocapture` (the delta lens's DELTA-M1, the file
run on `74c2c15` as it stands): a 1 MB `OP_NOP` lock on an UNPROVEN source and
on a PROVEN one, an 8 MB lock, and the route's shape (a small subject over an
unproven parent whose unlocking script is 1.9 MB of `OP_1`), each stopped at
the memory limb before any parse, at a peak of 1,584,568, 1,587,388,
12,581,644 and 3,151,800 bytes (the pin holds the peak under 4 bytes a byte of
the body and a MiB, and under the limb); RED on `74c2c15`: walked `Ok` at
67,555,578, 67,555,578 and 540,436,602, and the 1.9 MB parent at 136,364,598
before the interpreter's stack limit tripped (`limb: Work`). And
`e585f2_m1_the_scripts_charge_is_above_the_measured_heap` (ten shapes, each
stopped by a limb equal to its own measured peak; RED on `74c2c15`, `OP_NOP`
walked under 25,691,292; the land fold-6 added the two error-path shapes and
asserts the worst it measures is past 290 a byte with the charge at least 1.7
times it: "the worst measured: OP_3DUP to a stack just past 2^20 then a
refused OP_VERIFY at 297.0 a script byte; the charge 524 is 1.76x it", peak
103,810,642; RED on `e2c561c` by the shape's absence, "the worst shape
measured is 258.0 a script byte, the error path's 297 is not among the
shapes").

## The census reads the stream (bsv-low #366; #585 door 2; the doors lens fold of 2026-10-09)

`submit_census::census_verdict` classifies every `/submit` served on an
UNGATED path: would the same bytes have survived the gated arm's pre-network
structural checks? Measurement only: a log line and durable counters
(`/health/invariants.submitReadinessCensus`), never a refusal, never a change
of any request's answer. Three states: `gated-ready`, `would-fail(reason)`,
`could-not-evaluate(reason)`.

**The rule.** A body is CLASSIFIED whatever its size. Until #585 a body over
2 MiB was `could-not-evaluate(body-over-eval-bound)`: not looked at. That
stop is gone (`MAX_CENSUS_EVAL_BYTES`, the verdict's variant), and so is the
census's own bounded parse (`CENSUS_BEEF_LIMITS`). The third state has three
reasons: `mined-claim-unverified`, `subject-ambiguous` and
`ancestry-over-memory`.

The verdict is two things. (1) The gated arm's OWN functions, called on the
same bytes in the arm's order (`ef::beef_to_ef_batch`,
`routes::subject_ef_over_cap`, `ef::proven_subject_raw`): no second spelling
of the arm's checks, as before. (2) The census's one addition, the ancestry
check behind a green (is any data-carrying transaction outside the subject's
in-BEEF ancestor closure?), which read a hydrated BEEF and now reads the
STREAM: `AncestryShape`, two reads through bsv-rs 0.4.0's `BeefStream`, one
element in hand, into a number per raw transaction and a pair per input that
spends another transaction of the BEEF. Measured: 143 bytes for the 5,243,030
byte pin (2 transactions, 1 BUMP); 74 bytes per element over a 1,000
transaction chain.

**One reader** (the lens's E585-D12-L1; the land lens E585-LAND-L2, the land
fold-6). Over the parser of before NL-6 the streaming reader was stricter than
the arm's (a byte after the frame's end, a BUMP leaf flag or a V2 format byte
with unknown bits, a BUMP whose nodes disagree): the arm took those bytes and
would broadcast, and the doors lens fold answered their ancestry from a second,
hydrated parse of the arm, counted apart. Since NL-6 (`fc019c8`) the arm's
parse runs the same bsv-rs decoder as the stream, so it refuses those bytes
first and the census answers `would-fail(parse)` at step (1). The fallback, its
second spelling of the ancestry check (`outside_ancestry_by_the_arms_parse`
over `ef::parse_as_the_arm`) and its counter are GONE (Rule 10, one reader): a
stream that refuses the bytes is NO ANSWER (`Ancestry::Unread`), the third
state, never a green. `submit_census_stream_refused_total` and its health field
`submitReadinessCensus.streamRefused` are removed with it: they were added in
this land (not on `7730e20`), never deployed, and read by no dashboard, cell or
repository (a durable row of that name, if any, is simply no longer served).

**The memory of the ancestry read** (the lens's E585-D12-L3). `AncestryShape`
reads the same stream as the door, twice, so it holds the same element: on the
lens's wide BUMP (2^18 leaves in 9.8 MB) the SDK's BUMP element, 108 MB
natively, twice in sequence. Its own table is small. It is bounded the same
way: `stream_sizing::estimate` under `CENSUS_CHARGES` (the stream's element,
128 bytes a transaction and 24 an input kept) against `CENSUS_MEMORY_BYTES`,
the door's 48 MiB, before the stream is opened, under the DOOR'S rule
(`StreamEstimate::admission`, one function; the delta lens's E585-D12-DELTA-N1:
before the fold the census did not look at an unfollowed frame at all; now a
read the stream makes of a frame the sizing did not follow is
`ancestry-over-memory`, after that read, as at the door, limit (8) of the door
above). Past it the ancestry is NOT
read and the verdict is `could-not-evaluate(ancestry-over-memory)`
(`submit_census_reason_ancestry_over_memory_total`,
`submitReadinessCensus.wouldFailAndUnevalReasons.ancestryOverMemory`): no
green. On the lens's two bodies: the wide BUMP is `ancestry-over-memory` (151
MB by the estimate; up to 87,381 leaves are read), after the arm's own parse
at step (1) held 190 MB natively; the 900,000 minimal transactions of the lens
are `would-fail(parse)` since bsv-rs 0.4.2 (`NoInputs`, the arm's own parse at
step (1)): no body is refused for its transaction count (before 0.4.2 they were
`would-fail(ef-over-cap)`, the arm's 429, after its whole-body parse and EF
conversion of 1,427,616,056 bytes natively, the land
lens E585-LAND-M1) and before any stream read: no body is refused for its
transaction count since NL-6.

**No green on a non-answer** (the lens's E585-D12-L2). The verdict's last step
is one function, `verdict_of_ancestry`: a covered ancestry is the only green;
a stray, a read not made for its memory and a read no reader answered are the
third state. Pinned there (the mutant that answers `GatedReady` on either
non-answer is RED), and through `census_verdict_under` with a body: a valid
BEEF whose BUMP carries 2^12 leaves is `gated-ready` under a budget that holds
its read and `ancestry-over-memory` one byte under it. No body reaches the
other non-answer (`Unread`) through `census_verdict`: step (1)'s parse is the
same decoder as the stream. A stream refusal maps straight to it
(`StreamRead::Refused => Ancestry::Unread`), pinned by
`e585f2_n2_the_fallbacks_non_answer_is_never_green` (re-stated by the land
fold-6: RED against the refusal mapped to `Covered`, and by the shape on
`e2c561c`, where the refusal went to the fallback: "a stream refusal is no
answer, mapped at the one seam").

Where it meets NL-6, stated: `ef.rs` `beef_to_ef_batch` and
`proven_subject_raw` parse with `beef_limits::parse_beef`, the streaming door,
which refuses INVALID bytes only, then hydrate a `Beef` (`EF_BEEF_LIMITS` is a
name only: no count or size is compared). So no body is `would-fail(parse)`
for its size or its counts; the census adds no size or count check of its
own and calls the arm's functions, so it moves with `ef.rs`.
The census has no EF-size check since NL-6c (the arm defers that work, 202,
and refuses nothing for it): a body past the EF budget is classified like any
other; `submit_census_reason_ef_over_cap_total` stays in the read table, never
bumped again. The memory of a census pass is
therefore the arm's hydrated parse and EF conversion plus the small index;
only the index and the stream's element are #585's. The arm's parse runs in
step (1), BEFORE the census's own bound is reached, and nothing but the
isolate bounds it (the land lens E585-LAND-M1: 190 MB natively for the wide
BUMP, 1.43 GB for the 900,000 minimal transactions): on wasm32 such a body is
the isolate's to survive, with or without the census's limb.

Limits, stated. The classification is synchronous on every ungated submit,
and a body past 2 MiB (the route caps no body since NL-6) is now WORK where it
was skipped: the arm's parse, the subject's EF conversion, the sizing read and
two stream reads (each hashes every transaction once); not measured on
wasm32. The durable row `submit_census_reason_body_over_eval_bound_total`
stays in the read table
(`submitReadinessCensus.wouldFailAndUnevalReasons.bodyOverEvalBound`: a total
counted before is still served) and is never bumped again. `beef_limits::CENSUS_BEEF_LIMITS` has no production caller
left (the lens's N6: NL-6's file, theirs to drop).

Pins: `cargo test --manifest-path workers/Cargo.toml -p
bsv-overlay-cloudflare --lib e585 -- --nocapture`. Door 2's: `e585_d2_a` (a 5
MB valid BEEF is `gated-ready`, one whose SUBJECT is the 5 MB is
`gated-ready` too (until NL-6c it was `would-fail(ef-over-cap)`, the arm's
429; the arm now defers it), one with two tips is
`could-not-evaluate(subject-ambiguous)`; RED on `d6d2774`, grafted:
`CouldNotEvaluate(BodyOverEvalBound)`), `e585_d2_the_streams_ancestry_*` (the
stream's answer equals the hydrated parse's, kept verbatim in the test, for
every transaction of every body named as the subject, the client fixture's
bodies and a source written after its spender included),
`e585_d2_the_ancestry_index_*`. The fold's:
`e585f_l1_a_body_the_stream_alone_refuses_is_the_arms_verdict` (re-stated
over NL-6 and by the land fold-6: a trailing byte and a flag byte with unknown
bits, and two tips behind a trailing byte, are `would-fail(parse)` at step
(1); the census source holds no second reader; RED on `e2c561c` by the shape:
"the census reads the ancestry through ONE reader, the stream:
`parse_as_the_arm` is a second"), `e585f2_n2_*` (above),
`e585f_l2_a_body_the_census_cannot_evaluate_is_never_guessed_green` (RED
against both mutants) and `e585f_l3_the_ancestry_read_is_not_made_past_its_memory`.
The pre-#585 pin `census_over_eval_bound_is_uneval_not_a_guess` is retired: it
pinned the stop.

## GASP anchor check (bsv-low #551)

The `historical-tx-no-spv` skip above rests on
`OverlayGASPStorage::validate_graph_anchor` (`gasp_overlay.rs`), the
reference's `validateGraphAnchor`. Before a peer's graph is finalized: (1) the
ROOT node's BEEF goes through the same `verify_spv_like_the_reference` as a
submit, with the engine's chain tracker and script switch (no tracker is
`'scripts only'`); (2) the ordered BEEFs are replayed through the topic manager
over a set of coins (`historical-tx`, the coins as `previous_coins`), and the
graph is discarded WHOLE unless its root is a coin at the end. A refused graph
is counted (`TopicSyncResult::discarded_graphs`), is not an error, and the
cursor advances past it, as in the reference.

Additions to the reference, each stated in the doc comment of
`validate_graph_anchor`: a coin the storage already holds counts as a previous
coin (the walk strips held inputs, so the next head of a head chain arrives
alone); a held source is merged into the checked BEEF of an unproven node;
every node must be an input of its parent; a conflicting spend inside a graph
refuses it; and a chain tracker or storage FAULT is
`GASPError::AnchorUnavailable`, which fails the UTXO so the cursor gap guard
asks again instead of losing it. A topic manager `Err` in the replay is
`AnchorUnavailable` too (the lens fold of 2026-10-06): the manager's contract
there is one sentence, `Ok` with nothing admitted is a FINAL refusal and the
cursor moves (parity: the reference moves it past a refused and a failed graph
alike), `Err` is "not now" and the cursor waits. Its cost: a manager that
errors forever on one UTXO holds that peer's cursor below it, and what lies
above and is not yet held is walked again every tick. A manager whose rule
depends on previous coins must apply it under `historical-tx` too: that is the
mode of the replay.

The finalize submits stop at the first transaction that does not LAND
(`submit_finalized_graphs`: read from the durability report, since `submit`
answers `Ok` on a storage fault and on a manager failure): nothing behind it
in that graph is submitted, and the cursor does not pass that graph (under a
budget its UTXO fails; with none the peer's whole cursor stays for the tick).
Pins: `cargo test -p bsv-overlay-engine --features memory-storage --test
gasp_topic_manager i551` (and `fold_medium`).

The anchor verify ran peer-chosen scripts with no work bound under the Worker CPU cap (bsv-low #557); since #592 it walks under the engine's `DoorBudget`. Since the E592 lens fold a breach is HELD, never replayed: `GASPError::AnchorWalkHeld`, the graph not finalized, the UTXO failed (or, under a per-graph budget, its record kept with nothing pending, reason `anchor_walk_held`), the cursor below it, counted (`GASPSync::anchor_walk_held`, `anchor_walk_held_graphs`, the worker's `gasp_anchor_walk_held_total`). The roots the walk reached are checked first, so a bad proof is still a refusal. The hold ends when the peer serves the root PROVEN: #555's resume re-asks an unproven root and a proven one restarts the walk (`root_proven`), which trusts the proof and runs no script; so an honest 0-conf graph too big to walk lands after its block, a forgery never. What a forging peer costs: its own slot (the UTXO held below the cursor, its record re-asked each pass up to `DEFERRED_GRAPH_MAX_PASSES`) inside its per-peer budget, and D16's yieldless bound quarantines it if it never yields and serves nothing else, which holds ONLY with a per-graph budget (`set_graph_budget`, as the worker sets): the yieldless bound exists only there (the E592 delta lens, D1-L2). With NO per-graph budget (a library consumer that sets none) the held UTXO fails on every sync, the sync runs to its end and counts as a success under #302, the cursor stays below that UTXO for good, and the graph is fetched and its walk estimated again on every tick; nothing is admitted, the cost is ours, the residual D15 states for `AnchorUnavailable`. Not pinned by a test peer in the lane (stated).

## The pre-flight probe's stored word (bsv-low #636, #637, #652; lane B-DOOR-3 and its lens fold)

The pre-flight probe's stored SEEN (Arcade's `GET /tx`, the row's last transition at its stamp) is judged as #612 judges the POST echo (`broadcaster::probe_answer`, the arm's one decision): with no open eviction it admits PENDING in both modes, `network_seen` unlatched at the door; under an OPEN eviction only a stamp after the eviction and inside the 30 minute bound goes on, and a stored word before the eviction, unstamped, or stamped after it but past the bound is `STORED_ECHO_UNDER_EVICTION` (the retryable 502, the ladder skipped, reason `stored_echo_under_eviction`); for the last no door path readmits until its MINED push or the operator (the lens's M1; judging it by the indexers as #519 does is the owner's call, not built). "Never latches" holds at the door: the pending watch latches on the same store read 2 to 18 s later, not on the network's live word (the lens's L1). A probe that answers is a stored Reached echo (`take_probe` marks the duplicate: the fast mode counts it `fast-echo`). Only the SUBJECT's own Reached POST echo marks the duplicate #637 reads (`note_reached_echo`; an ancestor leg's echo does not), and the witnessed duplicate of a STANDING admission is `standing_duplicate_answer`'s pending ack. Pins `--lib bdoor3`, `i636`, `i637`, `i652`.

## The reorg word is chaintracks' typed chain-event feed (bsv-low #620, the words map's L4; #777, #778)

The overlay consumes the SAME feed the app layer's intake reads (#771): `GET /events?since=<cursor>` over the CHAINTRACKS binding (`chain_tracker::chaintracks_events_page`), on its OWN durable cursor (`chain_feed`, `overlay_chain_events`, migration 349, one versioned row written by compare-and-set, `chain_feed::STATE_WRITE`; a lost compare-and-set ends the pass and is counted ONCE, `overlay_chain_events_contended_total`, the W15E-L4 lens fold's LOW-3: the trailing write retried the same stale version and counted a second; a faulted write is still retried once there). Every envelope passes `bsv_tracker::decode_envelope`; one it refuses (an unknown kind or version, a malformed known shape, the feed's `unreadable`, the Go server's kindless shape) STOPS the pull at its cursor, recorded on the row, counted once (`overlay_chain_events_unreadable_total`). A 409 `cursorAhead` is a recorded stop at the stored cursor, never a reset to 0 (a replay re-judges the journal's bootstrap fork). A fresh row starts at the overlay's own `CHAIN_EVENTS_START_CURSOR` (the journal's head); unset, the feed reads nothing (`overlay_chain_events_unconfigured_total`). The operator's lever `GET`/`POST /internal/chain-events-cursor` (bearer `INTERNAL_TOKEN`) reads the row or sets the cursor; an event in hand is then released and RECORDED, never dropped.

The reaction per kind (the crate's `on_chain` set, `chain_feed::selection`): a `reorg` from `forkHeight`, a `fork` from `height` and a `split` from `forkHeight` select the rows anchored in that height up to the highest the event names (deactivated headers, new tip, competing or witness tips); an `invalidated` names no height and its header is no longer served as active, so it selects the last 144 heights below chaintracks' verified tip and the 144 above it (#896, below; stated: deeper is the operator's `/internal/reorg`; the producer does not emit it live yet, CHAIN-EVENTS.md); `tip`, `tipAge`, `frozen`, `splitResolved` select nothing. The selected rows are judged by D7's judgement, now ONE function both consumers call (`arcade_reorg::judge_window_legs`): each confirmed spender by its OWN stored proof against the active root at its height (the root rule, never the hash), a refuted one re-anchored IN PLACE from a chaintracks-verified courier proof before any demotion, demoted only when none is served, bound to the judged height; the pots' own and the hop proofs unlatched when refuted. No corroboration is asked: the event is the header source's own word. Bounds: two feed pages a pass, `PassLimits` (two selecting events on the cron, one on the block-event pass; a leg page each; a window wider than a page held as `pending` with each leg's cursor), the D7 ladder's budget; a window faulting `fault_ceiling` (12) passes is released unresolved with its heal (`POST /internal/reorg {"fromHeight": lo, "toHeight": hi}`). The pass bumps `overlay_chain_events_*_total` and the R2 lifetime totals (`reorg_*`) and clears the app layer's confirmed probe memos on evidence (`hop_probe_memos::feed_pass_rejudged`), as D7's pass does. Served: `/health/invariants.chainFeed` (one statement more on that read).

D7 is gated, not deleted (#620 (c); the W15E-L4 lens fold's MEDIUM-3, Rule 6): the cron and `/internal/tip-changed` run the feed first and run Arcade's orphan consumer ONLY where the feed is not the reorg word on that deployment THIS PASS, read through ONE gate function both call (`chain_feed::arcade_not_an_input`, over `FeedPassSummary::is_reorg_word`: false with no row and no start cursor, a row that did not read, a RECORDED STOP on the row (an envelope the crate refuses, a 409) or a FAULT of the pass (the feed's transport, a page that did not read, an HTTP status, a tip or leg read); the row's own write fault and a lost compare-and-set are not the feed's). As built at `40fec4ce` the word was the row's presence alone, so a recorded stop, permanent by the producer's contract (a kind, version or FIELD the strict decoder refuses, a reorg deeper than 400, a journal reset), switched off the working fallback until the lever: a self-healing failure traded for a permanent one, on the deployment where the kindless `reorgFrom` arm is dead (the app layer forwards height and hash only) and the sweep never revisits a height older than three. Now a stopped or faulting pass hands D7 back for THAT pass (a transport fault counts for its pass only, the lens's reading), the stop stays recorded and served (`/health/invariants.chainFeed.stop`), and a healthy pass (after the lever clears a recorded stop) re-takes the word; both consumers judging one height is idempotent (the lens's L4), so the overlap costs reads, never a double demotion. `POST /internal/arcade-reorg`, the `arcade_reorg_*` counters and `/health/invariants.arcadeReorg` stay until the register edit. Not changed: the R2 sweep, the kindless `reorgFrom` decode on `/internal/tip-changed` (`tip_pass`), and `/internal/reorg`. Pins `--lib e620` (`chain_feed_tests.rs`; a, b, c, d, a2 RED against a kindless selection, b against an unreadable envelope passed, a and four D7 pins against `reanchor_first: false` in the shared function, g against the cron's gate removed). The lens fold's: `e620f_m3` (each stop class, the unreadable envelope, the 409, a transport fault, an unparseable page, an HTTP status, a leg's read fault, no feed pass: the gate says run and D7's consumer, run on the same store, demotes the row the feed did not reach; the healthy journal is the word, a fault's next healthy pass and the lever's clear re-take it; RED against the row-presence word: `left: Some("not an input ..") right: None`), `e620_d` inverted (a 409 hands D7 back) and `lens_l1` re-aimed (RED: "pass 1: a stopped feed is not the reorg word"), `e620_g` on the one gate function at both sites; `lens_l7` (the lens's LOW-1: "never a blind demotion" as BEHAVIOUR, a confirmed spender with no stored bump left alone and counted `no_stored_proof` with no courier word, demoted on a courier proof naming another block; RED under `demote_proofless: true` in `judge_window_legs`: `left: (0, 1, 0) right: (1, 0, 0)`; `e620_g`'s text check stays as the belt); the lens's L2 to L6 and L8 document the edges, the invalidation depth, two consumers on one height, the one-statement health read, a dropped pass and the contention count.

The app layer (#777, #778): `/health.chainEvents` reads the DURABLE row and the intake's `ops_counters` rows in one batch (`chain_events::health_read`; `counters` the durable totals, `isolateCounters` the isolate's, `lastPullAt`, `source: "d1"`; a fault serves the memo, `source: "isolate"`); it read a per-isolate memo only a scheduled pass in that isolate filled. The route serves it through `health_served`, THIS isolate's 10 s memo of that read (`HEALTH_MEMO_TTL_MS`; `memoAgeMs` the age, 0 on a fresh read; `source` still `d1`, it is the durable read's answer; a fault is never memoized and a stale memo is never served over one): one D1 batch per isolate per 10 s whatever the read rate, where the fold found one per read of a public route the census lists (the W15E-L4 lens fold's LOW-2; pin `i777f_l2`, RED against a pass-through). The intake is its OWN scheduled task (`pot_hook::schedule_tick(drain, intake, tracker)`, `pass_meter::SCHEDULED_TASKS` 3), out of reach of the tracker's boot, board era and backfill awaits, and its first statement is the pass's durable witness (`chain_events::witness`: the `tracker_chain_events_last_pull_at` stamp read and written in one batch), so a pass that never reaches the intake is counted by the next that does (`tracker_chain_events_skipped_passes_total`, logged `[chain-events] SKIPPED`), beside `tracker_chain_events_pass_total`. Pins `--lib i777`, `i778` (each RED against its revert mutant).

## Terminal admission and confirmed conflicts (bsv-low #602)

For an Arcade-terminal subject, the courier presence fold retains the mined
height. Mined presence still admits (#214); mempool presence is a live race,
never a readmission or a SEEN witness. It answers retryable while the admission
is young or unknown, and the standing 422 while aged. Mempool presence is not
memoised and is counted as `submit_terminal_judged_race_total`.
A courier-mined acceptance answers `admit;desc="mined"` and leaves
`network_seen` unlatched. Readmitted and courier-mined Accepted subjects start a pending watch, which
must read a broadcaster's SEEN before latching.

Before proof retirement, a bounded conflict pass samples eight live,
unverified pot BEEFs, including retired and readmitted ones. It reads each
subject's actual inputs and the network's spender hints. Only a different,
content-addressed transaction consuming the same input with an indexer's
positive mined height triggers `evict_txid_everywhere`, the existing reversible
shadow move. Mempool competitors, unrelated bytes and courier faults evict
nothing. Failed reads remain candidates for a later pass; this sampling gives
no fixed convergence deadline. The fetcher has a separate 24-call budget.

Stated (the E602 lens fold, 2026-10-12). The pending watch's and the refusal
callback's evidence checks still read mempool presence as "a courier holds
it" and KEEP a refused pot (the #214 belt): such a pot is evicted by the
conflict pass after its competitor mines, not by the callback. The pass is
counted from 0 (`admit_fast_conflict_scanned_total`,
`admit_fast_conflict_evicted_total`, `admit_fast_conflict_faults_total`) and
summarised on the retire-pass line (`conflict: scanned= evicted= faults=`);
both callers pass the `WOC_API_KEY` secret (none is installed today). A
courier-mined subject's watch cannot latch while Arcade stays terminal: its
`network_seen` comes from the merkle bar or a later live SEEN; a witnessed
accept latches at once and starts no watch. The courier's `blockheight` is
trusted as the chain's word, unverified against chaintracks, for the #214
admission and for a competitor's confirmation: an orphaned block's height
evicts once, reversed by the pot's MINED push; or admits once, taken back by
the conflict pass when a competitor mines or by the retire pass when both
couriers go clean. A candidate whose stored BEEF does not read is counted a
fault and logged (`[conflict] <txid prefix>`). The race word applies
to EVERY Arcade-terminal status, not only a conflict-shaped one: an aged,
non-conflict REJECTED subject one node still holds is the 422.


The one-time historical repair (bsv-low #607) runs from the scheduled tick and
`POST /internal/repair-conflict-spenders` (empty body or `{}`, `INTERNAL_TOKEN`
checked before any database setup). `conflict_repair::REPAIR_LIMIT` is 16
ledger rows per pass, including unrelated reasons, with no courier requests.
The pass uses at most 39 D1 calls including its counter batch, excluding schema
boot and notification delivery; a finished pass uses one state read.
Migration 346 adds `conflict_spender_repair`: the first pass freezes the
ledger's highest rowid, each pass advances its cursor, and the final pass
stamps `done_at`. A completed repair performs only its state read, including
when called by the operator; there is no reset lever. Open conflict reasons
with valid outpoint, competitor and positive height use #605's existing
`record_conflict_spender`. Confirmed and foreign pointers are preserved;
the repaired pointer stays unconfirmed until the chaser verifies its proof.
A write fault leaves that row for retry. Four `conflict_spender_repair_*_total`
counters (`scanned`, `repaired`, `skipped`, `faults`) are served from zero on
`/health/invariants`; the lever returns those pass counts and `done`. Counts
are best effort: a lost checkpoint may replay a page, and a lost counter
flush may under-count. The owed judgement is unchanged. Pins: `e607` in
`--test e602_convergence`.

## The conflict pass holds a void pot (bsv-low #825, B-FLEET-4, 2026-10-23)

The conflict pass draws eight unverified live pots a pass (`CONFLICT_CANDIDATES_SQL`, `ORDER BY RANDOM()`); a pot whose JOIN never reached the network answers nothing at the couriers, so it was re-drawn every few ticks and each re-check was a counted fault (`admit_fast_conflict_faults_total` climbing on nothing new). Now each re-check's outcome is noted in the fault memo `conflict_fault_memo` (migration 343, transient, one row per pot whose last re-check faulted; `ConflictPassPort::memo`, called by the shipped loop before the candidate's counters): a fault adds one to the pot's CONSECUTIVE count (`CONFLICT_FAULT_MEMO_PUT_SQL`), a clean re-check (evicted or not) deletes the memo's row. At `CONFLICT_FAULT_MEMO_BOUND` (3) the hold is armed, `CONFLICT_FAULT_MEMO_HOLD_MS` (24 h) from that fault, counted `admit_fast_conflict_held_total` and logged `[conflict] <txid> HELD`; while it runs the draw leaves the pot out, so the eight places go to pots the couriers can answer. At its end the pot is drawn again; a fault re-arms the hold at once, so a void pot costs one re-check a day after its first three. NEVER HELD: a pot the index holds SEEN (`network_seen`) or funded (`has_proof = 1` in `pot_beefs` or `transactions`), checked at the write AND at the draw, so a latch landing during a hold puts the pot back in the next draw. Never a delete of a pot: the memo's rows only (the clear, and a bounded sweep each pass of rows whose last fault is 7 days old and whose hold ended, at most 64). `admit_fast_conflict_scanned_total` is unchanged, one a re-check, the denominator the monitor's rate rule reads (faults <= scanned). Served: `/health/invariants.conflictPass` {`scanned`, `evicted`, `faults`, `held`, `faultMemo` {`rows`, `held` (holding now and neither SEEN nor funded), `nextReleaseAt`, `maxConsecutiveFaults`, `bounds`}}. Stated: a lost row (transient table) costs the pot three more faulted draws; a pot held for a reason the couriers will later answer waits up to 24 h for its re-check (its #602 conflict eviction too; the retire pass and the MINED push are unaffected). Pins `--test b_fleet_4_conflict_fault_memo` (the shipped statements under real SQLite: RED with the draw's exclusion removed) and `--lib b_fleet_4` (the loop's memo call and the worker's wiring; RED with the call removed).

## The app layer's verified tip and pass freshness (bsv-low #877, TIP-877-FOLD-2)

`GET /tip` serves the verified header's `height`, `hash` and block time `at` in milliseconds, with `source: "verified-header"`. This route alone reads `/getInfo` and independently checks the active `findHeaderHexForHeight` row against `findChainTipHeaderHex` (height, hash, root and time) and the info's serving ceiling. `lastPassAt` is the producer's successful cron-pass timestamp, parsed from SQLite UTC or RFC3339 and served in milliseconds; `ageMs` is its nonnegative age. A pass age over 30 minutes or `lastSeenHeight > height + 1` answers 503 `ERR_TIP_UNAVAILABLE`, `freshness: "stale"`, `stale: true`, with `Retry-After: 60`. A fresh pass answers 200 `freshness: "fresh"` even when the block is old. If `lastPassAt` is ABSENT, a consistent snapshot within the height margin answers 200 `freshness: "unknown"`, `lastPassAt: null`, `ageMs: null`; no sync-age test runs. A present but null or malformed stamp is unreadable and answers the named 503. Neither `lastSyncedAt` nor `lastSeenAt` is a liveness signal or an input to the reader. The height margin still applies without a pass stamp.

An unreadable or inconsistent current snapshot also answers that named 503, carrying the last fully checked route snapshot as diagnostics if this isolate holds one (null facts otherwise). The last snapshot never supplies a 200. Every answer is `no-store`; no app-layer D1 read is added. Other routes, including `/utxo-status`, direct view heights and the owed recompute, read only the verified header, one CHAINTRACKS request per height read, with no `/getInfo` freshness check or its 503 arm. The words pass brackets N proof headers with two such tip reads: `2 + N` CHAINTRACKS calls, doubled only on its one retry. `/tip` alone makes three calls. Every 200 height remains the verified tip; the tower's oracle, client and monitor need no change to read it.

The owed recompute still runs with `tip: None` when its own verified-header read fails. The index's proof decides its rows first, while recovery gates and hop-probe freshness are unknown. NEW-LOW-2 choice: retain the conservative withdrawal of BOTH rejoin and refund-due presses during a tipless compute, including a previously open refund gate. We do not reuse a previous height as a floor for a current gate. This temporarily loses those presses' liveness but creates no claimable row by guessing. The unknown rows are written and pushed, then re-arm on the identity's next read (or hook) with a readable tip. A NULL-tip stored snapshot takes an uncached verified-header read, so a held height cannot keep triggering the refresh; `/internal/tip-changed` alone does not re-arm it. Proof-decided payouts remain claimable throughout. The binding wrapper leaves fault logging to its caller.

Deployment of overlay PR 172 stays held until chaintracks 0.7.1 is on LOW's renders: chaintracks-cloudflare #27 adds `lastPassAt`, stamped at the end of every successful cron pass, idle or with work; a failing pass does not stamp it. The older producer at `8e6547a` cannot distinguish a quiet chain from a stalled tracker. The overlay Worker's engine tracker reads the verified header tip too since bsv-low #896 (next section). No merge or deploy is part of this lane.

## The engine's chain tracker reads the verified header tip (bsv-low #896, lane ENGINE-TIP-896, 2026-10-25)

`WorkerChainTracker::current_height` (`chain_tracker.rs`) reads chaintracks' served, VERIFIED header tip, `GET /findChainTipHeaderHex` over the CHAINTRACKS binding (or `CHAIN_TRACKER_URL`), the reader the app layer's `/tip` uses (`value.height`, `value.hash`, `value.merkleRoot`, `value.time`; one chaintracks row, under the validation ceiling the proof headers `is_valid_root_for_height` reads stop at). It read `getPresentHeight`, the couriers' maximum, unaged and unverified (the TIP-877 lens's LOW-3). One request (`verified_tip_with`); the per-tracker 30 s memo (#289) holds the whole tip, success only (no caller reads its hash or root on this tip: the lens's NOTE-2 found `verified_tip()` uncalled and the fold removed it). The tip frame is parsed in two places, this reader and the app layer's direct reader (`tracker_host::verified_tip_with`, its fields checked by `internal_events::parse_header`'s rules): both read the same route and the same four fields (the overlay does not depend on the app layer), so a change to the frame must move both (the lens's NOTE-6). Every answer that is not a well formed verified header is a FAULT, never a height by a guess, never a fallback to the couriers. No `/getInfo` freshness read: no engine caller needs liveness from the tip (below), so the engine pays one subrequest where `/tip` pays three.

The callers, and what a verified tip one block behind the couriers (headers verifying) does to each:
- `ls_low`'s lobby expiry filter (`overlay-discovery` `low::lookup_service`, the per-lobby poll of #289): needs the chain's tip; a table whose expiry the couriers' block passed is listed one verification longer. The one caller that moves the permissive way, display only (the filter is fail-open by design, and no admission reads it).
- The advert reaper (`advert_lifecycle::resolve_tip`, fail-closed): reaps one verification later. Conservative.
- The #836 non-final hold (`door_words::chain_tip`: the door's `lock_bound`, the evidence check and the retire pass through `admit_fast::stored_lock_bound`): `tip + 1 <= nLockTime + 24` holds one verification longer, so Arcade's word turns fatal one verification later. Conservative (no refusal or eviction sooner; the 48 h absence arm unchanged).
- D7's corroboration (`arcade_reorg::EnvHeaderSource::tip`): `TrackerLagging` (Arcade's listing past the tip plus its tolerance) and `Uncorroborated` (the skip at `skip_depth` past the height) each come one verification later: an event is held a pass more, never skipped sooner. Conservative.
- The chain feed's `invalidated` window (`chain_feed`, `Selection::BelowTip`): the one caller whose coverage moved the other way. An invalidation of the tip's own header LOWERS the verified tip and leaves the rows anchored on that header above it, where the couriers' maximum usually reached. The window is now `tip - 144 ..= tip + INVALIDATED_ABOVE_TIP` (144): a row above the verified tip rests on a header chaintracks no longer serves (every proof was checked at or below its tip then); one whose height chaintracks does not yet serve faults the window (its 404 is a fault) until the chain regrows, as on the couriers' window. While chaintracks re-validates (its served ceiling below its stored tip) the verified tip IS that ceiling, so the window anchors there and misses rows on an invalidated header near the real tip that the couriers' maximum covered; bounded: the producer emits no `invalidated` live yet, a miss demotes nothing (the row keeps its confirmation until a later reorg event), and the heal is `POST /internal/reorg` (the lens's LOW-2).
- The GASP storage's `ChainTracker` (`gasp_overlay.rs`) only delegates; bsv-rs 0.7.2 (both locks; it calls `current_height` only in its own tests) asks a tracker for roots only (`Transaction::verify`, `MerklePath`), never `current_height`, so the anchor check does not read the tip (the lens named it; it reads none). The reorg sweep reads the announced tip, and the admission age floors are clock ages: neither reads this tracker's height.

Pins `--lib e896`: `chain_tracker::e896::e896_a_the_engine_tip_is_the_verified_header_not_the_courier_max` (a fixture whose couriers answer 972186 and whose verified tip is 972185: the engine reads 972185, its hash and root, in one request; RED on `8e5e73f7` with the read extracted unchanged, `6778f68e`: `left: Some(972186) right: Some(972185)`), `e896_b_a_chaintracks_fault_is_a_fault_never_a_height` (eleven faults, `getPresentHeight` never asked), `chain_feed_tests::e896_c_an_invalidated_window_reaches_the_rows_above_the_verified_tip` (RED with the window closed at the tip: `left: (1, 1, 1) right: (1, 3, 3)`; its first pass's "demoted" is the fake tracker's `Ok(false)` for an unserved height, where production's 404 is `BlockNotFound`, an `Err`: a second pass gives that answer and asserts the fault, the row kept and the window held, the lens's LOW-1). Not measured on wasm32 or beta: the binding's answer at the route (the app layer's `/tip` reads the same route there).

## The app layer's scheduled pass is bounded and measured (bsv-low #781, #778, B-FLEET-4, 2026-10-23)

The monitor's tip read timed out on some ticks while the app layer's `/tip` answered quickly from outside (#781), and a scheduled pass skipped the chain-event intake silently behind the backfill's unbounded CHAINTRACKS await (#778). (1) THE BOUND: `tracker_host::runtime::backfill` walks its page through `walk_backfill_page_within` under its own deadline, `BACKFILL_WALL_MS` (20 s) less `BACKFILL_SAVE_RESERVE_MS` (3 s, the cursor's save): every header await is raced against it, a dropped one counted `tracker_backfill_header_deadline_total` and answered `deadline_snapshot` (no tip), so the page stops at that subject as on a CHAINTRACKS fault and resumes there next pass. The backfill (20 s) and the intake (25 s, `chain_events::WALL_MS`) fit inside the drain task's 60 s (pinned). Stated: the backfill's D1 reads, writes and save, the tracker tick's events and the sweep are not raced here (D1, not CHAINTRACKS). (2) THE MEASURE: `pass_meter` times every phase of both scheduled tasks (`drain.boot`, `drain.pass`, `drain.flush`, `tracker.boot`, `tracker.board_era`, `tracker.backfill`, `tracker.chain_events`, `tracker.tick`, `tracker.sweep`); the task that ends last logs ONE line `[scheduled-pass] wall_ms=<pass> <phase>=<ms> ...` and writes ONE row, `applayer_scheduled_pass` (migration 344; the app layer runs the same CREATE first, byte for byte), served by the overlay as `/health/invariants.appLayerScheduledPass` {`at`, `startedAt`, `wallMs`, `phasesMs`, `passes`} (null before the first pass). Stated: a task the platform cancels never ends, so its pass writes nothing (the row's `at` stays at the previous pass). Pins `low-app-layer --test tracker_backfill b_fleet_4` (a header await that never answers stops the page at the wall less the reserve, on tokio's paused clock; RED with the race removed: the page never answers) and `--lib b_fleet_4` (the last task writes the pass through the shipped statements under real SQLite, read back by the overlay's read; the worker's wiring).

## A non-final word is "not now"; a rank below SEEN promotes nothing (bsv-low #836, #845)

Loop 44 (record 1066): two pre-signed refunds presented at their lock height (972185) were REJECTED by Arcade `TX_LOCK_TIME (35): lock time (972185) as block height is not less than block height (972172)` while the chain's tip was 972185 (Arcade's PEER thirteen blocks behind), both indexers absent, and the evidence check EVICTED them as a two-source refusal.

- **#836, the non-final word.** `door_words::lock_time_not_final` reads a REJECTED (any letter case) whose `extraInfo` names `TX_LOCK_TIME (35)` (arcade@85bfe5d `propagator.go:1796-1798`, its 476 `StatusNotFinal`) as `NotFinal { lock_height, peer_height, reason }`: the heights out of the text (the peer's a hint beside the word, never a gate input), the reason scrubbed as #519 scrubs a refusal's. It is the one decision that reads `extraInfo` (#213's trap), only under a REJECTED, and only ever turns a refusal into "not now" (the fail-safe direction). The gate's word is unchanged (`Fatal`); what moves: at the DOOR the pre-flight probe's terminal judgement (`terminal_at_arcade_by_presence`), the submit echo and the SEEN poll answer the retryable 502 (`lock_time_not_final_answer`; mined presence still admits), never a 422, never a resubmit's corroborated refusal, judged `TerminalJudgement::NotFinal`, counted `submit_lock_time_not_final_total`, reason `terminal_judged_not_final`; the live look is `ArcadeLook::NotFinal`, on which the evidence check never refuses (an indexer's presence is presence, else `Uncertain`), (a RECORDED lock-time word is any recorded word since the fold below); the pending watch's look is `WitnessLook::NotFinal`: no evidence check, no eviction, counted once a watch (`submit_pending_lock_time_not_final_total`), the watch keeps looking and ends with the subject unlatched; the `/arc-ingest` push is counted `arc_ingest_lock_time_not_final_total` and runs the evidence check as any refusal push (the fold's L1); the retire pass reads it as its ABSENCE arm (48 h, memo `retired`), never the 1 h terminal arm, while the hold lasts.
- **#845, the ranks below SEEN.** RECEIVED, SENT_TO_NETWORK and ACCEPTED_BY_NETWORK (`door_words::ARCADE_HOLDING_WORDS`) say Arcade holds it, never the network (bsv-wallet-toolbox-rs 0.11.2, `e28acdc`: its vector's three `status_*` rows `toolbox_status: unknown`; `overlay-discovery/vectors/arcade_status_verdicts.json` is now the 0.11.2 bytes, sha256 `4a28ff8b5cbe...`). The gate (`Pending`), the callback (`Ignore`) and the proof look (`Other`) already promoted nothing; the live look was `Present` and is now `ArcadeLook::Holding(word)`: the evidence check asks the indexers and refuses nothing (`HOLDING_UNCERTAIN_REASON`), and the retire pass takes its absence arm (48 h), so a pot no broadcaster saw SEEN is no longer kept live by Arcade's rank. Since the fold (L3) UNKNOWN and PENDING_RETRY (`door_words::ARCADE_NO_VERDICT_WORDS`, the vector's `toolbox_status: unknown` for both; the app layer reads neither as presence) read `Holding` too. Stated (the lens's N2): `RetireEvidence::held()` is false for a `Holding` look, so the backlog pass, #831's ghost-pot ending included, can end a pot Arcade holds below SEEN (or parks at PENDING_RETRY) once its own floors are met.
- **The fold (the lens `FIX-836-lens-2026-10-22.md`, M1 and the LOWs).** M1, the hold ENDS by the chain: `door_words::not_final_holds` honours the word only while `tip + 1 <= nLockTime + NOT_FINAL_LAG_BLOCKS` (24; loop 44 measured 13), or a time lock plus `NOT_FINAL_LAG_SECS` (24 blocks of 600 s and the median time's hour) against the clock. The lock is the SUBJECT'S OWN BYTES (`SubjectLock`, Arcade's `finality` rule: final when nLockTime is 0 or every nSequence is `0xffffffff`): at the door the EF it broadcasts, at the evidence check, the pending watch and the retire pass its stored BEEF (`admit_fast::stored_lock_bound`: `transactions`, then `pot_beefs`, two reads at most, asked of a non-final look only); the tip is chaintracks' (`lookup_service_chain_tracker`; the door asks it once a request, `run_retire_pass` takes it as its fifth argument). A subject FINAL by its bytes is never held (the text is not about those bytes). Past the bound the word is Arcade's fatal word again, judged as any other (`bound_look`, `bound_witness_look`, `lock_time_not_final_under`): the evidence check's two-source refusal (a live fatal word needs no age; absence keeps #513's floor), the watch's evidence check, the retire pass's 1 h arm, the door's 422 when aged and absent. And the door RE-FEEDS such a subject once an isolate (`broadcaster::refeed_once`, at most `REFEED_MEMO_CAP` 1,024 txids), subject only, never registering a callback, bounded at 2 s, then reads Arcade again: a SEEN takes the pre-flight accept, anything else is judged as above. Stated: with no tip (no tracker, a fault) the hold stands, and the 48 h absence arm still ends it; where no store holds the bytes the TEXT's lock height bounds the hold (Arcade's report of those bytes; it can only end it sooner than 48 h). L1: the push runs the evidence check (`callback_action`; `HoldNotFinal` and `callback_action_with` are gone), whose LIVE look decides, and the Recorded-NotFinal arm is gone: a recorded lock-time word is the public bearer's and takes #740's floor. L2 CARRIED: UTXO_NON_FINAL (71) still reads fatal: it names a PARENT's lock, which the subject's bytes do not carry, no honest LOW path spends a non-final parent, and an eviction on it reverses by the MINED proof. L3 above. L4 (the reference, stated): overlay-express EVICTS on any terminal Arcade callback (`isTerminalArcStatus`, `OverlayExpress.ts:1653` at f999e0c1a) and its `ArcadeProvider` counts RECEIVED, SENT_TO_NETWORK and ACCEPTED_BY_NETWORK as broadcast success (`ArcadeProvider.ts:59-68`): both are stated divergences of register row D1 (its sentence lands in bsv-low with the lens). NOTEs, stated: N3, the frozen body's `"status": 476` is an assumption, read by no check; N4, the timestamp-lock text (`lock time (T) as timestamp is not less than median block time (M)`) parses its two numbers as `lock_height` and `peer_height`, which mislabels a hint in log and answer text only (the bound reads the bytes); N5, `TerminalJudgement::of_word` folds `Inconclusive` and `Capped` into `NotFinal`, attribution only; N6, the gates run on 1.95.0, LOW's toolchain (the default 1.98 clippy reds three pre-existing lines).
- Pins (`--lib`): `door_answer_table` (the recorded table's 21 rows of the three words named in `fix845_moved`, RED on `00a5e613`: `recorded: live_look RECEIVED Holding("RECEIVED") / now: ... Present`), `arcade_vector_replay::fix836_the_loop44_lock_time_bodies_read_not_now` over the frozen `tests/fixtures/arcade_lock_time_vectors.json` (the loop 44 text with Arcade's suffix, reconstructed, not captured; RED on `00a5e613`: 12 of 25, `live_look expected not_final, got fatal`, `evidence_absent_aged expected uncertain, got refused`, `door_absent_aged expected retryable, got rejected`), `fix836_the_counters_the_recorded_word_and_the_peers_height`, `door_words::tests::fix836_*` and `fix845_*` (new API; RED against the reading made inert), and `overlay-discovery --test arcade_status_vector the_vector_is_the_toolbox_0_11_2_bytes` (RED on the 0.7.4 bytes). The fold's: the vector grows `expect_past_lag` (the tip `972210`, past `972185 + 24`) and a subject final by its bytes (RED on `b3e5426e` with the fixture grafted: 8 of 43, `callback expected evidence_check, got hold`, `subject_final_by_its_bytes_named_tx_lock_time: live_look expected fatal, got not_final`; RED with `not_final_holds` made inert: 21 of 83); `door_words::tests::fix836f_m1_*` (the window to the block, the bytes' lock, the once-only re-feed, every site under the bound); `admit_fast::tests::i831::fix836f_m1_the_retire_pass_bounds_a_non_final_look_by_the_chain` (the shipped `retire_evidence`); the table's 42 moved rows (L3's 21 RED with the L3 reading removed). No wire change, no migration.

## The owed judgement's three money gaps (bsv-low #770, #861, #649; lane MONEY-OWED, 2026-10-24)

- **#770, the settle arm's symmetry.** The settle payout's `claimable` and the unclassified spend's `awaiting_block` read ONE rule, `owed::settle_spend_confirmed`: the index's proof of the pot's spend first (`spentConfirmed` or the SPV-verified `at_height`), never vetoed by a word; else the settle's CHECKED `mined` word; with no word row the index's proof alone. It is the hop arm's rule for a spender the index's own pointer names (`non_pot_spender`'s index arm, the hop row naming the filed sweep). No reachable row differed on main: the filed sweep's `!contradicted` guard reads ANOTHER spender's name, which for an index-named spender only the chain rung (a courier) carries, a pot has no chain rung, and a rival cannot displace a confirmed latch (the displacement CAS guards `spentConfirmed = 0`). So the fold is the one function, not a behaviour change. Pin `--lib fix770` (32 evidence cases, settle against hop; green on main by construction; RED against the pre-#736 word-first settle rule, `left: Bool(false) right: Bool(true)` at the latch under an unchecked `mined`, and against `awaiting_block` with the latch dropped).
- **#861, a shed read never offers a stored press.** A read whose refresh #648's bound SHED serves the stored snapshot, whose rows rest on the facts the index held when they were judged (the FIX-859 lens's LOW-1: a door strand of a hop the admitted JOIN since spent). The read cannot re-judge them (that is the walk it was refused), so `owed::demote_shed_presses`, called by the read after #669's gate and before the body, demotes every served row whose `claimable` is `true` (a live seat's rejoin included, the MONEY-OWED lens's LOW-1), and every row with a claim whose `claimable` is ABSENT (the client reads `facts.claimable !== false`, so absent is a press; the lens's LOW-2, folded), in the served body only, as #837's carry does: `claimable: false`, `claimableBeforeShed` the prior reading (`true`, or `null` when absent), `refresh: "shed"`, `stale: true`, `retryAfterMs` (at least 1 s), `claimReason` `OWED_SHED_PRESS_REASON`; family, claim, sats and reason untouched; a row with no press and a carried row keep their own words; a fresh compute and an unshed read are unchanged; the stored snapshot is not written. Chosen over re-judging each row (a read the shed exists to bound) and over an unshed walk per admission (#648's ceiling). Its cost: an honest press waits for the next recompute while the identity is shed (the alarm walk, 60 to 120 s; a heavy identity's reads shed until it lands). Stated: a stale read that is NOT shed (a walk already in flight elsewhere, coalesced; an admitted walk not landed inside the read's 2 s) still serves the stored rows as they are. Pins `--lib fix861` (`fix861_a`: the derivation's door strand written and read back through `owed_write_plan` and `OWED_ROWS_READ_SQL` on real SQLite, the index since holding the hop spent, the route's served steps in order; RED with the demotion inert: `left: Bool(true) right: false`; `fix861_b`: the scope, the rejoin in its served shape `claimable: true`, and the read's call, RED `left: 0 right: 3` with the demotion inert and with the call removed); the lens's five cases `--lib money_owed_lens` (`money_owed_lens_2`: a claim with no `claimable` withheld, its fresh and unshed reads served as stored; RED on `f6c0b017`: `the client still reads a press: Object {"claim": String("internalize")}`).
- **#649, the eviction counted before the flush.** The pending watch and `/arc-ingest` refusal job use `admit_fast::settle_corroborated_refusal`: the refused memo, eviction counter and memo-write counter commit in one three-statement batch, followed by the log and delivery. `flush_inline_within` caps delivery at 20 seconds and reserves three seconds before the 30-second platform window for one atomic `counted_batch` containing all hop, pot and word counters and parks; no per-counter D1 round trips follow it. A batch fault logs and retains its counter deltas for the next flush, while notes remain in memory. The refusal facts precede this batch and survive its failure. Every pot, hop and tracker-word POST is raced against its shared deadline; an unacknowledged chunk is deferred and parked in the existing outbox. A walled word keeps its retry allowance and is also parked. The words' 10-second slice bounds only POST starts; an in-flight word is awaited under the full flush wall before notes run. Deferred pot/hop counters and word `walled` counters include in-flight cuts; `change_parked_total` includes word parks from every flush. Pins `--lib d2_649` run the production watch loop, eviction SQL and flush loops under a fake clock that cancels at 30 seconds: the facts survive, nine undelivered pot notes are counted and parked, and a hung word reaches the shared wall, then notes are deferred and all remain deliverable. The fold pins a word answered at slice + 1 second before its notes, and 384 parks plus 25 counters in one modelled 2.9-second round trip inside the reserve.

## The retire pass ends a ghost pot (bsv-low #831 half 2, FIX-831-B, 2026-10-22)

A GHOST POT is a pot row the index holds live whose funding the network never held: the JOIN was admitted PENDING under admit-fast, no broadcaster ever said SEEN, no proof ever came, and its stored BEEF is structurally unprovable. Before this change the retire pass latched such a pot `structurally_unprovable` (#765's verdict) and left its live row, so the owed list and every display view served a refund-due row for money that never existed (FIX-831 half 1 serves that row as `funding-not-held`; this half ENDS the pot).

- **One ending for every arm.** The engine arm (`retire_candidate`: a `transactions` row retired), the `pot_beefs` arm (a latched pot) and the BACKLOG (`run_retire_backlog`: the union of the latched set and the engine's retired set, a page of `RETIRE_BACKLOG_WINDOW` txids rotating from a start key that wraps past the top) all run the SAME verdict (#765's `retire_verdict`: Arcade's live word fatal or missing AND both indexers definitively absent past the age floor; a `Present` returns `None`), the same end-state read (`end_retired_pot`: the proof and the SEEN latch re-read just before the eviction, LOW-1 of the lens), and the EXISTING OPEN-marker eviction (`admit_fast::evict_txid_everywhere`, the shadow move): never a plain delete, never a pot the index holds funded or SEEN, never a pot with a parked refund that the index holds spent. An incomplete eviction is a FAULT, never an ending (LOW-2). A pot with no stored BEEF is never ended.
- **One budget.** The whole pass spends from `RETIRE_PASS_STATEMENT_BUDGET` (480 statements) with `RETIRE_END_RESERVE_STATEMENTS` (96) reserved per candidate before it is looked at; what does not fit is DEFERRED to the next pass and counted once a pass (`retire_end_deferred_total`); the retire-pass log line carries `statements=`. The conflict pass (#602) spends from the same budget first. Measured in the budget tier (`tools/lane-831/retire_pass_budget_ci.mjs`, `make ci-d1-budget`, `ceilings.json` by name): the worst tick 400 statements against a ceiling of 490 (5 pots ended, 19 deferred), the drain's largest pass 454; with the budget check undone one pass spent 1,784 statements and the cell went RED (the lens's RED run).
- **Reversal.** Every eviction here reverses as every eviction does: a pushed MINED proof (`readmit_if_evicted`), a fresh accept at the door (#513), or the operator. The reference engine has no retire pass and no eviction: this is register row D1's divergence at a higher age floor, no new row.
- **Stated and open (the delta lens's LOW-1, bsv-low's follow-up issue):** the backlog page is 32 txids of the union where the base paged 256 of the latched set alone, so a ghost pot that escaped the arms waits longer for the backlog (display only, never money; it converges); paging back to 256 needs the tier's ceilings re-measured.
- Pins: `i831_a` to `i831_m` in `crates/overlay-cloudflare/tests` (each RED when its fold alone is undone, except `i831_m`, a text pin); the lenses `docs/audit/FIX-831-B-lens-2026-10-22.md` (NOT SOUND, folded) and `FIX-831-B-delta-2026-10-22.md` (SOUND) in bsv-low.

## A late JOIN records its hops (bsv-low #837 with #793, FIX-837, 2026-10-22)

Loop 44 pair 1: a JOIN the door answered network-held (admitted nothing) later entered the index by another road, and both of its hop rows read `spent: false`; the owed list offered the hop sweep, Arcade refused it `UTXO_SPENT`, and from then on the game had no owed row. Three changes, one rule each:

- **The write (the fix).** `overlay_discovery::pot::lowfund_topic_manager::record_join_hop_spends(storage, pot_txid)` records a HELD JOIN's hop spends from its own stored bytes (`pot_beefs`): the subject must be the pot and carry a covenant output, the pot's row must be held, and each input whose existing row is a hop row (`lockKind = 'p2pkh'`) or that `join_hop_inputs` names, holding no pointer, is marked spent by the JOIN as an UNCONFIRMED pointer through a write GUARDED in its own statement (`PotStorage::mark_spent_if_unpointed`, `d1_discovery::MARK_SPENT_IF_UNPOINTED_SQL`: `spent = 0 AND spendingTxid IS NULL AND spentConfirmed = 0`, the FIX-837 lens's LOW-3, so a pointer another writer lands between the read and the write is never overwritten). Never re-points, never creates a row, idempotent. Called by every road without the door's `tm_lowfund` judgement (`admit_fast::record_join_hop_spends_on`): `readmit_if_evicted` (so every door accept, MINED push and operator readmission that closes an eviction), `/admin/readmit` in both arms, the door's re-present of a JOIN deduped under `tm_pot`, and the discovery pass's pot candidate (`discover_spend_for_row`, `MissingSpendSummary::join_hops_marked`), which also covers a hop admitted or readmitted AFTER its JOIN while that pot is still UNSPENT and at least an hour old (the pass's candidates; the lens's N2): a pot spent before the pass looked at it is never a candidate again, so its hops are healed at the write by nothing (`/utxo-status` keeps reading them `spent: false`), and the owed list covers them only through the belt's window and the chain rung where probed.
- **The write's bound (the FIX-837 lens's MEDIUM-1).** A stranger's covenant-shaped JOIN over hundreds of P2PKH inputs is a discovery candidate every hour while it stays unspent, and the write read one row per input. Now: at most `JOIN_HOP_INPUTS_MAX` (16) input rows a JOIN (an honest JOIN has two), the named inputs first then those whose source the bytes do not carry, a carried non-P2PKH source never read; a wider JOIN reads a window rotated by the pass's hour (`record_join_hop_spends_bounded`, `rotation`), the rest DEFERRED and counted (`JoinHopSpends::deferred`). Per pass at most `JOIN_HOP_READS_PER_PASS` (64) input rows over every pot candidate, with `JOIN_HOP_READS_RESERVED_PER_JOIN` (2) held back for each candidate still to come (`join_hop_allowance`), so wide JOINs never leave an honest JOIN unread; a candidate the budget leaves is counted and is a candidate again after its hour's backoff. Counted on the tick's line (`join_hop_reads`, `join_hop_inputs_deferred`, `join_hop_pots_deferred`) and durably (`missing_spend_join_hop_inputs_deferred_total`, `missing_spend_join_hop_pots_deferred_total`). The request-path roads (readmission, `/admin/readmit`, the re-present) read at most 16 a call, rotation 0; their deferred inputs are the discovery pass's. Measured in the budget tier (`tools/lane-837/join_hop_pass_ci.mjs` after the #831 cell, through `POST /internal/missing-spend-pass`, bearer `INTERNAL_TOKEN`, the tick's own function; `ceilings.json` `missing-spend-pass-join-flood`): five stranger JOINs of 64 inputs and eight honest two-hop JOINs, one pass 188 statements, 1,395 rows read, 404 written, 64 input reads, 272 inputs deferred, every honest JOIN healed; with the bound lifted 732 statements, 3,591 read, 2,036 written. In the full tier (`make ci-d1-budget`, on the #831 cell's drained D1, whose released rows are pot-class candidates and hold their reserve): 174 statements, 1,433 read, 320 written, 50 input reads, 286 inputs deferred, every honest JOIN healed. The recorded spend feeds `hop_pot_links` (its triggers need `hop.spent = 1`), so the pot enters the seat's results and its refund row is served.
- **The read (the belt).** Before an index-UNSPENT hop is called stranded, the owed recompute asks its seat's held pots (`owed::JOIN_SEAT_POTS_SQL`, `pot_hop_seats`) whether their stored JOIN spends it (`owed::join_spends_outpoint`, the stored bytes read under `APP_BEEF_LIMITS`, the lens's LOW-1), at most 8 hops and 4 pot reads a pass (`OwedInputs::join_spent_hops`): the newest index-unspent hop, then seven of the rest at an offset rotating by the recompute's minute (`owed::belt_window`, as the probes rotate; the lens's LOW-2), so an identity with more than eight reaches each over successive passes; a hop whose chain rung already names a spender is not asked. #793's loop-43 shape (a refused press re-offered) stays open only for a hop outside that window on a pass whose chain rung is silent. Such a hop is never a press. When the results carry no row for that pot the game keeps an `unbound` row (`JOINED_POT_UNLINKED_REASON`, `facts.joinSpent`, `joinSpentBy`); the chain arm's "spent by a pot" does the same for an index-unspent hop (before, both `continue`d with no row: "no owed row for this game"). An index-SPENT hop a pot took is no row, as before (a pot outside the era's results is a written-off game).
- **The cut.** A recompute the outward budget cut never serves fewer rows than the snapshot it replaces without saying so: each previous row it did not derive is carried (`owed::carry_shed_rows`, one `OWED_ROWS_READ_SQL` read on a cut pass), `refresh: "shed"`, `stale: true`, `carried: true`, `retryAfterMs` 30 s, `claimable: false` (its word kept as `claimableBeforeShed`), for at most `OWED_WITHHELD_KEEP_MAX_MS` from its first judgement; the answer serving one carries `refresh: "shed"` (`mark_shed_rows_refresh`). Since bsv-low #859 (FIX-859, loop 46 pair 16, 2026-10-23) a previous row whose SUBJECT the pass holds (a hop, result or refund entry of that outpoint, `owed::judged_outpoints`) is never carried: the derivation's "no row" for it is a judgement (a hop its admitted JOIN spent, a pot settled), so the admitted JOIN's hop loses its door strand (`hop-stranded`, `joinRefusedBy: "door"`) in the recompute the `pot-changed` push starts, cut or not; before it every cut pass of a heavy identity carried the strand `shed` for up to the keep window after the door had retired the refusal row (the fresh derivation cannot say `door` without that row: `fix859_a`). Pins `--lib fix859` (`fix859_b` RED on d370d906: `carried 1`, the strand served `shed`). The carry never takes the rows past `OWED_MAX_ROWS` (it runs after the pass's truncation; the lens's LOW-4), so the write plan's rows + 3 holds by construction. The outward phases already walk newest first (probes `hops_view::order_probe_targets`, spender reads by marker age); the candidate cursor reads every page before selection, which ranks newest first. Measured in the budget tier (`make ci-d1-budget`, identity C of `examples/d1_budget_seed.rs`, 300 results): C's first read 467 ms, 300 rows over 300 games, 426 statements, 8,073 rows read, not cut; the tier has no tip, so the newest game's row there is its rejoin row, not refund-due.

Stated: #793's chain rung already downgrades an index-unspent hop with a positive spent word (the `chainProbe: "spent"` arm); what it could not see is a hop with no word whose own JOIN is held, which the belt now reads. A hop whose spender is no pot and whose chain rung is silent (budget cut, couriers down) is still offered, #669's accepted residual. No wire field removed, no migration, no new statement shape in the overlay.

Pins, each RED with its hunk reverted alone: `--lib fix837` in `overlay-discovery` (`fix837_a`, the shared write: inert, `left: []`); the worker's `--lib fix837` (`fix837_a3`, every off-door road calls the one function: RED with the readmission's call, one `/admin/readmit` arm (`left: 1 right: 2`), the re-present's call or the discovery pass's call removed; `fix837_b`, the discovery pass marks the held JOIN's hop with no courier word: `join_hops_marked` 0); the app layer's `--lib fix837` (`fix837_c`, the belt and the chain arm: `left: HopStranded right: Unbound` without the belt, `left: 0 right: 1` without the chain arm's row; `fix837_d`, the cut's carry: `left: 0 right: 1` inert; `fix837_e`, the route's wiring of both and the answer's mark), and `the_index_unspent_offers_the_sweep_and_the_chain_rung_only_downgrades_it` amended (RED `left: 0 right: 1` without the chain arm's row). The tier's #837 cell runs after section 3 of `tools/lane-499/d1_budget_route_ci.mjs`: C's first read writes 300 rows (426 statements) and the app's `/owed` running maximum is held to A's named ceiling there. The fold's pins (the FIX-837 lens), each RED on 9d380f8c: `fix837f_m1_the_shared_write_reads_a_bounded_window_of_a_wide_joins_inputs` and `fix837f_l3_a_pointer_landed_after_the_read_is_never_overwritten` (`--lib fix837f` in `overlay-discovery`; `left: 65 right: 17` row reads, and the rival's pointer overwritten by the JOIN), the worker's `fix837f_m1_the_discovery_pass_reads_a_bounded_number_of_join_inputs` (326 marked), the app layer's `fix837f_l4_the_carry_stops_at_the_row_cap` (`left: 509 right: 500`), `fix837f_l1_*` (a source pin: on bsv-rs 0.7.2 `Beef::from_binary` refuses what the limits' parse refuses) and `fix837f_l2_*` (`belt_window`, absent on the base; `fix837_e` reds on the route's text), and `d1_fleet_plans` `fix837f_l5_*` (the belt's two reads and the guarded write are key searches; no RED possible, the plans were already index-served, the lens's LOW-5). The cell `tools/lane-837/join_hop_pass_ci.mjs` is RED on 9d380f8c (no lever, 404).
## The janitor's couriers are paced (bsv-low #834, FIX-834, 2026-10-22)

Loop 44 measured the janitor's WoC at 36.6 percent faults and its Bitails at 37.5 percent while the app layer's own reads of both answered (the #606 discriminator): the janitor's asks faulted, not the couriers. Each fetcher of a tick kept its own circuit, so a 429ing provider was asked three times per fetcher per tick, every tick, and pruned Bitails' non-answers were counted faults.

- **One pacer per pass** (`courier_counters::Counters::paced`, the tick and `/admin/complete-proofs` only; the door is never paced). Every courier GET goes through `proof_fetcher::http_get_counted`, which asks `Counters::pace` first: a per-provider budget per pass (`COURIER_TICK_BUDGET`: Arcade 128, WoC, Bitails and BananaBlocks 64 each), a BACK-OFF at the provider's third 429, 5xx or transport fault in the pass (`COURIER_BACKOFF_FAULTS`, the convention's "three in a tick", now over every fetcher of the pass), and a COOL-DOWN of 1200 s across passes (`COURIER_COOLDOWN_MS`, one `*/15` tick and a third), an isolate memo, never D1 (a fresh isolate asks again: the fail-open direction for a cost bound; no migration). A skipped ask is no subrequest, no fault and no fetcher budget; the ladder's next rung takes it. Since FIX-834-MERGE (the merged main's tier, `janitor-pass-woc-429` RED at total 70, WoC 6, Bitails 24) the RETIRE PASS asks through the same pacer at both janitor entrances: its #602 conflict pass (`admit_fast::conflict_pass_fetcher`, its 24-call fetcher, and `CourierConflictPresence::counters`), its own Arcade, Bitails and WoC looks (`arcade_look_counted`, the `*_presence_counted` reads), and the #273 rebroadcast backstop (`run_rebroadcast_backstop`'s counters; the tick flushes a second time after it); the presence reads are their own rungs (`bitails_presence`, `woc_presence`), so the proof ladder's pruned-Bitails cap never blinds the both-indexer reading those verdicts take. The operator's `/internal/retire-pass` lever runs unpaced, as the door. Pins `fix834m_a` (RED with the conflict fetcher unpaced: 37 subrequests for 26) and `fix834m_b`. A negative still needs a second provider's clean answer, so a pass that skips WoC decides fewer outpoints ("could not look", retried), never more.
- **Bitails skipped by construction.** The proof ladder never asks Bitails once Arcade has said the subject is held unmined (`SKIPPED_UNMINED_RESULT`). Pruned Bitails' `/tx` 404 for a proofless subject and its per-output `spent: ""` and 404 are "could not look" (`CourierOutcome::CouldNotLook`): never a fault on either family, never a clean negative (a Bitails 404 beside one BananaBlocks "unspent" decided an outpoint unspent before this), no circuit step, and a rung's third in a pass skips it for the rest of the pass (`COURIER_COULD_NOT_LOOK_CAP`). Bitails stays every other ladder's fallback (a mined subject's proof, its tx body naming a spender, the scripthash history, raw bytes). The #606 rule on the proof ladder: a body naming the asked txid with no height is unmined (`mempool_body_of`), not `invalid-height`.
- **Served.** New rows `courier_janitor_<p>_{couldnotlook,skipped,backoff}_total` (served from 0); `/health/invariants.indexJanitor.courierPacing` per provider `{asks, faults, couldNotLook, skipped, backoffs, faultRate}` where `asks` is `ok + notfound + fault + shape` and `faults` is `fault + shape`, exactly the fleet cell's spelling, so could-not-look and skips are outside both terms and the cell's existing rule compares like with like; plus the bounds. `/admin/complete-proofs` answers `courier_pacing` (the pass's asks and per-provider state); the tick logs `Scheduled: courier pacing`. Changed words, stated: the admin probe's `parseResult` for a Bitails 404 is `could-not-look` (it was `http-error`), and `courier_bitails_fault_total` no longer counts it (the legacy baseline rows above keep Arcade's and WoC's 404 a fault).
- Pins `--lib fix834` (`proof_fetcher::fix834`: `a` loop 44's shape over two fetchers, 30 subrequests, WoC 3 then cooled; `b` the budget; `c`, `d`, `e` RED on `00a5e613`; `f` the served rate; `g` only the janitor paced). RED, stated: `c` on `00a5e613` "one capable negative is no verdict: Ok(None)" (a Bitails 404 beside one BananaBlocks "unspent" decided the outpoint), `d` "left: 3 right: 2" (Bitails asked for an unmined subject), `e` "left: \"invalid-height\""; against mutants (the base has no pacer): `a` with the pacer inert, WoC asked 6 (each fetcher's circuit) and Bitails 24; `b` WoC asked 70 of a 64 budget; `f` with could-not-look folded into the asks, 881 for 381; `g` with the tick's counters unpaced. The tier: `tools/lane-834/janitor_pacing_budget_ci.mjs` in `make ci-d1-budget` on the #831 cell's overlay (WoC 429, the rest 404) over 40 proofless pot BEEFs (its own fixture, not the fleet seed), ceilings by name in `tools/lane-834/ceilings.json`: the first pass 46 courier subrequests (Arcade 40, Bitails 3, WoC 3), the next inside the cool-down 43 (WoC 0). Since #868 (re-measured by the COURIER-RUNG fold, 2026-10-24, LANE_BASE 8820, the retire overlay's three cells in the tier's order; the box's tier on `f5d2d700` measured the same): 70 (Arcade 40, Bitails 6, WoC 3, BananaBlocks 21) and 67 (BananaBlocks 21, WoC 0), ceilings 72 and 69. The ceilings MOVED, not bound: every BananaBlocks ask is the spend ladder's `/api/v1/txo/{txid}/{vout}/spend` (the cell prints the routes), the retire pass's #602 conflict pass reading the #837 cell's stranger JOINs' outputs inside its own 24-call fetcher budget and the pacer's 64 a pass; none is the presence rung. With BananaBlocks off that overlay those reads found no capable rung and were skipped: the pass was blind there, not cheaper.

## BananaBlocks is the second capable rung where WoC stood alone (bsv-low #868, lane COURIER-RUNG, 2026-10-24)

The probe (bsv-low `docs/audit/COURIER-CAPABILITY-2026-10-24.md`, 104 anonymous GETs on 2026-10-24 between 14:30Z and 15:05Z, no 429; the raw answers at `COURIER-PROBE/requests.jsonl`, the bodies the pins read kept verbatim in `crates/low-app-layer/tests/fixtures/courier_probe_868.json`) found the two reads with one capable courier were our parsers' doing. Probed facts, each read as recorded:

- **BananaBlocks `GET /api/v1/txo/{txid}/{vout}/spend`** answers a spent output with `spent: true`, `spentTxid` and `spendScope`: `"confirmed"` (the spender is in a block) or `"unconfirmed"` (mempool), truthfully on six outpoints and watched flipping at block 972477 (`d47bada4...:3`). Its docs: a spent answer WITHOUT `spendScope` means the spender's block status could not be checked, scope unknown, never confirmed (documented, never seen live). It carries no height.
- **BananaBlocks native `GET /api/v1/tx/{txid}`** answers an unmined transaction `confirmed: false`, `block_height: 0`, `block_time: 0`, no `block_hash`, no `confirmations`; a mined one `confirmed: true`, `block_height`, `block_hash`, and `confirmations` (OMITTED at one confirmation: every block-972476 subject). Its docs: test `confirmed` (or `block_height`), never `confirmations`; `confirmation_unknown: true` means the block read failed (documented, never seen live). An invented txid is `404 {"error":"Transaction not found"}`. It held the 3-sat `e3340463...` WoC's node held.
- **BananaBlocks' WoC-style mirror** (`/api/v1/bsv/main/tx/hash/{txid}`) answers WoC's mempool shape, but its MINED body has no `confirmations`: it is never read for presence.
- **Bitails `GET /tx/{txid}`** answers an unmined transaction 200 `confirmations: 0` (five of five; the record's "404 for every unmined tx" was wrong) and a mined one `blockheight`, `confirmations >= 1`; its 404 is NOT absence: it dropped `e3340463...` (404 at 14:51Z and 14:52Z) while WoC's node and BananaBlocks held it. It has no per-output spend data (`/tx/{txid}/output/{vout}/spent` is `Cannot GET`; its tx body's outputs are empty).

What reads them now:

- **The spender's MINED word** (`/spent-any`'s `spentConfirmed`, the app layer). `results::SpentObservation::Spent` carries `confirmed: Option<bool>`, the naming rung's own word: WoC's `status` (`Some`), BananaBlocks' `spendScope` through `results::bananablocks_spend_scope` (`"confirmed"` true, `"unconfirmed"` false, absent or other `None`), pruned Bitails `None`. `decide_spent_any` serves it as is, so under a WoC `/spent` fault a BananaBlocks-named spend is `spentConfirmed: true` or `false` with `spentConfirmedSource: "courier"`, and `null` only without a scope. #723's `spent_confirmed_unknown_after_fault` (which nulled every fallback rung's word) is gone: the observation says "unknown" itself. The ladder's order (WoC, BananaBlocks, Bitails' tx body), its raw verification and its corroboration are unchanged; the held-proof words (`spent_confirmed_word`) still outrank any courier. Since the COURIER-RUNG lens fold (L2, NS-3 and #692's rule) a courier's confirmed word is a HINT on the owed list, never a press: a filed sweep's payout (`owed::swept_home`, the filed arm) and an index-bytes sweep's are `claimable` only on the index's proof (its verified proof, its hop-row latch), a CHECKED tracker `mined` word or a chaintracks-checked mined proof of the sweep (`proof_mined_spenders`, `confirmedSource: "proof"`); on the chain rung's courier word alone (WoC's `/spent` status, BananaBlocks' scope under a WoC fault) the row is the pending chain-wait (`claimable: false`, `chainWait: "block"`) serving `spenderPresence` {`word: "spent"`, `tier: "pending"`, `source: "courier"`}. Before the fold, where the tracker held no row for the sweep, that word pressed (WoC's since before #868, BananaBlocks' with it). Its cost: such a row is OPEN (re-derived by the five-minute rule) until a checked word lands; courier-bytes rows were never claimable and keep the courier's word for their home look (#485). Pin `owed::tests::c868f_l2_a_couriers_confirmed_is_never_a_press_by_itself` (RED on `f5d2d700`: `left: (Payout, Some(true)) right: (Payout, Some(false))`); amended, each encoding the heal this removes: `a_filed_sweep_the_index_names_but_never_confirmed_is_claimable_once_a_checked_word_confirms_it` (renamed from `..._once_the_chain_rung_confirms_it`), `a_hop_spent_by_its_own_filed_sweep_*`, `a_spender_that_is_not_a_covenant_pot_*`, `a_swept_hops_payout_is_confirmed_only_by_a_chain_word_that_names_the_sweep`, `i736_a_filed_sweeps_index_latch_*`, `hops_view`'s two #484 reorg-grace pins (a courier-only row is an open row now) and `tests/i736_index_proof_first_fleet.rs` pin4's no-word baseline.
- **An unmined subject's PRESENCE on `/tx-any`** (the app layer's external leg, `routes::tx_any_courier_leg_via`). WoC's `/tx/hash` first, as before; when it holds no positive, BananaBlocks native (`txany::parse_bananablocks_tx_claim`: `confirmed: false` present unconfirmed, `confirmed: true` with a positive `block_height` a mined CLAIM, `confirmation_unknown: true`, another txid or any other body no word); when neither does, Bitails (`txany::parse_bitails_tx_presence`: `confirmations: 0` present, `confirmations >= 1` with `blockheight` a claim, its 404 "could not look"). The first positive answers with its source: an unmined one is the courier HINT (`present: null`, `hintSource` `woc`, `bananablocks` or `bitails`), a mined claim is proven exactly as WoC's is (the TSC proof bound against chaintracks, else a hint with `claimedHeight`, `tx_any_courier_proof_fault_total`). ABSENCE needs two capable sources that both LOOKED (#614): WoC's clean 404 behind its healthy route AND BananaBlocks' recorded not-found (`txany::bananablocks_tx_not_found`, the body, not the status alone) behind ITS proven route (the COURIER-RUNG lens fold, M1: `routes::bananablocks_route_healthy`, its native `/tx/{KNOWN_MINED_TXID}` served as a mined claim, `confirmed: true` with a positive `block_height`, one GET per isolate per `ARCADE_ANCHOR_PROBE_TTL_MS` (30 s), both outcomes memoised, counted `tx_any_bananablocks_anchor_{ok,unknown}_total`, asked only beside WoC's own look; behind an unproven route the not-found is `couldnotlook`; measured live 2026-10-24T19:36Z, two GETs: the anchor answers `confirmed: true`, `block_height: 958886`; any other BananaBlocks 404 is a fault): then `AbsenceCorroboration::CorroboratedAbsent`, `present: false`, counted `tx_any_two_capable_sources_absent_total`; still NO request-time memo is written (the answer lives the isolate's 15 s). One of them alone, or BananaBlocks' not-found behind an unproven route, is `present: null` (`tx_any_one_capable_source_unknown_total`). Before the fold BananaBlocks' not-found counted with no route check (the lens's `lens868_h`), where on `db3a0931` the external leg never asserted an absence at all. The one durable consumer of `present: false` is bsv-low's client, `app/src/lib/homeCards.ts` `stakeFundingProvablyAbsent` (record older than 6 h, `present === false`, `terminal` not read) feeding `stampStakeFundingAbsent`, a write-once "nothing owed forever" stamp on a legacy stake's refund record: the anchor bounds it here, and the client's reading of `terminal` (its doc still describes WoC + Bitails + a Bitails anchor) is bsv-low's to correct, not this repository's (the lens's M1, stated). The raw of a positive is hash-verified from WoC's hex, Bitails' binary, then BananaBlocks' native `/tx/{txid}/hex` (each tried until one verifies). Each rung's answer is counted `tx_any_courier_{woc,bananablocks,bitails}_{present,mined,absent,couldnotlook,fault}_total` (`txany::courier_rung_counter`; the family beside the proof fault counter, nothing renamed).
- **The #519 presence ladder** (the overlay: the rebroadcast backstop, the retire pass's look, the ladder's terminal read, the conflict pass's competitor, the evidence check). The host's two-source fold (`door_words::courier_presence`) is unchanged; what fills its first slot is `ProofSources::mempool_witness_presence_counted`: Bitails' positive (`proof_fetcher::bitails_presence_response`: its 404 is `None`, "could not look", where it was `Absent`), else BananaBlocks native on its own rung `bananablocks_presence` (`proof_fetcher::bananablocks_presence_response`; the provider's existing `courier_{door,janitor}_bananablocks_*` rows and the janitor's pacer). So an ABSENCE (a rebroadcast, a retire on absence, a conflict's or a terminal's absent word) needs WoC's clean 404 AND BananaBlocks' recorded not-found; Bitails' 404 beside WoC's 404 is `Inconclusive` (it was `Absent`). The counter of Bitails' 404 stays `notfound` (the family counts the HTTP answer; the fold reads it as no word). `confirmation_unknown` counts `shape`. A BananaBlocks 404 whose body is not its recorded not-found is counted `fault` (it was `notfound`) and votes toward no absence (the COURIER-RUNG lens fold, M1 at the overlay; `proof_fetcher::c868f`, RED on `f5d2d700`: `left: (None, Some(1)) right: (Some(1), None)`). The overlay's BananaBlocks not-found asks no route anchor, as its WoC 404 asks none (pre-existing, symmetric here; every absence it reads ends in a reversible eviction or a rebroadcast). Stated (the lens's L1, parity, not bound): in this fold a BananaBlocks mined body is an UNVERIFIED height, taken FIRST when Bitails holds no positive (`door_words::courier_presence`, `mined_claim(first slot).or_else(woc)`), for the #214 terminal admission and #602's competitor confirmation, exactly the trust D1 already gives WoC's and Bitails' `blockheight` (an eviction on it is reversed by the pot's MINED push; an admission on it is taken back by the conflict pass or the retire pass); pin `lens868_d` holds it as observed. Stated (the lens's L3): a deployment whose `PROOF_SOURCES` omits `bananablocks` (`wrangler.toml` sets none) reads no absence at all (fail-safe), `lens868_a`.

The route tier (`make ci-d1-budget`): the retire overlay of the #831 and #834 cells now runs `PROOF_SOURCES` with `bananablocks` (without it no absence can be read, so the #831 cell's ghost pots could never end), and both cells' stand-ins answer BananaBlocks' native `/api/v1/tx/{txid}` with its recorded not-found body. The #831 cell's leg 4 (the COURIER-RUNG lens fold, M1): one more ghost pot under a NON-STANDARD BananaBlocks 404 (`{"error":"Not Found"}`) survives three passes un-evicted (BananaBlocks asked about it six times), then ends under the recorded not-found (the control); green on `f5d2d700` too (the overlay already read that 404 as no word; only its counter was wrong). The #834 ceilings were re-measured by the fold (below, the #834 paragraph).

What stays one-sourced, stated: the spender's mined word has no HEIGHT from BananaBlocks (WoC's `/spent` gives none either; the proof words carry the height). Bitails stays incapable for spends. The probe measured mempool retention under a below-floor fee on ONE subject (BananaBlocks held what WoC's node held; whether either ever drops what the other holds is not measured), never saw `spendScope` omitted or `confirmation_unknown` live, and asked from a laptop, not a Worker's egress. A mined claim from BananaBlocks or Bitails is served `confirmed` on `/tx-any` only through WoC's `/proof/tsc` checked against chaintracks (no BananaBlocks proof route is read); in the overlay's presence fold it is the unverified courier height stated above (L1). Pins: `-p low-app-layer --lib c868` (`c868_a` the ladder under a WoC fault over the recorded scopes, RED on `db3a0931`: `left: None right: Some(true)`; `c868_b` the external leg, RED: `left: (None, None, None) right: (None, None, Some("bananablocks"))`, its absence arm amended by the fold to serve BananaBlocks' anchor; the readers over the recorded bodies; the lens's `courier_rung_lens` cases, `lens868_h` re-aimed: a never-seen txid under BananaBlocks' and WoC's 404 with BananaBlocks' anchor unproven is `present: null`, RED on `f5d2d700`: `left: (Some(false), CorroboratedAbsent) right: (None, Unknown)`; `c868f_m1_b` both routes proven and the anchor's memo, RED on `f5d2d700`: the anchor never asked, `left: 0 right: 1`; `c868f_m1_c` a non-standard 404 a fault, green there), `-p bsv-overlay-cloudflare --lib c868` (the ladder's fold through the conflict pass's production presence, RED on `db3a0931` at its first arm: Bitails' and WoC's 404 read `Absent`; the module was never declared until the COURIER-RUNG lens fold (M2): `--list` printed 0 tests on `f5d2d700`; declared, its fixture reader unwraps nothing (the crate's `clippy::unwrap_used`), and with Bitails' 404 grafted back to an absence `c868_the_presence_readers_read_the_recorded_bodies` is RED: `bitails_tx_dropped_404 left: Some(Absent) right: None`; with it `lens868` (the lens's four) and `c868f`).

## A faulted submit leaves one head (bsv-low #559, lens fold and three delta folds of 2026-10-07)

`Engine::submit` inserts the admitted outputs BEFORE it deletes the stale
coins, and deletes them once EVERY insert of that topic landed. The reference
deletes first (`applyTopicStorageMutation`: `removeStaleOutputs`, then
`admitOutput`), catches a throw per topic (`applyStorageMutations`) and
carries on, and never replays; here a fault is survived and replayed, so the
order is ours, an addition. With the delete first, one transient D1 fault on
the insert of a mid-chain head of a non-retaining chain left the chain with no
head, for good.

The invariant, per topic of one submit, for a non-retaining chain: whatever
SINGLE storage call, lookup hook or manager call faults, a call that lands
after its timeout or after it answered an error included, the store never
holds two unspent heads and never no head row: the new head unspent, or the
old one (unspent after a validation read fault, marked spent from the mark
on, so no UTXO is listed until the replay), and no applied row for the
faulted submit, and the queue's replay converges to the new head. (One head
ROW too, except the leftovers named under the limits: a stale coin whose
delete faulted or whose spender's insert did not answer can stay behind as a
SPENT row beside the new head, no UTXO.)

- A fault BEFORE every insert landed leaves the previous coins and NOTHING
  of the transaction. A validation read (the dedup read, since the delta
  fold: a faulted `does_applied_transaction_exist` is the topic's read fault,
  as the reference fails the topic; or a previous coin's `find_output`)
  writes nothing at all, so the kept coin is NOT marked spent. The spent mark
  of a coin the manager retains, or an insert: the outputs that did get
  inserted are taken out again (`undo_inserts`: only a row still unspent and
  consumed by nothing), and the kept stale coin IS marked spent by then, so
  it is not listed as a UTXO until the replay (a state the reference does not
  have: its kept spent rows are retained coins).
- A LOOKUP NOTIFICATION that faults is a fault BEFORE the transaction landed
  (bsv-low #657, lane ENGINE-657; zanaadu-v2 #414), whether it is a stale
  coin's `output_spent` (nothing is inserted) or an inserted output's
  `output_admitted_by_topic` (the inserts stop there and are undone, the
  output whose hook faulted included; no further lookup service is told):
  the stale coins are kept (marked spent, as on an insert fault) and NEVER
  deleted, no applied row, so the replay is judged over the coin it was
  judged with and re-applies the transaction whole, telling the hooks again.
  A deterministic fault answers the same word on every presentation (the
  store byte-equal after the first). Until #657 a notification fault was a
  fault after the inserts: the stale coin was deleted all the same and the
  applied row withheld, so the replay of the same bytes was judged WITHOUT
  the coin it deleted (Zanaadu's registry: the retry read as a first
  deployment and admitted output 0 alone). Option (b), the applied row
  recorded and a durable "notify owed" row settled by the replay, was not
  built: it needs a table and four `Storage` methods for every backend, and
  a deterministic fault would hold an owed row for ever while the topic said
  "applied"; (a) uses the undo #559 already proves and points where
  Zanaadu's atomic admission (bsv-low #657's design, open as PRs #41 to #43)
  goes: a refusal writes nothing. Divergence by addition: the reference
  (`Engine.submit`, `applyTopicStorageMutation`) deletes first, notifies
  after with no retraction, and keeps the output a throwing hook was told
  of. The register gets the sentence (LOW's captain records). Pins
  `e657_a` (deterministic, three presentations, one word), `e657_b` (once,
  then the replay whole and the hook told), `e657_c` (the spend's hook, both)
  in `--test gasp_topic_manager`, each RED on `d370d906`; moved with it:
  `fold559_f1_a_notification_fault_*`, `delta559_h2_x6_*` (H2 stays pinned
  by the applied-row fault and X5), `delta2_559_row6_*`, and
  `error_paths::lookup_service_admission_failure_doesnt_block_submit` (the
  output is no longer held).
- A fault AFTER them (the spent mark of a stale coin, the consumed-by update,
  the applied row) leaves the transaction's outputs and deletes the stale
  coins all the same, as the reference does. `c9921ee` skipped the delete on
  any fault and left two unspent heads.
- A submit that deletes a stale coin FIRST records that coin's transaction as
  applied (the delta fold, H2; `record_spent_coin_applied` in the report when
  that write faults, and the coin is then not deleted). A held coin proves
  its transaction's inserts landed; without the row, the replay of a
  transaction the manager admits with NO previous coin (the opener of a
  chain: every one of this workspace's 16 managers admits on output shape
  alone, and Zanaadu's pf head manager admits a configured genesis) inserted
  it a second time, unspent, beside the output that had spent it.
- A fault at the delete. BEFORE any delete was started (H2's record, or the
  read of the coin, answered an error): if every stale coin is read back
  still held, the inserts are undone (the first case); if one is gone or
  cannot be read, the outputs stay. Once a delete was STARTED nothing is
  undone, whether it answered an error (the second delta fold, M1) or did not
  answer (a finalize submit's bounded call; the delta fold, H1): a statement
  can land after its caller was told it failed or stopped waiting, the coin
  can be read back held and be deleted a moment later, and the undo then left
  no head. The outputs stay beside the stale coin (marked spent), with no
  applied row, and every order converges: the replay first finds the coin and
  finishes the delete (or finds none and is recorded with its outputs held);
  a successor first makes the replay a dupe (H2).

Who replays: the queue (`/submit`, `/arc-ingest`) replays a faulted submit
up to 3 times (`max_retries`), then the message goes to the dead letter
queue, whose consumer parks it in D1 (bsv-low #576, "The dead letters"
below); only the operator's lever replays it after that. GASP replays only a transaction whose outputs are NOT held (its UTXO
failed, the cursor stayed, the next tick walks it again): one whose outputs
landed is known to the walk and is never submitted again, so its applied row
is written by its spender (H2) or by a later submit of it at the door.

A successor that found no previous coin while a predecessor's landing is
UNKNOWN is reported as a fault (`predecessor_not_landed`), nothing of it is
written and it is not recorded as applied (#559 for one that admits nothing;
lane E1D, bsv-low #575 and zanaadu-v2 #365, 2026-10-08, for one the manager
admits WITHOUT its coin, the pf head manager's case: it was inserted and
recorded, and the predecessor, landed later, stood unspent beside the tip).
"Landed" is an applied row in the topic or an output held there. Known four
ways: the engine saw the predecessor fault (one invocation's memory, every
door, the finalize submit included), or the store says so
(`Engine::unlanded_predecessor`, any invocation, never at a finalize submit)
of an unlanded transaction that is a PREDECESSOR because (a) its body is in
the BEEF and it spends a coin the topic holds (or a coin of another such
transaction); (b) its body is in the BEEF, it spends no coin the topic holds,
the manager NAMES the output the walk spends of it (`identify_needed_inputs`
over the transaction that spends it) and a dry run of the manager over that
body with no coins would admit that named output (the OPENER, E1D: the cure
D17 named; a manager `Err` there is "not now", its typed refusal
`NoAdmissibleOutputs` is not); or (c) its body is NOT in the BEEF (a PROVEN
successor carries none) and the manager names the outpoint as overlay history
(D13's word; an `Err` names nothing). The dry run asks only of a NAMED output
(the E1D lens fold, H1): run on every unheld body, a manager that admits on
shape made a spend of a shape-admissible output this node never held (a
revocation of an ad it never saw; a stranger's own few-sat SHIP output) "not
now" on every presentation, three replays and a dead letter each. A
successor that admits nothing starts the walk from every input; one that
admits starts it only from the inputs the manager NAMES, and the engine's
memory looks only at those too (the lens fold, L1), so under a manager that
names nothing (15 of this workspace's 16) an admitting successor is asked
nothing, as in the reference, and its BEEF is not even parsed; a successor
that admits NOTHING is still asked of every input, so it waits for, or has
landed first, a carried body that spends a coin the topic holds (case a):
there the store differs from the reference's, which records the successor and
never admits that body (the E1D delta fold, L3). The manager is
asked over the SUBJECT-NAMED BEEF (BRC-95 atomic, what the lookup services
get; the lens fold, M2): over the submitted bytes a manager that parses
`from_beef(_, None)` took the wire-LAST transaction of an out-of-order BEEF
and named an ancestor's inputs, and the cure turned itself off.

The door lands a CARRIED predecessor first (the lens fold, M1; the walk's rule,
at the door; `Engine::land_carried`): when the question names an unlanded
predecessor whose body the BEEF carries (cases a and b), that body is
submitted on its own to the topic (its atomic BEEF out of the successor's, no
off-chain values, never a broadcast), ancestors first on an explicit stack,
and the successor's topic is then judged again over the coin it now finds.
The first that does not land ends it: the successor waits. Each link is
ADMITTED once (the E1D delta fold, L4): a link blocked by the one under it is
asked only a dry run and writes nothing, and is admitted for real once that
one landed. Each body is WALKED once per topic of the submit: the successor's own SPV walk
already covered every body it reaches without crossing a proven one, so
those are not walked again; a body reached only through a proven one gets
its own walk, trusting what the submit already walked. The submit's 256
reads bound EVERYTHING the door reads past the subject's own validation and
writes (the delta fold, M1): the question's reads, each landing's (its
applied row, one coin per input, and its writes' one read per coin, charged
before it starts) and each re-judgement of the successor after a landing
(its applied row, one coin per input, charged BEFORE the landing: a landing
whose re-judgement does not fit is not made). A carried chain too deep for
them is "not now"; the links that landed before the reads ran out stay
landed and the replay goes on from them. A wide successor lands what the
256 leave room for after each re-judgement (one per presentation at 240
inputs, none at 252). Not charged, and
stated: a landing's fault-path reads (the read-back of its coins, the undo of
its outputs, one each) and the deep delete's walk of retained history (as in
any submit). Nothing is landed into a topic whose manager or any lookup
service READS off-chain values (`TopicManager::reads_off_chain_values`,
`LookupService::reads_off_chain_values(topic)`, both DEFAULT `true`; the
delta fold, L1): the landing has none to give, the predecessor's own submit
would then be a dupe and its values never told, so that topic waits for the
predecessor's own submit. Every manager and lookup service of
`overlay-discovery` answers `false`; a consumer's that does not say lands
nothing first (683dffd's answer for it). The worker's queue replay carries no
off-chain values at all (`MutationMessage` has none): on LOW a faulted
submit's values were already lost at its replay, before and after this. Each
landed body is reported (`MutationReport::landed_predecessors`, ancestors
first; the delta fold, L2). Before a body is landed the engine asks the
caller's predicate (`Engine::set_landing_guard`, once per body, before its
submit and its reads; the delta-2 fold, L2): the worker installs its
eviction ledger (`admit_fast::landing_guard`) at every engine it builds, so
a body under an OPEN eviction, or one whose ledger read faults, is not
written, no lookup service is told, the successor's topic is "not now"
(`landing_refused_evicted_total`; a fault `admit_fast_ledger_unreadable_total`).
A gated door's accept of the successor does not readmit the predecessor's
row: it is the network's word on the successor, whose EF carries the
predecessor's outputs and not the predecessor, and a pending accept can turn
out an orphan; the predecessor is readmitted by its own word (its gated
submit, a MINED proof, `/admin/readmit`) and the successor's replay lands
after that, or its dead letter waits for #576. After the write the worker
still guards each landed body like the subject's own write
(`admit_fast::guard_landed`, at `/submit`, the queue replay and
`/admin/readmit`; the belt for a row opened while the landing wrote): one
under an OPEN eviction is re-evicted, counted
(`admit_fast_reevicted_after_write_total`); an unreadable ledger is counted
and logged and asked no more (the retry dedups and lands nothing). The peer
crawler guards its subjects not at all (pre-existing) and its landings only
before the write (its engine carries the predicate); a GASP finalize lands
nothing. A subject that already HOLDS an output in the topic (its own
earlier submit's leftover: the delete started and faulted, D17 M1) does not
wait: its replay finishes it (the lens fold, L4: a proven head spend naming a
decoy was "not now" on every replay); one read, only on the way to "not now".
"Landed" needs
a clean answer, so a read that faults or the question running out of its
reads is "not now" too (the delta fold, M1 and M2). Two bounds, both ours
(E1D's reads, an absent named body's applied row and outputs, are counted in
both as an unproven body's; a dry run reads nothing and runs at most once per
body the reads reached).
The question costs at most 256 store reads per SUBMIT over every body it
reads, in every topic of the submit together (the third delta fold, M1:
`PREDECESSOR_READS_PER_SUBMIT`; a later topic's question starts from what the
earlier ones left). It is the question's ONE bound (bsv-low #791, FIX-791-C,
the owner's ruling, record 901 item 2): every read counts, a proven or landed
body's included. Until it a topic also stopped at 16 reads spent on bodies
neither proven nor found landed (`PREDECESSOR_READS`, the second delta fold,
M2), which FIX-791-B (616a226) counted late for a coin read into a carried
body; that count bounded an honest hand's unproven DEPTH (about eight carried
single-input bodies: beta's two parked JOINs counted 16 and 18 on 4ec0507),
never the cost the 256 already bound, and it is gone with its constant. The
reads made are FIX-791-B's, read for read and in order. Its cost: the first
topic of a submit that asks can spend the whole 256 and leave a later topic's
question "not now"; no LOW submit's question reads in two topics (`tm_pot`
admits the JOIN's covenant and names nothing, `fix791c_e` audits all 16
managers; the spend-proof heal, a settle and its BUMP under both, is
`tm_pot`'s D17 (a) class, asked of every input, and reads nothing there
because the pot's body is absent and unnamed, pinned by `fix791c_e` too), and
the per-topic share returns the day one does. The boundary pins restated for
the 256 (`--test gasp_topic_manager`): `delta559_m2_x7_*` (a head chain's
question runs out at head 87, its landing does not fit at heads 85 and 86;
it was head 11 under the 16), `fix791_a` (85 funding hops read 256 and
record, one read more is "not now"; 8 hops recorded, refused under the 16),
`fix791b_b` (all three shapes record), `delta2_559_m2_y2` (six and 85
unproven single-input parents record, 86 run out). The design is in bsv-low
`docs/DESIGN-791-HOP-LEDGER-2026-10-20.md`. A `predecessor_not_landed`
fault keeps 320 characters in `MutationReport::summary` (120 for every
other site), so the dead letter keeps the budget's figure. A GASP finalize
submit does not ask the store at all (its graph passed the anchor check, its
in-graph parents were submitted just before it and the sequence stops at the
first that does not land); it keeps the engine's own memory.

`Engine::set_finalize_submit_budget` bounds each storage call, lookup hook
and manager call of ONE transaction's finalize submit (the write section no
deadline drops) against one deadline: a call that has not answered is dropped
ALONE and is that call's fault, no call is started after it, and the SUBMIT
runs to its end (never a drop between two writes); the undo has one more
allowance. The UTXO fails and the cursor stays (the worker sets 30 s).
`/admin/startGASPSync` runs under the same guarded 240 s race as the scheduled
step and answers 504 when it is dropped. Pins: `cargo test -p
bsv-overlay-engine --features memory-storage --test gasp_topic_manager i559`,
`fold559`, `delta559` (the delta lens's X4 to X9, each RED on `33e78fb`),
`delta2_559` (the delta-2 lens's Y1 and Y2, a finalize over a node with 17
landed parents, rows 2 and 6 of its table; the M1 and M2 pins RED on
`e24f962`), `delta3_559` (the delta-3 lens's M1: the 256 reads of one submit
to the read, and two topics sharing them; RED on `5ecf49c`) and `e1d` (lane
E1D: `e1d_a` Zanaadu's run on the GASP path, `e1d_b` the door with a proven
successor in both classes, `e1d_c` the opener with and without its body,
each RED on `8d147d7`; `e1d_d` and `e1d_e` what did not change) and
`e1d_fold` (the E1D lens fold: `h1` the lens's revocation of an unseen ad,
`m2` a subject-first BEEF, `m1_a` the door landing a carried predecessor and a
carried chain, `l4` a replay over a named decoy, `l3` the walk's capped reads,
each RED on `683dffd`; `m1_b` the landings' bound; `e1d_c` and `e1d_d` were
amended, both RED on `683dffd`) and `e1d_delta` (the E1D delta fold: `m1`
the read count of one submit over carried spends, to the read, `l4` each link
of a carried chain admitted once and walked once, `l1` a predecessor's
off-chain values never taken by a landing, each RED on `0da3a82`; the
worker's `admit_fast::tests::e1d_delta_l2`, RED on `0da3a82`, a source-shape
pin) and `e1d_fold3` (the delta-2 fold: `l2` the door asking the landing
guard before it lands, RED on `f057acc` with the API grafted inert; its
route-tier cell `tools/lane-e1d/landing_guard_route_ci.mjs`, run by `make
ci-d1-budget` over a real open eviction row, RED on `f057acc`). The pin of the
opener limit (`limit_opener`) is retired: `e1d_c` is its cure.

The GASP walk re-asks the predecessor FIRST (E1D, an addition to D13): a
PROVEN node whose own output the no-coin dry run ADMITS no longer ends the
walk blindly (the reference stops there). Its manager's named inputs are
asked, and those neither held nor landed are requested from the peer (or
the chain fetcher), so the predecessor joins the graph and its finalize
submit comes first; one that does not land stops the graph and the UTXO
waits for the next tick (the gap guard); one the peer cannot serve prunes
(D14). When every named input is held or landed, or the node spends
nothing, the walk stops as the reference's. The "landed" reads of one node
are capped at 16 (two per transaction at most; the lens fold, L3): past them
an input is requested, as on a read fault. A predecessor whose manager
answers a permanent `Err` in the anchor replay (pf_name's terminal
`head_race`) now holds the cursor below its successor's UTXO (D15's
contract), where the base recorded the admitted tip alone (lens N2). For
pf_name the re-ask does not widen #555 (its dry run refuses an untracked tip
on a fresh node); a STATELESS manager that admits on shape AND names its
inputs would walk to its genesis on every bootstrap, where the reference
stops at the tip (lens N1). Pins `e1d_a`, `e1d_e`, `e_admitted_output_*`
(the extra names call, stated), `e1d_fold_l3`.

The limits, stated. (1) Two faults in one submit: an undo is separate calls,
and one that faults too leaves the inserted output beside the kept coin
(`undo_insert_output` in the report). A successor that spends it before the
replay no longer brings a double head back (the replay is a dupe, H2); the
kept coin's spent row stays behind. (2) A lookup hook that faulted is told
again on the replay since #657 (the transaction is re-applied whole over the
coin it kept; under GASP its outputs are not held, so the UTXO fails and the
walk submits it again), so a lookup service's hooks must be idempotent; the
reference tells nobody twice. A hook that faults DETERMINISTICALLY holds the
transaction out for good (three retries and a dead letter at the door, the
cursor held under GASP) with the stale coin kept and marked spent, so the
topic lists no UTXO for that head until the hook is fixed: the cost of the
rule, stated. (3) The lookup
services are told of a stale coin's spend, and of each inserted output,
BEFORE the transaction has landed (the mark and `output_spent` run before
the inserts, as in the reference): on an insert fault, or a notification
fault since #657, the store keeps the old
head while every lookup service was told it is spent, and an output that was
inserted and undone was told as admitted, with no retraction. The replay
heals it; after the dead letter the split stays until the next head. (4) The
store's answer for a body the BEEF does not carry is the manager's word: a
manager that names nothing (or a predecessor it does not name) leaves a
PROVEN successor recorded as in the reference, and a successor the manager
admits without its coin is asked only of its named inputs, so the 16
workspace managers' admitted successors (a SHIP update over an ad this node
never saw) are recorded as in the reference, the phantom included. A named
input that never lands (a decoy no peer serves, with the real predecessor
landed and holding no coin) keeps its successor "not now" at the door,
where GASP prunes it (D14), unless the successor already holds its output
(L4 above). A successor whose predecessor's body is ABSENT converges only
when the predecessor lands (its replay, a GASP peer, a resubmit); after its
dead letter every successor at the door is dead-lettered too until an
operator re-drives it, a frozen chain at the door on a node with no GASP
peer (the lens fold, M1: the 2026-08-26 phantom-ack class). What converges
by itself: a carried predecessor that lands (in the successor's own submit);
an absent one that lands while the successor still has replays left (its
own replay, a GASP peer serving the chain, a resubmit), the successor's next
replay then landing over its coin; and on a node with a GASP peer, a chain
whose successors were dead-lettered, which the walk brings in from the
peer. What needs the operator's lever: a dead-lettered predecessor, and every
successor dead-lettered while waiting for it (the predecessor landing later
does not bring a dead letter back). The lever, bsv-low #576 (built; see
"The dead letters" below), parks `low-overlay-mutations-dlq` in D1
(`mutation_dead_letters`) and re-drives it on
`POST /internal/redrive-dead-letters`, at most 200 letters per call (default
25), oldest PARKED first (the order the letters died, not chain order: a
successor can die first, a re-parked predecessor moves to the back and the
main queue runs a successor and its predecessor concurrently, so re-drive
the predecessor BY TXID first, then the rest; lens N1); each replay is the
same bytes through the same door,
dedup-safe, and a letter is re-driven at most 3 times. What the lever
does NOT heal: a predecessor that never lands whatever replays it, carried
or absent (its manager fails it for good, its own walk refuses it, its
landing or its re-judgement does not fit in the 256 reads, a decoy no peer
serves): the replay is the same refusal and its successors are "not now"
at every presentation, re-driven or not (the delta lens, N7), until an
operator removes the cause. A finalize submit asks only the engine's memory,
so a successor whose named predecessor the peer pruned is recorded as in
the reference. The reference (ts-stack `Engine.submit`, `f999e0c1a`)
records every topic that did not fail, whatever its predecessor: each
"not now" here is a delta. At `/submit` an admitting successor's "not now"
is the S2 queued ack (the STEAK names what will be admitted once its
predecessor lands, and nothing is held until the replay); the dry runs and
the names call are manager CPU inside the Worker's cap, whose breach is the
platform's error and never a record. Pre-existing and not widened: a graph
deeper than the budget (#555; deferred and resumed since, with a per-graph
budget), the unbounded anchor verify (#557). A "not
now" over a BEEF above 90,000 bytes answered 502 on every presentation
(#568): CLOSED by #585 door 3 and its fold-2 (a body past the queue's inline
room rides by key in R2, and the door refuses none for its size; "The queued
BEEF in R2" below). It cannot tell a faulted predecessor from one nobody submitted
yet: the successor is "not now" until it lands. And a transaction that admits
nothing, found no coin and whose question needs more than 256 reads over the
submit's topics, whatever its bodies are (a landed body costs one read by its
applied row or two by a held output; an unlanded one two and one more per
input: 86 single-input parents nobody holds, proven or not, or 43 under two
topics; since FIX-791-C there is no per-topic 16 inside it), is "not now" on
every submit, where the reference records it: three retries and a dead letter
each. The question never follows the BEEF past a constant. (5) What D1 does with a statement whose caller stopped waiting, or was
answered an error, is not known here; the engine assumes it may still land,
at every door. A delete that never lands leaves the stale coin's spent row
until that transaction's replay at the door finishes it (GASP does not replay
a held head), or for good once a successor spent the new head first; an
insert that lands late leaves it the same way. (6) H2's row is per transaction, not
per output: a transaction with several admitted outputs of which only some
landed (a second insert that landed after its timeout) is a dupe once a
successor spent one of them, and the output its undo took out is not put
back. (7) "Landed" reads `applied_transactions` (lens N4, traced against
`storage-ownership.json`, `rebuild_class: chain`): a wipe of that table
ALONE makes every spent-and-deleted transaction read "not landed", and a
re-presented old head whose own row went with it would have the door land its
carried chain again from the opener, a second head beside the tip for a
manager that needs its coin. Never wipe it without `outputs` (a full rebuild,
which re-lands from nothing and converges). (8) A successor that is also a
predecessor cascades (it lands once its own predecessor does); N replays of
one write nothing but the manager's own idempotent state (lens N5, pinned by
`e1d_fold_m1_a`'s dupes). (9) The landings, by the delta lens's notes. N1: a
stranger's BEEF makes the node submit bodies it was not asked to admit, each
one the stranger's to submit directly (bound by txid to the subject's input
ancestry, into the subject's topic only, under the same manager and walk):
no new admission. A landed body skips the route's per-subject steps other
than the eviction guard (L2 above): the `network_seen` latch, the pending
watch and the census do not see it. N2: a landed body is a whole submit
(recorded, its lookup services told, its faults under #559's rules) that
nobody acks; a landing that does not land makes the successor "not now",
and the queue replays the SUCCESSOR's bytes, which land it again. N3: a body
reached only through a proven one gets its own walk (L4 above) and one that
fails it, or whose sources the BEEF lacks, does not land: the successor is
"not now" at every presentation, as on `683dffd`; not pinned. N4: two
successors in flight carrying one predecessor are two submits of one body,
which #559's rules cover (idempotent inserts, H2's record, one applied row);
not executed (`MemoryStorage` does not interleave). N5: GASP is unchanged by
the landings (a finalize never lands). N6: the replay that finishes a
subject already holding its output (the lens fold, L4) trusts that output;
one written by a finalize over a predecessor the peer pruned, or one an
eviction stripped of its predecessor's rows, is recorded over an unlanded
predecessor: the pruned case is limit (4), the eviction L2's guard.

### The hop ledger names a JOIN's hops (bsv-low #791, FIX-791-C; the owner's ruling, record 901 item 1)

The client's hop post is best-effort, so a JOIN reached `tm_lowfund` over hops
the ledger never saw: the JOIN was recorded and the hops' spend was not (beta's
two parked letters, `docs/DESIGN-791-HOP-LEDGER-2026-10-20.md` in bsv-low).
`LowFundTopicManager::identify_needed_inputs`
(`crates/overlay-discovery/src/pot/lowfund_topic_manager.rs`,
`join_hop_inputs`) now NAMES, for a JOIN (a transaction with a pot covenant
output, as `tm_pot` recognises it), each input whose source body the BEEF
CARRIES and whose spent output is a P2PKH (the rule the manager admits by),
in input order. Nothing else is named: a hop is no JOIN, an input whose source
the BEEF does not carry is never named (its lock cannot be read; a proven JOIN
presented alone names nothing and is recorded as before), bytes that do not
parse name nothing. The bound: the names are the JOIN's own inputs, never a
walk; no source is hydrated. For an honest JOIN that is the covenant's two
seats' hops; for a stranger, any number (the covenant check reads the
script's head and tail only, so any covenant-shaped output over N carried
P2PKH-sourced inputs names N), bounded by the submit's 256 (`fix791c_d`).

So the door lands each named carried hop first (D17 rule 2: the hop's dry run
admits its stake, an opener), ancestors first, each landing charged to the
submit's 256, and the JOIN then finds both coins: its admission marks each
stake spent, tells the lookup services (`ls_pot`, which indexes `tm_lowfund`
outputs into `pot_records`, records the JOIN as the spender), and deletes it (D17 rule 1; the hop's change stays
held, its applied row stays). A hop the ledger already holds is found as a
coin and landed nothing (no second applied row, no second insert). The 256
counts READS: a landed body's inserts (every P2PKH output of a hop) are its
own submit's writes, the ones its direct public submit makes, multiplied by
at most the landings the 256 allows (about 60 single-input bodies); the D1
per-invocation statement limit the 256 is sized against is the register's
stated unknown.

A JOIN whose named carried hop does NOT land (the landing guard refuses it
under an open eviction, its walk could not run on an unmarked replay, or its
landing does not fit the 256) is "not now" in `tm_lowfund`, where `0673eea`
recorded the JOIN alone there; `tm_pot` asks nothing and admits the pot all
the same, and a hop before it in input order that did land stays landed. It
is money-neutral (the hop's spend was unrecorded either way) and D17's
designed answer: the hop's MINED proof (or the operator's readmission) lets
the replay land it, else a `not_now` letter under #576's bounds. Pin
`fix791c_f` (RED with `join_hop_inputs` answering nothing: `left:
["tm_pot", "tm_lowfund"]`).

The engine change it needs (`Engine::successors_waiting`): a successor that
found SOME of its coins is now asked too, of the inputs the manager names that
the topic does not hold AND whose bodies the BEEF carries, and again after
each landing while one is left. Before, a found coin ended the question, so a
JOIN over two unposted hops landed the first and recorded without the second.
An absent named input is still no reason to wait once a coin was found
(`e1d_fold_l4`, a head spend over a held genesis that also names a decoy, is
unchanged). Under a manager that names nothing (the other 15) nothing more is
asked: one names call per such topic, no store read. Not at a GASP finalize.
For any consumer manager that NAMES inputs (Zanaadu's), this widens the
question: a carried named input that is unlanded is landed first even when
another input's coin was found.

On GASP the names are asked only for a proven node whose dry run admits
(`find_needed_inputs`): a JOIN under `tm_lowfund` admits nothing (its one
output is the covenant), and a node's BEEF there carries no sources, so the
walk is unchanged.

Pins (`--test gasp_topic_manager`, each RED on `0673eea`): `fix791c_a` (two
unposted hops land, both stakes told spent by the JOIN, the changes held; RED
`left: []`), `fix791c_b` (one hop held: the other lands once, the replay a
dupe; a JOIN alone names nothing; RED `[0]: landed once, left: []`),
`fix791c_d` (a stranger's JOIN over 2 to 200 named P2PKH bodies lands what the
256 hold: 2, 8, 5 of 40, 2 of 120, 1 of 200; the submit's reads at most its
own validation and writes plus 256), `fix791c_e` (the audit of the 16
managers over a JOIN, a hop, a settle and a sweep: only `tm_lowfund` names,
and only the JOIN's 2; a JOIN under `tm_pot` and `tm_lowfund` reads exactly
the sum of each alone; the spend-proof heal under both reads each topic's
applied row and coin and nothing more, RED with the absent-body name check
removed: `tm_pot` reads 5 and waits), `fix791c_f` (above), `fix791b_a` extended (beta's two letters each land
their JOIN's two hops, in 23 and 21 reads, the stakes gone, the changes held;
its old assertion, the JOIN recorded, kept). Two mutants: the found-a-coin
entry removed (RED `fix791c_b`), the loop ending at a found coin (RED
`fix791c_a`, `fix791b_a`: one hop landed of two). `--lib lowfund` in
`overlay-discovery` (`names_a_joins_carried_p2pkh_hop_inputs_and_nothing_else`).

## The dead letters (bsv-low #576, its lens fold, delta fold and delta-2 fold of 2026-10-08)

A mutation the queue dead-letters (a `/submit` or `/arc-ingest` replay that
was not durable on any of its 1 + `max_retries` deliveries, an e1d "not now"
over a predecessor that does not land included) is never lost unseen and
never re-driven blind (`dead_letters.rs`). The main consumer notes each
failed replay's fault and attempt on a SMALL `failing` row (no message
bytes: the queue holds them) and DELETES the key's row on an ack (its bytes
landed, or were refused under an open eviction, and the `RESOLVED` line says
which, delta fold D-L3; one of the two scoped deletes of this never-wipe
table, `storage-ownership.json`'s `delete_scope`; a parked or re-driven
letter so resolved is counted `dead_letters_resolved_total`). It hands back, never
acks, a body that does not decode and a BEEF that is not base64 (lens N4:
they were acked silently), so they dead-letter and are parked too.

The DLQ consumer is the Worker's `queue` export, written out in `lib.rs` as
worker-macros' `event(queue)` writes it, so it gets the platform's own
message objects and reads `attempts` (workers-rs 0.8.5's `Message` hides
it); it branches on a queue name ending `-dlq` (bound in `wrangler.toml`,
prod and beta of `wrangler.low.toml`: `max_batch_size = 10`, `max_retries =
100`, the platform's maximum, `retry_delay = 300`, no DLQ of its own). It
PARKS the message as is in `mutation_dead_letters` (key: subject txid by D5
and the sorted topics; the fault, the attempts, `parked_at`, a history entry
per park, at most 20 kept). PARK FIRST, ACK AFTER: a message is acked only
after its park answered. A park that faults (D1 down, the binding or the
migrations included: the batch no longer throws) is handed back with 60 s
doubling to 30 min from the platform's count (about 48 h over the 100
retries; an hour's outage is ridden by the 6th); its LAST delivery's fault
logs `[dead-letters] LOST <txid> [<topics>] sha256=<bytes' hash> class=<class>`
and counts it BY CLASS (the delta-3 fold, D3-L3): `dead_letters_lost_total`
for a `fault` letter or one whose class was not read (the honest loss to
alarm on), `dead_letters_lost_not_now_total` for a `not_now` one (the
designed fate of a flood's tail); both are served from 0 in the counters
(best effort: D1 is usually what faulted). Lens
H1: Cloudflare's queue docs give `retry_delay` no default and the base set
none, so a minute of D1 trouble lost every letter in the DLQ. A redelivery
of the same bytes changes nothing; ANOTHER copy of a parked key (a resubmit,
a different carried ancestry) keeps the LONGER bytes and leaves a `copy`
history entry (lens L2; since #585 door 3 "longer" is what a message
CARRIES: a keyed letter weighs its BEEF, an inline one its message, and the
dropped copy's R2 object is deleted). The `PARKED`, copy and redelivery lines print the
platform's `attempts=` (or `absent`; delta fold D-L2), so a drill that parks
cleanly reads it. At most 2000 letters hold bytes (`parked` +
`redriven`; a row is one queue message, at most 128 KB, and 20 history
entries; a letter whose BEEF is past the queue's inline room holds its R2
KEY, `r2_key` / `r2_bytes`, and the bytes stay in R2, #585 door 3): a NEW letter past that is not parked, it is handed back with the
same backoff, counted once as a letter on its first DLQ delivery
(`dead_letters_ceiling_deferred_total`, `attempts == 1`) and on every
delivery (`dead_letters_ceiling_deferrals_total`, up to 101 per fault
letter and up to 5 per `not_now` letter since #581, below; delta fold D-L1), shown as `ceiling.full`, and LOST after ~48 h at the
ceiling (lens M2: a stranger's "not now" bodies, limit (4) above, fill the
ceiling, not the shared D1).

The ceiling is split by CLASS (the delta-2 fold, D2-M1). The delta-2 lens ran
it: a subject with six unproven, unconfirmed ancestors is "not now" on every
presentation through the PUBLIC gated door, one letter per topic subset (up
to 8,191 on LOW's 13 topics) and per sibling subject. The 3 retries bound
the deliveries of a letter, not the number of letters, so a stranger filled
the 2000 places and every HONEST letter after them (a D1 fault on a real
admission that the door had acked "queued") was lost after ~48 h. Each row
now carries `class` (migrations 168-169), written by the main consumer's note
of each failed replay (the last note wins): `not_now` when EVERY fault of the
replay is the engine's `predecessor_not_landed`; `fault` for anything else,
and for a letter no note reached (an unknown is treated as an honest letter).
A NEW `not_now` letter is deferred (counted on every delivery,
`dead_letters_not_now_deferrals_total`, apart from the ceiling's counters)
at any of three bounds (`ceiling_verdict`, and again inside the park's own
statement, below). Since bsv-low #581 (B-COST-1) a HELD-BACK `not_now`
letter (at these bounds or at the whole ceiling) has its own plan
(`deferral_plan`): handed back for 12 h (`NOT_NOW_DEFER_DELAY_S`, the
platform's longest per-message delay) and given up at its 5th DLQ delivery
(`NOT_NOW_DEFER_DELIVERIES`): LOST, counted
`dead_letters_lost_not_now_total`, its R2 object deleted, ACKED. The 48 h is
kept (the day's bound frees in 24 h) in 5 deliveries where the shared
backoff (60 s doubling to 30 min) took 101, so the one consumer's work per
held-back letter is at most 5 deliveries and a fault letter is never behind
more than that per not-now letter (served:
`/health/invariants.deadLetters.classes.notNow.heldBackDeliveries` and
`heldBackDelaySecs`). A fault letter, a park that faulted and an unreadable
`attempts` keep the shared backoff; the give-up's ack is gated on the class
condition itself (a deferral of a `not_now` letter), not on the count alone
(the B-COST-1 lens's NOTE-2, pinned by `e576f_h1`). Stated (the lens's LOW-1,
chosen over keeping the shared backoff for the first hours, which would break
the bound of 5): the DEADLINE is as before, the ODDS are not. The letter
looks for room 5 times in its 48 h (at 0, 12, 24, 36 and 48 h) where it
looked about 101 times, every 30 min at most; every look runs the park
first, so a place free at a look is taken, but a place that frees and is
taken again BETWEEN two looks (a flood's fresh letters, delivered at once,
taking each freed place of the 1000 share or the day's 200; a third topic
set of the same txid taking the per-txid place) is one an honest `not_now`
letter could have parked under the shared backoff and is now LOST. Such a
letter converges as any lost `not_now` letter does (its client
re-presents it after its predecessor lands, or a GASP peer brings it in).
Pins `--lib b_cost_1_581` (RED with the not-now arm removed: `left: 101,
right: 5`).
- `NOT_NOW_PER_TXID` = 1: one `not_now` letter per txid over all its topic
  sets. Another topic set waits until that letter resolves or is discarded.
  An honest successor is presented under one topic set.
- `NOT_NOW_MAX` = 1000: half the ceiling. Fault letters always keep 1000
  places, however long a flood runs; fault letters themselves may fill all
  2000.
- `NOT_NOW_PER_DAY` = 200: new `not_now` letters parked in a trailing 24 h,
  counted from the held letters' `parked_at`, so a discard frees the day's
  room too. The gated door carries no caller identity ("not a per-identity
  handshake", `submit_gate.rs`), so the bound is per day, not per source. The
  share then fills in five days at the soonest, not at door speed.

The bounds hold under ONE consumer of the DLQ, and they hold under several too
(the delta-3 fold, D3-L1). The delta-3 lens ran the read and the park as two
consumers interleave them: 3 letters of one txid and a share of 1,004. Two
changes fix that:
- Every DLQ consumer is ONE consumer (`max_concurrency = 1` in `wrangler.toml`
  and in prod and beta of `wrangler.low.toml`; the DLQ is low-volume by nature).
- `PARK_SQL` is an `INSERT ... SELECT` whose `WHERE` re-reads the bounds of
  `ceiling_verdict`. D1 runs one statement at a time, so two parks that both
  read room put ONE letter in it. A park that returns no row is read again
  (`unparked`): a redelivery of held bytes (acked), the deferral it now is, or,
  when the room freed again since, a fault handed back with the backoff (never
  an ack).

The read before the park still gives the NEAR line and the deferral's words.
The count the NEAR line prints can be one letter off under a race.

A key that already holds bytes is never deferred: a copy, or a re-park of a
re-driven letter whose replay is now "not now". One row per (txid, topics) was
already the primary key. A re-presentation collapses onto that row: the
history grows (capped at 20 entries), the rows do not. A "not now" letter
keeps its bytes. The cheaper byte-less form was not taken because nothing
could re-read the body after the queue's ack. The health block names the
classes apart: `classes.fault` {`held`, `kept` (1000), `room`} and
`classes.notNow` {`held`, `max`, `full`, `perTxid`, `perDay`,
`parkedLast24h`, `dayFull`}.

What the split does NOT do, stated:
- Honest "not now" letters share the 1000 places and the 200 a day with a
  stranger's. During a flood an honest successor's letter waits and can be
  LOST. That letter converges again only when its client re-presents it after
  its predecessor lands, or when a GASP peer brings it in.
- A stranger who can make a FAULT-class letter at will is outside this bound
  (a storage fault, a body the replay cannot parse that the door accepted).
  None is known; the door builds every enqueued message from bytes it parsed.
  One was found and closed (#585 door 3's lens, E585-D3-L4): a re-presented
  twin whose R2 object the LOST of a deferred "not now" letter deleted was
  noted `fault` by the missing object; that note now KEEPS the row's class.

The ceiling does not drain by itself (delta fold, D-M1): only an ack of a
replay and the operator's discard take a letter out, and a letter that can
never land (exhausted, `undecodable:`, `unparsed:`, a body the engine
refuses for good) holds its place for ever. From 1600 letters (80 %) the
health block says `ceiling.near` (with `nearAt`, `room`) and every park at
or past it logs `[dead-letters] the ceiling is NEAR`. `POST
/internal/discard-dead-letters` (the same bearer), body `{"letters":
[{"txid": "<key>", "topics"?: "<sorted, comma-joined>"}, ...]}`, 1 to 50
keys per call (more is a 400, not clamped), deletes those PARKED letters
(no `topics`: the parked rows of the txid; never a `redriven` letter in
flight, never a `failing` note), at most 50 ROWS per call over all its keys,
oldest parked first (the delta-2 fold, D2-L1: a txid alone deleted all its
rows in one statement, 120 in the lens's run, every row's bytes returned at
once), the second scoped delete of the table
(`DISCARD_SQL`), logs each `[dead-letters] DISCARDED <txid> [<topics>]
sha256=<bytes' hash> bytes=<n>` with its re-drives and last fault, counts
`dead_letters_discarded_total`, and answers `discarded` (with each hash),
`notFound`, `faults`, `notTried` (keys past the call's 50 rows) and `more`
(a txid alone that may hold more rows: call again). A discarded letter's bytes exist nowhere after it:
it is the operator's word, as a re-drive is. A discarded letter whose BEEF is
an R2 object has that object deleted with its row (`r2Key`, `r2Bytes`,
`r2Deleted`, and `r2Fault` when the delete faulted: the row is gone, the
object stays, `wrangler r2 object delete` takes it). A discarded keyed
letter's object is deleted at once; an object nothing names any more (a bulk
discard whose deletes faulted, an R2 audit) is the orphan sweep's, and the
third lever of the same bearer, `POST /internal/beef-blob-sweep` (bsv-low
#585 door 3's fold-4; "The queued BEEF in R2" below), runs one pass of it on
demand.

`POST /internal/redrive-dead-letters` (bearer `INTERNAL_TOKEN`, as
`/internal/reorg`, compared in fixed time since the lens fold, L1), body
`{"limit"?: n, "txid"?: "<key>", "force"?: true}`: `limit` default 25,
clamped to 200. It first returns to `parked` every re-drive claimed more
than 1 h ago that neither resolved nor parked again (a send that never
left, an isolate that died after its claim, a re-driven message dropped on
the way; lens M1), keeping its place and its spent re-drive, counted
`dead_letters_stale_returned_total` and shown meanwhile as `staleRedriven`
and `oldestRedriven`; a wrong verdict (the copy still in flight) sends a
second copy, which is dedup-safe. Then it reads the oldest parked rows (or
one txid's), claims each by a compare-and-set (`status = 'parked' AND
redrives = <read>`) that RETURNS the bytes it claimed (lens L3), and only
then sends those once to `MUTATION_QUEUE` as a fresh message (its attempt
count reset to 0: 1 + `max_retries` deliveries again), stamped `reason =
"redrive"` and its key, so a re-death parks the SAME row with its history.
One claim is one enqueue; a send that answers an error but lands is
reverted and can be sent again by a later call, two copies, dedup-safe
(lens N2). A letter re-driven `MAX_REDRIVES` (3) times that parks again is
exhausted: never selected again, listed in
`/health/invariants.deadLetters.exhausted` (at most 20; `exhaustedCount` is
the total) until the operator removes its cause and forces one more re-drive
by txid (`"force": true`, recorded in its history; lens L5). The health
block also shows the counts by status, `parkedLast24h`, the oldest parked
and the last re-drive, from one index-only aggregate (lens L4, migrations
166-167). Counters `dead_letters_parked_total`,
`dead_letters_redriven_total`, `dead_letters_still_failing_total` and the
four above. Nothing re-drives on its own (S2: a dead letter is the
operator's decision).

The register sentence (D17, lens N7): "A dead-lettered mutation is parked in
D1 (`mutation_dead_letters`) by the worker's own DLQ consumer and re-driven
only by the operator (`POST /internal/redrive-dead-letters`, bearer
`INTERNAL_TOKEN`, at most 200 per call, oldest parked first, one enqueue per
claim, at most 3 re-drives per letter then forced by txid, a claim older
than an hour returned to the parked set by the NEXT lever call, listed on
`/health/invariants.deadLetters`; at most 2000 letters hold bytes, of which
at most 1000 are "not now" letters (one per txid, 200 new a day), so a
stranger's flood of them leaves 1000 places to fault letters; a full
ceiling drains only by an ack or the operator's discard by key, `POST
/internal/discard-dead-letters`, at most 50 letters per call, each logged with the
hash of its bytes). ts-stack's `overlay-express` has no queue and no dead
letter (`Engine.submit` catches per topic and never replays): the whole
lifecycle is our platform's addition, and so are its stated losses (a park
that faults on every DLQ delivery for ~48 h, logged LOST, or for ~8.3 h
with NO LOST line when the platform's `attempts` cannot be read, 101
deliveries at the configured 300 s; a new letter deferred at the ceiling
for as long, the ceiling filling for good with letters that never land
unless the operator discards them; a "not now" letter deferred at its
class's bounds for as long, an honest one included during a flood, which
since #581 looks for room 5 times in its 48 h, 12 h apart, and is given up
and acked at its 5th DLQ delivery, so a place freed and re-taken between two
looks is one it misses; a
discarded letter)."

Limits, stated. The first deferral is read from `attempts == 1`: if the
platform's count is absent, or does not restart at 1 on the DLQ move (the
docs are silent; the drill's `attempts=` settles it),
`dead_letters_ceiling_deferred_total` undercounts and the deferrals counter
is the one to read. Exhausted letters still count toward the 2000 (the
delta lens's optional separate cap is not built): the operator discards
them. The DLQ's retention must exceed the ~48 h window (the
default is four days; the captain's `wrangler queues info` says). A
`failing` note whose message the platform dropped stays, a small row,
counted in `failing`. The stale rule cannot tell a lost re-drive from one
whose replay is slow beyond an hour; its cost is a duplicate. Before deploy
the DLQs must exist (`wrangler queues create overlay-mutations-dlq` for
`wrangler.toml`, a NEW queue; the LOW ones were already the producers'
`dead_letter_queue`) and no other worker may consume them (lens N3). The
dead letters already in a DLQ at deploy park with `FAULT_UNRECORDED` (N5).
The lever's budget at 200 letters is about 400 D1 queries and 200 sends,
inside the paid plan's 1,000 queries per invocation; it is an operator route
outside #499's census (N6); so is the discard lever (at most 50 deletes,
each returning its letter's bytes, at most 128 KB, to hash them, and one
counter). Pins: `cargo test --manifest-path
workers/Cargo.toml -p bsv-overlay-cloudflare --lib e576` (the fold's
`e576f_*`: H1's config and backoff, park-first and the LOST line, M1's
stale return, M2's notes and acks and ceiling, L2 to L5, each RED on
`f8b5525`; the main consumer's shape pin amended for N4) and
`tip_pass::tests` (L1); the route tier
`tools/lane-e576/dead_letter_route_ci.mjs` (`make ci-d1-budget`: a real
"not now" successor dead-lettered and parked through the local queue, the
lever's bearer, limit, one-enqueue claim and ceiling, a re-park with its
history, the health block, RED on `835b80c`; the fold's legs 6 to 9, the
new health fields, a stale re-drive returned and re-driven, a forced
re-drive, a bad-base64 replay parked, RED on `f8b5525`). The delta fold's
pins `e576f2_*` (`m1` a full ceiling, a discard by key that spares a
re-drive in flight, a failing note and another key, then the next letter
parked, and the near line; `m1` the discard body; `l1` one letter, 101
deferrals; `l2` `attempts=` on the three ack lines; `l3` the `RESOLVED`
words per reason), each RED on `28f5d0b` (they do not compile there) and
against its fix reverted alone, and the route tier's leg 10 (a
full ceiling defers a real dead letter, the discard lever's bearer and
body, a discard, the letter then parked; RED on `28f5d0b`). The delta-2
fold's pins `e576f3_*`:
- `m1_a_strangers_flood_leaves_room_for_a_fault_letter`: eight days of a
  flood held to 200 a day and 1000 in all, then a fault letter parked, fault
  letters up to 2000, the NEAR count, the `classes` health, the wiring.
- `m1_one_txid_holds_one_not_now_letter`: five other topic sets deferred, ten
  re-presentations on one row, fault letters not bounded per txid, the class
  of a replay.
- `l1`: 120 rows of one txid taken 50 at a time, oldest first.
- `l2`: NEAR at 1600 and not at 1599, over the real read, its log line in a
  bounded slice.

Each is RED on `66d069f` (97 compile errors with the module grafted) and
against its fix reverted alone (8 mutants). Route leg 11: the real e1d letters
are `not_now`; the share is filled; a real "not now" letter is deferred,
counted apart; the same key as a fault letter is then parked.

The delta-3 fold's pins `e576f4_*`:
- `l1_interleaved_parks_never_overshoot_a_bound`: reads of room, then parks, at
  each of the four bounds; a held key at the full ceiling; a refusal whose room
  freed is not acked; the statement's literals; `max_concurrency = 1` in all
  three configs.
- `l2_every_failed_replay_path_notes_its_class`: six notes, only the "not
  durable" one by the report's sites, `predecessor_not_landed` as the engine
  writes it.
- `l2_the_last_note_wins`: both directions.
- `l2_the_migration_makes_existing_rows_fault_letters`.
- `l2_the_health_reads_both_classes_and_the_day`: `class_counts` over the real
  read, one `day_cutoff`.
- `l2_each_deferral_counts_under_its_class`: and the per-txid bound counts
  `not_now` letters only.
- `l2_the_discard_spans_both_classes`.
- `l3_lost_is_counted_by_class`.

The module does not compile on `f918b72` (33 errors). Over `f918b72`'s
`PARK_SQL` and configs, with only the new helpers grafted, `l1` fails. Each pin
is RED against a revert mutant of its wiring (15 mutants; the lens's A to F
among them). Two earlier pins were amended:
- `e576f2_m1_a`: it seeded past a full ceiling through the park, which now
  refuses.
- `e576f_h1`: the LOST counter is `class.lost_counter()`.

## The queued BEEF in R2 (bsv-low #585, door 3, 2026-10-09)

The queue producer capped the raw BEEF at 90,000 bytes (`queue.rs`
`QUEUE_BEEF_SIZE_LIMIT`: its base64 inside the platform's 128 KB message). A
faulted or "not now" submission above it was never queued: the door answered
502 (`BEEF too large for the mutation queue`) on every presentation, and the
admission was never held. The cap is gone. The rule: a queued submission of
any size the consumer admits is carried WHOLE; the platform's message bound is
routed around, never made a limit on a body.

- **The binding.** `BEEF_BLOBS`, an R2 bucket, in all three overlay configs:
  `overlay-beefs` (`wrangler.toml`; create it before a deploy, `wrangler r2
  bucket create overlay-beefs`), `low-overlay-beefs` (prod) and
  `low-overlay-beefs-beta` (`wrangler.low.toml`). A bucket name is not an
  account identifier and is committed. The bucket gets NO expiry (lifecycle)
  rule: a parked letter's bytes live there.
- **The room.** `QUEUE_MESSAGE_ROOM`, 124,000 bytes, measured on the real
  message JSON in its WORST form (`inline_worst_len`: as produced, or as the
  lever re-drives it, the reason `redrive` and the letter's key added). A
  message that fits is INLINE, byte for byte what it was (the frozen digest of
  the 90,000-byte case). At two topics the room is about 92,700 raw bytes. The
  var `MUTATION_QUEUE_INLINE_ROOM` only LOWERS it (1,024 to 124,000; the route
  tier runs at 4,096).
- **The producer.** A message past the room is written to R2 FIRST, under
  `mutations/<sha256 of the BEEF>/<32 hex of sha256(sorted topics, "\n",
  mode)>` (one object per bytes, topics and mode; R2 checks the sha256 it is
  given), and the message carries `r2: {beefR2Key, sha256, bytes, txid}` in
  place of `beef_b64` (`txid` is the subject by D5, so both consumers name the
  letter without reading the object; absent, the letter is `unparsed:` and the
  hash). A write that faults, or a missing binding, is the door's 502, as a
  failed send is (S2: an ack is never an ack over a dropped write). A send that
  faults AFTER the write deletes nothing (a twin's message may name the
  object; the client's re-presentation writes the same key).
- **No body is refused for its size (the fold-2, E585-D3-M1, #568 closed).**
  As built at `bc32851` the door refused a body past the consumer's
  `QUEUE_BEEF_LIMITS.max_bytes` (90,000) BEFORE choosing a carriage, so no
  object was ever written and a "not now" over a large BEEF still answered
  502 (the lens's word: a "long topic list" never reached R2 either, the route
  caps topics at 100). Now `plan_replay` refuses nothing by bytes and
  `QUEUE_BEEF_LIMITS.max_bytes` names the engine's own
  (`ENGINE_BEEF_LIMITS.max_bytes`; the one constant NL-6's rebase meets), a
  name only: NL-6's `parse_beef` reads no limit, so it bounds nothing. A valid 500 KB "not now" body is acked `queued`
  (200, `X-Overlay-Mutation: queued`), its object written under its key,
  read by the consumer, parked by key with its bytes at rest.
- **The consumer.** A keyed message's object is read, checked (length, sha256,
  the key naming that sha256, then the parse an inline body's bytes get,
  `beef_limits::parse_beef`, which refuses invalid bytes only;
  `queue::replay_object`, the one function the R2 read and the stored-rows
  reader call) and replayed exactly as an inline body: the same eviction read,
  `submit_with_report`, landing guard and write-side guard. A read fault and
  a mismatch are the replay's FAULT, class `fault`, never "not now": handed
  back, dead-lettered, parked. A MISSING object is judged (the twin, below).
  The read and the use of the verdict are ONE function,
  `queue::read_for_replay` over a `ReplayBytes` port (the fold-2,
  E585-D3-L3), which the handler calls and the lib's tests run as is: the
  lens's mutant C1 (the verdict discarded, every missing object acked) passed
  all 643 pins on `bc32851` and is RED now.
- **The twin (the d3 fold).** Two messages of the same bytes, topics and mode
  (a client's retry of a large JOIN) name ONE object, and the first ack
  deletes it. A message whose object is MISSING on read
  (`beef_blobs_missing_total`, every one) is judged by its SUBJECT, the
  message's own `r2.txid` (`queue::judge_missing`, two D1 reads at most,
  `missing_verdict`): (1) under an OPEN eviction it is acked as the replay
  with its bytes would be (that check reads the subject alone;
  `queue_replay_skipped_evicted_total`); (2) with an applied row in EVERY
  topic the message names (`TWIN_APPLIED_SQL`, one read of
  `applied_transactions`) it is a DUPE: acked, counted
  `queue_r2_twin_acked_total`, its note or parked row deleted (`RESOLVED ...
  a twin`), no letter. That is the engine's own dedup rule: the replay with
  the bytes would have written nothing in a topic that holds the row.
  (3) Otherwise (a topic without the row, a message naming no subject, a
  ledger or applied read that faulted) it is the replay's FAULT as before,
  counted apart (`queue_r2_missing_fault_total`): handed back, dead-lettered,
  parked. Its note KEEPS the row's class (`NOTE_FAILING_KEEP_CLASS_SQL`; a
  fresh row is a `fault` letter; the fold-2, E585-D3-L4): a missing object
  is a fact about the bucket, and a "not now" letter stays inside its
  bounds. Such a letter is no longer stuck for good: its
  re-drive is judged again and acks once the subject has landed by any road
  (the client's re-presentation, a GASP peer). The deletion stays on the
  ack. A REFERENCE COUNT was not chosen: the object has two writers (the
  door, the lever's re-drive) and its names live in the queue and in D1
  while the object lives in R2, with no transaction across them; a count
  could be wrong both ways (an object deleted under a live message, or kept
  for good), and the applied rows are the fact a count would stand for.
- **The dead letters.** The DLQ consumer parks the KEY without a read (the row
  is a few hundred bytes); the lever re-drives the key and the consumer
  re-reads R2; `/health/invariants.deadLetters.r2` serves `bound`, `letters`
  and `bytes` (the bytes at rest, an index-only sum, migrations 183-185),
  `inlineRoom` and `replayMaxBytes` (the value `QUEUE_BEEF_LIMITS` names,
  10,000,036; it bounds nothing since NL-6 and stays served because the route
  cell reads it).
- **The deletion rule** (the captain's decision; never a bucket expiry): an
  object is deleted (1) on the consumer's ACK, all three of them (landed,
  refused under an open eviction, re-evicted), after the batch's loop, with
  the object of the letter row that ack deleted, except a LANDED ack with a
  named topic that FAILED (the manager erred: durable, no applied row),
  whatever the other topics did (`queue::landed_ack`, the ONE decision the
  handler calls with the replay's report; the fold-2, E585-D3-L1, and the
  fold-3, E585-D3-DELTA-L1 and L2): that one LEAVES the object, so a twin in
  flight reads the bytes and replays as the first did (a landed topic a
  dupe, the failed one failing again, nothing written) and acks; an object
  so left that nothing names is the sweep's (8 days). Only an ack whose
  every topic is applied or deduped deletes, and a twin after it finds the
  applied rows in every topic (a dupe without the bytes). The fold-2 rule
  also needed a topic APPLIED, so an ack whose EVERY topic failed deleted
  and its twin was a fault letter for good (DELTA-L2 (1)); "a failed-only ack
  must delete, or no twin ever would" does not hold: the sweep ends a left
  object, where the delete bought only a fault letter holding a place until
  the operator's discard; (2) when its dead letter is LOST, which is the DLQ's last
  delivery (the platform's `attempts` past 100) of a letter that was DEFERRED
  at the ceiling or at a not-now bound for the ~48 h (a clean read said no
  row holds the key). A LOST letter whose park FAULTED leaves its object to
  the sweep (`dead_letters::lost_deletes_object`, the fold-2, E585-D3-L2: a
  statement can land after its caller was told it failed, #559 limit 5, and
  a twin's row may hold the key); (3) when the copy rule drops it
  (another copy of a parked key carried more); (4) on the operator's discard.
  Counted `beef_blobs_deleted_total`; a delete that faults is logged
  `[beef-blobs]` with its key and counted `beef_blobs_delete_faults_total`.
- **The orphan sweep (the d3 fold; `beef_blob_sweep.rs`).** An object NOTHING
  names (the orphans below) is found by listing the bucket, since nothing
  else knows it. CADENCE: the scheduled tick (`*/15`), one pass a tick,
  before the GASP step, under a 30 s race (a dropped pass keeps and counts
  the deletes it made, saved no cursor unless its save was in flight, and its
  page is listed again). THE LEVER (the fold-4, E585-D3-DELTA2-L1): `POST
  /internal/beef-blob-sweep` (bearer `INTERNAL_TOKEN` in fixed time, as the
  #576 levers; body empty or `{}`, else 400; 401 without the bearer) runs ONE
  pass through `beef_blob_sweep::run_pass`, the function the tick calls (the
  same race, bounds, cursor row and counters), and answers it: `ok`,
  `stopped` (why a pass ended early, then 503: no binding, a state, listing or
  named-keys read fault, each with nothing swept and no cursor moved; or the
  30 s budget, the fold-5's E585-D3-DELTA3-L1: the deletes made before the
  drop are real, each logged SWEPT, and are answered in `deleted` and
  `deletedBytes` and counted, the counters bumped from what the pass did
  after the race; `cursorAfter` is null, and `stopped` says whether the
  cursor's save was in flight at the drop, a statement that may land after
  it, which `queue.r2.round.startAfter` then shows), `listed`, `handled`, `unreadable`, `unreadableKey`, `deleted`,
  `deletedBytes`, `faults`, `cursorBefore`, `cursorAfter`, `lastPassAt`,
  `roundComplete` and the `budget`. For an operator after a bulk discard or an
  R2 audit (one call a page of 200; call until `roundComplete`), and for the
  route tier, which no longer fires the production tick. BOUND: a pass lists at most 200 objects under `mutations/`
  (`SWEEP_MAX_OBJECTS`) from the key the last pass stopped at, and deletes at
  most 50 (`SWEEP_MAX_DELETES`, a `head` and a `delete` each; a pass that
  meets a 51st stops there). The cursor is the last KEY handled, at rest in
  D1 (`beef_blob_sweep`, one row, transient, migrations 186-188): R2 lists in key
  order and a key outlives a listing token. An object is deleted when it is
  BOTH older than the WINDOW by its AGE STAMP, the LATER of R2's `uploaded`
  and our own `customMetadata.touched` that every put writes (the fold-2,
  E585-D3-N2 (b): whether R2 renews `uploaded` on a re-put of a key is not
  documented and no longer decides), AND named by no
  dead letter row (`NAMED_KEYS_SQL`: every `mutation_dead_letters.r2_key`,
  whatever the row's status; read only by a pass that listed an object past
  the window). Just before its delete the object is read again and left if
  it was written again since the listing (a re-presentation: the same key, a
  new `touched` stamp, a new message). A state, listing or named-keys read that faults
  deletes nothing and moves no cursor. THE STAMP'S READ never throws (the
  fold-3, E585-D3-DELTA-M1): the sweep lists and heads through the bucket's
  raw JS object (`list_page`, `head_of`, every field by `Reflect`, a `catch`
  binding) and reads `customMetadata.touched` only behind `is_object()`, so
  an object with NO custom metadata (an operator's CLI put, one written before
  the stamp) has no stamp and is aged by `uploaded` alone. workers-rs 0.8.5's
  `Object::custom_metadata` calls `js_sys::Object::keys` (no `catch`) on what
  may be `undefined`: a `TypeError` through the wasm frame that no `.ok()`
  sees, which ended the scheduled tick before its GASP step on every tick
  until the object was deleted by hand. The platform's answer for an unstamped
  object under `include` (`undefined` or `{}`) is not documented and miniflare
  always answers `{}`; the native pin proves the parse, the fallback and the
  source shape, beta the platform (the beta check below).
  AN OBJECT THE SWEEP CANNOT READ (its key or `uploaded` date does not read: a
  broken platform contract, never a throw) is SKIPPED and COUNTED (the fold-4,
  the delta-2 lens's N3): the pass goes on over the rest of its page, the
  cursor passes it by its key, the next round meets it again,
  `queue_r2_orphan_sweep_unreadable_total` counts it on every pass and the
  health block names the last pass's count and first key; it is never swept
  (the operator deletes it by hand if it is the door's). Before the fold one
  such object failed the whole page closed on every pass, nothing counted: the
  sweep stalled at it for good. A page of 200 entries none of whose keys read
  still leaves the cursor where it was (stated). THE BETA CHECK (after the
  first deploy carrying the sweep): put one object with NO metadata under
  `mutations/zz/` (`wrangler r2 object put`); it sorts after every hex key,
  so it is listed only when a round reaches the bucket's end: call the lever
  until its answer says `roundComplete: true` (or watch
  `/health/invariants.queue.r2.round.startAfter` pass `mutations/f` and
  `atRest.at` move); the answer's `unreadable` 0 and `stopped` null, then
  the next `*/15` tick moves `queue.r2.sweep.lastPassAt` and the tail shows
  `Scheduled: GASP sync` (the tick survived the sweep). Read `lastPassAt`,
  not the fault counter: a listing fault or a throw never bumps a counter
  and leaves `lastPassAt` unmoved. Then delete the object by hand. THE WINDOW is 8 days
  (`ORPHAN_WINDOW_S`, two `QUEUE_RETENTION_S` of 345,600 s, the platform's
  default retention, which neither queue changes): what no queue message can
  outlive. Every message naming an object was sent right after a write of it
  (the door writes, then sends; the lever's re-drive sends without a write
  and its row names the object from the park to the ack), lives at most one
  retention in the main queue and one in the dead letter queue, and ends
  there parked (a row), LOST (its object deleted) or dropped. #576's ~48 h of
  backoff (172,860 s, pinned) is NOT the bound alone: it starts at the
  letter's first DLQ delivery, after its whole life in the main queue, so a
  letter deferred to its last delivery and parked there names an object
  already older than 48 h, and an object swept under a live name is a replay
  with no bytes. Counted `queue_r2_orphans_swept_total` and
  `queue_r2_orphans_swept_bytes_total` (a read or delete that faulted:
  `queue_r2_orphan_sweep_faults_total`; an object that does not read:
  `queue_r2_orphan_sweep_unreadable_total`), each logged `[beef-blobs] SWEPT
  <key> bytes= age_s=`. `/health/invariants.queue.r2` serves the objects AT
  REST from the listing: `atRest` {`objects`, `bytes`, `at`} of the last
  COMPLETE round over the bucket (`null` before the first), `round` (the one
  in progress, with `startAfter`), `sweep` {`windowSecs`,
  `maxObjectsPerPass`, `maxDeletesPerPass`, `prefix`, `lastPassAt`,
  `lastListed`, `lastSwept`, `lastUnreadable`, `lastUnreadableKey`,
  `lever`}, `bound` and `readable`.

Limits, stated. (1) ORPHANS, cured (the sweep above): a send that faulted
after its write and was never re-presented, a delete that faulted, a batch
that died between its acks and its deletes, a message or dead letter the
platform dropped without the LOST line, a letter the operator deleted by SQL.
What is left: an orphan costs its bytes for 8 days, and longer in a big
bucket (a round is 200 objects a tick: 19,200 a day); `atRest` is as old as
the last complete round and is the bucket's count, where `deadLetters.r2`
is D1's count of the letters' objects (the difference is the messages still
in a queue and the orphans not yet swept). The window rests on the queues'
retention being the DEFAULT four days: it is set at `wrangler queues
create`, not in a config, so nothing here reads it (`wrangler queues info`
says; a queue given a longer retention needs `QUEUE_RETENTION_S` raised).
Whether a message's retention restarts when it moves to the DLQ is not
documented; the window takes the worse reading (it does). A re-presentation
landing between the sweep's last read of an object and its delete
(milliseconds) loses that object under a live message: a fault letter unless
its subject landed (the twin rule). The sweep runs only from cron: on a
worker whose scheduled event is not delivered nothing sweeps. Its cost a
tick: one R2 list, one D1 read and one write, plus the dead letters' keys
(at most the 2000 held rows) and two R2 calls an orphan when a page holds an
object past the window. (2) TWINS, cured (the
twin rule above); what is left: a twin whose first message was acked WITHOUT
landing in every topic it names is a fault letter still. The lens found an
instance (E585-D3-L1): a landed ack with a FAILED topic (the manager erred;
durable, no applied row), whose twin then read MISSING and no row in that
topic, a `fault` letter for good over bytes that did land; the fold-2 cures
it by leaving the object on that ack (the deletion rule), and the fold-3 leaves it
whatever the other topics did (DELTA-L2 (1): an ack whose EVERY topic failed
deleted). The fold-2's "third twin" is cured with it: an ack that leaves
nothing behind is one whose every topic holds its row, so every later twin
is a dupe. What remains: (a) an eviction closed between the two eviction
acks, with nothing landed, parks the twin as a fault letter whose re-drive
acks once the readmitted subject lands; (b) a twin of a RE-DRIVE
(DELTA-L2 (2), kept): the lever sends without a put, so a parked letter's
object can be past the 8-day window while its row names it; the stale return
(a claim older than 1 h) can put two copies in flight; the first lands with
a failed topic and LEAVES the object, but its ack's `resolve` deleted the
row, so nothing names it and a sweep pass between the two reads deletes it;
the second copy is then a `fault` letter for good. No bytes are lost (the
first copy's replay ran every topic: the landed ones hold their rows, the
failed one failed as the second's would), it re-drives to the same verdict,
and the operator's discard ends it. Narrow: a stale return, a failed topic
and a sweep pass inside one consumer's backoff. The cure not taken: a
leaving ack of a re-drive re-puts its bytes (an R2 write of the whole body
per such ack, to move `touched`). A twin acked
as a dupe told no lookup service anything and landed no carried predecessor:
the first message's replay did. "Landed" reads `applied_transactions`, so
limit (7) of the faulted-submit section holds here too (never wipe it
alone). (3) The whole object is
read into the isolate (its bytes, then the parse): memory is the body's.
(4) Whether the platform counts a message's 128 KB as this JSON is not
verified; 4,000 bytes are left under the decimal reading. (5) R2 costs a
write per keyed submission and a read per replay of it (up to four, and four
more per re-drive).

Pins: `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare
--lib e585_d3` (ten: the inline message byte-identical and its frozen digest;
the room on the real envelope, to the byte; a 500 KB body by key, read back
byte for byte, refused on a flipped byte, a short read and a foreign key; the
key per bytes, topics and mode; a valid 500 KB BEEF keyed by its subject and
checked under the consumer's policy; a keyed letter parked, re-driven, MISSING
as a fault, discarded; the copy rule; the write before the send and every ack
deleting; LOST and the discard deleting; the three configs' bindings). None
compiles on `6926f2c`, whose own pin asserts `None` past 90,000 bytes; each is
RED against a revert mutant of its rule (five run). The twin's pins, `--lib
e585_d3f` (`beef_door_replay::twin`):
`*_a_twin_whose_object_is_gone_is_a_dupe_and_an_unlanded_one_a_fault` (two
identical valid 500 KB submissions, one object; the first lands and its ack
deletes it; the second reads none, finds the applied rows over the shipped
read and is acked with no letter; a third whose bytes never landed parks as
a `fault` letter; half landed is a fault; RED with the verdict put back to
`45aceff`'s rule: `left: Fault(".. is MISSING ..") right: Acked(Twin)`) and
`*_the_verdict_reads_the_ledger_first_and_a_faulted_read_is_never_landed`
(the other arms and the consumer's source shape; RED over `45aceff`'s
`lib.rs`: "the consumer judges a MISSING object"). Neither compiles on
`45aceff`. The sweep's pins (`beef_blob_sweep::tests`, the pass run over a
bucket in key order and the SHIPPED statements under real SQLite):
`*_an_orphan_past_the_window_is_swept_a_named_and_a_young_one_are_not` (the
object of a faulted send, to the millisecond of the window; one a parked
letter names; a young one; one written again before its delete; a faulted
named-keys read; the count at rest), `*_the_sweep_is_bounded_per_pass_and_resumes_from_the_key_at_rest`
(450 objects, 130 orphans: passes of 50, 50, 30 and 0 deletes, 200 listed at
most, the cursor read from D1, the health block) and
`*_the_window_outlives_every_queue_message_and_the_cron_runs_the_pass` (the
window against #576's backoff, the configs, the tick's wiring, the pass's
order). The module does not exist on `45aceff`; RED over the tick without
the pass ("the scheduled tick runs one bounded pass of the sweep") and
against four revert mutants (no orphan named: `left: Pass { swept: [] ..`;
the window ignored; the dead letters' keys ignored; no read before the
delete). Amended: `e585_d3_the_write_precedes_the_send_and_the_ack_deletes`
(one ack names no object) and
`e576_the_main_consumer_notes_each_retry_and_resolves_each_ack` (four acks). Amended: three `e576*`
pins for the statements' new `r2_key` column and `park_query_r2`. The route
cell `tools/lane-e585/beef_blobs_route_ci.mjs` (`make ci-d1-budget`, the
overlay at `MUTATION_QUEUE_INLINE_ROOM:4096`): three ~8 KB "not now" letters
written to the local R2, parked by key, the object byte for byte; one
re-driven from R2 and LANDED, its object gone after the ack; one whose object
was deleted parked again as a fault; one discarded with its object; and a
500 KB submission, which on `bc32851` passed on the door's REFUSAL and is
now the full leg (acked, parked, re-driven, landed, object gone).

The fold-2's pins (the door 3 lens, `--lib e585_d3f2`; RED quoted on
`bc32851` or against `bc32851`'s rule grafted alone):
`beef_door_replay::twin::e585_d3f2_m1_*` (a valid 500 KB "not now" body
keyed under the SHIPPED policy, acked queued, read, parked `not_now` by key
with its object at rest; a landing one acked and its object deleted; on
`bc32851` the door step is `Err("BEEF too large for the mutation queue's
replay (500000 B > 90000 B, ...)")`), `*_l1_*` (the lens's twins over a
failed topic; RED: `left: Fault(".. is MISSING ..; .. holds no applied row in
[tm_b] ..")`), `*_l2_*` (LOST deletes only on a deferral), `*_l3_*`
(`read_for_replay` run over every verdict; RED under mutant C1: "an unlanded
missing object is acked"), `*_l4_*` (the lens's LOST-then-twin sequence;
RED: `left: [(.., "failing", "fault")]`) and
`beef_blob_sweep::tests::e585_d3f2_l3_the_age_is_the_later_of_uploaded_and_touched`.
Amended: `e585_d3_the_write_precedes_the_send_and_the_ack_deletes`,
`e585_d3f_*` (the model consumer runs the shipped `read_for_replay`). The
route cell's new legs (6 to 8: the twin ack, the sweep under
`/__scheduled`, a put twice moves `touched`) and leg 3's class (`not_now`
kept) were written in the fold-2 and not run there (`make ci-d1-budget`).
The captain's re-run of the fold-2's tier passed every leg of this cell (0
to 8); the first run's leg-6 CLI failure left no stderr and did not recur.

The fold-3's pins (the door 3 delta lens; RED on `4f592f0`, the graft
named): `beef_blob_sweep::tests::e585_d3f3_m1_an_unstamped_object_never_throws_and_ages_by_uploaded`
(the stamp's parse over an absent, empty and malformed value, the age by
`uploaded`, an unstamped orphan swept, and the source: no `custom_metadata()`
or `Object::keys`, the `is_object()` guard before the field, both readers
through `listed_of_js`; RED on `4f592f0`'s sweep with `touched_ms_of`
grafted: "the sweep reads customMetadata through workers-rs's Object::keys";
`js_sys` does not run off wasm32, so the guard itself is not executed
natively), `beef_door_replay::twin::e585_d3f3_l1_the_landed_ack_is_one_decision_the_handler_calls_as_is`
(`queue::landed_ack` over every arm and the handler's call; RED on
`4f592f0`'s `lib.rs`: "the handler calls the shipped decision with the
replay's report"; the delta lens's mutant G RED: `left: LandedAck { leaves:
false, .. }`; its G2 is an equivalent mutant now, the rule reading `applied`
and `deduped` alike) and `*_e585_d3f3_l2_a_twin_of_an_ack_whose_every_topic_failed_reads_the_bytes`
(RED with `4f592f0`'s rule grafted: `left: Landed { left: false }`).
Amended: `e585_d3f2_l1` (the second twin's ack now LEAVES the object: its
topic failed again) and `e585_d3_the_write_precedes_the_send_and_the_ack_deletes`
(the Landed ack deletes what `landed_ack` says). The route cell (the fold-3,
E585-D3-DELTA-M2): every wrangler CLI call is retried (4 tries, a growing
pause) and its last failure THROWS with the child's stderr in full; `r2get`
answers null only on wrangler's own "The specified key does not exist.", so
a deletion is asserted only on a true not-found; leg 7 puts its object with
NO custom metadata on purpose (the M1 path). `ci-d1-budget`'s logs carry the
port base (`/tmp/lane499-d1-<LANE_BASE>-app.log`, `-overlay.log`,
`-seed.log`). The cell runs LAST in `ci-d1-budget`: leg 7's whole
scheduled tick goes on in the background into its GASP step, which syncs
the worker's hard-coded `tm_ship`/`tm_slap`/`tm_uhrp` peers over the network
and DEFERS real graphs; run before the e555 cell it put 18 rows under that
cell's counts (the re-run's 4 e555 FAILs; the delta lens's N8 read it as
harmless).

The fold-4 (the door 3 delta-2 lens, E585-D3-DELTA2-L1 and its NOTEs). Leg 7
fired the WHOLE production tick from CI (`/__scheduled` under
`--test-scheduled`): live SHIP/SLAP/UHRP peers, WhatsOnChain, and, had the
tick reached its rebroadcast backstop, the tier's own transactions posted to
ARC; only the cell's place kept the assertions deterministic. Now leg 7 calls
the sweep's lever (above): 401 and 400, then one pass from a cursor seeded
before a young object, its answer, the cursor row and the health block agreeing,
the object untouched. `ci-d1-budget` runs no `--test-scheduled`, no cell fires
a tick and the order of the cells is free. `ci-route`'s own seven logs carry the
port base too (`/tmp/lane347-route-<LANE_BASE>-strict.log` and the rest; the
lens's N7). Pins (`--lib e585_d3f4`):
`beef_blob_sweep::tests::e585_d3f4_l1_the_lever_runs_the_scheduled_pass_function`
(the route, the bearer before the body before `run_pass`, no listing, plan,
delete or save of its own, the tick calling `run_pass`, the body's parse and
the answer's fields; RED on `1d1f7fe`: "the router sends POST
/internal/beef-blob-sweep to the lever"; RED against a lever that lists for
itself: "the lever runs the tick's pass, never its own listing (list_page()")
and `*_e585_d3f4_n3_an_unreadable_object_is_skipped_counted_and_passed` (a
keyed and a keyless unreadable entry between orphans: the orphans swept, the
two counted, the first key named, the round complete; a truncated page ending
on one passes it; a keyless page stays, counted; RED on `1d1f7fe`: "one
unreadable object fails the whole page"). Amended:
`e585_d3f_the_window_outlives_every_queue_message_and_the_cron_runs_the_pass`
(the tick calls `run_pass`, which races `sweep_pass`). Left, stated: the
lens's N1 (the strict worker's `wasm-opt` SIGKILL, a host memory matter), N5
(wrangler unpinned; a changed not-found text turns a true not-found into a
FAIL, never the reverse), N6 and N8 (no change asked), N4 (the fold-3's RED
wording: the L1 pin reaches its source assertion only with the fold's rule
body grafted too), and the e1d and e555 cells' unretried CLI calls (N7, the
class of M2, not asked).

The fold-5 (the door 3 delta-3 lens, E585-D3-DELTA3-L1 and L2). The pass is
one function over a port (`beef_blob_sweep::SweepPort`: the bucket, the state,
the dead letters' keys, the counters, the clock, the log; the worker's is
`WorkerSweep`, R2 and D1), `sweep_pass`, raced and counted by
`run_pass_with`, which `run_pass` calls; the lib's tests run both as shipped
over a model port (the shape of `read_for_replay`). L1: the pass records each
step in a `PassProgress` as it lands, so a pass the budget drops answers its
deletes and its listing, and the counters are bumped from that answer after
the race, ran or dropped (they were bumped inside the pass after its save, so
a dropped pass's deletes were never counted). L2: the N3 pin ran a model pass;
it now runs the shipped one. Pins (`--lib e585_d3f5` and `e585_d3f4_n3`):
`beef_blob_sweep::tests::e585_d3f5_l1_a_pass_the_budget_drops_answers_and_counts_its_deletes`
(four orphans, the third `head` never answers: `deleted` 2 of 201 bytes, the
counters by two, no save; dropped in its save: four, "IN FLIGHT"; RED with
`14f4b2c`'s drop answer grafted into `run_pass_with`: `left: (Number(0),
Number(0)) right: (Number(2), Number(201))`), and `e585_d3f4_n3` over the
shipped pass (RED against the lens's mutant B, the shipped pass stopping on
any unreadable entry, written as a struct literal: `the orphans on both sides
are swept, left: []`; 661 passed, that one failed). Amended: the window pin's
source checks follow the port (three fail-closed stops in the pass, the
binding's in `run_pass`). Stated: the state row's `lastSwept` and
`lastListed` are not written by a dropped pass (no save); its counters and its
answer carry it.

### Door 3 and the deferred EF jobs (NL-6c, NL-6d; the land onto `0eda4df`)

Main's NL-6c defers the broadcast-gated arm's EF work past one request's
budget to the queue, and NL-6d defers a corroboration's legs the same way: ONE
job (`ef_deferred_jobs`, its `legs_from` the corroboration's cursor). Where its
bytes rest, and under what bound:

- **The job's bytes** never ride the message (`ef_job_message` carries the
  reference; `r2` and `beef_b64` are empty). They rest whole, written BEFORE
  the enqueue (`ef_deferred::defer`): in R2 under `ef-deferred/<reference>`
  when `BEEF_BLOBS` is bound (now in all three configs, door 3's binding), or
  in D1 as `ef_deferred_chunks` rows of at most 1,000,000 bytes, as many as
  the body needs (under D1's 2,000,000-byte row). No path compares the body's
  length with a cap: the bound is the platform's (one isolate holds the body
  whole, the plan's request-body limit), named in `ef_deferred.rs`. The
  consumer reads them back with a length check (`load`) and the reference
  check at the run (`run_job`: the sha256 of the topics, the BEEF and the
  off-chain values recomputed over what was read), deletes them when the
  job settles (`release`), and the cron sweeps a settled job and its bytes
  after 7 days (`redrive`).
- **The job's "not now" replay** (the arm's admission after the corroboration,
  a run included) goes through `queue::enqueue_replay`, which is door 3's
  `plan_replay`: inline under the room, by key under `mutations/` past it,
  never refused for its size. NL-6d's stated 502 ("a BEEF over
  `QUEUE_BEEF_SIZE_LIMIT` cannot be queued", in the request and on every run of
  a deferred job) is closed by door 3 here: that constant no longer exists.
- **The orphan sweep** lists `mutations/` only (`SWEEP_PREFIX`), so it never
  names, reads or deletes a job's `ef-deferred/` object, live or settled.

So the three are one rule in what they refuse (nothing, for size). Not unified,
stated as a follow-up: the job's object is keyed by its reference, not by
door 3's `mutations/<sha256>/<scope>`. Its bytes are checked twice at the run:
the length (`load`), then the reference, the sha256 of the topics, the BEEF and
the off-chain values framed by length, recomputed over what was read
(`run_job`); a mismatch fails the job and releases the bytes, so no object is
replayed as another submission's. The R2 put is given no checksum (door 3's
is), so a corrupted upload is found at the run, not refused at the put. A job's
bytes whose row was never written (the R2 `put` or the D1 chunks landed,
`JOB_UPSERT_SQL` faulted: `ef_deferred.rs` `put_at_rest`) are named by no job
and no sweep reads them (the orphan sweep lists `mutations/` only), so they
stay until an operator deletes them (the request answered 503, retryable; the
client's re-presentation writes the same key, or deletes and rewrites the same
chunks).

The scheduled tick, in order (`lib.rs` `scheduled`): the ad sync; the R2
orphan sweep's one pass under its own 30 s race (`SWEEP_BUDGET_MS`, door 3's
fold); the stale deferred graphs (#555); the deferred EF jobs' hand-back and
keep sweep (`ef_deferred::redrive`, NL-6c/6d: two D1 reads; up to 20 quiet jobs sent to
the queue again, a send and a touch each; up to 50 jobs settled over 7 days
ago deleted with their bytes, two deletes each; no race of its own); then the GASP step
under its guarded 240 s belt (`GASP_SYNC_BUDGET_MS`), and the passes after it.
The 240 s belt bounds the GASP step alone: the sweep and the hand-back run
before it, outside it. The jobs' RUNS are the queue consumer's
(`ef_deferred::run_job`), never the tick's. `POST /internal/beef-blob-sweep`
runs one pass of the same sweep function on demand.

## The dry-run option (bsv-low #530 E1, zanaadu-v2 #314)

`TopicManager::identify_admissible_outputs` takes the reference's fifth
argument under the reference's own name, `context: &TopicAdmittanceContext`
(`types.rs`; the reference's `context?: TopicAdmittanceContext`, `dryRun`).
The two GASP calls pass `TopicAdmittanceContext::DRY_RUN`: the needed-input
walk over every proven node of a peer (`find_needed_inputs`) and the anchor
replay of #551
(`validate_graph_anchor`), and so does the predecessor question's dry run
of a candidate body (`unlanded_predecessor`, lane E1D: the opener cure,
under the submit's own mode). `Engine::submit` passes `dry_run: false` (the queue
replay, `/submit`, `/arc-ingest`, the peer crawler and the GASP finalize all go
through it), and `Engine::submit_validate_only`, the only other caller, passes
`true`: a validate-only call admits nothing. The door's dry run asks only of
an output the manager NAMES (the E1D lens fold, H1). A carried predecessor the
door lands first is judged as a DRY RUN until its topic is known not to wait,
then admitted with `dry_run: false` over the same coins, once (the E1D delta
fold, L4); the successor's re-judgement after a landing is real, as its first
judgement was (L5 below). On a dry run a manager must
leave NO durable trace: no storage write, no head advance, no counter an
operator reads as an admission. `mode` is not a substitute: the queue replays
real submissions under `historical-tx`, the mode of both dry runs. The
workspace's 16 managers hold no state and ignore the flag; a manager that
writes on admission must read it. And a manager that writes on admission must
RE-ADMIT the same transaction idempotently (lens L5): an admitting successor's
REAL admission call runs, and may write (pf_name advances its head and writes
its admit row), before the engine answers "not now" and the queue replays it;
pf_name's replay branch reads `find_admit`. The method stays required, so a manager
cannot miss the argument on a re-pin. Pins: `cargo test -p bsv-overlay-engine
--features memory-storage --test gasp_topic_manager dryrun`.

## The managers' abort (bsv-low #558)

`TopicManager::abort_admissible_outputs(beef, outputs_to_abort)`, the
reference's `abortAdmissibleOutputs` (`TopicManager.ts`, ts-stack
`f999e0c1a`), DEFAULT a no-op `Ok`, beside `aborts_admissions()` (DEFAULT
`false`), the Rust form of the reference's OPTIONAL member: a manager that
implements the abort answers `true`. Every manager of this workspace keeps
both defaults (pinned over the worker's factory, `lib.rs`
`WORKSPACE_TOPIC_MANAGERS` / `topic_manager_named`, sixteen). One spelling of
the reference's `abortProvisionalAdmissions` (`Engine.ts:685-700`):
`topic_manager::abort_provisional_admissions` (each `(topic, outputs)` with
outputs and a manager told once, an `Err` logged and the rest still told),
`Engine::abort_provisional_admissions` over the engine's managers.

Where it is called. The reference calls it when a `current-tx` broadcast
THROWS (`Engine.ts:1219-1223`), only under `throwOnBroadcastFailure = true`
(default `false`). This engine's ARC broadcast is never fatal (the default's
branch), so `Engine::submit` never calls it: parity with the reference's
default. This door (D1) broadcasts before it admits; a refusal learned AFTER
the write always ends in `admit_fast::evict_txid_everywhere_repointed`, which
reads the txid's live `outputs` rows AFTER the OPEN marker and before the
passes, inside the ordered core. Its BEEF is read from `transactions`, then
`pot_beefs`, then `transactions_evicted`, stopping at the first nonempty
body. It tells each aborting manager its outputs AFTER the spend release,
released-list merge and ledger close, only when the pass did not yield to a
readmission (`admission_abort.rs`). An empty or unreadable BEEF tells nobody:
logged by txid and counted `admission_abort_empty_beef_total` after the close.
With no aborting manager (today) it reads nothing; an opted-in manager costs
two to four key reads. The retire reserve and budget tiers still need the
captain's land run. A refusal before the write asked no manager a real
admission. Stated:
a readmission restores rows without telling the manager; a manager may be told
twice for one admission after an eviction that faulted half way and ran again
(idempotent). Pins: engine `--lib i558` (`i558_a`, `i558_b`, and
`topic_manager::tests::i558_the_abort_defaults_to_a_no_op`); worker `--lib
i558` (`i558_every_workspace_manager_keeps_the_default`,
`admit_fast::tests::i558_the_eviction_tells_an_aborting_manager_once` over the
shipped schema, with a source pin on the wrapper's order, RED with the tell
ungated). A consumer's manager compiles unchanged (two default methods).

## Progress under the per-peer sync budget (bsv-low #552)

`Engine::start_gasp_sync` races each peer's sync against
`set_peer_sync_budget` and drops the future at the deadline. With a budget
set, every graph is submitted AS IT FINALIZES, inside the raced future
(`gasp::FinalizedGraphHook`, the engine's `SubmitAsFinalized`; the reference
submits inside `finalizeGraph` too), after lane 551's anchor check of that
graph, so the deadline cannot take back what was finalized before it. The
deadline is COOPERATIVE around a write (the lens fold of 2026-10-06,
`gasp::SubmitGate`, `race_or_deadline_guarded`): `Engine::submit` is several
storage writes, each an await on D1, and a submit dropped between the delete
of the old head and the insert of the new one loses a head chain for good. So
each transaction's finalize submit is one write section; a deadline that falls
due inside it waits for that transaction and the sync is dropped at the
boundary, leaving an ancestors-first prefix of WHOLE transactions with the
cursor below that graph's UTXO. A request to the peer, the walk and the anchor
check are still dropped at once. The worker's outer 240 s race of
`start_gasp_sync` uses the same guarded race over
`Engine::finalize_submit_gate()`; any other caller that races
`start_gasp_sync` must too. At the
deadline the cursor is persisted at `GASPSync::completed_cursor`: strictly
below the lowest score of any UTXO not yet completed (the one in flight
included), the gap guard's rule. The graph in flight is lost WHOLE (its nodes
die with the storage adapter, nothing of it is admitted) and the next tick
walks it again, down to what the storage now holds (`find_known_utxos` skips
admitted roots, the known-input strip stops the walk at the admitted
frontier): the re-fetch cost is the nodes of that one graph fetched before
the deadline. A dropped tick that finalized a graph or moved the cursor is a
SUCCESSFUL attempt for the quarantine count. With NO budget nothing can drop
the sync and the graphs are submitted after it, as before.

The budget bounds a TICK. Without a per-graph budget nothing bounds a GRAPH
(parity: the reference has no node cap): a chain reaches a node over several
ticks only as far as the peer lists it as several UTXOs, and ONE graph whose
own walk outlasts the budget (a head chain whose tip is its only UTXO) is
dropped whole on every tick, never admitted. Since bsv-low #555 a per-graph
budget DEFERS such a graph and resumes it (next section).

`TopicSyncResult` reports `finalized_graphs`, `deadline_dropped_graphs` (at
most one per peer per sync; the same count tick after tick with no
`finalized_graphs` and no `cursor_moves` is the case above) and
`cursor_moves` (peer, from, to); the worker's `Scheduled: GASP sync` line
carries the totals and the per-topic line the cursors (`discarded_graphs` is
on the first totals line). Pins: `cargo test -p bsv-overlay-engine --features
memory-storage --test gasp_topic_manager i552` (and `fold_high1`).

## Deferred graphs (bsv-low #555, measured as #582)

The measured case (beta, 2026-10-09): a 0-conf picture head set by a fleet
wallet; LOW's node walked its unproven ancestry (every input an SPV necessity)
at about 1.2 s a peer request to the end of each pass, admitted nothing, and
the next pass restarted the same graph from its root; two thirds of the
minute cron's ticks were skipped behind it.

`Engine::set_graph_budget(sleep, calls, ms)` turns DEFERRAL on (unset: the
walk is the one before #555). A graph whose walk makes `calls` requests (to
the peer, or chain fetches) or runs `ms` in one pass is DEFERRED: its partial
walk is saved as ONE record (`gasp::DeferredGraph`: the nodes fetched with
their proofs, the inputs still pending as a stack, the calls spent, the
passes, the reason), keyed (peer, topic, root outpoint) and REPLACED on every
deferral. The UTXO is held below the cursor like a failed one (the gap guard)
and is not walked again in that sync (#554); the pass goes on to the next
UTXO and topic. The next sync that is served the UTXO RESUMES: the record's
nodes are appended again (no request), an UNPROVEN root is asked again (one
call: once its block lands the peer serves it proven, the record is dropped
`root_proven` and the walk restarts from it, shorter), and only the pending
inputs are asked. A walk the per-peer deadline (D16) cuts is deferred the
same way (`peer_deadline`). With a budget, a walk error of a RESUMED graph
defers it again with its progress (`fault`), except the peer's definite "not
held" for an input an UNPROVEN node needs (an SPV necessity): that fails the
UTXO as in the reference (`not_held`). A walk error of a FRESH walk keeps NO
record and fails the UTXO, as before #555 (the delta fold's D-M1: a fault is
not a budget cut; a stranger whose peer served each fabricated root and
answered its input with a quick 503 kept a one-node record per UTXO, 16 per
(peer, topic) in one tick with no time spent). A record is made only by a
budget cut (`calls`, `time`, `peer_deadline`), or by a fresh walk that faults
after it PAID half of one (the delta-2 fold's D2-L1: half its per-graph calls,
or half its time, `GraphBudget::half_deadline`; with no record an honest flaky
peer's deep graph got one only on a clean first pass, (1-p)^100). It costs a
SLOW peer the per-graph budget's time (the worker's 15 s), and a FAST one only
its calls (100 tiny responses, milliseconds: the delta-2 lens's DELTA2-2). A
RESUMED walk that faults having appended no node keeps its record (an honest
transient 5xx) and counts an idle fault; at `DEFERRED_GRAPH_MAX_IDLE_FAULTS`
(3) in a row the record is dropped (`idle_faults`) and the UTXO fails; a pass
that appends resets the count (the delta-2 fold's D2-M2: a quick 503 a tick
was a free re-deferral that held a stranger's row for its 60 passes).

Every per-graph deadline is LATCHED (`gasp::latched`, found by the delta-2
fold): the worker's sleeps are `async fn` futures, which panic when polled
after they completed, and the resumes' shared deadline (M1 below) is kept
after it fell due and polled again by the next record served. On `ef423da`
two records of one (peer, topic) and a spent resume budget panicked the sync
("`async fn` resumed after completion", a trap in wasm), on every tick while
both were held; the test factories were all re-pollable (`e555d2_x`).

The storage keeps the records through FOUR REQUIRED `Storage` methods
(`put_deferred_graph`, answering `Saved` or `AtCeiling`;
`find_deferred_graphs`, the KEYS of a (peer, topic) lowest score first;
`get_deferred_graph`, one record; `delete_deferred_graph`), mirrored by four
required `GASPStorage` methods (the lens fold's M2, the house style of
`identify_admissible_outputs`). With defaults, a storage or wrapper that did
not forward them compiled, and a budget over it saved nothing: every graph
past the call budget was walked from its root to the cap on every tick and
never admitted (`store_fault`), a hard cap on graph size. Now a re-pin fails
to compile until every impl and wrapper decides (measured: `E0046`, the four
named). A backend that keeps no records answers none and refuses every save,
and must not be given a budget. The three peer-health methods
(`record_peer_sync_outcome`, `get_peer_sync_health`, `record_peer_sync_yield`)
are REQUIRED too (the delta-2 fold's D2-L2: with defaults the test wrapper
`ScriptedStore` did not forward the yield, and a consumer's wrapper that did
not would turn the bound off unseen); a backend that keeps no peer health
answers the old defaults. Their `host` is the peer's NORMALIZED ORIGIN, not
its URL (D2-M2, below). A sync reads the KEYS up front and a record
only when its UTXO is served (L3: the whole records of a (peer, topic), up
to 16 MiB of JSON, were one D1 result inside the 128 MB isolate); a key read
that faults resumes nothing and takes the (peer, topic) as FULL for the sync
(L6: the count read 0 and the 16 could be passed by 16 more).

Progress, for #302's quarantine (the lens fold's H1): a walk PROGRESSED when
its pass appended at least one node or completed the graph; one cut by the
per-graph budget or the per-peer deadline having appended NOTHING is
STALLED, and keeps no record (a record holds at least one node; nothing is
lost by not saving it). A sync is a FAILED attempt when it stalled and
nothing else got done (no walk progressed, no graph finalized, the cursor did
not move), at the per-peer deadline and on a sync that ran to its end
(`errors` names it). Before the fold a deferral of 0 nodes counted as a live
peer: a peer that lists UTXOs and never answers a node request (or answers
each with a quick 5xx) was never quarantined, and about eight such hosts on a
SHIP-mode topic (peers from the permissionless `ls_ship`) took the worker's
240 s pass on every tick. A sync whose UTXOs failed in any other way is
unchanged (a sync that ran to its end is a success, as before #555).

What that catches, and what it does not (the delta fold's D-M2). "Progressed"
is the PEER's word: a node appended is whatever the peer chose to serve. The
H1 rule quarantines a BROKEN peer (hung, or failing with nothing served), not
a hostile one: a peer that lists one fresh fabricated UTXO a tick, serves its
root and hangs on its input "progressed" on every tick and kept its slice (30
s of the worker's pass per (host, topic)) for ever. Bounded since: with a
per-graph budget, a sync the peer served work it did not hold
(`GASPSync::graphs_attempted`) that finalized no graph and moved no cursor is
YIELDLESS; the yieldless STREAK is kept per (origin, topic)
(`Storage::record_peer_sync_yield`, answering `PeerYieldStreak`: the count and
the seconds since its first yieldless sync, on the storage's clock; the
worker's `gasp_peer_health.yieldless_syncs`, `first_yieldless_at` and
`last_yieldless_at`, migrations 171-173). A sync that finalized or moved the
cursor ends it; a quiet one (nothing served) leaves it. A yieldless sync is a
FAILED attempt (`errors` names it) only past BOTH bounds
(`gasp::yieldless_sync_failed`, the delta-2 fold's D2-M1): more than
`PEER_YIELDLESS_SYNCS_ALLOWED` (12) in the streak AND at least
`PEER_YIELDLESS_SECS_ALLOWED` (3 h) since it began. Counted in syncs alone, 12
was 3 h at `*/15` and 12 MINUTES at LOW's beta node (one tick a minute): an
honest peer whose only new work was #582's unproven head, waiting past 20 min
for its block (about one block in seven), was quarantined at sync 20 and
skipped the tick its block landed, then waited for the 6 h probe (DELTA2-1).
The DECAY (`PEER_YIELDLESS_DECAY_SECS`, 6 h): a yieldless sync more than 6 h
after the streak's LAST one starts a new streak, so an honest peer's rare
graphs that never land, hours apart, are not counted across days; quiet syncs
between leave the streak. The decay applies only to a peer that is NOT
failed (`consecutive_failures` 0; the delta-3 fold's D3-M1): it equals the 6 h
reprobe, so on `9280aed` the probe of a peer quarantined by this bound always
came more than 6 h after its last yieldless sync (a sync lasts seconds), started
a fresh streak, "progressed", reset the failures and lifted the quarantine at
every probe (100 of 200 ticks at `*/15`, 940 of 3000 at one a minute, over the
shipped SQL). The yield runs before the outcome, so the probe now continues
the streak and fails (27 of 200). Its cost: a peer that failed for any reason
(a hang, #302) keeps its old streak past 6 h quiet until it yields once (at the threshold an empty listing is no longer a success; D5-L1 below).
Stated too: a peer yieldless for 3 h, quiet
for 6 h, and so on, keeps its slice in the yieldless hours (a third of its
ticks at most), as #302's residual. At `*/15` a hostile peer is still counted
failed from its 13th yieldless sync (3 h) and quarantined at its 20th (5 h);
at one tick a minute from 3 h on and quarantined 8 ticks later; then one
probe per 6 h, and each probe that serves work fails.

A QUARANTINED peer is re-admitted only by a YIELD (the delta-4 fold's D4-L2;
with a per-graph budget). A peer that was at the quarantine threshold (8
failures) when its sync began, and whose sync did not fail and yielded
nothing (an EMPTY listing at its probe; a walk that only progressed), is NOT
RECORDED: `record_peer_sync_outcome` is not called, so its failures stay and
its last-attempt stamp stays. The peer is then attended on every tick (the
reprobe window stays open) until it yields, which lifts the quarantine and
clears the streak, or fails, which re-arms the quarantine for 6 h at once.
Before the fold (on `9280aed` too) the empty probe was a success: failures 0,
the next yieldless sync a fresh streak, and a hostile peer that answered the
first listing after its six silent hours with an empty list kept 100 of 200
ticks at `*/15` (the delta-4 lens's DELTA4-2, engine and shipped SQL). Now
its slices with work are one per reprobe window (27 of 200; 7 empty probes,
each a listing request and nothing else). The honest side, stated: a
quarantined peer that has gone QUIET is not stranded and needs no exit of its
own (no "N empty probes lift it"): it is asked its listing on every tick and
its first new UTXO that lands lifts it; until then one failed sync (a
timeout) costs it 6 h, where a peer below the threshold has eight. The delta-5 lens's D5-L1, stated and accepted (LOW): the streak does not decay while the failures are above 0, so a peer quarantined by the yieldless bound and then quiet keeps a streak past both bounds, and its next graph that needs more than one pass is a FAILED sync at every pass, each re-arming the 6 h quarantine (measured: a 9-link graph at 3 calls a pass converged in 12 h where the base took 180 s; the same peer quarantined by eight timeouts instead converged in 180 s). Nothing is lost (6 h is inside the 30 h sweep; the graph lands at the probe), the common honest quarantine (timeouts) is unaffected, and a decay here would reopen D4-L2 (a hostile peer can go quiet for 6 h too), so no code changes. Below the
threshold nothing changed: an empty listing is a success and resets the
failures (#302's accepted residual, one success in every 8 ticks, stands,
and an empty listing is such a success). A health read that faults reads 0
failures, the old rule. The delta-4 lens's D4-N1, stated: a row with an old
streak past 12 that no yield ever cleared (its graph vanished), then one
failed sync, then fresh yieldless work: the first such sync continues the old
streak (the decay is not for a failed peer) and fails at once, and each after
it until a yield; eight in a row quarantine the peer until a probe that
yields. Reasoned, not run; the path is narrow.

Why 12 and 3 h: an honest deep graph converges inside both.
The measured one (#582) is an unproven head; the pass after its block lands
restarts from the proven root (`root_proven`), one or two passes after a block
that comes in ~10 min on average (past 3 h about once in 10^8 by the
exponential model); walked to its end instead, 12 passes are about 150 nodes
on the worker (15 s a pass at 1.2 s a request) and 600 on LOW's node (60 s).
Its cost: an honest peer whose only new work is a graph that never lands (a
UTXO that fails every pass, a deferral past 12 passes and 3 h with no other
graph finalized) is quarantined too, its next new UTXO waiting up to 6 h for
the probe; the reprobe re-admits it the first time it yields. Without a
per-graph budget nothing changes (#302's rule alone).

The KEY of a peer's quarantine, its streak and its share of the worker's
ceiling is its NORMALIZED ORIGIN (`gasp::peer_origin`: the host, lowercased,
no scheme, user, port, path, query, fragment or trailing dot; the delta-2
fold's D2-M2). Keyed on the URL string, one server was a new peer for every
spelling it advertised: `is_advertisable_uri` passes `/?1`, `/?2`, `:8443`,
`/#x`, a trailing slash (DELTA2-3), each with its own share, its own
quarantine and a fresh streak. The engine passes the origin to the three
peer-health methods; the cursor and the records stay keyed by the URL the
peer is synced at. A SHIP topic's peers are ONE PER ORIGIN
(`gasp::ship_peers_by_origin`, D2-M2's rule, restored by the delta-4 fold's
D4-M1): each advert is canonicalized to `scheme://host[:port]` (the scheme's
default port dropped, so `:443` is no explicit port), and of the spellings of
one origin the first by `https`, then no explicit port, then the string is
the peer; every spelling of our own origin is ourselves. The delta-3 fold
(`c8a39fd`) kept one peer per `scheme://host[:port]` while the failures stayed
on ONE row per origin, and the delta-4 lens ran it (DELTA4-1): eight stranger
adverts of DEAD PORTS of an honest host's name quarantined the honest peer
(synced on 4 of 96 ticks), and seven were never quarantined (the honest
success reset the shared count every tick: 672 dead-port slices in 96 ticks,
seven 30 s slices of the 240 s belt). One peer per origin has neither: the
honest default-port advert is the peer, on 96 of 96 ticks, and no dead port
is asked. It also closes the lens's D4-L1 (`:0443`, `:00443`, `:08443`,
`https+bsvauth://h:443`, `wss://h` were each a peer: they are spellings of
one origin). The LIMIT, stated (the delta-3 lens's D3-L1, open by decision):
two overlays of one host on two ports, on one SHIP topic, are one peer (the
default port's), so a stranger's default-port spelling
(`https://honest.example/?x`) DISPLACES an honest overlay that serves only on
`:8443` for that topic. No instance today: this repository's discovered
topics have no such host, and a Cloudflare-hosted overlay serves 443. An
overlay that must be synced on another port is given as a CONFIGURED peer
(`SyncTarget::Peers` is not deduplicated). A
SUBDOMAIN is another origin (no public suffix list; the reserve below is what
protects configured peers from it); two overlays of one host on two ports on
one SHIP topic are one peer; two configured URLs of one host on one topic
are two peers sharing one quarantine (so D4-M1's shape exists for an
operator's own configuration: eight dead configured ports of a host silence
its live one); a peer's health rows written before the fold (keyed by
URL) are left behind and its streak starts afresh; a SHIP peer whose advert
ends in `/` gets a new cursor key (`https://h`) and is listed again from 0
once (its held UTXOs are skipped).

What a hostile peer can still do, stated: re-list a UTXO the node already
HOLDS at a score just above the cursor, then a fabricated one that hangs; the
cursor moves every tick (a yield), and the peer keeps its slice. That route
was open before #555 (the delta lens's DELTA-5, with no graph budget), and so
was #302's own accepted residual (one real success in every 8 ticks). Its cost
to the node is one per-peer budget slice per (host, topic) per tick inside
the 240 s outer belt; to the peer, keeping a listing and a hung socket per
tick. #302's quarantine is a defence against broken peers, not an adversarial
one.

One budget for the resumes (the lens fold's M1): the cursor is held below
the deferred UTXOs, so the peer serves them FIRST; with a budget each, two of
them filled the per-peer budget (the worker's 15 s of 30 s, LOW's 60 s of
120 s) and no UTXO above them was reached. Every resume of one sync now
shares ONE per-graph budget, a deadline made at the first resume and one
call count, so the resumes take at most one graph's budget and the first new
UTXO keeps its own. A record served once that budget is spent is HELD BACK
untouched (no read, no pass counted; `held_back_graphs`) for the next sync,
oldest score first. The limit, stated: a record held back behind another's
resumes waits for that one's convergence or its 60 passes. (Since bsv-low
#590 the TIME of that budget is no longer shared: below. The counted limbs
still are.)

A walk with nothing pending is completed as any graph is: D15's anchor check
over the WHOLE graph (the resumed nodes included), the finalize submits
ancestors first, stopping at the first that does not land (e1d: no
successor is recorded over a predecessor that did not land). A completed
graph whose finalize did not land keeps its record with nothing pending
(`not_landed`): the next pass completes it again without one request. An
anchor fault (`AnchorUnavailable`) keeps it too; an anchor REFUSAL deletes it
(`refused`, the cursor moves, as before).

The walk is an explicit stack (it was a recursion; the order is the same: an
input's whole branch before its next sibling; D13's pin B, byte-identical request
lists, holds) whose state lives in the `GASPSync`, outside the raced future:
each step is committed only when it finished, so a deadline that drops the
future leaves the walk as it was before that step.

Bounds, all stated in `gasp.rs`: `DEFAULT_GRAPH_BUDGET_CALLS` 100 and
`DEFAULT_GRAPH_BUDGET_MS` 60 s (half of LOW's 120 s topic slice; at the
measured 1.2 s a request the time binds first, about 50 nodes a pass); the
Cloudflare worker sets 100 calls and 15 s (half its 30 s per-peer budget,
`gasp_deferred.rs`; with M1 the resumes of a pass together take at most that
half). A record deferred `DEFERRED_GRAPH_MAX_PASSES` (60) times is dropped at
its next resume (`max_passes`) and the UTXO fails (the gap guard asks again:
the next pass walks it from its root, a new record); a record whose UTXO a
sync that ran to its end was not served (spent at the peer) is dropped
(`not_served`), one the node now holds (`held`); a write that faults
(`store_fault`); a resumed record whose pass ends still holding no node (one
saved before the fold) `no_progress`. A walk past its per-graph budget that
cannot be KEPT, past `DEFERRED_GRAPHS_PER_PEER_TOPIC` (16 per (peer, topic), `too_many`), at
the storage's ceiling (`AtCeiling`, counted `too_many`) or whose save FAULTED
(`store_fault`; the delta fold's D-L2: it failed the UTXO; a RESUMED walk's
held record is KEPT through a faulted save, the delta-2 lens's D2-N2: it was
deleted, and one transient D1 fault threw away every earlier pass), is
counted and then GOES ON under the per-peer budget alone, as every walk did before #555
(the lens fold's L4: such a graph completed in one pass before #555 and was
failed on every pass after it). A walk that went on is counted ONCE and the
storage is asked once (the delta fold's D-L1): a per-peer deadline that then
cuts it, or a completion that does not land, saves nothing and counts
nothing more (it counted `too_many` twice and upserted twice).

The worker: `gasp_deferred_graphs` (migration 170, transient, one row per
graph). Its upsert carries a GLOBAL ceiling in the statement itself (the
lens fold's M3, as #576's `PARK_SQL`): a new key only under 256 rows
(`DEFERRED_GRAPHS_MAX_ROWS`), and any save only while the rows' `bytes` stay
within 64 MiB (`DEFERRED_GRAPHS_MAX_TOTAL_BYTES`); a refused save returns no
row (`AtCeiling`). Half of it is RESERVED for CONFIGURED peers (the delta-2
fold's D2-M2; each record carries `configured`, set by the storage adapter
from `SyncTarget::Peers`; migrations 174-175 add `origin` and `configured`):
the records of DISCOVERED peers together hold at most 128 rows
(`DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS`) and 32 MiB
(`DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES`), and a configured peer's save is
checked against the global bounds only. The discovered half is SHARED by
origin (the delta fold's D-M1, keyed by origin since D2-M2): one origin holds
at most 32 rows (`DEFERRED_GRAPHS_MAX_ROWS_PER_HOST`, an eighth) and 8 MiB
(`DEFERRED_GRAPHS_MAX_BYTES_PER_HOST`) over all its topics, in the same
statement. It was one pool: four (host, topic) pairs of a stranger's hosts
held the 64 MiB, then (the delta-2 lens's DELTA2-4) eight spellings of one
server held all 256 rows within two ticks at no cost (a fast peer pays
`calls`, not time) and a configured peer's deferral (`overlay-us-1.bsvb.tech`,
`tm_uhrp`) was refused: #582's graph walked from its root again. A row saved
before the fold holds origin '' and counts as discovered until its next save.
The engine takes the topics
with CONFIGURED peers (`SyncTarget::Peers`) before those whose peers
`ls_ship` discovers (`engine::sync_order`, each class by name; the map's order
was arbitrary), so a place the ceiling frees goes to a configured peer first
and a pass the outer 240 s cuts has reached them (the delta-2 lens's D2-N1,
stated: the worker's nine configured peer syncs, `tm_ship` x4, `tm_slap` x4,
`tm_uhrp` x1, at 30 s each are 270 s, past the 240 s belt, so while the bsvb
peers are slow, until their quarantine, the SHIP topics after them are cut on
every tick; today those topics have no other host). The limit, stated: FOUR
origins (four subdomains of one server, `a.evil.example` ..; SHIP-mode hosts
are anyone's to advertise) fill the DISCOVERED half for the price of their
calls, and an honest discovered peer is then refused (`too_many`) and walks
from its root as before #555; configured peers keep their half. Its signal is
a count at 128 discovered rows or `totalBytes` near 32 MiB on
`/health/invariants.gasp.deferredGraphs` while `gasp_graph_dropped_too_many_total`
rises. The cron sweeps, before its GASP sync, every row not
written for 30 h (`DEFERRED_GRAPH_STALE_SECS`: twice 60 passes at `*/15`, so
a record held back behind another's whole life survives), logs each and
counts `gasp_graph_dropped_stale_total` (and `gasp_graph_dropped_total`):
before the fold a peer that never finished a sync again (dark, quarantined,
its advert revoked) kept its rows for good, and a stranger advertising hosts
on a SHIP-mode topic could fill about 1.4 GB a day into the D1 the overlay
shares with the app layer. The table stays transient, so its sweep needs no
`delete_scope` (the ownership checker refuses one on a table that is not
never-wipe). `/health/invariants.gasp.deferredGraphs` serves `count`,
`totalBytes`, `oldest`, the oldest 20 as {topic, peer, outpoint, nodes,
pending, calls, passes, reason, bytes, ageSecs}, and the `budget` (calls,
ms, maxPasses, chunkBytes, perPeerTopic, maxRows, maxTotalBytes,
maxRowsPerHost, maxBytesPerHost, discoveredMaxRows, discoveredMaxBytes,
staleSecs);
the counters `gasp_graph_deferred_total`, `gasp_graph_resumed_total`,
`gasp_graph_converged_total`, `gasp_graph_dropped_total` and
`gasp_graph_dropped_<reason>_total` (the reasons above and `stale`; bumped
by the cron's pass; `/admin/startGASPSync` returns the same figures in its
body and bumps none), and the `Scheduled: GASP sync:` line's
`deferred_graphs`, `resumed_graphs`, `converged_graphs`, `dropped_graphs`,
`stalled_graphs`, `held_back_graphs` (the per-topic line names each drop's
outpoint and reason). `TopicSyncResult` carries the same six.

Limits, stated. The reference (ts-stack `f999e0c1a`) has no budget and no
deferral: all of it is an addition. A resumed graph's earlier nodes are the
peer's bytes of an earlier pass (a transaction is immutable; a proof that
arrived since is not re-asked, except the root's). A PROVEN node saved before
a reorg is re-appended as is (the lens fold's L1): the anchor check at
completion re-checks against the chain tracker, so a stale proof is never
admitted, but it is a REFUSAL and the cursor moves past that graph, where a
fresh walk would have fetched the new proof; records live at most 60 passes
(15 h at `*/15`). Not built: a refusal of a resumed graph that walks it once
more from its root cannot be bounded without keeping a mark past the record's
deletion. "Converged" is counted when the graph's finalize landed, under a
per-peer budget (the hook); without one, when it completed; a graph restarted
`root_proven` is counted dropped (`root_proven`) and its later landing is a
fresh graph's, not counted `converged` (the lens's N1). Whether D1's binding
takes a 1 MiB bound value (a record's row, or one part of a chunked record,
bsv-low #585) is unverified (the lens's L5): the statements run
under rusqlite and the route cell seeds its row with `wrangler d1 execute`,
and local D1 does not enforce the platform's limits (2 MB a row; the 100 KB
statement limit does not count bound values). `byte_size` serializes a record
once more to measure it. Pins: `cargo test -p bsv-overlay-engine --features
memory-storage --test gasp_topic_manager e555` (a to h and b2, each RED on
`cf933e8` with the API grafted inert, pin B hangs there; the lens fold's
`e555f_h1` x2, `e555f_m1`, `e555f_l3`, `e555f_l4`, `e555f_l6`, each RED on
`03e1e17` with the fold's test knobs grafted inert; `e555_b`, `b2` and `c`
amended by H1, L4 and M1), the worker's `gasp_deferred::tests` (the shipped
statements under real SQLite, the health view, the counters; the fold's
`e555f_m3` ceiling and sweep, RED on `03e1e17`) and the route cell
`tools/lane-e555/deferred_graphs_route_ci.mjs` (`make ci-d1-budget`; the
fold's `totalBytes`, ceiling and counters, RED on `03e1e17`). The delta
fold's pins, each RED on `0974be5` with its test knobs grafted inert:
`e555d_m1` (a 5xx stranger and an honest deep graph beside it under a
one-record ceiling: no stranger record, the honest graph deferred and
converged), `e555d_m2` x2 (the hostile peer counted failed from its 13th
yieldless sync and skipped after its 20th; an honest unproven-root graph and
a 33-link walk converge inside 12 with no failure), `e555d_l1` (one
`too_many`, one save asked), `e555d_l2` (a faulted save goes on and the
graph lands in the pass); the engine's `delta555_m1` (the topic order); the
worker's `e555d_m1_the_upsert_shares_the_ceiling_by_host` and
`e555d_m2_peer_yield_upsert_real_sqlite`; the route cell's per-host budget
and `yieldless_syncs` column (`e555d_m2_a` amended by the delta-2 fold: its
clock advances 15 min a tick). The delta-2 fold's pins:
- `e555d2_m1_an_honest_unproven_head_at_one_tick_a_minute_*` (DELTA2-1 at one
  tick a minute: 30 yieldless syncs, no failure; the block lands and the
  graph converges; a second streak past 3 h fails), RED on `ef423da` (failed
  at tick 13);
- `e555d2_m1_the_streak_carries_its_age_and_decays_*` (the model and the pure
  rule), and the worker's `e555d2_m1_the_yield_upsert_keeps_the_streak_in_time`
  (the shipped statement under real SQLite);
- `e555d2_m2_spellings_of_one_server_are_one_origin_and_one_peer` (the key,
  the SHIP dedup, the health written under the origin);
- `e555d2_m2_a_configured_peers_record_says_so`, and the worker's
  `e555d2_m2_a_configured_peer_saves_with_the_discovered_half_full` (DELTA2-4
  inverted: eight spellings take one share of 32 rows, four subdomains fill
  the discovered half, the configured peer saves to the global 256);
- `e555d2_m2_a_resumed_walk_that_faults_idle_is_dropped_after_three`, RED on
  `ef423da` (the record kept);
- `e555d2_l1_a_fresh_walk_that_paid_half_its_budget_keeps_its_record`, RED on
  `ef423da` (no record);
- `e555d2_x_a_deadline_that_fell_due_is_never_polled_again`, RED on `ef423da`
  (the panic).

The old-API pins were grafted onto `ef423da` and run there; the others use
the fold's new API (`peer_origin`, `PeerYieldStreak`, `configured`, the
19-bind upsert) and do not compile there. The route cell runs the two shipped
statements, read verbatim out of the Rust source with literal binds, on local
D1. Stated (D2-N3, the delta-2 lens): `gasp_peer_health` is not served on
`/health/invariants`; an operator sees a streak only in the per-topic
`errors` line once it is past both bounds.

The delta-3 fold (the delta-3 lens on `9280aed`). D3-M1 and D3-L1 above.
Its NOTEs, stated:
- D3-N1: the engine's pins modelled a zero-duration sync (the yield and the
  attempt stamped at one instant, 900 s a tick), on the `>` boundary D1 never
  hits; that is how D3-M1 passed. A pin at a timing boundary steps past it
  (`e555d3_m1` runs 901 s a tick).
- D3-N2: `idle_faults` counts PASSES, not time: at one tick a minute a 3-minute
  partial outage of an honest peer (it lists, its node requests 5xx) drops
  its record; a flapping 5xx at rate p drops one about p^3 a pass. The cost
  is that record's progress; a fresh walk keeps a new record once it paid
  half its budget (D2-L1), and an unproven head restarts `root_proven`.
- D3-N3: D2-L1 halves a FAST stranger's price of a record (50 tiny nodes and
  a 503 keep a `fault` record), bounded as the `calls` records are: one
  origin's 32 rows inside the discovered 128; configured peers unaffected.
- D3-N4: the worker's configured peers (9 (peer, topic) pairs x 16) can hold
  144 rows, past the 128 reserved while the discovered half is full, and in
  bytes past the 64 MiB global; a configured save has no per-origin share, so
  one configured peer's records can crowd out another's (that save is
  `too_many` and the walk goes on, L4). In practice only `tm_uhrp` defers.
  `storage-ownership.json`'s `holds` for `gasp_deferred_graphs` does not name
  the discovered half (cosmetic).

Pins, each RED on `9280aed`'s sources (the tests over them):
`e555d3_m1_the_probe_of_a_quarantined_yieldless_peer_fails_and_rearms`
(DELTA3-1b, 901 s a tick, 27 of 200 attended, every probe failed; RED at tick
44, failures 0), `e555d3_m1_the_decay_is_only_for_a_peer_that_is_not_failed`
(the model), `e555d2_m2_spellings_of_one_server_are_one_origin_and_one_peer`
(amended: ports and schemes stand apart, `:443` collapses), and the worker's
`d1_storage::tests::e555d3_m1_the_probe_of_a_quarantined_peer_continues_its_streak`
(the lens's sequence over the SHIPPED statements, 15 s syncs at `*/15`: 27 of
200; RED at tick 44).

The delta-4 fold (the delta-4 lens on `2c8ab50`): D4-M1, D4-L1, D4-L2 and
D4-N1 above. Pins, each RED on `2c8ab50`'s sources (`gasp.rs` and `engine.rs`
put back in place, the tests over them):
- `e555d4_m1_dead_port_adverts_of_an_honest_host_do_not_silence_it`
  (DELTA4-1: 8, 12, 7 and 1 dead-port adverts, the honest peer on 96 of 96
  ticks and no dead-port slice; RED `(4, 32)` at eight);
- `e555d2_m2_spellings_of_one_server_are_one_origin_and_one_peer` (amended
  back: one peer per origin, D3-L1's displacement as the stated limit, the
  port spellings of D4-L1; RED, five peers);
- `e555d4_l2_an_empty_listing_at_the_probe_does_not_lift_the_quarantine`
  (DELTA4-2, 901 s a tick, 200 ticks: 27 slices with work, 7 empty probes,
  none lifted; RED at tick 44, failures 0);
- `e555d4_l2_a_quarantined_peer_gone_quiet_is_attended_and_a_yield_lifts_it`
  (five quiet ticks attended with the failures kept, a landing graph lifts
  it; seven failures and a quiet sync reset; no per-graph budget, #302's rule
  alone; RED at the first quiet tick).

The worker's sources did not change: the three shipped peer-health statements
are `2c8ab50`'s, and the rule is the engine's (a call it does not make).

### The time limb per resume, and the finish-the-node rule (bsv-low #590)

Zanaadu's measurement (zanaadu-v2 #377): the binding limb was TIME, the
resumes of one (peer, topic) shared one deadline (M1 above, then engine
0.1.11's share), so its deferred graphs resumed one or two a tick, and the
slice raced the node in flight, so a request started near the slice's end
was cut, discarded and fetched again the next pass.

- **Per resume.** Each resume walks under a fresh per-graph slice of its
  own (`gasp.rs`, the resume arm of `ingest`: the latched own deadline).
  The resumes' share (`with_resume_share`, set by the engine whenever a
  per-peer budget is) is read ONLY before a resume starts: one reached
  past it is held back, and a started resume keeps its own slice. The
  share is 33% of the peer budget, independent of the graph slice (39.6 s
  on a 120 s peer; LOW's 30 s peer still has 9.9 s). This also folds the
  lens's counterfactual A: case 3a's three seven-node graphs at 3 s a node
  all converge on pass two in 27 s. The per-peer deadline (D16) bounds them
  all. With no share no time bound is shared. The counted limbs (calls,
  bytes, nodes) stay shared by the resumes of a sync (`resume_left`): they bound what a
  peer's held records cost in requests, and #377 found no graph near its
  100 calls.
- **Finish the node.** The time limb is read BEFORE a step, as the counted
  limbs always were (`walk_graph`, and the resume's root re-ask): no node is
  started past the slice, and a node started inside it is not cut when the
  slice falls due. It is raced only against a fresh slice of its OWN from
  the step's start (`node_slice`), so a fetch that hangs is still a `time`
  cut (the stall rule, `DEADLINE_STALL_DEFERRAL_RUNS`, unchanged) and the
  per-peer deadline may still drop the sync mid-fetch.
- **The default slice.** `DEFAULT_GRAPH_BUDGET_MS` stays 60 s, sized from
  the measured pace (2 to 3 s a node on Zanaadu's cron, 1.2 s on beta: 20
  to 50 nodes). The config is the host's. Stated: `start_gasp_sync` holds
  any configured slice at most `meter::ROOT_MAX_MS` (15 s, the graph slice
  clamp), so the 60 s default and any host value above 15 s are in force as
  15 s. The clamp is unchanged by the fold; the owner's ruling on lifting
  it is pending. LOW's worker keeps its 15 s.

What it costs: a resume started inside the share can finish its own slice
and then its last node's slice. The held loop's walk bound is therefore the
share plus TWO slices, capped by the per-peer deadline, not the lens's
share plus one slice. The four-graph pin starts its last resume at 37.5 s
inside the 39.6 s share and finishes at 61.5 s, past share plus one slice.
LOW's 30 s peer budget remains the outer bound; no page time is guaranteed
in that worst case. With no peer budget, up to sixteen held records each
get a walk slice and a finishing-node slice, subject to shared counted
limbs. Finalize/storage latency is not promised by those walk bounds. The
reference has no budget. Pins (`-p bsv-overlay-engine --features
memory-storage --lib`):
`i590_a` (7 nodes, 2 s a request, a 5 s slice: 3 passes holding 3 then 5
nodes, each link fetched once, the root once a pass by #555's re-ask; RED on
`65027163`: `[Some(2), Some(3), Some(4), Some(5), Some(6), None]`), `i590_b`
(the node started at 4 s ends at 6 s and is kept; RED `Some(2)`), `i590_c`
(two resumes in one sync, none held back; RED `resumed_graphs` 1 for 2);
fold pins: `ident_fold_m1_*` (case 3a and a started resume finishing past
the share); amended: `obligations_0111_*` (the fourth resume finishes its
walk and strikes the unavailable ancestor, while eleven page roots
land and the twelfth, cut by the peer deadline, remains held behind the
earlier faults until their backoff frees its resume share, landing on pass
ten in this fourteen-fault fixture) and
`load_0112_call_limit_fallback_keeps_the_root_deadline` (its courier now
outlasts a whole node slice).

### The assembly and the two limbs (bsv-low #586, zanaadu-v2 #377)

The measured case (Zanaadu, 2026-10-09): the two Worker memory kills of the
e1d bootstrap came after the anchor check of an UNMINED graph. An unproven
node asks for every input, so a transaction is a node once per output index
spent, and `get_beef_for_node` rebuilt a shared ancestor once per REFERENCE: a
`source_transaction` tree by recursion, a cloned subtree per parent, each
`Transaction` holding its parse, its raw bytes and its hex, serialized at
every level. The cost followed the graph's PATHS: 13 nodes, 659 KB fetched,
24.1 MB native (36x), doubling with every unproven link of a chain whose
transactions spend two outputs of the one before (a covenant output and the
change, a wallet's ordinary second spend).

**The assembly rule.** A node's BEEF is assembled from the graph's raw
transactions and proofs parsed ONCE each (`gasp_overlay.rs` `GraphAssembly`):
a transaction is parsed once per graph (keyed by its bytes), a node names its
sources by KEY, and the BEEF is the walk `Transaction::to_beef` made over the
hydrated tree (bsv-rs `collect_ancestors`: depth first, inputs from the last
to the first, a proven node a leaf, a txid once, proofs deduplicated by height
and root) made over the keys, then the SDK's own `Beef` (`merge_bump`,
`merge_transaction`, `to_binary`, whose sort is the SDK's). Nothing is
hydrated. The bytes are the ones the tree gave, for every graph. The anchor
check assembles ONE node's BEEF at a time for its replay and drops it (it held
them all), and keeps the checked assembly for `finalize_graph`, so a graph is
parsed once; anything that changes or discards a pending graph drops it. The
walks are explicit stacks (the recursion used the stack once per unproven
link) and a link back into a node being walked ends there (the root is filed
under the graph id the PEER names, so the recursion could be made not to
return; reasoned, not run on the base).

The witness, `tests/gasp_fanin_memory.rs` (`e586_a`): the peak growth of the
LIVE HEAP (a counting global allocator, native, debug profile) inside
`validate_graph_anchor` and `finalize_graph`, through the real walk and
adapter, over unmined ~26 KB transactions whose bottom spends held coins:

| graph | fetched | anchor check, base | now | finalize net of its BEEFs, base | now |
|---|---|---|---|---|---|
| the diamond chain (7 transactions, 13 nodes, 23 requests) | 600,934 | 29,505,006 (49.10x) | 2,146,543 (3.57x) | 24.32x | 0.97x |
| a layered fan-in (13 transactions, 37 nodes, 85 requests) | 2,221,528 | 25,872,174 (11.65x) | 4,771,057 (2.15x) | 5.70x | 0.51x |
| the diamond chain, 13 links (25 nodes, 47 requests) | 1,228,006 | not run (64 times the 7-link tree) | 4,078,103 (3.32x) | | 0.91x |

The pin is at most 4.0x and 1.5x, and the digests of the finalized BEEFs,
frozen on `fbb7fa8`. What is left, stated: a parsed `Transaction` is 2.0x its
raw bytes (52,860 for 26,118: the SDK keeps the raw bytes beside the parse);
the anchor's Bitcoin check and the replay hand the SDK one BEEF, whose linked
form holds a transaction once per REFERENCE (a full link and bare stubs,
linear in the references, bsv-rs `add_input_proof`); and the finalize hands on
one BEEF per NODE, each holding its whole unproven ancestry (1,280,389 bytes
for the diamond's 182,886 of raw transactions; the sum grows with the square
of an unproven chain's depth). That last is the shape of the submit, one BEEF
per transaction, in the reference too. The reference
(`OverlayGASPStorage.ts` `getBEEFForNode`, f999e0c1a) hydrates as the base
did, a node parsed again for every input that reaches it, so it has the same
multiplier; its `computeOrderedBEEFsForGraph` means to drop a repeated BEEF
(`beefs.includes(currentBEEF)`) and compares arrays by identity, so it drops
none, as here. The ORDER of sibling BEEFs is the order of a `HashMap`
(`GASPNodeResponse::requested_inputs`), run to run, before and after; a
source is always finalized before its spender.

**The two limbs.** The per-graph budget counts two more things per pass
beside its calls and its time (`GraphBudget::max_bytes_fetched`,
`max_nodes`; `Engine::set_graph_budget_limbs`, the defaults in force with
`set_graph_budget`): the BYTES a graph's walk is served
(`DEFAULT_GRAPH_BUDGET_BYTES`, 917,504, a budget per pass and nothing else
since bsv-low #585: the hex of each node's raw transaction and proof, as the
peer sends it and as a record keeps it, every answer counted, a repeat too)
and the NODES it appends (`DEFAULT_GRAPH_BUDGET_NODES`, 64). The worker's
defaults are the engine's (`gasp_deferred.rs`, the consts
`GASP_GRAPH_BUDGET_BYTES` and `GASP_GRAPH_BUDGET_NODES`), and an operator sets
others with the VARS of the same names (`graph_budget_limbs`: a decimal
integer, clamped, bytes to at least 1 with no upper bound (#585: there is no
record cap for it to sit under), nodes to 1 ..= the 100
calls; unset, empty or not a number is the default; no rebuild). The limbs in
force are served as `budget.bytesFetched` and `budget.nodes`. A limb reached DEFERS the graph exactly as the call budget
does (reasons `bytes`, `nodes`): the record saved, the UTXO held below the
cursor, the walk resumed by the next pass from what is pending. Never a drop,
a refusal or a discarded graph: a budget PER PASS, not a limit on a graph or
a BEEF (the owner's ruling of 2026-10-09). A limb is read BEFORE a step, so a
pass always makes one request: a node bigger than the whole limb is walked,
one a pass. The resumes of one sync share the limbs as they share the calls
(M1); a walk the limbs cut that cannot be KEPT goes on under the per-peer
budget alone (L4). The reference has no budget. One residual of the delta lens, stated (its other, a limb above the record cap, went with the cap, #585): the bytes var's floor of 1 lets an operator set the limb below one node's served size, where a `root_unproven` resume makes no progress until `max_passes` releases the record (bounded, self-healing, unreachable at the defaults: an operator who sets a limb smaller than a node has set it wrong).

**A record has no byte bound (bsv-low #585, door 4; the owner's posture of
2026-10-09).** A record was capped at 1 MiB (`DEFERRED_GRAPH_MAX_BYTES`, half
of D1's 2 MB row): one past it was `too_big`, deleted, and its walk went on
under the per-peer budget alone, so a graph whose unwalked ancestry
outweighed the cap (36 links of 26 KB) never resumed past it. The cap, the
reason `too_big` and its counter are gone. The rule: a graph of any size is
deferred and resumed WHOLE; a platform bound (the row) is routed around, and
what bounds a pass is its budget.

- The engine refuses no record for its size (`save_walk`). `byte_size` is
  logged and is the storage's to budget.
- The worker keeps a record in ROWS (`gasp_deferred.rs` `chunk_plan`,
  `DEFERRED_GRAPH_CHUNK_BYTES` 1 MiB): its first part in its
  `gasp_deferred_graphs` row, each further part a row of
  `gasp_deferred_graph_chunks` (migrations 180-182: the table, the head's
  `chunks` and `gen`). A record of at most 1 MiB is written exactly as before
  (one row, no part). A save writes the parts of a NEW generation (16 hex of
  the JSON's sha256), then the head row, where the ceiling is read, then
  deletes the other generations; a read takes the head and each part by the
  generation it names, one part a statement. A refusal or a fault midway
  leaves the held record whole. A delete takes the head, then the parts; the
  cron sweeps a part no head names once it is an hour old.
- `AtCeiling` is a BUDGET of room, never the loss of a walk. The head row's
  `bytes` is the whole record's, so the worker's byte bounds (64 MiB, 32 MiB
  discovered, 8 MiB an origin) count a chunked record whole. A RESUMED walk
  refused room KEEPS the record it holds (`Saved::Ceiling`; it was deleted,
  and the graph walked from its root at the next pass): the pass is counted
  `too_many`, a budget cut goes on under the per-peer budget alone (L4), and
  a pass that does not finish resumes from the held record. A FRESH walk
  refused room has no record to keep and is walked again, as before.
- The bytes limb (917,504) is a budget per pass only. Its derivation (seven
  eighths of the cap, the lens fold's E586-L1) and the var's clamp to the cap
  are gone; the figure is kept for what it measures: **18 heads of 26 KB a
  fresh pass** (17 on a resume of an unproven root, whose re-ask is served
  first). The overheads measured under the old cap stand as measurements
  (`e586f_l1_*`): the chain of 26 KB heads 942,750 bytes for 938,772 served
  (1.0042x); the witness's diamond chain 630,272 for 940,248 served (0.67x:
  a repeat is served and not kept); 64 small proven nodes 29,501 for 15,804
  of hex (1.87x: the NODES limb is what bounds the JSON around a node).

Measured (`e585_d4_*`): a 46-link unmined chain of 26 KB heads (2,399,076
hex) under the default budget is three passes of 18, 18 and 12 requests, its
record 942,750 then 1,832,700 bytes (1.75x the old cap), converged with no
drop and no walk gone on; at its full 46 nodes a record of 2,406,975 bytes
is one head row of 1,048,576 and two part rows (1,048,576 and 309,823).

Limits, stated. (1) What is left of a size in this path is ROOM in the D1
the overlay shares, per origin: a discovered origin's records together hold
8 MiB, so ONE graph whose record outgrows that (about 160 heads of 26 KB)
stops growing its record there; each later pass resumes from the held 8 MiB
and walks the rest under the per-peer budget alone, and lands only if that
budget (30 s on the worker) covers the rest. A configured peer's bound is
the global 64 MiB. Raising a share is a constant; holding the nodes in R2
(the worker has no R2 binding today) would remove it. (2) A record is read
WHOLE into the isolate to resume (its JSON, then the parsed nodes, beside
the pending graph it becomes): memory is the graph's, not an element's. The
streaming reader (bsv-rs 0.4.0) does not apply to a walk in hand; a graph
of tens of MiB is bounded by the isolate before the shares. (3) A save of a
k MiB record is k + 2 statements, a read k + 1, each bound value at most
1 MiB (L5 above: unverified on the platform). (4) The parts are written
before the head's ceiling is read, so a refused save holds up to one
record's bytes past the ceiling until its undo; a save that dies between
its parts and its head leaves them an hour, for the cron. (5) A head whose
part is gone (the table is transient) is dropped at its read and the graph
walks from its root, as after any lost row. (6) A graph that fitted the old
limbs in one pass (up to 64 nodes and 4 MiB served) takes a pass per
917,504 bytes served, a tick each, 15 min at `*/15`. A FRESH walk whose
first node alone is past the limb is cut on it (a pass always makes one
request).
(7) The walk asks an outpoint again for
every parent that reaches it and drops the answer as seen AFTER the request
(85 requests for the fan-in's 37 nodes), as the reference does; the calls and
the bytes are charged for them. (8) A FRESH walk that faults has PAID for a
record at half its calls or half its time (D2-L1), not at half a limb.
(9) Bytes are charged when a node is served, a node when its step commits: a
step the deadline drops has its bytes charged and its node asked again.
(10) The witness is native; wasm32 was not measured here (Zanaadu's figure was
13.4 MiB for the 24.1 MB). (11) A record's nodes are a second copy of the
pending graph's while a walk is in hand (before and after).

**The assembly is a re-implementation, pinned (the lens, E586-L2).**
`GraphAssembly::beef_of` does not call `Transaction::to_beef`: that needs the
hydrated `source_transaction` tree the assembly exists to avoid. It
re-implements bsv-rs 0.3.35's ancestor collection (`collect_ancestors`: the
depth-first walk, inputs last to first, the proofs deduplicated by height and
root and combined) over the graph's keys, then hands the SDK's own `Beef` the
result. The byte parity with `to_beef` is therefore a PIN, not a delegation:
`--lib e586` (the recursion kept verbatim, 3,097 BEEFs and 385 refusals) and
the frozen digests (`e586_a`, `i551_c`, `i551_e`). A bsv-rs bump that changes
the collection order, the dedup or `clone_for_beef` moves `to_beef` and not
`beef_of`: both pins are re-proven at EVERY bsv-rs bump, and a red one there
is the SDK having moved, to be followed here. No correctness rests on it (a
node is emitted after its sources, and `from_beef` parses any valid order).
The finalize's OUTPUT is not bounded by the limbs: one BEEF per node, each
with its whole unproven ancestry, is O(N^2) bytes over an unproven chain of N
(the lens measured a 64-node single-input chain of 26 KB: 54,242,624 bytes of
BEEFs, a finalize peak of 59,325,046). That is the reference's shape and the
submit's. What the limbs bound is what one PASS adds to the pending graph and
its record; a graph completes with every node of every pass in hand.

The `root_proven` restart (the lens's NOTE): the walk that restarts from a
root served proven is fresh, its PASS is not, and what the pass spent of all
three counted limbs is carried to it (calls and bytes were; the nodes are
now, 0 today since the re-ask appends nothing). Pin `e586f_n`: RED against
the bytes carry dropped and against a wrong nodes carry; it cannot be RED on
`589bc2d`, where the value carried is 0.

Pins: `cargo test -p bsv-overlay-engine --test gasp_fanin_memory --
--nocapture` (`e586_a`, RED on `fbb7fa8`: 49.10x); `--lib e586` (the
assembly against the recursion kept verbatim, 3,097 BEEFs and 385 refusals
over 600 random graphs, tolerant and strict; the refusals' words; a
20,000-link chain and a cycle); `--test gasp_topic_manager e586` (`e586_b` a
graph past the bytes limb defers and completes over two passes, `e586_c` the
same for nodes, `e586_d` a node bigger than the limb is walked one a pass and
two resumes share one limb, `e586_e` a record holds raw nodes, no BEEF, and
weighs 1.0059x what was fetched; `Engine::set_graph_budget_limbs` does not
exist on `fbb7fa8`; RED against the limbs made inert and against the resumes
not sharing them). The frozen digests of pin D, the `i551_*` and the `e555*`
families hold unchanged. The lens fold: `--test gasp_topic_manager e586f`
(`e586f_l1_a_graph_of_26kb_heads_*`: 35 links under the defaults nobody set,
18 served, reason `bytes`, converged by the second pass with no drop; RED on
`589bc2d`, where all 35 are walked in one pass with no deferral;
`e586f_l1_the_margin_*` the measured overheads; `e586f_n`;
`e586f_l1_limit` is retired, #585's pin is its cure) and the worker's
`gasp_deferred::tests::e586f_l1_the_limbs_default_under_the_record_cap_and_are_vars`
(the default, the vars' parse and clamp, amended by #585: no upper clamp on
the bytes; the wiring of the engine and the health block).

Door 4's pins (bsv-low #585): `--test gasp_topic_manager e585_d4`
(`*_a_46_node_unmined_graph_of_26kb_heads_is_deferred_and_resumed_whole`,
RED on `e8ab762`: the second pass sends 29 requests, `too_big`, the walk
gone on; `*_a_resumed_walk_refused_room_keeps_its_record`, RED on `e8ab762`
(`too_big`) and against the `AtCeiling` arm reverted alone (the record
deleted)), `e555_b2` rewritten (a record past 1 MiB is kept and resumed; RED
on `e8ab762`), and the worker's `gasp_deferred::tests::e585_d4`
(`*_a_record_past_a_row_is_chunked_saved_and_read_back_whole` and
`*_a_refused_replacement_leaves_the_held_record_whole`: the shipped
statements in the storage's order under real SQLite; they do not compile on
`e8ab762`). The route cell binds the upsert's two new parameters and reads
`budget.chunkBytes`; it was not run in this lane (`make ci-d1-budget`).

## The stored-rows reader (bsv-low #585, E585-land; the land fold-6)

`crates/overlay-cloudflare/src/stored_rows.rs` (test-only) reads a D1 export
(and the queue bucket's objects saved in a directory) natively, before a
deploy of the streaming door, so a stored BEEF the door would refuse is named
first. Every path CALLS the Worker's own read (the land lens E585-LAND-L3):
the stored BEEFs (`transactions`, `transactions_evicted`, `pot_beefs`) by the
Worker's `hex(beef) AS beef` and `d1::beef_of_hex_column` (so a BEEF stored as
hex TEXT is its ASCII bytes and refused, as at the Worker; the one reader of
EVERY Worker read-back of that column since the land fold-7, E585-LAND-DELTA-L1,
`OutputRow::into_output` and `reorg_sweep`'s transactions leg included, which
keep an empty read-back as an empty BEEF as before; pinned by
`d1::tests::every_hex_beef_read_back_calls_the_one_reader`, RED on `6c7f527`),
then
`beef_limits::read_beef`, `own_proof` and `transaction_from_beef`; a parked
letter as the lever re-drives it (`dead_letters::redrive_message`) and the
consumer reads it (`queue::read_for_replay`, its R2 object through
`queue::replay_object`; the `r2_key` column is not read); a deferred record by
`gasp_deferred::read_deferred_graph` over the shipped statements and D1's
column types. The run prints a `SUMMARY` and a `VERDICT` line and FAILS on any
refusal and on any `UNREAD` row (an object not in the directory, a record's
part missing). Run: `STORED_ROWS_INPUT=<export.sql> STORED_ROWS_R2_DIR=<dir>
cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib
stored_rows::read_the_export -- --ignored --nocapture`. Pins
`stored_rows::self_test` (`e585_land_f6_l3_*`: a letter through the consumer's
own read, a deferred record as the Worker reads it, hex TEXT refused, an
unread row failing the run; each RED on `e2c561c`, grafted). Left, stated: a
table or column the export lacks reads as empty, and the deferred tables are
given their own migrations first, as the Worker's first request would.

## The app layer's mirrored counters (bsv-low #625, #626, #628)

The app layer's fault-shaped `/health` fields are isolate samples; their durable twins are rows of this worker's
`ops_counters`, written by the app layer through its existing courier-census flush (`courier::note_presence`,
`take_pending_if_due`, `flush`: the shared `OVERLAY_DB`, one D1 batch, in `wait_until`), and seeded at 0 by
`read_counters` (`applayer_counters::seeded_names`, pinned equal to the app layer's names by its
`obs_counters_tests`). The families: #625 `courier_applayer_<provider>_<outcome>_total` (the sibling overlay
readers' `courier_<reader>_<provider>_<outcome>_total` convention; outcomes `ok`, `notfound`, `fault`, `shape`,
and this reader's `ratelimited`; a 2xx whose body does not read as its route's format is `shape`, a 404 or 410
`notfound`, so a Bitails 404 on an unmined subject is never a fault and a WoC mempool body with no
`confirmations` is `ok`; the older `applayer_courier_<provider>_<outcome>_total` family is unchanged); #626
`applayer_txany_memo_hit_{absent,unconfirmable,refused,retired,anchor,isolate_unconfirmable}_total` and
`applayer_txany_memo_write_{anchor,isolate_unconfirmable}_total` (the app layer only READS the durable verdict
memo; this worker writes it and counts `txany_memo_write_{refused,retired}_total`, one statement per write: the
retire pass's at most 16 a tick and one per eviction; no writer of an `absent` or `unconfirmable` memo exists on
this tip); #628 `applayer_owed_*` (recompute faults, shed, deferred abandoned and faulted, arm failed, lock
takeovers, sweep-proof contradictions, read faults by kind and summed, and `applayer_owed_recomputes_total`, the
fleet's count of landed recomputes, the durable twin of the isolate's `/health.owed.recomputesThisIsolate`; both are
#628's and neither is per identity). #566's `recomputeSeq` is the one on the `/owed` ANSWER, beside `computedAtMs`:
the identity's own sequence, in its `owed_state` row (migration 260, the app layer's byte-identical ALTER catch-up),
bumped by the snapshot's own upsert once per LANDED write (a superseded walk's refused upsert leaves it), read back
by the batch's closing statement and the existing state read (no new statement), monotone across isolates, absent
from a body whose snapshot carries none (pin `obs_566_*`). Then `applayer_record_refused_<class>_total` beside the
total, `applayer_auth_refused_<reason>_total` beside the total (the session lane's `ERR_SESSION_REFUSED` by its
reason as `lane_<reason>`, the reference door's 401 by its code, read off the middleware's own response bytes;
the total and the isolate's `/health.auth.authRefused` count the same 19 arms, the app's `mismatch`,
`strict_unauthenticated` and `misconfigured` included, which keep their own isolate fields too: since the lens's L2
`authRefused` is no longer the middleware's arms alone)
and `applayer_identity_<route>_{upstream_fault,hash_mismatch,no_resolver,kill_list_fault}_total`. THE FLUSH
BOUND: a take is made when an answer leaves (the router's, the front door's own reply, an exempt identity route,
the BoardView actor's fetch and alarm, the scheduled drain) and flushes the isolate's first 4 takes at once, then
only once 64 increments are pending or 60 s have passed since its last flush; one flush is ONE D1 batch, one
subrequest. Worst case per isolate per minute: the 4 head flushes once per isolate's life, then
`ceil(R / 64) + 1` subrequests for R increments a minute, and never more than one per answered request (one take
per answer). Stated: an isolate that answers nothing more loses its unflushed tail (an under-count, never a wrong
answer), and a deferred owed walk's counts are flushed by its actor's isolate, not the fetch isolate. Alerts are
OFF (counters only, the owner's word). Pins: `cargo test --manifest-path workers/Cargo.toml -p low-app-layer
--lib obs_` (`obs_625_*`, `obs_626_*`, `obs_628_*`, the identity pin in `identity::tests`) and `-p
bsv-overlay-cloudflare --lib applayer_counters`.

The scheduled drain's courier flush (bsv-low #658) retains its taken batch
until every D1 result acknowledges success. A deadline during the flush or
an unacknowledged write merges the batch with any newer increments and makes
it eligible for the next take, even past the first four flushes. The take
and write remain inside the drain's one deadline. A remote commit whose
acknowledgement is lost can be counted twice on retry; an isolate recycle
still loses its in-memory tail. This affects telemetry only. Pins: `e658`
and the existing `e649f4_n9` pre-poll expiry pin in the app layer.

## The hot passes' D1 reads (bsv-low #643, #644, #645, W1.6)

The tick's signers and payouts backfills (`settle_signers_candidates_sql`, `spender_payouts_candidates_sql`) read
their pending rows through two partial indexes under their own predicates (`idx_pot_signers_pending`,
`idx_pot_payouts_pending`), never the whole `pot_records` (22,591 rows to 2 on the fleet fixture's local D1).
The app layer's per-block tip mark (`OWED_STALE_ON_TIP_SQL`) is an index range over the block's seven-block window
(`idx_pot_unspent_recovery` on `(spent, recoveryHeight)`), never every unspent pot (6,944 rows to 1 at a block
whose window holds none). `/health/invariants.networkSeenTotal` reads the one row `count_network_seen`, kept by
`network_seen`'s insert and delete triggers and backfilled once behind its own row (27,000 rows to 1), so no writer
may `INSERT OR REPLACE` into `network_seen`. Pins `crates/low-app-layer/tests/d1_hot_bounds.rs`, the ceilings in
`tests/support/cursor_measure.rs`, the figures in `docs/D1-BUDGETS.md`.

NS-COST-BAR-1 (the owner's cost word of 2026-10-18; the 24-hour census of 2026-10-17). The tick's proofless-watch GC
(`ops::PROOFLESS_WATCH_GC_SQL`) probes `pot_beefs` and `transactions` by key for each watched txid, never a list built
over a store (63,638 rows to 389 on the fixture). `retiredUnprovableTotal` reads `count_pot_beefs_unprovable`, kept by
the triggers of the membership set `pot_beefs_unprovable_set`, which `pot_beefs`' own triggers keep idempotently
because its writers `INSERT OR REPLACE` (20,839 to 1). `txRetiredTotal` and the expired-unproven total read
`count_transactions_retired`, the same shape over `transactions_retired_set` (17,438 to 1 each). The index janitor's
backlog is `count_pot_unspent_kind` (unspent pots per lock class, kept by `pot_records`' triggers) less the hour's
young pots through `idx_pot_unspent_created` (13,887 to 8). The four relatch censuses are a rowid range plus a
partial index of their NULL tier. The tick and `/health/invariants` count their own rows read by family
(`d1_rows_read_<family>_total`, `/health/invariants.d1RowsRead`) against the daily budget in
`tools/lane-499/ceilings.json` `d1RowsReadPerDay` (prod 20 M, beta 60 M). The keeper's `rust-chaintracks` count of
headers and its split trigger are its own (`docs/D1-BUDGETS.md`, the ask). Pins `d1_hot_bounds.rs` `ns_cost_*`, the
writers under `recursive_triggers` off and on in `ops::tests::ns_cost_*`.

THE COST BAR (bsv-low #635, B-COST-1). Each Worker of the shared D1 (the overlay and the app layer) sums what its
isolate cost the plan in a WINDOW (`d1_ledger.rs`, the shared source: every fetch request at the door,
`cost_on_request`, and every statement the ledger notes, scoped or not, the cron's, the queue's and the Durable
Object's walks included) and adds it to `worker_cost_daily` (migration 343: one row per UTC day and Worker, the
crate's package name) with ONE upsert (`COST_DAILY_UPSERT_SQL`) when the window holds 64 requests, 60 s have passed
since the isolate's last take, or the UTC day moved: at most `ceil(R / 64) + 1` writes per isolate per minute, never
one per request; the write is handed to `wait_until` at the fetch door and awaited after the cron and each queue
batch; a faulted write is restored into the window (a table not yet migrated, D1 overloaded: a delay, not a loss).
`/health/invariants.costBar` serves the last 7 UTC days, each Worker's figures and their total (requests, D1 rows
read, rows written, statements), beside the plan's included quotas per day, which are VARS
(`COST_QUOTA_REQUESTS_PER_DAY`, `COST_QUOTA_D1_ROWS_READ_PER_DAY`, `COST_QUOTA_D1_ROWS_WRITTEN_PER_DAY`, set in the
three overlay configs to the Workers Paid inclusions over 30 days, the ACCOUNT's, for the operator to lower to this
environment's share; unset or 0 is no quota), the ratio per measure and `overQuota` (today's; `true` past any
configured quota, `false` within all, `null` with none configured or the table unreadable), `overQuotaDays`, and the
flush bound. Stated: an isolate's unwritten tail is lost (an under-count); a statement awaited through the bare
`worker` API is not counted (the ledger's seam); a window straddling midnight lands on its first request's day;
cron, queue and Durable Object invocations are not counted as requests (their D1 rows are). No rate limiting is
built (the recommended rule is in the PR, the owner's). Pins `--lib b_cost_1` in both crates (`d1_ledger::b_cost_1`,
`cost_bar::tests`, the two wiring pins); RED against four revert mutants (a statement not costed: the window pin and the
overlay's wiring pin; the 64-request rule dropped; the upsert replacing instead of adding; the over-quota comparison
moved); the app layer's wiring pin is a source pin and was not run against a mutant.

## The records door: a no-op stands only on a valid row; a sweep's game is its hop's (bsv-low #798, #663, MONEY-DOOR, 2026-10-24)

The records door (`crates/low-app-layer/src/record_post.rs`, register row D11) runs its steps after the marker verified in ONE core, `record_post::file_verified` over `pot_hook::SqlPort` (the route over `routes::D1Sql`, the pins over real SQLite with the overlay's migrations): the refund's pot bind, the hop bind, the chain no-op, the caps, the write, the late latch.

- **#798, the potrefund chain no-op.** `tm_potrefund` admits by byte format, so a stranger could buy a `LOW/potrefund` marker naming a victim's identity, game and pot with a junk signature, and the victim's own valid backup then answered `alreadyIndexed` before the late latch: a rank-0 backup never latched and `/refund-backups` served the planted row first. Now a chain row makes the filing a no-op only when it is VALID for the filing's own binding (`record_post::potrefund_chain_row_is_own`: the filed path's whole bar over the row's stored bytes, the marker signature under the identity AND the raw the pot's pre-signed spend by both committed settle keys, rank 1 by `bind_refund_backup`), read per (identity, game, pot txid, pot vout), at most `POTREFUND_CHAIN_ROWS_READ` (16) rows. Each such row is LATCHED `refundValid = 1` by its own outpoint (`POTREFUND_CHAIN_LATCH_SQL`) before the no-op answers, so it leads `/refund-backups`; every other row is counted (`/health.record.potrefundChainRowsUnverified`, isolate) and never a no-op. With the pot not indexed no chain row is valid, so the filing writes its rank-0 row and the re-file latches it. The junk row is not released (`potrefund_records` is never-wipe, and the app layer holds no delete scope); it sorts below every latched row. The handfile arm takes the same decision (`record_post::potrefund_chain_noop`). The cap counts filed rows only (the poster's own, signature-verified), so a planted chain row never spends it.
- **#663, the hop sweep's game.** `hopsweep::bind_hop_sweep` compares the filing's gameId with the hop's own marker row in every arm (the latched `markerValid` and the signature replay over the row's game). The comparison follows the sweep's seat-signature check, so a wrongly signed filing receives the signature refusal under either game. A mismatch returns 422 with `code: "sweep_game_mismatch"` and the explanation in `error`, increments `applayer_record_refused_sweep_game_mismatch_total`, and writes no filing. The door never corrects a mismatched game. Pin `--lib d2_663`: a real signed sweep through the parser, SQLite hop lookup, binding and route reply preparation, with latched, unlatched and absent verdicts; the same signed sweep under its own game files successfully. The per-(poster, game, hop) cap stays 2.
- Pins (`--lib money_door`, the shipped core over real SQLite, real signed filings, the planted row through `tm_potrefund`'s byte check and the overlay's own `store_record` statement): `money_door_798_a` (the planted junk row, then the victim's valid backup written at rank 1 and served first, the planted row counted, the cap still 1), `money_door_798_b` (the controls: the identity's own valid chain row is still the no-op and is latched; the same content twice is one row; a victim-signed chain row whose raw is not the pre-signed spend is no no-op; rank 0 then the latch; the handfile arm's call), `money_door_663_a` (both arms refuse the wrong game, the honest control files to its cap, no other game buys it again). RED on `129125fb` (the core extracted, behaviour unchanged): `left: (AlreadyIndexed, true)`, the own chain row `None` where `Some(1)` was wanted, `left: "Written { row_written: true }" right: "Refused(SweepGameMismatch)"`; RED against a sig-only chain judge (`left: AlreadyIndexed`) and the game compared in the latched arm alone.
- **The fold (the MONEY-DOOR lens `MONEY-DOOR-LENS-2026-10-24.md` in bsv-low, SOUND with two LOWs; lane MONEY-DOOR-FOLD).** LOW-1: a chain no-op whose latch MOVED the poster's own chain row to `refundValid = 1` (`record_post::ChainNoop::latched`, `DoorOutcome::AlreadyIndexed { latched }`) moves what is owed, so it is marked as a written row is (`record_post::owed_marks_after`, then `mark_filing_stale` through the port: the pot's parties, the filer, and the filer's coalesced walk) while the answer stays `alreadyIndexed`; a repeat whose latch moves nothing marks nothing. The handfile arm: a batch that wrote no row is marked when the refund's no-op latched (`handfile::handfile_noop_moves_owed`), its answer still the re-file's. LOW-2: `pot_hook::StmtOut::changes` is `Option<u64>` (D1's `meta.changes` is optional; `routes::D1Sql` no longer reads an absent count as 0); the `collected` insert with no count re-reads its row by key (`record_post::collected_written_now`, `COLLECTED_ROW_STAMP_SQL`: stamped by this request is a write, stamped earlier the re-file, absent or unreadable a write), and a latch with no count is read as moved (it marks: the safe direction); the pot-changed parks read an absent count as 0, as before. Pins `money_door_fold_l1_a` (a latching no-op marks the pot's party and not a bystander; the repeat marks nothing; the route marks before it answers), `money_door_fold_l1_b` (the handfile decision and its call), `money_door_fold_l2` (a port answering no count: a first `collected` filing a write and marked, the re-file not; the latch read as moved), each RED on `55591bab` with the API grafted and the base's rules kept (`left: (0, 0) right: (1, 0)`; `left: (false, false, false, false) right: (true, false, false, false)`; `left: Written { row_written: false } right: Written { row_written: true }`). The lens's negative cases `lens_798_a`, `lens_798_b`, `lens_798_c`, `lens_663_b` are kept as written, the `AlreadyIndexed` patterns given the new field.

## Storage ownership (bsv-low #474)

The overlay and the app layer share ONE D1 per environment (`OVERLAY_DB`).
`storage-ownership.json` names every table's owner, further writers (by
crate and statement), readers, rebuild class and never-wipe flag;
`docs/STORAGE-OWNERSHIP.md` is its prose (with the never-wipe set of the
owner's ruling of 2026-10-07, `low-identity-db`, and the tower's and the
relay's stores, owners only). `make ci` runs
`scripts/check-storage-ownership.py`: a statement the manifest does not
grant, a table it does not list (a migration adding a table must add its
row in the JSON and on the page), or an unpinned string-built table name is
red with the file, the line and the table. A never-wipe table is enforced for
every crate, its owner included (the lens fold of 2026-10-07): no DROP,
TRUNCATE, destructive ALTER or DELETE with no WHERE, and a DELETE with a
WHERE only under the row's `delete_scope` (crate, file, statement). Its
parser limits are in the script's header and pinned by its `--self-test`.

## Testing

```bash
# Fast unit + integration (no network): the engine crates, then the workers
cargo test --workspace --features bsv-overlay-engine/memory-storage
cargo test --manifest-path workers/Cargo.toml --workspace

# Property tests (proptest, 256 cases each)
cargo test --workspace --features bsv-overlay-engine/memory-storage --test property_tests

# Live tests — hit a deployed overlay. Require OVERLAY_URL env var.
OVERLAY_URL=https://<your-overlay>.workers.dev \
    cargo test --workspace --features bsv-overlay-engine/memory-storage -- --ignored
```

## Parity harness

```bash
# Two shells, long-running:
make reference-up      # mainline @bsv/overlay-express@2.2.0 in docker on :8090
make wrangler-dev      # our Rust worker on :8787

# Then diff:
make harness           # writes PARITY_REPORT.md
make parity-clean      # wipe state + restart for a deterministic run
```

## End-to-end

`tools/e2e_bsv_storage.sh` (`make e2e-bsv-storage`) — round-trip smoke
against a deployed overlay + sibling UHRP storage worker. Defaults to
your prod URLs; override with `OVERLAY_URL` and `STORAGE_URL` env vars.

## Deployment

```bash
# One-time setup
wrangler d1 create bsv-overlay   # paste returned database_id into wrangler.toml
wrangler secret put ADMIN_TOKEN
wrangler secret put SERVER_PRIVATE_KEY
wrangler secret put TAAL_API_KEY   # optional — enables /arc-ingest
wrangler queues create overlay-mutations-dlq   # the dead letters (#576)
wrangler r2 bucket create overlay-beefs        # queued BEEFs past the inline room (#585 door 3); no expiry rule

# Deploy
cd crates/overlay-cloudflare
CLOUDFLARE_API_TOKEN="<token>" CLOUDFLARE_ACCOUNT_ID="<id>" wrangler deploy
```

**Three deployable configs**, all built from the one workers' lock (`workers/Cargo.lock`): `crates/overlay-cloudflare/wrangler.toml` (`bsv-overlay-cloudflare`), `crates/overlay-cloudflare/wrangler.low.toml` (`low-overlay`, LIVE: `wrangler deploy --config wrangler.low.toml`), and `crates/low-app-layer/wrangler.toml` (`low-app-layer`). Their `[build]` worker-build pins must all match each other and the lock's `worker`; `make ci-deploy` builds all three and refuses on drift. **A green `make ci` without `ci-deploy` is not evidence a worker can be deployed**: that gap is exactly bsv-low #348.

- **Admin auth**: Bearer token on all `/admin/*` routes except `/admin/config`.
- **Cron**: `*/15 * * * *` for ad sync + GASP peer sync.
- **Extensions**: set `ENABLE_EXTENSIONS=true` to register UHRP / Agent /
  DmDelegation topic managers + lookup services beyond the mainline
  SHIP/SLAP baseline.

- **Bounded (bsv-low #581, the delta-4 lens D4-L1; B-COST-1):** with one dead-letter consumer a held-back "not now" letter took up to 101 deliveries, so a flood could delay honest fault letters toward the queue's retention. Now a held-back not-now letter is handed back for 12 h and given up, counted (`dead_letters_lost_not_now_total`) and acked, at its 5th delivery (48 h, inside the 4-day retention): at most 5 deliveries per letter on the one consumer. Stated: a flood's FIRST deliveries still share the one consumer with fault letters (a queue per class would split them; not built), and an honest not-now letter during a flood is LOST after its 48 h, as before; its DEADLINE is as before, its odds are not (the lens's LOW-1, stated, not changed, so the bound of 5 holds): it looks for room 5 times where it looked about 101 times, so a place freed and re-taken between two 12 h looks is one it now misses. The give-up's ack is gated on the class condition itself (NOTE-2).
- **The change webhooks' outbox (bsv-low #560, B2-lens LOW-3; B-COST-1):** every flush of the pot, hop and lobby sets parks its NEW note-backs (a failed POST's entries, the part past the flush's bound) in D1 `change_parked` (#649's table, kinds `pot`, `hop`, `lobby`; `change_park::park_noted_back`, at most 128 a flush a set), beside the isolate's note-back (the fast path, unchanged). The cron's drain (`*/15` and `/admin/complete-proofs`) delivers each row, backs it off on a refusal and counts it exhausted after 8 (`change_park_exhausted_total`), so an isolate's end or a retry that fails too loses nothing; an entry the FULL note-back memory lets go (1,024 first failures, 4,096 deferrals) is parked too, first under the same bound, and still counted `*_undelivered_total` (the lens's LOW-3); a receiver's refusal (`dropped`) is not parked. Counted: `{pot,hop,lobby}_changed_outboxed_total` (parked), or the miss: `change_park_full_total` (the table's 2,048 rows, or a lobby key past the lobby's share of 512, a quarter, inside `PARK_SQL` so a lobby storm with the app layer down leaves 1,536 rows to the pot, hop and word parks: the lens's LOW-4), `change_park_faults_total` (the batch faulted), `change_outbox_over_bound_total` (past 128). `/health/invariants.changeParked` serves `lobby` and `lobbyExhausted`. The flush's account (`*_undelivered`, `*_retried`, `*_deferred`, `*_resent`) is written BEFORE the park's batch (the lens's LOW-2: `settle_and_park` takes it and awaits it first), so a task cancelled during the batch keeps those counters. Stated: a task cancelled after its last POST and before its park lands keeps that flush's note-backs in memory only, and an entry in flight when the task ends is the pre-existing loss of a POST in flight; an entry delivered from memory is delivered again by the drain (harmless). No migration; `PARK_SQL` gains the lobby share's bounded count (read only for a new lobby key; the gauge gains two bounded counts and serves `lobbyMaxRows`). Pins `--lib b_cost_1_560` (RED with the park removed: `parked: 0` against `parked: 11`); the lens fold's `b_cost_1_560f_l2` (RED with the account after the park: `left: 11 right: 0`, rows parked before the account), `b_cost_1_560f_l3` (RED with the let-go entries kept out of the outbox: `left: [] right: [1, 2, 3]`), `b_cost_1_560f_l4` (RED with the share removed: `left: (640, 0) right: (512, 128)`).

The owed recompute's lock never outlives its walk (bsv-low #800, FIX-800-LOCK; loops 39 to 41: a recompute's future cancelled at the platform's 30 s `wait_until` ceiling, or a folded rerun or the hook's second identity started behind a slow walk, held the identity's isolate lock until a later ask found it 60 s silent, 106 to 675 s, so a filing's row missed `/owed` in the cell's 90 to 120 s). Each walk runs in a SLICE of its invocation (`owed::owed_walk_slice`; the invocation's end `owed_invocation_end` = its start + `OWED_PLATFORM_WAIT_UNTIL_MS` 30 s - `OWED_RELEASE_RESERVE_MS` 3 s, captured where each `wait_until` starts: the read's refresh at its claim, the hooks' and the filing's at their block's top): a hard deadline at the invocation's end, `OWED_WALK_HARD_DEADLINE_MS` 27 s for a walk started with the invocation (the FIX-800 lens fold's MEDIUM-2: derived ONCE from the invocation's start, which already holds the start lag; as first built it was also capped at 24 s from the walk's own start, counting the lag twice and dropping the 24 to 27 s walks the base landed; the residual, a 27 to about 29 s walk the platform would have finished, is dropped), at which the claim DROPS the walk itself (`routes::owed_walk_bounded`, a `worker::Delay` race (`owed_with_deadline_on`, generic over its timer) inside the walk's D1 ledger: the drop the platform would make, made while the claim still runs; the snapshot batch is all or nothing and monotonic, #487), and an outward budget of `OWED_RECOMPUTE_TIME_BUDGET_MS` 12 s or half the slice when less is left (the candidate cursor's pages, the couriers, the home walk all read it; it was 12 s from after the tip read). Stated, not equalised (the fold's LOW-4): a rerun or the hook's next identity started T s into the invocation gets min(12, (27 - T) / 2), 9.5 s at 8, 5.5 s at 16, 4 s at 19, because the stored reads, the pay-vout pass and the batch are ungated and loop 39's cut walks ran 22 to 28 s; those walks are cut more often (a cut snapshot standing unmarked honours R-HIGH-1's latches until the 5 min age). A walk is not started on less than `OWED_WALK_MIN_SLICE_MS` 8 s. THE RELEASE POINT is `routes::owed_recompute_claimed`'s `OwedClaimStep::Release` arm (`owed::owed_claim_step`): the lock is released in the step the walk ended, before any further await, then the stale mark, then the pushes (the fold's LOW-2: a landed walk's push in the `Rerun` arm, and a shed's re-mark and deferred arm, run after the release too; nothing but the walk is awaited under the lock); a walk never started marks the identity stale AND pushes `owed-changed` with null rows (`owed::owed_stale_event_body`, `routes::owed_stale_nudge`; the fold's MEDIUM-1: the page re-reads only on events; its read walks once on a fresh invocation); a walk DROPPED at its deadline marks it stale and is HANDED TO THE DEFERRED ALARM WALK instead of nudging (the delta lens's MEDIUM-A, the fold-2: the nudge made the page re-read into another 27 s walk dropped the same way, one every ~28 s a tab, under #499's ceiling of 12 a minute, so the 60 s alarm was never reached): `owed::RecomputeRate::hand_to_alarm` arms the window's one reserved walk once (a second drop in the window arms nothing new; ceiling + 1 kept) and refuses the identity's request-path walks until that alarm has had its whole deadline (`max(window end, now) + 60 s`, 60 to 93 s past the drop), a read meanwhile shed with `retryAfterMs` to then; so a heavy identity costs one request-path walk per 87 to 120 s per isolate whatever its tabs, then the alarm's; only an arm that failed lifts the hand-off and nudges; the alarm's landed walk pushes its rows and its dropped walk nudges once; a claim pushes ONE `owed-changed`, its last landed walk's (the delta lens's LOW-A); a walk cut at its budget (`OwedComputed::budget_cut`), dropped at its deadline or not started never runs a folded ask under the lock (dropped or unstarted: the identity marked stale, the next read walks it; cut: its partial snapshot stands, stale only when an ask is folded); a quick walk with time left still serialises the twins (fleet loop 11). The take-over bound `OWED_IN_FLIGHT_STALE_MS` is the platform's 30 s (it was 60 s): no live claim goes untouched past one hard deadline, so a take-over is only an abandoned future, counted `applayer_owed_recompute_lock_takeovers_total` (the existing name, kept) with its age logged and the isolate's maximum on `/health.owed.recomputeLockTakeoverMaxAgeMs`; new durable counters `applayer_owed_recompute_walk_deadline_total` and `applayer_owed_recompute_no_slice_total` (seeded by the overlay). Every walk's log lines carry a recompute id (`r<hex>`, `owed::owed_recompute_id`, #566's ask). Stated: the hook's identities share one invocation, so behind a first walk past 19 s the second is not started (stale, the next read); the first read's inline compute is under no claim and is not dropped; the deferred alarm's walk runs in the Durable Object's alarm (no `wait_until` ceiling, no lock) under its OWN deadline, one #648 window, `OWED_DEFERRED_WALK_DEADLINE_MS` 60 s (the fold's MEDIUM-3: its faulted retry is armed for the next window's end, so it has ended before its retry can be due; the platform documents 15 min for an alarm's wall, not measured here), outward budget 12 s; one dropped there marks stale, pushes and answers a fault (its one re-arm). The fold's LOW-1: each claimed walk takes the folded asks it serves as it starts, and an ask still folded reads as stale on that isolate (`owed_folded_waiting`), so a dropped walk's batch landing after the claim's mark cannot strand it there (another isolate's read waits for the 5 min open-row age). No statement changed shape (the stale mark is the existing `OWED_STALE_FOR_IDENTITY_SQL`). Pins `--lib fix800` in `low-app-layer` (`routes::tests::fix800_a` to `fix800_j`; `i` a dropped walk arming the alarm once and pushing nothing, the hand-off's rate, `j` one push a claim; `e` the 26 s walk landing and the 28 s one cut through the shipped race over a virtual clock, `f` the alarm's 40 s walk landing, `g` the nudge, `h` the lock's awaits and the folded read; each RED against its rule reverted alone). The second delta lens's fold (FIX-800-FOLD-3): MEDIUM-B, a hook SHED while the window's alarm walks (the hand-off sheds every hook of the identity on its isolate for the alarm's flight; the base's ceiling case too) re-marked stale and the alarm's write, stamped at its start, cleared the mark: now a shed whose window alarm is armed already NOTES the actor first (`/owed-deferred-note`, `owed::owed_alarm_note`: `pending` before the walk starts, `noted` in flight, kept as `owed-dirty`, `idle` after), then marks, then tells the page unless `pending` (`owed::owed_shed_nudges`); a noted alarm walk (and the actor's first-read walk) marks the identity stale AGAIN after its write and before its push (`routes::owed_remark_after_alarm`, the shipped `OWED_STALE_FOR_IDENTITY_SQL`), so every isolate's read walks the evidence. Neither of the lens's shapes: its durable `staleAtMs` is a migration and every mark's bind (this lane changes no statement and no migration, 341 kept), its minimum (`OWED_RERUN`) is this isolate's alone; left as LOW-B, another isolate's walk in flight across a mark. LOW-C: a dropped walk's alarm FIRES AT THE DROP (`OwedHandOff::fire_at`, the actor's `owed-fire`, `&fire=`), its reservation still keyed to the window's end (ceiling + 1 kept), so the hand-off is 60 s from the drop and the identity lands at most 87 s after the dropped walk started (it was up to 120 s, past identityNone's 90 s); an alarm a shed armed at the ceiling before the drop keeps its window-end fire time (`max(end, now) + 60 s`, stated). NOTE-3: `[owed] recompute (<src>) r… for <identity> handed to its deferred walk until <ms> (armed: fires at <ms>, reserved for the window ending <ms> | already armed …); the walk dropped <n> ms after it started; the alarm walk's deadline <ms>` (`routes::owed_hand_off_line`). NOTE-2 stated at `hand_to_alarm` (a hand-off's arm replaces a faulted alarm's retry arm, so under sustained overload the bound is a rate, not #648's count); LOW-A's cut arm stated at `OWED_RELEASE_RESERVE_MS`. LOW-D: `fix800_j` reads the code alone (`fix800_code`) and `fix800_l` pins the `max(now)` floor. Pins `fix800_k` (every interleaving of the hook's note, mark and nudge with the alarm's write, dirty read, re-mark, push and clear, 56 of them, on the shipped statements under real SQLite: the page's last event finds the identity stale) and `fix800_l`. Since B-OWED-2 (bsv-low #567): a refreshing read whose own claim was ADMITTED waits up to `OWED_READ_INLINE_WAIT_MS` (2 s) for its walk and answers that walk's rows when its write landed whole (not superseded, not cut); the rest of the walk runs in `wait_until`. Who pays (the lens's LOW-2): that one read, at most 2 s; a shed read, a read whose claim another holds and a read the ceiling refuses never wait. What it buys: tip-current rows where the walk fits 2 s (the issue's 244 ms); on a heavy identity (12 to 26 s walks) the read pays the 2 s and still answers the stored rows. Not measured on beta (the admitted read's p50, the share of walks landing inside 2 s).

NS2 #648 / #499 fold (2026-10-13): a shed is never silent: one reserved walk per identity per window per isolate (the deferred alarm or a snapshotless first read, one durable claim), the bound ceiling + 1; a faulted reserved walk re-arms ONCE for the next window; the read answer marks a shed snapshot (`refresh: "shed"`, `retryAfterMs` >= 1,000); a filing walks the filer only, the pot's other parties are marked stale.
A stranger's filing buys one walk of the stranger's own identity, against its own ceiling (NOTE-A).
A `result` filing refreshes nobody (its identity is `None`, as on main); the honest seat's refresh rides its
`potparty` / `potrefund` / `collected` / `held` / `hopsweep` filings and the settle hooks (NOTE-B).
A faulted retry stops and counts `owed_recompute_deferred_abandoned_total`; the identity's next read past the
window repairs the stale or absent snapshot. One actor per identity/isolate merges carried and new reservations.
NOTE-C: deleteAll() preserves the alarm at compatibility date `2024-12-01` (the app-layer's pinned date).
workers-rs 0.8.5 `Storage::delete_all` delegates directly to the JS `deleteAll()` call; Cloudflare changed
both KV and SQLite backends to delete the alarm only at dates `2026-02-24` or later
([compatibility change](https://developers.cloudflare.com/changelog/post/2026-02-24-deleteall-deletes-alarms/)).
The workerd DO pin sets an alarm, deletes all keys, and observes the alarm still set at the configured date.
The folded cleanup sets its tombstone alarm AFTER delete_all and retains only the consumed deadline until
that alarm removes it; its correctness no longer depends on the old alarm surviving deleteAll().

The owed list's courier-bytes swept payout also reaches confirmed ("home on chain") from a CHAINTRACKS-checked mined proof of the sweep (`OwedInputs::proof_mined_spenders`, gated by `txany::proof_mined_word`'s `confirmed && height` bound through `mined_proof_matches`, never a courier's bare height) read beside the spent-any ladder's single-WoC `spent_confirmed` so a hop whose only confirming courier faulted still confirms, the rows staying `claimable:false`; the MEDIUM-1 residual (bsv-low #692, follow-up #693) is that the proof fallback is timing-dependent, firing only on a pass that asks the sweep's `/tx-any` before its bytes go durable (#672), narrowed not eliminated and self-healing either way. Since #693 the recompute re-asks `/tx-any` for a durable spender whose row still needs the proof (no tracker mined word, no ladder `spent_confirmed`; `owed::durable_spender_needs_proof`), at most one a pass (`OWED_DURABLE_REASKS_PER_RECOMPUTE`) and only from the courier slots phase 2 left under the pass's clock, so a pass still makes at most two courier asks, counted `owed_durable_spender_reask_{asked,mined_by_proof,still_unconfirmed,fault}_total`; the index-hop-row arm gets no memo upgrade (it confirms from the index's own word and reads no chain memo, the #692 lens's LOW-1, stated at the site). Since the #693 fold (the W1-OWED-693 lens's HIGH-1) a checked mined proof of a spender is LATCHED ONCE CHECKED, SERVED FROM THE LATCH ON EVERY LATER PASS: phase 2's answer or 2b's re-ask writes `proofmined:<spenderTxid>.<height>` into `hop_chain_probes` in the snapshot's atomic batch (the height and the canonical root, never bytes; `owed::proof_latch_memo`, only on `proof_mined_word` and a root, at most `OWED_PROOF_LATCH_WRITES_PER_RECOMPUTE` a pass), read byte-free each pass before the pass's own answers (`owed::latched_proof_mined_spenders`, seeding `proof_mined_spenders`; a latched spender is never re-asked), so a row confirmed on pass N reads confirmed whatever a later pass's slots, budget or re-ask order do. A reorg demotes it: the overlay's reorg clear takes it (a chain word, not spared), its TTL sweep spares it (`HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES`), and a latch whose root is no longer chaintracks' root at its height is not honoured. The latch's lifecycle since the #693 fold-2 (the W1-OWED-693 re-lens's R-HIGH-1): written only from a checked proof in the snapshot batch; rechecked newest-first within the reorg window (`owed::LATCH_RECHECK_DEPTH`, 6 blocks below the pass's tip, at least the 4 newest; deeper is reorg-proof); honoured unrechecked (a faulted header, `over_budget`) only past the grace window with a clean clear history (written at or after the last clear's tombstone + `PROBE_MEMO_REORG_GRACE_MS`, the tombstone read in the latch read); re-asked after two hours unrechecked (`PROOF_LATCH_UNRECHECKED_MAX_AGE_MS`) unless reorg-proof; deleted by the reorg clear, and (since the #693 fold-3, the delta lens's R2-HIGH-1 (a)) deleted by the first recheck that finds its root moved (`owed::moved_proof_latches`, `PROOF_LATCH_MOVED_DELETE_SQL`: one statement in the snapshot batch, none when nothing moved, a row taken only while it holds the refused root), so that refusal is durable and the spender is re-asked; a later checked proof latches its new height. Stated (R2-HIGH-1 (b)): a latch whose root a reorg moved is honoured again only when NO clear recorded the reorg (the clear's tombstone write AND its delete both faulted, or NO detector saw it: neither the overlay's 3-deep tip sweep, nor a chaintracks `reorgFrom`, nor an applied Arcade orphan event) AND no pass rechecked the latch while it was inside the window (an identity with no owed pass in the ~6 blocks after the reorg, or every such pass's header read for it faulted or was `over_budget`): once 6 blocks deep it is reorg-proof and is not re-read, so it is honoured by depth until a later recheck (it is among the 4 newest) or a clear; a single recheck that sees the moved root ends it for good. Display only either way: the latch feeds courier-bytes own-sweep rows, which stay `claimable:false`. Pin `e693f3_h1`. Until the overlay's sweep change is deployed a latch older than two hours is swept and re-latched by the next checked proof. Pins `e693f_h1_a` / `b` / `c`. `/spent-any` (#695, DESIGN-641 option (b)): when every primary rung faulted after WoC's `/spent` answered 400 (its word for an unknown tx), the ladder reads WoC's `/tx/hash/{txid}` once, and its conclusive 404 (the subject unmined, one capable source, the pruned Bitails unable to corroborate) answers `known:false, reason:"uncorroborated-unspent"`, never `provider-fault`, which stays for a 429, a 5xx, a transport or a body-shape fault (`results::sharpen_unmined_absence`; every other body byte-identical, pinned `e695_a`, `e695_b`). Stated (the W1-OWED-693 lens's LOW-1): `uncorroborated-unspent` now names TWO evidence shapes, (1) WoC answered 404/410 for the OUTPOINT and no second source corroborated (as before), and (2) every primary rung faulted and the TX is unknown to WoC's node (its `/tx/hash` 404); the word asserts no absence (`known:false`, no `terminal`, no memo), so it cannot fire the monitor's Void arm (which reads both reasons as `Blind`). The client's `hopSpenderRead` (bsv-low `app/src/lib/stake.ts`) reads this reason as `looked: true`, so a hop whose funding tx WoC never saw now reads "the network does not know it yet" where it read "could not be asked"; its one reader of `looked` (`orchestrator.ts`) feeds `rejoinSourceDecision` (`joinEvidence.ts`), which answers `blocked` either way, so no money decision changes. The lens's additive `basis: "subject-unknown"` is not built: a future reader that would act on "unspent" must not act on this reason alone. `/spent-any` `spentConfirmed` (#723): a KNOWN spend's confirmation rests on a checked proof of its VERIFIED spender where the app layer holds one, strongest first: the index's verified proof (`transactions.has_proof`, one byte-free PK read of the row's spenders, `hopsweep::sweep_proofs_sql`, none when no row is a known spend; a fault is no proof), the tracker's checked mined word (`owed::checked_tracker_mined` over the views the route already builds), an isolate-held chaintracks-bound `/tx-any` proof (`txany::proof_mined_word`); only when none is held does WoC `/0/spent`'s status speak, and when WoC faulted and a fallback rung named the spender (BananaBlocks, Bitails, which cannot say mined) the word is `null`, never `false` (`results::spent_confirmed_unknown_after_fault`, `results::spent_confirmed_word`). The source rides each known spend as the additive `spentConfirmedSource` (`index-proof`, `tracker-mined`, `txany-proof`, `courier`, or `null`); every other row's bytes are unchanged, and the probe memo keeps the ladder's own word. Readers audited: every money reader reads `spentConfirmed == true` (the monitor's `spender_is_network_witnessed`, the watchtower's `chain_oracle`, the client's `chainReads.ts` `decodeSpentFold` keeps `null` as unknown, `stake.ts` and `moneyTruth.ts` `=== true`), so `null` fails closed as `false` did; the resync diff harness (bsv-low `scripts/resync-diff.mjs`) compares both fields exactly and must exclude `spentConfirmedSource` and decide whether `null` against `true` is a diff. Pins `e723_a` / `b` / `c`.

The served words carry the host's SPEND DEPTH (bsv-low #862, L7-DEPTH; the L6 app-layer half of #622): `spendDepth`, an integer, the confirmations this host requires before a wallet credits a coin on a served word (R3 of the wallet charter), rides beside `tracker` on `/tx-any` (the subject and every `answers[id]`) and beside `lock` on every `/utxo-status` and `/spent-any` entry (`runtime::stamp_spend_depth`, after the words, at `body_loaded`, `outpoint_body` and the `/spent-any` route), and `/health.spendDepth` serves the read {`depth`, `source` `var`|`floor`, `floor`, `var`}. It is read from the render var `SPEND_DEPTH` (`"1"` in prod and beta of `crates/low-app-layer/wrangler.toml`; the value at risk is the owner's later word); 1 is the floor, served for a missing or malformed var (anything but decimal digits from 1 to `SPEND_DEPTH_MAX` 144, the bound shared with the client's `SERVED_SPEND_DEPTH_MAX`, which reads a value past 144 as not served; the L7-DEPTH lens's MEDIUM-1, lane L7-DEPTH-FOLD: a var past 144 was served as set while the client credited at 1), which counts `spend_depth_var_malformed_total` once per isolate (seeded by the overlay, so `/health/invariants` serves it from 0; the depth itself is the app layer's var and is read on its `/health`, the overlay holds none); an object the word decoration did not reach (a word-store fault's legacy body) gains nothing, so a decorated body is its old shape plus exactly one key. The client reads it in bsv-low's `app/src/lib/creditWord.ts` `readLanding` (L7-TOWER) and runs `CREDIT_DEPTH_FLOOR = 1` where it is absent. Pins `--test spend_depth_l7` (eight, each RED against its rule grafted inert: the stamp, the var read, the once-per-isolate count, the parse, the wiring, the health read, the renders, the seed); the fold's `l7f_m1_*`: 145, 1000 and `u32::MAX` the floor, `source: "floor"`, counted once; 144 and 1 served and never counted; RED on 6546ba67 with the constant grafted alone: `SPEND_DEPTH="145"`, `left: (145, true) right: (1, false)`), and `--test spend_depth_l7_lens` (the lens's seven, its MEDIUM-1 pin inverted to the floor and its `u32::MAX` row the floor). A depth served on a `/tx-any` word-store fault is still absent (the legacy body); the client's session rule (bsv-low `creditWord.ts` `servedDepthOf`: the largest depth any answer served wins, and once one was served a read with none is `pending`) covers it, and the outpoint rows the same credit reads carry the depth (the lens's LOW-1, judged, no server change).

A refund-due row is a press only where the pot's FUNDING is held (bsv-low #831, loop 43 pair 17: a JOIN admitted pending, never SEEN-latched, never mined, never evicted, served `present-refund`, and its pre-signed refund met `SEEN_IN_ORPHAN_MEMPOOL`). The owed recompute's paired hydration (`owed_cursor::hydrate_party_pair_sql`, the statement it already runs over the chosen pots) carries `fundingHeld` for an UNSPENT pot: `seen` (the pot txid's own `network_seen` latch), else `proof` (`pot_beefs.proof_verified`), else `none`; NULL for a spent pot (`FUNDING_HELD_COLUMN`, each probe a primary-key search gated by the CASE, the proof probe only without the latch). A `none` pot's funding txid joins the coin-word read of step 3b, and the judgement (`owed::derive_owed_rows`, `OwedInputs::funding_unheld`) serves such a refund-due pot as `unbound` with `claim: "funding-not-held"`, `claimable: false` and the reason `FUNDING_NOT_HELD_REASON`, unless the tracker's CHECKED `mined` word holds the funding; every other pot keeps its row byte for byte. The judgement accepts the overlay's own SEEN latch, the index's verified proof or a checked mined tracker word, not a tracker `seen` hint (the FIX-831 lens's L5, the safer reading than the issue's "a `seen` funding keeps today's judgement"): a tracker hint is no witness the network holds the output, and past the recovery gate an honest funding is mined, which the checked `mined` word honours. A word-store fault withholds for that pass (latch or proof only); each pot judged so bumps `owed_refund_funding_unheld_total` (seeded at 0 by the overlay), so the class withheld for good (a mined funding the index never verified, with no latch and no `tx_words` row) is visible. Extra reads per identity: one `network_seen` probe per unspent chosen pot, one `pot_beefs` probe per unlatched one, one `tx_words` key per unheld one; none for a spent pot. Pins `--lib fix831` (RED with the input grafted inert: `left: RefundDue right: Unbound`) and `--test d1_fleet_plans fix831` (the column's four values and the two key searches). Not built here (half 2, the overlay): the retire pass marks such a pot's BEEF `structurally_unprovable` and never evicts it, so its hops are not released. Since B-OWED-2 (bsv-low #598, the lens's HIGH-1 folded): a latched (`fundingHeld: seen`) funding the network refused takes the same judgement, served `fundingHeld: refused` with `fundingWord`, read from the one producer this tip has: the word event of the pot's own txid (`callback`, Arcade's push; `tick`, the re-ask) that read Arcade's REJECTED / INVALID / MALFORMED or a double spend marks its `tx_words` row (`runtime::mark_terminal`: `trackerTerminal`, `trackerTerminalWord`), which `stored::Row::view` serves as the additive `terminalHint` (absent on every unmarked row) on any word but `mined`, and `owed::funding_refusal_word` reads beside a CHAIN-tier `rejected` / `conflicted` (no producer feeds that today; the tier is read). The tracker keeps the word (NS-3): the mark is the evidence. The fail direction: a funding Arcade refused that the network still holds keeps the mark until a checked proof (a later non-terminal hint does not lift it, and `DUE` no longer refreshes the row), so its refund is withheld until the pot's block, whose `mined` word clears the mark and whose checked `mined` word rescues first in the arm; the refund is not pressable before that block. Pins `--lib fix598f` (the word as the producer stores it, through the shipped UPSERT under real SQLite, then the derivation; RED on bca99893's reading: `left: (RefundDue, "present-refund", true)`). (bsv-low #492) a payout whose home output the index holds spent is no row (`retire_index_spent_homes`, after the shed carry; one PK read per 45 such rows). (bsv-low #608) a could-not-judge row resting on no spender bytes (`spenderUnread`) is re-judged on the next read past a minute, except a spender the pass's own bound withheld (the courier cap, the clock, the stored-read tail: `unmark_withheld_spenders`, the lens's LOW-1), which waits for the open list's 5 minutes; a spender looked at with no bytes, or a faulted pot-spenders read, keeps the minute, bounded by #499's ceiling and #800's hand-off; each read that recomputes for that rule alone counts `owed_spender_rejudge_total` (seeded by the overlay). Pin `--lib fix608f`.

The paged leaderboard (bsv-low #642): `/leaderboard:view` ranks owners over ONE candidate read of the last `LEADERBOARD_CANDIDATE_WINS` (2,000) counted wins (`logic::chain_wins_candidates_sql`, each verdict arm walked backwards on `idx_pot_records_chain_wins` and stopped at the cap), pages by the keyset cursor `nextCursor` / `?cursor=<wins>.<owner>` (the deployed client's `?after` rank offset stays, resolved over the same window), and serves `candidates` with `candidatesBounded: true` only when the era holds more, so a body inside the bound is byte-identical to the whole-board read (pinned through the shipped pipeline over rusqlite, `routes::leaderboard_642`). Its rows-read ceilings run on local D1 in `make ci-d1-board` (28,403 rows at page 1 of a 1,500-win board where the whole-board read took 76,577; flat past the cap), and the write-time table that removes the bound is #674 (`docs/DESIGN-674-LEADERBOARD-WRITE-TIME.md`).

## Derived lookup tables are the write's

The owed list's source statements mark `(identity, gameId, source, sourceKey)` in `owed_dirty` in their own transactions (`derived_owed.rs`): pot and party changes, hop markers, refund latches, result and collected filings, hop sweeps, eviction and readmission, network witnesses, tracker words, BEEF proof latches, refusal inserts/updates/deletes, released-hop evidence and spender bytes. Each mark carries an AUTOINCREMENT `generation`; a conflict advances it to the attempted insert's fresh generation and keeps the newest source, source key and time, so a judge reading `max(generation)` and deleting `generation <= seen` leaves a concurrent re-mark pending even after the queue has been emptied. The mark lookups use outpoint and txid indexes, including the indexed source-hop lookup when a pot arrives before its derived links. `owed_rows` carries nullable judgement, claim, gate, belt and probe-tip keys; `owed_row_words` supplies the word joins, and an orphan trigger follows every old-worker row delete. `released_hop_evidence` and `owed_belt_state` hold the eviction evidence and named cursors; the app-layer judge and scheduled backfill own the row derivation and completion stamp. Shipped trigger text is immutable: append `DROP TRIGGER IF EXISTS` plus `CREATE` to the migration list for a change, never edit it in place. The real-writer drift pin removes each dirty trigger alone, the prepared-program pin bounds source-statement cost on the fleet, and the hot-route plans pin the new lookup indexes.

The app layer's `owed_judge.rs` owns the per-game owed judgement: `judge_subjects` hydrates only the selected games, chooses a pot's representative by validity and oldest marker, and commits the rows, word references, state and generation-bounded acknowledgement in one counted batch before pushing `owed-changed`. `judge_with_write` puts the source statements first in that same batch after the verified filing is applied to the hydrated inputs; `refund_subjects` includes both seats. Each invocation judges at most 16 subjects, `drain_dirty` prefers wanted marks, and `mark_wanted` writes one envelope without reading or judging. The 500-row stored ceiling prefers actionable rows and records overflow. The hydration requires `idx_potparty_game_identity` and `idx_evicted_party_identity_game`; its native pins use the same SQL and transaction as D1, and every owed input fixture checks decomposition and storage read-back against the unchanged pure derivation.

`GET /owed` reads `owed_read::{ROWS_SQL, STATE_SQL, PENDING_SQL}` in one counted batch: the identity's stored rows joined to their word references and `tx_words`, its marker, and its dirty count. The reader rebuilds words through the existing header recheck, preserves stored claims, serves `pending`, keeps `markerlessProgress: null` for the compatibility release and serves `beltNextAtMs` from the row's stored `beltDueAtMs` (null where the index or a checked word decides the row; lane D2-OWED-BELT), and degrades each claimable `hop-stranded` row IN THE SERVED BODY by #669's gate (`degrade_stale_sweeps`) against the tip the row was judged at (`probeTip`; NULL is unknown) and the present tip (unreadable is unknown): a block since, or no tip, is a pending chain-wait, never a dead sweep (the D2-OWED lens's HIGH-1). A missing marker permits only the indexed identity probe and one clock-free `mark_wanted`; an unknown identity writes nothing. The route cannot schedule work, and its shared accounting waits for a later request or scheduled pass. The native fleet-plan and body pins use the production SQL; `OWED_READ_ONLY=1 make ci-d1-budget` drives the unchanged 250-result, 270-hop fixture, judged by lane 2 into 500 stored rows on local D1 with external output selected by `OWED_READ_OUT`.

The leaderboard derived at the write (bsv-low #674, the #638 doctrine): `derived_board.rs` (migrations 296-327, the cutoff's 331-334) keeps `board_wins` (one row per counted win, `chain_wins_cte`'s predicate verbatim, the owners query's columns), `board_owners` (`wins`, `keyWins` per owner; `identityIsKey` is `keyWins > 0`), `board_hands` (each identity-owned win's earliest card-bearing verified claim, by `markerRowid`) and `board_total`, by 21 triggers: a write to `pot_records`, `network_seen` or `potparty_records` RECOMPUTES its pot's `board_wins` row from the source (delete, insert when the predicate holds), so the eviction's shadow move (a DELETE) and the readmission (an INSERT) are these triggers and a REPLACE of a pot still leaves the right row; `board_owners`, `board_total` and `board_hands` follow `board_wins` by its own triggers; a `result_markers_v2` write recomputes its pot's hand. A trigger's text is immutable once shipped (a change is a `DROP TRIGGER IF EXISTS` + `CREATE` pair appended), no production writer REPLACEs a source row (pinned), and every trigger text must survive wrangler 4.54's `--file` splitter, which pushes a frame on whitespace-`CASE` and pops on whitespace-`END` (a `,CASE` closed the trigger early): write `, CASE` (pinned under 4.54's model and `sqlite3_complete`, 4.148's). The one-time backfill (`WINS_BACKFILL`, guarded in its OUTER derived table, then the `board-v1` stamp) fills owners, total and hands through the same triggers; a stamped replay is 35 VM steps. `/leaderboard:view` (`routes::leaderboard_page_body`) reads the tables when the stamp exists and the tables are derived under the route's own era cutoff (`BOARD_READY_SQL`, which also reads `board_total` and the `board_era` stamp; an unstamped or unmigrated database, or a stamp that is not the route's cutoff, reads #642's candidates, never a 503 for it): the owner page by `idx_board_owners_rank` (offset, or the keyset cursor in two index arms with its rank counted for `nextAfter`), the page owners' wins by `idx_board_wins_owner`, the hands by `idx_board_hands_rank`, then #642's steps 3 to 6 unchanged; the body is #642's and the whole-board read's byte for byte (the #685 decoration included) with `candidatesBounded: false` and `candidates` every counted win. THE PAGE READ IS FLAT (the #674 lens fold, MEDIUM-1; as first built it read each page owner's `board_wins` rows and then every one of those pots' statuses, classification, markers and proofs, about 12 reads a page-owner win, 11,801 at 1,500 wins and 22,586 at 3,000): each `board_wins` row carries `frag` (the rows the page tail reads for that win: its status and decoded `pot_records` columns, its first `RESULT_ROWS_PER_POT` markers in `pot_markers_sql`'s order, each marker's proof pointers and posted bundle) and each `board_owners` row `card` (its wins' rows and frags by pot txid) and `packed`, kept by the triggers (`WIN_FRAG`; a result marker, `proof_markers` (`PROOF_*`), `proof_posts` (`POST_*`) or frag-only pot column (`POT_FRAG`) write refreshes the frags it names); the page reads the owner page, one card row per page owner (`logic::board_cards_sql`, `routes::leaderboard_table_body`), the hands and the hands' off-page pots, and steps 3 to 6 take the held rows from the cards (`routes::PagePrefetch`), so the fold is unchanged. Measured on local D1 (`make ci-d1-board`): the default page 323 rows at 1,500 AND at 3,000 wins (asserted equal), limit 200 978 / 1,003, the cursor page 485 / 467, largest statement 400; ceilings 728 / 1,505 / 600 (#642's read: 28,403 and 39,049). THE TRIGGERS' COST A STATEMENT (lane W1-LB-674-NOMEM): an update of `pot_records` or `potparty_records` recomputes the OLD pot only when the row moved to another pot (`recompute_pot!`'s guard; recomputing the same pot as OLD and NEW rewrote a packed win's card four times, 5,991,694 bytes of JSON at the bound, now two, 2,995,847), and the pin `lb674_nomem_the_triggers_prepare_and_write_within_their_bound` (`--test board_derived_sqlite`) bounds, over a card at `CARD_MAX_BYTES`, each source write's prepared program (256 KiB; measured 109,040 for a `network_seen` insert, 207,184 for a `pot_records` update) and its writes (`2 * CARD_MAX_BYTES` card bytes rewritten, about 745 WAL pages; a write that moves no packed win 3 to 15 pages). The route tiers load their seeds 2,000 statements a wrangler call (`scripts/d1-load-chunked.py`): Miniflare's local D1 holds every statement's compiled program, its triggers' programs inside it, until the isolate's GC, so the 102,473-statement fleet seed in one workerd died `SQLITE_NOMEM` at about 1.1 GB with the board's triggers (whole file, 10,000-line batches, and the NS2 cell's batches of 100 alike) and loads at 715 MB peak in 151 s; the NS2 cell opens a copy of the loaded `v3` (`D1_BUDGET_SEED_V3`) instead of loading it again. The fleet seed holds no counted win: its bytes on disk are the same with and without the board (44.36 against 44.22 MB). Stated: an owner whose card does not hold every win (a card past 1,500,000 bytes, about 600 wins of the fixture's size, or a frag past 200,000, or a pot txid not lowercase hex) is read by its wins as before (about 12 reads a win, for that owner alone); a counted win's insert and a marker, proof pointer or posted bundle write now build frags and cards (the `ci-d1-budget` write ceilings are owed a re-run). THE CUTOFF INSIDE THE DERIVATION (lane W2-LB-674-CUTOFF, 2026-10-18): beta and prod both set `WRITTEN_OFF_BEFORE_MS` (the #375 write-off), and as first shipped the route read the tables only with no cutoff, so on every deployment the triggers wrote a board nothing read (beta: 139 owners and 3,698 wins derived, 37 owners served by the candidate read at 34,077 rows a page). `board_era` (one row, `cutoffMs`, NULL for none) is the cutoff the board is derived under; `board_win_era` (BEFORE INSERT ON `board_wins`, `RAISE(IGNORE)`) refuses a win whose `potCreatedAt * 1000` is before it (the era filter's predicate; a NULL stamp is outside the era), so every shipped trigger that derives a win counts only the era's and none of their texts changed (the 32 statements' fingerprint is pinned; an old worker's re-run of its own list is a no-op); `board_era_update` (AFTER UPDATE OF `cutoffMs`, on a change) re-derives the whole board in the statement that moves the stamp (the owners cleared first so the per-win delete rewrites no card, then every counted win through `board_wins`' triggers). The overlay holds no cutoff: the app layer's scheduled pass (`routes::sync_board_era_env`, before the tracker's backfill) stamps its own clamped cutoff (`BOARD_ERA_SET_SQL` / `BOARD_ERA_CLEAR_SQL`, a no-op when it holds) and the route serves the tables only when the stamp equals its cutoff, so a moved var reads the candidates until the pass re-derives, never another era's tables. Measured: the default page under a cutoff 304 and 303 rows on local D1 (#642's 17,851 and 35,450); uncut 325 (the ready read's two `board_era` rows). Pins: `--lib lb674_cut` in the app layer (the table read under the cutoff is the candidate read's and the whole-board read's body at every page shape and through the cursor walk, the moved cutoff, past the candidate bound; RED with the main dispatcher and with `board_win_era` removed alone; `lb674_cut_beta_rows_*`, ignored, on a beta snapshot's rows under `LB674_BETA_ROWS`), `--test board_derived_sqlite lb674_cut` (the drift pin under the stamped cutoff after every hook path and every move, each of the 23 triggers removed alone RED, the deploy then the stamp, the re-runs benign), `--lib lb674_cut` in the overlay (the block appended, the shipped block untouched). Pins: `--test board_derived_sqlite` (the drift pin over every hook path, each frag equal to the app layer's own page statements run from scratch and each card to its owner's wins, the settle, tie, refund, witness and claims through the overlay's own writers (LOW-2), each of the 21 triggers removed alone RED; `lb674_fold_l1_*`: `HAND_SCORE_SQL` equal to `sql_hand_score_expr`; the backfill, the deploy window, the stamped replay), `--lib lb674` in both crates (`lb674_fold_m1_the_page_read_is_flat_in_the_page_owners_wins`: 8 statements and 203 rows at both boards, RED on `eaf122e`), `--test d1_fleet_plans the_board`.

## The owed belt asks the chain explorers (bsv-low #893 section 5, lane D2-OWED-BELT, 2026-10-25)

The explorers are break glass only: ONE place asks them for the owed list, `crates/low-app-layer/src/owed_belt.rs`, never a read. A `BOARD_VIEW` Durable Object named `owed_belt::OWED_BELT_ACTOR` (one, fleet-wide) runs `owed_belt::tick` from its alarm every `OWED_BELT_TICK_MS` (120 s); the alarm sets its next alarm BEFORE the tick runs, and the scheduled pass's owed task asks the actor on every cron (`owed_belt::arm`, phase `owed.belt_arm`, the watchdog: a lost alarm costs at most one 15 minute cron). The deferred owed walk's alarm is retired: the actor's `/owed-deferred` and `/owed-deferred-note` answer 410, and any other actor's alarm clears its storage and ends; with it the old walk's three probe-memo writes outside the snapshot batch are unreachable (their code is lane 6's to delete). `POST /internal/owed-belt-tick` (bearer `INTERNAL_TOKEN`) runs one tick on demand under the request's D1 ledger.

A tick takes its candidates round-robin by identity (the D2-OWED lens's HIGH-3, `owed_belt::due_candidates`): up to `OWED_BELT_IDENTITIES_PER_TICK` (64) identities holding a dated row walked past the cursor `owed_belt_state` `belt:identities` (`BELT_IDENTITIES_SQL`, wrapping), each one's oldest due rows in one batch (`BELT_DUE_SQL`, the covering partial index `idx_owed_rows_belt_turn` on (identity, beltDueAtMs, outpoint)), each identity's oldest before a second of any, the chosen rows read by key (`CANDIDATES_SQL`): bounded by the walk, never the due set, dated by the judge (`owed_judge::belt_due`: only a row whose evidence verifies, a hop row's marker or a pot row's chosen party `sigValid`; `hop-stranded` now + 600 s, an `unbound` row naming a spender whose bytes are NOT held now + 300 s, a `courier-bytes` payout now + 600 s, a young `in-progress` hop at its marker + 30 min; NULL otherwise), and a find whose row reads as before the look doubles the row's period to `OWED_BELT_BACKOFF_MAX_MS` (6 h; the judge's own dating restarts it). A tip dates the claimable sweeps judged below it due now (`owed_judge::DATE_SWEEPS_SQL`). Each candidate's ask (`owed_belt::ask_of`): a hop or pot outpoint through the spent-any ladder (`routes::belt_spent_any_via`), an `unbound` row's named spender through the tx-any courier leg for its hash-verified raw and, at a claimed height, WoC's `/proof/tsc` bound by `txany::mined_proof_matches` to the canonical root (a `proofmined:` latch), a `courier-bytes` payout's first unlatched home output through the ladder and `owed::home_word` (the home key's signature; a `homeproof:` latch). Every courier call passes ONE `owed_belt::Gate`: at most 3 an outpoint, 24 a tick, the three-fault circuit per rung (`woc`, `bitails`, `bananablocks`, `other`) per tick, a 429 closing its rung's turn at once, never the tick; a refused ask is no call and reads as that rung's fault. A FIND is ONE batch, `owed_judge::judge_with_write` with the find's statements first (the probe memo with the tip it was taken at, `PROBE_MEMO_UPSERT_SQL`; the spender's bytes, `OWED_SPENDER_BYTES_UPSERT_SQL`, DO NOTHING; the latch) and the same facts applied to the hydrated inputs, then the judge's rows, words, state and dirty acknowledgement for that subject: a fault rolls all of it back. A candidate no rung answered (a 429 on every rung, a spender whose raw no courier serves) keeps its row and facts and is re-armed ONCE at now + 300 s (`REARM_SQL` sets, never adds). Counted: `owed_belt_ticks_total`, `owed_belt_asked_total`, `owed_belt_found_total`, `owed_belt_faults_<rung>_total`, and the backlog gauge `owed_belt_due_total` (set each tick over the partial index), seeded by the overlay (`applayer_counters::OWED_BELT`, pinned equal to `owed_belt::counter_names`) and served as `/health/invariants.owedBelt` {`ticks`, `asked`, `found`, `due`, `faults`, `bounds`}. The overlay's hourly `discover_missing_spends` is unchanged.

A tick with NO chain tip asks nothing (`TickReport::no_tip`, served `noTip` by the lever, counted as a tick): the judge refuses every subject without one ("chain tip unavailable; previous judgement stands"), so a find could not land and its calls would be spent for nothing; every date stays and the next tick with a tip asks them first (pin `owed893_belt_a_tick_without_a_chain_tip_asks_nothing_and_moves_no_date`, RED with the guard inert: 16 calls, 8 write faults; the tier found it: before the stand-in tip all 8 finds a tick faulted while its asserts passed, so the tier now asserts `found == asked` and `writeFaults == 0`). Measured on local D1 (the tier, CHAINTRACKS a stand-in worker in the same dev session at the seed's tip 900000): a tick of 8 finds 17,223 rows read, 162 written, 270 statements, the largest 1,001, 16 calls; the next 17,205; a 429 tick 709 read, 3 calls (`ceilings.json` `owed-belt-tick`, `owed-belt-tick-429`, 1.5x). Stated: the rows are the JUDGE's commit over a 500-row identity (profiled natively over the tier's database: the 500-row cap DELETE, the per-game DELETE and the `owed_state` COUNT, each O(the identity's rows), about 2,100 a find; no full scan), so 8 finds a tick on such identities is about 12.4 M rows a day, past half of prod's 20 M budget: the judge's per-subject commit is the cost to bound (lane 2's statements, not changed here). For the harness lane (D2-OWED-HARNESS), the numbers `owedCourierRetire` reads under the belt: `owed_belt_due_total` on `/health/invariants.owedBelt.due`; 8 outpoints a tick every 120 s, so the three payout rows within 10 minutes of the sweeps' broadcast when `due` is under 32, else within `due / 8` ticks; an unanswered candidate re-armed at now + 300 s; the 6 block stall stays the outer red. Stated: a payout's ask hydrates its game once before the judge hydrates it again (the home outputs are the derivation's); two finds of one game in one tick are two judge batches; the tick's state row and gauge are one batch after the candidates, best effort. Pins `--test owed_belt_sqlite` (64 due hop rows through the shipped drain, a counting courier fake: one tick asks 8 by due time in at most 24 calls and the next tick the next 8; a 429 closes its rung after one call and three 503s close another, each candidate re-armed once at now + 300 s; a faulted row insert leaves no memo and no bytes, the next find writes memo, bytes and rows in one batch; a spender whose raw no courier serves spends exactly 24 calls and changes no fact; the candidate plan; the counters seeded and served), RED against five revert mutants, each run (the memo in its own batch: `left: 1 right: 0`, the memo left by a faulted row insert; no per-outpoint cap: `left: (0, 6) right: (0, 8)`, two candidates starved; no rung circuit: `left: 8 right: 1`, BananaBlocks asked after its 429; a re-arm that adds to the old date: `left: Some(1790000200000) right: Some(1790000300000)`; no bound at all, the outpoint's 3 and the tick's 24: `three an outpoint, 24 a tick, left: 32 right: 24`; the tick's 24 alone is a belt, equivalent under 8 outpoints x 3 calls, stated), and `--lib owed893_belt` (the alarm re-arms before its tick, the cron's arm, one actor). The module does not exist on `c055a052`. The tier: `OWED_BELT_ONLY=1 make ci-d1-budget` (`tools/lane-893/owed_belt_tick.sh`) over the READ lane's fleet seed with its dates an hour back, two ticks and a 429 tick against a counting stand-in (`ceilings.json` `owed-belt-tick`, `owed-belt-tick-429`).


## The owed backfill runs once behind a stamp, on the scheduled pass (bsv-low #893 section 6.1, lane D2-OWED-BACKFILL, 2026-10-25)

`crates/low-app-layer/src/owed_backfill.rs`. The one-time `owed-v2` derivation is a FIFTH detached task of the app layer's `scheduled` (`pass_meter::SCHEDULED_TASKS` 5, phase `owed.backfill`), handed after the tracker's, under its own wall (`OWED_BACKFILL_WALL_MS` 20 s, `OWED_BACKFILL_SAVE_RESERVE_MS` 3 s for the cursor's save); never a read's. `POST /internal/owed-backfill-pass` (bearer `INTERNAL_TOKEN`) runs one pass of the same function (the tier's lever).

A pass (`backfill_pass`): (1) `STAMP_SQL`, the `view_derivation_state` row `owed-v2`: present, the pass ends there, ONE primary-key read of one row (pinned on the populated fixture at zero full-scan steps and zero party rows). (2) `GATE_SQL`: the overlay's certified migration count (`ops_counters.overlay_migration_count`) must be at least `OWED_TRIGGERS_MIGRATION_COUNT` (407 since lane D2-OWED-FOLD, the list through the #893 block and the fold's indexes and row-count triggers; append-only, pinned against `OVERLAY_MIGRATIONS` because the app layer does not link the overlay), so the triggers exist before the rows do; and the cursor row `owed_belt_state` `backfill:owed-v2`: absent, the pass only ARMS it (cursor `''`) and the work begins one cron tick after the new version's first pass (risk 8.4). (3) `IDENTITIES_SQL`: 128 identities past the cursor, distinct and ascending, each side on `idx_potparty_identity` / `idx_hopparty_identity`. (4) Each identity: its games (the wanted expansion's union: party, hop, released-hop evidence, its own `owed_rows`) marked `source = 'backfill'` (`MARK_SQL`), the `open` row `backfill:owed-v2:open` naming it and its `owed_state.truncated` reset (the judge ORs it back), in one batch; then `GAMES_SQL` 16 marks at a time, each batch `owed_judge::judge_subjects` (the shipped judge, its per-game delete, its 500 ceiling, its generation-bounded acknowledgement); then the cursor saved (`CURSOR_SQL`), the stamp (`STAMP_WRITE_SQL`) in the same batch once the page was short and this was its last identity. Counted `owed_backfill_identities_total`, `owed_backfill_rows_total` (the identity's stored rows when it finished), seeded by the overlay (`applayer_counters::OWED_BACKFILL`). An identity the wall cuts keeps its marks (the drain may judge them too) and its `open` row: the next pass resumes its marks and never re-marks it, so an identity bigger than a pass converges. A legacy row (an old worker's keyless whole-identity write) is rewritten with its keys, and one whose game no source names is judged away.

Risk 8.2's third repair: the owed drain first runs `owed_backfill::mark_aged` (`AGE_MARK_SQL`): the subjects of rows judged more than 24 h ago (`OWED_AGE_REMARK_AFTER_MS`), oldest first on `idx_owed_rows_judged` (overlay migration 396, partial on `judgedAtMs IS NOT NULL`), `source = 'age'`, never more than 128 PENDING at once (a subject already holding an age mark is skipped, and the insert takes only the room left under 128), (since the D2-OWED lens's MEDIUM-5 its first statement, `AGE_MARK_UNJUDGED_SQL`, takes the keyless rows, `judgedAtMs IS NULL`, over `idx_owed_rows_unjudged`, in the same 128) and the drain (`drain_dirty`) judges a subject marked only `age` after every other subject, so the repair fills the drain's idle room (its existing 64 a pass) and never delays a source's mark; the judge restamps the row, so each row is re-judged about once a day while the drain has room. Counted `owed_age_remarked_total`.

Risk 8.4's window rule: the belt's `CANDIDATES_SQL` reads `COALESCE(claimable, facts.claimable)` (`Candidate::claimable`), so a row an old worker wrote with a NULL column is read by its facts. Ruling 1's end: a backfilled identity has its `owed_state` row, so its read is the three statements and writes nothing; `mark_wanted` stays for an identity no backfill reached (a later `owed-v3` reuses it).

Pins `--test owed_backfill_sqlite` (`owed893_backfill_a` to `i`, the shipped pass on real SQLite under the overlay's migrations; each RED against its rule reverted alone: the stamp read removed `left: Armed`; no arming tick `left: Finished`; the gate's count ignored `left: Armed`; the cursor saved once a page `left: 1`; the open identity re-marked `left: (Finished, 2, 4)`; the age mark's generation bumped `left: 128`; the age mark unbounded `left: 150`; the pending bound dropped `left: 22`; age marks not drained last `left: 1`; the candidate without the COALESCE `left: [None, None, Some(0), None]`; a batch of every game `left: [41, 25, 9, 0]`) and `--lib owed893_backfill` (the wiring). The tier: `OWED_BACKFILL_ONLY=1 make ci-d1-budget` (`tools/lane-893/owed_backfill_pass.sh`, the READ lane's seed turned back into an old worker's rows, `ceilings.json` `owed-backfill-pass`, `owed-backfill-stamped`). Measured on local D1: the arming pass 3 statements, 1 row read; the backfill of the lived-in identity (270 games, 17 batches) and the one-game identity in one pass, 53,424 rows read, 5,575 written, 3,062 statements, the largest statement 1,291, about 2,970 rows a batch; the stamped replay 1 statement, 1 row. Stated for the lens: the rows a batch are the judge's commit over a 500-row identity (the 500-row cap DELETE, the per-game DELETE on the identity prefix and the `owed_state` COUNT, each O(the identity's rows)) and its hydration, so a whale's one-time backfill is about 3,000 rows per 16 games; not bounded here (the judge's statements are lane 2's; an `(identity, gameId)` index and a maintained count would bound them), and the age repair spends only the drain's existing per-pass room.

## The money lens's folds (D2-OWED-FOLD, 2026-10-25)

bsv-low `docs/audit/D2-OWED-LENS-2026-10-25.md` (NOT SOUND: 3 HIGH, 5 MEDIUM, 5 LOW, 5 NOTE), folded on this branch after its rebase onto main `8e5e73f7` (PR 171's #663 and #649, PR 172's tipless judgement and end-of-derive `degrade_stale_sweeps`, both kept). Pins in `crates/low-app-layer/tests/d2_owed_fold.rs` unless named; each RED on the base rule, quoted in its commit.

- **HIGH-1, #669 at the read.** The judge stores `probeTip` = the tip it judged at (NULL: no tip); `owed_read::Snapshot::body` runs `degrade_stale_sweeps` per claimable sweep against it and the present tip, so a block since or an unreadable tip serves a pending chain-wait. A tip dates the claimable sweeps judged below it due now (`DATE_SWEEPS_SQL`, partial index `idx_owed_rows_sweep`) and re-marks rows judged with no tip (`MARK_UNTIPPED_SQL`, `idx_owed_rows_untipped`, 128 a tip). Every read with a row now reads the present tip (the 30 s isolate memo). Pins `d2fold_high1_a`, `_b`.
- **HIGH-2.** `MARK_TIP_SQL` marks `gateHeight BETWEEN tip - 6 AND tip` (the design's window); a gate the tip skipped waits for its source write or the age repair. Pin `d2fold_high2`, the 40-row fixture re-cut.
- **HIGH-3.** See the belt paragraph above (`belt_due`, the backoff, the bounded identity walk). Pins `d2fold_high3_a` (1,000 stranger rows due first, the honest look in the first tick), `_b`, `_c`.
- **MEDIUM-1.** The wanted envelope expands to the newest `OWED_WANTED_GAMES` (32) games, the rest marked `backfill`; `drain_dirty` walks up to 64 identities past its cursor (`owed_belt_state` `drain`, a recursive CTE of covering-index searches), reads each one's few oldest marks in one batch and takes subjects round-robin across identities, oldest first, `wanted` the tie-break inside an identity, age-only last. New indexes `owed_dirty(sourceKey)` and `(identity, markedAtMs)`; the wanted envelope and the age marks carry the constant keys `wanted` and `age` so the source-key index serves them, and `idx_owed_dirty_marked` and `idx_owed_dirty_identity` are dropped (no reader), so one wanted mark stays five physical writes (ruling 1, record 1277); no shipped drain statement scans `owed_dirty` (`d2fold_medium1_b`, and `d1_fleet_plans` now runs the shipped constants). Pin `d2fold_medium1_a`.
- **MEDIUM-2.** A filing whose judge cannot run lands its source statements ALONE (their marks wait for the drain), counted `owed_judge_filing_faults_total`; only that lone write's fault refuses the filing. No tip is no refusal (TIP-877). `refund_subjects` is the filer first, then the pot's verified seats only (`REFUND_SUBJECTS_SQL`), and the judge keeps its caller's order for the 16-subject cut. The handfile batch shares it. Pins `--lib owed893_app_filing_atomic_on_row_fault` (inverted), `d2fold_medium2_a`, `_b`, the handfile pin inverted, `d2fold_medium2_c`.
- **MEDIUM-3, the old walk against the judge.** The old walk's regions of `routes.rs` are `macro_rules!` expanded twice: in place over `worker` (aliases `WalkDb`, `WalkDate`, `WalkEnv`, `walk_read_soft`, `walk_views`, `walk_canonical_root`, `walk_counted_batch`: the same items) and in `old_walk_native.rs` over SQLite, a test clock and couriers that never answer. `--lib d2fold_medium3_the_old_walk_and_the_judge_store_the_same_rows` runs the walk (cursor, ring, probes, write) and the judge (the shipped drain, or 16 games a batch on the fleet) over the #638 fleet and three scenario fixtures and compares stored rows, word facts stripped: 1,213 rows, all equal (a cut walk and a list past the ceiling are compared on the rows both hold). It found, and the judge now does as the walk: `mark_pending_spender`, `mark_refused_word_rows`, `mark_reorg_grace_rows`; the released hop's opponent from its twin; a hop named under two games is one row, the oldest marker's (`HOP_COMPETITORS_SQL`; it was two upserts to one key, the last judge winning). Stated: the ring's own `markerless*` cursor facts are not compared (the ring is replaced by the evidence row); natively the couriers, the header recheck and `load_stored_beef`'s guard are shims, and an expired probe memo diverges by construction (the walk would re-ask a courier). The design's 20-seat live diff on beta is the window's, not run here.
- **MEDIUM-5 and the rollback.** The age repair also marks keyless rows (`judgedAtMs IS NULL`, `AGE_MARK_UNJUDGED_SQL`, first, inside the same 128 pending). For the window's runbook: a ROLLBACK of the app layer to the old walk writes keyless rows and the dirty triggers keep marking with nothing draining (`owed_dirty` grows; its statements are index searches since MEDIUM-1); a forward redeploy repairs them through the age repair at 128 subjects a pass (and the untipped re-mark at 128 a tip), the backfill stamp untouched. Pin `d2fold_medium5`.
- **LOW-1.** A judge faulting on one identity's backfill for `OWED_BACKFILL_IDENTITY_FAULTS_MAX` (3) passes is recorded (`backfill:owed-v2:skipped:<identity>`, `owed_backfill_skipped_identities_total`), its marks left for the drain, the cursor moved past it. Pin `d2fold_low1`.
- **LOW-2.** `idx_owed_rows_identity_game`; triggers keep `owed_state.rows`; the judge's state row is created with one COUNT, the ceiling joins from it (under 500 rows the list is never visited). One subject's commit on a 500-row identity: 362 SQLite VM steps where the base's took 16,283 (`d2fold_low2`, which also holds the count exact through an old worker's write).
- **LOW-3.** `POST /internal/owed-backfill-pass?aged=1` runs `mark_aged` alone; the backfill tier ages its rows on local D1 and calls it (`owed-age-remark`).
- **LOW-4.** The filing's judgement takes the filed (newest) valid backup (`--lib d2fold_low4`).
- **LOW-5, the deploy order.** The overlay first; read its certified count on `/health` at or past `OWED_TRIGGERS_MIGRATION_COUNT` (407); then the app layer. An app layer first would fault its drain and belt (`INDEXED BY` an absent index) and land filings alone (MEDIUM-2), refusing none.
- **NOTEs.** NOTE-1 (no stranger write derives a victim row), NOTE-2 (a word marks only rows naming it; `stale` reads the index's latch first), NOTE-3 (the eviction's evidence row is sound), NOTE-5 (the belt's evidence is the existing binds): stated, nothing to fold. NOTE-4, stated and not folded: `POT_UPDATE` fires on the admission UPSERT's UPDATE arm with unchanged values, so a re-submit of a known JOIN buys judges of the pot's parties (cost only); the lens's `WHEN` on changed columns needs every `pot_records` column the judge reads, and a column added later would be silently unmarked, a money risk the cost does not justify.

The migration list is 407 (fingerprint 1,328,337,484), up from 396: `idx_owed_rows_sweep`, `idx_owed_rows_untipped`, `idx_owed_rows_belt_turn`, `idx_owed_dirty_source_key`, `idx_owed_dirty_identity_marked`, `idx_owed_rows_unjudged`, `idx_owed_rows_identity_game`, `owed_rows_count_insert`, `owed_rows_count_delete`, and the drops of `idx_owed_dirty_marked` and `idx_owed_dirty_identity` (bsv-low's migration pin moves in its own commit before the prod promotion).

Measured on local D1 (the budget tier, re-recorded in `tools/lane-499/ceilings.json`; the CHAINTRACKS stand-in now serves the verified header tip PR 172 reads): `owed-belt-tick` reads 17,223 to 896, writes 162 to 260 (`-429` 709 to 266, 27 to 36); `owed-backfill-pass` reads 53,424 to 25,591, writes 5,575 to 7,344; `owed-backfill-stamped` 1 read; `owed-age-remark` (new) 286 reads, 296 writes, 73 subjects under the 128 bound; `owed-fleet-read` unchanged (2,000 reads, 3 statements); `owed-wanted-read` 5 writes; `owed-judge-filing` reads 118 to 126, writes 36 to 42, statements 59 to 61. The writes added are the fold's `owed_rows` index entries and the two row-count triggers: reads, which #638 budgets, fell by half or more on the belt and the backfill (the filing path reads 8 more, its state row created with one count), and writes rose by a third or more on each judging path, stated.
