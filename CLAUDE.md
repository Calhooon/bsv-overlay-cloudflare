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
- **#861, a shed read never offers a stored press.** Retired with the shed (bsv-low #893 section 6.2): there is no shed; a stored row is served as judged and a subject with a pending downgrade mark is served withheld (`refresh: "pending"`, the D2-L1 rule below). Its pins `fix861_a`, `fix861_b` and `money_owed_lens_*` were deleted with `demote_shed_presses`; the read's pending pins (`d2delta2_l1_*`, `d4n3_a`) are their equivalent.
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
- **The cut.** Retired with the walk (bsv-low #893 section 6.2): a judged subject's rows are replaced whole by its batch, so nothing is served from a previous row; `carry_shed_rows`, `fix837_d`, `fix837f_l4`, `fix859_b` went with it (the judge's per-game delete is pinned by `owed_judge_sqlite`).

Stated: #793's chain rung already downgrades an index-unspent hop with a positive spent word (the `chainProbe: "spent"` arm); what it could not see is a hop with no word whose own JOIN is held, which the belt now reads. A hop whose spender is no pot and whose chain rung is silent (budget cut, couriers down) is still offered, #669's accepted residual. No wire field removed, no migration, no new statement shape in the overlay.

Pins, each RED with its hunk reverted alone: `--lib fix837` in `overlay-discovery` (`fix837_a`, the shared write: inert, `left: []`); the worker's `--lib fix837` (`fix837_a3`, every off-door road calls the one function: RED with the readmission's call, one `/admin/readmit` arm (`left: 1 right: 2`), the re-present's call or the discovery pass's call removed; `fix837_b`, the discovery pass marks the held JOIN's hop with no courier word: `join_hops_marked` 0); the app layer's `--lib fix837` (`fix837_c`, the belt and the chain arm: `left: HopStranded right: Unbound` without the belt, `left: 0 right: 1` without the chain arm's row; `fix837_d`, the cut's carry: `left: 0 right: 1` inert; `fix837_e`, the route's wiring of both and the answer's mark), and `the_index_unspent_offers_the_sweep_and_the_chain_rung_only_downgrades_it` amended (RED `left: 0 right: 1` without the chain arm's row). The tier's #837 cell runs after section 3 of `tools/lane-499/d1_budget_route_ci.mjs`: C's first read writes 300 rows (426 statements) and the app's `/owed` running maximum is held to A's named ceiling there. The fold's pins (the FIX-837 lens), each RED on 9d380f8c: `fix837f_m1_the_shared_write_reads_a_bounded_window_of_a_wide_joins_inputs` and `fix837f_l3_a_pointer_landed_after_the_read_is_never_overwritten` (`--lib fix837f` in `overlay-discovery`; `left: 65 right: 17` row reads, and the rival's pointer overwritten by the JOIN), the worker's `fix837f_m1_the_discovery_pass_reads_a_bounded_number_of_join_inputs` (326 marked), the app layer's `fix837f_l4_the_carry_stops_at_the_row_cap` (`left: 509 right: 500`), `fix837f_l1_*` (a source pin: on bsv-rs 0.7.2 `Beef::from_binary` refuses what the limits' parse refuses) and `fix837f_l2_*` (`belt_window`, absent on the base; `fix837_e` reds on the route's text), and `d1_fleet_plans` `fix837f_l5_*` (the belt's two reads and the guarded write are key searches; no RED possible, the plans were already index-served, the lens's LOW-5). The cell `tools/lane-837/join_hop_pass_ci.mjs` is RED on 9d380f8c (no lever, 404).
## A found spend gets its verdict latched (bsv-low #986, READ-986 items 2(b), 3 and 5; lane FIX-986, 2026-10-30)

READ-986 family D: a pre-signed refund broadcast at its recovery height OUTSIDE the overlay's door was found by the hourly discovery pass (`discover_spend_for_row`: a pointer with NO verdict), proved by the confirmation pass (`complete_spend_confirmations`: `spentConfirmed`, `spentHeight`; no verdict), and skipped by the janitor's params backfill (`paramsDecoded = 0` rows only). D1's results view classified such a spend per request from the spender's BEEF (`results::classify_from_columns`); the owed actor's mirror holds every BEEF column as an empty blob (D2-ACTOR-B's "no bytes held"), so it served `unbound` / `outcome unresolved` where D1 served `payout refund claimable`: both seats of games 49c731be… and 10eb9cc9… (19,800 sats each) withheld on beta, and 27 spent, confirmed, decoded covenant pots found by the discovery pass with no verdict (13 with the spender's BEEF held). The stored column is now the ONE source both judges read.

- **One classifier** (`overlay-discovery` `pot::spend_verdict`: `classify_pot_spend_tx`, `classify_record_spend`), the evidence in order: the row's decoded params and funded value (`stakeA + stakeB == potSats`, the covenant's own assertion), the spend's own input for the outpoint (its index feeds the #406 sighash, its sequence the refund's height gate), `classify_covenant` over the four mandated exits, then `settle_signers_for_spend` (WHO SIGNED, the spend's signatures against the committed triple); `None` is unresolved, never a guess. The door's hook (`ls_pot::output_spent`) calls it and writes the group as before (a pair not established stays NULL, the signers backfill's retry); every janitor-side writer holds the spender's DURABLE bytes and CONCLUDES it (`SpendVerdict::write_concluded`, `'unresolved'`, the signers backfill's own rule).
- **The passes latch the group** (`proof_fetcher::stored_spend_verdict` / `classify_stored_spend`: a decoded covenant row with its value, else no read; `get_beef(spender)`; the parse HASH-BOUND to the spender; the classifier). The discovery pass's `mark_spent` carries the group in the pointer's own statement when the bytes are held (else the pointer is written bare, as before); the confirmation pass's chase latches it under the pointer CAS (`mark_verdict_for_spender`) on a confirmed row carrying no CURRENT verdict (`verdictTxid == spendingTxid`, the reader's guard: a stale group reads as none), and its displacement arm classifies the displaced-in spender from the proven bytes in hand under the NEW pointer. Counted `verdicts` on `MissingSpendSummary` and `SpendConfirmSummary` (`verdicts=` on both tick lines, `spends_verdicts` on `/admin/complete-proofs`).
- **The backfill's second candidate set** (`PotStorage::find_spent_unclassified`; D1 `spent_unclassified_candidates_sql`): spent, CONFIRMED, `lockKind = 'covenant'` with its params, no current verdict (NULL, or keyed to another pointer), and the spender's BEEF HELD (`EXISTS pot_beefs`, a primary-key probe per candidate): a row whose bytes are absent is NOT a candidate (nothing is read until they arrive), an unconfirmed pointer is not one either (the discovery pass classifies it inline, its confirmation latches it; a displaceable intent is never latched by a sweep). RANDOM-sampled, `PARAMS_BACKFILL_LIMIT` (16) a tick, read through the partial index `idx_pot_spent_unclassified` (migration 475; `INDEXED BY`, because the stat-less planner took the composite `(spent, spentConfirmed)` index for the two equality terms, every confirmed spent row read a tick), whose WHERE the scan repeats byte for byte (`SPENT_UNCLASSIFIED_INDEX_PREDICATE`, pinned); the set drains as rows are latched, and a row matching no mandated exit stays in it, logged by outpoint. Counted `spent_unclassified` / `spent_classified` (`unclassified=` / `classified=` on the params-backfill line, `params_spent_unclassified` / `params_spent_classified` on the admin route).
- **No version move** (item 5): the verdict CAS writes `pot_records`, so each latched row lands as a `pot` event (`pot_changes::note`, the `POT_UPDATE` trigger) and every actor applies it over the same column D1 reads. The mirror's schema digest moves ONCE, by the index (an index-only move, `schema_plan_kept`: the rows kept, no re-bootstrap, no re-judge).
- **Stated.** A confirmed row whose `spentHeight` is NULL and whose verified spender BEEF carries a bump differs on `atHeight` between the two judges (D1 reads the bump, the actor cannot); every confirmed write carries its bump's height, so the class is narrow and is 2(a)'s. Item 2(a) (the results view's `spenderBeef` / `fundingBeef` served from `txfacts`) is NOT fitted here, filed as the parity follow-up: it needs each input's sequence, the lock time and each output's full script in `TxFacts` (a stored-shape change, a `JUDGE_VERSION` move and a fleet re-bootstrap), and the signers cannot be read from facts at all (the BIP-143 digest needs the raw). 2(b) alone heals beta: the backfill latches the 13 rows whose bytes are held on its first ticks; the 14 without them wait for their bytes (not candidates, no cost).
- Pins. Overlay `--lib fix986`: `proof_fetcher::tests::fix986` (`a` the discovery pass's write carries the group, `coop` from the spend's own signatures, a pot whose bytes are absent written bare; `b` the chase on a bare pointer, a current group kept, an absent one confirmed bare, the displacement arm; `c` the second set: the 27-shaped row and a stale group latched, an absent, an unconfirmed, a bare and a current one never, the next pass empty, and `classify_stored_spend` over the shipped bytes), each RED with the shared classifier made inert (`left: (2, 0) right: (2, 1)`, `left: (3, 3, 0)`, `left: (0, 2, 0, 0)`), `c` alone RED with the set made empty; `d1_discovery::tests::fix986` (the candidate set under real SQLite, the plan on its partial index stat-less and after ANALYZE, no full-scan step, the index's WHERE equal to the scan's, the list's last migration). Discovery `--lib spend_verdict`. App layer `--lib fix986` (`owed_actor::tests::fix986`: `d` THE DIFFERENTIAL PIN, item 3: a discovery-found refund (`paramsDecoded 1`, `verdict NULL`, the spender's `pot_beefs` row `proof_verified 1`, no result marker, both seats verified), D1 `payout refund claimable` against the actor's `unbound` "no verdict" before the janitor's latch (the premise, today's beta), the latch replayed natively with the shipped scan, classifier and CAS, then equal rows byte for byte and equal `rows_digest`, by a `pot` event on the warm actor and by a fresh bootstrap for seat B; `e` the controls, a hop-sweep filing proven by the index (class C) and a collected filing (class A, the payout before it, no row with it) equal on both sides), `--test d1_hot_bounds fix986` (the scan on the fleet fixture: its index, no fleet table).

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

## A held pot with no index row is recorded at its spend (bsv-low #936, lane FIX-936-POT-ROWS-LEFT, 2026-10-30)

Loop 52's S4 (beta, 2026-10-27): the JOIN `fad01028...` was admitted PENDING at 16:10:47Z and SEEN-latched at 16:10:53Z, and at 17:5xZ D1 held its `tm_pot` applied row over ZERO outputs, no `pot_records`, no `pot_beefs`, no eviction record. HOW THE ROWS LEFT (READ-936, reproduced here on the engine over `tm_pot` and `ls_pot`): both seats' gated requests were cancelled by the client while they owned the engine write, which stopped AFTER the engine's `insert_output` and BEFORE `ls_pot`'s `store_record` (a `tm_pot` coin and a `transactions` row, no index row, no applied row); the refund's admission at 16:26Z then found the coin, recorded the JOIN applied (`engine.rs`, #559's H2, `record_spent_coin_applied`) and `delete_utxo_deep`'d it, which deleted the `outputs` row and, with none left, the `transactions` row. An applied row over zero outputs is every SPENT pot's normal shape (tm_pot retains nothing); the defect was the index's missing rows, which no road could write again: no replay was queued (the write was cut, not faulted), the retire, conflict and reorg passes and the eviction never touched it (no ledger row), and the JOIN's own replay would be a dupe by H2's row. PR 188 (READ-936) closed the first step for the client's cancel (the write owned by `wait_until`); the platform's own cut of that task remained, and so did the second step.

The fix (`crates/overlay-discovery/src/pot/lookup_service.rs`): the engine tells a lookup service of a spend only for a coin the TOPIC HOLDS (`Engine::submit`'s previous coins), so a spent outpoint with no record in `ls_pot` is an output the topic admitted whose admission hook never ran. `output_spent` now records such a pot FIRST, from the spender's own bytes (a valid BEEF carries its input's source transaction: the refund's carried the JOIN raw, its sources absent, as the door walk's INCONCLUSIVE line said), through the admission hook itself (`record_held_output_from_spender` calls `output_admitted_by_topic`, one writer, its per-topic shape gate included, so a P2PKH output under `tm_pot` still writes nothing and a held hop under `tm_lowfund` is recorded `p2pkh`), the pot's own BEEF cut out of the spender's (`to_beef(true)` of the carried source, named), then writes the pointer as before: the pot stands recorded and spent by its refund exactly as a whole write leaves it (`pot_records` spent by the refund with its params decoded from its own lock, `pot_beefs` under the JOIN's txid; the refund's bytes under its own, as before), and the owed judge serves the refund from the same rows as any settled pot's. The verdict classification reads the healed record (the re-read that also says whether the hook wrote: one more read, on the heal path only). A spender that does not carry the source (a proven spender, txid-only ancestry) has the pot recorded from the ENGINE's stored copy (the fold's LOW-2, below); a record read that FAULTS is the hook's fault, "not now" (the fold's LOW-1, below); a storage fault of the heal is the hook's fault, as the admission's own is (the engine keeps the coin marked spent and the replay tells the hook again). The engine is unchanged: H2's premise (a held coin's inserts landed) holds; what did not run was the hook, and the index is the one party that knows its row is missing at no added read (the verdict read it already made). A pot recorded WITHOUT its bytes has them written at its spend (the fold's LOW-3, below). Stated: the heal's `tracing::warn!` line is silent on the Worker (no subscriber), so its evidence is the D1 read L53/L54 make (a `pot_records` row for every `tm_pot` applied txid of a covenant transaction, or a `pot_evictions` row), and `D1PotStorage::note_recorded_at_spend` prints one `console_warn!` line `[pot-index] <txid>:<vout> recorded at its spend by <spender> (bsv-low #936)` for `worker-logs.py`.

Pins (`cargo test -p bsv-overlay-discovery --lib fix936`, each RED on `1ab40c95`, `/Volumes/Lanes/out/wt-overlay-fix-936/fix936-red-on-1ab40c95.txt`): `fix936_a_a_write_cut_at_the_hook_then_its_refund_leaves_the_pot_recorded` (a real engine over `tm_pot` and `ls_pot`, the first admission hook made to hang and the submit's future dropped there: the coin held, no row, no applied row; the SEEN latch; the refund admitted whole: H2's applied row over zero outputs, the coin deleted; RED: "the pot the engine admitted is recorded (bsv-low #936)"), `fix936_b_the_index_holds_every_pot_the_engine_recorded_applied` (one JOIN cut, one whole, each spent: every applied `tm_pot` row has its record spent by its spender; RED on the cut one), `fix936_c_the_spend_hook_records_a_held_pot_from_the_spenders_bytes` (the hook alone: recorded once and idempotent; a spender not carrying its source, with no engine copy, records nothing; a carried P2PKH source under `tm_pot` records nothing; a recorded pot's bytes untouched; a held hop under `tm_lowfund` recorded and spent by its JOIN).

The from-scratch lens (`docs/audit/FIX-936-lens-2026-10-30.md`, SOUND, 3 LOW, 4 NOTE) folded (lane FIX-936-FOLD, `docs/audit/FIX-936-fold-2026-10-30.md`). LOW-1: `output_spent`'s record read that FAULTS answers the fault (`StorageError`, "not now"): the engine keeps the coin unapplied (#657's notification fault) and the replay tells the hook again; it proceeded, so a cut pot's only spend could land with the coin deleted and no record, for good. Its cost: a transient read fault on ANY pot's spend (recorded or not) now replays the spender where it wrote the pointer without a verdict (the backfill's to fill); a pot row that deterministically fails to read holds its spender at the door (three retries, a dead letter). The lens's premise for the old branch (an admit write resets a verified latch) does not hold on either backend: `store_beef` writes nothing over a verified row. LOW-2: with no carried source the pot's bytes are read from the engine's own `transactions` row (`PotStorage::held_transaction_beef`, default none; the D1 backend runs `admission_abort::STORED_BEEF_SQL`; the row still stands at the hook, the engine deletes it after), hash-bound to the outpoint (the atomic BEEF's subject id must be the pot's txid, both sources: `held_output_bytes`). LOW-3: a RECORDED pot is probed for its bytes at its spend (`PotStorage::has_beef`, the D1 backend `POT_BEEF_PROBE_SQL`, no bytes read: one read a spend of a tm_pot or tm_lowfund outpoint) and, holding none, written them from the same two sources (`complete_held_output_bytes`); a row present (verified or not) is never touched; a probe or write fault is the hook's fault. NOTE-2: the heal's statements in its order under real SQLite (`d1_discovery::tests::fix936f_n2_the_heal_path_statements_under_real_sqlite`). NOTE-1 (an UNSPENT cut pot is recorded by nothing until its spend), NOTE-3 and NOTE-4 stated, not changed. Pins (`--lib fix936f` in `overlay-discovery`, each RED on `5eb071da` with only the memory store's test hooks grafted, `/Volumes/Lanes/out/wt-overlay-fix-936-fold/fix936f-red-on-5eb071da.txt`): `fix936f_l1_a_faulted_record_read_at_the_spend_is_not_now_and_the_replay_records_the_pot` (the real engine; RED: the refund landed over a faulted read, `applied_topics: ["tm_pot"]`), `fix936f_l2_a_spender_without_the_pots_bytes_records_it_from_the_engines_copy` (RED: nothing recorded; a copy naming another transaction records nothing), `fix936f_l3_a_pot_recorded_without_its_bytes_gets_them_at_the_spend` (the heal cut at its byte write then its replay, and an admission cut at its byte write then the spend; RED: no bytes). Lens round 2 (`docs/audit/FIX-936-lens-3-2026-10-30.md`, NOT CLEAN: 1 LOW, 3 NIT, 8 NOTE) folded (`docs/audit/FIX-936-fold-2-2026-10-30.md`). LOW-1: the heal (an absent record, or a recorded pot's absent bytes) first reads the eviction ledger (`PotStorage::evictions_for`, the read `spentStatus` already applies, `open_eviction_hides_row`) and writes NOTHING under an OPEN row. Two half-done ledger states reach the hook: a spend straddling the shadow move (`pot_records` moved first, the coin still held) and a readmission whose `pot_records` copy back faulted. No guard re-evicts a heal, so the pointer stays the no-op it was and the restore brings the real row back. A faulted ledger read is "not now". The common spend reads no ledger. NIT-1: `fix936f_n2` runs the record read, the ledger read and the two best-effort fills. NIT-3: `note_bytes_completed_at_spend` prints the completion's `[pot-index]` line. Pins `--lib fix936f2` (three, each RED on `e8866847`, `/Volumes/Lanes/out/wt-overlay-fix-936-fold-2/`).

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

The owed recompute walk is RETIRED (bsv-low #893 section 6.2, lane D2-OWED-RETIRE, 2026-10-26): the walk (`owed_recompute`), its claim, isolate lock and slice (#800), its rate, ceiling and shed (#499, #648, #861), the inline race (#567), the deferred walk and its alarm, the ranked cursor (`owed_cursor.rs`; the selection and hydration the judge keeps are `owed_judge::rank`), the markerless ring and its cursor (the eviction's evidence row replaces it, below), the home walk's route half, the walk's write plan and its `OWED_*` statements, the `owed_state.stale` marks (the filings', the hooks' and the tip's; nothing read them since the read went to the dirty marks), #608's re-judge mark (`spenderUnread`) and #672's keep of a previous judgement (the durable spender-bytes store stays) are deleted with their pins. The owed list is judged at the write ("Derived lookup tables are the write's" below), read by `owed_read`, dated for the chain by the belt. `/health.owed` serves the judge's isolate fields only (`markerlessReadFaults`, `markerlessIgnoredMarkers`, `probeMemoReadFaults`, `spenderStoreSweepPasses`, `hopStrandedAfterMs`); the walk's durable counter rows stay seeded and served at the totals they reached. The walk-against-judge pins (`owed_walk_frozen.rs`) compare the judge with the walk's stored rows frozen in `tests/fixtures/old_walk_893` at `0dae1fba` (written by the walk itself, `OLD_WALK_FREEZE=1`, twice, byte-identical). `/owed`'s `markerlessProgress` (served `null` for the D2 release) is gone.

NOTE-C: deleteAll() preserves the alarm at compatibility date `2024-12-01` (the app-layer's pinned date).
workers-rs 0.8.5 `Storage::delete_all` delegates directly to the JS `deleteAll()` call; Cloudflare changed
both KV and SQLite backends to delete the alarm only at dates `2026-02-24` or later
([compatibility change](https://developers.cloudflare.com/changelog/post/2026-02-24-deleteall-deletes-alarms/)).
The workerd DO pin sets an alarm, deletes all keys, and observes the alarm still set at the configured date.
An actor the retired deferred walk armed clears its storage at its alarm and arms nothing again (the belt's actor keeps its
own key); its correctness does not depend on the old alarm surviving deleteAll().

(bsv-low #893 section 6.2: the walk's phase 2 and 2b re-asks and their latch writes, `durable_reasks`, `proof_latch_memo`, `OWED_DURABLE_REASKS_PER_RECOMPUTE`, `OWED_PROOF_LATCH_WRITES_PER_RECOMPUTE`, and their pins `e693_a` to `e693_d`, `e693f_h1_*`, `e693f_m1`, `e693f2_h2`, `e693f3_h1`, retired with the walk: the belt asks the chain and writes the latch, the judge reads it; what follows is the latch's rule as the judge reads it, its history kept.) The owed list's courier-bytes swept payout also reaches confirmed ("home on chain") from a CHAINTRACKS-checked mined proof of the sweep (`OwedInputs::proof_mined_spenders`, gated by `txany::proof_mined_word`'s `confirmed && height` bound through `mined_proof_matches`, never a courier's bare height) read beside the spent-any ladder's single-WoC `spent_confirmed` so a hop whose only confirming courier faulted still confirms, the rows staying `claimable:false`; the MEDIUM-1 residual (bsv-low #692, follow-up #693) is that the proof fallback is timing-dependent, firing only on a pass that asks the sweep's `/tx-any` before its bytes go durable (#672), narrowed not eliminated and self-healing either way. Since #693 the recompute re-asks `/tx-any` for a durable spender whose row still needs the proof (no tracker mined word, no ladder `spent_confirmed`; `owed::durable_spender_needs_proof`), at most one a pass (`OWED_DURABLE_REASKS_PER_RECOMPUTE`) and only from the courier slots phase 2 left under the pass's clock, so a pass still makes at most two courier asks, counted `owed_durable_spender_reask_{asked,mined_by_proof,still_unconfirmed,fault}_total`; the index-hop-row arm gets no memo upgrade (it confirms from the index's own word and reads no chain memo, the #692 lens's LOW-1, stated at the site). Since the #693 fold (the W1-OWED-693 lens's HIGH-1) a checked mined proof of a spender is LATCHED ONCE CHECKED, SERVED FROM THE LATCH ON EVERY LATER PASS: phase 2's answer or 2b's re-ask writes `proofmined:<spenderTxid>.<height>` into `hop_chain_probes` in the snapshot's atomic batch (the height and the canonical root, never bytes; `owed::proof_latch_memo`, only on `proof_mined_word` and a root, at most `OWED_PROOF_LATCH_WRITES_PER_RECOMPUTE` a pass), read byte-free each pass before the pass's own answers (`owed::latched_proof_mined_spenders`, seeding `proof_mined_spenders`; a latched spender is never re-asked), so a row confirmed on pass N reads confirmed whatever a later pass's slots, budget or re-ask order do. A reorg demotes it: the overlay's reorg clear takes it (a chain word, not spared), its TTL sweep spares it (`HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES`), and a latch whose root is no longer chaintracks' root at its height is not honoured. The latch's lifecycle since the #693 fold-2 (the W1-OWED-693 re-lens's R-HIGH-1): written only from a checked proof in the snapshot batch; rechecked newest-first within the reorg window (`owed::LATCH_RECHECK_DEPTH`, 6 blocks below the pass's tip, at least the 4 newest; deeper is reorg-proof); honoured unrechecked (a faulted header, `over_budget`) only past the grace window with a clean clear history (written at or after the last clear's tombstone + `PROBE_MEMO_REORG_GRACE_MS`, the tombstone read in the latch read); re-asked after two hours unrechecked (`PROOF_LATCH_UNRECHECKED_MAX_AGE_MS`) unless reorg-proof; deleted by the reorg clear, and (since the #693 fold-3, the delta lens's R2-HIGH-1 (a)) deleted by the first recheck that finds its root moved (`owed::moved_proof_latches`, `PROOF_LATCH_MOVED_DELETE_SQL`: one statement in the snapshot batch, none when nothing moved, a row taken only while it holds the refused root), so that refusal is durable and the spender is re-asked; a later checked proof latches its new height. Stated (R2-HIGH-1 (b)): a latch whose root a reorg moved is honoured again only when NO clear recorded the reorg (the clear's tombstone write AND its delete both faulted, or NO detector saw it: neither the overlay's 3-deep tip sweep, nor a chaintracks `reorgFrom`, nor an applied Arcade orphan event) AND no pass rechecked the latch while it was inside the window (an identity with no owed pass in the ~6 blocks after the reorg, or every such pass's header read for it faulted or was `over_budget`): once 6 blocks deep it is reorg-proof and is not re-read, so it is honoured by depth until a later recheck (it is among the 4 newest) or a clear; a single recheck that sees the moved root ends it for good. Display only either way: the latch feeds courier-bytes own-sweep rows, which stay `claimable:false`. Pin `e693f3_h1`. Until the overlay's sweep change is deployed a latch older than two hours is swept and re-latched by the next checked proof. Pins `e693f_h1_a` / `b` / `c`. `/spent-any` (#695, DESIGN-641 option (b)): when every primary rung faulted after WoC's `/spent` answered 400 (its word for an unknown tx), the ladder reads WoC's `/tx/hash/{txid}` once, and its conclusive 404 (the subject unmined, one capable source, the pruned Bitails unable to corroborate) answers `known:false, reason:"uncorroborated-unspent"`, never `provider-fault`, which stays for a 429, a 5xx, a transport or a body-shape fault (`results::sharpen_unmined_absence`; every other body byte-identical, pinned `e695_a`, `e695_b`). Stated (the W1-OWED-693 lens's LOW-1): `uncorroborated-unspent` now names TWO evidence shapes, (1) WoC answered 404/410 for the OUTPOINT and no second source corroborated (as before), and (2) every primary rung faulted and the TX is unknown to WoC's node (its `/tx/hash` 404); the word asserts no absence (`known:false`, no `terminal`, no memo), so it cannot fire the monitor's Void arm (which reads both reasons as `Blind`). The client's `hopSpenderRead` (bsv-low `app/src/lib/stake.ts`) reads this reason as `looked: true`, so a hop whose funding tx WoC never saw now reads "the network does not know it yet" where it read "could not be asked"; its one reader of `looked` (`orchestrator.ts`) feeds `rejoinSourceDecision` (`joinEvidence.ts`), which answers `blocked` either way, so no money decision changes. The lens's additive `basis: "subject-unknown"` is not built: a future reader that would act on "unspent" must not act on this reason alone. `/spent-any` `spentConfirmed` (#723): a KNOWN spend's confirmation rests on a checked proof of its VERIFIED spender where the app layer holds one, strongest first: the index's verified proof (`transactions.has_proof`, one byte-free PK read of the row's spenders, `hopsweep::sweep_proofs_sql`, none when no row is a known spend; a fault is no proof), the tracker's checked mined word (`owed::checked_tracker_mined` over the views the route already builds), an isolate-held chaintracks-bound `/tx-any` proof (`txany::proof_mined_word`); only when none is held does WoC `/0/spent`'s status speak, and when WoC faulted and a fallback rung named the spender (BananaBlocks, Bitails, which cannot say mined) the word is `null`, never `false` (`results::spent_confirmed_unknown_after_fault`, `results::spent_confirmed_word`). The source rides each known spend as the additive `spentConfirmedSource` (`index-proof`, `tracker-mined`, `txany-proof`, `courier`, or `null`); every other row's bytes are unchanged, and the probe memo keeps the ladder's own word. Readers audited: every money reader reads `spentConfirmed == true` (the monitor's `spender_is_network_witnessed`, the watchtower's `chain_oracle`, the client's `chainReads.ts` `decodeSpentFold` keeps `null` as unknown, `stake.ts` and `moneyTruth.ts` `=== true`), so `null` fails closed as `false` did; the resync diff harness (bsv-low `scripts/resync-diff.mjs`) compares both fields exactly and must exclude `spentConfirmedSource` and decide whether `null` against `true` is a diff. Pins `e723_a` / `b` / `c`.

The served words carry the host's SPEND DEPTH (bsv-low #862, L7-DEPTH; the L6 app-layer half of #622): `spendDepth`, an integer, the confirmations this host requires before a wallet credits a coin on a served word (R3 of the wallet charter), rides beside `tracker` on `/tx-any` (the subject and every `answers[id]`) and beside `lock` on every `/utxo-status` and `/spent-any` entry (`runtime::stamp_spend_depth`, after the words, at `body_loaded`, `outpoint_body` and the `/spent-any` route), and `/health.spendDepth` serves the read {`depth`, `source` `var`|`floor`, `floor`, `var`}. It is read from the render var `SPEND_DEPTH` (`"1"` in prod and beta of `crates/low-app-layer/wrangler.toml`; the value at risk is the owner's later word); 1 is the floor, served for a missing or malformed var (anything but decimal digits from 1 to `SPEND_DEPTH_MAX` 144, the bound shared with the client's `SERVED_SPEND_DEPTH_MAX`, which reads a value past 144 as not served; the L7-DEPTH lens's MEDIUM-1, lane L7-DEPTH-FOLD: a var past 144 was served as set while the client credited at 1), which counts `spend_depth_var_malformed_total` once per isolate (seeded by the overlay, so `/health/invariants` serves it from 0; the depth itself is the app layer's var and is read on its `/health`, the overlay holds none); an object the word decoration did not reach (a word-store fault's legacy body) gains nothing, so a decorated body is its old shape plus exactly one key. The client reads it in bsv-low's `app/src/lib/creditWord.ts` `readLanding` (L7-TOWER) and runs `CREDIT_DEPTH_FLOOR = 1` where it is absent. Pins `--test spend_depth_l7` (eight, each RED against its rule grafted inert: the stamp, the var read, the once-per-isolate count, the parse, the wiring, the health read, the renders, the seed); the fold's `l7f_m1_*`: 145, 1000 and `u32::MAX` the floor, `source: "floor"`, counted once; 144 and 1 served and never counted; RED on 6546ba67 with the constant grafted alone: `SPEND_DEPTH="145"`, `left: (145, true) right: (1, false)`), and `--test spend_depth_l7_lens` (the lens's seven, its MEDIUM-1 pin inverted to the floor and its `u32::MAX` row the floor). A depth served on a `/tx-any` word-store fault is still absent (the legacy body); the client's session rule (bsv-low `creditWord.ts` `servedDepthOf`: the largest depth any answer served wins, and once one was served a read with none is `pending`) covers it, and the outpoint rows the same credit reads carry the depth (the lens's LOW-1, judged, no server change).

A refund-due row is a press only where the pot's FUNDING is held (bsv-low #831, loop 43 pair 17: a JOIN admitted pending, never SEEN-latched, never mined, never evicted, served `present-refund`, and its pre-signed refund met `SEEN_IN_ORPHAN_MEMPOOL`). The owed recompute's paired hydration (`owed_cursor::hydrate_party_pair_sql`, the statement it already runs over the chosen pots) carries `fundingHeld` for an UNSPENT pot: `seen` (the pot txid's own `network_seen` latch), else `proof` (`pot_beefs.proof_verified`), else `none`; NULL for a spent pot (`FUNDING_HELD_COLUMN`, each probe a primary-key search gated by the CASE, the proof probe only without the latch). A `none` pot's funding txid joins the coin-word read of step 3b, and the judgement (`owed::derive_owed_rows`, `OwedInputs::funding_unheld`) serves such a refund-due pot as `unbound` with `claim: "funding-not-held"`, `claimable: false` and the reason `FUNDING_NOT_HELD_REASON`, unless the tracker's CHECKED `mined` word holds the funding; every other pot keeps its row byte for byte. The judgement accepts the overlay's own SEEN latch, the index's verified proof or a checked mined tracker word, not a tracker `seen` hint (the FIX-831 lens's L5, the safer reading than the issue's "a `seen` funding keeps today's judgement"): a tracker hint is no witness the network holds the output, and past the recovery gate an honest funding is mined, which the checked `mined` word honours. A word-store fault withholds for that pass (latch or proof only); each pot judged so bumps `owed_refund_funding_unheld_total` (seeded at 0 by the overlay), so the class withheld for good (a mined funding the index never verified, with no latch and no `tx_words` row) is visible. Extra reads per identity: one `network_seen` probe per unspent chosen pot, one `pot_beefs` probe per unlatched one, one `tx_words` key per unheld one; none for a spent pot. Pins `--lib fix831` (RED with the input grafted inert: `left: RefundDue right: Unbound`) and `--test d1_fleet_plans fix831` (the column's four values and the two key searches). Not built here (half 2, the overlay): the retire pass marks such a pot's BEEF `structurally_unprovable` and never evicts it, so its hops are not released. Since B-OWED-2 (bsv-low #598, the lens's HIGH-1 folded): a latched (`fundingHeld: seen`) funding the network refused takes the same judgement, served `fundingHeld: refused` with `fundingWord`, read from the one producer this tip has: the word event of the pot's own txid (`callback`, Arcade's push; `tick`, the re-ask) that read Arcade's REJECTED / INVALID / MALFORMED or a double spend marks its `tx_words` row (`runtime::mark_terminal`: `trackerTerminal`, `trackerTerminalWord`), which `stored::Row::view` serves as the additive `terminalHint` (absent on every unmarked row) on any word but `mined`, and `owed::funding_refusal_word` reads beside a CHAIN-tier `rejected` / `conflicted` (no producer feeds that today; the tier is read). The tracker keeps the word (NS-3): the mark is the evidence. The fail direction: a funding Arcade refused that the network still holds keeps the mark until a checked proof (a later non-terminal hint does not lift it, and `DUE` no longer refreshes the row), so its refund is withheld until the pot's block, whose `mined` word clears the mark and whose checked `mined` word rescues first in the arm; the refund is not pressable before that block. Pins `--lib fix598f` (the word as the producer stores it, through the shipped UPSERT under real SQLite, then the derivation; RED on bca99893's reading: `left: (RefundDue, "present-refund", true)`). (bsv-low #492) a payout whose home output the index holds spent is no row (`retire_index_spent_homes`; one PK read per 45 such rows). (bsv-low #608's minute re-judge of a row resting on no spender bytes retired with the walk, #893: the belt dates such a row.)

The paged leaderboard (bsv-low #642): `/leaderboard:view` ranks owners over ONE candidate read of the last `LEADERBOARD_CANDIDATE_WINS` (2,000) counted wins (`logic::chain_wins_candidates_sql`, each verdict arm walked backwards on `idx_pot_records_chain_wins` and stopped at the cap), pages by the keyset cursor `nextCursor` / `?cursor=<wins>.<owner>` (the deployed client's `?after` rank offset stays, resolved over the same window), and serves `candidates` with `candidatesBounded: true` only when the era holds more, so a body inside the bound is byte-identical to the whole-board read (pinned through the shipped pipeline over rusqlite, `routes::leaderboard_642`). Its rows-read ceilings run on local D1 in `make ci-d1-board` (28,403 rows at page 1 of a 1,500-win board where the whole-board read took 76,577; flat past the cap), and the write-time table that removes the bound is #674 (`docs/DESIGN-674-LEADERBOARD-WRITE-TIME.md`).

## Derived lookup tables are the write's

The owed list's source statements mark `(identity, gameId, source, sourceKey)` in `owed_dirty` in their own transactions (`derived_owed.rs`): pot and party changes, hop markers, refund latches, result and collected filings, hop sweeps, eviction and readmission, network witnesses, tracker words, BEEF proof latches, refusal inserts/updates/deletes, released-hop evidence and spender bytes. Each mark carries an AUTOINCREMENT `generation`; a conflict advances it to the attempted insert's fresh generation and keeps the newest source, source key and time, so a judge reading `max(generation)` and deleting `generation <= seen` leaves a concurrent re-mark pending even after the queue has been emptied. The mark lookups use outpoint and txid indexes, including the indexed source-hop lookup when a pot arrives before its derived links. `owed_rows` carries nullable judgement, claim, gate, belt and probe-tip keys; `owed_row_words` supplies the word joins, and an orphan trigger follows every old-worker row delete. `released_hop_evidence` and `owed_belt_state` hold the eviction evidence and named cursors; the app-layer judge and scheduled backfill own the row derivation and completion stamp. Shipped trigger text is immutable: append `DROP TRIGGER IF EXISTS` plus `CREATE` to the migration list for a change, never edit it in place. The real-writer drift pin removes each dirty trigger alone, the prepared-program pin bounds source-statement cost on the fleet, and the hot-route plans pin the new lookup indexes.

The app layer's `owed_judge.rs` owns the per-game owed judgement: `judge_subjects` hydrates only the selected games, chooses a pot's representative by validity and oldest marker, and commits the rows, word references, state and generation-bounded acknowledgement in one counted batch before pushing `owed-changed`. `judge_with_write` puts the source statements first in that same batch after the verified filing is applied to the hydrated inputs; `refund_subjects` includes both seats. Each invocation judges at most 16 subjects, `drain_dirty` prefers wanted marks, and `mark_wanted` writes one envelope without reading or judging. The 500-row stored ceiling prefers actionable rows and records overflow. The hydration requires `idx_potparty_game_identity` and `idx_evicted_party_identity_game`; its native pins use the same SQL and transaction as D1, and every owed input fixture checks decomposition and storage read-back against the unchanged pure derivation.

`GET /owed` reads `owed_read::{ROWS_SQL, STATE_SQL, PENDING_SQL}` in one counted batch: the identity's stored rows joined to their word references and `tx_words`, its marker, and its dirty count, which names (the second delta lens's D2-L1, the captain's ruling: a read never does work, but never serves a press over a pending downgrade) the games holding an undrained mark of the DOWNGRADE class (since the third delta lens's D3-L1 the mark's own `owed_dirty.downgrade`, written by its trigger: a `pot_records` UPDATE that changes `spent`, `spendingTxid` or `spentConfirmed` or moves the key, a pot row admitted (since D4-L1, every admission: a JOIN consumes hops), a pot row deleted, an eviction, a word refusing its transaction (since D4-N3: a chain-tier `rejected` or `conflicted`, or #598's `trackerTerminal` on a word not `mined`); a discovery stamp, a facts fill, a re-present or any other word is a plain `pot` or `word` mark); a filing's, a tip's, a wanted or an age mark is none, and a belt find is judged in its own batch. Every claimable row of such a game is served withheld as re-judging (`claimable: false`, `claimableBeforePending: true`, `refresh: "pending"`, `pendingSource`, `stale`, `retryAfterMs` 1,500, `claimReason` `PENDING_DOWNGRADE_REASON`), across blocks too; the drain's judgement lifts it. A mark's presence is the test (the judge acknowledges `generation <= seen` of each subject it judges); `markedAtMs` cannot say "newer than the judgement" (a `pot` mark carries the pot row's `createdAt`). The count already reads every pending mark of the identity (over `idx_owed_dirty_identity_marked`, each mark's class from its row since D3-L1), so no statement is added (`owed-fleet-read` 2,000 reads, 3 statements, unchanged). Pin `d2delta2_l1_*` (a competitor recorded, the mark pending, no press served, across a block too; after the drain the row is downgraded; an eviction's mark the same; a filing's, a tip's and a wanted mark withhold nothing; RED on 6ec6eb9a: the stale press served, `left: Bool(true) right: false`). The reader rebuilds words through the existing header recheck, preserves stored claims, serves `pending`, keeps `markerlessProgress: null` for the compatibility release and serves `beltNextAtMs` from the row's stored `beltDueAtMs` (null where the index or a checked word decides the row; lane D2-OWED-BELT), and judges each claimable `hop-stranded` row IN THE SERVED BODY against the tip the row was judged at (`probeTip`; NULL is unknown) and the present tip (unreadable is unknown): a row whose hop the INDEX holds unspent (its stored `status` fact) keeps its press past a block, the move stated (`tipMoved` or `tipUnknown`, `snapshotTip`, `chainProbeTip`, `servedTip`; `owed::state_index_unspent_sweep`, main's `index-unspent` and #808 (b) rule), and a row whose press rests on a courier probe alone degrades by #669's gate (`degrade_stale_sweeps`) to a pending chain-wait, never a dead sweep (the D2-OWED lens's HIGH-1 as narrowed by the delta lens's D-H1). A missing marker permits only the indexed identity probe and one clock-free `mark_wanted`; an unknown identity writes nothing. The route cannot schedule work, and its shared accounting waits for a later request or scheduled pass. The native fleet-plan and body pins use the production SQL; `OWED_READ_ONLY=1 make ci-d1-budget` drives the unchanged 250-result, 270-hop fixture, judged by lane 2 into 500 stored rows on local D1 with external output selected by `OWED_READ_OUT`.

The leaderboard derived at the write (bsv-low #674, the #638 doctrine): `derived_board.rs` (migrations 296-327, the cutoff's 331-334) keeps `board_wins` (one row per counted win, `chain_wins_cte`'s predicate verbatim, the owners query's columns), `board_owners` (`wins`, `keyWins` per owner; `identityIsKey` is `keyWins > 0`), `board_hands` (each identity-owned win's earliest card-bearing verified claim, by `markerRowid`) and `board_total`, by 21 triggers: a write to `pot_records`, `network_seen` or `potparty_records` RECOMPUTES its pot's `board_wins` row from the source (delete, insert when the predicate holds), so the eviction's shadow move (a DELETE) and the readmission (an INSERT) are these triggers and a REPLACE of a pot still leaves the right row; `board_owners`, `board_total` and `board_hands` follow `board_wins` by its own triggers; a `result_markers_v2` write recomputes its pot's hand. A trigger's text is immutable once shipped (a change is a `DROP TRIGGER IF EXISTS` + `CREATE` pair appended), no production writer REPLACEs a source row (pinned), and every trigger text must survive wrangler 4.54's `--file` splitter, which pushes a frame on whitespace-`CASE` and pops on whitespace-`END` (a `,CASE` closed the trigger early): write `, CASE` (pinned under 4.54's model and `sqlite3_complete`, 4.148's). The one-time backfill (`WINS_BACKFILL`, guarded in its OUTER derived table, then the `board-v1` stamp) fills owners, total and hands through the same triggers; a stamped replay is 35 VM steps. `/leaderboard:view` (`routes::leaderboard_page_body`) reads the tables when the stamp exists and the tables are derived under the route's own era cutoff (`BOARD_READY_SQL`, which also reads `board_total` and the `board_era` stamp; an unstamped or unmigrated database, or a stamp that is not the route's cutoff, reads #642's candidates, never a 503 for it): the owner page by `idx_board_owners_rank` (offset, or the keyset cursor in two index arms with its rank counted for `nextAfter`), the page owners' wins by `idx_board_wins_owner`, the hands by `idx_board_hands_rank`, then #642's steps 3 to 6 unchanged; the body is #642's and the whole-board read's byte for byte (the #685 decoration included) with `candidatesBounded: false` and `candidates` every counted win. THE PAGE READ IS FLAT (the #674 lens fold, MEDIUM-1; as first built it read each page owner's `board_wins` rows and then every one of those pots' statuses, classification, markers and proofs, about 12 reads a page-owner win, 11,801 at 1,500 wins and 22,586 at 3,000): each `board_wins` row carries `frag` (the rows the page tail reads for that win: its status and decoded `pot_records` columns, its first `RESULT_ROWS_PER_POT` markers in `pot_markers_sql`'s order, each marker's proof pointers and posted bundle) and each `board_owners` row `card` (its wins' rows and frags by pot txid) and `packed`, kept by the triggers (`WIN_FRAG`; a result marker, `proof_markers` (`PROOF_*`), `proof_posts` (`POST_*`) or frag-only pot column (`POT_FRAG`) write refreshes the frags it names); the page reads the owner page, one card row per page owner (`logic::board_cards_sql`, `routes::leaderboard_table_body`), the hands and the hands' off-page pots, and steps 3 to 6 take the held rows from the cards (`routes::PagePrefetch`), so the fold is unchanged. Measured on local D1 (`make ci-d1-board`): the default page 323 rows at 1,500 AND at 3,000 wins (asserted equal), limit 200 978 / 1,003, the cursor page 485 / 467, largest statement 400; ceilings 728 / 1,505 / 600 (#642's read: 28,403 and 39,049). THE TRIGGERS' COST A STATEMENT (lane W1-LB-674-NOMEM): an update of `pot_records` or `potparty_records` recomputes the OLD pot only when the row moved to another pot (`recompute_pot!`'s guard; recomputing the same pot as OLD and NEW rewrote a packed win's card four times, 5,991,694 bytes of JSON at the bound, now two, 2,995,847), and the pin `lb674_nomem_the_triggers_prepare_and_write_within_their_bound` (`--test board_derived_sqlite`) bounds, over a card at `CARD_MAX_BYTES`, each source write's prepared program (256 KiB; measured 109,040 for a `network_seen` insert, 207,184 for a `pot_records` update) and its writes (`2 * CARD_MAX_BYTES` card bytes rewritten, about 745 WAL pages; a write that moves no packed win 3 to 15 pages). The route tiers load their seeds 2,000 statements a wrangler call (`scripts/d1-load-chunked.py`): Miniflare's local D1 holds every statement's compiled program, its triggers' programs inside it, until the isolate's GC, so the 102,473-statement fleet seed in one workerd died `SQLITE_NOMEM` at about 1.1 GB with the board's triggers (whole file, 10,000-line batches, and the NS2 cell's batches of 100 alike) and loads at 715 MB peak in 151 s; the NS2 cell opens a copy of the loaded `v3` (`D1_BUDGET_SEED_V3`) instead of loading it again. The fleet seed holds no counted win: its bytes on disk are the same with and without the board (44.36 against 44.22 MB). Stated: an owner whose card does not hold every win (a card past 1,500,000 bytes, about 600 wins of the fixture's size, or a frag past 200,000, or a pot txid not lowercase hex) is read by its wins as before (about 12 reads a win, for that owner alone); a counted win's insert and a marker, proof pointer or posted bundle write now build frags and cards (the `ci-d1-budget` write ceilings are owed a re-run). THE CUTOFF INSIDE THE DERIVATION (lane W2-LB-674-CUTOFF, 2026-10-18): beta and prod both set `WRITTEN_OFF_BEFORE_MS` (the #375 write-off), and as first shipped the route read the tables only with no cutoff, so on every deployment the triggers wrote a board nothing read (beta: 139 owners and 3,698 wins derived, 37 owners served by the candidate read at 34,077 rows a page). `board_era` (one row, `cutoffMs`, NULL for none) is the cutoff the board is derived under; `board_win_era` (BEFORE INSERT ON `board_wins`, `RAISE(IGNORE)`) refuses a win whose `potCreatedAt * 1000` is before it (the era filter's predicate; a NULL stamp is outside the era), so every shipped trigger that derives a win counts only the era's and none of their texts changed (the 32 statements' fingerprint is pinned; an old worker's re-run of its own list is a no-op); `board_era_update` (AFTER UPDATE OF `cutoffMs`, on a change) re-derives the whole board in the statement that moves the stamp (the owners cleared first so the per-win delete rewrites no card, then every counted win through `board_wins`' triggers). The overlay holds no cutoff: the app layer's scheduled pass (`routes::sync_board_era_env`, before the tracker's backfill) stamps its own clamped cutoff (`BOARD_ERA_SET_SQL` / `BOARD_ERA_CLEAR_SQL`, a no-op when it holds) and the route serves the tables only when the stamp equals its cutoff, so a moved var reads the candidates until the pass re-derives, never another era's tables. Measured: the default page under a cutoff 304 and 303 rows on local D1 (#642's 17,851 and 35,450); uncut 325 (the ready read's two `board_era` rows). Pins: `--lib lb674_cut` in the app layer (the table read under the cutoff is the candidate read's and the whole-board read's body at every page shape and through the cursor walk, the moved cutoff, past the candidate bound; RED with the main dispatcher and with `board_win_era` removed alone; `lb674_cut_beta_rows_*`, ignored, on a beta snapshot's rows under `LB674_BETA_ROWS`), `--test board_derived_sqlite lb674_cut` (the drift pin under the stamped cutoff after every hook path and every move, each of the 23 triggers removed alone RED, the deploy then the stamp, the re-runs benign), `--lib lb674_cut` in the overlay (the block appended, the shipped block untouched). Pins: `--test board_derived_sqlite` (the drift pin over every hook path, each frag equal to the app layer's own page statements run from scratch and each card to its owner's wins, the settle, tie, refund, witness and claims through the overlay's own writers (LOW-2), each of the 21 triggers removed alone RED; `lb674_fold_l1_*`: `HAND_SCORE_SQL` equal to `sql_hand_score_expr`; the backfill, the deploy window, the stamped replay), `--lib lb674` in both crates (`lb674_fold_m1_the_page_read_is_flat_in_the_page_owners_wins`: 8 statements and 203 rows at both boards, RED on `eaf122e`), `--test d1_fleet_plans the_board`.

## The owed belt asks the chain explorers (bsv-low #893 section 5, lane D2-OWED-BELT, 2026-10-25)

The explorers are break glass only: ONE place asks them for the owed list, `crates/low-app-layer/src/owed_belt.rs`, never a read. A `BOARD_VIEW` Durable Object named `owed_belt::OWED_BELT_ACTOR` (one, fleet-wide) runs `owed_belt::tick` from its alarm every `OWED_BELT_TICK_MS` (120 s); the alarm sets its next alarm BEFORE the tick runs, and the scheduled pass's owed task asks the actor on every cron (`owed_belt::arm`, phase `owed.belt_arm`, the watchdog: a lost alarm costs at most one 15 minute cron). The deferred owed walk's alarm is retired: the actor's `/owed-deferred` and `/owed-deferred-note` answer 410, and any other actor's alarm clears its storage and ends; with it the old walk's three probe-memo writes outside the snapshot batch are unreachable (their code is lane 6's to delete). `POST /internal/owed-belt-tick` (bearer `INTERNAL_TOKEN`) runs one tick on demand under the request's D1 ledger.

A tick takes its candidates in two classes (the second delta lens's D2-M1, the captain's ruling: a look a PRESS WAITS ON comes before a look that can only downgrade; `owed_belt::due_candidates`). FIRST the courier-only sweeps a block dated and not yet looked at (`BELT_SWEEPS_DUE_SQL` over `idx_owed_rows_sweep`, the tip's own scoped index: claimable `hop-stranded`, index status not `unspent`, judged below the present tip, due; at most `OWED_BELT_SWEEP_ROWS_READ` 256 a tick; since D3-N3 a SEEK of its due rows: the distinct judged tips below the present walked oldest first, one MIN seek each, at most 256, each tip's rows due by now on the index key `(probeTip, beltDueAtMs, identity, outpoint)`, so a re-armed row of the class is never read), round-robin by identity: after a block an honest courier-only sweep is looked at within `ceil(C / 8)` ticks, `C` the courier-dependent rows the block dated, whatever the strangers' index-unspent rows (pin `d2delta2_m1_*` in `tests/d2_owed_delta2.rs`: 640 strangers dated and not due, the honest look on the first tick; RED on 6ec6eb9a, no candidate at all on the first tick, the lens's probe 46 ticks). THEN the second class, round-robin by identity (the D2-OWED lens's HIGH-3), its walk run on EVERY tick and given at least `OWED_BELT_SECOND_CLASS_RESERVED` (2) of the 8 places whenever it holds a due row, the first class the rest, a class with nothing due yielding its places (`owed_belt::class_places`; the third delta lens's D3-M1: no class owns the whole tick, so under a flood of planted courier-dependent rows the young `in-progress` hop whose look creates a press still gets its look): up to `OWED_BELT_WALKED_PER_TICK` (512) identities holding a dated row walked past the cursor `owed_belt_state` `belt:identities` (`BELT_IDENTITIES_SQL`, wrapping, one index seek and one MIN seek each), of which only those holding a row DUE NOW take turns (at most `OWED_BELT_IDENTITIES_PER_TICK`, 64; S identities dated and not due delay this class by `ceil(S / 512)` ticks at most), each one's oldest due rows in one batch (`BELT_DUE_SQL`, the covering partial index `idx_owed_rows_belt_turn` on (identity, beltDueAtMs, outpoint)), each identity's oldest before a second of any, each turn in walk order and the cursor at the last identity GIVEN A TURN, or past the whole walk when none was (the delta lens's D-L1: an identity holding a due row is given its turn within `ceil(D / 8)` ticks of the walk's window reaching it, `D` the identities holding a due row ahead of it, and within `ceil(D / 2)` ticks under a first-class flood; a first-class row within `ceil(C / 6)` ticks under a second-class flood; the cursor moves only past identities given a turn or holding nothing due), the chosen rows read by key (`CANDIDATES_SQL`): bounded by the walk, never the due set, dated by the judge (`owed_judge::belt_due`: only a row whose evidence verifies, a hop row's marker or a pot row's chosen party `sigValid`; `hop-stranded` now + 600 s, an `unbound` row naming a spender whose bytes are NOT held now + 300 s, a `courier-bytes` payout now + 600 s, a young `in-progress` hop at its marker + 30 min; NULL otherwise), and a find whose row reads as before the look (its judged tip is not compared: a block is no change, the delta lens's D-L2) doubles the row's period to `OWED_BELT_BACKOFF_MAX_MS` (6 h; the judge's own dating restarts it). A tip dates due now only the claimable sweeps judged below it whose press rests on a courier probe alone (`owed_judge::DATE_SWEEPS_SQL`: the hop's `status` fact not `unspent`); an index-unspent sweep keeps the belt's own cadence (the delta lens's D-H1). Each candidate's ask (`owed_belt::ask_of`): a hop or pot outpoint through the spent-any ladder (`routes::belt_spent_any_via`), an `unbound` row's named spender through the tx-any courier leg for its hash-verified raw and, at a claimed height, WoC's `/proof/tsc` bound by `txany::mined_proof_matches` to the canonical root (a `proofmined:` latch), a `courier-bytes` payout's first unlatched home output through the ladder and `owed::home_word` (the home key's signature; a `homeproof:` latch). Every courier call passes ONE `owed_belt::Gate`: at most 3 an outpoint, 24 a tick, the three-fault circuit per rung (`woc`, `bitails`, `bananablocks`, `other`) per tick, a 429 closing its rung's turn at once, never the tick; a refused ask is no call and reads as that rung's fault. A FIND is ONE batch, `owed_judge::judge_with_write` with the find's statements first (the probe memo with the tip it was taken at, `PROBE_MEMO_UPSERT_SQL`; the spender's bytes, `OWED_SPENDER_BYTES_UPSERT_SQL`, DO NOTHING; the latch) and the same facts applied to the hydrated inputs, then the judge's rows, words, state and dirty acknowledgement for that subject: a fault rolls all of it back. A candidate no rung answered (a 429 on every rung, a spender whose raw no courier serves) keeps its row and facts and is re-armed ONCE, waiting as long again as it has since its judgement (`owed_belt::unanswered_period`: at least 300 s, so each unanswered look doubles the period to 6 h; the delta lens's D-L2; `REARM_SQL` sets, never adds). Counted: `owed_belt_ticks_total`, `owed_belt_asked_total`, `owed_belt_found_total`, `owed_belt_faults_<rung>_total`, and the backlog gauge `owed_belt_due_total` (set each tick over the partial index), seeded by the overlay (`applayer_counters::OWED_BELT`, pinned equal to `owed_belt::counter_names`) and served as `/health/invariants.owedBelt` {`ticks`, `asked`, `found`, `due`, `faults`, `bounds`}. The overlay's hourly `discover_missing_spends` is unchanged.

A tick with NO chain tip asks nothing (`TickReport::no_tip`, served `noTip` by the lever, counted as a tick): the judge refuses every subject without one ("chain tip unavailable; previous judgement stands"), so a find could not land and its calls would be spent for nothing; every date stays and the next tick with a tip asks them first (pin `owed893_belt_a_tick_without_a_chain_tip_asks_nothing_and_moves_no_date`, RED with the guard inert: 16 calls, 8 write faults; the tier found it: before the stand-in tip all 8 finds a tick faulted while its asserts passed, so the tier now asserts `found == asked` and `writeFaults == 0`). Measured on local D1 (the tier, CHAINTRACKS a stand-in worker in the same dev session at the seed's tip 900000): a tick of 8 finds 17,223 rows read, 162 written, 270 statements, the largest 1,001, 16 calls; the next 17,205; a 429 tick 709 read, 3 calls (`ceilings.json` `owed-belt-tick`, `owed-belt-tick-429`, 1.5x). Stated: the rows are the JUDGE's commit over a 500-row identity (profiled natively over the tier's database: the 500-row cap DELETE, the per-game DELETE and the `owed_state` COUNT, each O(the identity's rows), about 2,100 a find; no full scan), so 8 finds a tick on such identities is about 12.4 M rows a day, past half of prod's 20 M budget: the judge's per-subject commit is the cost to bound (lane 2's statements, not changed here). For the harness lane (D2-OWED-HARNESS), the numbers `owedCourierRetire` reads under the belt: `owed_belt_due_total` on `/health/invariants.owedBelt.due`; 8 outpoints a tick every 120 s, so the three payout rows within 10 minutes of the sweeps' broadcast when `due` is under 32, else within `due / 8` ticks; an unanswered candidate re-armed at now + 300 s at least, doubling while unanswered; the 6 block stall stays the outer red. Stated: a payout's ask hydrates its game once before the judge hydrates it again (the home outputs are the derivation's); two finds of one game in one tick are two judge batches; the tick's state row and gauge are one batch after the candidates, best effort. Pins `--test owed_belt_sqlite` (64 due hop rows through the shipped drain, a counting courier fake: one tick asks 8 by due time in at most 24 calls and the next tick the next 8; a 429 closes its rung after one call and three 503s close another, each candidate re-armed once at now + 300 s; a faulted row insert leaves no memo and no bytes, the next find writes memo, bytes and rows in one batch; a spender whose raw no courier serves spends exactly 24 calls and changes no fact; the candidate plan; the counters seeded and served), RED against five revert mutants, each run (the memo in its own batch: `left: 1 right: 0`, the memo left by a faulted row insert; no per-outpoint cap: `left: (0, 6) right: (0, 8)`, two candidates starved; no rung circuit: `left: 8 right: 1`, BananaBlocks asked after its 429; a re-arm that adds to the old date: `left: Some(1790000200000) right: Some(1790000300000)`; no bound at all, the outpoint's 3 and the tick's 24: `three an outpoint, 24 a tick, left: 32 right: 24`; the tick's 24 alone is a belt, equivalent under 8 outpoints x 3 calls, stated), and `--lib owed893_belt` (the alarm re-arms before its tick, the cron's arm, one actor). The module does not exist on `c055a052`. The tier: `OWED_BELT_ONLY=1 make ci-d1-budget` (`tools/lane-893/owed_belt_tick.sh`) over the READ lane's fleet seed with its dates an hour back, two ticks and a 429 tick against a counting stand-in (`ceilings.json` `owed-belt-tick`, `owed-belt-tick-429`).


## The owed backfill runs once behind a stamp, on the scheduled pass (bsv-low #893 section 6.1, lane D2-OWED-BACKFILL, 2026-10-25)

`crates/low-app-layer/src/owed_backfill.rs`. The one-time `owed-v2` derivation is a FIFTH detached task of the app layer's `scheduled` (`pass_meter::SCHEDULED_TASKS` 5, phase `owed.backfill`), handed after the tracker's, under its own wall (`OWED_BACKFILL_WALL_MS` 20 s, `OWED_BACKFILL_SAVE_RESERVE_MS` 3 s for the cursor's save); never a read's. `POST /internal/owed-backfill-pass` (bearer `INTERNAL_TOKEN`) runs one pass of the same function (the tier's lever).

A pass (`backfill_pass`): (1) `STAMP_SQL`, the `view_derivation_state` row `owed-v2`: present, the pass ends there, ONE primary-key read of one row (pinned on the populated fixture at zero full-scan steps and zero party rows). (2) `GATE_SQL`: the overlay's certified migration count (`ops_counters.overlay_migration_count`) must be at least `OWED_TRIGGERS_MIGRATION_COUNT` (407 since lane D2-OWED-FOLD, the list through the #893 block and the fold's indexes and row-count triggers; 409 since D2-OWED-FOLD-8, below; append-only, pinned against `OVERLAY_MIGRATIONS` because the app layer does not link the overlay), so the triggers exist before the rows do; and the cursor row `owed_belt_state` `backfill:owed-v2`: absent, the pass only ARMS it (cursor `''`) and the work begins one cron tick after the new version's first pass (risk 8.4). (3) `IDENTITIES_SQL`: 128 identities past the cursor, distinct and ascending, each side on `idx_potparty_identity` / `idx_hopparty_identity`. (4) Each identity: its games (the wanted expansion's union: party, hop, released-hop evidence, its own `owed_rows`) marked `source = 'backfill'` (`MARK_SQL`), the `open` row `backfill:owed-v2:open` naming it and its `owed_state.truncated` reset (the judge ORs it back), in one batch; then `GAMES_SQL` 16 marks at a time, each batch `owed_judge::judge_subjects` (the shipped judge, its per-game delete, its 500 ceiling, its generation-bounded acknowledgement); then the cursor saved (`CURSOR_SQL`), the stamp (`STAMP_WRITE_SQL`) in the same batch once the page was short and this was its last identity. Counted `owed_backfill_identities_total`, `owed_backfill_rows_total` (the identity's stored rows when it finished), seeded by the overlay (`applayer_counters::OWED_BACKFILL`). An identity the wall cuts keeps its marks (the drain may judge them too) and its `open` row: the next pass resumes its marks and never re-marks it, so an identity bigger than a pass converges. A legacy row (an old worker's keyless whole-identity write) is rewritten with its keys, and one whose game no source names is judged away.

Risk 8.2's third repair: the owed drain first runs `owed_backfill::mark_aged` (`AGE_MARK_SQL`): the subjects of rows judged more than 24 h ago (`OWED_AGE_REMARK_AFTER_MS`), oldest first on `idx_owed_rows_judged` (overlay migration 396, partial on `judgedAtMs IS NOT NULL`), `source = 'age'`, never more than 128 PENDING at once (a subject already holding an age mark is skipped, and the insert takes only the room left under 128), (since the D2-OWED lens's MEDIUM-5 its first statement, `AGE_MARK_UNJUDGED_SQL`, takes the keyless rows, `judgedAtMs IS NULL`, over `idx_owed_rows_unjudged`, in the same 128) and the drain (`drain_dirty`) judges a subject marked only `age` after every other subject, so the repair fills the drain's idle room (its existing 64 a pass) and never delays a source's mark; the judge restamps the row, so each row is re-judged about once a day while the drain has room. Counted `owed_age_remarked_total`.

Risk 8.4's window rule: the belt's `CANDIDATES_SQL` reads `COALESCE(claimable, facts.claimable)` (`Candidate::claimable`), so a row an old worker wrote with a NULL column is read by its facts. Ruling 1's end: a backfilled identity has its `owed_state` row, so its read is the three statements and writes nothing; `mark_wanted` stays for an identity no backfill reached (a later `owed-v3` reuses it).

Pins `--test owed_backfill_sqlite` (`owed893_backfill_a` to `i`, the shipped pass on real SQLite under the overlay's migrations; each RED against its rule reverted alone: the stamp read removed `left: Armed`; no arming tick `left: Finished`; the gate's count ignored `left: Armed`; the cursor saved once a page `left: 1`; the open identity re-marked `left: (Finished, 2, 4)`; the age mark's generation bumped `left: 128`; the age mark unbounded `left: 150`; the pending bound dropped `left: 22`; age marks not drained last `left: 1`; the candidate without the COALESCE `left: [None, None, Some(0), None]`; a batch of every game `left: [41, 25, 9, 0]`) and `--lib owed893_backfill` (the wiring). The tier: `OWED_BACKFILL_ONLY=1 make ci-d1-budget` (`tools/lane-893/owed_backfill_pass.sh`, the READ lane's seed turned back into an old worker's rows, `ceilings.json` `owed-backfill-pass`, `owed-backfill-stamped`). Measured on local D1: the arming pass 3 statements, 1 row read; the backfill of the lived-in identity (270 games, 17 batches) and the one-game identity in one pass, 53,424 rows read, 5,575 written, 3,062 statements, the largest statement 1,291, about 2,970 rows a batch; the stamped replay 1 statement, 1 row. Stated for the lens: the rows a batch are the judge's commit over a 500-row identity (the 500-row cap DELETE, the per-game DELETE on the identity prefix and the `owed_state` COUNT, each O(the identity's rows)) and its hydration, so a whale's one-time backfill is about 3,000 rows per 16 games; not bounded here (the judge's statements are lane 2's; an `(identity, gameId)` index and a maintained count would bound them), and the age repair spends only the drain's existing per-pass room.

## The money lens's folds (D2-OWED-FOLD, 2026-10-25)

bsv-low `docs/audit/D2-OWED-LENS-2026-10-25.md` (NOT SOUND: 3 HIGH, 5 MEDIUM, 5 LOW, 5 NOTE), folded on this branch after its rebase onto main `8e5e73f7` (PR 171's #663 and #649, PR 172's tipless judgement and end-of-derive `degrade_stale_sweeps`, both kept). Pins in `crates/low-app-layer/tests/d2_owed_fold.rs` unless named; each RED on the base rule, quoted in its commit.

- **HIGH-1, #669 at the read** (narrowed by the delta lens's D-H1, below). The judge stores `probeTip` = the tip it judged at (NULL: no tip); `owed_read::Snapshot::body` runs `degrade_stale_sweeps` per claimable sweep that rests on a courier probe alone against it and the present tip, so a block since or an unreadable tip serves a pending chain-wait. A tip dates those claimable sweeps judged below it due now (`DATE_SWEEPS_SQL`, partial index `idx_owed_rows_sweep`) and re-marks rows judged with no tip (`MARK_UNTIPPED_SQL`, `idx_owed_rows_untipped`, 128 a tip). A read with a gate, a `probeTip` or a claimable sweep reads the present tip (the 30 s isolate memo; the delta lens's D-N1). Pins `d2fold_high1_a`, `_b`.
- **HIGH-2.** `MARK_TIP_SQL` marks `gateHeight BETWEEN tip - 6 AND tip` (the design's window); a gate the tip skipped waits for its source write or the age repair. Pin `d2fold_high2`, the 40-row fixture re-cut.
- **HIGH-3.** See the belt paragraph above (`belt_due`, the backoff, the bounded identity walk). Pins `d2fold_high3_a` (1,000 stranger rows due first, the honest look in the first tick), `_b`, `_c`.
- **MEDIUM-1.** The wanted envelope expands to the newest `OWED_WANTED_GAMES` (32) games, the rest marked `backfill`; `drain_dirty` walks up to 64 identities past its cursor (`owed_belt_state` `drain`, a recursive CTE of covering-index searches), reads each one's few oldest marks in one batch and takes subjects round-robin across identities, oldest first, `wanted` the tie-break inside an identity, age-only last. New indexes `owed_dirty(sourceKey)` and `(identity, markedAtMs)`; the wanted envelope and the age marks carry the constant keys `wanted` and `age` so the source-key index serves them, and `idx_owed_dirty_marked` and `idx_owed_dirty_identity` are dropped (no reader), so one wanted mark stays five physical writes (ruling 1, record 1277); no shipped drain statement scans `owed_dirty` (`d2fold_medium1_b`, and `d1_fleet_plans` now runs the shipped constants). Pin `d2fold_medium1_a`.
- **MEDIUM-2.** A filing whose judge cannot run lands its source statements ALONE (their marks wait for the drain), counted `owed_judge_filing_faults_total`; only that lone write's fault refuses the filing. No tip is no refusal (TIP-877). `refund_subjects` is the filer first, then the pot's verified seats only (`REFUND_SUBJECTS_SQL`), and the judge keeps its caller's order for the 16-subject cut. The handfile batch shares it. Pins `--lib owed893_app_filing_atomic_on_row_fault` (inverted), `d2fold_medium2_a`, `_b`, the handfile pin inverted, `d2fold_medium2_c`.
- **MEDIUM-3, the old walk against the judge.** (Since bsv-low #893 section 6.2 the walk is deleted: its stored rows are frozen in `tests/fixtures/old_walk_893` and `owed_walk_frozen.rs` compares the judge with them; the macro expansion and `old_walk_native.rs` described here are gone.) The old walk's regions of `routes.rs` are `macro_rules!` expanded twice: in place over `worker` (aliases `WalkDb`, `WalkDate`, `WalkEnv`, `walk_read_soft`, `walk_views`, `walk_canonical_root`, `walk_counted_batch`: the same items) and in `old_walk_native.rs` over SQLite, a test clock and couriers that never answer. `--lib d2fold_medium3_the_old_walk_and_the_judge_store_the_same_rows` runs the walk (cursor, ring, probes, write) and the judge (the shipped drain, or 16 games a batch on the fleet) over the #638 fleet and three scenario fixtures and compares stored rows, word facts stripped: 1,213 rows, all equal (a cut walk and a list past the ceiling are compared on the rows both hold). It found, and the judge now does as the walk: `mark_pending_spender`, `mark_refused_word_rows`, `mark_reorg_grace_rows`; the released hop's opponent from its twin; a hop named under two games is one row, the oldest marker's (`HOP_COMPETITORS_SQL`; it was two upserts to one key, the last judge winning). Stated: the ring's own `markerless*` cursor facts are not compared (the ring is replaced by the evidence row); natively the couriers, the header recheck and `load_stored_beef`'s guard are shims, and an expired probe memo diverges by construction (the walk would re-ask a courier). The design's 20-seat live diff on beta is the window's, not run here.
- **MEDIUM-5 and the rollback.** The age repair also marks keyless rows (`judgedAtMs IS NULL`, `AGE_MARK_UNJUDGED_SQL`, first, inside the same 128 pending). For the window's runbook: a ROLLBACK of the app layer to the old walk writes keyless rows and the dirty triggers keep marking with nothing draining (`owed_dirty` grows; its statements are index searches since MEDIUM-1); a forward redeploy repairs them through the age repair at 128 subjects a pass (and the untipped re-mark at 128 a tip), the backfill stamp untouched. Pin `d2fold_medium5`.
- **LOW-1.** A judge faulting on one identity's backfill for `OWED_BACKFILL_IDENTITY_FAULTS_MAX` (3) passes is recorded (`backfill:owed-v2:skipped:<identity>`, `owed_backfill_skipped_identities_total`), its marks left for the drain, the cursor moved past it. Pin `d2fold_low1`.
- **LOW-2.** `idx_owed_rows_identity_game`; triggers keep `owed_state.rows`; the judge's state row is created with one COUNT, the ceiling joins from it (under 500 rows the list is never visited). One subject's commit on a 500-row identity: 362 SQLite VM steps where the base's took 16,283 (`d2fold_low2`, which also holds the count exact through an old worker's write).
- **LOW-3.** `POST /internal/owed-backfill-pass?aged=1` runs `mark_aged` alone; the backfill tier ages its rows on local D1 and calls it (`owed-age-remark`).
- **LOW-4.** The filing's judgement takes the filed (newest) valid backup (`--lib d2fold_low4`).
- **LOW-5, the deploy order.** The overlay first; read its certified count on `/health` at or past `OWED_TRIGGERS_MIGRATION_COUNT` (409 since D2-OWED-FOLD-8); then the app layer. An app layer first would fault its drain and belt (`INDEXED BY` an absent index) and land filings alone (MEDIUM-2), refusing none.
- **NOTEs.** NOTE-1 (no stranger write derives a victim row), NOTE-2 (a word marks only rows naming it; `stale` reads the index's latch first), NOTE-3 (the eviction's evidence row is sound), NOTE-5 (the belt's evidence is the existing binds): stated, nothing to fold. NOTE-4, stated and not folded: `POT_UPDATE` fires on the admission UPSERT's UPDATE arm with unchanged values, so a re-submit of a known JOIN buys judges of the pot's parties (cost only); the lens's `WHEN` on changed columns needs every `pot_records` column the judge reads, and a column added later would be silently unmarked, a money risk the cost does not justify.

The migration list is 407 (fingerprint 1,328,337,484), up from 396: `idx_owed_rows_sweep`, `idx_owed_rows_untipped`, `idx_owed_rows_belt_turn`, `idx_owed_dirty_source_key`, `idx_owed_dirty_identity_marked`, `idx_owed_rows_unjudged`, `idx_owed_rows_identity_game`, `owed_rows_count_insert`, `owed_rows_count_delete`, and the drops of `idx_owed_dirty_marked` and `idx_owed_dirty_identity` (bsv-low's migration pin moves in its own commit before the prod promotion).

Measured on local D1 (the budget tier, re-recorded in `tools/lane-499/ceilings.json`; the CHAINTRACKS stand-in now serves the verified header tip PR 172 reads): `owed-belt-tick` reads 17,223 to 896, writes 162 to 260 (`-429` 709 to 266, 27 to 36); `owed-backfill-pass` reads 53,424 to 25,591, writes 5,575 to 7,344; `owed-backfill-stamped` 1 read; `owed-age-remark` (new) 286 reads, 296 writes, 73 subjects under the 128 bound; `owed-fleet-read` unchanged (2,000 reads, 3 statements); `owed-wanted-read` 5 writes; `owed-judge-filing` reads 118 to 126, writes 36 to 42, statements 59 to 61. The writes added are the fold's `owed_rows` index entries and the two row-count triggers: reads, which #638 budgets, fell by half or more on the belt and the backfill (the filing path reads 8 more, its state row created with one count), and writes rose by a third or more on each judging path, stated.

## The delta lens's folds (D2-OWED-FOLD-2, 2026-10-25)

bsv-low `docs/audit/D2-OWED-DELTA-2026-10-25.md` (HOLD: one NEW HIGH, three LOWs, two NOTEs), folded on `lane/D2-OWED-FOLD-2` from `d521888f`. Pins in `crates/low-app-layer/tests/d2_owed_delta.rs` unless named; each RED on `d521888f`, quoted in its commit.

- **D-H1, the index-unspent press (the captain's ruling).** The owner's own stranded hop that the INDEX holds unspent (`HopStatus::Unspent`, a verified marker; the stored `status` fact every hop row already carries) is pressable on the index's word: the chain rung is a downgrade and a re-probe trigger, never a precondition of the honest press, because a dead sweep costs a refused broadcast of the owner's own output, never an honest party's sats, and heals on the next word; the explorers are break glass only, and a stranger buys only their own identity's walk. So a block neither withholds it at the read (`owed::state_index_unspent_sweep`: the press stands, `tipMoved` or `tipUnknown`, `snapshotTip`, `chainProbeTip`, `servedTip` stated, main's #808 (b) shape) nor dates it at the belt (`DATE_SWEEPS_SQL` dates only a claimable sweep whose `status` is not `unspent`, the few that rest on a courier probe alone, which the read still degrades until their look). The judge asks no explorer; the belt stays the one caller, 8 outpoints a 120 s tick by identity in turn; a KNOWN competitor (the index's spender, the eviction's released spend, a belt find) downgrades the row at the write. As first folded, every claimable sweep was re-dated fleet-wide at every block and withheld until the belt's look, so K strangers' own sweeps held every honest press about K/8 ticks after each block. Pin `d2delta_h1_*` (64 stranger identities: RED `left: 66 right: 1`); `d2fold_high1_a` and `_b` re-aimed at a courier-dependent sweep (their own commit). The tier `owed-tip-mark` (`OWED_BELT_ONLY=1 make ci-d1-budget`, after the belt's cells: 1,000 strangers' index-unspent sweeps and 40 courier-dependent ones planted, one block): reads 1,052 (the date's range scan reads every claimable sweep judged below the tip), writes 120, 4 statements, no courier call; `d521888f` writes 3,120 and dates all 1,040. Since D2-OWED-FOLD-3 (2026-10-26) `idx_owed_rows_sweep` carries the statement's own status term (`... AND claimable=1 AND json_extract(facts,'$.status') IS NOT 'unspent'`, written as the statement writes it, so SQLite admits its `INDEXED BY`; edited in place, migrations 396 to 407 shipped nowhere): the date reads only the rows it writes, 52 reads and maxReads 40 (it read 1,052 and 1,040), writes 120; pin `--test d2_owed_fold3` (1,856 VM steps at 10 and at 1,000 index-unspent sweeps; RED on 8396f8b2, 2,060 against 9,980).
- **D-L1.** The belt's cursor saves the last identity given a turn (it saved the last walked, 64 walked for 8 served), each turn taken in walk order (it was by due time inside a turn), as the drain does: no identity holding a due row is passed unserved. Pin `d2delta_l1_*` (RED `left: [32, .., 60] right: [0, .., 28]`).
- **D-L2.** `look_shape` no longer compares the judged tip (the find's judge stamps the present one, so every block reset the period); an unanswered look waits as long again as it has since its judgement (`unanswered_period`, 300 s to 6 h). Pins `d2delta_l2_*` (RED `left: 600000 right: 1200000`; `left: 300000 right: 600000`).
- **D-L3.** The wanted envelope's newest 32 games are ordered in milliseconds (`createdAt * 1000`, `evictedAt`, `updatedAtMs`; `evictedAt` is milliseconds, so a released hop's game always sorted newest). Pin `d2delta_l3_*` (RED `left: ["g31"] right: ["old"]`).
- **MEDIUM-3's gaps.** (`old_walk_text_diff.py` retired with the walk's text, #893 section 6.2.) `python3 tools/lane-893/old_walk_text_diff.py 8e5e73f7` diffs the old walk's six regions against main's `routes.rs` line for line: 3,282 lines, 53 alias edits, no logic line (exit 1 on one). `--lib d2delta_medium3_a_filing_judged_by_the_write_path_*` compares a filing that enters through `record_post` and is judged by `judge_with_write` with the walk's rows. The word facts are not a gap: a stored word fact is never served; the read joins the words identically from roles that are the compared facts' own txids (stated in the pin's doc).
- **NOTEs.** D-N1 corrected here: the read reads the present tip only for a row with a gate, a `probeTip` or a claimable sweep (the fold's "every read with a row" was too wide). D-N2 (a lost acknowledgement re-runs a filing's content-keyed source statements alone): stated, nothing to fold.

No migration moved (the list stays 407, fingerprint 1,328,337,484; since D2-OWED-FOLD-3's in-place scope of `idx_owed_rows_sweep`, 2,691,125,399). Every other owed tier re-ran unchanged (`owed-belt-tick` 896 / 260 / 313, `-429` 266 / 36 / 29, `owed-fleet-read` 2,000 reads, `owed-wanted-read` 5 writes, `owed-backfill-pass` 25,591 / 7,344 / 3,097, `owed-backfill-stamped` 1 read, `owed-age-remark` 286 / 296 / 3, `owed-judge-filing` 126 / 42 / 61).

## The second delta lens's folds (D2-OWED-FOLD-4, 2026-10-26)

bsv-low `docs/audit/D2-OWED-DELTA-2-2026-10-26.md` (LAND: one new MEDIUM, one LOW, two NOTEs), folded on `lane/D2-OWED-FOLD-2` from `6ec6eb9a`. Pins in `crates/low-app-layer/tests/d2_owed_delta2.rs` unless named.

- **D2-M1.** The belt's first class (the block-dated courier-only sweeps over `idx_owed_rows_sweep`) and the due-only walk: the belt paragraph above. After a block an honest courier-only sweep is looked at within `ceil(C / 8)` ticks, `C` the courier-dependent rows the block dated, whatever the strangers' index-unspent rows. Pins `d2delta2_m1_a` (640 strangers dated, not due: the honest look on the first tick) and `d2delta2_m1_b` (that class before older back-off re-looks), both RED on 6ec6eb9a. The new statement is in `d1_fleet_plans`' owed list on its index. Tier `owed-belt-tick` reads 896 to 898, statements 313 to 314; `-429` 266 to 268, 29 to 30.
- **D2-L1.** The read withholds a press over a pending downgrade: the `GET /owed` paragraph above. Pin `d2delta2_l1_*`, RED on 6ec6eb9a. `owed-fleet-read` unchanged.
- **D2-N1.** The D-H1 pin split into `d2delta_h1_a` to `_d` (`tests/d2_owed_delta.rs`): (a) and (b) RED on d521888f; (c) and (d) were d521888f's own behaviour and are RED against a mutant each (the find's facts not applied in `judge_with_write`; `state_index_unspent_sweep` without its status test).
- **D2-N2.** `owed_walk_frozen::pins::d2delta2_n2_*`: a press (the seat's own index-unspent hop, its sweep filed through `record_post`, judged by the write path `claimable: true`) stored byte for byte as the old walk stores it.

No migration moved (the list stays 407, fingerprint 2,691,125,399).

## The third delta lens's folds (D2-OWED-FOLD-5, 2026-10-26)

bsv-low `docs/audit/D2-OWED-DELTA-3-2026-10-26.md` (LAND: one new MEDIUM, one LOW, three NOTEs), folded on `lane/D2-OWED-FOLD-2` from `e53c1406`. Pins in `crates/low-app-layer/tests/d2_owed_delta3.rs` unless named.

- **D3-M1.** No class owns the belt's tick: the belt paragraph above (`class_places`, 2 of 8 reserved for the second class whenever it holds a due row; the first class within `ceil(C / 6)` ticks and the second within `ceil(D / 2)` under a flood of the other). Pin `d3m1_a` (96 planted courier-dependent rows over 24 identities sorting first, one honest young index-unspent hop due: the first tick looks at it and serves the press; RED on e53c1406, the first class took all 8), `d3m1_b` (the yield, both directions).
- **D3-L1.** The dirty mark carries its class (`owed_dirty.downgrade`, the CREATE edited in place, the app layer's byte-identical CREATE with it; a re-mark keeps the larger, MAX): the `GET /owed` paragraph above. Of the 28 marking statements only `owed_dirty_pot_records_insert` (arriving spent; every admission since D4-L1), `_update` (a spend column changed, or the key moved), `_delete` and `owed_dirty_pot_evictions_insert` / `_update` write the downgrade class. Pins `d3l1_a` (the shipped discovery stamp and a no-op facts fill withhold nothing; a competing spender withholds until the drain, a later stamp keeping the class; RED on e53c1406: `"pendingSource":"pot"`, `left: Bool(false) right: true`), `d3l1_b` (each trigger's class; a pot delete's downgrade).
- **D3-N1.** `owed_walk_frozen::pins::d3n1_a_*`: a press that RESTS on its filing (a refund backup filed through `record_post` for the seat of an indexed pot whose funding the network holds, gate open: no press before it, `refund-due` `present-refund` `claimable: true` by the write path after it), stored byte for byte as the old walk stores it; RED under the lens's mutant (the filing's facts dropped from `judge_with_write`).
- **D3-N3.** The first class's due set is a seek: the belt paragraph above. Pin `d3n3` (168 VM steps at 10 and at 1,000 re-armed rows of the class; the plan a seek on `(probeTip=? AND beltDueAtMs<?)`; RED on e53c1406, `probeTip<?` alone). The tip's date statement stays flat in the index-unspent sweeps (`d2fold3`, 3,024 VM steps at 10 and at 1,000; it was 1,856: the index now holds the due time, so the date rewrites each dated row's entry).
- **D3-N2.** The tier's `owed-belt-tick-fleet` (`OWED_BELT_ONLY=1 make ci-d1-budget`, after the tip mark's cell): over 512 dated identities beside a first-class flood and one second-class row due, one tick reads 2,180 rows (the walk's statement 1,537), writes 54, 178 statements, 16 calls; the flood holds 7 places and the second class its place. At 720 ticks a day a full-window tick is about 1.57 M rows read a day (the walk about 1.11 M), near 8 % of prod's 20 M; a tick whose window holds fewer dated identities reads less. Re-measured: `owed-belt-tick` reads 898 to 900, `-429` 268 to 270, `owed-tip-mark` reads 52 to 132, writes 120 to 160, maxReads 40 to 120 (D3-N3's key; still only the 40 rows it writes).

The migration list stays 407; the fingerprint moved in place twice, each re-recorded in its own commit: 4,115,140,776 (`owed_dirty.downgrade` and the triggers' classes), then 2,009,384,664 (`idx_owed_rows_sweep`'s key).

## The fourth delta lens's folds (D2-OWED-FOLD-6, 2026-10-26)

bsv-low `docs/audit/D2-OWED-DELTA-4-2026-10-26.md` (LAND: one new LOW, three NOTEs), folded on `lane/D2-OWED-FOLD-2` from `ae41223c`.

- **D4-L1.** Every pot admission is a DOWNGRADE mark (`derived_owed::POT_INSERT`, class 1 always, edited in place; it was 1 only for a row arriving spent): a new pot row is a spend of its inputs, so a JOIN admitted withholds its seats' sweep presses (the `GET /owed` paragraph above) until the judge reads the hops it consumed; a pot row is inserted once a game. The mark reaches a seat whose hop the index does not hold through its party filing for the pot (`pot_subjects!`' party arm); a party filed AFTER the admission writes a plain `party` mark (stated, as ruled: only the admission's class moved). Pin `--test d2_owed_delta4` `d4l1_a` (a courier-only hop, the seat's party filed, the JOIN admitted through the overlay's `store_record_sql` and its stored BEEF: the press withheld, `pendingSource: "pot"`, until the drain, whose judgement reads the hop spent by the held JOIN, #837's belt; RED on ae41223c: `left: Bool(true) right: false`).
- **D4-N3.** A word refusing its transaction is a DOWNGRADE mark (`derived_owed::word_class!`, the four `tx_words` triggers, edited in place): a chain-tier `rejected` or `conflicted`, or #598's producer mark `trackerTerminal` on a word that is not `mined` (read behind `json_valid`), exactly what `owed::funding_refusal_word` reads; every other word stays plain. So a refund press over a funding Arcade refused is withheld at the read until the judge serves FIX-831's `funding-not-held`. Pin `--lib d4n3_a` (`old_walk_native::pins`: a refund backup filed through `record_post` over a SEEN-latched pot, then Arcade's REJECTED callback stored through the shipped producer, `owed::tests::produce_words`, the tracker keeping `seen`; RED on ae41223c: `"claim":"present-refund","claimable":true`, `left: Bool(true) right: false`). `d3l1_b` amended (the word triggers' class is the refusal, never a constant).
- **D4-N2.** The default `make ci-d1-budget` runs the belt tier (`tools/lane-893/owed_belt_tick.sh`, under the recipe's seal, on `LANE_BASE + 20` to `+ 22`) after its last cell and before the egress guard: `owed-belt-tick`, `-next`, `-429`, `owed-tip-mark` and `owed-belt-tick-fleet`, each against its ceiling; measured alone 84 s warm. `OWED_BELT_ONLY=1` still runs it alone. Stated: on this branch the default tier stops before it, at 32 reds in `tools/lane-499/d1_budget_route_ci.mjs`'s owed and hook cells, the same 32 (and the same 40 passes, the same cost table) on `ae41223c`: those cells encode the old walk (`/owed` computing inline, the hooks' walk ceilings), stale since the READ lane's read-only route; their re-aim is outside this fold.
- **D4-N1**, stated, no pin: every block re-dates the whole first class due now; the 256-tip walk binds only after about 43 hours of unanswered looks, and a row it misses is the second class's.

The migration list stays 407; the fingerprint moved in place twice, each re-recorded in its own commit: 1,860,122,119 (`POT_INSERT`), then 65,748,599 (the word triggers' class). Tier (`OWED_BELT_ONLY=1 make ci-d1-budget`), unchanged: `owed-belt-tick` 900 reads / 260 writes / 314 statements / 16 calls, `-next` 902 / 254 / 315, `-429` 270 / 36 / 30 / 3 calls, `owed-tip-mark` 132 / 160 / 4, `owed-belt-tick-fleet` 2,180 / 54 / 178 / 16 calls (walk 1,537).

## The evictions made before the writer get their evidence (D2-OWED-FOLD-8, 2026-10-26)

bsv-low `docs/audit/D2-OWED-FOLD-7-FINDING-2026-10-26.md`: an eviction made BEFORE the eviction writer existed has no `released_hop_evidence` row, the judge's only source for a hop whose marker is missing or unverified, and the backfill visited live party and hop identities only. On the captured beta eviction `cfce07ce...` the twin-only seat 030d9e9f... had NO row where the old walk served its 20,190 sat hop sweep (`hop-stranded`, `sweep-hop`, `claimable: true`); p28 was served the planted marker's "does not verify" row where the old walk served `spent-elsewhere` (the old walk was right: the twin's verified party, the committed key and the JOIN's bytes prove the hop, and `merge_markerless_hops` lets verified evidence replace a stranger's marker once the evidence row exists); f1 lost its could-not-judge row; f2 kept an old worker's press (`claimable: true`) where the walk serves two could-not-judge rows. The ruling: not accepted.

- **The evidence phase** (`owed_backfill::evidence_phase`, first in `backfill_pass`, before any identity): every OPEN eviction naming no evidence row for any released outpoint (`EVICTIONS_SQL`), 32 a page past its own cursor (`owed_belt_state` `backfill:owed-v2:evictions`, done at `~`), judged by the WRITER's three checks: its two statements byte for byte (`EVIDENCE_INSERT_SQL`, `SEAT_KEYS_SQL`, pinned equal to the overlay's through `admit_fast`) over the evicted twins, and its check 3, now ONE function both call (`overlay_discovery::pot::released_evidence::evidence_writes`), over the JOIN's stored bytes read in the writer's order (`transactions`, `pot_beefs`, `transactions_evicted`; the shadow's absence is no bytes). The cursor is saved in each eviction's batch; the phase runs once. An eviction whose reads or write batch fault is counted across passes (`backfill:owed-v2:eviction-faults`; the count's write raced by the pass's deadline, so a count the wall falls on lags and the bound comes later, never earlier: D2-OWED-FOLD-10, NOTE-3); at its third faulted pass it is recorded (`backfill:owed-v2:skipped-eviction:<txid>`, `{"reason","faults"}`, `tickedAtMs` the skip's time), counted `owed_backfill_skipped_evictions_total`, named on the pass's line and the lever's answer, and passed, the phase going on (D2-OWED-FOLD-9). No skip is permanent (D2-OWED-FOLD-10, the sixth delta lens's NEW-L1; a fault is transient, a lost press is not): the age repair the drain runs on every scheduled pass (`owed_backfill::age_repair`, behind `mark_aged`) reads the skipped evictions oldest skip first (`SKIPPED_EVICTIONS_SQL`, a range of the state's key, at most 32 named on its `[owed-backfill] skipped evictions` line and the `?aged=1` lever's answer while skipped) and retries each a day after its skip (`OWED_SKIPPED_EVICTION_RETRY_AFTER_MS`, the repair's day), at most 4 a pass, by the phase's own `evidence_one` (its reads and statements, one write batch that also ends the skip; the phase's cursor never moves); a retry fault is counted on the skip's row with its time kept, and the round's third skips it again from now (counted, logged), so a fault that clears is judged within a day of its next round; a skip whose eviction no longer needs its evidence (readmitted, or the writer wrote it) is ended without a retry. The phase's page and the retry read one predicate (`eviction_unwritten!`, `EVICTIONS_SQL` byte for byte as shipped). The missing read honours evidence only under its open ledger at its own `evictedAt`, ONE predicate with `RELEASED_SQL` (`HONOURED_EVIDENCE`, pinned by text: NOTE-1; both plans pinned in `d1_fleet_plans`, with `GATE_SQL`: NOTE-2); a released spend with no `table` field is counted (`tablelessSpends`) and its eviction logged by txid. Pins `--test owed_backfill_sqlite fold9_l1`, `fold10_l1_a` (RED on 237ae72b: the healthy eviction never judged, `left: 0 right: 1`), `fold10_l1_b`, `fold10_l1_c`, `fold10_n1`, `fold10_n3`.
- **Never silence** (`released_hop_evidence_missing`, overlay migrations 408 and 409, the table and its dirty trigger; `MISSING_INSERT_SQL`): a JOIN whose bytes are absent or undecodable, or do not carry a released hop's output (`bytesRead` 1), names each released outpoint per verified seat (check 1's seats; a ledger entry with no `table` field is a `pot_records` spend, as the walk read it). The overlay's eviction writer writes it too, so a new eviction is never silent either. The judge reads it (`hydrate::MISSING_SQL`, under the open ledger row) as the old walk's could-not-judge row (`evidenceMissing`, `markerlessReadFault`, `claimable: false`, the walk's two reasons), merged by the walk's own rule (`merge_markerless_faults`); an outpoint the seat's own evidence names (an evidence row, its own pot, its verified hop marker) is left out at the read, so a game judged alone never lays the fault over the row its pot is judged from (`owed_rows` is keyed by identity and outpoint).
- **The identities**: `IDENTITIES_SQL` adds the evicted twin's verified seats of an open eviction (`sigValid = 1`, a settle key, the #610 rule) on `idx_evicted_party_identity_game`; ONE cursor over the union of the three sources in identity order, a twin-named identity named by its identity as the others; `MARK_SQL` and the wanted expansion name the missing table's games. The stamp means what it meant: every identity visited, now after the phase. The gate is 409.
- **Stated.** The writer's rule, kept: only the evicted twin's seats are evidence (the old walk also took a late live party row of the evicted pot). A seat whose twin rows disagree on game or key names nothing (the walk faulted it under its first seat). An unverified hop marker of the seat in ANOTHER game naming a missing outpoint is not excluded at the read: the two games' rows share one key and the later judge's stands (the walk replaced it with the fault). Counters `owed_backfill_evictions_total` (a landed retry counts one), `owed_backfill_evidence_candidates_total`, `owed_backfill_evidence_missing_total` count what the phase issued, `owed_backfill_skipped_evictions_total` each skip and each skip again (D1's `changes` counts the dirty trigger's writes too: measured doubled), seeded by the overlay.
- **Cost** (the backfill tier, `OWED_BACKFILL_ONLY=1 make ci-d1-budget`, its seed carrying pair 13 by `OWED_SEED_PAIR13=1`; `?evidence=1` runs the phase alone): ten open evictions, 171 reads, 59 writes, 58 statements (17.1 reads, 5.9 writes, 5.8 statements an eviction), two evidence rows and four missing rows; cell `owed-backfill-evidence`. The pass after it judges six identities: 27,168 reads, 7,449 writes, 3,244 statements (`owed-backfill-pass`, re-recorded). Peak loopback connections during that tier: 12,090 (wrangler dev's D1 shim, about two per statement for about ten seconds).
- Pins: `--lib fold8` in `low-app-layer` (`owed_walk_frozen::pins::fold8_a`: the tier's captured pair 13 and its missing-bytes JOIN at the finding's clock, tip 971,000, marks cleared as rows written before the triggers; after the backfill and the drain the judge stores the old walk's rows for 030d9e9f, p28, f1 and f2 byte for byte, word and ring cursor facts stripped, and a game judged alone keeps f1's healthy row; RED on f3d78557 with the pin alone, the four rows quoted in its commit; RED against the phase skipped, the missing rows dropped, the own-pot exclusion removed. `fold8_b`: a twin-only seat's old-worker press judged away as the walk does, RED with the twin arm removed. `owed_backfill::tests::fold8_*`: the statements are the writer's), `--test owed_backfill_sqlite fold8_c` (the fleet's evictions visited once, 32 a page, the done mark, no second run, a readmitted one never read, the plans indexed), the overlay's `--lib fold8` (absent, undecodable and short-of-vout bytes name every released hop per seat, readable bytes none). The migration list is 409 (fingerprint 740,971,678); bsv-low's design doc paragraph and migration pin are bsv-low's to move.

## A released hop's verified marker binds its seat (bsv-low #893's first live finding, lane D2-OWED-HOME-MARKER, 2026-10-26)

Beta's courier-retire cell (record 1396): of three evicted JOINs whose p1 hop was swept to p1's committed home, hands 1 and 2 judged `payout` `courier-bytes` and hand 3 (game 39cf..., JOIN e494f0b5..., hop 8a5098bf...:0) `unbound` with `HOME_UNKNOWN_REASON`: its handfile filing never landed, so p1 had no party row, live or evicted, only its VERIFIED hop marker, and the eviction's evidence statements took check 1 from the twin party row alone (no evidence row, counted `released_hop_evidence_skipped_total`), and the judge's other road to the twin (`hydrate.rs`, the per-hop eviction read) is reached through the identity's evicted party row too.

- **The writer.** Check 1's attestation is ANY ONE of the identity's verified twin party row or its VERIFIED hop marker for the hop (register row D1's #610): `eviction_write::EVIDENCE_INSERT_SQL`'s seats gain a marker arm (`hopparty_records` by `idx_hopparty_hop` at `?2:?3`, `markerValid = 1`, one game and key per identity) that names only an identity with NO verified twin party row in the pot, so every party-bound seat is written exactly as before; checks 2 and 3 are unchanged (the twin committing K on exactly one seat, the JOIN's bytes paying hash160(K) at the hop; K reaches the candidates from the JOIN's own covenant output). `MISSING_INSERT_SQL` names a marker-bound seat whose JOIN's bytes cannot judge its hop. A hop with NEITHER has no identity to key a row by: it stays counted, nothing names it. The app layer's backfill copies are the same bytes (pinned).
- **The evictions made before it** (beta's hand 3 itself, prod's): the judge cannot reach them from the hop (`releasedSpends` is unindexed JSON; `pot_hop_seats` and `hop_pot_links` go with the evicted pot), so the drain's age repair (`owed_backfill::repair_marker_evictions`, after the skipped evictions' retry) walks the ledger ONCE by its primary key, `OWED_MARKER_REPAIR_WINDOW` (16) evictions a pass from its cursor (`owed_belt_state` `backfill:owed-v2:marker-evictions`, `~` once a window comes back short; done, one primary-key read a pass), and writes each eviction `marker_unwritten!` names (a released hop whose verified marker's identity has no evidence row) through the phase's own `evidence_one`. A candidate that faults is recorded as a skipped eviction (counted, logged), whose retry's `open` now reads the same predicate. The lever's `?aged=1` answer carries `markerRepaired`, `markerCursor`.
- Pins: overlay `--lib home_marker` (`home_marker_a`: the marker's K and game write the row, an unverified marker or one naming a key the hop does not pay writes none; `home_marker_b`: unreadable bytes name the marker seat in the missing table, neither is counted; both RED with main's statements: `left: [(.., "identity-1", "game", .., 20191)]`, the marker seat absent); app layer `--lib home_marker` (`home_marker_c`, hand 3's shape over the captured pair 13 (its seat 030d9e9f...'s party rows removed, its verified marker, a courier sweep to `payPkhA`): `payout` `courier-bytes`; RED with main's statements on the finding's own row, `left: ("unbound", None)`, `joinRefused: false`, `beltNextAtMs: null`, `spenderPkhs ["3e7bb94f..."]`, the home-unknown reason; `home_marker_c2`, the repair on a deployment whose phase is done: the finding's row, then the repair, then the payout; RED with the repair inert: `left: []`), `--test d1_fleet_plans` (the repair's two statements on their indexes).

## The owed events ride a queue; the judge runs only from its consumer (bsv-low #911, lane D2-ACTOR-A, 2026-10-27)

`DESIGN-D2-ACTOR-2026-10-26.md` (bsv-low) sections 3 and 11, the audit's HIGH-1, HIGH-2, MEDIUM-1, MEDIUM-7, MEDIUM-8, LOW-3 and LOW-5. A Cloudflare Queue `owed-events` (`low-owed-events` prod, `low-owed-events-beta` beta, each with its `-dlq`; `owed-events` in the generic overlay config): a PRODUCER binding `OWED_EVENTS` in the overlay and in the app layer, ONE CONSUMER in the app layer (`max_batch_size = 1`, `max_batch_timeout = 0`, `max_retries = 3`, then the DLQ), the DLQ consumed by the app layer too (park first, ack after, `owed_event_dead_letters`, at most 2,000; re-driven by the overlay's #576 lever with `{"family":"owed-events"}`). Create the four queues before a deploy; deploy the overlay first (migrations 470 to 473), then the app layer.

- **The event** (`overlay-discovery` `owed_event`, frozen by `vectors/owed_event_v1.json`, read by both Workers' tests): `{v: 1, seq, source, sourceKey, facts, atMs}`, keyed on the SOURCE ROW: `source`/`sourceKey` exactly the pair the row's `owed_dirty` trigger writes (`pot` `txid:vout`, `party` the marker's txid, `hop` `txid:hopVout`, `word` txid, ...). Two routing kinds beside the trigger pairs: `identity` (the hop hook's identity-only note) and `txfacts` (a recorded transaction's inputs and outputs decoded once at the write, MEDIUM-3; the consumer acknowledges it untouched, the actor of lane B mirrors it).
- **The publish** (`overlay-cloudflare/src/owed_events.rs`, compiled into the app layer by `#[path]` as `owed_events_publish`, one source): a writer NOTES its pair (`owed_events::note`; every `pot_changes::note` notes `pot`; the stores, the eviction, the readmission, the SEEN latch, the BEEF latches and the refusals note theirs); the invocation's flush point PUBLISHES: ONE D1 batch takes `seq` from `owed_event_seq` (`SEQ_BUMP_SQL`, one token per event, database-monotonic, never a rowid) and, in the same transaction, each row's facts and its mark count (`read_sql`); a pair with no mark is not sent; then one `sendBatch` per 100. A request publishes before its answer (`publish_before_answer`, bounded at 1.5 s, the overlay's dispatch exit and the app layer's door and hooks); `pot_changes::flush`, `flush_inline` and `flush_inline_within` publish first inside their task (so the queue replay, the cron and every detached `wait_until` writer). The word frames (`tracker_events`) ride as `word` events with their frame: they LEAVE the `/internal/tx-word` POST, its wall and its park; a frame whose publish faulted goes back to that POST. The pot-changed POST (the felt's announce, `admittedAt`) is unchanged (MEDIUM-7).
- **The consumer** (`low-app-layer/src/owed_events.rs`): a `word` event's frame is ingested first (the tracker's write, no judge); then `route` reads the pair's marks (`ROUTE_MARKS_SQL` on `idx_owed_dirty_source_key`; an identity route its identity's), expands a wanted envelope, judges at most 64 subjects through `deliver` (the ONE judge call lane B swaps for the actor), 16 a call, the judge's own batch acknowledging the marks (`ACK_DIRTY_SQL`: never before the judge wrote); more than 64 is a continuation it publishes. A judge fault leaves the marks and hands the message back. Each batch writes `owed_belt_state` `owed-events:consumer` (the oldest message's enqueue age, MEDIUM-8).
- **The sweep** (HIGH-2's second belt): the belt's 120 s alarm (and the scheduled drain) no longer judges: `owed_events::sweep` walks `idx_owed_dirty_backlog` past its cursor and re-publishes the pairs whose marks waited past one tick, 64 a tick, oldest first, wrapping. With no `OWED_EVENTS` binding every caller (the drain, the hooks, the door, the word write) judges as before. Since FIX-956 (bsv-low #956: 604 re-publishes in 6.7 quiet hours of beta, the consumer's lag past one tick read as "lost") a tick-old mark is re-published only once the consumer has PASSED it: the consumer row carries its horizon (`horizonMs`, the greatest publish stamp `atMs` among the messages a batch acknowledged, never lowered, `CONSUMER_ROW_SQL`), and the bound (`sweep_bound`, one bind) is the horizon less one tick (`SWEEP_HORIZON_MARGIN_MS`, the queue's re-order of retries) or the long bound `SWEEP_STALLED_AFTER_MS` (600 s: the consumer dead or stalled). Once a pass: a re-published pair is stamped `owed-events:swept:<source> <key>` and skipped until the horizon passes the stamp by the margin or the long bound passes it; the consumer's landed judge clears it. A pair a publisher KNOWS it lost (its send or seq batch faulted, the answer wall dropped it) is noted `owed-events:lost:<source> <key>` by the publisher's next batch (both Workers; the overlay's one `owed_belt_state` write) and re-published at the next tick whatever the horizon, the note deleted in that tick's batch; a note that never lands (the isolate ends first) is the long bound's. In Phase C only (`OWED_D1_JUDGE=false`) a direct call the actor answered applied acknowledges its games' marks up to the generation read before the call (`owed_actor::phase_c_ack_statements`, the consumer's Phase C batch); in Phases A and B it acknowledges nothing. Counted `owed_events_sweep_{passed,stalled,lost}_total`, `owed_events_lost_noted_total`, `owed_events_direct_acked_total`; `/health.owedEvents` serves `horizonMs`, `horizonAgeMs`. Pins `-p low-app-layer --lib s956` (a, b, c, d, f, g RED on `26f7443c` with the file cut at its marked line: the issue's lagging-consumer shape re-published 75 times for 40 marks there, 0 now).
- **The doors**: the record door and the handfile batch publish their filings' pairs (and a filed refund's and sweep's `txfacts`) and answer; the detached judge of PR 182 runs only without the binding. `Server-Timing: write;dur, enqueue;dur`; `/health.record.enqueueMsP50/95`. The pot-changed, hop-changed and tip-changed hooks publish (`pot`, `identity`, `tip` events) and never judge. Not moved here: the belt's own find (`judge_with_write`, one batch, lane B's `belt-ask`) and the one-time backfill.
- **Served**: `/health.owedEvents` {`publishedTotal`, `consumedTotal`, `unackedMarks`, `oldestUnackedMarkAgeMs`, `oldestEnqueueAgeMs`, `consumerAtMs`, `serviceMs`, `totals`} (the dirty snapshot `owed.dirty` reads, the consumer's row and the two totals, numbers, 0 for an absent row, by `owed_events::HEALTH_TOTALS_SQL`, two `ops_counters` rows, in one batch: D2-ACTOR-A-FOLD-4 and FOLD-5; bsv-low's `owed-queue-backlog` row reads published minus consumed there; `totals` points at the rest) and `/health/invariants.owedEvents` (every `owed_events_*` total, the consumer's row, `faultMemo` and `deadLetters` in ONE statement, `owed_events::HEALTH_STATE_SQL`); the counters `owed_events_*_total` (seeded); the cost bar's `worker_cost_daily.queueOps` (messages sent, `d1_ledger::note_queue_ops`).
- **The lens's folds** (bsv-low `docs/audit/D2-ACTOR-A-lens-2026-10-27.md`, lane D2-ACTOR-A-FOLD): MEDIUM-1 `max_concurrency = 1` on the consumer in both app-layer renders (`owed_events::CONSUMER_MAX_CONCURRENCY`; two judges of one identity could commit the first's stale rows over the second's with every mark acknowledged). MEDIUM-2 the per-pair FAULT MEMO (`owed_belt_state` `owed-events:fault:<source> <key>`: `faults`, `lastFault`, `heldUntilMs`): from the third consecutive fault the pair is held 120 s doubling to 6 h (the sweep leaves it out, the message is handed back with that delay), a landed judge clears it; the dead letters are ONE ROW PER PAIR (`parks`, `lastFault`, `status` parked / redriven / exhausted, `redrives`; LOW-4: exhausted after 3 re-drives, the lever marks `redriven`, the consumer deletes the row when the pair's judge lands); a pair faulting for good: 13 attempts and one row a day (2,880 and 720 before). LOW-5: a park that does not land is handed back after the letter's age, 60 s to 30 min (about 48 h over the 100 retries). MEDIUM-3 the #798 chain latch notes its latched rows as `refund`; `d2_actor_a_fold_m3_every_production_writer_of_a_trigger_table_notes_its_pair` holds each writer function of both Workers to its note (the old trigger pin renamed to what it proves). LOW-1 `configured()` (async) also needs the overlay's certified count at or past `OWED_EVENTS_MIGRATION_COUNT` (473). LOW-2 `queueOps` served on the cost bar. LOW-6 the consumer plans (marks) before the tip read (memoised 30 s); an identity note is sent only with a mark. LOW-7 `owed_events_publish_wall_drops_total`. LOW-8 the eviction's and the refusal's notes after their writes. LOW-9 no queue binding drops the notes. Served (since D2-ACTOR-A-FOLD-4 on `/health/invariants.owedEvents`): `faultMemo`, `deadLetters`, `faultHeld`, `sweepHeld`, `publishWallDrops`. The dead letters' CREATE was edited in place (migration fingerprint 751,808,187; bsv-low's migration pin moves with it). Pins `--test d2_actor_a fold_`, `--lib d2_actor_a_fold` (both crates), `cost_bar`.
- **The delta lens's folds** (bsv-low `docs/audit/D2-ACTOR-A-delta-2026-10-27.md`, lane D2-ACTOR-A-FOLD-2). NEW-M1: `judge_plan` re-judges a faulting chunk ONE SUBJECT AT A TIME; a subject whose judge faults alone is skipped, its mark kept, its fault on its OWN memo (`owed-events:fault:subject <identity>/<game>`, held from the third fault, 120 s doubling to 6 h, skipped unjudged while held), the rest judged and acknowledged; the message is `Err` (the pair's memo and hold) only when nothing landed, and the sweep leaves out a pair whose every subject read is held, so one poisoned subject never stops a shared pair (`age`/`age`, `tip`/`gate`, `tip`/`untipped`, `backfill`/`owed-v2`). LOW-A: each message raced against `CONSUMER_MESSAGE_WALL_MS` (60 s; handed back in 60 s, its pair's memo noted, `owed_events_consumer_wall_drops_total`), the batch's longest service time served `serviceMs`. LOW-B: a `tip` event reads the tip fresh, never the 30 s memo. LOW-C: the lever returns a `redriven` letter older than an hour to `parked` (`redrivenAt`, the CREATE edited in place, fingerprint 1,823,919,051, count 473), and `POST /internal/discard-dead-letters` takes `{"family":"owed-events","letters":[{"source","sourceKey"}]}` (1 to 50, `parked` or `exhausted` only, the marks stay). NOTE-3: the gate's pin holds the publish's statements inside the first 473 migrations (`>=`, not equality). Pins `--test d2_actor_a fold2_` (`m1` RED on `7ad5279f`: "age/age: one poisoned subject failed the message"; the sweep's rule made inert RED: "a pair of held subjects is not re-published").
- **The second delta lens's folds** (bsv-low `docs/audit/D2-ACTOR-A-delta-2-2026-10-27.md`, lane D2-ACTOR-A-FOLD-3). NEW2-M1: the plan's mark reads (`ROUTE_MARKS_SQL`, `ROUTE_IDENTITY_SQL`) leave out a held subject's marks in the statement (its memo probed by key), so held subjects never fill the 64-game cap; `ROUTE_HELD_SQL` counts them in the same batch; `continues` (more marks AND a subject judged) is the one continuation rule, and an all-held message is acked with its marks standing, no continuation, counted `owed_events_all_held_total` (`allHeld`). LOW-1: the wall notes the subjects of the judge call in flight, the pair only with none. LOW-2: a memo whose hold ended more than 6 h ago counts from 0 (`stale_memo_faults`). The rate stays `max_concurrency = 1` (the captain's ruling; lane B's actor owns it). Pins `--test d2_actor_a fold3_` (`m1_a` RED on `7bb95ea2`: `left: (0, 0, 64, true) right: (1, 0, 64, false)`); the tier cell `tools/lane-911/poisoned_subject.mjs` (in `ci-d1-budget` after the belt tier).
- Pins: `-p bsv-overlay-cloudflare --lib owed_events` (`d2_actor_a_a_*`: the 29 triggers read from `derived_owed_triggers.rs`, each write under real SQLite, the writer's pair equal to the trigger's, one event with the row's facts; `d2_actor_a_d_*` seq; `d2_actor_a_g_*`), `-p bsv-overlay-discovery --lib owed_event` (`d2_actor_a_f_*`), `-p low-app-layer --test d2_actor_a` (c, e, f, g and the door's wiring); the tier's record-door cell (`tools/lane-908/record_door.mjs`, the queue's producer and consumer in one Miniflare).

## The owed actor per identity (bsv-low #912, lane D2-ACTOR-B, 2026-10-27)

`DESIGN-D2-ACTOR-2026-10-26.md` (bsv-low) sections 4 to 8 and 11. `OwedActor` (`crates/low-app-layer/src/owed_actor/object.rs`) is a Durable Object, one per identity (`idFromName` of the LOWERCASED 66-hex key, LOW-1; any other key is refused before a stub is made), its own SQLite: binding `OWED_ACTOR`, migration tag `v4-owed-actor`, `new_sqlite_classes = ["OwedActor"]` (appended, never renamed), KV `OWED_COPY` (placeholder ids). No public request reaches it: the router forwards none; the consumer, the writers, the read, the belt and the wipe's bearer levers call it by binding.

- **The mirror** (`owed_actor.rs`, `FAMILIES`): the record's 24 tables the judge reads, their DDL generated from the overlay's migration list (`owed_actor/schema.rs`, `@generated`, `#[rustfmt::skip]`; pin `d2_actor_b_the_mirror_is_the_overlays_schema`, `OWED_ACTOR_SCHEMA_WRITE=1` writes it; the actor holds `actor_digest()` in `meta`, the generated digest and `ACTOR_TABLES_VERSION`, the version of its own `txfacts`, `applied` and `meta`: a change of either REBUILDS the actor, below), plus the judge's own `owed_rows`, `owed_row_words`, `owed_state`, `owed_dirty` and the row-count triggers of `owed_rows` alone. An apply refreshes (identity, games) by a KEY CLOSURE: each family copied from D1 by its indexed key columns (the identity and its games, txids, hop marker rowids, the probe memo's ranges), the keys taken from the judge's own statements (binds and answers, `Noting`) and the rows copied, round after round, to a fixpoint (at most 8 rounds, 4,096 txids; past either the apply is a fault); each round's domain replaced whole (a row the record deleted leaves the mirror). The closure starts from what the mirror already names for the games (closed locally) and the last judgement's txids, so a steady-state apply is ONE round: one D1 batch of 24 SELECTs, one local write, one judgement. ONLY THE FIXPOINT ROUND'S JUDGEMENT IS WRITTEN (the D2-ACTOR-B lens's HIGH-2): each round's judge runs over `Noting`, which STAGES the judge's commit (the batch holding `GAME_DELETE_SQL`: its binds noted, answered as an empty judgement) instead of running it; at the fixpoint `commit_fixpoint` copies the identity's judged rows aside (`owed_rows_snap`, `owed_row_words_snap`, `owed_state_snap`, `owed_dirty_snap`, the whole identity, since the commit's ceiling may take another game's row), runs the round's staged commits in ONE batch and drops the copies; a fault of that batch restores them (a DO statement that faults ends its batch and its earlier statements stand; workers-rs exposes no `transactionSync`). Each statement runs synchronously on the object's SQLite, so `/rows`, `/stored` and the KV copy read the rows before the apply or its fixpoint's, never a round over a partial mirror or a commit cut half way, and a faulted apply (a record fault, the 8 rounds, the 4,096 txids, the commit) leaves them exactly as they stood. As built at `8af15944` every round committed: a fault on round 2 of a fresh actor kept round 1's judgement (the lens's probe 3: 10 of 10 fixture identities, 4 holding claimable rows the D1 judge does not). Its cost: the identity's judged rows copied twice per apply that judges (about 500 rows at the ceiling, local SQLite).
- **No bytes held** (MEDIUM-3): `JudgeDb::tx_facts(txid, FactsFrom::{PotBeefs, Stored})` (`owed_judge/hydrate.rs`): the D1 judge decodes the stored BEEF there (its reads unchanged: the JOIN's `pot_beefs` alone, a spender's first stored BEEF carrying it); the actor serves `txfacts` (decoded at the copy, or a `txfacts` event's, last-writer-by-seq) and stores an empty blob in the beef column.
- **The section** (MEDIUM-2): the object's turn (`apply_in_turn`, `apply_and_publish_in_turn`, one mutex) holds every apply, its outside reads (the word views' header rechecks, the canonical roots) and Phase C's ask publish included; `applied (kind, seq, game)` is the redelivery idempotency (the lens's HIGH-1: one event's games reach the actor in several calls, the consumer's chunks of 16 and its singles after a chunk fault, so a game is skipped only when the event itself applied it; as built at `8af15944` the key was `(kind, seq)` and the second chunk of an event answered `skipped_redelivery`, its games unjudged: the fleet whale stored 128 rows where the D1 judge stored 500); since the delta lens's NEW-H1 (b) a redelivery is APPLIED AGAIN, never skipped (the skip acknowledged a mark newer than the earlier delivery's apply, unjudged): `applied` records the event (`ackSeq`), the answer names the `appliedGames` of this call; each apply reads the record as it stands, so an out-of-order pair derives the same rows (MEDIUM-1).
- **The delivery** (`owed_events::deliver`): `OWED_D1_JUDGE` on (default, Phases A and B): the D1 judge writes `owed_rows` and the actor applies beside it, its rows compared per game (`owed_actor::beside`: `owed_actor_compared_total`, `owed_actor_diffs_total`, a sampled `[owed-actor] DIFF` line; a fault never fails the message, `owed_actor_faults_total`): Phase A. PHASE B (`OWED_ACTOR_READ` on with the D1 judge; the lens's HIGH-3, option (a), Phase C's rule): the read serves the actor's rows, so the marks that withhold a press over a pending downgrade (D2-L1) are acknowledged only after BOTH judges applied: the actor applies FIRST and its fault fails the delivery (the message handed back under lane A's fault memo, the marks standing, the read withholding the game's presses; `owed_actor_faults_total`), then the D1 judge, whose batch acknowledges the marks, then the compare (`owed_actor::actor_then_d1`). As built at `8af15944` the D1 judge acknowledged first and an actor fault was dropped, so the read served the actor's stale press with `pending` false until another event for that game. Option (b), `pending` from an ack watermark per game, was not taken: it needs a per-game seq in the actor and a second pending source at the read, where (a) is the rule Phase C already runs. Its cost: in Phase B an actor fault holds the message (and its D1 judgement) until the actor answers. Off (Phase C): the actor is the judge and the marks of the games it APPLIED (`appliedGames`) are acknowledged after it applied (`as_the_judge`; an answer applying none of them is a fault; as built every chunk's marks were acknowledged after the answer, a skipped chunk's included). The record door and the handfile batch call the identities' actors directly after their answer (`object::schedule_direct`, 2 s bound, `wait_until`; the queue stays the backstop; `owed_actor_direct_{calls,faults}_total`).
- **The belt's questions** (HIGH-3, Phase C). With the D1 judge off no D1 row is dated, so the actor PUBLISHES a `belt-ask {identity, outpoint, question, dueAtMs}` per dated row of the games it judged, inside its turn (`owed_actor::publish_belt_asks`): a row of the belt's own ledger `owed_belt_state`, `belt-ask:<identity> <outpoint>` (`question` the row as the belt reads a candidate, with the ask it owes: `outpoint`, `spender` or `home`; `tickedAtMs` the due time), rewritten only when its question moved, taken out when no dated row names it; counted `owed_actor_belt_asks_total`. The belt singleton stays the ONLY thing that asks a courier: its tick (`owed_belt::tick`, `!db.d1_judges()`) reads one page of at most 256 asks past its cursor (`belt:asks`, a range of the ledger's key), the due ones round-robin by identity up to 8, asks through its one gate (24 calls a tick, the rung circuits), writes the find's facts to the record ALONE, and hands the find back as a `belt-find` event (`JudgeDb::actor_apply`); the actor's closure re-reads them, judges and re-publishes its asks; the ask's own backoff and re-arm move its `tickedAtMs` (`ASK_DUE_SQL`). Counted `owed_belt_handed_back_total`; the due gauge counts the due asks. Phases A and B keep the D1 judge's dated rows as the belt's candidates, and since the lens's HIGH-3 a find reaches the actor there too (`actor_apply`, kind `belt-find`; it was judged only in D1 and reached the actor only when the sweep re-published its marks): in Phase A after the D1 judge's find batch, its fault counted (`owed_actor_faults_total`), never the find's; in Phase B (`phase_b_find`) the find's facts land ALONE, the actor applies them, then the D1 judge acknowledges, so an actor fault keeps the facts' marks (the read withholds; the candidate is re-armed). `owed_belt_handed_back_total` counts every phase's hand-back.
- **The reconcile** (MEDIUM-10). The exact first question (the un-acked marks re-published) is lane A's sweep on the belt's alarm; the sampled DIGEST compare runs on the same driver (`owed_actor::reconcile_step`, the alarm's `owed.reconcile` phase): one identity per 22.5 minutes (64 a day), in identity order over the least of four key seeks (`owed_state`, the party, hop marker and released evidence identities), wrapping; its state `reconcile:owed-actor`. Phases A and B: D1's stored rows against the actor's (`GET /stored`, no apply), a diff re-applies the differing games (`reconcile`); Phase C: the actor's rows against a re-apply of its games from the record (at most 256). The digest compares the judgement, not its clock (`compared_facts` drops `ageMs`, `chainProbeAgeMs`, `beltNextAtMs`, `asOfMs`). Counted `owed_actor_reconcile_{compared,diffs,republished}_total` (`republished`: the games a diff re-applied), logged `[owed-actor] RECONCILE DIFF`, served `/health/invariants.owedActor.reconcile` and `/health.owedActor.reconcileDiffs` (the durable count).
- **The read** (`GET /owed`, `OWED_ACTOR_READ`, off until E4): FIRST D1's state and pending marks (`owed_read::read_with(.., false)`, never D1's rows: the lens's LOW-5); an identity D1 does not know (no `owed_state` row, no party, hop marker or evicted party row: the existing indexed probe) is answered from D1 as before, NO actor called or minted, counted `owed_actor_unknown_reads_total` (MEDIUM-3; and the object's `GET /rows` on a fresh object writes nothing, no schema, no meta, no alarm). Then the actor's joined rows with every word column replaced by D1's `tx_words` (MEDIUM-4, `READ_WORDS_SQL`), D1's pending marks kept. An actor whose bootstrap is unfinished (fresh, REBUILT under a new digest, or reset on an era move) is armed if it is not (`/bootstrap`) and served its KV copy display only, or a 503 `pending` with no copy; an actor out of reach (1.5 s) the same (`owed_actor::fallback_body`, MEDIUM-1 and MEDIUM-2): a copy is a 200 in the owed wire's own shape through `owed_read::Snapshot::body` (`v`, the facts an object), every row `facts.claimable: false` with `facts.claimReason` and `facts.displayOnly`, the body `pending: true`, `refresh: "shed"`, `retryAfterMs` (bsv-low's `parseOwedBody`, run verbatim over the frozen `tests/fixtures/owed_actor_fallback_body.json`, keeps all 6 rows and offers no press); no copy is a 503 with `Retry-After` (the client keeps its last list: a 200 with `rows: []` would read "Nothing is owed", Rule 23). The actor writes the copy at most once a second, the rest coalesced on its alarm (MEDIUM-9), and once more when its bootstrap ends.
- **The rebuild** (the lens's HIGH-4; `object::schema_plan`): on a digest change the actor drops the mirror's families AND the judge's own tables (`owed_rows`, `owed_row_words`, `owed_state`, `owed_dirty`, their indexes and triggers with them; `CREATE ... IF NOT EXISTS` would keep an old shape, and each judge write naming a column the old `owed_rows` lacks would fault), `txfacts`, `applied` and the commit's copies, then recreates them; it clears `bootstrapped`, the ack, the gate, the judgement's time and the KV mark from `meta`, and where it knows its identity sets `bootCursor` 0 and its alarm (`rearm_after_rebuild`, at the next fetch or alarm), so a read serves `pending` in Phases B and C until the bootstrap ends. As built at `8af15944` only the families and `txfacts` were dropped: the October migrations moved `owed_rows` five times.
- **The tip alarm** (the design's MEDIUM-5; the lens's MEDIUM-4): the alarm judges the gates in `(meta.tipJudged, tip]` and the claimable courier-dated sweeps judged below the tip (`json_extract(facts,'$.status') IS NOT 'unspent'`, the D1 rule's D-H1), at most 1,024 games applied 64 at a time (`GATED_GAMES_SQL`), then stamps `tipJudged`; it arms at the NEXT gate's expected time (`NEXT_GATE_SQL`, `next_tip_alarm_ms`: a block of 600 s a height) or the next block while a courier-dated sweep is held, and arms nothing with neither (the hook's `tip` event stays the second belt). As built an opened gate was re-judged every ten minutes for as long as its row stood. **The push**: `owed-changed` through the relay with `judgedAtMs` and the event's `atMs`.
- **The wipe** (LOW-2; WIPE-REHEARSAL-3 section 2 step 4a): BEFORE D1 is wiped, page `GET /internal/owed-actor-identities?after=<next>` (bearer `INTERNAL_TOKEN`, 500 a page, `next` null at the end: every identity of `owed_state`, `potparty_records`, `hopparty_records`, `released_hop_evidence`), then `POST /internal/owed-actor-reset {"identities": [...]}` (the same bearer, 1 to 50 a call, answered `reset`, `skipped`, `faults`): each actor's `/reset` (`deleteAll` and its alarm). The alternative: a `deleted_classes = ["OwedActor"]` migration with the class re-added under a fresh name and tag.
- **Served** (the lens's MEDIUM-7 and MEDIUM-8, one D1 statement, `owed_actor::HEALTH_SQL`): `/health.owedActor` {`applyMsP50`, `applyMsP95`, `applyMsMax`, `applyCeilingMs` (5,000), `rowsVisibleMsP95` and `rowsVisibleMsMax` (the QUEUE class: the path every write takes, the design's bar (2) restated as the queue's figure: the overlay's writers make no direct call), `rowsVisibleDirectMsP95` (the record door's direct call), `collectAfterMinedMsP95`, `oldestUnappliedEventAgeMs` (Phases A and B: the actor's OWN, the oldest write time of an event whose actor apply faulted with no later apply of that identity landed; Phase C: the fleet's oldest un-acked mark), `classes` {`direct`, `queue`, `mined`: sample counts, p95s, flush time}, `comparedTotal`, `diffsTotal`, `faultsTotal` (the per-delivery compare's durable totals: Phase B's gate, zero diffs and zero faults over the window), `reconcileDiffs`, `bootstraps` (durable, `owed_actor_bootstraps_total`) beside `activeIdentities` (`owed_state` rows holding rows: a hint only since the delta lens's NEW-L3), `coverage` (the bootstrap lever's last pass: Phase B's other gate), `unknownReads`, `epochResets`, `deferred` (started) and `deferredDone` (NEW-L6), `heldDiffsTotal` (NEW2-L1) and `tipFaults` (NEW2-L5), `v3`, `isolate` (this isolate's own samples), `kvCopies`, `readFlag`, `d1Judge`, `bound`, `flags` {`OWED_ACTOR_READ`, `OWED_D1_JUDGE`}}. The bars are the FLEET's: each answer's samples by class go into one row of the belt's ledger a class (`owed-actor:bars:<class>`, the last 256 of each meter), added by an isolate at most once a minute or every 64 samples (`flush_bars`: after the consumer's batch, after the deferred applies, after the direct calls); two isolates flushing at once can lose one's samples (a sample, never a figure the other did not have). Each DIFF line names its identity and game (`[owed-actor] DIFF <identity> game <game>: ..`). Each object serves its own on `GET /bars`. `/health/invariants.owedActor` serves the durable totals (`applayer_counters::owed_actor_view`; the counters seeded by the overlay, `OWED_ACTOR`, nineteen, pinned equal). Since D2-ACTOR-B-FOLD-6 (the land tier's `health-dirty-snapshot` refused 49 reads, 6 statements, the largest 44) `/health.owedActor` reads none of that per request: the WRITERS fold the class rows, the counters, the `owed_state` count and the coverage into ONE snapshot row, `owed_belt_state` `owed-actor:health` (`owed_actor::HEALTH_SNAPSHOT_SQL`, its cursor plus `asOfMs`), in the consumer row's batch, the meter flush's batch and the bootstrap lever's coverage batch, at most once a minute fleet-wide (the stamp compared inside the statement; a skipped write runs none of the composition; the coverage pass's last call writes whatever the stamp), and `/health` reads it as the second primary-key row of `owed_events::HEALTH_CONSUMER_SQL` (measured 7/7 reads, 5/5 statements, the largest 2/5). An old snapshot is served with its `asOfMs`; none yet serves the totals and `asOfMs` null. `/health/invariants.owedActor` is unchanged (the overlay's existing counter read). Pins `--lib d2_actor_b_fold6`.
- **The lens's MEDIUM folds** (bsv-low `docs/audit/D2-ACTOR-B-lens-2026-10-28.md`, lane D2-ACTOR-B-FOLD-2; pins `--lib d2_actor_b_fold2`, each RED against its rule reverted, `/Volumes/Lanes/out/wt-overlay-actor-b/fold2-red-mutants.txt`): MEDIUM-1 an actor's first event arms its bootstrap (`object::arm_bootstrap` in `apply_route`), `POST /internal/owed-actor-bootstrap` (bearer `INTERNAL_TOKEN`, `{"after": ..}`, 50 a call, every identity D1 knows) arms the rest in Phase A, a finished bootstrap is counted durably, and the bootstrap pages by keyset on `(at, gameId)` (`owed_judge::IDENTITY_GAMES_AFTER_SQL`, the lens's LOW-6); MEDIUM-2 the fallback bodies (above); MEDIUM-3 no actor for an identity D1 does not know (above); MEDIUM-4 the tip alarm (above); MEDIUM-5 THE ERA: `meta.epoch` is `STORAGE_EPOCH|WRITTEN_OFF_BEFORE_MS` (`owed_actor::actor_epoch`, the two app-layer render vars the wipe's era bump moves, WIPE-REHEARSAL-2 step 6), checked at every fetch but `/reset` and `/bars` and at every alarm (`object::check_epoch`); another era's actor is reset inside its turn (`deleteAll`, its alarm), stamped, counted `owed_actor_epoch_resets_total`, so an actor that re-hydrated from the pre-wipe D1 between step 4a's reset and the bump serves nothing of it (an actor minted before the stamp existed is stamped, not reset); MEDIUM-6 Phase A's apply and compare run AFTER the consumer's batch (`JudgeDb::defer_actor`, `owed_actor::run_deferred` under the queue handler's `ctx.wait_until`, at most 256 an isolate between batches, counted `owed_actor_deferred_total`), so the serial consumer's service time is the D1 judge's alone (native, debug, the fleet whale's 16 games: inline p50 1,091 ms, max 2,064 ms; deferred p50 22 ms, max 66 ms); its compare reads D1's rows at its own time, so a D1 judgement landing between can count a diff the next apply heals (stated); the event's `txfacts` are not preferred over the copy's decode (stated: the copy reads the BEEF rows either way); MEDIUM-7 and MEDIUM-8 (above); MEDIUM-9 THE ROLLBACK: `POST /internal/owed-v3-pass` (bearer `INTERNAL_TOKEN`, `{}` or `{"restart": true}`; 409 while `OWED_D1_JUDGE` is off: the D1 judge is turned back on FIRST) re-judges every identity's `owed_rows` with the D1 judge from D1's own evidence (never a copy of the actors' rows), 256 games a call, resumable on `owed_belt_state` `rollback:owed-v3` (the last identity done, the open one and its last game), the identities the reconcile's four-seek union, then stamps `owed-v3` in `view_derivation_state` (the manifest grants the app layer the stamp's DELETE for the restart alone), counted `owed_v3_identities_total`, `owed_v3_rows_total`. The LOWs folded: LOW-2 `applied` keeps a week (`APPLIED_PRUNE_SQL` in each apply's marks), LOW-4 the `owed-v2` lever refuses (409) while the D1 judge is off, LOW-5 (above), LOW-6 (above), LOW-7 the direct call splits its games 64 an apply.
- **The delta lens's folds** (bsv-low `docs/audit/D2-ACTOR-B-delta-2026-10-28.md`, lane D2-ACTOR-B-FOLD-3; pins `--lib d2_actor_b_fold3`, the REDs in `/Volumes/Lanes/out/wt-overlay-actor-b/fold3-red-*.txt`). NEW-H1, which marks an acknowledgement may remove: Phase B reads the marks' greatest generation BEFORE the actor's call (`owed_judge::marks_bound`, `owed_judge.rs:598`) and the D1 judge acknowledges at most it (`judge_subjects_acked_to`, `owed_judge.rs:570`; `owed_actor.rs:2128` `actor_then_d1`; the belt's Phase B find, `owed_belt.rs:893`), so a downgrade written while the actor applies stands for its next apply; Phase C applies a redelivery again (`owed_actor.rs:856`, the skip gone) and acknowledges up to the generation read before the call (`as_the_judge`, `owed_actor.rs:2209`); under `OWED_ACTOR_READ` no D1-only judge acknowledges: the drain and the hooks' drain judge nothing (`owed_judge.rs:1067`, `owed_judge/writers.rs:89`), the `owed-v2` pass stops `ActorRead` and its lever answers 409 (`owed_backfill.rs:757`, `:1278`; `?aged=1` still runs), the `owed-v3` pass acknowledges nothing (`owed_actor.rs:2410`). NEW-M1: the actor read's identity probe writes no wanted mark (`owed_read.rs:157`, `Snapshot::known`; `object.rs:1004` `known_to_d1`), so a player first seen after the Phase C flip reads with the mark count flat. NEW-M2: an INDEX-ONLY digest move keeps the rows (`object.rs:117` `schema_plan_kept` over `meta.shape`, `owed_actor.rs:604` `actor_shape`: every DDL statement but a plain `CREATE INDEX`); each bootstrap page takes a token of a FLEET budget first, 60 pages (960 games) a minute (`owed_actor.rs:622`, `boot_pace`, `owed-actor:boot-budget` in the belt's ledger, `object.rs:715`), one actor's pages 1 s apart. NEW-M3: every Phase C acknowledgement stamps `owed-actor-judged` and deletes `owed-v3` (`owed_actor.rs:2265`, `PHASE_C_MARK_SQL`); while the mark stands without the stamp the D1 read serves no press (`owed_read.rs:131`, `:273`, `ROLLBACK_PENDING_REASON`, `PENDING_SQL`'s two probes, no statement added). THE ROLLBACK OF PHASE C is two renders, three steps: (1) `OWED_D1_JUDGE=true` with `OWED_ACTOR_READ=true` (the consumer applies the actor, then D1 under NEW-H1's bound); (2) `POST /internal/owed-v3-pass` until `stamped` (it acknowledges nothing under the actor's read); (3) `OWED_ACTOR_READ=false`. A digest- or era-moving deploy in Phase C: page `POST /internal/owed-actor-bootstrap` to its end and read its coverage first. NEW-L1: the tip alarm moves `tipJudged` only when every apply of its window answered (`object.rs:323`). NEW-L2: the KV copy carries its era (`object.rs:792`) and a read serves no copy of another (`owed_actor.rs:1239` `kv_copy_of_era`). NEW-L3: Phase B's gate is COVERAGE: the bootstrap lever folds each call into `owed-actor:coverage` (`owed_actor.rs:1742` `coverage_fold`, `object.rs:1180`), served `/health.owedActor.coverage`: a pass `complete`, `contiguous` and `uncovered` 0. NEW-L6: `owed_actor_deferred_total` at each deferred apply's start (`owed_actor.rs:2101`), `owed_actor_deferred_done_total` at its end. Stated, not changed: NEW-L4 (the HIGH-2 copy is the whole identity per apply: `transactionSync` is not wrapped by workers-rs 0.8 and a games-bounded copy cannot restore a ceiling victim of another game; release p95 322 ms), NEW-L5 (Phase B's inline service time is the Linux tier's measurement).
- **The second delta lens's folds** (bsv-low `docs/audit/D2-ACTOR-B-delta-2-2026-10-28.md`, lane D2-ACTOR-B-FOLD-4; pins `--lib d2_actor_b_fold4`, the REDs in `/Volumes/Lanes/out/wt-overlay-actor-b/fold4-red-mutants.txt`). NEW2-M1 (closes NEW-M3 and M9): every Phase C acknowledgement deletes BOTH the `owed-v3` stamp and the pass's cursor row `rollback:owed-v3` (`owed_actor.rs:2421`, `OWED_V3_RESTART_SQL[0]` and `[1]`), so every rollback's pass starts from the first identity and re-judges every one before it stamps; the rollback still calls `POST /internal/owed-v3-pass` with `{"restart": true}` first (pin `m1`, RED on `e3e70b65`: "stamped having re-judged 0 games", `b3..00:0` served a press). NEW2-L1: Phase B's compare reads the games holding a mark above its acknowledgement bound (`HELD_GAMES_SQL`, `owed_actor.rs:2257`; `compare`, `:2265`) and counts their DIFFs APART, `owed_actor_compare_held_total` (served `/health.owedActor.heldDiffsTotal`, `/health/invariants.owedActor.heldDiffs`), its line `DIFF (held mark, NEW2-L1; not a STOP)`; `owed_actor_diffs_total` stays the STOP (pin `l1`, RED on `e3e70b65`: `left: 1 right: 0`). NEW2-L4: `digest` and `shape` are stamped only when every statement of the plan answered (`ensure_schema`, `object.rs:122`, called at `:425`); a failed one leaves them for the next instance's ensure (pin `l4`). NEW2-L5: the tip alarm moves `tipJudged` past its window always; a faulting game is found by applying a faulted chunk one game at a time, carried in `meta.tipFaults`, retried, PARKED at its third consecutive fault (`tip_window`, `owed_actor.rs:1380`, `TIP_FAULTS_MAX`; `object.rs:338`), each alarm with a fault counted once (`owed_actor_tip_faults_total`, `/health.owedActor.tipFaults`) and its games named in `[owed-actor] TIP WINDOW FAULT`; an apply that lands by another road clears the entry (`object.rs:673`) (pin `l5`). NEW2-L3: the bootstrap budget's take is ONE conditional upsert (`BOOT_BUDGET_TAKE_SQL`, `owed_actor.rs:630`, `boot_taken`, `object.rs:771`): a full minute's take writes nothing and answers no row, so N waiting actors are N one-row reads a minute of `owed-actor:boot-budget` and no write; the order stays by the identity's hash slot (pin `l3`). NEW2-L2 (stated): under `OWED_ACTOR_READ` D1's mirror is rebuilt by the `owed-v3` pass with `{"restart": true}`; the `owed-v2` lever answers 409 there.
- **The third delta lens's folds** (bsv-low `docs/audit/D2-ACTOR-B-delta-3-2026-10-28.md`, lane D2-ACTOR-B-FOLD-5; pins `--lib d2_actor_b_fold5`, the REDs in `/Volumes/Lanes/out/wt-overlay-actor-b/fold5-red-mutants.txt`). LOW-1: a tip-window fault counts toward the park only in an alarm where some apply LANDED (`tip_window`, `owed_actor.rs:1411`); an alarm with none is the actor's fault, its games carried at their count (a 130-game window under three whole-actor alarms parks none; was 130), the alarm still counted once; a game faulting alone among landed games is parked at its third such alarm; a one-game window that always faults is retried every alarm, never parked (stated); each park counted `owed_actor_tip_parked_total` (`object.rs:385`, `/health.owedActor.tipParked`). LOW-2: `ensure_schema` (`object.rs:128`) stamps `shape` when every TABLE statement answered (`:148`), so a rebuild whose `CREATE INDEX` failed retries as the index-only move (rows and `bootstrapped` kept, no second bootstrap); the digest only when whole; a partial plan counted `owed_actor_schema_partial_total` once an instance (`object.rs:484`, `/health.owedActor.schemaPartial`). The overlay's `OWED_ACTOR` seed list is 21. LOW-3 and NOTE-1 are the cutover doc's.
- **The cold actor (lane D2-ACTOR-B-FOLD-7, bsv-low #912; beta's first window, 2026-10-28)**: pair 20's proof seats (hundreds of games) faulted every apply at the 8 rounds (`... did not close in 8 rounds (N txids)`, N 118 to 1,992), and NO OwedActor alarm ever fired: workers-rs 0.8.7 reads an `i64` alarm as an OFFSET (`ScheduledTime: From<i64>`) and seven sites passed an epoch stamp (about 56 years out; bootstraps 0, the budget row never written). Now: every alarm through `alarm_in` / `alarm_at` (`alarm_offset_ms`; the tip alarm's earlier time wins, so an old far alarm is pulled back); an actor without `bootstrapped` HOLDS the event in its own `meta` (`held:<seq>:<kind>:<games digest>`, no new table: the digest does not move), arms its bootstrap and answers 202 `{"held": true, "bootstrapArmed", "ackSeq": null, "heldGames"}` (`apply_or_hold`); the bootstrap's short page applies the held events in seq order over the warm mirror and only then stamps `bootstrapped` (`drain_held`, one statement refusing while a held row stands), so no read serves the actor's rows before a held event is applied; a bootstrapped actor whose closure meets its bound un-stamps itself, re-arms and holds (`owed_actor_closure_rearmed_total`); counted `owed_actor_apply_held_total` (served `held`, `closureRearmed`). An actor ARMED UNDER A DEAD ALARM (its cursor set, its alarm lost or set decades out by the old build: beta's 156 actors the lever armed at 12:30Z) is re-armed now by the lever's `/bootstrap` (answered `rearmed: true`) and by a held event (`alarm_is_dead`: none, or past now + the page delay + 60 s), counted `owed_actor_alarm_rearmed_total` (served `alarmRearmed`); the counter list 24 in both Workers. The consumer acknowledges a held event's marks as an applied one's (Phase C reads `heldGames`; Phase B's D1 judge acks; no compare, no fault, no sample). The cold time (the first held event's write to the held events applied) is its OWN bar, `coldMsP95` / `coldMsMax` / `coldSamples` (class `cold` of the durable bars), never `rowsVisibleMsP95`. The closure is keyed by the games: hop markers of the domain only (it read every marker of the identity), `pot_records` by its own txid only (no `spendingTxid` key; a pot row's spender is a key only where the judge names it: the seat's change chain was walked one link a round), and the mirror's own fixpoint walked to its end (it stopped at 8 too, so a deep closure never progressed across retries). `asOfMs`: D1 binds a JS number as a REAL, `json_object('asOfMs',?1)` stored `1793190803706.0` and `as_u64` served null: now `CAST(?1 AS INTEGER)`, read either form. Pins `--lib d2_actor_b_fold7` (`a` a 600-game, 3,000-row chained seat held, 38 pages, rows equal to the D1 judge's; `b` held events in seq order; `c` re-arm past the bound; `d` steady state one round; `e` the consumer's ack; `f` `asOfMs`; `g` the cold bar; `h` every alarm an offset) and the workerd cell `tools/lane-912/owed_actor_alarm.mjs` (in `ci-d1-budget`; `fold7_h`: the alarms fire, a 118-game seat bootstraps by its own alarms in about 7.3 s, RED on `d910ad5d`: "the alarm 3586384224170 ... 57 years out"; `fold7_i`: an actor armed under a dead alarm re-armed by the lever and by a held event, then paged, RED on `d910ad5d`: "the lever re-armed: {\"armed\":false,\"bootstrapped\":false,...}").
- **The fourth delta lens's folds** (bsv-low `docs/audit/D2-ACTOR-B-delta-4-2026-10-28.md`, lane D2-ACTOR-B-FOLD-8; pins `--lib d2_actor_b_fold8`, the REDs in `/Volumes/Lanes/out/wt-overlay-actor-b/fold8-red-mutants.txt`). NEW4-M1: the fleet's bootstrap budget reads its minute as `CAST(?1 AS INTEGER)/60000` (`BOOT_BUDGET_TAKE_SQL`): D1 binds a JS number as a REAL, so `?1/60000` was a new fractional minute every millisecond and every take answered 1; the cap of 60 pages a minute was never in force on D1 (pin `a`, the take bound as `f64`: 1 to 60 then refused; RED on `a1010ad0`: seventy 1s). NEW4-M2, the stuck actor: every bootstrap-page and drain fault is counted (`owed_actor_bootstrap_faults_total`, `/health.owedActor.bootstrapFaults`; the counter list 25 in both Workers); a holding actor keeps ONE D1 row, `owed_belt_state` `owed-actor:holding:<identity>`, written at its first hold (`note_holding`; a later hold writes nothing to D1) and deleted by its stamp (`clear_holding`, retried at each landed apply), served `/health.owedActor.holding {count, oldestMs}` from the health snapshot the writers compose (one more subquery inside `HEALTH_SNAPSHOT_SQL`, no statement on the read); a page that faults five alarms in a row is PARKED (`meta.bootParked`, `boot_fault`, the line `[owed-actor] bootstrap PARKED <identity> after 5 faults: <reason>`; the alarm retries it no more, a held event leaves it parked), and the lever's `/bootstrap` clears the park and re-arms from the parked cursor, answered `parked: true` once (pin `b`). NEW4-L1: the drain applies the UNION of the held events' games in chunks of 16, at most 8 chunks an alarm (`DRAIN_CHUNKS_PER_ALARM`), the rest at the next alarm (`BootStep::Draining`, the cursor `drain`, the cold start carried in `meta.coldFrom`); an event is recorded in `applied` and deleted once all its games are applied (pin `c`: 150 one-game events, 128 then 22). NEW4-L2: `alarm_is_dead` past `boot_pace`'s longest wait (`BOOT_ALARM_DEAD_AFTER_MS`, two minutes, a page delay and 5 s; pin `d`). NEW4-L3: `classes.*.flushedAtMs` an integer (`json_ms`, and `CAST(tickedAtMs AS INTEGER)` in `health_select!`; pin `e`). NEW4-L4: a held answer is counted in the isolate's pending bars and written once a flush (`note_held`), and the alarm runs no tip window while the boot cursor stands (`tip_window_due`; pin `f`). NOTE-2: every statement the judge runs over a family selects it by one of the family's keys (pin `g`, lexical over the 69 family reads of `judge_subjects` on every frozen fixture; RED with `rank.rs`'s `pot_records` join moved to `spendingTxid`). NOTE-1: `fold7_a` asserts ONE cold 600-game actor under the pace (38 pages a second apart plus the native work) under 60 s, stated as that and not the wasm32, D1 or mass-bootstrap time. No digest moves (the holding and park state live in `meta` and the belt's ledger). The captain's items from the window on PR 206: (9) THE FLEET BARS ARE THE BUILD'S: each bars row carries `libRev` (the app layer's `OVERLAY_LIB_REV`); a flush of another build REPLACES the row, never merges, and `fleet_bars` serves only the reading build's rows (`barsLibRev`), so a fixed build's p95s are never the old build's history (pin `h`); (10) `GET /internal/owed-actor-rows?identity=` (bearer `INTERNAL_TOKEN`, one identity, no write; a fresh actor's `/stored` writes nothing) answers the actor's state and stored rows beside D1's, `diff` (`diff_rows`) and both `rows_digest`s, the reconcile's own reads (`rows_view`, pin `i`); (11) Phase A's compare is evidence-bound like Phase B's: the acknowledgement's bound is read before the D1 judge (`deliver`, carried by the deferred apply), so a game marked between the two judges is a held DIFF (`owed_actor_compare_held_total`), never `owed_actor_diffs_total`; stated: a deferred compare that runs after the word's own event acknowledged the mark counts it real (pin `j`); (12) the `DIFF` and `RECONCILE DIFF` lines name, for the first three differing outpoints, each differing field with both values (`diff_detail`), and the reconcile's line whether the actor was bootstrapped at the compare (`JudgeDb::actor_stored`, pin `k`).
- **The courier-found spend is durable evidence (bsv-low #953, lane D2-ACTOR-B-FOLD-9; beta's 11-outpoint reconcile diff)**: the belt's find of a spend wrote only the memo (`<txid>.<vout>`, aged out of every reader in two hours and swept by the overlay's `HOP_PROBE_MEMO_EXPIRE_SQL`) and the spender's bytes (keyed by the SPENDER's txid), so a hop the index holds unspent was judged `hop-stranded` claimable once the memo went, by an actor bootstrapped afterwards and by any later D1 judgement alike. Now (1) `owed_belt::Find` also writes the EVIDENCE row `hop_chain_probes` `<txid>:<vout>` (`hops_view::probe_evidence_key`, `owed::outpoint_key`'s spelling; `spent`, `spendingTxid`, `spentConfirmed`, `tipHeight`; only a spent answer; a look that finds it unspent deletes it, `PROBE_EVIDENCE_CLEAR_SQL`), which the overlay's TTL sweep leaves while the outpoint's `pot_records` row stands (an evicted hop's ages out as a memo; the reorg clear still takes a CONFIRMED one, a chain word); `PROOF_LATCH_MOVED_DELETE_SQL` names `proofmined:` keys only. (2) The judge reads the evidence for every hop it judges after the memos (`owed_judge::hydrate`: a hop with no memo is judged on its evidence as a memo that is not fresh), so the spender it names is read (`owed_spender_bytes` by txid); the actor's closure reaches the row by the hop's txid (`Key::ProbeRange`) and the bytes by the spender it names: no new column, no new table, the digest unmoved (the brief's `spentOutpoints` column was not needed: the outpoint reaches the bytes through the evidence row). The finds made before the fold are seeded ONCE by the drain's age repair (`owed_backfill::seed_find_evidence`, 64 stored spenders a pass by txid from `owed_belt_state` `backfill:owed-v2:find-evidence`, `~` after a short page): each input of a stored spender that is a hop whose index row stands unspent naming no spender gets its evidence row (`spentConfirmed` unknown) and its seats a `hop` downgrade mark, in one batch. (3) The find is an event: `FIND_MARK_SQL` marks the candidate's game (`hop`, its outpoint; a downgrade when it carries evidence) in the find's own batch and the pair is noted and published at the end of the belt's tick (`tick_env`). In Phase A the D1 judge reads the find in that batch and acknowledges the mark there (`Find::owned_mark`; D-H1 (c), nothing left dirty; the actor is called directly after, as before); in Phase B the actor-then-D1 bound acknowledges it; in Phase C, with no D1 judge, the mark stands until the consumer has carried it to the actor. Measured on FOLD-7's 600-game seat, one steady-state apply of one game: the record 24 statements and 5 rows answered before and after; the mirror 163 statements to 164. (4) THE RE-APPLY KEEPS THE JUDGED TIP (the captain's item 6, read on beta through `GET /internal/owed-actor-rows`: the reconcile's re-apply carried no tip, `reconcile_env` built its judge with `None`, and rewrote identity 0247bf52..'s eleven rows `tipUnknown`, `chainWait: block`): `reconcile_env` builds its judge with the served tip (`owed_events::tip_memoised`), `D1Judge::actor_apply` sends it whenever its own is unset, and the actor's `apply_or_hold` judges a body that carries no tip at its HELD tip (`owed_actor::held_tip`, `HELD_TIP_SQL`: the greater of its rows' `probeTip` and `meta.tipJudged`); only an actor never judged at a tip judges tip-unknown. The budget tier's `owed-belt-tick-fleet` writes 54 to 110 (its ceiling re-recorded 81 to 153: about 7 writes a find, the evidence row and the mark with its acknowledgement). Stated: the reconcile's digest keeps `spenderPresence.at`, a clock fact, so D1 rows judged at another time than the actor's differ on rows with a pending spender (seen in pin (e)'s first form; not this fold's). Pins `--lib fold9` (`owed_actor::tests`: a to f and j RED on `046b721e` and `171d1ca3` with the API grafted inert (c by its publish note), g (item 6) RED on `171d1ca3` (`hop-stranded`, `chainWait: block`, tip-unknown; `fold9-red-on-171d1ca3.txt`), `/Volumes/Lanes/out/wt-overlay-actor-c/fold9-red-on-046b721e.txt`; h prints the cost).
- **The fifth delta lens's folds** (bsv-low `docs/audit/D2-ACTOR-B-delta-5-2026-10-28.md`, lane D2-ACTOR-B-FOLD-10; pins `--lib fold10`, the REDs in `/Volumes/Lanes/out/wt-overlay-actor-b/fold10/`). NEW5-M1, A DIRECT APPLY NEVER SWALLOWS A BLOCK'S TIP EVENT: the dupe rule read the marks only, and a block is an input no mark carries, so the hook's direct apply at the isolate's stale 30 s memo (T-1) covered the block's `tip` marks and the consumer's fresh delivery at T was a dupe (Phase A/B: a real DIFF on an ordinary block; B/C: a gate's press a block late). Now `/internal/tip-changed` seeds the memo with the announced height before it writes the marks (`owed_events::seed_tip`, a lower height never replacing a fresh higher one), a direct pass over a `tip` event reads the tip fresh (`direct_reads_fresh`, the overlay's hand-off included), and each apply that records its coverage records its TIP beside it (`applied` kind `covered-tip`, one row a game, its `seq` the tip or `-1`, replaced by each apply; no new table, the digest unmoved), `COVERED_SQL` a dupe only where the game's last apply judged at a tip no lower than the delivery's (a held-tip apply covers no fresher delivery). NEW5-M2, UNCONFIRMED COURIER EVIDENCE RETIRES: chosen shape, the belt's OUTPOINT RE-ASK, not a TTL sweep of unconfirmed rows: a sweep would serve the hop's press again every two hours while a foreign mempool spend still stands (a press over a spend the couriers hold, Rule 23 for the list), where the re-ask keeps the row `unbound` until a look says unspent. A hop row resting on a courier find with no confirmation (`owed_belt::rests_on_unconfirmed_find`: `chainProbe: "spent"`, `spentConfirmed` not true) is dated at now + 600 s whatever bytes are held (`owed_judge::belt_due`) and asked at the outpoint (`ask_of`), backing off to 6 h while the look reads as before; an unspent answer clears the evidence and the sweep press returns, a confirmed one ends the question. The one-time seed writes `spentConfirmed` 1 where a `proofmined:` latch or the index's verified proof (`transactions.has_proof`) holds the spender, NULL otherwise. NEW5-L1: `fold7_a` asserts its 38 pages always and its per-page native work (`COLD_PAGE_WORK_BAR_MS`, (60 s - 37 delays) / 38) only in a release build. NEW5-L2: the consumer takes only its own events from the isolate's direct set (`owed_events_publish::take_direct_where`, `owed_events::consumer_owns`: the delivery's own pair), a concurrent writer's left held for its flush point; the belt's tick calls its published set directly (`direct_held`). Stated: a writer that published the consumer's very pair in the same isolate meanwhile loses its direct call to the consumer's take (the queue carries it). NEW5-L3: a direct pass coalesces per identity (`coalesce_direct`: the union of the pass's games for it, 64 a call, under the first event's key, the earliest write time, the others' keys in `also`, which the object records in `applied` for the first-apply rule); the overlay's hand-off shares ONE wall, the earlier of 2 s and the flush's own deadline, read before it (`change_flush::owed_direct_end`; a chunk not started inside it is left to the queue, one fault). NEW5-L4: a build's replace of a bars row carries `oldestUnappliedAtMs` (`merge_bars_row`). NEW5-L5: the object notes its own answer's samples into the fleet's bars under the call's `class` (`direct`, else `queue`; an apply with no event, the tip alarm's, notes nothing) and answers `noted: true`, which the caller's note skips (its unapplied bookkeeping kept), so a walled direct call's first apply is sampled once. Item 8: the direct pass keeps its first fault's text (`DirectReport::first_fault`) and logs `[owed-actor] direct fault: <text>` at most once a minute an isolate (`direct_fault_line`). THE CAPTAIN'S ITEM (beta 2026-10-29, the fourth RECONCILE DIFF: an actor bootstrapped on FOLD-7's build kept a row the FOLD-9 judge no longer produces, because `arm_bootstrap` answered `already` whenever `bootstrapped` was set): the bootstrap's end stamps the build (`meta.judgedLibRev`, `lib_rev()`), and a bootstrapped actor whose stamp is another build's or absent is re-armed from the start (`owed_actor::rejudge_due`; its state kept and served until the pages rewrite it) by the lever's `/bootstrap` and after an event it applies, counted `owed_actor_rejudged_total` (the 27th of both Workers' lists), served `/health.owedActor.rejudged` and `/health/invariants.owedActor.rejudged`; the lever's answer counts `rejudged` apart from `armed`, and a re-judging actor is uncovered until its pages end. The stamp is written only at a bootstrap's end (the whole state), never at a single game's apply, which would hide the other games' old judgement. Stated: every deploy now re-judges each actor on its next event or lever call, paced by the fleet's 60 pages a minute; a game the identity no longer names keeps its row (the pages judge the identity's games). Pin `fold10_j`. No digest move.
- **Not built, stated**: the cost bar's actor families (the design's LOW-7, the lens's LOW-1: the actor's DO requests and duration, its SQLite rows read and written, its KV writes and Phase A's doubled D1 reads are not on `worker_cost_daily`; the D1 rows its closures read are counted where the D1 port counts them); an event applied from its facts with no D1 read (each apply reads its closure); the lens's LOW-2 beyond `applied` (a `txfacts` `event` row and a mirror row no closure names any more stay); LOW-3 (in Phase C the judge's `PROOF_LATCH_MOVED_DELETE_SQL` lands in the mirror only, so D1's moved latch is re-copied and its root re-checked on every apply: not handed back to D1); LOW-4's no-binding fallbacks (the D1 judge where no actor is bound stays, harmless while the `owed-v2` stamp stands); LOW-7's apply truncation past 64 games (only the consumer's chunks of 16 and the split direct call reach it; Phase C acknowledges only `appliedGames`).
- Pins `--lib d2_actor_b` (`owed_actor::tests`): the byte-identity pin over the frozen fixtures and a spender-bytes fixture (10 identities, 1,715 rows, each identity's chunks under ONE event's key since the fold, `applied` no longer deleted between them; RED with `hopparty_records`' identity key dropped: `d1_fleet 02ee..: d1 500 rows, actor 250 rows`), the slow-chaintracks pair (RED with the turn removed: the older judgement's claimable `hop-stranded` row stored over the newer `unbound` one), the out-of-order pair, Phase A's compare finding a planted diff, Phase C's ack, the belt-ask round trip (RED with the hand-back dropped: the press stands `hop-stranded`, claimable), the reconcile finding a planted diff in both phases (RED with the digest inert), the KV copy never an empty list (RED serving an empty copy as `rows: []`), the bars and ages served (RED with the oldest age dropped), the wipe lever's plan, the flags, the fleet-shaped apply timed (one game of the 1,033-game whale after its bootstrap, one round: release p50 278 ms, p95 302 ms, max 340 ms, the 1 s bar asserted in a release build; debug p95 about 1.1 s; after the lens fold's fixpoint commit, which copies the identity's judged rows aside on every judging apply: release p50 290 ms, p95 322 ms, max 325 ms, at a host load average of 31; its debug ceiling of 5 s was missed once with the whole `d2_actor_b` set in parallel under that load, never alone). The lens fold's (`--lib d2_actor_b_fold`), each RED on `8af15944` with the two APIs it names grafted inert (`answer_of`'s `appliedGames` empty, `JudgeDb::actor_reads` false): `h1` (one event of 20 games through the shipped `route` in Phase C stores the D1 judge's rows; RED `left: 32 right: 40`), `h2` (a round-2 record fault on every fixture identity leaves no row, a read sampled at every yield of a fresh actor's apply sees none but the fixpoint's, a commit faulted after three statements restores the rows; RED: 10 of 10 kept a partial judgement), `h3` (Phase B: a downgrade, the actor unreachable: no press served, the delivery handed back, then served downgraded; RED: `a press served over a downgrade the actor has not applied (delivery: None)`), `h4` (an older digest's `owed_rows(identity, outpoint)` rebuilt, the apply stores the D1 judge's rows, `bootCursor` 0; RED: `no such column: beltDueAtMs in CREATE INDEX ... idx_owed_rows_belt`).

## Every owed frame's publisher calls the actor directly; one visible sample an event (bsv-low #952, lane D2-ACTOR-D, 2026-10-28)

`DESIGN-D2-ACTOR-2026-10-26.md` section 7, bar 2 as amended 2026-10-27: "the writer calls the identity's actor directly after its answer, the queue is the durable backstop". Beta's window measured the opposite: only the record door called the actor (633 ms visible); every other frame reached it through the queue, whose ONE consumer (one message an invocation) ran a D1 judge, the actor's apply and the compare per event, about 6 s, so a burst drained at ten events a minute and the queue class's `visible` climbed 2,652 to 183,405 ms.

- **The one site.** `owed_events_publish::publish_env` (the shared publish every owed frame of both Workers passes) HOLDS what it sent (`hold_direct`, at most `DIRECT_HELD_MAX` 512; never a `txfacts` event). The invocation's flush point hands the held set on: the app layer's writers through `owed_events::schedule_direct` (`wait_until` after the answer: the record door and the handfile batch, whose own `object::schedule_direct` is gone; the pot-, hop- and tip-changed hooks; the `/internal/tx-word` ingest), inline on the scheduled word tick (`direct_held`) and on the un-acked mark sweep (`sweep_publish`); the overlay's three flush points (`pot_changes::flush`'s task, `flush_inline`, the background `Ship`) through `change_flush::ship_owed_direct`, `POST /internal/owed-direct` on its `APP_LAYER` binding (bearer `INTERNAL_TOKEN`, 64 events a body, 2 s wall; answered 202 at once, the calls after its answer). The consumer drops what it publishes itself (a continuation is the queue's). `owed_events::direct_with` (over a `DirectPort`) reads each event's subjects from its marks (`plan_with(.., false)`: no envelope expanded, no write), then calls every identity's actor at once, each under `DIRECT_CALL_WALL_MS` (2 s), with the EVENT'S kind, seq and write time (the queue delivery's key; the door's call carried `seq: null`) and no bound (a direct call always applies). Counted `owed_actor_direct_calls_total`, `owed_actor_direct_walls_total` (new, `is_wall`), `owed_actor_direct_faults_total` (the overlay's hand-off faults too); served `/health.owedActor.direct {calls, walls, faults}` and `/health/invariants.owedActor.directWalls`. Never a fault of the write: the write committed and answered before; the queue message goes regardless. A `word` event carrying its frame is left to the consumer (its ingest is the tracker's own write; its marks do not exist until then); at most `DIRECT_EVENTS_MAX` (64) events a pass.
- **The seq check (the actor's coverage).** Every apply reads, BEFORE its closure reads the record, the greatest generation of its games' marks (`owed_judge::marks_bound`) and records it, once the fixpoint's commit landed, as `applied` rows of kind `covered` (`seq` that generation; no new table, the digest does not move). A delivery naming its acknowledgement `bound` (the consumer's, read before its call, `JudgeDb::actor_deliver_bounded`, `ApplyBody.bound`) whose every game is covered at or past it is a DUPE (`COVERED_SQL`, inside the actor's turn): answered without an apply (`applied: false`, `rounds: 0`), its `appliedGames` the covered games (Phase C acknowledges their marks: each was written before the covering apply's read, generations being AUTOINCREMENT). Phase A now reads the bound before its D1 judge and acknowledges at most it (`judge_subjects_acked_to`, NEW-H1's rule, as Phase B), so a dupe never stands beside a mark the D1 judge acknowledged past the covering apply. An out-of-order seq needs no hold in the actor: the later event's apply read the earlier write (its seq was taken after that write committed), so the earlier delivery is covered; a lost event's mark stands until the sweep re-publishes it (a new seq, not covered, applied).
- **One visible sample an event.** The answer carries `applied` (not a dupe) and `firstApply` (no `applied` row of its `(kind, seq)` for these games; an event with no seq is its own). `owed_actor::samples_visible`: only an applied first apply samples `visible`, in the class of the path that applied it; a dupe samples nothing (not even `apply`); an applied redelivery samples `apply` only. `rowsVisibleMsP95` (and `Max`) is now the FIRST visibility over the `direct` and `queue` classes together (the design's bar 2); `backstopLagMsP95` / `Max` the queue class's own (the events the direct call missed) beside `backstopLagBarMs` (60,000, `BACKSTOP_LAG_BAR_MS`); `rowsVisibleDirectMsP95`, `applyMsP95`, `collectAfterMinedMsP95` and `oldestUnappliedEventAgeMs` keep their names and meanings (bsv-low's bars cell reads `backstopLagMsP95` under 60 s).
- **The consumer's per-event cost.** The actor's call first (a dupe answers from the seq check), the D1 judge stays in Phases A and B (the one writer of `owed_rows` there), the compare runs on ONE EVENT IN EIGHT (`compare_sampled`, the event's seq mod `COMPARE_SAMPLE_EVERY`; no event, always). THE STOP SIGNAL (`owed_actor_diffs_total`) IS KEPT STATISTICALLY: a diff that persists is found within about eight of its game's events; a diff living one event is found with probability one in eight; a sampled DIFF still counts and still names its game. Measured natively (debug, the fleet's 1,033-game whale, a chunk of 16 games, eight events): before (D1 judge, the actor's full apply, the compare) p50 100 ms, max 124 ms; after (the seq check, the D1 judge to the bound, one compare in eight) p50 31 ms, max 46 ms (`d2_actor_d_d_*`, printed). Not measured on wasm32 or beta.
- **The concurrency knob.** `max_concurrency` stays 1 in Phases A and B (the reason at each config line: two consumers judging one identity's games are two D1 writers); `OWED_CONSUMER_CONCURRENCY` in both renders' `[vars]` ("1"), pinned equal to each render's consumer `max_concurrency`: the cutover doc moves both with Phase C (the actor the one writer, each identity serialised by its own Durable Object). A value past 1 with `OWED_D1_JUDGE` on is named on every consumer batch (`consumer_concurrency_refused`); the platform's concurrency is the config's, never the var's.
- Pins (`-p low-app-layer --lib d2_actor_d`): `d_b` (a dupe samples nothing, an applied event once; the fleet's first visibility and backstop keys; RED against `samples_visible` answering true: `left: 2 right: 1`), `d_c` (the direct pass's wall and fault counted apart, the event's key carried, the sites never awaited by a write; RED against `is_wall` false: `left: (0, 2) right: (1, 1)`), `d_e` (Phase C: seq 7 direct, seq 5 a covered dupe, a lost event's mark filled by the sweep's re-publish, the rows the record's; RED against `covered_at` false: seq 5 `applied: true`), the knob's pin, and `d_d`'s measurement. Each uses API this lane adds and does not compile on `046b721e`. The workerd cell `tools/lane-912/owed_direct_flood.mjs` (`d_a`, in `ci-d1-budget` after the FOLD-7 alarm cell): 32 one-game seats bootstrapped, four hop-hook bodies at once (32 identity events, the queue a producer with no consumer), each actor's `judgedAt` within 1 s of the flood; RED on `046b721e` (no actor called: "timed out"). Amended on the rebase over FOLD-8 (PR 208): `d2_actor_b_fold8_j` gives each delivery its own event's mark (a delivery with none is covered, a dupe) and seq 8 inside the one-in-eight sample, its source assertions the merged Phase A (`judge_subjects_acked_to` to the bound); FOLD-8's `owed_actor_bootstrap_faults_total` is `COUNTERS[24]`, `owed_actor_direct_walls_total` `COUNTERS[25]` (26 in both Workers).

## The consumer delivers a batch's identities at once; Phase C's concurrency (bsv-low #952 under #912, lane FIX-952-PHASE-C, 2026-10-29)

The L53 loop (records 1544 to 1548; the fold's S1 and S2) ran Phase B at 33 pairs: the `owed-events` consumer delivered a batch of 16 messages ONE identity at a time, each identity the actor's apply then the D1 judge's commit, so a batch took 61 to 70 s against its 60 s wall and about 15 events a minute were applied of 243 published; the actor-served owed list lagged the write by the backlog (5 to 32 min), and Phase C's entry condition could not be met. D2-ACTOR-D had already built every writer's direct call (the overlay's through `POST /internal/owed-direct` on its `APP_LAYER` binding), the one visible sample an event and the compare's one-in-eight sample; what this lane adds, by the issue's four asks:

- **Item 3, the pass.** `owed_events::judge_plan` delivers the merged plan's IDENTITIES AT ONCE (`judge_identity` each, `buffer_unordered` at `JUDGE_IDENTITIES_AT_ONCE` = `CONSUMER_BATCH_MAX` 16; the direct pass already called 64 actors at once), each identity's chunks still in order (one D1 writer of its rows at a time; in Phase B HIGH-3's order, the actor before the acknowledgement, holds per identity), the outcome merged after every identity answered (landed and faulted subjects, cleared memos, the first fault). The per-identity state the pass keeps is keyed by identity: the judge's marks-touch (`JudgeDb::note_marks_touch(identity, ..)`, `D1Judge` a map; a single slot let identity B's marks replace A's between A's note and A's deferred apply) and the in-flight set (`enter_in_flight` / `leave_in_flight`; the wall's branch takes whatever is in flight across identities). Pin `fix952_c` (`--test fix952`, tokio's paused clock, the actor 100 ms and the judge's commit 200 ms): a Phase B batch of 16 identities 300 ms of virtual time, every row judged and every mark acknowledged, the actor before each identity's write; RED against the serial pass (`JUDGE_IDENTITIES_AT_ONCE` 1, the base's rule): 4,800 ms.
- **Item 3, Phase C.** With `OWED_D1_JUDGE=false` the consumer makes NO D1 judge call and writes no `owed_rows`: per event it reads the pair's marks (2 statements, one round trip), the marks' bound (1), calls the actor once and acknowledges in one batch (4); measured by pin `fix952_d` over 16 events, 7.1 statements and 3.1 round trips an event inside the pass, then 4 resolution statements a message and three batch-level statements once (the counters, the consumer row, the health snapshot). THE PHASE C PAIR: `CONSUMER_CONCURRENCY_PHASE_C` = 4 (`consumer_concurrency_for(d1_judges)`: 1 in A and B, 4 in C), the two commented `PHASE C:` lines in each render of `crates/low-app-layer/wrangler.toml` (`# PHASE C: OWED_CONSUMER_CONCURRENCY = "4"` under `[vars]` / `[env.beta.vars]`, `# PHASE C: max_concurrency = 4` in the live `low-owed-events` consumer block, never the DLQ's) pinned equal to it: the Phase C deploy's lever applies BOTH, the var and the consumer block's config, in one render. Why four: one consumer clears a batch of 16 in about one actor apply and three D1 round trips (320 to 960 events a minute); four cover the L53 peak five times over while the actor applies in flight stay at most 4 x 16 = 64, the direct pass's own fan-out (`DIRECT_EVENTS_MAX`), so no more of the actors' compound D1 reads land behind the alarm's batches (record 1537) than one direct pass already puts there. Why N consumers cannot race on D1 in C: they write no `owed_rows` (the actor is the one writer, in its own SQLite per identity, its events ordered by its `seq`, never by the queue); their only D1 writes are the acknowledgement's deletes of marks at or below a generation each read before its own call (`phase_c_ack_statements`), idempotent and monotone in any order, and the two idempotent restart statements.
- **The one-writer rule per phase** (the toml's comment at both sites, the PR body): A (`OWED_ACTOR_READ=false`, `OWED_D1_JUDGE=true`) the D1 judge writes `owed_rows` (the consumer's delivery and the drain), the actor writes its own SQLite beside it; B (`true`, `true`) the D1 judge still writes `owed_rows` (the rollback's mirror), the actor's rows are served and the actor applies before the D1 judge acknowledges; C (`true`, `false`) the actor is the ONE writer, nothing writes `owed_rows`, the consumer makes no judge call. A and B: concurrency 1 (two consumers would be two D1 writers of one identity's rows). C: 4.
- **Item 1, every writer's direct call** (D2-ACTOR-D's, re-proven natively). The overlay's hand-off is `change_flush::ship_owed_direct_with` over its ports (the POST, the clock, the timer; the report's lines logged by the wrapper `ship_owed_direct`, which supplies the `APP_LAYER` binding's `post_body_answer`): the shipped chunking (`OWED_DIRECT_BODY_MAX` 64), the one wall (`owed_direct_end`) and the fault rule run natively. The route's shape: `POST /internal/owed-direct`, body `{"events": [<the queue's own message text>, ...]}` (each event as it rides the queue: its `seq`, `atMs`, source and facts, so the actor call carries the queue delivery's key), answered 202 `{"accepted": n}` before any actor is called, the calls under `wait_until`, each body `class: "direct"`, no `bound` (a direct call always applies). Its reach: the binding alone. The platform gives the callee no proof a request came over a service binding, so the route keeps the internal bearer (`internal_bearer_ok`; the overlay holds `INTERNAL_TOKEN`, the binding carries it): on the public origin it answers 401, it sets no CORS header and answers no preflight, so no browser reaches it; the binding is what makes the hop possible at all (a plain fetch between two Workers of one zone is 1042). Pin `fix952_e` (new API; the hand-off was `env`-bound): the overlay's shipped hand-off over four held events, its POST answered by the app layer's own `parse_direct_body` and `direct_with` over fake actors: one body, every event read back with its seq and atMs, one call an identity carrying the first event's key and the others in `also`, the frame-carrying word left to the queue; a POST that never answers one fault at the wall with its events the queue's; a refused POST one fault; 65 events two POSTs, and a wall that fell before the second its event to the queue, counted; the route's bearer, 202 and no CORS header, and the transport's `env.service("APP_LAYER")`, by source.
- **Item 2, the sample.** Unchanged from D2-ACTOR-D (`samples_visible`: only an applied first apply samples `visible`; a covered dupe samples nothing, an applied redelivery `apply` only; `rowsVisibleMsP95` the first visibility over the direct and queue classes, `backstopLagMsP95` the queue's own against `backstopLagBarMs` 60,000; the served keys kept). Pin `fix952_b` re-states it through `note_answer_into` and the consumer's own Phase B delivery (`actor_then_d1` with the actor answering a dupe: the isolate's bars unchanged; then a first apply: one sample). Green on `149869bf`.
- **Item 4 / fix952_a**: the workerd cell `tools/lane-912/owed_direct_flood.mjs` (`make ci-d1-budget`) runs D2-ACTOR-D's flood (32 frames for 32 identities, each visible under 1 s by the direct call, the queue behind) in Phase B as before and then in PHASE C on the same persisted state with 32 fresh seats, where the direct call also acknowledges the marks (`owed_dirty` empty for the 32, `owed_events_direct_acked_total` +32) and no `owed_rows` is written for them.

Not changed, stated: Phase A's deferred applies after the batch stay sequential (`run_deferred`); the compare's sample; the frame, the judgement, the schema digest, the bootstrap and the pace; `max_concurrency` and `OWED_CONSUMER_CONCURRENCY` stay 1 in both renders until the Phase C deploy (the pins hold the live lines at 1 and the `PHASE C:` lines at 4). Pins `--test fix952` (`fix952_b`, `_c`, `_d`, `_e`; `_c` RED against the serial rule, `_d` and `_e` new API, `_b` green on the base) and the cell's `fix952_a` leg; amended: `d2_actor_a`'s in-flight source pin (`enter_` / `leave_in_flight`) and `owed_actor/tests.rs`' marks-touch text (`note_marks_touch(identity, marks_touch)`).

## The sixth delta lens's folds (D2-ACTOR-B-FOLD-11, bsv-low #954, #955, #957; 2026-10-29)

bsv-low `docs/audit/D2-ACTOR-B-delta-6-2026-10-29.md` (LAND: NEW6-M1, NEW6-M2, NEW6-L1, NEW6-L2) and the captain's #957 items. Pins `--lib fold11` (`owed_actor::tests`) and `--test d2_actor_a fold11`; each RED against its rule reverted (`/Volumes/Lanes/out/wt-overlay-actor-b/fold-11/red-*.txt`).

- **NEW6-M1, a warm apply after ANY write equals the D1 judge's over the full closure.** D2-ACTOR-E's narrowed read took only the event's and the marks' rows, so a write no trigger marks (the overlay's reorg clear of confirmed evidence, `TRANSACTION_UNPROVE_SQL` / `MARK_TX_PROVEN_AT_SQL`, the TTL sweep, an opponent's party row) left the warm mirror stale until a full read. Now the narrowed read takes EVERY family over the mirror's whole key set plus the event's and the marks' txids (`apply_with`). The lens's narrower shape (only the nine untriggered families whole) was not taken: no family's every writer marks every identity its rows reach (partial triggers, cross-identity txid keys), so the narrowing kept is the statement count, never the rows. Cost: 2 record statements a warm apply (unchanged), the compound statement 10,488 bytes (unchanged: only its binds grow), rows read back to the game's closure. `fold11_a` (the lens's reorg-clear shape: RED `unbound` against D1's `hop-stranded`), `fold11_b` (a proven filed sweep's payout, the proof withdrawn: RED, the warm actor kept the claimable `internalize` payout).
- **NEW6-M2, the coalesced batch is bounded and its wall noted** (`owed_events::batch_core`, the queue's batch arm over it). A message whose pair or a planned subject carries a fault memo goes to the one-message arm; the merged plan takes messages while it stays within ONE message's bound (64 games, `ROUTE_READ_LIMIT` marks), the rest to the one-message arm (`batch_selection`). Each message's fate by its OWN plan (`Fate::Ack { resolve, continues }`: a message whose every subject was held resolves nothing; `Fate::Fault` where its every subject faulted, its pair's memo noted). At the wall the subjects in flight are noted (`wall_fate`, the one rule both arms call; `fault_fate` the Err arm's): their messages handed back with the hold, the rest at once. `Routed` carries `landed` and `faulted` subjects. `fold11_c` (16 messages, the first subject never answers: held by its third wall, the other 15 judged; and the bound: three 30-game messages, two coalesced), `fold11_d` (an all-held message and a memo'd pair keep their dead letters and memo). Stated: the batch's ingest and plans are outside its wall (the judge pass is inside).
- **NEW6-L1, a coalesced delivery samples per event.** `EventCtx::also` carries the batch's other events with their write times (`apply_body`'s `also`, `alsoAtMs`); the object reads each one's first apply before recording it and samples each first apply born warm (`event_visibility`, the answer's `visibleMs`, read by both bars' notes). `fold11_e`. Stated: two concurrent deliveries of one event can both sample it (the also check is outside the turn).
- **NEW6-L2, the judge's version and cold pages first.** `meta.judgedLibRev` is stamped and compared with `owed_actor::JUDGE_VERSION` (`fold11-2026-10-29`, moved by hand when what an actor stores for the same record changes; moved here by M1), never git HEAD; the bars stay keyed by `lib_rev`. A re-judge's page takes from `BOOT_BUDGET_REJUDGE_TAKE_SQL` (the same minute's count, refused at `BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE` 30), so 30 pages a minute stay for cold actors, and a waiting cold page's slot is in the next minute's first half, a re-judge's in its second (`boot_pace_of`). `fold11_f`; `fold10_j` amended.
- **#957 item 1, the era on the released-hop road.** Beta's actor held a claimable `hop-stranded` (20,190 sats, an Oct 21 hop, `joinRefusedBy: eviction`) that D1 held no row for: its `markerCreatedAtMs` is the eviction's `evictedAt`, the evidence road's own stamp, the one hop road the era cutoff did not filter. `owed_judge::rank::before_era` is now the one rule of the representatives, the hop markers, the released evidence and its could-not-judge rows (the hop's pot row, else `evictedAt`). Why the live D1 held no row is not established by a run (no beta read in the lane); under the fix both judges write the hop off. `fold11_g` (RED: both judges store the press under the era).
- **#957 item 2.** `tip` joins `CLOCK_FACTS` (the compare reads the judgement without its clock; `claimable`, `blocksToGate`, `tipUnknown` still differ). `fold11_h`.
- Not done: NOTE-1 (the floor at the stored `probeTip`); the one-round-trip warm read (the captain's item 9: the marks then the compound are still two sequential reads), the named follow-up.

## The seventh delta lens's folds (D2-ACTOR-B-FOLD-12, bsv-low #956, #954; 2026-10-29)

bsv-low `docs/audit/D2-ACTOR-B-delta-7-2026-10-29.md` (LAND: NEW7-M1, NEW7-L1, NEW7-L2, NEW7-L3). Pins `--lib fold12` (`owed_events::tests` a to c, `owed_actor::tests` d); each RED against its rule reverted to `fd57eb831`'s (`/Volumes/Lanes/out/wt-overlay-fold-12/fold12-red-mutants.txt`).

- **NEW7-M1, the swept stamp blocks on D1.** D1 binds every `Bind::Int` as a JS number, so `json_object('sweptAtMs',?2)` stored `….0` and the `as_i64` reader read none: the once-a-pass rule never held (a passed pair re-published every tick until consumed, a stalled consumer's every 120 s, not 600 s). Written `CAST(?2 AS INTEGER)` and read through `owed_events::ms_of` (an integer, or a REAL's value), as the horizon was. The class is closed in the module (its doc): every other `owed_belt_state` cursor the sweep, the publisher and the consumer write is serde-built JSON text (the sweep's cursor, the consumer's row and its `json_set`, the fault and subject memos, the lost notes), each read through `ms_of` all the same. The test port binds integers as REALs while `pot_hook::tests::INTS_AS_REAL` is set (D1's binding). `fold12_a` (RED: the stamp `{"sweptAtMs":1800000240001.0}`, `left: 3, right: 0`; ten stalled ticks re-publish 2).
- **NEW7-L1, the sweep's two range reads read at most what they take.** The lost notes (`LOST_READ_SQL`) and the prune's window (`SWEPT_WINDOW_SQL`, then `SWEPT_PRUNE_SQL` by name, the age read again in the statement) are primary-key ranges read to their LIMIT past cursors kept in the sweep's own cursor row (`lostAfter`, `pruneAfter`; a short read wraps, `range_cursor`, `next_range_cursor`), one batch, one statement more a tick. No sort and no migration: the names keep their pair keys (the consumer's landed judge clears a pair's stamp and note by name). Measured over 5,000 notes and 20,000 stamps: 238 VM steps for the 32 notes taken (40,614 sorting) and 462 for the 64 stamps read (100,023 scanning). The rule for a held note: the cursor passes it, so a newer note is read within `ceil(N / 32)` ticks of N notes (oldest-first kept up to 32 held notes in every tick's places for as long as their holds ran). A round of the prune over N stamps is `ceil(N / 64)` ticks. `fold12_b` (RED at its held-note leg: `left: [] right: [("hop", "ff..:0")]`).
- **NEW7-L3, each identity's apply carries its own events.** `owed_events::coalesced_event_of` files each event of a coalesced batch under the identities its plan names (`EventCtx::by_identity`), and `apply_body` takes `EventCtx::for_identity`: the identity's oldest event under the delivery's key, its others in `also`. One `also` had served every identity, so an event naming another identity read as a first apply in each actor (sampled once per identity). `fold12_c` (two identities, two events each: four samples, each the event's own lag; RED against the one context: 8).
- **NEW7-L2, a tip-only difference counted apart.** `owed_actor::tip_lag_rows`: the outpoints `diff_rows` reads equal whose `facts.tip` or `probeTip` column differs; the delivery's compare and the reconcile (Phases A and B) count them `owed_actor_compare_tip_lag_total` (the 29th of both Workers' lists, served `/health.owedActor.tipLagDiffsTotal` and `/health/invariants.owedActor.tipLagDiffs`), the compare's line `TIP LAG (NEW7-L2; not a STOP)`; a verdict difference stays `owed_actor_diffs_total`. The probeTip floor (the sixth lens's NOTE-1) is NOT built: a floor at the stored `probeTip` would judge above a tip a reorg lowered. `fold12_d` (RED: `left: [] right: ["329d:0"]`).

## The reconcile judges D1 fresh; D1 re-judged after a judge-version move (D2-ACTOR-B-FOLD-13, bsv-low #958; 2026-10-29)

Beta at `c08be8b51`: identity 02b2f90e11…'s actor held 13 rows and D1 23, the ten D1-only rows old-era payouts judged by the old build ("seen, awaiting the block", settles mined at 971017 and 971034, below the cutoff 972762) that FOLD-11's era rule writes off. The fresh actor was right; D1 had no re-judge on a judge change, and the reconcile compared the fresh actor against D1's stale rows and re-applied the games.

- **The fresh compare** (`owed_actor::reconcile_step`, Phases A and B). D1's stored rows are compared with the actor's first, only to find the differing games. Those games (at most `RECONCILE_JUDGE_GAMES_MAX`, 64, in game order, 16 a judge call) are judged again by the D1 judge (`judge_subjects_acked_to(.., 0)`, which acknowledges nothing; the marks stay for the consumer). The FRESH rows are then compared. A stored row is never the comparison's side. Games past the bound are marked `backfill` (`RECONCILE_MARK_REST_SQL`) and left out of the compare (`ReconcileReport::unjudged`). A fresh judgement that faults ends the reconcile with no compare. A stored difference the fresh judgement heals is a STALE D1: logged `[owed-actor] RECONCILE stale-d1 <identity>` (whether D1's re-judge had passed the identity), counted `owed_actor_reconcile_stale_total` (served `/health.owedActor.reconcileStaleTotal`, `/health/invariants.owedActor.reconcile.stale`), never a diff, nothing re-applied. A difference that stands after the fresh judgement is a judge disagreement (`RECONCILE DIFF ... a judge disagreement: D1 judged fresh over N games`), and only then are the games re-applied into the actor; its closure re-reads the record, never a stored row. Phase C is unchanged.
- **The fleet re-judge** (`owed_actor::d1_rejudge_step`, the belt alarm's `owed.d1_rejudge` phase, before the reconcile). One row of the belt's ledger, `owed-judge:version` (no schema change), holds `{version, startedAtMs, endedAtMs, done, identities, games, rows, faults, skipped, after, open, openFaults, previous}`. When the running build's `JUDGE_VERSION` differs, or no row stands, the step walks every identity of the reconcile's union (`RECONCILE_NEXT_SQL`), each identity's games by game id (`IDENTITY_GAMES_BY_ID_SQL`). It judges them with the D1 judge, acknowledges nothing, and sets the row `done` at the end. With the same version, one primary-key read a tick and no write. The pace is `D1_REJUDGE_GAMES_PER_TICK`, 32 games a 120 s alarm (23,040 a day; the 1,033-game whale in about 65 minutes). The cost is the judge's commit, about 3,000 rows read per 16 games on a 500-row identity by the backfill tier's measure, so at most about 4.3 M rows a day while a run lasts and none after. A faulted identity is retried the next tick, counted `owed_d1_rejudge_faults_total`, and skipped at its third consecutive fault (logged `SKIPPED`, `skipped`). Identities done are counted `owed_d1_rejudge_identities_total`. Phase C runs nothing (the `owed-v3` pass is the rollback's). Served: `/health.owedActor.d1Rejudge` {`version`, `running`, `identities`, `skipped`, `done`, `startedAtMs`, `endedAtMs`, `faults`}, from the health snapshot (one more primary-key subquery; the step's own save writes the snapshot, forced at the end).
- **Which side is stale.** `GET /internal/owed-actor-rows` serves `d1JudgedUnder` (`owed_actor::d1_judged_under`): the stamp's version once its run passed the identity; the previous run's version when the identity's `owed_state.computedAtMs` is older than the run; `unknown` otherwise. It also serves `d1Rejudge` {`stamp`, `passed`} (`D1_JUDGED_UNDER_SQL`, one statement beside the rows). The window's read order: the deploy, the fleet re-judge done on both sides (`d1Rejudge.done` and the actors' `rejudged`), then the route and the reconcile.
- Counters 30 to 32 in both Workers' lists. Pins `--lib fold13` (`owed_actor::tests`): `fold13_a` (an old-era press in D1, the fresh actor's write-off: equal, a stale D1, nothing re-applied; a planted actor row still a diff), `fold13_b` (no stamp, then a version move: every identity once, paced, the stamp and the health body; the same version none; a faulted identity retried, three skip it), `fold13_c` (`d1JudgedUnder` for each stamp state, the lever's statement, the `stale-d1` line before and after the re-judge passed). Each is RED against its rule reverted (`/Volumes/Lanes/out/wt-overlay-fold-13/fold13-red-*.txt`): the stored rows compared, `left: (true, 1) right: (false, 0)`; the step inert, no run; the version not served for a passed identity.

## The warm apply is one D1 read; the apply's stages served (D2-ACTOR-B-FOLD-14, bsv-low #954 item 9; 2026-10-29)

- **One read round trip.** A warm apply reads the record in ONE statement (`owed_actor::marked_select`): the games' pending marks a CTE (`mk_m`), their txids folded into the key set (`mk_t`, `?3` and each mark's `txid` or `txid:n`, lowercased, 64 hex), the probe ranges after them (`mk_p`, `?5` and each `?6` prefix before each key), every family read over `mk_t`/`mk_p`; answered `marks`, `keys` (the domain the mirror write deletes and fills) and `f<n>`, from one snapshot. It was the marks read and then the compound over keys that depended on it. A mark of `FULL_SOURCES` takes the marked read as the full apply's first round. `TOOBIG` falls back to the two reads and their own fallbacks. A bootstrap page (`ApplyOpts::page`, `page_select`) reads its games (`IDENTITY_GAMES_AFTER_SQL` as `mk_g`, `?7`/`?8`) in its first round's statement: a page is its rounds' reads, where it was one read more. The mirror's own keys for the page's games are read in the next round (none on a fresh actor). The cold full read was already one statement a round. Every CTE is `MATERIALIZED` and has no column list (the ownership checker's CTE rule).
- **The stages.** `ApplyReport` and the answer carry `readMs`, `judgeMs` (the rest of the apply), `writeMs` (the mirror's writes) and `waitMs` (the turn; a joined caller waits whatever the pass did not take). They sum to the apply's time on the local clock. Each class's bars row keeps a ring per stage (`APPLY_STAGES`), and `/health.owedActor` serves `classes.<c>.<stage>MsP95` and `stages.<stage>MsP95` over direct and queue beside `applyMsP95`. Ceilings are unchanged.
- **A burst's callers.** A caller whose event a holder's pass applied takes its answer when the pass leaves it (`Burst::answer`, `AnswerOf` raced against the turn), never after the turn. futures-util's `Mutex` wakes its lowest slab slot, so a joiner could wait out a later caller's whole pass.
- Pins `--lib fold14` (`a` one warm apply 1 record call, the rows byte-equal to the two reads'; `b` a page's calls equal to its rounds; `c` the stages sum and are served; `d` a joiner answered while the turn is held). Each was RED against its rule reverted: `a` `left: 2 right: 1`, `b` `left: 3 right: 2`, `c` no stage answered, `d` timed out (`/Volumes/Lanes/out/wt-overlay-fold-14/fold14-red-mutants.txt`). `fold3_m2` and `fold8_b`'s `PageFaults` were amended for the page's one statement.

## The reconcile compares fresh with fresh; the alarm's phases bounded by calls (D2-ACTOR-B-FOLD-15, bsv-low #958; 2026-10-29)

bsv-low `docs/audit/D2-ACTOR-B-delta-8-2026-10-29.md` (LAND: NEW8-M1, NEW8-M2, NEW8-L1, NOTE-3). Pins `--lib fold15` (`owed_actor::tests`), each RED on `800d8b2e3` (`/Volumes/Lanes/out/wt-overlay-fold-15/fold15-red-on-800d8b2e3.txt`).

- **NEW8-M1, fresh with fresh at one tip** (`owed_actor::reconcile_fresh`, Phases A and B). The stored rows only pick the games. The picked games' marks are read first (`RECONCILE_MARKS_SQL`: the games a mark holds and the bound); then D1 judges them (acknowledging nothing) AND the actor is re-applied over them, both at the reconcile's served tip, and the two fresh judgements are compared. A game holding a mark written above the bound while the two judged (`HELD_GAMES_SQL`) is held (FOLD-16: a mark pending at the start holds only the actor's stored-against-re-apply difference, never the fresh comparison): its difference counted `owed_actor_compare_held_total`, `RECONCILE DIFF (held mark; not a STOP)`. Fresh against fresh is a judge disagreement (the STOP). The actor's stored rows against its own re-apply: only tip-derived (`tip_derived_rows`: equal once `blocksToGate` and `tipUnknown` are taken out, judged at two tips) is `RECONCILE TIP LAG (not a STOP)`, counted `owed_actor_compare_tip_lag_total`; anything else is the actor's miss, healed by the re-apply, a diff, its games `republished`. D1 moved where the fresh sides agree is `stale-d1`. The route (`rows_view`) serves a tip-derived-only difference as `tipLag` with `tipLagTips {d1, actor}`, never in `diff`. Stated: the reconcile now re-applies the actor on every step that picks a game (it re-applied only on a disagreement); a held actor (bootstrapping) or an unreachable one is no compare (`noActor`).
- **NEW8-M2, bounded by calls; the pace a lever.** `d1_rejudge_step` opens at most `D1_REJUDGE_IDENTITIES_PER_TICK` (32) identities a tick, an identity with no game counted, and judges at most the PACE's games in at most `ceil(pace / 16)` judge calls; an identity whose page is short is done with no empty page; the state is saved after each identity or judge call, its write riding the next read's batch, so a dying tick loses one identity at most. THE PACE (the captain's lever): the row `owed-judge:pace` (`POST /internal/owed-rejudge {"gamesPerTick": N}`, bearer `INTERNAL_TOKEN`; `null` clears it; `GET` answers the row, the pace in force, the var, the ceiling and the stamp), read with the stamp once per alarm, else the render's `OWED_D1_REJUDGE_GAMES_PER_TICK` ("32" in both renders), else 32; clamped to `D1_REJUDGE_GAMES_PER_TICK_MAX` (32); `0` PAUSES the walk, its cursor kept, the stamp and `/health.owedActor.d1Rejudge` serving `paused` (and `gamesPerTick`). A tick that made more than one judge call yields the alarm's reconcile to the next alarm (`reconcile_yields`). The reconcile's fresh judge is one call (`RECONCILE_JUDGE_GAMES_MAX` 16, was 64), the rest marked `backfill`. Measured natively (the record port, the actor's own record reads counted in): 2,000 no-game identities 66 calls / 99 statements a tick (4,003 / 4,004 on the base); on the fleet the worst re-judge tick at 32 games 635 statements (the reconcile yielding: the alarm 949 with the belt tick's 314), at 16 games 325 with the whale's reconcile 282 beside it (921); on the base 542 + 1,013 + 314 = 1,869. The drain's sweep is not counted. A judge call's commit is one atomic batch and is not split; every other read of the walk is a batch of one or two statements, so the actors' own D1 calls interleave between them.
- **NEW8-L1.** `d1_judged_under` names `previous.version` only when `previous.done`.
- **NOTE-3.** The bootstrap page's `at` is read through `owed_events::ms_of`.

## A start mark never holds a judge disagreement; the drain in the alarm's bound (D2-ACTOR-B-FOLD-16, bsv-low #958; 2026-10-29)

bsv-low `docs/audit/D2-ACTOR-B-delta-9-2026-10-29.md` (LAND: NEW9-M1, NEW9-L1, NOTE-1 to 4). Pins `--lib fold16` (`owed_actor::tests`), each RED against its rule reverted to `d57bc0fa0`'s (`/Volumes/Lanes/out/wt-overlay-fold-16/fold16-red-*.txt`).

- **NEW9-M1.** `reconcile_fresh` partitions the two FRESH judgements' difference (`disagree`) only by the games `HELD_GAMES_SQL` names above the bound (marks written while the two judged); the games holding a mark at the START (`RECONCILE_MARKS_SQL`) excuse only the actor's stored rows against its re-apply (`missed`). Both fresh judgements read the record after a start mark, so a difference on such a game is a judge disagreement: `owed_actor_reconcile_diffs_total`, the line "a judge disagreement" (the STOP). `fold16_a` (the lens's scratch a: an `age` mark at the start, still `reconcileDiffs` 1 and `heldDiffs` 0; a mark the actor's re-apply writes, held; RED on the base rule: `left: 0 right: 1`).
- **NEW9-L1.** The alarm's drain counts in its statement bound. Measured natively, a full sweep (64 pairs, a stalled consumer: the sweep's tick, its publish batch, the direct pass's mark reads, the bars' flush, the snapshot and 12 counter statements) is 276 statements, not the lens's 65. A drain that did work (`DrainReport::swept` or `subjects` above 0, or a drain that faulted) holds the re-judge to ONE judge call (`owed_actor::rejudge_calls_max`, `d1_rejudge_step_after_drain`, `drainBound` on the `[owed-alarm]` line) and the reconcile yields (`reconcile_yields`). The worst alarm: 276 + 314 + 325 = 915; with no re-judge call, the whale's reconcile beside the drain 872 (on the base rule 276 + 314 + 635 = 1,225). The ceiling stays 32. `fold16_b` (RED with the drain ignored: "one judge call under a working drain"). Stated: Phase C's direct pass also reads each body's bound and writes its acknowledgement (up to 128 statements more), but Phase C runs no re-judge and no reconcile.
- **The captain's item 5 (beta at 5f820ebf9: two `direct` samples of 3,533 ms apply, 3,105 judge, with no writer's event behind them).** The bars are the writers'. Found at `reconcile_fresh`'s re-apply: `D1Judge::actor_apply` sent an event's `atMs` and no class, so the object sampled the reconcile into the QUEUE class, not direct. The DIRECT samples are the drain's sweep: it re-publishes the `backfill` marks the reconcile writes for the games past its 16 (`RECONCILE_MARK_REST_SQL`), and its direct pass called the actor with up to 64 games a body under `class: "direct"` (a multi-game apply, read 428 ms, wait 0). Now the reconcile's body carries no `atMs` (`owed_actor::reconcile_apply_body`) and the sweep's pass `class: "sweep"` (`SWEEP_CLASS`, `owed_events::direct_with_class`, `direct_events_as`); the object samples only `samples_bars` (an `atMs` and not the sweep's class), at both its own bars and the fleet's, and the sweep's caller notes nothing. The D1 re-judge calls no actor. A writer's direct pass samples as before. Pin `fold16_c` (RED against the sweep's pass as on the base: its body under `class: "direct"`). Stated: the object's gate is pinned by source (wasm32), not run on beta.
- **FIX-960 (bsv-low #960, the tenth delta lens's NEW10-L1).** The held partition is cut at the greatest mark generation the actor's closure READ: the apply answers it (`readGeneration`, `ApplyReport::read_generation`: the marks read with the record's rows in its first round; absent past one round, on a dupe, from an older build) and `HELD_GAMES_SQL` holds only `bound < generation <= readGeneration` (absent: no upper cut, FOLD-16's rule). A mark written after the actor's read is neither judgement's input, so a difference on its game is a judge disagreement (`reconcileDiffs`). One rule for both compares (its doc comment): a mark explains a difference only when one reader read it and the other did not; the delivery compare's later reader is the D1 judge, which answers no read generation, so its `whileMarks` stay open above, and FIX-964's `lagMarks` are the delivery's own class. Stated: a mark between the bound's read and D1's judge is read by both and still held. Pin `--lib fix960` (RED on `1ab40c95`: `left: (0, 1) right: (1, 0)`).
- **NOTE-2.** At 32 the reconcile yields almost every tick on beta's seats, and on every tick the sweep re-publishes: a Phase B read of `reconcileDiffs` needs the walk paused or at 16 or below with no sweep backlog, AND `owed_actor_reconcile_compared_total` rising.
- **NOTE-3.** The docs that named 16 games and one judge call as the pace now say 32, two calls with the reconcile yielding, the lever 0 to 32.

## The mined class is sampled at the actor's apply of the MINED word (FIX-974, bsv-low #974; L53's S3; 2026-10-29)

L53 served `collectAfterMinedMsP95` null through the loop (`classes.mined.applySamples` 0 over 8 blocks and 186 confirmed spends), so `invariants/owed-actor-collect-after-mined-p95` could neither pass nor fail. Where the sample is noted: the actor's answer echoes the body's `mined` (`object.rs`, `answer["mined"]`), the object notes `bars.collect_after_mined` on a first warm apply and `PendingBars::note_answer` puts the same answer's visibility into the `mined` class (`owed_actor.rs`), served `collectAfterMinedMsP95`. Why none of 133 cells noted one: `EventCtx::of` read the word at `facts.word` and `facts.frame.word`, fields NO published shape holds: a word event carries the overlay's tracker frame under `facts.frame` as `{txid, kind, body}` (`owed_events_publish::frame_note`, `tracker_events::note`) or the row's facts under `facts.row` (`READ_WORD_SQL`, wrapped `{marks, row}`), so `mined` was false on every event and every mined apply was sampled into the `queue` class instead. Traced on pair 12's hop sweep (`spec-hop-sweep`, vultr): the sweep `71fa3ec66d30…` broadcast at 16:17:00Z, `POST /arc-ingest` MINED at 16:24:17.499Z on the overlay; the app layer's logs name no txid past that line (the consumer logs per batch, the actor per DIFF), and the frame rode the queue behind 6,000 messages, where its `EventCtx` carried `mined: false`. Also stated: the direct pass left EVERY frame to the consumer, so even with the flag read the mined word's first apply was the queue's delivery and its sample the backlog's lag.

Now (1) `owed_actor::mined_word_event`: the MINED word's event is a `word` event carrying the `proof` frame (`MINED_FRAME_KIND`; the overlay notes it for a bump chaintracks verified, `verify_bump_detailed`'s `Valid` arm, and of a MINED push's two frames the publish carries that one); a SEEN, submit or MINED `callback` frame and a row-shaped word event (the tracker's own write: the tick's re-ask, the POST belt) are not: the class is the callback's road, the design's bar 3 (the callback's `at` is the event's write time). (2) The direct pass (`owed_events::direct_with_class`) ingests the mined word's frame through the port first (`DirectPort::ingest_frame`, the worker's is the consumer's own `ingest_frame`: the tracker's write, whose trigger marks the seat the spend names, the `word` note forgotten), then plans and calls it as any direct event under the `direct` class, counted `DirectReport::mined_frames`; a frame whose ingest faults and every other frame stay the consumer's. So the sample is the first apply on the writer's road (`/arc-ingest` MINED, the overlay's flush, `POST /internal/owed-direct`, the ingest, the actor), never the queue's backlog; the consumer's later delivery of the event re-ingests the frame (idempotent) and is a covered dupe or a redelivery, which samples nothing (`samples_visible`, #952's rule). Stated: a `proof` frame whose ingest the tracker refuses (a chaintracks read fault at the header) still samples the class once with the row's word not yet `mined` (rare, the next frame heals it); `publish_env` keeps ONE frame per txid per publish (`frame_of` by txid: a MINED push's `callback` frame is neither sent nor returned to the POST, the `proof` frame carries the same bump), pre-existing, not changed. The harness: no cell must change; the credit cells (`spec-hop-sweep`, `spec-refund-credit`, `spec-reload-credit`, `spec-owed-collect`, `collectedCrossDevice`) wait for their sweep's or settle's block, whose MINED push reaches `/arc-ingest` inside the loop, and the bars row reads the served key. Pins `--lib fix974` in `low-app-layer`: `fix974_a` (the shapes, the body, the echo, the note, the served bar; RED on 149869bf: "a proof frame is the mined word's event"), `fix974_b` (a dupe, a redelivery and a cold-born event never sample it; the key stands; green on 149869bf, the guard), `fix974_c` (the direct road over a frame port: the proof frame ingested, planned off its mark, called `mined: true`, noted once; the seen frame and a faulted ingest left to the queue; new API).

## The actor's judge stage in Phase C is the derivation; the unapplied figure is the actor's (FIX-983, FIX-982; bsv-low #983, #982 under #912; Wave E row E3, 2026-10-30)

The Phase C window (beta, 2026-10-30 03:15Z on a31436e6) read the direct class's apply p95 at 3,383 ms, its judge stage at 3,002 ms, against the design's 300 ms; the app layer's `POST https://owed-actor/apply` events of the flood minute (56 applies) read wall p95 3,649 ms against CPU p95 135 ms: the judge stage was AWAITS, not derivation. Named, with the fixture's counts (a warm apply of one game of the stranded hand): Phase B made ONE record batch (the marked read); Phase C made TWO (the read, then `ASKS_CLEAR` and one `ASK_PUT`) and a counters round trip, every one inside the measured apply, because `publish_belt_asks` ran in the turn and `apply_coalesced` folded the pass's remainder into `judge_ms`; and the judge asked the outside (CHAINTRACKS: the word views' header rechecks, the canonical roots) on every ROUND of an apply (4 asks on a three-round cold apply).

- **The asks lane** (`owed_actor::queue_belt_asks`, `ASK_LANE`, `drain_ask_lane`, `drain_lane_once`): the apply QUEUES its belt asks inside the turn (the lane's order is the turns' order, so a later apply's asks are never overwritten by an earlier one's) and the object drains the lane AFTER its answer under `wait_until` (`OwedActor::after_apply`, with the closure misses' counter and the bars' flush, which left the response path too; the alarm drains after its applies): one record batch a queued apply, the asks counted once a drain (`owed_actor_belt_asks_total`). An apply whose dated rows did not move since its games' last publish queues nothing (a per-game digest in the mirror's `meta`, `askdigest:<game>`); a drain that faults marks its identity (`ASK_FAULTED`) so its next apply publishes every game again. A warm Phase C apply is now ONE record read, as Phase B's (pin `fix983_a`; the lane's base-API probe RED on a31436e6: `2 against 1`).
- **The outside once per apply** (`ActorJudge::word_views`, `canonical_root`): a key answered in an earlier round of the same apply is taken from `Held`; the asks' time is its own stage, `outsideMs` (`APPLY_STAGES`, the fifth ring, served per class and over the classes), so `judgeMs` is the derivation and the mirror's own reads (pin `fix983_c`, RED on a31436e6: `left: 4 right: 1`). An empty ask is no ask (the slow-chaintracks pin seeds a word row per spender).
- **The flood served apart** (`FLOOD_CONCURRENCY` 8, `bars_class(class, concurrent)`, the class row `flood`): the caller's fan-out rides the body (`concurrent`: the direct pass's bodies, `direct_with_class`; the consumer batch's identities, `JudgeDb::note_concurrency` from `judge_plan`), and at 8 or more the object samples the call into `classes.flood` (`rowsVisibleFloodMsP95`, `floodConcurrency` beside), never into `classes.direct`, `rowsVisibleDirectMsP95` or the first-visibility bar, so the design's two bars read a WARM HAND. Measured natively, the hand's Phase C apply: 42 to 73 ms debug, 5 to 23 ms release (pin `fix983_b`, the 300 ms bar asserted in release); `fix983_d` (does not compile on a31436e6); `fix983_e` (the object's deferral, source).
- **#982, `oldestUnappliedEventAgeMs`** (`health_body`, `unapplied_event_age`): with the D1 judge off it read the fleet's oldest un-acked `owed_dirty` mark (the drain's `backlog:dirty` snapshot), and Phase C's marks are the age repair's (128 a scheduled pass: every D1 row older than a day with no D1 writer), the reconcile's `backfill` marks and the sweep's, each waiting for the sweep's stalled bound (600 s with a quiet consumer): beta read 590 s with `holding {count: 0}`, no event an actor held. Now, in EVERY phase, the figure is the oldest event an actor holds unapplied: the older of the oldest HELD event (`holding.oldestAtMs`, an actor not bootstrapped) and the oldest event whose apply FAULTED with no later apply of its identity landed (the queue row's `oldestUnappliedAtMs`, noted by the consumer's Phase C delivery and the direct pass), its source named (`oldestUnappliedEventSource`: `held`, `faulted`, null when every event is applied); the mark's age is served beside as `owedActor.oldestUnackedMarkAgeMs`. The bars row `owed-actor-oldest-unapplied` reads the amended key unchanged; bsv-low's `invariants.mjs` already reads the `owed-dirty-age` row informational once `OWED_ACTOR_READ` is served on (the cutover doc's "leaves the rules file" is bsv-low's edit). Pin `fix982_a` (RED on a31436e6, grafted: `left: Number(590000) right: Null`); `d2_actor_b_fold2_m7` amended.
- Stated, not in the apply: the compare's sampled fresh judgement (Phases A and B only; the reconcile's re-apply samples nothing since FOLD-16), the tip memo (the caller's, before the body), the consumer's acknowledgement batch (after the answer). Nothing else moves: the judgement's output, the frame, the schema digest, the bootstrap, the pace, the one-writer rule, the counters list.

## A courier-bytes payout waits on its sweep's block at the hop (FIX-988, bsv-low #988; L54's S3, 2026-10-30)

L54 pair 17: three `payout` `courier-bytes` rows ("the spend is seen but not mined yet", `chainWait: "block"`) never confirmed after their sweeps mined. The fold read it as the belt selecting from D1 `owed_rows` in Phase C; it does not: with `OWED_D1_JUDGE=false` the tick reads the actors' published `belt-ask` ledger (`ask_candidates`), and beta's own lines show the asks selected at their due dates (`candidates=6 asked=4 rearmed=2 calls` with the two pair-17 asks re-armed at 07:17Z, 07:29Z and 07:53Z). The defect was the ASK: `ask_of` answered `Ask::Home` for every courier-bytes payout, and `owed::courier_home_outputs` yields a CONFIRMED sweep's homes only, so an unconfirmed sweep's ask answered nothing, made no courier call and re-armed (`unanswered_period`, doubling) for good; nothing else asks the chain for it. Now (`owed_belt::payout_waits_on_block`) a courier-bytes payout whose row says `chainWait: "block"` asks the HOP's spend (the outpoint the row is keyed by): the spent-any ladder's word on it (the sweep `confirmed`) lands as the find's memo and #953 evidence, the actor re-judges on the hand-back and the row says the no-credit sentence; a confirmed one asks its home as before. Every phase reads `ask_of`, so Phases A and B change the same way. No migration, no new statement, no new table. Pins `--lib fix988` (`owed_actor::tests`): `fix988_a` (the captured hand-3 seat swept to its committed home by a courier-only sweep under the real actor mirror in Phase C: the ask published at its due date, D1 `owed_rows` empty, the tick at the due date asks `<hop>/0/spent`, the actor's row confirms, its next ask `home`; RED on `1ab40c95` as beta read it: `candidates: 1, rearmed: 1, calls: 0`), `fix988_b` (`ask_of` over both shapes; RED with the guard inert).
## Every first-party push to a seat goes through ONE gate (bsv-low #989, lane FIX-989, L54's S2, 2026-10-30)

PR 194's coalescer (`push_coalesce`, `PUSH_BUDGET_PER_MIN` 40, the sender's 80) lived per ISOLATE. L54's refusals at 06:55Z read `541 sends, and an identity makes at most 100 in the window`, the count rising across recipients (the relay charges the SENDER, the app layer's one key). 27 of 40 sampled refusals were inside an `OwedActor` (18 objects) and the rest in fetch isolates. Every owed actor (one Durable Object a seat, each in an isolate of its own) and every fetch isolate kept its own buckets, so the fleet sent 541. The actor's `owed-changed` was already queued and flushed through `push_to_seat`; the bypass was the per-isolate cell, not a raw call. Now `push_to_seat` and `flush_queued` call `push_coalesce::deliver`, which FORWARDS the pushes (at most `PUSH_GATE_BODY_MAX` 256 a body) to the `BOARD_VIEW` object named `PUSH_GATE_ACTOR` (`push-gate:v1`, route `/push-gate`, `serve_gate`). That object's isolate coalescer is the fleet's: one seat bucket a seat and one sender bucket, and the actor's and the D1 judge's pushes of one judgement coalesce under one key. A push in the gate's own isolate (`GATE_HOSTED`) is submitted there. With no `BOARD_VIEW` binding the isolate's coalescer stands. A gate fault answers `Failed` and never sends around it: the pot hook's park re-drives its event, and an `owed-changed` costs the seat its next re-read. No migration and no new class (a named `BoardView`). Stated: one DO subrequest per flush or pot push, and the gate's request lasts while its pushes wait out their windows. Pins `--lib fix989` (`a`: L54's burst through the gate, 30 actor isolates with p22's 90 events in a second plus 4 fetch isolates; 80 relay calls in the busiest minute, 0 refused, 0 given up, p22 at most 2 sends; RED with the gate made inert, 900 in a minute, 800 refused, 200 given up; `b`: that control; `c`: a gate fault sends nothing; `d`: the wiring).
## The operator's compare judges D1 fresh; the fleet compare is a standing surface (FIX-986-COMPARE-2, bsv-low #986; READ-986 items 1 and 4, 2026-10-30)

READ-986 (`docs/audit/READ-986-2026-10-30.md` on `lane/READ-986`) read the fleet compare of 2026-10-30 09:52Z: 42 of its 46 "diffs" were D1 STALE (a collected marker, a new game or a sweep filing written after D1's stored row was judged), because `GET /internal/owed-actor-rows` compared the actor with D1's STORED `owed_rows`, which Phase C leaves with no writer.

- **One compare** (`crates/low-app-layer/src/owed_actor/compare.rs`, `fresh_compare`): the actor's stored answer against the D1 judge's FRESH judgement of the identity's games, writing nothing (`owed_judge::judge_fresh_rows`: the judge's own hydration and derivation, `judged_rows`, the one the write calls too; each row built from the write's own binds, `stored_row_of`; ranked at the 500 ceiling as `CEILING_DELETE_SQL` ranks; no mark read or acknowledged, no state row, no push). The games: every game an actor row or a stored D1 row names, then the identity's games newest first, at most `FRESH_COMPARE_GAMES_MAX` (64); past it every side is restricted to the judged games and the answer says `d1.games.partial`. `diff`, `tipLag`, `tipLagTips`, `digests` and `heldByLag` keep their names and shapes over the fresh side; `d1.fresh: true`, `d1.tip`; D1's stored rows ride beside as `d1Stored` {`rows` (each `stale`: the D1 judge off, or judged before the Phase C flip the ring observed), `stale`, `d1Judges`, `phaseC`, `staleOutpoints` (stored against fresh), `digest`}; `diffFields` names each differing outpoint's fields with both values. A fault of the fresh judgement is a 503, never a compare against a stored row.
- **The routes** (bearer `INTERNAL_TOKEN`, 401 before any read): `GET /internal/owed-actor-rows?identity=` serves `fresh_compare`; `GET /internal/owed-actor-compare?after=&identity=` serves the stored compares {`identities`, `identitiesDiffering`, `identitiesHeldByLag`, `comparedWithinMs`, `staleD1`, `faultCount`, `diffs` [{`identity`, `diffCount`, `outpoints` [{`outpoint`, `held`, `fields`}], ..}], `heldByLag`, `faults`, `ringCursor`, `revolution`, `lastRevolutionMs`, `lastTickAt`, `next`}, 64 rows a page (`?identity=` adds its `row`); `POST {"identity"}` re-compares one identity now and writes its row; `POST {"tick": true}` runs one ring tick.
- **The ring** (`ring_tick`): its own `BOARD_VIEW` actor (`owed-fleet-compare`), whose alarm ticks every 60 s (set before the tick; every cron arms it, phase `owed.compare_arm`), so its D1 statements never share the belt alarm's invocation. Up to `OWED_FLEET_COMPARE_PER_TICK` identities a tick (16 by default, 0 pauses, at most 64) past its cursor (`owed_belt_state` `fleet-compare:ring`) over the identity set `GET /internal/owed-actor-identities` pages, walked by index SEEKS (`RING_IDENTITIES_SQL`: each step the least of four `MIN(identity)` seeks, the reconcile's `RECONCILE_NEXT_SQL`; the union the lever reads would read every identity past the cursor each tick, 551 rows on the tier's small seed; pin `fix986c2_f`, flat VM steps beside 2,000 more identities); an identity whose actor is out of reach or not bootstrapped is its row's fault with NO fresh judgement made; each identity's row written WITH the cursor in one batch (a tick the platform ends resumes after the last row written); no identity started past 800 statements less the largest identity's cost so far this tick, or past a 20 s wall. A short page wraps the revolution and drops the rows it did not refresh. The ring observes the Phase C flip (`owed-actor:phase-c` {`sinceMs`, `endedAtMs`}).
- **The table** `owed_actor_compare` (overlay migration 474; owner `low-app-layer`, transient): one row per identity, `diffCount` (the differences no pending mark holds), `heldByLagCount`, `staleD1Count`, `tipLagCount`, `outpoints` (JSON, at most 32, each with its fields and `held`), both digests, or `fault` (the actor out of reach or not bootstrapped, the fresh judgement's fault). `/health/invariants.owedActor.fleetCompare` {`identities`, `diffs`, `heldByLag`, `faults`, `oldestCompareAgeMs`} rides the owed events' one health statement (`owed_events::HEALTH_STATE_SQL`, a subquery). Deploy the overlay first (its certified count at 474).
- **The cost, stated.** The judge's hydration reads about four to eight statements a game (its per-pot and per-hop reads, not its chunk): measured natively on the frozen fleet (`fix986c2_e`), one compare at 64 games is 341 to 553 statements (the whale 553; 1,101 at 128 games). On local D1 (the budget tier, `owed-actor-rows-fresh`) the lived-in seat's fresh compare (64 of its 270 games) read 6,849 rows in 341 statements and wrote none. So a tick of heavy seats compares one or two identities, not sixteen, and a revolution of beta's 153 takes longer than the ten minutes sixteen a tick would give; the ring names each tick's `stopped` reason and statements on its `[owed-fleet-compare]` line. Rows read a day: up to about two such seats a minute is about 20 M rows a day, prod's whole `d1RowsReadPerDay`; a light seat reads far less, and the figure follows the fleet's weight. NOT bounded in rows here (the tick counts statements): for prod, set `OWED_FLEET_COMPARE_PER_TICK` low or `0` (the owner's call); beta's budget is 60 M.
- Pins (`-p low-app-layer --lib fix986c2`): `a` the route's D1 side is a fresh judgement (a held collected filing written after D1's stored judgement: no `diff`, the stored row `stale` and named in `staleOutpoints`, nothing written; the fresh rows equal the write's; RED with main's rule grafted, the stored rows as the D1 side: the payout named in `diff`); `b` the ring covers every identity once a revolution (ticks of 3, 3, 1 at three a tick), resumes after a tick the platform ends, pauses, walls, drops a gone identity, observes the flip (RED with the cursor saved only at the tick's end, and with the wrap removed); `c` the ring's row is the route's compare (one function; the POST's row equal; `GET`'s shapes; a pending mark moves the difference to `heldByLag`; the health aggregate); `d` the routes' bearer and wiring; `e` one compare's statements on the frozen fleet and no statement for an identity it cannot compare; `f` the ring's identity page is seeks over the identity list (RED with the union: 92,725 then 124,725 VM steps). The budget tier's cells `owed-fleet-compare-tick`, `-one`, `owed-actor-rows-fresh`, `owed-fleet-compare-read` (`tools/lane-893/owed_fleet_compare.mjs`, in the belt tier after its fleet cell; the tier binds no owed actor, so the tick and the POST record each identity's fault and the fresh judgement's cost is the rows route's). Measured on local D1 (`ceilings.json`, 1.5x): the tick 60 reads, 8 writes, 9 statements; the fresh route 6,849 reads, 0 writes, 341 statements; the read 5 reads, 3 statements. The full `make ci-d1-budget` of this lane went red only at the #934 burst cell (2,844 ms against its 2,000 ms bound, the host at load 12 with this lane's tests running beside it); run alone it was green (1,655 ms), and the cells after it (the belt tier with these, #911's and #912's three) each green alone. Not built here: READ-986 items 2, 3 and 5 (the actor's classification of a settle with no stored verdict, its differential pin, the re-bootstrap) are lane FIX-986's.
