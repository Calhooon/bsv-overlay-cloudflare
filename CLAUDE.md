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
- `bsv-middleware-cloudflare` — BRC-103/104 auth middleware for CF Workers (crates.io), pinned exactly `=0.3.8` since 2026-10-08 (bsv-low #577; the git `[patch]` to the session-lane branch is gone) — the same lineage the low-watchtower and low-app-layer pin. `low-app-layer` asks `0.3.8` and the workers' lock holds the registry 0.3.8 (main `c39642f`) with no `[patch]` (bsv-low #577); its session lane (D12) rode a git rev of the `session-lane` branch until 0.3.7 folded it into main. Never 0.3.7
- `worker` — Cloudflare Workers Rust SDK, pinned `0.8`

**THE WORKSPACE MAY HOLD EXACTLY ONE `worker` VERSION** (bsv-low #348). `worker-build`, which runs only at DEPLOY time, resolves `worker` from the *workspace* `Cargo.lock` (the workers' one, `workers/Cargo.lock`; the root lock holds no `worker`) and takes the LOWEST version it finds there, for every crate, regardless of which crate directory it was invoked from; its per-crate disambiguation is dead code (off-by-one in `Lockfile::get_package_version`, verified in worker-build 0.7.5 / 0.8.4 / 0.8.5). Plain `cargo build` is perfectly happy with two majors, so a split is invisible to every native gate: `low-app-layer` sat undeployable for a month with `make ci` green throughout. If a crate ever needs a different `worker`, it must leave the workspace *and* stop depending on anything in it: a path dev-dep drags the other version straight back into the lock. `make ci-deploy` (part of `make ci`) is what enforces this: a lock/pin preflight plus a real `wrangler deploy --dry-run` of all three deployable configs.

## HTTP route set

27 routes total, matching `@bsv/overlay-express@2.2.0` exactly:

```
GET  /, /health, /health/live, /health/ready,
     /listTopicManagers, /listLookupServiceProviders,
     /getDocumentationForTopicManager,
     /getDocumentationForLookupServiceProvider
POST /submit, /lookup, /arc-ingest (gated on TAAL_API_KEY),
     /requestSyncResponse, /requestForeignGASPNode
GET  /admin/config (unauth)
GET  /admin/stats, /admin/ship-records, /admin/slap-records,
     /admin/bans
POST /admin/health-check, /admin/ban, /admin/unban,
     /admin/remove-token, /admin/syncAdvertisements,
     /admin/startGASPSync, /admin/evictOutpoint, /admin/janitor
```

Admin routes except `/admin/config` require `Authorization: Bearer <ADMIN_TOKEN>`.

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
than native; budget CPU accordingly for big covenant legs.

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

The anchor verify runs peer-chosen scripts with no work bound under the Worker CPU cap (bsv-low #557).

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
- A fault AFTER them (the spent mark of a stale coin, a lookup notification,
  the consumed-by update, the applied row) leaves the transaction's outputs
  and deletes the stale coins all the same, as the reference does. `c9921ee`
  skipped the delete on any fault and left two unspent heads.
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
names nothing (all 16 of this workspace) an admitting successor is asked
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
earlier ones left). Inside that, at most 16 per topic are spent on bodies the
BEEF does not prove and the store does not hold as landed (the second delta
fold, M2): a proven body and one whose applied row or held output answers
"landed" are read and cost nothing against the 16, though an unproven body
needs room under it for the reads that find it landed (with 15 spent, one
landed by a held output and no applied row is refused at its second read).
"Proven" is the BEEF's word here (a body carrying a merkle path): no proof is
checked at this question (the SPV walk and the anchor check are where proofs
are checked), it switches the 16 off for that body and nothing else, and the
256 do not ask it. A GASP finalize
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
again only if the manager admits again on the replay: a manager whose rule
needs the previous coin admits nothing there (the coin is deleted), under
GASP there is no replay, and since H2 the replay is a dupe once a successor
spent ANY output of that transaction, so a sibling output whose hook faulted
is not told again either; the reference tells nobody twice. (3) The lookup
services are told of a stale coin's spend, and of each inserted output,
BEFORE the transaction has landed (the mark and `output_spent` run before
the inserts, as in the reference): on an insert fault the store keeps the old
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
25), OLDEST first, so a predecessor's letter is replayed before its
successors'; each replay is the same bytes through the same door,
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
deeper than the budget (#555), the unbounded anchor verify (#557), a "not
now" over a BEEF above 90,000 bytes answering 502 (#568). It cannot tell a faulted predecessor from one nobody submitted
yet: the successor is "not now" until it lands. And a transaction that admits
nothing, found no coin and carries more UNPROVEN, UNLANDED bodies than 16
reads settle (five single-input ancestors, fewer with more inputs) is "not
now" on every submit, where the reference records it: three retries and a
dead letter each. So is one whose question needs more than 256 reads over
the submit's topics, whatever its bodies are (a landed body costs one read by
its applied row or two by a held output; a proven, unlanded one two and one
more per input: 86 proven single-input parents nobody holds, or 43 under two
topics): the reads of proven and landed bodies are free of the 16 and not of
the 256, so the question never follows the BEEF past a constant. (5) What D1 does with a statement whose caller stopped waiting, or was
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

## The dead letters (bsv-low #576)

A mutation the queue dead-letters (a `/submit` or `/arc-ingest` replay that
was not durable on any of its 1 + `max_retries` deliveries, an e1d "not now"
over a predecessor that does not land included) is never lost and never
re-driven blind (`dead_letters.rs`). The main consumer notes each failed
replay's fault and attempt on a `failing` row (the platform gives a Rust
consumer no delivery count) and resolves the row on an ack. The DLQ consumer
(the same `#[event(queue)]`, branched on a queue name ending `-dlq`; bound in
`wrangler.toml`, prod and beta of `wrangler.low.toml`, `max_batch_size = 10`,
`max_retries = 10`, no DLQ of its own) PARKS the message as is in
`mutation_dead_letters` (key: subject txid by D5 and the sorted topics; the
fault, the attempts, `parked_at`, a history entry per park), once: a
redelivery of a parked letter changes nothing. Nothing deletes from the table
(never-wipe, rebuild `lost`: once the DLQ acks, the row is the only copy).

`POST /internal/redrive-dead-letters` (bearer `INTERNAL_TOKEN`, as
`/internal/reorg`), body `{"limit"?: n, "txid"?: "<key>"}`: `limit` default
25, clamped to 200; reads the oldest parked rows (or one txid's), claims each
by a compare-and-set (`status = 'parked' AND redrives = <read>`) and only then
sends it once to `MUTATION_QUEUE` as a fresh message (its attempt count reset
to 0: 1 + `max_retries` deliveries again), stamped `reason = "redrive"` and
its key, so a re-death parks the SAME row with its history. Two calls never
enqueue one letter twice; a send fault reverts the claim. A letter re-driven
`MAX_REDRIVES` (3) times that parks again is exhausted: never selected again,
listed in `/health/invariants.deadLetters.exhausted` (with the counts by
status, the oldest parked and the last re-drive). Counters
`dead_letters_parked_total`, `dead_letters_redriven_total`,
`dead_letters_still_failing_total`. Nothing re-drives on its own (S2: a dead
letter is the operator's decision). Reference parity: ts-stack's
`overlay-express` has no queue and no dead letter; this lifecycle is our
platform's addition. Limits, stated: eleven D1 faults in a row on the DLQ
consumer's park lose that letter (logged with its txid); a claimed letter
whose send faulted and whose revert faulted too stays `redriven`, unsent,
shown in the health counts. Pins: `cargo test --manifest-path
workers/Cargo.toml -p bsv-overlay-cloudflare --lib e576`; the route tier
`tools/lane-e576/dead_letter_route_ci.mjs` (`make ci-d1-budget`: a real
"not now" successor dead-lettered and parked through the local queue, the
lever's bearer, limit, one-enqueue claim and ceiling, a re-park with its
history, the health block), RED on `835b80c`.

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

The budget bounds a TICK. Nothing bounds a GRAPH (parity: the reference has
no node cap). So a chain reaches a node over several ticks only as far as the
peer lists it as several UTXOs (a record or a second output along the way,
several shards); ONE graph whose own walk outlasts the budget (a head chain
whose tip is its only UTXO) is still dropped whole on every tick, never
admitted, each tick a failed attempt toward quarantine. Resuming a walk needs
its fetched nodes persisted across ticks, which the `Storage` trait does not
offer.

`TopicSyncResult` reports `finalized_graphs`, `deadline_dropped_graphs` (at
most one per peer per sync; the same count tick after tick with no
`finalized_graphs` and no `cursor_moves` is the case above) and
`cursor_moves` (peer, from, to); the worker's `Scheduled: GASP sync` line
carries the totals and the per-topic line the cursors (`discarded_graphs` is
on the first totals line). Pins: `cargo test -p bsv-overlay-engine --features
memory-storage --test gasp_topic_manager i552` (and `fold_high1`).

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
