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
- `bsv-middleware-cloudflare`: BRC-103/104 auth middleware for CF Workers (crates.io), pinned exactly `=0.4.1` in both workers crates since 2026-10-09 (bsv-low #588: `low-app-layer` at `b7777bd`, `overlay-cloudflare` at `2749841`, which links only `init_panic_hook`), so the workers' lock holds ONE middleware (0.4.1) beside `bsv-middleware-core` 0.1.0, the same lineage the low-watchtower pins. 0.4.0 was a major bump: it moved the rules to `bsv-middleware-core` 0.1.0, re-exported at the 0.3 paths, the `AuthSessionStore` DO and the session lane (D12) unchanged; 0.4.1 answers every authentication refusal as its own 401 `{"status":"error","code":"ERR_INVALID_AUTH"|...}` where 0.3.8 raised four as `Err`, which the app layer's front door rendered 400 `authentication handshake failed: ...`; an `Err` is now a fault only and is still that 400. Before it, `=0.3.8` from 2026-10-08 (bsv-low #577: the registry 0.3.8, main `c39642f`, the git `[patch]` to the session-lane branch gone; 0.3.7 had folded the session lane into main). Never 0.3.7.
- `worker`: Cloudflare Workers Rust SDK, pinned `0.8` (the workers' lock holds 0.8.5)

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
charged 570: 30 MB). It stops 111,848 transactions of no input and no output.

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
compared) refuse invalid bytes only; then the route's EF work bound
(`routes.rs` `MAX_SUBJECT_EF_BYTES` 256 KB, `MAX_BATCH_EF_BYTES` 2 MB: a 429
"retry via fallback" before the door, #211, the one size answer left on the
path). The pin's 2 MB subject passes the DOOR; through the route it meets that
429 first. Those parses hydrate the body before the door's limb is read, and
nothing but the isolate bounds them (the land lens E585-LAND-M1, measured
natively, release: a 9,900,010 byte body of 900,000 minimal transactions
peaks at 1,427,616,056 bytes in the route's parse and EF conversion, then
answers the 429; the wide BUMP, 2^18 leaves in 9,830,056 bytes, 190,320,572
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
at step (1) held 190 MB natively; the 900,000 minimal transactions are
`would-fail(ef-over-cap)`, the arm's 429 (#211, NL-6's to fix), after its
whole-body parse and EF conversion (1,427,616,056 bytes natively, the land
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
`would-fail(ef-over-cap)` is the arm's own 429 (`MAX_SUBJECT_EF_BYTES` 256
KB, `MAX_BATCH_EF_BYTES` 2 MB), mirrored. The memory of a census pass is
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
`would-fail(ef-over-cap)`, one with two tips is
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
nothing, found no coin and carries more UNPROVEN, UNLANDED bodies than 16
reads settle (six single-input ancestors or more: five are settled, measured
by the #576 delta-2 lens; fewer with more inputs) is "not
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
delivery (`dead_letters_ceiling_deferrals_total`, up to 101 per letter;
delta fold D-L1), shown as `ceiling.full`, and LOST after ~48 h at the
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
A NEW `not_now` letter is deferred like a letter at the ceiling (the same
backoff, LOST after ~48 h; counted on every delivery,
`dead_letters_not_now_deferrals_total`, apart from the ceiling's counters)
at any of three bounds (`ceiling_verdict`, and again inside the park's own
statement, below):
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
class's bounds for as long, an honest one included during a flood; a
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
resumes waits for that one's convergence or its 60 passes.

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

- **Stated limit (bsv-low #581, the delta-4 lens D4-L1):** with one dead-letter consumer and up to 101 deliveries per held-back "not now" letter, a flood can delay honest fault letters and let them expire at the queue's retention; the fix when it matters is a queue per class.
