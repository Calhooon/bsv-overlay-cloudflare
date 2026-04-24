# overlay-engine

Core BSV Overlay Services engine. Platform-agnostic — no HTTP, no
storage implementation, no Cloudflare deps. Defines the traits that
everything else plugs into.

Part of the [`bsv-overlay-cloudflare`][root] workspace. See the
root README for how the four crates compose.

[root]: ../..

## What this crate provides

- **`Engine`** — the central orchestrator. Its public surface mirrors
  mainline `@bsv/overlay@2.2.0`:
  - `submit(&TaggedBEEF, SubmitMode)` — 3-phase pipeline: SPV verify,
    dedupe, dispatch to the topic manager(s), record admissions, call
    `Broadcaster`.
  - `lookup(&LookupQuestion)` — dispatch to a registered lookup service.
  - `start_gasp_sync()` — pull outputs from peers per-topic using the
    GASP protocol.
  - `sync_advertisements()` — drive the `Advertiser` to issue /
    find / revoke SHIP/SLAP records for the topics we host.
  - `evict_output()` — surgical removal of a stored output.

- **Traits** that wrap the moving parts of an overlay:

  | Trait | Purpose | Implementations |
  |---|---|---|
  | `Storage` | Transaction + output persistence, applied-tx dedupe, GASP host-sync cursor | `MemoryStorage` (in-crate, test/dev); `D1Storage` (in `overlay-cloudflare`) |
  | `TopicManager` | Per-topic PushDrop admission validator. Returns `outputsToAdmit` + `coinsToRetain` from a BEEF. | `SHIPTopicManager`, `SLAPTopicManager`, etc. (in `overlay-discovery`) |
  | `LookupService` | Per-service query handler. Responds to `LookupQuestion` with an `output-list` or `formula`. | `SHIPLookupService`, etc. (in `overlay-discovery`) |
  | `Advertiser` | Issues, finds, and revokes on-chain SHIP/SLAP advertisements for self. | `WalletAdvertiser` (in `overlay-discovery`), `CloudflareAdvertiser` (in `overlay-cloudflare`) |
  | `Broadcaster` | Propagates an admitted transaction (e.g. ARC, peer overlays). | `WorkerArcBroadcaster`, `WorkerBroadcaster` (in `overlay-cloudflare`) |
  | `ChainTracker` | Merkle-root verification for SPV. | `WorkerChainTracker` (in `overlay-cloudflare`) |

- **`MemoryStorage`** — a reference in-memory `Storage` impl for tests
  and local dev. Enable with `features = ["memory-storage"]`.

- **`EngineBuilder`** — chainable config for constructing an `Engine`
  without dozens of constructor positional args.

- **GASP sync** (`gasp.rs`) — pure-Rust implementation of the Graph
  Aware Sync Protocol for overlay-to-overlay ingestion. Works over any
  `GASPRemote` transport.

## Quick taste

```rust
use overlay_engine::{
    engine::{Engine, EngineConfig},
    storage::MemoryStorage,
    types::{TaggedBEEF, SubmitMode},
};

let engine = Engine::with_all(
    topic_managers,            // HashMap<String, Box<dyn TopicManager>>
    lookup_services,           // HashMap<String, Box<dyn LookupService>>
    Box::new(MemoryStorage::new()),
    advertiser,                // Option<Box<dyn Advertiser>>
    broadcaster,               // Option<Box<dyn Broadcaster>>
    arc_broadcaster,
    chain_tracker,
    EngineConfig::default(),
);

let steak = engine.submit(&tagged_beef, SubmitMode::CurrentTx).await?;
```

## Testing

```bash
# All tests, including property tests and integration tests
cargo test -p overlay-engine --features memory-storage

# Live tests (hit a deployed overlay) — requires OVERLAY_URL env var
OVERLAY_URL=https://<your-overlay>.workers.dev \
    cargo test -p overlay-engine --features memory-storage -- --ignored
```

Categories: unit tests, engine-parity integration tests (ported from
the TS Engine's test suite), cross-SDK (byte-exact BEEF/txid checks
against `@bsv/sdk`), property tests via proptest, and live smoke
tests against deployed worker(s).

## License

Licensed under either of [Apache-2.0](../../LICENSE-APACHE) or
[MIT](../../LICENSE-MIT) at your option.
