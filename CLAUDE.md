# bsv-overlay-cloudflare

Rust port of [`@bsv/overlay-express`][ts] deployed on Cloudflare Workers.
Three-crate workspace compiled to WebAssembly. 1:1 protocol parity with
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
└── parity-harness/             # Rust CLI that diffs our Worker vs the
    ├── src/                    #   reference @bsv/overlay-express@2.2.0 docker
    └── corpus/                 #   43 JSON request/response scenarios
```

## Dependencies

- `bsv-rs` — BSV SDK for Rust (crates.io, `overlay` feature)
- `bsv-middleware-cloudflare` — BRC-103/104 auth middleware for CF Workers (crates.io), pinned `0.3` — the same lineage the low-watchtower and low-app-layer pin
- `worker` — Cloudflare Workers Rust SDK, pinned `0.8`

**THE WORKSPACE MAY HOLD EXACTLY ONE `worker` VERSION** (bsv-low #348). `worker-build` — which runs only at DEPLOY time — resolves `worker` from the *workspace* `Cargo.lock` and takes the LOWEST version it finds there, for every crate, regardless of which crate directory it was invoked from; its per-crate disambiguation is dead code (off-by-one in `Lockfile::get_package_version`, verified in worker-build 0.7.5 / 0.8.4 / 0.8.5). Plain `cargo build` is perfectly happy with two majors, so a split is invisible to every native gate: `low-app-layer` sat undeployable for a month with `make ci` green throughout. If a crate ever needs a different `worker`, it must leave the workspace *and* stop depending on anything in it — a path dev-dep drags the other version straight back into the lock. `make ci-deploy` (part of `make ci`) is what enforces this: a lock/pin preflight plus a real `wrangler deploy --dry-run` of all three deployable configs.

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

## Testing

```bash
# Fast unit + integration (no network)
cargo test --workspace --features overlay-engine/memory-storage

# Property tests (proptest, 256 cases each)
cargo test --workspace --features overlay-engine/memory-storage --test property_tests

# Live tests — hit a deployed overlay. Require OVERLAY_URL env var.
OVERLAY_URL=https://<your-overlay>.workers.dev \
    cargo test --workspace --features overlay-engine/memory-storage -- --ignored
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

**Three deployable configs**, all built from the one workspace lock: `crates/overlay-cloudflare/wrangler.toml` (`bsv-overlay-cloudflare`), `crates/overlay-cloudflare/wrangler.low.toml` (`low-overlay`, LIVE — `wrangler deploy --config wrangler.low.toml`), and `crates/low-app-layer/wrangler.toml` (`low-app-layer`). Their `[build]` worker-build pins must all match each other and the lock's `worker`; `make ci-deploy` builds all three and refuses on drift. **A green `make ci` without `ci-deploy` is not evidence a worker can be deployed** — that gap is exactly bsv-low #348.

- **Admin auth**: Bearer token on all `/admin/*` routes except `/admin/config`.
- **Cron**: `*/15 * * * *` for ad sync + GASP peer sync.
- **Extensions**: set `ENABLE_EXTENSIONS=true` to register UHRP / Agent /
  DmDelegation topic managers + lookup services beyond the mainline
  SHIP/SLAP baseline.
