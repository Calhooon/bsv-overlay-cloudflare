# bsv-overlay-cloudflare

**A BSV Overlay Services node on Cloudflare Workers — byte-for-byte
protocol-compatible with [`@bsv/overlay-express`][ts-express]@2.2.0.
Rust, compiled to WebAssembly. SHIP/SLAP-native, GASP-synced, $5/mo.**

[ts-express]: https://github.com/bsv-blockchain/overlay-express

---

## What this is

A drop-in replacement for the reference TypeScript overlay node
that runs entirely on Cloudflare primitives — Workers (WASM) + D1 +
Queues. Every HTTP response shape, PushDrop admission rule, GASP
wire frame, and admin-route contract matches
`@bsv/overlay-express@2.2.0` at wire level. A `@bsv/sdk` client
pointed at this worker behaves identically to one pointed at
`users.bapp.dev` or any other mainline overlay.

```
┌──────────┐    /submit       ┌─────────────────────────┐    D1 SQLite  ┌──────────┐
│ Client   │─────────────────▶│  Cloudflare Worker      │──────────────▶│ outputs  │
│ (bsv-sdk)│    /lookup       │  (Rust → WASM)          │               │ applied_ │
└──────────┘                  │                         │               │ transac- │
                              │  tm_ship  ls_ship       │               │ tions    │
                              │  tm_slap  ls_slap       │               │ ship_rec │
                              │  tm_uhrp  ls_uhrp       │               │ slap_rec │
                              │  tm_agent …             │               │ uhrp_rec │
                              └────┬───────────┬────────┘               │ …        │
                                   │           │                        └──────────┘
                   /request        │           │  ad sync +
                   SyncResponse    │           │  GASP peer sync
                                   ▼           ▼
                             ┌───────────┐ ┌────────────────────┐
                             │  Peer     │ │  Scheduled         │
                             │ overlays  │ │  (*/15 * * * *)    │
                             │ (bsvb.tech│ └────────────────────┘
                             │  babbage, │
                             │  …)       │
                             └───────────┘
```

## Workspace layout

Four Rust crates, each with its own README:

| Crate | What it is |
|---|---|
| [`crates/overlay-engine`](./crates/overlay-engine) | **Core library.** Platform-agnostic Engine — the 3-phase `submit()` pipeline, `lookup()`, GASP sync orchestration, merkle proofs. Defines the `Storage`, `TopicManager`, `LookupService`, `Advertiser`, `Broadcaster` traits. No HTTP, no storage implementation. |
| [`crates/overlay-discovery`](./crates/overlay-discovery) | **Topic managers + lookup services.** SHIP, SLAP, UHRP, Agent-Registry, DmDelegation. Each implements `TopicManager` and `LookupService` from `overlay-engine`. PushDrop admission rules, BRC-87 name + URI validation, WalletAdvertiser for on-chain SHIP/SLAP issuance. |
| [`crates/overlay-cloudflare`](./crates/overlay-cloudflare) | **Cloudflare Worker binary.** Wires the engine + discovery plugins into `#[event(fetch)]` (HTTP), `#[event(scheduled)]` (cron), and `#[event(queue)]` (mutation queue). D1-backed `Storage` and discovery-storage impls. BRC-31 wallet client for advertiser signing. |
| [`parity-harness`](./parity-harness) | **Differential test oracle.** Replays a 43-entry JSON corpus against both a mainline `@bsv/overlay-express@2.2.0` reference (running in Docker) and this worker (`wrangler dev`), canonicalises responses, diffs byte-for-byte. Exits non-zero on any unnoted divergence. |

Composition:

```
           ┌───────────────────────────────┐
           │     overlay-engine            │◀── traits: Storage, TopicManager,
           │  (Engine, GASP, Advertiser)   │    LookupService, Advertiser,
           └──────────┬────────────────────┘    Broadcaster, ChainTracker
                      │ depends on
                      ▼
           ┌───────────────────────────────┐
           │     overlay-discovery         │    SHIP/SLAP/UHRP/Agent/Delegation
           │  (TopicManager + LookupService│    implementations
           │   impls for 5 protocols)      │
           └──────────┬────────────────────┘
                      │ depends on
                      ▼
           ┌───────────────────────────────┐
           │     overlay-cloudflare        │    CF Worker binary — D1 storage,
           │  (wrangler.toml + #[event]s)  │    wallet-infra advertiser, cron
           └───────────────────────────────┘
                            ▲
                            │ black-box HTTP
                            │
           ┌───────────────────────────────┐
           │     parity-harness            │
           │  (diffs wrangler-dev vs the   │    Also calls mainline
           │   mainline 2.2.0 Docker)      │    reference/ docker on :8090
           └───────────────────────────────┘
```

`overlay-engine` is the substitutable core: another deployment target
(Axum on bare metal, Lambda, etc.) swaps `overlay-cloudflare` for a
different binary crate that wires the same engine + storage trait
differently.

## Parity

`make parity-clean && make harness` stands up the actual reference
`@bsv/overlay-express@2.2.0` in Docker, runs this worker on
`wrangler dev`, and diffs 43 corpus scenarios byte-for-byte.

Every divergence in the harness output is classified — either a
documented behavioral difference, a platform-specific adaptation, or
a state-dependent asymmetry. No unexplained surprises.

Run the harness locally — it writes `PARITY_REPORT.md` to the repo
root on each invocation:

```bash
make reference-up      # two shells, long-running
make wrangler-dev
make harness           # single-shot diff
```

## The 27 routes

Same set as mainline 2.2.0 — nothing added, nothing missing:

```
GET  /, /health, /health/live, /health/ready
GET  /listTopicManagers, /listLookupServiceProviders
GET  /getDocumentationForTopicManager
GET  /getDocumentationForLookupServiceProvider
POST /submit, /lookup
POST /arc-ingest              (gated on TAAL_API_KEY)
POST /requestSyncResponse, /requestForeignGASPNode
GET  /admin/config            (unauth)
GET  /admin/stats, /admin/ship-records, /admin/slap-records, /admin/bans
POST /admin/health-check, /admin/ban, /admin/unban
POST /admin/remove-token
POST /admin/syncAdvertisements, /admin/startGASPSync, /admin/evictOutpoint
POST /admin/janitor
```

All `/admin/*` routes except `/admin/config` require
`Authorization: Bearer <ADMIN_TOKEN>`.

## Topic managers + lookup services

Env-var opt-in — set `TOPIC_MANAGERS` and `LOOKUP_SERVICES` in
`wrangler.toml`:

| Topic manager | Lookup service | Purpose |
|---|---|---|
| `tm_ship` | `ls_ship` | Service Host Identity Protocol — who hosts what topics |
| `tm_slap` | `ls_slap` | Service Lookup Availability Protocol — who hosts what lookup services |
| `tm_uhrp` | `ls_uhrp` | Universal Hash Resolution — content-addressed file advertisements |
| `tm_agent` | `ls_agent` | Agent Registry — identity key → endpoint + capabilities |
| `tm_dm_delegation` | `ls_dm_delegation` | Delegation-revocation anchors |

Default (both unset) = mainline parity set: `tm_ship,tm_slap` only.

## Quickstart

```bash
# Install deps (wrangler, worker-build)
npm install

# Local dev
npx wrangler dev --local

# Or run the parity harness in two long-running shells + a diff shell:
make reference-up      # mainline @bsv/overlay-express@2.2.0 docker on :8090
make wrangler-dev      # this worker on :8787
make harness           # writes PARITY_REPORT.md
```

## Deploy

```bash
# One-time setup
npx wrangler d1 create bsv-overlay
# → paste the returned database_id into crates/overlay-cloudflare/wrangler.toml

# Required secrets
npx wrangler secret put ADMIN_TOKEN
npx wrangler secret put SERVER_PRIVATE_KEY
# Optional — enables /arc-ingest and ARC broadcast of admitted transactions
npx wrangler secret put TAAL_API_KEY

# Deploy
cd crates/overlay-cloudflare
CLOUDFLARE_API_TOKEN="<token>" CLOUDFLARE_ACCOUNT_ID="<id>" wrangler deploy
```

See [`CLAUDE.md`](./CLAUDE.md) for the deeper architecture, testing
matrix, and deployment reference.

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](./LICENSE-APACHE))
- MIT License ([`LICENSE-MIT`](./LICENSE-MIT))

at your option.
