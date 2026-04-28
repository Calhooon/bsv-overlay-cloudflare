# overlay-cloudflare

The Cloudflare Workers deployment for BSV Overlay Services. This is
the binary crate — wires the [`overlay-engine`][engine] + 
[`overlay-discovery`][discovery] plugins into a WASM worker that
serves the 27 mainline-parity HTTP routes, runs a `*/15 * * * *` cron
for ad sync + GASP peer sync, and consumes a mutation queue for
onSteakReady-style delayed processing.

Part of the [`bsv-overlay-cloudflare`][root] workspace.

[engine]: ../overlay-engine
[discovery]: ../overlay-discovery
[root]: ../..

## What this crate provides

### Three Worker events

| Event | Module | Purpose |
|---|---|---|
| `#[event(fetch)]` | `lib.rs::main` | Dispatches HTTP requests to handlers in `routes.rs`. |
| `#[event(scheduled)]` | `lib.rs::scheduled` | Every 15 min: `engine.sync_advertisements()` (re-issues our SHIP/SLAP records), `engine.start_gasp_sync()` (pulls from configured peers), peer-crawl bridge for non-GASP peers, janitor health-check sweep. |
| `#[event(queue)]` | `lib.rs::queue_handler` | Consumes `overlay-mutations` queue messages (deferred `engine.submit()` calls). Dedup-safe via `applied_transactions`. |

### D1-backed trait implementations

- **`D1Storage`** — `overlay_engine::Storage` over Cloudflare D1 (SQLite).
- **`D1SHIPStorage` / `D1SLAPStorage` / `D1UHRPStorage` / `D1AgentStorage` / `D1DmDelegationStorage` / `D1SonicstarStorage`** — per-plugin discovery storage.
- **`D1BanStorage`** — BanService equivalent (tracks `{type, value, bannedAt, bannedBy, reason}`).

### Cloudflare-native adapters

- **`CloudflareAdvertiser`** (`advertiser.rs`) — `overlay_engine::Advertiser` impl that talks to a `@bsv/wallet-toolbox` storage backend via [`bsv-middleware-cloudflare`][mwc]'s `WorkerStorageClient`. Creates / finds / revokes on-chain SHIP/SLAP PushDrops.
- **`WorkerBroadcaster`** / **`WorkerArcBroadcaster`** — `Broadcaster` impls for peer fan-out (SHIP) and TAAL ARC (miner broadcast).
- **`WorkerChainTracker`** — `ChainTracker` impl over a ChainTracks-API endpoint.
- **`WorkerHealthChecker`** — `HealthChecker` impl used by the janitor.
- **`WorkerGASPRemote(Factory)`** — GASP-over-HTTP transport.

### Supplementary modules

- **`janitor.rs`** — periodic health-check sweep of every advertised host; auto-bans hosts failing 3 consecutive checks.
- **`peer_crawler.rs`** — compatibility bridge for mainline overlays that don't expose `/requestSyncResponse` (pulls via `/lookup findAll` + `/submit` instead).
- **`mainnet_fanout.rs`** — post-admission broadcast of admitted records to other mainnet overlays.
- **`queue.rs`** — mutation queue envelope + message encoding.

[mwc]: https://crates.io/crates/bsv-middleware-cloudflare

## Configuration

Fully env-var driven via `wrangler.toml` `[vars]` + CF secrets. The
real `wrangler.toml` is gitignored on purpose — copy the tracked
`wrangler.toml.example` and fill in your own values:

```bash
cp wrangler.toml.example wrangler.toml
```

```
Vars (in wrangler.toml):
  ENVIRONMENT          free-text deploy tag
  HOSTING_URL          this worker's public URL (FQDN we advertise as)
  CHAIN_TRACKER_URL    ChainTracks-compatible SPV endpoint
  WALLET_STORAGE_URL   @bsv/wallet-toolbox storage backend
  TOPIC_MANAGERS       csv: tm_ship,tm_slap[,tm_uhrp,tm_agent,tm_dm_delegation,tm_sonicstar]
  LOOKUP_SERVICES      csv: ls_ship,ls_slap[,ls_uhrp,ls_agent,ls_dm_delegation,ls_sonicstar]
  ENABLE_EXTENSIONS    "true" / "false"

Secrets (via `wrangler secret put`):
  ADMIN_TOKEN          Bearer token for /admin/* routes
  SERVER_PRIVATE_KEY   secp256k1 hex — overlay identity; signs SHIP/SLAP ads
  TAAL_API_KEY         optional — enables /arc-ingest + ARC broadcast
```

## Deploying

```bash
# One-time
npx wrangler d1 create bsv-overlay
# paste database_id into wrangler.toml

npx wrangler secret put ADMIN_TOKEN
npx wrangler secret put SERVER_PRIVATE_KEY
npx wrangler secret put TAAL_API_KEY      # optional

# Ship
CLOUDFLARE_API_TOKEN="<token>" \
CLOUDFLARE_ACCOUNT_ID="<id>" \
    npx wrangler deploy
```

## Local dev

```bash
npx wrangler dev --local
# Worker on http://127.0.0.1:8787
```

Or use the workspace-root Makefile target that pins harness-compatible
vars:

```bash
make wrangler-dev
```

## License

Licensed under either of [Apache-2.0](../../LICENSE-APACHE) or
[MIT](../../LICENSE-MIT) at your option.
