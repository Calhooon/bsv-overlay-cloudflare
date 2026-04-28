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
│   ├── overlay-discovery/      # SHIP/SLAP/UHRP/Agent/DmDelegation/SonicStar plugins
│   │   └── src/
│   │       ├── ship/           # SHIPTopicManager, SHIPLookupService, storage trait
│   │       ├── slap/           # SLAP equivalents
│   │       ├── uhrp/           # UHRP advert topic manager + lookup
│   │       ├── agent/          # Agent registry topic manager + lookup
│   │       ├── dm_delegation/  # Delegation-revocation topic manager + lookup
│   │       ├── sonicstar/      # SonicStar Song Source Protocol (sssp) — the only
│   │       │                   #   bare-OP_RETURN plugin (every other uses PushDrop)
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
- `bsv-middleware-cloudflare` — BRC-31 authentication middleware for CF Workers (crates.io)
- `worker` — Cloudflare Workers Rust SDK

## HTTP route set

27 mainline-parity routes + 1 sonicstar-specific extension:

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

# Extension routes — outside the mainline-parity surface.
POST /sonicstar/records   # rich SonicStar record shape (records[] payload),
                          #   mirrors the records[] field returned by
                          #   sonicstar.net/api/overlay-parity/lookup
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

## Sonicstar plugin

`tm_sonicstar` / `ls_sonicstar` admit + index SonicStar Song Source
Protocol tracks (a single `OP_RETURN <utf-8 sssp JSON>` output per
track). This is the only plugin in `overlay-discovery` that uses bare
OP_RETURN; every other plugin (`ship` / `slap` / `uhrp` / `agent` /
`dm_delegation`) decodes `PushDrop` fields. See
`crates/overlay-discovery/docs/sonicstar_topic.md` for the on-wire
format, admission rules, and the permissive 3-path candidate decoder
that mirrors Ruth's TS reference at `sonicstarProtocol.ts`.

### Live parity (sonicstar.net)

The integration test at
`crates/overlay-cloudflare/tests/sonicstar_live_parity.rs` exercises
12 ignored tests against Ruth's published reference at
`https://sonicstar.net/api/overlay-parity/{admit,lookup,docs}`.

Run with the deploy URL set:

```bash
OVERLAY_URL=https://<your-overlay>.workers.dev \
  cargo test -p bsv-overlay-cloudflare \
    --test sonicstar_live_parity -- --ignored --nocapture
```

The full real-sat e2e test (`live_e2e_mint_and_admit_parity`) mints a
fresh sssp tx via the local MetaNet Client wallet (defaults to
`http://localhost:3321`) and round-trips it through both overlays.
Spends ~1 sat on each invocation; gated behind `SONICSTAR_E2E_MINT=yes`:

```bash
SONICSTAR_E2E_MINT=yes \
OVERLAY_URL=https://<your-overlay>.workers.dev \
  cargo test -p bsv-overlay-cloudflare \
    --test sonicstar_live_parity \
    live_e2e_mint_and_admit_parity -- --ignored --nocapture
```

### Empirical findings

* Ruth's `/api/overlay-parity/admit` returns the topic-manager
  admission decision but does **not** write through to her lookup-
  service Mongo. Her stored `records[]` come from her live production
  overlay's `outputAdmittedByTopic`, not from `/admit`.
* Her stored records carry DB drift on `pricePerPlay` / `royaltyRate`
  / `satoshis` relative to what her own decoder would produce on a
  fresh admission. The drift is surfaced (not asserted) by
  `live_record_field_diff_for_known_txids`.

## Deployment

The committed `crates/overlay-cloudflare/wrangler.toml.example`
carries a placeholder template; the real `wrangler.toml` is
gitignored. Copy and fill in your own values:

```bash
cp crates/overlay-cloudflare/wrangler.toml.example \
   crates/overlay-cloudflare/wrangler.toml
$EDITOR crates/overlay-cloudflare/wrangler.toml
```

Then provision + deploy:

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

- **Admin auth**: Bearer token on all `/admin/*` routes except `/admin/config`.
- **Cron**: `*/15 * * * *` for ad sync + GASP peer sync.
- **Extensions**: set `ENABLE_EXTENSIONS=true` to register UHRP / Agent /
  DmDelegation topic managers + lookup services beyond the mainline
  SHIP/SLAP baseline. Sonicstar opts in via the env CSVs:
  ```toml
  TOPIC_MANAGERS = "tm_ship,tm_slap,tm_uhrp,tm_agent,tm_dm_delegation,tm_sonicstar"
  LOOKUP_SERVICES = "ls_ship,ls_slap,ls_uhrp,ls_agent,ls_dm_delegation,ls_sonicstar"
  ```

## Repo security

A pre-commit hook at `.githooks/pre-commit` scans staged content for
known-private patterns (Cloudflare API tokens, account/D1 IDs, server
private keys, internal infra URLs, deployed worker URL) and refuses
commits that introduce them. Activate locally:

```bash
git config core.hooksPath .githooks
```

The hook also refuses any attempt to commit
`crates/overlay-cloudflare/wrangler.toml` directly (it's gitignored on
purpose; use the `.example` template).
