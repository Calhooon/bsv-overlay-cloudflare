# Parity reference stack

Local instance of mainline **`@bsv/overlay-express@2.2.0`** (the TS reference
the differential parity harness diffs rust-overlay against).

## Quickstart

```bash
docker compose up -d --build
curl localhost:8090/health    # expect: {"status":"ok"}
docker compose logs -f overlay-express
```

Down:
```bash
docker compose down -v        # -v wipes Mongo/MySQL volumes (start fresh)
```

Port `:8090` is used because `:8080` is taken by `dolphinmilk` on this host.

## What's running

| Service | Image | Host port | Role |
|---|---|---|---|
| overlay-express | built from `./Dockerfile` | 8090 → 8080 | mainline TS reference — the parity target |
| mongo | `mongo:7` | internal | lookup-service storage |
| mysql | `mysql:8` | internal | knex storage (outputs, applied_transactions) — overlay-express 2.2.0 hardcodes `client: mysql2` in `configureKnex(string)` |

## Env vars

All set in `docker-compose.yml` — safe dev defaults. Override per-service via
`environment:` block. Notable:

- `SERVER_PRIVATE_KEY` — dev key. Any hex32 works; Rust side must seed the
  same key for identity-sensitive comparisons.
- `HOSTING_URL` — `http://localhost:8080`. Advertisements will encode this URL.
- `KNEX_URL` / `MONGO_URL` — wired to the sibling containers.

## Version pin

`Dockerfile` installs `@bsv/overlay-express@2.2.0` exactly. Bump intentionally
when mainline releases — any version drift invalidates the committed
`PARITY_REPORT.md`.
