# parity-harness

Differential parity oracle. Replays a JSON corpus of HTTP requests against
both the mainline `@bsv/overlay-express@2.2.0` reference (running in the
`reference/` Docker stack on `:8090`) and the Rust implementation (`wrangler
dev` on `:8787`), canonicalises the responses, diffs them byte-for-byte,
and writes a markdown report.

## Quick run

```bash
# Assumes reference stack is up (`cd reference && docker compose up -d`)
# and `wrangler dev` is running for crates/overlay-cloudflare.
cargo run -p parity-harness -- \
    --ts http://localhost:8090 \
    --rust http://127.0.0.1:8787 \
    --corpus ./parity-harness/corpus \
    --report ./PARITY_REPORT.md
```

Exit code is non-zero if any corpus entry diverges.

## Corpus format

Each entry is a JSON file under `corpus/<category>/`:

```json
{
  "name": "health",
  "method": "GET",
  "path": "/health",
  "headers": {},
  "body": null
}
```

- `body` may be `null`, a JSON value (sent as `Content-Type: application/json`),
  or `{"base64": "..."}` for binary payloads.

## Canonicalisation

Responses are parsed as JSON (when the content-type is JSON), keys are sorted
recursively, and the following ephemeral fields are normalised to
`"<NORMALIZED>"` so only structural divergences surface:

- `startedAt`, `startTime`, `uptimeMs`, `uptime_secs`, `uptimeSecs`
- `scheduler_last_tick_secs_ago`
- `durationMs`
- `timestamp`, `time`, `date`

Non-JSON responses are compared verbatim as strings.
