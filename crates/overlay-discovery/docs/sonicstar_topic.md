# tm_sonicstar — SonicStar Song Source Protocol Topic

## Purpose

Indexes on-chain track listings published using the **SonicStar Song Source
Protocol** (`sssp`). SonicStar is a music distribution platform on BSV; each
song is anchored as a single OP_RETURN output that carries a JSON envelope
describing the track (title, artist, file URLs, royalty rate, etc.).
Clients query the paired `ls_sonicstar` lookup service to discover tracks
by artist, genre, or free-text search.

## On-wire format

Unlike every other plugin in the `overlay-discovery` crate, sonicstar does
**not** use `PushDrop`. Each admissible output is a bare:

```text
OP_RETURN <push: utf-8 JSON>
```

The single push payload is a UTF-8 encoded JSON object with `protocol:
"sssp"`. The locking script has no spend condition — sonicstar tracks are
unspendable data carriers; `ls_sonicstar` removes records via the engine's
eviction path, not via spend notifications.

### JSON envelope schema

```json
{
  "protocol": "sssp",
  "securityLevel": 2,
  "songTitle": "Hello",
  "artistName": "Adele",
  "duration": 295,
  "songFileURL": "uhrp://<sha256>",
  "artFileURL": "uhrp://<sha256>",
  "previewURL": "https://...",
  "genre": "Pop",
  "album": "25",
  "releaseDate": "2015-10-23",
  "pricePerPlay": 1000,
  "royaltyRate": 75,
  "description": "..."
}
```

`songTitle`, `artistName`, and `songFileURL` are required and must be
non-empty strings. Every other field is optional. `securityLevel` is
informational and not enforced today (matches Ruth's reference at
`sonicstarTopic.ts:34-37`).

## Admission rules

1. The first script chunk must be `OP_RETURN`. The "safe data carrier"
   pattern (`OP_FALSE OP_RETURN`) is **rejected**, matching the literal
   `chunks[0].op !== OP.OP_RETURN` check in the TS reference.
2. The push payload must contain a JSON object whose `protocol` field is
   `"sssp"` (after stripping any leading/trailing junk via
   `find('{')..rfind('}')`).
3. After applying the falsy-`||` defaulting rules, `songTitle`,
   `artistName`, and `songFileURL` must be non-empty strings.

## Decoder permissiveness

The `bsv-rs` script parser collapses everything after `OP_RETURN` into
`chunks[0].data` as a single buffer (push prefix + payload). To match
Ruth's TS decoder byte-for-byte, three candidate buffers are tried in
order; the first that round-trips as an `sssp` JSON object wins:

1. Each non-empty `chunks[i].data` for `i >= 1` (separate-push form;
   defensive — almost always empty under the bsv-rs parser).
2. The raw `chunks[0].data` itself; the `find('{')..rfind('}')` slice
   tolerates push-prefix bytes (`0x4c 0x9e` etc.) prefixed to the JSON.
3. `chunks[0].data` re-parsed as an inner script, each non-empty push
   payload.

## Defaulting (TS falsy-`||` parity)

| Field | Missing/null/non-string | Explicit `0` |
|-------|-------------------------|--------------|
| `songTitle` / `artistName` / `songFileURL` | empty string → admission rejected | not applicable |
| `description` / `artFileURL` / `previewURL` / `genre` / `album` / `releaseDate` | dropped from persisted record | not applicable |
| `duration` | `0` | `0` |
| `pricePerPlay` | `1000` | `1000` |
| `royaltyRate` | `75` | `75` |
| `artistIdentityKey` | always `""` (TS hard codes this with a `TODO: extract from transaction context`) | n/a |

## Lookup queries (`ls_sonicstar`)

| Query | Description |
|-------|-------------|
| `"findAll"` (string), `{}`, or `{"findAll": true}` | Enumerate all admitted tracks. |
| `{"txid": "..."}` | Exact-match `txid`. |
| `{"artistName": "..."}` | Case-insensitive substring against `artist_name`. |
| `{"genre": "..."}` | Exact match (case sensitive, matches Mongo equality). |
| `{"searchText": "..."}` | Case-insensitive substring across `songTitle`, `artistName`, `album` (three fields only). |
| Any combination above | Combined via AND, matching Mongo filter object semantics. |
| `{"limit": n, "skip": m}` | `limit` clamped to `[1, 200]`, default `50`. `skip` defaults to `0`. |

Results are sorted by `admittedAt` descending and returned as
`UTXOReference` outpoints.

## Reference implementation

- TS topic manager: `sonicstar.net` repo `server/overlay/sonicstarTopic.ts`.
- TS lookup service: `server/overlay/sonicstarLookup.ts`.
- TS protocol + decoder: `server/sonicstarProtocol.ts`.
- Live parity endpoints: `https://sonicstar.net/api/overlay-parity/{admit,lookup,docs}`.
