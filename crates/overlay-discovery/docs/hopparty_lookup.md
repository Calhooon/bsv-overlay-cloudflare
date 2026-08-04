# ls_hopparty — LOW Hop-In-Flight Marker Lookup Service

Answers "which funding hops did this identity mark?" (`hopsFor`) and
"which markers name this hop outpoint?" (`byHop`) over `LOW/hopparty/v1`
markers admitted by `tm_hopparty` (bsv-low #315).

## Queries

```json
{"type": "hopsFor", "identity": "<66 hex chars>", "limit": 50}
{"type": "byHop", "hopTxid": "<64 hex chars>", "hopVout": 0, "limit": 50}
```

`limit` is optional (default 100, clamped 1..=500). `hopsFor` counts HOP
OUTPOINTS (newest first) and returns a BOUNDED SUPERSET of up to 4 oldest
rows per outpoint — the overlay cannot verify signatures, so it never
chooses which row is real (the verifying reader — `/hops-view`, the
client — decides). `byHop` returns markers oldest first.

## Answer

A freeform JSON array, one entry per marker row, bytes carried back
verbatim (the overlay never verifies either signature):

```json
[{"identity": "<hex>", "opponentIdentity": "<hex>", "gameId": "<hex>",
  "hopTxid": "<hex>", "hopVout": 0, "hopSats": 1234567,
  "seatSettlePubkey": "<hex>", "seatSigHex": "<hex>",
  "identitySigHex": "<hex>", "txid": "<hex>", "outputIndex": 0,
  "createdAt": 1234567890}]
```

Records are permanent: spend/eviction notifications never remove a row.
