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

The marker rides the hop transaction, so `byHop`'s `hopTxid` is the
marker's **containing** txid and the hop outpoint is `(txid, hopVout)`.

## Answer

A freeform JSON array, one entry per marker row. The marker's own bytes
are carried back verbatim (the overlay never verifies either signature);
the `hopLockHex` / `hopSatsOnChain` / `containerOutputs` fields are the
CONTAINER's facts, decoded once at admission from the very transaction
being admitted — what a reader compares the marker's claims against:

```json
[{"identity": "<hex>", "opponentIdentity": "<hex>", "gameId": "<hex>",
  "hopTxid": "<hex>", "hopVout": 0, "hopSats": 1234567,
  "seatSettlePubkey": "<hex>", "seatSigHex": "<hex>",
  "identitySigHex": "<hex>", "hopLockHex": "<hex|null>",
  "hopSatsOnChain": 1234567, "containerOutputs": 2,
  "txid": "<hex>", "outputIndex": 1, "createdAt": 1234567890}]
```

`hopTxid` is a re-presentation of `txid` (one transaction).
`hopLockHex` / `hopSatsOnChain` are `null` exactly when the container has
no output at `hopVout` — an absence made provable by `containerOutputs`,
which REFUTES the marker rather than leaving it open.

Records are permanent: spend/eviction notifications never remove a row.
