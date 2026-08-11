# HAND Lookup (`ls_hand`)

Queries LOW per-seat showdown-hand markers (bsv-low #382).

## Query

```json
{"type": "handsForGames", "gameIds": ["<64 hex>", "..."]}
```

At most 100 gameIds per query; over-cap requests are refused explicitly
(never silently truncated).

## Answer

A freeform JSON array — ONE ENTRY PER STORED ROW (both seats publish for a
game, and junk rows coexist with genuine ones by design):

```json
[{"gameId": "<hex>", "identity": "<hex>", "potTxid": "<hex>",
  "cardsHex": "<10 hex>", "txid": "<hex>", "outputIndex": 0,
  "sigHex": "<hex|null>"}]
```

A gameId with no markers contributes no rows. Consumers MUST verify each
row's `sigHex` publicly under the row's own `identity` before rendering —
the overlay never verifies and a row's presence proves nothing.
