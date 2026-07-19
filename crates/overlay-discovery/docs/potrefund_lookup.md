# ls_potrefund — LOW Pre-Signed Refund-Backup Marker Lookup Service

Answers "give me the pre-signed refund backup(s) for this pot outpoint?"
(`byPot`) and "which pots have I published a refund backup for?"
(`partyFor`) over markers admitted to `tm_potrefund` (bsv-low #191, the
keyless recovery index). A recovering client asks `byPot` for a pot it
learned from the `potparty` index (#188), pulls the pre-signed refund
bytes, and re-broadcasts them after the refund's recovery height even if
the tower failed.

## Queries

```json
{"type": "byPot", "potTxid": "<64 hex chars>", "potVout": 0, "limit": 50}
{"type": "partyFor", "identity": "<66 hex chars>", "limit": 50}
```

`identity` is a compressed identity pubkey (33 bytes hex). `limit` is
optional — default 100, clamped to 1..=500.

## Answer

A freeform JSON array, newest first, one entry per stored marker:

```json
[{"identity": "<hex>", "gameId": "<hex>", "potTxid": "<hex>",
  "potVout": 0, "refundRawHex": "<hex>", "sigHex": "<hex>",
  "txid": "<hex>", "outputIndex": 0, "createdAt": 1234567890}]
```

The marker's bytes come back VERBATIM. The overlay never parses or verifies
the refund tx / `sig` — a client does that itself. `potTxid` / `potVout`
locate the pot; `refundRawHex` is the pre-signed refund ready to
re-broadcast (non-final until its own recovery height).

## Index semantics

One row per marker **outpoint** `(txid, outputIndex)`; a replayed /
duplicate submit of the same output is a no-op (`INSERT OR IGNORE`) and
rows are **never deleted** — a pre-signed refund backup is permanent
recovery history and the admitted output is a provably-unspendable
`OP_RETURN`. `spend_notification_mode` is `none` and spend/eviction are
deliberate no-ops (mirrors `ls_pot`'s permanence). BOTH seats may publish a
backup for the same pot, and keying on the outpoint keeps a garbage
front-run from censoring a genuine refund (the `tm_result` lesson) — so
`byPot` returns every backup.
