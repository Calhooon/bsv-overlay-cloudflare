# ls_potparty — LOW Pot-Participation Marker Lookup Service

Answers "which pots is this identity a party to?" (`partyFor`) and "who
are the two parties to this pot outpoint?" (`byPot`) over markers admitted
to `tm_potparty` (bsv-low #188, the seed-only recovery index). A fresh
client with nothing but its identity key uses `partyFor` to enumerate its
pots, then re-derives keys and drives the refund/settle exits.

## Queries

```json
{"type": "partyFor", "identity": "<66 hex chars>", "limit": 50}
{"type": "byPot", "potTxid": "<64 hex chars>", "potVout": 0, "limit": 50}
```

`identity` is a compressed identity pubkey (33 bytes hex). `limit` is
optional — default 100, clamped to 1..=500.

## Answer

A freeform JSON array, newest first, one entry per stored marker:

```json
[{"identity": "<hex>", "opponentIdentity": "<hex>", "gameId": "<hex>",
  "potTxid": "<hex>", "potVout": 0, "recoveryHeight": 800000,
  "sigHex": "<hex>", "txid": "<hex>", "outputIndex": 0,
  "createdAt": 1234567890}]
```

The marker's bytes come back VERBATIM. The overlay never verifies the
`sig` — a client verifies it itself if it cares. `potTxid` / `potVout`
locate the pot; `recoveryHeight` is the pre-signed refund's height gate.

## Index semantics

One row per marker **outpoint** `(txid, outputIndex)`; a replayed /
duplicate submit of the same output is a no-op (`INSERT OR IGNORE`) and
rows are **never deleted** — a pot-participation fact is permanent
recovery history and the admitted output is a provably-unspendable
`OP_RETURN`. `spend_notification_mode` is `none` and spend/eviction are
deliberate no-ops (mirrors `ls_pot`'s permanence). Markers for the same
identity from DIFFERENT txs are all kept — keying on the outpoint keeps a
garbage front-run from censoring a genuine marker (the `tm_result`
lesson); each seat publishes its own marker, so `byPot` returns both.
