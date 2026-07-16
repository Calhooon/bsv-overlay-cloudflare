# ls_collected — LOW "Already Collected" Marker Lookup Service

Answers "which of these games has this identity already collected?" over
markers admitted to `tm_collected` (bsv-low #161). LOW clients ask this
during the home/History card gather; a returned marker whose signature
the client's own wallet verifies flips the card to "collected on another
device".

## Query

```json
{"type": "collectedFor", "identity": "<66 hex chars>",
 "gameIds": ["<64 hex chars>", "..."]}
```

`identity` is the querying device's compressed identity pubkey (33 bytes
hex); `gameIds` the games being gathered.

## Answer

A freeform, input-ordered JSON array — one entry per requested gameId:

```json
[{"gameId": "<hex>", "identity": "<hex>", "txid": "<hex|null>",
  "sigHex": "<hex|null>", "present": true}]
```

A `(identity, gameId)` with no stored marker answers
`{"present": false, "txid": null, "sigHex": null}` — fail-safe: an absent
marker means "still offer Collect", never a hidden card. `sigHex` is the
marker's raw DER signature push; the CLIENT verifies it under its own
wallet (`verifySignature`, `[1,'low collected']` / keyID = gameId / self)
— the overlay never does.

## Index semantics

One row per `(identity, gameId)`; **first marker wins** (`INSERT OR
IGNORE` — a later marker for the same pair never overwrites the first)
and rows are **never deleted**. A collected marker is a permanent fact
and the admitted output is a provably-unspendable `OP_RETURN`;
`spend_notification_mode` is `none` and spend/eviction are deliberate
no-ops.
