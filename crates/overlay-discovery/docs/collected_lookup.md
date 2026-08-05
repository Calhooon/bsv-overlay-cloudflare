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

A freeform, input-ordered JSON array — **one entry per stored MARKER ROW**,
grouped in request order:

```json
[{"gameId": "<hex>", "identity": "<hex>", "txid": "<hex>",
  "outputIndex": 0, "sigHex": "<hex|null>", "present": true}]
```

**A gameId may appear MORE THAN ONCE.** Markers for one `(identity, gameId)`
published by different transactions all coexist in the index, so the array can
carry several entries for the same gameId. **Consumers MUST NOT assume one entry
per requested gameId, and MUST NOT treat the first entry for a gameId as
authoritative** — admission is byte-format-only and the overlay never verifies
the signature, so any single row may be a stranger's. Verify `sigHex` under your
own wallet (`verifySignature`, `[1,'low collected']` / keyID = gameId / self) and
select the row that checks out; a row's PRESENCE proves nothing.

A `(identity, gameId)` with no stored marker answers exactly one entry,
`{"present": false, "txid": null, "outputIndex": null, "sigHex": null}` —
fail-safe: an absent marker means "still offer Collect", never a hidden card.

## Index semantics

One row per marker **OUTPOINT** `(txid, outputIndex)`; `INSERT OR IGNORE` on
that key, so a replayed submit of the same output is a no-op while markers for
one `(identity, gameId)` from different txs are ALL kept. Rows are **never
deleted**. A collected marker is a permanent fact and the admitted output is a
provably-unspendable `OP_RETURN`; `spend_notification_mode` is `none` and
spend/eviction are deliberate no-ops.

The superseded `(identity, gameId)` first-marker-wins key (bsv-low #327 S8) was
a squattable namespace: both halves are public, so one free submit naming a
victim could occupy that victim's slot at deal time and censor their genuine
marker permanently. Keying on the outpoint removes the collision entirely — a
squatter can only occupy the outpoint it actually fabricated.
