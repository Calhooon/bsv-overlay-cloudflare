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

A freeform JSON array. Ordering + windowing (bsv-low #281):

**PRINCIPLE — verification before collapse.** Marker admission is
byte-format-only: anyone can file a marker naming any identity for a dust
`OP_RETURN`, and this layer never verifies signatures. A layer that cannot
verify must not choose which row is real, so this query does NOT pick a
"representative" marker per pot. It returns a BOUNDED SUPERSET and the CLIENT
— which does verify, and drops a row whose signatures fail — decides. The
contract is: **the answer CONTAINS the honest row; the caller picks it.**

- `partyFor` counts POTS (newest pot first) and returns up to 4 rows in EACH
  of two groups per pot outpoint — the v1 markers and the v2 (seat-binding)
  markers. Both groups are needed: without a v2 row a client can never latch
  `v2Indexed` and republishes a paid marker forever; without a v1 row a pot
  whose only v2 row fails the client's signature check disappears from
  recovery entirely.
- MITIGATION, NOT CLOSURE: an attacker who files more than 4 markers stamped
  earlier than yours in a group still evicts it. This raises erasure from ONE
  dust marker to four; it does not eliminate it. Do not read the window as a
  proof of anything.
- Rows naming a pot the overlay has never indexed sort behind the rest (so
  they cannot displace a real pot) but are still served, with a small reserved
  quota promoted so a pot whose admission is merely in flight stays visible.
- `byPot` returns markers oldest first.

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
