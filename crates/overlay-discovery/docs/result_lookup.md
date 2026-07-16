# ls_result — LOW Hand-Result Marker Lookup Service

Answers "which hands has this identity won?" and "what settled
recently?" over markers admitted to `tm_result` (bsv-low #38, the
on-chain leaderboard). Leaderboard clients count only the claims they
can VERIFY: both carried signatures are over the SAME canonical
challenge, verified client-side with the 'anyone' ProtoWallet
round-trip — the overlay never verifies a signature.

## Queries

```json
{"type": "resultsFor", "identity": "<66 hex chars>", "limit": 50}
{"type": "recentResults", "limit": 50}
```

`identity` is a compressed identity pubkey (33 bytes hex); the answer
lists hands that identity WON. `limit` is optional — default 100,
clamped to 1..=500.

## Answer

A freeform JSON array, newest first, one entry per stored marker:

```json
[{"gameId": "<hex>", "winner": "<hex>", "loser": "<hex>",
  "potTxid": "<hex>", "settleTxid": "<hex>",
  "winnerSigHex": "<hex>", "loserSigHex": "<hex|null>",
  "txid": "<hex|null>", "createdAt": 1234567890}]
```

The marker's bytes come back VERBATIM — there is **no derived
"confirmed" flag**. `loserSigHex` is `null` when the marker's loserSig
push was empty (an UNCONFIRMED claim — the winner's word alone); a hex
string is the loser's countersignature, and a client judges the claim
CONFIRMED only after verifying it. `potTxid` / `settleTxid` anchor the
claim to a real settled pot via `/pots-view`. The record surface must
never lie: bytes in, bytes out.

## Index semantics

One row per `(gameId, winner)`; **first marker wins** (`INSERT OR
IGNORE` — a later marker for the same pair never overwrites the first)
and rows are **never deleted**. A settled result is a permanent fact and
the admitted output is a provably-unspendable `OP_RETURN`;
`spend_notification_mode` is `none` and spend/eviction are deliberate
no-ops.
