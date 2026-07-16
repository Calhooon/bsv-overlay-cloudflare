# ls_proof — LOW Transcript-Proof Bundle Lookup Service

Answers "which transcript-proof bundles exist for this settled hand's
winner?" over markers admitted to `tm_proof` (leaderboard rung 3 —
transcript-proven hands). Clients fetch bundles during the badge gather
and verify the transcript cryptography themselves; a bundle that
verifies earns the "transcript-proven" badge on that claim's row.

## Query

```json
{"type": "proofsFor", "gameId": "<64 hex chars>",
 "winner": "<66 hex chars>", "limit": 3}
```

`limit` is optional — default 3, clamped to 1..=10 (bundles run
~10–15 KB each; keep pages small).

## Answer

A freeform JSON array, newest first, one entry per stored marker:

```json
[{"gameId": "<hex>", "winner": "<hex>", "sigHex": "<hex>",
  "bundleBase64": "<base64>", "txid": "<hex>", "outputIndex": 0,
  "createdAt": 1234567890}]
```

`bundleBase64` is the marker's bundle push verbatim, base64-encoded at
this edge only (raw canonical-JSON bytes on-chain and in storage).
`sigHex` is the winner's DER identity signature over the canonical
challenge. The overlay verifies NEITHER — the CLIENT checks the identity
sig ('anyone' round-trip) and the bundle's transcript cryptography, and
a failing bundle simply earns no badge (never hidden, never upgraded).

## Index semantics

One row per marker **outpoint** `(txid, outputIndex)`; a replayed /
duplicate submit of the same output is a no-op (`INSERT OR IGNORE`) and
rows are **never deleted**. Bundles for the same `(gameId, winner)` from
DIFFERENT txs are ALL kept — the `tm_result` censorship lesson applies
identically: admission is byte-format-only, so a pair-keyed
first-marker-wins index would let a garbage bundle front-run the real
proof for one OP_RETURN fee. Instead, garbage and genuine bundles
coexist and the CLIENT verifies each, using the one that proves. A
published proof is a permanent fact and the admitted output is a
provably-unspendable `OP_RETURN`; `spend_notification_mode` is `none`
and spend/eviction are deliberate no-ops.
