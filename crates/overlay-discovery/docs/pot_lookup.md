# ls_pot — LOW Pot-Spend Landing-Proof Lookup Service

Answers "is this pot outpoint spent, and by which txid?" over outputs
admitted to `tm_pot`. This is the on-chain landing proof a LOW client
requires before crediting a settle / refund / sweep payout, served from the
overlay's own spend bookkeeping instead of a WhatsOnChain `/spent` read.

## Query

```json
{"type": "spentStatus", "outpoints": [{"txid": "<hex>", "vout": 0}, ...]}
```

Ask the spent status of a batch of pot outpoints.

## Answer

A freeform, **input-ordered** JSON array — one entry per requested outpoint:

```json
[
  {"txid": "<hex>", "vout": 0, "known": true,  "spent": true,  "spendingTxid": "<hex>", "spentConfirmed": true},
  {"txid": "<hex>", "vout": 1, "known": true,  "spent": false, "spendingTxid": null,    "spentConfirmed": false},
  {"txid": "<hex>", "vout": 2, "known": false, "spent": null,  "spendingTxid": null,    "spentConfirmed": null}
]
```

- `known` — a record exists (the outpoint was admitted to `tm_pot`).
- `spent` — whether a spender tx has been seen (`null` when `known:false`).
- `spendingTxid` — the settle / refund / sweep txid that spent the pot
  (`null` until the spend is recorded, or when `known:false`).
- `spentConfirmed` — whether the recorded spend was SPV-confirmed (the
  spending tx's merkle path validated against the chain tracker) when it was
  recorded (`null` when `known:false`).

A missing record is **fail-safe**: `{"known": false, "spent": null,
"spendingTxid": null, "spentConfirmed": null}` — the service never asserts
"unspent" for an output it never admitted.

## Index semantics

Rows are inserted on admission (`spent:false`) and UPDATED on spend
(`spent:true` + the `spendingTxid`). Rows are **never deleted** — a spent
pot is the permanent landing proof.

Spend pointers are **prefer-confirmed / never-clobber-with-unconfirmed**
(the public `/submit` surface means anyone can submit a tx claiming to spend
a pot): a spend whose merkle path the chain tracker validates writes
unconditionally and latches `spentConfirmed` (last-confirmed-wins); an
UNCONFIRMED spend claim writes only while no confirmed pointer exists —
last-writer-wins among unconfirmed claims is deliberately preserved so an
honest later submit can still set the pointer. `spend_notification_mode` is `txid`
(the service needs the spender), and `output_evicted` is a no-op. Contrast
`ls_reveal` (which never sees a spend at all) and `ls_low` (which deletes a
spent token).
