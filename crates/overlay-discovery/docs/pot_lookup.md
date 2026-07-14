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
  {"txid": "<hex>", "vout": 0, "known": true,  "spent": true,  "spendingTxid": "<hex>"},
  {"txid": "<hex>", "vout": 1, "known": true,  "spent": false, "spendingTxid": null},
  {"txid": "<hex>", "vout": 2, "known": false, "spent": null,  "spendingTxid": null}
]
```

- `known` — a record exists (the outpoint was admitted to `tm_pot`).
- `spent` — whether a spender tx has been seen (`null` when `known:false`).
- `spendingTxid` — the settle / refund / sweep txid that spent the pot
  (`null` until the spend is recorded, or when `known:false`).

A missing record is **fail-safe**: `{"known": false, "spent": null,
"spendingTxid": null}` — the service never asserts "unspent" for an output
it never admitted.

## Index semantics

Rows are inserted on admission (`spent:false`) and UPDATED on spend
(`spent:true` + the `spendingTxid`). Rows are **never deleted** — a spent
pot is the permanent landing proof. `spend_notification_mode` is `txid`
(the service needs the spender), and `output_evicted` is a no-op. Contrast
`ls_reveal` (which never sees a spend at all) and `ls_low` (which deletes a
spent token).
