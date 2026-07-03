# ls_reveal — LOW Break-Glass Reveal Lookup Service

Answers queries over reveal artifacts admitted to `tm_reveal`. Results are
the standard output list (UTXO references, hydrated to BEEF by the
`/lookup` route). The watchtower parses the returned BEEF back into the
raw reveal tx and feeds it to `break_glass::parse_reveal_artifact` /
`case::adjudicate_break_glass`.

## Queries

```json
{"type": "byGameSeat", "gameId": "<64 hex chars>", "seat": 0}
```

All reveal records for one game AND seat (`seat` is `0` for A, `1` for B)
— the tower's primary "did the accused seat reveal?" query. Returns every
matching reveal (a flood of decoys for the same key all come back), so the
tower can adjudicate each candidate.

```json
{"type": "byGameId", "gameId": "<64 hex chars>"}
```

All reveal records for one game, both seats.

## Index semantics

Rows are inserted on admission and **never deleted**. A reveal is a
permanent on-chain fact, and the admitted output is a provably-unspendable
`OP_RETURN`; `spend_notification_mode` is `none` and spend/eviction are
no-ops. (Contrast `ls_low`, where a spent token is removed from the
index.)
