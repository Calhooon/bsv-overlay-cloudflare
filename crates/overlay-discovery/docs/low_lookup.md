# ls_low — LOW Poker Lobby Lookup Service

Answers queries over tokens admitted to `tm_low`. Results are the
standard output list (UTXO references, hydrated with BEEF by the
`/lookup` route) — decode the returned PushDrop fields to read stake,
relay URL, or the pot outpoint.

## Queries

```json
{"type": "findOpenTables", "stakeMin": 100, "stakeMax": 5000}
```

All unspent TABLE_OPEN records. `stakeMin` / `stakeMax` (satoshis,
inclusive) are optional.

```json
{"type": "byGameId", "gameId": "<64 hex chars>"}
```

All records (TABLE_OPEN and GAME_UTXO pointers) for one game — use
this to resolve a table's live game-UTXO.

```json
{"type": "byHost", "identityKey": "<66 hex chars>"}
```

All records published by one host identity key.

## Index semantics

Rows are inserted on admission and deleted on spend or eviction:
a spent TABLE_OPEN means the table closed; a spent GAME_UTXO pointer
means it was superseded by a newer pointer. The index only ever holds
live records.
