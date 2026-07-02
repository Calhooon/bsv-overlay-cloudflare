# tm_low — LOW Poker Lobby Topic Manager

Admits PushDrop tokens that announce LOW poker tables and point at a
table's live game-UTXO (pot escrow outpoint). Tracks bsv-low issues
#39/#40.

## Record types

### TABLE_OPEN (`LOW.table.v1`, 8 fields)

| # | Field               | Encoding                                  |
|---|---------------------|-------------------------------------------|
| 0 | protocol tag        | UTF-8 `LOW.table.v1`                      |
| 1 | host identity key   | 33-byte compressed secp256k1 pubkey       |
| 2 | gameId              | 32 bytes                                  |
| 3 | stake satoshis      | 8-byte little-endian u64                  |
| 4 | rules hash          | 32 bytes                                  |
| 5 | relay URL           | UTF-8, 1..=512 bytes, `https://`/`wss://` |
| 6 | expiry block height | 4-byte little-endian u32                  |
| 7 | signature           | ECDSA over concat(fields 0..7)            |

An unspent TABLE_OPEN token is an open table. Spending it closes the
table.

### GAME_UTXO (`LOW.gameutxo.v1`, 6 fields)

| # | Field             | Encoding                            |
|---|-------------------|-------------------------------------|
| 0 | protocol tag      | UTF-8 `LOW.gameutxo.v1`             |
| 1 | host identity key | 33-byte compressed secp256k1 pubkey |
| 2 | gameId            | 32 bytes                            |
| 3 | pot txid          | 32 bytes                            |
| 4 | pot vout          | 4-byte little-endian u32            |
| 5 | signature         | ECDSA over concat(fields 0..5)      |

Lets the host announce the live escrow outpoint for a table without
modifying the funding transaction. A spent pointer is superseded.

## Signature linkage

Same scheme as SHIP advertisements: the host signs the concatenated
data fields with a BRC-42 derived key — protocol `[2, "low poker
lobby"]`, key ID `"1"`, counterparty `anyone` — and the PushDrop
locking key must be the host's own derived child for that protocol.
Tokens with a bad signature, a mismatched identity key, or the wrong
locking key are not admitted.
