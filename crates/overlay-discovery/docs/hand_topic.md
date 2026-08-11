# HAND Topic (`tm_hand`)

Indexes LOW per-seat showdown-hand markers (bsv-low #382): tiny owner-signed
`OP_RETURN` data carriers each seat publishes at settle observation, carrying
its OWN revealed five cards for a game.

## Wire format (`LOW/hand/v1`)

`OP_FALSE OP_RETURN` followed by six minimal data pushes:

| # | Push        | Encoding                                       |
|---|-------------|------------------------------------------------|
| 0 | tag         | UTF-8 `LOW/hand/v1` (11 bytes)                 |
| 1 | gameId      | 32 bytes                                       |
| 2 | identityKey | 33 bytes (compressed identity pubkey)          |
| 3 | potTxid     | 32 bytes                                       |
| 4 | cards       | 5 bytes — ordinals, each 0..=51, distinct      |
| 5 | sig         | DER ECDSA signature, 68..=74 bytes             |

Admission is BYTE FORMAT ONLY — no server-side signature verification. The
sig is verified client-side, publicly ('anyone' ProtoWallet round-trip) under
the marker's own named identity (`[1,'low hand']`, keyID = gameId,
counterparty 'anyone', challenge binding gameId + identity + potTxid + cards).
Display index only: no money path reads it.

Records are keyed by marker OUTPOINT — every admitted marker is kept, junk
and genuine rows coexist, and the reader's signature verify separates them.
