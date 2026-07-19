# tm_potparty — LOW Pot-Participation Marker Topic Manager

Indexes LOW pot-participation markers for seed-only recovery
(bsv-low #188, recovery architecture P1). When a LOW seat funds (or is
funding) a pot, it publishes a tiny `OP_RETURN` marker under this topic
naming its identity, the opponent, the game, the pot outpoint, and the
pre-signed refund's `recoveryHeight`. A fresh, seed-only client queries
`ls_potparty` (`partyFor`) for its identity and learns every pot it is a
party to — enough to re-derive keys, pull the funding BEEF, and drive the
refund/settle exits.

## What it admits

An output is admitted IFF its locking script is a well-formed
`LOW/potparty/v1` marker — BYTE FORMAT ONLY, like `tm_result` /
`tm_collected`. There is **no server-side signature verification**: the
overlay is an INDEX, not an authority, and it carries the marker's bytes
(including the `sig` push) back verbatim. A client that cares about
authenticity verifies the signature itself. The index keeps EVERY
admitted marker (keyed by outpoint) — a garbage marker can never occupy a
slot and censor a later genuine one.

One structural rule beyond lengths: `identity != opponentIdentity` (byte
compare). A self-paired marker is rejected — a pot is between two DISTINCT
seats.

## Marker wire format (`LOW/potparty/v1`)

`OP_FALSE OP_RETURN` (0x00 0x6a) followed by exactly eight minimal data
pushes — byte-identical to the app's builder:

| # | Push             | Encoding                                       |
|---|------------------|------------------------------------------------|
| 0 | tag              | UTF-8 `LOW/potparty/v1` (15 bytes)             |
| 1 | identity         | 33 bytes (publishing seat's compressed pubkey) |
| 2 | opponentIdentity | 33 bytes (the other seat's compressed pubkey)  |
| 3 | gameId           | 32 bytes                                       |
| 4 | potTxid          | 32 bytes                                       |
| 5 | potVout          | 4 bytes little-endian (u32)                    |
| 6 | recoveryHeight   | 4 bytes little-endian (u32)                    |
| 7 | sig              | DER ECDSA, 68..=74 bytes (preserved, never     |
|   |                  | verified by the overlay)                       |

Wrong tag / wrong lengths / extra or missing pushes / a truncated push /
`identity == opponentIdentity` → not admitted. The strict format check
keeps junk out of the index; genuineness is the client's job.
