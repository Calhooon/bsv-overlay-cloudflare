# tm_result — LOW Hand-Result Marker Topic Manager

Indexes LOW hand-result markers for the on-chain leaderboard
(bsv-low #38). When a LOW hand settles, the WINNER publishes a tiny
`OP_RETURN` marker under this topic after its landing-proof-gated payout
credit. Leaderboard clients query `ls_result` and count only the claims
they can VERIFY — both carried signatures are over the SAME canonical
challenge, built client-side and verified client-side with the 'anyone'
ProtoWallet round-trip.

## What it admits

An output is admitted IFF its locking script is a well-formed
`LOW/result/v1` marker — BYTE FORMAT ONLY, like `tm_collected`. There is
**no server-side signature verification**: the overlay is an INDEX, not
an authority, and it carries the marker's bytes back verbatim. A forged
marker is worthless client-side (its signatures fail the verify), and
the carried `potTxid` / `settleTxid` let a client anchor the claim to a
REAL settled pot via `/pots-view`. The index keeps EVERY admitted marker
(keyed by outpoint) — a garbage-sig marker naming the real winner cannot
occupy a slot and censor the later genuine one.

One structural rule beyond lengths: `winnerIdentity != loserIdentity`
(byte compare). A self-paired marker is rejected — it would let one key
sign both slots and fake a "confirmed" win against itself.

## Marker wire format (`LOW/result/v1`)

`OP_FALSE OP_RETURN` (0x00 0x6a) followed by exactly eight minimal data
pushes — byte-identical to the app's builder:

| # | Push           | Encoding                                        |
|---|----------------|-------------------------------------------------|
| 0 | tag            | UTF-8 `LOW/result/v1` (13 bytes)                |
| 1 | gameId         | 32 bytes                                        |
| 2 | winnerIdentity | 33 bytes (compressed pubkey)                    |
| 3 | loserIdentity  | 33 bytes (compressed pubkey)                    |
| 4 | potTxid        | 32 bytes                                        |
| 5 | settleTxid     | 32 bytes                                        |
| 6 | winnerSig      | DER ECDSA, 68..=74 bytes                        |
| 7 | loserSig       | EMPTY push (0 bytes — an UNCONFIRMED claim) OR  |
|   |                | DER ECDSA 68..=74 bytes (a CONFIRMED claim)     |

`loserSig`, when present, is the counterparty's countersignature making
the claim "confirmed" — confirmed as judged by the verifying CLIENT, not
by the overlay. Wrong tag / wrong lengths / extra or missing pushes /
winner == loser → not admitted. The strict format check keeps junk out
of the index; genuineness is the client's job.
