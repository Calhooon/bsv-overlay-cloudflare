# tm_proof — LOW Transcript-Proof Bundle Topic Manager

Indexes LOW rung-3 transcript-proof bundle markers (the leaderboard
verification ladder: countersigned → covenant-anchored →
**transcript-proven**; `bsv-low docs/DESIGN-rung3-transcript-proven-hands.md`).
After a winner's `LOW/result/v2` claim, it publishes a `LOW/proof/v1`
marker carrying the canonical JSON proof bundle — the signed envelopes
(blind scalar commitments, final masked deck d4, mask commits/reveals,
the winner's reveal, the loser's released keys) from which the claim's
five cards are provable from the game's cryptographic transcript itself.

## What it admits

An output is admitted IFF its locking script is a well-formed
`LOW/proof/v1` marker — BYTE FORMAT ONLY, like `tm_result`. There is
**no server-side verification of anything**: the bundle JSON is not
parsed, the identity signature is not checked. The CLIENT verifies the
winner's identity signature ('anyone' ProtoWallet round-trip) and the
bundle's transcript cryptography (envelope signatures,
scalar-commitment openings, unmasking — all wasm-exported); a bundle
that fails any check simply earns no badge (the claim stays merely
countersigned — never hidden, never upgraded). The overlay is an INDEX,
not an authority.

What rung 3 can NEVER prove (locked no-VDF decision, accepted residual):
a single person holding BOTH seats controls the joint shuffle end-to-end
— no transcript check defeats self-play.

## Marker wire format (`LOW/proof/v1`)

`OP_FALSE OP_RETURN` (0x00 0x6a) followed by exactly five minimal data
pushes — byte-identical to the app's builder:

| # | Push           | Encoding                                         |
|---|----------------|--------------------------------------------------|
| 0 | tag            | UTF-8 `LOW/proof/v1` (12 bytes)                  |
| 1 | gameId         | 32 bytes                                         |
| 2 | winnerIdentity | 33 bytes (compressed pubkey)                     |
| 3 | sig            | DER ECDSA, 68..=74 bytes — the winner's identity |
|   |                | signature over the canonical challenge           |
| 4 | bundle         | the canonical JSON proof bundle bytes,           |
|   |                | 1..=65536 (big pushes use OP_PUSHDATA2)          |

Wrong tag / wrong lengths / extra or missing pushes / an empty or
oversized bundle → not admitted. Real bundles run ~10–15 KB. The strict
format check keeps junk out of the index; genuineness is the client's
job.
