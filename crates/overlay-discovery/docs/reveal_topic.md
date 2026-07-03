# tm_reveal — LOW Break-Glass Reveal Topic Manager

Indexes LOW break-glass REVEAL artifacts so the watchtower can look up
"did `(gameId, seat)` publish a reveal?" by key, instead of scanning a
payout address's WhatsOnChain history. Phase 1a of moving the tower's
reveal lookup onto our own overlay infra.

## What it admits

A LOW reveal transaction carries two outputs: a `LOW/reveal/v2`
`OP_RETURN` artifact and a P2PKH "beacon". This topic admits the
**OP_RETURN artifact ONLY** — it is provably unspendable, so the indexed
record is permanent (a reveal is a permanent fact). The beacon P2PKH is
reclaimed/spent later, so indexing it would let its spend evict the
record.

There is **no signature**: a reveal is an unsigned public fact anyone may
publish. The topic manager validates the byte format only and extracts
`(gameId, seat)`; the tower adjudicates genuineness downstream
(`case::adjudicate_break_glass`). A well-formed-but-"cooked" artifact IS
admitted — the tower sorts genuine from cooked among all indexed matches.

## Artifact wire format (`LOW/reveal/v2`)

`OP_FALSE OP_RETURN` (or a bare `OP_RETURN`) followed by six minimal data
pushes — byte-identical to the app's `revealArtifactScriptHex` and the
tower's `break_glass::parse_reveal_artifact`:

| # | Push         | Encoding                                   |
|---|--------------|--------------------------------------------|
| 0 | tag          | UTF-8 `LOW/reveal/v2` (13 bytes)          |
| 1 | gameId       | 32 bytes                                   |
| 2 | seat         | 1 byte, `0x00` (A) or `0x01` (B)          |
| 3 | positions    | 5 bytes (the seat's final deck positions)  |
| 4 | own scalars  | 160 bytes (5 × 32-byte remask scalars)     |
| 5 | peer scalars | 160 bytes (5 × 32-byte claimant scalars)   |

Any reveal-TAGGED output that violates these lengths is rejected (not
admitted) so the index can't be spammed with junk. Non-reveal outputs
(the beacon, change, foreign tokens) are skipped silently.
