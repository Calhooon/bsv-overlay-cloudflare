# tm_hopparty — LOW Hop-In-Flight Marker Topic Manager

Indexes LOW hop-in-flight markers (bsv-low #315, #252 stage 2b). When a
LOW seat funds its HOP (the staging P2PKH coin paid to its own
`[2,'low settle']` key before the JOIN assembles the pot), it publishes a
tiny `OP_RETURN` marker **as a second output on the hop transaction
itself**, naming its identity, the opponent, the game, the hop VOUT, the
hop value, and its settle pubkey. The app-layer `/hops-view` joins these
rows to the `tm_lowfund`-indexed hop outpoint so a seat that died before
the JOIN can still enumerate its hops in flight.

## The container supplies the txid

The marker carries **no `hopTxid` push**. A transaction cannot embed its
own txid (a hash fixed point), so a marker riding the hop tx could never
name it — and `hopTxid` was never information the marker needed to carry:
it is information the container already has. **The hop outpoint is
`(the marker's own containing txid, hopVout)`**, and the overlay knows the
containing txid at admission.

## What it admits

An output is admitted IFF its locking script is a well-formed
`LOW/hopparty/v1` marker — BYTE FORMAT ONLY, like `tm_potparty`. There is
**no server-side signature verification at admission**: the overlay is an
INDEX, not an authority, and it carries the marker's bytes (both
signature pushes) back verbatim. Verification is a READ-time display
filter (`/hops-view`) and a client-side check. The index keeps EVERY
admitted marker (keyed by the marker outpoint) — a garbage marker can
never occupy a genuine marker's slot.

## Wire format (`LOW/hopparty/v1`)

`OP_FALSE OP_RETURN` (0x00 0x6a) + EXACTLY 9 minimal data pushes:

| # | push             | encoding                                    |
|---|------------------|---------------------------------------------|
| 0 | tag              | UTF-8 `LOW/hopparty/v1` (15 bytes)          |
| 1 | identity         | 33 bytes (compressed pubkey)                |
| 2 | opponentIdentity | 33 bytes                                    |
| 3 | gameId           | 32 bytes                                    |
| 4 | hopVout          | 4 bytes little-endian (u32)                 |
| 5 | hopSats          | 8 bytes little-endian (u64)                 |
| 6 | seatSettlePubkey | 33 bytes (compressed settle pubkey)         |
| 7 | seatSig          | DER ECDSA, 67..=74 bytes (by the settle key)|
| 8 | identitySig      | DER ECDSA, 67..=74 bytes (by the identity)  |

Structural rule: `identity != opponentIdentity`. Any other tag (both
potparty tags included), any other push count, or any wrong length is
simply not admitted — the tag/topic/table separation is server-enforced
by the strict parse.

## Digests (domain-separated by the version tag)

- **seatSig** — plain ECDSA by the settle key over `sha256` of
  `"LOW/hopparty/v1/seatsig|" ‖ gameId(32) ‖ hopVout(4 LE) ‖
  identity(33)` (93 bytes).
- **identitySig** — BRC-42/43 under protocol `[1,'low potparty']`,
  keyID = lowercase gameId, counterparty `anyone`, over
  `"LOW/hopparty/v1" ‖ identity(33) ‖ opponentIdentity(33) ‖ gameId(32) ‖
  hopVout(4 LE) ‖ hopSats(8 LE) ‖ seatSettlePubkey(33)`.

Readers must accept only **canonical strict DER** — re-encode the parsed
`(r, s)` and demand byte-equality, with low-S. The length bounds above pin
nothing about the encoding.

Neither digest can bind the containing txid (the same fixed point). What
binds the marker to its transaction is that transaction's own funding
signature under `SIGHASH_ALL`, which commits every output.
