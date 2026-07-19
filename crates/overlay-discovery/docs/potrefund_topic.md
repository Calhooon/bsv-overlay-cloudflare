# tm_potrefund — LOW Pre-Signed Refund-Backup Marker Topic Manager

Indexes LOW pre-signed refund-backup markers for keyless recovery
re-broadcast (bsv-low #191, recovery defense-in-depth). A fully-wiped
device recovers via seed + the `potparty` index (#188) — but the actual
on-chain REFUND that brings a both-players-vanished pot home still relies
on the tower's dead-man's switch. This marker is the BELT: each seat
publishes the pre-signed 2-of-2 refund transaction itself — public-safe
(non-final until its recovery height, and by the covenant it can only pay
the mandated refund homes) — so ANY client can re-broadcast it after
recovery even if the tower failed. Public data only, non-custodial.

## What it admits

An output is admitted IFF its locking script is a well-formed
`LOW/potrefund/v1` marker — BYTE FORMAT ONLY, like `tm_result` /
`tm_collected` / `tm_potparty`. There is **no server-side signature
verification and no transaction validation**: the overlay is an INDEX, not
an authority, and it carries the marker's bytes (including the
`refundRawHex` + `sig` pushes) back verbatim. A client that cares about
authenticity parses and verifies the refund itself. The index keeps EVERY
admitted marker (keyed by outpoint) — a garbage marker can never occupy a
slot and censor a later genuine one.

## Marker wire format (`LOW/potrefund/v1`)

`OP_FALSE OP_RETURN` (0x00 0x6a) followed by exactly seven minimal data
pushes — byte-identical to the app's builder:

| # | Push          | Encoding                                          |
|---|---------------|---------------------------------------------------|
| 0 | tag           | UTF-8 `LOW/potrefund/v1` (16 bytes)              |
| 1 | identity      | 33 bytes (publishing seat's compressed pubkey)   |
| 2 | gameId        | 32 bytes                                         |
| 3 | potTxid       | 32 bytes                                         |
| 4 | potVout       | 4 bytes little-endian (u32)                      |
| 5 | refundRawHex  | VARIABLE — the pre-signed refund tx bytes        |
|   |               | (non-empty, <= 100_000; OP_PUSHDATA2/4 path)     |
| 6 | sig           | DER ECDSA, 68..=74 bytes (preserved, never       |
|   |               | verified by the overlay)                         |

Wrong tag / wrong lengths / extra or missing pushes / a truncated push /
an empty or oversized refund → not admitted. The strict format check keeps
junk out of the index; genuineness is the client's job.
