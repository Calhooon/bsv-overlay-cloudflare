# tm_collected — LOW "Already Collected" Marker Topic Manager

Indexes LOW cross-device "already collected" markers (bsv-low #161).
When a LOW device successfully collects a credit (a landing-proof-gated
internalize succeeds), it publishes a tiny owner-signed `OP_RETURN`
marker under this topic. Other devices of the SAME identity query
`ls_collected` during the home/History card gather; a marker they can
VERIFY was signed by their own identity flips the card to "collected on
another device" instead of offering Collect.

## What it admits

An output is admitted IFF its locking script is a well-formed
`LOW/collected/v1` marker — BYTE FORMAT ONLY, like `tm_reveal`. There is
**no server-side signature verification**: the security lives in the
CLIENT verify (which holds the wallet). A querying device trusts a marker
only after its own `wallet.verifySignature` validates the carried sig
under its OWN identity's derived key (`[1,'low collected']`,
keyID = gameId, counterparty = 'self'). A forged or foreign-identity
marker is ignored client-side, and the marker is a UI HINT only — it
never gates a credit (the fail-safe is always toward SHOWING the Collect
card).

## Marker wire format (`LOW/collected/v1`)

`OP_FALSE OP_RETURN` (0x00 0x6a) followed by exactly four minimal data
pushes — byte-identical to the app's `collectedMarkerScriptHex`:

| # | Push        | Encoding                              |
|---|-------------|---------------------------------------|
| 0 | tag         | UTF-8 `LOW/collected/v1` (16 bytes)   |
| 1 | gameId      | 32 bytes                              |
| 2 | identityKey | 33 bytes (compressed identity pubkey) |
| 3 | sig         | DER ECDSA signature, 68..=74 bytes    |

Wrong tag / wrong lengths / extra or missing pushes → not admitted. The
strict format check keeps junk out of the index; genuineness is the
client's job.
