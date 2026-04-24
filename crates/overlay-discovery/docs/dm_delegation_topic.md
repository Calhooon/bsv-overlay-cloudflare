# tm_dm_delegation — Dolphin Milk Delegation Revocation Topic

## Purpose

Indexes the on-chain UTXOs that anchor revocation status for **dolphin-milk
macaroon-style cross-agent delegation certificates**. Recipients of a
delegation cert query the paired `ls_dm_delegation` lookup service to
confirm the cert hasn't been revoked before honoring it.

## On-wire format

Each admissible output is a 1-sat **3-field PushDrop** locked to the
issuer's identity key:

| Field | Encoding | Contents |
|-------|----------|----------|
| 0 | `b"delegation_revocation"` (literal bytes) | Protocol marker |
| 1 | UTF-8 JSON object | Delegation envelope (see below) |
| 2 | UTF-8 ASCII string | Unix timestamp at creation (e.g. `"1700000000"`) |

The locking script ends with `<issuer_identity_pubkey> OP_CHECKSIG`, so only
the issuer can spend (revoke).

### Field 1 envelope schema

```json
{
  "type": "DelegationRevocation",
  "serial_number": "delegation-<issuer_prefix>-<purpose_prefix>-<unix_ms>",
  "subject": "<66-hex recipient identity key>",
  "certifier": "<66-hex issuer identity key>",
  "purpose_hash": "sha256:<64-hex>",
  "issued_at": "<RFC3339 UTC>",
  "expires_at": "<RFC3339 UTC>"
}
```

The topic manager validates the marker, the JSON envelope structure
(`type`, `serial_number`, `certifier` required), and the timestamp field.
It does **not** verify the cert signature itself — that happens in the
recipient's verifier path against the cert envelope delivered separately
via MessageBox.

## Revocation semantics

While the UTXO is in the `ls_dm_delegation` unspent set, the cert is valid.
When the issuer spends it, the overlay's standard spent-output handling
removes it from the index, and `ls_dm_delegation` lookups for the same
outpoint return the empty list — which the recipient interprets as
**revoked**.

## Lookup queries

| Query | Description |
|-------|-------------|
| `{"findByOutpoint": "<txid>.<vout>"}` | **Primary.** Used by `OverlayRevocationChecker` on every revocation re-check. |
| `{"findBySerial": "<serial_number>"}` | Issuer-side lookup when the outpoint isn't tracked. |
| `{"findByCertifier": "<66-hex pubkey>"}` | List all live revocation UTXOs for an issuer. |
| `{"findAll": true}` | Debugging only. |

## Reference implementation

- Producer: `rust-bsv-worm/src/tools/delegation_tools.rs` — Phase 2 of
  EPIC #329 (`delegate_task` LLM tool).
- Consumer: `rust-bsv-worm/src/delegation/revocation.rs::OverlayRevocationChecker`
  — Phase 3 wiring.
- Spec: `rust-bsv-worm/docs/DELEGATION-DESIGN.md` §6 (revocation).
