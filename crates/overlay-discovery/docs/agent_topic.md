# Agent Topic Manager

The Agent Registry Topic Manager validates agent registration PushDrop outputs, enabling agent discovery on the overlay network.

## PushDrop Token Format

Agent registrations are encoded as PushDrop locking scripts with exactly 6 fields:

| Field | Type | Description |
|-------|------|-------------|
| 0 | UTF-8 | Protocol identifier: `"AGENT"` |
| 1 | Binary | Subject identity key: 33-byte compressed secp256k1 public key |
| 2 | Binary | Certifier identity key: 33-byte compressed secp256k1 public key |
| 3 | UTF-8 | Endpoint URI (must pass BRC-101 validation) |
| 4 | UTF-8 | Capabilities: comma-separated list (e.g. `"image-generation,upscaling"`) |
| 5 | Binary | ECDSA signature linking identity key to locking key |

## Validation Rules

1. Output must be a valid PushDrop script
2. Must have exactly 6 fields
3. Field 0 must be `"AGENT"`
4. Field 1 must be a 33-byte compressed public key (subject)
5. Field 2 must be a 33-byte compressed public key (certifier)
6. Field 3 must be an advertisable URI (no localhost, valid scheme)
7. Field 4 must be a non-empty capabilities string
8. Field 5 must contain a non-empty ECDSA signature

## Self-signed vs Certifier-signed

- **Self-signed**: certifier key == subject key, counterparty = Anyone
- **Certifier-signed**: certifier signs on behalf of subject
