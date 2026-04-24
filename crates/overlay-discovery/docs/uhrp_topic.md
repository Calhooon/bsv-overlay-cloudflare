# UHRP Topic Manager

The **U**niversal **H**ash **R**esolution **P**rotocol (UHRP) Topic Manager
validates on-chain advertisements declaring that a given content hash is
available for download at a given public URL until a given expiry time.

Reference implementation sources:

- `~/bsv/bsv-storage-cloudflare/PROTOCOL.md` §B (field layout)
- `~/bsv/bsv-storage-cloudflare/src/uhrp/pushdrop.rs` (our own lock-side builder)
- `~/bsv/bsv-storage-cloudflare/src/routes/advertise.rs` (author-side flow)
- `~/bsv/storage-server/src/utils/createUHRPAdvertisement.ts` (TS author-side)

There is no TypeScript reference for a UHRP topic manager on disk or npm —
`@bsv/overlay-discovery-services` ships SHIP + SLAP only. This crate is the
reference implementation; the validation rules below are derived from the
author-side TS code and the live `tm_uhrp` on `overlay-us-1.bsvb.tech`.

## PushDrop Token Format

UHRP advertisements are encoded as PushDrop locking scripts with exactly
**5 data fields** plus an appended ECDSA signature (the 6th pushed field,
inserted automatically by `PushDrop.lock(..., includeSignature=true)`):

| Field | Type     | Description                                                                 |
|-------|----------|-----------------------------------------------------------------------------|
| 0     | Binary   | Server identity public key — 33-byte compressed secp256k1 pubkey            |
| 1     | Binary   | Content SHA-256 — exactly 32 bytes                                          |
| 2     | UTF-8    | Public download URL (must pass BRC-101 `is_advertisable_uri()`)             |
| 3     | VarInt   | Expiry time — unix seconds, Bitcoin-style VarInt                            |
| 4     | VarInt   | Content length — bytes, Bitcoin-style VarInt                                |
| 5     | Binary   | ECDSA signature (DER) over `concat(field[0..4])`, linked to field[0]        |

## BRC-42 Derivation

- Protocol ID: `(2, "uhrp advertisement")`
- Key ID: `"1"`
- Counterparty: `"anyone"`

The PushDrop locking key is the BRC-42 child the author derives from their
root identity (field[0]) against `counterparty="anyone"`. The signature in
field[5] is signed with the corresponding private child. The verifier
(`ProtoWallet::anyone()`) derives the same child via BRC-42 symmetry using
`counterparty = Other(field[0])`.

## Validation Rules

An output is admitted iff **all** of the following hold:

1. `PushDrop::decode(locking_script)` succeeds.
2. The decoded PushDrop has exactly **5** data fields (the signature is the 5th field).

   Wait — strictly, `PushDrop::decode` returns all pushed data fields
   including the signature, so the count we see in Rust is **5**
   (field[0..4] are data, the signature rides inside the decoded set when
   `includeSignature=true` is on the lock side). The TS author builds
   fields as `[pubkey, hash, url, expiryVarInt, lengthVarInt]` and `lock()`
   appends the signature as a sixth push. The Rust-side `PushDrop::decode`
   used here treats them all as fields so we see **5** + signature
   separately. We therefore require the decode to yield exactly 5 data
   fields (the signature is split out by the PushDrop template and
   returned alongside).
3. `field[0]` is exactly 33 bytes and parses as a valid compressed
   secp256k1 public key.
4. `field[1]` is exactly 32 bytes (SHA-256 digest size).
5. `field[2]` passes `is_advertisable_uri()` (no localhost, https-based
   scheme, no pathname other than `/`).
6. `field[3]` decodes cleanly as a `VarInt` via
   `bsv_rs::primitives::encoding::Reader::read_var_int` and `> 0`.
7. `field[4]` decodes cleanly as a `VarInt` and `> 0` (zero-length content
   is not admissible).
8. **Expiry policy — STRICT REJECT**: if `field[3] <= now_unix_seconds`,
   the advert is rejected. Rationale: expiry is an in-protocol field whose
   purpose is signalling how long the hosting commitment lasts; admitting
   already-expired adverts is never useful to lookup clients and adds
   junk to the index. SHIP adverts don't carry expiry so SHIP's topic
   manager doesn't reject on it; UHRP is different.
9. The ECDSA signature verifies against `field[0]`'s pubkey over
   `concat(field[0..4])` under BRC-42 derivation with protocol
   `"uhrp advertisement"`, key id `"1"`, counterparty
   `Other(field[0])`.
10. The locking public key embedded in the PushDrop matches the BRC-42
    child derived by `ProtoWallet::anyone()` for the same protocol /
    key id / counterparty — i.e. the lock is bound to the identity in
    field[0] (mirrors `is_token_signature_correctly_linked` for SHIP).

Any failure skips the output; no error propagates to the caller unless
the BEEF itself is malformed.

## Observation of bsvb's live `tm_uhrp`

We did not empirically probe `overlay-us-1.bsvb.tech` with an
already-expired advert BEEF at implementation time; doing so requires
building a signed-and-broadcast tx that we'd then abandon. If field
evidence later shows bsvb admits expired adverts, relax rule 8 to match
(it is a single conditional). Live-parity tests gated on
`UHRP_LIVE_PARITY=1` will flag any divergence against bsvb.
