//! `tm_dm_delegation` / `ls_dm_delegation` — overlay topic manager and
//! lookup service for **dolphin-milk delegation revocation UTXOs**.
//!
//! ## Why this exists
//!
//! dolphin-milk's macaroon-style cross-agent delegation (EPIC #329 in the
//! `rust-bsv-worm` repo) lets one agent (the *issuer*) hand a scoped
//! capability cert to another agent (the *recipient*). The recipient must
//! be able to ask: *"is this cert still valid, or has the issuer revoked it?"*
//!
//! Revocation works by spending an on-chain UTXO that the issuer creates
//! when minting the cert. While the UTXO exists, the cert is valid; once
//! the issuer spends it, the cert is revoked.
//!
//! The recipient can't scan the issuer's wallet to learn this. Instead, the
//! issuer publishes the revocation UTXO transaction to **this overlay**
//! under the `tm_dm_delegation` topic. The overlay engine indexes the
//! output and tracks its spent status. The recipient then queries
//! `ls_dm_delegation` with the cert's outpoint to learn whether the UTXO
//! is still in the unspent set.
//!
//! ## On-wire format
//!
//! Phase 2 of the dolphin-milk delegation work creates each revocation UTXO
//! as a **3-field PushDrop** locked to the issuer's identity key:
//!
//! 1. `b"delegation_revocation"` — fixed protocol marker
//! 2. JSON object: `{type, serial_number, subject, certifier, purpose_hash,
//!    issued_at, expires_at}`
//! 3. Unix timestamp string of the creation time (unencrypted, for age queries)
//!
//! The locking script ends with `<issuer_identity_pubkey> OP_CHECKSIG` so
//! only the issuer can spend (revoke). The topic manager validates the
//! marker + the JSON envelope structure; it does NOT verify the cert
//! signature itself — that happens in dolphin-milk's verifier path against
//! the cert envelope delivered separately via MessageBox.
//!
//! ## Lookup queries
//!
//! - `{"findByOutpoint": "<txid>.<vout>"}` — primary query, used by the
//!   recipient's `OverlayRevocationChecker` on every revocation re-check.
//!   Returns the matching UTXO if unspent, empty list if spent or unknown.
//! - `{"findBySerial": "<serial_number>"}` — useful for the issuer when
//!   they know the cert serial but not the outpoint.
//! - `{"findByCertifier": "<66-hex pubkey>"}` — list all live revocation
//!   UTXOs for a given issuer. Useful for audit / observability.
//! - `{"findAll": true}` — debugging.

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
