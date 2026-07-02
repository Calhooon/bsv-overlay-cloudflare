//! `tm_low` / `ls_low` — LOW poker lobby topic manager + lookup service.
//!
//! Players discover open LOW poker tables and resolve a table's live
//! game-UTXO (the pot escrow outpoint) via the overlay. Two PushDrop
//! record types are admitted under `tm_low`; both are signed by the
//! table host's identity key with the same BRC-42/43 key linkage that
//! SHIP/SLAP advertisements use.
//!
//! # Record types
//!
//! ## TABLE_OPEN — announces an open table (8 fields)
//!
//! | # | Field                 | Encoding                                   |
//! |---|-----------------------|--------------------------------------------|
//! | 0 | protocol tag          | UTF-8 `LOW.table.v1` (12 bytes)            |
//! | 1 | host identity key     | 33-byte compressed secp256k1 pubkey        |
//! | 2 | gameId                | 32 bytes                                   |
//! | 3 | stake satoshis        | 8-byte little-endian u64                   |
//! | 4 | rules hash            | 32 bytes                                   |
//! | 5 | relay URL             | UTF-8, 1..=512 bytes, `https://`/`wss://`  |
//! | 6 | expiry block height   | 4-byte little-endian u32                   |
//! | 7 | signature             | ECDSA over concat(fields\[0..7\])          |
//!
//! An unspent TABLE_OPEN token = an open table. The host closes the
//! table by spending the token; the engine's spend notification then
//! removes it from the `ls_low` index.
//!
//! ## GAME_UTXO — points at the live pot escrow outpoint (6 fields)
//!
//! | # | Field                 | Encoding                                   |
//! |---|-----------------------|--------------------------------------------|
//! | 0 | protocol tag          | UTF-8 `LOW.gameutxo.v1` (15 bytes)         |
//! | 1 | host identity key     | 33-byte compressed secp256k1 pubkey        |
//! | 2 | gameId                | 32 bytes                                   |
//! | 3 | pot txid              | 32 bytes (display-hex byte order)          |
//! | 4 | pot vout              | 4-byte little-endian u32                   |
//! | 5 | signature             | ECDSA over concat(fields\[0..5\])          |
//!
//! This lets the host announce the live escrow outpoint for a table
//! WITHOUT modifying the funding transaction itself — the pointer token
//! is a separate 1-sat output. A spent GAME_UTXO pointer = superseded
//! (the host publishes a fresh pointer whenever the pot outpoint moves).
//!
//! # Signature scheme (identical to SHIP's linkage validation)
//!
//! The host signs the concatenation of all data fields (everything
//! except the trailing signature field) with a BRC-42 derived key:
//!
//! - protocol ID: `[2, "low poker lobby"]` (security level Counterparty)
//! - key ID: `"1"`
//! - counterparty: `anyone`
//!
//! The PushDrop locking key MUST be the host's own derived child for
//! that protocol/key ID (`for_self = true` on the signer side). The
//! overlay verifies with `ProtoWallet::anyone()` — signature valid AND
//! derived key == locking key — via
//! [`crate::validation::is_token_signature_correctly_linked`] with
//! protocol name `"LOW"`.
//!
//! # Lookup (`ls_low`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "findOpenTables", "stakeMin": 100, "stakeMax": 5000}
//! {"type": "byGameId", "gameId": "<64 hex chars>"}
//! {"type": "byHost", "identityKey": "<66 hex chars>"}
//! ```
//!
//! Answers are the engine's standard output-list (UTXO references,
//! hydrated with BEEF by `/lookup`), exactly like `ls_ship`. Clients
//! decode the returned PushDrop fields to read stake / relay URL / pot
//! outpoint.

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
