//! `tm_reveal` / `ls_reveal` — LOW break-glass reveal index.
//!
//! Indexes LOW break-glass REVEAL artifacts so the watchtower can look
//! up "did `(gameId, seat)` publish a reveal?" by key, instead of
//! scanning a payout address's WhatsOnChain history. Phase 1a of moving
//! the tower's reveal lookup onto our own overlay infra.
//!
//! Unlike `tm_low` (a signed-PushDrop lobby topic), a reveal is an
//! unsigned, provably-unspendable `OP_RETURN` data-carrier: when the
//! tower stalls/misbehaves an honest seat publishes its opening on-chain,
//! making "I revealed" a public, timestamped fact. Anyone can publish
//! one, so there is nothing to sign — the tower ADJUDICATES genuineness
//! downstream (`case::adjudicate_break_glass`). This topic only proves
//! the artifact is well-formed and extracts `(gameId, seat)` for keying.
//!
//! # The indexed output — OP_RETURN, never the beacon
//!
//! A LOW reveal transaction (`app/src/lib/stake.ts::publishOnChainReveal`)
//! carries TWO outputs:
//!
//! - an `OP_FALSE OP_RETURN` artifact (the `LOW/reveal/v2` data carrier), and
//! - a P2PKH "beacon" paying the accused's `pay_pkh`.
//!
//! We admit the **OP_RETURN artifact**, NOT the beacon. The OP_RETURN is
//! provably unspendable, so the indexed record is PERMANENT — a reveal is
//! a permanent fact. The beacon P2PKH gets reclaimed/spent later
//! (`stake.ts::reclaimRevealBeacon`); had we indexed it, its spend would
//! evict the record. The lookup service therefore uses
//! [`SpendNotificationMode::None`] and treats spend/eviction as no-ops:
//! a reveal record is NEVER removed.
//!
//! # Artifact wire format (`LOW/reveal/v2`)
//!
//! `OP_FALSE OP_RETURN` (or a bare `OP_RETURN`) followed by six minimal
//! data pushes — byte-identical to the app's `revealArtifactScriptHex`
//! and the tower's `break_glass::parse_reveal_artifact`:
//!
//! | # | Push        | Encoding                                        |
//! |---|-------------|-------------------------------------------------|
//! | 0 | tag         | UTF-8 `LOW/reveal/v2` (13 bytes)                |
//! | 1 | gameId      | 32 bytes                                        |
//! | 2 | seat        | 1 byte, `0x00` (A) or `0x01` (B)               |
//! | 3 | positions   | 5 bytes (the seat's final deck positions)       |
//! | 4 | own scalars | 160 bytes (5 × 32-byte remask scalars)          |
//! | 5 | peer scalars| 160 bytes (5 × 32-byte claimant scalars)        |
//!
//! The topic manager validates this shape strictly (reject non-LOW /
//! malformed txs so the index can't be spammed with junk) and extracts
//! `(gameId, seat)`. It does NOT adjudicate the hand (the tower does):
//! a well-formed but "cooked" artifact IS admitted, and the tower sorts
//! genuine from cooked among ALL indexed matches.
//!
//! # Lookup (`ls_reveal`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "byGameSeat", "gameId": "<64 hex chars>", "seat": 0}
//! {"type": "byGameId",   "gameId": "<64 hex chars>"}
//! ```
//!
//! Answers are the engine's standard output list (UTXO references,
//! hydrated to BEEF by `/lookup`, exactly like `ls_low`). The tower
//! parses the returned BEEF back into the raw reveal tx and feeds it to
//! `break_glass::parse_reveal_artifact` / `case::adjudicate_break_glass`.

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
