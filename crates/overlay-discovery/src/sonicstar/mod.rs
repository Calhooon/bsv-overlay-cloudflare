//! `tm_sonicstar` topic manager and `ls_sonicstar` lookup service for the
//! SonicStar Song Source Protocol (`sssp`).
//!
//! ## Why this exists
//!
//! SonicStar is a music distribution platform built on top of BSV. Each track
//! is published as a single OP_RETURN output that carries a UTF-8 JSON
//! envelope describing the song (title, artist, file URLs, royalty rate,
//! etc.). This plugin admits those outputs into the `tm_sonicstar` topic and
//! lets clients query the index via `ls_sonicstar`.
//!
//! ## On-wire format
//!
//! Unlike every other plugin in this crate, sonicstar does **not** use
//! `PushDrop`. The locking script is a bare:
//!
//! ```text
//! OP_RETURN <push: utf-8 JSON>
//! ```
//!
//! where the JSON envelope is:
//!
//! ```json
//! {
//!   "protocol": "sssp",
//!   "securityLevel": 2,
//!   "songTitle": "...",
//!   "artistName": "...",
//!   "duration": 240,
//!   "songFileURL": "uhrp://...",
//!   "artFileURL": "uhrp://...",
//!   "previewURL": "https://...",
//!   "genre": "...",
//!   "album": "...",
//!   "releaseDate": "2025-04-25",
//!   "pricePerPlay": 1000,
//!   "royaltyRate": 75
//! }
//! ```
//!
//! ## Admission rules
//!
//! 1. JSON parses as an object (after stripping any leading/trailing junk
//!    around `{` ... `}`).
//! 2. `protocol === "sssp"`.
//! 3. `songTitle`, `artistName`, `songFileURL` are all non-empty strings.
//!
//! `securityLevel` is informational and not enforced (matches Ruth's TS
//! reference at `sonicstarTopic.ts:34-37`). If she later promotes it to
//! enforced policy we would add a check here.
//!
//! ## Decoder permissiveness
//!
//! The bsv-rs `Script::from_binary` parser collapses everything after
//! `OP_RETURN` into `chunks[0].data` as a single buffer. Real-world encoders
//! also occasionally emit the JSON push as a separate trailing chunk. To
//! match Ruth's TypeScript decoder byte for byte, we try three candidate
//! buffers in order:
//!
//! 1. Each non-empty `chunks[i].data` for `i >= 1` (separate-push form).
//! 2. The raw `chunks[0].data` itself (the bsv-rs collapsed form).
//! 3. `chunks[0].data` re-parsed as an inner script, each push payload
//!    tried in turn.
//!
//! The first candidate that round-trips as a JSON object whose `protocol`
//! is `"sssp"` wins.
//!
//! ## Lookup queries
//!
//! - `"findAll"` (string) or `{}` or `{ "findAll": true }` enumerate all.
//! - `{ "txid": "..." }` exact match.
//! - `{ "artistName": "..." }` case insensitive substring.
//! - `{ "genre": "..." }` exact match (case sensitive, matches Mongo).
//! - `{ "searchText": "..." }` case insensitive substring across
//!   `songTitle`, `artistName`, `album`.
//! - `limit` clamped to `[1, 200]`, default `50`.
//! - `skip` clamped to `[0, ..]`, default `0`.
//! - Multiple filter keys combine via AND (matches Mongo filter object).

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
