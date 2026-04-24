//! Error codes for the overlay-cloudflare deployment.
//!
//! Mirrors `~/bsv/bsv-storage-cloudflare/src/error.rs` naming. Only the
//! codes actually referenced by the ported `wallet/` module are exposed —
//! the advertiser bubbles up to overlay-engine's `AdvertiserError`, so
//! most of the HTTP-oriented route codes from bsv-storage don't belong
//! here.

/// Wallet-infra endpoint is unreachable or returned a non-auth transport
/// failure (network down, HTTP 5xx).
pub const ERR_WALLET_UNAVAILABLE: &str = "ERR_WALLET_UNAVAILABLE";

/// BRC-103 handshake or subsequent BRC-104 signed RPC failed with an
/// auth-related error (HTTP 401 / InvalidAuthentication / unauthorized).
pub const ERR_WALLET_AUTH_FAILED: &str = "ERR_WALLET_AUTH_FAILED";

/// BEEF bytes failed to parse, or a sub-structure (input/output/sighash)
/// came back in an unexpected shape from wallet-infra.
pub const ERR_BEEF_PARSE: &str = "ERR_BEEF_PARSE";

/// Local signing failed — typically a sighash computation error, a
/// signature serialization failure, or an unsupported input script.
pub const ERR_BEEF_SIGN: &str = "ERR_BEEF_SIGN";
