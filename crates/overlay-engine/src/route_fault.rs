//! Route faults (engine 0.2.0, bsv-overlay-cloudflare #57).
//!
//! A ROUTE FAULT is a failure the platform answers before the peer is
//! reached: the peer never saw the request, so the failure says nothing about
//! the peer. It counts nothing against the peer's health rows (the
//! consecutive failures that quarantine it, its yieldless streak, its quiet
//! cadence). Only an answer FROM the peer counts: a 5xx, a body that does not
//! parse, a verification failure, a GASP error.
//!
//! The witness (2026-10-14 16:23Z): one overlay Worker synced from another
//! Worker of the same account over its `workers.dev` name. The platform
//! refuses that route with HTTP 404 and the body `error code: 1042`; every
//! call failed there, the failures were charged to the serving peer, and
//! after [`crate::gasp::PEER_QUARANTINE_THRESHOLD`] of them a live peer was
//! quarantined for six hours. The quarantine judges the peer, never the
//! route.
//!
//! The classes:
//!
//! - [`class::CF_1042`]: an HTTP 404 whose body carries the platform's
//!   `error code: 1042` (the witnessed shape).
//! - [`class::DNS`], [`class::TLS`], [`class::CONNECT_TIMEOUT`]: a fetch that
//!   never got an HTTP answer, named by its error text.
//!
//! The rule is narrow on purpose: a failure this module does not recognize
//! counts against the peer as it did before 0.2.0, so a dead peer is still
//! quarantined, and only a named route is excused.

use serde::{Deserialize, Serialize};

/// The route-fault classes, as named on the tick line (`route_fault:<class>`).
pub mod class {
    /// The platform refused a Worker-to-Worker call over `workers.dev`
    /// (HTTP 404, `error code: 1042`).
    pub const CF_1042: &str = "cf_1042";
    /// The peer's name did not resolve.
    pub const DNS: &str = "dns";
    /// The TLS handshake with the peer's name failed.
    pub const TLS: &str = "tls";
    /// The connection to the peer was never made within the platform's time.
    pub const CONNECT_TIMEOUT: &str = "connect_timeout";
}

/// The class of an HTTP answer the platform gave in the peer's place, or
/// `None` when the answer is the peer's.
pub fn of_answer(status: u16, body: &str) -> Option<&'static str> {
    (status == 404 && body.to_ascii_lowercase().contains("error code: 1042"))
        .then_some(class::CF_1042)
}

/// The class of a fetch that never got an HTTP answer, by its error text, or
/// `None` when the text names no route fault (it then counts as before).
pub fn of_transport(message: &str) -> Option<&'static str> {
    let m = message.to_ascii_lowercase();
    let any = |words: &[&str]| words.iter().any(|w| m.contains(w));
    if any(&[
        "dns",
        "getaddrinfo",
        "enotfound",
        "name not resolved",
        "could not resolve",
    ]) {
        Some(class::DNS)
    } else if any(&["tls", "ssl", "certificate", "handshake"]) {
        Some(class::TLS)
    } else if any(&["etimedout", "connect timeout", "connection timed out"])
        || (m.contains("connect") && any(&["timed out", "timeout"]))
    {
        Some(class::CONNECT_TIMEOUT)
    } else {
        None
    }
}

/// One route fault a sync met: the peer it was meant for and its class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteFaultSeen {
    pub peer: String,
    pub class: String,
}

impl RouteFaultSeen {
    /// The tick line's name of it: `<peer> route_fault:<class>`.
    pub fn label(&self) -> String {
        format!("{} route_fault:{}", self.peer, self.class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_057_the_1042_shape_is_the_platforms_and_nothing_else_is() {
        assert_eq!(of_answer(404, "error code: 1042"), Some(class::CF_1042));
        assert_eq!(of_answer(404, "Error Code: 1042\n"), Some(class::CF_1042));
        // The peer's own answers stay the peer's.
        for (status, body) in [
            (404, "Not Found"),
            (404, r#"{"status":"error","message":"Route not found"}"#),
            (500, "error code: 1042"),
            (502, "Bad Gateway"),
            (503, "error code: 1102"),
            (400, "error code: 1042"),
        ] {
            assert_eq!(of_answer(status, body), None, "{status} {body}");
        }
    }

    #[test]
    fn route_057_a_transport_failure_is_named_only_by_a_route_word() {
        for (text, want) in [
            ("Fetch to https://p/x failed: DNS lookup failed", class::DNS),
            ("getaddrinfo ENOTFOUND peer.example", class::DNS),
            ("TLS handshake failed", class::TLS),
            ("Fetch failed: certificate has expired", class::TLS),
            ("connect ETIMEDOUT 10.0.0.1:443", class::CONNECT_TIMEOUT),
            ("Fetch failed: connection timed out", class::CONNECT_TIMEOUT),
        ] {
            assert_eq!(of_transport(text), Some(want), "{text}");
        }
        for text in [
            "Fetch to https://p/x failed: Network connection lost",
            "connection reset by peer",
            "internal error",
        ] {
            assert_eq!(of_transport(text), None, "{text}");
        }
    }
}
