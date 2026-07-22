//! `GET /tx-any/:txid` — tx-level presence / confirmation / raw bytes for
//! ARBITRARY txids, honoring the READ HIERARCHY (owner doctrine, bsv-low
//! #229, 2026-07-22):
//!
//!   1. **Index-native leg (system of record).** Every tx LOW ever broadcast
//!      is admitted to the overlay (network-accept-gated) and its BEEF is
//!      stored durably (`pot_beefs` / `transactions`). If the stored BEEF
//!      carries a chaintracks-verified BUMP (stitched by the completion pass /
//!      arc-ingest merkle push), the tx is PROVEN mined — presence, raw bytes,
//!      confirmation, and height all answer from the index, zero external
//!      reads. A stored BEEF without a BUMP still proves presence + raw bytes
//!      (admission was network-accept-gated); only the confirmation question
//!      falls through to the external leg.
//!   2. **Break-glass external leg (WoC + Bitails, SERVER-SIDE).** Only for
//!      txids the index has never seen: legacy pre-overlay-era txs (the
//!      2026-07-21 incident class) and foreign txs. The trust bars of the
//!      client code this replaces are preserved server-side:
//!        - POSITIVE presence requires the raw bytes fetched AND hash-verified
//!          against the txid (never a bare pointer/claim — the raw is also
//!          returned so the caller gets verified bytes for free);
//!        - `confirmed` carries WoC's `confirmations >= 1` claim — the exact
//!          trust the client's `wocTxConfirmed` placed in a direct WoC read;
//!        - NEGATIVE (provably absent) requires BOTH indexers to answer a
//!          definitive 404 AND the Bitails tx route to prove itself healthy
//!          against a known-mined anchor (the client's
//!          `bitailsConclusively404` route-rot guard, ported verbatim) — one
//!          provider's 404, or a 404 on a rotten route, is never absence;
//!        - anything else is the honest unknown (`present: null`) — the
//!          callers' fail-safe "unknown ⇒ retry, never a conclusion".
//!
//! Wire body: `{"txid","present","confirmed","height","rawHex","source"}`
//! where `source` is `"index"` / `"index+external"` / `"external"` / `null`
//! (unknown). All-null fields = nothing could be established.

use serde_json::json;

/// A tx that is unquestionably mined (mainnet, height 958886 — the same
/// route-sanity anchor the client used: bsv-low `homeCards.ts
/// KNOWN_MINED_TXID`). If Bitails 404s THIS txid, its tx route is
/// broken/moved and its 404s prove nothing.
pub const KNOWN_MINED_TXID: &str = "f358a4dd67c9d7b3a295d05d7a23abc0b85ba1f95c8afa756f1f466419be5e1c";

/// Hard TTL for the in-isolate `/tx-any` cache, milliseconds (same figure as
/// `/spent-any` — bounds upstream pressure; isolate recycling empties it).
pub const TX_ANY_CACHE_TTL_MS: f64 = 15_000.0;

/// The external (WoC) observation of a txid, already shape-validated by the
/// route glue. `Present.raw_hex` is `Some` ONLY when the fetched raw bytes
/// HASHED to the txid (the route verifies before constructing this).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxObservation {
    /// WoC 200 on `/tx/hash/{txid}`: `confirmed` = `confirmations >= 1`;
    /// `raw_hex` = the hash-VERIFIED raw (None when the raw fetch failed or
    /// the bytes didn't hash to the txid).
    Present {
        confirmed: bool,
        raw_hex: Option<String>,
    },
    /// WoC definitive 404 — "this txid is not in my index".
    Absent,
    /// Transport / 5xx / rate-limit / malformed body.
    Fault,
}

/// Bitails' corroboration of an ABSENT claim (negatives are never one
/// provider's word — the #212/#213/#214 lesson).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsenceCorroboration {
    /// Bitails 404 for the txid AND its tx route proved healthy against the
    /// known-mined anchor.
    CorroboratedAbsent,
    /// Anything else — fault, 200 (contradiction), rotten route.
    Unknown,
}

/// The assembled `/tx-any` answer, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TxAnyAnswer {
    /// `Some(true)` = provably present (index BEEF, or externally
    /// hash-verified raw); `Some(false)` = provably absent (corroborated
    /// double-404); `None` = unknown.
    pub present: Option<bool>,
    /// `Some(true)` = proven/claimed mined (index BUMP, or WoC
    /// confirmations≥1); `Some(false)` = present but not yet confirmed per
    /// the external leg; `None` = unknown.
    pub confirmed: Option<bool>,
    /// The mined block height per the stored BEEF's verified BUMP (index leg
    /// only — the external leg never claims a height).
    pub height: Option<u64>,
    /// The raw tx bytes as lowercase hex — index-extracted or externally
    /// hash-verified. Never an unverified byte.
    pub raw_hex: Option<String>,
    /// Which leg answered: `"index"` / `"index+external"` / `"external"`.
    pub source: Option<&'static str>,
}

/// The pure `/tx-any` decision table (unit-tested; the route feeds it real
/// observations). `index_raw_hex` is the raw extracted from a STORED BEEF
/// (already txid-bound by `extract_raw_tx_hex`); `index_height` is the BUMP
/// height when the completion pass has stitched one.
pub fn decide_tx_any(
    index_raw_hex: Option<String>,
    index_height: Option<u64>,
    external: Option<&TxObservation>,
    absence: AbsenceCorroboration,
) -> TxAnyAnswer {
    if let Some(raw) = index_raw_hex {
        // Index-native: admission was network-accept-gated, so a stored BEEF
        // IS presence. A verified BUMP is the strongest confirmation anchor
        // there is (chaintracks-verified merkle path) — fully index-native.
        if let Some(h) = index_height {
            return TxAnyAnswer {
                present: Some(true),
                confirmed: Some(true),
                height: Some(h),
                raw_hex: Some(raw),
                source: Some("index"),
            };
        }
        // Admitted but no BUMP yet: presence + raw from the index; only the
        // confirmation question consults the external leg. An external
        // Absent/Fault must NEVER contradict index presence (the index is the
        // system of record; a WoC 404 for an admitted tx only means "not
        // indexed there yet") — it just leaves `confirmed` unknown.
        return match external {
            Some(TxObservation::Present { confirmed: true, .. }) => TxAnyAnswer {
                present: Some(true),
                confirmed: Some(true),
                height: None,
                raw_hex: Some(raw),
                source: Some("index+external"),
            },
            _ => TxAnyAnswer {
                present: Some(true),
                confirmed: None,
                height: None,
                raw_hex: Some(raw),
                source: Some("index"),
            },
        };
    }
    // Break-glass external leg (legacy / foreign txids only).
    match external {
        Some(TxObservation::Present { confirmed, raw_hex }) => match raw_hex {
            // Positive presence ONLY with hash-verified bytes in hand — a
            // bare WoC pointer whose raw could not be fetched/verified is an
            // honest unknown, never a positive.
            Some(raw) => TxAnyAnswer {
                present: Some(true),
                confirmed: Some(*confirmed),
                height: None,
                raw_hex: Some(raw.clone()),
                source: Some("external"),
            },
            None => TxAnyAnswer::default(),
        },
        Some(TxObservation::Absent) => match absence {
            AbsenceCorroboration::CorroboratedAbsent => TxAnyAnswer {
                present: Some(false),
                confirmed: None,
                height: None,
                raw_hex: None,
                source: Some("external"),
            },
            AbsenceCorroboration::Unknown => TxAnyAnswer::default(),
        },
        Some(TxObservation::Fault) | None => TxAnyAnswer::default(),
    }
}

/// Parse a WoC `GET /tx/hash/{txid}` 200 body into the confirmation claim:
/// `confirmations >= 1`. A malformed body is simply "present, unconfirmed
/// claim unknown" → treated as `confirmed: false` (the caller's
/// `wocTxConfirmed` parity: anything unsure is false, never a landing).
pub fn parse_woc_confirmations(v: &serde_json::Value) -> bool {
    v.get("confirmations")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|c| c >= 1)
}

/// Verify externally-fetched raw bytes: they must parse AND hash to `txid`.
/// Returns the lowercase hex, or `None` (a lying/garbled provider byte never
/// leaves the server).
pub fn verify_raw_bytes(raw: &[u8], txid: &str) -> Option<String> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    if !tx.id().eq_ignore_ascii_case(txid) {
        return None;
    }
    Some(hex::encode(raw))
}

/// The `/tx-any` wire body.
pub fn tx_any_body(txid: &str, a: &TxAnyAnswer) -> String {
    json!({
        "txid": txid,
        "present": a.present,
        "confirmed": a.confirmed,
        "height": a.height,
        "rawHex": a.raw_hex,
        "source": a.source,
    })
    .to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn raw() -> String {
        "aabbccdd00".into() // opaque placeholder bytes — the decision table never parses them
    }

    #[test]
    fn index_leg_with_bump_is_fully_native() {
        // Even a contradicting external observation is irrelevant — the index
        // never consults it once the BUMP proves the mine.
        let a = decide_tx_any(
            Some(raw()),
            Some(958_886),
            Some(&TxObservation::Absent),
            AbsenceCorroboration::CorroboratedAbsent,
        );
        assert_eq!(a.present, Some(true));
        assert_eq!(a.confirmed, Some(true));
        assert_eq!(a.height, Some(958_886));
        assert_eq!(a.raw_hex, Some(raw()));
        assert_eq!(a.source, Some("index"));
    }

    #[test]
    fn index_leg_without_bump_keeps_presence_and_only_confirms_via_external_positive() {
        // External confirmed:true upgrades confirmation.
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Present {
                confirmed: true,
                raw_hex: None,
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), Some(true)));
        assert_eq!(a.source, Some("index+external"));

        // An external ABSENT can never contradict index presence — the index
        // is the system of record; confirmation just stays unknown.
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Absent),
            AbsenceCorroboration::CorroboratedAbsent,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), None));
        assert_eq!(a.source, Some("index"));

        // External present-but-unconfirmed adds nothing over the index.
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Present {
                confirmed: false,
                raw_hex: None,
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), None));
        assert_eq!(a.source, Some("index"));
    }

    #[test]
    fn external_positive_requires_verified_raw() {
        // Verified raw in hand → positive with the raw served.
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Present {
                confirmed: true,
                raw_hex: Some(raw()),
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), Some(true)));
        assert_eq!(a.raw_hex, Some(raw()));
        assert_eq!(a.source, Some("external"));

        // A bare pointer whose raw could not be verified is an honest
        // unknown — never a positive off an unverified claim.
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Present {
                confirmed: true,
                raw_hex: None,
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!(a, TxAnyAnswer::default());
    }

    #[test]
    fn absence_requires_corroboration() {
        // WoC 404 alone → unknown (one provider's negative is never the
        // network verdict).
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Absent),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!(a, TxAnyAnswer::default());

        // Both 404 + healthy route → provably absent.
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Absent),
            AbsenceCorroboration::CorroboratedAbsent,
        );
        assert_eq!(a.present, Some(false));
        assert_eq!(a.source, Some("external"));
    }

    #[test]
    fn faults_are_unknown() {
        let a = decide_tx_any(
            None,
            None,
            Some(&TxObservation::Fault),
            AbsenceCorroboration::CorroboratedAbsent, // even a "corroborated" absence can't rescue a WoC fault
        );
        assert_eq!(a, TxAnyAnswer::default());
        let a = decide_tx_any(None, None, None, AbsenceCorroboration::Unknown);
        assert_eq!(a, TxAnyAnswer::default());
    }

    #[test]
    fn woc_confirmations_parse() {
        assert!(parse_woc_confirmations(&json!({"confirmations": 3})));
        assert!(!parse_woc_confirmations(&json!({"confirmations": 0})));
        assert!(!parse_woc_confirmations(&json!({})));
        assert!(!parse_woc_confirmations(&json!({"confirmations": "3"})));
    }

    #[test]
    fn raw_verification_binds_the_hash() {
        // A real minimal tx: version|0 inputs|0 outputs|locktime.
        let bytes = hex::decode("01000000000000000000").unwrap();
        let txid = bsv_rs::transaction::Transaction::from_binary(&bytes)
            .unwrap()
            .id();
        assert_eq!(verify_raw_bytes(&bytes, &txid), Some(hex::encode(&bytes)));
        // Wrong txid → refused.
        assert_eq!(verify_raw_bytes(&bytes, &"0".repeat(64)), None);
        // Garbage bytes → refused.
        assert_eq!(verify_raw_bytes(&[0x00, 0x01], &txid), None);
    }

    #[test]
    fn wire_body_shape() {
        let a = TxAnyAnswer {
            present: Some(true),
            confirmed: Some(true),
            height: Some(1),
            raw_hex: Some("aa".into()),
            source: Some("index"),
        };
        let v: serde_json::Value = serde_json::from_str(&tx_any_body("ab", &a)).unwrap();
        assert_eq!(v["txid"], "ab");
        assert_eq!(v["present"], true);
        assert_eq!(v["confirmed"], true);
        assert_eq!(v["height"], 1);
        assert_eq!(v["rawHex"], "aa");
        assert_eq!(v["source"], "index");
        let empty: serde_json::Value =
            serde_json::from_str(&tx_any_body("ab", &TxAnyAnswer::default())).unwrap();
        assert!(empty["present"].is_null());
        assert!(empty["source"].is_null());
    }
}
