//! `GET /tx-any/:txid` — tx-level presence / confirmation / raw bytes for
//! ARBITRARY txids, honoring the READ HIERARCHY (owner doctrine, bsv-low
//! #229, 2026-07-22):
//!
//!   1. **Index-native leg (system of record for BYTES, not for network
//!      presence).** Every tx LOW ever broadcast is admitted to the overlay
//!      and its BEEF is stored durably (`pot_beefs` / `transactions`). If the
//!      stored BEEF carries a chaintracks-verified BUMP (stitched by the
//!      completion pass / arc-ingest merkle push), the tx is PROVEN mined —
//!      presence, raw bytes, confirmation, and height all answer from the
//!      index, zero external reads. A stored BEEF WITHOUT a BUMP proves only
//!      that we HOLD the bytes (bsv-low #247): the broadcast gate has had
//!      holes (#267/#268) and the sibling admission modes
//!      (historical-tx / GASP sync / peer crawl) are ungated by design, so
//!      the PRESENCE question falls through to the external leg alongside
//!      the confirmation question — the raw is still served either way.
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
pub const KNOWN_MINED_TXID: &str =
    "f358a4dd67c9d7b3a295d05d7a23abc0b85ba1f95c8afa756f1f466419be5e1c";

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
    /// `Some(true)` = provably NETWORK-present (index BEEF with a verified
    /// BUMP, or an external indexer's hash-verified positive);
    /// `Some(false)` = provably absent (corroborated double-404);
    /// `None` = unknown.
    ///
    /// bsv-low #247: own-store bytes WITHOUT a BUMP no longer assert
    /// presence on their own. Admission is broadcast-gated on the money
    /// path, but (a) the gate had holes (#267 degraded-Arcade false-SEEN,
    /// #268 fake-bump efs==0 — both since closed) and (b) the sibling
    /// admission modes (historical-tx / GASP / peer-crawl) are ungated by
    /// design — so "we hold the bytes" is NOT "the network saw it", and the
    /// client treats `present` as network truth (a zombie orphan JOIN served
    /// present:true kept its bounded rebroadcasts alive forever).
    pub present: Option<bool>,
    /// `Some(true)` = proven/claimed mined (index BUMP, or WoC
    /// confirmations≥1); `Some(false)` = present but not yet confirmed per
    /// the external leg; `None` = unknown.
    pub confirmed: Option<bool>,
    /// The mined block height per the stored BEEF's verified BUMP (index leg
    /// only — the external leg never claims a height).
    pub height: Option<u64>,
    /// The raw tx bytes as lowercase hex — index-extracted or externally
    /// hash-verified. Never an unverified byte. Still served when the
    /// network verdict is absent/unknown (they are the caller's own admitted
    /// bytes — e.g. for a rebroadcast).
    pub raw_hex: Option<String>,
    /// Which leg answered: `"index"` / `"index+external"` / `"external"`.
    pub source: Option<&'static str>,
    /// bsv-low #247: `true` = PROVABLY UNCONFIRMABLE — an input of this tx
    /// is spent by a DIFFERENT, CONFIRMED tx, so this tx can never land.
    /// A terminal skip signal the client may consume to stop bounded
    /// rebroadcasts. Only ever set alongside `present: Some(false)` (the
    /// route probes inputs only for a corroborated-absent index-held tx);
    /// `false` means "not proven unconfirmable", never "confirmable".
    pub unconfirmable: bool,
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
        // Index-native ONLY with a verified BUMP: a chaintracks-verified
        // merkle path is the strongest network truth there is.
        if let Some(h) = index_height {
            return TxAnyAnswer {
                present: Some(true),
                confirmed: Some(true),
                height: Some(h),
                raw_hex: Some(raw),
                source: Some("index"),
                unconfirmable: false,
            };
        }
        // Stored bytes WITHOUT a BUMP (#247): the store proves we HOLD the
        // bytes, not that the network ever saw them (see the `present` doc
        // — gate holes + deliberately ungated sibling admission modes), so
        // the PRESENCE question falls through to the external leg:
        //  - an external positive corroborates network presence
        //    (mempool `confirmed:false` or mined `confirmed:true`);
        //  - a CORROBORATED double-404 is an honest network-absent — the
        //    raw is still served (the caller's own bytes, rebroadcastable);
        //  - anything else is the honest unknown (`present: null`), raw
        //    still served. Fail-safe either way: the client's
        //    positive-anywhere-outranks-negatives read stays intact.
        return match external {
            Some(TxObservation::Present { confirmed, .. }) => TxAnyAnswer {
                present: Some(true),
                confirmed: Some(*confirmed),
                height: None,
                raw_hex: Some(raw),
                source: Some("index+external"),
                unconfirmable: false,
            },
            Some(TxObservation::Absent) if absence == AbsenceCorroboration::CorroboratedAbsent => {
                TxAnyAnswer {
                    present: Some(false),
                    confirmed: None,
                    height: None,
                    raw_hex: Some(raw),
                    source: Some("index+external"),
                    unconfirmable: false,
                }
            }
            _ => TxAnyAnswer {
                present: None,
                confirmed: None,
                height: None,
                raw_hex: Some(raw),
                source: Some("index"),
                unconfirmable: false,
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
                unconfirmable: false,
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
                unconfirmable: false,
            },
            AbsenceCorroboration::Unknown => TxAnyAnswer::default(),
        },
        Some(TxObservation::Fault) | None => TxAnyAnswer::default(),
    }
}

/// PURE (#247): does ONE input's spend observation prove the subject can
/// never confirm? True iff the input's outpoint is VERIFIED spent by a
/// DIFFERENT tx that is CONFIRMED — a confirmed conflicting spend is
/// permanent (absent a reorg), so the subject is provably unconfirmable.
/// Everything else (unknown, unspent, spent by the subject itself, spent
/// unconfirmed) proves nothing — fail toward `false` (keep retrying),
/// never toward a fabricated terminal verdict.
pub fn input_proves_unconfirmable(
    subject_txid: &str,
    known: bool,
    spent: Option<bool>,
    spending_txid: Option<&str>,
    spent_confirmed: Option<bool>,
) -> bool {
    known
        && spent == Some(true)
        && spent_confirmed == Some(true)
        && spending_txid.is_some_and(|s| !s.eq_ignore_ascii_case(subject_txid))
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

/// The `/tx-any` wire body. `unconfirmable` is additive (#247) — pre-#247
/// clients ignore it; a client that consumes it gets the terminal-skip
/// signal for a provably-dead tx.
pub fn tx_any_body(txid: &str, a: &TxAnyAnswer) -> String {
    json!({
        "txid": txid,
        "present": a.present,
        "confirmed": a.confirmed,
        "height": a.height,
        "rawHex": a.raw_hex,
        "source": a.source,
        "unconfirmable": a.unconfirmable,
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
    fn index_leg_without_bump_defers_presence_to_the_external_leg() {
        // bsv-low #247: own-store bytes with no BUMP are not network truth.
        // An external positive corroborates presence (and confirmation).
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

        // External present-but-unconfirmed still corroborates PRESENCE
        // (mempool) — confirmed honestly false.
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Present {
                confirmed: false,
                raw_hex: None,
            }),
            AbsenceCorroboration::Unknown,
        );
        assert_eq!((a.present, a.confirmed), (Some(true), Some(false)));
        assert_eq!(a.source, Some("index+external"));

        // THE #247 fix: a CORROBORATED double-404 for an index-held,
        // bump-less tx is an honest network-absent (the zombie orphan JOIN
        // class) — present:false, raw still served (the caller's own bytes).
        let a = decide_tx_any(
            Some(raw()),
            None,
            Some(&TxObservation::Absent),
            AbsenceCorroboration::CorroboratedAbsent,
        );
        assert_eq!((a.present, a.confirmed), (Some(false), None));
        assert_eq!(a.raw_hex, Some(raw()));
        assert_eq!(a.source, Some("index+external"));

        // An UNCORROBORATED 404 / a fault is the honest unknown — never a
        // negative on one provider's word, and no longer a fabricated
        // positive from our own store either.
        for (external, absence) in [
            (TxObservation::Absent, AbsenceCorroboration::Unknown),
            (TxObservation::Fault, AbsenceCorroboration::Unknown),
            (
                TxObservation::Fault,
                AbsenceCorroboration::CorroboratedAbsent,
            ),
        ] {
            let a = decide_tx_any(Some(raw()), None, Some(&external), absence);
            assert_eq!((a.present, a.confirmed), (None, None), "{external:?}");
            assert_eq!(a.raw_hex, Some(raw()), "raw is still served");
            assert_eq!(a.source, Some("index"));
        }
    }

    #[test]
    fn unconfirmable_requires_a_confirmed_conflicting_spender() {
        // Provably unconfirmable: an input spent by a DIFFERENT confirmed tx.
        let subject = "aa".repeat(32);
        let other = "bb".repeat(32);
        assert!(input_proves_unconfirmable(
            &subject,
            true,
            Some(true),
            Some(&other),
            Some(true)
        ));
        // Everything weaker proves NOTHING (fail toward retry):
        // spent by the subject itself (i.e. the subject IS the spender),
        for (known, spent, spender, conf) in [
            (true, Some(true), Some(subject.as_str()), Some(true)), // self-spend
            (true, Some(true), Some(other.as_str()), Some(false)),  // unconfirmed conflict
            (true, Some(true), Some(other.as_str()), None),         // confirmation unknown
            (true, Some(false), None, None),                        // unspent
            (false, Some(true), Some(other.as_str()), Some(true)),  // unverified read
            (true, None, Some(other.as_str()), Some(true)),         // spend unknown
        ] {
            assert!(
                !input_proves_unconfirmable(&subject, known, spent, spender, conf),
                "({known},{spent:?},{spender:?},{conf:?}) must not prove unconfirmable"
            );
        }
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
            unconfirmable: false,
        };
        let v: serde_json::Value = serde_json::from_str(&tx_any_body("ab", &a)).unwrap();
        assert_eq!(v["txid"], "ab");
        assert_eq!(v["present"], true);
        assert_eq!(v["confirmed"], true);
        assert_eq!(v["height"], 1);
        assert_eq!(v["rawHex"], "aa");
        assert_eq!(v["source"], "index");
        assert_eq!(v["unconfirmable"], false);
        let empty: serde_json::Value =
            serde_json::from_str(&tx_any_body("ab", &TxAnyAnswer::default())).unwrap();
        assert!(empty["present"].is_null());
        assert!(empty["source"].is_null());
    }
}
