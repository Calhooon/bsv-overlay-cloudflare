//! bsv-low #911 (lane D2-ACTOR-A, `DESIGN-D2-ACTOR-2026-10-26.md` section 3): THE OWED EVENT, the one message shape
//! of the `owed-events` queue. Both Workers link this crate: the overlay and the app layer PUBLISH it beside the
//! `owed_dirty` mark their write's trigger made, and the app layer's consumer DECODES it and routes it through those
//! marks to the judge. One spelling of the shape, pinned byte for byte by `vectors/owed_event_v1.json` (read by this
//! crate's tests and by both Workers' tests).
//!
//! The event is keyed on the SOURCE ROW, never on an identity (the audit's HIGH-1): `source` and `sourceKey` are
//! exactly the pair the 29 triggers of `overlay-cloudflare::derived_owed` write into `owed_dirty` (the trigger knows
//! the fan-out to identities, in SQL; the writer holds the row). The consumer reads the marks of that pair, judges
//! their subjects and acknowledges them after the judge wrote.
//!
//! `seq` is DATABASE-MONOTONIC (MEDIUM-1): one counter row of the shared D1 (`owed_event_seq`), bumped by the
//! publish's own D1 batch, in the SAME transaction as the read of the row's facts. So, per row, a larger `seq` always
//! carries facts at least as new (a later publish reads the row after every write committed before it): the
//! per-row last-writer-by-`seq` rule of the actor (lane B) holds whichever writer publishes first. Never a rowid.
//! `facts` are the row's own columns read in that batch (no bytes, MEDIUM-3), or, for a `word` event, the tracker
//! frame the writer holds (the consumer ingests it), or, for a `txfacts` event, the transaction decoded once at the
//! write ([`txfacts_of_raw`]).

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The shape's version.
pub const OWED_EVENT_V: u32 = 1;

/// Every `owed_dirty.source` a trigger writes, with the table whose write marks it (the 29 triggers of
/// `overlay-cloudflare::derived_owed_triggers`, read at test time by the overlay's pin, never a hand-fed list there).
/// A writer publishes `(source, key)` for exactly the pair its row's trigger marks.
pub const TRIGGER_SOURCES: [(&str, &str); 15] = [
    ("pot_records", "pot"),
    ("potparty_records", "party"),
    ("hopparty_records", "hop"),
    ("potrefund_records", "refund"),
    ("result_markers_v2", "result"),
    ("collected_markers_v2", "collected"),
    ("hopsweep_records", "sweep"),
    ("pot_evictions", "eviction"),
    ("network_seen", "seen"),
    ("tx_words", "word"),
    ("pot_beefs", "beef"),
    ("submit_refusals", "refusal"),
    ("released_hop_evidence", "hop"),
    ("released_hop_evidence_missing", "hop"),
    ("owed_spender_bytes", "word"),
];

/// The `owed_dirty` sources no table trigger writes: the read's wanted envelope and its expansion, the age repair,
/// the backfill and the tip's gate and untipped marks. The un-acked sweep re-publishes their marks like any other.
pub const OWN_SOURCES: [&str; 4] = ["wanted", "age", "backfill", "tip"];

/// A routing kind that is NOT a trigger pair: an identity-only note (the `/internal/hop-changed` hook carries
/// identities, not rows). The consumer routes it by the identity's own marks, oldest first, bounded.
pub const ROUTE_IDENTITY: &str = "identity";
/// A transaction's decoded facts (MEDIUM-3), keyed by txid; no `owed_dirty` mark is ever written for it. The
/// consumer of lane A acknowledges it untouched (the actor of lane B mirrors it).
pub const TXFACTS: &str = "txfacts";

/// The most `(identity, game)` subjects one message is judged for; the consumer re-publishes a continuation for the
/// rest (the design's section 4: "a message naming more is split by the consumer").
pub const MAX_GAMES_PER_MESSAGE: usize = 64;
/// The most messages one `sendBatch` carries (the platform's bound is 100 messages and 256 KB a batch).
pub const SEND_BATCH_MAX: usize = 100;
/// The most events one publish batch takes from a writer's notes (the rest stay noted for its next publish, or are
/// swept from their marks): a request notes a handful, a tick pass at most a few hundred.
pub const PUBLISH_MAX: usize = 256;

/// One queue message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwedEvent {
    /// [`OWED_EVENT_V`].
    pub v: u32,
    /// The publish's token from the shared D1 counter, monotonic across the database.
    pub seq: i64,
    /// The trigger's `owed_dirty.source` (or [`ROUTE_IDENTITY`], [`TXFACTS`]).
    pub source: String,
    /// The trigger's `owed_dirty.sourceKey` (`txid:vout`, `txid`, an identity, a constant key).
    #[serde(rename = "sourceKey")]
    pub source_key: String,
    /// The row's own facts at `seq` (see the module doc).
    pub facts: Value,
    /// When the writer published, ms since the epoch.
    #[serde(rename = "atMs")]
    pub at_ms: i64,
}

impl OwedEvent {
    /// The message body: the canonical JSON (serde's field order, the vector's bytes).
    pub fn encode(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Read one message body. A body of another version, or one that does not read, is refused.
    pub fn decode(body: &str) -> Result<Self, String> {
        let event: OwedEvent =
            serde_json::from_str(body).map_err(|e| format!("owed event: {e}"))?;
        if event.v != OWED_EVENT_V {
            return Err(format!(
                "owed event: version {} is not {OWED_EVENT_V}",
                event.v
            ));
        }
        if event.source.is_empty() || event.source_key.is_empty() {
            return Err("owed event: empty source or sourceKey".into());
        }
        Ok(event)
    }

    /// The tracker frame a `word` event carries (`{txid, kind, body}`), if any.
    pub fn frame(&self) -> Option<&Value> {
        (self.source == "word")
            .then(|| self.facts.get("frame"))
            .flatten()
            .filter(|f| f.is_object())
    }
}

/// `true` for a pair a trigger can write (a source named by [`TRIGGER_SOURCES`] or [`OWN_SOURCES`]).
pub fn routable_source(source: &str) -> bool {
    TRIGGER_SOURCES.iter().any(|(_, s)| *s == source) || OWN_SOURCES.contains(&source)
}

/// The key of a `txid:vout` source (`pot`, `hop`, `refusal`), lowercased as the stored rows are.
pub fn outpoint_key(txid: &str, vout: u32) -> String {
    format!("{}:{vout}", txid.to_ascii_lowercase())
}

/// Split a `txid:vout` key; a bare key answers `(key, None)`.
pub fn split_key(key: &str) -> (&str, Option<u32>) {
    match key.rsplit_once(':') {
        Some((txid, vout)) => match vout.parse::<u32>() {
            Ok(v) => (txid, Some(v)),
            Err(_) => (key, None),
        },
        None => (key, None),
    }
}

/// MEDIUM-3: a transaction's facts, decoded ONCE at the write from its raw bytes, as the app layer's hydrate reads
/// them from the bytes today (`routes::spender_outputs_of`, `spender_inputs_of`): its inputs (`txid:vout`), its
/// outputs (`vout`, `sats`, `kind` `p2pkh` | `pot` | `other`, `pkh` for a P2PKH output, `scriptSha256` for any other)
/// and, for a pot covenant output, the keys and pay hashes it commits. `None` for bytes that do not parse.
pub fn txfacts_of_raw(raw: &[u8]) -> Option<Value> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    let inputs: Vec<String> = tx
        .inputs
        .iter()
        .filter_map(|i| {
            i.source_txid
                .as_deref()
                .map(|t| outpoint_key(t, i.source_output_index))
        })
        .collect();
    let outputs: Vec<Value> = tx
        .outputs
        .iter()
        .enumerate()
        .map(|(vout, o)| {
            let lock = o.locking_script.to_binary();
            let sats = o.satoshis.unwrap_or(0);
            if lock.len() == 25
                && lock[..3] == [0x76, 0xa9, 0x14]
                && lock[23..] == [0x88, 0xac]
            {
                json!({"vout": vout, "sats": sats, "kind": "p2pkh", "pkh": hex::encode(&lock[3..23])})
            } else if crate::pot::is_pot_covenant_script(&lock) {
                let committed = crate::pot::covenant::extract_covenant_params(&lock);
                json!({
                    "vout": vout, "sats": sats, "kind": "pot",
                    "scriptSha256": hex::encode(bsv_rs::primitives::hash::sha256(&lock)),
                    "committedKeys": committed.as_ref().map(|p| vec![hex::encode(p.pub_a), hex::encode(p.pub_b)]),
                    "payHashes": committed.as_ref().map(|p| vec![hex::encode(p.pay_pkh_a), hex::encode(p.pay_pkh_b)]),
                })
            } else {
                json!({"vout": vout, "sats": sats, "kind": "other", "scriptSha256": hex::encode(bsv_rs::primitives::hash::sha256(&lock))})
            }
        })
        .collect();
    Some(json!({"txid": tx.id(), "inputs": inputs, "outputs": outputs}))
}

/// The raw bytes of `txid` as its (Atomic)BEEF carries them (the subject a store writes); `None` when the BEEF does
/// not parse or does not carry it.
pub fn subject_raw_of_beef(beef: &[u8], txid: &str) -> Option<Vec<u8>> {
    let beef = overlay_engine::beef_limits::parse_beef(
        beef,
        &overlay_engine::beef_limits::DISCOVERY_BEEF_LIMITS,
    )
    .ok()?;
    let subject = beef.find_txid(txid)?;
    subject
        .raw_tx()
        .map(<[u8]>::to_vec)
        .or_else(|| subject.tx().map(|t| t.to_binary()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const VECTOR: &str = include_str!("../vectors/owed_event_v1.json");

    /// (f) THE SHAPE: every vector case encodes to its frozen body byte for byte and decodes back to itself; the
    /// refused bodies are refused. Both Workers' tests read the same file.
    #[test]
    fn d2_actor_a_f_the_event_shape_is_the_vector() {
        let vector: Value = serde_json::from_str(VECTOR).unwrap();
        let cases = vector["cases"].as_array().unwrap();
        assert!(cases.len() >= 4);
        for case in cases {
            let event: OwedEvent = serde_json::from_value(case["event"].clone()).unwrap();
            let body = case["body"].as_str().unwrap();
            assert_eq!(event.encode(), body, "{}", case["name"]);
            assert_eq!(OwedEvent::decode(body).unwrap(), event, "{}", case["name"]);
        }
        for bad in vector["refused"].as_array().unwrap() {
            assert!(OwedEvent::decode(bad.as_str().unwrap()).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_word_frame_rides_only_a_word_event() {
        let mut e = OwedEvent {
            v: 1,
            seq: 1,
            source: "word".into(),
            source_key: "ab".into(),
            facts: json!({"frame": {"txid": "ab", "kind": "seen", "body": null}}),
            at_ms: 0,
        };
        assert!(e.frame().is_some());
        e.source = "pot".into();
        assert!(e.frame().is_none());
        assert_eq!(split_key("ab:3"), ("ab", Some(3)));
        assert_eq!(split_key("ab"), ("ab", None));
        assert_eq!(outpoint_key("AB", 3), "ab:3");
        assert!(routable_source("pot") && routable_source("tip") && !routable_source("txfacts"));
    }

    #[test]
    fn txfacts_reads_inputs_and_outputs_once() {
        // A one-input, two-output transaction: a P2PKH and an OP_RETURN.
        let mut raw = vec![1, 0, 0, 0, 1];
        raw.extend([0x11; 32]);
        raw.extend([2, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 2]);
        raw.extend(1000u64.to_le_bytes());
        raw.push(25);
        raw.extend([0x76, 0xa9, 0x14]);
        raw.extend([0x22; 20]);
        raw.extend([0x88, 0xac]);
        raw.extend(0u64.to_le_bytes());
        raw.extend([2, 0x6a, 0x00]);
        raw.extend([0, 0, 0, 0]);
        let facts = txfacts_of_raw(&raw).expect("parses");
        assert_eq!(facts["inputs"], json!([format!("{}:2", "11".repeat(32))]));
        assert_eq!(facts["outputs"][0]["kind"], "p2pkh");
        assert_eq!(facts["outputs"][0]["pkh"], "22".repeat(20));
        assert_eq!(facts["outputs"][0]["sats"], 1000);
        assert_eq!(facts["outputs"][1]["kind"], "other");
        assert!(txfacts_of_raw(&[0, 1]).is_none());
    }
}
