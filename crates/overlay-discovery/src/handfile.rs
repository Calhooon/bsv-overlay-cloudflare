//! bsv-low #848 (FAST-C, #461 design C): the served `batch` of a row the app layer's `handfile` family exploded.
//!
//! The app layer's `POST /record?kind=handfile` (its `handfile.rs`) verifies ONE identity signature and ONE
//! settle-key signature over four canonical payloads (hopparty, potparty v1, potparty v2, potrefund) and writes
//! each payload as a row of its family's table; the batch itself rests in `handfile_batches`, each row's place in
//! `handfile_rows`. A reader that serves such a row may carry the batch beside it, the ONE additive wire delta, so
//! the client verifies the row's inclusion (its payload at `index` is the row's canonical bytes) and the two
//! signatures over the digest. A row filed directly or admitted from the chain carries no `batch`: its bytes are
//! unchanged.
//!
//! One type and one JSON for every reader (the overlay's lookup services, the app layer's views).
use serde::{Deserialize, Serialize};

/// The payloads a batch carries, in order: hopparty, potparty v1, potparty v2, potrefund.
pub const HANDFILE_PAYLOADS: usize = 4;

/// A row's batch as served: `{key, hopTxid, payloads, sigs: {identity, settle}, index}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandfileBatch {
    /// `handfile:<sha256 of the preimage>`.
    pub key: String,
    /// The funding hop's txid, the first thing the preimage binds after its domain tag.
    #[serde(rename = "hopTxid")]
    pub hop_txid: String,
    /// The four canonical payloads, hex, in order.
    pub payloads: Vec<String>,
    /// The identity's signature under `[1,'low potparty']` (keyID the gameId), hex DER.
    #[serde(rename = "identitySigHex")]
    pub identity_sig_hex: String,
    /// The seat's settle key's signature, hex DER.
    #[serde(rename = "settleSigHex")]
    pub settle_sig_hex: String,
    /// The served row's place among the payloads.
    pub index: u32,
}

impl HandfileBatch {
    /// The wire shape (`docs/HANDFILE-WIRE.md`).
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "key": self.key,
            "hopTxid": self.hop_txid,
            "payloads": self.payloads,
            "sigs": { "identity": self.identity_sig_hex, "settle": self.settle_sig_hex },
            "index": self.index,
        })
    }

    /// A stored batch row's `payloadsHex` column (a JSON array of four hex strings) and the row's place, as a
    /// reader decodes them; `None` when the column does not read (the row is served without its batch, never with
    /// a wrong one).
    pub fn of_stored(
        key: &str,
        hop_txid: &str,
        payloads_json: &str,
        identity_sig_hex: &str,
        settle_sig_hex: &str,
        index: u32,
    ) -> Option<Self> {
        let payloads: Vec<String> = serde_json::from_str(payloads_json).ok()?;
        if payloads.len() != HANDFILE_PAYLOADS {
            return None;
        }
        Some(Self {
            key: key.to_string(),
            hop_txid: hop_txid.to_string(),
            payloads,
            identity_sig_hex: identity_sig_hex.to_string(),
            settle_sig_hex: settle_sig_hex.to_string(),
            index,
        })
    }
}

/// The family name a `handfile_rows` row carries for a `hopparty_records` row.
pub const HANDFILE_FAMILY_HOPPARTY: &str = "hopparty";
/// The family name a `handfile_rows` row carries for a `potparty_records` row (v1 and v2 alike).
pub const HANDFILE_FAMILY_POTPARTY: &str = "potparty";
/// The family name a `handfile_rows` row carries for a `potrefund_records` row.
pub const HANDFILE_FAMILY_POTREFUND: &str = "potrefund";

/// Is this row key a door-filed one (a `filed:` content key, the app layer's `/record`)? Only such a potparty or
/// potrefund row can carry a batch; a hopparty row carries one at a handfile vout
/// (`crate::hopparty::is_handfile_marker_vout`).
pub fn is_filed_key(txid: &str) -> bool {
    txid.starts_with("filed:")
}

/// A served row's batch, attached when the reader found one (`serde_json::Value::Object` rows only).
pub fn attach_batch(row: &mut serde_json::Value, batch: Option<&HandfileBatch>) {
    if let (Some(b), Some(obj)) = (batch, row.as_object_mut()) {
        obj.insert("batch".to_string(), b.json());
    }
}

/// The key of a served potparty or potrefund row that MAY carry a batch (a door-filed `filed:` key), else `None`.
pub fn filed_row_key(row: &serde_json::Value) -> Option<(String, u32)> {
    let txid = row.get("txid")?.as_str()?;
    if !is_filed_key(txid) {
        return None;
    }
    let vout = row.get("outputIndex")?.as_u64()?;
    Some((txid.to_string(), u32::try_from(vout).ok()?))
}

/// The key of a served hopparty row that MAY carry a batch (a handfile vout), else `None`.
pub fn handfile_hop_row_key(row: &serde_json::Value) -> Option<(String, u32)> {
    let txid = row.get("txid")?.as_str()?;
    let vout = u32::try_from(row.get("outputIndex")?.as_u64()?).ok()?;
    crate::hopparty::is_handfile_marker_vout(vout).then(|| (txid.to_string(), vout))
}

/// Attach each served row's batch: the rows `key_of` names are asked of `read` ONCE (none asked when no row
/// qualifies), and a row whose batch the read did not answer is served as it was.
pub async fn attach_batches<F, Fut>(
    mut rows: Vec<serde_json::Value>,
    key_of: fn(&serde_json::Value) -> Option<(String, u32)>,
    read: F,
) -> Vec<serde_json::Value>
where
    F: FnOnce(Vec<(String, u32)>) -> Fut,
    Fut: core::future::Future<Output = Option<Vec<((String, u32), HandfileBatch)>>>,
{
    let keys: Vec<(String, u32)> = rows.iter().filter_map(key_of).collect();
    if keys.is_empty() {
        return rows;
    }
    let Some(found) = read(keys).await else {
        return rows;
    };
    for row in rows.iter_mut() {
        if let Some(key) = key_of(row) {
            if let Some((_, b)) = found.iter().find(|(k, _)| *k == key) {
                attach_batch(row, Some(b));
            }
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a future that never pends to its end (the attach helper awaits only the caller's read).
    fn block_on<F: core::future::Future>(f: F) -> F::Output {
        use std::task::{Context, Poll, Waker};
        let mut cx = Context::from_waker(Waker::noop());
        let mut f = std::pin::pin!(f);
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    #[test]
    fn the_batch_json_is_the_wire_shape_and_a_row_without_one_is_untouched() {
        let b = HandfileBatch::of_stored(
            "handfile:ab",
            "77",
            "[\"00\",\"01\",\"02\",\"03\"]",
            "30",
            "31",
            2,
        )
        .unwrap();
        assert_eq!(
            b.json(),
            serde_json::json!({"key": "handfile:ab", "hopTxid": "77", "payloads": ["00","01","02","03"], "sigs": {"identity": "30", "settle": "31"}, "index": 2})
        );
        assert!(HandfileBatch::of_stored("k", "h", "[\"00\"]", "a", "b", 0).is_none());
        assert!(HandfileBatch::of_stored("k", "h", "nope", "a", "b", 0).is_none());
        let mut row = serde_json::json!({"txid": "filed:x"});
        let before = row.to_string();
        attach_batch(&mut row, None);
        assert_eq!(
            row.to_string(),
            before,
            "no batch: the row's bytes are unchanged"
        );
        attach_batch(&mut row, Some(&b));
        assert_eq!(row["batch"]["index"], 2);
        assert!(is_filed_key("filed:ab") && !is_filed_key(&"ab".repeat(32)));
    }

    /// The served rows: only a door-filed row is asked for and only an answered one gains `batch`; a chain row
    /// and an unanswered filed row are byte for byte what they were, and nothing is read when no row qualifies.
    #[test]
    fn attach_batches_asks_once_for_the_filed_rows_and_leaves_every_other_row_alone() {
        let rows = vec![
            serde_json::json!({"txid": "ab".repeat(32), "outputIndex": 1}),
            serde_json::json!({"txid": "filed:one", "outputIndex": 0}),
            serde_json::json!({"txid": "filed:two", "outputIndex": 0}),
        ];
        let before: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
        let b = HandfileBatch::of_stored(
            "handfile:k",
            "77",
            "[\"00\",\"01\",\"02\",\"03\"]",
            "30",
            "31",
            3,
        )
        .unwrap();
        let asked = std::cell::RefCell::new(Vec::new());
        let out = block_on(attach_batches(rows.clone(), filed_row_key, |keys| {
            asked.borrow_mut().extend(keys.clone());
            let b = b.clone();
            async move { Some(vec![(("filed:one".to_string(), 0), b)]) }
        }));
        assert_eq!(
            asked.borrow().as_slice(),
            &[("filed:one".to_string(), 0), ("filed:two".to_string(), 0)],
            "the chain row is never asked for"
        );
        assert_eq!(out[0].to_string(), before[0]);
        assert_eq!(
            out[2].to_string(),
            before[2],
            "an unanswered filed row is unchanged"
        );
        assert_eq!(out[1]["batch"]["index"], 3);
        // no row qualifies: the read is never made
        let chain_only = vec![rows[0].clone()];
        let out = block_on(attach_batches(
            chain_only.clone(),
            filed_row_key,
            |_| async { panic!("asked with no filed row") },
        ));
        assert_eq!(out, chain_only);
        // a faulted read: every row as it was
        let out = block_on(attach_batches(rows.clone(), filed_row_key, |_| async {
            None
        }));
        assert_eq!(out, rows);
        // the hopparty key: a handfile vout only
        assert!(
            handfile_hop_row_key(&serde_json::json!({"txid": "ab", "outputIndex": 1})).is_none()
        );
        assert_eq!(
            handfile_hop_row_key(&serde_json::json!({"txid": "ab", "outputIndex": 2147483648u64})),
            Some(("ab".to_string(), 2147483648))
        );
    }
}
