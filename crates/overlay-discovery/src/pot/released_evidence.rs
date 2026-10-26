//! Check 3 of a released hop's evidence (bsv-low #893; D2-OWED-FOLD-8): the hop's output, read from the evicted
//! JOIN's own stored BEEF, is the standard P2PKH to hash160(K) of a seat key K. One function, called by the
//! overlay's eviction writer at an eviction and by the app layer's `owed-v2` backfill over an eviction made before
//! that writer existed, so both write the same evidence from the same bytes. Checks 1 and 2 (the identity's verified
//! twin party naming K, the twin lock committing K on exactly one seat) are the writer's statement, run on the rows.

use bsv_rs::transaction::Transaction;

/// One released hop whose output pays a candidate seat key (the evidence statement's binds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasedHopOutput {
    /// The hop's txid, lowercase.
    pub hop_txid: String,
    /// The hop's output index.
    pub hop_vout: u32,
    /// The seat key K the output pays (P2PKH to hash160(K)), lowercase hex.
    pub seat_key: String,
    /// The output's satoshis.
    pub sats: i64,
}

/// What the evicted JOIN's stored bytes say of its released hops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinEvidence {
    /// No stored bytes, or bytes that do not parse: no released hop can be judged (the could-not-judge row).
    Unreadable,
    /// The bytes parse. `found`: the hop outputs paying a candidate key. `unread`: the released hops whose
    /// transaction the bytes do not carry (absent, txid-only or short of the vout): no output to judge.
    Read {
        /// The hop outputs paying a candidate key.
        found: Vec<ReleasedHopOutput>,
        /// The released hops whose output the bytes do not carry.
        unread: Vec<(String, u32)>,
    },
}

/// `released`: the eviction's released `pot_records` outpoints. `seat_keys`: the verified seats' settle keys. The
/// JOIN's own inputs join the released hops and its covenant outputs' keys join the candidates (a pointer or party
/// row that landed after a snapshot); the statement still requires a released outpoint and a verified twin party.
pub fn released_hop_outputs(
    beef: Option<&[u8]>,
    join_txid: &str,
    released: &[(String, u32)],
    seat_keys: &[String],
) -> JoinEvidence {
    let Some(beef) = beef else {
        return JoinEvidence::Unreadable;
    };
    let Ok(mut beef) = overlay_engine::beef_limits::parse_beef(
        beef,
        &overlay_engine::beef_limits::STORED_BEEF_LIMITS,
    ) else {
        return JoinEvidence::Unreadable;
    };
    let mut seat_keys: std::collections::BTreeSet<_> = seat_keys.iter().cloned().collect();
    let mut hops: Vec<(String, u32)> = released
        .iter()
        .map(|(t, v)| (t.to_ascii_lowercase(), *v))
        .collect();
    if let Some(raw) = beef
        .find_txid_mut(&join_txid.to_ascii_lowercase())
        .and_then(|t| t.raw_tx_or_compute())
    {
        if let Ok(join) = Transaction::from_binary(&raw) {
            for input in &join.inputs {
                if let Ok(txid) = input.get_source_txid() {
                    let hop = (txid.to_ascii_lowercase(), input.source_output_index);
                    if !hops.contains(&hop) {
                        hops.push(hop);
                    }
                }
            }
            for output in &join.outputs {
                if let Some(keys) =
                    super::covenant::extract_covenant_params(&output.locking_script.to_binary())
                {
                    seat_keys.insert(hex::encode(keys.pub_a));
                    seat_keys.insert(hex::encode(keys.pub_b));
                }
            }
        }
    }
    let keys: Vec<_> = seat_keys
        .iter()
        .filter_map(|key| {
            let key = key.to_ascii_lowercase();
            let bytes = hex::decode(&key).ok()?;
            (bytes.len() == 33).then(|| {
                let lock = super::p2pkh_lock(&bsv_rs::primitives::hash::hash160(&bytes));
                (key, lock)
            })
        })
        .collect();
    let mut found = Vec::new();
    let mut unread = Vec::new();
    for (hop_txid, vout) in &hops {
        let output = beef
            .find_txid_mut(hop_txid)
            .and_then(|t| t.raw_tx_or_compute())
            .and_then(|raw| Transaction::from_binary(&raw).ok())
            .and_then(|tx| tx.outputs.get(*vout as usize).cloned());
        let Some(output) = output else {
            if released
                .iter()
                .any(|(t, v)| t.eq_ignore_ascii_case(hop_txid) && v == vout)
            {
                unread.push((hop_txid.clone(), *vout));
            }
            continue;
        };
        let Some(sats) = output.satoshis.and_then(|n| i64::try_from(n).ok()) else {
            continue;
        };
        let lock = output.locking_script.to_binary();
        for (key, key_lock) in &keys {
            if lock == *key_lock {
                found.push(ReleasedHopOutput {
                    hop_txid: hop_txid.clone(),
                    hop_vout: *vout,
                    seat_key: key.clone(),
                    sats,
                });
            }
        }
    }
    JoinEvidence::Read { found, unread }
}

/// One write of an eviction's evidence: an evidence row's binds, or the released hops the bytes cannot judge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceWrite {
    /// The evidence statement's binds `?2..?5` (the JOIN's txid is `?1`).
    Found(ReleasedHopOutput),
    /// The missing-evidence statement's binds: `?2` a JSON array of `{"txid","vout"}`, lowercase; `?3` whether the
    /// JOIN's bytes parsed (then each named hop's transaction is absent from them) or not (absent or undecodable).
    Missing {
        /// The released hops the bytes cannot judge.
        list: String,
        /// The bytes parsed.
        bytes_read: bool,
    },
}

/// The writes of one eviction, in order: every hop output paying a candidate key, then one missing-evidence write
/// naming the released hops the bytes cannot judge (all of them when the bytes are absent or undecodable). The
/// overlay's eviction writer and the app layer's `owed-v2` backfill each bind these to the same two statements.
pub fn evidence_writes(
    beef: Option<&[u8]>,
    join_txid: &str,
    released: &[(String, u32)],
    seat_keys: &[String],
) -> Vec<EvidenceWrite> {
    let mut out = Vec::new();
    let (unread, bytes_read) = match released_hop_outputs(beef, join_txid, released, seat_keys) {
        JoinEvidence::Unreadable => (released.to_vec(), false),
        JoinEvidence::Read { found, unread } => {
            out.extend(found.into_iter().map(EvidenceWrite::Found));
            (unread, true)
        }
    };
    if !unread.is_empty() {
        let list: Vec<_> = unread
            .iter()
            .map(|(t, v)| serde_json::json!({"txid": t.to_ascii_lowercase(), "vout": v}))
            .collect();
        out.push(EvidenceWrite::Missing {
            list: serde_json::Value::Array(list).to_string(),
            bytes_read,
        });
    }
    out
}
