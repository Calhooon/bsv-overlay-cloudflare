//! BEEF → Extended Format (BRC-30) conversion for the broadcast-gated submit
//! path (bsv-low overlay-first broadcast, 2026-07-17).
//!
//! Ported from zanaadu `overlay/src/ef.rs` (itself from teragunv2
//! `gun/src/utils/beef.rs::beef_to_ef_batch`). ARC cannot look up spent parent
//! outputs for a bare raw tx whose parents are unconfirmed, so every
//! *unproven* (unmined) transaction in a BEEF is broadcast as its own
//! Extended Format binary, in dependency order (ARC dedupes re-submitted
//! ancestors for free).
//!
//! Source satoshis + locking scripts come from the BEEF's own ancestry:
//! `Transaction::to_ef()` requires each input's `source_transaction` to be
//! linked, and the bsv-rs BEEF parser does NOT link them, so we build a
//! txid→tx map from the BEEF and wire the sources one level deep ourselves
//! (the map's transactions are themselves flat/unlinked, so the clones stay
//! shallow — exactly enough for `to_ef`).

use std::collections::HashMap;

use bsv_rs::transaction::{Beef, Transaction};
use thiserror::Error;

/// Error converting a BEEF into Extended-Format binaries.
#[derive(Debug, Error)]
pub enum EfError {
    /// The BEEF bytes could not be parsed.
    #[error("BEEF parse error: {0}")]
    Parse(String),
    /// A transaction inside the BEEF could not be encoded as EF (missing
    /// source data, missing txid-only ancestor, or a `to_ef` failure).
    #[error("EF conversion failed: {0}")]
    EfConversion(String),
}

/// Convert BEEF bytes into Extended Format (BRC-30) binaries for ARC.
///
/// # Returns
/// `(efs, subject_txid)` — EF binaries for every **unproven** transaction in
/// dependency order, plus the txid of the BEEF's subject (last) transaction.
/// `efs` is empty when every transaction already carries a merkle proof
/// (already mined → nothing to broadcast; the caller treats this as a no-op
/// success and admits directly, matching the engine's "skip ARC when the tx
/// already has a merkle path" behaviour).
pub fn beef_to_ef_batch(beef_bytes: &[u8]) -> Result<(Vec<Vec<u8>>, String), EfError> {
    let mut beef = Beef::from_binary(beef_bytes).map_err(|e| EfError::Parse(e.to_string()))?;
    beef.sort_txs();

    // txid → parsed transaction, for linking input sources one level deep.
    let mut tx_map: HashMap<String, Transaction> = HashMap::with_capacity(beef.txs.len());
    for btx in &beef.txs {
        if let Some(tx) = btx.tx() {
            tx_map.insert(btx.txid(), tx.clone());
        }
    }

    let mut efs = Vec::new();
    let mut subject_txid = String::new();

    for btx in &beef.txs {
        let txid = btx.txid();
        subject_txid = txid.clone();

        if btx.has_proof() {
            // Already mined — provides source data for children, nothing to broadcast.
            continue;
        }

        let tx = btx.tx().ok_or_else(|| {
            EfError::EfConversion(format!("txid-only entry {txid} has no transaction data"))
        })?;

        let mut tx = tx.clone();
        for input in &mut tx.inputs {
            if input.source_transaction.is_some() {
                continue;
            }
            let src_txid = input.source_txid.clone().ok_or_else(|| {
                EfError::EfConversion(format!("input in {txid} has no source txid"))
            })?;
            let src = tx_map.get(&src_txid).ok_or_else(|| {
                EfError::EfConversion(format!(
                    "source tx {src_txid} for {txid} not present in BEEF"
                ))
            })?;
            input.source_transaction = Some(Box::new(src.clone()));
        }

        let ef = tx
            .to_ef()
            .map_err(|e| EfError::EfConversion(format!("{txid}: {e}")))?;
        efs.push(ef);
    }

    Ok((efs, subject_txid))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;

    // Real mainnet transaction pair (subject + funding parent), committed raw
    // so the EF round-trip runs offline (fixtures shared with zanaadu's suite):
    //   subject e98cdd1fda72bed87fefd4f8436fbf64b668a0dceaf5e507d8aa84dcd5c1f03b
    //   parent  a7d76588b278b8eef89d6d21e0d29171db6a83b54ff935aed46ae555beb5e6df
    const SUBJECT_RAW_HEX: &str = include_str!("../tests/fixtures/ef/subject_e98cdd1f.rawhex");
    /// Parent's real BEEF (V1, carries the parent tx + its merkle proof) — in
    /// the composed BEEF the parent is MINED (skipped by the batch, only
    /// supplying source data), exactly like a real unmined-subject envelope.
    const PARENT_BEEF_HEX: &str = include_str!("../tests/fixtures/ef/parent_a7d76588_beef.hex");
    const SUBJECT_TXID: &str = "e98cdd1fda72bed87fefd4f8436fbf64b668a0dceaf5e507d8aa84dcd5c1f03b";

    /// Build a BEEF with a PROVEN parent + an UNMINED subject — the exact
    /// shape a freshly-signed tx rides to the network with.
    fn build_unmined_beef() -> Vec<u8> {
        let mut beef = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap();
        let subject = Transaction::from_hex(SUBJECT_RAW_HEX.trim()).unwrap();
        beef.merge_transaction(subject); // subject last, proofless
        beef.to_binary()
    }

    #[test]
    fn ef_batch_from_real_beef_has_marker_and_source_data() {
        let beef = build_unmined_beef();
        let (efs, subject_txid) =
            beef_to_ef_batch(&beef).expect("real BEEF must convert to EF");

        assert_eq!(efs.len(), 1, "only the unmined subject is broadcast as EF");
        assert_eq!(subject_txid, SUBJECT_TXID, "subject is the last (spending) tx");

        for ef in &efs {
            // BRC-30 marker: version (4 bytes LE) then `00 00 00 00 00 EF`.
            assert!(ef.len() > 10, "EF binary too short to hold the marker");
            assert_eq!(
                &ef[4..10],
                &[0x00, 0x00, 0x00, 0x00, 0x00, 0xEF],
                "EF marker 0000000000EF must follow the version"
            );

            // Round-trips back through the EF parser — proving each input
            // carries source satoshis + locking script.
            let hex_str = hex::encode(ef);
            let parsed = Transaction::from_hex_ef(&hex_str)
                .expect("emitted EF must parse back through from_hex_ef");
            assert!(!parsed.inputs.is_empty());
            for input in &parsed.inputs {
                let src = input
                    .source_transaction
                    .as_ref()
                    .expect("each EF input must carry its source transaction");
                let out = &src.outputs[input.source_output_index as usize];
                assert!(out.satoshis.unwrap_or(0) > 0, "EF source output must carry sats");
            }
        }
    }

    #[test]
    fn ef_batch_skips_proven_ancestors() {
        let beef = build_unmined_beef();
        let (efs, _subject_txid) = beef_to_ef_batch(&beef).unwrap();
        assert_eq!(efs.len(), 1, "proven ancestor is skipped");
    }

    #[test]
    fn ef_batch_rejects_garbage_beef() {
        assert!(matches!(beef_to_ef_batch(&[0xde, 0xad]), Err(EfError::Parse(_))));
    }
}
