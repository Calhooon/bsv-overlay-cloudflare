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

/// One broadcastable Extended-Format entry from a BEEF.
pub struct EfTx {
    pub txid: String,
    pub ef: Vec<u8>,
}

/// Convert BEEF bytes into Extended Format (BRC-30) binaries for ARC.
///
/// # Returns
/// The submitted BEEF's SUBJECT — the reference rule of `Transaction.fromBEEF`
/// (`txid ?? beef.atomicTxid ?? lastTx`, mirrored by bsv-rs `from_beef`) with
/// ONE hardening rung between the atomic name and the last-tx fallback: the
/// UNIQUE TIP, the one transaction no other transaction in the BEEF spends.
///
/// LOOP-2 FLEET FINDING (2026-09-05, pair-17 DEFINITIVE + the mini's refund
/// red — `docs/FLEET-LOOP-2026-09-05.md`): a JOIN whose ancestry lacked ONE
/// source (a hop's parent the wallet had not BEEF'd) is `notValid` to
/// `sort_txs`, which files it FIRST behind the with-missing-inputs group and
/// the fully-sourced HOP LAST — so "sorted last" named the HOP the subject,
/// the pre-flight probe found the hop already SEEN, `tm_pot` judged a hop (no
/// covenant output → admitted nothing) and the route answered 200: the pot
/// never entered the index while its JOIN mined (`5ad2764c…`, block 965500).
/// The SDK's `toBinary()` writes the same order, so the reference's `lastTx`
/// makes the same choice on an incomplete BEEF; only the atomic name and the
/// tip are ORDER-INDEPENDENT. Two tips (a malformed multi-subject body) fall
/// back to the reference's sorted-last so an old client is never refused for
/// a shape it always sent. `None` only for a BEEF with no transaction data.
pub fn subject_txid_of(beef: &mut Beef) -> Option<String> {
    if let Some(atomic) = beef.atomic_txid.clone() {
        if beef.txs.iter().any(|b| b.txid().eq_ignore_ascii_case(&atomic)) {
            return Some(atomic.to_ascii_lowercase());
        }
    }
    let mut spent: std::collections::HashSet<String> = std::collections::HashSet::new();
    for b in &beef.txs {
        if let Some(tx) = b.tx() {
            for input in &tx.inputs {
                let src = input
                    .source_txid
                    .clone()
                    .or_else(|| input.source_transaction.as_ref().map(|t| t.id()));
                if let Some(src) = src {
                    spent.insert(src.to_ascii_lowercase());
                }
            }
        }
    }
    let tips: Vec<String> = beef
        .txs
        .iter()
        .filter(|b| b.tx().is_some())
        .map(|b| b.txid().to_ascii_lowercase())
        .filter(|t| !spent.contains(t))
        .collect();
    if tips.len() == 1 {
        return tips.into_iter().next();
    }
    beef.sort_txs();
    beef.txs.last().filter(|b| b.tx().is_some()).map(|b| b.txid().to_ascii_lowercase())
}

/// The sorted-last txid — the pre-loop-2 subject rule, kept ONLY so callers
/// and pins can name the disagreement with [`subject_txid_of`] (a log line,
/// never a decision).
pub fn sorted_last_txid_of(beef: &Beef) -> Option<String> {
    let mut sorted = beef.clone();
    sorted.sort_txs();
    sorted.txs.last().map(|b| b.txid().to_ascii_lowercase())
}

/// `(efs, subject_txid)` — EF entries for **unproven** transactions in
/// dependency order, plus the txid of the BEEF's subject (last) transaction.
/// `efs` is empty when every transaction already carries a merkle proof
/// (already mined → nothing to broadcast; the caller treats this as a no-op
/// success and admits directly).
///
/// ANCESTOR conversion failures are SKIPPED, not fatal (adversarial review
/// 2026-07-17, finding 5): a caller's BEEF often carries an ancestor raw
/// without THAT ancestor's own parents (e.g. a recovery refund's BEEF carries
/// the JOIN but not the hops) — those ancestors were broadcast long ago by
/// construction, and if one truly never reached the network the SUBJECT's own
/// broadcast fails with missing-inputs, which is exactly the right signal.
/// Only the SUBJECT failing to convert is an error.
pub fn beef_to_ef_batch(beef_bytes: &[u8]) -> Result<(Vec<EfTx>, String), EfError> {
    let mut beef = Beef::from_binary(beef_bytes).map_err(|e| EfError::Parse(e.to_string()))?;
    // The subject is order-independent (atomic name → unique tip → the
    // reference's sorted-last); the sort below only orders the EF legs.
    let subject_txid = subject_txid_of(&mut beef).unwrap_or_default();
    beef.sort_txs();

    // txid → parsed transaction, for linking input sources one level deep.
    let mut tx_map: HashMap<String, Transaction> = HashMap::with_capacity(beef.txs.len());
    for btx in &beef.txs {
        if let Some(tx) = btx.tx() {
            tx_map.insert(btx.txid(), tx.clone());
        }
    }

    let mut efs = Vec::new();

    for btx in &beef.txs {
        let txid = btx.txid();

        if btx.has_proof() {
            // Already mined — provides source data for children, nothing to broadcast.
            continue;
        }

        let convert = || -> Result<Vec<u8>, EfError> {
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
            tx.to_ef()
                .map_err(|e| EfError::EfConversion(format!("{txid}: {e}")))
        };

        match convert() {
            Ok(ef) => efs.push(EfTx {
                txid: txid.clone(),
                ef,
            }),
            Err(e) if txid == subject_txid => return Err(e),
            Err(e) => {
                // Ancestor without its own sources — broadcast long ago by
                // construction; the subject's verdict is the arbiter.
                tracing::debug!("ef: skipping unconvertible ancestor {txid}: {e}");
            }
        }
    }

    Ok((efs, subject_txid))
}

/// PURE (loop-2 hardening, 2026-09-05 — F-D): the SUBJECT's input source
/// txids that the BEEF does NOT carry as full transactions (absent, or
/// txid-only entries). The reference engine admits a tx whose sources the
/// store lacks (`previousCoins` lists only the held inputs); our broadcast
/// gate needs every source's bytes for EF conversion, so a JOIN carrying a
/// txid-only hop was refused 400 while the hop was on the network — this is
/// the list the route completes from the courier ladder before converting.
pub fn missing_source_txids(beef_bytes: &[u8]) -> Vec<String> {
    let Ok(mut beef) = Beef::from_binary(beef_bytes) else {
        return Vec::new();
    };
    let subject_txid = subject_txid_of(&mut beef).unwrap_or_default();
    let held: std::collections::HashSet<String> = beef
        .txs
        .iter()
        .filter(|b| b.tx().is_some())
        .map(|b| b.txid())
        .collect();
    let Some(subject) = beef
        .txs
        .iter()
        .find(|b| b.txid().eq_ignore_ascii_case(&subject_txid))
        .and_then(|b| b.tx().cloned())
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for input in &subject.inputs {
        if input.source_transaction.is_some() {
            continue;
        }
        if let Some(src) = input.source_txid.clone() {
            if !held.contains(&src) && !out.contains(&src) {
                out.push(src);
            }
        }
    }
    out
}

/// PURE (F-D): merge courier-served raw transactions into the BEEF as
/// proofless ancestors, each VERIFIED by hash against the txid it was fetched
/// for (a courier byte that does not hash to its txid is dropped, never
/// merged). Returns the re-serialized BEEF; a BEEF that does not parse, or a
/// batch with nothing verified, comes back byte-identical.
pub fn merge_raw_sources(beef_bytes: &[u8], raws: &[(String, String)]) -> Vec<u8> {
    let Ok(mut beef) = Beef::from_binary(beef_bytes) else {
        return beef_bytes.to_vec();
    };
    let mut merged = 0usize;
    for (txid, raw_hex) in raws {
        let Ok(tx) = Transaction::from_hex(raw_hex) else {
            continue;
        };
        if !tx.id().to_string().eq_ignore_ascii_case(txid) {
            continue;
        }
        let Ok(raw) = hex::decode(raw_hex) else {
            continue;
        };
        beef.merge_raw_tx(raw, None);
        merged += 1;
    }
    if merged == 0 {
        return beef_bytes.to_vec();
    }
    beef.to_binary()
}

/// Strip the SUBJECT's (submitter-supplied, NEVER-validated) bump from a
/// BEEF before storage — bsv-low#268 gate finding M1.
///
/// The mined-claim corroboration proves NETWORK ACCEPTANCE (already-known /
/// SEEN on a second broadcaster), NOT SPV-mined-ness — yet the submitted
/// BEEF was previously stored VERBATIM, fake bump included. The read side
/// serves that bump with zero SPV (`low-app-layer` `beef_block_height` →
/// `/tx-any` answers `present:true confirmed:true height:<attacker-chosen>`
/// straight from the stored bytes), so an attacker-fabricated bump became a
/// served "confirmation" forever. Stripping the subject's bump makes the
/// stored row byte-equivalent to every honestly-submitted UNMINED tx: the
/// completion pass later attaches a chaintracks-VERIFIED bump (or the row
/// honestly stays proofless), and the #273 backstop keeps covering it.
///
/// Ancestor bumps are PRESERVED (they are source-data context, identical to
/// every existing submit; no per-ancestor row is stored under them). The
/// subject's raw + ancestry survive; only its own proof claim is dropped.
///
/// `None` = the BEEF could not be safely rebuilt (unparseable, subject
/// missing/txid-only, or the rebuilt BEEF failed the proofless/raw guard) —
/// the caller REFUSES admission (fail-closed: never store an unverified
/// mined-claim verbatim).
pub fn strip_subject_bump(beef_bytes: &[u8], subject_txid: &str) -> Option<Vec<u8>> {
    let mut tx = Transaction::from_beef(beef_bytes, Some(subject_txid)).ok()?;
    if !tx.id().eq_ignore_ascii_case(subject_txid) {
        return None; // content-addressing belt — never rebuild the wrong tx
    }
    tx.merkle_path = None;
    // allow_partial: a mined-claim BEEF may carry bump-only parents with no
    // raws — the subject's own raw is what admission stores.
    let rebuilt = match tx.to_beef(true) {
        Ok(b) => b,
        Err(_) => {
            // Fallback: a minimal single-tx BEEF (parents dropped — they
            // were claim-context only; the completion pass re-anchors the
            // subject against chaintracks directly).
            let mut nb = Beef::new();
            tx.merkle_path = None;
            nb.merge_transaction(tx);
            nb.to_binary()
        }
    };
    // GUARD: the rebuilt BEEF must carry the subject's raw and must NOT
    // claim a proof for it — otherwise refuse (the caller 502s).
    let b = Beef::from_binary(&rebuilt).ok()?;
    let btx = b.find_txid(subject_txid)?;
    if btx.has_proof() || btx.tx().is_none() {
        return None;
    }
    Some(rebuilt)
}

/// The SUBJECT's raw bytes when it CLAIMS to be already mined (bsv-low#268).
///
/// [`beef_to_ef_batch`] returns an EMPTY `efs` when every tx in the BEEF —
/// including the subject — carries a bump. That bump is submitter-asserted
/// and NOT validated at this layer, so the broadcast gate must corroborate
/// the "already mined" claim against a real provider instead of admitting
/// on it; the corroboration body is the subject's RAW (a genuinely mined
/// tx's parents are on-chain, so raw suffices and any honest provider
/// answers "already known/mined").
///
/// `None` when the BEEF does not parse, is empty, or the subject is a
/// txid-only entry with no tx data — the caller then refuses admission
/// (fail-closed: an unverifiable claim never admits).
pub fn proven_subject_raw(beef_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut beef = Beef::from_binary(beef_bytes).ok()?;
    let subject_txid = subject_txid_of(&mut beef)?;
    let subject = beef
        .txs
        .iter()
        .find(|b| b.txid().eq_ignore_ascii_case(&subject_txid))?;
    let tx = subject.tx()?;
    Some(tx.to_binary())
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

    /// F-D (2026-09-05): a subject whose parents are only NAMED in the BEEF
    /// lists them as missing; merging a parent's courier-served raw (hash-
    /// verified) removes it from the list; a raw that does not hash to its
    /// txid is dropped and the BEEF stays byte-identical. Real fixtures: the
    /// subject `e98cdd1f…` and its parent `a7d76588…`.
    #[test]
    fn missing_sources_are_named_and_completed_by_verified_raws_only() {
        let subject = Transaction::from_hex(SUBJECT_RAW_HEX.trim()).expect("subject raw");
        let mut only_subject = Beef::new();
        only_subject.merge_raw_tx(subject.to_binary(), None);
        let bytes = only_subject.to_binary();
        let missing = missing_source_txids(&bytes);
        assert!(!missing.is_empty(), "the lone subject names its parents as missing");
        assert!(missing.iter().all(|t| t.len() == 64));
        let parent_beef = Beef::from_binary(&hex::decode(PARENT_BEEF_HEX.trim()).expect("parent beef hex")).expect("parent beef");
        let (parent_txid, parent_raw_hex) = parent_beef
            .txs
            .iter()
            .filter_map(|b| b.tx().map(|t| (b.txid(), t.to_hex())))
            .find(|(id, _)| missing.contains(id))
            .expect("the parent fixture covers one of the missing sources");
        // an impostor raw under the parent's txid is dropped: byte-identical, still missing
        let untouched = merge_raw_sources(&bytes, &[(parent_txid.clone(), subject.to_hex())]);
        assert_eq!(untouched, bytes);
        assert!(missing_source_txids(&untouched).contains(&parent_txid));
        // the real raw completes that parent
        let completed = merge_raw_sources(&bytes, &[(parent_txid.clone(), parent_raw_hex)]);
        assert!(completed.len() > bytes.len());
        assert!(!missing_source_txids(&completed).contains(&parent_txid));
        // a garbage BEEF lists nothing and merges nothing
        assert!(missing_source_txids(b"nope").is_empty());
        assert_eq!(merge_raw_sources(b"nope", &[]), b"nope".to_vec());
    }


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
        let (efs, subject_txid) = beef_to_ef_batch(&beef).expect("real BEEF must convert to EF");

        assert_eq!(efs.len(), 1, "only the unmined subject is broadcast as EF");
        assert_eq!(
            subject_txid, SUBJECT_TXID,
            "subject is the last (spending) tx"
        );
        assert_eq!(efs[0].txid, SUBJECT_TXID, "the EF entry carries its txid");

        for EfTx { ef, .. } in &efs {
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
                assert!(
                    out.satoshis.unwrap_or(0) > 0,
                    "EF source output must carry sats"
                );
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
        assert!(matches!(
            beef_to_ef_batch(&[0xde, 0xad]),
            Err(EfError::Parse(_))
        ));
    }

    #[test]
    fn strip_subject_bump_makes_the_stored_row_honestly_proofless() {
        // #268 gate M1, through the REAL producers: the mined-claim admit
        // stores `strip_subject_bump`'s output, and the read/storage sides
        // judge it by `BeefTx::has_proof` (the same predicate
        // `D1Storage::beef_has_proof` and the /tx-any height read key on).
        //
        // RED-VERIFY: neuter `strip_subject_bump` (backup copy) to return
        // the input verbatim → the stripped BEEF still claims a proof →
        // both assertions below fail.
        let beef = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap().to_binary();
        let (efs, subject_txid) = beef_to_ef_batch(&beef).unwrap();
        assert!(
            efs.is_empty(),
            "fixture is the all-proven (mined-claim) shape"
        );
        // Before: the subject CLAIMS a proof (the M1 poison, stored verbatim
        // pre-fix).
        let before = Beef::from_binary(&beef).unwrap();
        assert!(before.find_txid(&subject_txid).unwrap().has_proof());

        let stripped = strip_subject_bump(&beef, &subject_txid)
            .expect("a parseable mined-claim BEEF must sanitize");
        let after = Beef::from_binary(&stripped).unwrap();
        let btx = after.find_txid(&subject_txid).expect("subject survives");
        assert!(
            !btx.has_proof(),
            "the submitter's unverified bump must NOT survive into storage"
        );
        // The raw survives byte-meaningfully (content-addressed identity).
        assert_eq!(btx.tx().unwrap().id(), subject_txid);

        // Fail-closed arms: garbage bytes and a wrong subject txid refuse.
        assert!(strip_subject_bump(&[0xde, 0xad], &subject_txid).is_none());
        assert!(strip_subject_bump(&beef, &"0".repeat(64)).is_none());
    }

    #[test]
    fn strip_subject_bump_is_a_noop_shape_for_a_proofless_subject() {
        // A subject with NO bump (the efs>=1 shape) round-trips proofless —
        // stripping never manufactures or destroys anything but the
        // subject's own proof claim.
        let beef = build_unmined_beef();
        let stripped = strip_subject_bump(&beef, SUBJECT_TXID).expect("must rebuild");
        let after = Beef::from_binary(&stripped).unwrap();
        let btx = after.find_txid(SUBJECT_TXID).unwrap();
        assert!(!btx.has_proof());
        assert_eq!(btx.tx().unwrap().id(), SUBJECT_TXID);
    }

    #[test]
    fn proven_subject_raw_extracts_the_subject_bytes_for_the_mined_claim() {
        // bsv-low#268: an all-proven BEEF (efs empty) needs the SUBJECT's raw
        // for the mined-claim corroboration. The parent fixture BEEF alone is
        // exactly that shape — its subject (the parent tx) carries a bump, so
        // the EF batch is empty and the raw must come from here.
        let beef = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap().to_binary();
        let (efs, subject_txid) = beef_to_ef_batch(&beef).unwrap();
        assert!(efs.is_empty(), "all-proven BEEF yields no EF legs");
        let raw = proven_subject_raw(&beef).expect("subject raw must extract");
        let tx = Transaction::from_binary(&raw).unwrap();
        assert_eq!(
            tx.id(),
            subject_txid,
            "raw is content-addressed to the subject"
        );
        // Garbage → None (the caller refuses admission, fail-closed).
        assert!(proven_subject_raw(&[0xde, 0xad]).is_none());
        assert!(proven_subject_raw(&[]).is_none());
    }

    #[test]
    fn ef_batch_skips_unconvertible_ancestor_but_still_emits_the_subject() {
        // Adversarial review 2026-07-17, finding 5: a recovery-refund-shaped
        // BEEF — the parent rides UNPROVEN and WITHOUT its own parents (its
        // sources are absent), the subject spends it. The parent cannot be
        // EF'd (no source data) and must be SKIPPED; the subject sources from
        // the parent's raw and must still convert.
        let mut beef = Beef::new();
        let parent = Transaction::from_hex(
            // Extract the parent raw from its BEEF fixture.
            {
                let pb = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap();
                &pb.txs.last().unwrap().tx().unwrap().to_hex()
            },
        )
        .unwrap();
        beef.merge_transaction(parent); // proofless, sources absent
        let subject = Transaction::from_hex(SUBJECT_RAW_HEX.trim()).unwrap();
        beef.merge_transaction(subject);
        let (efs, subject_txid) = beef_to_ef_batch(&beef.to_binary())
            .expect("subject must convert even when the ancestor cannot");
        assert_eq!(subject_txid, SUBJECT_TXID);
        assert_eq!(efs.len(), 1, "the unconvertible ancestor is skipped");
        assert_eq!(efs[0].txid, SUBJECT_TXID);
    }
    /// LOOP-2 FLEET PIN (2026-09-05, pair-17 DEFINITIVE; the real bytes p2's
    /// felt POSTed at 23:45:57.885Z — 23 txs, 57,100 B): the JOIN
    /// `5ad2764c…` spends the two hops; p1's hop `a347ed36…` spends
    /// `294fced3…:1`, a source the BEEF does NOT carry. `sort_txs` files the
    /// JOIN as not-valid and p2's hop LAST; the pre-loop-2 rule named the hop
    /// the subject and the pot never entered the index. The subject is the
    /// unique tip, order be damned; the EF batch carries the JOIN's own leg
    /// (its direct sources are present) and the true subject's missing
    /// sources are NONE (the gap is a grandparent — F-D's completion is not
    /// what this shape needed, the subject rule was).
    const LOOP2_INCOMPLETE_JOIN_BEEF: &[u8] =
        include_bytes!("../tests/fixtures/ef/loop2_join_5ad2764c_incomplete.beef");
    const LOOP2_JOIN_TXID: &str = "5ad2764c5151592915ccfc2e1ac2cbc763a34c3c522aa6f98655f1fc88559bb8";
    const LOOP2_P2_HOP_TXID: &str = "6ec7a0e8c453019fe665627031eb33a15e8891e1b123fa96a067d1a9cd54d8c8";

    #[test]
    fn loop2_incomplete_join_beef_subject_is_the_join_not_the_sorted_last_hop() {
        let mut beef = Beef::from_binary(LOOP2_INCOMPLETE_JOIN_BEEF).expect("the captured body parses");
        assert!(!beef.is_atomic(), "the loop-2 client sent a plain BEEF (no atomic name)");
        // The trap: every order-dependent reader picks something OTHER than
        // the JOIN (the SDK's `toBinary()` order and bsv-rs's `sort_txs`
        // differ in which valid tx lands last — p2's hop for the SDK, the
        // last resolved valid tx for bsv-rs — and neither is the subject).
        let sorted_last = sorted_last_txid_of(&beef).expect("a sorted last");
        assert_ne!(sorted_last, LOOP2_JOIN_TXID, "the trap: sorted-last is never the JOIN here");
        assert!(
            beef.txs.iter().any(|b| b.txid().eq_ignore_ascii_case(&sorted_last)),
            "the sorted-last is one of the BEEF's txs"
        );
        assert!(
            beef.txs.iter().any(|b| b.txid().eq_ignore_ascii_case(LOOP2_P2_HOP_TXID)),
            "p2's hop is in the body (the SDK's sorted-last)"
        );
        assert_eq!(subject_txid_of(&mut beef).as_deref(), Some(LOOP2_JOIN_TXID), "the unique tip is the JOIN");

        let (efs, subject_txid) = beef_to_ef_batch(LOOP2_INCOMPLETE_JOIN_BEEF).expect("converts");
        assert_eq!(subject_txid, LOOP2_JOIN_TXID);
        assert!(efs.iter().any(|e| e.txid == LOOP2_JOIN_TXID), "the JOIN's own EF leg is in the batch");
        assert!(
            missing_source_txids(LOOP2_INCOMPLETE_JOIN_BEEF).is_empty(),
            "the JOIN's DIRECT sources are present — the gap is a grandparent"
        );
        let raw = proven_subject_raw(LOOP2_INCOMPLETE_JOIN_BEEF).expect("subject raw");
        assert_eq!(Transaction::from_binary(&raw).unwrap().id(), LOOP2_JOIN_TXID);
    }

    /// The same loop's SECOND capture: p2's felt re-presenting the transient
    /// hand's JOIN `f9e85aab…` (4 txs, 9,026 B) with p1's hop `4a89ca60…`
    /// carrying three sources the body lacks — sorted-last is p2's hop
    /// `6760361c…`; the unique tip is the JOIN.
    const LOOP2_REPRESENT_BEEF: &[u8] =
        include_bytes!("../tests/fixtures/ef/loop2_join_f9e85aab_represent.beef");

    #[test]
    fn loop2_represent_beef_subject_is_the_join_too() {
        let mut beef = Beef::from_binary(LOOP2_REPRESENT_BEEF).expect("parses");
        assert_eq!(beef.txs.len(), 4);
        assert_eq!(
            sorted_last_txid_of(&beef).as_deref(),
            Some("6760361c9fbed4ccb68caa32b63f239623c8c6861380d68aeb1154ee7450394e"),
            "sorted-last: p2's hop"
        );
        assert_eq!(
            subject_txid_of(&mut beef).as_deref(),
            Some("f9e85aab7cc018cc47040eb7029cc94dd7cec1c7b981c5d1084fbfe862e2bb1d"),
            "the unique tip: the JOIN"
        );
    }

    #[test]
    fn atomic_beef_names_its_subject_over_wire_order() {
        // parent proven + subject unmined, serialized ATOMIC for the subject.
        let mut beef = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap();
        let subject = Transaction::from_hex(SUBJECT_RAW_HEX.trim()).unwrap();
        beef.merge_transaction(subject);
        let atomic = beef.to_binary_atomic(SUBJECT_TXID).unwrap();
        let mut parsed = Beef::from_binary(&atomic).unwrap();
        assert!(parsed.is_atomic());
        assert_eq!(subject_txid_of(&mut parsed).as_deref(), Some(SUBJECT_TXID));
        let (efs, subject_txid) = beef_to_ef_batch(&atomic).unwrap();
        assert_eq!(subject_txid, SUBJECT_TXID);
        assert_eq!(efs.len(), 1);
        // an atomic name that is NOT in the BEEF is ignored, never trusted
        let mut stray = Beef::from_binary(&atomic).unwrap();
        stray.atomic_txid = Some("00".repeat(32));
        assert_eq!(subject_txid_of(&mut stray).as_deref(), Some(SUBJECT_TXID), "falls to the unique tip");
    }

    #[test]
    fn two_tips_fall_back_to_the_reference_sorted_last() {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{TransactionInput, TransactionOutput};
        // a spends b (the fixtures): ONE tip → a, whatever the wire order.
        let a = Transaction::from_hex(SUBJECT_RAW_HEX.trim()).unwrap();
        let b = {
            let pb = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap();
            Transaction::from_hex(&pb.txs.last().unwrap().tx().unwrap().to_hex()).unwrap()
        };
        let mut related = Beef::new();
        related.merge_transaction(a.clone());
        related.merge_transaction(b.clone());
        assert_eq!(subject_txid_of(&mut related).as_deref(), Some(SUBJECT_TXID), "a spends b: a is the tip");

        // Two UNRELATED unmined txs (each spends a source the BEEF lacks): no
        // unique tip → the reference's sorted-last, whichever that is.
        let stray = |seed: u8| -> Transaction {
            let mut tx = Transaction::new();
            tx.inputs.push(TransactionInput {
                source_txid: Some(format!("{:02x}", seed).repeat(32)),
                source_output_index: 0,
                ..Default::default()
            });
            tx.outputs.push(TransactionOutput::new(1, LockingScript::from_hex("51").unwrap()));
            tx
        };
        let (c, d) = (stray(0xaa), stray(0xbb));
        assert_ne!(c.id(), d.id());
        let mut two = Beef::new();
        two.merge_transaction(c.clone());
        two.merge_transaction(d.clone());
        let expect = sorted_last_txid_of(&two);
        assert!(expect.is_some());
        assert_eq!(subject_txid_of(&mut two), expect, "no unique tip → the reference's sorted-last");
    }
}
