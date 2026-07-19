//! Serve-time BEEF compaction (#192/#193, P4): shrink an oversized served
//! BEEF without ever breaking SPV.
//!
//! ## Why
//!
//! Once the overlay's proof-completion cron (or the Arcade MINED callback)
//! stitches a merkle BUMP into a stored BEEF, every ancestor raw-tx reachable
//! ONLY through a now-proven transaction is dead weight: a wallet doing SPV
//! stops descending at the first proven transaction. `/beef` serves those
//! bytes to the frontend `createAction`, so trimming them here is the actual
//! shrink the completion pass unlocks.
//!
//! ## How
//!
//! `bsv_rs::transaction::Beef::trim_known_proven` BFS-walks from the tip txs,
//! STOPS descending at proven txs (`bump_index.is_some()`), drops every
//! now-unreachable ancestor raw tx, and GCs orphaned BUMPs. Mined-detection is
//! the in-BEEF BUMP presence signal ONLY — no WhatsOnChain, no header lookup —
//! honoring the "our own chain data only" invariant. The proof itself was
//! already chaintracks-verified before it was ever stitched in (the overlay
//! cron / callback path); serve-time compaction never fabricates a proof, it
//! only drops ancestry a retained proof already covers.
//!
//! ## Safety contract — STRICTLY passthrough-on-failure
//!
//! Compaction must NEVER corrupt or drop a BEEF. Every failure path returns
//! the ORIGINAL bytes unchanged:
//!
//! 1. Parse error -> passthrough (log + return original).
//! 2. Nothing trimmed (tx count unchanged) -> return original.
//! 3. Any post-trim verification failure (`verify_valid(true)`) -> passthrough.
//!
//! The output is only ever the compacted bytes when the result re-parses,
//! still contains the subject txid, is structurally valid, and (when any BUMP
//! is retained) yields a non-empty roots map. The compacted bytes are
//! re-serialized in the SAME on-wire format as the input (plain BEEF vs Atomic
//! BEEF) so `/beef` consumers keep parsing them unchanged.
//!
//! Ported/adapted from `~/bsv/zanaadu/overlay/src/beef_compaction.rs`.

use bsv_rs::transaction::Beef;

/// Diagnostic log that maps to `worker::console_log!` in the CF Worker
/// (wasm32) build and to `eprintln!` on the host build (so the host-target
/// unit tests below can run — the `worker` console macro calls a wasm-bindgen
/// extern that aborts on the host target).
macro_rules! compaction_log {
    ($($arg:tt)*) => {{
        #[cfg(target_arch = "wasm32")]
        {
            worker::console_log!($($arg)*);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            eprintln!($($arg)*);
        }
    }};
}

/// Compacts a served BEEF by trimming redundant mined-ancestor raw txs.
///
/// `subject_txid` is the transaction the BEEF is "about" (the tip / the output
/// owner, lowercase hex). It MUST remain present after compaction so `/beef`'s
/// self-containment expectation (and the frontend that asked for it) still
/// holds.
///
/// Returns the compacted bytes on success, or the original `beef` bytes
/// unchanged on ANY failure or no-op. Never panics, never returns a corrupt or
/// non-self-contained BEEF.
pub fn compact_beef(subject_txid: &str, beef: &[u8]) -> Vec<u8> {
    // 1. Parse. On parse error, passthrough (never drop a BEEF we can't parse).
    let mut parsed = match Beef::from_binary(beef) {
        Ok(b) => b,
        Err(e) => {
            compaction_log!(
                "beef compaction: parse failed for {subject_txid} (passthrough, {} bytes): {e}",
                beef.len()
            );
            return beef.to_vec();
        },
    };

    // Record the input on-wire format so we can re-serialize identically.
    // Atomic BEEF carries `atomic_txid` (set by from_binary on the 0x01010101
    // prefix); plain BEEF does not.
    let input_atomic_txid = parsed.atomic_txid.clone();

    let before_tx_count = parsed.txs.len();

    // 2. Trim. BFS from tips, stop at proven txs, GC orphan BUMPs.
    parsed.trim_known_proven();

    // 3. No-op short-circuit: nothing removable, so the original bytes are
    //    already minimal. Return them untouched (cheapest + zero risk).
    if parsed.txs.len() == before_tx_count {
        return beef.to_vec();
    }

    // 4. Verify the trimmed BEEF is still a self-contained, SPV-verifiable
    //    BEEF that contains the subject. On ANY failure, passthrough.
    if !verify_compacted(&mut parsed, subject_txid) {
        compaction_log!(
            "beef compaction: verify failed for {subject_txid} (passthrough, {} bytes)",
            beef.len()
        );
        return beef.to_vec();
    }

    // 5. Re-serialize in the SAME format as the input.
    let compacted = match &input_atomic_txid {
        Some(_) => match parsed.to_binary_atomic(subject_txid) {
            Ok(bytes) => bytes,
            Err(e) => {
                compaction_log!(
                    "beef compaction: atomic re-serialize failed for {subject_txid} \
                     (passthrough, {} bytes): {e}",
                    beef.len()
                );
                return beef.to_vec();
            },
        },
        None => parsed.to_binary(),
    };

    compaction_log!(
        "beef compaction: {subject_txid} {} -> {} bytes ({} -> {} txs)",
        beef.len(),
        compacted.len(),
        before_tx_count,
        parsed.txs.len()
    );

    compacted
}

/// Verifies a trimmed BEEF is safe to serve in place of the original.
///
/// Requires, on the trimmed `beef`:
/// - the subject txid is still present (self-containment),
/// - structural validity allowing txid-only entries (`verify_valid(true)`),
/// - and a non-empty roots map whenever any BUMP is retained (proves the
///   retained merkle proofs actually compute a root).
fn verify_compacted(beef: &mut Beef, subject_txid: &str) -> bool {
    // Subject must survive, so the frontend can find the tx it asked about.
    // trim_known_proven keeps the subject as a tip; assert it defensively.
    if beef.find_txid(subject_txid).is_none() {
        return false;
    }

    // Structural validity (txid-only allowed: trimmed proven ancestors may be
    // represented by BUMP + txid). verify_valid returns the roots to verify.
    let result = beef.verify_valid(true);
    if !result.valid {
        return false;
    }

    // If any BUMP is retained, verify_valid must have computed at least one
    // merkle root from it. An empty roots map alongside a retained BUMP would
    // mean the proof did not compute — a corrupt compaction.
    if !beef.bumps.is_empty() && result.roots.is_empty() {
        return false;
    }

    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bsv_rs::transaction::{Beef, MerklePath, MerklePathLeaf, Transaction};

    /// A real mainnet raw tx. Used as the unproven "grandparent" in the
    /// synthetic shrink chain below.
    const RAW_TX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

    /// A garbage / unparseable input must never be dropped: passthrough.
    #[test]
    fn garbage_input_passes_through_unchanged() {
        let garbage = [0x00, 0x01, 0x02, 0xff, 0xab];
        let out = compact_beef("deadbeef", &garbage);
        assert_eq!(out, garbage);
    }

    /// Builds a minimal raw tx spending `source_txid:vout` with one OP_TRUE
    /// output, returned as a hex string.
    fn build_child_spending(source_txid: &str, vout: u32) -> String {
        let mut s = String::from("01000000"); // version
        s.push_str("01"); // 1 input
        let mut prev = hex::decode(source_txid).unwrap();
        prev.reverse(); // little-endian prev txid
        s.push_str(&hex::encode(prev));
        s.push_str(&hex::encode(vout.to_le_bytes())); // vout LE
        s.push_str("00"); // scriptSig len 0
        s.push_str("ffffffff"); // sequence
        s.push_str("01"); // 1 output
        s.push_str(&hex::encode(1000u64.to_le_bytes())); // value LE
        s.push_str("0151"); // scriptPubKey len 1, OP_TRUE
        s.push_str("00000000"); // locktime
        s
    }

    /// grandparent (unproven) <- parent (PROVEN, has a BUMP) <- child (tip,
    /// unproven). trim_known_proven stops at the proven parent, so the
    /// grandparent (reachable only through it) is dropped and the BEEF shrinks.
    #[test]
    fn synthetic_redundant_ancestor_is_trimmed_and_shrinks() {
        // grandparent: unproven, real raw tx.
        let gp = Transaction::from_hex(RAW_TX).unwrap();
        let gp_txid = gp.id();

        // parent: spends gp:0, PROVEN via a single-leaf BUMP.
        let parent_hex = build_child_spending(&gp_txid, 0);
        let mut parent = Transaction::from_hex(&parent_hex).unwrap();
        let parent_txid = parent.id();
        let pleaf = MerklePathLeaf::new_txid(0, parent_txid.clone());
        parent.merkle_path = Some(MerklePath::new_unchecked(800_000, vec![vec![pleaf]]).unwrap());

        // child: tip, spends parent:0, unproven. This is the subject.
        let child_hex = build_child_spending(&parent_txid, 0);
        let child = Transaction::from_hex(&child_hex).unwrap();
        let child_txid = child.id();

        // Assemble: gp (unproven) + parent (with its proof) + child (tip).
        let mut beef = Beef::new();
        beef.merge_transaction(gp.clone());
        let parent_beef = parent.to_beef(true).unwrap();
        beef.merge_beef(&Beef::from_binary(&parent_beef).unwrap());
        beef.merge_transaction(child.clone());

        let input = beef.to_binary(); // plain V2 (not atomic)
        let in_len = input.len();
        assert_eq!(beef.txs.len(), 3);

        let out = compact_beef(&child_txid, &input);

        // Genuinely shrank.
        assert!(out.len() < in_len, "expected shrink: {} -> {}", in_len, out.len());

        // Result: plain (format preserved), re-parses, subject + proven parent
        // retained, grandparent gone, SPV-verifiable.
        let mut re = Beef::from_binary(&out).unwrap();
        assert!(!re.is_atomic(), "format preserved: plain input -> plain output");
        assert!(re.find_txid(&child_txid).is_some(), "subject (child) retained");
        assert!(re.find_txid(&parent_txid).is_some(), "proven parent retained");
        assert!(re.find_txid(&gp_txid).is_none(), "redundant grandparent must be trimmed");
        assert_eq!(re.txs.len(), 2);
        let vr = re.verify_valid(true);
        assert!(vr.valid, "compacted beef must be structurally valid");
        assert!(!vr.roots.is_empty(), "a BUMP is retained, so a merkle root must compute");
    }
}
