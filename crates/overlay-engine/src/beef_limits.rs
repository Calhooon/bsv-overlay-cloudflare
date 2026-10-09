//! Parser policies for stranger BEEF, including bytes read back from storage.
//!
//! These are application resource limits, not transaction validity rules.
//! The submit shape is a subject, its unconfirmed ancestors and proven funding
//! inputs (ts-stack@fb1b2da packages/overlays/overlay-express/src/OverlayExpress.ts
//! 2655-2693; packages/overlays/overlay/src/Engine.ts 1711-1724).

use bsv_rs::transaction::{Beef, MerklePath, Transaction};

/// A door's policy. bsv-rs 0.4.0 took `max_bytes` off its own `BeefLimits`
/// and its limits refuse nothing (they are reserve hints), so the three
/// numbers a door names live here and the checks below are this module's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeefLimits {
    pub max_txs: usize,
    pub max_bumps: usize,
    pub max_bytes: usize,
}

/// Keep the Worker's existing decimal 10 MB request cap, including off-chain
/// values. The reference Express default is 64 MiB binary, so this is our policy.
pub const SUBMIT_BODY_MAX_BYTES: usize = 10_000_000;
/// BRC-95 adds a four-byte marker and a 32-byte subject to a plain BEEF.
pub const ATOMIC_HEADER_BYTES: usize = 36;
/// The engine supports 256 direct inputs (`engine.rs` at cf933e8, line 552),
/// and its existing GASP wide-parent test carries 256 proven parents plus
/// their subject. 512 admits that shape and another 255 ancestry bodies.
/// This is application policy, not a protocol maximum.
const MAX_SUBMISSION_TXS: usize = 512;
/// Every carried body may be proven in a different block: one BUMP per body.
/// A BUMP can also prove several bodies, so this covers the unshared case.
const MAX_SUBMISSION_BUMPS: usize = 512;

/// HTTP submit: the whole request has already been capped at 10 MB.
pub const SUBMIT_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_txs: MAX_SUBMISSION_TXS,
    max_bumps: MAX_SUBMISSION_BUMPS,
    max_bytes: SUBMIT_BODY_MAX_BYTES,
};
/// Core library submits and their carried/atomic bodies get the same counts
/// and room for the subject header the engine itself adds.
pub const ENGINE_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_bytes: SUBMIT_BODY_MAX_BYTES + ATOMIC_HEADER_BYTES,
    ..SUBMIT_BEEF_LIMITS
};
/// EF readers also see courier-completed/subject-named submit bodies. Reapply
/// the admission policy before conversion, even outside the HTTP handler.
pub const EF_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// D1/storage may contain old client bytes; admission is never a permanent
/// exemption from the limits. Include the engine's atomic-header allowance.
pub const STORED_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// A peer's lookup output can carry the same subject and funding ancestry.
pub const PEER_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// Whole-transaction discovery hooks receive a subject-named submission.
pub const DISCOVERY_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// App-layer reads/parent merges have the same stored-body envelope; they do
/// not relax limits because a row or a courier supplied the bytes.
pub const APP_BEEF_LIMITS: BeefLimits = STORED_BEEF_LIMITS;
/// Keep the queue producer's 90 KB raw cap: base64 leaves room within the
/// platform's 128 KB message envelope (queue.rs at cf933e8, lines 28-31).
pub const QUEUE_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_bytes: 90_000,
    ..SUBMIT_BEEF_LIMITS
};
/// A dead letter holds that same queue payload; parking is no exemption.
pub const DEAD_LETTER_BEEF_LIMITS: BeefLimits = QUEUE_BEEF_LIMITS;
/// The census already stops at 2 MiB; keep its separate evaluation budget.
pub const CENSUS_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_bytes: 2 * 1024 * 1024,
    ..SUBMIT_BEEF_LIMITS
};

/// An 8 KiB canonical JSON hex field reserves two bytes for quotes and two
/// hex characters per decoded byte. A 64-level branch needs less than 3 KiB;
/// this also admits a shared 120-leaf proof within the field budget.
pub const COURIER_PROOF_MAX_BYTES: usize = (8 * 1024 - 2) / 2;
/// A pushed proof uses the same envelope as one fetched from a courier.
pub const PUSH_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;
/// Stored proofs are rechecked under the same envelope on every parse.
pub const STORED_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;
/// GASP's proof field is a single proof, under the courier envelope.
pub const PEER_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;

/// Refuse the body before constructing a reader. Each caller supplies its
/// named policy. The BUMP count is checked on its prefix before the parse;
/// the transaction count sits behind the BUMPs on the wire, so it is checked
/// on the parse of a body the byte check has already bounded.
pub fn parse_beef(bytes: &[u8], limits: &BeefLimits) -> bsv_rs::Result<Beef> {
    check_size(bytes.len(), limits.max_bytes, "BEEF")?;
    if let Some(bump_count) = claimed_bump_count(bytes) {
        if bump_count > limits.max_bumps as u64 {
            return Err(bsv_rs::Error::BeefError(format!(
                "BEEF claims {bump_count} BUMPs, over max_bumps {}",
                limits.max_bumps
            )));
        }
    }
    let beef = Beef::from_binary(bytes)?;
    if beef.txs.len() > limits.max_txs {
        return Err(bsv_rs::Error::BeefError(format!(
            "BEEF claims {} transactions, over max_txs {}",
            beef.txs.len(),
            limits.max_txs
        )));
    }
    Ok(beef)
}

/// The BUMP count a body's prefix claims: behind the four-byte version, or
/// behind the 36-byte BRC-95 header and the version. `None` where the prefix
/// is too short to hold one; the parser names that fault.
fn claimed_bump_count(bytes: &[u8]) -> Option<u64> {
    const ATOMIC_MARKER: [u8; 4] = [0x01, 0x01, 0x01, 0x01];
    let at = if bytes.get(..4)? == ATOMIC_MARKER {
        ATOMIC_HEADER_BYTES + 4
    } else {
        4
    };
    let rest = bytes.get(at..)?;
    let wide = |n: usize| {
        let mut le = [0u8; 8];
        le[..n].copy_from_slice(rest.get(1..1 + n)?);
        Some(u64::from_le_bytes(le))
    };
    match *rest.first()? {
        0xfd => wide(2),
        0xfe => wide(4),
        0xff => wide(8),
        short => Some(u64::from(short)),
    }
}

/// Link from the bounded parse, keeping the SDK's exact target selection:
/// explicit txid, then atomic txid, then wire-last. Do not parse again through
/// `Transaction::from_beef`, whose reader is intentionally unbounded.
pub fn transaction_from_beef(
    bytes: &[u8],
    txid: Option<&str>,
    limits: &BeefLimits,
) -> bsv_rs::Result<Transaction> {
    let parsed = parse_beef(bytes, limits)?;
    let target = txid
        .map(str::to_string)
        .or_else(|| parsed.atomic_txid.clone())
        .or_else(|| parsed.txs.last().map(bsv_rs::transaction::BeefTx::txid))
        .ok_or_else(|| bsv_rs::Error::TransactionError("No transactions in BEEF".into()))?;
    parsed.find_atomic_transaction(&target).ok_or_else(|| {
        bsv_rs::Error::TransactionError(format!("Transaction {target} not found in BEEF"))
    })
}

/// Bound hex before allocating decoded bytes, then parse the proof.
pub fn merkle_path_from_hex(hex: &str, max_bytes: usize) -> bsv_rs::Result<MerklePath> {
    check_size(hex.len(), max_bytes.saturating_mul(2), "merkle proof hex")?;
    let bytes = bsv_rs::primitives::from_hex(hex)?;
    MerklePath::from_binary(&bytes)
}

/// Whether a bounded parse failed on a LIMIT (the size cap of [`check_size`],
/// the SDK's `max_txs` / `max_bumps` prefix counts) rather than on the bytes
/// themselves. A door refuses a breach before anything else looks at the body;
/// a body under the limits that does not parse is an ARRIVAL the route still
/// counts (the #366 census) and answers with its own parse error, as before
/// the doors. The SDK named a breach only in its message ("over max_bumps",
/// "over max_txs"; ours "over max_bytes") until 0.3.35; since the bump to
/// 0.4.0 all three messages are this module's, and the pin below holds them.
pub fn is_limit_breach(e: &bsv_rs::Error) -> bool {
    matches!(e, bsv_rs::Error::BeefError(msg) if msg.contains("over max_"))
}

/// Shared pre-read/pre-decode length check. A malformed body over the cap is
/// refused for size without asking its parser to look at it.
pub fn check_size(size: usize, max_bytes: usize, kind: &str) -> bsv_rs::Result<()> {
    if size > max_bytes {
        return Err(bsv_rs::Error::BeefError(format!(
            "{kind} of {size} bytes is over max_bytes {max_bytes}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod limit_breach_pin {
    use super::*;

    #[test]
    fn a_size_breach_is_a_breach() {
        let e = check_size(11, 10, "BEEF").unwrap_err();
        assert!(is_limit_breach(&e), "{e}");
    }

    #[test]
    fn a_count_breach_is_a_breach_and_garbage_is_not() {
        // A BEEF prefix (version 4022206465 LE) claiming 300 BUMPs under a cap of 1.
        let mut bin = vec![0x01, 0x00, 0xbe, 0xef];
        bin.extend_from_slice(&[0xfd, 0x2c, 0x01]); // varint 300
        let tight = BeefLimits {
            max_txs: 1,
            max_bumps: 1,
            max_bytes: SUBMIT_BODY_MAX_BYTES,
        };
        let e = parse_beef(&bin, &tight).unwrap_err();
        assert!(
            is_limit_breach(&e),
            "a count breach must classify as a breach: {e}"
        );
        let garbage = [0xde, 0xad, 0xbe, 0xef];
        let e = parse_beef(&garbage, &SUBMIT_BEEF_LIMITS).unwrap_err();
        assert!(
            !is_limit_breach(&e),
            "garbage under the limits is a parse fault, not a breach: {e}"
        );
    }
}
