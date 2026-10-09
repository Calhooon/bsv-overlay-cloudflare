//! Parser policies for stranger BEEF, including bytes read back from storage.
//!
//! These are application resource limits, not transaction validity rules.
//! The submit shape is a subject, its unconfirmed ancestors and proven funding
//! inputs (ts-stack@fb1b2da packages/overlays/overlay-express/src/OverlayExpress.ts
//! 2655-2693; packages/overlays/overlay/src/Engine.ts 1711-1724).

use bsv_rs::transaction::{Beef, BeefLimits, MerklePath, Transaction};

/// Keep the Worker's existing decimal 10 MB request cap, including off-chain
/// values. The reference Express default is 64 MiB binary, so this is our policy.
pub const SUBMIT_BODY_MAX_BYTES: usize = 10_000_000;
/// BRC-95 adds a four-byte marker and a 32-byte subject to a plain BEEF.
pub const ATOMIC_HEADER_BYTES: usize = 36;
/// A subject plus up to 255 funded inputs/ancestor bodies per invocation.
/// This intentionally admits accumulated wallet ancestry rather than the old
/// eight-unproven-transaction gate. It is a policy cap, not a protocol maximum.
const MAX_SUBMISSION_TXS: usize = 256;
/// Every carried body may be proven in a different block: one BUMP per body.
/// A BUMP can also prove several bodies, so this covers the unshared case.
const MAX_SUBMISSION_BUMPS: usize = 256;

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

/// A single proof with 64 tree levels needs only a few KB. 64 KiB leaves
/// headroom for honest shared proofs while bounding decode/parse allocations.
pub const COURIER_PROOF_MAX_BYTES: usize = 64 * 1024;
/// A pushed proof uses the same envelope as one fetched from a courier.
pub const PUSH_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;
/// Stored proofs are rechecked under the same envelope on every parse.
pub const STORED_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;
/// GASP's proof field is a single proof, under the courier envelope.
pub const PEER_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;

/// Parse a door's BEEF. The witness commit preserves the old unbounded reader.
pub fn parse_beef(bytes: &[u8], _limits: &BeefLimits) -> bsv_rs::Result<Beef> {
    Beef::from_binary(bytes)
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
    let target = txid.map(str::to_string).or_else(|| parsed.atomic_txid.clone())
        .or_else(|| parsed.txs.last().map(bsv_rs::transaction::BeefTx::txid))
        .ok_or_else(|| bsv_rs::Error::TransactionError("No transactions in BEEF".into()))?;
    parsed.find_atomic_transaction(&target).ok_or_else(|| {
        bsv_rs::Error::TransactionError(format!("Transaction {target} not found in BEEF"))
    })
}

/// Bound hex before decoding, then parse the proof. The witness preserves the
/// old reader until the red tests have been recorded.
pub fn merkle_path_from_hex(hex: &str, _max_bytes: usize) -> bsv_rs::Result<MerklePath> {
    MerklePath::from_hex(hex)
}
