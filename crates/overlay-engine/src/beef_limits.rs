//! The engine's BEEF doors: a stranger's BEEF, and bytes read back from storage.
//!
//! The posture (the charter "a BEEF of any size", 2026-10-09): a valid BEEF is
//! never refused for its size or its counts. A door reads the bytes through
//! bsv-rs 0.4.2's [`BeefStream`], one element in hand, and refuses invalid
//! bytes only; a refusal names the offset of the byte and the kind
//! ([`Refusal`], [`Kind`]). No function in this module compares a BEEF's
//! length or its counts against anything.
//!
//! The module keeps its name and its constants as NAMES. Each `*_BEEF_LIMITS`
//! says which door a reader stands at and nothing more: the three numbers are
//! what P0-5f refused by, kept so a caller that still reads one compiles
//! (LOW's #585 removes its uses), and no reader here consults them.
//!
//! What streams and what does not. [`fold_beef`] reads any byte source
//! ([`std::io::Read`]: a slice today, an object body at rest when the Worker
//! has one). [`read_beef`], [`has_proof`], [`own_proof`] and [`own_bump`]
//! fold over held bytes and keep one element at a time. [`parse_beef`] and
//! [`transaction_from_beef`] run the same streaming door and then build the
//! in-memory [`Beef`], because their callers merge, link, sort or
//! re-serialize one; the caller already holds the bytes whole, so the object
//! is a second copy of a body the layer above read whole. That is the bound
//! the platform sets (one isolate), not a refusal of this module.
//!
//! The door is the frame and the elements alone (bsv-rs 0.4.2's
//! `BeefDecoder`): the version, the varints, each BUMP's tree height (at
//! most 64, the format's rule) and the agreement of its nodes, each
//! transaction's fields and at least one input, no byte after the frame.
//! Whether a root is the chain's and whether an input names an element are
//! the SPV walk's questions (`engine.rs`), asked after the door; the walk's
//! structure is [`structure_roots`], the same reader's.
//!
//! The submit shape is a subject, its unconfirmed ancestors and proven funding
//! inputs (ts-stack@fb1b2da packages/overlays/overlay-express/src/OverlayExpress.ts
//! 2655-2693; packages/overlays/overlay/src/Engine.ts 1711-1724).

use std::io::Read;

use bsv_rs::transaction::beef_stream::{display_hex, Hash32, Step, StreamError};
use bsv_rs::transaction::{
    verify_stream_structure, Beef, BeefDecoder, BeefStream, Element, Headers, MerklePath,
    Transaction, Verdict,
};

pub use bsv_rs::transaction::beef_stream::StreamError as DoorError;
pub use bsv_rs::transaction::{Kind, Reason, Refusal};

/// A door's name. Until NL-6 these three numbers were refusals; they are not
/// read by any function of this module now. bsv-rs 0.4.0's own `BeefLimits`
/// lost `max_bytes` and refuses nothing, so the struct lives here for the
/// callers that still name a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeefLimits {
    pub max_txs: usize,
    pub max_bumps: usize,
    pub max_bytes: usize,
}

/// The Worker's former whole-request cap (decimal 10 MB). A name only: the
/// submit route no longer answers 413 by it.
pub const SUBMIT_BODY_MAX_BYTES: usize = 10_000_000;
/// BRC-95 adds a four-byte marker and a 32-byte subject to a plain BEEF.
pub const ATOMIC_HEADER_BYTES: usize = 36;
/// The former count policy (P0-5f). A name only.
const MAX_SUBMISSION_TXS: usize = 512;
/// The former count policy (P0-5f). A name only.
const MAX_SUBMISSION_BUMPS: usize = 512;

/// HTTP submit.
pub const SUBMIT_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_txs: MAX_SUBMISSION_TXS,
    max_bumps: MAX_SUBMISSION_BUMPS,
    max_bytes: SUBMIT_BODY_MAX_BYTES,
};
/// Core library submits and their carried/atomic bodies.
pub const ENGINE_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_bytes: SUBMIT_BODY_MAX_BYTES + ATOMIC_HEADER_BYTES,
    ..SUBMIT_BEEF_LIMITS
};
/// EF readers: courier-completed and subject-named submit bodies.
pub const EF_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// Bytes read back from D1 or another store.
pub const STORED_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// A peer's lookup output.
pub const PEER_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// Whole-transaction discovery hooks.
pub const DISCOVERY_BEEF_LIMITS: BeefLimits = ENGINE_BEEF_LIMITS;
/// App-layer reads and parent merges (LOW's; #585 re-wires them).
pub const APP_BEEF_LIMITS: BeefLimits = STORED_BEEF_LIMITS;
/// The queue's byte bound is the engine's: a body past the platform's 128 KB
/// message rides by key in R2 (bsv-low #585 door 3).
pub const QUEUE_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_bytes: ENGINE_BEEF_LIMITS.max_bytes,
    ..SUBMIT_BEEF_LIMITS
};
/// A dead letter holds the queue's payload.
pub const DEAD_LETTER_BEEF_LIMITS: BeefLimits = QUEUE_BEEF_LIMITS;
/// The census. The 2 MiB was its evaluation budget (LOW's door 2); it is not
/// a refusal here.
pub const CENSUS_BEEF_LIMITS: BeefLimits = BeefLimits {
    max_bytes: 2 * 1024 * 1024,
    ..SUBMIT_BEEF_LIMITS
};

/// A COURIER's wire bound, not a BEEF refusal, and it stays: one merkle proof
/// arrives as an 8 KiB canonical JSON hex field (two bytes for the quotes, two
/// hex characters per decoded byte). A 64-level branch needs less than 3 KiB;
/// this also admits a shared 120-leaf proof within the field.
pub const COURIER_PROOF_MAX_BYTES: usize = (8 * 1024 - 2) / 2;
/// A pushed proof arrives in the same field as one fetched from a courier:
/// the same wire bound, and it stays.
pub const PUSH_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;
/// A name only since NL-6: a proof read back from a store is read whatever
/// its length ([`proof_from_hex`]).
pub const STORED_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;
/// A name only since NL-6: a GASP peer's proof field is read whatever its
/// length ([`proof_from_hex`]).
pub const PEER_PROOF_MAX_BYTES: usize = COURIER_PROOF_MAX_BYTES;

/// What one pass of the door learned of a BEEF it accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeefRead {
    /// The version word (`BEEF_V1` or `BEEF_V2`).
    pub version: u32,
    /// The BRC-95 subject, display hex, when the atomic prefix led.
    pub subject: Option<String>,
    /// The BUMPs read.
    pub bumps: u64,
    /// The transaction entries read, txid-only entries among them.
    pub txs: u64,
    /// The last transaction entry's txid on the wire, display hex.
    pub last_txid: Option<String>,
    /// The bytes the frame occupied.
    pub bytes: u64,
}

/// What a pass counts as the elements go by.
#[derive(Default)]
struct Tally {
    bumps: u64,
    txs: u64,
    last_txid: Option<[u8; 32]>,
}

impl Tally {
    fn see(&mut self, element: &Element) {
        match element {
            Element::Bump(_) => self.bumps += 1,
            Element::Tx { txid, .. } | Element::TxidOnly { txid, .. } => {
                self.txs += 1;
                self.last_txid = Some(*txid);
            }
        }
    }

    fn read(self, version: Option<u32>, subject: Option<[u8; 32]>, bytes: u64) -> BeefRead {
        BeefRead {
            version: version.unwrap_or_default(),
            subject: subject.as_ref().map(display_hex),
            bumps: self.bumps,
            txs: self.txs,
            last_txid: self.last_txid.as_ref().map(display_hex),
            bytes,
        }
    }
}

/// The door over any byte source: every element is read once, handed to
/// `visit` and dropped, so the reader holds one 16 KiB chunk of the source
/// and one element whatever the BEEF's length. `Err(Refused)` names the
/// offset and the kind of the invalid bytes; `Err(Io)` is the source's fault
/// and says nothing about the bytes.
pub fn fold_beef<R: Read>(
    source: R,
    mut visit: impl FnMut(Element),
) -> Result<BeefRead, StreamError> {
    let mut stream = BeefStream::new(source);
    let mut tally = Tally::default();
    while let Some(element) = stream.next_element()? {
        tally.see(&element);
        visit(element);
    }
    Ok(tally.read(stream.version(), stream.subject(), stream.offset()))
}

/// [`fold_beef`] over held bytes: the same decoder fed the one slice, so no
/// chunk is copied and no source can fail. The only error is a refusal.
pub fn fold_held(bytes: &[u8], mut visit: impl FnMut(Element)) -> Result<BeefRead, Refusal> {
    let mut decoder = BeefDecoder::new();
    let mut tally = Tally::default();
    let mut input = bytes;
    loop {
        match decoder.next(&mut input)? {
            Step::Element(element) => {
                tally.see(&element);
                visit(element);
            }
            Step::NeedMore => {
                decoder.finish()?;
                break;
            }
            Step::Done => break,
        }
    }
    Ok(tally.read(decoder.version(), decoder.subject(), decoder.offset()))
}

/// The door over held bytes: refuses invalid bytes only, with the offset and
/// the kind.
pub fn read_beef(bytes: &[u8]) -> Result<BeefRead, Refusal> {
    fold_held(bytes, |_| {})
}

fn refused(refusal: &Refusal) -> bsv_rs::Error {
    bsv_rs::Error::BeefError(refusal.to_string())
}

/// Carries every root: [`structure_roots`] hands the roots back and its
/// caller asks the chain, whose tracker answers asynchronously.
struct EveryRoot;

impl Headers for EveryRoot {
    fn carries(&self, _height: u64, _root: &Hash32) -> bool {
        true
    }
}

/// The BEEF's structure on the streaming reader's rules (bsv-rs 0.4.2
/// `verify_stream_structure`, the Lean's validity but the chain): one pass,
/// one element in hand, each BUMP's root walked once, linear in its leaves;
/// each input of an unproven transaction resolved against the elements
/// before it (the wire's order); each txid-only entry proven by a BUMP of
/// this BEEF; an atomic BEEF held to its subject. The answer is each BUMP's
/// block height and root (display hex), in BUMP order, for the caller to ask
/// of the chain, or the refusal with the offset and the kind.
pub fn structure_roots(bytes: &[u8]) -> Result<Vec<(u64, String)>, Refusal> {
    match verify_stream_structure(bytes, EveryRoot, None) {
        Ok(Verdict::Valid { roots, .. }) => Ok(roots
            .iter()
            .map(|(height, root)| (*height, display_hex(root)))
            .collect()),
        Ok(Verdict::Invalid { offset, reason, .. }) => Err(Refusal { offset, reason }),
        // The structure-only reader runs no script, and a slice never fails.
        Ok(Verdict::SpendRefused { .. }) | Err(_) => {
            unreachable!("the structure check of held bytes ran a script or failed to read")
        }
    }
}

/// The streaming door, then the in-memory [`Beef`] for a caller that builds
/// with one. The refusal is for invalid bytes and reads
/// `invalid BEEF at byte <offset>: <kind and its data>`. `_door` names where
/// the reader stands; nothing of it is consulted.
pub fn parse_beef(bytes: &[u8], _door: &BeefLimits) -> bsv_rs::Result<Beef> {
    read_beef(bytes).map_err(|refusal| refused(&refusal))?;
    Beef::from_binary(bytes)
}

/// Link from the door's parse, keeping the SDK's exact target selection:
/// explicit txid, then atomic txid, then wire-last.
pub fn transaction_from_beef(
    bytes: &[u8],
    txid: Option<&str>,
    door: &BeefLimits,
) -> bsv_rs::Result<Transaction> {
    let parsed = parse_beef(bytes, door)?;
    let target = txid
        .map(str::to_string)
        .or_else(|| parsed.atomic_txid.clone())
        .or_else(|| parsed.txs.last().map(bsv_rs::transaction::BeefTx::txid))
        .ok_or_else(|| bsv_rs::Error::TransactionError("No transactions in BEEF".into()))?;
    parsed.find_atomic_transaction(&target).ok_or_else(|| {
        bsv_rs::Error::TransactionError(format!("Transaction {target} not found in BEEF"))
    })
}

/// The BUMP index the BEEF gives `txid`'s own entry (the last entry of that
/// txid, as the in-memory index resolves a repeat), in one streaming pass.
/// `Ok(None)`: no such entry, or an entry with no proof.
fn own_bump_index(bytes: &[u8], txid: &str) -> Result<Option<u64>, Refusal> {
    let mut found = None;
    fold_held(bytes, |element| match element {
        Element::Tx {
            txid: id,
            bump_index,
            ..
        } if display_hex(&id) == txid => found = Some(bump_index),
        Element::TxidOnly { txid: id, .. } if display_hex(&id) == txid => found = Some(None),
        _ => {}
    })?;
    Ok(found.flatten())
}

/// Whether the BEEF carries a proof for `txid`'s OWN entry (its bump index,
/// never an ancestor's). Streaming: one element in hand. Invalid bytes, an
/// absent entry and a proofless entry are all `false` (fail closed).
pub fn has_proof(bytes: &[u8], txid: &str) -> bool {
    matches!(own_bump_index(bytes, txid), Ok(Some(_)))
}

/// The BUMP the BEEF names for `txid`'s OWN entry, never the one
/// `Beef::find_bump` would pick (the earliest BUMP that carries the txid).
/// Two streaming passes over the held bytes, because the BUMPs precede the
/// entry that names one; each pass has one element in hand and the one BUMP
/// wanted is the only thing kept. `Err`: invalid bytes, named. `Ok(None)`: no
/// entry, a proofless entry, or an index that names no BUMP.
pub fn own_proof(bytes: &[u8], txid: &str) -> Result<Option<MerklePath>, Refusal> {
    let Some(wanted) = own_bump_index(bytes, txid)? else {
        return Ok(None);
    };
    let (mut seen, mut own) = (0u64, None);
    fold_held(bytes, |element| {
        if let Element::Bump(bump) = element {
            if seen == wanted {
                own = bump.to_merkle_path().ok();
            }
            seen += 1;
        }
    })?;
    Ok(own)
}

/// [`own_proof`], invalid bytes read as no proof (fail closed).
pub fn own_bump(bytes: &[u8], txid: &str) -> Option<MerklePath> {
    own_proof(bytes, txid).ok().flatten()
}

/// A courier's or a push's proof field: the wire bound is checked on the hex
/// before the decode, then the proof is parsed. The bound is the field's
/// (see [`COURIER_PROOF_MAX_BYTES`]), not a verdict on a BEEF.
pub fn merkle_path_from_hex(hex: &str, max_bytes: usize) -> bsv_rs::Result<MerklePath> {
    check_size(hex.len(), max_bytes.saturating_mul(2), "merkle proof hex")?;
    proof_from_hex(hex)
}

/// A proof read back from a store or handed by a GASP peer: read whatever its
/// length, refused only if the bytes do not parse.
pub fn proof_from_hex(hex: &str) -> bsv_rs::Result<MerklePath> {
    let bytes = bsv_rs::primitives::from_hex(hex)?;
    MerklePath::from_binary(&bytes)
}

/// Always `false` since NL-6: no door of this module refuses by a limit, so
/// no parse error is a breach. Kept exported for the callers that still ask.
pub fn is_limit_breach(_e: &bsv_rs::Error) -> bool {
    false
}

/// A length check for a WIRE field or a non-BEEF body (a courier's proof hex,
/// the ARC callback's JSON). No reader here applies it to a BEEF.
pub fn check_size(size: usize, max_bytes: usize, kind: &str) -> bsv_rs::Result<()> {
    if size > max_bytes {
        return Err(bsv_rs::Error::BeefError(format!(
            "{kind} of {size} bytes is over max_bytes {max_bytes}"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/support/beef_doors.rs"]
mod door_shapes;

#[cfg(test)]
mod door_pin {
    use super::door_shapes as shapes;
    use super::*;

    /// `0200BEEF`, one BUMP at height 800,000 whose tree-height byte is 65.
    fn tree_height_65() -> Vec<u8> {
        let mut bin = vec![0x02, 0x00, 0xbe, 0xef, 0x01];
        bin.extend_from_slice(&[0xfe, 0x00, 0x35, 0x0c, 0x00]);
        bin.push(65);
        bin
    }

    #[test]
    fn a_refusal_names_the_offset_and_the_kind() {
        let refusal = read_beef(&tree_height_65()).unwrap_err();
        assert_eq!(
            (refusal.offset, refusal.kind()),
            (10, Kind::TreeHeightOver64)
        );
        let refusal = read_beef(&[0xde, 0xad, 0xbe, 0xef]).unwrap_err();
        assert_eq!((refusal.offset, refusal.kind()), (0, Kind::BadVersion));
        let e = parse_beef(&tree_height_65(), &SUBMIT_BEEF_LIMITS).unwrap_err();
        assert_eq!(
            e.to_string()
                .matches("invalid BEEF at byte 10: TreeHeightOver64")
                .count(),
            1,
            "{e}"
        );
    }

    #[test]
    fn a_claimed_count_is_refused_for_the_bytes_never_as_a_count() {
        // The prefix claims 300 BUMPs and carries none.
        let bin = [0x02, 0x00, 0xbe, 0xef, 0xfd, 0x2c, 0x01];
        let refusal = read_beef(&bin).unwrap_err();
        // The stream ends where the first BUMP's height varint would start.
        assert_eq!((refusal.offset, refusal.kind()), (7, Kind::BadVarint));
        let e = parse_beef(&bin, &SUBMIT_BEEF_LIMITS).unwrap_err();
        assert!(!e.to_string().contains("max_"), "{e}");
        assert!(!is_limit_breach(&e));
    }

    /// A source that hands out at most seven bytes a read.
    struct Drip<'a>(&'a [u8]);
    impl Read for Drip<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.0.len().min(buf.len()).min(7);
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    /// A source that fails after its bytes.
    struct Fails<'a>(&'a [u8]);
    impl Read for Fails<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.0.is_empty() {
                return Err(std::io::Error::other("the object body broke"));
            }
            let n = self.0.len().min(buf.len());
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    #[test]
    fn a_source_is_read_as_the_held_bytes_are_one_element_in_hand() {
        for (bytes, _) in [
            shapes::body(1),
            shapes::transactions(2_048),
            shapes::bumps(2_048),
            shapes::sized_body(SUBMIT_BODY_MAX_BYTES + 1),
        ] {
            let held = read_beef(&bytes).unwrap();
            assert_eq!(held.bytes, bytes.len() as u64);
            let (mut elements, mut largest) = (0u64, 0u64);
            let streamed = fold_beef(Drip(&bytes), |element| {
                elements += 1;
                largest = largest.max(element.wire_len());
            })
            .unwrap();
            assert_eq!(streamed, held);
            assert_eq!(elements, held.bumps + held.txs);
            // What the reader held at once: one element, never the frame.
            assert!(largest < held.bytes);
        }
        // A source's failure is the source's, never a refusal of the bytes.
        let (bytes, _) = shapes::body(1);
        let cut = &bytes[..bytes.len() - 3];
        assert!(matches!(
            fold_beef(Fails(cut), |_| {}),
            Err(StreamError::Io(_))
        ));
        assert_eq!(read_beef(cut).unwrap_err().kind(), Kind::Truncated);
    }

    /// The streaming readers answer as the in-memory index does.
    #[test]
    fn the_own_proof_is_the_in_memory_index_s() {
        let (stub_only, _) = shapes::transactions(3);
        for (bytes, id) in [
            shapes::body(1),
            shapes::transactions(600),
            shapes::bumps(600),
        ] {
            let beef = Beef::from_binary(&bytes).unwrap();
            let entry = beef.find_txid(&id).unwrap();
            let in_memory = entry.bump_index().and_then(|i| beef.bumps.get(i));
            assert!(has_proof(&bytes, &id));
            assert_eq!(
                own_bump(&bytes, &id).map(|b| b.to_hex()),
                in_memory.map(MerklePath::to_hex)
            );
            // A txid-only entry and an absent txid carry no proof.
            let stub = format!("{:064x}", 1);
            assert!(!has_proof(&stub_only, &stub));
            assert!(matches!(own_proof(&stub_only, &stub), Ok(None)));
            assert!(matches!(own_proof(&bytes, &"ee".repeat(32)), Ok(None)));
        }
        // Invalid bytes: named by `own_proof`, no proof for the other two.
        for (bytes, offset, _) in shapes::invalid() {
            let id = "11".repeat(32);
            assert_eq!(own_proof(&bytes, &id).unwrap_err().offset, offset as u64);
            assert!(!has_proof(&bytes, &id) && own_bump(&bytes, &id).is_none());
        }
    }

    /// The own entry's BUMP, not the earliest BUMP that carries the txid.
    #[test]
    fn the_own_proof_follows_the_bump_index_not_the_earliest_carrier() {
        let (bytes, id) = shapes::body(1);
        let mut beef = Beef::from_binary(&bytes).unwrap();
        beef.bumps
            .push(MerklePath::from_coinbase_txid(&id, 900_000));
        beef.find_txid_mut(&id).unwrap().set_bump_index(Some(1));
        let moved = beef.to_binary();
        assert_eq!(own_bump(&moved, &id).unwrap().block_height, 900_000);
        assert_eq!(beef.find_bump(&id).unwrap().block_height, 800_000);
    }

    /// A stored or a peer's proof is read whatever its length; the courier's
    /// and the push's FIELD keeps its wire bound.
    #[test]
    fn a_proof_over_the_wire_bound_is_read_where_no_wire_bounds_it() {
        let (at, over) = shapes::proof_boundaries();
        assert_eq!(over.to_binary().len(), COURIER_PROOF_MAX_BYTES + 1);
        assert_eq!(
            proof_from_hex(&over.to_hex()).unwrap().to_hex(),
            over.to_hex()
        );
        assert_eq!(proof_from_hex(&at.to_hex()).unwrap().to_hex(), at.to_hex());
        assert!(proof_from_hex("zz").is_err());
        for bound in [COURIER_PROOF_MAX_BYTES, PUSH_PROOF_MAX_BYTES] {
            assert!(merkle_path_from_hex(&at.to_hex(), bound).is_ok());
            assert!(merkle_path_from_hex(&over.to_hex(), bound).is_err());
        }
    }

    /// The structure check (NL-6e): each BUMP's height and root for the
    /// chain, or the reader's refusal with its offset and kind; the wire's
    /// order is read as written.
    #[test]
    fn the_structure_check_answers_the_roots_or_the_refusal() {
        let (bytes, id) = shapes::body(1);
        let beef = Beef::from_binary(&bytes).unwrap();
        let root = beef
            .find_bump(&id)
            .unwrap()
            .compute_root(Some(&id))
            .unwrap();
        assert_eq!(structure_roots(&bytes).unwrap(), vec![(800_000, root)]);
        for (bytes, offset, _) in shapes::no_input() {
            let refusal = structure_roots(&bytes).unwrap_err();
            assert_eq!(
                (refusal.offset, refusal.kind()),
                (offset as u64, Kind::NoInputs)
            );
        }
        for (bytes, offset, kind) in shapes::invalid() {
            let refusal = structure_roots(&bytes).unwrap_err();
            assert_eq!(refusal.offset, offset as u64, "{kind}");
        }
        // A child written before its parent: the in-memory check sorts and
        // accepts it; the reader refuses the child's input at its first byte
        // (after the version and the input count).
        let parent = Transaction::from_binary(beef.txs[0].raw_tx().unwrap()).unwrap();
        let mut child = Transaction::new();
        let mut input = bsv_rs::transaction::TransactionInput::new(id.clone(), 0);
        input.unlocking_script = Some(bsv_rs::script::UnlockingScript::new());
        child.inputs.push(input);
        child.outputs.push(parent.outputs[0].clone());
        let mut wire = vec![0x01, 0x00, 0xbe, 0xef, 0x01];
        wire.extend_from_slice(&beef.bumps[0].to_binary());
        wire.push(0x02);
        let child_at = wire.len();
        wire.extend_from_slice(&child.to_binary());
        wire.push(0x00);
        wire.extend_from_slice(&parent.to_binary());
        wire.extend_from_slice(&[0x01, 0x00]);
        assert!(Beef::from_binary(&wire).unwrap().is_valid(false));
        let refusal = structure_roots(&wire).unwrap_err();
        assert_eq!(
            (refusal.offset, refusal.kind()),
            (child_at as u64 + 5, Kind::InputNamesNoElement)
        );
    }

    #[test]
    fn an_empty_beef_is_read() {
        let read = read_beef(&Beef::new().to_binary()).unwrap();
        assert_eq!((read.bumps, read.txs, read.bytes), (0, 0, 6));
        assert_eq!(read.subject, None);
    }
}
