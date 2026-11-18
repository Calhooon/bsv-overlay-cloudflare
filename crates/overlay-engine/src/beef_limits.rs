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

pub use bsv_rs::transaction::BeefLinkLimits;

pub use bsv_rs::transaction::beef_stream::StreamError as DoorError;
pub use bsv_rs::transaction::{Kind, Reason, Refusal};

/// A door's name. Until NL-6 these three numbers were refusals; they are not
/// read by any function of this module now. bsv-rs 0.4.0's own `BeefLimits`
/// lost `max_bytes` and refuses nothing, so the struct lives here for the
/// callers that still name a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeefLimits {
    /// The former cap on transactions in one BEEF; a name only, nothing refuses by it.
    pub max_txs: usize,
    /// The former cap on merkle paths (BUMPs) in one BEEF; a name only.
    pub max_bumps: usize,
    /// The former cap on a BEEF's bytes; a name only.
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
        // SDK 0.5.0 contextual checks run only with spend checks enabled;
        // verify_stream_structure disables them (BeefIndex::structure_only).
        Ok(Verdict::Unverifiable { .. }) => {
            unreachable!("the structure-only reader requested mining context")
        }
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


/// The subject a read names: the explicit txid, then the atomic txid, then,
/// for a read that names none, the house's ONE subject rule
/// ([`crate::subject::subject_txid_of`]: the unique tip, else the reference's
/// sorted-last), never the wire-last entry (a BEEF whose last entry is an
/// ancestor, a 0.7.2 build's stored order among them, names its subject by
/// its graph, not by its order).
fn target_of(parsed: &mut Beef, txid: Option<&str>) -> bsv_rs::Result<String> {
    if let Some(txid) = txid {
        return Ok(txid.to_string());
    }
    if let Some(atomic) = parsed.atomic_txid.clone() {
        return Ok(atomic);
    }
    crate::subject::subject_txid_of(parsed)
        .ok_or_else(|| bsv_rs::Error::TransactionError("No transactions in BEEF".into()))
}

/// The engine's link bound IN HEAP: the most heap one link of a BEEF's input
/// ANCESTRY may hold (16 MiB, native): every copy the link makes of a source
/// (its full link and its memo stub), every repeated stub (one per input past
/// the first that spends a source), and every BUMP those copies carry. Not
/// counted: the subject's own clone and its own BUMP, and the link's index of
/// the BEEF's level-0 leaves, one copy of what the body holds (as
/// [`subject_from_beef`] makes), linear in the body as the door's own parse
/// is. Every linking read of this
/// repository ([`transaction_from_beef`], [`linked`]) checks
/// [`link_heap_estimate`] against it before bsv-rs links anything, and passes
/// [`engine_link_limits`] as the belt. A link past it is
/// `Err(bsv_rs::Error::BeefResourceLimit)` ([`is_link_bound`]), which each
/// door classifies by its own rule.
///
/// Where it sits in the isolate's arithmetic (CLAUDE.md, "The default's
/// arithmetic"): on an ungated submit the link is held while the engine's walk
/// runs, so 24 MB of request copies, the walk's 48 MiB and this 16 MiB are 88
/// MB of the 128 MB isolate beside the linear copies; on the gated arm the
/// door's walk has returned before the engine's submit links. wasm32 holds
/// less than native for each charge (its pointers and `usize` are 4 bytes; an
/// output is 80 bytes there against 136 natively), so the native figure bounds
/// it there too.
pub const LINK_HEAP_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// The heap a linked transaction holds per byte of its wire form, at most, on
/// the shapes measured (native, release, `tests/p0811_link_heap.rs`): empty
/// locks 16.2, one-byte locks 14.8, P2PKH 5.8, an input-heavy parent 7.1,
/// large locks 2.3. bsv-rs's own meter charges each repeated stub its wire
/// bytes, so [`LINK_WIRE_MAX_BYTES`] is the heap bound divided by this.
pub const LINK_HEAP_PER_WIRE_BYTE: u64 = 17;

/// The wire figure handed to bsv-rs's meter ([`engine_link_limits`]):
/// `LINK_HEAP_MAX_BYTES / LINK_HEAP_PER_WIRE_BYTE`, 986,895 bytes of repeated
/// stubs. It declines before [`link_heap_estimate`] does for every lock
/// longer than a byte (a parent of 300 outputs with 50-byte locks: 56 spent
/// link, 57 do not, the estimate there 7.2 MB); the estimate is what bounds
/// empty and one-byte locks and the BUMPs the stubs carry, which this meter
/// does not charge.
pub const LINK_WIRE_MAX_BYTES: u64 = LINK_HEAP_MAX_BYTES / LINK_HEAP_PER_WIRE_BYTE;

/// The estimate's charges, native upper bounds, each pinned above the measured
/// heap by `tests/p0811_link_heap.rs`: a transaction's struct and its tables,
/// an input, an output, a byte of the transaction's wire form (its scripts'
/// raw bytes and their copies), a leaf of an attached BUMP, and a level-0 leaf
/// of the link's index of the BEEF's BUMPs.
pub const LINK_CHARGE_TX: u64 = 512;
/// An input's charge ([`LINK_CHARGE_TX`]).
pub const LINK_CHARGE_INPUT: u64 = 320;
/// An output's charge ([`LINK_CHARGE_TX`]).
pub const LINK_CHARGE_OUTPUT: u64 = 176;
/// A wire byte's charge ([`LINK_CHARGE_TX`]).
pub const LINK_CHARGE_WIRE_BYTE: u64 = 4;
/// A BUMP leaf's charge ([`LINK_CHARGE_TX`]).
pub const LINK_CHARGE_BUMP_LEAF: u64 = 128;
/// A level-0 leaf's charge in the link's BUMP index ([`LINK_CHARGE_TX`]).
pub const LINK_CHARGE_INDEX_LEAF: u64 = 64;

/// The most BUMP nodes an ancestry [`link_heap_estimate`] admits can carry,
/// and the default of the door's PROOF limb
/// (`DoorBudget::max_proof_nodes`, lane INTAKE-BOUND).
///
/// The derivation, from the estimate as it stands: every BUMP a source of the
/// subject's ancestry carries is charged in `beyond` at
/// [`LINK_CHARGE_BUMP_LEAF`] a node (every level) TWICE, once for the
/// source's full link and once for its memo stub (a shared BUMP once more for
/// each further source it proves, and each repeated stub once more), beside
/// the sources' own weight; `beyond` is bounded by [`LINK_HEAP_MAX_BYTES`]. So
/// an ancestry the estimate admits carries fewer than
/// `LINK_HEAP_MAX_BYTES / (2 * LINK_CHARGE_BUMP_LEAF)` nodes in all (65,536):
/// a body carrying more in its ancestry is declined at the link today, after
/// it was parsed. The level-0 index charge ([`LINK_CHARGE_INDEX_LEAF`]) is
/// the estimate's `linear` part, which no bound reads, so it does not enter.
///
/// What the figure does NOT cover, stated: a PROVEN subject's own BUMP (its
/// link walks no ancestry; the estimate counts it as linear and bsv-rs's heap
/// meter bounds its clone), and BUMPs no source of the ancestry uses. Both
/// are now bounded by the proof limb at the frame.
pub const LINK_ANCESTRY_PROOF_NODES_MAX: u64 = LINK_HEAP_MAX_BYTES / (2 * LINK_CHARGE_BUMP_LEAF);

/// The heap figure handed to bsv-rs's own heap meter ([`engine_link_limits`];
/// bsv-rs 0.8.12, GHSA-33fm-jcm5-4m3m: `BeefLinkLimits::max_link_heap_bytes`),
/// 56 MiB, named here so the figure is the engine's own and never whatever
/// bsv-rs's default holds. On bsv-rs 0.8.12 it equals what
/// `BeefLinkLimits::default()` holds for `max_link_heap_bytes`.
///
/// bsv-rs charges, per clone, the stub's wire bytes plus `size_of` of its
/// transaction (248 bytes natively), inputs (168) and outputs (136), and 104
/// bytes a BUMP leaf; [`link_heap_estimate`] charges each of those at least as
/// much ([`LINK_CHARGE_TX`] 512, [`LINK_CHARGE_INPUT`] 320,
/// [`LINK_CHARGE_OUTPUT`] 176, 4 a wire byte, [`LINK_CHARGE_BUMP_LEAF`] 128)
/// and charges the full links bsv-rs does not. So on the ancestry bsv-rs's
/// count is under the estimate's `beyond`, which [`LINK_HEAP_MAX_BYTES`]
/// bounds: this figure is 3.5 times the most an admitted ancestry counts (the
/// largest measured, 7.5 MB, five unproven parents, `tests/p0812_link_heap_meter.rs`).
/// What it alone bounds is the clone of a PROVEN subject's own BUMP, which the
/// estimate counts as linear: such a link is declined past 564,617 leaves
/// (104 bytes a leaf; a body of about 21 MB), where the clone alone would hold
/// the figure beside the body's own parse.
pub const LINK_METER_HEAP_MAX_BYTES: u64 = 56 * 1024 * 1024;

/// The link limits handed to bsv-rs at every linking read: its wire meter at
/// [`LINK_WIRE_MAX_BYTES`] and its heap meter at [`LINK_METER_HEAP_MAX_BYTES`].
/// A trip of either is `Err(bsv_rs::Error::BeefResourceLimit)`
/// ([`is_link_bound`]).
#[must_use]
pub fn engine_link_limits() -> BeefLinkLimits {
    BeefLinkLimits::default()
        .with_max_link_bytes(Some(LINK_WIRE_MAX_BYTES))
        .with_max_link_heap_bytes(Some(LINK_METER_HEAP_MAX_BYTES))
}

/// An upper bound on the heap bsv-rs's link of a transaction holds, in two
/// parts ([`link_heap_estimate`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LinkHeap {
    /// One copy of what the body holds: the subject's clone and its own BUMP,
    /// and the link's index of the BEEF's level-0 leaves.
    pub linear: u64,
    /// The ancestry: every copy of a source and of its BUMP, every repeated
    /// stub. [`LINK_HEAP_MAX_BYTES`] bounds this part.
    pub beyond: u64,
}

impl LinkHeap {
    /// Both parts: the estimate of the whole link.
    #[must_use]
    pub fn total(self) -> u64 {
        self.linear.saturating_add(self.beyond)
    }
}

/// An upper bound on the heap bsv-rs's link of `txid` out of `beef` holds,
/// computed before anything is linked (its walk is bsv-rs's own,
/// `Beef::add_input_proof`: a transaction a BUMP proves is not descended, a
/// source the BEEF does not carry is not linked). Each copy of a transaction
/// is charged its inputs, outputs and wire bytes, each copy of a BUMP its
/// leaves. The count stops once `beyond` is past `stop_at`. Each source's wire
/// form is read once (one transient copy of one transaction); no script is
/// parsed.
///
/// Its premise: the BEEF's scripts are not chunk-parsed before the link. A
/// script parsed in place is cloned with its chunks (about 32 bytes a
/// one-byte opcode natively, against the 4 a wire byte is charged here); no
/// production caller parses an entry of a BEEF before linking it.
#[must_use]
pub fn link_heap_estimate(beef: &Beef, txid: &str, stop_at: u64) -> LinkHeap {
    use std::collections::{HashMap, HashSet};
    let mut bump_of: HashMap<&str, &MerklePath> = HashMap::new();
    let mut index_leaves: u64 = 0;
    for bump in &beef.bumps {
        for leaf in bump.path.first().into_iter().flatten() {
            if let Some(hash) = leaf.hash.as_deref() {
                index_leaves += 1;
                bump_of.entry(hash).or_insert(bump);
            }
        }
    }
    let tx_weight = |tx: &Transaction| -> u64 {
        LINK_CHARGE_TX
            .saturating_add(LINK_CHARGE_INPUT.saturating_mul(tx.inputs.len() as u64))
            .saturating_add(LINK_CHARGE_OUTPUT.saturating_mul(tx.outputs.len() as u64))
            .saturating_add(LINK_CHARGE_WIRE_BYTE.saturating_mul(tx.to_binary().len() as u64))
    };
    let bump_weight = |bump: &MerklePath| -> u64 {
        LINK_CHARGE_BUMP_LEAF.saturating_mul(bump.path.iter().map(|l| l.len() as u64).sum::<u64>())
    };
    let mut heap = LinkHeap {
        linear: LINK_CHARGE_INDEX_LEAF.saturating_mul(index_leaves),
        beyond: 0,
    };
    let copy_bump = |heap: &mut LinkHeap, bump: Option<&MerklePath>| {
        heap.beyond = heap.beyond.saturating_add(bump.map_or(0, bump_weight));
    };
    let Some(subject) = beef.find_txid(txid).and_then(|e| e.tx()) else {
        return heap;
    };
    let subject_bump = subject
        .merkle_path
        .as_ref()
        .or_else(|| bump_of.get(txid).copied());
    heap.linear = heap
        .linear
        .saturating_add(tx_weight(subject))
        .saturating_add(subject_bump.map_or(0, bump_weight));
    if subject_bump.is_some() {
        return heap;
    }
    let mut stubs: HashMap<&str, (u64, Option<&MerklePath>)> = HashMap::new();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut stack: Vec<&Transaction> = vec![subject];
    while let Some(tx) = stack.pop() {
        for input in &tx.inputs {
            if heap.beyond > stop_at {
                return heap;
            }
            let Some(src) = input.source_txid.as_deref() else {
                continue;
            };
            if let Some((weight, bump)) = stubs.get(src).copied() {
                heap.beyond = heap.beyond.saturating_add(weight);
                copy_bump(&mut heap, bump);
                continue;
            }
            if !seen.insert(src) {
                continue;
            }
            let Some(source) = beef.find_txid(src).and_then(|e| e.tx()) else {
                continue;
            };
            let bump = source
                .merkle_path
                .as_ref()
                .or_else(|| bump_of.get(src).copied());
            let weight = tx_weight(source);
            stubs.insert(src, (weight, bump));
            heap.beyond = heap.beyond.saturating_add(weight.saturating_mul(2));
            copy_bump(&mut heap, bump);
            copy_bump(&mut heap, bump);
            if bump.is_none() {
                stack.push(source);
            }
        }
    }
    heap
}

/// THE one linking read of this repository: [`link_heap_estimate`]'s
/// `beyond` against [`LINK_HEAP_MAX_BYTES`], then bsv-rs's link under
/// [`engine_link_limits`]. `Ok(None)` when the BEEF does not carry `txid`'s
/// body; `Err` with `BeefResourceLimit` ([`is_link_bound`]) when any bound
/// declines (the estimate, bsv-rs's wire meter or its heap meter).
fn link_bounded(beef: &Beef, txid: &str) -> bsv_rs::Result<Option<Transaction>> {
    let heap = link_heap_estimate(beef, txid, LINK_HEAP_MAX_BYTES);
    if heap.beyond > LINK_HEAP_MAX_BYTES {
        return Err(bsv_rs::Error::BeefResourceLimit(format!(
            "linking {txid}'s ancestry would hold more than {LINK_HEAP_MAX_BYTES} bytes of heap \
             (estimated past {}); the engine's link bound",
            heap.beyond
        )));
    }
    beef.find_atomic_transaction_limited(txid, &engine_link_limits())
}

/// Link from the door's parse: the explicit txid, then the atomic txid, then
/// the subject rule (`target_of`).
///
/// The link runs under the engine's link bound ([`LINK_HEAP_MAX_BYTES`] of
/// heap, `link_bounded`). A bound's trip is
/// `Err(bsv_rs::Error::BeefResourceLimit)` ([`is_link_bound`]), never an
/// invalid BEEF and never an absent txid: each caller classifies it by the
/// rule of its own door. A caller that reads the subject's own fields alone
/// reads it through [`subject_from_beef`], which links nothing.
pub fn transaction_from_beef(
    bytes: &[u8],
    txid: Option<&str>,
    door: &BeefLimits,
) -> bsv_rs::Result<Transaction> {
    let mut parsed = parse_beef(bytes, door)?;
    let target = target_of(&mut parsed, txid)?;
    link_bounded(&parsed, &target)?.ok_or_else(|| {
        bsv_rs::Error::TransactionError(format!("Transaction {target} not found in BEEF"))
    })
}

/// The SUBJECT alone, its input ancestry NOT linked (bsv-rs 0.8.11's
/// `Transaction::from_beef_subject`, through this module's door): the same
/// target selection as [`transaction_from_beef`], the subject's own fields
/// (txid, inputs' outpoints and sequences, outputs, lock time) and its own
/// proof, attached as the link attaches it (`Beef::find_bump` of its txid).
/// Each input's `source_transaction` is `None`. No link bound applies: there
/// is nothing to re-materialize, so no size of an honest BEEF is refused here.
pub fn subject_from_beef(
    bytes: &[u8],
    txid: Option<&str>,
    door: &BeefLimits,
) -> bsv_rs::Result<Transaction> {
    let mut parsed = parse_beef(bytes, door)?;
    let target = target_of(&mut parsed, txid)?;
    let mut tx = parsed
        .find_txid(&target)
        .and_then(|entry| entry.tx().cloned())
        .ok_or_else(|| {
            bsv_rs::Error::TransactionError(format!("Transaction {target} not found in BEEF"))
        })?;
    if tx.merkle_path.is_none() {
        tx.merkle_path = parsed.find_bump(&tx.id()).cloned();
    }
    Ok(tx)
}

/// `txid` linked out of a parsed BEEF under the engine's link bound
/// (`link_bounded`): `Ok(None)` when the BEEF does not carry its body,
/// `Err` when the bound declines ([`is_link_bound`]).
/// `Beef::find_atomic_transaction` answers `None` for both, so no reader of
/// this crate calls it.
pub fn linked(beef: &Beef, txid: &str) -> bsv_rs::Result<Option<Transaction>> {
    link_bounded(beef, txid)
}

/// Whether `e` is the link bound's trip: the engine's estimate, bsv-rs's wire
/// meter (GHSA-gvmh-f5gf-5vjj) or its heap meter (bsv-rs 0.8.12,
/// GHSA-33fm-jcm5-4m3m), all one class: a resource decline, never a verdict on
/// the bytes.
#[must_use]
pub fn is_link_bound(e: &bsv_rs::Error) -> bool {
    matches!(e, bsv_rs::Error::BeefResourceLimit(_))
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

/// A proof read back from a store, handed by a GASP peer, or fetched from a
/// courier or pushed (through [`merkle_path_from_hex`]): read whatever its
/// length, refused if the bytes do not parse, and NOT parsed when its nodes
/// are past the proof limb's default
/// ([`LINK_ANCESTRY_PROOF_NODES_MAX`], counted from the path's frame by
/// `stream_sizing::path_proof_limb` before its root is computed; lane
/// INTAKE-BOUND): `Err(BeefResourceLimit)` with the stop's words.
pub fn proof_from_hex(hex: &str) -> bsv_rs::Result<MerklePath> {
    let bytes = bsv_rs::primitives::from_hex(hex)?;
    crate::stream_sizing::path_proof_limb(&bytes, LINK_ANCESTRY_PROOF_NODES_MAX)
        .map_err(|stop| bsv_rs::Error::BeefResourceLimit(stop.to_string()))?;
    MerklePath::from_binary(&bytes)
}

/// Whether `e` is the proof limb's stop of [`proof_from_hex`] (a path past
/// [`LINK_ANCESTRY_PROOF_NODES_MAX`], not parsed), never a parse fault.
pub fn is_proof_limb(e: &bsv_rs::Error) -> bool {
    matches!(e, bsv_rs::Error::BeefResourceLimit(why) if why.contains(crate::stream_sizing::PROOF_LIMB_WORDS))
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
