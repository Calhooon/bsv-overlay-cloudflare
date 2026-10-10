//! The script door's walk over the STREAM (bsv-low #585, door 1).
//!
//! [`crate::engine::Engine::verify_scripts_only`] is the reference's
//! `tx.verify('scripts only')` run before a broadcast: every unproven
//! transaction the subject reaches has its inputs executed, a proven one is
//! trusted as it stands. Until #585 it parsed the whole BEEF into a `Beef`
//! and stopped at four bounds on the BODY (unproven transactions, inputs per
//! transaction, bytes per transaction, and the parse's own size and counts).
//! A valid BEEF is never refused, or left unjudged, for its size or its
//! counts: those bounds are gone.
//!
//! The walk is three reads of the bytes the caller already holds:
//!
//! 0. THE SIZING ([`crate::stream_sizing`], the doors lens E585-D12-L3). The
//!    frame's lengths and counts, read with no allocation and no hash, give
//!    what the two reads below will hold ([`DOOR_CHARGES`]). Past the
//!    budget's memory limb the stream is not opened: "the network judges".
//! 1. THE INDEX. bsv-rs 0.4.0's `BeefStream` cuts the body into its elements
//!    one at a time. Of each raw transaction the door keeps its txid, the
//!    offset of its bytes and their length; of each BUMP, the txids its level
//!    0 carries. Nothing else of an element outlives its step.
//! 2. THE WALK, from the subject, the order of the walk before #585 (a stack:
//!    an input's source is walked after the transaction that spends it). A
//!    transaction is read where the index says it lies, in the caller's
//!    bytes: no transaction is copied out of the body, and a source's output
//!    is read in place by the offset of that output.
//!
//! The door does not use `BeefIndex`: it serves no lookup and no offset, and
//! its fold refuses a body the door has always walked (an input that names no
//! EARLIER element, a transaction no later one spends). The door asks less:
//! which bytes are a transaction, and which txids a BUMP carries.
//!
//! TWO LIMBS ([`crate::engine::DoorBudget`]), each a budget whose breach is
//! the answer the door always gave, "the network judges", never a refusal.
//!
//! THE WORK: the estimate of what the interpreter will hash, push and verify,
//! charged per input before anything runs. A signature check is charged its
//! transaction's bytes or the budget's floor, whichever is more (the doors
//! lens E585-D12-M1: the digest follows the transaction, the EC verification
//! does not, and the counts #585 removed were what bounded the verifications
//! of small transactions). Every other cost of the walk is linear in the
//! body: each element is cut once, each judged transaction is laid out once,
//! each source's outputs are located once, and an input that checks no
//! signature is run with no copy of its transaction (the interpreter reads
//! the other inputs and the outputs only to build a signature's digest; the
//! copy of them per input was what the inputs bound held down, and an input
//! that DOES check a signature is charged at least its transaction's bytes
//! per check, which covers the copy).
//!
//! THE MEMORY: what the door holds beside the body follows the body's element
//! COUNT and a BUMP's LEAVES, not its bytes (an index entry per transaction,
//! a `Leaf` and four table entries per leaf while the stream computes a
//! root). It is estimated by the sizing read and never spent past the limb.
//! And per input, what the SCRIPTS cost once parsed (the delta lens
//! E585-D12-DELTA-M1): bsv-rs cuts a script into one 32 byte record per
//! chunk, several times over, and the interpreter can push up to three
//! stack entries per opcode, so a script of one-byte opcodes is about 32 to
//! 230 bytes of heap a byte. That is charged from the script's chunk count
//! ([`script_parse_charge`]), beside the frame's estimate, BEFORE the input's
//! scripts are parsed.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use bsv_rs::primitives::bsv::sighash::{TxInput, TxOutput};
use bsv_rs::script::{LockingScript, Script, Spend, SpendParams, UnlockingScript};
use bsv_rs::transaction::beef_stream::{display_hex, BeefStream, Element, Hash32};

use crate::engine::{script_census, DoorBudget, DoorLimb, EngineError, WalkStats};
use crate::stream_sizing::{self, Admission, StreamCharges};

/// What the door holds per thing of a BEEF, for the sizing read: the stream's
/// own element ([`StreamCharges::STREAM`]) and the door's.
///
/// KEPT, per raw transaction: its entry in the index (a bucket of 49 bytes in
/// a table that doubles: three tables' worth at the fullest load, 168), and,
/// charged to every transaction though only a walked one pays them, its mark
/// in the walk's `seen` (114) and its entry in the walk's `sources` (168):
/// 450. Per input: the walk's queue (32 bytes, a growing buffer: 96). Per
/// output: a source's output offsets (a word, a growing buffer: 24). Per txid
/// a BUMP proves: its bucket in the proven set (33 bytes, as the index: 114).
///
/// IN HAND, a raw transaction: the heavier of the stream's element (3 bytes a
/// byte, 192 an input, 72 an output) and the walk's own view of it, which is
/// the layout (an input's place 56 bytes and an output's 24, growing buffers:
/// 168 and 72) beside the digest view of an input that checks a signature
/// (the scripts copied once for the view and twice for the interpreter, and a
/// record of 64 bytes an input and 32 an output three times: 3 bytes a byte,
/// 192 an input, 96 an output), and one more copy for the interpreter's own
/// serialization of the outputs: 4 bytes a byte, 360 an input, 168 an output.
/// A BUMP leaf is the stream's. What an input's scripts hold once PARSED is
/// not a frame charge: it is charged per input ([`script_parse_charge`]).
pub(crate) const DOOR_CHARGES: StreamCharges = StreamCharges {
    kept_tx: 450,
    kept_input: 96,
    kept_output: 24,
    kept_proven: 114,
    tx_byte: 4,
    tx_input: 360,
    tx_output: 168,
    bump_leaf: StreamCharges::STREAM.bump_leaf,
};

/// What one input's two scripts hold once parsed and run, per CHUNK of each
/// (the delta lens E585-D12-DELTA-M1). bsv-rs 0.4.0 cuts a script into one
/// `ScriptChunk` of 32 bytes per chunk (natively; 16 on wasm32): the census
/// parses and clones both scripts, `Spend::new` parses and clones them again
/// and holds them, a reached signature check clones the script and its chunks
/// once more for the subscript, and the interpreter pushes up to three stack
/// entries of 24 bytes per opcode (`OP_3DUP`), which its memory limit does
/// not count when they are empty. Measured (`tests/script_door_parse.rs`,
/// the peak of the live heap of a walk over one input whose lock is 262,145
/// chunks, one past a power of two so every growing buffer is at its
/// slackest; bytes a chunk, the frame's own 4 a byte included): `OP_NOP`
/// 98.0, `OP_0` 169.0, `OP_NOP`s then a reached `OP_CHECKSIG` 162.0,
/// `OP_3DUP` 241.0, and 258.0 with a reached `OP_CHECKSIG` after them. The
/// WORST known is an error path (the doors delta-2 lens E585-D12-DELTA2-L1):
/// bsv-rs's `Spend::error` clones the stack into the error, so `OP_3DUP`s
/// over empty entries to a stack just past a power of two (1,048,581
/// entries) and then a refused `OP_VERIFY` is 297.0 a byte. Pushes cost less
/// a byte (`0x01 xx OP_DROP` 66.7, `0x01 xx` unlocking 62.5). The charge is
/// 512 a chunk (520 with the byte's 8, 524 with the frame's 4): 1.76 times
/// the worst known natively, and about 3.5 times on wasm32 by arithmetic (a
/// chunk 16 bytes and a `Vec` 12 there, about 150 a byte; not run). The
/// interpreter's own 128 KB limit covers the element BYTES; this charge
/// covers what it does not count: the records and their clones, the
/// subscript, the stack ENTRIES (24 bytes natively, at most three net per
/// opcode, in a buffer that doubles) and the error's clone of them.
pub(crate) const SCRIPT_CHARGE_PER_CHUNK: u64 = 512;

/// What one input's two scripts hold once parsed and run, per BYTE of each,
/// past the frame's own charge of the transaction in hand (the delta lens
/// E585-D12-DELTA-M1): a pushed byte's copies in the chunk records, their
/// clone and the subscript's. Measured: ten 100,000 byte pushes dropped, then
/// a reached `OP_CHECKSIG`, 6.7 bytes a byte of peak, the frame's 4 included
/// (2.7 past it). Charged 8. The frame (`DOOR_CHARGES.tx_byte`) already
/// counts the transaction in hand, the interpreter's raw copies of the
/// unlocking script among them; this counts what the PARSE adds.
pub(crate) const SCRIPT_CHARGE_PER_BYTE: u64 = 8;

/// The chunks bsv-rs 0.4.0's `Script::parse_chunks` cuts `script` into,
/// counted with no allocation: a push (`0x01..=0x4b`, `OP_PUSHDATA1/2/4`) is
/// one chunk with its data, cut short at the script's end as the SDK cuts it;
/// an `OP_RETURN` outside a conditional is one chunk with everything after it;
/// any other byte is one chunk. Pinned against the SDK's own parse
/// (`e585f2_m1_the_chunk_count_is_the_sdks`): re-proven at every bsv-rs bump.
pub(crate) fn chunk_count(script: &[u8]) -> u64 {
    use bsv_rs::script::op::{
        OP_ENDIF, OP_IF, OP_NOTIF, OP_PUSHDATA1, OP_PUSHDATA2, OP_PUSHDATA4, OP_RETURN, OP_VERIF,
        OP_VERNOTIF,
    };
    let len = script.len();
    let (mut at, mut chunks, mut depth) = (0usize, 0u64, 0usize);
    let width = |at: usize, n: usize| {
        (0..n).fold(0usize, |acc, i| {
            acc | usize::from(script.get(at + i).copied().unwrap_or(0)) << (8 * i)
        })
    };
    while at < len {
        let op = script[at];
        at += 1;
        chunks += 1;
        if op == OP_RETURN && depth == 0 {
            break;
        }
        if matches!(op, OP_IF | OP_NOTIF | OP_VERIF | OP_VERNOTIF) {
            depth += 1;
        } else if op == OP_ENDIF {
            depth = depth.saturating_sub(1);
        }
        let header = match op {
            OP_PUSHDATA1 => 1,
            OP_PUSHDATA2 => 2,
            OP_PUSHDATA4 => 4,
            _ => 0,
        };
        let pushed = if op > 0 && op < OP_PUSHDATA1 {
            usize::from(op)
        } else if header > 0 {
            let n = width(at, header);
            at = (at + header).min(len);
            n
        } else {
            0
        };
        at = at.saturating_add(pushed).min(len);
    }
    chunks
}

/// bsv-rs's own words for an unlocking script that is not push-only
/// (`Spend::validate`).
pub(crate) const NOT_PUSH_ONLY: &str =
    "Unlocking scripts can only contain push operations, and no other opcodes.";

/// Whether `script` is push-only as bsv-rs 0.4.3's `Script::is_push_only`
/// reads it (every chunk's opcode at most `OP_16`), with no allocation: the
/// chunks are cut as [`chunk_count`] cuts them (a push cut short at the end
/// is one push chunk, as the SDK cuts it). Pinned against the SDK
/// (`e592f_h1_push_only_is_the_sdks`): re-proven at every bsv-rs bump.
pub(crate) fn push_only(script: &[u8]) -> bool {
    use bsv_rs::script::op::{OP_16, OP_PUSHDATA1, OP_PUSHDATA2, OP_PUSHDATA4};
    let len = script.len();
    let mut at = 0usize;
    let width = |at: usize, n: usize| {
        (0..n).fold(0usize, |acc, i| {
            acc | usize::from(script.get(at + i).copied().unwrap_or(0)) << (8 * i)
        })
    };
    while at < len {
        let op = script[at];
        at += 1;
        if op > OP_16 {
            return false;
        }
        let header = match op {
            OP_PUSHDATA1 => 1,
            OP_PUSHDATA2 => 2,
            OP_PUSHDATA4 => 4,
            _ => 0,
        };
        let pushed = if op > 0 && op < OP_PUSHDATA1 {
            usize::from(op)
        } else if header > 0 {
            let n = width(at, header);
            at = (at + header).min(len);
            n
        } else {
            0
        };
        at = at.saturating_add(pushed).min(len);
    }
    true
}

/// What one input's scripts hold once parsed and run, beside the frame's
/// estimate: [`SCRIPT_CHARGE_PER_CHUNK`] a chunk and
/// [`SCRIPT_CHARGE_PER_BYTE`] a byte of both scripts.
pub(crate) fn script_parse_charge(unlocking: &[u8], locking: &[u8]) -> u64 {
    (chunk_count(unlocking) + chunk_count(locking))
        .saturating_mul(SCRIPT_CHARGE_PER_CHUNK)
        .saturating_add(
            ((unlocking.len() + locking.len()) as u64).saturating_mul(SCRIPT_CHARGE_PER_BYTE),
        )
}

/// Where a raw transaction lies in the body.
#[derive(Debug, Clone, Copy)]
struct Place {
    offset: usize,
    len: usize,
}

/// What the door keeps of a BEEF: one entry per raw transaction and one per
/// txid a BUMP carries at level 0. A txid-only entry carries no bytes and is
/// not kept (the walk before #585 did not find one either).
#[derive(Debug, Default)]
struct DoorIndex {
    txs: HashMap<Hash32, Place>,
    proven: HashSet<Hash32>,
}

impl DoorIndex {
    fn read(beef_bytes: &[u8]) -> Result<Self, EngineError> {
        let mut index = Self::default();
        let mut stream = BeefStream::new(beef_bytes);
        while let Some(element) = stream
            .next_element()
            .map_err(|e| EngineError::BeefParseError(e.to_string()))?
        {
            match element {
                Element::Bump(bump) => index.proven.extend(bump.proven().copied()),
                Element::Tx {
                    offset, txid, body, ..
                } => {
                    if let Ok(offset) = usize::try_from(offset) {
                        index.txs.entry(txid).or_insert(Place {
                            offset,
                            len: body.raw.len(),
                        });
                    }
                }
                Element::TxidOnly { .. } => {}
            }
        }
        Ok(index)
    }

    /// The heap the two tables hold, by their capacity (a bucket is its
    /// entry and one control byte).
    #[cfg(test)]
    fn heap_bytes(&self) -> usize {
        self.txs.capacity() * (std::mem::size_of::<(Hash32, Place)>() + 1)
            + self.proven.capacity() * (std::mem::size_of::<Hash32>() + 1)
    }

    fn raw<'a>(&self, beef_bytes: &'a [u8], txid: &Hash32) -> Option<&'a [u8]> {
        let place = self.txs.get(txid)?;
        beef_bytes.get(place.offset..place.offset.checked_add(place.len)?)
    }
}

/// A reader over one raw transaction (BRC-12), or over a frame's fields.
pub(crate) struct Fields<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) at: usize,
}

impl<'a> Fields<'a> {
    pub(crate) fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let bytes = self.raw.get(self.at..end)?;
        self.at = end;
        Some(bytes)
    }

    pub(crate) fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    pub(crate) fn varint(&mut self) -> Option<u64> {
        let lead = *self.take(1)?.first()?;
        let wide = |bytes: &[u8]| {
            let mut le = [0u8; 8];
            le[..bytes.len()].copy_from_slice(bytes);
            u64::from_le_bytes(le)
        };
        Some(match lead {
            0xfd => wide(self.take(2)?),
            0xfe => wide(self.take(4)?),
            0xff => wide(self.take(8)?),
            short => u64::from(short),
        })
    }

    /// A script: its length, then its place in the raw bytes.
    pub(crate) fn script(&mut self) -> Option<Range<usize>> {
        let len = usize::try_from(self.varint()?).ok()?;
        let start = self.at;
        self.take(len)?;
        Some(start..self.at)
    }

    /// Past the version and the inputs: at the output count.
    fn skip_to_outputs(&mut self) -> Option<()> {
        self.take(4)?;
        for _ in 0..self.varint()? {
            self.take(36)?;
            self.script()?;
            self.take(4)?;
        }
        Some(())
    }
}

struct InputAt {
    prev: Hash32,
    vout: u32,
    script: Range<usize>,
    sequence: u32,
}

struct OutputAt {
    satoshis: u64,
    script: Range<usize>,
}

/// Where the fields of one transaction lie in its raw bytes. No script is
/// copied: the layout of a 2 MB transaction is its inputs' and outputs'
/// places, not its bytes.
struct Layout {
    version: u32,
    inputs: Vec<InputAt>,
    outputs: Vec<OutputAt>,
    lock_time: u32,
}

impl Layout {
    fn of(raw: &[u8]) -> Option<Self> {
        let mut f = Fields { raw, at: 0 };
        let version = f.u32()?;
        // A count read from the bytes reserves nothing.
        let mut inputs = Vec::new();
        for _ in 0..f.varint()? {
            let prev: Hash32 = f.take(32)?.try_into().ok()?;
            let vout = f.u32()?;
            let script = f.script()?;
            let sequence = f.u32()?;
            inputs.push(InputAt {
                prev,
                vout,
                script,
                sequence,
            });
        }
        let mut outputs = Vec::new();
        for _ in 0..f.varint()? {
            let satoshis = f.u64()?;
            let script = f.script()?;
            outputs.push(OutputAt { satoshis, script });
        }
        let lock_time = f.u32()?;
        (f.at == raw.len()).then_some(Self {
            version,
            inputs,
            outputs,
            lock_time,
        })
    }
}

/// The offset of each output of a raw transaction (one word per output): what
/// the walk keeps of a SOURCE, so that an input finds the output it spends
/// without the source being laid out again.
fn output_offsets(raw: &[u8]) -> Option<Box<[usize]>> {
    let mut f = Fields { raw, at: 0 };
    f.skip_to_outputs()?;
    let mut offsets = Vec::new();
    for _ in 0..f.varint()? {
        offsets.push(f.at);
        f.take(8)?;
        f.script()?;
    }
    Some(offsets.into_boxed_slice())
}

/// The output at `at` of a raw transaction: its satoshis and its script.
fn output_at(raw: &[u8], at: usize) -> Option<(u64, &[u8])> {
    let mut f = Fields { raw, at };
    let satoshis = f.u64()?;
    let script = f.script()?;
    Some((satoshis, raw.get(script)?))
}

/// The door's walk: `'scripts only'` from `subject_txid` over `beef_bytes`,
/// under the budget.
pub(crate) fn walk(
    beef_bytes: &[u8],
    subject_txid: &str,
    budget: DoorBudget,
) -> Result<WalkStats, EngineError> {
    walk_trusting(
        beef_bytes,
        subject_txid,
        budget,
        &HashSet::new(),
        &mut WalkTrace::default(),
    )
}

/// What a walk saw beside its statistics, for the engine's submit (bsv-low
/// #592): the PROVEN transactions it reached, in the order it reached them
/// (the submit asks the chain tracker for their roots once the scripts
/// passed; the door asks nothing), and whether a breach of the work limb was
/// the interpreter's own memory limit tripping (the door's error says `Work`
/// for both).
///
/// `proven_sources` (the E592 lens fold, H1): every PROVEN source whose
/// output an input read, in that order, recorded when it is read and before
/// that input is charged. A walk that breaches at an input over a proven
/// source has read its lock but not yet reached it as a transaction (it is
/// queued only once the input ran); the submit checks its root all the same,
/// so a breach never skips the root of a proof the walk relied on.
#[derive(Debug, Default)]
pub(crate) struct WalkTrace {
    pub(crate) proven: Vec<String>,
    pub(crate) proven_sources: Vec<String>,
    pub(crate) interpreter_tripped: bool,
}

/// [`walk`] trusting `trusted` (display txids an earlier walk of the same
/// submit passed over: neither checked nor descended), and recording what it
/// reached in `trace`. The door trusts nothing; the engine's submit trusts
/// what a carried predecessor's landing already walked (the E1D delta fold,
/// L4). One walker for both (bsv-low #592).
pub(crate) fn walk_trusting(
    beef_bytes: &[u8],
    subject_txid: &str,
    budget: DoorBudget,
    trusted: &HashSet<String>,
    trace: &mut WalkTrace,
) -> Result<WalkStats, EngineError> {
    let mut stats = WalkStats::default();
    // The memory limb, from the frame alone: nothing is allocated before it.
    let sized = stream_sizing::estimate(beef_bytes, &DOOR_CHARGES, budget.max_memory_bytes);
    let over_memory = |what: String| EngineError::ScriptWalkOverBudget {
        at_txid: subject_txid.to_string(),
        subject_judged: false,
        limb: DoorLimb::Memory,
        what,
    };
    let admission = sized.admission();
    if admission == Admission::Over {
        return Err(over_memory(format!(
            "estimated memory {} bytes exceeds the door memory budget of {} (the element at byte \
             {} of the BEEF)",
            sized.bytes,
            budget.max_memory_bytes,
            sized.over_at.unwrap_or_default()
        )));
    }
    stats.memory_bytes = sized.bytes;
    let index = DoorIndex::read(beef_bytes)?;
    if admission == Admission::Unfollowed {
        // The stream read a frame the sizing read did not follow, so what was
        // just held was never estimated (`Admission::Unfollowed`, the rule
        // the census follows too). No body is known to do this; if a later
        // SDK reads a frame this reader does not, the walk stops here.
        return Err(over_memory(
            "the sizing read did not follow a BEEF the stream read: memory not estimated".into(),
        ));
    }

    let fault =
        |at_txid: &str, subject_judged: bool, reason: String| EngineError::ScriptWalkInconclusive {
            at_txid: at_txid.to_string(),
            subject_judged,
            reason,
        };
    let over =
        |at_txid: &str, subject_judged: bool, what: String| EngineError::ScriptWalkOverBudget {
            at_txid: at_txid.to_string(),
            subject_judged,
            limb: DoorLimb::Work,
            what,
        };

    let subject: Option<Hash32> = bsv_rs::primitives::from_hex(subject_txid)
        .ok()
        .and_then(|bytes| Hash32::try_from(bytes).ok())
        .map(|mut wire| {
            wire.reverse();
            wire
        });
    let Some(subject) = subject else {
        return Err(fault(
            subject_txid,
            false,
            format!("transaction {subject_txid} is not in the BEEF"),
        ));
    };

    // The outputs of each source the walk spent from, by offset.
    let mut sources: HashMap<Hash32, Box<[usize]>> = HashMap::new();
    let mut seen: HashSet<Hash32> = trusted
        .iter()
        .filter_map(|txid| {
            let mut wire = Hash32::try_from(bsv_rs::primitives::from_hex(txid).ok()?).ok()?;
            wire.reverse();
            Some(wire)
        })
        .collect();
    // THE STATIC PRE-PASS (the E592 delta lens, D1-L1): before any charge,
    // every unlocking script of every transaction this walk can reach is read
    // for push-only. The per-input check below made "a static refusal is a
    // refusal, never a breach" true per input only: a non-push unlocking
    // script on input 1 (or on an ancestor) was never read when input 0's
    // lock breached the census first, and the answer was "the walk could not
    // run". Now the interpreter's refusal is answered whatever a lock costs.
    if let Some(refused) = first_not_push_only(beef_bytes, &index, subject, &seen) {
        return Err(refused);
    }
    let mut queue: Vec<Hash32> = vec![subject];
    while let Some(wire_txid) = queue.pop() {
        if !seen.insert(wire_txid) {
            continue;
        }
        let judged = stats.subject_judged;
        let txid = display_hex(&wire_txid);
        let Some(raw) = index.raw(beef_bytes, &wire_txid) else {
            return Err(fault(
                &txid,
                judged,
                format!("transaction {txid} is not in the BEEF"),
            ));
        };
        if index.proven.contains(&wire_txid) {
            // 'scripts only': a proven transaction is trusted as-is, no root
            // computed, no tracker asked. The caller's bar is the network's
            // acceptance, never this proof. The engine's submit asks the
            // tracker for its root once the walk passed (bsv-low #592).
            trace.proven.push(txid);
            continue;
        }

        // Unproven: the value rule and every input's script, sources by txid.
        let Some(tx) = Layout::of(raw) else {
            return Err(fault(
                &txid,
                judged,
                format!("transaction {txid} does not parse"),
            ));
        };
        let tx_bytes = raw.len();
        stats.unproven_txs += 1;
        // The other inputs and the outputs, as a signature's digest reads
        // them: built at the first input that checks one.
        let mut digest_view: Option<(Vec<TxInput>, Vec<TxOutput>)> = None;
        let mut input_total: u64 = 0;
        for (vin, input) in tx.inputs.iter().enumerate() {
            let Some(source_raw) = index.raw(beef_bytes, &input.prev) else {
                return Err(fault(
                    &txid,
                    judged,
                    format!("input {vin} of transaction {txid} has no source transaction"),
                ));
            };
            let source_output = if let Some(offsets) = sources.get(&input.prev) {
                offsets.get(input.vout as usize).copied()
            } else {
                let Some(offsets) = output_offsets(source_raw) else {
                    return Err(fault(
                        &txid,
                        judged,
                        format!(
                            "input {vin} of transaction {txid}: source transaction does not parse"
                        ),
                    ));
                };
                let at = offsets.get(input.vout as usize).copied();
                sources.insert(input.prev, offsets);
                at
            };
            let Some((source_sats, locking_bytes)) =
                source_output.and_then(|at| output_at(source_raw, at))
            else {
                return Err(fault(
                    &txid,
                    judged,
                    format!("input {vin} of transaction {txid}: source output index out of bounds"),
                ));
            };
            input_total = input_total.checked_add(source_sats).ok_or_else(|| {
                fault(
                    &txid,
                    judged,
                    format!("satoshi total overflows in transaction {txid}"),
                )
            })?;
            if index.proven.contains(&input.prev) {
                trace.proven_sources.push(display_hex(&input.prev));
            }
            let unlocking_bytes = &raw[input.script.clone()];
            // A STATIC refusal is the interpreter's own, before any charge
            // (the E592 lens fold, H1; the pre-pass above already read every
            // reachable one, so this is the belt): an unlocking script that is not
            // push-only is refused by bsv-rs's `Spend::validate` before it
            // runs a byte, at every transaction version (its
            // `REQUIRE_PUSH_ONLY_UNLOCKING`, the reference's rule), in these
            // words. Charged first, a one-byte `OP_CHECKMULTISIG` unlock was
            // estimated 260 MB of work and the walk "could not run".
            if !push_only(unlocking_bytes) {
                return Err(EngineError::ScriptVerificationFailed {
                    subject_txid: txid.clone(),
                    input_index: vin as u32,
                    reason: NOT_PUSH_ONLY.into(),
                });
            }
            // The memory of this input's scripts once parsed and run, beside
            // the frame's estimate, charged BEFORE either is parsed (the
            // delta lens E585-D12-DELTA-M1). It is held for this input only.
            let held = sized
                .bytes
                .saturating_add(script_parse_charge(unlocking_bytes, locking_bytes));
            if held > budget.max_memory_bytes {
                return Err(EngineError::ScriptWalkOverBudget {
                    at_txid: txid.clone(),
                    subject_judged: judged,
                    limb: DoorLimb::Memory,
                    what: format!(
                        "estimated memory {held} bytes exceeds the door memory budget of {} \
                         (the scripts of input {vin} of {txid}, before their parse)",
                        budget.max_memory_bytes
                    ),
                });
            }
            stats.memory_bytes = stats.memory_bytes.max(held);
            // The door's static charge for this input (nothing has run yet).
            let (bytes, hash_ops, sig_ops) =
                script_census(unlocking_bytes, locking_bytes, budget.memory_limit).map_err(
                    |e| {
                        fault(
                            &txid,
                            judged,
                            format!("input {vin} of transaction {txid}: {e}"),
                        )
                    },
                )?;
            stats.script_bytes += bytes;
            stats.hash_ops += hash_ops;
            stats.sig_ops += sig_ops;
            stats.work_bytes += bytes as u64
                + (hash_ops as u64) * (budget.memory_limit as u64)
                + (sig_ops as u64) * (tx_bytes as u64).max(budget.sig_check_floor);
            if stats.work_bytes > budget.max_work_bytes {
                return Err(over(
                    &txid,
                    judged,
                    format!(
                        "estimated work {} bytes exceeds the door budget of {} (input {vin} of {txid})",
                        stats.work_bytes, budget.max_work_bytes
                    ),
                ));
            }
            // The interpreter reads the other inputs and the outputs for one
            // thing, a signature's digest. An input whose scripts hold no
            // signature check is run without them; one that does was just
            // charged at least this transaction's bytes per check.
            let (other_inputs, outputs) = if sig_ops == 0 {
                (Vec::new(), Vec::new())
            } else {
                let (inputs, outputs) = digest_view.get_or_insert_with(|| {
                    (
                        tx.inputs
                            .iter()
                            .map(|i| TxInput {
                                txid: i.prev,
                                output_index: i.vout,
                                script: raw[i.script.clone()].to_vec(),
                                sequence: i.sequence,
                            })
                            .collect(),
                        tx.outputs
                            .iter()
                            .map(|o| TxOutput {
                                satoshis: o.satoshis,
                                script: raw[o.script.clone()].to_vec(),
                            })
                            .collect(),
                    )
                });
                (
                    inputs
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != vin)
                        .map(|(_, other)| other.clone())
                        .collect(),
                    outputs.clone(),
                )
            };
            let locking_script =
                LockingScript::from_script(Script::from_binary(locking_bytes).map_err(|e| {
                    fault(
                        &txid,
                        judged,
                        format!("locking script of {}: {e}", display_hex(&input.prev)),
                    )
                })?);
            let unlocking_script =
                UnlockingScript::from_script(Script::from_binary(unlocking_bytes).map_err(
                    |e| fault(&txid, judged, format!("unlocking script of {txid}: {e}")),
                )?);
            let mut spend = Spend::new(SpendParams {
                source_txid: input.prev,
                source_output_index: input.vout,
                source_satoshis: source_sats,
                locking_script,
                transaction_version: tx.version.cast_signed(),
                other_inputs,
                outputs,
                input_index: vin,
                unlocking_script,
                input_sequence: input.sequence,
                lock_time: tx.lock_time,
                memory_limit: Some(budget.memory_limit),
            });
            stats.inputs_executed += 1;
            match spend.validate() {
                Ok(true) => {}
                Ok(false) => {
                    return Err(EngineError::ScriptVerificationFailed {
                        subject_txid: txid.clone(),
                        input_index: vin as u32,
                        reason: "script evaluated to false".into(),
                    });
                }
                // The door's OWN limit tripping inside the interpreter (the
                // stack memory limit is the unit of the work estimate) is the
                // door's verdict, never the network's: over budget, not
                // refused. bsv-rs reports it as its own class
                // (`resource_limit`, the ts-sdk's `ScriptResourceLimitError`:
                // the stack budget, the alt stack, NUM2BIN's element-size
                // pre-check), so no wording is matched.
                Err(e) if e.is_resource_limit() => {
                    trace.interpreter_tripped = true;
                    return Err(over(
                        &txid,
                        judged,
                        format!("input {vin} of transaction {txid}: {}", e.message),
                    ));
                }
                Err(e) => {
                    return Err(EngineError::ScriptVerificationFailed {
                        subject_txid: txid.clone(),
                        input_index: vin as u32,
                        reason: e.message.clone(),
                    });
                }
            }
            queue.push(input.prev);
        }
        if wire_txid == subject {
            stats.subject_judged = true;
        }
        let mut output_total: u64 = 0;
        for output in &tx.outputs {
            output_total = output_total.checked_add(output.satoshis).ok_or_else(|| {
                fault(
                    &txid,
                    stats.subject_judged,
                    format!("satoshi total overflows in transaction {txid}"),
                )
            })?;
        }
        if output_total > input_total {
            return Err(fault(
                &txid,
                stats.subject_judged,
                format!(
                    "transaction {txid} creates {output_total} sats from {input_total} sats of inputs"
                ),
            ));
        }
    }
    Ok(stats)
}

/// The first unlocking script that is not push-only among the transactions
/// a walk from `subject` can reach, in the walk's own order (the subject's
/// inputs first to last, then its unproven sources depth first, the last
/// input's first), as the interpreter's refusal. O(script bytes): no charge,
/// no parse of a script, one layout of a transaction at a time. A proven
/// transaction is not descended (the walk trusts it), nor one in `trusted`;
/// a transaction the BEEF lacks or that does not lay out ends that branch
/// (the walk answers its own fault there). It reads only the unlocking
/// scripts, so it can answer a refusal where the walk would have stopped at a
/// structural fault EARLIER in its order (a source output out of bounds, the
/// value rule): both are a body the network refuses; the refusal names the
/// input the interpreter refuses.
fn first_not_push_only(
    beef_bytes: &[u8],
    index: &DoorIndex,
    subject: Hash32,
    trusted: &HashSet<Hash32>,
) -> Option<EngineError> {
    let mut seen = trusted.clone();
    let mut queue: Vec<Hash32> = vec![subject];
    while let Some(wire_txid) = queue.pop() {
        if !seen.insert(wire_txid) || index.proven.contains(&wire_txid) {
            continue;
        }
        let tx = index.raw(beef_bytes, &wire_txid).and_then(|raw| {
            let tx = Layout::of(raw)?;
            Some((raw, tx))
        });
        let Some((raw, tx)) = tx else { continue };
        for (vin, input) in tx.inputs.iter().enumerate() {
            if !push_only(&raw[input.script.clone()]) {
                return Some(EngineError::ScriptVerificationFailed {
                    subject_txid: display_hex(&wire_txid),
                    input_index: vin as u32,
                    reason: NOT_PUSH_ONLY.into(),
                });
            }
        }
        queue.extend(
            tx.inputs
                .iter()
                .map(|input| input.prev)
                .filter(|prev| index.raw(beef_bytes, prev).is_some()),
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One raw transaction: `inputs` empty-script inputs, then outputs with
    /// the given scripts.
    fn raw_tx(inputs: usize, scripts: &[&[u8]]) -> Vec<u8> {
        let mut raw = 1u32.to_le_bytes().to_vec();
        raw.push(inputs as u8);
        for i in 0..inputs {
            raw.extend_from_slice(&[i as u8; 32]);
            raw.extend_from_slice(&(i as u32).to_le_bytes());
            raw.push(0);
            raw.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        raw.push(scripts.len() as u8);
        for (i, script) in scripts.iter().enumerate() {
            raw.extend_from_slice(&(1000 + i as u64).to_le_bytes());
            raw.extend_from_slice(&[0xfd, script.len() as u8, (script.len() >> 8) as u8]);
            raw.extend_from_slice(script);
        }
        raw.extend_from_slice(&7u32.to_le_bytes());
        raw
    }

    /// The door's own reading of a raw transaction agrees with the SDK's, and
    /// a source's output is found by its offset.
    #[test]
    fn e585_d1_the_layout_reads_what_the_sdk_parses() {
        let big = vec![0x6a; 300];
        let raw = raw_tx(3, &[&[0x51], &big, &[]]);
        let sdk = bsv_rs::transaction::Transaction::from_binary(&raw).expect("the SDK parses it");
        let layout = Layout::of(&raw).expect("the door lays it out");
        assert_eq!(layout.version, sdk.version);
        assert_eq!(layout.lock_time, sdk.lock_time);
        assert_eq!(layout.inputs.len(), sdk.inputs.len());
        for (ours, theirs) in layout.inputs.iter().zip(&sdk.inputs) {
            assert_eq!(Some(display_hex(&ours.prev)), theirs.source_txid);
            assert_eq!(ours.vout, theirs.source_output_index);
            assert_eq!(ours.sequence, theirs.sequence);
        }
        let offsets = output_offsets(&raw).expect("the outputs are located");
        assert_eq!(offsets.len(), sdk.outputs.len());
        for ((ours, theirs), at) in layout.outputs.iter().zip(&sdk.outputs).zip(offsets.iter()) {
            assert_eq!(Some(ours.satoshis), theirs.satoshis);
            assert_eq!(raw[ours.script.clone()], theirs.locking_script.to_binary());
            let (satoshis, script) = output_at(&raw, *at).expect("the output is read in place");
            assert_eq!(satoshis, ours.satoshis);
            assert_eq!(script, &raw[ours.script.clone()]);
        }
        // Bytes cut short or carrying more than a transaction are no layout.
        assert!(Layout::of(&raw[..raw.len() - 1]).is_none());
        let mut longer = raw.clone();
        longer.push(0);
        assert!(Layout::of(&longer).is_none());
    }

    /// THE INDEX: an entry per raw transaction and one per txid a BUMP
    /// carries, whatever the transactions weigh; and each transaction is
    /// found again where the index says, in the caller's bytes.
    #[test]
    fn e585_d1_the_index_is_an_entry_per_element() {
        use bsv_rs::transaction::{Beef, MerklePath, MerklePathLeaf};
        let fat = vec![0x6a; 60_000];
        let mut beef = Beef::new();
        let mut txids = Vec::new();
        for i in 0..1000u32 {
            let mut script = i.to_le_bytes().to_vec();
            if i % 100 == 0 {
                script.extend_from_slice(&fat);
            }
            let raw = raw_tx(1, &[&script]);
            let txid = bsv_rs::transaction::Transaction::from_binary(&raw)
                .expect("a transaction")
                .id();
            if i % 2 == 0 {
                let bump = MerklePath::new(
                    800_000 + i,
                    vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]],
                )
                .expect("a one-leaf BUMP");
                let at = beef.merge_bump(bump);
                beef.merge_raw_tx(raw, Some(at));
            } else {
                beef.merge_raw_tx(raw, None);
            }
            txids.push(txid);
        }
        let body = beef.to_binary();
        let index = DoorIndex::read(&body).expect("the stream reads it");
        assert_eq!(index.txs.len(), 1000);
        assert_eq!(index.proven.len(), 500);
        for txid in &txids {
            let mut wire: Hash32 = bsv_rs::primitives::from_hex(txid)
                .expect("hex")
                .try_into()
                .expect("32 bytes");
            wire.reverse();
            let raw = index.raw(&body, &wire).expect("the transaction is indexed");
            let found = bsv_rs::transaction::Transaction::from_binary(raw)
                .expect("the bytes at the index's place are the transaction")
                .id();
            assert_eq!(&found, txid);
        }
        let elements = 1500;
        let held = index.heap_bytes();
        println!(
            "e585_d1 index: body {} bytes, {elements} elements (1000 transactions, 500 BUMPs), \
             the index {held} bytes = {} per element",
            body.len(),
            held / elements
        );
        assert!(
            held <= elements * 128,
            "the index holds {held} bytes for {elements} elements"
        );
    }

    /// THE SIZING READ FOLLOWS EVERY FRAME THE STREAM READS (the doors lens
    /// E585-D12-L3). The memory limb is charged by a second reader of the
    /// frame; a frame the stream reads and that reader does not would be
    /// walked unestimated. Over a body in its three frames (V1, V2 with a
    /// txid-only entry, atomic), every cut of it and three changes of every
    /// byte: whenever the stream reads the bytes, the sizing read followed
    /// them to their end.
    #[test]
    fn e585f_l3_the_sizing_follows_every_frame_the_stream_reads() {
        use bsv_rs::transaction::{Beef, MerklePath, MerklePathLeaf, Transaction, BEEF_V1};
        let build = |version: Option<u32>| {
            let mut beef = version.map_or_else(Beef::new, Beef::with_version);
            let proven = raw_tx(1, &[&[0x51], &[0x52, 0x53]]);
            let proven_txid = Transaction::from_binary(&proven).expect("a tx").id();
            let bump = MerklePath::new(
                800_000,
                vec![
                    vec![
                        MerklePathLeaf::new_txid(2, proven_txid.clone()),
                        MerklePathLeaf::new_duplicate(3),
                    ],
                    vec![MerklePathLeaf::new(0, "11".repeat(32))],
                ],
            )
            .expect("a BUMP of two levels");
            let at = beef.merge_bump(bump);
            beef.merge_raw_tx(proven, Some(at));
            beef.merge_raw_tx(raw_tx(2, &[&[0x6a; 70], &[]]), None);
            let last = raw_tx(3, &[&[0x51]]);
            let last_txid = Transaction::from_binary(&last).expect("a tx").id();
            beef.merge_raw_tx(last, None);
            (beef, proven_txid, last_txid)
        };
        let (mut v2, proven_txid, last_txid) = build(None);
        let atomic = v2.to_binary_atomic(&last_txid).expect("atomic");
        let plain = v2.to_binary();
        v2.make_txid_only(&proven_txid);
        let with_stub = v2.to_binary();
        let v1 = build(Some(BEEF_V1)).0.to_binary();

        let (mut read, mut refused) = (0usize, 0usize);
        let mut check = |bytes: &[u8]| {
            let followed = stream_sizing::estimate(bytes, &DOOR_CHARGES, u64::MAX);
            assert_eq!(followed.over_at, None);
            if DoorIndex::read(bytes).is_ok() {
                read += 1;
                assert!(
                    followed.read && followed.bytes > 0,
                    "the stream reads {} and the sizing read does not: {followed:?}",
                    bsv_rs::primitives::to_hex(bytes)
                );
            } else {
                refused += 1;
            }
        };
        for body in [&plain, &with_stub, &atomic, &v1] {
            check(body);
            for cut in 0..body.len() {
                check(&body[..cut]);
            }
            for at in 0..body.len() {
                for flip in [0x01u8, 0x80, 0xff] {
                    let mut changed = body.clone();
                    changed[at] ^= flip;
                    check(&changed);
                }
            }
        }
        println!("e585f_l3 sizing: {read} bodies the stream reads, {refused} it refuses");
        assert!(
            read >= 500 && refused >= 500,
            "{read} read, {refused} refused"
        );
    }

    /// ONE ADMISSION RULE (the delta lens E585-D12-DELTA-N1), the door's and
    /// the census's: a frame followed within the limit is `Open`, one past it
    /// `Over`, one the sizing read did not follow `Unfollowed`.
    #[test]
    fn e585f2_n1_the_admission_rule() {
        use stream_sizing::Admission;
        let body = {
            let mut beef = bsv_rs::transaction::Beef::new();
            beef.merge_raw_tx(raw_tx(1, &[&[0x51]]), None);
            beef.to_binary()
        };
        let at = |bytes: &[u8], limit| stream_sizing::estimate(bytes, &DOOR_CHARGES, limit);
        assert_eq!(at(&body, u64::MAX).admission(), Admission::Open);
        assert_eq!(at(&body, 1).admission(), Admission::Over);
        assert_eq!(
            at(&body[..body.len() - 1], u64::MAX).admission(),
            Admission::Unfollowed
        );
        // The door answers an unfollowed frame the stream refuses with the
        // stream's own refusal, as before the rule.
        assert!(matches!(
            walk(&body[..body.len() - 1], "00", DoorBudget::DEFAULT),
            Err(EngineError::BeefParseError(_))
        ));
    }

    /// THE CHUNK COUNT IS THE SDK'S (the delta lens E585-D12-DELTA-M1). The
    /// memory limb charges an input's scripts by their chunk count before
    /// either is parsed; the count is a second reader of the script, so it is
    /// held equal to bsv-rs's own parse over every edge the parser has (each
    /// push width cut short, an `OP_RETURN` inside and outside a conditional,
    /// an `OP_ENDIF` with no `OP_IF`) and 20,000 random scripts biased toward
    /// those bytes. Re-run at every bsv-rs bump.
    /// The E592 lens fold, H1: the door's static push-only read is the SDK's
    /// own (`Script::is_push_only`, which `Spend::validate` asks first), over
    /// every one- and two-byte script and pseudo-random scripts with cut-short
    /// pushes. Re-proven at every bsv-rs bump.
    #[test]
    fn e592f_h1_push_only_is_the_sdks() {
        let mut scripts: Vec<Vec<u8>> = Vec::new();
        for a in 0..=255u8 {
            scripts.push(vec![a]);
            for b in 0..=255u8 {
                scripts.push(vec![a, b]);
            }
        }
        let mut seed: u64 = 0x5eed_e592;
        for _ in 0..20_000 {
            let mut script = Vec::new();
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            for _ in 0..(seed >> 58) as usize {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                // mostly pushes, sometimes an opcode past OP_16
                let byte = if seed.is_multiple_of(7) {
                    (seed >> 40) as u8
                } else {
                    ((seed >> 40) % 0x61) as u8
                };
                script.push(byte);
            }
            scripts.push(script);
        }
        for script in &scripts {
            let sdk = Script::from_binary(script).map(|s| s.is_push_only());
            if let Ok(sdk) = sdk {
                assert_eq!(push_only(script), sdk, "{}", hex::encode(script));
            }
        }
    }

    #[test]
    fn e585f2_m1_the_chunk_count_is_the_sdks() {
        use bsv_rs::script::op::*;
        let sdk = |bytes: &[u8]| {
            Script::from_binary(bytes)
                .expect("any bytes are a script")
                .chunks()
                .len() as u64
        };
        let mut edges: Vec<Vec<u8>> = vec![
            vec![],
            vec![OP_NOP; 10],
            vec![0x4b],
            vec![0x05, 1, 2],
            vec![OP_PUSHDATA1],
            vec![OP_PUSHDATA1, 9, 1],
            vec![OP_PUSHDATA2, 0x01],
            vec![OP_PUSHDATA2, 0x02, 0x00, 7, 7, OP_1],
            vec![OP_PUSHDATA4, 1, 0, 0],
            vec![OP_PUSHDATA4, 1, 0, 0, 0, 9, OP_2],
            vec![OP_1, OP_RETURN, OP_1, 0x05, OP_2],
            vec![OP_RETURN],
            vec![OP_IF, OP_RETURN, OP_ENDIF, OP_1],
            vec![
                OP_NOTIF, OP_VERIF, OP_ENDIF, OP_RETURN, OP_1, OP_ENDIF, OP_RETURN, 1, 2,
            ],
            vec![OP_ENDIF, OP_ENDIF, OP_RETURN, 0x01],
            vec![OP_VERNOTIF, OP_ENDIF, OP_RETURN],
        ];
        let interesting = [
            0x00,
            0x01,
            0x02,
            0x4b,
            OP_PUSHDATA1,
            OP_PUSHDATA2,
            OP_PUSHDATA4,
            OP_RETURN,
            OP_IF,
            OP_NOTIF,
            OP_VERIF,
            OP_VERNOTIF,
            OP_ENDIF,
            OP_NOP,
            OP_1,
            0xff,
        ];
        let mut seed: u64 = 0x5eed_e585;
        let mut next = || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) as usize
        };
        for _ in 0..20_000 {
            let len = next() % 40;
            let script: Vec<u8> = (0..len)
                .map(|_| {
                    let r = next();
                    if r % 3 == 0 {
                        (r >> 8) as u8
                    } else {
                        interesting[(r >> 8) % interesting.len()]
                    }
                })
                .collect();
            edges.push(script);
        }
        for script in &edges {
            assert_eq!(
                chunk_count(script),
                sdk(script),
                "{}",
                bsv_rs::primitives::to_hex(script)
            );
        }
        // The charge: a chunk and a byte of each script, both scripts.
        assert_eq!(
            script_parse_charge(&[OP_1], &[OP_NOP, OP_NOP]),
            3 * SCRIPT_CHARGE_PER_CHUNK + 3 * SCRIPT_CHARGE_PER_BYTE
        );
    }

    /// The door's charges, against the sizes they are derived from: a change
    /// of the SDK's element or of the door's own records is seen here.
    #[test]
    fn e585f_l3_the_charges_are_derived_from_the_sizes() {
        use bsv_rs::transaction::beef_stream::{InputRef, Leaf, OutputRef};
        use std::mem::size_of;
        // A growing buffer holds its old and its new allocation while it
        // moves (3x its contents); a table of buckets of `b` bytes and a
        // control byte holds up to 16/7 buckets an entry, and three such
        // tables' worth while it doubles.
        let grown = |bytes: usize| 3 * bytes as u64;
        let table = |bucket: usize| (3 * 8 * (bucket + 1)).div_ceil(7) as u64;
        assert_eq!(size_of::<Leaf>(), 56);
        assert_eq!(grown(size_of::<InputRef>()), StreamCharges::STREAM.tx_input);
        assert_eq!(
            grown(size_of::<OutputRef>()),
            StreamCharges::STREAM.tx_output
        );
        assert_eq!(table(size_of::<(Hash32, Place)>()), 168);
        assert_eq!(table(size_of::<Hash32>()), 114);
        assert_eq!(table(size_of::<(Hash32, Box<[usize]>)>()), 168);
        assert_eq!(DOOR_CHARGES.kept_tx, 168 + 114 + 168);
        assert_eq!(DOOR_CHARGES.kept_proven, 114);
        assert_eq!(DOOR_CHARGES.kept_input, grown(size_of::<Hash32>()));
        assert_eq!(DOOR_CHARGES.kept_output, grown(size_of::<usize>()));
        assert_eq!(
            DOOR_CHARGES.tx_input,
            grown(size_of::<InputAt>()) + grown(size_of::<TxInput>())
        );
        assert_eq!(
            DOOR_CHARGES.tx_output,
            grown(size_of::<OutputAt>()) + grown(size_of::<TxOutput>())
        );
    }
}
