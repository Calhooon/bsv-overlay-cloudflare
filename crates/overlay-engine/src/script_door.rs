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
//! The walk is two reads of the bytes the caller already holds:
//!
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
//! THE ONE LIMB is the work budget ([`crate::engine::DoorBudget`]): the
//! estimate of what the interpreter will hash and push, charged per input
//! before anything runs, as before. Past it the door's answer is the one it
//! always was, "the network judges", never a refusal. Every other cost of the
//! walk is linear in the body: each element is cut once, each judged
//! transaction is laid out once, each source's outputs are located once, and
//! an input that checks no signature is run with no copy of its transaction
//! (the interpreter reads the other inputs and the outputs only to build a
//! signature's digest; the copy of them per input was what the inputs bound
//! held down, and an input that DOES check a signature is charged its
//! transaction's bytes per check, which covers the copy).

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use bsv_rs::primitives::bsv::sighash::{TxInput, TxOutput};
use bsv_rs::script::{LockingScript, Script, Spend, SpendParams, UnlockingScript};
use bsv_rs::transaction::beef_stream::{display_hex, BeefStream, Element, Hash32};

use crate::engine::{script_census, DoorBudget, EngineError, WalkStats};

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

/// A reader over one raw transaction (BRC-12).
struct Fields<'a> {
    raw: &'a [u8],
    at: usize,
}

impl<'a> Fields<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let bytes = self.raw.get(self.at..end)?;
        self.at = end;
        Some(bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn varint(&mut self) -> Option<u64> {
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
    fn script(&mut self) -> Option<Range<usize>> {
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
/// under the work budget.
pub(crate) fn walk(
    beef_bytes: &[u8],
    subject_txid: &str,
    budget: DoorBudget,
) -> Result<WalkStats, EngineError> {
    let mut stats = WalkStats::default();
    let index = DoorIndex::read(beef_bytes)?;

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
    let mut seen: HashSet<Hash32> = HashSet::new();
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
            // acceptance, never this proof.
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
            // The door's static charge for this input (nothing has run yet).
            let unlocking_bytes = &raw[input.script.clone()];
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
                + (sig_ops as u64) * (tx_bytes as u64);
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
            // charged this transaction's bytes per check.
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
}
