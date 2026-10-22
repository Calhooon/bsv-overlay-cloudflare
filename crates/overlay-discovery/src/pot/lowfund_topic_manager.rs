//! LOWFUND Topic Manager — admits LOW HOP (funding staging coin) outputs.
//!
//! See [`super`] for the design. The hop is an ORDINARY P2PKH to the seat's
//! derived stake key — there is nothing structural to distinguish it from
//! any other P2PKH, so this manager admits every P2PKH output of a tx that
//! is EXPLICITLY submitted under `tm_lowfund` (the LOW client submits the
//! hop-carrying funding tx at hop time, and the JOIN / hop-sweep spenders
//! under the same topic so the engine records the hop's spend). Wallet
//! change outputs get rows too — harmless extra facts about already-public
//! txs; the store's rows are HINTS the client anchors before any money
//! decision (the same public-/submit trust posture as `tm_pot`).
//!
//! # A JOIN names its hops (bsv-low #791, FIX-791-C; the owner's ruling,
//! record 901 item 1)
//!
//! The client's hop post is best-effort, so a JOIN can reach this topic over
//! hops the ledger never saw: the JOIN was recorded and the hops' spend was
//! not. [`LowFundTopicManager::identify_needed_inputs`] now NAMES, for a JOIN
//! (a transaction with a pot covenant output, as `tm_pot` recognises it),
//! each input whose source body the BEEF CARRIES and whose spent output is a
//! P2PKH (the rule this manager admits by). The door then lands each named
//! carried hop first (D17 rule 2) and the JOIN's admission spends its coin.
//! The bound: the names are the JOIN's own inputs, never a walk: for an
//! honest JOIN the covenant's two seats' hops; for a stranger, any number
//! (the covenant check reads the script's head and tail only), bounded by
//! the submit's 256 reads (the door's; a landed hop's inserts are its own
//! submit's writes, not counted there); a hop's own body is no JOIN and
//! names nothing, so nothing above the hops is named. An input whose source the BEEF does not
//! carry is never named (its lock cannot be read, and an absent named body
//! would hold the JOIN "not now" until it landed: a proven JOIN presented
//! alone names nothing and is recorded as before).

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::beef_limits;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{
    AdmittanceInstructions, Outpoint, ServiceMetadata, SubmitMode, TopicAdmittanceContext,
};
use tracing::debug;

use super::{is_p2pkh_script, is_pot_covenant_script};

/// The hop inputs of the JOIN `beef` names (its atomic subject, else its last
/// transaction, as `Transaction::from_beef(_, None)` reads it): every input
/// whose source body the BEEF carries and whose spent output is a P2PKH, in
/// input order. Nothing for a transaction with no pot covenant output, or for
/// bytes that do not parse. No source is hydrated: each is looked up by txid.
pub fn join_hop_inputs(beef: &[u8]) -> Vec<Outpoint> {
    let Ok(beef) = beef_limits::parse_beef(beef, &beef_limits::DISCOVERY_BEEF_LIMITS) else {
        return Vec::new();
    };
    let subject = match beef.atomic_txid.as_deref() {
        Some(txid) => beef.find_txid(txid),
        None => beef.txs.last(),
    };
    let Some(join) = subject.and_then(|subject| subject.tx()) else {
        return Vec::new();
    };
    if !join
        .outputs
        .iter()
        .any(|o| is_pot_covenant_script(&o.locking_script.to_binary()))
    {
        return Vec::new();
    }
    join.inputs
        .iter()
        .filter_map(|input| {
            let source = input.get_source_txid().ok()?;
            let lock = beef
                .find_txid(&source)?
                .tx()?
                .outputs
                .get(input.source_output_index as usize)?
                .locking_script
                .to_binary();
            is_p2pkh_script(&lock).then(|| Outpoint::new(source, input.source_output_index))
        })
        .collect()
}

/// bsv-low #837 (the FIX-837 lens's MEDIUM-1): the most input rows one call of [`record_join_hop_spends`] reads for one
/// JOIN. An honest JOIN has two seats' hops (bsv-low `template_tx_sizes.rs`), so 16 is eight times that; a JOIN with
/// more candidate inputs reads a window of 16 of them, rotated by the caller's `rotation` (the discovery pass's hour),
/// and the rest are DEFERRED, counted in [`JoinHopSpends::deferred`], never skipped silently: the next pass's window
/// reads the next 16 (the shape of D17's bounds: a constant, never a walk the body sizes).
pub const JOIN_HOP_INPUTS_MAX: usize = 16;

/// bsv-low #837 (the FIX-837 lens's MEDIUM-1): the most JOIN input rows the scheduled discovery pass reads over ALL
/// its pot candidates together (four JOINs at [`JOIN_HOP_INPUTS_MAX`], 32 honest JOINs). A candidate the budget
/// left is not looked at this pass and is counted; it is a candidate again after its examine backoff (an hour).
/// Beside these reads each JOIN costs two fixed ones (its stored bytes and its pot row), bounded by the pass's own
/// candidate limit.
pub const JOIN_HOP_READS_PER_PASS: usize = 64;

/// bsv-low #837 (the FIX-837 lens's MEDIUM-1): the reads the discovery pass holds back, out of
/// [`JOIN_HOP_READS_PER_PASS`], for EACH pot candidate still to come: an honest JOIN's two hops. A JOIN is given at
/// most what is left after that reserve, so a stranger's wide JOINs, however many and wherever they sort, never
/// leave an honest JOIN later in the pass unread (the pass's 20 candidates reserve at most 38 of the 64).
pub const JOIN_HOP_READS_RESERVED_PER_JOIN: usize = 2;

/// The read allowance of one JOIN in a pass: what is left of the pass's budget after the reserve of the pot
/// candidates still to come, at most [`JOIN_HOP_INPUTS_MAX`].
pub fn join_hop_allowance(reads_left: usize, pots_after: usize) -> usize {
    reads_left
        .saturating_sub(pots_after.saturating_mul(JOIN_HOP_READS_RESERVED_PER_JOIN))
        .min(JOIN_HOP_INPUTS_MAX)
}

/// What [`record_join_hop_spends`] did for one pot.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct JoinHopSpends {
    /// The hop outpoints this call marked spent by the JOIN, in the order it read them.
    pub marked: Vec<Outpoint>,
    /// Hop rows that already carried a spend pointer, or gained one before the guarded write (left as they were).
    pub already_spent: usize,
    /// Why nothing was looked at: no stored bytes, no pot covenant output, the pot not held, a read fault.
    pub skipped: Option<&'static str>,
    /// A read or write that faulted (the pass converges on the next call).
    pub faults: usize,
    /// The input rows this call read (at most its bound).
    pub examined: usize,
    /// The candidate inputs past the bound, not read this call (the next window reads them).
    pub deferred: usize,
}

/// bsv-low #837 (FIX-837): record a HELD JOIN's hop spends from its own stored bytes, for a pot that entered the
/// index by any road other than the door's `tm_lowfund` submit (a readmission of its rows, `/admin/readmit` under
/// `tm_pot`, a hop that landed or was readmitted AFTER the JOIN, the discovery pass). The door records them through
/// the engine: the JOIN judged under `tm_lowfund` spends each hop coin the topic holds (and FIX-791-C lands a carried
/// one first, [`join_hop_inputs`]); every other road wrote the pot and left its hops `spent = 0`, so the owed list
/// offered a sweep of a hop the chain had spent into the pot (loop 44 pair 1: Arcade `UTXO_SPENT`).
///
/// The rule is the door's over the same bytes: the pot's stored BEEF (`pot_beefs`) parsed as the manager parses it
/// (its atomic subject, else its last transaction), which must BE the pot (its txid) and carry a pot covenant output;
/// the pot's own row must be held (an evicted pot's rows are in the twins: nothing is written for a JOIN the index
/// does not hold). Its inputs are the candidates: those [`join_hop_inputs`] names (a carried P2PKH source) and every
/// other input whose EXISTING row is a hop row (`lockKind = 'p2pkh'`, the shape `tm_lowfund` admitted; the stored
/// bytes of a mined JOIN can be compacted to the JOIN alone, its inputs still name the hops). Each such row that holds
/// no spend pointer is marked spent by the JOIN as an UNCONFIRMED pointer (`mark_spent(.., false, ..)`, the discovery
/// pass's write; the confirmation pass proves it, and an unconfirmed claim never clobbers a confirmed one). A row with
/// a pointer is left alone (never re-pointed: the guard is in the write itself,
/// [`PotStorage::mark_spent_if_unpointed`](crate::pot::storage::PotStorage::mark_spent_if_unpointed), the FIX-837
/// lens's LOW-3), a row that does not exist is never created (a hop is admitted by its own submit), and nothing else
/// is written. Idempotent: a second call marks nothing.
///
/// THE BOUND (the FIX-837 lens's MEDIUM-1): at most [`JOIN_HOP_INPUTS_MAX`] input rows are read per call, so a
/// stranger's covenant-shaped JOIN over hundreds of inputs costs the same as sixteen. A carried source whose spent
/// output is not P2PKH is never a hop and is never read; the inputs read are the named ones first, then those whose
/// source the bytes do not carry (a compacted JOIN), in input order. The road that passes the call the bounded form
/// ([`record_join_hop_spends_bounded`]) is the one road that calls it for a stranger's pot unasked, the discovery
/// pass, under its own per-pass budget ([`JOIN_HOP_READS_PER_PASS`]).
pub async fn record_join_hop_spends(
    storage: &dyn crate::pot::storage::PotStorage,
    pot_txid: &str,
) -> JoinHopSpends {
    record_join_hop_spends_bounded(storage, pot_txid, JOIN_HOP_INPUTS_MAX, 0).await
}

/// [`record_join_hop_spends`] under a caller's read budget (`max_reads`, clamped to [`JOIN_HOP_INPUTS_MAX`]) and a
/// window `rotation`: with more candidate inputs than the budget, the window starts at `rotation * budget` (mod the
/// candidates), so successive rotations read every candidate in turn. A budget of 0 reads no input row and defers
/// every candidate.
pub async fn record_join_hop_spends_bounded(
    storage: &dyn crate::pot::storage::PotStorage,
    pot_txid: &str,
    max_reads: usize,
    rotation: u64,
) -> JoinHopSpends {
    let mut out = JoinHopSpends::default();
    let pot_txid = pot_txid.to_ascii_lowercase();
    let beef = match storage.get_beef(&pot_txid).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            out.skipped = Some("no stored bytes");
            return out;
        }
        Err(_) => {
            out.faults += 1;
            out.skipped = Some("stored bytes unreadable");
            return out;
        }
    };
    let Ok(parsed) = beef_limits::parse_beef(&beef, &beef_limits::DISCOVERY_BEEF_LIMITS) else {
        out.skipped = Some("stored bytes do not parse");
        return out;
    };
    let subject = match parsed.atomic_txid.as_deref() {
        Some(txid) => parsed.find_txid(txid),
        None => parsed.txs.last(),
    };
    let Some(join) = subject.and_then(|s| s.tx()) else {
        out.skipped = Some("no subject transaction");
        return out;
    };
    if !join.id().eq_ignore_ascii_case(&pot_txid) {
        out.skipped = Some("the stored subject is another transaction");
        return out;
    }
    if !join
        .outputs
        .iter()
        .any(|o| is_pot_covenant_script(&o.locking_script.to_binary()))
    {
        out.skipped = Some("no pot covenant output");
        return out;
    }
    match storage.get_spent_status(&pot_txid, 0).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            out.skipped = Some("the pot is not held");
            return out;
        }
        Err(_) => {
            out.faults += 1;
            out.skipped = Some("the pot's row is unreadable");
            return out;
        }
    }
    // The candidates, with no read: a NAMED input (its source carried, the spent output P2PKH, `join_hop_inputs`'
    // rule) is written once its row exists; an input whose source the bytes do not carry is written only if its row is
    // a hop row; a carried source whose spent output is not P2PKH is never a hop and is never read.
    let mut named: Vec<(String, u32, bool)> = Vec::new();
    let mut uncarried: Vec<(String, u32, bool)> = Vec::new();
    for input in &join.inputs {
        let Ok(source) = input.get_source_txid() else {
            continue;
        };
        let vout = input.source_output_index;
        match parsed.find_txid(&source).and_then(|t| t.tx()) {
            Some(source_tx) => {
                let p2pkh = source_tx
                    .outputs
                    .get(vout as usize)
                    .is_some_and(|o| is_p2pkh_script(&o.locking_script.to_binary()));
                if p2pkh {
                    named.push((source.to_ascii_lowercase(), vout, true));
                }
            }
            None => uncarried.push((source.to_ascii_lowercase(), vout, false)),
        }
    }
    named.extend(uncarried);
    let candidates = named;
    let n = candidates.len();
    let take = n.min(max_reads.min(JOIN_HOP_INPUTS_MAX));
    out.deferred = n - take;
    let start = if take < n && take > 0 {
        ((u128::from(rotation) * take as u128) % n as u128) as usize
    } else {
        0
    };
    let final_bytes = !(join.lock_time > 0 && join.inputs.iter().any(|i| i.sequence < 0xffff_ffff));
    for i in 0..take {
        let (source, vout, is_named) = &candidates[(start + i) % n];
        let vout = *vout;
        out.examined += 1;
        let row = match storage.get_spent_status(source, vout).await {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(_) => {
                out.faults += 1;
                continue;
            }
        };
        if !(*is_named || crate::pot::storage::is_hop_row(&row)) {
            continue;
        }
        if row.spent || row.spending_txid.is_some() {
            out.already_spent += 1;
            continue;
        }
        match storage
            .mark_spent_if_unpointed(source, vout, &pot_txid, Some(final_bytes))
            .await
        {
            Ok(true) => out.marked.push(Outpoint::new(source.clone(), vout)),
            Ok(false) => out.already_spent += 1,
            Err(_) => out.faults += 1,
        }
    }
    out
}

/// LOWFUND Topic Manager — identifies admissible LOW hop (P2PKH) outputs.
pub struct LowFundTopicManager;

impl LowFundTopicManager {
    /// Create a new LOWFUND Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for LowFundTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for LowFundTopicManager {
    /// Admission is judged on the transaction alone: no off-chain value is read.
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
        // Pure: this manager holds no state, so a dry run is the same work.
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let mut outputs_to_admit = Vec::new();

        for (i, output) in tx.outputs.iter().enumerate() {
            if is_p2pkh_script(&output.locking_script.to_binary()) {
                debug!("LOWFUND: admitted P2PKH output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Non-P2PKH outputs (the pot covenant, OP_RETURN carriers, …)
            // are skipped silently — the pot side is tm_pot's job.
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    /// A JOIN's hop inputs whose bodies the BEEF carries ([`join_hop_inputs`]);
    /// nothing for any other transaction.
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        Ok(join_hop_inputs(beef))
    }

    async fn get_documentation(&self) -> String {
        "tm_lowfund — LOW hop (funding staging coin) index. Admits every \
         standard P2PKH output of an explicitly-submitted tx so the hop \
         outpoint's spend (by the JOIN or the pre-signed hop sweep) is \
         queryable from the same pot_records landing-proof store as tm_pot. \
         Rows are hints; money decisions anchor on ARC/SPV evidence."
            .to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "LOWFUND Topic Manager".to_string(),
            description: Some(
                "Indexes LOW hop (P2PKH funding staging) outputs so their spend \
                 (JOIN / hop sweep) is queryable — the last WhatsOnChain client \
                 read retires."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pot::topic_manager::tests::{covenant_script, dummy_params};
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    /// A standard 25-byte P2PKH lock with a salted pkh.
    fn p2pkh_script(salt: u8) -> Vec<u8> {
        let mut s = vec![0x76, 0xa9, 0x14];
        s.extend([salt; 20]);
        s.extend([0x88, 0xac]);
        s
    }

    /// A one-input tx carrying the given locking scripts, as BEEF bytes.
    fn beef_with_outputs(scripts: Vec<Vec<u8>>) -> Vec<u8> {
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
        for s in scripts {
            tx.add_output(TransactionOutput {
                satoshis: Some(546),
                locking_script: LockingScript::from_binary(&s).unwrap(),
                change: false,
            })
            .unwrap();
        }
        tx.to_beef(true).expect("BEEF serialization")
    }

    #[tokio::test]
    async fn admits_every_p2pkh_output_and_nothing_else() {
        let mgr = LowFundTopicManager::new();
        // vout 0: P2PKH (the hop) · vout 1: pot covenant · vout 2: OP_RETURN
        // · vout 3: P2PKH (wallet change).
        let beef = beef_with_outputs(vec![
            p2pkh_script(0x11),
            covenant_script(&dummy_params()),
            vec![0x00, 0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef],
            p2pkh_script(0x22),
        ]);
        let got = mgr
            .identify_admissible_outputs(
                &Tx::from_beef(&beef, None).expect("engine-side parse"),
                &[],
                None,
                SubmitMode::CurrentTx,
                &TopicAdmittanceContext::default(),
            )
            .await
            .unwrap();
        assert_eq!(got.outputs_to_admit, vec![0, 3]);
    }

    #[tokio::test]
    async fn no_p2pkh_outputs_admits_nothing() {
        let mgr = LowFundTopicManager::new();
        let beef = beef_with_outputs(vec![covenant_script(&dummy_params())]);
        let got = mgr
            .identify_admissible_outputs(
                &Tx::from_beef(&beef, None).expect("engine-side parse"),
                &[],
                None,
                SubmitMode::CurrentTx,
                &TopicAdmittanceContext::default(),
            )
            .await
            .unwrap();
        assert!(got.outputs_to_admit.is_empty());
    }

    /// A JOIN (a covenant output) over carried sources: its P2PKH-sourced
    /// inputs are named, a non-P2PKH source and an absent one are not, and
    /// a transaction with no covenant names nothing (FIX-791-C).
    #[tokio::test]
    async fn names_a_joins_carried_p2pkh_hop_inputs_and_nothing_else() {
        let source = |scripts: Vec<Vec<u8>>| {
            let mut tx = Tx::new();
            tx.add_input(TransactionInput::new("00".repeat(32), 0))
                .unwrap();
            for s in scripts {
                tx.add_output(TransactionOutput {
                    satoshis: Some(546),
                    locking_script: LockingScript::from_binary(&s).unwrap(),
                    change: false,
                })
                .unwrap();
            }
            tx
        };
        let hop_a = source(vec![p2pkh_script(0x11)]);
        let hop_b = source(vec![vec![0x51], p2pkh_script(0x12)]);
        let spend = |tx: &Tx, vout: u32| {
            let mut input = TransactionInput::new(tx.id(), vout);
            input.source_transaction = Some(Box::new(tx.clone()));
            input
        };
        let mut join = Tx::new();
        join.inputs.push(spend(&hop_a, 0));
        join.inputs.push(spend(&hop_b, 0)); // its source output is no P2PKH
        join.inputs.push(spend(&hop_b, 1));
        join.inputs.push(TransactionInput::new("cc".repeat(32), 0)); // absent
        join.outputs.push(TransactionOutput {
            satoshis: Some(1000),
            locking_script: LockingScript::from_binary(&covenant_script(&dummy_params())).unwrap(),
            change: false,
        });
        let atomic = |tx: &Tx| {
            bsv_rs::transaction::Beef::from_binary(&tx.to_beef(true).unwrap())
                .unwrap()
                .to_binary_atomic(&tx.id())
                .unwrap()
        };
        let mgr = LowFundTopicManager::new();
        let named = mgr
            .identify_needed_inputs(&atomic(&join), None)
            .await
            .unwrap();
        assert_eq!(
            named,
            vec![Outpoint::new(hop_a.id(), 0), Outpoint::new(hop_b.id(), 1)]
        );
        // The same inputs with no covenant: no JOIN, nothing named.
        let mut plain = join.clone();
        plain.outputs[0].locking_script = LockingScript::from_binary(&p2pkh_script(0x33)).unwrap();
        plain.invalidate_caches();
        assert!(mgr
            .identify_needed_inputs(&atomic(&plain), None)
            .await
            .unwrap()
            .is_empty());
        // A hop names nothing; bytes that do not parse name nothing.
        assert!(mgr
            .identify_needed_inputs(&atomic(&hop_a), None)
            .await
            .unwrap()
            .is_empty());
        assert!(join_hop_inputs(&[0xde, 0xad]).is_empty());
    }

    /// bsv-low #837 (FIX-837) pin (a): a JOIN held by the index whose hop rows read unspent (it entered by a road
    /// other than the door's `tm_lowfund` submit) has each hop marked spent by the JOIN, from its own stored bytes
    /// (carried, and compacted to the JOIN alone); a pointer already there, a row that does not exist, a pot the index
    /// does not hold and a transaction that is no JOIN are left alone; a second call marks nothing.
    #[tokio::test]
    async fn fix837_a_a_held_joins_hop_rows_are_marked_spent_from_its_stored_bytes() {
        use crate::pot::storage::{MemoryPotStorage, PotRecord, PotStorage};
        let source = |salt: u8| {
            let mut tx = Tx::new();
            tx.add_input(TransactionInput::new(format!("{salt:02x}").repeat(32), 0))
                .unwrap();
            tx.add_output(TransactionOutput {
                satoshis: Some(20_000),
                locking_script: LockingScript::from_binary(&p2pkh_script(salt)).unwrap(),
                change: false,
            })
            .unwrap();
            tx
        };
        let (hop_a, hop_b, hop_c) = (source(0x41), source(0x42), source(0x43));
        let join_over = |carry: bool, hops: &[&Tx]| {
            let mut join = Tx::new();
            for h in hops {
                let mut input = TransactionInput::new(h.id(), 0);
                if carry {
                    input.source_transaction = Some(Box::new((*h).clone()));
                }
                join.inputs.push(input);
            }
            join.outputs.push(TransactionOutput {
                satoshis: Some(39_800),
                locking_script: LockingScript::from_binary(&covenant_script(&dummy_params()))
                    .unwrap(),
                change: false,
            });
            join
        };
        let atomic = |tx: &Tx| {
            bsv_rs::transaction::Beef::from_binary(&tx.to_beef(true).unwrap())
                .unwrap()
                .to_binary_atomic(&tx.id())
                .unwrap()
        };
        let hop_row = |tx: &Tx, spender: Option<&str>| PotRecord {
            txid: tx.id(),
            output_index: 0,
            spent: spender.is_some(),
            spending_txid: spender.map(str::to_string),
            lock_kind: Some("p2pkh".into()),
            ..Default::default()
        };
        let pot_row = |txid: &str| PotRecord {
            txid: txid.to_string(),
            output_index: 0,
            lock_kind: Some("covenant".into()),
            ..Default::default()
        };

        // The carried JOIN over hops a and b, b already pointed elsewhere; c has no row at all.
        let join = join_over(true, &[&hop_a, &hop_b, &hop_c]);
        let s = MemoryPotStorage::default();
        s.store_record(&pot_row(&join.id())).await.unwrap();
        s.store_record(&hop_row(&hop_a, None)).await.unwrap();
        s.store_record(&hop_row(&hop_b, Some(&"ee".repeat(32))))
            .await
            .unwrap();
        s.store_beef(&join.id(), &atomic(&join)).await.unwrap();
        let got = record_join_hop_spends(&s, &join.id()).await;
        assert_eq!(got.marked, vec![Outpoint::new(hop_a.id(), 0)], "{got:?}");
        assert_eq!(got.already_spent, 1);
        let a = s.get_spent_status(&hop_a.id(), 0).await.unwrap().unwrap();
        assert!(
            a.spent && !a.spent_confirmed,
            "an unconfirmed pointer: {a:?}"
        );
        assert_eq!(a.spending_txid.as_deref(), Some(join.id().as_str()));
        let b = s.get_spent_status(&hop_b.id(), 0).await.unwrap().unwrap();
        assert_eq!(b.spending_txid, Some("ee".repeat(32)), "never re-pointed");
        assert!(
            s.get_spent_status(&hop_c.id(), 0).await.unwrap().is_none(),
            "never created"
        );
        assert!(
            record_join_hop_spends(&s, &join.id())
                .await
                .marked
                .is_empty(),
            "idempotent"
        );

        // A mined JOIN whose stored bytes were compacted to the JOIN alone: the existing hop rows still name it.
        let bare = join_over(false, &[&hop_a, &hop_c]);
        let s = MemoryPotStorage::default();
        s.store_record(&pot_row(&bare.id())).await.unwrap();
        s.store_record(&hop_row(&hop_a, None)).await.unwrap();
        s.store_beef(&bare.id(), &atomic(&bare)).await.unwrap();
        let got = record_join_hop_spends(&s, &bare.id()).await;
        assert_eq!(got.marked, vec![Outpoint::new(hop_a.id(), 0)], "{got:?}");

        // The pot not held (its rows evicted): nothing written.
        let s = MemoryPotStorage::default();
        s.store_record(&hop_row(&hop_a, None)).await.unwrap();
        s.store_beef(&join.id(), &atomic(&join)).await.unwrap();
        let got = record_join_hop_spends(&s, &join.id()).await;
        assert!(
            got.marked.is_empty() && got.skipped == Some("the pot is not held"),
            "{got:?}"
        );
        assert!(
            !s.get_spent_status(&hop_a.id(), 0)
                .await
                .unwrap()
                .unwrap()
                .spent
        );

        // No JOIN (no covenant output), and stored bytes of another transaction: nothing written.
        let mut plain = join_over(true, &[&hop_a]);
        plain.outputs[0].locking_script = LockingScript::from_binary(&p2pkh_script(0x44)).unwrap();
        plain.invalidate_caches();
        let s = MemoryPotStorage::default();
        s.store_record(&pot_row(&plain.id())).await.unwrap();
        s.store_record(&hop_row(&hop_a, None)).await.unwrap();
        s.store_beef(&plain.id(), &atomic(&plain)).await.unwrap();
        assert_eq!(
            record_join_hop_spends(&s, &plain.id()).await.skipped,
            Some("no pot covenant output")
        );
        s.store_beef(&join.id(), &atomic(&plain)).await.unwrap();
        assert_eq!(
            record_join_hop_spends(&s, &join.id()).await.skipped,
            Some("the stored subject is another transaction"),
            "the FIX-837 lens's N5: its own reason"
        );
        assert!(
            !s.get_spent_status(&hop_a.id(), 0)
                .await
                .unwrap()
                .unwrap()
                .spent
        );
    }

    /// The FIX-837 fold's store: a [`MemoryPotStorage`] that counts the row reads, and (when armed) lands ANOTHER
    /// writer's unconfirmed pointer on a hop row right after the read that saw it unpointed (the conflict pass's
    /// competitor between the shared write's read and its write, the lens's LOW-3).
    struct Fix837Store {
        inner: crate::pot::storage::MemoryPotStorage,
        reads: std::sync::atomic::AtomicUsize,
        race_with: Option<String>,
    }

    #[async_trait(?Send)]
    impl crate::pot::storage::PotStorage for Fix837Store {
        async fn store_record(
            &self,
            r: &crate::pot::storage::PotRecord,
        ) -> Result<(), crate::pot::storage::PotStorageError> {
            self.inner.store_record(r).await
        }
        async fn get_spent_status(
            &self,
            txid: &str,
            output_index: u32,
        ) -> Result<Option<crate::pot::storage::PotRecord>, crate::pot::storage::PotStorageError>
        {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let row = self.inner.get_spent_status(txid, output_index).await?;
            if let (Some(r), Some(rival)) = (&row, &self.race_with) {
                if crate::pot::storage::is_hop_row(r) && !r.spent {
                    self.inner
                        .mark_spent(txid, output_index, rival, false, None, None, None)
                        .await?;
                }
            }
            Ok(row)
        }
        #[allow(clippy::too_many_arguments)]
        async fn mark_spent(
            &self,
            txid: &str,
            output_index: u32,
            spending_txid: &str,
            confirmed: bool,
            verdict: Option<crate::pot::storage::VerdictWrite<'_>>,
            spent_height: Option<u64>,
            spender_final: Option<bool>,
        ) -> Result<(), crate::pot::storage::PotStorageError> {
            self.inner
                .mark_spent(
                    txid,
                    output_index,
                    spending_txid,
                    confirmed,
                    verdict,
                    spent_height,
                    spender_final,
                )
                .await
        }
        async fn mark_spent_if_unpointed(
            &self,
            txid: &str,
            output_index: u32,
            spending_txid: &str,
            spender_final: Option<bool>,
        ) -> Result<bool, crate::pot::storage::PotStorageError> {
            self.inner
                .mark_spent_if_unpointed(txid, output_index, spending_txid, spender_final)
                .await
        }
        async fn store_beef(
            &self,
            txid: &str,
            beef: &[u8],
        ) -> Result<(), crate::pot::storage::PotStorageError> {
            self.inner.store_beef(txid, beef).await
        }
        async fn get_beef(
            &self,
            txid: &str,
        ) -> Result<Option<Vec<u8>>, crate::pot::storage::PotStorageError> {
            self.inner.get_beef(txid).await
        }
    }

    /// A stranger's covenant-shaped JOIN over `width` carried P2PKH outputs of one funding transaction, its pot row,
    /// its stored atomic bytes and a hop row for each input, in a [`Fix837Store`].
    async fn fix837_wide_join(width: u32, race_with: Option<String>) -> (Fix837Store, Tx, Tx) {
        use crate::pot::storage::{MemoryPotStorage, PotRecord, PotStorage};
        let mut funding = Tx::new();
        funding
            .add_input(TransactionInput::new("5a".repeat(32), 0))
            .unwrap();
        for i in 0..width {
            funding
                .add_output(TransactionOutput {
                    satoshis: Some(1_000),
                    locking_script: LockingScript::from_binary(&p2pkh_script((i % 200) as u8))
                        .unwrap(),
                    change: false,
                })
                .unwrap();
        }
        let mut join = Tx::new();
        for i in 0..width {
            let mut input = TransactionInput::new(funding.id(), i);
            input.source_transaction = Some(Box::new(funding.clone()));
            join.inputs.push(input);
        }
        join.outputs.push(TransactionOutput {
            satoshis: Some(u64::from(width) * 900),
            locking_script: LockingScript::from_binary(&covenant_script(&dummy_params())).unwrap(),
            change: false,
        });
        let s = Fix837Store {
            inner: MemoryPotStorage::default(),
            reads: Default::default(),
            race_with,
        };
        s.store_record(&PotRecord {
            txid: join.id(),
            output_index: 0,
            lock_kind: Some("covenant".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        for i in 0..width {
            s.store_record(&PotRecord {
                txid: funding.id(),
                output_index: i,
                lock_kind: Some("p2pkh".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        let atomic = bsv_rs::transaction::Beef::from_binary(&join.to_beef(true).unwrap())
            .unwrap()
            .to_binary_atomic(&join.id())
            .unwrap();
        s.store_beef(&join.id(), &atomic).await.unwrap();
        (s, funding, join)
    }

    /// bsv-low #837, the FIX-837 lens's MEDIUM-1: the shared write reads at most `JOIN_HOP_INPUTS_MAX` input rows of
    /// one JOIN, whatever its width; the rest are DEFERRED and counted, and successive rotations read them in turn.
    /// An honest two-hop JOIN is read whole. RED on 9d380f8c: one call read every one of the 64 inputs.
    #[tokio::test]
    async fn fix837f_m1_the_shared_write_reads_a_bounded_window_of_a_wide_joins_inputs() {
        use crate::pot::storage::PotStorage;
        let (s, funding, join) = fix837_wide_join(64, None).await;
        let got = record_join_hop_spends(&s, &join.id()).await;
        let reads = s.reads.load(std::sync::atomic::Ordering::SeqCst);
        // the pot's own row, then at most JOIN_HOP_INPUTS_MAX input rows
        assert_eq!(reads, 1 + JOIN_HOP_INPUTS_MAX, "one call's row reads");
        assert_eq!(got.marked.len(), JOIN_HOP_INPUTS_MAX, "{got:?}");
        assert_eq!((got.examined, got.deferred), (16, 48), "{got:?}");
        assert_eq!(
            got.marked,
            (0..16)
                .map(|i| Outpoint::new(funding.id(), i))
                .collect::<Vec<_>>(),
            "the first window, in input order"
        );
        // the next three windows (the discovery pass's successive hours) read the rest, and only once each
        for rotation in 1..4u64 {
            let got =
                record_join_hop_spends_bounded(&s, &join.id(), JOIN_HOP_INPUTS_MAX, rotation).await;
            assert_eq!(
                (got.marked.len(), got.examined, got.deferred),
                (16, 16, 48),
                "rotation {rotation}: {got:?}"
            );
            let lo = rotation as u32 * 16;
            assert_eq!(
                got.marked.first(),
                Some(&Outpoint::new(funding.id(), lo)),
                "rotation {rotation}"
            );
        }
        for i in 0..64 {
            let row = s
                .inner
                .get_spent_status(&funding.id(), i)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                row.spending_txid.as_deref(),
                Some(join.id().as_str()),
                "input {i}"
            );
        }
        // a caller's smaller budget: fewer reads, the rest counted; a budget of 0 reads no input row
        let (s, _, join) = fix837_wide_join(40, None).await;
        let got = record_join_hop_spends_bounded(&s, &join.id(), 5, 0).await;
        assert_eq!(
            (got.marked.len(), got.examined, got.deferred),
            (5, 5, 35),
            "{got:?}"
        );
        let got = record_join_hop_spends_bounded(&s, &join.id(), 0, 0).await;
        assert_eq!(
            (got.marked.len(), got.examined, got.deferred),
            (0, 0, 40),
            "{got:?}"
        );
        // an honest JOIN (two hops) is read whole, nothing deferred
        let (s, _, join) = fix837_wide_join(2, None).await;
        let got = record_join_hop_spends(&s, &join.id()).await;
        assert_eq!(
            (got.marked.len(), got.examined, got.deferred),
            (2, 2, 0),
            "{got:?}"
        );
    }

    /// bsv-low #837, the FIX-837 lens's LOW-3: a pointer another writer lands between the shared write's read and its
    /// write is never overwritten by the JOIN's (the guard is in the write). RED on 9d380f8c: the JOIN's unconfirmed
    /// `mark_spent` overwrote the rival's pointer.
    #[tokio::test]
    async fn fix837f_l3_a_pointer_landed_after_the_read_is_never_overwritten() {
        use crate::pot::storage::PotStorage;
        let rival = "cc".repeat(32);
        let (s, funding, join) = fix837_wide_join(2, Some(rival.clone())).await;
        let got = record_join_hop_spends(&s, &join.id()).await;
        for i in 0..2 {
            let row = s
                .inner
                .get_spent_status(&funding.id(), i)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                row.spending_txid.as_deref(),
                Some(rival.as_str()),
                "input {i}: the rival's pointer stands ({got:?})"
            );
        }
        assert!(got.marked.is_empty(), "{got:?}");
        assert_eq!(got.already_spent, 2, "counted as pointed: {got:?}");
    }

    #[test]
    fn p2pkh_recognizer_is_exact() {
        use crate::pot::is_p2pkh_script;
        let good = p2pkh_script(0x33);
        assert!(is_p2pkh_script(&good));
        // Wrong length (24 / 26 bytes).
        assert!(!is_p2pkh_script(&good[..24]));
        let mut long = good.clone();
        long.push(0x00);
        assert!(!is_p2pkh_script(&long));
        // Wrong opcodes at each fixed position.
        for i in [0usize, 1, 2, 23, 24] {
            let mut bad = good.clone();
            bad[i] ^= 0xff;
            assert!(!is_p2pkh_script(&bad), "byte {i} must be pinned");
        }
        // The covenant is not a P2PKH.
        assert!(!is_p2pkh_script(&covenant_script(&dummy_params())));
    }
}
