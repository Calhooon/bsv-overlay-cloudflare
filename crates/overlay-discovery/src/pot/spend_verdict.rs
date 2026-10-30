//! bsv-low #986 (READ-986 item 2(b), FIX-986): ONE classifier of a covenant pot's spend into its #284 VERDICT
//! GROUP, called by every writer that latches the group onto `pot_records`:
//!
//! - the door's spend hook (`lookup_service::PotLookupService::output_spent`, the settle's own admission);
//! - the discovery pass's spend write (`proof_fetcher::discover_spend_for_row`: a spend nobody presented, found
//!   by the couriers) and the confirmation pass (`proof_fetcher::complete_spend_confirmations`, its chase and its
//!   displacement arm), each when the spender's durable BEEF is held;
//! - the janitor's params backfill (`proof_fetcher::backfill_decoded_params`), over a row decoded late and over a
//!   spent, confirmed, decoded covenant row that carries no current verdict (the 27 beta rows of READ-986).
//!
//! Before this module the discovery and confirmation passes wrote the pointer with NO verdict and the backfill
//! skipped a decoded row, so a pre-signed refund broadcast at its recovery height, outside the overlay's door,
//! stayed unclassified in the record for good. D1's results view classified it per request from the spender's
//! BEEF (`results::classify_from_columns`); the owed actor's mirror holds no BEEF bytes (D2-ACTOR-B's "no bytes
//! held"), so it served `unbound` / `outcome unresolved` where D1 served `payout refund claimable`: both seats of
//! two games withheld on beta. The stored column is the ONE source both judges read.
//!
//! The evidence every write rests on, in order:
//! 1. the pot's COMMITTED params, decoded at admission from the funding output's own bytes (`PotRecord`'s
//!    decoded columns, `decoded_covenant_params`), and the funded value (`pot_sats`), which must equal
//!    `stakeA + stakeB` (the covenant's own in-script assertion; a disagreeing funding is not the pot the params
//!    describe, so nothing is classified);
//! 2. the SPENDER'S OWN BYTES, parsed hash-bound to the recorded pointer by the caller
//!    (`transaction_from_beef(.., Some(spending_txid), ..)`), which must spend the pot outpoint (the input's index
//!    feeds the #406 sighash, its sequence the refund's height gate);
//! 3. `classify_covenant`: the outputs against the four mandated exits, the refund's gate observed on the wire;
//! 4. `settle_signers_for_spend` (#406): WHO SIGNED, the spend's signatures verified against the committed triple
//!    over the BIP-143 digest; `None` is "not established", never a guess.
//!
//! A pure function of those two inputs: the same bytes give the same group at every door, so the hook's write at
//! admission and a janitor's latch a day later cannot disagree.

use bsv_rs::transaction::Transaction;

use super::covenant::{classify_covenant, CovenantParams, PotVerdict, RawTx};
use super::spend_signers::{settle_signers_for_spend, SettleSigners};
use super::storage::{PotRecord, VerdictWrite};

/// The #284 verdict group of ONE covenant pot spend, as classified from the spend's own bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpendVerdict {
    /// Which mandated exit the spend fired.
    pub verdict: PotVerdict,
    /// #406: which committed pair signed it; `None` when no pair verified ("not established").
    pub signers: Option<SettleSigners>,
}

impl SpendVerdict {
    /// The group as the DOOR writes it at admission: a pair not established stays NULL, and the signers backfill
    /// retries it once from the durable spender BEEF.
    pub fn write(&self) -> VerdictWrite<'static> {
        VerdictWrite {
            verdict: self.verdict.as_str(),
            settle_signers: self.signers.map(SettleSigners::as_str),
        }
    }

    /// The group as a writer holding the spender's DURABLE bytes concludes it: no pair verified is latched
    /// `'unresolved'` (re-derived and concluded, never re-scanned: the signers backfill's own rule).
    pub fn write_concluded(&self) -> VerdictWrite<'static> {
        VerdictWrite {
            verdict: self.verdict.as_str(),
            settle_signers: Some(self.signers.map_or("unresolved", SettleSigners::as_str)),
        }
    }
}

/// Classify the spend `spending_tx` of the covenant pot `pot_txid:pot_vout` whose committed params are `params`
/// and whose funded value is `pot_sats`. `None` is UNRESOLVED (never a guess): the stakes do not sum to the funded
/// value, the transaction does not spend the outpoint, an input lacks its source or an output its value, or the
/// outputs match no mandated exit.
pub fn classify_pot_spend_tx(
    params: &CovenantParams,
    pot_sats: u64,
    pot_txid: &str,
    pot_vout: u32,
    spending_tx: &Transaction,
) -> Option<SpendVerdict> {
    if params.stake_a.checked_add(params.stake_b)? != pot_sats {
        return None; // conservation failed: not the pot the params describe
    }
    let (pot_input_index, pot_input_sequence) =
        spending_tx.inputs.iter().enumerate().find_map(|(n, i)| {
            (i.source_txid
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case(pot_txid))
                && i.source_output_index == pot_vout)
                .then_some((n, i.sequence))
        })?;
    let spender = RawTx::from_transaction(spending_tx)?;
    let verdict = classify_covenant(params, &spender, pot_input_sequence)?;
    let signers =
        settle_signers_for_spend(params, pot_sats, &spending_tx.to_binary(), pot_input_index);
    Some(SpendVerdict { verdict, signers })
}

/// [`classify_pot_spend_tx`] over a stored row: its decoded params (a bare pot, an undecoded row and a malformed
/// column each answer `None`: bare pots never carry a stored verdict) and its funded value.
pub fn classify_record_spend(
    record: &PotRecord,
    spending_tx: &Transaction,
) -> Option<SpendVerdict> {
    let params = record.decoded_covenant_params()?;
    let pot_sats = record.pot_sats?;
    classify_pot_spend_tx(
        &params,
        pot_sats,
        &record.txid,
        record.output_index,
        spending_tx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pot::{covenant_lock_of, p2pkh_lock};
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{TransactionInput, TransactionOutput};

    fn params() -> CovenantParams {
        CovenantParams {
            pub_a: [0x02; 33],
            pub_b: [0x03; 33],
            pub_tower: [0x04; 33],
            pay_pkh_a: [0xAA; 20],
            pay_pkh_b: [0xBB; 20],
            rake_pkh: [0xCC; 20],
            stake_a: 10_000,
            stake_b: 10_000,
            fee_sats: 200,
            recovery_height: 900_000,
        }
    }

    fn record(p: &CovenantParams, pot_sats: u64) -> PotRecord {
        PotRecord {
            txid: "aa".repeat(32),
            output_index: 0,
            params_decoded: true,
            lock_kind: Some("covenant".into()),
            pub_a: Some(hex::encode(p.pub_a)),
            pub_b: Some(hex::encode(p.pub_b)),
            pub_tower: Some(hex::encode(p.pub_tower)),
            pay_pkh_a: Some(hex::encode(p.pay_pkh_a)),
            pay_pkh_b: Some(hex::encode(p.pay_pkh_b)),
            rake_pkh: Some(hex::encode(p.rake_pkh)),
            stake_a: Some(p.stake_a),
            stake_b: Some(p.stake_b),
            fee_sats: Some(p.fee_sats),
            recovery_height: Some(p.recovery_height),
            pot_sats: Some(pot_sats),
            ..Default::default()
        }
    }

    /// The pre-signed refund of the pot (the T_refund exit: each seat's stake less its fee share, nLockTime at the
    /// committed height, the pot input non-final).
    fn refund(
        p: &CovenantParams,
        pot: &str,
        vout: u32,
        lock_time: u32,
        sequence: u32,
    ) -> Transaction {
        let mut tx = Transaction::new();
        let mut input = TransactionInput::new(pot.to_string(), vout);
        input.sequence = sequence;
        tx.inputs.push(input);
        let fee_b = p.fee_sats / 2;
        let fee_a = p.fee_sats - fee_b;
        for (sats, pkh) in [
            (p.stake_a - fee_a, p.pay_pkh_a),
            (p.stake_b - fee_b, p.pay_pkh_b),
        ] {
            tx.outputs.push(TransactionOutput {
                satoshis: Some(sats),
                locking_script: LockingScript::from_binary(&p2pkh_lock(&pkh)).unwrap(),
                change: false,
            });
        }
        tx.lock_time = lock_time;
        tx
    }

    #[test]
    fn fix986_the_refund_exit_classifies_from_the_row_and_the_spenders_bytes() {
        let p = params();
        let r = record(&p, 20_000);
        let tx = refund(&p, &r.txid, 0, 900_100, 0xffff_fffe);
        let got = classify_record_spend(&r, &tx).expect("a refund-template spend classifies");
        assert_eq!(got.verdict, PotVerdict::Refund);
        assert_eq!(got.signers, None, "an unsigned fixture establishes no pair");
        assert_eq!(got.write(), VerdictWrite::bare("refund"));
        assert_eq!(
            got.write_concluded(),
            VerdictWrite {
                verdict: "refund",
                settle_signers: Some("unresolved")
            }
        );
        // the lock the params describe is the one the signers would verify against
        assert!(!covenant_lock_of(&p).is_empty());
    }

    #[test]
    fn fix986_nothing_is_classified_without_its_evidence() {
        let p = params();
        let r = record(&p, 20_000);
        // the gate not observed on the wire: a final sequence disables nLockTime
        assert_eq!(
            classify_record_spend(&r, &refund(&p, &r.txid, 0, 900_100, 0xffff_ffff)),
            None
        );
        // the spend of ANOTHER outpoint
        assert_eq!(
            classify_record_spend(&r, &refund(&p, &"bb".repeat(32), 0, 900_100, 0xffff_fffe)),
            None
        );
        // the funded value disagreeing with the committed stakes
        assert_eq!(
            classify_record_spend(
                &record(&p, 19_999),
                &refund(&p, &r.txid, 0, 900_100, 0xffff_fffe)
            ),
            None
        );
        // a bare pot (no decoded params) never carries a stored verdict
        let bare = PotRecord {
            lock_kind: Some("bare".into()),
            ..record(&p, 20_000)
        };
        assert_eq!(
            classify_record_spend(&bare, &refund(&p, &r.txid, 0, 900_100, 0xffff_fffe)),
            None
        );
        // a missing funded value
        let no_value = PotRecord {
            pot_sats: None,
            ..record(&p, 20_000)
        };
        assert_eq!(
            classify_record_spend(&no_value, &refund(&p, &r.txid, 0, 900_100, 0xffff_fffe)),
            None
        );
    }
}
