//! WHO SIGNED A POT SPEND — the server-side settle-signer classifier
//! (bsv-low #406).
//!
//! ── WHY THIS EXISTS ──────────────────────────────────────────────────────────
//! Since bsv-low #399 the leaderboard counts a covenant win from the chain
//! verdict alone, and #406 retires the loser's post-loss claim countersign —
//! so on every display surface "no countersignature" stops meaning
//! "tower-enforced" (it becomes the NORMAL ending of a cooperative hand).
//! The surfaces that narrate an ending (the withheld-ending line, the
//! chain-proven badge) still owe the player the true story, and the ONLY
//! evidence that distinguishes a cooperative settle from a tower-enforced one
//! is WHO SIGNED THE SPEND: the two signatures on the pot input, verified
//! against the committed `{pubA, pubB, pubTower}` triple. Signers `[A, B]` ⇒
//! cooperative (the tower never acted). A `Tower` in the pair ⇒ the tower's
//! key co-signed (an enforced settle — or the pre-signed 2-of-2 refund's
//! tower-parked sibling, which never carries a winner verdict, so the
//! serving guard `verdictTxid == spendingTxid` + verdict ∈ {winner-*}
//! bounds what the client may narrate from it).
//!
//! This is a MIRROR of the missing_j alarm's discriminator,
//! `bsv-low crates/low-spend/src/pot_spend.rs::pot_spend_signers` — the one
//! SSOT for "did the tower's key sign this spend", proven both ways on beta
//! (silent on honest endings, firing on the rogue-tower drill). The pair-scan
//! semantics below are kept byte-for-byte compatible with it; a divergence is
//! a stated finding, never silent.
//!
//! ── MALLEATION TOLERANCE (copied from the SSOT, and load-bearing) ────────────
//! `SIGHASH_ALL|FORKID` does not commit the input script, and a counterparty
//! holds both seat signatures after the 2-of-2 exchange — so it can PREPEND
//! junk pushes before the real pair, broadcast that variant first, and win the
//! race with a valid coop settle whose first two pushes are garbage. A
//! first-two-pushes reader would leave that honest outcome unclassified. So
//! [`classify_spend_signers`] SCANS every push for a lock-order pair that
//! verifies, and only reports `None` when NO pair anywhere does.
//! (Malleability does not weaken the verdict: a signature that verifies
//! against a lock key over the network's own digest is a signature that key
//! produced — re-encoding it proves nothing about who signed.)
//!
//! ── STATED RESIDUALS (gate 2026-08-21; both SSOT-identical, so changing them
//! here alone would be a silent divergence) ───────────────────────────────────
//! 1. PAIR PRECEDENCE: the scan tries `[A,B]` before the tower pairs, so a
//!    tower-enforced spend to which the LOSING seat voluntarily appends its
//!    own signature over the same digest classifies 'coop'. Narration-only
//!    (the money outcome is identical, and the seat ENDORSED the spend), and
//!    it grants no alarm evasion: making a tower-key spend read as coop
//!    requires BOTH seat keys — with which a thief needs no tower key at all.
//! 2. LEADING NON-PUSH OPCODES: `read_pushes` stops at the first non-push
//!    byte, so a malleator-prepended opcode (or an OP_1 CHECKMULTISIG dummy —
//!    BSV has no NULLDUMMY rule) hides every signature and the answer
//!    degrades to "not established" (the backfill latches 'unresolved').
//!    Never a wrong value — a forced abstention on a hand-malleated spend.
//!
//! ── TRUST POSTURE ────────────────────────────────────────────────────────────
//! Chain bytes in, chain facts out: the keys come from the covenant params
//! decoded at admission (hash-bound to the funding tx), the digest is the
//! BIP-143 `SIGHASH_ALL|FORKID` digest the network itself validated, and no
//! caller claim enters the decision. The result is DISPLAY-TIER: it feeds
//! narration (which ending sentence a row may print), never a count, a rank,
//! or a credit — and it must not become a WHERE/rank bar without its own
//! gate (the same rule the claimValid latch carries).

use bsv_rs::primitives::bsv::sighash::{
    compute_sighash_for_signing, parse_transaction, SighashParams, SIGHASH_ALL, SIGHASH_FORKID,
};
use bsv_rs::primitives::ec::{PublicKey, Signature};

use super::covenant::CovenantParams;

/// Cap on signature CANDIDATES the pair-scan will consider (gate MED-1,
/// 2026-08-21): `classify_spend_signers` runs INLINE in the spend hook, and
/// the unlock's push count is attacker-controlled — an uncapped scan over a
/// junk-padded unlock is a cheap CPU burn on the submit path (~72-byte valid
/// junk at 100 sat/KB). An honest unlock carries ≤3 pushes and the SSOT's
/// motivating malleation prepends a handful; 16 is far above both, and
/// over-cap degrades to `None` = "not established" — the contract's safe
/// direction, never a wrong value. (Junk fails its FIRST verify against a
/// lock key, so the capped worst case is ~a few hundred ECDSA ops, not
/// tens of thousands.)
pub const MAX_SIG_CANDIDATES: usize = 16;

/// Which pair of the three lock keys signed a pot spend, as stored in
/// `pot_records.settleSigners` and served to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleSigners {
    /// `[A, B]` — the seats settled it themselves; the tower never acted.
    Coop,
    /// `[A, Tower]` — the tower co-signed with SEAT A.
    TowerA,
    /// `[B, Tower]` — the tower co-signed with SEAT B.
    TowerB,
}

impl SettleSigners {
    /// The wire string — ALSO the exact value stored in the
    /// `pot_records.settleSigners` column (`from_wire` is its inverse).
    pub fn as_str(self) -> &'static str {
        match self {
            SettleSigners::Coop => "coop",
            SettleSigners::TowerA => "tower-a",
            SettleSigners::TowerB => "tower-b",
        }
    }

    /// Parse a stored/wire value. Anything unrecognized is `None` (an unknown
    /// stored value degrades to not-established, never a guess).
    pub fn from_wire(s: &str) -> Option<SettleSigners> {
        match s {
            "coop" => Some(SettleSigners::Coop),
            "tower-a" => Some(SettleSigners::TowerA),
            "tower-b" => Some(SettleSigners::TowerB),
            _ => None,
        }
    }

    /// True when the tower's key is one of the two signers — the enforced
    /// family (a `J` is owed for this spend, bsv-low's missing_j contract).
    pub fn tower_signed(self) -> bool {
        !matches!(self, SettleSigners::Coop)
    }
}

/// The pushed blobs of a script, in order, stopping at the first non-push
/// opcode or a truncated push. Mirrors the SSOT's `pot_spend::read_pushes`
/// (and `potparty::read_pushes` — kept separate because that one is private
/// to its module and borrows). All offset math is checked: wasm32 `usize` is
/// 32-bit and a push length can be four attacker-controlled bytes.
fn read_pushes(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let op = bytes[i];
        i += 1;
        let len = match op {
            n if n < 0x4c => n as usize,
            0x4c => {
                if i >= bytes.len() {
                    return out;
                }
                let l = bytes[i] as usize;
                i += 1;
                l
            }
            0x4d => {
                if i + 2 > bytes.len() {
                    return out;
                }
                let l = bytes[i] as usize | ((bytes[i + 1] as usize) << 8);
                i += 2;
                l
            }
            0x4e => {
                if i + 4 > bytes.len() {
                    return out;
                }
                let l = u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]])
                    as usize;
                i += 4;
                l
            }
            _ => return out,
        };
        // Subtract-form bound: `i <= bytes.len()` holds here, so
        // `bytes.len() - i` cannot underflow and no addition can overflow.
        if i > bytes.len() || len > bytes.len() - i {
            return out;
        }
        out.push(bytes[i..i + len].to_vec());
        i += len;
    }
    out
}

/// DER-verify one signature candidate against one key over the digest.
/// Any parse failure (garbage push, foreign key bytes) is simply "does not
/// verify" — adversarial pushes must never panic or abort the scan.
fn verifies(key: &[u8; 33], digest: &[u8; 32], der: &[u8]) -> bool {
    let Ok(pk) = PublicKey::from_bytes(key) else {
        return false;
    };
    let Ok(sig) = Signature::from_der(der) else {
        return false;
    };
    sig.verify(digest, &pk)
}

/// The signer pair of a pot spend, from its own bytes — malleation-tolerant
/// (see the module docs).
///
/// * `params` — the covenant params decoded at admission (the key triple).
/// * `funding_lock` / `pot_sats` — the funded output being spent (the BIP-143
///   scriptCode + value; callers take them from the stored funding BEEF,
///   never from a caller claim).
/// * `spender_raw` — the raw bytes of the spending transaction.
/// * `pot_input_index` — the spender's input that spends the pot outpoint
///   (the caller locates it; this function does not re-check the match).
///
/// Every non-empty push (trailing sighash-type byte stripped) is a signature
/// candidate; the first lock-order pair `(i, j)` with distinct pushes
/// verifying against `keys[i]` and `keys[j]` wins, matching `CHECKMULTISIG`'s
/// in-order requirement. `None` = no pair verifies (a foreign signer, a
/// non-multisig spend, unreadable bytes) — honestly not established, never a
/// guess.
pub fn classify_spend_signers(
    params: &CovenantParams,
    funding_lock: &[u8],
    pot_sats: u64,
    spender_raw: &[u8],
    pot_input_index: usize,
) -> Option<SettleSigners> {
    let keys: [[u8; 33]; 3] = [params.pub_a, params.pub_b, params.pub_tower];
    let parsed = parse_transaction(spender_raw).ok()?;
    let input = parsed.inputs.get(pot_input_index)?;

    // The digest the network validated: BIP-143 SIGHASH_ALL|FORKID over the
    // funding lock + value, in internal byte order (what ECDSA verify takes).
    let digest = compute_sighash_for_signing(&SighashParams {
        version: parsed.version,
        inputs: &parsed.inputs,
        outputs: &parsed.outputs,
        locktime: parsed.locktime,
        input_index: pot_input_index,
        subscript: funding_lock,
        satoshis: pot_sats,
        scope: SIGHASH_ALL | SIGHASH_FORKID,
    });

    // Every non-empty push with the trailing sighash-type byte stripped is a
    // candidate. Position is NOT assumed — a malleator can pad the stack.
    let ders: Vec<Vec<u8>> = read_pushes(&input.script)
        .into_iter()
        .filter(|p| !p.is_empty())
        .filter_map(|mut p| {
            p.pop()?; // drop the trailing sighash-type byte
            if p.is_empty() {
                None
            } else {
                Some(p)
            }
        })
        .take(MAX_SIG_CANDIDATES) // gate MED-1: bound the submit-path scan
        .collect();
    if ders.len() < 2 {
        return None;
    }

    // In lock order: find i < j and DISTINCT pushes verifying against keys[i]
    // and keys[j]. Scanning all push pairs makes prepended/appended junk
    // harmless (the SSOT's exact loop shape).
    for i in 0..3usize {
        for j in (i + 1)..3usize {
            for a in 0..ders.len() {
                if !verifies(&keys[i], &digest, &ders[a]) {
                    continue;
                }
                for (b, der_b) in ders.iter().enumerate() {
                    if b == a {
                        continue;
                    }
                    if verifies(&keys[j], &digest, der_b) {
                        return Some(match (i, j) {
                            (0, 1) => SettleSigners::Coop,
                            (0, 2) => SettleSigners::TowerA,
                            _ => SettleSigners::TowerB, // (1, 2)
                        });
                    }
                }
            }
        }
    }
    None
}

/// The ONE production entry point (bsv-low #406): classify a covenant pot
/// spend's signers from the committed params + the spender's raw bytes.
///
/// Rebuilds the funding lock from the params ([`covenant_lock_of`] — proven
/// inverse of the extractor, so byte-exact for every builder-made lock; a
/// non-canonical hand-built lock degrades to `None`, never a wrong answer)
/// and re-checks stake conservation (`stakeA + stakeB == pot_sats`, the
/// covenant's own in-script assert) before verifying anything — a funding
/// value that disagrees with the committed stakes is not the pot the params
/// describe, exactly the verdict classifier's refusal.
pub fn settle_signers_for_spend(
    params: &CovenantParams,
    pot_sats: u64,
    spender_raw: &[u8],
    pot_input_index: usize,
) -> Option<SettleSigners> {
    if params.stake_a.checked_add(params.stake_b)? != pot_sats {
        return None; // conservation failed — never classify
    }
    let lock = crate::pot::covenant_lock_of(params);
    classify_spend_signers(params, &lock, pot_sats, spender_raw, pot_input_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;

    /// Deterministic keys: scalar 1 = seat A, 2 = seat B, 3 = the tower.
    fn key(scalar: u8) -> PrivateKey {
        let mut b = [0u8; 32];
        b[31] = scalar;
        PrivateKey::from_bytes(&b).expect("nonzero scalar")
    }

    fn params_with(keys: &[PrivateKey; 3]) -> CovenantParams {
        CovenantParams {
            pub_a: keys[0].public_key().to_compressed(),
            pub_b: keys[1].public_key().to_compressed(),
            pub_tower: keys[2].public_key().to_compressed(),
            pay_pkh_a: [0x11; 20],
            pay_pkh_b: [0x22; 20],
            rake_pkh: [0x33; 20],
            stake_a: 500,
            stake_b: 500,
            fee_sats: 6,
            recovery_height: 900_000,
        }
    }

    /// An arbitrary (but fixed) funding lock: classification depends only on
    /// the digest it feeds, not on the lock being a real covenant — the
    /// production callers pass the stored funding output's actual script.
    const LOCK: &[u8] = &[0x51, 0x52, 0x53, 0xae]; // OP_1 OP_2 OP_3 CHECKMULTISIG (shape irrelevant)
    const POT_SATS: u64 = 1000;

    /// A minimal 1-input 1-output spender with the given unlock script.
    fn spender_raw(unlock: &[u8]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1i32.to_le_bytes()); // version
        raw.push(1); // input count
        raw.extend_from_slice(&[0xaa; 32]); // prev txid (internal order)
        raw.extend_from_slice(&0u32.to_le_bytes()); // prev vout
        assert!(unlock.len() < 0xfd, "test unlock fits a 1-byte varint");
        raw.push(unlock.len() as u8);
        raw.extend_from_slice(unlock);
        raw.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // sequence
        raw.push(1); // output count
        raw.extend_from_slice(&994u64.to_le_bytes()); // value
        raw.push(3); // script len
        raw.extend_from_slice(&[0x76, 0xa9, 0x88]); // arbitrary
        raw.extend_from_slice(&0u32.to_le_bytes()); // locktime
        raw
    }

    /// Sign the spend's own digest with `sk` and return the wire push
    /// (DER || sighash byte), exactly what a real unlock carries.
    fn wire_sig(sk: &PrivateKey, raw_without_sigs: &[u8]) -> Vec<u8> {
        let parsed = parse_transaction(raw_without_sigs).expect("test tx parses");
        let digest = compute_sighash_for_signing(&SighashParams {
            version: parsed.version,
            inputs: &parsed.inputs,
            outputs: &parsed.outputs,
            locktime: parsed.locktime,
            input_index: 0,
            subscript: LOCK,
            satoshis: POT_SATS,
            scope: SIGHASH_ALL | SIGHASH_FORKID,
        });
        let mut push = sk.sign(&digest).expect("signs").to_der();
        push.push((SIGHASH_ALL | SIGHASH_FORKID) as u8);
        push
    }

    /// Build the spend: sign over an EMPTY unlock (the digest does not commit
    /// the input script under SIGHASH_ALL|FORKID — scriptCode is the funding
    /// lock), then splice the pushes in.
    fn spend_signed_by(first: &PrivateKey, second: &PrivateKey) -> Vec<u8> {
        let skeleton = spender_raw(&[]);
        let s1 = wire_sig(first, &skeleton);
        let s2 = wire_sig(second, &skeleton);
        let mut unlock = vec![0x00]; // the CHECKMULTISIG null dummy
        unlock.push(s1.len() as u8);
        unlock.extend_from_slice(&s1);
        unlock.push(s2.len() as u8);
        unlock.extend_from_slice(&s2);
        spender_raw(&unlock)
    }

    #[test]
    fn coop_and_both_tower_pairs_classify_by_who_signed() {
        let keys = [key(1), key(2), key(3)];
        let p = params_with(&keys);
        let cases: [(usize, usize, SettleSigners); 3] = [
            (0, 1, SettleSigners::Coop),
            (0, 2, SettleSigners::TowerA),
            (1, 2, SettleSigners::TowerB),
        ];
        for (i, j, want) in cases {
            let raw = spend_signed_by(&keys[i], &keys[j]);
            assert_eq!(
                classify_spend_signers(&p, LOCK, POT_SATS, &raw, 0),
                Some(want),
                "pair ({i},{j})"
            );
        }
    }

    #[test]
    fn push_order_is_not_trusted_a_reversed_pair_still_classifies() {
        // CHECKMULTISIG requires lock order on the STACK, but the scan must
        // not assume it in the BYTES (malleation tolerance): reversed pushes
        // still name the same two keys.
        let keys = [key(1), key(2), key(3)];
        let p = params_with(&keys);
        let raw = spend_signed_by(&keys[1], &keys[0]); // B then A
        assert_eq!(
            classify_spend_signers(&p, LOCK, POT_SATS, &raw, 0),
            Some(SettleSigners::Coop)
        );
    }

    #[test]
    fn prepended_junk_pushes_do_not_hide_the_real_pair() {
        // The malleation the SSOT exists for: junk pushes BEFORE the real
        // signatures (a first-two-pushes reader reports garbage).
        let keys = [key(1), key(2), key(3)];
        let p = params_with(&keys);
        let skeleton = spender_raw(&[]);
        let s1 = wire_sig(&keys[0], &skeleton);
        let s2 = wire_sig(&keys[2], &skeleton);
        let mut unlock = vec![0x00];
        for junk in [&[0xde, 0xad][..], &[0xbe, 0xef, 0x01][..]] {
            unlock.push(junk.len() as u8);
            unlock.extend_from_slice(junk);
        }
        unlock.push(s1.len() as u8);
        unlock.extend_from_slice(&s1);
        unlock.push(s2.len() as u8);
        unlock.extend_from_slice(&s2);
        let raw = spender_raw(&unlock);
        assert_eq!(
            classify_spend_signers(&p, LOCK, POT_SATS, &raw, 0),
            Some(SettleSigners::TowerA)
        );
    }

    #[test]
    fn the_candidate_cap_bounds_the_scan_and_fails_safe(// gate MED-1
    ) {
        let keys = [key(1), key(2), key(3)];
        let p = params_with(&keys);
        let skeleton = spender_raw(&[]);
        let s1 = wire_sig(&keys[0], &skeleton);
        let s2 = wire_sig(&keys[1], &skeleton);
        let build = |junk_count: usize| {
            let mut unlock = vec![0x00];
            for _ in 0..junk_count {
                unlock.push(2);
                unlock.extend_from_slice(&[0xde, 0xad]);
            }
            unlock.push(s1.len() as u8);
            unlock.extend_from_slice(&s1);
            unlock.push(s2.len() as u8);
            unlock.extend_from_slice(&s2);
            spender_raw(&unlock)
        };
        // Junk within the cap: the real pair is still found.
        let within = build(MAX_SIG_CANDIDATES - 2);
        assert_eq!(
            classify_spend_signers(&p, LOCK, POT_SATS, &within, 0),
            Some(SettleSigners::Coop)
        );
        // A flood pushing the real pair PAST the cap degrades to None — the
        // safe direction ("not established"), never a wrong value, and the
        // submit-path scan stays bounded.
        let beyond = build(MAX_SIG_CANDIDATES);
        assert_eq!(classify_spend_signers(&p, LOCK, POT_SATS, &beyond, 0), None);
    }

    #[test]
    fn foreign_signers_single_sig_and_wrong_digest_are_none_never_a_guess() {
        let keys = [key(1), key(2), key(3)];
        let p = params_with(&keys);
        // A pair of FOREIGN keys (a spend of some other lock entirely).
        let foreign = spend_signed_by(&key(7), &key(8));
        assert_eq!(
            classify_spend_signers(&p, LOCK, POT_SATS, &foreign, 0),
            None
        );
        // Only ONE real signature — never enough for a pair verdict.
        let skeleton = spender_raw(&[]);
        let s1 = wire_sig(&keys[0], &skeleton);
        let mut unlock = vec![0x00];
        unlock.push(s1.len() as u8);
        unlock.extend_from_slice(&s1);
        let single = spender_raw(&unlock);
        assert_eq!(classify_spend_signers(&p, LOCK, POT_SATS, &single, 0), None);
        // The right pair over the WRONG value (a lying pot_sats shifts the
        // digest): nothing verifies — the classifier can never be fed a
        // mismatched funding fact and still produce a verdict.
        let real = spend_signed_by(&keys[0], &keys[1]);
        assert_eq!(
            classify_spend_signers(&p, LOCK, POT_SATS + 1, &real, 0),
            None
        );
        // A missing input index is None, not a panic.
        assert_eq!(classify_spend_signers(&p, LOCK, POT_SATS, &real, 5), None);
        // Unparseable spender bytes are None, not a panic.
        assert_eq!(
            classify_spend_signers(&p, LOCK, POT_SATS, &[0x01, 0x02], 0),
            None
        );
    }

    #[test]
    fn the_production_entry_point_reconstructs_the_real_covenant_lock() {
        // End-to-end through `settle_signers_for_spend`: the digest is
        // computed over the RECONSTRUCTED Poc5 lock (head ‖ params ‖ tail),
        // so this cell fails if the rebuild ever drifts from what a signer
        // would have signed against.
        let keys = [key(1), key(2), key(3)];
        let p = params_with(&keys);
        let lock = crate::pot::covenant_lock_of(&p);
        assert!(
            crate::pot::is_pot_covenant_script(&lock),
            "rebuild is a covenant lock"
        );
        assert_eq!(
            crate::pot::extract_covenant_params(&lock).as_ref(),
            Some(&p),
            "roundtrip"
        );
        // Sign against the reconstructed lock (what the network's digest uses).
        let skeleton = spender_raw(&[]);
        let sig_of = |sk: &PrivateKey| {
            let parsed = parse_transaction(&skeleton).unwrap();
            let digest = compute_sighash_for_signing(&SighashParams {
                version: parsed.version,
                inputs: &parsed.inputs,
                outputs: &parsed.outputs,
                locktime: parsed.locktime,
                input_index: 0,
                subscript: &lock,
                satoshis: 1000,
                scope: SIGHASH_ALL | SIGHASH_FORKID,
            });
            let mut push = sk.sign(&digest).unwrap().to_der();
            push.push((SIGHASH_ALL | SIGHASH_FORKID) as u8);
            push
        };
        let (s1, s2) = (sig_of(&keys[1]), sig_of(&keys[2]));
        let mut unlock = vec![0x00];
        unlock.push(s1.len() as u8);
        unlock.extend_from_slice(&s1);
        unlock.push(s2.len() as u8);
        unlock.extend_from_slice(&s2);
        let raw = spender_raw(&unlock);
        // stake_a + stake_b = 1000 = pot_sats → classifies; the tower pair
        // with seat B names the enforced family.
        assert_eq!(
            settle_signers_for_spend(&p, 1000, &raw, 0),
            Some(SettleSigners::TowerB)
        );
        // Conservation refusal: a lying pot value never classifies.
        assert_eq!(settle_signers_for_spend(&p, 999, &raw, 0), None);
    }

    #[test]
    fn wire_strings_round_trip_and_unknowns_degrade() {
        for v in [
            SettleSigners::Coop,
            SettleSigners::TowerA,
            SettleSigners::TowerB,
        ] {
            assert_eq!(SettleSigners::from_wire(v.as_str()), Some(v));
        }
        assert_eq!(SettleSigners::from_wire("towerb"), None);
        assert_eq!(SettleSigners::from_wire(""), None);
        assert!(SettleSigners::TowerA.tower_signed());
        assert!(SettleSigners::TowerB.tower_signed());
        assert!(!SettleSigners::Coop.tower_signed());
    }
}
