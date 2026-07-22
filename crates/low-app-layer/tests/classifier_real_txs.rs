//! The settle classifier (bsv-low #227) against REAL MAINNET transactions.
//!
//! Fixtures (fetched from WhatsOnChain, byte-verified — each raw hashes to
//! its filename txid):
//! - `91309122…` — the TOWER-ENFORCED covenant settle (decision log
//!   2026-07-05: `J.declaredWinner=A`, the #103 destination-binding
//!   proof). Expected: `winner-a`.
//! - `17688504…` — the ACCEPTANCE-HAND cooperative covenant settle (real
//!   MetaNet Client as seat A, seat A won). Expected: `winner-a`.
//! - `3ca368b0…` — the 2026-07-21 refundLanded pre-signed refund
//!   (recovery_height 958846, un-raked). Expected: `refund`.
//! - `2ba8b852…` — the Tier-B B1 rematch hand-1 coop settle
//!   (WoC-confirmed outputs [20-sat rake, 1580-sat winner], ante 1000,
//!   seat B won). Expected: `winner-b`.
//!
//! Each spend's pot FUNDING tx (`c571d433…` / `f513aaba…` / `5533ca32…` /
//! `336ca104…`) is included so the classifier reads the committed params
//! from the REAL funded lock (the producer path — never hand-fed params).
//!
//! No mainnet TIE settle txid exists in the decision log (searched
//! 2026-07-22), so the tie template is pinned two ways: (a) a synthetic
//! spender paying the EXACT T_tie derived from a REAL pot's committed
//! params, and (b) the template-collision case from the covenant's own test
//! matrix (rakeless equal-stakes ⇒ T_tie == T_refund byte-identical).

use low_app_layer::results::{
    beef_block_height, classify_pot_spend, decide_spent_any, extract_covenant_params,
    is_bare_2of3_lock, parse_bitails_unspent, parse_raw_tx_verified, parse_woc_spent_body,
    spender_raw_verifies, PotSpendFacts, PotVerdict, SpentObservation, UnspentCorroboration,
};

// ── real mainnet fixtures ───────────────────────────────────────────────────

const ENFORCED_SETTLE_TXID: &str =
    "91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6";
const ENFORCED_SETTLE_HEX: &str = include_str!(
    "fixtures/91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6.hex"
);
const ENFORCED_FUNDING_TXID: &str =
    "c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563";
const ENFORCED_FUNDING_HEX: &str = include_str!(
    "fixtures/c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563.hex"
);

const COOP_SETTLE_TXID: &str = "176885049985a858244993ab5591bfeb61b9368ac12fc7f12540d0824e5f891e";
const COOP_SETTLE_HEX: &str = include_str!(
    "fixtures/176885049985a858244993ab5591bfeb61b9368ac12fc7f12540d0824e5f891e.hex"
);
const COOP_FUNDING_TXID: &str = "f513aaba305633f82d0a61bbff18b45deed20cb9e38bb52230fb32736b3ad410";
const COOP_FUNDING_HEX: &str = include_str!(
    "fixtures/f513aaba305633f82d0a61bbff18b45deed20cb9e38bb52230fb32736b3ad410.hex"
);

const REFUND_TXID: &str = "3ca368b0ca4dcb31ba87977d7aaf3a4671eafa2c980864c880f96080c68cee36";
const REFUND_HEX: &str = include_str!(
    "fixtures/3ca368b0ca4dcb31ba87977d7aaf3a4671eafa2c980864c880f96080c68cee36.hex"
);
const REFUND_FUNDING_TXID: &str =
    "5533ca32a296c58778a240cd7649392bf2e6b11ef63e1c71765913ebba093c59";
const REFUND_FUNDING_HEX: &str = include_str!(
    "fixtures/5533ca32a296c58778a240cd7649392bf2e6b11ef63e1c71765913ebba093c59.hex"
);

const B_WINS_SETTLE_TXID: &str =
    "2ba8b8520081a969406de03486ae27411d8f819d4f7c914593880413e0274cde";
const B_WINS_SETTLE_HEX: &str = include_str!(
    "fixtures/2ba8b8520081a969406de03486ae27411d8f819d4f7c914593880413e0274cde.hex"
);
const B_WINS_FUNDING_TXID: &str =
    "336ca104240721c1c62a22786fb117381b02040b149d45cf6a1fbaf7fe033783";
const B_WINS_FUNDING_HEX: &str = include_str!(
    "fixtures/336ca104240721c1c62a22786fb117381b02040b149d45cf6a1fbaf7fe033783.hex"
);

fn raw(hexstr: &str) -> Vec<u8> {
    hex::decode(hexstr.trim()).expect("fixture hex decodes")
}

fn facts<'a>(
    pot_txid: &'a str,
    funding: &'a [u8],
    spender_txid: &'a str,
    spender: &'a [u8],
) -> PotSpendFacts<'a> {
    PotSpendFacts {
        pot_txid,
        pot_vout: 0,
        funding_raw: funding,
        spender_txid,
        spender_raw: spender,
        marker_recovery_height: None,
    }
}

// ── the four real spends classify to their known ground truth ───────────────

#[test]
fn enforced_covenant_settle_91309122_classifies_winner_a() {
    let funding = raw(ENFORCED_FUNDING_HEX);
    let spender = raw(ENFORCED_SETTLE_HEX);
    let v = classify_pot_spend(&facts(
        ENFORCED_FUNDING_TXID,
        &funding,
        ENFORCED_SETTLE_TXID,
        &spender,
    ));
    assert_eq!(v, Some(PotVerdict::WinnerA), "J.declaredWinner=A (decision log)");
}

#[test]
fn acceptance_hand_coop_settle_17688504_classifies_winner_a() {
    let funding = raw(COOP_FUNDING_HEX);
    let spender = raw(COOP_SETTLE_HEX);
    let v = classify_pot_spend(&facts(
        COOP_FUNDING_TXID,
        &funding,
        COOP_SETTLE_TXID,
        &spender,
    ));
    assert_eq!(v, Some(PotVerdict::WinnerA), "seat A (MetaNet) won the acceptance hand");
}

#[test]
fn refund_landed_3ca368b0_classifies_refund() {
    let funding = raw(REFUND_FUNDING_HEX);
    let spender = raw(REFUND_HEX);
    // The committed recovery height is read from the REAL lock (958846 per
    // the decision log) — assert it first via the extraction path.
    let ftx = parse_raw_tx_verified(&funding, REFUND_FUNDING_TXID).expect("funding parses");
    let p = extract_covenant_params(&ftx.outputs[0].1).expect("covenant params extract");
    assert_eq!(p.recovery_height, 958_846);
    let v = classify_pot_spend(&facts(
        REFUND_FUNDING_TXID,
        &funding,
        REFUND_TXID,
        &spender,
    ));
    assert_eq!(v, Some(PotVerdict::Refund), "the 2026-07-21 refundLanded refund");
}

#[test]
fn rematch_coop_settle_2ba8b852_classifies_winner_b() {
    let funding = raw(B_WINS_FUNDING_HEX);
    let spender = raw(B_WINS_SETTLE_HEX);
    let v = classify_pot_spend(&facts(
        B_WINS_FUNDING_TXID,
        &funding,
        B_WINS_SETTLE_TXID,
        &spender,
    ));
    assert_eq!(
        v,
        Some(PotVerdict::WinnerB),
        "Tier-B B1 hand-1: [20 rake, 1580 winner] paid seat B's committed home"
    );
}

// ── committed-param extraction against the real funded lock ─────────────────

#[test]
fn real_covenant_lock_params_extract_exactly() {
    let ftx = parse_raw_tx_verified(&raw(ENFORCED_FUNDING_HEX), ENFORCED_FUNDING_TXID).unwrap();
    let (pot_sats, lock) = &ftx.outputs[0];
    let p = extract_covenant_params(lock).expect("the live pot lock is the Poc5 covenant");
    // ante 2000 per seat, pot 4000, committed fee 400 (the ARC-floor bind),
    // recovery height 956656 — the enforced-settle pot's known terms.
    assert_eq!((p.stake_a, p.stake_b), (2000, 2000));
    assert_eq!(p.stake_a + p.stake_b, *pot_sats);
    assert_eq!(p.fee_sats, 400);
    assert_eq!(p.recovery_height, 956_656);
    assert_ne!(p.pay_pkh_a, p.pay_pkh_b, "distinct committed pay homes");
    // The settle paid floor(pot/100)=40 rake + net 3560 to A's home: assert
    // the classifier's derivation against the REAL spend bytes.
    let stx = parse_raw_tx_verified(&raw(ENFORCED_SETTLE_HEX), ENFORCED_SETTLE_TXID).unwrap();
    assert_eq!(stx.outputs[0].0, 40, "rake output = floor(4000/100)");
    assert_eq!(stx.outputs[1].0, 3560, "winner output = pot − fee − rake");
}

// ── synthetic tie: the EXACT T_tie derived from a REAL pot's params ─────────

/// Serialize a minimal spender tx: one input spending `pot_txid:0` with the
/// given sequence, the given outputs, the given locktime.
fn build_spender(
    pot_txid: &str,
    sequence: u32,
    outputs: &[(u64, Vec<u8>)],
    lock_time: u32,
) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_le_bytes()); // version
    b.push(1); // one input
    let mut prev = hex::decode(pot_txid).unwrap();
    prev.reverse();
    b.extend_from_slice(&prev);
    b.extend_from_slice(&0u32.to_le_bytes()); // vout 0
    b.push(0); // empty scriptSig (classification never validates sigs)
    b.extend_from_slice(&sequence.to_le_bytes());
    b.push(u8::try_from(outputs.len()).unwrap());
    for (sats, script) in outputs {
        b.extend_from_slice(&sats.to_le_bytes());
        b.push(u8::try_from(script.len()).unwrap());
        b.extend_from_slice(script);
    }
    b.extend_from_slice(&lock_time.to_le_bytes());
    b
}

fn txid_of(rawtx: &[u8]) -> String {
    bsv_rs::transaction::Transaction::from_binary(rawtx).unwrap().id()
}

fn p2pkh(pkh: &[u8; 20]) -> Vec<u8> {
    let mut s = vec![0x76, 0xa9, 0x14];
    s.extend_from_slice(pkh);
    s.extend_from_slice(&[0x88, 0xac]);
    s
}

#[test]
fn synthetic_tie_paying_the_real_pots_t_tie_classifies_tie() {
    let funding = raw(ENFORCED_FUNDING_HEX);
    let ftx = parse_raw_tx_verified(&funding, ENFORCED_FUNDING_TXID).unwrap();
    let p = extract_covenant_params(&ftx.outputs[0].1).unwrap();
    // T_tie for pot 4000 / fee 400 / rake 40: net 3600, halves stay even →
    // tie_rake 40, half 1780 each.
    let tie_outputs = vec![
        (40u64, p2pkh(&p.rake_pkh)),
        (1780u64, p2pkh(&p.pay_pkh_a)),
        (1780u64, p2pkh(&p.pay_pkh_b)),
    ];
    let spender = build_spender(ENFORCED_FUNDING_TXID, 0xffff_ffff, &tie_outputs, 0);
    let spender_txid = txid_of(&spender);
    let v = classify_pot_spend(&PotSpendFacts {
        pot_txid: ENFORCED_FUNDING_TXID,
        pot_vout: 0,
        funding_raw: &funding,
        spender_txid: &spender_txid,
        spender_raw: &spender,
        marker_recovery_height: None,
    });
    assert_eq!(v, Some(PotVerdict::Tie));
}

// ── conservatism: never a guessed verdict ───────────────────────────────────

#[test]
fn redirect_or_unknown_shapes_are_unresolved() {
    let funding = raw(ENFORCED_FUNDING_HEX);
    let ftx = parse_raw_tx_verified(&funding, ENFORCED_FUNDING_TXID).unwrap();
    let p = extract_covenant_params(&ftx.outputs[0].1).unwrap();

    // A redirect: right values, WRONG destination (attacker pkh).
    let attacker = [0xEEu8; 20];
    let redirect = vec![(40u64, p2pkh(&p.rake_pkh)), (3560u64, p2pkh(&attacker))];
    let spender = build_spender(ENFORCED_FUNDING_TXID, 0xffff_ffff, &redirect, 0);
    let id = txid_of(&spender);
    assert_eq!(
        classify_pot_spend(&PotSpendFacts {
            pot_txid: ENFORCED_FUNDING_TXID,
            pot_vout: 0,
            funding_raw: &funding,
            spender_txid: &id,
            spender_raw: &spender,
            marker_recovery_height: None,
        }),
        None,
        "a non-template shape must never classify"
    );

    // Rake short by one sat (winner shape, wrong split).
    let short = vec![(39u64, p2pkh(&p.rake_pkh)), (3561u64, p2pkh(&p.pay_pkh_a))];
    let spender = build_spender(ENFORCED_FUNDING_TXID, 0xffff_ffff, &short, 0);
    let id = txid_of(&spender);
    assert_eq!(
        classify_pot_spend(&PotSpendFacts {
            pot_txid: ENFORCED_FUNDING_TXID,
            pot_vout: 0,
            funding_raw: &funding,
            spender_txid: &id,
            spender_raw: &spender,
            marker_recovery_height: None,
        }),
        None
    );
}

#[test]
fn refund_shape_without_the_height_gate_is_unresolved() {
    // The refund OUTPUT SET with a zero locktime / final sequence: the
    // covenant would reject it on-chain; classification must refuse too
    // (conservative — no observed gate, no refund verdict).
    let funding = raw(REFUND_FUNDING_HEX);
    let real_refund = parse_raw_tx_verified(&raw(REFUND_HEX), REFUND_TXID).unwrap();
    let spender = build_spender(REFUND_FUNDING_TXID, 0xffff_ffff, &real_refund.outputs, 0);
    let id = txid_of(&spender);
    assert_eq!(
        classify_pot_spend(&PotSpendFacts {
            pot_txid: REFUND_FUNDING_TXID,
            pot_vout: 0,
            funding_raw: &funding,
            spender_txid: &id,
            spender_raw: &spender,
            marker_recovery_height: None,
        }),
        None
    );
}

#[test]
fn wrong_or_garbled_bytes_are_unresolved_never_wrong() {
    let funding = raw(ENFORCED_FUNDING_HEX);
    let spender = raw(ENFORCED_SETTLE_HEX);
    // Spender bytes swapped for a DIFFERENT real tx (hash mismatch).
    assert_eq!(
        classify_pot_spend(&facts(
            ENFORCED_FUNDING_TXID,
            &funding,
            ENFORCED_SETTLE_TXID,
            &raw(COOP_SETTLE_HEX),
        )),
        None
    );
    // Funding bytes garbled (hash mismatch).
    let mut garbled = funding.clone();
    garbled[100] ^= 0xff;
    assert_eq!(
        classify_pot_spend(&facts(
            ENFORCED_FUNDING_TXID,
            &garbled,
            ENFORCED_SETTLE_TXID,
            &spender,
        )),
        None
    );
    // The spender does not spend the claimed pot (funding/spender pair from
    // different games).
    assert_eq!(
        classify_pot_spend(&facts(
            COOP_FUNDING_TXID,
            &raw(COOP_FUNDING_HEX),
            ENFORCED_SETTLE_TXID,
            &spender,
        )),
        None
    );
}

// ── the T_tie == T_refund collision (covenant test-matrix case) ─────────────

/// Fill the frozen Poc5 template with synthetic params — the same fill
/// `build_template_bound_lock` performs (push_data for keys/hashes,
/// push_minimal_int for numbers), so the recognizer + extractor see a
/// byte-faithful covenant lock.
#[allow(clippy::too_many_arguments)] // mirrors the 10-param constructor ABI
fn fill_template(
    pub_a: &[u8; 33],
    pub_b: &[u8; 33],
    pub_tower: &[u8; 33],
    pay_pkh_a: &[u8; 20],
    pay_pkh_b: &[u8; 20],
    rake_pkh: &[u8; 20],
    stake_a: u64,
    stake_b: u64,
    fee_sats: u64,
    recovery_height: u64,
) -> Vec<u8> {
    fn push_data(d: &[u8]) -> Vec<u8> {
        let mut v = vec![u8::try_from(d.len()).unwrap()];
        v.extend_from_slice(d);
        v
    }
    fn push_minimal_int(n: u64) -> Vec<u8> {
        if n == 0 {
            return vec![0x00];
        }
        if n <= 16 {
            return vec![0x50 + u8::try_from(n).unwrap()];
        }
        let mut bytes = Vec::new();
        let mut v = n;
        while v > 0 {
            bytes.push((v & 0xff) as u8);
            v >>= 8;
        }
        if bytes.last().is_some_and(|b| b & 0x80 != 0) {
            bytes.push(0x00);
        }
        let mut out = vec![u8::try_from(bytes.len()).unwrap()];
        out.extend_from_slice(&bytes);
        out
    }
    let fills = [
        push_data(pub_a),
        push_data(pub_b),
        push_data(pub_tower),
        push_data(pay_pkh_a),
        push_data(pay_pkh_b),
        push_data(rake_pkh),
        push_minimal_int(stake_a),
        push_minimal_int(stake_b),
        push_minimal_int(fee_sats),
        push_minimal_int(recovery_height),
    ];
    let markers = [
        "<pubA>", "<pubB>", "<pubTower>", "<payPkhA>", "<payPkhB>", "<rakePkh>", "<stakeA>",
        "<stakeB>", "<feeSats>", "<recoveryHeight>",
    ];
    let mut filled = overlay_discovery::pot::POC5_TEMPLATE_HEX.trim().to_string();
    for (marker, fill) in markers.iter().zip(fills.iter()) {
        filled = filled.replace(marker, &hex::encode(fill));
    }
    assert!(!filled.contains('<'), "all placeholders filled");
    hex::decode(filled).unwrap()
}

/// A minimal funding tx with the given output-0 lock and value.
fn build_funding(lock: &[u8], sats: u64) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(1); // one dummy input
    b.extend_from_slice(&[0x11u8; 32]);
    b.extend_from_slice(&0u32.to_le_bytes());
    b.push(0);
    b.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
    b.push(1); // one output — the pot
    b.extend_from_slice(&sats.to_le_bytes());
    // fd-length (the covenant lock is ~3150 bytes)
    b.push(0xfd);
    b.extend_from_slice(&u16::try_from(lock.len()).unwrap().to_le_bytes());
    b.extend_from_slice(lock);
    b.extend_from_slice(&0u32.to_le_bytes());
    b
}

#[test]
fn rakeless_equal_stakes_collision_resolves_by_the_wire_locktime() {
    // pot 80 < 100 ⇒ rake 0; equal stakes + even fee ⇒ T_tie == T_refund
    // byte-identically (the covenant's own pinned collision case,
    // unreachable at prod stakes). Money-identical either way — the wire
    // locktime/sequence picks the label.
    let pay_a = [0xAAu8; 20];
    let pay_b = [0xBBu8; 20];
    let lock = fill_template(
        &[0x02u8; 33],
        &[0x03u8; 33],
        &[0x04u8; 33],
        &pay_a,
        &pay_b,
        &[0xCCu8; 20],
        40,
        40,
        10,
        900_000,
    );
    let p = extract_covenant_params(&lock).expect("synthetic covenant lock extracts");
    assert_eq!((p.stake_a, p.stake_b, p.fee_sats), (40, 40, 10));
    assert_eq!(p.recovery_height, 900_000);

    let funding = build_funding(&lock, 80);
    let funding_txid = txid_of(&funding);
    // The identical output set: [35 → A, 35 → B].
    let outs = vec![(35u64, p2pkh(&pay_a)), (35u64, p2pkh(&pay_b))];

    // Final sequence + zero locktime ⇒ a coop settle shape ⇒ tie.
    let settle = build_spender(&funding_txid, 0xffff_ffff, &outs, 0);
    let settle_id = txid_of(&settle);
    assert_eq!(
        classify_pot_spend(&PotSpendFacts {
            pot_txid: &funding_txid,
            pot_vout: 0,
            funding_raw: &funding,
            spender_txid: &settle_id,
            spender_raw: &settle,
            marker_recovery_height: None,
        }),
        Some(PotVerdict::Tie)
    );

    // Non-final sequence + locktime at the committed height ⇒ refund.
    let refund = build_spender(&funding_txid, 0xffff_fffe, &outs, 900_000);
    let refund_id = txid_of(&refund);
    assert_eq!(
        classify_pot_spend(&PotSpendFacts {
            pot_txid: &funding_txid,
            pot_vout: 0,
            funding_raw: &funding,
            spender_txid: &refund_id,
            spender_raw: &refund,
            marker_recovery_height: None,
        }),
        Some(PotVerdict::Refund)
    );
}

#[test]
fn degenerate_same_pay_home_never_names_a_winner() {
    let pay = [0xAAu8; 20];
    let lock = fill_template(
        &[0x02u8; 33],
        &[0x03u8; 33],
        &[0x04u8; 33],
        &pay,
        &pay, // payPkhA == payPkhB
        &[0xCCu8; 20],
        2000,
        2000,
        400,
        900_000,
    );
    let funding = build_funding(&lock, 4000);
    let funding_txid = txid_of(&funding);
    let outs = vec![(40u64, p2pkh(&[0xCCu8; 20])), (3560u64, p2pkh(&pay))];
    let spender = build_spender(&funding_txid, 0xffff_ffff, &outs, 0);
    let id = txid_of(&spender);
    assert_eq!(
        classify_pot_spend(&PotSpendFacts {
            pot_txid: &funding_txid,
            pot_vout: 0,
            funding_raw: &funding,
            spender_txid: &id,
            spender_raw: &spender,
            marker_recovery_height: None,
        }),
        None,
        "winner-A vs winner-B is indistinguishable — no verdict"
    );
}

// ── bare (pre-covenant) pots: refund-only classification ────────────────────

fn bare_lock() -> Vec<u8> {
    let mut s = vec![0x52];
    for seed in [0x02u8, 0x03, 0x04] {
        s.push(33);
        s.extend_from_slice(&[seed; 33]);
    }
    s.push(0x53);
    s.push(0xae);
    s
}

#[test]
fn bare_pot_refund_classifies_only_with_the_exact_marker_height_gate() {
    let lock = bare_lock();
    assert!(is_bare_2of3_lock(&lock));
    let funding = build_funding(&lock, 4000);
    let funding_txid = txid_of(&funding);
    let outs = vec![(1800u64, p2pkh(&[0xAAu8; 20])), (1800u64, p2pkh(&[0xBBu8; 20]))];

    let refund = build_spender(&funding_txid, 0xffff_fffe, &outs, 900_000);
    let refund_id = txid_of(&refund);
    let classify = |marker_h: Option<u32>, rawtx: &[u8], id: &str| {
        classify_pot_spend(&PotSpendFacts {
            pot_txid: &funding_txid,
            pot_vout: 0,
            funding_raw: &funding,
            spender_txid: id,
            spender_raw: rawtx,
            marker_recovery_height: marker_h,
        })
    };
    // Marker height matches the wire locktime → refund.
    assert_eq!(classify(Some(900_000), &refund, &refund_id), Some(PotVerdict::Refund));
    // No marker / wrong marker height → unresolved (never guessed).
    assert_eq!(classify(None, &refund, &refund_id), None);
    assert_eq!(classify(Some(899_999), &refund, &refund_id), None);
    // Final sequence (nLockTime disabled) → unresolved.
    let finalized = build_spender(&funding_txid, 0xffff_ffff, &outs, 900_000);
    let fid = txid_of(&finalized);
    assert_eq!(classify(Some(900_000), &finalized, &fid), None);
    // A bare-pot WINNER-like shape (2 outs incl. a rake-looking one, locktime
    // 0) never classifies — legacy claims cover those games.
    let winnerish = build_spender(
        &funding_txid,
        0xffff_ffff,
        &[(40u64, p2pkh(&[0xCCu8; 20])), (3560u64, p2pkh(&[0xAAu8; 20]))],
        0,
    );
    let wid = txid_of(&winnerish);
    assert_eq!(classify(Some(900_000), &winnerish, &wid), None);
}

// ── /spent-any building blocks with the real fixtures ───────────────────────

#[test]
fn spender_raw_verification_uses_real_bytes() {
    let refund_raw = raw(REFUND_HEX);
    // The real refund spends the real pot outpoint.
    assert!(spender_raw_verifies(
        &refund_raw,
        REFUND_TXID,
        REFUND_FUNDING_TXID,
        0
    ));
    // Wrong vout / wrong outpoint / wrong claimed txid all refuse.
    assert!(!spender_raw_verifies(&refund_raw, REFUND_TXID, REFUND_FUNDING_TXID, 1));
    assert!(!spender_raw_verifies(&refund_raw, REFUND_TXID, ENFORCED_FUNDING_TXID, 0));
    assert!(!spender_raw_verifies(&refund_raw, ENFORCED_SETTLE_TXID, REFUND_FUNDING_TXID, 0));
}

#[test]
fn spent_any_decision_table_is_fail_safe() {
    let spent = SpentObservation::Spent {
        txid: REFUND_TXID.to_string(),
        confirmed: true,
    };
    // Positive + verified raw → known spent with the spender.
    let st = decide_spent_any(&spent, true, UnspentCorroboration::Unknown);
    assert!(st.known);
    assert_eq!(st.spent, Some(true));
    assert_eq!(st.spending_txid.as_deref(), Some(REFUND_TXID));
    assert_eq!(st.spent_confirmed, Some(true));
    // Positive with an UNVERIFIABLE raw → honest unknown (never a bare claim).
    let st = decide_spent_any(&spent, false, UnspentCorroboration::Unknown);
    assert!(!st.known);
    assert_eq!(st.spent, None);
    // Negative WITHOUT corroboration → honest unknown (never WoC-only).
    let st = decide_spent_any(&SpentObservation::NotSpent, false, UnspentCorroboration::Unknown);
    assert!(!st.known);
    // Negative WITH clean corroboration → known unspent.
    let st = decide_spent_any(
        &SpentObservation::NotSpent,
        false,
        UnspentCorroboration::ConfirmedUnspent,
    );
    assert!(st.known);
    assert_eq!(st.spent, Some(false));
    // Fault → unknown.
    let st = decide_spent_any(&SpentObservation::Fault, true, UnspentCorroboration::ConfirmedUnspent);
    assert!(!st.known);
}

#[test]
fn provider_body_parsers_are_strict() {
    // The real WoC 200 body observed for the enforced-settle pot outpoint.
    let woc = serde_json::json!({
        "txid": ENFORCED_SETTLE_TXID,
        "vin": 0,
        "status": "confirmed"
    });
    assert_eq!(
        parse_woc_spent_body(&woc),
        SpentObservation::Spent {
            txid: ENFORCED_SETTLE_TXID.to_string(),
            confirmed: true
        }
    );
    // Unconfirmed / missing status → spent but not confirmed.
    let woc = serde_json::json!({ "txid": ENFORCED_SETTLE_TXID });
    assert!(matches!(
        parse_woc_spent_body(&woc),
        SpentObservation::Spent { confirmed: false, .. }
    ));
    // Malformed txid → Fault.
    assert_eq!(
        parse_woc_spent_body(&serde_json::json!({ "txid": "nope" })),
        SpentObservation::Fault
    );
    assert_eq!(parse_woc_spent_body(&serde_json::json!({})), SpentObservation::Fault);

    // Bitails: ONLY an explicit spent:false at 200 corroborates unspent.
    let unspent = serde_json::json!({ "spent": false });
    assert_eq!(
        parse_bitails_unspent(200, Some(&unspent)),
        UnspentCorroboration::ConfirmedUnspent
    );
    // Their live 500 fault (observed 2026-07-22), contradictions, and
    // unknown shapes are all Unknown.
    let fault = serde_json::json!({ "statusCode": 500, "message": "Unhandled Error." });
    assert_eq!(parse_bitails_unspent(500, Some(&fault)), UnspentCorroboration::Unknown);
    assert_eq!(
        parse_bitails_unspent(200, Some(&serde_json::json!({ "spent": true }))),
        UnspentCorroboration::Unknown
    );
    assert_eq!(parse_bitails_unspent(200, None), UnspentCorroboration::Unknown);
    assert_eq!(parse_bitails_unspent(404, None), UnspentCorroboration::Unknown);
}

// ── the settle's mined height rides its BEEF BUMP ───────────────────────────

#[test]
fn beef_block_height_reads_the_bump_or_honestly_none() {
    use bsv_rs::transaction::{Beef, MerklePath, MerklePathLeaf, Transaction};
    // Unproven BEEF (raw only) → None.
    let refund_tx = Transaction::from_binary(&raw(REFUND_HEX)).unwrap();
    let mut beef = Beef::new();
    beef.merge_transaction(refund_tx.clone());
    assert_eq!(beef_block_height(&beef.to_binary(), REFUND_TXID), None);
    // Proven (a stitched BUMP) → the block height.
    let mut proven = refund_tx;
    let leaf = MerklePathLeaf::new_txid(0, REFUND_TXID.to_string());
    proven.merkle_path = Some(MerklePath::new_unchecked(959_000, vec![vec![leaf]]).unwrap());
    let bytes = proven.to_beef(true).unwrap();
    assert_eq!(beef_block_height(&bytes, REFUND_TXID), Some(959_000));
}
