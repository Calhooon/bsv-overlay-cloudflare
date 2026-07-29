//! `Poc5TemplatePot` covenant PARAM DECODER + spend-template CLASSIFIER
//! (bsv-low #284 — moved here from `low-app-layer/src/results.rs` so the
//! overlay can decode covenant params ONCE at admission and classify a spend
//! verdict ONCE at spend-detection, instead of the app-layer re-parsing the
//! stored BLOBs on every read).
//!
//! # Doctrine boundary — what this module is and is NOT
//!
//! Everything here is a PURE FUNCTION of hash-bound chain bytes: the
//! committed params are read out of the funding lock itself (the exact bytes
//! the pot's money sat under), and the verdict is an exact output-set match
//! of a spend against the four templates those params mandate in-script.
//! Nothing here verifies a signature, reads a marker, or chooses between
//! competing claims — that stays the app-layer's job (the overlay is raw
//! admitted truth, byte-format-only). In particular the BARE-pot refund
//! rule (`classify_bare_refund`) did NOT move: it depends on an unverified
//! `ls_potparty` marker hint, so it remains app-layer-only.
//!
//! # Template math provenance
//!
//! The template derivation is byte-identical to the authoritative pair
//! `pot.ts::settleOutputs` ≡ `cosign.rs::mandated_outputs` and
//! `settle.rs::refund_outputs` (bsv-low `crates/low-spend/src/tower_settle.rs`
//! `template_settle_outputs` / `template_refund_outputs`). The classification
//! conservatism contract (exact one-template match, observed refund height
//! gate, degenerate-pot refusal) is documented on [`classify_covenant`] and
//! pinned by the golden-vector tests in
//! `low-app-layer/tests/classifier_real_txs.rs` (which exercise these very
//! functions through the app-layer's re-export).

use super::pot_covenant_param_region;

// ── minimal raw-tx model ────────────────────────────────────────────────────

/// One parsed input of a raw tx: the outpoint it spends + its sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawInput {
    /// Previous txid, lowercase hex (display order).
    pub prev_txid: String,
    pub prev_vout: u32,
    pub sequence: u32,
}

/// A parsed raw tx — just the fields classification needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTx {
    pub inputs: Vec<RawInput>,
    /// `(satoshis, locking_script)` in output order.
    pub outputs: Vec<(u64, Vec<u8>)>,
    pub lock_time: u32,
}

impl RawTx {
    /// Project an already-parsed `bsv_rs` transaction into the minimal model.
    /// `None` when any input lacks a source txid or any output lacks a satoshi
    /// amount (impossible on a mined tx, but the type allows it) — the caller
    /// must degrade to "unresolved", never feed the classifier partial bytes.
    pub fn from_transaction(tx: &bsv_rs::transaction::Transaction) -> Option<RawTx> {
        let mut inputs = Vec::with_capacity(tx.inputs.len());
        for i in &tx.inputs {
            inputs.push(RawInput {
                prev_txid: i.source_txid.clone()?.to_ascii_lowercase(),
                prev_vout: i.source_output_index,
                sequence: i.sequence,
            });
        }
        let mut outputs = Vec::with_capacity(tx.outputs.len());
        for o in &tx.outputs {
            outputs.push((o.satoshis?, o.locking_script.to_binary()));
        }
        Some(RawTx {
            inputs,
            outputs,
            lock_time: tx.lock_time,
        })
    }
}

// ── covenant param extraction ───────────────────────────────────────────────

/// The 10 committed params of a `Poc5TemplatePot` lock, as funded on-chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CovenantParams {
    pub pub_a: [u8; 33],
    pub pub_b: [u8; 33],
    pub pub_tower: [u8; 33],
    pub pay_pkh_a: [u8; 20],
    pub pay_pkh_b: [u8; 20],
    pub rake_pkh: [u8; 20],
    pub stake_a: u64,
    pub stake_b: u64,
    pub fee_sats: u64,
    pub recovery_height: u64,
}

/// One push read out of the covenant param region: raw data or a minimal
/// script number (the builder emits `push_data` for keys/hashes and
/// `push_minimal_int` for amounts/height — `covenant.rs::push_minimal_int`).
enum ParamPush {
    Data(Vec<u8>),
    Num(u64),
}

/// Walk the param region as the builder wrote it: direct data pushes
/// (opcode 1..=75) plus the minimal-int encodings OP_0 and OP_1..=OP_16.
/// Anything else (OP_PUSHDATA*, non-push opcodes, truncation) is `None` —
/// the frozen template's fills never use them, so their presence means this
/// is not a well-formed param region. All offset math is checked (wasm32
/// usize=u32 lesson — see `potparty::read_pushes`).
fn read_param_pushes(bytes: &[u8]) -> Option<Vec<ParamPush>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let op = bytes[i];
        i = i.checked_add(1)?;
        match op {
            0x00 => out.push(ParamPush::Num(0)),
            0x51..=0x60 => out.push(ParamPush::Num(u64::from(op - 0x50))),
            1..=0x4b => {
                let end = i.checked_add(op as usize)?;
                if end > bytes.len() {
                    return None;
                }
                out.push(ParamPush::Data(bytes[i..end].to_vec()));
                i = end;
            }
            _ => return None,
        }
    }
    Some(out)
}

/// Decode a minimal, NON-NEGATIVE Bitcoin script number push (≤ 8 value
/// bytes; an optional 0x00 sign-guard byte allowed — `push_minimal_int`
/// appends one when the top bit is set). A sign bit (negative) refuses:
/// no committed param is ever negative.
fn script_num_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || bytes.len() > 9 {
        return None;
    }
    let last = *bytes.last()?;
    if last & 0x80 != 0 {
        return None; // negative — never a valid committed param
    }
    let mut v: u64 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        if i >= 8 {
            // 9th byte is only ever the 0x00 sign guard.
            if b != 0 {
                return None;
            }
            continue;
        }
        v |= u64::from(b) << (8 * i);
    }
    Some(v)
}

/// Extract the COMMITTED params from a covenant funding lock, or `None` when
/// `lock` is not a `Poc5TemplatePot` script / its param region is malformed.
/// The recognizer + region split come from this crate's frozen template (the
/// SAME bytes `tm_pot` admits with, drift-pinned in `pot/mod.rs`).
pub fn extract_covenant_params(lock: &[u8]) -> Option<CovenantParams> {
    let region = pot_covenant_param_region(lock)?;
    let pushes = read_param_pushes(region)?;
    if pushes.len() != 10 {
        return None;
    }
    fn data<const N: usize>(p: &ParamPush) -> Option<[u8; N]> {
        match p {
            ParamPush::Data(d) if d.len() == N => {
                let mut a = [0u8; N];
                a.copy_from_slice(d);
                Some(a)
            }
            _ => None,
        }
    }
    fn num(p: &ParamPush) -> Option<u64> {
        match p {
            ParamPush::Num(n) => Some(*n),
            ParamPush::Data(d) => script_num_u64(d),
        }
    }
    Some(CovenantParams {
        pub_a: data(&pushes[0])?,
        pub_b: data(&pushes[1])?,
        pub_tower: data(&pushes[2])?,
        pay_pkh_a: data(&pushes[3])?,
        pay_pkh_b: data(&pushes[4])?,
        rake_pkh: data(&pushes[5])?,
        stake_a: num(&pushes[6])?,
        stake_b: num(&pushes[7])?,
        fee_sats: num(&pushes[8])?,
        recovery_height: num(&pushes[9])?,
    })
}

/// Rebuild [`CovenantParams`] from stored HEX column values (the #284
/// decoded-column read path). STRICT: every field must be present, decodable
/// hex of the exact committed length (33-byte keys, 20-byte hashes) — a
/// malformed stored value yields `None` so the caller falls back to the BEEF
/// parse (never a trust-shortcut, never a panic).
#[allow(clippy::too_many_arguments)] // mirrors the 10-param committed ABI
pub fn covenant_params_from_hex(
    pub_a: &str,
    pub_b: &str,
    pub_tower: &str,
    pay_pkh_a: &str,
    pay_pkh_b: &str,
    rake_pkh: &str,
    stake_a: u64,
    stake_b: u64,
    fee_sats: u64,
    recovery_height: u64,
) -> Option<CovenantParams> {
    fn arr<const N: usize>(h: &str) -> Option<[u8; N]> {
        let d = hex::decode(h).ok()?;
        if d.len() != N {
            return None;
        }
        let mut a = [0u8; N];
        a.copy_from_slice(&d);
        Some(a)
    }
    Some(CovenantParams {
        pub_a: arr(pub_a)?,
        pub_b: arr(pub_b)?,
        pub_tower: arr(pub_tower)?,
        pay_pkh_a: arr(pay_pkh_a)?,
        pay_pkh_b: arr(pay_pkh_b)?,
        rake_pkh: arr(rake_pkh)?,
        stake_a,
        stake_b,
        fee_sats,
        recovery_height,
    })
}

/// Encode the 10 committed params as the client builder's param-region
/// pushes (`push_data` for keys/hashes, `push_minimal_int` for
/// amounts/height) — the exact inverse of [`extract_covenant_params`] over a
/// well-formed region. Exists so tests across crates can build byte-faithful
/// covenant locks (HEAD ‖ this ‖ TAIL) without hand-copying the push rules;
/// the round-trip is pinned below.
pub fn encode_covenant_param_pushes(p: &CovenantParams) -> Vec<u8> {
    fn push_data(out: &mut Vec<u8>, d: &[u8]) {
        debug_assert!(d.len() <= 0x4b);
        out.push(d.len() as u8);
        out.extend_from_slice(d);
    }
    fn push_minimal_int(out: &mut Vec<u8>, n: u64) {
        if n == 0 {
            out.push(0x00);
            return;
        }
        if n <= 16 {
            out.push(0x50 + n as u8);
            return;
        }
        let mut bytes = Vec::new();
        let mut v = n;
        while v > 0 {
            bytes.push((v & 0xff) as u8);
            v >>= 8;
        }
        if bytes.last().is_some_and(|b| b & 0x80 != 0) {
            bytes.push(0x00); // sign guard
        }
        out.push(bytes.len() as u8);
        out.extend_from_slice(&bytes);
    }
    let mut out = Vec::new();
    push_data(&mut out, &p.pub_a);
    push_data(&mut out, &p.pub_b);
    push_data(&mut out, &p.pub_tower);
    push_data(&mut out, &p.pay_pkh_a);
    push_data(&mut out, &p.pay_pkh_b);
    push_data(&mut out, &p.rake_pkh);
    push_minimal_int(&mut out, p.stake_a);
    push_minimal_int(&mut out, p.stake_b);
    push_minimal_int(&mut out, p.fee_sats);
    push_minimal_int(&mut out, p.recovery_height);
    out
}

/// The canonical 25-byte P2PKH locking script for a 20-byte hash160.
pub fn p2pkh_lock(pkh: &[u8; 20]) -> Vec<u8> {
    let mut s = Vec::with_capacity(25);
    s.extend_from_slice(&[0x76, 0xa9, 0x14]);
    s.extend_from_slice(pkh);
    s.extend_from_slice(&[0x88, 0xac]);
    s
}

/// True iff `s` is a bare 2-of-3 multisig lock (`build_2of3_lock` shape):
/// `OP_2 <33> <33> <33> OP_3 OP_CHECKMULTISIG` — the pre-covenant pot lock.
pub fn is_bare_2of3_lock(s: &[u8]) -> bool {
    s.len() == 105
        && s[0] == 0x52
        && s[1] == 33
        && s[35] == 33
        && s[69] == 33
        && s[103] == 0x53
        && s[104] == 0xae
}

// ── template derivation (mirrors low-spend tower_settle byte-for-byte) ──────

/// The rake divisor the covenant hardcodes: `rake = floor(pot / 100)` (1%).
/// MUST stay equal to `tower_settle.rs::TEMPLATE_RAKE_DIVISOR` /
/// `pot.ts::POT_RAKE_DIVISOR` / `case.rs::POT_RAKE_DIVISOR`.
pub const TEMPLATE_RAKE_DIVISOR: u64 = 100;

/// nLockTime values ≥ this are unix-time, < this are block heights.
pub const LOCKTIME_THRESHOLD: u32 = 500_000_000;

/// One derived output template: `(satoshis, locking_script)` in tx order.
type OutputSet = Vec<(u64, Vec<u8>)>;

/// Derive the winner-A / winner-B / tie output sets from the committed
/// params — `tower_settle.rs::template_settle_outputs` verbatim (fee from
/// pot, absolute rake `floor(pot/100)`, tie odd sat → rake, rake output
/// first, omitted when 0). `None` when the params could never build a lock
/// (fee ≥ pot / rake ≥ net — refused at funding time, so on-chain pots never
/// hit this; conservative anyway).
fn settle_templates(p: &CovenantParams) -> Option<[OutputSet; 3]> {
    let pot = p.stake_a.checked_add(p.stake_b)?;
    if p.fee_sats >= pot {
        return None;
    }
    let net = pot - p.fee_sats;
    let rake = pot / TEMPLATE_RAKE_DIVISOR;
    if rake >= net {
        return None;
    }
    let winner = |pkh: &[u8; 20]| -> OutputSet {
        let mut outs = Vec::with_capacity(2);
        if rake > 0 {
            outs.push((rake, p2pkh_lock(&p.rake_pkh)));
        }
        outs.push((net - rake, p2pkh_lock(pkh)));
        outs
    };
    // Tie: the odd sat joins the rake so the halves stay equal.
    let mut tie_rake = rake;
    if (net - tie_rake.min(net)) % 2 == 1 {
        tie_rake += 1;
    }
    if tie_rake >= net {
        return None;
    }
    let half = (net - tie_rake) / 2;
    let mut tie = Vec::with_capacity(3);
    if tie_rake > 0 {
        tie.push((tie_rake, p2pkh_lock(&p.rake_pkh)));
    }
    tie.push((half, p2pkh_lock(&p.pay_pkh_a)));
    tie.push((half, p2pkh_lock(&p.pay_pkh_b)));
    Some([winner(&p.pay_pkh_a), winner(&p.pay_pkh_b), tie])
}

/// Derive the refund output set — `settle.rs::refund_outputs` verbatim over
/// the committed pay homes: un-raked, seat A absorbs the odd fee sat
/// (`feeA = fee − fee/2`, `feeB = fee/2`). `None` if a fee share exceeds its
/// stake (an unbuildable lock).
fn refund_template(p: &CovenantParams) -> Option<OutputSet> {
    let fee_b = p.fee_sats / 2;
    let fee_a = p.fee_sats - fee_b;
    if fee_a > p.stake_a || fee_b > p.stake_b {
        return None;
    }
    Some(vec![
        (p.stake_a - fee_a, p2pkh_lock(&p.pay_pkh_a)),
        (p.stake_b - fee_b, p2pkh_lock(&p.pay_pkh_b)),
    ])
}

// ── classification ──────────────────────────────────────────────────────────

/// Which mandated exit a pot spend fired. Seat-lettered, NOT identity-mapped
/// (seat → identity is not derivable server-side — the app-layer's job).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PotVerdict {
    WinnerA,
    WinnerB,
    Tie,
    Refund,
}

impl PotVerdict {
    /// The wire string for JSON bodies — ALSO the exact value stored in the
    /// #284 `pot_records.verdict` column (`from_wire` is its inverse).
    pub fn as_str(self) -> &'static str {
        match self {
            PotVerdict::WinnerA => "winner-a",
            PotVerdict::WinnerB => "winner-b",
            PotVerdict::Tie => "tie",
            PotVerdict::Refund => "refund",
        }
    }

    /// Parse a stored/wire verdict string. Anything unrecognized is `None`
    /// (an unknown stored value degrades to unresolved, never a guess).
    pub fn from_wire(s: &str) -> Option<PotVerdict> {
        match s {
            "winner-a" => Some(PotVerdict::WinnerA),
            "winner-b" => Some(PotVerdict::WinnerB),
            "tie" => Some(PotVerdict::Tie),
            "refund" => Some(PotVerdict::Refund),
            _ => None,
        }
    }
}

/// Covenant classification: exact output-set match against the four derived
/// templates, refund height-gate observed, collisions resolved only when
/// money-identical. `pot_sequence` is the sequence of the SPENDING tx's input
/// that spends the pot outpoint (the caller locates it — this function does
/// not re-check the input match).
///
/// A verdict is emitted ONLY when the classification is unambiguous:
/// - the outputs must equal EXACTLY ONE template (value + script, in order);
/// - the refund template additionally requires its in-script height gate
///   observed on the wire (`nLockTime ≥` the committed `recoveryHeight`,
///   block-height semantics, non-final sequence) — except the known
///   T_tie == T_refund byte-collision (rakeless equal-stakes pot,
///   unreachable at prod stakes), where the output sets are identical and
///   the wire locktime picks the money-identical label;
/// - a degenerate pot committing `payPkhA == payPkhB` can never distinguish
///   winner-A from winner-B → no verdict.
///
/// NOTE for callers: the covenant asserts `ctx.utxo.value == stakeA + stakeB`
/// in-script, so a caller holding the funded output value MUST refuse to
/// classify when it disagrees with the committed stakes (the app-layer's
/// `classify_pot_spend` and the overlay's spend hook both enforce this
/// before calling here).
pub fn classify_covenant(
    p: &CovenantParams,
    spender: &RawTx,
    pot_sequence: u32,
) -> Option<PotVerdict> {
    // A degenerate pot committing the same pay home for both seats can never
    // distinguish winner-A from winner-B — no verdict.
    if p.pay_pkh_a == p.pay_pkh_b {
        return None;
    }
    let [t_a, t_b, t_tie] = settle_templates(p)?;
    let t_refund = refund_template(p)?;

    let matches_a = spender.outputs == t_a;
    let matches_b = spender.outputs == t_b;
    let matches_tie = spender.outputs == t_tie;
    let matches_refund = spender.outputs == t_refund;

    // The refund's in-script height gate, observed on the wire: nLockTime is
    // a BLOCK HEIGHT ≥ the committed recoveryHeight and the pot input's
    // sequence is non-final (a final sequence disables nLockTime entirely).
    let refund_gate_ok = u64::from(spender.lock_time) >= p.recovery_height
        && spender.lock_time < LOCKTIME_THRESHOLD
        && spender.lock_time > 0
        && pot_sequence != 0xffff_ffff;

    match (matches_a, matches_b, matches_tie, matches_refund) {
        (true, false, false, false) => Some(PotVerdict::WinnerA),
        (false, true, false, false) => Some(PotVerdict::WinnerB),
        (false, false, true, false) => Some(PotVerdict::Tie),
        (false, false, false, true) => {
            // Pure refund shape: the covenant enforces the gate in-script, so
            // an on-chain spend always satisfies it — but classification is
            // conservative: no observed gate, no refund verdict.
            if refund_gate_ok {
                Some(PotVerdict::Refund)
            } else {
                None
            }
        }
        (false, false, true, true) => {
            // The known T_tie == T_refund byte-collision (rakeless
            // equal-stakes pot — unreachable at prod stakes, pinned in the
            // covenant's own tests). The output sets are IDENTICAL, so the
            // money outcome is the same either way; the wire locktime picks
            // the honest label.
            if refund_gate_ok {
                Some(PotVerdict::Refund)
            } else {
                Some(PotVerdict::Tie)
            }
        }
        _ => None, // no match, or an impossible multi-match — unresolved
    }
}

// ============================================================================
// Tests (the pure-decoder pins moved here with the code — bsv-low #284)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pot::POC5_TEMPLATE_HEX;

    #[test]
    fn script_num_decoding_is_minimal_and_non_negative() {
        assert_eq!(script_num_u64(&[0x90, 0x01]), Some(400));
        assert_eq!(script_num_u64(&[0xd0, 0x07]), Some(2000));
        // Sign-guard byte accepted (top bit of the value byte set).
        assert_eq!(script_num_u64(&[0x80, 0x00]), Some(128));
        // Negative refused.
        assert_eq!(script_num_u64(&[0x90]), None);
        // Empty / oversized refused.
        assert_eq!(script_num_u64(&[]), None);
        assert_eq!(script_num_u64(&[1; 10]), None);
    }

    #[test]
    fn bare_lock_recognizer_is_exact() {
        let mut s = vec![0x52];
        for seed in [0x02u8, 0x03, 0x04] {
            s.push(33);
            s.extend_from_slice(&[seed; 33]);
        }
        s.push(0x53);
        s.push(0xae);
        assert!(is_bare_2of3_lock(&s));
        // Any perturbation refuses.
        assert!(!is_bare_2of3_lock(&s[..104]));
        let mut t = s.clone();
        t[0] = 0x51; // OP_1-of-3 is not the pot lock
        assert!(!is_bare_2of3_lock(&t));
        assert!(!is_bare_2of3_lock(&[0x76, 0xa9]));
    }

    fn sample_params() -> CovenantParams {
        CovenantParams {
            pub_a: [0x02; 33],
            pub_b: [0x03; 33],
            pub_tower: [0x04; 33],
            pay_pkh_a: [0xAA; 20],
            pay_pkh_b: [0xBB; 20],
            rake_pkh: [0xCC; 20],
            stake_a: 2000,
            stake_b: 2000,
            fee_sats: 400,
            recovery_height: 956_656,
        }
    }

    /// A full covenant lock: frozen HEAD ‖ encoded params ‖ frozen TAIL.
    fn lock_of(p: &CovenantParams) -> Vec<u8> {
        let t = POC5_TEMPLATE_HEX;
        let mut s = hex::decode(&t[..t.find('<').unwrap()]).unwrap();
        s.extend(encode_covenant_param_pushes(p));
        s.extend(hex::decode(&t[t.rfind('>').unwrap() + 1..]).unwrap());
        s
    }

    #[test]
    fn encode_extract_roundtrip_is_identity() {
        // The encoder is the exact inverse of the extractor over the frozen
        // template — the pin that lets tests across crates build locks.
        let p = sample_params();
        assert_eq!(extract_covenant_params(&lock_of(&p)), Some(p));
        // Edge encodings: zero, small-int (OP_N) and sign-guarded values.
        let p = CovenantParams {
            stake_a: 0,
            stake_b: 16,
            fee_sats: 128, // needs the 0x00 sign guard
            recovery_height: 900_000,
            ..sample_params()
        };
        assert_eq!(extract_covenant_params(&lock_of(&p)), Some(p));
    }

    #[test]
    fn hex_reconstruction_matches_extraction_and_refuses_malformed() {
        let p = sample_params();
        let rebuilt = covenant_params_from_hex(
            &hex::encode(p.pub_a),
            &hex::encode(p.pub_b),
            &hex::encode(p.pub_tower),
            &hex::encode(p.pay_pkh_a),
            &hex::encode(p.pay_pkh_b),
            &hex::encode(p.rake_pkh),
            p.stake_a,
            p.stake_b,
            p.fee_sats,
            p.recovery_height,
        );
        assert_eq!(rebuilt, Some(p.clone()));
        // Wrong length / non-hex → None (fall back to the BEEF parse).
        assert_eq!(
            covenant_params_from_hex(
                "02aa", // truncated key
                &hex::encode(p.pub_b),
                &hex::encode(p.pub_tower),
                &hex::encode(p.pay_pkh_a),
                &hex::encode(p.pay_pkh_b),
                &hex::encode(p.rake_pkh),
                p.stake_a,
                p.stake_b,
                p.fee_sats,
                p.recovery_height,
            ),
            None
        );
        assert_eq!(
            covenant_params_from_hex(
                &"zz".repeat(33),
                &hex::encode(p.pub_b),
                &hex::encode(p.pub_tower),
                &hex::encode(p.pay_pkh_a),
                &hex::encode(p.pay_pkh_b),
                &hex::encode(p.rake_pkh),
                p.stake_a,
                p.stake_b,
                p.fee_sats,
                p.recovery_height,
            ),
            None
        );
    }

    #[test]
    fn verdict_wire_strings_roundtrip() {
        for v in [
            PotVerdict::WinnerA,
            PotVerdict::WinnerB,
            PotVerdict::Tie,
            PotVerdict::Refund,
        ] {
            assert_eq!(PotVerdict::from_wire(v.as_str()), Some(v));
        }
        assert_eq!(PotVerdict::from_wire("winner-c"), None);
        assert_eq!(PotVerdict::from_wire(""), None);
    }

    #[test]
    fn classify_covenant_matches_templates_exactly() {
        // pot 4000, fee 400 → net 3600, rake 40: winner gets 3560.
        let p = sample_params();
        let winner_a = RawTx {
            inputs: vec![],
            outputs: vec![(40, p2pkh_lock(&p.rake_pkh)), (3560, p2pkh_lock(&p.pay_pkh_a))],
            lock_time: 0,
        };
        assert_eq!(
            classify_covenant(&p, &winner_a, 0xffff_ffff),
            Some(PotVerdict::WinnerA)
        );
        // A redirect (right values, wrong home) never classifies.
        let redirect = RawTx {
            outputs: vec![(40, p2pkh_lock(&p.rake_pkh)), (3560, p2pkh_lock(&[0xEE; 20]))],
            ..winner_a.clone()
        };
        assert_eq!(classify_covenant(&p, &redirect, 0xffff_ffff), None);
        // The refund shape needs the observed height gate.
        let refund_outs = vec![(1800, p2pkh_lock(&p.pay_pkh_a)), (1800, p2pkh_lock(&p.pay_pkh_b))];
        let gated = RawTx {
            inputs: vec![],
            outputs: refund_outs.clone(),
            lock_time: 956_656,
        };
        assert_eq!(
            classify_covenant(&p, &gated, 0xffff_fffe),
            Some(PotVerdict::Refund)
        );
        let ungated = RawTx {
            lock_time: 0,
            ..gated
        };
        assert_eq!(classify_covenant(&p, &ungated, 0xffff_ffff), None);
    }
}
