//! Server-derived pot RESULTS — chain-truth settle classification (bsv-low
//! #227, campaign #219).
//!
//! ## Why the server can derive results at all — the trust model
//!
//! Since bsv-low #103 the live pot lock is the `Poc5TemplatePot` COVENANT: a
//! 2-of-3 settle-key multisig whose locking script ALSO commits the payout
//! parameters (`payPkhA/payPkhB/rakePkh/stakeA/stakeB/feeSats/
//! recoveryHeight`) and consensus-mandates that ANY spend pay one of four
//! output templates derived in-script from those params:
//!
//!   T_a      `[rake → rakePkh (omit if 0), pot − fee − rake → payPkhA]`
//!   T_b      `[rake → rakePkh (omit if 0), pot − fee − rake → payPkhB]`
//!   T_tie    `[rake' → rakePkh (omit if 0), half → payPkhA, half → payPkhB]`
//!   T_refund `[stakeA − (fee − fee/2) → payPkhA, stakeB − fee/2 → payPkhB]`
//!            (height-gated: nLockTime ≥ recoveryHeight + non-final sequence)
//!
//! with `rake = floor(pot / 100)` (bsv-low #102) and the tie's odd sat joining
//! the rake. The math here is byte-identical to the authoritative pair
//! `pot.ts::settleOutputs` ≡ `cosign.rs::mandated_outputs` and
//! `settle.rs::refund_outputs` (bsv-low `crates/low-spend/src/tower_settle.rs`
//! `template_settle_outputs` / `template_refund_outputs`).
//!
//! THEREFORE: a mined spend of a covenant pot is **co-signed by construction**
//! (the covenant only validates under two of the three settle keys) and can
//! only pay a mandated shape — so matching the spend's outputs against the
//! four templates derived from the pot's OWN committed params is a
//! chain-truth verdict of WHICH exit fired, requiring no client claim at all.
//! The committed params are read out of the funding lock itself (the exact
//! bytes the pot's money sat under), and both raw txs are HASH-VERIFIED
//! against their txids before anything is derived — a lying/garbled store row
//! degrades to `unresolved`, never to a wrong verdict.
//!
//! ## Conservatism — the leaderboard bar is verified-only
//!
//! A verdict is emitted ONLY when the classification is unambiguous:
//! - the spender must actually spend the pot outpoint (input match);
//! - the outputs must equal EXACTLY ONE template (value + script, in order);
//! - the refund template additionally requires its in-script height gate
//!   observed on the wire (`nLockTime ≥` the committed `recoveryHeight`,
//!   block-height semantics, non-final sequence) — except the known T_tie ==
//!   T_refund byte-collision (rakeless equal-stakes pot, unreachable at prod
//!   stakes), where the covenant itself waives the gate and the wire
//!   locktime/sequence picks the label (money-identical either way);
//! - a degenerate pot committing `payPkhA == payPkhB` can never distinguish
//!   winner-A from winner-B → no verdict.
//!
//! Anything else — a bare-era (pre-covenant) lock, a missing/garbled BEEF, a
//! non-matching output set — is `None` (unresolved), NEVER a guessed win.
//!
//! Bare (pre-covenant) 2-of-3 pots get ONE conservative classification:
//! the pre-signed nLockTime REFUND (2 P2PKH outputs, `nLockTime` equal to the
//! `ls_potparty` marker's `recoveryHeight`, non-final sequence, conservation
//! ≤ pot). A refund is money-neutral for the leaderboard (it never counts as
//! a win), so a hostile marker's fake `recoveryHeight` cannot mint a win —
//! at worst it mislabels an exit the legacy claim path already covers.
//! Bare-pot WINNER/TIE shapes are NOT classified (a bare 2-of-3 can pay any
//! outputs, so shape alone proves nothing) — legacy client claims keep
//! covering those games.
//!
//! ## Seat → identity (what is and is NOT derivable server-side)
//!
//! The covenant commits SETTLE keys (`[2,'low settle']`, BRC-42 derived with
//! `counterparty = opponent` — an ECDH the server cannot perform) and payout
//! P2PKH homes (BRC-29 payment derivations, `counterparty = self`). The
//! `ls_potparty` markers carry each seat's IDENTITY key but no seat letter,
//! no settle key, and no pay home. So the server CANNOT map "winner-A" to an
//! identity from indexed data alone:
//! - `tie` / `refund` are seat-symmetric → BOTH identities get the outcome
//!   (pure chain truth, no claim involved);
//! - `winner-a` / `winner-b` are exposed verbatim (a PARTICIPANT knows its
//!   own seat locally and derives won/lost client-side), and the per-identity
//!   `outcome` upgrades to `won`/`lost` when either
//!   * (#230, strongest) the caller's `LOW/potparty/v2` SEAT-BINDING marker
//!     proves which committed lock key it holds — the settle pubkey was
//!     committed in the covenant lock at FUNDING, before the outcome, and
//!     the marker's `seatSig` is BY that key, so the seat → identity map is
//!     unforgeable, un-back-datable, and needs NO countersignature
//!     (`outcomeSource = "chain+seatkey"`); or
//!   * every on-record `tm_result` claim for the game agrees on one winner
//!     among the two parties AND names the chain-classified settle —
//!     claim-corroborated chain truth, never a bare claim
//!     (`outcomeSource = "chain+claim"`).
//!
//! ## Claim signatures are VERIFIED server-side before they corroborate
//!
//! `tm_result` admits markers by BYTE FORMAT only — anyone can publish a
//! marker naming any winner/loser/settle. So before a claim participates in
//! won/lost attribution its signatures are re-verified HERE, with the exact
//! recipe the client's `result.ts::verifyResultRow` uses: BRC-42/43
//! 'anyone'-key verification (`ProtoWallet::anyone()`, protocol
//! `[1, 'low result']`, keyID = gameId) — the winner's sig under the WINNER
//! identity over the canonical result challenge, the loser's countersig
//! under the LOSER identity over the same bytes. A claim whose winner sig
//! does not verify contributes NOTHING (as if never published); a
//! present-but-unverifiable countersig degrades to "no countersig" (the
//! client's `unconfirmed` demotion). The outcome tiers stay honest:
//! `won` needs the winner's VERIFIED sig; `lost` needs the loser's (the
//! caller's own) VERIFIED countersig — so a fabricated marker naming the
//! real settle txid can never flip the reported winner when the honest side
//! never published (adversarial-review finding, 2026-07-22).

use serde_json::json;

use crate::logic::ResultMarkerRow;

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

/// Parse raw tx bytes via `bsv_rs` and require the bytes HASH to
/// `expected_txid` — a garbled or substituted store row must degrade to
/// `None` (→ unresolved), never feed the classifier wrong bytes. An output
/// with no satoshi amount (impossible on a mined tx, but the type allows it)
/// also refuses.
pub fn parse_raw_tx_verified(raw: &[u8], expected_txid: &str) -> Option<RawTx> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    if !tx.id().eq_ignore_ascii_case(expected_txid) {
        return None;
    }
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
/// The recognizer + region split come from `overlay-discovery`'s frozen
/// template (the SAME bytes `tm_pot` admits with, drift-pinned there).
pub fn extract_covenant_params(lock: &[u8]) -> Option<CovenantParams> {
    let region = overlay_discovery::pot::pot_covenant_param_region(lock)?;
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

/// The canonical 25-byte P2PKH locking script for a 20-byte hash160.
fn p2pkh_lock(pkh: &[u8; 20]) -> Vec<u8> {
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
const LOCKTIME_THRESHOLD: u32 = 500_000_000;

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
/// (see the module note: seat → identity is not derivable server-side).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PotVerdict {
    WinnerA,
    WinnerB,
    Tie,
    Refund,
}

impl PotVerdict {
    /// The wire string for JSON bodies.
    pub fn as_str(self) -> &'static str {
        match self {
            PotVerdict::WinnerA => "winner-a",
            PotVerdict::WinnerB => "winner-b",
            PotVerdict::Tie => "tie",
            PotVerdict::Refund => "refund",
        }
    }
}

/// Everything the classifier consumes for one pot spend. All txids lowercase
/// hex; the raws are hash-verified inside (a mismatch → unresolved).
pub struct PotSpendFacts<'a> {
    /// The pot funding txid + the pot output index within it.
    pub pot_txid: &'a str,
    pub pot_vout: u32,
    /// The funding tx raw bytes (must hash to `pot_txid`).
    pub funding_raw: &'a [u8],
    /// The recorded spender's txid + raw bytes (must hash to it).
    pub spender_txid: &'a str,
    pub spender_raw: &'a [u8],
    /// The `ls_potparty` marker's recoveryHeight — an UNVERIFIED hint, used
    /// ONLY for the bare-pot refund rule (money-neutral; see module note).
    pub marker_recovery_height: Option<u32>,
}

/// Classify one pot spend. `None` = unresolved (never a guess).
///
/// See the module docs for the full rule set + conservatism rationale.
pub fn classify_pot_spend(f: &PotSpendFacts) -> Option<PotVerdict> {
    let funding = parse_raw_tx_verified(f.funding_raw, f.pot_txid)?;
    let spender = parse_raw_tx_verified(f.spender_raw, f.spender_txid)?;

    // The recorded spender must ACTUALLY spend the pot outpoint.
    let pot_input = spender
        .inputs
        .iter()
        .find(|i| i.prev_txid.eq_ignore_ascii_case(f.pot_txid) && i.prev_vout == f.pot_vout)?;

    let (pot_sats, pot_lock) = spender_pot_prevout(&funding, f.pot_vout)?;

    if let Some(p) = extract_covenant_params(&pot_lock) {
        // The covenant asserts `ctx.utxo.value == stakeA + stakeB` in-script;
        // a funding output that disagrees is not the pot the params describe.
        if p.stake_a.checked_add(p.stake_b)? != pot_sats {
            return None;
        }
        return classify_covenant(&p, &spender, pot_input.sequence);
    }

    if is_bare_2of3_lock(&pot_lock) {
        return classify_bare_refund(&spender, pot_input.sequence, pot_sats, f.marker_recovery_height);
    }

    None // unknown lock shape — never classified
}

/// The pot prevout `(satoshis, lock)` from the parsed funding tx.
fn spender_pot_prevout(funding: &RawTx, vout: u32) -> Option<(u64, Vec<u8>)> {
    let (sats, lock) = funding.outputs.get(vout as usize)?;
    Some((*sats, lock.clone()))
}

/// Covenant classification: exact output-set match against the four derived
/// templates, refund height-gate observed, collisions resolved only when
/// money-identical.
fn classify_covenant(p: &CovenantParams, spender: &RawTx, pot_sequence: u32) -> Option<PotVerdict> {
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

/// Bare-era (pre-covenant) pots: classify ONLY the pre-signed nLockTime
/// refund (see module note — winner/tie shapes prove nothing on a bare
/// 2-of-3). Requires the potparty marker's recoveryHeight to EQUAL the wire
/// nLockTime (the pre-signed refund sets it exactly), non-final sequence,
/// exactly 2 P2PKH outputs, and conservation (outputs ≤ pot).
fn classify_bare_refund(
    spender: &RawTx,
    pot_sequence: u32,
    pot_sats: u64,
    marker_recovery_height: Option<u32>,
) -> Option<PotVerdict> {
    let h = marker_recovery_height?;
    if h == 0 || h >= LOCKTIME_THRESHOLD {
        return None;
    }
    if spender.lock_time != h || pot_sequence == 0xffff_ffff {
        return None;
    }
    if spender.outputs.len() != 2 {
        return None;
    }
    let mut sum: u64 = 0;
    for (sats, script) in &spender.outputs {
        if !is_p2pkh(script) {
            return None;
        }
        sum = sum.checked_add(*sats)?;
    }
    if sum > pot_sats {
        return None;
    }
    Some(PotVerdict::Refund)
}

/// Standard 25-byte P2PKH lock check (same shape `tm_lowfund` recognizes).
fn is_p2pkh(s: &[u8]) -> bool {
    s.len() == 25 && s[0] == 0x76 && s[1] == 0xa9 && s[2] == 0x14 && s[23] == 0x88 && s[24] == 0xac
}

/// The mined block height of `txid` per its stored BEEF's BUMP, when the
/// completion pass has stitched one in. `None` when unproven/unknown — a
/// missing height is presented as `null`, never guessed.
pub fn beef_block_height(beef_bytes: &[u8], txid: &str) -> Option<u64> {
    let beef = bsv_rs::transaction::Beef::from_binary(beef_bytes).ok()?;
    let btx = beef.find_txid(&txid.to_ascii_lowercase())?;
    let bump = beef.bumps.get(btx.bump_index()?)?;
    Some(u64::from(bump.block_height))
}

// ── #230 potparty-v2 SEAT ATTRIBUTION (the countersignature-free winner map) ─
//
// A `LOW/potparty/v2` marker carries `seatSettlePubkey` — the seat's
// `[2,'low settle']` pubkey, the EXACT key the covenant lock committed as
// `pubA`/`pubB` at FUNDING time, before the outcome was known — plus
// `seatSig`, a signature BY that settle key binding {gameId, potOutpoint,
// identity}. Verifying the sig and matching the pubkey against the pot's OWN
// committed lock keys yields seat → identity: unforgeable (only the key
// holder can sign), un-back-datable (the key predates the outcome), and
// slot-exact ("both seats claim seat A" is structurally impossible — the
// bytes decide, not the claim). Joined to the chain verdict, `winner-a` +
// "identity X held pubA" ⇒ X won — with NO countersignature and no claim.

/// The frozen seatSig domain tag — MUST equal the client's
/// `potParty.ts::POTPARTY_SEATSIG_DOMAIN` byte-for-byte.
pub const POTPARTY_SEATSIG_DOMAIN: &[u8] = b"LOW/potparty/v2/seatsig|";

/// One raw v2 potparty marker row (from `potparty_records`), NOT yet
/// verified — [`verify_seat_marker`] decides whether it attributes anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatMarkerRow {
    /// The publishing identity (66-hex compressed pubkey).
    pub identity: String,
    /// The marker's gameId (64-hex).
    pub game_id: String,
    /// The pot outpoint the marker names.
    pub pot_txid: String,
    pub pot_vout: u32,
    /// The claimed settle pubkey (66-hex) — matched against the lock.
    pub seat_settle_pubkey: String,
    /// The settle key's DER signature over the seat-binding preimage.
    pub seat_sig_hex: String,
}

/// The EXACT cross-repo seatSig preimage (bsv-low #230; the client's
/// `potPartySeatSigPreimage`):
///
///   `"LOW/potparty/v2/seatsig|" ‖ gameId(32) ‖ potTxid(32) ‖ potVout(4 LE)
///    ‖ identity(33)`
///
/// `seatSig` is ECDSA over a SINGLE sha256 of these bytes (the BRC-100
/// `createSignature({data})` hash). `None` when any hex field is malformed —
/// an unbuildable preimage can never verify (fail-safe).
pub fn seatsig_preimage(
    game_id_hex: &str,
    pot_txid_hex: &str,
    pot_vout: u32,
    identity_hex: &str,
) -> Option<Vec<u8>> {
    let game_id = hex::decode(game_id_hex).ok()?;
    let pot_txid = hex::decode(pot_txid_hex).ok()?;
    let identity = hex::decode(identity_hex).ok()?;
    if game_id.len() != 32 || pot_txid.len() != 32 || identity.len() != 33 {
        return None;
    }
    let mut out = Vec::with_capacity(POTPARTY_SEATSIG_DOMAIN.len() + 32 + 32 + 4 + 33);
    out.extend_from_slice(POTPARTY_SEATSIG_DOMAIN);
    out.extend_from_slice(&game_id);
    out.extend_from_slice(&pot_txid);
    out.extend_from_slice(&pot_vout.to_le_bytes());
    out.extend_from_slice(&identity);
    Some(out)
}

/// Verify one v2 marker's SEAT signature: plain secp256k1 ECDSA under the
/// marker's OWN `seatSettlePubkey` over sha256(preimage). No BRC-42
/// derivation is needed (or possible) server-side — the settle key attests
/// its own identity binding, and lock membership is checked separately by
/// [`attribute_seats`]. Any malformed key/sig/field is `false` (refused).
pub fn verify_seat_marker(m: &SeatMarkerRow) -> bool {
    let Some(preimage) = seatsig_preimage(
        &m.game_id.to_ascii_lowercase(),
        &m.pot_txid.to_ascii_lowercase(),
        m.pot_vout,
        &m.identity.to_ascii_lowercase(),
    ) else {
        return false;
    };
    let Ok(pubkey) = bsv_rs::primitives::ec::PublicKey::from_hex(&m.seat_settle_pubkey) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(&m.seat_sig_hex) else {
        return false;
    };
    let Ok(sig) = bsv_rs::primitives::ec::Signature::from_der(&sig_bytes) else {
        return false;
    };
    let hash = bsv_rs::primitives::hash::sha256(&preimage);
    pubkey.verify(&hash, &sig)
}

/// Which identity (if provably any) holds each committed settle-key slot of
/// one pot. `None` = unattributed (no verified marker for the slot, or a
/// conflict — never a guess).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeatAttribution {
    /// The identity that PROVED holding `pubA` (the seat-A settle key).
    pub identity_a: Option<String>,
    /// The identity that PROVED holding `pubB`.
    pub identity_b: Option<String>,
}

impl SeatAttribution {
    /// The attributed winner identity for a chain verdict, when the winning
    /// seat's slot is attributed (tie/refund/None attribute nobody).
    pub fn winner_for(&self, verdict: PotVerdict) -> Option<&str> {
        match verdict {
            PotVerdict::WinnerA => self.identity_a.as_deref(),
            PotVerdict::WinnerB => self.identity_b.as_deref(),
            PotVerdict::Tie | PotVerdict::Refund => None,
        }
    }
}

/// Fold v2 markers against the pot's COMMITTED lock keys (bsv-low #230 —
/// the risk-register bars, all refusals silent):
///
/// - a marker whose `seatSettlePubkey` is NOT `pubA`/`pubB` is REFUSED (the
///   lock is the authority; a foreign key attributes nothing);
/// - a marker whose `seatSig` does not verify is REFUSED;
/// - a marker naming a different (gameId-irrelevant) pot outpoint than the
///   one being attributed is REFUSED (the preimage binds the outpoint);
/// - each key matches only its OWN slot, so both seats claiming "seat A" is
///   impossible by construction — and CONFLICTING identities for one slot
///   (two verified markers, different identities, same key — only the key
///   holder can mint them, i.e. self-sabotage) poison that slot to `None`;
/// - a degenerate lock with `pubA == pubB` attributes NOTHING (one key
///   cannot distinguish the seats);
/// - duplicate identical markers (the outpoint-replay case) are idempotent.
pub fn attribute_seats(
    params: &CovenantParams,
    pot_txid_lc: &str,
    pot_vout: u32,
    markers: &[SeatMarkerRow],
) -> SeatAttribution {
    let mut out = SeatAttribution::default();
    if params.pub_a == params.pub_b {
        return out; // degenerate lock — seats indistinguishable
    }
    let pub_a_hex = hex::encode(params.pub_a);
    let pub_b_hex = hex::encode(params.pub_b);
    // Some(None) = slot poisoned by conflict; None = untouched.
    let mut slot_a: Option<Option<String>> = None;
    let mut slot_b: Option<Option<String>> = None;
    for m in markers {
        if !m.pot_txid.eq_ignore_ascii_case(pot_txid_lc) || m.pot_vout != pot_vout {
            continue; // a marker for a different pot never attributes this one
        }
        let pk = m.seat_settle_pubkey.to_ascii_lowercase();
        let slot = if pk == pub_a_hex {
            &mut slot_a
        } else if pk == pub_b_hex {
            &mut slot_b
        } else {
            continue; // key not committed in this pot's lock — refused
        };
        if !verify_seat_marker(m) {
            continue; // signature does not verify — refused
        }
        let id = m.identity.to_ascii_lowercase();
        match slot {
            None => *slot = Some(Some(id)),
            Some(Some(existing)) if *existing == id => {} // idempotent replay
            Some(_) => *slot = Some(None),                // conflicting identities — poison
        }
    }
    out.identity_a = slot_a.flatten();
    out.identity_b = slot_b.flatten();
    out
}

/// A committed-lock seat slot (`pubA` = A, `pubB` = B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatLetter {
    A,
    B,
}

/// The caller's proven seat for one pot, from its OWN verified v2 marker(s)
/// against the committed lock keys — the `/results` consumer. `None` unless
/// exactly one of the two slots is attributed TO THE CALLER.
pub fn my_seat(
    params: &CovenantParams,
    pot_txid_lc: &str,
    pot_vout: u32,
    identity_lc: &str,
    markers: &[SeatMarkerRow],
) -> Option<SeatLetter> {
    let attr = attribute_seats(params, pot_txid_lc, pot_vout, markers);
    let a = attr.identity_a.as_deref() == Some(identity_lc);
    let b = attr.identity_b.as_deref() == Some(identity_lc);
    match (a, b) {
        (true, false) => Some(SeatLetter::A),
        (false, true) => Some(SeatLetter::B),
        _ => None, // unattributed, or the impossible both-slots case
    }
}

/// [`derive_outcome`] with the #230 seat attribution layered ON TOP: for a
/// winner verdict, the caller's PROVEN seat decides won/lost directly
/// (`outcomeSource = "chain+seatkey"`) — no countersignature, no claim. The
/// seat proof is strictly stronger than claim corroboration (committed
/// pre-outcome, signed by the lock key itself), so it takes precedence;
/// without one, the claim-corroboration rules apply unchanged. `lost` via
/// seat key is fair to show: the caller's own wallet published the seat
/// proof at funding, and the chain verdict is the covenant's own mandate.
pub fn derive_outcome_with_seat(
    verdict: Option<PotVerdict>,
    my_seat: Option<SeatLetter>,
    identity_lc: &str,
    opponent_lc: &str,
    settle_txid_lc: Option<&str>,
    claims: Option<&GameClaims>,
) -> (Outcome, Option<&'static str>) {
    if let (Some(v @ (PotVerdict::WinnerA | PotVerdict::WinnerB)), Some(seat)) = (verdict, my_seat)
    {
        let won = matches!(
            (v, seat),
            (PotVerdict::WinnerA, SeatLetter::A) | (PotVerdict::WinnerB, SeatLetter::B)
        );
        return (
            if won { Outcome::Won } else { Outcome::Lost },
            Some("chain+seatkey"),
        );
    }
    derive_outcome(verdict, identity_lc, opponent_lc, settle_txid_lc, claims)
}

// ── /results assembly ───────────────────────────────────────────────────────

/// One pot the identity is a party to, ready for classification: the
/// `potparty_records` facts joined to the spend pointer and both stored
/// BEEFs. The route dedupes marker rows to one entry per pot outpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultsRow {
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    pub recovery_height: u32,
    pub opponent_identity: String,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// `hex(pot_beefs.beef)` for the FUNDING tx (keyed by potTxid).
    pub funding_beef_hex: Option<String>,
    /// `hex(pot_beefs.beef)` for the recorded spender.
    pub spender_beef_hex: Option<String>,
    /// #230 v2 seat-binding fields from the caller's own potparty row —
    /// `None` for a v1 row. UNVERIFIED here; `my_seat` verifies before they
    /// attribute anything.
    pub seat_settle_pubkey: Option<String>,
    pub seat_sig_hex: Option<String>,
}

/// A game's SIGNATURE-VERIFIED `tm_result` claims relevant to won/lost
/// attribution. Only markers whose WINNER signature verified under the
/// claimed winner's identity make it in at all (`verified_claim`); claims
/// remain corroboration-only — a claim can never create a result the chain
/// did not classify, and conflicting claims yield `unresolved`.
#[derive(Debug, Clone, Default)]
pub struct GameClaims {
    pub claims: Vec<ClaimFact>,
}

/// One verified claim fact (all fields lowercased). Existence of a
/// `ClaimFact` MEANS the winner's signature verified over the canonical
/// challenge; `loser_sig_verified` reports the countersig independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimFact {
    /// Claimed winner identity — its signature VERIFIED.
    pub winner: String,
    /// Claimed loser identity (whose countersig, if any, is judged below).
    pub loser: String,
    /// The settle txid the claim names (the chain-verdict binding).
    pub settle_txid: String,
    /// True iff a loser countersig was present AND verified under `loser`.
    pub loser_sig_verified: bool,
    /// The WINNER's revealed 5 cards, CANONICAL 10-hex (sorted ascending
    /// ordinals) — `None` for a v1 (no-cards) claim. These bytes are BOUND by
    /// the (already-verified) winner signature (the v2 challenge commits the
    /// canonical cards), so a present value is the winner's true showdown hand.
    /// Only the winner's hand is ever revealed on-chain; the loser's is not.
    pub cards_hex: Option<String>,
}

// ── server-side claim signature verification ────────────────────────────────
//
// The exact recipe of the client's `result.ts` — same protocol, same keyID,
// same challenge bytes, same 'anyone' verifier. BRC-42 derivation is
// byte-identical across the Rust and TS SDKs (cross-vectored in bsv-rs;
// production-proven by the LOW lobby tokens, which the TS app signs and
// `overlay-discovery`'s Rust topic manager verifies with this same
// `ProtoWallet::anyone()` pattern).

/// The BRC-43 protocol result claims are signed under —
/// `result.ts::RESULT_PROTOCOL` = `[1, 'low result']`.
fn result_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low result")
}

/// Canonicalize a v2 cards push: 10 hex chars → five DISTINCT ordinals
/// 0..=51, sorted ascending, re-encoded lowercase — `result.ts::cardsToHex ∘
/// cardsFromHex`. `None` = malformed (an unverifiable claim: the sigs bind
/// the canonical cards, so we must be able to reconstruct them).
fn canonical_cards_hex(cards_hex: &str) -> Option<String> {
    let mut cards = hex::decode(cards_hex).ok()?;
    if cards.len() != 5 || cards.iter().any(|&c| c > 51) {
        return None;
    }
    cards.sort_unstable();
    if cards.windows(2).any(|w| w[0] == w[1]) {
        return None;
    }
    Some(hex::encode(cards))
}

/// The canonical signed challenge — byte-identical to
/// `result.ts::resultChallenge` (all fields lowercased; v2 binds the
/// canonical sorted cards). Inputs must already be lowercase.
fn result_challenge_bytes(
    game_id_lc: &str,
    winner_lc: &str,
    loser_lc: &str,
    pot_lc: &str,
    settle_lc: &str,
    cards_hex: Option<&str>,
) -> Option<Vec<u8>> {
    let base = format!(
        "gid={game_id_lc}\nwinner={winner_lc}\nloser={loser_lc}\npot={pot_lc}\nsettle={settle_lc}"
    );
    let s = match cards_hex {
        Some(ch) => {
            let cards = canonical_cards_hex(ch)?;
            format!("LOW-result\nv2\n{base}\ncards={cards}")
        }
        None => format!("LOW-result\nv1\n{base}"),
    };
    Some(s.into_bytes())
}

/// Verify one DER signature under `signer_identity_hex` over `challenge`
/// with the public 'anyone' verifier — the mirror of the client's
/// `anyoneVerifier.verifySignature({counterparty: signer, forSelf: false})`.
/// Any malformed key/sig/derivation failure is simply `false` (fail-safe:
/// an unverifiable signature never corroborates).
fn anyone_sig_verifies(
    signer_identity_hex: &str,
    key_id: &str,
    challenge: &[u8],
    sig_hex: &str,
) -> bool {
    let Ok(signer) = bsv_rs::primitives::ec::PublicKey::from_hex(signer_identity_hex) else {
        return false;
    };
    let Ok(sig) = hex::decode(sig_hex) else {
        return false;
    };
    bsv_rs::wallet::ProtoWallet::anyone()
        .verify_signature(bsv_rs::wallet::VerifySignatureArgs {
            data: Some(challenge.to_vec()),
            hash_to_directly_verify: None,
            signature: sig,
            protocol_id: result_protocol(),
            key_id: key_id.to_string(),
            counterparty: Some(bsv_rs::wallet::Counterparty::Other(signer)),
            for_self: Some(false),
        })
        .map(|r| r.valid)
        .unwrap_or(false)
}

/// Verify one raw `result_markers_v2` row into a [`ClaimFact`], or `None`
/// when it must contribute nothing: self-paired, malformed cards, or a
/// winner signature that does not verify under the claimed winner identity.
/// A present-but-unverifiable LOSER countersig does not kill the claim — it
/// degrades to `loser_sig_verified: false` (the client's `unconfirmed`
/// demotion in `verifyResultRow`): the winner's own claim still stands,
/// only the confirmation tier is garbage.
pub fn verified_claim(m: &ResultMarkerRow) -> Option<ClaimFact> {
    let winner_lc = m.winner.to_ascii_lowercase();
    let loser_lc = m.loser.to_ascii_lowercase();
    if winner_lc == loser_lc {
        return None; // self-paired claims are invalid (client parity)
    }
    let game_lc = m.game_id.to_ascii_lowercase();
    let challenge = result_challenge_bytes(
        &game_lc,
        &winner_lc,
        &loser_lc,
        &m.pot_txid.to_ascii_lowercase(),
        &m.settle_txid.to_ascii_lowercase(),
        m.cards_hex.as_deref(),
    )?;
    if !anyone_sig_verifies(&winner_lc, &game_lc, &challenge, &m.winner_sig_hex) {
        return None; // fabricated/garbled claim — as if never published
    }
    let loser_sig_verified = m
        .loser_sig_hex
        .as_deref()
        .is_some_and(|s| anyone_sig_verifies(&loser_lc, &game_lc, &challenge, s));
    // The cards are re-canonicalized from the SAME field the challenge bound
    // (present ⇒ it verified as part of the winner sig above), so a Some value
    // is trustworthy. A malformed field can't reach here (the challenge would
    // have failed to reconstruct → early `None`), but re-canonicalize
    // defensively so downstream always sees canonical 10-hex or nothing.
    let cards_hex = m.cards_hex.as_deref().and_then(canonical_cards_hex);
    Some(ClaimFact {
        winner: winner_lc,
        loser: loser_lc,
        settle_txid: m.settle_txid.to_ascii_lowercase(),
        loser_sig_verified,
        cards_hex,
    })
}

/// One `/results` response entry, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultEntry {
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    pub recovery_height: u32,
    pub opponent_identity: String,
    pub settle_txid: Option<String>,
    pub spent: Option<bool>,
    pub spent_confirmed: Option<bool>,
    /// The chain-truth template classification (`winner-a`/`winner-b`/`tie`/
    /// `refund`), `None` = not classified.
    pub verdict: Option<PotVerdict>,
    /// The per-identity outcome (see [`derive_outcome`]).
    pub outcome: Outcome,
    /// How `outcome` was derived: `"chain"` (seat-symmetric verdict),
    /// `"chain+seatkey"` (winner verdict + the caller's #230 seat-binding
    /// proof — no countersignature involved), `"chain+claim"` (winner
    /// verdict + unanimous claims), `None` for `unresolved`.
    pub outcome_source: Option<&'static str>,
    /// The settle's mined block height per its BEEF BUMP, when proven.
    pub at_height: Option<u64>,
    /// The provable showdown hand (bsv-low #245): the WINNER's five cards +
    /// low-sum, or `None` when no hand is provable (refund, unrevealed settle,
    /// unresolved winner). Only the winner's hand is on-chain — the loser's is
    /// never fabricated. See [`resolve_winner_hand`].
    pub winner_hand: Option<WinnerHand>,
}

/// The per-identity outcome enum (wire strings match bsv-low #227's spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Won,
    Lost,
    Tie,
    Refund,
    Unresolved,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Won => "won",
            Outcome::Lost => "lost",
            Outcome::Tie => "tie",
            Outcome::Refund => "refund",
            Outcome::Unresolved => "unresolved",
        }
    }
}

/// Map a chain verdict (+ VERIFIED claims) to the identity's outcome.
///
/// - `tie` / `refund` are seat-symmetric → pure chain truth.
/// - a winner verdict upgrades ONLY when every verified on-record claim that
///   names the classified settle txid agrees on ONE winner and that winner
///   is one of the two parties (the caller or its opponent). No claims,
///   conflicting claims, or a claimed winner outside the pair →
///   `unresolved` (the chain alone cannot name the seat's identity — module
///   note).
/// - the tiers are key-honest (every `ClaimFact` already carries a VERIFIED
///   winner sig — `claims_by_game` drops the rest):
///   * `won` — the unanimous verified winner is the caller. Nobody can put
///     the caller here without the caller's own key.
///   * `lost` — the unanimous verified winner is the opponent AND some claim
///     naming this settle carries the CALLER's verified countersig
///     (`loser == identity`, `loser_sig_verified`). The caller attested the
///     loss itself; an opponent-only (or third-party-countersigned) claim
///     never shows the caller a loss.
pub fn derive_outcome(
    verdict: Option<PotVerdict>,
    identity_lc: &str,
    opponent_lc: &str,
    settle_txid_lc: Option<&str>,
    claims: Option<&GameClaims>,
) -> (Outcome, Option<&'static str>) {
    match verdict {
        Some(PotVerdict::Tie) => (Outcome::Tie, Some("chain")),
        Some(PotVerdict::Refund) => (Outcome::Refund, Some("chain")),
        Some(PotVerdict::WinnerA) | Some(PotVerdict::WinnerB) => {
            let (Some(settle), Some(gc)) = (settle_txid_lc, claims) else {
                return (Outcome::Unresolved, None);
            };
            // The verified claims naming THIS settle.
            let relevant: Vec<&ClaimFact> = gc
                .claims
                .iter()
                .filter(|c| c.settle_txid.eq_ignore_ascii_case(settle))
                .collect();
            let mut winners: Vec<&str> = relevant.iter().map(|c| c.winner.as_str()).collect();
            winners.sort_unstable();
            winners.dedup();
            match winners.as_slice() {
                [w] if w.eq_ignore_ascii_case(identity_lc) => (Outcome::Won, Some("chain+claim")),
                [w] if w.eq_ignore_ascii_case(opponent_lc) => {
                    // Lost needs the caller's OWN verified countersig.
                    if relevant
                        .iter()
                        .any(|c| c.loser.eq_ignore_ascii_case(identity_lc) && c.loser_sig_verified)
                    {
                        (Outcome::Lost, Some("chain+claim"))
                    } else {
                        (Outcome::Unresolved, None)
                    }
                }
                _ => (Outcome::Unresolved, None),
            }
        }
        None => (Outcome::Unresolved, None),
    }
}

// ── hand-score exposure (bsv-low #245) ──────────────────────────────────────
//
// Your Games wants the SHOWDOWN, not just win/lose: the winner's five cards +
// its low-sum, honestly attributed. The only cards on-chain are the WINNER's
// (a coop/enforced settle never reveals the loser's hand), carried in a
// `tm_result` marker as `cardsHex` and BOUND by the winner's signature. So the
// exposed hand rides on the SAME verified, unanimous claim that already drives
// `won`/`lost` — never a bare/forged marker, never a fabricated loser hand.

/// The provable showdown hand for a `/results` row: the WINNER's five cards +
/// its low-sum, plus whose hand it is. The loser's hand is NEVER on-chain for a
/// settle, so it is never present here (see [`ResultEntry::winner_hand`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WinnerHand {
    /// Whose five cards these are (the winning identity; for a tie, the seat
    /// whose equal-sum hand was revealed — `is_tie` flags it).
    pub identity: String,
    /// The five cards, CANONICAL 10-hex (sorted ascending ordinals).
    pub cards_hex: String,
    /// The LOW low-sum of `cards_hex` (`logic::hand_score` — Ace=1, 2..9 pip,
    /// T/J/Q/K=10). For a tie this is BOTH players' equal sum.
    pub score: u32,
    /// True when the chain verdict was a TIE (both sums equal by definition);
    /// the exposed hand is one provable side, `identity` its owner.
    pub is_tie: bool,
}

/// Parse + score a claim's `cards_hex` into a [`WinnerHand`], or `None` if the
/// cards are malformed (fail-safe: never expose an unparseable hand).
fn winner_hand_from(identity: &str, cards_hex: &str, is_tie: bool) -> Option<WinnerHand> {
    let arr = crate::logic::leaderboard_cards_from_hex(cards_hex)?;
    let canon = canonical_cards_hex(cards_hex)?;
    Some(WinnerHand {
        identity: identity.to_ascii_lowercase(),
        cards_hex: canon,
        score: crate::logic::hand_score(&arr),
        is_tie,
    })
}

/// Resolve the provable showdown hand for a row from the chain verdict + the
/// VERIFIED claims naming the classified settle. Viewer-INDEPENDENT (a per-game
/// fact both parties see identically). `None` unless a hand is genuinely
/// provable — a refund, an unrevealed (v1) settle, an unresolved/conflicting
/// winner, or a claim winner outside the two parties all yield `None` (never a
/// guess, never a fabricated loser hand).
///
/// - `winner-a`/`winner-b`: needs the SAME unanimous verified winner
///   `derive_outcome` requires (one winner among the two parties, naming this
///   settle) AND a claim by that winner carrying cards → the winner's hand.
/// - `tie`: any verified claim by a party naming this settle that carries cards
///   → that (equal-sum) hand, `is_tie = true`. Both sums are equal by the tie
///   verdict; only the one revealed side is exposed (the other isn't on-chain).
/// - `refund` / `None`: no showdown → `None`.
pub fn resolve_winner_hand(
    verdict: Option<PotVerdict>,
    identity_lc: &str,
    opponent_lc: &str,
    settle_txid_lc: Option<&str>,
    claims: Option<&GameClaims>,
) -> Option<WinnerHand> {
    let settle = settle_txid_lc?;
    let gc = claims?;
    let is_party = |id: &str| {
        id.eq_ignore_ascii_case(identity_lc) || id.eq_ignore_ascii_case(opponent_lc)
    };
    let relevant: Vec<&ClaimFact> = gc
        .claims
        .iter()
        .filter(|c| c.settle_txid.eq_ignore_ascii_case(settle))
        .collect();
    match verdict {
        Some(PotVerdict::WinnerA) | Some(PotVerdict::WinnerB) => {
            let mut winners: Vec<&str> = relevant.iter().map(|c| c.winner.as_str()).collect();
            winners.sort_unstable();
            winners.dedup();
            let [w] = winners.as_slice() else {
                return None; // no claim, or conflicting winners → unresolved
            };
            if !is_party(w) {
                return None; // a foreign claim never attributes this pot's hand
            }
            let cards = relevant
                .iter()
                .filter(|c| c.winner.eq_ignore_ascii_case(w))
                .find_map(|c| c.cards_hex.as_deref())?;
            winner_hand_from(w, cards, false)
        }
        Some(PotVerdict::Tie) => {
            let c = relevant
                .iter()
                .find(|c| is_party(&c.winner) && c.cards_hex.is_some())?;
            winner_hand_from(&c.winner, c.cards_hex.as_deref()?, true)
        }
        Some(PotVerdict::Refund) | None => None,
    }
}

/// Assemble the `/results` entries: dedupe rows to one per pot outpoint
/// (newest first, as the SQL orders), classify each spent pot, and derive
/// the caller's outcome. Missing bytes anywhere degrade THAT entry to
/// `unresolved` — never an error, never a guess.
pub fn assemble_results(
    identity_lc: &str,
    rows: Vec<ResultsRow>,
    claims_by_game: &std::collections::HashMap<String, GameClaims>,
) -> Vec<ResultEntry> {
    // #230: gather the caller's v2 seat markers per (game, pot) key BEFORE
    // dedup — the same pot can have a v1 row AND a v2 row (both published),
    // and whichever the dedup keeps, the seat proof must not be lost.
    let mut seat_markers: std::collections::HashMap<(String, String, u32), Vec<SeatMarkerRow>> =
        std::collections::HashMap::new();
    for r in &rows {
        if let (Some(pk), Some(sig)) = (&r.seat_settle_pubkey, &r.seat_sig_hex) {
            let key = (
                r.game_id.to_ascii_lowercase(),
                r.pot_txid.to_ascii_lowercase(),
                r.pot_vout,
            );
            seat_markers.entry(key).or_default().push(SeatMarkerRow {
                identity: identity_lc.to_string(),
                game_id: r.game_id.to_ascii_lowercase(),
                pot_txid: r.pot_txid.to_ascii_lowercase(),
                pot_vout: r.pot_vout,
                seat_settle_pubkey: pk.to_ascii_lowercase(),
                seat_sig_hex: sig.to_ascii_lowercase(),
            });
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for r in rows {
        let key = (
            r.game_id.to_ascii_lowercase(),
            r.pot_txid.to_ascii_lowercase(),
            r.pot_vout,
        );
        if !seen.insert(key.clone()) {
            continue; // duplicate marker rows (garbage coexists by design)
        }
        let settle_lc = r.spending_txid.as_ref().map(|s| s.to_ascii_lowercase());
        let mut verdict = None;
        let mut at_height = None;
        let mut seat = None;
        if let (Some(true), Some(settle), Some(fb_hex), Some(sb_hex)) =
            (r.spent, settle_lc.as_deref(), &r.funding_beef_hex, &r.spender_beef_hex)
        {
            if let (Some(fb), Some(sb)) =
                (crate::logic::decode_beef_hex(fb_hex), crate::logic::decode_beef_hex(sb_hex))
            {
                let pot_txid_lc = r.pot_txid.to_ascii_lowercase();
                let funding_raw = crate::logic::extract_raw_tx_hex(&fb, &pot_txid_lc)
                    .and_then(|h| hex::decode(h).ok());
                let spender_raw = crate::logic::extract_raw_tx_hex(&sb, settle)
                    .and_then(|h| hex::decode(h).ok());
                if let (Some(fraw), Some(sraw)) = (funding_raw, spender_raw) {
                    verdict = classify_pot_spend(&PotSpendFacts {
                        pot_txid: &pot_txid_lc,
                        pot_vout: r.pot_vout,
                        funding_raw: &fraw,
                        spender_txid: settle,
                        spender_raw: &sraw,
                        marker_recovery_height: Some(r.recovery_height),
                    });
                    // #230: the caller's PROVEN seat, from its v2 marker(s)
                    // verified against the pot's OWN committed lock (the
                    // hash-verified funding bytes — never a store claim).
                    if verdict.is_some() {
                        seat = parse_raw_tx_verified(&fraw, &pot_txid_lc)
                            .and_then(|f| spender_pot_prevout(&f, r.pot_vout))
                            .and_then(|(_, lock)| extract_covenant_params(&lock))
                            .and_then(|p| {
                                my_seat(
                                    &p,
                                    &pot_txid_lc,
                                    r.pot_vout,
                                    identity_lc,
                                    seat_markers.get(&key).map(Vec::as_slice).unwrap_or(&[]),
                                )
                            });
                    }
                }
                at_height = beef_block_height(&sb, settle);
            }
        }
        let game_lc = r.game_id.to_ascii_lowercase();
        let opponent_lc = r.opponent_identity.to_ascii_lowercase();
        let game_claims = claims_by_game.get(&game_lc);
        let (outcome, outcome_source) = derive_outcome_with_seat(
            verdict,
            seat,
            identity_lc,
            &opponent_lc,
            settle_lc.as_deref(),
            game_claims,
        );
        let winner_hand = resolve_winner_hand(
            verdict,
            identity_lc,
            &opponent_lc,
            settle_lc.as_deref(),
            game_claims,
        );
        out.push(ResultEntry {
            game_id: game_lc,
            pot_txid: r.pot_txid.to_ascii_lowercase(),
            pot_vout: r.pot_vout,
            recovery_height: r.recovery_height,
            opponent_identity: opponent_lc,
            settle_txid: settle_lc,
            spent: r.spent,
            spent_confirmed: r.spent_confirmed,
            verdict,
            outcome,
            outcome_source,
            at_height,
            winner_hand,
        });
    }
    out
}

/// Build the claims-by-game map from raw `result_markers_v2` rows — each
/// marker goes through `verified_claim` (real ECDSA over the reconstructed
/// challenge) and an unverifiable one is DROPPED here, so nothing downstream
/// ever sees a claim whose winner signature did not verify.
pub fn claims_by_game(markers: &[ResultMarkerRow]) -> std::collections::HashMap<String, GameClaims> {
    let mut map: std::collections::HashMap<String, GameClaims> = std::collections::HashMap::new();
    for m in markers {
        if let Some(fact) = verified_claim(m) {
            map.entry(m.game_id.to_ascii_lowercase())
                .or_default()
                .claims
                .push(fact);
        }
    }
    map
}

/// Assemble the `/results` wire body:
/// `{"identity","results":[{gameId,potTxid,potVout,recoveryHeight,
/// opponentIdentity,settleTxid,spent,spentConfirmed,verdict,outcome,
/// outcomeSource,at,hand}]}`. `at` is `{"height": <n|null>}` (block height
/// when the settle's BEEF carries a verified BUMP; time is not tracked).
/// `hand` (bsv-low #245) is the provable showdown —
/// `{winnerIdentity,winnerCardsHex,winnerScore,isTie,loserCardsOnChain,note}`
/// — or `null` when no hand is provable (refund / unrevealed / unresolved).
/// Only the winner's five cards are on-chain; the loser's is never fabricated.
pub fn results_body(identity: &str, entries: &[ResultEntry]) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            json!({
                "gameId": e.game_id,
                "potTxid": e.pot_txid,
                "potVout": e.pot_vout,
                "recoveryHeight": e.recovery_height,
                "opponentIdentity": e.opponent_identity,
                "settleTxid": e.settle_txid,
                "spent": e.spent,
                "spentConfirmed": e.spent_confirmed,
                "verdict": e.verdict.map(PotVerdict::as_str),
                "outcome": e.outcome.as_str(),
                "outcomeSource": e.outcome_source,
                "at": { "height": e.at_height },
                // The showdown (#245): the winner's five cards + low-sum, or
                // null when no hand is provable. `loserCardsOnChain` is always
                // false — the loser's hand is never revealed for a settle, and
                // is never fabricated here. `note` explains the caveat for the
                // client's honest "—" rendering.
                "hand": e.winner_hand.as_ref().map(|h| json!({
                    "winnerIdentity": h.identity,
                    "winnerCardsHex": h.cards_hex,
                    "winnerScore": h.score,
                    "isTie": h.is_tie,
                    "loserCardsOnChain": false,
                    "note": if h.is_tie {
                        "tie — both sums are equal; only the revealed side's five \
                         cards are on-chain (the other player's hand is not)"
                    } else {
                        "only the winner's five cards are revealed on-chain; the \
                         loser's hand is not (do not fabricate it)"
                    },
                })),
            })
        })
        .collect();
    json!({ "identity": identity, "results": arr }).to_string()
}

/// The `/results` potparty join SQL: the caller's marker rows JOINed to the
/// pot spend status plus BOTH stored BEEFs (funding keyed by potTxid,
/// spender keyed by spendingTxid). Bounded: newest [`RESULTS_MAX_ROWS`]
/// marker rows (the D1 work + BLOB transfer bound — the >50-outpoint 503
/// lesson; a heavier history paginates in a future rev).
pub fn results_sql() -> String {
    format!(
        "SELECT pp.gameId, pp.potTxid, pp.potVout, pp.recoveryHeight, \
                pp.opponentIdentity, \
                pp.seatSettlePubkey, pp.seatSigHex, \
                r.spent, r.spendingTxid, r.spentConfirmed, \
                hex(fb.beef) AS fundingBeef, \
                hex(sb.beef) AS spenderBeef \
         FROM potparty_records pp \
         LEFT JOIN pot_records r ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
         LEFT JOIN pot_beefs fb ON fb.txid = lower(pp.potTxid) \
         LEFT JOIN pot_beefs sb ON sb.txid = lower(r.spendingTxid) \
         WHERE pp.identity = ? \
         ORDER BY pp.createdAt DESC, pp.rowid DESC LIMIT {RESULTS_MAX_ROWS}"
    )
}

/// Hard bound on `/results` marker rows per request (BLOB-weight bound).
pub const RESULTS_MAX_ROWS: usize = 100;

/// The claims query for a chunk of gameIds (1 bind each — chunk at
/// [`crate::logic::D1_CHUNK_OUTPOINTS`] to stay far under D1's 100-param cap).
pub fn claims_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let placeholders = vec!["?"; n].join(",");
    format!(
        "SELECT gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
         loserSigHex, cardsHex, txid, createdAt FROM result_markers_v2 \
         WHERE gameId IN ({placeholders}) ORDER BY createdAt DESC, rowid DESC LIMIT 1000"
    )
}

// ── /spent-any — server-side legacy outpoint spend reads ────────────────────
//
// (bsv-low #227 addendum.) LEGACY (pre-pot-index) escrow outpoints were never
// admitted to `pot_records`, so `/utxo-status` answers `known:false` forever
// and the browser used to fall back to direct WhatsOnChain calls — slow,
// rate-limited, CORS-hostile. `/spent-any` answers spend status for ARBITRARY
// outpoints by querying the upstream providers SERVER-SIDE, with the
// proof-source-order doctrine applied:
//
//  - POSITIVE (a spender exists): WoC's pointer alone is accepted ONLY after
//    RAW VERIFICATION — the spender's raw bytes are fetched, hash-checked
//    against the reported txid, and input-matched to the requested outpoint.
//    A pointer that fails verification is a provider fault → `known:false`.
//  - NEGATIVE (unspent): NEVER concluded from WoC alone. A second provider
//    (Bitails) must cleanly corroborate "unspent"; any fault/ambiguity on
//    either side → `known:false` (honest unknown, the caller's existing
//    fail-safe shape). NOTE: Bitails' outpoint-spent endpoint was faulting
//    (HTTP 500) at build time — until it recovers, negatives surface as
//    `known:false`, which is exactly the fail-safe the doctrine demands.
//
// Responses reuse the `/utxo-status` row shape so the client parser is
// shared. A short in-isolate cache (~15 s) bounds upstream pressure.

/// Hard cap on `/spent-any` outpoints per request (each may cost up to two
/// upstream subrequests — bound the fan-out).
pub const SPENT_ANY_MAX_OUTPOINTS: usize = 20;

/// In-isolate cache TTL for `/spent-any` entries, milliseconds.
pub const SPENT_ANY_CACHE_TTL_MS: f64 = 15_000.0;

/// One provider observation for an outpoint, already shape-validated by the
/// route glue. The pure decision logic below is what unit tests pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpentObservation {
    /// WoC 200: a spender txid (lowercase hex) + whether WoC reports the
    /// spend confirmed.
    Spent { txid: String, confirmed: bool },
    /// WoC 4xx: "unspent or not yet indexed".
    NotSpent,
    /// Transport / 5xx / rate-limit / malformed body.
    Fault,
}

/// Bitails' corroboration of an UNSPENT claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnspentCorroboration {
    /// Bitails cleanly indicated the outpoint is unspent.
    ConfirmedUnspent,
    /// Bitails faulted / answered ambiguously / contradicted.
    Unknown,
}

/// The `/spent-any` decision table (pure — unit-tested with mock upstream
/// observations). `spender_raw_ok` = the spender raw was fetched, hashed to
/// the reported txid, and input-matched to the outpoint.
pub fn decide_spent_any(
    woc: &SpentObservation,
    spender_raw_ok: bool,
    bitails_unspent: UnspentCorroboration,
) -> crate::logic::OutpointStatus {
    // Filled in by the route with the real outpoint; the decision only sets
    // the known/spent/spender fields, so use a placeholder outpoint here.
    let op = crate::logic::Outpoint {
        txid: String::new(),
        vout: 0,
    };
    match woc {
        SpentObservation::Spent { txid, confirmed } => {
            if spender_raw_ok {
                crate::logic::OutpointStatus::known(&op, true, Some(txid.clone()), *confirmed)
            } else {
                // Unverifiable pointer → honest unknown, never a bare claim.
                crate::logic::OutpointStatus::unknown(&op)
            }
        }
        SpentObservation::NotSpent => match bitails_unspent {
            UnspentCorroboration::ConfirmedUnspent => {
                crate::logic::OutpointStatus::known(&op, false, None, false)
            }
            UnspentCorroboration::Unknown => crate::logic::OutpointStatus::unknown(&op),
        },
        SpentObservation::Fault => crate::logic::OutpointStatus::unknown(&op),
    }
}

/// Verify a fetched spender raw: hashes to `spender_txid` AND spends
/// `(outpoint_txid, vout)`. The one-provider-positive rule rests on this.
pub fn spender_raw_verifies(
    raw: &[u8],
    spender_txid: &str,
    outpoint_txid: &str,
    vout: u32,
) -> bool {
    match parse_raw_tx_verified(raw, spender_txid) {
        Some(tx) => tx
            .inputs
            .iter()
            .any(|i| i.prev_txid.eq_ignore_ascii_case(outpoint_txid) && i.prev_vout == vout),
        None => false,
    }
}

/// Parse a WoC `/tx/{txid}/{vout}/spent` 200 body: `{"txid": "...",
/// "status": "confirmed"|...}`. Strict: a malformed txid is a Fault.
pub fn parse_woc_spent_body(v: &serde_json::Value) -> SpentObservation {
    let Some(txid) = v.get("txid").and_then(|t| t.as_str()) else {
        return SpentObservation::Fault;
    };
    if !crate::logic::valid_txid(txid) {
        return SpentObservation::Fault;
    }
    let confirmed = v.get("status").and_then(|s| s.as_str()) == Some("confirmed");
    SpentObservation::Spent {
        txid: txid.to_ascii_lowercase(),
        confirmed,
    }
}

/// Parse a Bitails outpoint-spent response into an unspent corroboration.
/// STRICT: only an explicit, well-formed `{"spent": false}` counts as a
/// clean unspent signal; everything else (their current 500 fault, unknown
/// shapes, `spent:true` — which would CONTRADICT WoC's negative) is Unknown.
pub fn parse_bitails_unspent(status: u16, v: Option<&serde_json::Value>) -> UnspentCorroboration {
    if status == 200 {
        if let Some(val) = v {
            if val.get("spent").and_then(serde_json::Value::as_bool) == Some(false) {
                return UnspentCorroboration::ConfirmedUnspent;
            }
        }
    }
    UnspentCorroboration::Unknown
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::logic::ResultMarkerRow;

    fn ident(b: u8) -> String {
        format!("02{}", format!("{b:02x}").repeat(32))
    }
    fn tx(b: u8) -> String {
        format!("{b:02x}").repeat(32)
    }

    // ── outcome derivation ─────────────────────────────────────────────

    /// Post-verification claim facts (winner, loser, settle, loser_verified)
    /// — the shape `claims_by_game` emits AFTER real signature verification.
    /// The adversarial tests further below go through the REAL producer
    /// (`claims_by_game` over signed markers); these tuples unit-test the
    /// outcome table itself.
    fn claims_of(entries: &[(&str, &str, &str, bool)]) -> GameClaims {
        GameClaims {
            claims: entries
                .iter()
                .map(|(w, l, s, lv)| ClaimFact {
                    winner: w.to_string(),
                    loser: l.to_string(),
                    settle_txid: s.to_string(),
                    loser_sig_verified: *lv,
                    cards_hex: None,
                })
                .collect(),
        }
    }

    #[test]
    fn tie_and_refund_are_seat_symmetric_chain_truth() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        // No claims needed at all — pure chain truth.
        let (o, src) = derive_outcome(Some(PotVerdict::Tie), &me, &opp, None, None);
        assert_eq!((o, src), (Outcome::Tie, Some("chain")));
        let (o, src) = derive_outcome(Some(PotVerdict::Refund), &me, &opp, None, None);
        assert_eq!((o, src), (Outcome::Refund, Some("chain")));
    }

    #[test]
    fn winner_verdict_upgrades_only_on_unanimous_corroborating_claims() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        let settle = tx(0x22);

        // No claims → unresolved (the chain alone cannot name the seat's
        // identity — the module-doc seat→identity truth).
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), None);
        assert_eq!((o, src), (Outcome::Unresolved, None));

        // A unanimous countersigned claim naming ME for THIS settle → won.
        let gc = claims_of(&[(&me, &opp, &settle, true)]);
        let (o, src) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));

        // The same claim from the OPPONENT's perspective → lost (its OWN
        // countersig verified).
        let (o, src) =
            derive_outcome(Some(PotVerdict::WinnerA), &opp, &me, Some(&settle), Some(&gc));
        assert_eq!((o, src), (Outcome::Lost, Some("chain+claim")));

        // Conflicting claims (both parties claim the same settle) → nobody.
        let gc = claims_of(&[(&me, &opp, &settle, true), (&opp, &me, &settle, true)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // A winner-sig-only claim (no verified countersig): the WINNER's tier
        // is earned (its own verified key), but the LOSER is NEVER shown a
        // loss it did not countersign.
        let gc = claims_of(&[(&me, &opp, &settle, false)]);
        let (o, src) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));
        let (o, src) =
            derive_outcome(Some(PotVerdict::WinnerA), &opp, &me, Some(&settle), Some(&gc));
        assert_eq!((o, src), (Outcome::Unresolved, None));

        // A countersig by a THIRD PARTY (claim's loser is not the caller)
        // never shows the caller a loss.
        let gc = claims_of(&[(&me, &ident(0xcc), &settle, true)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &opp, &me, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // A claim naming a DIFFERENT settle never corroborates this one.
        let gc = claims_of(&[(&me, &opp, &tx(0x33), true)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // A claimed winner OUTSIDE the two parties → unresolved (a foreign
        // marker can't award this pot to anyone).
        let gc = claims_of(&[(&ident(0xcc), &ident(0xdd), &settle, true)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // No verdict at all → unresolved even with a pretty claim (a claim
        // alone NEVER makes a server-derived result — the owner directive).
        let gc = claims_of(&[(&me, &opp, &settle, true)]);
        let (o, _) = derive_outcome(None, &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);
    }

    // ── #230 seat attribution (REAL secp256k1 keys + signatures — the
    //    risk-register B1/B5 bars; never a mocked verify) ─────────────────

    /// A real keypair: (privkey, 66-hex compressed pubkey).
    fn real_key(seed: u8) -> (bsv_rs::primitives::ec::PrivateKey, String) {
        let k = bsv_rs::primitives::ec::PrivateKey::from_bytes(&{
            let mut b = [0u8; 32];
            b[31] = seed;
            b
        })
        .unwrap();
        let pk = k.public_key().to_hex();
        (k, pk.to_ascii_lowercase())
    }

    /// A REAL seat marker: `seat_sig` is a genuine ECDSA signature by
    /// `settle_key` over sha256 of the exact cross-repo preimage.
    fn real_seat_marker(
        settle_key: &bsv_rs::primitives::ec::PrivateKey,
        settle_pub_hex: &str,
        identity_hex: &str,
        game_id: &str,
        pot_txid: &str,
        pot_vout: u32,
    ) -> SeatMarkerRow {
        let preimage = seatsig_preimage(game_id, pot_txid, pot_vout, identity_hex).unwrap();
        let hash = bsv_rs::primitives::hash::sha256(&preimage);
        let sig = settle_key.sign(&hash).unwrap();
        SeatMarkerRow {
            identity: identity_hex.to_string(),
            game_id: game_id.to_string(),
            pot_txid: pot_txid.to_string(),
            pot_vout,
            seat_settle_pubkey: settle_pub_hex.to_string(),
            seat_sig_hex: hex::encode(sig.to_der()),
        }
    }

    /// CovenantParams committing the two settle pubkeys (rest arbitrary).
    fn params_with_keys(pub_a_hex: &str, pub_b_hex: &str) -> CovenantParams {
        let mut pub_a = [0u8; 33];
        pub_a.copy_from_slice(&hex::decode(pub_a_hex).unwrap());
        let mut pub_b = [0u8; 33];
        pub_b.copy_from_slice(&hex::decode(pub_b_hex).unwrap());
        CovenantParams {
            pub_a,
            pub_b,
            pub_tower: [2u8; 33],
            pay_pkh_a: [0xaa; 20],
            pay_pkh_b: [0xbb; 20],
            rake_pkh: [0xcc; 20],
            stake_a: 500,
            stake_b: 500,
            fee_sats: 10,
            recovery_height: 900_000,
        }
    }

    #[test]
    fn seat_attribution_real_roundtrip_and_slot_exactness() {
        let (ka, pa) = real_key(41); // seat A settle key (committed as pubA)
        let (kb, pb) = real_key(42); // seat B settle key
        let ida = ident(0xaa); // seat A's identity
        let idb = ident(0xbb);
        let gid = tx(0x01);
        let pot = tx(0x22);
        let p = params_with_keys(&pa, &pb);

        let ma = real_seat_marker(&ka, &pa, &ida, &gid, &pot, 0);
        let mb = real_seat_marker(&kb, &pb, &idb, &gid, &pot, 0);
        assert!(verify_seat_marker(&ma), "a genuine seat sig verifies");
        let attr = attribute_seats(&p, &pot, 0, &[ma.clone(), mb.clone()]);
        assert_eq!(attr.identity_a.as_deref(), Some(ida.as_str()));
        assert_eq!(attr.identity_b.as_deref(), Some(idb.as_str()));
        assert_eq!(attr.winner_for(PotVerdict::WinnerA), Some(ida.as_str()));
        assert_eq!(attr.winner_for(PotVerdict::WinnerB), Some(idb.as_str()));
        assert_eq!(attr.winner_for(PotVerdict::Tie), None);
        assert_eq!(attr.winner_for(PotVerdict::Refund), None);

        // BOTH-SEATS-CLAIM-SEAT-A (risk register): B's key can only ever
        // match its OWN lock slot — even if B "claims seat A" socially, the
        // bytes put it in slot B. And B CANNOT occupy slot A: a marker
        // pairing pubA with B's identity needs a signature by A's key over
        // B's identity, which only A can mint. Simulate the best theft
        // available (copy A's marker, swap the identity): the sig no longer
        // verifies and the marker is refused.
        let stolen = SeatMarkerRow {
            identity: idb.clone(),
            ..ma.clone()
        };
        assert!(!verify_seat_marker(&stolen), "preimage binds the identity");
        let attr = attribute_seats(&p, &pot, 0, &[stolen, mb.clone()]);
        assert_eq!(attr.identity_a, None, "slot A stays unattributed");
        assert_eq!(attr.identity_b.as_deref(), Some(idb.as_str()));

        // Replayed identical markers (outpoint-dup) are idempotent.
        let attr = attribute_seats(&p, &pot, 0, &[ma.clone(), ma.clone(), mb]);
        assert_eq!(attr.identity_a.as_deref(), Some(ida.as_str()));

        // My seat, from my own markers only (the /results consumer).
        assert_eq!(
            my_seat(&p, &pot, 0, &ida, std::slice::from_ref(&ma)),
            Some(SeatLetter::A)
        );
        assert_eq!(my_seat(&p, &pot, 0, &idb, &[ma]), None);
    }

    #[test]
    fn seat_attribution_refusals_are_exhaustive() {
        let (ka, pa) = real_key(41);
        let (_kb, pb) = real_key(42);
        let (kf, pf) = real_key(43); // a FOREIGN key, not in the lock
        let ida = ident(0xaa);
        let gid = tx(0x01);
        let pot = tx(0x22);
        let p = params_with_keys(&pa, &pb);

        // (B1) a marker whose settle pubkey is NOT in the pot's lock is
        // refused at read — even with a perfectly valid signature.
        let foreign = real_seat_marker(&kf, &pf, &ida, &gid, &pot, 0);
        assert!(verify_seat_marker(&foreign), "the sig itself is fine…");
        let attr = attribute_seats(&p, &pot, 0, &[foreign]);
        assert_eq!(attr, SeatAttribution::default(), "…but the lock refuses it");

        // A marker whose seatSig does not verify is refused (tampered sig).
        let mut bad = real_seat_marker(&ka, &pa, &ida, &gid, &pot, 0);
        let mut sig = hex::decode(&bad.seat_sig_hex).unwrap();
        let last = sig.len() - 1;
        sig[last] ^= 0x01;
        bad.seat_sig_hex = hex::encode(sig);
        assert!(!verify_seat_marker(&bad));
        assert_eq!(
            attribute_seats(&p, &pot, 0, &[bad]),
            SeatAttribution::default()
        );

        // A marker for a DIFFERENT pot outpoint contributes nothing to this
        // one (and a marker for a NONEXISTENT pot simply never joins — there
        // is no params/verdict for it to attribute).
        let other_pot = real_seat_marker(&ka, &pa, &ida, &gid, &tx(0x33), 0);
        assert!(verify_seat_marker(&other_pot));
        assert_eq!(
            attribute_seats(&p, &pot, 0, std::slice::from_ref(&other_pot)),
            SeatAttribution::default()
        );
        let wrong_vout = real_seat_marker(&ka, &pa, &ida, &gid, &pot, 1);
        assert_eq!(
            attribute_seats(&p, &pot, 0, &[wrong_vout]),
            SeatAttribution::default()
        );

        // Malformed fields never panic, never verify.
        let mut junk = other_pot;
        junk.seat_settle_pubkey = "zz".repeat(33);
        assert!(!verify_seat_marker(&junk));
        assert!(seatsig_preimage("nothex", &pot, 0, &ida).is_none());
        assert!(seatsig_preimage(&gid, &pot, 0, "02aabb").is_none()); // short identity

        // CONFLICTING identities for one slot (two verified markers by the
        // SAME key naming different identities — only the key holder can
        // mint both, i.e. self-sabotage): the slot poisons to None.
        let m1 = real_seat_marker(&ka, &pa, &ida, &gid, &pot, 0);
        let m2 = real_seat_marker(&ka, &pa, &ident(0xcc), &gid, &pot, 0);
        assert!(verify_seat_marker(&m2));
        let attr = attribute_seats(&p, &pot, 0, &[m1, m2]);
        assert_eq!(attr.identity_a, None, "conflicting slot poisons");

        // A degenerate lock (pubA == pubB) attributes NOTHING.
        let p_degen = params_with_keys(&pa, &pa);
        let m = real_seat_marker(&ka, &pa, &ida, &gid, &pot, 0);
        assert_eq!(
            attribute_seats(&p_degen, &pot, 0, &[m]),
            SeatAttribution::default()
        );
    }

    /// The CROSS-REPO crypto round-trip: the client's FROZEN golden v2 marker
    /// (real `@bsv/sdk` ProtoWallet output, pinned in `potParty.test.ts` and
    /// in `overlay-discovery`'s parser tests) must verify under THIS crate's
    /// server-side ECDSA — proving the preimage layout AND the single-sha256
    /// hash convention match the wallet byte-for-byte.
    #[test]
    fn golden_client_v2_marker_verifies_server_side() {
        const GOLDEN_V2_HEX: &str = "006a0f4c4f572f706f7470617274792f7632210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee520cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc20dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd04010000000428a00e002103d3e37fc9edbd1c225d703873b45f66368e86c633cb613252b3254ffe0b8ad5ee4630440220106a632f58753f6b9ebaf20d105874d3aed43c28dab90e8b6a8a51dbd610e1e402204c7837248995842ec551eb3c8510b5862f87bf0c54368534fd3d7c1e3b9a50fd473045022100d3ea901d46fa588cb2f20e0bb0a3c7e23f6320138efee69f9e506a8e79abbaa102207cfccbd475e5d9e789091acdfa7d81503b950ebf51da6a1ac9fec44c84553773";
        let script = hex::decode(GOLDEN_V2_HEX).unwrap();
        let m = overlay_discovery::potparty::parse_potparty_marker(&script)
            .expect("the golden v2 marker parses");
        let row = SeatMarkerRow {
            identity: hex::encode(&m.identity),
            game_id: hex::encode(m.game_id),
            pot_txid: hex::encode(m.pot_txid),
            pot_vout: m.pot_vout,
            seat_settle_pubkey: hex::encode(m.seat_settle_pubkey.as_ref().unwrap()),
            seat_sig_hex: hex::encode(m.seat_sig.as_ref().unwrap()),
        };
        assert!(
            verify_seat_marker(&row),
            "the client's REAL wallet-derived seat signature must verify server-side"
        );
        // Tamper the identity → refused (the preimage binds it).
        let mut swapped = row;
        swapped.identity = hex::encode(&m.opponent);
        assert!(!verify_seat_marker(&swapped));
    }

    #[test]
    fn derive_outcome_with_seat_precedence_and_fallback() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        let settle = tx(0x22);

        // Seat proof decides won/lost with NO claims at all (#276 fixed).
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            Some(SeatLetter::A),
            &me,
            &opp,
            Some(&settle),
            None,
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+seatkey")));
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            Some(SeatLetter::B),
            &me,
            &opp,
            Some(&settle),
            None,
        );
        assert_eq!((o, src), (Outcome::Lost, Some("chain+seatkey")));
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerB),
            Some(SeatLetter::B),
            &me,
            &opp,
            Some(&settle),
            None,
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+seatkey")));

        // The seat proof outranks a conflicting claim set (which alone would
        // yield unresolved).
        let gc = claims_of(&[(&me, &opp, &settle, true), (&opp, &me, &settle, true)]);
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            Some(SeatLetter::A),
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+seatkey")));

        // No seat proof → the claim-corroboration rules verbatim.
        let gc = claims_of(&[(&me, &opp, &settle, true)]);
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            None,
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));

        // A seat proof NEVER manufactures an outcome without a verdict, and
        // tie/refund stay pure chain truth (seat-symmetric).
        let (o, _) = derive_outcome_with_seat(None, Some(SeatLetter::A), &me, &opp, None, None);
        assert_eq!(o, Outcome::Unresolved);
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::Tie),
            Some(SeatLetter::A),
            &me,
            &opp,
            None,
            None,
        );
        assert_eq!((o, src), (Outcome::Tie, Some("chain")));
    }

    // ── assemble_results (BEEF-fed producer path) ──────────────────────

    /// Wrap a raw tx in a minimal (unproven) BEEF, as uppercase hex — the
    /// SQLite `hex()` read-back shape the route feeds in.
    fn beef_hex_of(raw: &[u8]) -> String {
        let tx = bsv_rs::transaction::Transaction::from_binary(raw).unwrap();
        let mut beef = bsv_rs::transaction::Beef::new();
        beef.merge_transaction(tx);
        hex::encode(beef.to_binary()).to_ascii_uppercase()
    }

    /// A tiny synthetic pot: bare-era lock is NOT used here — we build a
    /// spend-shape that stays UNRESOLVED (unknown lock), which is all the
    /// assembly plumbing needs (classification itself is pinned against the
    /// real mainnet fixtures in `tests/classifier_real_txs.rs`).
    fn fake_funding_and_spender() -> (Vec<u8>, String, Vec<u8>, String) {
        // funding: one dummy input, one 1000-sat OP_TRUE output.
        let mut f = Vec::new();
        f.extend_from_slice(&1u32.to_le_bytes());
        f.push(1);
        f.extend_from_slice(&[0x11u8; 32]);
        f.extend_from_slice(&0u32.to_le_bytes());
        f.push(0);
        f.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        f.push(1);
        f.extend_from_slice(&1000u64.to_le_bytes());
        f.push(1);
        f.push(0x51);
        f.extend_from_slice(&0u32.to_le_bytes());
        let f_id = bsv_rs::transaction::Transaction::from_binary(&f).unwrap().id();
        // spender: spends funding:0.
        let mut s = Vec::new();
        s.extend_from_slice(&1u32.to_le_bytes());
        s.push(1);
        let mut prev = hex::decode(&f_id).unwrap();
        prev.reverse();
        s.extend_from_slice(&prev);
        s.extend_from_slice(&0u32.to_le_bytes());
        s.push(0);
        s.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        s.push(1);
        s.extend_from_slice(&900u64.to_le_bytes());
        s.push(1);
        s.push(0x51);
        s.extend_from_slice(&0u32.to_le_bytes());
        let s_id = bsv_rs::transaction::Transaction::from_binary(&s).unwrap().id();
        (f, f_id, s, s_id)
    }

    #[test]
    fn assemble_results_dedupes_and_fail_safes() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        let (f_raw, f_id, s_raw, s_id) = fake_funding_and_spender();
        let row = ResultsRow {
            game_id: tx(0x01),
            pot_txid: f_id.clone(),
            pot_vout: 0,
            recovery_height: 900_000,
            opponent_identity: opp.clone(),
            spent: Some(true),
            spending_txid: Some(s_id.clone()),
            spent_confirmed: Some(true),
            funding_beef_hex: Some(beef_hex_of(&f_raw)),
            spender_beef_hex: Some(beef_hex_of(&s_raw)),
            seat_settle_pubkey: None,
            seat_sig_hex: None,
        };
        // A duplicate marker row (garbage coexists by outpoint keying) and an
        // unspent pot with no bytes at all.
        let unspent = ResultsRow {
            game_id: tx(0x02),
            pot_txid: tx(0x44),
            pot_vout: 0,
            recovery_height: 900_100,
            opponent_identity: opp.clone(),
            spent: None,
            spending_txid: None,
            spent_confirmed: None,
            funding_beef_hex: None,
            spender_beef_hex: None,
            seat_settle_pubkey: None,
            seat_sig_hex: None,
        };
        let rows = vec![row.clone(), row.clone(), unspent];
        let entries = assemble_results(&me, rows, &std::collections::HashMap::new());
        assert_eq!(entries.len(), 2, "duplicate pot rows dedupe");
        // Unknown lock shape → verdict None, outcome unresolved — never a
        // guess, and the pointer facts still serve.
        assert_eq!(entries[0].verdict, None);
        assert_eq!(entries[0].outcome, Outcome::Unresolved);
        assert_eq!(entries[0].settle_txid.as_deref(), Some(s_id.as_str()));
        assert_eq!(entries[0].at_height, None, "unproven BEEF → no height");
        // The never-spent pot keeps its fail-safe nulls.
        assert_eq!(entries[1].spent, None);
        assert_eq!(entries[1].outcome, Outcome::Unresolved);
    }

    #[test]
    fn results_body_shape() {
        let me = ident(0xaa);
        let e = ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 958_846,
            opponent_identity: ident(0xbb),
            settle_txid: Some(tx(0x03)),
            spent: Some(true),
            spent_confirmed: Some(true),
            verdict: Some(PotVerdict::Refund),
            outcome: Outcome::Refund,
            outcome_source: Some("chain"),
            at_height: Some(958_900),
            winner_hand: None,
        };
        let v: serde_json::Value = serde_json::from_str(&results_body(&me, &[e])).unwrap();
        assert_eq!(v["identity"], me);
        let r = &v["results"][0];
        assert_eq!(r["gameId"], tx(0x01));
        assert_eq!(r["potTxid"], tx(0x02));
        assert_eq!(r["verdict"], "refund");
        assert_eq!(r["outcome"], "refund");
        assert_eq!(r["outcomeSource"], "chain");
        assert_eq!(r["at"]["height"], 958_900);
        assert_eq!(r["settleTxid"], tx(0x03));
        // A refund has no showdown → `hand` is JSON null (never fabricated).
        assert!(r["hand"].is_null());
    }

    /// A winner ResultEntry carrying a showdown hand serializes the full
    /// `hand` object (winner-only cards + score + the loser caveat).
    #[test]
    fn results_body_carries_the_winner_hand() {
        let me = ident(0xaa);
        let e = ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 958_846,
            opponent_identity: ident(0xbb),
            settle_txid: Some(tx(0x03)),
            spent: Some(true),
            spent_confirmed: Some(true),
            verdict: Some(PotVerdict::WinnerA),
            outcome: Outcome::Won,
            outcome_source: Some("chain+claim"),
            at_height: Some(958_900),
            winner_hand: Some(WinnerHand {
                identity: me.clone(),
                cards_hex: "000102030c".to_string(), // A-2-3-4-5 wheel = 15
                score: 15,
                is_tie: false,
            }),
        };
        let v: serde_json::Value = serde_json::from_str(&results_body(&me, &[e])).unwrap();
        let h = &v["results"][0]["hand"];
        assert_eq!(h["winnerIdentity"], me);
        assert_eq!(h["winnerCardsHex"], "000102030c");
        assert_eq!(h["winnerScore"], 15);
        assert_eq!(h["isTie"], false);
        assert_eq!(h["loserCardsOnChain"], false);
        assert!(h["note"].as_str().unwrap().contains("winner"));
    }

    // ── server-side claim signature verification (adversarial) ─────────
    //
    // Real ECDSA round-trips through the REAL producer path
    // (`claims_by_game` → `derive_outcome`) — never a mocked verify, never
    // hand-fed post-verification facts (repo doctrine). The signing recipe
    // is the client's exactly: `createSignature` for counterparty 'anyone'
    // under `[1,'low result']` with keyID = gameId.

    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, ProtoWallet};

    /// Deterministic test wallet (same test-key crypto the workspace's
    /// topic-manager tests use — a pinned root private key).
    fn wallet_of(seed: u8) -> ProtoWallet {
        let key = PrivateKey::from_hex(&format!("{seed:064x}")).unwrap();
        ProtoWallet::new(Some(key))
    }

    fn identity_of(w: &ProtoWallet) -> String {
        w.identity_key_hex().to_ascii_lowercase()
    }

    /// Sign the canonical result challenge as the client does (counterparty
    /// 'anyone', keyID = gameId), returning DER hex.
    fn sign_result(w: &ProtoWallet, game_id: &str, challenge: &[u8]) -> String {
        let sig = w
            .create_signature(CreateSignatureArgs {
                data: Some(challenge.to_vec()),
                hash_to_directly_sign: None,
                protocol_id: result_protocol(),
                key_id: game_id.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();
        hex::encode(sig.signature)
    }

    /// A marker row over the standard test claim shape; sigs supplied by the
    /// caller (real, forged, or absent).
    #[allow(clippy::too_many_arguments)]
    fn marker(
        game: &str,
        winner: &str,
        loser: &str,
        pot: &str,
        settle: &str,
        cards_hex: Option<&str>,
        winner_sig_hex: String,
        loser_sig_hex: Option<String>,
    ) -> ResultMarkerRow {
        ResultMarkerRow {
            game_id: game.to_string(),
            winner: winner.to_string(),
            loser: loser.to_string(),
            pot_txid: pot.to_string(),
            settle_txid: settle.to_string(),
            winner_sig_hex,
            loser_sig_hex,
            cards_hex: cards_hex.map(str::to_string),
            txid: tx(0x04),
            created_at: Some(1),
        }
    }

    /// A plausibly-shaped but FABRICATED DER sig (valid hex, garbage bytes).
    fn garbage_sig() -> String {
        format!("3045{}", "ab".repeat(69))
    }

    #[test]
    fn fabricated_sig_claim_contributes_nothing_and_never_flips_the_winner() {
        // THE finding scenario: the loser fabricates a marker naming the REAL
        // settle txid and themselves as winner, with garbage sig bytes — and
        // pads a garbage "countersig" so the old `loser_sig_hex.is_some()`
        // check would have counted it as both-signed. The honest side never
        // published. Server-side verification must drop the whole claim.
        let honest = wallet_of(0x11);
        let liar = wallet_of(0x22);
        let (h, l) = (identity_of(&honest), identity_of(&liar));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));

        let fabricated = marker(
            &game, &l, &h, &pot, &settle, None,
            garbage_sig(),
            Some(garbage_sig()),
        );
        let map = claims_by_game(&[fabricated]);
        assert!(
            !map.contains_key(&game),
            "a claim with an unverifiable winner sig must contribute NOTHING"
        );
        // Outcome: unresolved for BOTH parties — the honest player is never
        // shown a fabricated loss, the liar never a fabricated win.
        for (me, opp) in [(&h, &l), (&l, &h)] {
            let (o, src) =
                derive_outcome(Some(PotVerdict::WinnerA), me, opp, Some(&settle), map.get(&game));
            assert_eq!((o, src), (Outcome::Unresolved, None));
        }
    }

    #[test]
    fn real_signed_claim_upgrades_and_forged_countersig_never_shows_a_loss() {
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let challenge = result_challenge_bytes(&game, &w, &l, &pot, &settle, None).unwrap();
        let w_sig = sign_result(&winner_w, &game, &challenge);
        let l_sig = sign_result(&loser_w, &game, &challenge);

        // Fully countersigned: winner → won, loser → lost (both verified).
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &settle, None, w_sig.clone(), Some(l_sig),
        )]);
        let gc = map.get(&game).unwrap();
        assert_eq!(gc.claims.len(), 1);
        assert!(gc.claims[0].loser_sig_verified);
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &w, &l, Some(&settle), Some(gc));
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &l, &w, Some(&settle), Some(gc));
        assert_eq!((o, src), (Outcome::Lost, Some("chain+claim")));

        // FORGED countersig (garbage bytes next to a REAL winner sig): the
        // claim survives at winner-sig tier (client's `unconfirmed` demotion)
        // — winner still won, but the loser is NEVER shown a loss it did not
        // itself countersign. (The old presence-only check called this
        // both-signed and reported `lost`.)
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &settle, None, w_sig.clone(), Some(garbage_sig()),
        )]);
        let gc = map.get(&game).unwrap();
        assert!(!gc.claims[0].loser_sig_verified);
        let (o, _) = derive_outcome(Some(PotVerdict::WinnerA), &w, &l, Some(&settle), Some(gc));
        assert_eq!(o, Outcome::Won);
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &l, &w, Some(&settle), Some(gc));
        assert_eq!((o, src), (Outcome::Unresolved, None));

        // A countersig by the WRONG key (the winner signing "the loser's"
        // countersig) never verifies under the loser identity either.
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &settle, None, w_sig, Some(sign_result(&winner_w, &game, &challenge)),
        )]);
        assert!(!map.get(&game).unwrap().claims[0].loser_sig_verified);
    }

    #[test]
    fn disagreeing_verified_claims_stay_unresolved() {
        // Both parties publish REAL self-signed claims for the same settle —
        // unanimity fails, nobody gets an outcome (verdict-only honesty).
        let a = wallet_of(0x11);
        let b = wallet_of(0x22);
        let (ia, ib) = (identity_of(&a), identity_of(&b));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let ch_a = result_challenge_bytes(&game, &ia, &ib, &pot, &settle, None).unwrap();
        let ch_b = result_challenge_bytes(&game, &ib, &ia, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[
            marker(&game, &ia, &ib, &pot, &settle, None, sign_result(&a, &game, &ch_a), None),
            marker(&game, &ib, &ia, &pot, &settle, None, sign_result(&b, &game, &ch_b), None),
        ]);
        let gc = map.get(&game).unwrap();
        assert_eq!(gc.claims.len(), 2, "both real claims verify");
        for (me, opp) in [(&ia, &ib), (&ib, &ia)] {
            let (o, src) =
                derive_outcome(Some(PotVerdict::WinnerA), me, opp, Some(&settle), Some(gc));
            assert_eq!((o, src), (Outcome::Unresolved, None));
        }
    }

    #[test]
    fn sig_over_a_different_challenge_never_corroborates() {
        // A REAL signature, but the marker's fields disagree with what was
        // signed (here: a different settle txid) — the reconstructed
        // challenge differs, the sig fails, the claim contributes nothing.
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot) = (tx(0x01), tx(0x02));
        let signed_settle = tx(0x03);
        let named_settle = tx(0x33);
        let challenge =
            result_challenge_bytes(&game, &w, &l, &pot, &signed_settle, None).unwrap();
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &named_settle, None,
            sign_result(&winner_w, &game, &challenge),
            None,
        )]);
        assert!(!map.contains_key(&game));
    }

    #[test]
    fn v2_cards_are_bound_by_the_signatures() {
        // v2: the sigs bind the canonical cards. A marker whose cardsHex was
        // tampered (or garbled) after signing must contribute nothing; the
        // untampered claim verifies (including a non-canonical but
        // set-identical cards encoding — client parity: both sides
        // canonicalize before challenge reconstruction).
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let cards = "0001020304"; // ordinals 0..4, canonical
        let challenge =
            result_challenge_bytes(&game, &w, &l, &pot, &settle, Some(cards)).unwrap();
        let w_sig = sign_result(&winner_w, &game, &challenge);

        // Untampered → verifies.
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &settle, Some(cards), w_sig.clone(), None,
        )]);
        assert_eq!(map.get(&game).unwrap().claims.len(), 1);

        // Unsorted-but-identical set → same canonical challenge → verifies.
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &settle, Some("0403020100"), w_sig.clone(), None,
        )]);
        assert_eq!(map.get(&game).unwrap().claims.len(), 1);

        // Tampered hand → dropped.
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &settle, Some("0001020305"), w_sig.clone(), None,
        )]);
        assert!(!map.contains_key(&game));

        // Malformed cards (duplicate ordinal) → unverifiable → dropped.
        let map = claims_by_game(&[marker(
            &game, &w, &l, &pot, &settle, Some("0000010203"), w_sig, None,
        )]);
        assert!(!map.contains_key(&game));
    }

    #[test]
    fn self_paired_and_case_variant_markers_are_handled() {
        // winner === loser is invalid regardless of signatures (client
        // parity), and an upper-cased marker row still verifies (all
        // challenge fields are lowercased before reconstruction).
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let ch_self = result_challenge_bytes(&game, &w, &w, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[marker(
            &game, &w, &w, &pot, &settle, None,
            sign_result(&winner_w, &game, &ch_self),
            None,
        )]);
        assert!(!map.contains_key(&game), "self-paired claim never counts");

        let challenge = result_challenge_bytes(&game, &w, &l, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[marker(
            &game.to_ascii_uppercase(),
            &w.to_ascii_uppercase(),
            &l.to_ascii_uppercase(),
            &pot.to_ascii_uppercase(),
            &settle.to_ascii_uppercase(),
            None,
            sign_result(&winner_w, &game, &challenge),
            None,
        )]);
        let gc = map.get(&game).unwrap();
        assert_eq!(gc.claims.len(), 1);
        assert_eq!(gc.claims[0].settle_txid, settle);
    }

    // ── hand-score exposure (bsv-low #245) ─────────────────────────────

    #[test]
    fn hand_score_matches_frozen_oracle_vectors() {
        // (cardsHex, expected low-sum) — cross-checked against
        // oracle/eval5_lowsum.py, spanning aces (Ace=1) and face cards
        // (T/J/Q/K=10). Ordinal = 13*suit + rank, rank 0='2'..8='T', 9='J',
        // 10='Q', 11='K', 12='A' (low-core `card_from_ordinal`).
        let vectors = [
            ("0c19263300", 6),  // A A A A 2  (min_quad_ace)
            ("000102030c", 15), // A 2 3 4 5  (ace_low_wheel)
            ("0001020304", 20), // 2 3 4 5 6  (run_two_to_six)
            ("0c08090a0b", 41), // A T J Q K  (ace_and_faces)
            ("09160a170b", 50), // J J Q Q K  (all_face_cards)
            ("0b1825320a", 50), // K K K K Q  (max_quad_king)
        ];
        for (hexs, want) in vectors {
            let cards = crate::logic::leaderboard_cards_from_hex(hexs)
                .unwrap_or_else(|| panic!("vector {hexs} must parse"));
            assert_eq!(crate::logic::hand_score(&cards), want, "sum for {hexs}");
            // The exposure helper agrees end-to-end (parse → score → canonical).
            let h = winner_hand_from("02aa", hexs, false).unwrap();
            assert_eq!(h.score, want);
        }
    }

    #[test]
    fn resolve_winner_hand_exposes_winner_only_and_is_viewer_independent() {
        // Real signed v2 claim through the REAL producer (`claims_by_game`):
        // a winner verdict + the unanimous verified winner claim surfaces the
        // WINNER's five cards + low-sum — identically for either viewer.
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let cards = "000102030c"; // A-2-3-4-5 wheel = 15
        let ch = result_challenge_bytes(&game, &w, &l, &pot, &settle, Some(cards)).unwrap();
        let map =
            claims_by_game(&[marker(&game, &w, &l, &pot, &settle, Some(cards),
                sign_result(&winner_w, &game, &ch), None)]);
        let gc = map.get(&game);

        let hand =
            resolve_winner_hand(Some(PotVerdict::WinnerA), &w, &l, Some(&settle), gc).unwrap();
        assert_eq!(hand.identity, w);
        assert_eq!(hand.cards_hex, "000102030c");
        assert_eq!(hand.score, 15);
        assert!(!hand.is_tie);
        // The loser sees the SAME winner hand (a per-game chain+claim fact).
        let from_loser =
            resolve_winner_hand(Some(PotVerdict::WinnerA), &l, &w, Some(&settle), gc).unwrap();
        assert_eq!(from_loser, hand);

        // A refund / no verdict never exposes a hand (no showdown).
        assert!(resolve_winner_hand(Some(PotVerdict::Refund), &w, &l, Some(&settle), gc).is_none());
        assert!(resolve_winner_hand(None, &w, &l, Some(&settle), gc).is_none());
    }

    #[test]
    fn resolve_winner_hand_null_when_cards_absent_or_winner_unresolved() {
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));

        // A v1 (no-cards) claim: the winner resolves, but no hand is on-chain
        // → None (never a fabricated hand).
        let ch = result_challenge_bytes(&game, &w, &l, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[marker(&game, &w, &l, &pot, &settle, None,
            sign_result(&winner_w, &game, &ch), None)]);
        assert!(resolve_winner_hand(Some(PotVerdict::WinnerA), &w, &l, Some(&settle),
            map.get(&game)).is_none());

        // Both parties publish REAL claims-with-cards for the same settle:
        // winner unanimity fails → no attributable hand.
        let cw = "000102030c";
        let cl = "0001020304";
        let chw = result_challenge_bytes(&game, &w, &l, &pot, &settle, Some(cw)).unwrap();
        let chl = result_challenge_bytes(&game, &l, &w, &pot, &settle, Some(cl)).unwrap();
        let map = claims_by_game(&[
            marker(&game, &w, &l, &pot, &settle, Some(cw), sign_result(&winner_w, &game, &chw), None),
            marker(&game, &l, &w, &pot, &settle, Some(cl), sign_result(&loser_w, &game, &chl), None),
        ]);
        assert!(resolve_winner_hand(Some(PotVerdict::WinnerA), &w, &l, Some(&settle),
            map.get(&game)).is_none());
    }

    #[test]
    fn resolve_winner_hand_tie_exposes_one_provable_equal_sum_side() {
        // A TIE verdict is seat-symmetric; only ONE hand is ever on-chain, so
        // we expose that provable (equal-sum) side, flagged `is_tie`.
        let a_w = wallet_of(0x11);
        let b_w = wallet_of(0x22);
        let (a, b) = (identity_of(&a_w), identity_of(&b_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let cards = "000102030c"; // 15
        let ch = result_challenge_bytes(&game, &a, &b, &pot, &settle, Some(cards)).unwrap();
        let map = claims_by_game(&[marker(&game, &a, &b, &pot, &settle, Some(cards),
            sign_result(&a_w, &game, &ch), None)]);
        let hand =
            resolve_winner_hand(Some(PotVerdict::Tie), &a, &b, Some(&settle), map.get(&game))
                .unwrap();
        assert!(hand.is_tie);
        assert_eq!(hand.score, 15);
        assert_eq!(hand.identity, a);
    }

    // ── param-push / script-number hygiene ─────────────────────────────

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

    #[test]
    fn results_and_claims_sql_are_bounded() {
        // The results query is single-bind and bounded (the over-50-outpoint
        // 503 lesson: bound every D1 statement).
        let sql = results_sql();
        assert_eq!(sql.matches('?').count(), 1);
        assert!(sql.contains(&format!("LIMIT {RESULTS_MAX_ROWS}")));
        assert!(sql.contains("LEFT JOIN pot_beefs fb ON fb.txid = lower(pp.potTxid)"));
        assert!(sql.contains("LEFT JOIN pot_beefs sb ON sb.txid = lower(r.spendingTxid)"));
        // Claims chunks bind one param per gameId.
        assert_eq!(claims_sql(3).matches('?').count(), 3);
        assert_eq!(
            claims_sql(crate::logic::D1_CHUNK_OUTPOINTS)
                .matches('?')
                .count(),
            crate::logic::D1_CHUNK_OUTPOINTS
        );
    }
}
