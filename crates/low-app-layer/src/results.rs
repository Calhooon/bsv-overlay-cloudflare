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
//!   `outcome` upgrades to `won`/`lost` only when every on-record
//!   `tm_result` claim for the game agrees on one winner among the two
//!   parties AND names the chain-classified settle — claim-corroborated
//!   chain truth, never a bare claim (`outcomeSource` says which).

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
}

/// A game's `tm_result` claims relevant to won/lost attribution: every
/// distinct claimed winner over markers naming `settle_txid` as the settle.
/// (Claims are BYTE-FORMAT admitted and NOT signature-verified server-side —
/// the same trust posture as `/leaderboard`; that is why a claim can only
/// CORROBORATE a chain-classified winner spend, never create one, and why
/// conflicting claims yield `unresolved`.)
#[derive(Debug, Clone, Default)]
pub struct GameClaims {
    /// Lowercased distinct (winner identity, settle txid, both_sigs) tuples.
    pub claims: Vec<(String, String, bool)>,
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
    /// `"chain+claim"` (winner verdict + unanimous claims), `None` for
    /// `unresolved`.
    pub outcome_source: Option<&'static str>,
    /// The settle's mined block height per its BEEF BUMP, when proven.
    pub at_height: Option<u64>,
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

/// Map a chain verdict (+ claims) to the identity's outcome.
///
/// - `tie` / `refund` are seat-symmetric → pure chain truth.
/// - a winner verdict upgrades to won/lost ONLY when every on-record claim
///   that names the classified settle txid agrees on ONE winner and that
///   winner is one of the two parties (the caller or its opponent). No
///   claims, conflicting claims, or a claimed winner outside the pair →
///   `unresolved` (the chain alone cannot name the seat's identity — module
///   note).
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
            // Distinct claimed winners naming THIS settle, both sigs present.
            let mut winners: Vec<&str> = gc
                .claims
                .iter()
                .filter(|(_, s, both)| *both && s.eq_ignore_ascii_case(settle))
                .map(|(w, _, _)| w.as_str())
                .collect();
            winners.sort_unstable();
            winners.dedup();
            match winners.as_slice() {
                [w] if w.eq_ignore_ascii_case(identity_lc) => (Outcome::Won, Some("chain+claim")),
                [w] if w.eq_ignore_ascii_case(opponent_lc) => (Outcome::Lost, Some("chain+claim")),
                _ => (Outcome::Unresolved, None),
            }
        }
        None => (Outcome::Unresolved, None),
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
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for r in rows {
        let key = (
            r.game_id.to_ascii_lowercase(),
            r.pot_txid.to_ascii_lowercase(),
            r.pot_vout,
        );
        if !seen.insert(key) {
            continue; // duplicate marker rows (garbage coexists by design)
        }
        let settle_lc = r.spending_txid.as_ref().map(|s| s.to_ascii_lowercase());
        let mut verdict = None;
        let mut at_height = None;
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
                }
                at_height = beef_block_height(&sb, settle);
            }
        }
        let game_lc = r.game_id.to_ascii_lowercase();
        let opponent_lc = r.opponent_identity.to_ascii_lowercase();
        let (outcome, outcome_source) = derive_outcome(
            verdict,
            identity_lc,
            &opponent_lc,
            settle_lc.as_deref(),
            claims_by_game.get(&game_lc),
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
        });
    }
    out
}

/// Build the claims-by-game map from raw `result_markers_v2` rows.
pub fn claims_by_game(markers: &[ResultMarkerRow]) -> std::collections::HashMap<String, GameClaims> {
    let mut map: std::collections::HashMap<String, GameClaims> = std::collections::HashMap::new();
    for m in markers {
        map.entry(m.game_id.to_ascii_lowercase())
            .or_default()
            .claims
            .push((
                m.winner.to_ascii_lowercase(),
                m.settle_txid.to_ascii_lowercase(),
                m.loser_sig_hex.is_some(),
            ));
    }
    map
}

/// Assemble the `/results` wire body:
/// `{"identity","results":[{gameId,potTxid,potVout,recoveryHeight,
/// opponentIdentity,settleTxid,spent,spentConfirmed,verdict,outcome,
/// outcomeSource,at}]}`. `at` is `{"height": <n|null>}` (block height when
/// the settle's BEEF carries a verified BUMP; time is not tracked).
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

    fn claims_of(entries: &[(&str, &str, bool)]) -> GameClaims {
        GameClaims {
            claims: entries
                .iter()
                .map(|(w, s, both)| (w.to_string(), s.to_string(), *both))
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

        // A unanimous both-sig claim naming ME for THIS settle → won.
        let gc = claims_of(&[(&me, &settle, true)]);
        let (o, src) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));

        // The same claim from the OPPONENT's perspective → lost.
        let (o, src) =
            derive_outcome(Some(PotVerdict::WinnerA), &opp, &me, Some(&settle), Some(&gc));
        assert_eq!((o, src), (Outcome::Lost, Some("chain+claim")));

        // Conflicting claims (both parties claim the same settle) → nobody.
        let gc = claims_of(&[(&me, &settle, true), (&opp, &settle, true)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // A single-sig (unconfirmed) claim never upgrades.
        let gc = claims_of(&[(&me, &settle, false)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // A claim naming a DIFFERENT settle never corroborates this one.
        let gc = claims_of(&[(&me, &tx(0x33), true)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // A claimed winner OUTSIDE the two parties → unresolved (a foreign
        // marker can't award this pot to anyone).
        let gc = claims_of(&[(&ident(0xcc), &settle, true)]);
        let (o, _) =
            derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);

        // No verdict at all → unresolved even with a pretty claim (a claim
        // alone NEVER makes a server-derived result — the owner directive).
        let gc = claims_of(&[(&me, &settle, true)]);
        let (o, _) = derive_outcome(None, &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);
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
    }

    #[test]
    fn claims_map_carries_settle_binding_and_sig_presence() {
        let m = ResultMarkerRow {
            game_id: tx(0x01).to_ascii_uppercase(), // case-normalized
            winner: ident(0xaa),
            loser: ident(0xbb),
            pot_txid: tx(0x02),
            settle_txid: tx(0x03),
            winner_sig_hex: "3045ab".into(),
            loser_sig_hex: None,
            cards_hex: None,
            txid: tx(0x04),
            created_at: Some(1),
        };
        let map = claims_by_game(&[m]);
        let gc = map.get(&tx(0x01)).unwrap();
        assert_eq!(gc.claims.len(), 1);
        assert_eq!(gc.claims[0].1, tx(0x03));
        assert!(!gc.claims[0].2, "single-sig marker is not both-signed");
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
