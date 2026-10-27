//! bsv-low #469 — THE OWED LIST: the games page as a renderer of ONE served
//! answer (`docs/DESIGN-OWED-LIST-2026-09-18.md`, the owner's rulings of
//! 2026-09-19 03:3xZ: a new route `/owed`, every step before the loop).
//!
//! A ROW is one output (or one outpoint this identity funded) the brain has
//! JUDGED, with the FAMILY that names what the player can do:
//!
//! * `payout`       — a spend of a pot this identity is a party to pays MY
//!   home (winner-a / winner-b / tie / refund) and no
//!   `collected` filing names the game. Claim: `internalize`.
//! * `refund-due`   — the pot is UNSPENT, the recovery gate is OPEN and a
//!   filed refund is `refundValid`. Claim: `present-refund`. Only when the
//!   pot's FUNDING is held (bsv-low #831: its SEEN latch, the index's proof
//!   or a checked `mined` word); else an `unbound` row, claim
//!   `funding-not-held`, no press.
//! * `hop-stranded` — my hop outpoint is unspent past the join window, or
//!   spent by a transaction that is not a LOW pot. Claim:
//!   `sweep-hop`, or the story `spent-elsewhere` (no claim).
//! * `in-progress`  — the pot is unspent and the gate is not open yet: the
//!   hand's window. Claim: `rejoin` (the #449 source).
//! * `unbound`      — "could not judge" is a SENTENCE, not silence: a spent
//!   pot whose seat binding is unknown, an unclassified
//!   spend, a gate-open pot with no valid filed refund. No
//!   claim; the reason rides the row.
//!
//! NOT a row, by construction: a decided LOSS; a payout with a `collected`
//! filing; a payout whose paying transaction the identity's own held filing
//! names (bsv-low #492: the wallet already holds it); a written-off era pot (the views' era clause); a pot the identity
//! is not a party to (the views' party window).
//!
//! The derivation is PURE over rows this crate already serves (`ResultEntry`,
//! `RefundEntry`, `HopEntry`, the valid filed refund raws, the collected
//! markers) — the families REUSE the views' own judgments at compute time
//! (`derive_outcome_with_seat`, `derive_refund_status`, `derive_hop_status`,
//! `served_recovery_height`); the rows are a SNAPSHOT of those judgments, judged
//! per game at the write (bsv-low #893): the dirty marks the source tables'
//! triggers write, drained by the judge (`owed_judge`: its hydration, its batch),
//! dated for the chain by the belt (`owed_belt`), read by `owed_read`.
//!
//! Money posture (the trust model, unchanged): the row STEERS display and the
//! affordance; the client VERIFIES what it credits (the raw against the txid,
//! the paying output against its own derived home, the BUMP against
//! chaintracks). A wrong row costs a press that fails honestly, never sats.

use crate::hops_view::{HopEntry, HopStatus, MarkerVerification};
use crate::refund_view::{RefundEntry, RefundStatus};
use crate::results::{Outcome, ResultEntry, SeatLetter};
use overlay_discovery::pot::PotVerdict;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// The wire version: additive keys only; a bump un-renders every deployed client.
pub const OWED_WIRE_VERSION: u32 = 1;

/// A hop older than this with no spend is STRANDED (the funding never joined
/// it): the join window of the hand plus a generous margin. A younger unspent
/// hop is an `in-progress` row with the felt's rejoin (2026-09-19), never a
/// sweep claim. Compared against
/// `HopEntry.marker_created_at`, which the D1 mapper converts from the
/// marker table's unix SECONDS to ms (the gate's HIGH-3).
pub const HOP_STRANDED_AFTER_MS: i64 = 30 * 60 * 1000;
/// The sentence on a young unspent hop's `in-progress` row (the stake is funded, the hand has not started).
pub const YOUNG_HOP_REASON: &str =
    "your stake is in its funding hop and the hand has not started: rejoin to continue; if it never starts, the stake can be swept back after 30 minutes";
/// Fleet loop 11 (2026-09-19): the sentence on a hop whose JOIN the network REFUSED (the pot evicted, never
/// readmitted) — the hand can never start, so the stake is sweepable NOW, not after the young-hop window: the brain
/// KNOWS (the eviction ledger), and "rejoin to continue" was a lie by omission (`spec-admit-fast-join-refused`).
pub const JOIN_REFUSED_REASON: &str =
    "the network refused the transaction that spent this stake (it was evicted from the index): the hand cannot start from it; your stake can be swept back now";

/// bsv-low #486: the sentence on a hop whose JOIN the NETWORK refused definitively at the door (the 422 arm:
/// nothing was ever admitted, so no eviction names it, and the hop read "rejoin to continue" for the whole young
/// window). The overlay's refusal ledger (`submit_refusals`) names the hop only when the hop's own key signed the
/// refused transaction.
pub const DOOR_REFUSED_REASON: &str =
    "the transaction that would have spent this stake was refused before it reached the network's index: the hand cannot start from it; your stake can be swept back now";
/// The lens fold's LOW-1 (2026-10-06): the sentence of the 400 arm (the door's interpreter refused a script). That
/// says ONE copy was refused, never that the hand cannot start: the opponent holds this seat's signed JOIN input and
/// can corrupt its own input in a copy (another txid than the good JOIN), have it refused, and so turn this seat's
/// young hop to the sweep press while the good JOIN is still broadcastable. STATED RESIDUAL: that buys an early
/// sweep press on the seat's OWN money and nothing else. The press still needs the chain rung's corroborated
/// unspent, it returns the seat's own stake (no loss, no profit to the griefer), a landed JOIN makes the hop read
/// spent and retires the refusal (`submit_refusals::retire_admitted`), and sweep versus JOIN is the race the
/// 30-minute rule already allows a withholding opponent.
/// ACCEPTED (the delta lens's LOW-5, 2026-10-06): the wait such an opponent had to sit through is zero now, so
/// the seat's hop reads "sweep now" at minute 0 instead of minute 30 while the hand is still startable: no loss.
pub const DOOR_SCRIPT_REFUSED_REASON: &str =
    "a transaction that would have spent this stake was refused at the door and nothing was broadcast: your stake can be swept back now (the sweep returns your own stake)";
/// Which door arm named the hop (the ledger's `reason`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoorRefusal {
    /// The 400 arm: the door's interpreter refused a script of one copy.
    Script,
    /// The 422 arm: the network's definitive word on bytes whose scripts verified.
    Network,
}
impl DoorRefusal {
    /// The ledger's reason is `script-refused`, or `network-rejected: <word>` (the overlay's `submit_refusals`).
    /// Anything else reads as the weaker word.
    pub fn of_reason(reason: &str) -> DoorRefusal {
        if reason.starts_with("network-rejected") {
            DoorRefusal::Network
        } else {
            DoorRefusal::Script
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            DoorRefusal::Script => "script",
            DoorRefusal::Network => "network",
        }
    }
    pub fn sentence(self) -> &'static str {
        match self {
            DoorRefusal::Script => DOOR_SCRIPT_REFUSED_REASON,
            DoorRefusal::Network => DOOR_REFUSED_REASON,
        }
    }
}
/// Rows one walk reads from the door's ledger (an identity's young unspent hops are a handful).
pub const OWED_DOOR_REFUSALS_MAX: usize = 64;
const _: () = assert!(OWED_DOOR_REFUSALS_MAX == 64);

/// How far back the ledger is read (evictions are rare: 2 in 47 admissions on beta; a hop past the young window
/// is stranded by AGE regardless, so the window only has to cover the young period, with margin).
pub const OWED_EVICTION_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

/// A `payout` row is CLAIMABLE only once the spend is confirmed (the credit
/// path's landing bar); an unconfirmed spend is a row that says so.
pub const UNCONFIRMED_PAYOUT_REASON: &str =
    "the spend is seen but not mined yet: the credit lands with the block";
/// The story on a pot whose spend the index HOLDS (the seat's own refund or settle, submitted here) but the chain has
/// not confirmed: the verdict (and any press) waits for the block — never "not classified" (fleet loop 11's wave,
/// 2026-09-19: `refundLandedVerify` read that word for 27 minutes while the refund sat seen-but-unmined between two
/// blocks, and the harness counted it as a machinery wedge). `facts.chainWait = "block"` says it by machine.
pub const SPEND_AWAITING_BLOCK_REASON: &str =
    "the pot's spend is on the network, waiting for its block: the story (and the credit) lands with it";
/// The gate's M1 (2026-09-20): the hop's spender carries a pot covenant output — a JOIN the index never held. The
/// felt's own records tell that hand's story; the owed list presses nothing and claims nothing about custody.
pub const POT_UNINDEXED_REASON: &str =
    "the hop was taken by a pot the index does not hold (a join that reached the chain around this index): nothing to press here; the table's own records tell that hand's story";
/// The gate's M2: "pays none of your homes" needs a KNOWN home; a hop-only game (no pot committed one) gets the
/// spender's pay-to addresses instead, for the device that knows its own home to match.
pub const HOME_UNKNOWN_REASON: &str =
    "the hop was spent by a transaction this list cannot match to a home (it cannot tell which committed home is yours here): if one of its pay-to addresses is your wallet's, the money is already there";
/// The gate's M3: bytes the couriers supplied prove the payout but the index holds no proof to assemble the credit.
pub const COURIER_BYTES_NO_CREDIT_REASON: &str =
    "the sats sit at your home on chain, but the index holds no proof for that transaction, so the credit cannot be assembled here: a wallet resync finds them";
/// bsv-low #485: what the brain established about ONE home output of a courier-proven sweep (the sats the sweep
/// paid to this seat's committed home). The row RETIRES only on `Proven`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeSpendWord {
    /// A transaction whose bytes consume the home output carries an unlocking script that VERIFIES against the
    /// home's own lock, executed here (`home_output_spend_proven`): the home key signed the sats away, so the wallet
    /// held them and moved on. No courier's word is in it: a courier only carried the bytes.
    Proven,
    /// The chain rung names a spender, but its bytes could not be read or do not carry the home key's signature:
    /// a pointer, nothing more. The row stands.
    Unproven,
    /// The chain rung's corroborated absence (a second provider's clean answer): the sats sit at the home.
    Unspent,
}
/// The durable latch of a `Proven` word: a `hop_chain_probes` row under this key prefix (`homeproof:<txid>.<vout>`),
/// written by this crate alone, only after the signature verified here. The table is the app layer's own probe memo;
/// `/spent-any` keys its rows `<txid>.<vout>`, so the prefix cannot collide. The overlay DELETES from the table
/// (bsv-low #484: its reorg clear and its TTL sweep of the probe memos) and spares this prefix and the cursor's by
/// name (`bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_APP_LAYER_PREFIXES`, pinned equal and executed
/// on real SQLite here: the merged lens's MEDIUM-1, where both took the latch and the retirement lasted two hours).
/// So the latch is durable: no reorg unmakes a signature and no window ages it.
pub const HOME_SPEND_LATCH_PREFIX: &str = "homeproof:";
/// PURE (bsv-low #485): does `spender_raw` consume `home_txid:home_vout` with an unlocking script that VERIFIES
/// against the P2PKH lock of `home_pkh_hex` for `sats`? The interpreter runs here
/// (`overlay_discovery::pot::p2pkh_input_signed`: `bsv_rs::script::Spend`, the engine's own walk), over the sighash
/// of the bytes given: only the home key's holder can produce a `true`, whoever carried the bytes and whether or
/// not the spender is mined. Anything malformed, oversized or unverifiable is `false`.
pub fn home_output_spend_proven(
    spender_raw: &[u8],
    home_txid: &str,
    home_vout: u32,
    home_pkh_hex: &str,
    sats: u64,
) -> bool {
    let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(spender_raw) else {
        return false;
    };
    let Some(pkh) = hex::decode(home_pkh_hex)
        .ok()
        .and_then(|b| <[u8; 20]>::try_from(b).ok())
    else {
        return false;
    };
    let Some(vin) = tx.inputs.iter().position(|inp| {
        inp.source_output_index == home_vout
            && inp
                .source_txid
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case(home_txid))
    }) else {
        return false;
    };
    overlay_discovery::pot::p2pkh_input_signed(
        &tx,
        vin,
        &overlay_discovery::pot::p2pkh_lock(&pkh),
        sats,
    )
}

/// PURE: the probe-memo target of a home output's LATCH (`read_probe_memos` and the latch write both key it through
/// `hops_view::probe_memo_key`, so the key written is the key read back: pinned on real SQLite).
pub fn home_latch_target(sweep_txid: &str, vout: u32) -> (String, u32) {
    (
        format!(
            "{HOME_SPEND_LATCH_PREFIX}{}",
            sweep_txid.to_ascii_lowercase()
        ),
        vout,
    )
}

/// PURE (bsv-low #485, the lens fold's MEDIUM-2): the first rung of the home walk. A candidate whose latch memo
/// reads spent with a named spender is `Proven` with no network ask; every other candidate is a target of the
/// chain rung, in candidate order. A memo for a key no candidate has is ignored.
pub fn latched_home_words(
    candidates: &[CourierHomeOutput],
    latch_memos: &[crate::hops_view::ProbeMemo],
) -> (HashMap<String, HomeSpendWord>, Vec<(String, u32)>) {
    let latched: HashSet<&str> = latch_memos
        .iter()
        .filter(|m| m.spent && m.spending_txid.is_some())
        .map(|m| m.outpoint.as_str())
        .collect();
    let mut words: HashMap<String, HomeSpendWord> = HashMap::new();
    let mut targets: Vec<(String, u32)> = Vec::new();
    for c in candidates {
        let (latch_txid, vout) = home_latch_target(&c.sweep_txid, c.vout);
        if latched.contains(crate::hops_view::probe_memo_key(&latch_txid, vout).as_str()) {
            words.insert(outpoint_key(&c.sweep_txid, c.vout), HomeSpendWord::Proven);
        } else {
            targets.push((c.sweep_txid.clone(), c.vout));
        }
    }
    (words, targets)
}

/// The most spenders one byte-free latch read binds: two range bounds each, under D1's 100-bound limit.
pub const HOME_LATCH_READ_CHUNK: usize = 45;
const _: () = assert!(2 * HOME_LATCH_READ_CHUNK <= crate::logic::D1_MAX_BOUND_PARAMS);

/// PURE (bsv-low L25, the courier-retire story-row flap): the byte-free read of the `homeproof:` latches of `n`
/// named spenders. The home output's vout is not known without the sweep's bytes, so the read is vout-agnostic:
/// an INDEXED RANGE of the `hop_chain_probes` PRIMARY KEY (`outpoint`) over each spender's `homeproof:<spender>.`
/// prefix, OR'd into one statement (a range per spender, `>= lo AND < hi`, uses the PK index; never a scan). The
/// columns are [`crate::hops_view::ProbeMemoRow`]'s, so the read deserializes as any probe-memo read does.
pub fn home_latch_range_read_sql(n: usize) -> String {
    let term = "(outpoint >= ? AND outpoint < ?)";
    let terms = std::iter::repeat_n(term, n.max(1))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("SELECT outpoint, probedAtMs, spent, spendingTxid, spentConfirmed FROM hop_chain_probes WHERE {terms}")
}

/// PURE: the lo/hi range bounds for each spender's `homeproof:` latch prefix, in the statement's bind order.
/// lo = `homeproof:<spender>.` (inclusive); hi = `homeproof:<spender>/` (exclusive: `/` = 0x2F is the byte after
/// `.` = 0x2E, so the range is exactly the spender's latch rows `homeproof:<spender>.<vout>`, no other prefix and
/// no other spender, since every txid is the same 64-hex length).
pub fn home_latch_range_binds(spenders: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(spenders.len() * 2);
    for sp in spenders {
        let sp = sp.to_ascii_lowercase();
        out.push(format!("{HOME_SPEND_LATCH_PREFIX}{sp}."));
        out.push(format!("{HOME_SPEND_LATCH_PREFIX}{sp}/"));
    }
    out
}

/// PURE (bsv-low L25): the named spenders carrying a durable PROVEN `homeproof:` latch, read byte-free from
/// [`home_latch_range_read_sql`]. A latch memo is PROVEN when its home output reads spent with a named spender
/// (`spent` and `spendingTxid` set) — the word [`home_word`] latched only after THIS crate verified the home key's
/// own signature over the spend (`home_output_spend_proven`), the sole writer of a `homeproof:` row. Its mere
/// presence proves "this sweep paid my committed home and my key spent it", so the hop is retired on this recompute
/// whatever this pass's courier reads did. The latch key's body is the sweep (spender) txid
/// (`homeproof:<sweepTxid>.<vout>`), matched against the named spenders.
///
/// The HOMEPROOF lens's LOW-1: the match is per vout but retires on ANY proven latch of the spender, which equals
/// Arm A's `.all()` over this identity's home outputs (`swept_home`'s `home_spent`) ONLY because a LOW sweep pays
/// this identity exactly one home output by construction (the sole output's latch is every home output). The
/// retirement arm in [`derive_owed_rows`] carries the invariant and its client-builder citation.
pub fn latched_retired_spenders(
    named_spenders: &[String],
    latch_memos: &[crate::hops_view::ProbeMemo],
) -> HashSet<String> {
    let mut out = HashSet::new();
    for sp in named_spenders {
        let sp = sp.to_ascii_lowercase();
        let prefix = format!("{HOME_SPEND_LATCH_PREFIX}{sp}.");
        if latch_memos
            .iter()
            .any(|m| m.spent && m.spending_txid.is_some() && m.outpoint.starts_with(&prefix))
        {
            out.insert(sp);
        }
    }
    out
}

/// PURE (bsv-low #485, the lens fold's MEDIUM-2), THE RETIREMENT DECISION for one home output: the chain rung's
/// probe, plus the named spender's bytes when the walk could read them. `None` = nothing established (a faulted or
/// unknown probe: the outpoint stays unnamed and the row stands). A corroborated absence is `Unspent`. A word of
/// spent is `Unproven` unless the bytes carry the home key's signature over this very output
/// (`home_output_spend_proven`); only then `Proven`, with the latch to write. The latch names the transaction the
/// PROVEN BYTES hash to (the lens's NIT: never a spender the courier named for other bytes).
pub fn home_word(
    probe: &crate::hops_view::ChainSpendProbe,
    spender_raw: Option<&[u8]>,
    home: &CourierHomeOutput,
    now_ms: i64,
) -> Option<(HomeSpendWord, Option<crate::hops_view::ProbeMemo>)> {
    if !probe.known {
        return None;
    }
    match probe.spent? {
        false => Some((HomeSpendWord::Unspent, None)),
        true => {
            let Some(raw) = spender_raw.filter(|raw| {
                home_output_spend_proven(raw, &home.sweep_txid, home.vout, &home.pkh_hex, home.sats)
            }) else {
                return Some((HomeSpendWord::Unproven, None));
            };
            let proven_txid = bsv_rs::transaction::Transaction::from_binary(raw)
                .ok()?
                .id()
                .to_ascii_lowercase();
            let (latch_txid, vout) = home_latch_target(&home.sweep_txid, home.vout);
            Some((
                HomeSpendWord::Proven,
                Some(crate::hops_view::ProbeMemo {
                    outpoint: crate::hops_view::probe_memo_key(&latch_txid, vout),
                    probed_at_ms: now_ms,
                    spent: true,
                    spending_txid: Some(proven_txid),
                    spent_confirmed: probe.spent_confirmed,
                    tip_height: None,
                }),
            ))
        }
    }
}

/// The delta fold's LOW-1 (2026-10-06): the home outputs ONE pass looks at (one memo read holds the cursor, their
/// latches and their chain memos inside D1's bind limit).
pub const OWED_HOME_WINDOW: usize = crate::logic::D1_CHUNK_OUTPOINTS;
const _: () = assert!(1 + 2 * OWED_HOME_WINDOW < crate::logic::D1_MAX_BOUND_PARAMS);

/// The `spent-elsewhere` story (design §2): the hop was spent by a transaction that is not a LOW pot and pays no
/// home of this seat — a sweep to another home, or the wallet's own spend. Nothing here can move it.
pub const SPENT_ELSEWHERE_REASON: &str =
    "the hop was spent outside this game by a transaction that pays none of your homes (your wallet's own spend, or a sweep elsewhere): if it was yours, the money is already there; nothing here can move it";

/// bsv-low #669: the sweep claim of a `hop-stranded` row was computed against a chain probe taken at an EARLIER tip
/// than the one being served (a block has arrived since, or the present tip cannot be read at all). The chain probe
/// is no longer provably current, so the claim is no longer trustworthy: the served row degrades to a `pending`
/// chain-wait (`claimable:false`, no press), never a dead sweep. A fresh probe at the new tip re-points it to the
/// claim (still unspent) or to `spent-elsewhere` (a competitor took it).
pub const SWEEP_TIP_MOVED_REASON: &str =
    "the chain moved since this was judged; re-checking the hop against the new block";
/// A missing tip cannot establish whether a hop probe is still current.
pub const SWEEP_TIP_UNKNOWN_REASON: &str =
    "the chain tip is unavailable; the hop probe's freshness is unknown";

/// The families, in the order the page lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum OwedFamily {
    Payout,
    RefundDue,
    HopStranded,
    InProgress,
    Unbound,
}

impl OwedFamily {
    pub const ALL: [OwedFamily; 5] = [
        OwedFamily::Payout,
        OwedFamily::RefundDue,
        OwedFamily::HopStranded,
        OwedFamily::InProgress,
        OwedFamily::Unbound,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            OwedFamily::Payout => "payout",
            OwedFamily::RefundDue => "refund-due",
            OwedFamily::HopStranded => "hop-stranded",
            OwedFamily::InProgress => "in-progress",
            OwedFamily::Unbound => "unbound",
        }
    }
    pub fn parse(s: &str) -> Option<OwedFamily> {
        OwedFamily::ALL.into_iter().find(|f| f.as_str() == s)
    }
    pub fn index(self) -> usize {
        OwedFamily::ALL.iter().position(|f| *f == self).unwrap_or(0)
    }
}

/// One owed row, pre-JSON and pre-D1.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwedRow {
    pub identity: String,
    /// `txid:vout`, lowercase — the PK with the identity.
    pub outpoint: String,
    pub family: OwedFamily,
    pub game_id: String,
    /// What the network assigned (or pre-signed) to MY home, in sats; `None`
    /// where the brain could not measure it (a byteless legacy spend, an
    /// unknown seat) — never a guess.
    pub sats: Option<u64>,
    pub opponent_identity: Option<String>,
    pub at_height: Option<u64>,
    /// The proof pointers + the witness + the narration facts, as the page
    /// renders them (additive by construction; a column is added only for a
    /// query key).
    pub facts: Value,
    /// The sentence for an `unbound` row (and a `hop-stranded` story).
    pub reason: Option<String>,
}

/// The CHAIN rung's word on an index-unspent hop (the memoised `/spent-any` probe, or a fresh one this recompute
/// bought within its budget): the sweep claim rests on the index AND the chain, the shipped client's own bar
/// (`hopSpenderRead.looked`). `looked = false` = the providers could not answer (a fault, no corroboration).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopChainWord {
    pub looked: bool,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    /// The spender's confirmation, when the rung said (a swept hop's payout is claimable only once its sweep mined).
    pub spent_confirmed: Option<bool>,
    /// The word is a memo older than the fresh window (this recompute's probe budget was spent): still the last
    /// thing the chain said, named as stale.
    pub stale: bool,
    /// The memo's age when it answered (the gate's L5, 2026-09-19): `None` for a probe made this recompute; rides the
    /// row as `chainProbeAgeMs` so a two-hour-old confirmed word is not read as this second's.
    pub age_ms: Option<i64>,
    /// bsv-low #808: the chain tip the walk had read when this word was taken (`hop_chain_probes.tipHeight`; this
    /// walk's own tip for a probe it made). `None`: a memo written before the column, or by a walk with no tip.
    pub probe_tip: Option<u64>,
    /// bsv-low #808: an UNSPENT word taken below this walk's tip ([`crate::hops_view::memo_predates_tip`]), served
    /// past the probe budget. On an index-UNKNOWN hop the derivation withholds the sweep on it (a pending chain-wait);
    /// on an index-UNSPENT hop it is served as silence is (`index-unspent`, `tipMoved` stated; the lens's MEDIUM-1).
    pub predates_tip: bool,
}

/// A VALID filed refund for one pot (`potrefund_records.refundValid = 1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidRefund {
    pub raw_hex: String,
    /// The refund's output to MY pay home, in sats, when the raw parses and
    /// my seat is known.
    pub my_sats: Option<u64>,
}

/// One output of a hop's NON-POT spender as the index's stored bytes show it (a `tm_lowfund`-admitted transaction:
/// a sweep, a wallet's own spend): the P2PKH pkh when the output is one, its sats, and the index's own spend word
/// for that output (the swept sats collected and moved on, or still sitting at the home).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpenderOutput {
    pub vout: u32,
    pub pkh_hex: Option<String>,
    pub sats: u64,
    pub spent: Option<bool>,
    /// The output is a LOW pot covenant lock (the gate's M1, 2026-09-20): the spender is a JOIN the index does not
    /// hold (refused at the door and broadcast around it, or wrongly evicted) — never a custody story.
    pub pot_lock: bool,
}

/// True when a parsed spender's bytes do NOT spend this hop: the pointer that named it (the index's, a courier's,
/// a planted row's) is refuted and the hop is judged as if the bytes were never read ("could not judge"), never a
/// payout or a custody story off a wrong pointer. A spender whose inputs were not recorded is not refuted.
fn pointer_refuted(i: &OwedInputs, spender: &str, h: &HopEntry) -> bool {
    i.spender_inputs.get(spender).is_some_and(|ins| {
        !ins.iter()
            .any(|(t, v)| *v == h.hop_vout && t.eq_ignore_ascii_case(&h.hop_txid))
    })
}

/// A hop spent by its OWN seat's sweep — the FILED sweep, or a spender whose stored bytes pay the seat's committed
/// pay home: the stake is at the home, uncollected, a `payout` row (`source` names which proof).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweptHome {
    pub sweep_txid: String,
    pub raw_hex: Option<String>,
    pub pays_sats: Option<u64>,
    pub confirmed: bool,
    /// #517: `confirmed` came from the INDEX's own verified proof of the filed sweep (the strongest word there is).
    pub index_proven: bool,
    pub index_proof_height: Option<u64>,
    pub source: &'static str,
    /// The index shows the home output itself SPENT: collected and moved on — not a row.
    pub output_spent: bool,
    /// The sweep's outputs to the seat's home (vout, sats), when its bytes were read and the home is known.
    pub home_outputs: Vec<(u32, u64)>,
    /// The COURIER-RUNG lens fold (L2): a courier's word alone said the sweep is confirmed (WoC's `/spent` status,
    /// BananaBlocks' `spendScope`), which presses nothing: `confirmed` is false on a filed or index-bytes sweep and the
    /// row serves this as its `spenderPresence` hint. Always false where `confirmed` holds.
    pub courier_confirmed: bool,
}

/// Everything one subject's judgement gathered (every one a served-view row).
pub struct OwedInputs<'a> {
    /// Production always supplies tracker projections. None is retained for
    /// the pre-tracker body compatibility oracle, never the production path.
    pub spender_words: Option<&'a std::collections::BTreeMap<String, Value>>,
    pub identity_lc: &'a str,
    pub tip: Option<u64>,
    pub now_ms: i64,
    pub results: &'a [ResultEntry],
    pub refunds: &'a [RefundEntry],
    pub hops: &'a [HopEntry],
    /// pot outpoint (`txid:vout`, lowercase) → the valid filed refund.
    pub valid_refunds: &'a HashMap<String, ValidRefund>,
    /// game ids (lowercase) whose `collected` marker's signature VERIFIED under this identity (the gate's HIGH-1:
    /// only a row the identity itself signed retires a payout; presence is display provenance).
    pub collected_verified: &'a HashSet<String>,
    /// game ids (lowercase) with ANY `collected` row naming this identity (byte-format admission: plantable; never a gate).
    pub collected_present: &'a HashSet<String>,
    /// bsv-low #492 / #512: the payouts this identity's WALLET ALREADY HOLDS, by its own signed word: one
    /// [`held_key`] per (game, paying txid) a held filing (`LOW/collected/v2`, `record_post`) of this identity
    /// names ([`CollectedFold::held`]: the door verified its signature and wrote the txid). The app layer cannot
    /// see a wallet; the device can (its wallet owns the paying output the live credit landed, or answered a
    /// press as already known), and it says so with no press and no credit. A payout row whose paying
    /// transaction is named here is not a row. Unlike the v1 marker this names the TRANSACTION, so it retires
    /// exactly one row whatever the game's candidate count (N10), and it stops applying if the pot's spend is
    /// ever replaced by another transaction.
    pub held_verified: &'a HashSet<String>,
    /// hop spender txids (lowercase) that ARE LOW pots (`pot_records` holds them).
    pub pot_spenders: &'a HashSet<String>,
    /// True when the pot-spenders read FAULTED: a spent hop is then "could not check", never a story.
    pub pot_spenders_faulted: bool,
    /// hop outpoint (`txid:vout`) → the chain rung's word, for the index-unspent hops past the stranded window.
    pub hop_chain: &'a HashMap<String, HopChainWord>,
    /// hop outpoints (`txid:vout`, lowercase) whose spend pointer an EVICTED, never readmitted JOIN released
    /// (`released_hop_outpoints`): the network refused the hand's funding — the hop is stranded at once.
    pub evicted_hop_outpoints: &'a HashSet<String>,
    /// bsv-low #486: hop outpoints (`txid:vout`, lowercase) of THIS identity that a synchronously refused JOIN
    /// would have spent, as the overlay's refusal ledger names them (only a hop whose own key signed the refused
    /// transaction is ever recorded), with the door arm that named each (`door_refused_hops`): stranded at once,
    /// like an evicted JOIN's.
    pub door_refused_hop_outpoints: &'a HashMap<String, DoorRefusal>,
    /// hop outpoint (`txid:vout`) → the newest FILED sweep of this identity for that hop (bsv-low #469 decision 3):
    /// the press's bytes for a stranded hop; a hop spent by that very sweep is a `payout` row (the sweep's credit).
    pub hop_sweeps: &'a HashMap<String, crate::hopsweep::FiledHopSweep>,
    /// pot txids (lowercase) the overlay EVICTED and never readmitted (`pot_evictions`): the JOIN the network refused
    /// never formed a pot — its results entry is not a row; the seat's hop carries the money's story.
    pub evicted_pots: &'a HashSet<String>,
    /// non-pot spender txid (lowercase) → its outputs as the index's stored bytes show them (bounded per recompute).
    pub spender_outputs: &'a HashMap<String, Vec<SpenderOutput>>,
    /// Every parsed spender's INPUT outpoints (index-held or courier-supplied): a story stands only for a hop the
    /// transaction consumes — a pointer (the index's, a courier's, a planted row's) the bytes refute is judged as
    /// bytes never read, per hop (the delta-verify's NEW-4 and its round-2 asymmetry).
    pub spender_inputs: &'a HashMap<String, Vec<(String, u32)>>,
    /// The spenders whose bytes came from the COURIERS (the tx-any resolver), not the index's stored BEEF (the gate's
    /// M3): a payout they prove is real but has no index proof to assemble a credit from.
    pub courier_spenders: &'a HashSet<String>,
    /// game id (lowercase) → my committed pay pkh (hex) from the results entries (the covenant's own commitment): the
    /// home an unfiled sweep must pay to be MY payout.
    pub my_pkh_by_game: &'a HashMap<String, String>,
    /// bsv-low #485: home outpoint (`<sweepTxid>:<vout>`, lowercase) → what the brain established about its spend
    /// (the route's bounded walk over `courier_home_outputs`). Absent = not looked this pass.
    pub home_spends: &'a HashMap<String, HomeSpendWord>,
    /// bsv-low L25 (the courier-retire story-row flap): the hops' named spenders (lowercase) carrying a durable
    /// PROVEN `homeproof:` latch, read BYTE-FREE from `hop_chain_probes` ([`latched_retired_spenders`]). A spender
    /// here retires its hop on this recompute even when this pass's courier cap WITHHELD the sweep's bytes
    /// (`swept_home` is then `None` and Arm A cannot run): the latch is an earlier pass's PROVEN verdict that MY
    /// key spent the sweep's home output, the same retirement Arm A's `output_spent` makes with the bytes in hand.
    /// Consulted in [`derive_owed_rows`] only on the byte-free path (`swept_home` `None`), so the bytes-in-hand
    /// answer is unchanged. Empty when no latch was read or the read faulted (then the pass judges as before).
    pub retired_spenders: &'a HashSet<String>,
    /// bsv-low #692 (RC-L34-OWED-COURIER-RETIRE): non-pot spender txids (lowercase) for which THIS recompute holds
    /// a CHAINTRACKS-CHECKED mined proof — the `/tx-any` answer's `confirmed == Some(true)` with a proven `height`
    /// (the index BUMP's, or the external leg's `/proof/tsc` bound through [`crate::txany::mined_proof_matches`],
    /// `caller=owed`). A courier's bare `claimedHeight` is NEVER here ([`crate::txany::proof_mined_word`]). A swept
    /// payout's `confirmed` reads it BESIDE the spent-any ladder's `spent_confirmed` ([`non_pot_spender`]), so a
    /// hop's mined-confirmation gains a fallback when the ladder's only confirming courier (WoC `/0/spent`) faults
    /// and the fallback (BananaBlocks) cannot carry the confirmation. The proof stays chaintracks-bound; absence
    /// stays "could not look" (the set is simply empty for that spender). Empty when no spender was proof-mined
    /// this pass.
    pub proof_mined_spenders: &'a HashSet<String>,
    /// bsv-low #831: pot txids (lowercase) of UNSPENT pots whose FUNDING the hydration read found held by nothing:
    /// no `network_seen` latch on the pot's own txid and no verified index proof (`pot_beefs.proof_verified`) of it
    /// ([`FUNDING_HELD_NONE`], `owed_judge::rank::hydrate_party_pair_sql`'s `fundingHeld`). A refund-due pot named here
    /// serves no press unless the tracker's CHECKED `mined` word for the funding is in `spender_words`
    /// ([`checked_tracker_mined`]): its pre-signed refund spends an output the network does not hold, so ARC answers
    /// it `SEEN_IN_ORPHAN_MEMPOOL`. Absence from the set is today's judgement (a seen or proven funding, or a pot whose
    /// funding was not read: a spent pot, which is never refund-due). A word-store fault leaves the set's pots withheld
    /// for that pass (the strict rule, latch or proof), never pressed on an unread word.
    pub funding_unheld: &'a HashSet<String>,
    /// bsv-low #837 (FIX-837), THE BELT: index-UNSPENT hop outpoints (`txid:vout`, lowercase) whose own pot's stored
    /// JOIN spends them, mapped to that pot's txid (lowercase). The route reads it before any hop is called stranded
    /// ([`JOIN_SEAT_POTS_SQL`] for the hop's seat, then the pot's stored bytes, [`join_spends_outpoint`]): the index
    /// held the pot but never recorded the hop's spend (a JOIN admitted by a road with no `tm_lowfund` judgement, the
    /// overlay's `record_join_hop_spends` not yet run). Such a hop is no sweep: the chain spent it into the pot.
    pub join_spent_hops: &'a HashMap<String, String>,
}

/// bsv-low #837: the seat pots of one index-unspent hop of this identity (`?1` identity, `?2` hop txid, `?3` vout):
/// the pots the hop marker's seat key is committed in (`pot_hop_seats`, the seat-key derivation the links use), held
/// in the index (`pot_records`, the covenant row at the seat's vout). At most four; one statement per hop.
pub const JOIN_SEAT_POTS_SQL: &str =
    "SELECT s.potTxid AS potTxid FROM hopparty_records hp INDEXED BY idx_hopparty_hop \
     CROSS JOIN pot_hop_seats s ON s.markerRowid = hp.rowid \
     CROSS JOIN pot_records pot ON pot.txid = s.potTxid AND pot.outputIndex = s.potVout \
     WHERE hp.txid = ?2 AND hp.hopVout = ?3 AND hp.identity = ?1 LIMIT 4";
/// bsv-low #837: the pot's own stored bytes, read by the belt (no trust gate: only the JOIN's input list is read).
pub const JOIN_POT_BEEF_SQL: &str = "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?1";

/// PURE (bsv-low #837): does the transaction `pot_txid`'s stored `beef` names (its atomic subject, else the one whose
/// txid is `pot_txid`) spend `hop_txid:hop_vout`? `false` for bytes that do not parse or name another transaction.
/// The bytes are read as every production read of stored bytes in the app layer reads them, under
/// `APP_BEEF_LIMITS` (the FIX-837 lens's LOW-1): a body the limits refuse is no word.
pub fn join_spends_outpoint(beef: &[u8], pot_txid: &str, hop_txid: &str, hop_vout: u32) -> bool {
    let Ok(parsed) = overlay_engine::beef_limits::parse_beef(
        beef,
        &overlay_engine::beef_limits::APP_BEEF_LIMITS,
    ) else {
        return false;
    };
    let Some(join) = parsed
        .find_txid(&pot_txid.to_ascii_lowercase())
        .and_then(|t| t.tx())
    else {
        return false;
    };
    join.inputs.iter().any(|input| {
        input.source_output_index == hop_vout
            && input
                .get_source_txid()
                .is_ok_and(|t| t.eq_ignore_ascii_case(hop_txid))
    })
}

/// bsv-low #837: the sentence on a hop whose spender is a HELD pot this identity's results do not carry (the index has
/// not linked the seat to the pot yet: its hop spend unrecorded, or recorded this moment). The game keeps a row; it is
/// no press. The pot's own row (its refund or payout) is served once the link lands.
pub const JOINED_POT_UNLINKED_REASON: &str =
    "your stake went into this game's pot (the pot's funding transaction spent this hop on chain): nothing to sweep here; the pot's own row appears once the index links your seat to it";

/// PURE (bsv-low #837): is `pot_txid` a pot this pass's results carry (and the index holds)? Only then does "the pot's
/// own row tells the story" hold for a hop the JOIN took; otherwise the hop keeps a row for its game.
fn pot_row_served(i: &OwedInputs, pot_txid: &str) -> bool {
    !i.evicted_pots.contains(&pot_txid.to_ascii_lowercase())
        && i.results
            .iter()
            .any(|e| e.pot_txid.eq_ignore_ascii_case(pot_txid))
}

/// bsv-low #837: the row of a hop a HELD pot spent while the results carry no row for that pot (never a press).
fn joined_pot_unlinked_row(
    me: &str,
    outpoint: String,
    game: String,
    h: &HopEntry,
    facts_base: &Value,
    pot_txid: &str,
    by: &str,
) -> OwedRow {
    let mut facts = facts_base.clone();
    facts["claim"] = Value::Null;
    facts["claimable"] = json!(false);
    facts["joinSpent"] = json!(pot_txid.to_ascii_lowercase());
    facts["joinSpentBy"] = json!(by);
    OwedRow {
        identity: me.to_string(),
        outpoint,
        family: OwedFamily::Unbound,
        game_id: game,
        sats: None,
        opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
        at_height: None,
        facts,
        reason: Some(JOINED_POT_UNLINKED_REASON.to_string()),
    }
}

/// bsv-low #831: the hydration's `fundingHeld` value for an unspent pot whose funding has no SEEN latch and no index
/// proof (the others: `seen`, `proof`; NULL for a spent pot, which the read does not probe).
pub const FUNDING_HELD_NONE: &str = "none";
/// bsv-low #598: the hydration's `fundingHeld` value for an unspent pot whose funding the SEEN latch alone holds (no
/// index proof): a broadcaster's word, which the network's later refusal of those very bytes outranks.
pub const FUNDING_HELD_SEEN: &str = "seen";
/// bsv-low #598: the served `fundingHeld` of a refund-due pot whose latched funding the network then REFUSED (the
/// terminal mark of the pot's own txid, or a chain-tier `rejected` or `conflicted` word: [`funding_refusal_word`]):
/// the same judgement as
/// [`FUNDING_HELD_NONE`], named apart.
pub const FUNDING_HELD_REFUSED: &str = "refused";
/// PURE (bsv-low #598): the network's refusal of `txid` as the tracker's RECHECKED view states it. Two producers:
/// (1) the TERMINAL MARK, the one this tip writes (the B-OWED-2 lens's HIGH-1): the word event of the pot's own txid
/// (`callback`, Arcade's push; `tick`, the scheduled re-ask) read Arcade's REJECTED / INVALID / MALFORMED or a double
/// spend (`tracker_host::runtime::terminal_word`), `runtime::mark_terminal` stored it in the row, and
/// `stored::Row::view` serves it as `terminalHint` on any word but `mined` (a checked proof clears it); (2) the
/// chain-tier word `rejected` or `conflicted` itself (the tier read, never a hint-tier word of the same name), which
/// only a node verdict or a competitor would produce (none is fed on this tip). Any other view, or no row, is
/// `None`: never a refusal by absence. The fail direction: a funding Arcade refused that the network still holds (a
/// readmission by a fresh accept, #513, keeps the mark) has its refund withheld until the pot's block, whose
/// checked proof clears the mark and whose checked `mined` word rescues first in the arm; the refund is not
/// pressable before that block.
pub fn funding_refusal_word(
    words: Option<&std::collections::BTreeMap<String, Value>>,
    txid: &str,
) -> Option<&'static str> {
    let w = words?.get(&txid.to_ascii_lowercase())?;
    let chain = w["tier"] == "chain";
    if chain && w["word"] == "rejected" {
        return Some("rejected");
    }
    if chain && w["word"] == "conflicted" {
        return Some("conflicted");
    }
    if w["word"] == "mined" {
        return None;
    }
    match w["terminalHint"].as_str()? {
        "rejected" => Some("rejected"),
        "conflicted" => Some("conflicted"),
        _ => Some("terminal"),
    }
}
/// PURE (bsv-low #598): of the unspent pots whose funding the SEEN latch alone holds, those whose funding the network
/// REFUSED since ([`funding_refusal_word`]): the route adds them to `funding_unheld`, so a refused JOIN's refund row
/// takes FIX-831's judgement (no press, the same word). A pot the index PROVED is never in `seen_only` (the index's
/// proof first, #736), so no word can withhold it.
pub fn refused_fundings(
    seen_only: &HashSet<String>,
    words: &std::collections::BTreeMap<String, Value>,
) -> HashSet<String> {
    seen_only
        .iter()
        .filter(|t| funding_refusal_word(Some(words), t).is_some())
        .map(|t| t.to_ascii_lowercase())
        .collect()
}
/// bsv-low #831: the claim kind of a refund-due pot whose funding the network does not hold. Not a press: the client's
/// `claimOf` knows no such claim and the `unbound` family offers none.
pub const FUNDING_NOT_HELD_CLAIM: &str = "funding-not-held";
/// bsv-low #831: the sentence on that row.
pub const FUNDING_NOT_HELD_REASON: &str =
    "the pot's funding is not held by the network: nothing can spend it until the network sees it or the pot is retired";

/// The 20-byte pkh of a standard P2PKH locking script (`76 a9 14 <20> 88 ac`), lowercase hex; `None` for any other lock.
pub fn p2pkh_pkh_hex(lock: &[u8]) -> Option<String> {
    if lock.len() == 25
        && lock[0] == 0x76
        && lock[1] == 0xa9
        && lock[2] == 0x14
        && lock[23] == 0x88
        && lock[24] == 0xac
    {
        Some(hex::encode(&lock[3..23]))
    } else {
        None
    }
}

pub fn outpoint_key(txid: &str, vout: u32) -> String {
    format!("{}:{vout}", txid.to_ascii_lowercase())
}

/// fleet loop 11, the wave's batch 3 (2026-09-20): the home the JOIN the network REFUSED committed for my seat, by
/// game — so the hop that JOIN spent is judged from its real spender's bytes (`spent-elsewhere`, a sweep home), never
/// left at "cannot tell which committed home is yours" (`HOME_UNKNOWN_REASON`).
///
/// The eviction moves the pot's `pot_records` row AND the seats' party rows into their twins, so the results view
/// holds NO entry for the refused JOIN (the delta-verify's HIGH-A) and its committed keys reach no map. The overlay's
/// decode of that lock lives on in `pot_records_evicted`; this reads it KEYED ON THE UTXO the ledger names: the
/// eviction row's `releasedSpends` names the hop the evicted JOIN spent, only this seat's key spends this seat's hop
/// (`released_hops_by_eviction`), the hop's own VERIFIED marker attests the settle key it paid, and the lock committed
/// that key as `pubA` or `pubB` — the seat, hence the home; the game is MY hop marker's. Never a results entry, never
/// a game id from a plantable party row (the review's MEDIUM-1: a stranger's evicted pot committing my public settle
/// key beside its own pay pkh, planted under my name, would have named its pkh my home).
///
/// One home per game (`BTreeMap`, deterministic); a twin that disagrees with itself, two refused JOINs of one game
/// that disagree, a lock committing my key on both seats or on neither, an unverified marker → nothing named (the
/// pre-change sentence stands; never a guess). The caller folds with `or_insert`: a live pot's word is never overwritten.
pub fn evicted_pot_homes(
    released: &[(String, String)],
    hops: &[HopEntry],
    twin_keys: &[(String, u32, crate::results::CommittedKeys)],
) -> Vec<(String, String)> {
    // the twin's word per evicted txid: every covenant row of the JOIN must agree (a JOIN funds ONE pot)
    let mut keys_by_txid: HashMap<String, Option<&crate::results::CommittedKeys>> = HashMap::new();
    for (txid, _vout, keys) in twin_keys {
        keys_by_txid
            .entry(txid.to_ascii_lowercase())
            .and_modify(|held| {
                if held.is_some_and(|h| h != keys) {
                    *held = None;
                }
            })
            .or_insert(Some(keys));
    }
    // my hops by outpoint; two entries of one outpoint that disagree on the key or the game name nothing
    let mut by_outpoint: HashMap<String, Option<&HopEntry>> = HashMap::new();
    for h in hops {
        by_outpoint
            .entry(outpoint_key(&h.hop_txid, h.hop_vout))
            .and_modify(|held| {
                if held.is_some_and(|x| {
                    !x.seat_settle_pubkey
                        .eq_ignore_ascii_case(&h.seat_settle_pubkey)
                        || !x.game_id.eq_ignore_ascii_case(&h.game_id)
                }) {
                    *held = None;
                }
            })
            .or_insert(Some(h));
    }
    let mut homes: std::collections::BTreeMap<String, Option<String>> =
        std::collections::BTreeMap::new();
    for (hop_outpoint, evicted) in released {
        let Some(Some(h)) = by_outpoint.get(hop_outpoint) else {
            continue;
        };
        if h.marker_verified != MarkerVerification::Verified {
            continue;
        }
        let Some(Some(keys)) = keys_by_txid.get(&evicted.to_ascii_lowercase()) else {
            continue;
        };
        let my_key = h.seat_settle_pubkey.to_ascii_lowercase();
        if my_key.is_empty() {
            continue;
        }
        let pkh = match (my_key == keys.pub_a, my_key == keys.pub_b) {
            (true, false) => keys.pay_pkh_a.to_ascii_lowercase(),
            (false, true) => keys.pay_pkh_b.to_ascii_lowercase(),
            _ => continue, // neither committed key is mine, or both are: no seat to name
        };
        homes
            .entry(h.game_id.to_ascii_lowercase())
            .and_modify(|held| {
                if held.as_deref().is_some_and(|x| x != pkh) {
                    *held = None;
                }
            })
            .or_insert(Some(pkh));
    }
    homes
        .into_iter()
        .filter_map(|(g, p)| p.map(|p| (g, p)))
        .collect()
}

// =============================================================================
// bsv-low #672 (2026-10-15, RC-L30-COULD-NOT-JUDGE): THE DURABLE SPENDER-BYTES STORE.
//
// THE RULE, in one place (cites #608 and the north-star program):
//   A JUDGED owed row never regresses on a courier budget cut. A hop spender's OUTPUTS (pkh/sats/pot-lock per vout)
//   and INPUT outpoints are IMMUTABLE facts of the spender's bytes, so the owed judgement PERSISTS them here at the
//   first successful hash-verified fetch (DERIVE AT WRITE TIME, the north-star program's "next step" for the owed
//   list on evidence) content-addressed by the spender txid, and READS the store BEFORE the recompute's clock on
//   every later pass. A spender the store holds is never asked of a courier again, so a budget cut (the loop-30
//   regression: a cut recompute lost a competitor's bytes and clobbered "spent outside this game" with "could not
//   judge the spender") cannot starve a hop's known confirmed spender. As a BELT for a row JUDGED before the store
//   existed (the store holds nothing for it), [`keep_withheld_elsewhere`] carries the PREVIOUS snapshot's judged word
//   forward, keyed by the hop UTXO plus the content-addressed spender txid, while the couriers fill the store in the
//   background; the word changes only on NEW evidence (a DIFFERENT spender, a reorg, the eviction ledger). This is
//   #608's "stale could-not-judge mark re-judged on the next recompute" made durable at the source instead of
//   re-derived from scratch every pass.
// =============================================================================

/// Byte-identical to overlay migration #672 (pinned by `schema.rs`). Content-addressed by the spender txid.
pub const OWED_SPENDER_BYTES_CREATE: &str =
    "CREATE TABLE IF NOT EXISTS owed_spender_bytes (txid TEXT PRIMARY KEY, bytesJson TEXT NOT NULL, createdAtMs INTEGER NOT NULL)";
/// Persist a spender's parsed bytes once. ON CONFLICT DO NOTHING: content-addressed, so a re-write is the SAME parse
/// (never `INSERT OR REPLACE` — #638's ban is for the guarded derived tables, and a REPLACE here would reset the age
/// stamp a sweep rests on). A row written once is never written again.
pub const OWED_SPENDER_BYTES_UPSERT_SQL: &str =
    "INSERT INTO owed_spender_bytes (txid, bytesJson, createdAtMs) VALUES (?1, ?2, ?3) ON CONFLICT(txid) DO NOTHING";
/// Read the store for the pass's named spenders in ONE statement (the txids as a JSON array), by the PRIMARY KEY.
pub const OWED_SPENDER_BYTES_SELECT_SQL: &str =
    "SELECT txid, bytesJson FROM owed_spender_bytes WHERE txid IN (SELECT value FROM json_each(?1))";
/// The age sweep (the existing janitor's shape, `tracker_host::stored::SWEEP`): a bounded DELETE of rows older than
/// the retention, run in the scheduled pass. `?1` the cutoff ms, `?2` the per-pass limit.
pub const OWED_SPENDER_BYTES_SWEEP_SQL: &str =
    "DELETE FROM owed_spender_bytes WHERE txid IN (SELECT txid FROM owed_spender_bytes WHERE createdAtMs < ?1 ORDER BY createdAtMs, txid LIMIT ?2)";
/// The row-cap sweep: delete the OLDEST rows beyond the count ceiling, bounded per pass. `?1` the ceiling (OFFSET),
/// `?2` the per-pass limit. A guard against a high write-rate outrunning the age sweep; the age sweep is the primary.
pub const OWED_SPENDER_BYTES_CAP_SWEEP_SQL: &str =
    "DELETE FROM owed_spender_bytes WHERE txid IN (SELECT txid FROM owed_spender_bytes ORDER BY createdAtMs DESC, txid LIMIT ?2 OFFSET ?1)";
/// Eight days, as the tracker's stranger-projection sweep (`tracker_host::stored::SWEEP`). A spender's bytes outlive
/// every honest recompute window of its hop many times over; past it an un-revisited row is dead weight.
pub const OWED_SPENDER_BYTES_RETENTION_MS: i64 = 8 * 86_400_000;
/// The store's hard row ceiling. Writes are bounded (at most the courier cap per recompute), so the age sweep bounds
/// the table under any honest load; this ceiling is the belt against an adversarial write-rate.
pub const OWED_SPENDER_BYTES_ROW_CAP: i64 = 50_000;
/// How many rows one sweep pass may delete (the tracker's `LIMIT 64` shape, larger because this table has no read
/// cost per row). Each sweep statement is bounded; a backlog drains over passes.
pub const OWED_SPENDER_BYTES_SWEEP_LIMIT: i64 = 256;
/// The per-ROW size cap: a spender whose serialized bytes exceed this is not persisted (it is re-read if asked
/// again — the honest could-not-judge word, never a wrong one). A normal competitor/sweep is well under a KiB; this
/// bounds one row against a pathological many-output transaction.
pub const OWED_SPENDER_BYTES_MAX_JSON: usize = 64 * 1024;

/// One output of a stored spender, the IMMUTABLE facts only (never the index's mutable per-output spend word): the
/// vout, the P2PKH pkh when the output is one, the sats, and whether the output is a LOW pot covenant lock.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredSpenderOutput {
    pub vout: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pkh: Option<String>,
    pub sats: u64,
    #[serde(default)]
    pub pot: bool,
}

/// A spender's bytes as the store holds them: its input outpoints and its outputs' immutable facts. Content-addressed
/// (the bytes were hash-checked against the txid before this was written), so the parse never goes stale.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredSpenderBytes {
    pub inputs: Vec<(String, u32)>,
    pub outputs: Vec<StoredSpenderOutput>,
}

impl StoredSpenderBytes {
    /// Build from the owed brain's spender outputs + input outpoints, dropping the mutable per-output spend word.
    pub fn from_parts(inputs: &[(String, u32)], outputs: &[SpenderOutput]) -> Self {
        StoredSpenderBytes {
            inputs: inputs.to_vec(),
            outputs: outputs
                .iter()
                .map(|o| StoredSpenderOutput {
                    vout: o.vout,
                    pkh: o.pkh_hex.clone(),
                    sats: o.sats,
                    pot: o.pot_lock,
                })
                .collect(),
        }
    }
    /// The owed brain's outputs, with `spent: None` — the store holds immutable facts, and the index's own spend word
    /// (a different, mutable fact) is read from `pot_records` on the phases that have the bytes already.
    pub fn to_outputs(&self) -> Vec<SpenderOutput> {
        self.outputs
            .iter()
            .map(|o| SpenderOutput {
                vout: o.vout,
                pkh_hex: o.pkh.clone(),
                sats: o.sats,
                spent: None,
                pot_lock: o.pot,
            })
            .collect()
    }
    /// The JSON this is persisted as, or `None` when it is larger than [`OWED_SPENDER_BYTES_MAX_JSON`] (not kept).
    pub fn to_json_capped(&self) -> Option<String> {
        let s = serde_json::to_string(self).ok()?;
        (s.len() <= OWED_SPENDER_BYTES_MAX_JSON).then_some(s)
    }
    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }
}

/// PURE (bsv-low #662): the spender a hop's row is judged on (the index's recorded spend, else the chain rung's
/// spent word), lowercase: the same name the route asks the spender's bytes of.
fn hop_named_spender(i: &OwedInputs, h: &HopEntry) -> Option<String> {
    if h.spent == Some(true) {
        return h.spending_txid.as_deref().map(str::to_ascii_lowercase);
    }
    i.hop_chain
        .get(&outpoint_key(&h.hop_txid, h.hop_vout))
        .filter(|w| w.looked && w.spent == Some(true))
        .and_then(|w| w.spending_txid.as_deref().map(str::to_ascii_lowercase))
}

/// Missing evidence cannot replace a row independently judged from other evidence for the same UTXO.
/// Unverified/Unknown marker sentences without a claim and older candidate faults may be replaced.
pub fn merge_markerless_faults(rows: &mut Vec<OwedRow>, faults: Vec<OwedRow>) {
    for fault in faults {
        if let Some(index) = rows.iter().position(|r| r.outpoint == fault.outpoint) {
            let replace = (matches!(
                rows[index].facts["markerVerified"].as_str(),
                Some("unknown" | "unverified")
            ) && rows[index].facts["claim"].is_null())
                || rows[index].facts["markerlessReadFault"] == true;
            if !replace {
                continue;
            }
            rows.remove(index);
        }
        rows.push(fault);
    }
}

/// Verified evidence wins over a planted marker; count discarded attestations, never accept their key.
pub fn merge_markerless_hops(hops: &mut Vec<HopEntry>, rebuilt: Vec<HopEntry>) -> usize {
    let keys: HashSet<_> = rebuilt
        .iter()
        .map(|h| outpoint_key(&h.hop_txid, h.hop_vout))
        .collect();
    let before = hops.len();
    hops.retain(|h| {
        h.marker_verified == MarkerVerification::Verified
            || !keys.contains(&outpoint_key(&h.hop_txid, h.hop_vout))
    });
    let ignored = before - hops.len();
    hops.extend(rebuilt);
    ignored
}

/// The refund's output to `pay_pkh` (hex, 20 bytes), summed — `None` when the
/// raw does not parse or the pkh is not 20 bytes.
pub fn refund_output_sats(raw_hex: &str, pay_pkh_hex: &str) -> Option<u64> {
    let raw = hex::decode(raw_hex).ok()?;
    let tx = bsv_rs::transaction::Transaction::from_binary(&raw).ok()?;
    let pkh: [u8; 20] = hex::decode(pay_pkh_hex).ok()?.try_into().ok()?;
    let lock = overlay_discovery::pot::p2pkh_lock(&pkh);
    let mut sum = 0u64;
    for o in &tx.outputs {
        if o.locking_script.to_binary() == lock {
            sum = sum.checked_add(o.satoshis?)?;
        }
    }
    Some(sum)
}

fn seat_str(s: Option<SeatLetter>) -> Option<&'static str> {
    s.map(|s| match s {
        SeatLetter::A => "A",
        SeatLetter::B => "B",
    })
}

/// My side of a measured settle: the verdict names the winner's home; a tie
/// or a refund pays both, so my SEAT picks.
fn my_settle_sats(e: &ResultEntry) -> Option<u64> {
    let settle = e.money.settle.as_ref()?;
    match (e.verdict, e.my_seat) {
        (Some(PotVerdict::WinnerA), Some(SeatLetter::A)) => settle.pay_a_sats,
        (Some(PotVerdict::WinnerB), Some(SeatLetter::B)) => settle.pay_b_sats,
        (Some(PotVerdict::Tie | PotVerdict::Refund), Some(SeatLetter::A)) => settle.pay_a_sats,
        (Some(PotVerdict::Tie | PotVerdict::Refund), Some(SeatLetter::B)) => settle.pay_b_sats,
        _ => None, // a winner verdict that contradicts my own seat proof sizes nothing (never the other home's amount)
    }
}

/// MY committed pay home in this pot's lock (the covenant's own commitment, by my proven seat), lowercase hex:
/// the home the pot's spend pays me at. `None` without a seat or without decoded keys.
fn my_committed_pay_pkh(e: &ResultEntry) -> Option<String> {
    let keys = e.committed_keys.as_ref()?;
    Some(match e.my_seat? {
        SeatLetter::A => keys.pay_pkh_a.to_ascii_lowercase(),
        SeatLetter::B => keys.pay_pkh_b.to_ascii_lowercase(),
    })
}

/// PURE (bsv-low #512, the B3 lens fold M2): THE output of a transaction that pays `pkh_hex` by a standard P2PKH,
/// when there is EXACTLY ONE (`outputs` = each output's index and its P2PKH pkh, if it is one). None or several:
/// `None`, never a pick (a device then reads the transaction itself and must own every home output).
pub fn sole_home_vout<'a>(
    outputs: impl Iterator<Item = (u32, Option<&'a str>)>,
    pkh_hex: &str,
) -> Option<u32> {
    let mut mine = outputs
        .filter(|(_, p)| p.is_some_and(|p| p.eq_ignore_ascii_case(pkh_hex)))
        .map(|(v, _)| v);
    let first = mine.next()?;
    mine.next().is_none().then_some(first)
}

/// PURE: [`sole_home_vout`] over a RAW transaction, which must hash to `txid` (the bytes are content-addressed:
/// whoever handed them over, an output layout read from bytes that hash to the paying txid is that transaction's).
pub fn sole_home_vout_of_raw(raw: &[u8], txid: &str, pkh_hex: &str) -> Option<u32> {
    let pkhs = p2pkh_outputs_of_raw(raw, txid)?;
    sole_home_vout(
        pkhs.iter()
            .enumerate()
            .map(|(v, p)| (v as u32, p.as_deref())),
        pkh_hex,
    )
}

/// PURE (bsv-low L25): the ONE P2PKH output of a RAW transaction that hashes to `txid`, as (index, pkh), when
/// there is EXACTLY ONE; zero or several: `None`, never a pick. For a filed hop sweep whose game has no committed
/// pot home (the hop's JOIN never assembled): the sweep is the identity's own signed filing and pays one output.
pub fn sole_p2pkh_output_of_raw(raw: &[u8], txid: &str) -> Option<(u32, String)> {
    let pkhs = p2pkh_outputs_of_raw(raw, txid)?;
    let mut p2pkh = pkhs
        .into_iter()
        .enumerate()
        .filter_map(|(v, p)| Some((v as u32, p?)));
    let first = p2pkh.next()?;
    p2pkh.next().is_none().then_some(first)
}

/// The one parse and txid guard of the raw readers above: each output's P2PKH pkh (if it is one), in output order,
/// of bytes that parse and hash to `txid`; `None` otherwise.
fn p2pkh_outputs_of_raw(raw: &[u8], txid: &str) -> Option<Vec<Option<String>>> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    if !tx.id().eq_ignore_ascii_case(txid) {
        return None;
    }
    Some(
        tx.outputs
            .iter()
            .map(|o| p2pkh_pkh_hex(&o.locking_script.to_binary()))
            .collect(),
    )
}

/// The served `payPkhSource` (the L25 lens's NOTE-1): where a payout row's `payPkh` was read. The identity's
/// COMMITTED home (the pot's decoded keys), or, on a hop whose game has no committed home, the FILED SWEEP's own
/// sole P2PKH output. ABSENT when `payPkh` is `null`. An additive fact: nothing compares a derivation to a
/// `filed-sweep` pkh, which is the sweep's own destination and would make such a check tautological.
pub const PAY_PKH_COMMITTED_HOME: &str = "committed-home";
pub const PAY_PKH_FILED_SWEEP: &str = "filed-sweep";

/// The served key of one (paying txid, home pkh) pair in the route's `payVout` map, lowercase.
pub fn pay_vout_key(pay_txid: &str, pkh_hex: &str) -> String {
    format!(
        "{}:{}",
        pay_txid.to_ascii_lowercase(),
        pkh_hex.to_ascii_lowercase()
    )
}

/// PURE (the B3 lens fold M2): the (paying txid, home pkh) of every derived payout row whose `payVout` the
/// derivation could not name from what it held (a pot's payout: the index stores the settle's sums per home,
/// never its output order, and the order depends on whether the spend carries a rake output). The route reads
/// those transactions' stored bytes and [`mark_pay_vouts`] writes the answer. Sorted, deduplicated.
pub fn pay_vout_wanted(rows: &[OwedRow]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = rows
        .iter()
        .filter(|r| {
            r.family == OwedFamily::Payout && r.facts.get("payVout").is_some_and(Value::is_null)
        })
        .filter_map(|r| {
            Some((
                r.facts.get("payTxid")?.as_str()?.to_ascii_lowercase(),
                r.facts.get("payPkh")?.as_str()?.to_ascii_lowercase(),
            ))
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// PURE (the B3 lens fold M2): write `payVout` on every payout row [`pay_vout_wanted`] named and `vouts`
/// ([`pay_vout_key`] to the sole home output) answers. A row it does not answer keeps `payVout: null`. Returns the
/// rows written.
pub fn mark_pay_vouts(rows: &mut [OwedRow], vouts: &HashMap<String, u32>) -> usize {
    let mut marked = 0usize;
    for r in rows.iter_mut().filter(|r| {
        r.family == OwedFamily::Payout && r.facts.get("payVout").is_some_and(Value::is_null)
    }) {
        let key = match (
            r.facts.get("payTxid").and_then(Value::as_str),
            r.facts.get("payPkh").and_then(Value::as_str),
        ) {
            (Some(t), Some(p)) => pay_vout_key(t, p),
            _ => continue,
        };
        if let Some(vout) = vouts.get(&key) {
            r.facts["payVout"] = json!(vout);
            marked += 1;
        }
    }
    marked
}

/// PURE (bsv-low #492): the HOME OUTPUTS (`payTxid`:`payVout`, lowercase txid) of the payout rows that name one: the
/// settle's (named by [`mark_pay_vouts`]) and the sweep's (named at the derivation). Sorted, deduplicated.
pub fn payout_home_outpoints(rows: &[OwedRow]) -> Vec<(String, u32)> {
    let mut out: Vec<(String, u32)> = rows
        .iter()
        .filter(|r| r.family == OwedFamily::Payout)
        .filter_map(|r| {
            let txid = r.facts.get("payTxid")?.as_str()?;
            let vout = u32::try_from(r.facts.get("payVout")?.as_u64()?).ok()?;
            crate::logic::valid_txid(txid).then(|| (txid.to_ascii_lowercase(), vout))
        })
        .collect();
    out.sort();
    out.dedup();
    out
}
/// bsv-low #492: the read of the INDEX's own spend word for `n` home outputs (one PK term per outpoint): the outputs
/// `pot_records` holds SPENT (a `tm_lowfund`-admitted P2PKH home, consumed by an admitted spend: only the home key
/// signs it). `n` is at most [`crate::logic::D1_CHUNK_OUTPOINTS`] (two binds each).
pub fn payout_home_spent_sql(n: usize) -> String {
    let terms = (0..n.max(1))
        .map(|k| format!("(txid = ?{} AND outputIndex = ?{})", 2 * k + 1, 2 * k + 2))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "SELECT lower(txid) AS txid, outputIndex FROM pot_records WHERE spent = 1 AND ({terms})"
    )
}
const _: () = assert!(2 * crate::logic::D1_CHUNK_OUTPOINTS <= crate::logic::D1_MAX_BOUND_PARAMS);
/// PURE (bsv-low #492): a payout row whose home output the INDEX holds spent is not a row: the wallet held the sats
/// (the live credit landed them) and its own key spent them on (the next stake's hop), so there is nothing to collect
/// and nothing to press, exactly as a sweep's `output_spent` retires its row and the identity's held filing retires
/// a payout. `spent` holds [`outpoint_key`]s. Returns how many rows it retired. The writers of that word (the
/// B-OWED-2 lens's LOW-3): the engine's admitted spend (`d1_discovery` `mark_spent_sql`), the discovery pass's
/// courier-found pointer bound to a content-addressed raw that consumes the outpoint, the conflict pass's mined
/// competitor (#605) and FIX-837's guarded hop write; each rests on a transaction that spends a P2PKH home, which
/// only the home key signs. The fail direction is a hidden row, the sats at the home key, never a false press; a
/// pointer still UNCONFIRMED (`spentConfirmed = 0`) that the wallet later abandons (#818's class) hides the row
/// until the pointer is released (the optional `homeSpentConfirmed` is not built).
pub fn retire_index_spent_homes(rows: &mut Vec<OwedRow>, spent: &HashSet<String>) -> usize {
    let before = rows.len();
    rows.retain(|r| {
        r.family != OwedFamily::Payout
            || !matches!(
                (
                    r.facts.get("payTxid").and_then(Value::as_str),
                    r.facts.get("payVout").and_then(Value::as_u64),
                ),
                (Some(t), Some(v)) if u32::try_from(v).is_ok_and(|v| spent.contains(&outpoint_key(t, v)))
            )
    });
    before - rows.len()
}

/// The hop's NON-POT spender named by the index (a recorded spend) or the chain rung (a word of spent), with the
/// confirmation the source carried; `None` when nothing names a spender, or the spender IS a covenant pot (the JOIN
/// took it: the pot's own row tells the story).
fn spender_mined(i: &OwedInputs, txid: Option<&str>, legacy: bool) -> bool {
    // no views (the base index arm) or no row for the spender: the legacy word
    txid.and_then(|id| checked_tracker_mined(i.spender_words, id))
        .unwrap_or(legacy)
}
/// bsv-low #736 (NS-3: the index's verified proof decides first, and no word vetoes it): [`spender_mined`] with the
/// INDEX's own proven spend read BEFORE the word. `index_proven` is `pot_records.spentConfirmed` for this spender
/// (the latch only an SPV-verified height or a chaintracks-verified proof writes), with or without a stored
/// `spentHeight`; a `stale`, `cannotCheck` or unchecked `mined` word no longer turns it into a chain wait. `legacy` is
/// what the caller passed before (a courier's claim among it), still yielding to the word as before.
fn index_proven_or_spender_mined(
    i: &OwedInputs,
    txid: Option<&str>,
    index_proven: bool,
    legacy: bool,
) -> bool {
    index_proven || spender_mined(i, txid, legacy)
}
/// bsv-low #770 (the FIX-736 lens's pre-existing note): the settle payout's confirmation, the ONE rule its two
/// sites read (the payout's `claimable` and the unclassified spend's `awaiting_block`). It is the hop arms' rule for
/// a spender the index's own pointer names ([`non_pot_spender`]'s index arm, the filed sweep the hop row names):
/// the INDEX's proof of this outpoint's spend first (`pot_records.spentConfirmed`, or the SPV-verified height
/// `at_height`, the settle's own verified bump), never vetoed by a word; else the spender's CHECKED `mined` word;
/// with no word row the index's proof alone. A courier never reaches it: the pot has no chain rung, and the latch
/// is written only by a verified proof. The filed sweep's `!contradicted` guard has no input here: when the index's
/// pointer names the payout's spender, the guard's only other name is the chain rung's (a courier naming another
/// spender), and a pot has none; a rival cannot displace a confirmed latch (the displacement CAS guards
/// `spentConfirmed = 0`). Pinned equal to the filed-sweep arm over every evidence the pass holds
/// (`fix770_a_*`).
fn settle_spend_confirmed(i: &OwedInputs, e: &ResultEntry) -> bool {
    let index_proven = e.at_height.is_some() || e.spent_confirmed == Some(true);
    index_proven_or_spender_mined(i, e.settle_txid.as_deref(), index_proven, index_proven)
}
/// PURE (bsv-low #693 fold, the W1-OWED-693 lens's MEDIUM-1): the tracker's word for `txid` as the RECHECKED view
/// reads it, the one rule [`spender_mined`] and the re-ask's candidate predicate share: `None` when there are no
/// views or no row, else `word == "mined"` AND a non-null `verifyPath` (a stored `mined` row whose recheck could not
/// check this pass is NOT mined).
pub fn checked_tracker_mined(
    words: Option<&std::collections::BTreeMap<String, Value>>,
    txid: &str,
) -> Option<bool> {
    words?
        .get(&txid.to_ascii_lowercase())
        .map(|v| v["word"] == "mined" && !v["verifyPath"].is_null())
}
/// bsv-low #618 (L2): what the owed judgement may conclude from a recorded spender's coin word, the one input that
/// replaces the shared landing bar's hand-written SEEN+final arm (`logic::is_confirmed_landing_with_proof`'s third
/// arm, the overlay's `network_seen` latch AND the spender's bytes-finality) on the owed path. The index's own proof
/// (`spentConfirmed`, a chaintracks-verified spender proof) is the bar's first two arms and is never read here, so no
/// word vetoes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoinLanding {
    /// The spender's word is a CHECKED `mined` (a merkle path against our header at its height, rechecked this
    /// pass): the spend landed; the payout's `claimable` reads the same word.
    Landed,
    /// The spend is decided but not confirmed: `seen` with final bytes (a broadcaster's SEEN; a non-final spend
    /// cannot mine before its height, #323), or a chain-tier word this pass could not hold as checked (`stale`, a
    /// stored `mined` whose recheck failed, `cannotCheck`). The row tells the outcome, "awaiting the chain", and is
    /// never claimable on it.
    Awaiting,
    /// `announced`, `unknown`, `built`, `abandoned`, `rejected`, `conflicted`, a non-final `seen`, or no word: the
    /// outcome is withheld until the index's proof or a checked word lands.
    Withheld,
}

/// PURE (bsv-low #618): the [`CoinLanding`] of a spender from its RECHECKED tracker view (`runtime::views`); `None`
/// (no row) is `Withheld`.
pub fn coin_landing(word: Option<&Value>, spender_final: Option<bool>) -> CoinLanding {
    let Some(w) = word else {
        return CoinLanding::Withheld;
    };
    if w["word"] == "mined" && !w["verifyPath"].is_null() {
        CoinLanding::Landed
    } else if w["word"] == "mined"
        || w["word"] == "stale"
        || w.get("cannotCheck").is_some()
        || (w["word"] == "seen" && spender_final == Some(true))
    {
        CoinLanding::Awaiting
    } else {
        CoinLanding::Withheld
    }
}

/// PURE (bsv-low #618): the shared bar's third-arm pair `(spender_seen, spender_final)` the owed path hands the
/// unchanged assemblers, carrying the coin word's [`CoinLanding`] instead of the `network_seen` latch: `Landed` and
/// `Awaiting` are `(true, true)` (a word the chain tier wrote is final by the chain), `Withheld` is `(false, final)`.
/// The pair feeds nothing but the bar (`results`, `refund_view`, `hops_view`), so the latch is no longer read by the
/// owed judgement.
pub fn word_arm(
    spending_txid: Option<&str>,
    spender_final: Option<bool>,
    words: &std::collections::BTreeMap<String, Value>,
) -> (Option<bool>, Option<bool>) {
    let word = spending_txid.and_then(|t| words.get(&t.to_ascii_lowercase()));
    match coin_landing(word, spender_final) {
        CoinLanding::Landed | CoinLanding::Awaiting => (Some(true), Some(true)),
        CoinLanding::Withheld => (Some(false), spender_final),
    }
}

/// bsv-low #618: the owed path's hydrated rows take [`word_arm`]'s pair in place of the latch's.
pub fn apply_word_arm(
    results: &mut [crate::results::ResultsRow],
    refunds: &mut [crate::refund_view::RefundViewRow],
    hops: &mut [crate::hops_view::HopsViewRow],
    words: &std::collections::BTreeMap<String, Value>,
) {
    for r in results {
        (r.spender_seen, r.spender_final) =
            word_arm(r.spending_txid.as_deref(), r.spender_final, words);
    }
    for r in refunds {
        (r.spender_seen, r.spender_final) =
            word_arm(r.spending_txid.as_deref(), r.spender_final, words);
    }
    for r in hops {
        (r.spender_seen, r.spender_final) =
            word_arm(r.spending_txid.as_deref(), r.spender_final, words);
    }
}

/// The coin judgement's tracker input; a competitor only wins on a checked
/// mined word, never on a courier's positive height or a SEEN pointer.
pub fn spender_coin_word(word: &Value, txid: &str, intended: Option<&str>) -> Value {
    let checked = word["word"] == "mined" && !word["verifyPath"].is_null();
    let coin = if checked && intended.is_some_and(|t| !t.eq_ignore_ascii_case(txid)) {
        "lost"
    } else if checked {
        "spent"
    } else if word["word"] == "stale" {
        "suspended"
    } else {
        "pending"
    };
    coin_word_value(coin, txid, word)
}
/// bsv-low #617 (L1): one coin word as served, the tuple `{word, tier, height, verifyPath}` beside the spender's
/// `txid`. The tier and the height are the spender word's own; the verify path is served only on a coin the
/// spender's CHECKED mined word spent (`spent`, `lost`), so a client re-verifies exactly what made the coin spent.
pub fn coin_word_value(coin: &str, txid: &str, word: &Value) -> Value {
    let verify = if matches!(coin, "spent" | "lost") {
        word["verifyPath"].clone()
    } else {
        Value::Null
    };
    json!({"word":coin,"txid":txid,"height":word["height"],"tier":word["tier"],"verifyPath":verify})
}
fn owed_words(i: &OwedInputs, facts: &mut Value, txid: Option<&str>, intended: Option<&str>) {
    if let (Some(words), Some(id)) = (i.spender_words, txid) {
        let word = words
            .get(&id.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| crate::tracker_host::served::fields(&bsv_tracker::State::new(id)));
        facts["coinWord"] = spender_coin_word(&word, id, intended);
        facts["spenderWord"] = word;
    }
}
fn tracker_confirmed_by_other(
    i: &OwedInputs,
    h: &HopEntry,
    word: Option<&HopChainWord>,
    sweep: &str,
) -> bool {
    let Some(_) = i.spender_words else {
        return confirmed_by_other(h, word, sweep);
    };
    [
        h.spending_txid.as_deref(),
        word.and_then(|w| w.spending_txid.as_deref()),
    ]
    .into_iter()
    .flatten()
    .any(|id| !id.eq_ignore_ascii_case(sweep) && spender_mined(i, Some(id), false))
}

/// Answers (spender, confirmed, courier_only): `courier_only` is the COURIER-RUNG lens fold's (L2) flag, set when
/// `confirmed` rests on the chain rung's courier word alone (no checked tracker word, no checked mined proof).
fn non_pot_spender(i: &OwedInputs, h: &HopEntry, outpoint: &str) -> Option<(String, bool, bool)> {
    if h.spent == Some(true) {
        if let Some(s) = h.spending_txid.as_deref() {
            let s = s.to_ascii_lowercase();
            return if i.pot_spenders.contains(&s) || pointer_refuted(i, &s, h) {
                None
            } else {
                // bsv-low #736: the hop row's `spentConfirmed` is the index's own latch for this pointer
                let index_proven = h.spent_confirmed == Some(true);
                let confirmed =
                    index_proven_or_spender_mined(i, Some(&s), index_proven, index_proven)
                        || i.proof_mined_spenders.contains(&s);
                Some((s.clone(), confirmed, false))
            };
        }
    }
    let w = i.hop_chain.get(outpoint)?;
    if !(w.looked && w.spent == Some(true)) {
        return None;
    }
    let s = w.spending_txid.as_deref()?.to_ascii_lowercase();
    if i.pot_spenders.contains(&s) || pointer_refuted(i, &s, h) {
        return None;
    }
    // bsv-low #692 (RC-L34): the spent-any ladder's `spent_confirmed` OR a chaintracks-checked mined proof of the
    // sweep this pass holds (the only confirming courier, WoC `/0/spent`, can fault; the proof is the fallback).
    let checked = spender_mined(i, Some(&s), false) || i.proof_mined_spenders.contains(&s);
    let confirmed = checked || spender_mined(i, Some(&s), w.spent_confirmed == Some(true));
    Some((s.clone(), confirmed, confirmed && !checked))
}

/// What one identity's `collected_markers_v2` rows say, folded (the recompute's step 5).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CollectedFold {
    /// games whose v1 marker VERIFIED under the identity ([`OwedInputs::collected_verified`]).
    pub verified: HashSet<String>,
    /// games with any row at all ([`OwedInputs::collected_present`]).
    pub present: HashSet<String>,
    /// [`held_key`]s of the identity's held filings (bsv-low #492): [`OwedInputs::held_verified`]. One per row
    /// that names a paying transaction under its own content key; no cap, no order, no signature (the B3 lens
    /// fold M1).
    pub held: HashSet<String>,
    /// rows that carry a `payTxid` their own key was NOT filed for (retire nothing; expected zero for good).
    pub held_key_mismatches: usize,
}

impl CollectedFold {
    /// Fold one row (`gameId`, `sigHex`, `txid`, `payTxid` as [`crate::owed_judge::COLLECTED_SQL`] reads them) of `identity_lc`.
    ///
    /// A row that names a paying transaction is a HELD FILING and nothing else. It retires that transaction's
    /// payout by STRING MATCH, on one condition, THE GATE: the row's own key is the content key the filing door
    /// derives from (game, identity, that txid) (`record_post::is_held_row_key`). The door is the only writer of
    /// such a key and of the column, and it writes both in one INSERT after it verified the identity's signature
    /// over the same three fields; so the signature is checked ONCE, at the door, and never again here. The gate
    /// keeps out every row the door did not write that way: a chain row (keyed by an outpoint; a stranger plants
    /// those free under lenient submit) and a row whose column and key disagree retire nothing, whatever they
    /// carry. No signature is spent on a held filing (it is not tried as the v1 marker either).
    pub fn row(
        &mut self,
        identity_lc: &str,
        game_id: &str,
        sig_hex: Option<&str>,
        txid: Option<&str>,
        pay_txid: Option<&str>,
    ) {
        let g = game_id.to_ascii_lowercase();
        self.present.insert(g.clone());
        if let Some(pay) = pay_txid {
            if txid
                .is_some_and(|key| crate::record_post::is_held_row_key(key, &g, identity_lc, pay))
            {
                self.held.insert(held_key(&g, pay));
            } else {
                self.held_key_mismatches += 1;
            }
            return;
        }
        if sig_hex
            .is_some_and(|sig| crate::record_post::collected_sig_verifies(identity_lc, &g, sig))
        {
            self.verified.insert(g);
        }
    }
}

/// The key of [`OwedInputs::held_verified`]: `<gameId>:<payTxid>`, lowercase.
pub fn held_key(game_id: &str, pay_txid: &str) -> String {
    format!(
        "{}:{}",
        game_id.to_ascii_lowercase(),
        pay_txid.to_ascii_lowercase()
    )
}

/// The fact a payout row carries beside `payTxid` (bsv-low #492): the filing a device whose wallet already OWNS
/// that transaction's output to its home (`payVout`) signs and posts to `/record?kind=collected` to retire the
/// row with no press (the client contract is at the door: `record_post`, "THE CLIENT CONTRACT").
///
/// WHAT REMAINS after the B3 lens fold M1 (there is no signature budget and no sorted cut any more: every held
/// filing the read returns retires its payout on every recompute, however many the identity holds): a held filing
/// retires nothing while (a) the `collected` read FAULTS (counted, `collectedReadFaults`; every row stays served
/// that pass, the safe direction), (b) its game is outside the identity's walk window (the row it would retire is
/// not derived either), or (c) the paying transaction of the row is no longer the one it names (a replaced spend:
/// the row is served again, by design). A fifth payout transaction of one game cannot be filed (the door's 409)
/// and its row stays served.
pub const HELD_FILING_TAG: &str = "LOW/collected/v2";

/// PURE (#517, the gate's LOW-1): the hop's own row or the chain rung names a DIFFERENT tx as its CONFIRMED spender.
/// The index's proof of the filed sweep never outranks that word (a reorg replaced the sweep's block with a competing
/// spend and the latch was missed): the ladder below follows the chain's spender, as before the proof existed.
/// Stated asymmetry (the delta-verify's N-C): this does not consult `pointer_refuted`, so a confirmed pointer whose
/// bytes were read and do not spend the hop still neutralises the proof; the outcome is the pre-#517 "could not
/// judge", never worse, and a confirmed refuted pointer would be an overlay attribution bug with no producer today.
pub fn confirmed_by_other(h: &HopEntry, word: Option<&HopChainWord>, sweep_txid: &str) -> bool {
    let other = |s: &str| !s.eq_ignore_ascii_case(sweep_txid);
    (h.spent == Some(true)
        && h.spent_confirmed == Some(true)
        && h.spending_txid.as_deref().is_some_and(other))
        || word.is_some_and(|w| {
            w.looked
                && w.spent == Some(true)
                && w.spent_confirmed == Some(true)
                && w.spending_txid.as_deref().is_some_and(other)
        })
}

/// The hop is spent by ITS OWN SEAT'S SWEEP: the sweep this identity FILED (the index's spender or the chain rung's
/// names it), else a spender whose stored bytes pay the seat's committed pay home for the game. The payout's
/// claimability is the spend's confirmation from the source that named it; `output_spent` says the index saw the
/// home output spent since (collected and moved on).
fn swept_home(i: &OwedInputs, h: &HopEntry) -> Option<SweptHome> {
    let outpoint = outpoint_key(&h.hop_txid, h.hop_vout);
    let game = h.game_id.to_ascii_lowercase();
    let my_pkh = i.my_pkh_by_game.get(&game).map(|p| p.to_ascii_lowercase());
    // the home outputs' own spend word, when the index holds the spender's bytes
    let home_outputs_of = |spender: &str| -> Vec<&SpenderOutput> {
        let (Some(outs), Some(pkh)) = (i.spender_outputs.get(spender), my_pkh.as_deref()) else {
            return Vec::new();
        };
        outs.iter()
            .filter(|o| {
                o.pkh_hex
                    .as_deref()
                    .is_some_and(|p| p.eq_ignore_ascii_case(pkh))
            })
            .collect()
    };
    // bsv-low #485: a home output is SPENT by the index's own word, or by the home key's signature this crate
    // verified over its spender's bytes (`HomeSpendWord::Proven`); never by a courier's word of spent
    let home_proven = |spender: &str, o: &SpenderOutput| {
        i.home_spends.get(&outpoint_key(spender, o.vout)) == Some(&HomeSpendWord::Proven)
    };
    let home_spent = |spender: &str| -> bool {
        let mine = home_outputs_of(spender);
        !mine.is_empty()
            && mine
                .iter()
                .all(|o| o.spent == Some(true) || home_proven(spender, o))
    };
    if let Some(filed) = i.hop_sweeps.get(&outpoint) {
        let named_by_index = h
            .spending_txid
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case(&filed.sweep_txid));
        let by_chain = i.hop_chain.get(&outpoint).filter(|w| {
            w.looked
                && w.spent == Some(true)
                && w.spending_txid
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(&filed.sweep_txid))
        });
        // bsv-low #517 (loop 19, pair 11, 2026-09-21): the INDEX's own VERIFIED proof of the filed sweep names the
        // spender and confirms the payout FIRST (`transactions.has_proof`, the latch only the chaintracks-verified stitch
        // sets: a mined sweep that spends the hop is the hop's spender, definitively; the same word `/tx-any` serves and
        // `/credit-beef` assembles the credit from). Without it the row rested on the hop row (never attributed to a
        // sweep once the JOIN's eviction released its pointer) or a courier's word (an indexer read a 1,997-tx block's
        // sweep "unconfirmed" seven minutes after the mine, memoised five more; three blocks passed in nine minutes).
        // the gate's LOW-1 (Rule 6): the proof never outranks a CONTRADICTING confirmed word; the ladder below decides
        // (counted once per hop in the row pass: this fn also runs in the candidates pre-pass)
        let contradicted =
            tracker_confirmed_by_other(i, h, i.hop_chain.get(&outpoint), &filed.sweep_txid);
        let index_word = filed.index_proven && !contradicted;
        let tracker_word = spender_mined(i, Some(&filed.sweep_txid), false) && !contradicted;
        if index_word || tracker_word || named_by_index || by_chain.is_some() {
            // the index's proof, OR its hop-row word, OR the chain rung's (a spend the index recorded before its block
            // and never re-checked read "not mined yet" for days on the pair: the recompute now probes such hops and
            // the courier's confirmation heals the row without a client action)
            // the gate's LOW-3 (2026-09-19): the confirming chain word must NAME the filed sweep (`by_chain` does) — a
            // hop the JOIN took after all (evicted, then readmitted on its mine) reads spent+confirmed by a DIFFERENT
            // tx, and that must never turn the sweep's payout claimable for a tx that can never mine
            // bsv-low #736: the hop row's latch naming the filed sweep is the index's proof (never vetoed by the
            // sweep's word, still yielding to a contradicting confirmed spender); the chain rung's is a courier's
            let index_latched = named_by_index && h.spent_confirmed == Some(true) && !contradicted;
            // the COURIER-RUNG lens fold (L2, NS-3, #692): the chain rung's `spent_confirmed` (WoC's status, or
            // BananaBlocks' scope under a WoC fault) is a courier's word, never a press: it is no longer `legacy` here.
            // What presses is the index (its proof, its hop-row latch), a CHECKED tracker `mined` word, or a
            // chaintracks-checked mined proof of the sweep; a courier's confirmed alone is the pending wait's hint.
            let confirmed = index_word
                || tracker_word
                || index_proven_or_spender_mined(
                    i,
                    Some(&filed.sweep_txid),
                    index_latched,
                    index_word || (named_by_index && h.spent_confirmed == Some(true)),
                )
                || i.proof_mined_spenders.contains(&filed.sweep_txid);
            let courier_confirmed =
                !confirmed && by_chain.and_then(|w| w.spent_confirmed) == Some(true);
            return Some(SweptHome {
                sweep_txid: filed.sweep_txid.clone(),
                raw_hex: Some(filed.raw_hex.clone()),
                pays_sats: filed.pays_sats,
                confirmed,
                index_proven: index_word,
                index_proof_height: if index_word {
                    filed.index_proof_height
                } else {
                    None
                },
                source: "hopsweep-filing",
                output_spent: home_spent(&filed.sweep_txid),
                home_outputs: home_outputs_of(&filed.sweep_txid)
                    .iter()
                    .map(|o| (o.vout, o.sats))
                    .collect(),
                courier_confirmed,
            });
        }
    }
    // an UNFILED sweep (a device before the filing existed, a recovery tool): the spender's stored bytes pay MY home
    let (spender, confirmed, courier_only) = non_pot_spender(i, h, &outpoint)?;
    let outs = i.spender_outputs.get(&spender)?;
    if outs.iter().any(|o| o.pot_lock) {
        return None; // a transaction that CREATES a pot is a JOIN, never a sweep (the delta-verify's NEW-3)
    }
    let pkh = my_pkh?;
    let mine: Vec<&SpenderOutput> = outs
        .iter()
        .filter(|o| {
            o.pkh_hex
                .as_deref()
                .is_some_and(|p| p.eq_ignore_ascii_case(&pkh))
        })
        .collect();
    if mine.is_empty() {
        return None;
    }
    let mut sum = 0u64;
    for o in &mine {
        sum = sum.checked_add(o.sats)?;
    }
    let source = if i.courier_spenders.contains(&spender) {
        "courier-bytes"
    } else {
        "index-bytes"
    };
    // the COURIER-RUNG lens fold (L2): an index-bytes sweep is a press, so a courier's confirmed alone is its hint; a
    // courier-bytes sweep is never claimable (the gate's M3) and keeps the courier's word for its home look (#485)
    let courier_confirmed = source == "index-bytes" && courier_only;
    let confirmed = confirmed && !courier_confirmed;
    Some(SweptHome {
        raw_hex: None,
        pays_sats: Some(sum),
        confirmed,
        courier_confirmed,
        index_proven: false,
        index_proof_height: None,
        source,
        output_spent: mine
            .iter()
            .all(|o| o.spent == Some(true) || home_proven(&spender, o)),
        home_outputs: mine.iter().map(|o| (o.vout, o.sats)).collect(),
        sweep_txid: spender,
    })
}

/// One home output of a courier-proven sweep the route should look at (bsv-low #485).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CourierHomeOutput {
    pub sweep_txid: String,
    pub vout: u32,
    pub pkh_hex: String,
    pub sats: u64,
}

/// PURE (bsv-low #485): the home outputs of this identity's CONFIRMED courier-proven payouts, the candidates of the
/// route's home-spend walk. The same judgment the row is derived from (`swept_home`), so the walk looks at exactly
/// the outputs a served row rests on; a game the identity itself filed `collected` for is skipped (its row is
/// retired or retiring by the filing). Sorted, deduplicated.
pub fn courier_home_outputs(i: &OwedInputs) -> Vec<CourierHomeOutput> {
    let mut out: Vec<CourierHomeOutput> = Vec::new();
    for h in i.hops {
        let game = h.game_id.to_ascii_lowercase();
        if i.collected_verified.contains(&game) {
            continue;
        }
        let Some(swept) = swept_home(i, h) else {
            continue;
        };
        if swept.source != "courier-bytes" || !swept.confirmed || swept.output_spent {
            continue;
        }
        if i.held_verified
            .contains(&held_key(&game, &swept.sweep_txid))
        {
            continue; // bsv-low #492: retired by the identity's held filing, nothing to walk
        }
        let Some(pkh) = i.my_pkh_by_game.get(&game) else {
            continue;
        };
        for (vout, sats) in &swept.home_outputs {
            out.push(CourierHomeOutput {
                sweep_txid: swept.sweep_txid.to_ascii_lowercase(),
                vout: *vout,
                pkh_hex: pkh.to_ascii_lowercase(),
                sats: *sats,
            });
        }
    }
    out.sort_by(|a, b| (&a.sweep_txid, a.vout).cmp(&(&b.sweep_txid, b.vout)));
    out.dedup();
    out
}

/// What a hop spender's BYTES say about the hop it took, once `swept_home` found no payout in them (the gate's M1
/// and M2, 2026-09-20): a pot covenant output = a JOIN the index does not hold (never a custody story); a home this
/// list does not know = the pay-to addresses, for the device to match; else spent outside this game.
enum SpenderStory {
    Pot,
    HomeUnknown { pkhs: Vec<String> },
    Elsewhere,
}

fn spender_story(i: &OwedInputs, spender: &str, game: &str) -> Option<SpenderStory> {
    let outs = i.spender_outputs.get(spender)?;
    if outs.iter().any(|o| o.pot_lock) {
        return Some(SpenderStory::Pot);
    }
    if !i.my_pkh_by_game.contains_key(game) {
        let mut pkhs: Vec<String> = outs
            .iter()
            .filter_map(|o| o.pkh_hex.as_deref().map(str::to_ascii_lowercase))
            .collect();
        pkhs.sort_unstable();
        pkhs.dedup();
        return Some(SpenderStory::HomeUnknown { pkhs });
    }
    Some(SpenderStory::Elsewhere)
}

/// The story's reason, its machine facts written; `unknown` when the bytes were never read.
fn story_reason(
    facts: &mut Value,
    story: Option<SpenderStory>,
    unknown: &'static str,
) -> &'static str {
    match story {
        Some(SpenderStory::Pot) => {
            facts["spendKind"] = json!("pot-unindexed");
            POT_UNINDEXED_REASON
        }
        Some(SpenderStory::HomeUnknown { pkhs }) => {
            facts["spenderPkhs"] = json!(pkhs);
            HOME_UNKNOWN_REASON
        }
        Some(SpenderStory::Elsewhere) => {
            facts["spendKind"] = json!("spent-elsewhere");
            SPENT_ELSEWHERE_REASON
        }
        None => unknown,
    }
}

/// THE DERIVATION. Pure; one row per outpoint; the families exclusive by the
/// pot's spend state (spent → payout / unbound; unspent → refund-due /
/// in-progress / unbound; a hop outpoint → hop-stranded).
pub fn derive_owed_rows(i: &OwedInputs) -> Vec<OwedRow> {
    // #517, the gate's LOW-1: the hop outpoints whose proven filing met a contradicting confirmed spender this pass
    let mut contradicted_hops: HashSet<String> = HashSet::new();
    let me = i.identity_lc.to_ascii_lowercase();
    let mut rows: Vec<OwedRow> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    // N10: the `collected` marker names (identity, game) — a CLAIMABLE NAME, not an outpoint. It retires a payout only
    // when the game has exactly ONE payout candidate for this identity (a re-funded game with two pots keeps both rows
    // and shows the marker as provenance): one marker can never hide a second pot's payout.
    let mut payout_candidates_by_game: HashMap<String, usize> = HashMap::new();
    for e in i.results {
        if e.spent == Some(true)
            && e.verdict.is_some()
            && matches!(e.outcome, Outcome::Won | Outcome::Tie | Outcome::Refund)
        {
            *payout_candidates_by_game
                .entry(e.game_id.to_ascii_lowercase())
                .or_insert(0) += 1;
        }
    }
    // a hop spent by ITS OWN filed sweep is a payout candidate of the game too (the sweep's credit): the one
    // `collected` marker of a game whose stake came back by a sweep AND whose pot paid keeps both rows
    for h in i.hops {
        if swept_home(i, h).is_some_and(|s| !s.output_spent) {
            *payout_candidates_by_game
                .entry(h.game_id.to_ascii_lowercase())
                .or_insert(0) += 1;
        }
    }

    // The pots, by the results view (the identity's party window, era-filtered).
    for e in i.results {
        let outpoint = outpoint_key(&e.pot_txid, e.pot_vout);
        if !seen.insert(outpoint.clone()) {
            continue;
        }
        let game = e.game_id.to_ascii_lowercase();
        // MEDIUM-9: the ONE shared predicate for the gate height (the committed height when it is a valid block
        // height, else the marker's; a timestamp-range value is no height) — never a fourth copy.
        let gate =
            crate::refund_view::served_recovery_height(e.cov_recovery_height, e.recovery_height);
        let refund = i
            .refunds
            .iter()
            .find(|r| r.pot_txid.eq_ignore_ascii_case(&e.pot_txid) && r.pot_vout == e.pot_vout);
        let mut base_facts = json!({
            "potTxid": e.pot_txid,
            "potVout": e.pot_vout,
            "recoveryHeight": gate,
            // N6: ONE unit inside `facts` — milliseconds, named; the two sources are unix SECONDS (`pot_records.createdAt`,
            // `potparty_records.createdAt`, the same columns `era_filter_sql` multiplies by 1000).
            "potAdmittedAtMs": refund.and_then(|r| r.pot_admitted_at).map(|s| s.saturating_mul(1000)),
            "firstPartyMarkerAtMs": refund.and_then(|r| r.first_party_marker_at).map(|s| s.saturating_mul(1000)),
            "mySeat": seat_str(e.my_seat),
            "stakeSats": e.stake_sats,
            "spent": e.spent,
            "spentConfirmed": e.spent_confirmed,
            "verdict": e.verdict.map(PotVerdict::as_str),
            "settleSigners": e.settle_signers,
            "outcome": e.outcome.as_str(),
            "outcomeSource": e.outcome_source,
            "settleTxid": e.settle_txid,
            "committedKeys": crate::results::CommittedKeys::to_json(e.committed_keys.as_ref()),
        });
        owed_words(
            i,
            &mut base_facts,
            e.settle_txid.as_deref(),
            e.settle_txid.as_deref(),
        );
        // bsv-low #518 (loop 20's pre-flight, 2026-09-21): a JOIN the network REFUSED (evicted, never readmitted)
        // never formed a pot — not a row on ANY arm, whatever its pot row says (a row that outran its eviction
        // before #513 read "unspent" here and the brain offered a refund for a pot the network never held). The
        // seat's hop carries the money's story (unspent → stranded; swept → the sweep's payout).
        if i.evicted_pots.contains(&e.pot_txid.to_ascii_lowercase()) {
            continue;
        }
        match e.spent {
            Some(true) => {
                // A DECIDED LOSS is not a row. A payout the identity ITSELF filed as collected (the marker's
                // signature verified under the identity) is not a row; a merely PRESENT collected row is display
                // provenance and retires nothing (the gate's HIGH-1: `collected_markers_v2` is byte-format admitted,
                // a stranger can plant one naming any (identity, game)).
                if e.outcome == Outcome::Lost {
                    continue;
                }
                let verified_collected = i.collected_verified.contains(&game);
                if verified_collected
                    && payout_candidates_by_game.get(&game).copied().unwrap_or(0) <= 1
                {
                    continue;
                }
                let pays_me = matches!(e.outcome, Outcome::Won | Outcome::Tie | Outcome::Refund);
                if pays_me && e.verdict.is_some() {
                    // bsv-low #492 / #512: the identity's wallet ALREADY HOLDS this payout, by its own verified held
                    // filing naming the paying transaction (the live credit landed it and the closing page dropped
                    // the v1 filing; 118 presses of the two collect passes said "Collected" over such rows). Not a
                    // row: there is nothing to collect and so nothing to press. It names the TRANSACTION, so the
                    // candidate count (N10) does not matter, and a pot whose spend becomes another transaction is
                    // served again.
                    if e.settle_txid
                        .as_deref()
                        .is_some_and(|t| i.held_verified.contains(&held_key(&game, t)))
                    {
                        continue;
                    }
                    let mut facts = base_facts.clone();
                    facts["claim"] = json!("internalize");
                    // NOTE-22: the press is offered only once the spend is CONFIRMED (the credit path's landing bar).
                    // the review's LOW-1 (2026-09-20): a spend with a VERIFIED proof height is mined whatever the flag says
                    // (the landing bar's third arm, the unbound arm's L1 rule) — never a chain wait said by machine on a mined tx
                    // bsv-low #736: `spentConfirmed` is the index's latch, a proof with or without its height;
                    // bsv-low #770: the one rule of the settle's two sites, the hop pointer arm's
                    let confirmed = settle_spend_confirmed(i, e);
                    facts["claimable"] = json!(confirmed);
                    if !confirmed {
                        facts["claimReason"] = json!(UNCONFIRMED_PAYOUT_REASON);
                        facts["chainWait"] = json!("block"); // a CHAIN wait by machine (the wave's homeKeyRecovery: 25 min counted as a wedge)
                    }
                    facts["collectedMarkerPresent"] =
                        json!(i.collected_present.contains(&game) || verified_collected);
                    facts["collectedSigVerified"] = json!(verified_collected);
                    facts["creditBeef"] =
                        json!(e.settle_txid.as_ref().map(|t| format!("/credit-beef/{t}")));
                    // bsv-low #492 / #512: WHAT a device asks its own wallet about before it offers the press (does
                    // the ledger already hold this transaction?) and what its held filing names when it does
                    facts["payTxid"] =
                        json!(e.settle_txid.as_ref().map(|t| t.to_ascii_lowercase()));
                    facts["heldFiling"] = json!(HELD_FILING_TAG);
                    // the B3 lens fold M2: the OUTPUT the device must own before it files (`payTxid`:`payVout`, the
                    // spend's output to my committed home `payPkh`). The spend's output order is not in the index
                    // (a rake output may lead it), so the vout is `null` here and the route names it from the
                    // spend's stored bytes (`pay_vout_wanted`, `mark_pay_vouts`); `null` stays when it could not.
                    let pay_pkh = my_committed_pay_pkh(e);
                    if pay_pkh.is_some() {
                        facts["payPkhSource"] = json!(PAY_PKH_COMMITTED_HOME);
                    }
                    facts["payPkh"] = json!(pay_pkh);
                    facts["payVout"] = Value::Null;
                    facts["payASats"] = json!(e.money.settle.as_ref().and_then(|s| s.pay_a_sats));
                    facts["payBSats"] = json!(e.money.settle.as_ref().and_then(|s| s.pay_b_sats));
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::Payout,
                        game_id: game,
                        sats: my_settle_sats(e),
                        opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                        at_height: e.at_height,
                        facts,
                        reason: None,
                    });
                } else {
                    let mut facts = base_facts.clone();
                    // the gate's L1: a spend with a VERIFIED proof height is mined (the landing bar's third arm),
                    // whatever `spentConfirmed` says — a classification gap on a mined tx is not a block wait
                    // bsv-low #770: the claimable arm's rule, so the two sentences never disagree
                    let awaiting_block = e.verdict.is_none()
                        && e.settle_txid.is_some()
                        && !settle_spend_confirmed(i, e);
                    let reason = if awaiting_block {
                        // the index holds the spend (the results view names it) and the chain has not confirmed it:
                        // the verdict is computed at the landing bar, so the row is an honest chain wait
                        facts["chainWait"] = json!("block");
                        facts["spendTxid"] = json!(e.settle_txid);
                        SPEND_AWAITING_BLOCK_REASON
                    } else if e.verdict.is_none() {
                        "the spend is not classified yet (no verdict)"
                    } else {
                        "the seat binding is unknown (which home is mine could not be established)"
                    };
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::Unbound,
                        game_id: game,
                        sats: None,
                        opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                        at_height: e.at_height,
                        facts,
                        reason: Some(reason.to_string()),
                    });
                }
            }
            Some(false) => {
                let gate_open = matches!((gate, i.tip), (Some(h), Some(t)) if t >= h);
                let valid = i.valid_refunds.get(&outpoint);
                let refund_due = gate_open
                    && refund.is_none_or(|r| {
                        r.status == RefundStatus::GateOpen || r.status == RefundStatus::Armed
                    })
                    && valid.is_some();
                // bsv-low #831: a refund spends the pot's funding output, so it can land only where the network holds
                // that funding: the pot's SEEN latch, the index's proof, or the tracker's checked `mined` word. A
                // funding held by nothing (a JOIN admitted pending, never witnessed, never mined) is no press. A
                // word-store fault leaves the set's pots withheld for that pass (the strict rule, latch or proof),
                // never pressed on an unread word.
                let funding_unheld = refund_due
                    && i.funding_unheld.contains(&e.pot_txid.to_ascii_lowercase())
                    && checked_tracker_mined(i.spender_words, &e.pot_txid) != Some(true);
                if funding_unheld {
                    let mut facts = base_facts.clone();
                    facts["claim"] = json!(FUNDING_NOT_HELD_CLAIM);
                    facts["claimable"] = json!(false);
                    facts["claimReason"] = json!(FUNDING_NOT_HELD_REASON);
                    // bsv-low #598: a latched funding the network refused since is the same judgement, named apart
                    match funding_refusal_word(i.spender_words, &e.pot_txid) {
                        Some(word) => {
                            facts["fundingHeld"] = json!(FUNDING_HELD_REFUSED);
                            facts["fundingWord"] = json!(word);
                        }
                        None => facts["fundingHeld"] = json!(FUNDING_HELD_NONE),
                    }
                    facts["tip"] = json!(i.tip);
                    facts["refundStatus"] = json!(refund.map(|r| r.status.as_str()));
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::Unbound,
                        game_id: game,
                        sats: e.stake_sats,
                        opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                        at_height: None,
                        facts,
                        reason: Some(FUNDING_NOT_HELD_REASON.to_string()),
                    });
                } else if refund_due {
                    let mut facts = base_facts.clone();
                    facts["claim"] = json!("present-refund");
                    facts["claimable"] = json!(true);
                    facts["tip"] = json!(i.tip);
                    facts["refundSource"] = json!("refund-backups");
                    facts["refundStatus"] = json!(refund.map(|r| r.status.as_str()));
                    // LOW-15: a refund that pays MY home nothing is a sizing FAULT (the raw or the seat is wrong),
                    // said as such — never "0 sats" on a claimable row.
                    let sized = valid.and_then(|v| v.my_sats);
                    if sized == Some(0) {
                        facts["refundSizing"] = json!("zero: the filed refund pays this seat's home nothing (the raw or the seat is wrong)");
                    }
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::RefundDue,
                        game_id: game,
                        sats: sized.filter(|v| *v > 0),
                        opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                        at_height: None,
                        facts,
                        reason: None,
                    });
                } else if i.tip.is_none() {
                    // TIP-877-FOLD-2 / NEW-LOW-2: deliberately withhold both
                    // rejoin and previously due refunds until a current tip
                    // is readable. An earlier tip is not a gate floor here.
                    let mut facts = base_facts.clone();
                    facts["claim"] = Value::Null;
                    facts["claimable"] = json!(false);
                    facts["tip"] = Value::Null;
                    facts["tipUnknown"] = json!(true);
                    facts["blocksToGate"] = Value::Null;
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::Unbound,
                        game_id: game,
                        sats: e.stake_sats,
                        opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                        at_height: None,
                        facts,
                        reason: Some(
                            "the chain tip is unavailable; the recovery gate is unknown".into(),
                        ),
                    });
                } else if !gate_open {
                    // The rejoin row carries the served recovery gate as the `recoveryHeight` FACT (inherited
                    // from `base_facts` above, == `served_recovery_height`). It is DISPLAY-TIER: the bsv-low #469
                    // home card reads it as a SECOND rejoin-safe recovery source (`RejoinWindowLine recoverable`,
                    // ORed beside this device's own `low_pot_refund_` record), so a seat mid-hand shows "rejoin to
                    // finish — your stake is safe in the pot" even when the local refund record has not surfaced
                    // (the fleet-loop-16 R1 gap). The fact rides `facts.recoveryHeight`, NOT `at_height`, ON
                    // PURPOSE: the gate is a FUTURE block height, and every reader renders `atHeight` as a mined
                    // "· at height N" (the Your Games owed list) and sorts on it — a future gate there would
                    // mislabel and mis-order the row. `at_height` stays None (no spend has mined for an unspent pot).
                    let mut facts = base_facts.clone();
                    facts["claim"] = json!("rejoin");
                    facts["claimable"] = json!(true);
                    facts["tip"] = json!(i.tip);
                    facts["blocksToGate"] = json!(match (gate, i.tip) {
                        (Some(h), Some(t)) => Some(h.saturating_sub(t)),
                        _ => None,
                    });
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::InProgress,
                        game_id: game,
                        sats: e.stake_sats,
                        opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                        at_height: None,
                        facts,
                        reason: None,
                    });
                } else {
                    let mut facts = base_facts.clone();
                    facts["tip"] = json!(i.tip);
                    facts["refundStatus"] = json!(refund.map(|r| r.status.as_str()));
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::Unbound,
                        game_id: game,
                        sats: e.stake_sats,
                        opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                        at_height: None,
                        facts,
                        reason: Some(
                            "the recovery gate is open and no valid filed refund names this pot (the tower's dead-man switch is the path)"
                                .to_string(),
                        ),
                    });
                }
            }
            None => {
                // (an evicted pot never reaches here since #518: the check above covers every arm)
                // The index has no spend word for this pot (never admitted): the brain cannot judge it. A
                // sentence, not silence.
                let mut facts = base_facts.clone();
                facts["tip"] = json!(i.tip);
                rows.push(OwedRow {
                    identity: me.clone(),
                    outpoint,
                    family: OwedFamily::Unbound,
                    game_id: game,
                    sats: e.stake_sats,
                    opponent_identity: Some(e.opponent_identity.to_ascii_lowercase()),
                    at_height: None,
                    facts,
                    reason: Some("the index holds no spend word for this pot".to_string()),
                });
            }
        }
    }

    // The hops, by the hops view (my funded outpoints).
    for h in i.hops {
        let outpoint = outpoint_key(&h.hop_txid, h.hop_vout);
        if !seen.insert(outpoint.clone()) {
            continue;
        }
        let game = h.game_id.to_ascii_lowercase();
        // bsv-low L25 (the courier-retire story-row flap): a hop an earlier pass PROVED home-spent (THIS crate
        // verified MY key's signature over the spender and latched it durably under `homeproof:`) is RETIRED on
        // every recompute, byte-free. On the BYTE-FREE path only (`swept_home` is `None` because this pass's
        // spender courier cap withheld the sweep's bytes), the durable latch retires the hop exactly as the
        // bytes-in-hand Arm A does (`swept_home`'s `output_spent`); with the bytes present `swept_home` is `Some`
        // and Arm A/B decide as before.
        //
        // The HOMEPROOF lens's LOW-1 (the one-home invariant): this byte-free match reads the latch per vout
        // (`homeproof:<spender>.<vout>`) but retires on ANY proven latch for the named spender
        // (`latched_retired_spenders`, an `.any()`), whereas Arm A retires only when EVERY one of this identity's
        // home outputs in the sweep is spent-or-Proven (`swept_home`'s `home_spent`, an `.all()`). The two agree
        // because a LOW sweep pays this identity EXACTLY ONE home output by construction: the client builds every
        // hop sweep as one hop input to one `dest_script_hex` home (`money_p2pkh_sweep_assemble`'s
        // `P2pkhSweepPlanJson`: one `hop`, one `dest_script_hex`; `owedPress::buildHopSweepForRow`,
        // `Pot::ensureHopSweepInternal`), and two hops of one game are TWO sweeps with one home output each
        // (stake.ts: "both hops pay the same script"). So the sole home output's latch IS every home output, and
        // the byte-free `.any()` equals Arm A's `.all()`: the answer is the same with and without the bytes, and a
        // still-claimable sibling home output a later pass sees is never hidden (there is no sibling to hide). WERE
        // a sweep ever to pay this identity two home outputs, one latched and one still claimable, the byte-free
        // path could not see the unlatched sibling and would retire where Arm A serves a payout; the one-home
        // invariant, enforced by the client builder and NOT by this read, is what keeps the two equal (pinned:
        // `l25_low1_one_home_sweep_retires_the_same_byte_free_and_with_bytes`). The latch only ever serves LESS (no
        // row), never mints a claim; a planted spender cannot forge a Proven latch. Closes the #662 gap at its
        // source: the latch, not a byte-gated candidate window, is the durable retirement authority.
        if swept_home(i, h).is_none()
            && hop_named_spender(i, h).is_some_and(|sp| i.retired_spenders.contains(&sp))
        {
            continue;
        }
        let mut facts_base = json!({
            "hopTxid": h.hop_txid,
            "hopVout": h.hop_vout,
            "hopSats": h.hop_sats,
            "spent": h.spent,
            "spendingTxid": h.spending_txid,
            "spentConfirmed": h.spent_confirmed,
            "status": h.status.as_str(),
            "statusSource": h.status_source,
            "markerVerified": h.marker_verified.as_str(),
            "markerCreatedAtMs": h.marker_created_at,
            // the key that owns the hop (the marker's claim): a device without the filing signs its own sweep
            // against it (`owedPress.buildHopSweepForRow`), matching the key it derives
            "seatSettlePubkey": h.seat_settle_pubkey,
        });
        owed_words(
            i,
            &mut facts_base,
            h.spending_txid.as_deref(),
            i.hop_sweeps.get(&outpoint).map(|f| f.sweep_txid.as_str()),
        );
        // THE SWEPT HOP (bsv-low #469 decision 3): the hop is spent by its own seat's sweep — the sweep this identity
        // FILED, or a spender whose stored bytes pay the seat's committed home (an unfiled sweep) — so the stake is
        // at the seat's own home, uncollected: a `payout` row whose credit is the sweep (`/credit-beef/<sweepTxid>`),
        // claimable once the sweep mined; retired by `collected` like every payout (one candidate per game, N10
        // above), or by the index's word that the home output was spent since (collected and moved on).
        // #517, the gate's LOW-1: a PROVEN filing met a contradicting confirmed spender (the hop row's or the chain
        // rung's): the proof did not decide (see `confirmed_by_other`). The fact rides the hop's row (whatever family
        // the ladder gives it) so the operator sees WHICH hop; the route counts the rows (the delta-verify's N-A/N-B:
        // this derivation stays pure, no global moves here)
        if i.hop_sweeps.get(&outpoint).is_some_and(|f| {
            f.index_proven && confirmed_by_other(h, i.hop_chain.get(&outpoint), &f.sweep_txid)
        }) {
            contradicted_hops.insert(outpoint.clone());
        }
        if let Some(swept) = swept_home(i, h) {
            if swept.output_spent {
                continue;
            }
            let verified_collected = i.collected_verified.contains(&game);
            if verified_collected && payout_candidates_by_game.get(&game).copied().unwrap_or(0) <= 1
            {
                continue;
            }
            // bsv-low #492 / #512: the wallet already holds this sweep's payout (the identity's held filing names
            // the sweep): not a row, whichever proof named the sweep (a courier-proven one included)
            if i.held_verified
                .contains(&held_key(&game, &swept.sweep_txid))
            {
                continue;
            }
            let mut facts = facts_base.clone();
            owed_words(
                i,
                &mut facts,
                Some(&swept.sweep_txid),
                Some(&swept.sweep_txid),
            );
            facts["claim"] = json!("internalize");
            facts["claimable"] = json!(swept.confirmed);
            if !swept.confirmed {
                facts["claimReason"] = json!(UNCONFIRMED_PAYOUT_REASON);
                facts["chainWait"] = json!("block");
            }
            if swept.courier_confirmed {
                // the COURIER-RUNG lens fold (L2): the courier's confirmed is served, as a hint, never a press (the
                // route's `mark_pending_spender` may name the courier's rung where it holds that answer)
                facts["spenderPresence"] =
                    json!({"word": "spent", "tier": "pending", "source": "courier"});
            }
            if swept.source == "courier-bytes" {
                // the gate's M3 (2026-09-20): the index holds no bytes and no proof for this transaction, so
                // `/credit-beef` cannot assemble the credit; the row tells the truth instead of a press that pends
                // forever. An UNCONFIRMED one keeps its waiting word (the round-2 LOW-1): the pointer's spend may
                // still be displaced, so "the sats sit at your home" is said only once the chain confirmed it.
                facts["claimable"] = json!(false);
                facts["claimReason"] = json!(if swept.confirmed {
                    COURIER_BYTES_NO_CREDIT_REASON
                } else {
                    UNCONFIRMED_PAYOUT_REASON
                });
                // bsv-low #485: the row retires by a `collected` filing, or once every home output's spend is PROVEN
                // (`swept_home`'s `output_spent`: the home key's signature, verified here). Until then it says what the
                // chain rung's look at the home outputs found, when it looked: `unspent` (a corroborated absence: the
                // sats were seen at the home) or `unproven` (a spender is named, its bytes do not carry the proof yet).
                facts["creditKind"] = json!("courier-bytes");
                let words: Vec<Option<&HomeSpendWord>> = swept
                    .home_outputs
                    .iter()
                    .map(|(vout, _)| i.home_spends.get(&outpoint_key(&swept.sweep_txid, *vout)))
                    .collect();
                if words
                    .iter()
                    .any(|w| matches!(w, Some(HomeSpendWord::Unproven)))
                {
                    facts["homeSpend"] = json!("unproven");
                } else if !words.is_empty()
                    && words
                        .iter()
                        .all(|w| matches!(w, Some(HomeSpendWord::Unspent | HomeSpendWord::Proven)))
                {
                    facts["homeSpend"] = json!("unspent");
                }
            }
            facts["outcome"] = json!("hop-sweep");
            facts["sweepTxid"] = json!(swept.sweep_txid);
            facts["sweepRawHex"] = json!(swept.raw_hex);
            facts["sweepSource"] = json!(swept.source);
            // the delta-verify's NEW-1 (2026-09-19): WHICH word made the payout claimable, and how old the chain's word
            // was — a confirmation from a memo (≤ 2 h) after a reorg is a wrong word an operator must be able to see
            if swept.confirmed {
                let by_index = h
                    .spending_txid
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(&swept.sweep_txid))
                    && h.spent_confirmed == Some(true);
                let word = i.hop_chain.get(&outpoint);
                // bsv-low #692 (RC-L34): the spent-any ladder's own word (the hop row's `by_index`, or the chain
                // rung's `spent_confirmed`) is the courier-confirmed source; a proof-confirmation the ladder did NOT
                // supply (its only confirming courier WoC `/0/spent` faulted) is named "proof" — a chaintracks-checked
                // mined word of the sweep, never a courier's bare status or height. The order keeps every prior label.
                let chain_rung_confirmed = word.is_some_and(|w| w.spent_confirmed == Some(true));
                facts["confirmedSource"] = json!(if swept.index_proven {
                    "index-proof"
                } else if spender_mined(i, Some(&swept.sweep_txid), false) {
                    "tracker-proof"
                } else if by_index {
                    "index"
                } else if swept.source != "courier-bytes"
                    && i.proof_mined_spenders.contains(&swept.sweep_txid)
                {
                    // the COURIER-RUNG lens fold (L2): on a press the chain rung's word confirmed nothing; the proof did
                    "proof"
                } else if chain_rung_confirmed {
                    if word.and_then(|w| w.age_ms).is_some() {
                        "chain-memo"
                    } else {
                        "chain-probe"
                    }
                } else if i.proof_mined_spenders.contains(&swept.sweep_txid) {
                    "proof"
                } else if word.and_then(|w| w.age_ms).is_some() {
                    "chain-memo"
                } else {
                    "chain-probe"
                });
                if let Some(height) = swept.index_proof_height {
                    facts["sweepProofHeight"] = json!(height);
                }
                if let Some(age) = word.and_then(|w| w.age_ms) {
                    facts["chainProbeAgeMs"] = json!(age);
                }
            }
            facts["creditBeef"] = json!(format!("/credit-beef/{}", swept.sweep_txid));
            // bsv-low #492 / #512: the paying transaction a device asks its wallet about and names in a held filing
            facts["payTxid"] = json!(swept.sweep_txid.to_ascii_lowercase());
            facts["heldFiling"] = json!(HELD_FILING_TAG);
            // the B3 lens fold M2: the sweep's output to my home, the OUTPUT the device must own before it files:
            // the home outputs the sweep's bytes showed, else the filed sweep's own raw (it hashes to `sweepTxid`),
            // and only when exactly one output pays the home (`null` otherwise: never a pick)
            // bsv-low L25: a game with NO committed home (a hop whose JOIN never assembled, so no pot row) names the
            // filed sweep's own output instead, when its bytes hash to `sweepTxid` and carry EXACTLY ONE P2PKH output
            // (its index and its pkh; zero or several: `null` for both). The filing is the identity's own signed
            // sweep, and the client's press derives its home itself and reads neither field
            let raw = swept.raw_hex.as_deref().and_then(|h| hex::decode(h).ok());
            let committed = i.my_pkh_by_game.get(&game).map(|p| p.to_ascii_lowercase());
            let (pay_pkh, pay_vout, pay_pkh_source) = match committed {
                Some(pkh) => {
                    let vout = match swept.home_outputs.as_slice() {
                        [(vout, _)] => Some(*vout),
                        [] => raw
                            .as_deref()
                            .and_then(|r| sole_home_vout_of_raw(r, &swept.sweep_txid, &pkh)),
                        _ => None,
                    };
                    (Some(pkh), vout, Some(PAY_PKH_COMMITTED_HOME))
                }
                None => match raw
                    .as_deref()
                    .and_then(|r| sole_p2pkh_output_of_raw(r, &swept.sweep_txid))
                {
                    Some((vout, pkh)) => (Some(pkh), Some(vout), Some(PAY_PKH_FILED_SWEEP)),
                    None => (None, None, None),
                },
            };
            if let Some(source) = pay_pkh_source {
                facts["payPkhSource"] = json!(source);
            }
            facts["payPkh"] = json!(pay_pkh);
            facts["payVout"] = json!(pay_vout);
            facts["collectedMarkerPresent"] =
                json!(i.collected_present.contains(&game) || verified_collected);
            facts["collectedSigVerified"] = json!(verified_collected);
            rows.push(OwedRow {
                identity: me.clone(),
                outpoint,
                family: OwedFamily::Payout,
                game_id: game,
                sats: swept.pays_sats,
                opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                // the gate's NIT-3: the sweep's proven block is the row's height (the service order and the page read it)
                at_height: swept.index_proof_height,
                facts,
                reason: None,
            });
            continue;
        }
        // bsv-low #837, THE BELT: an index-UNSPENT hop its own held pot's JOIN spends (read from the pot's stored bytes)
        // is never stranded: the chain spent it into the pot. The pot's row tells the story when the results carry it;
        // otherwise the game keeps this row (never a press, never no row).
        if let Some(pot) = i.join_spent_hops.get(&outpoint) {
            if !pot_row_served(i, pot) {
                rows.push(joined_pot_unlinked_row(
                    &me,
                    outpoint,
                    game,
                    h,
                    &facts_base,
                    pot,
                    "pot-bytes",
                ));
            }
            continue;
        }
        match h.status {
            HopStatus::Spent => {
                let spender = h.spending_txid.as_deref().map(str::to_ascii_lowercase);
                let by_pot = spender
                    .as_deref()
                    .is_some_and(|s| i.pot_spenders.contains(s));
                if by_pot {
                    // the JOIN took it: the pot's own row tells the story. bsv-low #837: kept as it was for an
                    // index-SPENT hop (a pot outside the era's results is a written-off game, no row); the kept row is
                    // for an index-UNSPENT hop a pot spent (the belt above, the chain arm below)
                    continue;
                }
                // MEDIUM-7: a spender ABSENT from the index is not evidence of a wallet spend (an unindexed or
                // evicted JOIN is the common honest case); without positive evidence the brain says it could not
                // judge, never a custody story. With the spender's stored BYTES (a `tm_lowfund`-admitted spend that
                // pays no home of mine) the story is positive: spent outside this game (`spent-elsewhere`).
                let mut facts = facts_base.clone();
                facts["claim"] = Value::Null;
                let story = spender
                    .as_deref()
                    .filter(|s| !pointer_refuted(i, s, h))
                    .and_then(|s| spender_story(i, s, &game));
                let reason = if i.pot_spenders_faulted {
                    "could not check the hop's spender against the index this pass (a read faulted)"
                } else {
                    story_reason(
                        &mut facts,
                        story,
                        "the hop's spender is not in the index (an unindexed join, or a spend outside the game): could not judge",
                    )
                };
                rows.push(OwedRow {
                    identity: me.clone(),
                    outpoint,
                    family: OwedFamily::Unbound,
                    game_id: game,
                    sats: Some(h.hop_sats),
                    opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                    at_height: None,
                    facts,
                    reason: Some(reason.to_string()),
                });
            }
            HopStatus::Unknown if h.spent == Some(true) => {
                // MEDIUM-10: "could not judge" is a SENTENCE — a recorded spend the network has not confirmed. (The
                // other case the view folds into Unknown, a container the index never listed, is judged by the chain
                // rung below: the sats are almost certainly still there — the loudest stranded case.)
                let mut facts = facts_base.clone();
                facts["claim"] = Value::Null;
                rows.push(OwedRow {
                    identity: me.clone(),
                    outpoint,
                    family: OwedFamily::Unbound,
                    game_id: game,
                    sats: Some(h.hop_sats),
                    opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                    at_height: None,
                    facts,
                    reason: Some("the hop's spend is recorded but not confirmed: the outcome is not established yet".to_string()),
                });
            }
            // The stranded judgment: the index's Unspent, and the index's Unknown WITHOUT a recorded spend (the hop's
            // container never indexed: the chain rung is the only word there is), share one ladder.
            HopStatus::Unspent | HopStatus::Unknown => {
                let age_ms = h.marker_created_at.map(|c| i.now_ms.saturating_sub(c));
                // fleet loop 11: a JOIN the network refused makes its hop stranded NOW — the hand cannot start
                // (keyed on THIS hop's outpoint in the eviction ledger's released spends, never on the game's name)
                // bsv-low #486: the same for a JOIN the door refused synchronously (the overlay's refusal ledger names
                // the hop only when the hop's own key signed the refused transaction)
                let evicted = i.evicted_hop_outpoints.contains(&outpoint);
                let door_refused = i.door_refused_hop_outpoints.get(&outpoint).copied();
                let join_refused = evicted || door_refused.is_some();
                let stranded = join_refused || age_ms.is_some_and(|a| a >= HOP_STRANDED_AFTER_MS);
                if !stranded {
                    // A YOUNG unspent hop (the stranded cell's run 4 and the device-switch unit, 2026-09-19): the
                    // stake is in its funding hop and the hand has not started. It IS money of this identity's, so
                    // it is a row — `in-progress` with the felt's `rejoin` (the #449 hop-only rejoin source adopts
                    // the hop from served facts; the felt's own stalled-offer door sweeps it back) — never a sweep
                    // claim before the window (the JOIN may still land). An age the brain cannot say, or a marker
                    // not yet verified, stays no row (the latch runs within seconds).
                    if age_ms.is_none() || h.marker_verified != MarkerVerification::Verified {
                        continue;
                    }
                    let mut facts = facts_base.clone();
                    facts["claim"] = json!("rejoin");
                    facts["claimable"] = json!(true);
                    facts["stage"] = json!("hop-funded");
                    facts["ageMs"] = json!(age_ms);
                    facts["strandedAfterMs"] = json!(HOP_STRANDED_AFTER_MS);
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::InProgress,
                        game_id: game,
                        sats: Some(h.hop_sats),
                        opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                        at_height: None,
                        facts,
                        reason: Some(YOUNG_HOP_REASON.to_string()),
                    });
                    continue;
                }
                // N1: the claim rides a VERIFIED marker only (the shipped client's own bar): `tm_hopparty` admits a
                // row by byte format, so an unverified or not-yet-latched marker naming this identity is a sentence,
                // never a money figure with a sweep press.
                if h.marker_verified != MarkerVerification::Verified {
                    let mut facts = facts_base.clone();
                    facts["claim"] = Value::Null;
                    facts["ageMs"] = json!(age_ms);
                    let reason = if h.marker_verified == MarkerVerification::Unverified {
                        "the hop marker's signature does not verify under this identity (a planted or garbled row): nothing to claim here"
                    } else {
                        "the hop marker is not verified yet (the latch has not run): the sweep waits for the verification"
                    };
                    rows.push(OwedRow {
                        identity: me.clone(),
                        outpoint,
                        family: OwedFamily::Unbound,
                        game_id: game,
                        sats: None,
                        opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                        at_height: None,
                        facts,
                        reason: Some(reason.to_string()),
                    });
                    continue;
                }
                // THE CHAIN RUNG. The arms, by what the chain rung's probe (`/spent-any`) found for THIS outpoint:
                //   - unspent (a positive clean word) → the claim (`chainProbe:"unspent"`);
                //   - spent by a pot → no row (the JOIN took it);
                //   - spent by something else (a KNOWN competitor) → could not judge / `spent-elsewhere`;
                //   - NO positive word (the couriers could not look, 429/fault, or the probe has not run) → the `_`
                //     arm. #451 held this case PENDING ("a read that could not look is never unspent"). bsv-low #669b
                //     (the loop-28/deploy #669 red: WoC 429 held p1's genuine stranded sweep `no-affordance` for the
                //     whole 45 s budget): for an index-UNSPENT stranded hop with a verified marker, the chain rung's
                //     SILENCE no longer holds the honest press. No competitor is KNOWN (the spent arm above never
                //     fired), the INDEX itself says unspent, and the hop is the owner's OWN output — the sweep is the
                //     owner's own spend. So the index's unspent OFFERS the sweep (`chainProbe:"index-unspent"`); a
                //     later positive spent word still DOWNGRADES it (the arm above), and a block arriving since the
                //     probe still degrades it read-side (`degrade_stale_sweeps`) and re-probes it (`should_recompute`).
                //     The dead case — the owner already spent its own hop elsewhere, unseen by LOW's index — MINES to
                //     take the hop, which MOVES the tip and is caught by the tip gate; and a dead sweep that slips the
                //     tip gate (a same-tip spend) costs at most a refused broadcast (the network refuses a second
                //     spend of the owner's own output), never an honest party's sats. An index-UNKNOWN hop (nothing
                //     says unspent: the container never indexed, the chain rung silent) stays pending as before.
                let chain = i.hop_chain.get(&outpoint);
                let mut facts = facts_base.clone();
                facts["ageMs"] = json!(age_ms);
                facts["joinRefused"] = json!(join_refused); // on every arm: a waiting press says why it is sweepable
                if join_refused {
                    facts["joinRefusedBy"] = json!(if evicted { "eviction" } else { "door" });
                    if let (false, Some(door)) = (evicted, door_refused) {
                        facts["doorRefusal"] = json!(door.as_str());
                    }
                }
                match chain {
                    // bsv-low #808 (loop 41, pair 13): the chain's "unspent" was taken BELOW this walk's tip and the
                    // budget left no fresh ask (`hops_view::memo_predates_tip`): a block has arrived since, and it may
                    // hold the competitor. Within the budget the walk asks again (the fix's real win: it closes the
                    // loop-41 occurrence whenever a slot exists). Past it (the eighth candidate, or the 12 s clock),
                    // the word's fate follows the INDEX (the FIX-808 lens's MEDIUM-1, option (b)):
                    //   - index-UNKNOWN (this arm): nothing else says unspent, so the out-of-date word withholds the
                    //     sweep as a pending chain-wait; the walk's rotation asks the hop again.
                    //   - index-UNSPENT (the `index-unspent` arm below): served EXACTLY as silence is, the sweep offered
                    //     with `chainProbeTip` and `tipMoved` stated. An out-of-date "unspent" is strictly more evidence
                    //     than none, and silence offers (#669b); withholding here would bring back the loop-28 class,
                    //     an honest Collect held by the walk's OWN budget (a block makes every unspent memo predate, so
                    //     N - 8 hops, or all of them past the clock, were held until the rotation or the 2 h memo TTL).
                    //     A dead sweep costs a refused broadcast of the owner's own output, never an honest party's sats.
                    Some(w)
                        if w.looked
                            && w.spent == Some(false)
                            && w.predates_tip
                            && h.status != HopStatus::Unspent =>
                    {
                        facts["claim"] = Value::Null;
                        facts["claimable"] = json!(false);
                        facts["chainWait"] = json!("block");
                        facts["tipMoved"] = json!(true);
                        facts["chainProbe"] = json!("unspent-predates-tip");
                        if let Some(age) = w.age_ms {
                            facts["chainProbeAgeMs"] = json!(age);
                        }
                        facts["chainProbeTip"] = json!(w.probe_tip);
                        if let Some(tn) = i.tip {
                            facts["servedTip"] = json!(tn);
                        }
                        rows.push(OwedRow {
                            identity: me.clone(),
                            outpoint,
                            family: OwedFamily::HopStranded,
                            game_id: game,
                            sats: Some(h.hop_sats),
                            opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                            at_height: None,
                            facts,
                            reason: Some(SWEEP_TIP_MOVED_REASON.to_string()),
                        });
                    }
                    Some(w) if w.looked && w.spent == Some(false) && !w.predates_tip => {
                        facts["claim"] = json!("sweep-hop");
                        facts["claimable"] = json!(true);
                        facts["chainProbe"] =
                            json!(if w.stale { "unspent-stale" } else { "unspent" });
                        if let Some(age) = w.age_ms {
                            facts["chainProbeAgeMs"] = json!(age);
                        }
                        // bsv-low #808: the tip the word was taken at, beside its age (at or above this walk's tip)
                        if let Some(pt) = w.probe_tip {
                            facts["chainProbeTip"] = json!(pt);
                        }
                        // the FILED sweep rides the row when this identity filed one (any device presses it as it
                        // is); without one the press signs a sweep on the device that owns the key
                        match i.hop_sweeps.get(&outpoint) {
                            Some(f) => {
                                facts["sweepRawHex"] = json!(f.raw_hex);
                                facts["sweepTxid"] = json!(f.sweep_txid);
                                facts["sweepSource"] = json!("hopsweep-filing");
                            }
                            None => {
                                facts["sweepSource"] = json!("sign-here");
                            }
                        }
                        rows.push(OwedRow {
                            identity: me.clone(),
                            outpoint,
                            family: OwedFamily::HopStranded,
                            game_id: game,
                            sats: Some(h.hop_sats),
                            opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                            at_height: None,
                            facts,
                            reason: if evicted {
                                Some(JOIN_REFUSED_REASON.to_string())
                            } else {
                                door_refused.map(|door| door.sentence().to_string())
                            },
                        });
                    }
                    Some(w) if w.looked && w.spent == Some(true) => {
                        let spender = w.spending_txid.as_deref().map(str::to_ascii_lowercase);
                        if spender
                            .as_deref()
                            .is_some_and(|s| i.pot_spenders.contains(s))
                        {
                            // the JOIN took it after all (the index lagged): the pot's row tells the story, when the
                            // results carry that pot (#837: after a refused sweep of such a hop the game kept no row)
                            if let Some(pot) = spender.as_deref().filter(|p| !pot_row_served(i, p))
                            {
                                rows.push(joined_pot_unlinked_row(
                                    &me, outpoint, game, h, &facts, pot, "chain",
                                ));
                            }
                            continue;
                        }
                        facts["claim"] = Value::Null;
                        facts["chainProbe"] = json!("spent");
                        if let Some(age) = w.age_ms {
                            facts["chainProbeAgeMs"] = json!(age);
                        }
                        facts["chainSpender"] = json!(w.spending_txid);
                        facts["spentConfirmed"] = json!(w.spent_confirmed);
                        let story = spender
                            .as_deref()
                            .filter(|s| !pointer_refuted(i, s, h))
                            .and_then(|s| spender_story(i, s, &game));
                        let reason = story_reason(
                            &mut facts,
                            story,
                            "the chain shows the hop spent by a transaction the index does not hold: could not judge the spender",
                        );
                        rows.push(OwedRow {
                            identity: me.clone(),
                            outpoint,
                            family: OwedFamily::Unbound,
                            game_id: game,
                            sats: Some(h.hop_sats),
                            opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                            at_height: None,
                            facts,
                            reason: Some(reason.to_string()),
                        });
                    }
                    _ if h.status == HopStatus::Unspent => {
                        // bsv-low #669b: the index POSITIVELY says unspent and no competitor is KNOWN (the spent arm
                        // above never fired), so the owner's own stranded hop is claimable on the index's own word.
                        // The chain rung's silence (couriers 429/faulting, or an unrun probe) no longer holds the
                        // honest press. `chainProbe:"index-unspent"` records that the chain rung did not corroborate
                        // (distinct from the courier-corroborated `"unspent"`); the read-side `degrade_stale_sweeps`
                        // and the `should_recompute` re-probe still withhold the sweep the instant a block arrives.
                        facts["claim"] = json!("sweep-hop");
                        facts["claimable"] = json!(true);
                        facts["chainProbe"] = json!("index-unspent");
                        // bsv-low #808 (the lens's MEDIUM-1 (b)): a past-budget "unspent" taken below this walk's tip
                        // is served as silence is, its staleness stated: the tip it was taken at and that the tip moved
                        if let Some(w) =
                            chain.filter(|w| w.looked && w.spent == Some(false) && w.predates_tip)
                        {
                            facts["tipMoved"] = json!(true);
                            facts["chainProbeTip"] = json!(w.probe_tip);
                            if let Some(age) = w.age_ms {
                                facts["chainProbeAgeMs"] = json!(age);
                            }
                            if let Some(tn) = i.tip {
                                facts["servedTip"] = json!(tn);
                            }
                        }
                        // the FILED sweep rides the row when this identity filed one; without one the press signs a
                        // sweep on the device that owns the key (the same source block as the corroborated arm above)
                        match i.hop_sweeps.get(&outpoint) {
                            Some(f) => {
                                facts["sweepRawHex"] = json!(f.raw_hex);
                                facts["sweepTxid"] = json!(f.sweep_txid);
                                facts["sweepSource"] = json!("hopsweep-filing");
                            }
                            None => {
                                facts["sweepSource"] = json!("sign-here");
                            }
                        }
                        rows.push(OwedRow {
                            identity: me.clone(),
                            outpoint,
                            family: OwedFamily::HopStranded,
                            game_id: game,
                            sats: Some(h.hop_sats),
                            opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                            at_height: None,
                            facts,
                            reason: if evicted {
                                Some(JOIN_REFUSED_REASON.to_string())
                            } else {
                                door_refused.map(|door| door.sentence().to_string())
                            },
                        });
                    }
                    _ => {
                        // index-UNKNOWN with no positive chain word: NOTHING says the hop is unspent (the container
                        // never indexed, the chain rung is silent), so the sweep waits for the chain rung's word.
                        facts["claim"] = Value::Null;
                        facts["chainProbe"] = json!("pending");
                        rows.push(OwedRow {
                            identity: me.clone(),
                            outpoint,
                            family: OwedFamily::Unbound,
                            game_id: game,
                            sats: Some(h.hop_sats),
                            opponent_identity: Some(h.opponent_identity.to_ascii_lowercase()),
                            at_height: None,
                            facts,
                            reason: Some("the hop's outpoint is not in the index (the funding never reached it, or it was evicted): the sats are likely still there; the sweep waits for the chain rung's word".to_string()),
                        });
                    }
                }
            }
        }
    }
    if !contradicted_hops.is_empty() {
        for r in rows.iter_mut() {
            if contradicted_hops.contains(&r.outpoint) {
                r.facts["sweepProofContradicted"] = json!(true);
            }
        }
    }
    // A compute without a tip cannot assert that a hop probe is current.
    // Proof-decided payouts survive; tip-dependent presses wait for a tip.
    degrade_stale_sweeps(&mut rows, i.tip, i.tip);
    rows
}

/// The wire body of `GET /owed`.
pub fn owed_body(
    identity: &str,
    tip: Option<u64>,
    rows: &[OwedRow],
    computed_at_ms: i64,
    truncated: bool,
    recompute_seq: Option<i64>,
) -> String {
    let arr: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "outpoint": r.outpoint,
                "family": r.family.as_str(),
                "gameId": r.game_id,
                "sats": r.sats,
                "opponentIdentity": r.opponent_identity,
                "atHeight": r.at_height,
                "facts": r.facts,
                "reason": r.reason,
            })
        })
        .collect();
    let mut body = json!({
        "v": OWED_WIRE_VERSION,
        "identity": identity.to_ascii_lowercase(),
        "tip": tip,
        "rows": arr,
        "truncated": truncated,
        "computedAtMs": computed_at_ms,
    });
    // bsv-low #566: the identity's own recompute sequence (its `owed_state` row), additive: absent when the served
    // snapshot carries none (no snapshot yet, a row no write has stamped since the column, a superseded walk's
    // fallback), so a body without one is byte-identical to the one before the field.
    if let Some(seq) = recompute_seq {
        body["recomputeSeq"] = json!(seq);
    }
    body.to_string()
}

/// bsv-low #669: the freshness gate for a `hop-stranded` sweep claim (the design's §2a). The claimable
/// verdict in a served row rests on a chain probe taken when its snapshot was computed, at `snapshot_tip`. If the
/// chain has moved on since (`present_tip > snapshot_tip`), or the present tip cannot be read at all, that verdict
/// is no longer provably current and the row degrades to a `pending` chain-wait:
/// `claim:null`, `claimable:false`, `chainWait:"block"`, so the client offers no press and the harness reads a block
/// wait. Computation without a tip stores the same unknown verdict; cached reads also apply this one rule.
/// A compute with a readable tip is current; a later tip-triggered recompute re-points the row at the new tip.
pub fn degrade_stale_sweeps(
    rows: &mut [OwedRow],
    snapshot_tip: Option<u64>,
    present_tip: Option<u64>,
) {
    // The chain has moved past the tip this snapshot's probe was taken at (a block arrived), or the present tip
    // cannot be read at all (a cold isolate, chaintracks down). Either way the sweep claim is no longer provably
    // tip-current. A fresh compute never reaches here with a moved tip: its snapshot_tip IS the present tip.
    enum Degrade {
        TipCurrent, // present tip == snapshot tip: the claim stands
        Moved,      // present tip > snapshot tip: a block arrived
        Unknown,    // the present (or snapshot) tip cannot be read: fail-safe
    }
    let verdict = match (snapshot_tip, present_tip) {
        (Some(t0), Some(tn)) if tn > t0 => Degrade::Moved,
        (Some(_), Some(_)) => Degrade::TipCurrent,
        _ => Degrade::Unknown,
    };
    let tip_unknown = match verdict {
        Degrade::TipCurrent => return,
        Degrade::Moved => false,
        Degrade::Unknown => true,
    };
    for r in rows.iter_mut() {
        if r.family != OwedFamily::HopStranded || r.facts["claimable"] != Value::Bool(true) {
            continue;
        }
        r.facts["claim"] = Value::Null;
        r.facts["claimable"] = json!(false);
        r.facts["chainWait"] = json!("block");
        if tip_unknown {
            r.facts["tipUnknown"] = json!(true);
        } else {
            r.facts["tipMoved"] = json!(true);
        }
        if let Some(t0) = snapshot_tip {
            r.facts["snapshotTip"] = json!(t0);
        }
        if let Some(tn) = present_tip {
            r.facts["servedTip"] = json!(tn);
        }
        r.reason = Some(
            if tip_unknown {
                SWEEP_TIP_UNKNOWN_REASON
            } else {
                SWEEP_TIP_MOVED_REASON
            }
            .to_string(),
        );
    }
}

/// The D2-OWED delta lens's D-H1 (the captain's ruling): the read's tip arm for a STORED claimable sweep whose hop the
/// INDEX holds unspent (the row's `status` fact, `pot_records`' word as the judge read it). Main's rule
/// (`index-unspent` and #808 (b)): the owner's own index-unspent stranded hop is pressable on the index's word; the
/// chain rung is a downgrade and a re-probe trigger, never a precondition of the honest press. So a block since the
/// judgement (or a tip that cannot be read) does not withhold it: the press stands and the move is STATED (`tipMoved`
/// or `tipUnknown`, `snapshotTip` the tip it was judged at, `chainProbeTip` the probe's own tip or else that one,
/// `servedTip`). A dead sweep costs a refused broadcast of the owner's own output, never an honest party's sats, and
/// heals on the next word. Answers `false` (nothing touched) for every other row: those take
/// [`degrade_stale_sweeps`].
pub fn state_index_unspent_sweep(
    facts: &mut Value,
    snapshot_tip: Option<u64>,
    present_tip: Option<u64>,
) -> bool {
    if facts["claimable"] != Value::Bool(true) || facts["status"] != HopStatus::Unspent.as_str() {
        return false;
    }
    match (snapshot_tip, present_tip) {
        (Some(t0), Some(tn)) if tn <= t0 => return true,
        (Some(_), Some(_)) => facts["tipMoved"] = json!(true),
        _ => facts["tipUnknown"] = json!(true),
    }
    if let Some(t0) = snapshot_tip {
        facts["snapshotTip"] = json!(t0);
        if facts["chainProbeTip"].is_null() {
            facts["chainProbeTip"] = json!(t0);
        }
    }
    if let Some(tn) = present_tip {
        facts["servedTip"] = json!(tn);
    }
    true
}

/// The `owed_rows` table (the overlay's migration 156 owns the DDL; this crate
/// issues the byte-identical catch-up). `facts` is the row's JSON.
pub const OWED_ROWS_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_rows (identity TEXT NOT NULL, outpoint TEXT NOT NULL, family TEXT NOT NULL, gameId TEXT NOT NULL, sats INTEGER, opponentIdentity TEXT, atHeight INTEGER, facts TEXT NOT NULL, updatedAtMs INTEGER NOT NULL, reason TEXT, PRIMARY KEY (identity, outpoint))";
/// The per-identity computed marker (migration 157): a never-computed identity
/// is COMPUTED on its first read, never served as "nothing owed".
pub const OWED_STATE_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_state (identity TEXT PRIMARY KEY, computedAtMs INTEGER NOT NULL, tip INTEGER, rows INTEGER NOT NULL, stale INTEGER NOT NULL DEFAULT 0, truncated INTEGER NOT NULL DEFAULT 0)";

/// bsv-low #566: the per-identity recompute sequence served on `/owed` (overlay migration 260, byte-identical; a cold
/// app-layer isolate adds it, `schema::LATCH_COLUMN_ALTERS`, because the snapshot's upsert and read name it).
pub const OWED_STATE_SEQ_ALTER: &str = "ALTER TABLE owed_state ADD COLUMN recomputeSeq INTEGER";

/// The gate's MEDIUM-1 (2026-09-19): an outpoint the pot-changed hook cannot attribute through decoded params (an
/// EVICTED pot's row is gone; a released HOP is a P2PKH row) is attributed through the seats' OWN markers — the
/// party rows naming the pot and the hop rows naming the hop — so an eviction re-derives both seats' owed rows at
/// once instead of at the next cadence (up to 5 minutes, with a live Rejoin press on a hand the network refused).
pub const OWED_ATTRIBUTE_BY_POT_SQL: &str = "SELECT DISTINCT identity FROM potparty_records WHERE potTxid = ?1 AND potVout = ?2 UNION SELECT identity FROM potparty_records_evicted WHERE potTxid = ?1 AND potVout = ?2";
pub const OWED_ATTRIBUTE_BY_HOP_SQL: &str =
    "SELECT DISTINCT identity FROM hopparty_records WHERE txid = ?1 AND hopVout = ?2";
/// Fleet loop 15 (2026-09-20, the review's MED-2): the REFUND-BACKUP filing names the pot too (`idx_potrefund_pot`) —
/// the one marker a seat killed at the funding still filed (its party marker never was), so a pot with no party
/// rows and no hop match still reaches its seats' owed rows on its confirm event.
pub const OWED_ATTRIBUTE_BY_POTREFUND_SQL: &str =
    "SELECT DISTINCT identity FROM potrefund_records WHERE potTxid = ?1 AND potVout = ?2";
/// MEDIUM-5: the probe of an identity with no state row: one with no party row and no hop row has nothing to judge
/// and gets NO state row written (a public read must not write state keyed on a claimable name).
pub const OWED_IDENTITY_PROBE_SQL: &str = "SELECT 1 AS present FROM potparty_records WHERE identity = ?1 UNION ALL SELECT 1 FROM hopparty_records WHERE identity = ?1 UNION ALL SELECT 1 FROM potparty_records_evicted WHERE identity = ?1 LIMIT 1";

/// The ONE service order (N2b), for the write and for the serve: the actionable families first, the newest spend
/// first inside a family, the outpoint as the tie-break. The write keeps the first `OWED_MAX_ROWS` of it (N2: a
/// planted-marker flood can derive thousands of `unbound` rows; the list is CUT after the actionable ones and says so).
pub fn sort_rows_for_service(rows: &mut [OwedRow]) {
    rows.sort_by(|a, b| {
        a.family
            .index()
            .cmp(&b.family.index())
            .then_with(|| b.at_height.unwrap_or(0).cmp(&a.at_height.unwrap_or(0)))
            .then_with(|| a.outpoint.cmp(&b.outpoint))
    });
}
/// The page bound (one probe row past it decides `truncated`).
pub const OWED_MAX_ROWS: usize = 500;

// ── counters (isolate-scoped, on /health like the filings') ─────────────────
/// bsv-low #628 (the observability bar, issue 4): the owed list's fault-shaped isolate counters, each mirrored on
/// the overlay's `ops_counters` through the courier census's batched flush (`courier::note_presence`). The
/// isolate fields on `/health.owed` stay the sample; these are the census.
pub const OWED_MIRRORED_COUNTERS: [&str; 9] = [
    "applayer_owed_recompute_faults_total",
    "applayer_owed_recompute_shed_total",
    "applayer_owed_recompute_deferred_abandoned_total",
    "applayer_owed_recompute_deferred_faulted_total",
    "applayer_owed_recompute_arm_failed_total",
    "applayer_owed_recompute_lock_takeovers_total",
    "applayer_owed_sweep_proof_contradictions_total",
    // bsv-low #800: a walk the claim dropped at its hard deadline; a walk not started for want of a slice
    "applayer_owed_recompute_walk_deadline_total",
    "applayer_owed_recompute_no_slice_total",
];
/// The owed reads whose faults are mirrored, each as `applayer_owed_read_faults_<kind>_total` beside the sum
/// `applayer_owed_read_faults_total` (the probe memo read keeps its own durable row,
/// [`COUNTER_PROBE_MEMO_READ_FAULTS`]).
pub const OWED_READ_FAULT_KINDS: [&str; 9] = [
    "spender",
    "withheld_previous",
    "spender_store",
    "collected",
    "pay_vout",
    "pot_spenders",
    "hop_sweeps",
    "sweep_proofs",
    "markerless",
];
fn mirror_read_fault(kind: &str) {
    crate::courier::note_presence("applayer_owed_read_faults_total");
    crate::courier::note_presence(&format!("applayer_owed_read_faults_{kind}_total"));
}
/// Every owed name this worker mirrors (the overlay seeds each at 0).
pub fn owed_mirrored_counter_names() -> Vec<String> {
    let mut names: Vec<String> = OWED_MIRRORED_COUNTERS
        .iter()
        .map(|n| n.to_string())
        .collect();
    // bsv-low #893 section 6.2: the walk that bumped these three (and the recompute rows above) is retired; their
    // durable rows stay seeded and served at the totals they reached.
    names.push("applayer_owed_recomputes_total".to_string());
    names.push("owed_refund_funding_unheld_total".to_string());
    names.push("owed_spender_rejudge_total".to_string());
    names.push("applayer_owed_read_faults_total".to_string());
    names.extend(
        OWED_READ_FAULT_KINDS
            .iter()
            .map(|k| format!("applayer_owed_read_faults_{k}_total")),
    );
    names
}
static SPENDER_STORE_SWEEP_PASSES: AtomicU64 = AtomicU64::new(0);
pub fn note_spender_store_sweep_pass() {
    SPENDER_STORE_SWEEP_PASSES.fetch_add(1, Ordering::Relaxed);
}
/// The merged lens's LOW-3 (2026-10-06): a probe memo read faulted (`routes::read_probe_memos`, all or nothing: the
/// answer is empty). It was a `console_warn` only, and it is not free: the hop walk gets no memo and no reorg
/// tombstone for that pass (every candidate is "never probed", and a fresh "confirmed" taken inside a real grace
/// is not marked `chainWordInReorgGrace`), the home walk gets no latch (every retired courier-proven row is back
/// for the pass) and no cursor (the ring is walked from its head). Always the safe direction, now counted: here
/// for the isolate (`/health.owed.probeMemoReadFaults`) and in the overlay's `ops_counters` under
/// [`COUNTER_PROBE_MEMO_READ_FAULTS`], served on the overlay's `/health/invariants.counters`.
static MARKERLESS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
static MARKERLESS_IGNORED_MARKERS: AtomicU64 = AtomicU64::new(0);
pub fn note_markerless_read_fault() {
    MARKERLESS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
    mirror_read_fault("markerless");
}
pub fn note_markerless_ignored_markers(n: usize) {
    MARKERLESS_IGNORED_MARKERS.fetch_add(n as u64, Ordering::Relaxed);
}

static PROBE_MEMO_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
pub fn note_probe_memo_read_fault() {
    PROBE_MEMO_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
}
/// The durable row of [`note_probe_memo_read_fault`] (the overlay seeds it at 0:
/// `bsv_overlay_cloudflare::hop_probe_memos::COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS`, pinned equal).
pub const COUNTER_PROBE_MEMO_READ_FAULTS: &str = "applayer_probe_memo_read_faults_total";

/// bsv-low #693 fold (the W1-OWED-693 lens's HIGH-1): THE PROOF LATCH. The proof set of one pass is filled only by
/// that pass's `/tx-any` answers, and a durable spender gets at most one re-ask a pass, only from a slot phase 2
/// left and never on a `budget_cut` pass, so a row the re-ask confirmed on pass N read "waiting for the block" again
/// on the next pass that had no slot (or re-asked another spender). A chaintracks-checked mined proof of a spender,
/// once in hand on any pass, is therefore LATCHED durably: a `hop_chain_probes` row keyed
/// `proofmined:<spenderTxid>.<height>` (the memo key's shape, so the `homeproof:` range read serves it byte-free),
/// `spent = 1`, `spendingTxid` the canonical merkle ROOT the proof was checked against at that height,
/// `spentConfirmed = 1`. It holds the height and the root's identity, never bytes. It is written in the snapshot's
/// atomic batch and read each pass before the pass's own answers ([`latched_proof_mined_spenders`]), so the row is
/// served from the latch on every later pass. A reorg DEMOTES it: the overlay's reorg clear deletes every
/// `spentConfirmed = 1` row outside the `homeproof:`/`homecursor:` prefixes (this prefix is NOT spared by it), and
/// the TTL sweep spares it (`bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES`,
/// pinned equal); and a latch whose root is no longer the canonical root at its height is not honoured.
pub const PROOF_MINED_LATCH_PREFIX: &str = "proofmined:";
/// The FEWEST latch heights one pass re-reads against chaintracks (a header each, cached per isolate): the newest
/// four, whatever their depth (bsv-low #693 fold-2, the W1-OWED-693 re-lens's R-HIGH-1 (1)).
pub const OWED_PROOF_LATCH_RECHECKS_PER_RECOMPUTE: usize = 4;
/// bsv-low #693 fold-2 (the re-lens's R-HIGH-1 (1)): the REORG WINDOW of a proof latch, in blocks. A latch less than
/// this deep below the served tip is reorg-reachable and its height is re-read every pass (newest first); a latch
/// this deep or deeper is reorg-proof and needs no recheck. Twice the overlay's revalidation window
/// (`bsv_overlay_cloudflare::tip_pass::REORG_SWEEP_DEPTH`, 3, the reference's `reorgScanDepth`; pinned at least
/// twice it from the tests). The rechecks of one pass are therefore at most `max(LATCH_RECHECK_DEPTH,
/// OWED_PROOF_LATCH_RECHECKS_PER_RECOMPUTE)` header reads.
pub const LATCH_RECHECK_DEPTH: u64 = 6;

/// bsv-low #693 fold-2 (the re-lens's R-HIGH-1 (2), (3)): the oldest an UNRECHECKED, reorg-reachable latch may be
/// and still be honoured: the two hours the confirmed memos had (`hops_view::PROBE_MEMO_CONFIRMED_MAX_AGE_MS`, the
/// overlay's `HOP_PROBE_MEMO_TTL_MS`). Past it, with no successful recheck this pass, the latch is not honoured and
/// its spender is re-asked; a new proof re-latches it with a new write time. The TTL sweep still spares the row.
pub const PROOF_LATCH_UNRECHECKED_MAX_AGE_MS: i64 =
    crate::hops_view::PROBE_MEMO_CONFIRMED_MAX_AGE_MS;

/// One proof latch read back: the spender, the height and the root it was checked against, and when it was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofLatch {
    pub spender: String,
    pub height: u64,
    pub root: String,
    pub written_at_ms: i64,
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// PURE: the range binds of each spender's `proofmined:` latches, for [`home_latch_range_read_sql`] (the same
/// indexed PK range read; `/` is the byte after `.`, so the range is exactly `proofmined:<spender>.<height>`), and
/// one range more, the overlay's reorg TOMBSTONE (`reorg-clear.0` exactly; bsv-low #693 fold-2, the re-lens's
/// R-HIGH-1 (2)), so the latches and the time of the last clear arrive in one read, together or not at all, as the
/// memo walk reads them (`hops_view::reorg_clear_at`). One range per spender plus one: [`HOME_LATCH_READ_CHUNK`]
/// spenders stay under D1's bound limit.
pub fn proof_latch_range_binds(spenders: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(spenders.len() * 2 + 2);
    for sp in spenders {
        let sp = sp.to_ascii_lowercase();
        out.push(format!("{PROOF_MINED_LATCH_PREFIX}{sp}."));
        out.push(format!("{PROOF_MINED_LATCH_PREFIX}{sp}/"));
    }
    out.push(crate::hops_view::PROBE_MEMO_REORG_MARK.to_string());
    // the mark is `reorg-clear.0`: the range up to `reorg-clear.1` (exclusive) holds it and no other key
    out.push("reorg-clear.1".to_string());
    out
}
const _: () = assert!(2 * (HOME_LATCH_READ_CHUNK + 1) <= crate::logic::D1_MAX_BOUND_PARAMS);

/// PURE: the proof latches the range read returned for the named spenders. Only a well-formed latch counts: the key
/// `proofmined:<spender>.<height>`, `spent`, `spentConfirmed = 1` and a 64-hex root.
pub fn proof_latches_of(
    named_spenders: &[String],
    memos: &[crate::hops_view::ProbeMemo],
) -> Vec<ProofLatch> {
    let mut out = Vec::new();
    for sp in named_spenders {
        let sp = sp.to_ascii_lowercase();
        let prefix = format!("{PROOF_MINED_LATCH_PREFIX}{sp}.");
        for m in memos {
            let Some(height) = m
                .outpoint
                .strip_prefix(&prefix)
                .and_then(|h| h.parse::<u64>().ok())
            else {
                continue;
            };
            let Some(root) = m.spending_txid.as_deref().filter(|r| is_hex64(r)) else {
                continue;
            };
            if m.spent && m.spent_confirmed == Some(true) {
                out.push(ProofLatch {
                    spender: sp.clone(),
                    height,
                    root: root.to_ascii_lowercase(),
                    written_at_ms: m.probed_at_ms,
                });
            }
        }
    }
    out
}

/// PURE: the latch heights a pass re-reads against chaintracks, NEWEST first (the only ones a reorg can move; the
/// re-lens's R-HIGH-1 (1): the oldest-first bound rechecked the four deepest, reorg-proof heights every pass and never
/// the newest). Every height less than [`LATCH_RECHECK_DEPTH`] below the tip, and at least the
/// [`OWED_PROOF_LATCH_RECHECKS_PER_RECOMPUTE`] newest. `tip` is the served tip (`None`: unreadable); the tip is never
/// below the newest latched height, so that height stands in for an unreadable or stale tip (the window then
/// counts from it, the conservative side).
pub fn proof_latch_recheck_heights(latches: &[ProofLatch], tip: Option<u64>) -> Vec<u64> {
    let mut h: Vec<u64> = latches.iter().map(|l| l.height).collect();
    h.sort_unstable_by(|a, b| b.cmp(a));
    h.dedup();
    let tip = latch_tip(latches, tip);
    h.into_iter()
        .enumerate()
        .take_while(|(i, height)| {
            *i < OWED_PROOF_LATCH_RECHECKS_PER_RECOMPUTE || !latch_is_deep(*height, tip)
        })
        .map(|(_, height)| height)
        .collect()
}

/// PURE: the tip a latch's depth counts from: the served tip, never below the newest latched height.
pub fn latch_tip(latches: &[ProofLatch], tip: Option<u64>) -> u64 {
    latches
        .iter()
        .map(|l| l.height)
        .chain(tip)
        .max()
        .unwrap_or(0)
}

/// PURE: a latch at `height` is REORG-PROOF under `tip`: [`LATCH_RECHECK_DEPTH`] blocks deep or deeper.
pub fn latch_is_deep(height: u64, tip: u64) -> bool {
    tip.saturating_sub(height) >= LATCH_RECHECK_DEPTH
}

/// PURE: the spenders whose latch is HONOURED this pass, the seed of the pass's `proof_mined_spenders` (before its
/// own answers). `canonical` is chaintracks' root at each re-read height (`None`: the header read faulted). A latch
/// whose height was re-read THIS pass is honoured exactly when its root is the canonical one (a reorg moved the
/// block: the row waits again until a new proof, which latches at its new height). A latch NOT re-read this pass
/// (its header read faulted, the pass was `over_budget`, or it is past the rechecks) is honoured only on a CLEAN
/// CLEAR HISTORY (bsv-low #693 fold-2, the re-lens's R-HIGH-1 (2), (3); the #484 grace rule,
/// `hops_view::in_reorg_grace`): written at or after `reorg_clear_at_ms + PROBE_MEMO_REORG_GRACE_MS`. A latch
/// written before a clear survived a clear that faulted, and one written inside the grace after it may carry a root
/// checked before the reorg (a pass's proof outlives the clear by up to its 30 s header cache and 12 s clock): both
/// wait for a recheck. With that history it is honoured when reorg-proof ([`latch_is_deep`] under `tip`), or when
/// written more than the grace ago and less than [`PROOF_LATCH_UNRECHECKED_MAX_AGE_MS`] ago; past two hours
/// unrechecked it is re-asked (a clear that faulted with no tombstone leaves a latch for two hours, not for ever).
/// Not honoured is display only: the row reads "waiting for the block" one pass more, never `claimable`.
pub fn latched_proof_mined_spenders(
    latches: &[ProofLatch],
    canonical: &HashMap<u64, Option<String>>,
    tip: Option<u64>,
    reorg_clear_at_ms: Option<i64>,
    now_ms: i64,
) -> HashSet<String> {
    let tip = latch_tip(latches, tip);
    latches
        .iter()
        .filter(|l| match canonical.get(&l.height) {
            Some(Some(root)) => root.eq_ignore_ascii_case(&l.root),
            Some(None) | None => unrechecked_latch_honoured(l, tip, reorg_clear_at_ms, now_ms),
        })
        .map(|l| l.spender.clone())
        .collect()
}

/// bsv-low #693 fold-3 (the W1-OWED-693 delta lens's R2-HIGH-1 (a)): the latches a pass DELETES, in its snapshot's
/// atomic batch (ONE statement, one bind: the JSON array of `[key, root]` pairs, [`moved_proof_latch_delete_bind`]).
/// A row is taken only while it still holds the root the recheck refused, so a fresh latch a concurrent pass wrote at
/// the same key with another root stands. PK-indexed (`outpoint=?`, pinned in `d1_fleet_plans`).
pub const PROOF_LATCH_MOVED_DELETE_SQL: &str = "DELETE FROM hop_chain_probes WHERE (outpoint, spendingTxid) IN (SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?1))";

/// PURE: the latches whose height was re-read THIS pass and whose root is no longer chaintracks' root there (a reorg
/// moved the block): `(key, root)`, the key `proofmined:<spender>.<height>` as written. The refusal is made DURABLE by
/// deleting them (R2-HIGH-1 (a)): a refusal that only filtered held for that pass, and once the latch was
/// [`LATCH_RECHECK_DEPTH`] deep and outside the newest rechecks (or its header read faulted, or the pass was
/// `over_budget`) it was honoured again by depth. A DELETE, not a `proofmoved:` tombstone: it is what the overlay's
/// reorg clear does to a confirmed memo (rule 1), it needs no new prefix in the TTL sweep's exemptions and no new
/// arm in the read, and the absence of a latch already means "re-ask" (`durable_spender_needs_proof`); a later
/// checked proof writes a fresh latch at its new height. A faulted header read (`None`) deletes nothing.
pub fn moved_proof_latches(
    latches: &[ProofLatch],
    canonical: &HashMap<u64, Option<String>>,
) -> Vec<(String, String)> {
    latches
        .iter()
        .filter(|l| {
            matches!(canonical.get(&l.height), Some(Some(root)) if !root.eq_ignore_ascii_case(&l.root))
        })
        .filter_map(|l| {
            let height = u32::try_from(l.height).ok()?;
            Some((
                crate::hops_view::probe_memo_key(
                    &format!("{PROOF_MINED_LATCH_PREFIX}{}", l.spender),
                    height,
                ),
                l.root.clone(),
            ))
        })
        .collect()
}

/// PURE: the one bind of [`PROOF_LATCH_MOVED_DELETE_SQL`]; `None` when nothing moved (no statement is added).
pub fn moved_proof_latch_delete_bind(moved: &[(String, String)]) -> Option<String> {
    if moved.is_empty() {
        return None;
    }
    let pairs: Vec<[&str; 2]> = moved
        .iter()
        .map(|(k, r)| [k.as_str(), r.as_str()])
        .collect();
    serde_json::to_string(&pairs).ok()
}

/// PURE: the rule [`latched_proof_mined_spenders`] applies to a latch whose height was not re-read this pass.
pub fn unrechecked_latch_honoured(
    l: &ProofLatch,
    tip: u64,
    reorg_clear_at_ms: Option<i64>,
    now_ms: i64,
) -> bool {
    if crate::hops_view::in_reorg_grace(l.written_at_ms, reorg_clear_at_ms) {
        return false;
    }
    if latch_is_deep(l.height, tip) {
        return true;
    }
    let age = now_ms.saturating_sub(l.written_at_ms);
    (crate::hops_view::PROBE_MEMO_REORG_GRACE_MS..PROOF_LATCH_UNRECHECKED_MAX_AGE_MS).contains(&age)
}

pub fn owed_health_json() -> Value {
    json!({
        "countersScope": "isolate",
        "markerlessReadFaults": MARKERLESS_READ_FAULTS.load(Ordering::Relaxed),
        "markerlessIgnoredMarkers": MARKERLESS_IGNORED_MARKERS.load(Ordering::Relaxed),
        "probeMemoReadFaults": PROBE_MEMO_READ_FAULTS.load(Ordering::Relaxed),
        "spenderStoreSweepPasses": SPENDER_STORE_SWEEP_PASSES.load(Ordering::Relaxed),
        "hopStrandedAfterMs": HOP_STRANDED_AFTER_MS,
    })
}

/// The `owed-changed` event body, pushed to the identity's durable box after a
/// recompute a change triggered (the page re-reads `/owed` on it).
pub fn owed_changed_event_body(
    identity: &str,
    source: &str,
    rows: usize,
    computed_at_ms: i64,
) -> Value {
    json!({
        "v": 1,
        "kind": "owed-changed",
        "identity": identity.to_ascii_lowercase(),
        "recipient": identity.to_ascii_lowercase(),
        "source": source,
        "rows": rows,
        "computedAtMs": computed_at_ms,
    })
}

/// bsv-low #638: valid backup bytes for a chunk of pot outpoints.
pub fn valid_refunds_sql(n: usize) -> String {
    let pairs = (0..n)
        .map(|i| format!("(potTxid = ?{} AND potVout = ?{})", 2 * i + 1, 2 * i + 2))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("SELECT potTxid, potVout, refundRawHex FROM potrefund_records WHERE refundValid = 1 AND refundRawHex IS NOT NULL AND ({pairs}) ORDER BY createdAt DESC, rowid DESC LIMIT {}", n * 4)
}

#[cfg(test)]
pub(crate) mod tests {
    /// The derived rows' facts the pins count (bsv-low #893: the counters the retired walk bumped from them are
    /// gone; the facts are the derivation's and are pinned here).
    pub(crate) fn count_swept_confirmed_by_proof(rows: &[OwedRow]) -> u64 {
        rows.iter()
            .filter(|r| r.facts.get("confirmedSource").and_then(|v| v.as_str()) == Some("proof"))
            .count() as u64
    }
    pub(crate) fn count_refund_funding_unheld(rows: &[OwedRow]) -> u64 {
        rows.iter()
            .filter(|r| {
                r.facts.get("claim").and_then(|v| v.as_str()) == Some(FUNDING_NOT_HELD_CLAIM)
            })
            .count() as u64
    }
    pub(crate) fn count_sweep_proof_contradictions(rows: &[OwedRow]) -> u64 {
        rows.iter()
            .filter(|r| {
                r.facts
                    .get("sweepProofContradicted")
                    .and_then(|v| v.as_bool())
                    == Some(true)
            })
            .count() as u64
    }
    /// The eviction ledger's released spends (`pot_evictions.releasedSpends`) as the pins hand them to the
    /// derivation: the hop outpoints a refused JOIN released, keyed on the UTXO (the judge reads the same column by
    /// `json_each`, `owed_judge::hydrate`; this is the pins' fixture form, the walk's parse it replaced retired with
    /// the walk, bsv-low #893 section 6.2).
    pub(crate) fn released_hop_outpoints(
        evictions: &[(String, Option<String>)],
    ) -> HashSet<String> {
        let mut out = HashSet::new();
        for (_txid, released) in evictions {
            let Some(raw) = released.as_deref() else {
                continue;
            };
            let Ok(Value::Array(entries)) = serde_json::from_str::<Value>(raw) else {
                continue;
            };
            for e in entries {
                let (Some(txid), Some(vout)) = (
                    e.get("txid").and_then(Value::as_str),
                    e.get("vout").and_then(Value::as_u64),
                ) else {
                    continue;
                };
                if txid.len() == 64 && txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                    out.insert(outpoint_key(txid, vout as u32));
                }
            }
        }
        out
    }
    /// (my hop outpoint, the evicted txid that released it), the same ledger rows keyed the same way.
    pub(crate) fn released_hops_by_eviction(
        evictions: &[(String, Option<String>)],
        hops: &[HopEntry],
    ) -> Vec<(String, String)> {
        let mine: HashSet<String> = hops
            .iter()
            .map(|h| outpoint_key(&h.hop_txid, h.hop_vout))
            .collect();
        let mut out: Vec<(String, String)> = Vec::new();
        for (evicted, released) in evictions {
            if evicted.len() != 64 || !evicted.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            let Some(raw) = released.as_deref() else {
                continue;
            };
            let Ok(Value::Array(entries)) = serde_json::from_str::<Value>(raw) else {
                continue;
            };
            for e in entries {
                let (Some(txid), Some(vout)) = (
                    e.get("txid").and_then(Value::as_str),
                    e.get("vout").and_then(Value::as_u64),
                ) else {
                    continue;
                };
                if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                    continue;
                }
                let key = outpoint_key(txid, vout as u32);
                if mine.contains(&key) {
                    out.push((key, evicted.to_ascii_lowercase()));
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }
    /// The door's refusal ledger rows (hopTxid, hopVout, reason) of the identity's own hops, by outpoint, as the
    /// judge's hydration builds `door_refused_hop_outpoints` (`owed_judge::hydrate`, `DoorRefusal::of_reason`).
    pub(crate) fn door_refused_hops(
        rows: &[(String, u32, String)],
        hops: &[HopEntry],
    ) -> HashMap<String, DoorRefusal> {
        let mine: HashSet<String> = hops
            .iter()
            .map(|h| outpoint_key(&h.hop_txid, h.hop_vout))
            .collect();
        let mut out: HashMap<String, DoorRefusal> = HashMap::new();
        for (txid, vout, reason) in rows {
            let key = outpoint_key(txid, *vout);
            if mine.contains(&key) {
                out.insert(key, DoorRefusal::of_reason(reason));
            }
        }
        out
    }
    /// The spenders a hop names (the index's pointer, else the chain rung's), deduplicated: what the belt asks.
    fn hop_named_spenders(i: &OwedInputs) -> Vec<String> {
        let mut out: Vec<String> = i
            .hops
            .iter()
            .filter_map(|h| hop_named_spender(i, h))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    use super::*;
    fn derive_and_pin(i: &OwedInputs<'_>) -> Vec<OwedRow> {
        crate::owed_judge::assert_decomposition(i);
        crate::owed_judge::assert_fixture_roundtrip(i);
        super::derive_owed_rows(i)
    }

    #[test]
    fn owed893_decomposition_a_pot_named_by_two_games_has_one_representative() {
        let first = entry(Some(false), None, Outcome::Unresolved, None);
        let mut second = first.clone();
        second.game_id = tx(0x77);
        let entries = [first, second];
        let i = inputs(&entries, &[], &[], &NO_REFUNDS, &NONE, &NONE, Some(900_200));
        crate::owed_judge::assert_decomposition(&i);
    }
    #[test]
    fn owed893_decomposition_keeps_a_linked_pots_representative_as_context() {
        let e = [entry(Some(false), None, Outcome::Unresolved, None)];
        let mut h = hop(crate::hops_view::HopStatus::Unspent, None, Some(1));
        h.game_id = tx(0x77);
        let links = HashMap::from([(outpoint_key(&h.hop_txid, h.hop_vout), e[0].pot_txid.clone())]);
        let hops = [h];
        let mut i = inputs(&e, &[], &hops, &NO_REFUNDS, &NONE, &NONE, Some(900_200));
        i.join_spent_hops = &links;
        crate::owed_judge::assert_decomposition(&i);
    }

    static NO_REFUNDS: std::sync::LazyLock<HashMap<String, ValidRefund>> =
        std::sync::LazyLock::new(HashMap::new);

    use crate::hops_view::MarkerVerification;
    use crate::refund_view::RefundStatus;
    use crate::results::{MoneyFacts, PotBinding, TxMoneyFacts};

    pub(crate) const ME: &str =
        "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OPP: &str = "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    pub(crate) fn tx(b: u8) -> String {
        hex::encode([b; 32])
    }

    pub(crate) fn entry(
        spent: Option<bool>,
        verdict: Option<PotVerdict>,
        outcome: Outcome,
        my_seat: Option<SeatLetter>,
    ) -> ResultEntry {
        ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 900_000,
            cov_recovery_height: Some(900_000),
            opponent_identity: OPP.to_string(),
            settle_txid: if spent == Some(true) {
                Some(tx(0x03))
            } else {
                None
            },
            spent,
            spent_confirmed: spent,
            pot_binding: PotBinding::Chain,
            game_id_binding: PotBinding::Chain,
            verdict,
            settle_signers: Some("coop".into()),
            outcome,
            outcome_source: Some("chain+seatkey"),
            at_height: if spent == Some(true) {
                Some(900_100)
            } else {
                None
            },
            winner_hand: None,
            marker_hands: Default::default(),
            hands_source: None,
            committed_keys: None,
            money: MoneyFacts {
                funding: None,
                settle: if spent == Some(true) {
                    Some(TxMoneyFacts {
                        txid: tx(0x03),
                        size_bytes: Some(3577),
                        fee_sats: Some(400),
                        pay_a_sats: Some(39_200),
                        pay_b_sats: Some(0),
                    })
                } else {
                    None
                },
                hops: Vec::new(),
            },
            my_seat,
            stake_sats: my_seat.map(|_| 20_000),
        }
    }

    /// bsv-low #736: the payout of a settle the index has proven (`spentConfirmed`) with NO stored height (an
    /// unparseable bump) is claimable whatever the settle's word says, as with no word row; the `awaiting_block`
    /// sentence of an unclassified spend reads the same proof. `ns3_m2` above holds only with a height.
    #[test]
    fn i736_a_heightless_index_proof_is_read_before_the_settles_word() {
        let mut won = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        won.at_height = None;
        let mut unclassified = entry(Some(true), None, Outcome::Unresolved, Some(SeatLetter::A));
        unclassified.at_height = None;
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let judge = |e: &ResultEntry, words: &std::collections::BTreeMap<String, Value>| {
            let entries = [e.clone()];
            let mut i = inputs(&entries, &[], &[], &v, &c, &p, Some(900_200));
            i.spender_words = Some(words);
            let mut row = serde_json::to_value(&derive_and_pin(&i)[0]).unwrap();
            let facts = row["facts"].as_object_mut().unwrap();
            facts.remove("coinWord");
            facts.remove("spenderWord");
            row
        };
        let none = std::collections::BTreeMap::new();
        for word in [
            json!({"word":"mined","verifyPath":null,"cannotCheck":"header snapshot changed"}),
            json!({"word":"stale","verifyPath":null}),
            json!({"word":"mined","verifyPath":null}),
        ] {
            let words = std::collections::BTreeMap::from([(tx(3), word.clone())]);
            let (base, judged) = (judge(&won, &none), judge(&won, &words));
            assert_eq!(base["facts"]["claimable"], true);
            assert_eq!(
                judged, base,
                "the payout under {word} is the payout with no word row"
            );
            let (base, judged) = (judge(&unclassified, &none), judge(&unclassified, &words));
            assert_eq!(
                base["reason"], "the spend is not classified yet (no verdict)",
                "the index's proof is no block wait"
            );
            assert_eq!(
                judged, base,
                "the unclassified row under {word} is the row with no word row"
            );
        }
        // a courier never reaches this field: `spent_confirmed` here is the index's latch. With no index proof the
        // word still decides, as before #736
        let mut unproven = won.clone();
        unproven.spent_confirmed = Some(false);
        let words = std::collections::BTreeMap::from([(
            tx(3),
            json!({"word":"mined","verifyPath":null,"cannotCheck":"header snapshot changed"}),
        )]);
        assert_eq!(judge(&unproven, &words)["facts"]["claimable"], false);
    }

    #[test]
    fn ns3_m2_index_proof_survives_missing_row_and_cannot_check() {
        let entries = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        for words in [
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::from([(
                tx(3),
                json!({"word":"mined","verifyPath":null,"cannotCheck":"header snapshot changed"}),
            )]),
        ] {
            let mut i = inputs(&entries, &[], &[], &v, &c, &p, Some(900_200));
            i.spender_words = Some(&words);
            assert_eq!(
                derive_and_pin(&i)[0].facts["claimable"],
                true,
                "index verified proof first"
            );
        }
    }

    /// NS-3 (bsv-low #736 folded this pin, which asserted the veto): with the index's latch and no height the settle
    /// payout is claimable under a checked `seen`, `stale` or `cannotCheck` word; without the latch the word decides.
    #[test]
    fn ns3_owed_index_latch_claims_under_any_word_and_the_word_controls_without_it() {
        let mut latched = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        latched.at_height = None;
        let mut unlatched = latched.clone();
        unlatched.spent_confirmed = Some(false);
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let derive = |e: &ResultEntry, words: &std::collections::BTreeMap<String, Value>| {
            let entries = [e.clone()];
            let mut i = inputs(&entries, &[], &[], &v, &c, &p, Some(900_200));
            i.spender_words = Some(words);
            derive_and_pin(&i)[0].facts.clone()
        };
        let mut state = bsv_tracker::State::new(tx(3));
        let headers = crate::tracker_host::HeaderSnapshot {
            tip: Ok(900_100),
            headers: std::collections::BTreeMap::from([(
                900_100,
                Ok(Some(bsv_tracker::Header {
                    hash: tx(4),
                    merkle_root: tx(3),
                })),
            )]),
        };
        let params = bsv_tracker::Params { age_threshold: 60 };
        state
            .step(
                &params,
                &headers,
                bsv_tracker::Input::Host(bsv_tracker::HostAction::Build),
            )
            .unwrap();
        state
            .step(
                &params,
                &headers,
                bsv_tracker::Input::Hint(bsv_tracker::Hint::new(
                    "arcade",
                    bsv_tracker::HintStatus::Seen,
                    123,
                )),
            )
            .unwrap();
        let seen = std::collections::BTreeMap::from([(
            tx(3),
            crate::tracker_host::served::fields(&state),
        )]);
        // (a) the index's latch, no height: claimable under the checked `seen` word, the word still served as it says
        let facts = derive(&latched, &seen);
        assert_eq!(
            facts["claimable"], true,
            "a seen word vetoed the index's latch"
        );
        assert_eq!(facts["spentConfirmed"], true);
        assert_eq!(facts["spenderWord"]["word"], "seen");
        assert_eq!(facts["coinWord"]["word"], "pending");
        for word in [
            json!({"word":"stale","verifyPath":null}),
            json!({"word":"mined","verifyPath":null,"cannotCheck":"header snapshot changed"}),
        ] {
            let words = std::collections::BTreeMap::from([(tx(3), word.clone())]);
            assert_eq!(
                derive(&latched, &words)["claimable"],
                true,
                "{word} vetoed the index's latch"
            );
        }
        // (b) no latch: the word controls, as the pin asserted before #736
        let facts = derive(&unlatched, &seen);
        assert_eq!(facts["claimable"], false, "a seen word with no index latch");
        assert_eq!(facts["spentConfirmed"], false);
        assert_eq!(facts["spenderWord"]["word"], "seen");
        state
            .step(
                &params,
                &headers,
                bsv_tracker::Input::Evidence(bsv_tracker::Evidence::Proof(
                    bsv_tracker::Proof::new(
                        tx(3),
                        bsv_rs::transaction::MerklePath::from_coinbase_txid(&tx(3), 900_100),
                    )
                    .unwrap(),
                )),
            )
            .unwrap();
        let mined = std::collections::BTreeMap::from([(
            tx(3),
            crate::tracker_host::served::fields(&state),
        )]);
        assert_eq!(derive(&unlatched, &mined)["claimable"], true);
        assert_eq!(derive(&latched, &mined)["claimable"], true);
        let checked = &mined[&tx(3)];
        assert_eq!(
            spender_coin_word(checked, &tx(3), Some(&tx(5)))["word"],
            "lost"
        );
        assert_eq!(
            spender_coin_word(&seen[&tx(3)], &tx(3), Some(&tx(5)))["word"],
            "pending"
        );
    }

    /// The gate of 2026-09-20 (M1): a hop spender whose bytes carry a POT covenant output is a JOIN the index does
    /// not hold — never the custody story, on the index pointer and on the chain rung alike.
    #[test]
    fn a_spender_with_a_pot_covenant_output_is_an_unindexed_pot_never_spent_elsewhere() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let join = tx(0x0c);
        let mut m: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        m.insert(
            join.clone(),
            vec![
                SpenderOutput {
                    vout: 0,
                    pkh_hex: None,
                    sats: 40_000,
                    spent: None,
                    pot_lock: true,
                },
                // the change pays MY home: a pot-creating tx is still a JOIN, never a 190-sat "sweep" (NEW-3)
                SpenderOutput {
                    vout: 1,
                    pkh_hex: Some("cc".repeat(20)),
                    sats: 190,
                    spent: None,
                    pot_lock: false,
                },
            ],
        );
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "cc".repeat(20));
        // 1. the index pointer names the JOIN (status Spent)
        let hops = [hop(HopStatus::Spent, Some(&join), Some(10_000_000))];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &m;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].facts["spendKind"], "pot-unindexed");
        assert_eq!(rows[0].reason.as_deref(), Some(POT_UNINDEXED_REASON));
        assert!(rows[0].facts.get("claim").is_none_or(|c| c.is_null()));
        // 2. the chain rung names it (the index says unspent)
        let old = [hop(HopStatus::Unspent, None, Some(10_000_000))];
        let by_chain = chain_confirmed(
            &format!("{}:0", tx(0x07)),
            true,
            Some(true),
            Some(&join),
            Some(true),
        );
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &m;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["spendKind"], "pot-unindexed");
        assert_eq!(rows[0].reason.as_deref(), Some(POT_UNINDEXED_REASON));
    }

    /// Fleet loop 11, the wave's batch 3 (`spec-admit-fast-join-refused`): the JOIN the network refused was EVICTED
    /// with the seats' party rows, so the results view holds no entry and the hop it spent read "cannot tell which
    /// committed home is yours". The ledger names the hop the JOIN spent (the UTXO), the twin holds the lock's
    /// decode, my VERIFIED hop marker names my settle key: the home follows and the hop's real spender is judged.
    #[test]
    fn a_refused_joins_committed_home_is_read_from_the_twin_keyed_on_the_released_hop_so_its_spender_is_judged(
    ) {
        use crate::results::CommittedKeys;
        let pub_a = format!("02{}", "aa".repeat(32));
        let pub_b = format!("03{}", "bb".repeat(32));
        let keys = CommittedKeys {
            pub_a: pub_a.clone(),
            pub_b: pub_b.clone(),
            pay_pkh_a: "cc".repeat(20),
            pay_pkh_b: "dd".repeat(20),
        };
        let join = tx(0x0c); // the refused JOIN (evicted: its pot row and the party rows live in the twins)
        let sweep = tx(0x0d); // what took the hop instead (courier-read bytes, pays elsewhere)
        let released_json = |hop_txid: &str| {
            Some(format!(
                r#"[{{"table":"pot_records","txid":"{hop_txid}","vout":0}}]"#
            ))
        };
        let ledger = [(join.clone(), released_json(&tx(0x07)))];
        let my_hop = |key: &str| {
            let mut h = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
            h.seat_settle_pubkey = key.to_string();
            h
        };
        let hops = [my_hop(&pub_a.to_ascii_uppercase())]; // case-insensitive on the key
        let released = released_hops_by_eviction(&ledger, &hops);
        assert_eq!(released, vec![(format!("{}:0", tx(0x07)), join.clone())]);
        let twin = [(join.to_ascii_uppercase(), 0u32, keys.clone())];
        let homes = evicted_pot_homes(&released, &hops, &twin);
        assert_eq!(homes, vec![(tx(0x01), "cc".repeat(20))]);
        // through THE derivation: the hop's spender pays elsewhere → spent-elsewhere, never "cannot tell which home"
        let pkhs: HashMap<String, String> = homes.into_iter().collect();
        let elsewhere = pays(&sweep, &[(0, &"99".repeat(20), 20_000, Some(false))]);
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        // without the twin's word nothing is named (a faulted read) and the pre-change sentence stands
        assert!(evicted_pot_homes(&released, &hops, &[]).is_empty());
        let none: HashMap<String, String> = HashMap::new();
        i.my_pkh_by_game = &none;
        assert_eq!(
            derive_and_pin(&i)[0].reason.as_deref(),
            Some(HOME_UNKNOWN_REASON)
        );
        // THE KEY IS THE RELEASED HOP UTXO, never a name: an eviction that released a stranger's hop names nothing,
        // and a malformed ledger row contributes nothing
        assert!(
            released_hops_by_eviction(&[(join.clone(), released_json(&tx(0x08)))], &hops)
                .is_empty()
        );
        assert!(released_hops_by_eviction(
            &[
                (join.clone(), None),
                ("zz".repeat(32), released_json(&tx(0x07))),
                (join.clone(), Some("nope".into()))
            ],
            &hops
        )
        .is_empty());
        // the seat must be PROVEN by the hop's own attested key
        let mut unverified = my_hop(&pub_a);
        unverified.marker_verified = MarkerVerification::Unverified;
        assert!(evicted_pot_homes(&released, &[unverified], &twin).is_empty());
        assert!(evicted_pot_homes(
            &released,
            &[my_hop(&format!("02{}", "ee".repeat(32)))],
            &twin
        )
        .is_empty());
        assert!(evicted_pot_homes(&released, &[my_hop("")], &twin).is_empty());
        let both = [(
            join.clone(),
            0u32,
            CommittedKeys {
                pub_b: pub_a.clone(),
                ..keys.clone()
            },
        )];
        assert!(evicted_pot_homes(&released, &hops, &both).is_empty());
        let split = [
            twin[0].clone(),
            (
                join.clone(),
                1u32,
                CommittedKeys {
                    pay_pkh_a: "ee".repeat(20),
                    ..keys.clone()
                },
            ),
        ];
        assert!(evicted_pot_homes(&released, &hops, &split).is_empty());
        // two refused JOINs of ONE game: agreeing twins name the home once; disagreeing twins name nothing
        let join2 = tx(0x0e);
        let ledger2 = [ledger[0].clone(), (join2.clone(), released_json(&tx(0x09)))];
        let mut second = my_hop(&pub_a);
        second.hop_txid = tx(0x09);
        let two = [my_hop(&pub_a), second];
        let released2 = released_hops_by_eviction(&ledger2, &two);
        assert_eq!(released2.len(), 2);
        let agree = [twin[0].clone(), (join2.clone(), 0u32, keys.clone())];
        assert_eq!(
            evicted_pot_homes(&released2, &two, &agree),
            vec![(tx(0x01), "cc".repeat(20))]
        );
        let disagree = [
            twin[0].clone(),
            (
                join2,
                0u32,
                CommittedKeys {
                    pay_pkh_a: "ee".repeat(20),
                    ..keys.clone()
                },
            ),
        ];
        assert!(evicted_pot_homes(&released2, &two, &disagree).is_empty());
        // seat B by the same rule
        assert_eq!(
            evicted_pot_homes(&released, &[my_hop(&pub_b)], &twin),
            vec![(tx(0x01), "dd".repeat(20))]
        );
    }

    /// The E606 lens's M1 (bsv-low #605): a CONFLICT eviction re-points my hop at the confirmed competitor BEFORE the
    /// release, and the overlay's ledger now still names that hop beside the released ones (`releasedSpends`, the
    /// overlay's `e605_a`). Once the chaser latches the pointer (`Spent`, the competitor) the home follows from the
    /// twin and the story is "spent outside this game". The ledger as `7b285f8` wrote it (the other hop alone) names
    /// no home for the game: "cannot match to a home", the word the cell's bar would miss.
    #[test]
    fn e606_m1_a_conflict_evicted_hand_reads_spent_outside_this_game() {
        use crate::results::CommittedKeys;
        let pub_a = format!("02{}", "aa".repeat(32));
        let keys = CommittedKeys {
            pub_a: pub_a.clone(),
            pub_b: format!("03{}", "bb".repeat(32)),
            pay_pkh_a: "cc".repeat(20),
            pay_pkh_b: "dd".repeat(20),
        };
        let (join, competitor, other_hop) = (tx(0x0c), tx(0x0d), tx(0x08));
        let mut mine = hop(HopStatus::Spent, Some(&competitor), Some(10_000_000));
        mine.seat_settle_pubkey = pub_a;
        let hops = [mine];
        let entry = |t: &str| format!(r#"{{"table":"pot_records","txid":"{t}","vout":0}}"#);
        let twin = [(join.clone(), 0u32, keys)];
        let elsewhere = pays(&competitor, &[(0, &"99".repeat(20), 20_190, Some(false))]);
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let story = |ledger: &str| {
            let released =
                released_hops_by_eviction(&[(join.clone(), Some(ledger.to_string()))], &hops);
            let homes: HashMap<String, String> = evicted_pot_homes(&released, &hops, &twin)
                .into_iter()
                .collect();
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.spender_outputs = &elsewhere;
            i.my_pkh_by_game = &homes;
            let rows = derive_and_pin(&i);
            assert_eq!(rows.len(), 1, "{rows:?}");
            (
                rows[0].reason.clone(),
                rows[0].facts.get("spendKind").cloned(),
            )
        };
        // the folded ledger: the re-pointed hop (mine) and the released one
        let folded = format!("[{},{}]", entry(&other_hop), entry(&tx(0x07)));
        assert_eq!(
            story(&folded),
            (
                Some(SPENT_ELSEWHERE_REASON.to_string()),
                Some(json!("spent-elsewhere"))
            )
        );
        assert!(SPENT_ELSEWHERE_REASON.contains("spent outside this game"));
        // `7b285f8`'s ledger: the re-pointed hop absent
        assert_eq!(
            story(&format!("[{}]", entry(&other_hop))),
            (Some(HOME_UNKNOWN_REASON.to_string()), None)
        );
    }

    /// The gate's M2: "pays none of your homes" needs a KNOWN home. A hop-only game (no pot committed a home) gets the
    /// spender's pay-to addresses and a sentence, never `spent-elsewhere`.
    #[test]
    fn spent_elsewhere_needs_a_known_home_else_the_addresses_are_served_for_the_device_to_match() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0d);
        let elsewhere = pays(
            &sweep,
            &[
                (0, &"99".repeat(20), 20_000, Some(false)),
                (1, &"88".repeat(20), 100, None),
            ],
        );
        let hops = [hop(HopStatus::Spent, Some(&sweep), Some(10_000_000))];
        // home UNKNOWN (no results entry for the game): the addresses, no custody story
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].reason.as_deref(), Some(HOME_UNKNOWN_REASON));
        assert!(rows[0].facts.get("spendKind").is_none());
        assert_eq!(
            rows[0].facts["spenderPkhs"],
            json!(["88".repeat(20), "99".repeat(20)])
        );
        // home KNOWN and not paid: spent outside this game, as before
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "cc".repeat(20));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
    }

    /// The gate's M3: a payout proven by COURIER bytes is real but the index holds no proof to assemble a credit
    /// from: the row says so and offers no press (never a press that pends forever).
    #[test]
    fn a_courier_proven_payout_is_a_row_without_a_press_and_says_why() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0e);
        let my_pkh = "cc".repeat(20);
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        let mut spent = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        spent.spent_confirmed = Some(true);
        let hops = [spent];
        let mut courier: HashSet<String> = HashSet::new();
        courier.insert(sweep.clone());
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::Payout, Some(20_000))
        );
        assert_eq!(rows[0].facts["sweepSource"], "courier-bytes");
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON);
        assert_eq!(rows[0].facts["creditKind"], "courier-bytes");
        // the round-2 LOW-1: an UNCONFIRMED courier payout keeps its waiting word (the spend may yet be displaced)
        let mut unconfirmed = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        unconfirmed.spent_confirmed = Some(false);
        let hops_u = [unconfirmed];
        let mut i = inputs(&[], &[], &hops_u, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        assert_eq!(rows[0].facts["creditKind"], "courier-bytes");
        // NEW-4: bytes that do NOT spend this hop refute the pointer — courier-supplied AND index-held alike:
        // no payout, "could not judge", no custody story
        let mut refuted: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        refuted.insert(sweep.clone(), vec![(tx(0x09), 3)]);
        for courier_sourced in [true, false] {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.spender_outputs = &mine;
            i.spender_inputs = &refuted;
            i.my_pkh_by_game = &pkhs;
            if courier_sourced {
                i.courier_spenders = &courier;
            }
            let rows = derive_and_pin(&i);
            assert_eq!(
                rows[0].family,
                OwedFamily::Unbound,
                "courier_sourced={courier_sourced}"
            );
            assert!(
                rows[0]
                    .reason
                    .as_deref()
                    .unwrap()
                    .contains("could not judge"),
                "{:?}",
                rows[0].reason
            );
            assert!(rows[0].facts.get("spendKind").is_none());
        }
        // the same bytes from the INDEX: claimable, as before
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &mine;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["sweepSource"], "index-bytes");
        assert_eq!(rows[0].facts["claimable"], true);
    }

    /// bsv-low #692 (RC-L34-OWED-COURIER-RETIRE), the fleet-shaped fixture: an AROUND-THE-OVERLAY sweep (the index
    /// never held it, so the hop is index-UNSPENT and the chain rung names the sweep). WoC `/0/spent` 429'd on both
    /// of the hop's probes, so the spent-any ladder fell to BananaBlocks, which cannot say whether the spend is
    /// mined (`spent_confirmed == Some(false)`). A CHAINTRACKS-CHECKED mined proof of the sweep is in hand this pass
    /// (`proof_mined_spenders`, the `/tx-any`, caller=owed, whose `/proof/tsc` bound against the canonical root):
    /// the courier-bytes payout reaches "home on chain", named `confirmedSource = "proof"`. RED on main (the field
    /// and the OR do not exist: the row read `UNCONFIRMED_PAYOUT_REASON`, the loop-34 red).
    #[test]
    fn e692_a_woc_fault_bananablocks_unconfirmed_but_the_checked_proof_confirms_home_on_chain() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0e);
        let my_pkh = "cc".repeat(20);
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        // index-UNSPENT hop, stranded (its JOIN evicted around the overlay); the chain rung names the sweep SPENT
        // but UNCONFIRMED (WoC faulted, BananaBlocks could not carry the confirmation)
        let hops = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(false));
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let mut courier: HashSet<String> = HashSet::new();
        courier.insert(sweep.clone());
        let mut proof: HashSet<String> = HashSet::new();
        proof.insert(sweep.clone());
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        i.proof_mined_spenders = &proof;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["sweepSource"], "courier-bytes");
        assert_eq!(
            rows[0].facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON,
            "the sats sit at your home on chain"
        );
        assert_eq!(rows[0].facts["confirmedSource"], "proof");
        assert_eq!(count_swept_confirmed_by_proof(&rows), 1);
    }

    /// bsv-low #692 (RC-L34): the SAME fixture with NO checked proof in hand (`proof_mined_spenders` empty) and WoC
    /// still faulting: the row STAYS "not mined yet", never a false confirmed. Absence is "could not look", not
    /// "not mined": the row waits for the proof, a succeeding WoC, or a `collected` filing, exactly as before.
    #[test]
    fn e692_b_without_a_checked_proof_a_woc_fault_leaves_the_swept_payout_unconfirmed() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0e);
        let my_pkh = "cc".repeat(20);
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        let hops = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(false));
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let mut courier: HashSet<String> = HashSet::new();
        courier.insert(sweep.clone());
        let no_proof: HashSet<String> = HashSet::new();
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        i.proof_mined_spenders = &no_proof;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["sweepSource"], "courier-bytes");
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        assert_ne!(rows[0].facts["confirmedSource"], "proof");
        assert_eq!(count_swept_confirmed_by_proof(&rows), 0);
    }

    const E693F_ROOT: &str = "abababababababababababababababababababababababababababababababab";
    /// The clock the latch pins write and judge at.
    const E693F_NOW: i64 = 1_800_000_000_000;

    /// bsv-low #693 fold-2 (the W1-OWED-693 re-lens's R-HIGH-1 (1)): THE RECHECK TAKES THE NEWEST HEIGHTS. Five
    /// latches at 900000..900004 and the newest's root moved by a reorg: its height is re-read and the latch is NOT
    /// honoured. Every height inside `LATCH_RECHECK_DEPTH` of the tip is re-read, at least the four newest; a latch
    /// deeper than the window is reorg-proof and is not re-read; an unreadable tip counts from the newest latch. The
    /// route hands the pass's own tip (read at its start). RED
    /// on `ffb4a98` (the lowest four re-read: `honoured: 5`).
    #[test]
    fn e693f2_h1_the_recheck_takes_the_newest_heights() {
        use bsv_overlay_cloudflare::tip_pass::REORG_SWEEP_DEPTH;
        const _: () = assert!(LATCH_RECHECK_DEPTH >= 2 * REORG_SWEEP_DEPTH);
        let latches: Vec<ProofLatch> = (0..5u8)
            .map(|i| ProofLatch {
                spender: tx(0x40 + i),
                height: 900_000 + u64::from(i),
                root: E693F_ROOT.to_string(),
                written_at_ms: E693F_NOW,
            })
            .collect();
        let moved = "cd".repeat(32);
        let chain = |h: u64| {
            Some(if h == 900_004 {
                moved.clone()
            } else {
                E693F_ROOT.to_string()
            })
        };
        for tip in [None, Some(900_004), Some(900_005)] {
            let heights = proof_latch_recheck_heights(&latches, tip);
            let canonical: HashMap<u64, Option<String>> =
                heights.iter().map(|h| (*h, chain(*h))).collect();
            let honoured = latched_proof_mined_spenders(&latches, &canonical, tip, None, E693F_NOW);
            assert_eq!(
                honoured.len(),
                4,
                "tip {tip:?}: honoured: {}",
                honoured.len()
            );
            assert!(!honoured.contains(&tx(0x44)), "the newest, its root moved");
            assert_eq!(
                heights,
                vec![900_004, 900_003, 900_002, 900_001, 900_000],
                "tip {tip:?}: every height inside the window, newest first"
            );
        }
        // 900000 is six blocks under 900006: reorg-proof, not re-read; the four newest are, whatever their depth
        assert_eq!(
            proof_latch_recheck_heights(&latches, Some(900_006)),
            vec![900_004, 900_003, 900_002, 900_001]
        );
        assert_eq!(
            proof_latch_recheck_heights(&latches, Some(901_000)),
            vec![900_004, 900_003, 900_002, 900_001]
        );
        assert!(latch_is_deep(900_000, 900_006) && !latch_is_deep(900_001, 900_006));
        // a bound: at most the window's heights, or the four newest
        let many: Vec<ProofLatch> = (0..40u64)
            .map(|i| ProofLatch {
                spender: tx(0x50),
                height: 900_000 + i,
                root: E693F_ROOT.to_string(),
                written_at_ms: E693F_NOW,
            })
            .collect();
        assert_eq!(
            proof_latch_recheck_heights(&many, Some(900_039)).len() as u64,
            LATCH_RECHECK_DEPTH.max(OWED_PROOF_LATCH_RECHECKS_PER_RECOMPUTE as u64)
        );
        // the judge's hydration hands the judgement's tip to the shipped choice (bsv-low #893: the walk that did
        // it retired)
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let hydrate = squash(include_str!("owed_judge/hydrate.rs"));
        let recheck = "forheightinowed::proof_latch_recheck_heights(&latches,i.tip){";
        assert!(hydrate.contains(recheck), "the judge's recheck: {recheck}");
    }

    /// bsv-low #672 (Pin 2b): the store's bytes JUDGE. A spender served from the durable store
    /// (`StoredSpenderBytes::to_outputs`) drives `derive_owed_rows` to the SAME `spent-elsewhere` word a fresh fetch
    /// gives. The round-trip through the production SQL (and the bounds) is `owed_spender_store_sqlite.rs`.
    #[test]
    fn i672_store_bytes_judge_spent_elsewhere() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let competitor = tx(0x0d);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "cc".repeat(20));
        // the store's parse (immutable facts, spent dropped) round-tripped through JSON
        let stored = StoredSpenderBytes::from_parts(
            &[(tx(0x07), 0)],
            &[SpenderOutput {
                vout: 0,
                pkh_hex: Some("99".repeat(20)),
                sats: 20_170,
                spent: Some(true), // a mutable word the store must NOT keep
                pot_lock: false,
            }],
        );
        let json = stored.to_json_capped().expect("under the size cap");
        let back = StoredSpenderBytes::from_json(&json).expect("round-trips");
        assert_eq!(back, stored);
        assert!(
            back.to_outputs().iter().all(|o| o.spent.is_none()),
            "the store holds immutable facts only; the mutable spend word is dropped"
        );
        let mut from_store: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        from_store.insert(competitor.clone(), back.to_outputs());
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(competitor.clone(), back.inputs.clone());
        let hops = [hop(
            HopStatus::Unknown,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let chain = chain_confirmed(&key, true, Some(true), Some(&competitor), Some(true));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &from_store;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
    }

    /// bsv-low L25 (the courier-retire story-row flap; RC-L25POST2, pair 19, 05:34:17Z): a hop an EARLIER pass
    /// PROVED home-spent — THIS crate verified MY key's signature over the spender and latched it durably under
    /// `homeproof:` — is RETIRED on every recompute, BYTE-FREE. On the 05:34Z recompute the spender courier cap
    /// (`OWED_SPENDER_COURIER_READS_PER_RECOMPUTE` = 2, three hops) WITHHELD one hop's sweep bytes; `swept_home`
    /// answered `None`, the hop fell to the raw chain-spent arm, and it served a claimless `unbound` "could not
    /// judge the spender" row for money that was home and spent (series ...3, 1, 0, 1). The durable latch now
    /// retires it byte-free, the SAME answer the bytes-in-hand Arm A gives (`output_spent`), so the list is stable
    /// across recomputes with and without the bytes. The REVERSE (no latch, bytes withheld) is unchanged: the
    /// could-not-judge row still stands (#662's keep then covers it when a prior courier-bytes payout row survives,
    /// which it does not after a retirement — the gap this closes at its source). Beside the #485 retirement pin
    /// (`a_courier_proven_payout_retires_...`) and the #662 keep pin
    /// (`i662_a_withheld_spender_read_keeps_...`).
    /// To red: revert the byte-free retirement `continue` in `derive_owed_rows` (the field grafted inert): h2
    /// serves the could-not-judge row again and the forward assertion fails.
    #[test]
    fn l25_a_proven_homeproof_latch_retires_the_swept_hop_byte_free_when_the_courier_cap_withholds_its_bytes(
    ) {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let my_pkh = "cc".repeat(20);
        // three hops, each swept to MY committed home by its own courier sweep; the chain rung names each sweep
        // spent + confirmed; none is in the index's own spend word (swept around the overlay), the live shape.
        let mk = |hop_b: u8, game_b: u8| {
            let mut h = hop(HopStatus::Unknown, None, Some(HOP_STRANDED_AFTER_MS + 1));
            h.hop_txid = tx(hop_b);
            h.game_id = tx(game_b);
            h
        };
        let hops = [mk(0x11, 0x21), mk(0x12, 0x22), mk(0x13, 0x23)];
        let (sa, sb, sc) = (tx(0x31), tx(0x32), tx(0x33));
        let (ka, kb, kc) = (
            format!("{}:0", tx(0x11)),
            format!("{}:0", tx(0x12)),
            format!("{}:0", tx(0x13)),
        );
        let mut chain: HashMap<String, HopChainWord> = HashMap::new();
        for (k, s) in [(&ka, &sa), (&kb, &sb), (&kc, &sc)] {
            chain.extend(chain_confirmed(k, true, Some(true), Some(s), Some(true)));
        }
        let mut pkhs: HashMap<String, String> = HashMap::new();
        for g in [0x21u8, 0x22, 0x23] {
            pkhs.insert(tx(g), my_pkh.clone());
        }
        let courier: HashSet<String> = [sa.clone(), sb.clone(), sc.clone()].into_iter().collect();
        // h1 and h3 have their sweep bytes in hand (pay MY home) and the home walk proved them Proven; h2's bytes
        // were WITHHELD by the courier cap this pass, so `swept_home` is `None` and `home_spends` carries nothing.
        let mut bytes: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        bytes.extend(pays(&sa, &[(0, &my_pkh, 20_000, None)]));
        bytes.extend(pays(&sc, &[(0, &my_pkh, 20_000, None)]));
        let mut home_spends: HashMap<String, HomeSpendWord> = HashMap::new();
        home_spends.insert(format!("{sa}:0"), HomeSpendWord::Proven);
        home_spends.insert(format!("{sc}:0"), HomeSpendWord::Proven);
        let build = |retired: &HashSet<String>| -> Vec<OwedRow> {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.hop_chain = &chain;
            i.spender_outputs = &bytes;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = &courier;
            i.home_spends = &home_spends;
            i.retired_spenders = retired;
            derive_and_pin(&i)
        };
        // the pure byte-free read: every hop's named spender, and every spender's durable Proven `homeproof:`
        // latch, matched against them. A latch for a stranger's sweep, and a non-proven memo, retire nothing.
        let probe = |i: &OwedInputs| hop_named_spenders(i);
        let named = {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.hop_chain = &chain;
            probe(&i)
        };
        assert_eq!(named, {
            let mut s = [sa.clone(), sb.clone(), sc.clone()];
            s.sort();
            s.to_vec()
        });
        let latch = |sweep: &str, vout: u32, spent: bool, named_spender: bool| {
            let (t, vv) = home_latch_target(sweep, vout);
            crate::hops_view::ProbeMemo {
                outpoint: crate::hops_view::probe_memo_key(&t, vv),
                probed_at_ms: 1_800_000_000_000,
                spent,
                spending_txid: named_spender.then(|| tx(0x99)),
                spent_confirmed: Some(true),
                tip_height: None,
            }
        };
        let memos = [
            latch(&sa, 0, true, true),
            latch(&sb, 0, true, true),
            latch(&sc, 0, true, true),
            latch(&tx(0x41), 0, true, true), // a stranger's sweep: not a named spender
            latch(&sb, 1, false, true),      // a non-proven memo (spent=false) of a named spender
        ];
        let latched = latched_retired_spenders(&named, &memos);
        assert_eq!(latched, {
            let mut s = HashSet::new();
            s.extend([sa.clone(), sb.clone(), sc.clone()]);
            s
        });
        // FORWARD: with every latch read (h2's included), ALL THREE retire — h1 and h3 via Arm A's `output_spent`
        // (their bytes are in hand), h2 BYTE-FREE (its bytes withheld). No could-not-judge row for h2.
        assert!(
            build(&latched).is_empty(),
            "a proven homeproof latch retires every swept hop, byte-free when its bytes were withheld: {:?}",
            build(&latched)
        );
        // REVERSE: with NO latch read, h1/h3 still retire via Arm A (bytes in hand), but h2 — bytes withheld,
        // `swept_home` `None`, no latch — serves the existing claimless `unbound` "could not judge the spender"
        // row. This is the flap the fix removes; it reds the forward assertion when the `continue` is reverted.
        let without = build(&NONE);
        assert_eq!(without.len(), 1, "{without:?}");
        assert_eq!(
            (
                without[0].outpoint.as_str(),
                without[0].family,
                without[0].sats
            ),
            (kb.as_str(), OwedFamily::Unbound, Some(20_190))
        );
        assert!(without[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not judge the spender"));
        assert!(without[0].facts["claim"].is_null());
    }

    /// bsv-low L25 (the HOMEPROOF lens's LOW-1): the byte-free retirement rests on ONE invariant - a LOW sweep pays
    /// this identity EXACTLY ONE home output. The client builds every hop sweep as a single hop input to a single
    /// `dest_script_hex` home (`money_p2pkh_sweep_assemble`'s `P2pkhSweepPlanJson`; `owedPress::buildHopSweepForRow`,
    /// `Pot::ensureHopSweepInternal`); two hops of one game are TWO sweeps, one home output each (stake.ts: "both
    /// hops pay the same script"). So the sole home output's `homeproof:` latch IS every home output, and the
    /// byte-free `.any()` match (`latched_retired_spenders`) equals Arm A's `.all()` over the single `mine`
    /// (`swept_home`'s `home_spent`). This pins that equivalence on the realized ONE-home shape: the hop retires the
    /// SAME byte-free (its bytes withheld, the latch read) as bytes-in-hand (Arm A's `output_spent`); and the
    /// UNPROVEN sole output is retired by NEITHER road (a claimable payout stands byte-in-hand, an honest
    /// could-not-judge row byte-free). Were a sweep ever to pay two home outputs, one latched one claimable, the
    /// byte-free path could not see the unlatched sibling; the invariant, enforced by the client builder and not by
    /// this read, is what keeps the two answers equal.
    #[test]
    fn l25_low1_one_home_sweep_retires_the_same_byte_free_and_with_bytes() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let my_pkh = "cc".repeat(20);
        let mut h = hop(HopStatus::Unknown, None, Some(HOP_STRANDED_AFTER_MS + 1));
        h.hop_txid = tx(0x11);
        h.game_id = tx(0x21);
        let hops = [h];
        let sweep = tx(0x31);
        let k = format!("{}:0", tx(0x11));
        let mut chain: HashMap<String, HopChainWord> = HashMap::new();
        chain.extend(chain_confirmed(
            &k,
            true,
            Some(true),
            Some(sweep.as_str()),
            Some(true),
        ));
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x21), my_pkh.clone());
        let courier: HashSet<String> = [sweep.clone()].into_iter().collect();
        // ONE home output (vout 0): the shape the client builder always makes.
        let bytes_one = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let mut proven: HashMap<String, HomeSpendWord> = HashMap::new();
        proven.insert(format!("{sweep}:0"), HomeSpendWord::Proven);
        let no_bytes: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        let no_home: HashMap<String, HomeSpendWord> = HashMap::new();
        let build = |spenders: &HashMap<String, Vec<SpenderOutput>>,
                     home: &HashMap<String, HomeSpendWord>,
                     retired: &HashSet<String>|
         -> Vec<OwedRow> {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.hop_chain = &chain;
            i.spender_outputs = spenders;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = &courier;
            i.home_spends = home;
            i.retired_spenders = retired;
            derive_and_pin(&i)
        };
        // the byte-free latch of the SOLE home output
        let (t, vv) = home_latch_target(&sweep, 0);
        let memo = crate::hops_view::ProbeMemo {
            outpoint: crate::hops_view::probe_memo_key(&t, vv),
            probed_at_ms: 1_800_000_000_000,
            spent: true,
            spending_txid: Some(tx(0x99)),
            spent_confirmed: Some(true),
            tip_height: None,
        };
        let named = {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.hop_chain = &chain;
            hop_named_spenders(&i)
        };
        assert_eq!(named, vec![sweep.clone()]);
        let latched = latched_retired_spenders(&named, std::slice::from_ref(&memo));
        assert_eq!(latched, HashSet::from([sweep.clone()]));
        // EQUIVALENCE: the one-home sweep's sole output proven retires the hop, the SAME answer both roads.
        let with_bytes = build(&bytes_one, &proven, &NONE);
        let byte_free = build(&no_bytes, &no_home, &latched);
        assert!(
            with_bytes.is_empty(),
            "bytes in hand, the sole home output proven: Arm A retires: {with_bytes:?}"
        );
        assert!(
            byte_free.is_empty(),
            "byte-free, the sole home output's latch: retires the same: {byte_free:?}"
        );
        // THE HONEST BOUNDARY: the UNPROVEN sole output is retired by NEITHER road.
        let with_bytes_unspent = build(&bytes_one, &no_home, &NONE);
        let byte_free_no_latch = build(&no_bytes, &no_home, &NONE);
        assert_eq!(with_bytes_unspent.len(), 1, "{with_bytes_unspent:?}");
        assert_eq!(with_bytes_unspent[0].family, OwedFamily::Payout);
        assert_eq!(byte_free_no_latch.len(), 1, "{byte_free_no_latch:?}");
        assert_eq!(byte_free_no_latch[0].family, OwedFamily::Unbound);
        assert!(byte_free_no_latch[0].facts["claim"].is_null());
    }

    /// The gate's L1: a spend with a VERIFIED proof height is mined whatever `spentConfirmed` says — a verdict gap on
    /// it is the old "not classified" sentence, never a block wait.
    #[test]
    fn a_proof_verified_spend_without_a_verdict_is_not_a_block_wait() {
        let mut e = entry(Some(true), None, Outcome::Refund, Some(SeatLetter::A));
        e.spent_confirmed = Some(false);
        e.at_height = None; // unconfirmed: no verified mined height (the review's LOW-1 bar)
        e.at_height = Some(900_100);
        let results = vec![e];
        let refunds: Vec<RefundEntry> = Vec::new();
        let hops: Vec<HopEntry> = Vec::new();
        let (valid, none, pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let i = inputs(
            &results,
            &refunds,
            &hops,
            &valid,
            &none,
            &pots,
            Some(900_200),
        );
        let rows = derive_and_pin(&i);
        assert_eq!(
            rows[0].reason.as_deref(),
            Some("the spend is not classified yet (no verdict)")
        );
        assert!(rows[0].facts.get("chainWait").is_none());
    }

    /// Fleet loop 11's wave (2026-09-19): a pot whose spend the index HOLDS but the chain has not confirmed (the
    /// refund submitted here, seen, between two blocks) is an honest CHAIN wait, said by machine
    /// (`facts.chainWait = "block"`), never "not classified" — the verdict is computed at the landing bar.
    #[test]
    fn an_index_held_unconfirmed_spend_without_a_verdict_is_a_chain_wait_not_an_unclassified_story()
    {
        let mut e = entry(Some(true), None, Outcome::Refund, Some(SeatLetter::A));
        e.spent_confirmed = Some(false);
        e.at_height = None; // unconfirmed: no verified mined height (the review's LOW-1 bar)
        e.at_height = None; // unconfirmed: no proven height (the fixture's default models a mined spend)
        let results = vec![e];
        let refunds: Vec<RefundEntry> = Vec::new();
        let hops: Vec<HopEntry> = Vec::new();
        let valid = HashMap::new();
        let none = HashSet::new();
        let pots = HashSet::new();
        let i = inputs(
            &results,
            &refunds,
            &hops,
            &valid,
            &none,
            &pots,
            Some(900_200),
        );
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "one story row");
        let r = &rows[0];
        assert_eq!(r.family, OwedFamily::Unbound);
        assert_eq!(r.reason.as_deref(), Some(SPEND_AWAITING_BLOCK_REASON));
        assert_eq!(r.facts["chainWait"], json!("block"));
        assert_eq!(r.facts["spendTxid"], json!(tx(0x03)));
        assert!(
            r.facts.get("claim").is_none_or(|c| c.is_null()),
            "no press before the block"
        );

        // an unconfirmed PAYOUT (the verdict known, the block not yet): the press waits, the row says the chain wait
        let mut w = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        w.spent_confirmed = Some(false);
        w.at_height = None;
        let results_w = vec![w];
        let i_w = inputs(
            &results_w,
            &refunds,
            &hops,
            &valid,
            &none,
            &pots,
            Some(900_200),
        );
        let rows_w = derive_and_pin(&i_w);
        assert_eq!(rows_w.len(), 1);
        assert_eq!(rows_w[0].family, OwedFamily::Payout);
        assert_eq!(rows_w[0].facts["claimable"], false);
        assert_eq!(rows_w[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        assert_eq!(rows_w[0].facts["chainWait"], json!("block"));
        // the same spend CONFIRMED without a verdict keeps the old, honest word (a classification gap, not a wait)
        let mut c = entry(Some(true), None, Outcome::Refund, Some(SeatLetter::A));
        c.spent_confirmed = Some(true);
        let results2 = vec![c];
        let i2 = inputs(
            &results2,
            &refunds,
            &hops,
            &valid,
            &none,
            &pots,
            Some(900_200),
        );
        let rows2 = derive_and_pin(&i2);
        assert_eq!(rows2.len(), 1);
        assert_eq!(
            rows2[0].reason.as_deref(),
            Some("the spend is not classified yet (no verdict)")
        );
        assert!(rows2[0].facts.get("chainWait").is_none());
    }

    /// bsv-low #618 (L2): the coin word's landing table, the one input that replaces the SEEN+final arm on the owed
    /// path: a checked `mined` lands; a final `seen` and the chain words this pass could not check (`stale`, a stored
    /// `mined` with no verify path, `cannotCheck`) await; `announced`, `unknown`, a non-final `seen`, the terminal
    /// words and no word withhold. The arm's pair carries the verdict; the latch is never read.
    #[test]
    fn l2_coin_landing_table() {
        let w = |word: &str, path: bool| json!({"word": word, "tier": "x", "height": null, "verifyPath": if path { json!({"root": "r"}) } else { Value::Null }});
        let mut cannot = w("unknown", false);
        cannot["cannotCheck"] = json!("header snapshot changed");
        for (word, fin, want) in [
            (Some(w("mined", true)), Some(false), CoinLanding::Landed),
            (Some(w("mined", true)), None, CoinLanding::Landed),
            (Some(w("mined", false)), Some(true), CoinLanding::Awaiting),
            (Some(w("stale", false)), None, CoinLanding::Awaiting),
            (Some(cannot.clone()), None, CoinLanding::Awaiting),
            (Some(w("seen", false)), Some(true), CoinLanding::Awaiting),
            (Some(w("seen", false)), Some(false), CoinLanding::Withheld),
            (Some(w("seen", false)), None, CoinLanding::Withheld),
            (
                Some(w("announced", false)),
                Some(true),
                CoinLanding::Withheld,
            ),
            (Some(w("unknown", false)), Some(true), CoinLanding::Withheld),
            (Some(w("built", false)), Some(true), CoinLanding::Withheld),
            (
                Some(w("rejected", false)),
                Some(true),
                CoinLanding::Withheld,
            ),
            (
                Some(w("conflicted", false)),
                Some(true),
                CoinLanding::Withheld,
            ),
            (None, Some(true), CoinLanding::Withheld),
        ] {
            assert_eq!(
                coin_landing(word.as_ref(), fin),
                want,
                "{word:?} final {fin:?}"
            );
        }
        let words = std::collections::BTreeMap::from([
            (tx(1), w("mined", true)),
            (tx(2), w("seen", false)),
            (tx(3), w("announced", false)),
        ]);
        // the pair: the latch's own (true, true) is ignored, the word decides
        assert_eq!(
            word_arm(Some(&tx(1)), Some(false), &words),
            (Some(true), Some(true))
        );
        assert_eq!(
            word_arm(Some(&tx(2)), Some(true), &words),
            (Some(true), Some(true))
        );
        assert_eq!(
            word_arm(Some(&tx(3)), Some(true), &words),
            (Some(false), Some(true))
        );
        assert_eq!(
            word_arm(Some(&tx(4)), Some(true), &words),
            (Some(false), Some(true))
        );
        assert_eq!(word_arm(None, None, &words), (Some(false), None));
    }

    /// bsv-low #618: the judge's hydration applies the word arm to every hydrated row (the party rows, results and
    /// refunds, AND the hop rows: the W2-WORDS lens's LOW-2, mutant M4) in ONE call BEFORE it assembles the three
    /// views (bsv-low #893 section 6.2: the walk that did it retired; this pin reads the judge's hydration).
    #[test]
    fn l2_the_judge_assembles_after_the_word_arm() {
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let hydrate = squash(include_str!("owed_judge/hydrate.rs"));
        let arm = hydrate
            .find(&squash(
                "owed::apply_word_arm(&mut result_rows, &mut refund_rows, &mut hop_rows, &i.spender_words,);",
            ))
            .expect("the word arm is applied to the party and the hop rows");
        for assembler in [
            "assemble_results(",
            "crate::refund_view::assemble_refund_view(",
            "hops_view::assemble_hops_view(",
        ] {
            let at = hydrate
                .find(&squash(assembler))
                .unwrap_or_else(|| panic!("{assembler} missing"));
            assert!(arm < at, "{assembler} runs before the word arm");
        }
    }

    static NONE: std::sync::LazyLock<HashSet<String>> = std::sync::LazyLock::new(HashSet::new);
    pub(crate) fn inputs<'a>(
        results: &'a [ResultEntry],
        refunds: &'a [RefundEntry],
        hops: &'a [HopEntry],
        valid: &'a HashMap<String, ValidRefund>,
        collected_verified: &'a HashSet<String>,
        pots: &'a HashSet<String>,
        tip: Option<u64>,
    ) -> OwedInputs<'a> {
        OwedInputs {
            spender_words: None,
            identity_lc: ME,
            tip,
            now_ms: 1_800_000_000_000,
            results,
            refunds,
            hops,
            valid_refunds: valid,
            collected_verified,
            collected_present: &NONE,
            held_verified: &NONE,
            pot_spenders: pots,
            pot_spenders_faulted: false,
            hop_chain: &NO_CHAIN,
            hop_sweeps: &NO_SWEEPS,
            evicted_pots: &NONE,
            evicted_hop_outpoints: &NONE,
            door_refused_hop_outpoints: &NO_DOOR,
            spender_outputs: &NO_SPENDERS,
            spender_inputs: &NO_INPUTS,
            courier_spenders: &NONE,
            my_pkh_by_game: &NO_PKHS,
            home_spends: &NO_HOME_SPENDS,
            retired_spenders: &NONE,
            proof_mined_spenders: &NONE,
            funding_unheld: &NONE,
            join_spent_hops: &NO_PKHS,
        }
    }
    static NO_SPENDERS: std::sync::LazyLock<HashMap<String, Vec<SpenderOutput>>> =
        std::sync::LazyLock::new(HashMap::new);
    static NO_DOOR: std::sync::LazyLock<HashMap<String, DoorRefusal>> =
        std::sync::LazyLock::new(HashMap::new);
    static NO_INPUTS: std::sync::LazyLock<HashMap<String, Vec<(String, u32)>>> =
        std::sync::LazyLock::new(HashMap::new);
    static NO_PKHS: std::sync::LazyLock<HashMap<String, String>> =
        std::sync::LazyLock::new(HashMap::new);
    fn pays(
        spender: &str,
        outs: &[(u32, &str, u64, Option<bool>)],
    ) -> HashMap<String, Vec<SpenderOutput>> {
        let mut m = HashMap::new();
        m.insert(
            spender.to_string(),
            outs.iter()
                .map(|(v, pkh, sats, spent)| SpenderOutput {
                    vout: *v,
                    pkh_hex: Some((*pkh).to_string()),
                    sats: *sats,
                    spent: *spent,
                    pot_lock: false,
                })
                .collect(),
        );
        m
    }
    static NO_CHAIN: std::sync::LazyLock<HashMap<String, HopChainWord>> =
        std::sync::LazyLock::new(HashMap::new);
    static NO_HOME_SPENDS: std::sync::LazyLock<HashMap<String, HomeSpendWord>> =
        std::sync::LazyLock::new(HashMap::new);
    static NO_SWEEPS: std::sync::LazyLock<HashMap<String, crate::hopsweep::FiledHopSweep>> =
        std::sync::LazyLock::new(HashMap::new);
    fn chain(
        outpoint: &str,
        looked: bool,
        spent: Option<bool>,
        spender: Option<&str>,
    ) -> HashMap<String, HopChainWord> {
        chain_confirmed(outpoint, looked, spent, spender, None)
    }
    pub(crate) fn chain_confirmed(
        outpoint: &str,
        looked: bool,
        spent: Option<bool>,
        spender: Option<&str>,
        spent_confirmed: Option<bool>,
    ) -> HashMap<String, HopChainWord> {
        chain_confirmed_aged(outpoint, looked, spent, spender, spent_confirmed, None)
    }
    fn chain_confirmed_aged(
        outpoint: &str,
        looked: bool,
        spent: Option<bool>,
        spender: Option<&str>,
        spent_confirmed: Option<bool>,
        age_ms: Option<i64>,
    ) -> HashMap<String, HopChainWord> {
        let mut m = HashMap::new();
        m.insert(
            outpoint.to_string(),
            HopChainWord {
                looked,
                spent,
                spending_txid: spender.map(str::to_string),
                spent_confirmed,
                stale: false,
                age_ms,
                probe_tip: None,
                predates_tip: false,
            },
        );
        m
    }
    fn filed(outpoint: &str, sweep_txid: &str) -> HashMap<String, crate::hopsweep::FiledHopSweep> {
        let mut m = HashMap::new();
        m.insert(
            outpoint.to_string(),
            crate::hopsweep::FiledHopSweep {
                sweep_txid: sweep_txid.to_string(),
                raw_hex: "0100".repeat(20),
                pays_sats: Some(20_000),
                index_proven: false,
                index_proof_height: None,
            },
        );
        m
    }
    /// #517: the same filing, held PROVEN by the index (`transactions.has_proof = 1`, the bump's block when recorded).
    fn filed_proven(
        outpoint: &str,
        sweep_txid: &str,
        height: Option<u64>,
    ) -> HashMap<String, crate::hopsweep::FiledHopSweep> {
        let mut m = filed(outpoint, sweep_txid);
        if let Some(s) = m.get_mut(outpoint) {
            s.index_proven = true;
            s.index_proof_height = height;
        }
        m
    }

    #[test]
    fn a_won_confirmed_spend_paying_my_home_is_a_payout_row_with_my_sats() {
        let e = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(&e, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].sats, Some(39_200));
        assert_eq!(rows[0].outpoint, format!("{}:0", tx(0x02)));
        assert_eq!(rows[0].facts["claim"], "internalize");
        assert_eq!(
            rows[0].facts["creditBeef"],
            format!("/credit-beef/{}", tx(0x03))
        );
        assert_eq!(rows[0].at_height, Some(900_100));
    }

    #[test]
    fn a_decided_loss_is_not_a_row_and_a_verified_collected_payout_is_not_a_row_but_a_planted_one_stays(
    ) {
        let lost = [entry(
            Some(true),
            Some(PotVerdict::WinnerB),
            Outcome::Lost,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        assert!(derive_and_pin(&inputs(&lost, &[], &[], &v, &c, &p, Some(900_200))).is_empty());
        let won = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        // the identity's OWN collected filing (signature verified under it): retired
        let verified: HashSet<String> = [tx(0x01)].into_iter().collect();
        assert!(
            derive_and_pin(&inputs(&won, &[], &[], &v, &verified, &p, Some(900_200))).is_empty()
        );
        // the gate's HIGH-1: a PRESENT collected row nobody verified (a stranger's dust marker naming the winner)
        // retires NOTHING — the row stays, with the presence named as provenance
        let present: HashSet<String> = [tx(0x01)].into_iter().collect();
        let mut i = inputs(&won, &[], &[], &v, &c, &p, Some(900_200));
        i.collected_present = &present;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["collectedMarkerPresent"], true);
        assert_eq!(rows[0].facts["collectedSigVerified"], false);
        assert_eq!(rows[0].facts["claimable"], true);
    }

    #[test]
    fn an_unconfirmed_payout_is_a_row_that_is_not_claimable_yet() {
        let mut e = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        e.spent_confirmed = Some(false);
        e.at_height = None; // unconfirmed: no verified mined height (the review's LOW-1 bar)
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(
            std::slice::from_ref(&e),
            &[],
            &[],
            &v,
            &c,
            &p,
            Some(900_200),
        ));
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
    }

    #[test]
    fn a_winner_verdict_that_contradicts_my_seat_sizes_nothing() {
        // NOTE-16: winner-a with my seat B (a self-signed claim contradicting the binding) never takes home A's amount
        let e = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::B),
        );
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(
            std::slice::from_ref(&e),
            &[],
            &[],
            &v,
            &c,
            &p,
            Some(900_200),
        ));
        assert_eq!(rows[0].sats, None);
    }

    #[test]
    fn a_tie_and_a_refund_verdict_pay_my_seat_side() {
        let mut e = entry(
            Some(true),
            Some(PotVerdict::Tie),
            Outcome::Tie,
            Some(SeatLetter::B),
        );
        e.money.settle.as_mut().unwrap().pay_a_sats = Some(19_600);
        e.money.settle.as_mut().unwrap().pay_b_sats = Some(19_600);
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(
            std::slice::from_ref(&e),
            &[],
            &[],
            &v,
            &c,
            &p,
            Some(900_200),
        ));
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::Payout, Some(19_600))
        );
        // the same tie with the seat unknown: a payout the brain cannot size, never a guess
        let unknown = entry(Some(true), Some(PotVerdict::Tie), Outcome::Tie, None);
        let rows = derive_and_pin(&inputs(
            std::slice::from_ref(&unknown),
            &[],
            &[],
            &v,
            &c,
            &p,
            Some(900_200),
        ));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Payout, None));
    }

    #[test]
    fn a_spent_pot_the_brain_cannot_judge_is_unbound_with_its_sentence() {
        let no_verdict = [entry(
            Some(true),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(&no_verdict, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("no verdict"));
        let no_seat = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Unresolved,
            None,
        )];
        let rows = derive_and_pin(&inputs(&no_seat, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("seat binding"));
        let no_word = [entry(None, None, Outcome::Unresolved, Some(SeatLetter::A))];
        let rows = derive_and_pin(&inputs(&no_word, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("no spend word"));
    }

    fn refund_entry(status: RefundStatus) -> RefundEntry {
        RefundEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: Some(900_000),
            blocks_to_gate: Some(0),
            gate_passed: true,
            backup_marker_present: true,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: Some(false),
            verdict: None,
            spent_height: None,
            status,
            status_source: Some("index"),
            pot_admitted_at: None,
            first_party_marker_at: None,
            first_spent_at: None,
            seat_anchor: None,
        }
    }

    #[test]
    fn tip877_fold2_tipless_rejoin_and_refund_due_are_withheld_then_rearmed() {
        let entries = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let (collected, pots) = (HashSet::new(), HashSet::new());
        let valid = HashMap::from([(
            outpoint_key(&tx(2), 0),
            ValidRefund {
                raw_hex: "00".into(),
                my_sats: Some(19_800),
            },
        )]);
        for (tip, status, family, claim) in [
            (
                899_999,
                RefundStatus::Armed,
                OwedFamily::InProgress,
                "rejoin",
            ),
            (
                900_005,
                RefundStatus::GateOpen,
                OwedFamily::RefundDue,
                "present-refund",
            ),
        ] {
            let refunds = [refund_entry(status)];
            let derive = |tip| {
                derive_owed_rows(&inputs(
                    &entries,
                    &refunds,
                    &[],
                    &valid,
                    &collected,
                    &pots,
                    tip,
                ))
            };
            let before = derive(Some(tip));
            assert_eq!(before[0].family, family);
            assert_eq!(before[0].facts["claim"], claim);
            assert_eq!(before[0].facts["claimable"], true);

            let unknown = derive(None);
            assert_eq!(unknown[0].family, OwedFamily::Unbound);
            assert_eq!(unknown[0].facts["tipUnknown"], true);
            assert_eq!(unknown[0].facts["claimable"], false);
            assert!(unknown[0].facts["claim"].is_null());
            assert!(unknown[0].facts["blocksToGate"].is_null());
            assert_eq!(derive(Some(tip)), before);
        }
    }

    /// bsv-low #831 (loop 43, pair 17, pot `2e6bcb0d…`): a refund-due pot whose FUNDING the network does not hold (no
    /// SEEN latch, no index proof, no checked mined word) serves NO press: its pre-signed refund spends an output the
    /// node does not know (`SEEN_IN_ORPHAN_MEMPOOL`). A latched pot, and an unlatched one whose funding the tracker's
    /// checked word says is mined, keep today's `present-refund` byte for byte.
    #[test]
    fn fix831_a_refund_due_pot_whose_funding_is_held_by_nothing_serves_no_press() {
        let e = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let r = [refund_entry(RefundStatus::GateOpen)];
        let mut valid = HashMap::new();
        valid.insert(
            format!("{}:0", tx(0x02)),
            ValidRefund {
                raw_hex: "00".into(),
                my_sats: Some(19_800),
            },
        );
        let (c, p) = (HashSet::new(), HashSet::new());
        // the funding latched (absent from the unheld set): today's row, byte for byte
        let held = derive_and_pin(&inputs(&e, &r, &[], &valid, &c, &p, Some(900_005)));
        assert_eq!(held[0].family, OwedFamily::RefundDue);
        assert_eq!(held[0].facts["claim"], "present-refund");
        assert_eq!(held[0].facts["claimable"], true);
        assert_eq!(held[0].reason, None);
        // the funding held by nothing: no press, a named reason
        let unheld: HashSet<String> = [tx(0x02)].into_iter().collect();
        let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
        i.funding_unheld = &unheld;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].family,
            OwedFamily::Unbound,
            "a refund of a pot the network does not hold is never a press: {:?}",
            rows[0].facts["claim"]
        );
        assert_eq!(rows[0].facts["claim"], FUNDING_NOT_HELD_CLAIM);
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["claimReason"], FUNDING_NOT_HELD_REASON);
        assert_eq!(rows[0].facts["fundingHeld"], FUNDING_HELD_NONE);
        assert_eq!(rows[0].reason.as_deref(), Some(FUNDING_NOT_HELD_REASON));
        assert_eq!(rows[0].sats, Some(20_000));
        // a word store that holds no row for the funding, or an unchecked `mined`, keeps it unheld
        let mut words = std::collections::BTreeMap::new();
        words.insert(tx(0x02), json!({"word": "mined", "verifyPath": null}));
        i.spender_words = Some(&words);
        assert_eq!(derive_and_pin(&i)[0].family, OwedFamily::Unbound);
        // the tracker's CHECKED mined word for the funding: the network holds it, today's row byte for byte
        let mut checked = std::collections::BTreeMap::new();
        checked.insert(tx(0x02), json!({"word": "mined", "verifyPath": "header"}));
        i.spender_words = Some(&checked);
        let mined = derive_and_pin(&i);
        let today = derive_and_pin(&{
            let mut j = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
            j.spender_words = Some(&checked);
            j
        });
        assert_eq!(mined, today);
        assert_eq!(mined[0].facts["claim"], "present-refund");
        // the gate not open yet: the rejoin row is unchanged by the funding word (not a refund claim)
        let mut k = inputs(&e, &r, &[], &valid, &c, &p, Some(899_990));
        k.funding_unheld = &unheld;
        assert_eq!(derive_and_pin(&k)[0].facts["claim"], "rejoin");
    }

    /// bsv-low #598 (loop 22, p27/p28, the `spec-admit-fast-join-refused` cell's pair): a refund-due pot whose funding
    /// the SEEN latch held and the network then REFUSED (the tracker's chain-tier `rejected` or `conflicted` word for
    /// the pot's own txid) serves no press, under FIX-831's word (`funding-not-held`), named `fundingHeld: refused`.
    /// A latched funding whose word is `seen` or a checked `mined`, and a pot the index PROVED (never in the seen-only
    /// set: the index's proof first), keep today's `present-refund`. RED with `refused_fundings` answering nothing
    /// (the latched pot stays `refund-due`, `present-refund`, claimable).
    #[test]
    fn fix598_a_refused_joins_latched_funding_takes_the_funding_not_held_judgement() {
        let e = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let r = [refund_entry(RefundStatus::GateOpen)];
        let mut valid = HashMap::new();
        valid.insert(
            format!("{}:0", tx(0x02)),
            ValidRefund {
                raw_hex: "00".into(),
                my_sats: Some(19_800),
            },
        );
        let (c, p) = (HashSet::new(), HashSet::new());
        let seen_only: HashSet<String> = [tx(0x02)].into_iter().collect();
        let judge = |words: &std::collections::BTreeMap<String, Value>, seen: &HashSet<String>| {
            // the route's step: the latched fundings the network refused join the unheld set
            let unheld = refused_fundings(seen, words);
            let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
            i.spender_words = Some(words);
            i.funding_unheld = &unheld;
            derive_and_pin(&i)
        };
        for word in ["rejected", "conflicted"] {
            let mut words = std::collections::BTreeMap::new();
            words.insert(
                tx(0x02),
                json!({"word": word, "tier": "chain", "verifyPath": null}),
            );
            let rows = judge(&words, &seen_only);
            assert_eq!(rows.len(), 1);
            assert_eq!(
                (
                    rows[0].family,
                    &rows[0].facts["claim"],
                    &rows[0].facts["claimable"]
                ),
                (
                    OwedFamily::Unbound,
                    &json!(FUNDING_NOT_HELD_CLAIM),
                    &json!(false)
                ),
                "a refund of a funding the network refused ({word}) is never a press"
            );
            assert_eq!(rows[0].facts["fundingHeld"], FUNDING_HELD_REFUSED);
            assert_eq!(rows[0].facts["fundingWord"], word);
            assert_eq!(rows[0].reason.as_deref(), Some(FUNDING_NOT_HELD_REASON));
            assert_eq!(
                count_refund_funding_unheld(&rows),
                1,
                "counted with FIX-831's pots"
            );
            // the index PROVED the funding (not in the seen-only set): no word withholds it
            let proven = judge(&words, &HashSet::new());
            assert_eq!(
                proven[0].facts["claim"], "present-refund",
                "the index's proof first"
            );
        }
        for word in [
            json!({"word": "seen", "tier": "network", "verifyPath": null}),
            json!({"word": "mined", "tier": "chain", "verifyPath": "header"}),
            json!({"word": "announced", "verifyPath": null}),
        ] {
            let mut words = std::collections::BTreeMap::new();
            words.insert(tx(0x02), word.clone());
            let rows = judge(&words, &seen_only);
            assert_eq!(
                rows[0].family,
                OwedFamily::RefundDue,
                "{word}: no refusal, today's row"
            );
            assert_eq!(rows[0].facts["claim"], "present-refund");
        }
        // no word row at all: a latched funding is never refused by absence
        assert_eq!(
            judge(&std::collections::BTreeMap::new(), &seen_only)[0].facts["claim"],
            "present-refund"
        );
    }

    /// bsv-low #598 (the B-OWED-2 lens's HIGH-1): the word events of the pot's own txid as this tip PRODUCES them,
    /// stored through the shipped [`crate::tracker_host::stored::UPSERT`] under real SQLite and read back as the
    /// recompute reads them (`tx_words.*`, [`crate::tracker_host::stored::Row::rechecked`]): the overlay's `seen`
    /// event (the latch), then each `callback` event with Arcade's live body `bodies[i]` (the producer's own
    /// `Verdict::from_arc_body`, `into_hint`, `terminal_word`, the step, `stored::project` and `mark_terminal`).
    pub(crate) fn produced_word_views(
        txid: &str,
        bodies: &[&str],
    ) -> std::collections::BTreeMap<String, Value> {
        use crate::tracker_host::{stored, HeaderSnapshot};
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(stored::CREATE).unwrap();
        let row = produce_words(&db, txid, bodies, true, 1_000);
        let empty = HeaderSnapshot {
            tip: Ok(0),
            headers: Default::default(),
        };
        [(txid.to_string(), row.rechecked(&empty).unwrap())]
            .into_iter()
            .collect()
    }

    /// The writes of [`produced_word_views`] into `db` (a `tx_words` table already made): the overlay's `seen` event
    /// when `seen_first`, then each `callback` body, 60 s apart from `start`; the stored row after the last.
    pub(crate) fn produce_words(
        db: &rusqlite::Connection,
        txid: &str,
        bodies: &[&str],
        seen_first: bool,
        start: u64,
    ) -> crate::tracker_host::stored::Row {
        use crate::tracker_host::{runtime, stored, HeaderSnapshot};
        let empty = || HeaderSnapshot {
            tip: Ok(0),
            headers: Default::default(),
        };
        let read = |db: &rusqlite::Connection| -> Option<stored::Row> {
            db.query_row(
                "SELECT tx_words.* FROM tx_words WHERE txid = ?1",
                [txid],
                |r| {
                    Ok(stored::Row {
                        txid: r.get("txid")?,
                        word: r.get("word")?,
                        tier: r.get("tier")?,
                        height: r.get("height")?,
                        proof_root: r.get("proof_root")?,
                        proof_height: r.get("proof_height")?,
                        proof_hex: r.get("proof_hex")?,
                        header_hash: r.get("header_hash")?,
                        hint_source: r.get("hint_source")?,
                        hint_at: r.get("hint_at")?,
                        updated_at: r.get("updated_at")?,
                        legacy_body: r.get("legacy_body")?,
                    })
                },
            )
            .ok()
        };
        let write = |db: &rusqlite::Connection, row: &stored::Row| {
            let binds: Vec<rusqlite::types::Value> = stored::upsert_binds(row)
                .into_iter()
                .map(|v| match v {
                    Value::String(s) => rusqlite::types::Value::Text(s),
                    Value::Number(n) => rusqlite::types::Value::Real(n.as_f64().unwrap()),
                    _ => rusqlite::types::Value::Null,
                })
                .collect();
            db.execute(stored::UPSERT, rusqlite::params_from_iter(binds))
                .unwrap();
        };
        let params = bsv_tracker::Params { age_threshold: 60 };
        let mut now = start;
        if seen_first {
            // the overlay's `seen` event: the latch's broadcaster
            let old = read(db);
            let mut state = stored::seed_hint(old.as_ref(), txid);
            runtime::event_hints(&mut state, "seen", true, None, now);
            let mut row = stored::project(old.as_ref(), &state, now, None);
            runtime::mark_terminal(&mut row, old.as_ref(), None);
            write(db, &row);
        }
        for body in bodies {
            now += 60;
            let old = read(db);
            let mut state = stored::seed_hint(old.as_ref(), txid);
            runtime::event_hints(&mut state, "callback", true, None, now);
            let v = bsv_tracker::Verdict::from_arc_body(txid, body.as_bytes()).unwrap();
            let hint = v.into_hint("arcade", now);
            let terminal = runtime::terminal_word(&hint);
            state
                .step(&params, &empty(), bsv_tracker::Input::Hint(hint))
                .unwrap();
            let mut row = stored::project(old.as_ref(), &state, now, None);
            runtime::mark_terminal(&mut row, old.as_ref(), terminal);
            write(db, &row);
        }
        read(db).unwrap()
    }

    /// bsv-low #598 (the B-OWED-2 lens's HIGH-1, loop 22's p27/p28 shape): a SEEN-latched JOIN that Arcade then
    /// answers REJECTED (or a double spend) on its callback, its word stored as this tip's producer stores it, takes
    /// FIX-831's judgement through the real derivation (`unbound`, `funding-not-held`, `fundingHeld: refused`,
    /// `fundingWord` the hint's refusal); a later SEEN callback does not lift it; a SEEN-only history, or a pot the
    /// index proved, keeps `present-refund`. RED on bca99893's reading (the chain word alone, the mark unread): the
    /// produced word is `seen`, the row `refund-due`, `present-refund`, claimable.
    #[test]
    fn fix598f_h1_a_refused_joins_produced_word_takes_the_funding_not_held_judgement() {
        let pot = tx(0x02);
        let body = |status: &str, extra: &str| {
            format!(r#"{{"txid":"{pot}","txStatus":"{status}","extraInfo":"{extra}"}}"#)
        };
        let rejected = body("REJECTED", "arc error 461: missing inputs");
        let double = body("DOUBLE_SPEND_ATTEMPTED", "");
        let seen = body("SEEN_ON_NETWORK", "");
        let e = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let r = [refund_entry(RefundStatus::GateOpen)];
        let mut valid = HashMap::new();
        valid.insert(
            format!("{pot}:0"),
            ValidRefund {
                raw_hex: "00".into(),
                my_sats: Some(19_800),
            },
        );
        let (c, p) = (HashSet::new(), HashSet::new());
        let seen_only: HashSet<String> = [pot.clone()].into_iter().collect();
        let judge = |words: &std::collections::BTreeMap<String, Value>, seen: &HashSet<String>| {
            let unheld = refused_fundings(seen, words);
            let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
            i.spender_words = Some(words);
            i.funding_unheld = &unheld;
            derive_and_pin(&i)
        };
        for (history, want) in [
            (vec![rejected.as_str()], "rejected"),
            (vec![double.as_str()], "conflicted"),
            (vec![rejected.as_str(), seen.as_str()], "rejected"),
        ] {
            let words = produced_word_views(&pot, &history);
            assert_eq!(
                words[&pot]["word"], "seen",
                "the tracker keeps the word (NS-3): only the mark says refused"
            );
            let rows = judge(&words, &seen_only);
            assert_eq!(rows.len(), 1);
            assert_eq!(
                (
                    rows[0].family,
                    &rows[0].facts["claim"],
                    &rows[0].facts["claimable"]
                ),
                (
                    OwedFamily::Unbound,
                    &json!(FUNDING_NOT_HELD_CLAIM),
                    &json!(false)
                ),
                "{history:?}: a refund of a funding Arcade refused is never a press"
            );
            assert_eq!(rows[0].facts["fundingHeld"], FUNDING_HELD_REFUSED);
            assert_eq!(rows[0].facts["fundingWord"], want);
            assert_eq!(rows[0].reason.as_deref(), Some(FUNDING_NOT_HELD_REASON));
            assert_eq!(count_refund_funding_unheld(&rows), 1);
            assert_eq!(
                judge(&words, &HashSet::new())[0].facts["claim"],
                "present-refund",
                "the index's proof first"
            );
        }
        let words = produced_word_views(&pot, &[seen.as_str()]);
        assert!(
            words[&pot].get("terminalHint").is_none(),
            "an unmarked view is as before"
        );
        assert_eq!(
            judge(&words, &seen_only)[0].facts["claim"],
            "present-refund"
        );
        assert_eq!(
            judge(&produced_word_views(&pot, &[]), &seen_only)[0].facts["claim"],
            "present-refund",
            "the latch alone"
        );
    }

    /// bsv-low #598 (the B-OWED-2 lens's HIGH-1 (c)): the refusal reads a CHAIN-tier word, never a hint-tier word of
    /// the same name, and a mined view (a checked proof) is never refused whatever its body carries; the producer's
    /// mark is cleared by a mined word (`mark_terminal`). RED with the tier unread.
    #[test]
    fn fix598f_h1_c_the_refusal_reads_the_chain_tier_and_a_mined_word_clears_the_mark() {
        let pot = tx(0x02);
        let one = |v: Value| -> std::collections::BTreeMap<String, Value> {
            [(pot.clone(), v)].into_iter().collect()
        };
        for word in ["rejected", "conflicted"] {
            assert_eq!(
                funding_refusal_word(Some(&one(json!({"word": word, "tier": "hint"}))), &pot),
                None,
                "{word} at the hint tier"
            );
            assert_eq!(
                funding_refusal_word(Some(&one(json!({"word": word, "tier": "chain"}))), &pot),
                Some(word)
            );
        }
        assert_eq!(
            funding_refusal_word(
                Some(&one(
                    json!({"word": "mined", "tier": "chain", "terminalHint": "rejected"})
                )),
                &pot
            ),
            None
        );
        let mut words = produced_word_views(&pot, &[r#"{"txStatus":"REJECTED"}"#]);
        assert_eq!(words[&pot]["terminalHint"], "rejected");
        let marked = crate::tracker_host::stored::Row {
            txid: pot.clone(),
            word: "mined".into(),
            tier: "chain".into(),
            height: None,
            proof_root: None,
            proof_height: None,
            proof_hex: None,
            header_hash: None,
            hint_source: None,
            hint_at: None,
            updated_at: 1.0,
            legacy_body: Some(
                r#"{"trackerTerminal":true,"trackerTerminalWord":"rejected"}"#.into(),
            ),
        };
        let mut row = marked.clone();
        crate::tracker_host::runtime::mark_terminal(&mut row, Some(&marked), Some("rejected"));
        assert_eq!(
            row.legacy_body.as_deref(),
            Some(r#"{"trackerTerminal":false}"#)
        );
        assert_eq!(row.terminal_hint(), None);
        words.clear();
    }

    #[test]
    fn an_unspent_pot_past_the_gate_with_a_valid_filed_refund_is_refund_due_sized_by_the_refunds_output(
    ) {
        let e = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let r = [refund_entry(RefundStatus::GateOpen)];
        let mut valid = HashMap::new();
        valid.insert(
            format!("{}:0", tx(0x02)),
            ValidRefund {
                raw_hex: "00".into(),
                my_sats: Some(19_800),
            },
        );
        let (c, p) = (HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(&e, &r, &[], &valid, &c, &p, Some(900_005)));
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::RefundDue, Some(19_800))
        );
        assert_eq!(rows[0].facts["claim"], "present-refund");
        assert_eq!(rows[0].facts["claimable"], true);
        // LOW-15: a refund paying my home NOTHING is a sizing fault, said as such, never "0 sats"
        let mut zero = HashMap::new();
        zero.insert(
            format!("{}:0", tx(0x02)),
            ValidRefund {
                raw_hex: "00".into(),
                my_sats: Some(0),
            },
        );
        let rows = derive_and_pin(&inputs(&e, &r, &[], &zero, &c, &p, Some(900_005)));
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::RefundDue, None)
        );
        assert!(rows[0].facts["refundSizing"]
            .as_str()
            .unwrap()
            .starts_with("zero"));
        // the gate NOT open yet: the hand's window, a rejoin row sized by my stake
        let rows = derive_and_pin(&inputs(&e, &r, &[], &valid, &c, &p, Some(899_990)));
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::InProgress, Some(20_000))
        );
        assert_eq!(rows[0].facts["claim"], "rejoin");
        // bsv-low #518 (loop 20's pre-flight, 2026-09-21): an EVICTED, never readmitted pot is never a refund-due row
        // (nor any row of its own), whatever its pot row says — a row that outran its eviction before #513 read
        // "unspent" and the brain offered a refund for a pot the network never held (SEEN_IN_ORPHAN_MEMPOOL on
        // every press). The JOIN never formed a pot: the seat's hop carries the money's story.
        let mut evicted = HashSet::new();
        evicted.insert(tx(0x02));
        let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
        i.evicted_pots = &evicted;
        let rows_ev = derive_and_pin(&i);
        assert!(
            rows_ev
                .iter()
                .all(|row| row.family != OwedFamily::RefundDue
                    && row.family != OwedFamily::InProgress),
            "{rows_ev:?}"
        );
        let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(899_990));
        i.evicted_pots = &evicted;
        let rows_ev = derive_and_pin(&i);
        assert!(rows_ev.is_empty(), "{rows_ev:?}");
        assert_eq!(rows[0].facts["blocksToGate"], 10);
        // bsv-low #469 / fleet-loop-16 R1: the rejoin row CARRIES the served recovery gate as the
        // `recoveryHeight` FACT — the client's SECOND rejoin-safe recovery source (ORed beside this device's own
        // refund record in `RejoinWindowLine`), so a seat mid-hand shows "rejoin to finish" even before its local
        // refund record surfaces. It rides `facts.recoveryHeight`, never `at_height` (a future gate is not a mined
        // "at height": the owed list renders/sorts `atHeight` as one), which stays None on an unspent pot.
        assert_eq!(rows[0].facts["recoveryHeight"], 900_000);
        assert!(
            rows[0].at_height.is_none(),
            "an unspent in-progress pot has no mined height"
        );
        // the gate open and NO valid filed refund: unbound, with the tower named as the path
        let none = HashMap::new();
        let rows = derive_and_pin(&inputs(&e, &r, &[], &none, &c, &p, Some(900_005)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("dead-man"));
        // An unknown tip decides no recovery gate or rejoin claim.
        let rows = derive_and_pin(&inputs(&e, &r, &[], &valid, &c, &p, None));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].facts["tipUnknown"], true);
        assert_eq!(rows[0].facts["claimable"], false);
        assert!(rows[0].facts["blocksToGate"].is_null());
        // MEDIUM-9: the gate height is the ONE shared predicate — a committed 0 falls back to the marker's height
        // (`/refund-view` serves gate-open for the same pot), a timestamp-range value is no height at all
        let mut cov_zero = entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A));
        cov_zero.cov_recovery_height = Some(0);
        let rows = derive_and_pin(&inputs(
            std::slice::from_ref(&cov_zero),
            &r,
            &[],
            &valid,
            &c,
            &p,
            Some(900_005),
        ));
        assert_eq!(
            rows[0].family,
            OwedFamily::RefundDue,
            "the marker height 900_000 gates, the committed 0 does not veto it"
        );
        assert_eq!(rows[0].facts["recoveryHeight"], 900_000);
        let mut stamp = entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A));
        stamp.cov_recovery_height = Some(1_800_000_000);
        stamp.recovery_height = 0;
        let rows = derive_and_pin(&inputs(
            std::slice::from_ref(&stamp),
            &[],
            &[],
            &valid,
            &c,
            &p,
            Some(900_005),
        ));
        assert!(
            rows[0].facts["recoveryHeight"].is_null(),
            "a timestamp-range value is no gate height"
        );
        assert_eq!(rows[0].family, OwedFamily::InProgress);
    }

    pub(crate) fn hop(
        status: HopStatus,
        spender: Option<&str>,
        created_ms_ago: Option<i64>,
    ) -> HopEntry {
        HopEntry {
            game_id: tx(0x01),
            hop_txid: tx(0x07),
            hop_vout: 0,
            hop_sats: 20_190,
            opponent_identity: OPP.to_string(),
            seat_settle_pubkey: String::new(),
            seat_sig_hex: String::new(),
            identity_sig_hex: String::new(),
            marker_txid: String::new(),
            marker_vout: 0,
            spent: Some(status == HopStatus::Spent),
            spending_txid: spender.map(str::to_string),
            spent_confirmed: None,
            status,
            status_source: Some("index"),
            marker_verified: MarkerVerification::Verified,
            marker_created_at: created_ms_ago.map(|a| 1_800_000_000_000 - a),
            batch: None,
        }
    }

    #[test]
    fn a_hop_unspent_past_the_window_is_stranded_with_a_sweep_claim_a_young_one_is_in_progress_with_rejoin(
    ) {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let unspent = chain(&key, true, Some(false), None);
        let mut i = inputs(&[], &[], &old, &v, &c, &p, Some(900_000));
        i.hop_chain = &unspent;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::HopStranded, Some(20_190))
        );
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["chainProbe"], "unspent");
        // a YOUNG unspent hop is an `in-progress` row with the felt's rejoin (the stake is funded, the hand has not
        // started), sized by the hop, never a sweep claim before the window
        let young = [hop(HopStatus::Unspent, None, Some(60_000))];
        let rows = derive_and_pin(&inputs(&[], &[], &young, &v, &c, &p, Some(900_000)));
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::InProgress, Some(20_190))
        );
        assert_eq!(rows[0].facts["claim"], "rejoin");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["stage"], "hop-funded");
        assert_eq!(rows[0].facts["strandedAfterMs"], HOP_STRANDED_AFTER_MS);
        assert_eq!(rows[0].reason.as_deref(), Some(YOUNG_HOP_REASON));
        assert!(rows[0].facts.get("sweepRawHex").is_none());
        // an age the brain cannot say, or a marker not yet verified: no row
        let ageless = [hop(HopStatus::Unspent, None, None)];
        assert!(derive_and_pin(&inputs(&[], &[], &ageless, &v, &c, &p, Some(900_000))).is_empty());
        let mut unverified = hop(HopStatus::Unspent, None, Some(60_000));
        unverified.marker_verified = MarkerVerification::Unknown;
        assert!(
            derive_and_pin(&inputs(&[], &[], &[unverified], &v, &c, &p, Some(900_000))).is_empty()
        );
    }

    #[test]
    fn a_hop_spent_by_a_low_pot_is_no_row_a_hop_spent_by_an_unindexed_spender_is_unbound_never_a_custody_story(
    ) {
        let (v, c) = (HashMap::new(), HashSet::new());
        let join = tx(0x02);
        let by_pot = [hop(HopStatus::Spent, Some(&join), Some(10_000_000))];
        let pots: HashSet<String> = [join.clone()].into_iter().collect();
        assert!(
            derive_and_pin(&inputs(&[], &[], &by_pot, &v, &c, &pots, Some(900_000))).is_empty()
        );
        // MEDIUM-7: a spender absent from the index is NOT evidence of a wallet spend (an unindexed JOIN is the common
        // honest case): a sentence, no claim, never "spent elsewhere"
        let elsewhere = [hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000))];
        let rows = derive_and_pin(&inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].facts["claim"].is_null());
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not judge"));
        // a FAULTED spender check says so
        let mut i = inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000));
        i.pot_spenders_faulted = true;
        let rows = derive_and_pin(&i);
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not check"));
    }

    #[test]
    fn the_index_unspent_offers_the_sweep_and_the_chain_rung_only_downgrades_it() {
        // bsv-low #669b: for the owner's own index-UNSPENT stranded hop the index's own word OFFERS the sweep; the
        // chain rung only DOWNGRADES it (a spent word -> spent-elsewhere / no row) and never holds the honest press
        // on its silence. (Pre-#669b this was #451's "the index's unspent alone is a sentence; the chain's unspent is
        // the claim", which held p1's GENUINE sweep no-affordance under a courier outage — the deploy #669 red.)
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        // no chain word yet: the index's unspent offers the sweep (index-unspent), no competitor known
        let rows = derive_and_pin(&inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        // the providers could not look (a fault): still no competitor known -> still claimable on the index
        let not_looked = chain(&key, false, None, None);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &not_looked;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        // the chain's UNSPENT word corroborates: the claim, with the courier-corroborated provenance
        let looked_unspent = chain(&key, true, Some(false), None);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &looked_unspent;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["chainProbe"], "unspent");
        // spent by a POT the index holds (the index lagged): no row
        let join = tx(0x02);
        let pots: HashSet<String> = [join.clone()].into_iter().collect();
        let by_pot = chain(&key, true, Some(true), Some(&join));
        let held = [entry(
            Some(false),
            None,
            Outcome::Refund,
            Some(SeatLetter::A),
        )];
        let mut i = inputs(&held, &[], &old, &v, &c, &pots, Some(900_000));
        i.hop_chain = &by_pot;
        assert!(derive_and_pin(&i).iter().all(|r| r.outpoint != key));
        // bsv-low #837 (amended): the results carrying no row for that pot, the game keeps one row, never a press
        let mut i = inputs(&[], &[], &old, &v, &c, &pots, Some(900_000));
        i.hop_chain = &by_pot;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].facts["joinSpentBy"], "chain");
        assert_eq!(rows[0].facts["claimable"], false);
        // spent by something the index does not hold (a KNOWN competitor): could not judge, the spender named
        let elsewhere = chain(&key, true, Some(true), Some(&tx(0x09)));
        let mut i = inputs(&[], &[], &old, &v, &c, &pots, Some(900_000));
        i.hop_chain = &elsewhere;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].facts["chainProbe"], "spent");
        assert_eq!(rows[0].facts["chainSpender"], tx(0x09));
    }

    #[test]
    fn an_unverified_hop_marker_is_a_sentence_never_a_money_figure_with_a_sweep_press() {
        // N1: `tm_hopparty` admits by byte format; a planted or garbled marker naming me is unbound, sats None
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let mut planted = hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1));
        planted.marker_verified = MarkerVerification::Unverified;
        let rows = derive_and_pin(&inputs(
            &[],
            &[],
            std::slice::from_ref(&planted),
            &v,
            &c,
            &p,
            Some(900_000),
        ));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Unbound, None));
        assert!(rows[0].facts["claim"].is_null());
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("does not verify"));
        let mut pending = hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1));
        pending.marker_verified = MarkerVerification::Unknown;
        let rows = derive_and_pin(&inputs(
            &[],
            &[],
            std::slice::from_ref(&pending),
            &v,
            &c,
            &p,
            Some(900_000),
        ));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("not verified yet"));
    }

    #[test]
    fn i736_a_filed_sweeps_index_latch_is_read_before_the_word_and_a_couriers_is_not() {
        // bsv-low #736: the hop row's latch naming my filed sweep (the index's proof, unproven filing) is not vetoed
        // by the sweep's unchecked word; the chain rung's confirmation (a courier's) still is, as before
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let sweeps = filed(&key, &sweep);
        let unchecked = std::collections::BTreeMap::from([(
            sweep.clone(),
            json!({"word":"stale","verifyPath":null}),
        )]);
        let none = std::collections::BTreeMap::new();
        let mut latched = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        latched.spent_confirmed = Some(true);
        let hops = [latched];
        for words in [&none, &unchecked] {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.hop_sweeps = &sweeps;
            i.spender_words = Some(words);
            assert_eq!(derive_and_pin(&i)[0].facts["claimable"], true);
        }
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        // the COURIER-RUNG lens fold (L2): with no word row the courier's confirmation no longer presses either
        // (it was `(&none, true)`): a courier's word is a hint, never a press
        for (words, claimable) in [(&none, false), (&unchecked, false)] {
            let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
            i.hop_sweeps = &sweeps;
            i.hop_chain = &by_chain;
            i.spender_words = Some(words);
            assert_eq!(derive_and_pin(&i)[0].facts["claimable"], claimable);
        }
    }

    #[test]
    fn a_hop_spent_by_its_own_filed_sweep_is_a_payout_row_claimable_once_the_sweep_mined() {
        // decision 3: the index names my filed sweep as the hop's spender — a payout of the sweep's credit
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let sweeps = filed(&key, &sweep);
        let mut spent = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        spent.spent_confirmed = Some(false);
        let hops = [spent];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::Payout, Some(20_000))
        );
        assert_eq!(rows[0].facts["claim"], "internalize");
        assert_eq!(
            rows[0].facts["claimable"], false,
            "the sweep is seen, not mined: the credit lands with the block"
        );
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        assert_eq!(rows[0].facts["outcome"], "hop-sweep");
        assert_eq!(rows[0].facts["sweepTxid"], sweep);
        assert_eq!(rows[0].facts["creditBeef"], format!("/credit-beef/{sweep}"));
        assert_eq!(rows[0].facts["sweepSource"], "hopsweep-filing");
        // mined: claimable
        let mut mined = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        mined.spent_confirmed = Some(true);
        let hops = [mined];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claimable"], true);
        // the CHAIN rung naming my sweep on an index-unspent hop is the same payout (a sweep broadcast direct to ARC);
        // the COURIER-RUNG lens fold (L2): the courier's confirmation is its hint, the press waits for a checked word
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        i.hop_chain = &by_chain;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["claimable"].as_bool()),
            (OwedFamily::Payout, Some(false))
        );
        assert_eq!(rows[0].facts["spenderPresence"]["source"], "courier");
        // a spender that is NOT my filing (another tx) stays could-not-judge, as before
        let other = [hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000))];
        let mut i = inputs(&[], &[], &other, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        assert_eq!(derive_and_pin(&i)[0].family, OwedFamily::Unbound);
    }

    /// The COURIER-RUNG lens fold (L2, NS-3 and #692's rule): a courier's "confirmed" is a HINT, never a press. A
    /// filed sweep whose only confirming word is the chain rung's (WoC's `/spent` status, or since #868 BananaBlocks'
    /// `spendScope: "confirmed"` under a WoC fault) and that the tracker holds no row for is the pending chain-wait,
    /// never `claimable`, the courier's word served as the row's `spenderPresence` hint; the index's proof, a CHECKED
    /// tracker `mined` word or a chaintracks-checked mined proof of the sweep (`proof_mined_spenders`) presses it.
    /// RED on `f5d2d700`: `left: (Payout, Some(true)) right: (Payout, Some(false))`.
    #[test]
    fn c868f_l2_a_couriers_confirmed_is_never_a_press_by_itself() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let hops = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let filing = filed(&key, &sweep);
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &filing;
        i.hop_chain = &by_chain;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["claimable"].as_bool()),
            (OwedFamily::Payout, Some(false))
        );
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        assert_eq!(rows[0].facts["chainWait"], "block");
        assert_eq!(
            rows[0].facts["spenderPresence"],
            json!({"word": "spent", "tier": "pending", "source": "courier"}),
            "the courier's confirmed is served as the hint"
        );
        assert!(rows[0].facts.get("confirmedSource").is_none());
        // a chaintracks-checked mined proof of the sweep presses it ("proof"), the hint gone
        let proofs = HashSet::from([sweep.clone()]);
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &filing;
        i.hop_chain = &by_chain;
        i.proof_mined_spenders = &proofs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["confirmedSource"], "proof");
        assert!(rows[0].facts.get("spenderPresence").is_none());
        // the index's proof presses it, whatever the courier says
        let proven = filed_proven(&key, &sweep, Some(899_990));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &proven;
        i.hop_chain = &by_chain;
        assert_eq!(derive_and_pin(&i)[0].facts["claimable"], true);
        // a courier that says "unconfirmed" is no hint of a confirmation: the plain wait, no `spenderPresence` here
        let lagging = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(false));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &filing;
        i.hop_chain = &lagging;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claimable"], false);
        assert!(rows[0].facts.get("spenderPresence").is_none());
    }

    #[test]
    fn ns3d_l2_tracker_mined_sweep_names_tracker_proof() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let hops = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let sweep = tx(0x0c);
        let filing = filed(&format!("{}:0", tx(0x07)), &sweep);
        let headers = crate::tracker_host::HeaderSnapshot {
            tip: Ok(10),
            headers: std::collections::BTreeMap::from([(
                10,
                Ok(Some(bsv_tracker::Header {
                    hash: tx(2),
                    merkle_root: sweep.clone(),
                })),
            )]),
        };
        let mut state = bsv_tracker::State::new(&sweep);
        state
            .step(
                &bsv_tracker::Params { age_threshold: 60 },
                &headers,
                bsv_tracker::Input::Evidence(bsv_tracker::Evidence::Proof(
                    bsv_tracker::Proof::new(
                        &sweep,
                        bsv_rs::transaction::MerklePath::from_coinbase_txid(&sweep, 10),
                    )
                    .unwrap(),
                )),
            )
            .unwrap();
        let words = std::collections::BTreeMap::from([(
            sweep,
            crate::tracker_host::served::fields(&state),
        )]);
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &filing;
        i.spender_words = Some(&words);
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(
            rows[0].facts["confirmedSource"], "tracker-proof",
            "tracker proof mislabeled as index proof"
        );
        assert!(rows[0].facts.get("sweepProofHeight").is_none());
    }

    /// bsv-low #517 (loop 19, pair 11, 2026-09-21): the INDEX's own verified proof of the filed sweep confirms the
    /// payout whatever the hop row says (never attributed to a sweep after the JOIN's eviction released its pointer)
    /// and whatever a courier says (an indexer's "unconfirmed" seven minutes after a 1,997-tx block, memoised five
    /// more, kept the row "seen, not mined" thirteen minutes past the mine while three blocks passed in nine).
    #[test]
    fn a_filed_sweep_the_index_holds_proven_is_claimable_whatever_the_hop_row_or_a_courier_says() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        // the hop row as the fleet read it: index-unspent (the pointer released), old enough to be stranded
        let hops = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        // the courier memo: spent by my sweep, NOT confirmed (an indexer lagging the block)
        let lagging = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(false));
        let proven = filed_proven(&key, &sweep, Some(967_696));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        i.hop_chain = &lagging;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::Payout, Some(20_000))
        );
        assert_eq!(
            rows[0].facts["claimable"], true,
            "the index's verified proof outranks the courier's lag"
        );
        assert_eq!(rows[0].facts["confirmedSource"], "index-proof");
        assert_eq!(rows[0].facts["sweepProofHeight"], 967_696);
        assert!(rows[0].facts.get("claimReason").is_none());
        // no courier word at all (never probed): the proof alone names the spender and confirms it
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["claimable"].as_bool()),
            (OwedFamily::Payout, Some(true))
        );
        assert_eq!(rows[0].facts["confirmedSource"], "index-proof");
        // the same filing UNPROVEN by the index keeps the old word: the courier's lag is the row's wait
        let unproven = filed(&key, &sweep);
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &unproven;
        i.hop_chain = &lagging;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        // the proof is keyed on the FILED sweep: a hop the index names spent+confirmed by ANOTHER tx never turns an
        // unproven filing's payout claimable (the 2026-09-19 gate's LOW-3 shape stands)
        let mut other = hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000));
        other.spent_confirmed = Some(true);
        let hops_o = [other];
        let mut i = inputs(&[], &[], &hops_o, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &unproven;
        let rows = derive_and_pin(&i);
        assert_ne!(rows[0].family, OwedFamily::Payout);
        // the row carries the sweep's proven block as its height (the gate's NIT-3)
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        assert_eq!(derive_and_pin(&i)[0].at_height, Some(967_696));
        // an old latch without a recorded height (the gate's NIT-5): proven is proven, no height on the row
        let proven_no_height = filed_proven(&key, &sweep, None);
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven_no_height;
        i.hop_chain = &lagging;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].facts["claimable"].as_bool(), rows[0].at_height),
            (Some(true), None)
        );
        assert!(rows[0].facts.get("sweepProofHeight").is_none());
        // the precedence: the hop row names the sweep spent+confirmed AND the index holds it proven: "index-proof"
        let mut named = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        named.spent_confirmed = Some(true);
        let hops_n = [named];
        let mut i = inputs(&[], &[], &hops_n, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        assert_eq!(
            derive_and_pin(&i)[0].facts["confirmedSource"],
            "index-proof"
        );
    }

    /// #517, the gate's LOW-1 (Rule 6): the proof never outranks a CONTRADICTING confirmed word. A hop the index's own
    /// row, or the chain rung, says was spent+CONFIRMED by ANOTHER tx (a reorg replaced the sweep's block with a
    /// competing spend and the latch was missed) is judged by the ladder that stood before the proof existed: the
    /// chain's spender, never the filing's proof; counted for the operator.
    #[test]
    fn a_proven_filing_never_outranks_a_contradicting_confirmed_spender() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let proven = filed_proven(&key, &sweep, Some(967_696));
        // the hop row: spent+confirmed by ANOTHER tx (0x09), whose bytes the pass did not read
        let mut other = hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000));
        other.spent_confirmed = Some(true);
        let hops_o = [other];
        let mut i = inputs(&[], &[], &hops_o, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        let rows = derive_and_pin(&i);
        assert_ne!(
            rows[0].family,
            OwedFamily::Payout,
            "the proof did not decide against the row's confirmed spender"
        );
        assert!(
            rows[0]
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("could not judge"),
            "{:?}",
            rows[0].reason
        );
        assert_eq!(
            rows[0].facts["sweepProofContradicted"], true,
            "the operator sees WHICH hop"
        );
        assert_eq!(count_sweep_proof_contradictions(&rows), 1);
        // the chain rung naming ANOTHER confirmed spender on an index-unspent hop: the same
        let unspent = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let chain_other = chain_confirmed(&key, true, Some(true), Some(&tx(0x09)), Some(true));
        let mut i = inputs(&[], &[], &unspent, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        i.hop_chain = &chain_other;
        let rows = derive_and_pin(&i);
        assert_ne!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["sweepProofContradicted"], true);
        assert_eq!(count_sweep_proof_contradictions(&rows), 1);
        // an UNCONFIRMED word for another tx does not contradict a proof (a stale pointer the block already settled)
        let mut stale = hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000));
        stale.spent_confirmed = Some(false);
        let hops_s = [stale];
        let mut i = inputs(&[], &[], &hops_s, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["confirmedSource"].as_str()),
            (OwedFamily::Payout, Some("index-proof"))
        );
        assert!(rows[0].facts.get("sweepProofContradicted").is_none());
        assert_eq!(count_sweep_proof_contradictions(&rows), 0);
    }

    #[test]
    fn a_stranded_hop_carries_its_filed_sweep_or_says_sign_here_and_an_unindexed_hop_is_judged_by_the_chain_too(
    ) {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let unspent = chain(&key, true, Some(false), None);
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        // without a filing: the press signs on the owning device
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &unspent;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["sweepSource"], "sign-here");
        assert!(rows[0].facts["sweepRawHex"].is_null());
        assert!(
            rows[0].facts["seatSettlePubkey"].is_string(),
            "the key the owning device matches its derivation against"
        );
        // with a filing: the bytes ride the row
        let sweeps = filed(&key, &tx(0x0c));
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &unspent;
        i.hop_sweeps = &sweeps;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["sweepSource"], "hopsweep-filing");
        assert_eq!(rows[0].facts["sweepTxid"], tx(0x0c));
        assert_eq!(rows[0].facts["sweepRawHex"], "0100".repeat(20));
        // an UNKNOWN hop (the container never indexed) past the window with the chain's unspent: the same claim
        let mut unknown = hop(HopStatus::Unknown, None, Some(HOP_STRANDED_AFTER_MS + 1));
        unknown.spent = None;
        let hops = [unknown];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &unspent;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["claim"].as_str()),
            (OwedFamily::HopStranded, Some("sweep-hop"))
        );
        // …and without a chain word it is the sentence naming the missing index row
        let mut unknown = hop(HopStatus::Unknown, None, Some(HOP_STRANDED_AFTER_MS + 1));
        unknown.spent = None;
        let hops = [unknown];
        let rows = derive_and_pin(&inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("not in the index"));
        // a recorded-but-unconfirmed spend on an Unknown hop keeps its own sentence
        let mut recorded = hop(
            HopStatus::Unknown,
            Some(&tx(0x09)),
            Some(HOP_STRANDED_AFTER_MS + 1),
        );
        recorded.spent = Some(true);
        let hops = [recorded];
        let rows = derive_and_pin(&inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000)));
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("recorded but not confirmed"));
    }

    #[test]
    fn a_spender_that_is_not_a_covenant_pot_never_reads_as_the_join_took_it() {
        // the audit's silent class (2026-09-19): a hop swept by the seat's OLD unfiled sweep; the sweep's output row is
        // a `tm_lowfund` p2pkh row in pot_records, so the recompute's pot-spender set is COVENANT rows only and this
        // spender is not in it → with the bytes paying MY committed home → a payout; paying elsewhere → the story
        let (v, c) = (HashMap::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0d);
        let my_pkh = "11".repeat(20);
        let mut pkhs = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        // index: unspent (the pointer released by the eviction); chain: spent by the sweep, mined
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let no_pots: HashSet<String> = HashSet::new();
        // 1. the bytes pay my home, the home output unspent → a claimable payout (source index-bytes, no raw)
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, Some(false))]);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &mine;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::Payout, Some(20_000))
        );
        assert_eq!(rows[0].facts["sweepSource"], "index-bytes");
        assert_eq!(rows[0].facts["sweepTxid"], sweep);
        assert!(rows[0].facts["sweepRawHex"].is_null());
        // the COURIER-RUNG lens fold (L2): the chain rung's courier confirmation alone is the hint (it was claimable);
        // the sweep's checked mined proof presses it
        assert_eq!(rows[0].facts["claimable"], false);
        let proofs = HashSet::from([sweep.clone()]);
        i.proof_mined_spenders = &proofs;
        assert_eq!(derive_and_pin(&i)[0].facts["claimable"], true);
        // 2. the home output already SPENT (collected and moved on) → not a row
        let moved = pays(&sweep, &[(0, &my_pkh, 20_000, Some(true))]);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &moved;
        i.my_pkh_by_game = &pkhs;
        assert!(derive_and_pin(&i).is_empty());
        // 3. the bytes pay ELSEWHERE → the spent-elsewhere story, no claim
        let elsewhere = pays(&sweep, &[(0, &"99".repeat(20), 20_000, Some(false))]);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        // 4. the bytes unknown (not read this pass) → could not judge, as before
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not judge the spender"));
        // 5. the same through the INDEX pointer (status Spent, the spender not a covenant pot)
        let mut spent = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        spent.spent_confirmed = Some(true);
        let hops = [spent];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &mine;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["claimable"].as_bool()),
            (OwedFamily::Payout, Some(true))
        );
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        assert_eq!(derive_and_pin(&i)[0].facts["spendKind"], "spent-elsewhere");
    }

    #[test]
    fn the_p2pkh_pkh_reader_takes_the_standard_shape_only() {
        let mut lock = vec![0x76, 0xa9, 0x14];
        lock.extend_from_slice(&[0x42; 20]);
        lock.extend_from_slice(&[0x88, 0xac]);
        assert_eq!(
            p2pkh_pkh_hex(&lock).as_deref(),
            Some("42".repeat(20).as_str())
        );
        assert!(p2pkh_pkh_hex(&lock[..24]).is_none());
        assert!(p2pkh_pkh_hex(&[0x51]).is_none());
        let mut covenant = lock.clone();
        covenant.push(0x00);
        assert!(p2pkh_pkh_hex(&covenant).is_none());
    }

    #[test]
    fn a_refused_join_makes_its_young_hop_stranded_now_with_the_sweep_press() {
        // fleet loop 11 (2026-09-19): the JOIN evicted (never readmitted) → the pot is no row (the test below) and
        // the hop, though YOUNG, is stranded at once with its sweep press (the chain word unspent), the reason
        // naming the refusal; without the eviction the young hop keeps the design's in-progress rejoin. The key is
        // the HOP OUTPOINT the eviction ledger released (the gate's HIGH-1), never the game's name.
        let e = [entry(None, None, Outcome::Unresolved, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let young = [hop(HopStatus::Unspent, None, Some(60_000))];
        let key = format!("{}:0", tx(0x07));
        let mut chain: HashMap<String, HopChainWord> = HashMap::new();
        chain.insert(
            key.clone(),
            HopChainWord {
                looked: true,
                spent: Some(false),
                spending_txid: None,
                spent_confirmed: None,
                stale: false,
                age_ms: None,
                probe_tip: None,
                predates_tip: false,
            },
        );
        let mut i = inputs(&e, &[], &young, &v, &c, &p, Some(900_000));
        i.hop_chain = &chain;
        let rows = derive_and_pin(&i);
        assert!(
            rows.iter()
                .any(|r| r.family == OwedFamily::InProgress && r.facts["claim"] == "rejoin"),
            "a young hop of a live game is in-progress"
        );
        assert!(!rows.iter().any(|r| r.family == OwedFamily::HopStranded));
        // the eviction row as the overlay writes it: the pot txid + the released spends (this hop's outpoint)
        let released = format!(
            "[{{\"table\":\"pot_records\",\"txid\":\"{}\",\"vout\":0}}]",
            tx(0x07)
        );
        let refused = released_hop_outpoints(&[(tx(0x02), Some(released))]);
        assert_eq!(refused.len(), 1);
        assert!(refused.contains(&key));
        let evicted: HashSet<String> = [tx(0x02)].into_iter().collect();
        i.evicted_pots = &evicted;
        i.evicted_hop_outpoints = &refused;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "the pot is no row; the hop is the one row");
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["joinRefused"], true);
        assert_eq!(rows[0].reason.as_deref(), Some(JOIN_REFUSED_REASON));
        // HIGH-1: a PLANTED party row naming this identity, a live game and a stranger's own evicted txid releases the
        // STRANGER's hops, never this seat's: the young hop stays in-progress (a name is never the key)
        let planted = format!(
            "[{{\"table\":\"pot_records\",\"txid\":\"{}\",\"vout\":0}}]",
            tx(0x09)
        );
        let strangers = released_hop_outpoints(&[(tx(0x02), Some(planted))]);
        i.evicted_hop_outpoints = &strangers;
        let rows = derive_and_pin(&i);
        assert!(
            rows.iter()
                .any(|r| r.family == OwedFamily::InProgress && r.facts["claim"] == "rejoin"),
            "a plant strands nothing"
        );
        assert!(!rows.iter().any(|r| r.family == OwedFamily::HopStranded));
        // the parser: NULL, malformed and non-hex entries contribute nothing (fail-safe)
        assert!(released_hop_outpoints(&[(tx(0x02), None)]).is_empty());
        assert!(released_hop_outpoints(&[(tx(0x02), Some("not json".into()))]).is_empty());
        assert!(released_hop_outpoints(&[(
            tx(0x02),
            Some("[{\"txid\":\"zz\",\"vout\":0}]".into())
        )])
        .is_empty());
    }

    /// The gate's LOW-3 (2026-09-19): a filed sweep's payout is confirmed by a chain word that NAMES the sweep; a
    /// word naming a DIFFERENT spender (the JOIN, readmitted on its mine) confirms nothing.
    #[test]
    fn a_swept_hops_payout_is_confirmed_only_by_a_chain_word_that_names_the_sweep() {
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let sweeps = filed(&key, &sweep);
        let mut named = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        named.spent_confirmed = None; // the index names the sweep, unconfirmed
        let hops = [named];
        let verified: HashSet<String> = HashSet::new();
        // the chain: spent + confirmed by ANOTHER txid → not confirmed
        let other = tx(0x0d);
        let by_other = chain_confirmed(&key, true, Some(true), Some(&other), Some(true));
        let mut i = inputs(&[], &[], &hops, &v, &verified, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        i.hop_chain = &by_other;
        let rows = derive_and_pin(&i);
        let payout = rows
            .iter()
            .find(|r| r.family == OwedFamily::Payout)
            .expect("the sweep's payout row");
        assert_eq!(
            payout.facts["claimable"], false,
            "a different spender's confirmation is not the sweep's"
        );
        // the chain naming the sweep, confirmed: the COURIER-RUNG lens fold (L2) makes that a hint, not a press (it
        // was claimable); with the sweep's checked mined proof → claimable
        let by_sweep = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        i.hop_chain = &by_sweep;
        let rows = derive_and_pin(&i);
        let payout = rows
            .iter()
            .find(|r| r.family == OwedFamily::Payout)
            .expect("the sweep's payout row");
        assert_eq!(payout.facts["claimable"], false);
        let proofs = HashSet::from([sweep.clone()]);
        i.proof_mined_spenders = &proofs;
        let rows = derive_and_pin(&i);
        let payout = rows
            .iter()
            .find(|r| r.family == OwedFamily::Payout)
            .expect("the sweep's payout row");
        assert_eq!(payout.facts["claimable"], true);
    }

    #[test]
    fn the_attribution_sql_names_the_marker_tables() {
        // MEDIUM-1: the eviction trigger attributes through the seats' own markers, by the marker tables' real columns
        assert!(OWED_ATTRIBUTE_BY_POT_SQL
            .contains("FROM potparty_records WHERE potTxid = ?1 AND potVout = ?2"));
        assert!(OWED_ATTRIBUTE_BY_HOP_SQL
            .contains("FROM hopparty_records WHERE txid = ?1 AND hopVout = ?2"));
    }

    #[test]
    fn an_evicted_pot_is_not_a_row_the_hop_carries_the_story() {
        // the JOIN the network refused never formed a pot: its results entry (spent None) is skipped when the
        // eviction ledger names it; an unadmitted pot without an eviction record keeps its sentence
        let e = [entry(None, None, Outcome::Unresolved, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(&e, &[], &[], &v, &c, &p, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("no spend word"));
        let evicted: HashSet<String> = [tx(0x02)].into_iter().collect();
        let mut i = inputs(&e, &[], &[], &v, &c, &p, Some(900_000));
        i.evicted_pots = &evicted;
        assert!(derive_and_pin(&i).is_empty());
    }

    #[test]
    fn one_collected_marker_retires_a_swept_hops_payout_when_it_is_the_games_only_candidate() {
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let sweeps = filed(&key, &sweep);
        let mut mined = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        mined.spent_confirmed = Some(true);
        let hops = [mined];
        let verified: HashSet<String> = [tx(0x01)].into_iter().collect();
        let mut i = inputs(&[], &[], &hops, &v, &verified, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        assert!(
            derive_and_pin(&i).is_empty(),
            "collected: the sweep's credit is in the wallet"
        );
        // the same game ALSO has a paid pot: two candidates, the marker cannot say which; both rows stay
        let paid = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let mut i = inputs(&paid, &[], &hops, &v, &verified, &no_pots, Some(900_200));
        i.hop_sweeps = &sweeps;
        assert_eq!(derive_and_pin(&i).len(), 2);
    }

    /// bsv-low #492 / #512 (the collect pass's class: "collected N but the wallet read X to X"): a payout whose
    /// paying transaction the identity's wallet ALREADY HOLDS, said by its own verified held filing, is not a row,
    /// so there is nothing to press. The filing names the transaction: another transaction's payout stays.
    #[test]
    fn i492_a_payout_the_wallet_already_holds_is_not_served_and_the_filing_names_one_transaction_only(
    ) {
        let won = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        // before any filing: the row is served claimable, and it names what a device asks its wallet about
        let rows = derive_and_pin(&inputs(&won, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["payTxid"], tx(0x03));
        assert_eq!(rows[0].facts["heldFiling"], HELD_FILING_TAG);
        // the wallet holds the settle (the live credit landed it): not a row
        let held: HashSet<String> = [held_key(&tx(0x01), &tx(0x03))].into_iter().collect();
        let mut i = inputs(&won, &[], &[], &v, &c, &p, Some(900_200));
        i.held_verified = &held;
        assert!(
            derive_and_pin(&i).is_empty(),
            "held: nothing to collect, nothing to press"
        );
        // a filing naming ANOTHER transaction (a spend of the pot that was replaced, another game's) retires nothing
        for other in [
            held_key(&tx(0x01), &tx(0x04)),
            held_key(&tx(0x09), &tx(0x03)),
        ] {
            let held: HashSet<String> = [other].into_iter().collect();
            let mut i = inputs(&won, &[], &[], &v, &c, &p, Some(900_200));
            i.held_verified = &held;
            let rows = derive_and_pin(&i);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].facts["claimable"], true);
        }
        // a decided loss stays no row, an unspent pot is untouched by a filing naming a txid it does not have
        let open = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let mut i = inputs(&open, &[], &[], &v, &c, &p, Some(899_000));
        i.held_verified = &held;
        assert_eq!(derive_and_pin(&i).len(), 1);
    }

    /// N10 closed for a device that says WHICH: two payouts under one game (the pot paid AND a hop's sweep came
    /// home). The v1 marker retires neither (it cannot say which); a held filing retires exactly the one it names,
    /// and the other stays claimable until its own.
    #[test]
    fn i492_a_held_filing_retires_exactly_its_payout_when_the_game_has_two() {
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let sweeps = filed(&key, &sweep);
        let mut mined = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        mined.spent_confirmed = Some(true);
        let hops = [mined];
        let paid = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let v1: HashSet<String> = [tx(0x01)].into_iter().collect();
        let rows_with = |held: &HashSet<String>| {
            let mut i = inputs(&paid, &[], &hops, &v, &v1, &no_pots, Some(900_200));
            i.hop_sweeps = &sweeps;
            i.held_verified = held;
            derive_and_pin(&i)
        };
        assert_eq!(
            rows_with(&HashSet::new()).len(),
            2,
            "the v1 marker alone cannot say which"
        );
        // the swept hop's row names its sweep as the paying transaction
        let both = rows_with(&HashSet::new());
        let sweep_row = both.iter().find(|r| r.outpoint == key).unwrap();
        assert_eq!(sweep_row.facts["payTxid"], sweep);
        assert_eq!(sweep_row.facts["heldFiling"], HELD_FILING_TAG);
        // the wallet holds the pot's payout: the sweep's row stays
        let held_pot: HashSet<String> = [held_key(&tx(0x01), &tx(0x03))].into_iter().collect();
        let rows = rows_with(&held_pot);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outpoint, key);
        assert_eq!(rows[0].facts["claimable"], true);
        // the wallet holds the sweep: the pot's row stays
        let held_sweep: HashSet<String> = [held_key(&tx(0x01), &sweep)].into_iter().collect();
        let rows = rows_with(&held_sweep);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outpoint, format!("{}:0", tx(0x02)));
        // both held: nothing is served
        let held_both: HashSet<String> = held_pot.union(&held_sweep).cloned().collect();
        assert!(rows_with(&held_both).is_empty());
        // a swept hop's payout that is the game's only one retires by its held filing with no v1 marker at all
        let mut i = inputs(&[], &[], &hops, &v, &NONE, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        i.held_verified = &held_sweep;
        assert!(derive_and_pin(&i).is_empty());
    }

    /// The production schema on real SQLite (every overlay migration, the re-run "duplicate column" ignored).
    fn production_sqlite() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    e.to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate column"),
                    "production migration failed under real SQLite: {e}"
                );
            }
        }
        conn
    }

    /// The judge's read (`owed_judge::COLLECTED_SQL`, one game per call here) and fold of `me`'s rows, as the
    /// judge's hydration runs them: the statement, the four columns, `CollectedFold::row`.
    fn fold_collected(conn: &rusqlite::Connection, me: &str, games: &[String]) -> CollectedFold {
        let mut fold = CollectedFold::default();
        let mut stmt = conn.prepare(crate::owed_judge::COLLECTED_SQL).unwrap();
        for game in games {
            let one = serde_json::json!([game]).to_string();
            let rows = stmt
                .query_map(rusqlite::params![me, one], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                })
                .unwrap();
            for r in rows {
                let (g, sig, txid, pay) = r.unwrap();
                fold.row(me, &g, sig.as_deref(), txid.as_deref(), pay.as_deref());
            }
        }
        fold
    }

    /// The filing door for a held filing, as the route runs it on the table: the verdict (`verify_record_post`),
    /// the record's own cap statement and cap (`cap_queries`, `cap_refusal_for`), the ONE insert with its bind
    /// order. Answers the row's key and whether the insert WROTE a row.
    fn file_held(
        conn: &rusqlite::Connection,
        signer: &bsv_rs::wallet::ProtoWallet,
        id: &[u8],
        gid: &[u8; 32],
        pay: &[u8; 32],
        created_at: i64,
    ) -> std::result::Result<(String, bool), crate::record_post::RecordRefusal> {
        use crate::record_post::tests::{script, sign};
        use crate::record_post::{
            cap_queries, cap_refusal_for, collected_protocol, held_challenge, insert_wrote_a_row,
            verify_record_post, RecordKind, VerifiedRecord, COLLECTED_FILE_SQL, HELD_TAG,
        };
        let (me, game) = (hex::encode(id), hex::encode(gid));
        let sig = sign(
            signer,
            collected_protocol(),
            &game,
            &held_challenge(&game, &me, &hex::encode(pay)),
        );
        let verified = verify_record_post(
            RecordKind::Collected,
            &script(&[HELD_TAG, gid, id, pay, &sig]),
            &me,
        )?;
        let VerifiedRecord::Held(r, pay_txid) = &verified else {
            panic!("a held filing")
        };
        let (rows_sql, binds, _, _) = cap_queries(&verified);
        let others: i64 = conn
            .query_row(
                rows_sql,
                rusqlite::params![binds[0], binds[1], binds[2]],
                |row| row.get(0),
            )
            .unwrap();
        if let Some(refusal) = cap_refusal_for(&verified, others, 0) {
            return Err(refusal);
        }
        let changes = conn
            .execute(
                COLLECTED_FILE_SQL,
                rusqlite::params![
                    r.identity, r.game_id, r.txid, 0i64, r.sig_hex, created_at, pay_txid
                ],
            )
            .unwrap();
        Ok((r.txid.clone(), insert_wrote_a_row(Some(changes))))
    }

    /// THE B3 LENS FOLD M1 (the bounded-walk starvation class, ledger 2026-09-19): an identity with 300 held
    /// filings in its window, one per game, on real SQLite with real signatures through the door. EVERY one
    /// retires its payout row, on EVERY recompute. On `f9fd009` the read replayed each stored signature inside a
    /// budget of 256 per recompute over the games in sorted order: the same 44 games past the cut kept their rows
    /// served claimable on every pass, for good. Now the door writes the txid it verified and the read matches
    /// it: no signature, no budget, no order.
    #[test]
    fn i492_m1_three_hundred_held_filings_retire_every_row_on_every_recompute() {
        use crate::record_post::tests::{identity, wallet};
        const N: usize = 300;
        let conn = production_sqlite();
        let w = wallet(31);
        let id = identity(&w);
        let me = hex::encode(&id);
        // game k: its own pot, its own settle, this seat the winner
        let b32 = |tag: u8, k: usize| -> [u8; 32] {
            let mut b = [tag; 32];
            b[30] = (k >> 8) as u8;
            b[31] = k as u8;
            b
        };
        let paid: Vec<ResultEntry> = (0..N)
            .map(|k| {
                let mut e = entry(
                    Some(true),
                    Some(PotVerdict::WinnerA),
                    Outcome::Won,
                    Some(SeatLetter::A),
                );
                e.game_id = hex::encode(b32(0xa0, k));
                e.pot_txid = hex::encode(b32(0xb0, k));
                e.settle_txid = Some(hex::encode(b32(0xc0, k)));
                e
            })
            .collect();
        let games: Vec<String> = paid.iter().map(|e| e.game_id.clone()).collect();
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let served = || -> Vec<OwedRow> {
            let fold = fold_collected(&conn, &me, &games);
            assert_eq!(fold.held_key_mismatches, 0);
            let mut i = inputs(&paid, &[], &[], &v, &fold.verified, &no_pots, Some(900_200));
            i.identity_lc = &me;
            i.collected_present = &fold.present;
            i.held_verified = &fold.held;
            derive_and_pin(&i)
        };
        assert_eq!(served().len(), N, "nothing filed: every payout is served");
        for k in 0..N {
            let (_, written) = file_held(
                &conn,
                &w,
                &id,
                &b32(0xa0, k),
                &b32(0xc0, k),
                1_000 + k as i64,
            )
            .unwrap();
            assert!(written);
        }
        assert_eq!(
            fold_collected(&conn, &me, &games).held.len(),
            N,
            "every filing is read as held: no cut at 256"
        );
        // every recompute, not only the first: three passes, each from the table
        for pass in 0..3 {
            let rows = served();
            assert!(
                rows.is_empty(),
                "pass {pass}: {} of {N} held payouts are still served",
                rows.len()
            );
        }
        // and the retire is exact at this scale too: a game whose spend is replaced is served again, alone
        let mut replaced = paid.clone();
        replaced[N - 1].settle_txid = Some(tx(0xee));
        let fold = fold_collected(&conn, &me, &games);
        let mut i = inputs(
            &replaced,
            &[],
            &[],
            &v,
            &fold.verified,
            &no_pots,
            Some(900_200),
        );
        i.identity_lc = &me;
        i.held_verified = &fold.held;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].game_id, games[N - 1]);
    }

    /// THE B3 LENS FOLD L3: THE GATE of the fold (`CollectedFold::row`: a row retires by its `payTxid` only when
    /// its own key is the content key the door files that (game, identity, txid) under). Five rows a stranger
    /// planted through the chain's byte-format admission (keyed by a chain outpoint, unsigned junk, free under
    /// lenient submit), here given every advantage: each one even carries the victim's real paying txid in the
    /// column, which no writer but the door ever sets. They retire NOTHING, and they do not crowd the real
    /// filing out: filed after them, it retires its row. Remove the gate and the first half turns RED (the
    /// planted rows retire the victim's payout with no signature of the victim anywhere).
    #[test]
    fn i492_l3_planted_chain_rows_never_retire_a_payout_nor_crowd_out_the_real_filing() {
        use crate::record_post::tests::{identity, wallet};
        let conn = production_sqlite();
        let w = wallet(41);
        let id = identity(&w);
        let me = hex::encode(&id);
        let (gid, settle) = ([0x01u8; 32], [0x03u8; 32]);
        let game = tx(0x01);
        let won = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let served = || -> (Vec<OwedRow>, CollectedFold) {
            let fold = fold_collected(&conn, &me, std::slice::from_ref(&game));
            let mut i = inputs(&won, &[], &[], &v, &fold.verified, &no_pots, Some(900_200));
            i.identity_lc = &me;
            i.collected_present = &fold.present;
            i.held_verified = &fold.held;
            (derive_and_pin(&i), fold)
        };
        // five planted chain rows naming (me, the game), read BEFORE any real filing (lower rowids)
        for b in 0..5u8 {
            conn.execute(
                "INSERT INTO collected_markers_v2 (identity, gameId, txid, outputIndex, sigHex, createdAt, payTxid) VALUES (?1, ?2, ?3, 0, ?4, 50, ?5)",
                rusqlite::params![me, game, tx(0xe0 + b), "30060201010201".to_string() + &hex::encode([b]), tx(0x03)],
            )
            .unwrap();
        }
        let (rows, fold) = served();
        assert_eq!(
            rows.len(),
            1,
            "planted rows retire nothing, whatever their column says"
        );
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(
            rows[0].facts["collectedMarkerPresent"], true,
            "they are provenance, as before"
        );
        assert!(fold.held.is_empty());
        assert_eq!(fold.held_key_mismatches, 5, "and they are counted");
        // three more with a `filed:` key that is not this content's key (no writer makes one; three, so the door's
        // own held cap still admits the real filing below): still nothing (the key is derived, not a prefix)
        for b in 0..3u8 {
            conn.execute(
                "INSERT INTO collected_markers_v2 (identity, gameId, txid, outputIndex, sigHex, createdAt, payTxid) VALUES (?1, ?2, ?3, 0, NULL, 60, ?4)",
                rusqlite::params![me, game, format!("filed:{}", hex::encode([0xd0 + b; 28])), tx(0x03)],
            )
            .unwrap();
        }
        assert_eq!(served().0.len(), 1);
        // the real filing, the ninth row of the pair: it retires its payout
        assert!(file_held(&conn, &w, &id, &gid, &settle, 100).unwrap().1);
        let (rows, fold) = served();
        assert!(
            rows.is_empty(),
            "the identity's own filing is not crowded out"
        );
        assert_eq!(
            fold.held,
            [held_key(&game, &tx(0x03))]
                .into_iter()
                .collect::<HashSet<String>>()
        );
    }

    /// bsv-low #492 / #512 END TO END on real SQLite and real signatures: the door's verdict on a device's held
    /// filing, the production table's row with the txid the door verified, the recompute's read and fold, and
    /// the row gone. The shape is the collect pass's: the wallet holds the settle (the live credit), no v1 marker
    /// was ever filed, the row was served claimable and a press said "Collected". With it: the two caps counted
    /// APART (the B3 lens fold L1) and the re-file that writes nothing (L2).
    #[test]
    fn i492_a_devices_held_filing_retires_its_payout_through_the_door_the_table_and_the_read_on_real_sqlite(
    ) {
        use crate::record_post::tests::{identity, script, sign, wallet};
        use crate::record_post::{
            already_filed_body, cap_queries, cap_refusal_for, collected_challenge,
            collected_protocol, held_challenge, insert_wrote_a_row, verify_record_post, RecordKind,
            RecordRefusal, VerifiedRecord, COLLECTED_FILED_ROWS_SQL, COLLECTED_FILE_SQL,
            HELD_FILED_ROWS_SQL, HELD_FILINGS_PER_GAME,
        };
        let conn = production_sqlite();
        let w = wallet(21);
        let id = identity(&w);
        let me = hex::encode(&id);
        let (gid, settle, sweep_b) = ([0x01u8; 32], [0x03u8; 32], [0x0cu8; 32]);
        let (game, sweep) = (tx(0x01), tx(0x0c));
        // the game pays this seat twice: the pot's payout (settle 03..) and a swept hop's (sweep 0c..)
        let paid = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let hop_key = format!("{}:0", tx(0x07));
        let sweeps = filed(&hop_key, &sweep);
        let mut mined = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        mined.spent_confirmed = Some(true);
        let hops = [mined];
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let file = |pay: &[u8; 32], signer: &bsv_rs::wallet::ProtoWallet, created_at: i64| {
            file_held(&conn, signer, &id, &gid, pay, created_at)
        };
        // the recompute's read and fold, then the derivation
        let served = || -> Vec<OwedRow> {
            let fold = fold_collected(&conn, &me, std::slice::from_ref(&game));
            let mut i = inputs(
                &paid,
                &[],
                &hops,
                &v,
                &fold.verified,
                &no_pots,
                Some(900_200),
            );
            i.identity_lc = &me;
            i.collected_present = &fold.present;
            i.held_verified = &fold.held;
            i.hop_sweeps = &sweeps;
            derive_and_pin(&i)
        };
        // RED's shape: nothing filed, both payouts served claimable (the press that said "Collected")
        let rows = served();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|r| r.family == OwedFamily::Payout && r.facts["claimable"] == true));
        // a stranger cannot retire it: its signature is refused at the door, and a row it plants through the
        // chain's byte-format admission (any signature bytes, a chain txid) is presence only
        assert_eq!(
            file(&settle, &wallet(22), 100).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        let planted = hex::encode(sign(
            &wallet(22),
            collected_protocol(),
            &game,
            &held_challenge(&game, &me, &tx(0x03)),
        ));
        conn.execute(
            COLLECTED_FILE_SQL,
            rusqlite::params![
                me,
                game,
                tx(0xee),
                0i64,
                planted,
                100i64,
                Option::<String>::None
            ],
        )
        .unwrap();
        assert_eq!(served().len(), 2);
        // the device's wallet holds the settle: it files, and the pot's payout is gone; the sweep's stays
        let (key, written) = file(&settle, &w, 200).unwrap();
        assert!(written);
        let stored: Option<String> = conn
            .query_row(
                "SELECT payTxid FROM collected_markers_v2 WHERE txid = ?1",
                rusqlite::params![key],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            stored,
            Some(tx(0x03)),
            "the door writes the txid it verified, in the row's own insert"
        );
        let rows = served();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outpoint, hop_key);
        assert_eq!(rows[0].facts["payTxid"], sweep);
        assert_eq!(
            rows[0].facts["collectedMarkerPresent"], true,
            "the planted row is provenance, as before"
        );
        // L2: filed again (a fresh signature, another device of the identity, a stranger's replay): the same key,
        // NO row written, so the door stales nothing, pushes nothing and counts no filing (`insert_wrote_a_row`
        // is the route's own test of the insert), and it answers 200 with the existing key and `filed: false`
        let (again, written) = file(&settle, &w, 300).unwrap();
        assert_eq!(again, key);
        assert!(!written, "a re-file is a no-op insert");
        assert!(
            insert_wrote_a_row(Some(1)) && insert_wrote_a_row(None) && !insert_wrote_a_row(Some(0))
        );
        let sig = sign(
            &w,
            collected_protocol(),
            &game,
            &held_challenge(&game, &me, &tx(0x03)),
        );
        let refiled = verify_record_post(
            RecordKind::Collected,
            &script(&[crate::record_post::HELD_TAG, &gid, &id, &settle, &sig]),
            &me,
        )
        .unwrap();
        assert_eq!(
            already_filed_body(&refiled),
            json!({ "filed": false, "kind": "collected", "key": key, "alreadyFiled": true, "heldTxid": tx(0x03) })
        );
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM collected_markers_v2 WHERE identity = ?1 AND txid LIKE 'filed:%'", rusqlite::params![me], |r| r.get(0)).unwrap();
        assert_eq!(n, 1);
        assert_eq!(served().len(), 1);
        // the wallet holds the sweep too: nothing is served
        file(&sweep_b, &w, 400).unwrap();
        assert!(served().is_empty());
        // L1, the held cap: HELD_FILINGS_PER_GAME held rows per game, counted apart from the v1 marker
        for b in 0..(HELD_FILINGS_PER_GAME - 2) as u8 {
            file(&[0x90 + b; 32], &w, 500).unwrap();
        }
        assert_eq!(
            file(&[0xa0; 32], &w, 500).unwrap_err(),
            RecordRefusal::TooManyFiled,
            "a fifth held filing of the game"
        );
        assert!(
            file(&settle, &w, 600).is_ok(),
            "a re-file of a held filing on file is never the cap's (its own key is excluded)"
        );
        // L1, the other way (the lens's scenario): the game's four held rows do NOT refuse the identity's v1 marker
        let v1_sig = sign(
            &w,
            collected_protocol(),
            &game,
            &collected_challenge(&game, &me),
        );
        let v1 = verify_record_post(
            RecordKind::Collected,
            &script(&[b"LOW/collected/v1", &gid, &id, &v1_sig]),
            &me,
        )
        .unwrap();
        let VerifiedRecord::Collected(r) = &v1 else {
            panic!("the v1 marker")
        };
        let (rows_sql, binds, _, _) = cap_queries(&v1);
        assert_eq!(rows_sql, COLLECTED_FILED_ROWS_SQL);
        let count = |sql: &str, key: &str| -> i64 {
            conn.query_row(sql, rusqlite::params![me, game, key], |row| row.get(0))
                .unwrap()
        };
        assert_eq!(
            count(rows_sql, &binds[2]),
            0,
            "the v1 marker counts v1 rows only"
        );
        assert_eq!(cap_refusal_for(&v1, 0, 0), None);
        assert_eq!(
            conn.execute(
                COLLECTED_FILE_SQL,
                rusqlite::params![
                    r.identity,
                    r.game_id,
                    r.txid,
                    0i64,
                    r.sig_hex,
                    700i64,
                    Option::<String>::None
                ]
            )
            .unwrap(),
            1
        );
        // and the v1 marker takes no held slot: the held count is still four, a second v1 content is the v1 cap's
        assert_eq!(
            count(HELD_FILED_ROWS_SQL, "filed:none"),
            HELD_FILINGS_PER_GAME
        );
        assert_eq!(count(COLLECTED_FILED_ROWS_SQL, "filed:none"), 1);
        assert_eq!(
            cap_refusal_for(&v1, 1, 0),
            Some(RecordRefusal::TooManyFiled)
        );
        assert!(
            served().is_empty(),
            "filings that name no payout transaction of the game change nothing"
        );
    }

    /// bsv-low #492 (the collect pass of 2026-09-20, p3's game `09f991df…`: the wallet held `+39,200 f40fa0d4…` since
    /// the live credit, the row stayed claimable and the press said "Collected" over sats already held): a payout row
    /// whose HOME OUTPUT (`payTxid`:`payVout`) the index holds SPENT is not a row (the wallet held it and its own key
    /// spent it on), by the INDEX's word alone; an unspent or unread home keeps the row byte for byte, and no other
    /// family is touched. RED with the retirement inert (`retire_index_spent_homes` keeping every row).
    #[test]
    fn fix492_a_payout_whose_home_output_the_index_holds_spent_is_not_a_row() {
        let paid = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let derived = derive_and_pin(&inputs(&paid, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].family, OwedFamily::Payout);
        assert_eq!(derived[0].facts["claimable"], true, "today: a press");
        let settle = derived[0].facts["payTxid"].as_str().unwrap().to_string();
        // no vout named yet: no home to read, nothing retired
        assert!(payout_home_outpoints(&derived).is_empty());
        // the route names the vout from the spend's bytes (`mark_pay_vouts`)
        let mut named = derived.clone();
        named[0].facts["payVout"] = json!(1);
        assert_eq!(payout_home_outpoints(&named), vec![(settle.clone(), 1)]);
        // the index holds the home output UNSPENT (or another output spent): the row stands, byte for byte
        let other: HashSet<String> = [outpoint_key(&settle, 0)].into_iter().collect();
        let mut kept = named.clone();
        assert_eq!(retire_index_spent_homes(&mut kept, &other), 0);
        assert_eq!(kept, named);
        // the index holds it SPENT: not a row
        let spent: HashSet<String> = [outpoint_key(&settle, 1)].into_iter().collect();
        let mut gone = named.clone();
        assert_eq!(
            retire_index_spent_homes(&mut gone, &spent),
            1,
            "a payout the wallet already received and spent on is never a press"
        );
        assert!(gone.is_empty());
        // a non-payout row naming the same outpoint is untouched
        let mut unbound = named.clone();
        unbound[0].family = OwedFamily::Unbound;
        assert_eq!(retire_index_spent_homes(&mut unbound, &spent), 0);
        // the statement: one PK term per outpoint, two binds each, spent rows only
        assert_eq!(
            payout_home_spent_sql(2),
            "SELECT lower(txid) AS txid, outputIndex FROM pot_records WHERE spent = 1 AND \
             ((txid = ?1 AND outputIndex = ?2) OR (txid = ?3 AND outputIndex = ?4))"
        );
    }

    #[test]
    fn i512_m2_a_payout_row_names_the_owned_paying_output() {
        use crate::results::CommittedKeys;
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Transaction, TransactionOutput};
        let (home, other, rake) = ([0x11u8; 20], [0x22u8; 20], [0x33u8; 20]);
        let out = |sats: u64, pkh: &[u8; 20]| TransactionOutput {
            satoshis: Some(sats),
            locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(pkh))
                .unwrap(),
            change: false,
        };
        let raw_of = |outs: &[(u64, &[u8; 20])]| -> (String, Vec<u8>) {
            let mut t = Transaction::new();
            for (sats, pkh) in outs {
                t.outputs.push(out(*sats, pkh));
            }
            (t.id(), t.to_binary())
        };
        let home_hex = hex::encode(home);
        // the three settle shapes: a winner-claim behind a rake output, the same with no rake, a tie
        let (claim, claim_raw) = raw_of(&[(800, &rake), (39_200, &home)]);
        let (bare, bare_raw) = raw_of(&[(900, &home)]);
        let (tie, tie_raw) = raw_of(&[(800, &rake), (19_600, &other), (19_600, &home)]);
        assert_eq!(
            sole_home_vout_of_raw(&claim_raw, &claim, &home_hex),
            Some(1)
        );
        assert_eq!(
            sole_home_vout_of_raw(&bare_raw, &bare, &home_hex),
            Some(0),
            "no rake output: the order moves, so it is read, never assumed"
        );
        assert_eq!(sole_home_vout_of_raw(&tie_raw, &tie, &home_hex), Some(2));
        assert_eq!(
            sole_home_vout_of_raw(&tie_raw, &tie, &hex::encode(other)),
            Some(1),
            "the tie's other seat: its own output"
        );
        // never a pick: no output to the home, two outputs to the home, bytes of another transaction, junk
        let (twice, twice_raw) = raw_of(&[(1, &home), (2, &home)]);
        assert_eq!(
            sole_home_vout_of_raw(&claim_raw, &claim, &hex::encode([0x44u8; 20])),
            None
        );
        assert_eq!(sole_home_vout_of_raw(&twice_raw, &twice, &home_hex), None);
        assert_eq!(
            sole_home_vout_of_raw(&bare_raw, &claim, &home_hex),
            None,
            "the bytes must hash to the paying txid"
        );
        assert_eq!(sole_home_vout_of_raw(&[0u8; 4], &claim, &home_hex), None);

        // THE POT'S ROW: `payPkh` is my committed home by my seat; `payVout` is null until the route read the bytes
        let mut won = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        won.settle_txid = Some(claim.clone());
        won.committed_keys = Some(CommittedKeys {
            pub_a: "02".repeat(33),
            pub_b: "03".repeat(33),
            pay_pkh_a: home_hex.clone(),
            pay_pkh_b: hex::encode(other),
        });
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let paid = [won.clone()];
        let mut rows = derive_and_pin(&inputs(&paid, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (&rows[0].facts["payTxid"], &rows[0].facts["payPkh"]),
            (&json!(claim), &json!(home_hex))
        );
        assert!(rows[0].facts["payVout"].is_null());
        assert_eq!(rows[0].facts["payPkhSource"], "committed-home");
        assert_eq!(
            pay_vout_wanted(&rows),
            vec![(claim.clone(), home_hex.clone())]
        );
        // the route's answer for that (txid, home), and only that: another home's answer marks nothing
        let mut vouts: HashMap<String, u32> = HashMap::new();
        vouts.insert(pay_vout_key(&claim, &hex::encode(other)), 7);
        assert_eq!(mark_pay_vouts(&mut rows, &vouts), 0);
        assert!(rows[0].facts["payVout"].is_null());
        vouts.insert(
            pay_vout_key(&claim, &home_hex),
            sole_home_vout_of_raw(&claim_raw, &claim, &home_hex).unwrap(),
        );
        assert_eq!(mark_pay_vouts(&mut rows, &vouts), 1);
        assert_eq!(rows[0].facts["payVout"], 1);
        assert!(
            pay_vout_wanted(&rows).is_empty(),
            "a named row is asked for no more"
        );
        // seat B of the same pot is paid at ITS home; a seat the binding does not prove names no home and is not asked
        let mut tied = won.clone();
        (tied.verdict, tied.outcome, tied.my_seat) =
            (Some(PotVerdict::Tie), Outcome::Tie, Some(SeatLetter::B));
        let tied = [tied];
        let rows_b = derive_and_pin(&inputs(&tied, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows_b[0].facts["payPkh"], hex::encode(other));
        let mut keyless = won.clone();
        keyless.committed_keys = None;
        let keyless = [keyless];
        let rows_k = derive_and_pin(&inputs(&keyless, &[], &[], &v, &c, &p, Some(900_200)));
        assert!(rows_k[0].facts["payPkh"].is_null() && rows_k[0].facts["payVout"].is_null());
        assert!(
            rows_k[0].facts.get("payPkhSource").is_none(),
            "no payPkh: no source"
        );
        assert!(pay_vout_wanted(&rows_k).is_empty());

        // THE SWEPT HOP'S ROW: the filed sweep's own raw names the output (it hashes to the sweep's txid)
        let (sweep, sweep_raw) = raw_of(&[(19_800, &home)]);
        let hop_key = format!("{}:0", tx(0x07));
        let mut sweeps = filed(&hop_key, &sweep);
        sweeps.get_mut(&hop_key).unwrap().raw_hex = hex::encode(&sweep_raw);
        let mut mined = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        mined.spent_confirmed = Some(true);
        let hops = [mined];
        let pkhs: HashMap<String, String> = [(tx(0x01), home_hex.clone())].into_iter().collect();
        let mut i = inputs(&[], &[], &hops, &v, &NONE, &p, Some(900_000));
        i.hop_sweeps = &sweeps;
        i.my_pkh_by_game = &pkhs;
        let rows_s = derive_and_pin(&i);
        assert_eq!(rows_s.len(), 1);
        assert_eq!(
            (
                &rows_s[0].facts["payTxid"],
                &rows_s[0].facts["payPkh"],
                &rows_s[0].facts["payVout"]
            ),
            (&json!(sweep), &json!(home_hex), &json!(0))
        );
        assert_eq!(rows_s[0].facts["payPkhSource"], "committed-home");
        // a home this list does not know: the filed sweep's sole P2PKH output names itself (bsv-low L25, pinned by
        // `l25_a_*`), and the route is not asked (a sweep is not in `pot_beefs`' contract)
        let mut i = inputs(&[], &[], &hops, &v, &NONE, &p, Some(900_000));
        i.hop_sweeps = &sweeps;
        let rows_n = derive_and_pin(&i);
        assert_eq!(
            (&rows_n[0].facts["payPkh"], &rows_n[0].facts["payVout"]),
            (&json!(home_hex), &json!(0))
        );
        assert_eq!(rows_n[0].facts["payPkhSource"], "filed-sweep");
        assert!(pay_vout_wanted(&rows_n).is_empty());
    }

    #[test]
    fn l25_a_pot_less_hopsweep_row_names_the_filed_sweeps_sole_p2pkh_output() {
        // beta pair 17, 2026-10-14: a hop whose JOIN never assembled has no pot home (`my_pkh_by_game` is empty), and
        // its filed sweep was served `payPkh: null, payVout: null`. The bytes are the trace's own `sweepRawHex` (the
        // L25 lens's NOTE-2): one input spending `edd91866..c3db:0`, one output of 20,170 sats to `3d0e8bcc..669d`
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Transaction, TransactionOutput};
        const SWEEP: &str = "4e3d7436ff8529e7cf56a87682b1b50cb1291ac987202647f6bfab781a654ed7";
        const SWEEP_RAW: &str = "0100000001dbc374dbe28fef26aeb49a01e928d4b77c2069ff394adbfb7d773c986618d9ed000000006b483045022100bee8d7e1dc239a995fa3ebfedd5a02f7a0a33504d9cfc7453e08324a604a192b0220226c667d7e719078486dacdf72bbaad6d87b5af7b94b583b644869d5c2990d17412102752292ff79e0c1827caf0b6e43df06014523ef632b27f7633bc0cd72fd50054affffffff01ca4e0000000000001976a9143d0e8bccb9f1b8f314cd9fa8a478e61d6445669d88ac00000000";
        let pkh_hex = "3d0e8bccb9f1b8f314cd9fa8a478e61d6445669d";
        let pkh: [u8; 20] = hex::decode(pkh_hex).unwrap().try_into().unwrap();
        let real_raw = hex::decode(SWEEP_RAW).unwrap();
        let real = Transaction::from_binary(&real_raw).unwrap();
        assert_eq!(
            real.id(),
            SWEEP,
            "the trace's bytes hash to the sweep's txid"
        );
        assert_eq!(real.inputs.len(), 1);
        assert_eq!(
            real.inputs[0].source_txid.as_deref(),
            Some("edd91866983c777dfbdb4a39ff69207cb7d428e9019ab4ae26ef8fe2db74c3db")
        );
        assert_eq!(real.inputs[0].source_output_index, 0);
        assert_eq!(real.outputs.len(), 1);
        assert_eq!(real.outputs[0].satoshis, Some(20_170));
        assert_eq!(
            hex::encode(real.outputs[0].locking_script.to_binary()),
            format!("76a914{pkh_hex}88ac")
        );
        // the edge rows: the real sweep's version, input and lock time with its outputs replaced (each script as raw
        // bytes); a fresh transaction, since a parsed one caches its hash
        let p2pkh = |p: &[u8; 20]| overlay_discovery::pot::p2pkh_lock(p);
        let raw_of = |outs: &[(u64, Vec<u8>)]| -> (String, Vec<u8>) {
            let mut t = Transaction::new();
            (t.version, t.inputs, t.lock_time) =
                (real.version, real.inputs.clone(), real.lock_time);
            t.outputs = outs
                .iter()
                .map(|(sats, script)| TransactionOutput {
                    satoshis: Some(*sats),
                    locking_script: LockingScript::from_binary(script).unwrap(),
                    change: false,
                })
                .collect();
            (t.id(), t.to_binary())
        };
        let (v, p) = (HashMap::new(), HashSet::new());
        let row_of = |sweep: &str, raw: &[u8]| -> OwedRow {
            let hop_key = format!("{}:0", tx(0x07));
            let mut sweeps = filed(&hop_key, sweep);
            sweeps.get_mut(&hop_key).unwrap().raw_hex = hex::encode(raw);
            let mut mined = hop(HopStatus::Spent, Some(sweep), Some(10_000_000));
            mined.spent_confirmed = Some(true);
            let hops = [mined];
            let mut i = inputs(&[], &[], &hops, &v, &NONE, &p, Some(900_000));
            i.hop_sweeps = &sweeps;
            let mut rows = derive_and_pin(&i);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].facts["sweepSource"], "hopsweep-filing");
            rows.remove(0)
        };
        let named = |row: &OwedRow| {
            (
                row.facts["payPkh"].clone(),
                row.facts["payVout"].clone(),
                row.facts.get("payPkhSource").cloned(),
            )
        };
        let unnamed = (Value::Null, Value::Null, None);
        // THE TRACE'S SWEEP: named, its index and its own pkh, the source `filed-sweep`, the route asked nothing
        let row = row_of(SWEEP, &real_raw);
        assert_eq!(row.facts["payTxid"], json!(SWEEP));
        assert_eq!(
            named(&row),
            (json!(pkh_hex), json!(0), Some(json!("filed-sweep"))),
            "a pot-less hop's filed sweep names its one P2PKH output"
        );
        assert!(pay_vout_wanted(std::slice::from_ref(&row)).is_empty());
        // TWO P2PKH outputs: never a pick
        let (two, two_raw) = raw_of(&[(10_000, p2pkh(&pkh)), (10_170, p2pkh(&[0x22u8; 20]))]);
        assert_eq!(named(&row_of(&two, &two_raw)), unnamed);
        // bytes that do not hash to the sweep's txid: nothing named
        assert_eq!(named(&row_of(&two, &real_raw)), unnamed);
        // NOTE-3 (a): an OP_RETURN BEFORE the one P2PKH: the P2PKH is named at ITS index, never the OP_RETURN's
        let op_return = vec![0x00, 0x6a, 0x03, b'L', b'O', b'W'];
        let (opr, opr_raw) = raw_of(&[(0, op_return), (20_170, p2pkh(&pkh))]);
        assert_eq!(
            named(&row_of(&opr, &opr_raw)),
            (json!(pkh_hex), json!(1), Some(json!("filed-sweep")))
        );
        // NOTE-3 (b): a 25-byte P2PKH LOOKALIKE (OP_CHECKSIGVERIFY for OP_CHECKSIG) is not P2PKH: alone it names
        // nothing; beside the real one it is not counted, so the real one is named
        let mut lookalike = p2pkh(&[0x22u8; 20]);
        assert_eq!((lookalike.len(), lookalike[24]), (25, 0xac));
        lookalike[24] = 0xad;
        let (look, look_raw) = raw_of(&[(20_170, lookalike.clone())]);
        assert_eq!(named(&row_of(&look, &look_raw)), unnamed);
        let (both, both_raw) = raw_of(&[(100, lookalike), (20_070, p2pkh(&pkh))]);
        assert_eq!(
            named(&row_of(&both, &both_raw)),
            (json!(pkh_hex), json!(1), Some(json!("filed-sweep")))
        );
        // NOTE-3 (c): a 0-sat P2PKH beside a funded one is a second P2PKH output: null, never a pick by value. The
        // rule counts SCRIPTS, never sats, so a 0-sat P2PKH that is the SOLE P2PKH output is named all the same
        // (its pkh is the seat's own signed destination; the device's hold rule still judges the outpoint)
        let (zero, zero_raw) = raw_of(&[(20_170, p2pkh(&pkh)), (0, p2pkh(&[0x22u8; 20]))]);
        assert_eq!(named(&row_of(&zero, &zero_raw)), unnamed);
        let (sole0, sole0_raw) = raw_of(&[(0, p2pkh(&pkh))]);
        assert_eq!(
            named(&row_of(&sole0, &sole0_raw)),
            (json!(pkh_hex), json!(0), Some(json!("filed-sweep")))
        );
    }

    #[test]
    fn one_collected_marker_never_hides_a_second_pots_payout_of_the_same_game() {
        // N10: two pots funded under one game id (a re-funding); one verified collected marker names the game
        let a = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        let mut b = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        b.pot_txid = tx(0x05);
        b.settle_txid = Some(tx(0x06));
        let both = [a, b];
        let verified: HashSet<String> = [tx(0x01)].into_iter().collect();
        let (v, p) = (HashMap::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(&both, &[], &[], &v, &verified, &p, Some(900_200)));
        assert_eq!(
            rows.len(),
            2,
            "both payout rows stay; the marker cannot say which pot it meant"
        );
        assert!(rows.iter().all(|r| r.facts["collectedSigVerified"] == true
            && r.facts["collectedMarkerPresent"] == true));
    }

    #[test]
    fn the_service_order_puts_the_actionable_families_first_then_the_newest_spend() {
        // N2b
        let mut rows = vec![
            OwedRow {
                identity: ME.into(),
                outpoint: "b".into(),
                family: OwedFamily::Unbound,
                game_id: String::new(),
                sats: None,
                opponent_identity: None,
                at_height: Some(5),
                facts: Value::Null,
                reason: None,
            },
            OwedRow {
                identity: ME.into(),
                outpoint: "a".into(),
                family: OwedFamily::Payout,
                game_id: String::new(),
                sats: None,
                opponent_identity: None,
                at_height: Some(1),
                facts: Value::Null,
                reason: None,
            },
            OwedRow {
                identity: ME.into(),
                outpoint: "c".into(),
                family: OwedFamily::Payout,
                game_id: String::new(),
                sats: None,
                opponent_identity: None,
                at_height: Some(9),
                facts: Value::Null,
                reason: None,
            },
            OwedRow {
                identity: ME.into(),
                outpoint: "d".into(),
                family: OwedFamily::RefundDue,
                game_id: String::new(),
                sats: None,
                opponent_identity: None,
                at_height: None,
                facts: Value::Null,
                reason: None,
            },
        ];
        sort_rows_for_service(&mut rows);
        assert_eq!(
            rows.iter().map(|r| r.outpoint.as_str()).collect::<Vec<_>>(),
            ["c", "a", "d", "b"]
        );
    }

    #[test]
    fn a_hop_the_view_could_not_judge_is_a_sentence_not_silence() {
        // MEDIUM-10: the hops view's Unknown covers the loudest stranded case (the container never indexed)
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let mut unknown = hop(HopStatus::Unknown, None, Some(10_000_000));
        unknown.spent = None;
        let rows = derive_and_pin(&inputs(
            &[],
            &[],
            std::slice::from_ref(&unknown),
            &v,
            &c,
            &p,
            Some(900_000),
        ));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("not in the index"));
        assert_eq!(rows[0].sats, Some(20_190));
        let mut pending = hop(HopStatus::Unknown, Some(&tx(0x09)), Some(10_000_000));
        pending.spent = Some(true);
        let rows = derive_and_pin(&inputs(
            &[],
            &[],
            std::slice::from_ref(&pending),
            &v,
            &c,
            &p,
            Some(900_000),
        ));
        assert!(rows[0].reason.as_deref().unwrap().contains("not confirmed"));
    }

    #[test]
    fn the_wire_body_is_versioned_and_carries_every_row_field() {
        let e = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_and_pin(&inputs(&e, &[], &[], &v, &c, &p, Some(900_200)));
        let body: Value = serde_json::from_str(&owed_body(
            ME,
            Some(900_200),
            &rows,
            1_800_000_000_000,
            false,
            None,
        ))
        .unwrap();
        assert_eq!(body["v"], 1);
        assert_eq!(body["identity"], ME);
        assert_eq!(body["tip"], 900_200);
        assert_eq!(body["rows"][0]["family"], "payout");
        assert_eq!(body["rows"][0]["sats"], 39_200);
        assert_eq!(body["rows"][0]["gameId"], tx(0x01));
        assert_eq!(body["rows"][0]["facts"]["mySeat"], "A");
        assert_eq!(body["truncated"], false);
        assert_eq!(body["computedAtMs"], 1_800_000_000_000i64);
        assert_eq!(OwedFamily::parse("refund-due"), Some(OwedFamily::RefundDue));
        assert_eq!(OwedFamily::parse("nope"), None);
        // NOTE-21: the event carries the per-copy recipient stamp (the #452 misfiled-copy belt applies)
        let ev = owed_changed_event_body(ME, "filing", 2, 5);
        assert_eq!(ev["recipient"], ME);
        assert_eq!(ev["kind"], "owed-changed");
    }

    #[test]
    fn refund_output_sats_sums_the_outputs_to_my_home() {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Transaction, TransactionOutput};
        let pkh = [0x11u8; 20];
        let other = [0x22u8; 20];
        let mut t = Transaction::new();
        t.outputs.push(TransactionOutput {
            satoshis: Some(19_800),
            locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&pkh))
                .unwrap(),
            change: false,
        });
        t.outputs.push(TransactionOutput {
            satoshis: Some(19_800),
            locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&other))
                .unwrap(),
            change: false,
        });
        t.outputs.push(TransactionOutput {
            satoshis: Some(5),
            locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&pkh))
                .unwrap(),
            change: false,
        });
        let raw = hex::encode(t.to_binary());
        assert_eq!(refund_output_sats(&raw, &hex::encode(pkh)), Some(19_805));
        assert_eq!(
            refund_output_sats(&raw, &hex::encode([0x33u8; 20])),
            Some(0)
        );
        assert_eq!(refund_output_sats("zz", &hex::encode(pkh)), None);
        assert_eq!(refund_output_sats(&raw, "abcd"), None);
    }

    #[test]
    fn a_filed_sweep_the_index_names_but_never_confirmed_is_claimable_once_a_checked_word_confirms_it(
    ) {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0c);
        let key = format!("{}:0", tx(0x07));
        let mut h = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        h.spent_confirmed = Some(false); // the index recorded the spend before its block and never re-checked
        let filed_sweeps = filed(&key, &sweep);
        let mut i = inputs(
            &[],
            &[],
            std::slice::from_ref(&h),
            &v,
            &c,
            &p,
            Some(900_000),
        );
        i.hop_sweeps = &filed_sweeps;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["claimable"], false); // the index's word alone: not yet
                                                       // the COURIER-RUNG lens fold (L2): the courier's word (the same spender, confirmed) no longer heals the press;
                                                       // it is the row's hint (it healed it before the fold)
        let word = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        i.hop_chain = &word;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["spenderPresence"]["source"], "courier");
        // a chaintracks-checked mined proof of the sweep heals it
        let proofs = HashSet::from([sweep.clone()]);
        i.proof_mined_spenders = &proofs;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["sweepSource"], "hopsweep-filing");
    }

    /// A real sweep paying `home_key`'s P2PKH `sats`, and a real spend of that home output signed by `signer`.
    fn sweep_and_home_spend(
        home_key: &bsv_rs::primitives::PrivateKey,
        signer: &bsv_rs::primitives::PrivateKey,
        sats: u64,
    ) -> (String, Vec<u8>) {
        use bsv_rs::script::templates::P2PKH;
        use bsv_rs::script::{Script, ScriptTemplate, SignOutputs, UnlockingScript};
        use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
        let mut sweep = Transaction::new();
        sweep.inputs.push(TransactionInput {
            source_txid: Some(tx(0x07)), // the hop
            source_output_index: 0,
            unlocking_script: Some(UnlockingScript::from_script(Script::new())),
            ..Default::default()
        });
        sweep.outputs.push(TransactionOutput::new(
            sats,
            P2PKH::new().lock(&home_key.public_key().hash160()).unwrap(),
        ));
        let sweep_txid = sweep.id();
        let mut spend = Transaction::new();
        spend
            .add_input_from_tx(sweep, 0, P2PKH::unlock(signer, SignOutputs::All, false))
            .unwrap();
        spend.outputs.push(TransactionOutput::new(
            sats - 100,
            P2PKH::new().lock(&[0x11u8; 20]).unwrap(),
        ));
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(spend.sign())
            .expect("template signing");
        (sweep_txid, spend.to_binary())
    }

    /// bsv-low #485, the proof itself: the home output's spend is PROVEN only by the home key's own signature,
    /// executed here over the bytes; bytes a courier could fabricate (another key's signature, a signature over a
    /// different amount or outpoint, a damaged one) prove nothing.
    #[test]
    fn a_home_outputs_spend_is_proven_by_the_home_keys_signature_and_by_nothing_a_courier_can_fabricate(
    ) {
        let home = bsv_rs::primitives::PrivateKey::random();
        let stranger = bsv_rs::primitives::PrivateKey::random();
        let home_pkh = hex::encode(home.public_key().hash160());
        let (sweep_txid, spend_raw) = sweep_and_home_spend(&home, &home, 20_000);
        assert!(home_output_spend_proven(
            &spend_raw,
            &sweep_txid,
            0,
            &home_pkh,
            20_000
        ));
        assert!(home_output_spend_proven(
            &spend_raw,
            &sweep_txid.to_ascii_uppercase(),
            0,
            &home_pkh.to_ascii_uppercase(),
            20_000
        ));
        // a courier's fabrication: bytes that consume the home output under a key that is not the home's
        let (forged_sweep, forged_raw) = sweep_and_home_spend(&home, &stranger, 20_000);
        assert!(!home_output_spend_proven(
            &forged_raw,
            &forged_sweep,
            0,
            &home_pkh,
            20_000
        ));
        // the signature commits to the amount and to the outpoint
        assert!(!home_output_spend_proven(
            &spend_raw,
            &sweep_txid,
            0,
            &home_pkh,
            20_001
        ));
        assert!(!home_output_spend_proven(
            &spend_raw,
            &sweep_txid,
            1,
            &home_pkh,
            20_000
        ));
        assert!(!home_output_spend_proven(
            &spend_raw,
            &tx(0x0e),
            0,
            &home_pkh,
            20_000
        ));
        // another home, a damaged signature, junk
        assert!(!home_output_spend_proven(
            &spend_raw,
            &sweep_txid,
            0,
            &"cc".repeat(20),
            20_000
        ));
        let mut damaged = spend_raw.clone();
        let at = damaged.len() / 3; // inside input 0's unlocking script (the DER signature)
        damaged[at] ^= 0x01;
        assert!(!home_output_spend_proven(
            &damaged,
            &sweep_txid,
            0,
            &home_pkh,
            20_000
        ));
        assert!(!home_output_spend_proven(
            &[],
            &sweep_txid,
            0,
            &home_pkh,
            20_000
        ));
        assert!(!home_output_spend_proven(
            &spend_raw,
            &sweep_txid,
            0,
            "nothex",
            20_000
        ));
    }

    /// bsv-low #485: a courier-proven payout row (a sweep the index never held, courier outputs carry `spent: null`)
    /// never retired. It now retires when EVERY home output's spend is `Proven` (the home key's signature, verified
    /// by this crate over bytes any courier may carry), and on nothing weaker: a chain word of spent without the
    /// proof, a corroborated unspent, or no look at all leave the row standing, claimable false, saying what was seen.
    /// To red: drop the `home_proven` arm of `swept_home`'s `output_spent`.
    #[test]
    fn a_courier_proven_payout_retires_when_the_home_key_signed_the_sats_away_and_on_no_couriers_word(
    ) {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0e);
        let my_pkh = "cc".repeat(20);
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, None)]); // the issue's shape: courier outputs carry spent: null
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        let mut spent = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        spent.spent_confirmed = Some(true);
        let hops = [spent];
        let courier: HashSet<String> = [sweep.clone()].into_iter().collect();
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let home_key = format!("{sweep}:0");
        let word = |w: Option<HomeSpendWord>| -> HashMap<String, HomeSpendWord> {
            w.map(|w| (home_key.clone(), w)).into_iter().collect()
        };
        let derive = |outs: &HashMap<String, Vec<SpenderOutput>>,
                      words: &HashMap<String, HomeSpendWord>,
                      courier: &HashSet<String>| {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.spender_outputs = outs;
            i.spender_inputs = &spends_it;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = courier;
            i.home_spends = words;
            (derive_and_pin(&i), courier_home_outputs(&i))
        };
        // not looked yet: the row of 2026-09-20, unchanged, and the walk's one candidate
        let (rows, candidates) = derive(&mine, &word(None), &courier);
        assert_eq!((rows.len(), rows[0].family), (1, OwedFamily::Payout));
        assert_eq!(rows[0].facts["creditKind"], "courier-bytes");
        assert_eq!(rows[0].facts["claimable"], false);
        assert!(rows[0].facts.get("homeSpend").is_none());
        assert_eq!(
            candidates,
            vec![CourierHomeOutput {
                sweep_txid: sweep.clone(),
                vout: 0,
                pkh_hex: my_pkh.clone(),
                sats: 20_000
            }]
        );
        // the chain rung's corroborated unspent: the row stands and says the sats were SEEN at the home
        let (rows, _) = derive(&mine, &word(Some(HomeSpendWord::Unspent)), &courier);
        assert_eq!(
            (
                rows.len(),
                rows[0].facts["claimable"].clone(),
                rows[0].facts["homeSpend"].clone()
            ),
            (1, json!(false), json!("unspent"))
        );
        assert_eq!(rows[0].facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON);
        // a courier's word of spent WITHOUT the proof retires nothing
        let (rows, _) = derive(&mine, &word(Some(HomeSpendWord::Unproven)), &courier);
        assert_eq!(
            (
                rows.len(),
                rows[0].facts["claimable"].clone(),
                rows[0].facts["homeSpend"].clone()
            ),
            (1, json!(false), json!("unproven"))
        );
        // THE RETIREMENT: the home key signed the output away
        let (rows, candidates) = derive(&mine, &word(Some(HomeSpendWord::Proven)), &courier);
        assert!(rows.is_empty(), "{rows:?}");
        assert!(
            candidates.is_empty(),
            "a retired output is not walked again inside the pass"
        );
        // two home outputs: one proven is not enough
        let two = pays(
            &sweep,
            &[(0, &my_pkh, 20_000, None), (1, &my_pkh, 190, None)],
        );
        let (rows, candidates) = derive(&two, &word(Some(HomeSpendWord::Proven)), &courier);
        assert_eq!((rows.len(), rows[0].sats), (1, Some(20_190)));
        assert_eq!(candidates.len(), 2);
        let mut both = word(Some(HomeSpendWord::Proven));
        both.insert(format!("{sweep}:1"), HomeSpendWord::Proven);
        assert!(derive(&two, &both, &courier).0.is_empty());
        // a word for ANOTHER outpoint (a stranger's sweep, a planted key) retires nothing of mine
        let other: HashMap<String, HomeSpendWord> =
            [(format!("{}:0", tx(0x0f)), HomeSpendWord::Proven)]
                .into_iter()
                .collect();
        assert_eq!(derive(&mine, &other, &courier).0.len(), 1);
        // an UNCONFIRMED courier payout is not a candidate (the pointer may yet be displaced) and keeps its waiting word
        let mut unconfirmed = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        unconfirmed.spent_confirmed = Some(false);
        let hops_u = [unconfirmed];
        let mut i = inputs(&[], &[], &hops_u, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        assert!(courier_home_outputs(&i).is_empty());
        assert_eq!(
            derive_and_pin(&i)[0].facts["claimReason"],
            UNCONFIRMED_PAYOUT_REASON
        );
        // the index's own bytes are not this walk's business (the index's spend word retires those, as before)
        let none: HashSet<String> = HashSet::new();
        let (rows, candidates) = derive(&mine, &word(None), &none);
        assert_eq!(rows[0].facts["sweepSource"], "index-bytes");
        assert!(candidates.is_empty());
    }

    /// bsv-low #486: a JOIN the door refused SYNCHRONOUSLY (never admitted: no eviction row) left its hops on the
    /// in-progress row with the felt's rejoin, the sweep press only after the 30-minute window. The overlay's
    /// refusal ledger names the hops whose own keys signed the refused transaction, and the young hop is stranded
    /// at once, exactly as the eviction ledger already strands one. This is the DERIVATION's half (the ledger, the
    /// door and the read run on real SQLite in `tests/hops_view_sqlite.rs`).
    /// The lens fold's LOW-1: each door arm has its own sentence, and the 400 arm's no longer says the hand cannot
    /// start (one refused copy does not prove it).
    /// To red: drop `door_refused` from the hop ladder's `join_refused`.
    #[test]
    fn a_join_refused_at_the_door_strands_its_young_hop_at_once_with_the_sentence_of_its_arm() {
        let hop_txid = tx(0x07);
        let young = [hop(HopStatus::Unspent, None, Some(60_000))]; // one minute old
        let key = format!("{hop_txid}:0");
        let ledger = |reason: &str| vec![(hop_txid.to_ascii_uppercase(), 0u32, reason.to_string())];
        let script = door_refused_hops(&ledger("script-refused"), &young);
        let network = door_refused_hops(&ledger("network-rejected: REJECTED"), &young);
        assert_eq!(
            script,
            [(key.clone(), DoorRefusal::Script)]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );
        assert_eq!(network.get(&key), Some(&DoorRefusal::Network));
        let unspent = chain(&key, true, Some(false), None);
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let derive = |refused: &HashMap<String, DoorRefusal>,
                      chain_word: &HashMap<String, HopChainWord>| {
            let mut i = inputs(&[], &[], &young, &v, &c, &p, Some(900_000));
            i.hop_chain = chain_word;
            i.door_refused_hop_outpoints = refused;
            derive_and_pin(&i)
        };
        // THE FIX: stranded now, the sweep press (the Collect on a stranded hop), never the felt's rejoin
        let rows = derive(&script, &unspent);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::HopStranded, Some(20_190))
        );
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["joinRefused"], true);
        assert_eq!(rows[0].facts["joinRefusedBy"], "door");
        assert_eq!(rows[0].facts["doorRefusal"], "script");
        // LOW-1: the 400 arm says what one refused copy proves, and that the sweep returns the seat's own stake
        let said = rows[0].reason.as_deref().unwrap();
        assert_eq!(said, DOOR_SCRIPT_REFUSED_REASON);
        assert!(
            !said.contains("cannot start"),
            "one refused copy does not prove the hand cannot start"
        );
        assert!(
            said.contains("refused at the door")
                && said.contains("swept back now")
                && said.contains("your own stake")
        );
        // the 422 arm (the network's definitive word on bytes whose scripts verified) keeps its sentence
        let rows = derive(&network, &unspent);
        assert_eq!(
            (
                rows[0].family,
                rows[0].facts["doorRefusal"].clone(),
                rows[0].reason.as_deref()
            ),
            (
                OwedFamily::HopStranded,
                json!("network"),
                Some(DOOR_REFUSED_REASON)
            )
        );
        // bsv-low #669b: the index's unspent offers the sweep even with no chain word (the honest press stays fast
        // under a courier outage); the door-refusal fact still rides the claimable row
        let rows = derive(&script, &NO_CHAIN);
        assert_eq!(
            (rows[0].family, rows[0].facts["joinRefused"].clone()),
            (OwedFamily::HopStranded, json!(true))
        );
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        // without the ledger's word: in progress, rejoin, and no door fact
        let rows = derive(&HashMap::new(), &unspent);
        assert_eq!(
            (rows[0].family, rows[0].facts["claim"].clone()),
            (OwedFamily::InProgress, json!("rejoin"))
        );
        assert!(rows[0].facts.get("doorRefusal").is_none());
        // a ledger row for an outpoint the walk does not hold names nothing (the belt behind the SQL's own join)
        assert!(
            door_refused_hops(&[(tx(0x09), 0, "script-refused".to_string())], &young).is_empty()
        );
        assert!(door_refused_hops(
            &[(hop_txid.clone(), 1, "script-refused".to_string())],
            &young
        )
        .is_empty());
        // an eviction's word keeps its own sentence when both ledgers name the hop
        let evicted: HashSet<String> = [key.clone()].into_iter().collect();
        let mut i = inputs(&[], &[], &young, &v, &c, &p, Some(900_000));
        i.hop_chain = &unspent;
        i.door_refused_hop_outpoints = &script;
        i.evicted_hop_outpoints = &evicted;
        let rows = derive_and_pin(&i);
        assert_eq!(
            (
                rows[0].facts["joinRefusedBy"].clone(),
                rows[0].reason.as_deref()
            ),
            (json!("eviction"), Some(JOIN_REFUSED_REASON))
        );
        assert!(rows[0].facts.get("doorRefusal").is_none());
    }

    // ---- the delta fold (2026-10-06): the home walk's ring, its caps, the superseded fallback ----

    fn memo_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    e.to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate column"),
                    "migration failed under real SQLite: {e}"
                );
            }
        }
        conn
    }
    fn write_memos(conn: &rusqlite::Connection, memos: &[crate::hops_view::ProbeMemo]) {
        for m in memos {
            conn.execute(
                crate::hops_view::PROBE_MEMO_UPSERT_SQL,
                rusqlite::params![
                    m.outpoint,
                    m.probed_at_ms,
                    i64::from(m.spent),
                    m.spending_txid,
                    m.spent_confirmed.map(i64::from),
                    m.tip_height.map(|t| t as i64)
                ],
            )
            .unwrap();
        }
    }

    /// bsv-low #484 (merge fold, the merged lens's LOW-1 and N3): THE BUDGET-WAIT SENTENCE PROMISES A CREDIT ONLY
    /// WHERE ONE CAN BE OFFERED. `hops_view::mark_refused_word_rows` wrote "the credit is offered once the answer
    /// is fresh" on every payout it renamed, and two of those can be offered none: a COURIER-BYTES payout (the
    /// index holds no proof, `/credit-beef` assembles nothing: with a fresh confirmed word the same row says so)
    /// and a payout whose hop had its index proof set aside by a rival word (the fresh answer may be that the
    /// rival took the hop). This is where the two lanes' facts meet (#485's `creditKind`, #484's refused word),
    /// through the walk's own words and the full derivation. Each shape has its own sentence, the fact
    /// `chainWordAwaitsProbe` stands on all three, and `chainWait` no longer says "block" on a budget wait.
    /// To red: write `CHAIN_WORD_AWAITS_PROBE_REASON` on every renamed row again.
    #[test]
    fn the_budget_wait_promises_a_credit_only_on_a_row_that_can_be_offered_one() {
        use crate::hops_view::{
            confirmation_refused, mark_refused_word_rows, probe_memo_of,
            set_aside_proofs_a_refused_word_contradicts, stale_memo_word, ChainSpendProbe,
            ProbeMemo, CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON,
            CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON, CHAIN_WORD_AWAITS_PROBE_REASON,
            OWED_CHAIN_WAIT_PROBE, OWED_FACT_CHAIN_WORD_AWAITS_PROBE,
        };
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let now: i64 = 1_800_000_000_000;
        let (clear, read_at) = (now - 7 * 60_000, now - 6 * 60_000); // the memo was read one minute into the grace
        let (sweep, rival, my_pkh) = (tx(0x0e), tx(0x9b), "cc".repeat(20));
        let key = outpoint_key(&tx(0x07), 0);
        let hops = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let pkhs: HashMap<String, String> = [(tx(0x01), my_pkh.clone())].into_iter().collect();
        let memo_naming = |spender: &str| -> ProbeMemo {
            probe_memo_of(
                &tx(0x07),
                0,
                &ChainSpendProbe {
                    known: true,
                    spent: Some(true),
                    spending_txid: Some(spender.to_string()),
                    spent_confirmed: Some(true),
                },
                read_at,
            )
            .expect("a known answer is memoised")
        };
        // THE WALK past the budget, in the route's order: the stale word, the refusal remembered, the set-aside,
        // the derivation, the mark
        let walk =
            |memo: &ProbeMemo, spender: &str, courier: bool, filed: Option<&str>| -> OwedRow {
                let mut refused_words: HashMap<String, Option<String>> = HashMap::new();
                assert!(confirmation_refused(memo, Some(clear)));
                refused_words.insert(
                    key.clone(),
                    memo.spending_txid.as_deref().map(str::to_ascii_lowercase),
                );
                let chain: HashMap<String, HopChainWord> =
                    [(key.clone(), stale_memo_word(memo, now, Some(clear), None))]
                        .into_iter()
                        .collect();
                let mut sweeps: HashMap<String, crate::hopsweep::FiledHopSweep> = filed
                    .map(|f| {
                        (
                            key.clone(),
                            crate::hopsweep::FiledHopSweep {
                                sweep_txid: f.to_string(),
                                raw_hex: "0100".repeat(20),
                                pays_sats: Some(20_000),
                                index_proven: true,
                                index_proof_height: Some(899_990),
                            },
                        )
                    })
                    .into_iter()
                    .collect();
                let set_aside =
                    set_aside_proofs_a_refused_word_contradicts(&mut sweeps, &refused_words);
                let outs = pays(spender, &[(0, &my_pkh, 20_000, None)]);
                let ins: HashMap<String, Vec<(String, u32)>> =
                    [(spender.to_string(), vec![(tx(0x07), 0)])]
                        .into_iter()
                        .collect();
                let couriers: HashSet<String> = if courier {
                    [spender.to_string()].into_iter().collect()
                } else {
                    HashSet::new()
                };
                let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
                i.hop_chain = &chain;
                i.hop_sweeps = &sweeps;
                i.spender_outputs = &outs;
                i.spender_inputs = &ins;
                i.my_pkh_by_game = &pkhs;
                i.courier_spenders = &couriers;
                let mut rows = derive_and_pin(&i);
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    (
                        rows[0].family,
                        rows[0].facts["claimable"].as_bool(),
                        rows[0].facts["claimReason"].as_str()
                    ),
                    (
                        OwedFamily::Payout,
                        Some(false),
                        Some(UNCONFIRMED_PAYOUT_REASON)
                    ),
                    "the row the mark renames"
                );
                assert_eq!(rows[0].facts["chainWait"], "block");
                mark_refused_word_rows(&mut rows, &refused_words, &set_aside);
                rows.remove(0)
            };
        // 1. COURIER BYTES: no credit can be assembled here, and the row never says one is coming
        let row = walk(&memo_naming(&sweep), &sweep, true, None);
        assert_eq!(row.facts["creditKind"], "courier-bytes");
        assert_eq!(
            row.facts["claimReason"],
            CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON
        );
        assert_eq!(row.facts[OWED_FACT_CHAIN_WORD_AWAITS_PROBE], true);
        assert_eq!(row.facts["chainWait"], OWED_CHAIN_WAIT_PROBE);
        assert!(row.facts.get("sweepProofContradicted").is_none());
        //    the same row on a FRESH confirmed word: the sentence the wait now agrees with
        let fresh = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let outs = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let ins: HashMap<String, Vec<(String, u32)>> =
            [(sweep.clone(), vec![(tx(0x07), 0)])].into_iter().collect();
        let couriers: HashSet<String> = [sweep.clone()].into_iter().collect();
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &fresh;
        i.spender_outputs = &outs;
        i.spender_inputs = &ins;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &couriers;
        assert_eq!(
            derive_and_pin(&i)[0].facts["claimReason"],
            COURIER_BYTES_NO_CREDIT_REASON
        );
        // 2. THE PROOF SET ASIDE: a proven filing, a refused word naming a rival whose bytes pay this home
        let row = walk(&memo_naming(&rival), &rival, false, Some(&sweep));
        assert_eq!(row.facts["sweepProofContradicted"], true);
        assert_eq!(
            row.facts["claimReason"],
            CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON
        );
        assert_eq!(row.facts[OWED_FACT_CHAIN_WORD_AWAITS_PROBE], true);
        assert_eq!(row.facts["chainWait"], OWED_CHAIN_WAIT_PROBE);
        //    and it outranks the courier-bytes word (the filed sweep's credit may yet stand)
        let row = walk(&memo_naming(&rival), &rival, true, Some(&sweep));
        assert_eq!(
            (
                row.facts["creditKind"].as_str(),
                row.facts["claimReason"].as_str()
            ),
            (
                Some("courier-bytes"),
                Some(CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON)
            )
        );
        // 3. AN INDEX-HELD SWEEP, nothing set aside: the one row a credit can be offered on keeps the promise
        let row = walk(&memo_naming(&sweep), &sweep, false, None);
        assert!(
            row.facts.get("creditKind").is_none()
                && row.facts.get("sweepProofContradicted").is_none()
        );
        assert_eq!(row.facts["claimReason"], CHAIN_WORD_AWAITS_PROBE_REASON);
        assert_eq!(row.facts["chainWait"], OWED_CHAIN_WAIT_PROBE);
        // the words: only the third speaks of a credit being offered; all three name the reorg and the re-ask
        assert!(CHAIN_WORD_AWAITS_PROBE_REASON.contains("the credit is offered"));
        for other in [
            CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON,
            CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON,
        ] {
            assert!(!other.contains("offered"), "{other}");
            assert!(
                other.contains("read just after a reorg") && other.contains("asked again in turn"),
                "{other}"
            );
        }
        assert!(
            CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON.contains("no credit can be assembled here")
        );
        assert_ne!(OWED_CHAIN_WAIT_PROBE, "block");
    }

    /// The merged lens's LOW-3: A FAULTED PROBE MEMO READ IS COUNTED UNDER ITS OWN NAME, on the isolate's `/health`
    /// and as a durable row the overlay seeds and serves on `/health/invariants.counters`. The row is written with
    /// the shipped counter upsert over the shipped schema (real SQLite); the route's two fault arms call the
    /// counter before they answer empty (the source pin in `hops_view`, beside the all-or-nothing one).
    /// To red: drop `probe_memo_read_faulted` from either fault arm, or rename either constant alone.
    #[test]
    fn a_faulted_probe_memo_read_is_counted_on_health_and_in_the_overlays_counters_real_sqlite() {
        assert_eq!(
            COUNTER_PROBE_MEMO_READ_FAULTS,
            bsv_overlay_cloudflare::hop_probe_memos::COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS,
            "the overlay seeds the row this worker writes"
        );
        let before = owed_health_json()["probeMemoReadFaults"]
            .as_u64()
            .expect("the count is on /health.owed");
        note_probe_memo_read_fault();
        assert_eq!(
            owed_health_json()["probeMemoReadFaults"].as_u64().unwrap(),
            before + 1
        );
        let conn = memo_db();
        for _ in 0..2 {
            conn.execute(
                crate::beef_guard::BUMP_COUNTER_SQL,
                rusqlite::params![COUNTER_PROBE_MEMO_READ_FAULTS, 1i64],
            )
            .unwrap();
        }
        let held: i64 = conn
            .query_row(
                "SELECT value FROM ops_counters WHERE name = ?1",
                [COUNTER_PROBE_MEMO_READ_FAULTS],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(held, 2, "one per faulted read");
        let squash = |s: &str| {
            s.split_whitespace()
                .collect::<String>()
                .replace(",)", ")")
                .replace(",]", "]")
        };
        let routes = squash(include_str!("routes.rs"));
        assert!(routes.contains(&squash("crate::owed::note_probe_memo_read_fault(); let binds = [JsValue::from_str(crate::owed::COUNTER_PROBE_MEMO_READ_FAULTS), JsValue::from_f64(1.0)];")));
    }

    #[test]
    fn ns2_filing_commits_before_the_detached_judge_and_push() {
        let source: String = include_str!("record_post/owed_write.rs").split_whitespace().collect();
        assert!(source.contains("db.run_batch(writes).await?"));
        assert!(!source.contains("judge_with_write"));
        let task = include_str!("record_post/detached.rs");
        assert!(task.contains("wait.wait_until(async move"));
        assert!(task.contains("owed_judge::drain_identities"));
        assert!(task.contains("deadline.run_reserving(work).await"));
        assert!(source.contains("judge::refund_subjects"));
        let door = include_str!("record_post.rs").split("#[cfg(test)]").next().unwrap();
        assert!(!door.contains("OWED_STALE_FOR_POT_SQL"));
        assert!(!door.contains("owed_recompute_and_push_coalesced"));
    }

    #[test]
    fn ns2_read_uses_no_admission_or_detached_walk() {
        let code = include_str!("routes.rs");
        let read = code
            .split("pub async fn owed(req:")
            .nth(1)
            .unwrap()
            .split("async fn gather_result_entries(")
            .next()
            .unwrap();
        assert!(read.contains("crate::owed_read::read"));
        assert!(!read.contains("owed_recompute_admit"));
        assert!(!read.contains("wait_until"));
    }

    // ── bsv-low #669: the owed list's hop-stranded `claimable` rests on a chain probe at the SERVED tip ──────────
    //
    // The loop-28 defect: a `hop-stranded sweep-hop` snapshot computed at tip 971114 (chain probe `unspent`, because
    // the competitor was a mempool double-spend) was served byte-identical through and past block 971115, so Collect
    // offered a dead sweep of an already-mined input. These pins drive the pure fix (`degrade_stale_sweeps`,
    // `should_recompute`, the SQL twin's budget) with NO network; the SQL twin's selectivity under real SQLite and
    // the live cell are elsewhere (`tests/owed_tip_probe_sqlite.rs`, `app/e2e/admit-fast-join-refused.spec.ts`).

    /// A claimable `hop-stranded` row the derivation produces at `snapshot_tip`, chain probe `unspent` (the loop-28
    /// shape): index-unspent past the window, verified marker, the chain rung says unspent.
    fn stranded_sweep_rows(snapshot_tip: u64) -> Vec<OwedRow> {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let unspent = chain(&key, true, Some(false), None);
        let mut i = inputs(&[], &[], &old, &v, &c, &p, Some(snapshot_tip));
        i.hop_chain = &unspent;
        let rows = derive_and_pin(&i);
        assert_eq!(rows[0].family, OwedFamily::HopStranded, "precondition");
        assert_eq!(rows[0].facts["claim"], "sweep-hop", "precondition");
        assert_eq!(rows[0].facts["claimable"], true, "precondition");
        assert_eq!(rows[0].facts["chainProbe"], "unspent", "precondition");
        rows
    }

    /// PIN A1 (RED on the stub): the loop-28 body (probe tip 971114, served tip 971115) degrades the sweep to a
    /// `pending` chain-wait — never `claim: sweep-hop` pressable. RED line: `assert_eq!(…["claimable"], false)`.
    #[test]
    fn pin669_a1_a_moved_tip_degrades_the_loop28_sweep_to_pending() {
        let mut rows = stranded_sweep_rows(971_114);
        degrade_stale_sweeps(&mut rows, Some(971_114), Some(971_115));
        assert_eq!(
            rows[0].facts["claimable"], false,
            "a block arrived since the compute: never a dead sweep"
        );
        assert!(rows[0].facts["claim"].is_null());
        assert_eq!(rows[0].facts["chainWait"], "block");
        assert_eq!(rows[0].facts["tipMoved"], true);
        assert_eq!(rows[0].facts["snapshotTip"], 971_114);
        assert_eq!(rows[0].facts["servedTip"], 971_115);
        assert_eq!(rows[0].reason.as_deref(), Some(SWEEP_TIP_MOVED_REASON));
        // the row stays hop-stranded (a degraded display, re-probed on the next tip), not a different family
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
    }

    /// PIN A2 (guard): a probe at the SERVED tip stays claimable — the fix does not pend an honest sweep on a quiet
    /// chain. A cold isolate (present tip unknown) is the fail-safe: it degrades.
    #[test]
    fn pin669_a2_a_tip_current_probe_stays_claimable() {
        let mut rows = stranded_sweep_rows(971_114);
        degrade_stale_sweeps(&mut rows, Some(971_114), Some(971_114));
        assert_eq!(
            rows[0].facts["claim"], "sweep-hop",
            "tip-current: the claim stands"
        );
        assert_eq!(rows[0].facts["claimable"], true);
        assert!(rows[0].facts.get("chainWait").is_none());
        assert!(rows[0].facts.get("tipMoved").is_none());
        // fail-safe: the present tip cannot be read -> degrade (cannot prove tip-current)
        let mut rows = stranded_sweep_rows(971_114);
        degrade_stale_sweeps(&mut rows, Some(971_114), None);
        assert_eq!(
            rows[0].facts["claimable"], false,
            "cannot prove tip-current -> pending"
        );
        assert_eq!(rows[0].facts["chainWait"], "block");
        assert_eq!(rows[0].facts["tipUnknown"], true);
    }

    /// bsv-low #837 (FIX-837) pin (a2), THE BELT and the game's row: the loop-44 pair-1 hop, index-UNSPENT and past
    /// the stranded window with a verified marker and a silent chain rung (the `index-unspent` arm OFFERED its sweep;
    /// Arcade refused it `UTXO_SPENT`), whose held pot's own JOIN spends it (`join_spent_hops`) is never a press; when
    /// the results carry no row for that pot the game keeps an unbound row; when they do, the pot's row tells the story.
    /// The same holds for an index-unspent hop the CHAIN rung says a held pot spent (the refused sweep's eviction
    /// released it): before the fix it `continue`d with no row, so a game whose pot the results did not carry had none
    /// ("no owed row for this game"). An index-SPENT hop a pot took is no row, as before (a written-off game).
    #[test]
    fn fix837_c_a_hop_its_held_pots_join_spent_is_no_press_and_its_game_keeps_a_row() {
        let (v, c) = (HashMap::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let pot = tx(0x02);
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let joined: HashMap<String, String> = [(key.clone(), pot.clone())].into_iter().collect();
        // (1) the belt, the pot not in the results: one unbound row, no claim
        let none = HashSet::new();
        let mut i = inputs(&[], &[], &old, &v, &c, &none, Some(972_190));
        i.join_spent_hops = &joined;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "the game keeps a row: {rows:?}");
        assert_eq!(rows[0].family, OwedFamily::Unbound, "{rows:?}");
        assert_eq!(rows[0].facts["claimable"], false);
        assert!(rows[0].facts["claim"].is_null());
        assert_eq!(rows[0].facts["joinSpent"], pot.as_str());
        assert_eq!(rows[0].facts["joinSpentBy"], "pot-bytes");
        assert_eq!(rows[0].reason.as_deref(), Some(JOINED_POT_UNLINKED_REASON));
        // (1b) the belt with the pot in the results: the pot's own row, no hop row, no sweep
        let held = [entry(
            Some(false),
            None,
            Outcome::Refund,
            Some(SeatLetter::A),
        )];
        let mut i = inputs(&held, &[], &old, &v, &c, &none, Some(972_190));
        i.join_spent_hops = &joined;
        let rows = derive_and_pin(&i);
        assert!(rows.iter().all(|r| r.outpoint != key), "{rows:?}");
        assert!(
            rows.iter().all(|r| r.facts["claim"] != "sweep-hop"),
            "{rows:?}"
        );
        assert!(
            rows.iter().any(|r| r.outpoint == format!("{pot}:0")),
            "{rows:?}"
        );
        // (2) the INDEX says a held pot spent the hop: no hop row, whatever the results (unchanged: a pot outside the
        // era's results is a written-off game)
        let spent = [hop(
            HopStatus::Spent,
            Some(&pot),
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let pots: HashSet<String> = [pot.clone()].into_iter().collect();
        assert!(derive_and_pin(&inputs(&[], &[], &spent, &v, &c, &pots, Some(972_190))).is_empty());
        // (3) the CHAIN rung says a held pot spent the index-unspent hop (the refused sweep's eviction released it):
        // the game keeps a row, never a press
        let word = chain_confirmed(&key, true, Some(true), Some(&pot), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &pots, Some(972_190));
        i.hop_chain = &word;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].facts["joinSpentBy"], "chain");
        assert_eq!(rows[0].facts["claimable"], false);
        // (4) #793: an index-unspent hop whose chain rung holds a CONFIRMED competitor this index does not know reads
        // unbound (spent elsewhere or could not judge), never a press
        let word = chain_confirmed(&key, true, Some(true), Some(&tx(0x0a)), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &none, Some(972_190));
        i.hop_chain = &word;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_ne!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["chainProbe"], "spent");
    }

    /// bsv-low #837 (FIX-837) pin (e), the wiring of (a2): the judge's hydration reads the belt before it derives
    /// (each own hop's seat pots, then the pot's stored bytes, the JOIN's spend of the hop read from them) and hands
    /// it to the derivation. bsv-low #893 section 6.2: the walk's per-pass window and its cut's carry retired with
    /// the walk (a judged subject's rows are replaced whole by its batch; nothing is served from a previous row).
    #[test]
    fn fix837_e_the_judge_wires_the_join_seat_belt() {
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let hydrate = squash(include_str!("owed_judge/hydrate.rs"));
        let mut at = Vec::new();
        for wired in [
            "owed::JOIN_SEAT_POTS_SQL,",
            "read(db, owed::JOIN_POT_BEEF_SQL, vec![Bind::Text(pot.clone())])",
            ".is_some_and(|b| owed::join_spends_outpoint(&b, &pot, hop, *vout))",
            "i.join_spent_hops.insert(owed::outpoint_key(hop, *vout), pot);",
        ] {
            at.push(
                hydrate
                    .find(&squash(wired))
                    .unwrap_or_else(|| panic!("the judge is wired: {wired}")),
            );
        }
        assert!(at.windows(2).all(|w| w[0] < w[1]), "in order: {at:?}");
        let judge = squash(include_str!("owed_judge.rs"));
        assert!(
            judge.contains(&squash("join_spent_hops: &self.join_spent_hops,")),
            "the derivation reads the belt"
        );
    }

    /// bsv-low #837, the FIX-837 lens's LOW-1: the belt reads the pot's stored bytes under `APP_BEEF_LIMITS`, as
    /// every production read of stored bytes in the app layer does: a body the stream refuses (a trailing byte) is no
    /// word. A SOURCE pin: on bsv-rs 0.7.2 `Beef::from_binary` refuses the same bytes the limits' parse does (the
    /// trailing byte below is refused by both, measured on 9d380f8c), so no body tells the two apart today; the pin
    /// holds the one reader. RED on 9d380f8c: "the belt parses through the app's limits".
    #[test]
    fn fix837f_l1_the_belt_reads_the_stored_bytes_under_the_app_limits() {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};
        let mut hop = Tx::new();
        hop.add_input(TransactionInput::new("41".repeat(32), 0))
            .unwrap();
        let mut lock = vec![0x76, 0xa9, 0x14];
        lock.extend_from_slice(&[0x41; 20]);
        lock.extend_from_slice(&[0x88, 0xac]);
        hop.add_output(TransactionOutput {
            satoshis: Some(20_000),
            locking_script: LockingScript::from_binary(&lock).unwrap(),
            change: false,
        })
        .unwrap();
        let mut join = Tx::new();
        let mut input = TransactionInput::new(hop.id(), 0);
        input.source_transaction = Some(Box::new(hop.clone()));
        join.inputs.push(input);
        join.outputs.push(TransactionOutput {
            satoshis: Some(19_800),
            locking_script: LockingScript::from_binary(&[0x51]).unwrap(),
            change: false,
        });
        let beef = bsv_rs::transaction::Beef::from_binary(&join.to_beef(true).unwrap())
            .unwrap()
            .to_binary_atomic(&join.id())
            .unwrap();
        assert!(
            join_spends_outpoint(&beef, &join.id(), &hop.id(), 0),
            "the stored JOIN spends its hop"
        );
        assert!(
            !join_spends_outpoint(&beef, &join.id(), &hop.id(), 1),
            "not another vout"
        );
        let mut trailing = beef.clone();
        trailing.push(0x00);
        assert!(
            !join_spends_outpoint(&trailing, &join.id(), &hop.id(), 0),
            "a body the app's limits refuse is no word"
        );
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let src = squash(include_str!("owed.rs"));
        let at = src.find("pubfnjoin_spends_outpoint(").unwrap();
        let body = &src[at..at + src[at..].find("\n").unwrap_or(600).min(600)];
        assert!(
            body.contains("overlay_engine::beef_limits::parse_beef(beef,&overlay_engine::beef_limits::APP_BEEF_LIMITS"),
            "the belt parses through the app's limits: {body}"
        );
        assert!(!body.contains("Beef::from_binary(beef)"), "{body}");
    }

    /// bsv-low #859 (FIX-859, loop 46 pair 16), the DERIVATION's half: a hop whose JOIN the index holds (the hop's
    /// own row spent by the pot, `spentConfirmed` as the admission leaves it: none) with NO `submit_refusals` row
    /// (the door retires it on the admission) is never a `hop-stranded` row: the JOIN took it, the pot's row tells
    /// the story. The same for the belt (#837): an index-UNSPENT hop its held pot's stored JOIN spends. Expected
    /// green on d370d906: the fresh derivation cannot say `door` without the ledger's row; the loop's source is the
    /// carry, pinned next.
    #[test]
    fn fix859_a_a_hop_its_admitted_join_spent_is_never_derived_stranded_without_the_doors_row() {
        let pot = tx(0x0a);
        let (v, c) = (HashMap::new(), HashSet::new());
        let pots: HashSet<String> = [pot.clone()].into_iter().collect();
        // the index's own word: the hop row spent by the pot, its confirmation unknown (admitted pending)
        let spent = [hop(HopStatus::Spent, Some(&pot), Some(60_000))];
        let i = inputs(&[], &[], &spent, &v, &c, &pots, Some(972_190));
        let rows = derive_and_pin(&i);
        assert!(
            rows.iter().all(|r| r.family != OwedFamily::HopStranded),
            "{rows:?}"
        );
        assert!(rows.is_empty(), "the pot's row tells the story: {rows:?}");
        // the belt: the index never recorded the spend, the pot's stored JOIN spends the hop
        let key = format!("{}:0", tx(0x07));
        let unspent = [hop(HopStatus::Unspent, None, Some(60_000))];
        let by_bytes: HashMap<String, String> = [(key.clone(), pot.clone())].into_iter().collect();
        let mut j = inputs(&[], &[], &unspent, &v, &c, &pots, Some(972_190));
        j.join_spent_hops = &by_bytes;
        let rows = derive_and_pin(&j);
        assert!(
            rows.iter().all(|r| r.family != OwedFamily::HopStranded),
            "{rows:?}"
        );
        assert!(
            rows.iter().all(|r| r.facts.get("joinRefusedBy").is_none()),
            "{rows:?}"
        );
    }

    /// bsv-low #770 (the FIX-736 lens's pre-existing note): the settle payout arm judges `claimable` and
    /// `awaiting_block` exactly as the hop arm judges a payout whose spender the index's own pointer names (the hop
    /// row naming my filed sweep), over every evidence the pass holds for it: the index's proof (none, the latch, a
    /// verified height: the settle's `at_height`, the filed sweep's `transactions.has_proof`) against every word
    /// (no word map, no row, a checked `mined`, an unchecked `mined`, `stale`, `cannotCheck`, a final `seen`,
    /// `rejected`). The two arms already agreed on main (no reachable row differs: the filed sweep's `!contradicted`
    /// guard reads another spender's name, which for an index-named spender only the chain rung carries, and a pot
    /// has no chain rung); the fold makes the settle's two sites one function. RED against the pre-#736 settle rule
    /// (the word first, `spender_mined(i, settle, proof)`): the latch under an unchecked `mined` claims on the hop and
    /// not on the settle; RED against `awaiting_block` reading its own rule (the latch dropped from it): the latched spend
    /// under a final `seen` is a block wait beside a claimable payout.
    #[test]
    fn fix770_a_the_settle_arm_judges_as_the_index_named_hop_arm_over_every_evidence() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let sweep = tx(0x0c);
        let settle = tx(0x03);
        let words = [
            None,
            Some(json!({"word":"mined","verifyPath":{"height":900_100,"root":tx(0x04)}})),
            Some(json!({"word":"mined","verifyPath":null})),
            Some(json!({"word":"mined","verifyPath":null,"cannotCheck":"header snapshot changed"})),
            Some(json!({"word":"stale","verifyPath":null})),
            Some(json!({"word":"seen","verifyPath":null})),
            Some(json!({"word":"rejected","verifyPath":null})),
        ];
        // (the settle's latch, the settle's verified height) beside (the hop row's latch, the filed sweep's proof)
        let proofs: [(Option<bool>, Option<u64>, &str); 4] = [
            (Some(false), None, "no index proof"),
            (None, None, "no latch read"),
            (Some(true), None, "the latch, heightless"),
            (Some(false), Some(900_100), "a verified height"),
        ];
        let mut judged = 0;
        for (latch, height, proof) in proofs {
            for word in &words {
                for map in [false, true] {
                    if !map && word.is_some() {
                        continue;
                    }
                    let settle_words: std::collections::BTreeMap<String, Value> =
                        word.iter().map(|w| (settle.clone(), w.clone())).collect();
                    let sweep_words: std::collections::BTreeMap<String, Value> =
                        word.iter().map(|w| (sweep.clone(), w.clone())).collect();
                    // the hop arm: the hop row's pointer names my filed sweep
                    let sweeps = if height.is_some() {
                        filed_proven(&key, &sweep, height)
                    } else {
                        filed(&key, &sweep)
                    };
                    let mut spent = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
                    spent.spent_confirmed = latch;
                    let hops = [spent];
                    let mut h = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_200));
                    h.hop_sweeps = &sweeps;
                    h.spender_words = map.then_some(&sweep_words);
                    let hop_rows = derive_and_pin(&h);
                    assert_eq!(hop_rows[0].family, OwedFamily::Payout, "{proof} {word:?}");
                    let hop_claimable = hop_rows[0].facts["claimable"].clone();
                    // the settle arm: a won pot (the payout) and an unclassified one (the block-wait sentence)
                    let mut won = entry(
                        Some(true),
                        Some(PotVerdict::WinnerA),
                        Outcome::Won,
                        Some(SeatLetter::A),
                    );
                    won.spent_confirmed = latch;
                    won.at_height = height;
                    let mut unclassified = won.clone();
                    unclassified.verdict = None;
                    unclassified.outcome = Outcome::Unresolved;
                    let judge = |e: &ResultEntry| {
                        let entries = [e.clone()];
                        let mut s = inputs(&entries, &[], &[], &v, &c, &no_pots, Some(900_200));
                        s.spender_words = map.then_some(&settle_words);
                        derive_and_pin(&s).remove(0)
                    };
                    let payout = judge(&won);
                    assert_eq!(payout.family, OwedFamily::Payout);
                    assert_eq!(
                        payout.facts["claimable"], hop_claimable,
                        "{proof}, word {word:?} (map {map}): the settle's press is the hop's"
                    );
                    let wait = judge(&unclassified);
                    assert_eq!(
                        wait.reason.as_deref() == Some(SPEND_AWAITING_BLOCK_REASON),
                        hop_claimable == json!(false),
                        "{proof}, word {word:?} (map {map}): the settle is a block wait exactly where the hop's \
                         payout waits: {wait:?}"
                    );
                    judged += 1;
                }
            }
        }
        assert_eq!(
            judged,
            4 * 8,
            "every evidence judged: seven words with a map, and no map"
        );
    }

    // ---- MONEY-OWED-LENS (2026-10-24): the lens's five cases on PR 168, folded by MONEY-OWED-FOLD ----

    /// LENS-5 (#770): `settle_spend_confirmed` IS main's two expressions, over every evidence the settle arm can
    /// hold: the latch (unread, false, true), the verified height (none, some), the settle txid (none, some) and
    /// every word (no map; a map with no row; a checked `mined`; an unchecked `mined`; `cannotCheck`; `stale`;
    /// `seen`; `rejected`). This is the refactor's own parity, RED against any change of the function; `fix770_a`
    /// is the cross-arm parity.
    #[test]
    fn money_owed_lens_5_the_settle_rule_is_mains_two_expressions_over_every_evidence() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let settle = tx(0x03);
        let words = [
            None,
            Some(json!({"word":"mined","verifyPath":{"height":900_100,"root":tx(0x04)}})),
            Some(json!({"word":"mined","verifyPath":null})),
            Some(json!({"word":"mined","verifyPath":null,"cannotCheck":"header snapshot changed"})),
            Some(json!({"word":"stale","verifyPath":null})),
            Some(json!({"word":"seen","verifyPath":null})),
            Some(json!({"word":"rejected","verifyPath":null})),
        ];
        let mut judged = 0;
        for latch in [None, Some(false), Some(true)] {
            for height in [None, Some(900_100u64)] {
                for settle_txid in [None, Some(settle.clone())] {
                    for word in &words {
                        for map in [false, true] {
                            if !map && word.is_some() {
                                continue;
                            }
                            let m: std::collections::BTreeMap<String, Value> =
                                word.iter().map(|w| (settle.clone(), w.clone())).collect();
                            let mut i = inputs(&[], &[], &[], &v, &c, &p, Some(900_200));
                            i.spender_words = map.then_some(&m);
                            let mut e =
                                entry(Some(true), None, Outcome::Unresolved, Some(SeatLetter::A));
                            e.spent_confirmed = latch;
                            e.at_height = height;
                            e.settle_txid = settle_txid.clone();
                            // main's claimable (f6c0b017^, the settle payout arm)
                            let main_confirmed = e.at_height.is_some()
                                || index_proven_or_spender_mined(
                                    &i,
                                    e.settle_txid.as_deref(),
                                    e.spent_confirmed == Some(true),
                                    e.spent_confirmed == Some(true) || e.at_height.is_some(),
                                );
                            // main's awaiting_block, its evidence half
                            let main_awaiting = e.at_height.is_none()
                                && !index_proven_or_spender_mined(
                                    &i,
                                    e.settle_txid.as_deref(),
                                    e.spent_confirmed == Some(true),
                                    e.spent_confirmed == Some(true) || e.at_height.is_some(),
                                );
                            let now = settle_spend_confirmed(&i, &e);
                            assert_eq!(
                                now, main_confirmed,
                                "{latch:?} {height:?} {settle_txid:?} {word:?} {map}"
                            );
                            assert_eq!(
                                !now, main_awaiting,
                                "{latch:?} {height:?} {settle_txid:?} {word:?} {map}"
                            );
                            judged += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(judged, 3 * 2 * 2 * 8);
    }

    /// PIN A3 (RED on the stub): a competitor mined between two reads. Read 1 (tip 971114) is claimable; served
    /// across block 971115 it degrades to pending (RED line: the middle assert); the re-probe at the new tip sees
    /// the competitor and re-points the hop to `spent-elsewhere` (Unbound, no claim) — never a dead sweep.
    #[test]
    fn pin669_a3_a_competitor_mined_between_two_reads_flips_to_spent_elsewhere() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        // read 1 at tip 971114: index + chain unspent -> claimable sweep-hop
        let mut read1 = stranded_sweep_rows(971_114);
        // the block arrives (971115): the SAME snapshot, served, degrades to a block wait
        degrade_stale_sweeps(&mut read1, Some(971_114), Some(971_115));
        assert_eq!(
            read1[0].facts["claimable"], false,
            "the stale snapshot must not keep offering a dead sweep across a block"
        );
        assert_eq!(read1[0].facts["chainWait"], "block");
        // read 2 at tip 971115: the re-probe sees the competitor spend -> the hop left this game
        let competitor = tx(0x1c);
        let spent = chain_confirmed(&key, true, Some(true), Some(&competitor), Some(true));
        let mut i2 = inputs(&[], &[], &old, &v, &c, &p, Some(971_115));
        i2.hop_chain = &spent;
        let read2 = derive_and_pin(&i2);
        assert_eq!(read2[0].family, OwedFamily::Unbound);
        assert!(read2[0].facts["claim"].is_null());
        assert_eq!(read2[0].facts["chainProbe"], "spent");
        assert_eq!(read2[0].facts["chainSpender"], competitor);
    }

    /// PIN A6 (RED on the stub): the served body is ADDITIVE. A tip-current serve is byte-identical to the
    /// pre-degrade body; a degraded row keeps every other fact and gains only the freshness fields. (The #638
    /// byte-identity pins are untouched: this change writes no `pot_records`/`hopparty_records`/trigger.)
    #[test]
    fn pin669_a6_the_served_body_shape_is_additive() {
        // tip-current: the body does not change
        let mut rows = stranded_sweep_rows(971_114);
        let before = owed_body(ME, Some(971_114), &rows, 1_792_000_000_000, false, None);
        degrade_stale_sweeps(&mut rows, Some(971_114), Some(971_114));
        let after = owed_body(ME, Some(971_114), &rows, 1_792_000_000_000, false, None);
        assert_eq!(before, after, "a tip-current serve is byte-identical");
        // degraded: every other fact survives; only the freshness fields are added and the claim flips
        let mut rows = stranded_sweep_rows(971_114);
        let chain_probe = rows[0].facts["chainProbe"].clone();
        let age = rows[0].facts.get("chainProbeAgeMs").cloned();
        let sweep_source = rows[0].facts["sweepSource"].clone();
        degrade_stale_sweeps(&mut rows, Some(971_114), Some(971_115));
        assert!(rows[0].facts["claim"].is_null());
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(
            rows[0].facts["chainProbe"], chain_probe,
            "chainProbe is kept as provenance"
        );
        assert_eq!(rows[0].facts.get("chainProbeAgeMs").cloned(), age);
        assert_eq!(rows[0].facts["sweepSource"], sweep_source);
    }

    // ── bsv-low #669b: the honest stranded-hop press stays fast; the #451 chain-rung SILENCE no longer holds it ──
    //
    // The deploy of #669 (beta `9330dbe6`, the `spec-admit-fast-join-refused` pair-13 red of 2026-10-15 07:08Z):
    // p1's GENUINELY-UNSPENT stranded hop (`a67c52c0…:0`, 20,190 sats, JOIN evicted) was served `claim:null`,
    // `chainProbe:"pending"`, family Unbound — "the sweep waits for its word" — for the WHOLE 45 s Collect budget,
    // because the couriers (`/spent-any`) were rate-limiting (WoC 429) and never delivered a positive unspent word.
    // The served tip was STATIC (971132) and NONE of the 10 served reads carried `tipMoved`/`tipUnknown`/`chainWait`,
    // so `degrade_stale_sweeps` never fired: the hold was the pre-existing #451 `_` (no-word) arm, NOT the #669 tip
    // gate. The hop is the owner's OWN output and the index itself says unspent, so the honest press must not wait on
    // an external courier. A positive spent word (the spent arm) and the tip gate (`degrade_stale_sweeps` +
    // `should_recompute`) still withhold a dead sweep; a dead sweep that slips both costs at most a refused broadcast.

    /// The deploy shape as a reusable fixture: an index-UNSPENT, JOIN-refused (evicted), verified-marker stranded hop
    /// with NO chain-rung word (`hop_chain` empty: the couriers 429'd), computed at a static `tip`.
    fn index_unspent_no_word_rows(tip: u64) -> Vec<OwedRow> {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let evicted: HashSet<String> = [key].into_iter().collect();
        let mut i = inputs(&[], &[], &old, &v, &c, &p, Some(tip));
        i.evicted_hop_outpoints = &evicted; // the JOIN was evicted: the hop is stranded now
        derive_and_pin(&i)
    }

    /// PIN 669b-A (RED on `9330dbe6`): the deploy's served shape is now claimable on the index's own unspent word.
    /// RED line: `assert_eq!(rows[0].facts["claim"], "sweep-hop")` — today the `_` arm serves `claim:null`,
    /// `chainProbe:"pending"`, family Unbound (the honest press held `no-affordance` for the whole budget).
    #[test]
    fn pin669b_a_an_index_unspent_stranded_hop_with_no_chain_word_is_claimable() {
        let rows = index_unspent_no_word_rows(971_132);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].facts["claim"], "sweep-hop",
            "the owner's own index-unspent stranded hop is sweepable on the index's word (RED on 9330dbe6: claim was null)"
        );
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(
            rows[0].facts["chainProbe"], "index-unspent",
            "the chain rung did not corroborate: the provenance says so, distinct from the courier-corroborated `unspent`"
        );
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["joinRefused"], true);
        assert_eq!(rows[0].facts["joinRefusedBy"], "eviction");
        // no filing in hand -> the device that owns the key signs the sweep (the corroborated arm's own source block)
        assert_eq!(rows[0].facts["sweepSource"], "sign-here");
        assert_eq!(rows[0].reason.as_deref(), Some(JOIN_REFUSED_REASON));
    }

    /// PIN 669b-B (GUARD, green before and after): an index-UNKNOWN hop (the container never indexed) with no chain
    /// word has NOTHING saying unspent, so it stays a pending chain-wait. The fix broadens ONLY the index-UNSPENT case.
    #[test]
    fn pin669b_b_an_index_unknown_hop_with_no_chain_word_stays_pending() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(
            HopStatus::Unknown,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let evicted: HashSet<String> = [key].into_iter().collect();
        let mut i = inputs(&[], &[], &old, &v, &c, &p, Some(971_132));
        i.evicted_hop_outpoints = &evicted;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].facts["claim"].is_null(),
            "index-unknown + no word: nothing says unspent -> pending, never a claim"
        );
        assert_eq!(rows[0].facts["chainProbe"], "pending");
        assert_eq!(rows[0].family, OwedFamily::Unbound);
    }

    /// PIN 669b-C (GUARD, green before and after): a KNOWN competitor (the chain rung returned a positive spent word)
    /// still flips the hop to `spent-elsewhere` / could-not-judge — the spent arm is unchanged, the dead sweep withheld.
    #[test]
    fn pin669b_c_a_known_competitor_still_withholds_the_sweep() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let evicted: HashSet<String> = [key.clone()].into_iter().collect();
        let competitor = tx(0x1c);
        let spent = chain_confirmed(&key, true, Some(true), Some(&competitor), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &p, Some(971_132));
        i.evicted_hop_outpoints = &evicted;
        i.hop_chain = &spent;
        let rows = derive_and_pin(&i);
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].facts["claim"].is_null(),
            "a known competitor: never a claim"
        );
        assert_eq!(rows[0].facts["chainProbe"], "spent");
        assert_eq!(rows[0].facts["chainSpender"], competitor);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
    }

    /// PIN 669b-D (RED on `9330dbe6` via the precondition): the new index-unspent claimable row is STILL subject to
    /// the #669 read-side degrade — a block arriving since the compute (the dead-sweep case: the owner's own
    /// competitor MINED, moving the tip) degrades it to a pending block-wait, never a dead sweep "on its way back".
    #[test]
    fn pin669b_d_the_index_unspent_sweep_still_degrades_when_a_block_arrived() {
        let mut rows = index_unspent_no_word_rows(971_132);
        assert_eq!(
            rows[0].facts["claimable"], true,
            "precondition: the index-unspent sweep is claimable (RED on 9330dbe6, where it is pending)"
        );
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        degrade_stale_sweeps(&mut rows, Some(971_132), Some(971_133)); // a block arrived since the compute
        assert_eq!(
            rows[0].facts["claimable"], false,
            "a block arrived: the sweep degrades, never a dead sweep"
        );
        assert_eq!(rows[0].facts["chainWait"], "block");
        assert_eq!(rows[0].facts["tipMoved"], true);
        assert!(rows[0].facts["claim"].is_null());
    }

    // ── bsv-low #808: a hop-stranded probe memo is bound to the tip it was taken at ─────────────────────────────────
    //
    // Loop 41, pair 13 (`docs/audit/L41-REDS-2026-10-21.md` row 1 in bsv-low): at tip 971994 p2's `/owed` served
    // `chainProbe unspent, chainProbeAgeMs 277950, claim sweep-hop, claimable true` for a hop whose competitor mined AT
    // 971994. The walk re-used a memo taken before the block (inside the five-minute window) and stamped its snapshot at
    // 971994, so `degrade_stale_sweeps` (snapshot tip against present tip) let it through. These pins run the walk's
    // step 4b in the route's order over the SHIPPED memo statements under real SQLite: the memo written by
    // `PROBE_MEMO_UPSERT_SQL` with its tip, read back by `probe_memo_read_sql`, split at the walk's tip, the past-budget
    // word, the derivation, and the read-time gate at a tip-current snapshot.

    /// The walk's step 4b for the one hop of `stranded_sweep_rows` (`tx(0x07):0`), as `routes::owed_recompute_inner`
    /// runs it: read the memo back, split at `walk_tip`, and either name the hop for a fresh ask (`Err(())`: within the
    /// budget) or serve the past-budget word (`budget_left == false`). `Ok` is the chain map the derivation reads.
    fn walk808(
        conn: &rusqlite::Connection,
        walk_tip: u64,
        budget_left: bool,
        now: i64,
    ) -> Result<HashMap<String, HopChainWord>, ()> {
        use crate::hops_view::{
            probe_memo_read_sql, split_probe_targets_at_tip, stale_memo_word, ProbeMemo,
            PROBE_MEMO_CONFIRMED_MAX_AGE_MS, PROBE_MEMO_MAX_AGE_MS,
        };
        let target = (tx(0x07), 0u32);
        let mut stmt = conn.prepare(&probe_memo_read_sql(1)).unwrap();
        let memos: Vec<ProbeMemo> = stmt
            .query_map([format!("{}.0", tx(0x07))], |r| {
                Ok(ProbeMemo {
                    outpoint: r.get(0)?,
                    probed_at_ms: r.get(1)?,
                    spent: r.get::<_, i64>(2)? != 0,
                    spending_txid: r.get(3)?,
                    spent_confirmed: r.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                    tip_height: r.get::<_, Option<i64>>(5)?.map(|t| t as u64),
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let (answered, to_probe) = split_probe_targets_at_tip(
            std::slice::from_ref(&target),
            &memos,
            now,
            PROBE_MEMO_MAX_AGE_MS,
            PROBE_MEMO_CONFIRMED_MAX_AGE_MS,
            None,
            Some(walk_tip),
        );
        let key = format!("{}:0", tx(0x07));
        let mut out = HashMap::new();
        for (_, _, p) in answered {
            let m = memos.iter().max_by_key(|m| m.probed_at_ms).unwrap();
            out.insert(
                key.clone(),
                HopChainWord {
                    looked: p.known,
                    spent: p.spent,
                    spending_txid: p.spending_txid,
                    spent_confirmed: p.spent_confirmed,
                    stale: false,
                    age_ms: Some(now - m.probed_at_ms),
                    probe_tip: m.tip_height,
                    predates_tip: false,
                },
            );
        }
        if !to_probe.is_empty() {
            if budget_left {
                return Err(());
            }
            if let Some(m) = memos.iter().max_by_key(|m| m.probed_at_ms) {
                out.insert(key, stale_memo_word(m, now, None, Some(walk_tip)));
            }
        }
        Ok(out)
    }

    /// The derivation over `chain` at `tip`, then the read-time gate at a TIP-CURRENT snapshot (the loop-41 shape: the
    /// snapshot was stamped at the served tip, so #669's snapshot-against-present check passes it through).
    fn served808(chain: &HashMap<String, HopChainWord>, tip: u64) -> Vec<OwedRow> {
        served808_as(chain, tip, HopStatus::Unspent)
    }

    /// [`served808`] for a hop the index calls `status` (`Unknown`: the container never indexed it).
    fn served808_as(
        chain: &HashMap<String, HopChainWord>,
        tip: u64,
        status: HopStatus,
    ) -> Vec<OwedRow> {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let mut old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        if status == HopStatus::Unknown {
            old[0].status = status;
            old[0].spent = None;
        }
        let mut i = inputs(&[], &[], &old, &v, &c, &p, Some(tip));
        i.hop_chain = chain;
        let mut rows = derive_and_pin(&i);
        degrade_stale_sweeps(&mut rows, Some(tip), Some(tip));
        rows
    }

    fn unspent_probe808() -> crate::hops_view::ChainSpendProbe {
        crate::hops_view::ChainSpendProbe {
            known: true,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: None,
        }
    }

    /// PIN 808-B: what the tip rule leaves standing. A memo at the served tip answers and offers; a memo ABOVE the
    /// walk's tip (another isolate's fresher tip cache) answers; a SPENT memo below the tip answers (it only
    /// withholds). A memo with NO recorded tip (written before migration 342, or by a tipless writer) is asked again.
    /// With no walk tip the rule is off (the read-time gate's `Unknown` arm degrades the sweep instead).
    #[test]
    fn pin808_b_a_memo_at_or_above_the_tip_answers_and_a_tipless_one_is_asked() {
        use crate::hops_view::{memo_predates_tip, probe_memo_at_tip, probe_memo_of};
        let (h, now) = (971_994u64, 10_000_000i64);
        let conn = memo_db();
        write_memos(
            &conn,
            &[
                probe_memo_at_tip(&tx(0x07), 0, &unspent_probe808(), now - 60_000, Some(h))
                    .unwrap(),
            ],
        );
        let rows = served808(&walk808(&conn, h, true, now).expect("answered"), h);
        assert_eq!(
            rows[0].facts["claim"], "sweep-hop",
            "a memo at the served tip offers the press"
        );
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["chainProbeTip"], h);
        assert!(
            walk808(&conn, h - 1, true, now).is_ok(),
            "a memo above the walk's tip stands"
        );
        // a tipless memo (the column NULL), as the shipped upsert writes it
        let conn = memo_db();
        write_memos(
            &conn,
            &[probe_memo_of(&tx(0x07), 0, &unspent_probe808(), now - 60_000).unwrap()],
        );
        assert_eq!(
            walk808(&conn, h, true, now),
            Err(()),
            "a memo with no recorded tip is asked again"
        );
        let tipless = walk808(&conn, h, false, now).unwrap();
        let rows = served808_as(&tipless, h, HopStatus::Unknown);
        assert_eq!(rows[0].facts["claimable"], false);
        assert!(rows[0].facts["chainProbeTip"].is_null());
        let rows = served808(&tipless, h);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["tipMoved"], true);
        assert!(rows[0].facts["chainProbeTip"].is_null());
        // the pure rule
        let unspent_at =
            |t: Option<u64>| probe_memo_at_tip(&tx(0x07), 0, &unspent_probe808(), now, t).unwrap();
        assert!(memo_predates_tip(&unspent_at(Some(h - 1)), Some(h)));
        assert!(memo_predates_tip(&unspent_at(None), Some(h)));
        assert!(!memo_predates_tip(&unspent_at(Some(h)), Some(h)));
        assert!(!memo_predates_tip(&unspent_at(Some(h + 1)), Some(h)));
        assert!(
            !memo_predates_tip(&unspent_at(Some(h - 1)), None),
            "no walk tip: nothing to compare"
        );
        let spent = crate::hops_view::ChainSpendProbe {
            known: true,
            spent: Some(true),
            spending_txid: Some(tx(0x5d)),
            spent_confirmed: None,
        };
        let spent_below = probe_memo_at_tip(&tx(0x07), 0, &spent, now, Some(h - 1)).unwrap();
        assert!(
            !memo_predates_tip(&spent_below, Some(h)),
            "a spent word only withholds: never stale by tip"
        );
    }

    /// PIN 808-C: #669's narrowing for the owner's own index-unspent stranded hop is unchanged. With NO chain word (no
    /// memo, the budget spent before it was asked) or a fresh probe that could not look (`looked: false`, a 429), the
    /// index's own unspent offers the sweep (`chainProbe: "index-unspent"`); the rung's positive spent word still
    /// downgrades it. Only a word the rung DID give, and gave below the tip, withholds (808-A).
    #[test]
    fn pin808_c_the_index_unspent_arm_is_unchanged() {
        let (h, now) = (971_994u64, 10_000_000i64);
        let conn = memo_db();
        assert_eq!(walk808(&conn, h, true, now), Err(()), "never probed: asked");
        let silent = walk808(&conn, h, false, now).unwrap();
        assert!(silent.is_empty(), "no memo, no budget: no chain word");
        let rows = served808(&silent, h);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["claimable"], true);
        let key = format!("{}:0", tx(0x07));
        let faulted = chain(&key, false, None, None);
        let rows = served808(&faulted, h);
        assert_eq!(
            rows[0].facts["chainProbe"], "index-unspent",
            "a fresh ask that could not look"
        );
        assert_eq!(rows[0].facts["claimable"], true);
        let spent = chain(&key, true, Some(true), Some(&tx(0x5d)));
        let rows = served808(&spent, h);
        assert_eq!(
            rows[0].facts["chainProbe"], "spent",
            "the rung still downgrades"
        );
        assert_ne!(rows[0].facts["claimable"], true);
    }
}
