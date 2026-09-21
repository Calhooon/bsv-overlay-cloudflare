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
//!   filed refund is `refundValid`. Claim: `present-refund`.
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
//! filing; a written-off era pot (the views' era clause); a pot the identity
//! is not a party to (the views' party window).
//!
//! The derivation is PURE over rows this crate already serves (`ResultEntry`,
//! `RefundEntry`, `HopEntry`, the valid filed refund raws, the collected
//! markers) — the families REUSE the views' own judgments at compute time
//! (`derive_outcome_with_seat`, `derive_refund_status`, `derive_hop_status`,
//! `served_recovery_height`); the rows are a SNAPSHOT of those judgments, kept
//! current by the write-side hooks (pot-changed, the filings, the tip) and the
//! read's staleness rule (`routes.rs`). The D1 gathering + the write live in
//! `routes.rs` (`owed_recompute`), beside the views' own plumbing.
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

/// PURE: the HOP OUTPOINTS (`txid:vout`, lowercase) whose spend pointer an EVICTED, never readmitted JOIN released —
/// the overlay's `pot_evictions.releasedSpends` (`[{"table","txid","vout"}, …]`, one entry per `pot_records` row the
/// evicted tx spent). ONE derivation for the route's probe candidates and the derivation's hop rule: such a hop is
/// STRANDED at once (never `in-progress` with a rejoin the hand can never honour).
///
/// Keyed on the UTXO, never on a name (the gate's HIGH-1, 2026-09-19): the results view serves potparty rows by
/// byte format, so a stranger can plant a row naming a victim's identity, a live game id and its OWN evicted txid;
/// keying the refusal on that game id would have handed the victim's live hop a sweep press. A released spend is
/// unforgeable evidence of WHICH hop the evicted tx spent, and only this seat's key spends this seat's hop. A NULL,
/// malformed or entry-less column contributes nothing (the pre-change sentence stands: fail-safe).
pub fn released_hop_outpoints(evictions: &[(String, Option<String>)]) -> HashSet<String> {
    let mut out = HashSet::new();
    for (_txid, released) in evictions {
        let Some(raw) = released.as_deref() else { continue };
        let Ok(Value::Array(entries)) = serde_json::from_str::<Value>(raw) else { continue };
        for e in entries {
            let (Some(txid), Some(vout)) = (e.get("txid").and_then(Value::as_str), e.get("vout").and_then(Value::as_u64)) else { continue };
            if txid.len() == 64 && txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                out.insert(outpoint_key(txid, vout as u32));
            }
        }
    }
    out
}

/// PURE (pinned): what a coalesced recompute ask leaves behind — the latest source, replacing an earlier one
/// (`routes::owed_recompute_and_push_coalesced`).
pub fn owed_rerun_note(map: &mut HashMap<String, String>, identity_lc: &str, source: &str) {
    map.insert(identity_lc.to_string(), source.to_string());
}

/// The eviction ledger's RECENT rows, the candidate set for a refused JOIN (the delta-verify's HIGH-A, 2026-09-19):
/// the results view cannot feed it — the eviction moves the party rows keyed by the pot into their twin, so an
/// identity's results hold NO entry for an evicted pot and a candidate set derived from them is empty on exactly
/// the path that matters. The ledger itself is the source: every eviction not yet readmitted inside the window,
/// intersected below with the identity's OWN hops (`refused_hop_outpoints_of`). Bound `?1` = now − the window.
pub const OWED_EVICTIONS_WINDOW_SQL: &str =
    "SELECT lower(txid) AS txid, releasedSpends FROM pot_evictions WHERE readmittedAt IS NULL AND evictedAt >= ?1";
/// How far back the ledger is read (evictions are rare: 2 in 47 admissions on beta; a hop past the young window
/// is stranded by AGE regardless, so the window only has to cover the young period, with margin).
pub const OWED_EVICTION_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

/// PURE: the identity's own hop outpoints among the released spends of the ledger's recent evictions — the
/// refusal's key. A released spend names a `pot_records` row the evicted tx spent, and only this seat's key
/// spends this seat's hop, so a stranger's eviction can never name a hop of ours.
pub fn refused_hop_outpoints_of(released: &HashSet<String>, hops: &[HopEntry]) -> HashSet<String> {
    hops.iter()
        .map(|h| outpoint_key(&h.hop_txid, h.hop_vout))
        .filter(|k| released.contains(k))
        .collect()
}
/// The courier probes ONE recompute may buy for its index-unspent hops (the memoised `hop_chain_probes` answer the
/// rest; a hop past the budget keeps its last memo's word, named stale, or waits with no claim). Counted per caller
/// `owed` on the courier census.
pub const OWED_PROBES_PER_RECOMPUTE: usize = 8;
/// A `payout` row is CLAIMABLE only once the spend is confirmed (the credit
/// path's landing bar); an unconfirmed spend is a row that says so.
pub const UNCONFIRMED_PAYOUT_REASON: &str = "the spend is seen but not mined yet: the credit lands with the block";
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
/// The `spent-elsewhere` story (design §2): the hop was spent by a transaction that is not a LOW pot and pays no
/// home of this seat — a sweep to another home, or the wallet's own spend. Nothing here can move it.
pub const SPENT_ELSEWHERE_REASON: &str =
    "the hop was spent outside this game by a transaction that pays none of your homes (your wallet's own spend, or a sweep elsewhere): if it was yours, the money is already there; nothing here can move it";

/// The families, in the order the page lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    i.spender_inputs
        .get(spender)
        .is_some_and(|ins| !ins.iter().any(|(t, v)| *v == h.hop_vout && t.eq_ignore_ascii_case(&h.hop_txid)))
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
}

/// Spenders whose stored bytes one recompute reads to classify a hop's spend (bounded: a lived-in identity's
/// adversarial history can hold dozens; the newest strands first, the rest read "not judged this pass").
pub const OWED_SPENDER_READS_PER_RECOMPUTE: usize = 16;
/// Of those, how many may go to the COURIERS for a spender the index never held (the seat's own competitor, a sweep
/// broadcast elsewhere): each ask is the tx-any resolver's external leg (WoC's tx read gates it; the raw from WoC,
/// then Bitails; hash-verified), so the pass buys two attempts, faulted or not; the bytes are content-addressed and
/// memoised in the isolate — a spender is fetched once per isolate, a faulted ask not repeated inside its cache TTL.
pub const OWED_SPENDER_COURIER_READS_PER_RECOMPUTE: usize = 2;
/// The wall-clock budget one recompute spends on its OUTWARD legs (the courier probes, the stored-bytes reads): a
/// pass past it writes what it has and the next pass continues — a recompute must always finish inside the
/// worker's own limits (the stranded cell's run 3, 2026-09-19: a pass that never finished kept a list stale for good).
pub const OWED_RECOMPUTE_TIME_BUDGET_MS: i64 = 12_000;

/// Everything one identity's recompute gathered (every one a served-view row).
pub struct OwedInputs<'a> {
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
    /// hop spender txids (lowercase) that ARE LOW pots (`pot_records` holds them).
    pub pot_spenders: &'a HashSet<String>,
    /// True when the pot-spenders read FAULTED: a spent hop is then "could not check", never a story.
    pub pot_spenders_faulted: bool,
    /// hop outpoint (`txid:vout`) → the chain rung's word, for the index-unspent hops past the stranded window.
    pub hop_chain: &'a HashMap<String, HopChainWord>,
    /// hop outpoints (`txid:vout`, lowercase) whose spend pointer an EVICTED, never readmitted JOIN released
    /// (`released_hop_outpoints`): the network refused the hand's funding — the hop is stranded at once.
    pub evicted_hop_outpoints: &'a HashSet<String>,
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
}

/// The 20-byte pkh of a standard P2PKH locking script (`76 a9 14 <20> 88 ac`), lowercase hex; `None` for any other lock.
pub fn p2pkh_pkh_hex(lock: &[u8]) -> Option<String> {
    if lock.len() == 25 && lock[0] == 0x76 && lock[1] == 0xa9 && lock[2] == 0x14 && lock[23] == 0x88 && lock[24] == 0xac {
        Some(hex::encode(&lock[3..23]))
    } else {
        None
    }
}

pub fn outpoint_key(txid: &str, vout: u32) -> String {
    format!("{}:{vout}", txid.to_ascii_lowercase())
}

/// PURE: (my hop outpoint, the evicted txid that released it) — the same ledger rows and the same UTXO key as
/// [`released_hop_outpoints`], keeping WHICH eviction named the hop (the twin row to read). Sorted, deduplicated.
pub fn released_hops_by_eviction(evictions: &[(String, Option<String>)], hops: &[HopEntry]) -> Vec<(String, String)> {
    let mine: HashSet<String> = hops.iter().map(|h| outpoint_key(&h.hop_txid, h.hop_vout)).collect();
    let mut out: Vec<(String, String)> = Vec::new();
    for (evicted, released) in evictions {
        if evicted.len() != 64 || !evicted.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        let Some(raw) = released.as_deref() else { continue };
        let Ok(Value::Array(entries)) = serde_json::from_str::<Value>(raw) else { continue };
        for e in entries {
            let (Some(txid), Some(vout)) = (e.get("txid").and_then(Value::as_str), e.get("vout").and_then(Value::as_u64)) else { continue };
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
                if held.is_some_and(|x| !x.seat_settle_pubkey.eq_ignore_ascii_case(&h.seat_settle_pubkey) || !x.game_id.eq_ignore_ascii_case(&h.game_id)) {
                    *held = None;
                }
            })
            .or_insert(Some(h));
    }
    let mut homes: std::collections::BTreeMap<String, Option<String>> = std::collections::BTreeMap::new();
    for (hop_outpoint, evicted) in released {
        let Some(Some(h)) = by_outpoint.get(hop_outpoint) else { continue };
        if h.marker_verified != MarkerVerification::Verified {
            continue;
        }
        let Some(Some(keys)) = keys_by_txid.get(&evicted.to_ascii_lowercase()) else { continue };
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
    homes.into_iter().filter_map(|(g, p)| p.map(|p| (g, p))).collect()
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

/// The hop's NON-POT spender named by the index (a recorded spend) or the chain rung (a word of spent), with the
/// confirmation the source carried; `None` when nothing names a spender, or the spender IS a covenant pot (the JOIN
/// took it: the pot's own row tells the story).
fn non_pot_spender(i: &OwedInputs, h: &HopEntry, outpoint: &str) -> Option<(String, bool)> {
    if h.spent == Some(true) {
        if let Some(s) = h.spending_txid.as_deref() {
            let s = s.to_ascii_lowercase();
            return if i.pot_spenders.contains(&s) || pointer_refuted(i, &s, h) { None } else { Some((s, h.spent_confirmed == Some(true))) };
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
    Some((s, w.spent_confirmed == Some(true)))
}

/// The games whose `collected` filings the recompute must read: every SPENT pot the results name AND every hop the
/// hops view names (a swept hop's payout is retired by a `collected` filing for its GAME, and a hop-only game — the
/// stake swept before any JOIN — has no pot row at all; the collect pass of 2026-09-19 pressed five such payouts to
/// "already in your wallet", filed each, and the rows stood because this lookup read the pots' games only). Sorted,
/// deduped, lower-case: one chunked `IN (...)` read.
pub fn collected_lookup_games<'a>(spent_result_games: impl Iterator<Item = &'a str>, hop_games: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut games: Vec<String> = spent_result_games.chain(hop_games).map(str::to_ascii_lowercase).collect();
    games.sort_unstable();
    games.dedup();
    games
}

/// PURE (#517, the gate's LOW-1): the hop's own row or the chain rung names a DIFFERENT tx as its CONFIRMED spender.
/// The index's proof of the filed sweep never outranks that word (a reorg replaced the sweep's block with a competing
/// spend and the latch was missed): the ladder below follows the chain's spender, as before the proof existed.
/// Stated asymmetry (the delta-verify's N-C): this does not consult `pointer_refuted`, so a confirmed pointer whose
/// bytes were read and do not spend the hop still neutralises the proof; the outcome is the pre-#517 "could not
/// judge", never worse, and a confirmed refuted pointer would be an overlay attribution bug with no producer today.
pub fn confirmed_by_other(h: &HopEntry, word: Option<&HopChainWord>, sweep_txid: &str) -> bool {
    let other = |s: &str| !s.eq_ignore_ascii_case(sweep_txid);
    (h.spent == Some(true) && h.spent_confirmed == Some(true) && h.spending_txid.as_deref().is_some_and(other))
        || word.is_some_and(|w| w.looked && w.spent == Some(true) && w.spent_confirmed == Some(true) && w.spending_txid.as_deref().is_some_and(other))
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
    let home_spent = |spender: &str| -> bool {
        let (Some(outs), Some(pkh)) = (i.spender_outputs.get(spender), my_pkh.as_deref()) else {
            return false;
        };
        let mine: Vec<&SpenderOutput> = outs.iter().filter(|o| o.pkh_hex.as_deref().is_some_and(|p| p.eq_ignore_ascii_case(pkh))).collect();
        !mine.is_empty() && mine.iter().all(|o| o.spent == Some(true))
    };
    if let Some(filed) = i.hop_sweeps.get(&outpoint) {
        let named_by_index = h.spending_txid.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(&filed.sweep_txid));
        let by_chain = i.hop_chain.get(&outpoint).filter(|w| w.looked && w.spent == Some(true) && w.spending_txid.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(&filed.sweep_txid)));
        // bsv-low #517 (loop 19, pair 11, 2026-09-21): the INDEX's own VERIFIED proof of the filed sweep names the
        // spender and confirms the payout FIRST (`transactions.has_proof`, the latch only the chaintracks-verified stitch
        // sets: a mined sweep that spends the hop is the hop's spender, definitively; the same word `/tx-any` serves and
        // `/credit-beef` assembles the credit from). Without it the row rested on the hop row (never attributed to a
        // sweep once the JOIN's eviction released its pointer) or a courier's word (an indexer read a 1,997-tx block's
        // sweep "unconfirmed" seven minutes after the mine, memoised five more; three blocks passed in nine minutes).
        // the gate's LOW-1 (Rule 6): the proof never outranks a CONTRADICTING confirmed word; the ladder below decides
        // (counted once per hop in the row pass: this fn also runs in the candidates pre-pass)
        let contradicted = confirmed_by_other(h, i.hop_chain.get(&outpoint), &filed.sweep_txid);
        let index_word = filed.index_proven && !contradicted;
        if index_word || named_by_index || by_chain.is_some() {
            // the index's proof, OR its hop-row word, OR the chain rung's (a spend the index recorded before its block
            // and never re-checked read "not mined yet" for days on the pair: the recompute now probes such hops and
            // the courier's confirmation heals the row without a client action)
            // the gate's LOW-3 (2026-09-19): the confirming chain word must NAME the filed sweep (`by_chain` does) — a
            // hop the JOIN took after all (evicted, then readmitted on its mine) reads spent+confirmed by a DIFFERENT
            // tx, and that must never turn the sweep's payout claimable for a tx that can never mine
            let confirmed = index_word || (named_by_index && h.spent_confirmed == Some(true)) || by_chain.and_then(|w| w.spent_confirmed) == Some(true);
            return Some(SweptHome {
                sweep_txid: filed.sweep_txid.clone(),
                raw_hex: Some(filed.raw_hex.clone()),
                pays_sats: filed.pays_sats,
                confirmed,
                index_proven: index_word,
                index_proof_height: if index_word { filed.index_proof_height } else { None },
                source: "hopsweep-filing",
                output_spent: home_spent(&filed.sweep_txid),
            });
        }
    }
    // an UNFILED sweep (a device before the filing existed, a recovery tool): the spender's stored bytes pay MY home
    let (spender, confirmed) = non_pot_spender(i, h, &outpoint)?;
    let outs = i.spender_outputs.get(&spender)?;
    if outs.iter().any(|o| o.pot_lock) {
        return None; // a transaction that CREATES a pot is a JOIN, never a sweep (the delta-verify's NEW-3)
    }
    let pkh = my_pkh?;
    let mine: Vec<&SpenderOutput> = outs.iter().filter(|o| o.pkh_hex.as_deref().is_some_and(|p| p.eq_ignore_ascii_case(&pkh))).collect();
    if mine.is_empty() {
        return None;
    }
    let mut sum = 0u64;
    for o in &mine {
        sum = sum.checked_add(o.sats)?;
    }
    let source = if i.courier_spenders.contains(&spender) { "courier-bytes" } else { "index-bytes" };
    Some(SweptHome {
        sweep_txid: spender,
        raw_hex: None,
        pays_sats: Some(sum),
        confirmed,
        index_proven: false,
        index_proof_height: None,
        source,
        output_spent: mine.iter().all(|o| o.spent == Some(true)),
    })
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
        let mut pkhs: Vec<String> = outs.iter().filter_map(|o| o.pkh_hex.as_deref().map(str::to_ascii_lowercase)).collect();
        pkhs.sort_unstable();
        pkhs.dedup();
        return Some(SpenderStory::HomeUnknown { pkhs });
    }
    Some(SpenderStory::Elsewhere)
}

/// The story's reason, its machine facts written; `unknown` when the bytes were never read.
fn story_reason(facts: &mut Value, story: Option<SpenderStory>, unknown: &'static str) -> &'static str {
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
        if e.spent == Some(true) && e.verdict.is_some() && matches!(e.outcome, Outcome::Won | Outcome::Tie | Outcome::Refund) {
            *payout_candidates_by_game.entry(e.game_id.to_ascii_lowercase()).or_insert(0) += 1;
        }
    }
    // a hop spent by ITS OWN filed sweep is a payout candidate of the game too (the sweep's credit): the one
    // `collected` marker of a game whose stake came back by a sweep AND whose pot paid keeps both rows
    for h in i.hops {
        if swept_home(i, h).is_some_and(|s| !s.output_spent) {
            *payout_candidates_by_game.entry(h.game_id.to_ascii_lowercase()).or_insert(0) += 1;
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
        let gate = crate::refund_view::served_recovery_height(e.cov_recovery_height, e.recovery_height);
        let refund = i.refunds.iter().find(|r| r.pot_txid.eq_ignore_ascii_case(&e.pot_txid) && r.pot_vout == e.pot_vout);
        let base_facts = json!({
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
                if verified_collected && payout_candidates_by_game.get(&game).copied().unwrap_or(0) <= 1 {
                    continue;
                }
                let pays_me = matches!(e.outcome, Outcome::Won | Outcome::Tie | Outcome::Refund);
                if pays_me && e.verdict.is_some() {
                    let mut facts = base_facts.clone();
                    facts["claim"] = json!("internalize");
                    // NOTE-22: the press is offered only once the spend is CONFIRMED (the credit path's landing bar).
                    // the review's LOW-1 (2026-09-20): a spend with a VERIFIED proof height is mined whatever the flag says
                    // (the landing bar's third arm, the unbound arm's L1 rule) — never a chain wait said by machine on a mined tx
                    let confirmed = e.spent_confirmed == Some(true) || e.at_height.is_some();
                    facts["claimable"] = json!(confirmed);
                    if !confirmed {
                        facts["claimReason"] = json!(UNCONFIRMED_PAYOUT_REASON);
                        facts["chainWait"] = json!("block"); // a CHAIN wait by machine (the wave's homeKeyRecovery: 25 min counted as a wedge)
                    }
                    facts["collectedMarkerPresent"] = json!(i.collected_present.contains(&game) || verified_collected);
                    facts["collectedSigVerified"] = json!(verified_collected);
                    facts["creditBeef"] = json!(e.settle_txid.as_ref().map(|t| format!("/credit-beef/{t}")));
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
                    let awaiting_block =
                        e.verdict.is_none() && e.settle_txid.is_some() && e.spent_confirmed != Some(true) && e.at_height.is_none();
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
                    && refund.is_none_or(|r| r.status == RefundStatus::GateOpen || r.status == RefundStatus::Armed)
                    && valid.is_some();
                if refund_due {
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
        let facts_base = json!({
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
        // THE SWEPT HOP (bsv-low #469 decision 3): the hop is spent by its own seat's sweep — the sweep this identity
        // FILED, or a spender whose stored bytes pay the seat's committed home (an unfiled sweep) — so the stake is
        // at the seat's own home, uncollected: a `payout` row whose credit is the sweep (`/credit-beef/<sweepTxid>`),
        // claimable once the sweep mined; retired by `collected` like every payout (one candidate per game, N10
        // above), or by the index's word that the home output was spent since (collected and moved on).
        // #517, the gate's LOW-1: a PROVEN filing met a contradicting confirmed spender (the hop row's or the chain
        // rung's): the proof did not decide (see `confirmed_by_other`). The fact rides the hop's row (whatever family
        // the ladder gives it) so the operator sees WHICH hop; the route counts the rows (the delta-verify's N-A/N-B:
        // this derivation stays pure, no global moves here)
        if i.hop_sweeps.get(&outpoint).is_some_and(|f| f.index_proven && confirmed_by_other(h, i.hop_chain.get(&outpoint), &f.sweep_txid)) {
            contradicted_hops.insert(outpoint.clone());
        }
        if let Some(swept) = swept_home(i, h) {
            if swept.output_spent {
                continue;
            }
            let verified_collected = i.collected_verified.contains(&game);
            if verified_collected && payout_candidates_by_game.get(&game).copied().unwrap_or(0) <= 1 {
                continue;
            }
            let mut facts = facts_base.clone();
            facts["claim"] = json!("internalize");
            facts["claimable"] = json!(swept.confirmed);
            if !swept.confirmed {
                facts["claimReason"] = json!(UNCONFIRMED_PAYOUT_REASON);
                facts["chainWait"] = json!("block");
            }
            if swept.source == "courier-bytes" {
                // the gate's M3 (2026-09-20): the index holds no bytes and no proof for this transaction, so
                // `/credit-beef` cannot assemble the credit; the row tells the truth instead of a press that pends
                // forever. An UNCONFIRMED one keeps its waiting word (the round-2 LOW-1): the pointer's spend may
                // still be displaced, so "the sats sit at your home" is said only once the chain confirmed it.
                facts["claimable"] = json!(false);
                facts["claimReason"] = json!(if swept.confirmed { COURIER_BYTES_NO_CREDIT_REASON } else { UNCONFIRMED_PAYOUT_REASON });
                facts["creditKind"] = json!("courier-bytes"); // the residual: this row retires only by a `collected` filing (bsv-low issue)
            }
            facts["outcome"] = json!("hop-sweep");
            facts["sweepTxid"] = json!(swept.sweep_txid);
            facts["sweepRawHex"] = json!(swept.raw_hex);
            facts["sweepSource"] = json!(swept.source);
            // the delta-verify's NEW-1 (2026-09-19): WHICH word made the payout claimable, and how old the chain's word
            // was — a confirmation from a memo (≤ 2 h) after a reorg is a wrong word an operator must be able to see
            if swept.confirmed {
                let by_index = h.spending_txid.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(&swept.sweep_txid)) && h.spent_confirmed == Some(true);
                let word = i.hop_chain.get(&outpoint);
                facts["confirmedSource"] = json!(if swept.index_proven { "index-proof" } else if by_index { "index" } else if word.and_then(|w| w.age_ms).is_some() { "chain-memo" } else { "chain-probe" });
                if let Some(height) = swept.index_proof_height {
                    facts["sweepProofHeight"] = json!(height);
                }
                if let Some(age) = word.and_then(|w| w.age_ms) {
                    facts["chainProbeAgeMs"] = json!(age);
                }
            }
            facts["creditBeef"] = json!(format!("/credit-beef/{}", swept.sweep_txid));
            facts["collectedMarkerPresent"] = json!(i.collected_present.contains(&game) || verified_collected);
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
        match h.status {
            HopStatus::Spent => {
                let spender = h.spending_txid.as_deref().map(str::to_ascii_lowercase);
                let by_pot = spender.as_deref().is_some_and(|s| i.pot_spenders.contains(s));
                if by_pot {
                    continue; // the JOIN took it: the pot's own row tells the story
                }
                // MEDIUM-7: a spender ABSENT from the index is not evidence of a wallet spend (an unindexed or
                // evicted JOIN is the common honest case); without positive evidence the brain says it could not
                // judge, never a custody story. With the spender's stored BYTES (a `tm_lowfund`-admitted spend that
                // pays no home of mine) the story is positive: spent outside this game (`spent-elsewhere`).
                let mut facts = facts_base.clone();
                facts["claim"] = Value::Null;
                let story = spender.as_deref().filter(|s| !pointer_refuted(i, s, h)).and_then(|s| spender_story(i, s, &game));
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
                let join_refused = i.evicted_hop_outpoints.contains(&outpoint);
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
                // THE CHAIN RUNG: the index's "unspent" alone never offers the sweep (the client's bar since #451: a
                // read that could not look is never "unspent"). A chain word of unspent → the claim; spent by a pot →
                // no row (the JOIN took it); spent by something else → could not judge; no word yet → the claim waits.
                let chain = i.hop_chain.get(&outpoint);
                let mut facts = facts_base.clone();
                facts["ageMs"] = json!(age_ms);
                facts["joinRefused"] = json!(join_refused); // on every arm: a waiting press says why it is sweepable
                match chain {
                    Some(w) if w.looked && w.spent == Some(false) => {
                        facts["claim"] = json!("sweep-hop");
                        facts["claimable"] = json!(true);
                        facts["chainProbe"] = json!(if w.stale { "unspent-stale" } else { "unspent" });
                        if let Some(age) = w.age_ms { facts["chainProbeAgeMs"] = json!(age); }
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
                            reason: if join_refused { Some(JOIN_REFUSED_REASON.to_string()) } else { None },
                        });
                    }
                    Some(w) if w.looked && w.spent == Some(true) => {
                        let spender = w.spending_txid.as_deref().map(str::to_ascii_lowercase);
                        if spender.as_deref().is_some_and(|s| i.pot_spenders.contains(s)) {
                            continue; // the JOIN took it after all (the index lagged): the pot's row tells the story
                        }
                        facts["claim"] = Value::Null;
                        facts["chainProbe"] = json!("spent");
                        if let Some(age) = w.age_ms { facts["chainProbeAgeMs"] = json!(age); }
                        facts["chainSpender"] = json!(w.spending_txid);
                        let story = spender.as_deref().filter(|s| !pointer_refuted(i, s, h)).and_then(|s| spender_story(i, s, &game));
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
                    _ => {
                        facts["claim"] = Value::Null;
                        facts["chainProbe"] = json!("pending");
                        let reason = if h.status == HopStatus::Unknown {
                            "the hop's outpoint is not in the index (the funding never reached it, or it was evicted): the sats are likely still there; the sweep waits for the chain rung's word"
                        } else {
                            "the index says the hop is unspent; the chain rung has not corroborated it yet (the sweep waits for its word)"
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
    rows
}

/// The wire body of `GET /owed`.
pub fn owed_body(
    identity: &str,
    tip: Option<u64>,
    rows: &[OwedRow],
    computed_at_ms: i64,
    truncated: bool,
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
    json!({
        "v": OWED_WIRE_VERSION,
        "identity": identity.to_ascii_lowercase(),
        "tip": tip,
        "rows": arr,
        "truncated": truncated,
        "computedAtMs": computed_at_ms,
    })
    .to_string()
}

/// The `owed_rows` table (the overlay's migration 156 owns the DDL; this crate
/// issues the byte-identical catch-up). `facts` is the row's JSON.
pub const OWED_ROWS_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_rows (identity TEXT NOT NULL, outpoint TEXT NOT NULL, family TEXT NOT NULL, gameId TEXT NOT NULL, sats INTEGER, opponentIdentity TEXT, atHeight INTEGER, facts TEXT NOT NULL, updatedAtMs INTEGER NOT NULL, reason TEXT, PRIMARY KEY (identity, outpoint))";
/// The per-identity computed marker (migration 157): a never-computed identity
/// is COMPUTED on its first read, never served as "nothing owed".
pub const OWED_STATE_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_state (identity TEXT PRIMARY KEY, computedAtMs INTEGER NOT NULL, tip INTEGER, rows INTEGER NOT NULL, stale INTEGER NOT NULL DEFAULT 0, truncated INTEGER NOT NULL DEFAULT 0)";

pub const OWED_ROWS_DELETE_SQL: &str = "DELETE FROM owed_rows WHERE identity = ?1";
pub const OWED_ROW_INSERT_SQL: &str = "INSERT INTO owed_rows (identity, outpoint, family, gameId, sats, opponentIdentity, atHeight, facts, updatedAtMs, reason) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)";
pub const OWED_STATE_UPSERT_SQL: &str = "INSERT OR REPLACE INTO owed_state (identity, computedAtMs, tip, rows, stale, truncated) VALUES (?1, ?2, ?3, ?4, 0, ?5)";
pub const OWED_STATE_READ_SQL: &str = "SELECT identity, computedAtMs, tip, rows, stale, truncated FROM owed_state WHERE identity = ?1";
pub const OWED_ROWS_READ_SQL: &str = "SELECT identity, outpoint, family, gameId, sats, opponentIdentity, atHeight, facts, updatedAtMs, reason FROM owed_rows WHERE identity = ?1 ORDER BY CASE family WHEN 'payout' THEN 0 WHEN 'refund-due' THEN 1 WHEN 'hop-stranded' THEN 2 WHEN 'in-progress' THEN 3 ELSE 4 END, COALESCE(atHeight, 0) DESC, outpoint ASC LIMIT 501";
/// HIGH-4: the tip flips the gate — every identity party to an UNSPENT pot whose recovery height the new tip has
/// reached (a small band below it, so a missed block still lands) is marked STALE; its next read recomputes.
pub const OWED_STALE_ON_TIP_SQL: &str = "UPDATE owed_state SET stale = 1 WHERE identity IN (SELECT DISTINCT pp.identity FROM potparty_records pp JOIN pot_records p ON p.txid = pp.potTxid AND p.outputIndex = pp.potVout WHERE p.spent = 0 AND p.recoveryHeight IS NOT NULL AND p.recoveryHeight <= ?1 AND p.recoveryHeight >= ?1 - 6)";
/// HIGH-4: a filing on a pot marks BOTH parties stale (the counterparty's valid refund makes MY pot claimable).
/// The gate's MEDIUM-1 (2026-09-19): an outpoint the pot-changed hook cannot attribute through decoded params (an
/// EVICTED pot's row is gone; a released HOP is a P2PKH row) is attributed through the seats' OWN markers — the
/// party rows naming the pot and the hop rows naming the hop — so an eviction re-derives both seats' owed rows at
/// once instead of at the next cadence (up to 5 minutes, with a live Rejoin press on a hand the network refused).
pub const OWED_ATTRIBUTE_BY_POT_SQL: &str = "SELECT DISTINCT identity FROM potparty_records WHERE potTxid = ?1 AND potVout = ?2";
pub const OWED_ATTRIBUTE_BY_HOP_SQL: &str = "SELECT DISTINCT identity FROM hopparty_records WHERE txid = ?1 AND hopVout = ?2";
/// Fleet loop 15 (2026-09-20, the review's MED-2): the REFUND-BACKUP filing names the pot too (`idx_potrefund_pot`) —
/// the one marker a seat killed at the funding still filed (its party marker never was), so a pot with no party
/// rows and no hop match still reaches its seats' owed rows on its confirm event.
pub const OWED_ATTRIBUTE_BY_POTREFUND_SQL: &str = "SELECT DISTINCT identity FROM potrefund_records WHERE potTxid = ?1 AND potVout = ?2";
pub const OWED_STALE_FOR_POT_SQL: &str = "UPDATE owed_state SET stale = 1 WHERE identity IN (SELECT DISTINCT identity FROM potparty_records WHERE potTxid = ?1 AND potVout = ?2)";
// N3: both stale marks join EXACTLY (`idx_potparty_pot`, the `pot_records` PK), as every sibling query in this crate
// does (`results_sql`: `r.txid = pp.potTxid`); a `lower()` on a join key would scan the table per block / per filing.
// N8: the filing mark is keyed on a CALLER-CHOSEN pot: a stranger's verified filing can mark both parties of any pot
// stale — bounded by the per-(poster, family, day) filing caps, and the cost is one recompute on the victim's next
// read, never a wrong row.
// N9: the pot-changed hook reaches the identities with a VERIFIED seat marker whose entry sits on their first results
// page; a heavier identity and a hop-only seat are covered by the read's staleness rule below.
/// HIGH-4: a filing by an identity marks it stale (a `collected` retires a payout on its next read).
pub const OWED_STALE_FOR_IDENTITY_SQL: &str = "UPDATE owed_state SET stale = 1 WHERE identity = ?1";
/// MEDIUM-5: the probe before a first-read compute — an identity with no party row and no hop row has nothing to
/// derive and gets NO state row written (a public read must not write state keyed on a claimable name).
pub const OWED_IDENTITY_PROBE_SQL: &str = "SELECT 1 AS present FROM potparty_records WHERE identity = ?1 UNION ALL SELECT 1 FROM hopparty_records WHERE identity = ?1 LIMIT 1";
/// HIGH-4: the read's staleness rule — a list with an OPEN row (anything but a confirmed payout) is re-derived after
/// this age; any list after `OWED_RECOMPUTE_ANY_AFTER_MS` (a stranded hop crosses its window with no other trigger).
pub const OWED_RECOMPUTE_OPEN_AFTER_MS: i64 = 5 * 60 * 1000;
pub const OWED_RECOMPUTE_ANY_AFTER_MS: i64 = 15 * 60 * 1000;

/// A row is OPEN when it is anything but a claimable payout: it can change without a pot-changed event (a gate opens,
/// a hop crosses its window, a counterparty files).
pub fn row_is_open(r: &OwedRow) -> bool {
    r.family != OwedFamily::Payout || r.facts["claimable"] == Value::Bool(false)
}

/// The read's rule (N5, the ONE predicate): recompute when the marker says stale; when the tip advanced past an
/// in-progress row's recovery height since the compute; when an open list is older than 5 minutes; when any list is
/// older than 15 minutes (an empty list must not stay "nothing owed" forever; a stranded hop crosses its window with
/// no other trigger).
pub fn should_recompute(stale: bool, age_ms: i64, prev_tip: Option<u64>, tip_now: Option<u64>, rows: &[OwedRow]) -> bool {
    if stale {
        return true;
    }
    let gate_flipped = match tip_now {
        Some(t) if prev_tip.is_none_or(|p| p < t) => rows.iter().any(|r| {
            r.family == OwedFamily::InProgress && r.facts["recoveryHeight"].as_u64().is_some_and(|h| t >= h)
        }),
        _ => false,
    };
    if gate_flipped {
        return true;
    }
    let has_open = rows.iter().any(row_is_open);
    (has_open && age_ms > OWED_RECOMPUTE_OPEN_AFTER_MS) || age_ms > OWED_RECOMPUTE_ANY_AFTER_MS
}

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
pub const RECOMPUTE_SOURCES: [&str; 6] = ["pot-changed", "hop-changed", "filing", "read-first", "read-stale", "read-aged"];
static RECOMPUTE_BY_SOURCE: [AtomicU64; 6] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static ROWS_BY_FAMILY: [AtomicU64; 5] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static RECOMPUTE_FAULTS: AtomicU64 = AtomicU64::new(0);
static COLLECTED_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
static POT_SPENDERS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
static HOP_SWEEPS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
/// #517: the filed sweeps' index-proof read faulted (the pass confirms swept payouts on the courier path alone).
static SWEEP_PROOFS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
/// #517 (the gate's LOW-1): a proven filed sweep met a CONTRADICTING confirmed spender (the hop row's or the chain
/// rung's) and the proof did not decide — a reorg the latch missed, for the operator to see.
static SWEEP_PROOF_CONTRADICTIONS: AtomicU64 = AtomicU64::new(0);
static SPENDER_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
pub fn note_spender_read_fault() {
    SPENDER_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
}
/// Reads that served the rows in hand and kicked a background refresh (the stale / aged arms of the read rule).
static READ_REFRESHES: AtomicU64 = AtomicU64::new(0);
/// Reads that found a refresh of the same identity already in flight on this isolate (served, no second kick).
static READ_REFRESHES_SKIPPED: AtomicU64 = AtomicU64::new(0);
pub fn note_read_refresh(kicked: bool) {
    if kicked {
        READ_REFRESHES.fetch_add(1, Ordering::Relaxed);
    } else {
        READ_REFRESHES_SKIPPED.fetch_add(1, Ordering::Relaxed);
    }
}
pub fn note_collected_read_fault() {
    COLLECTED_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
}
pub fn note_pot_spenders_read_fault() {
    POT_SPENDERS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
}
pub fn note_hop_sweeps_read_fault() {
    HOP_SWEEPS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
}
pub fn note_sweep_proofs_read_fault() {
    SWEEP_PROOFS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
}
/// The route counts the rows that carry `sweepProofContradicted` (one per hop per recompute).
pub fn note_sweep_proof_contradictions(n: u64) {
    if n > 0 {
        SWEEP_PROOF_CONTRADICTIONS.fetch_add(n, Ordering::Relaxed);
    }
}
/// PURE: how many rows of one derivation carry the contradiction fact (the route's count, pinned).
pub fn count_sweep_proof_contradictions(rows: &[OwedRow]) -> u64 {
    rows.iter().filter(|r| r.facts.get("sweepProofContradicted").and_then(|v| v.as_bool()) == Some(true)).count() as u64
}

pub fn note_recompute(source: &str, rows: &[OwedRow]) {
    if let Some(i) = RECOMPUTE_SOURCES.iter().position(|s| *s == source) {
        RECOMPUTE_BY_SOURCE[i].fetch_add(1, Ordering::Relaxed);
    }
    for r in rows {
        ROWS_BY_FAMILY[r.family.index()].fetch_add(1, Ordering::Relaxed);
    }
}
pub fn note_recompute_fault() {
    RECOMPUTE_FAULTS.fetch_add(1, Ordering::Relaxed);
}
/// Asks that found a recompute of the same identity in flight on this isolate and were folded into ONE re-run
/// after it (fleet loop 11, 2026-09-19: the hooks' twins under the t=0 herd).
static RECOMPUTE_COALESCED: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_coalesced() {
    RECOMPUTE_COALESCED.fetch_add(1, Ordering::Relaxed);
}
/// An in-flight mark older than the stale bound was TAKEN OVER (the gate's MEDIUM-2: a future the runtime abandoned
/// would otherwise hold the identity's lock for the isolate's life).
static RECOMPUTE_LOCK_TAKEOVERS: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_lock_takeover() {
    RECOMPUTE_LOCK_TAKEOVERS.fetch_add(1, Ordering::Relaxed);
}

pub fn owed_health_json() -> Value {
    let mut by_source = serde_json::Map::new();
    for (i, s) in RECOMPUTE_SOURCES.iter().enumerate() {
        by_source.insert((*s).to_string(), json!(RECOMPUTE_BY_SOURCE[i].load(Ordering::Relaxed)));
    }
    let mut by_family = serde_json::Map::new();
    for f in OwedFamily::ALL {
        by_family.insert(f.as_str().to_string(), json!(ROWS_BY_FAMILY[f.index()].load(Ordering::Relaxed)));
    }
    json!({
        "countersScope": "isolate",
        "recomputeBySource": by_source,
        "rowsWrittenByFamily": by_family,
        "recomputeFaults": RECOMPUTE_FAULTS.load(Ordering::Relaxed),
        "recomputeCoalesced": RECOMPUTE_COALESCED.load(Ordering::Relaxed),
        "recomputeLockTakeovers": RECOMPUTE_LOCK_TAKEOVERS.load(Ordering::Relaxed),
        "collectedReadFaults": COLLECTED_READ_FAULTS.load(Ordering::Relaxed),
        "potSpendersReadFaults": POT_SPENDERS_READ_FAULTS.load(Ordering::Relaxed),
        "hopSweepsReadFaults": HOP_SWEEPS_READ_FAULTS.load(Ordering::Relaxed),
        "sweepProofsReadFaults": SWEEP_PROOFS_READ_FAULTS.load(Ordering::Relaxed),
        "sweepProofContradictions": SWEEP_PROOF_CONTRADICTIONS.load(Ordering::Relaxed),
        "spenderReadFaults": SPENDER_READ_FAULTS.load(Ordering::Relaxed),
        "spenderReadsPerRecompute": OWED_SPENDER_READS_PER_RECOMPUTE,
        "recomputeTimeBudgetMs": OWED_RECOMPUTE_TIME_BUDGET_MS,
        "readRefreshesKicked": READ_REFRESHES.load(Ordering::Relaxed),
        "readRefreshesSkippedInFlight": READ_REFRESHES_SKIPPED.load(Ordering::Relaxed),
        "hopStrandedAfterMs": HOP_STRANDED_AFTER_MS,
        "recomputeOpenAfterMs": OWED_RECOMPUTE_OPEN_AFTER_MS,
        "recomputeAnyAfterMs": OWED_RECOMPUTE_ANY_AFTER_MS,
    })
}

/// The `owed-changed` event body, pushed to the identity's durable box after a
/// recompute a change triggered (the page re-reads `/owed` on it).
pub fn owed_changed_event_body(identity: &str, source: &str, rows: usize, computed_at_ms: i64) -> Value {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hops_view::MarkerVerification;
    use crate::refund_view::RefundStatus;
    use crate::results::{MoneyFacts, PotBinding, TxMoneyFacts};

    const ME: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OPP: &str = "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    fn tx(b: u8) -> String {
        hex::encode([b; 32])
    }

    fn entry(spent: Option<bool>, verdict: Option<PotVerdict>, outcome: Outcome, my_seat: Option<SeatLetter>) -> ResultEntry {
        ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 900_000,
            cov_recovery_height: Some(900_000),
            opponent_identity: OPP.to_string(),
            settle_txid: if spent == Some(true) { Some(tx(0x03)) } else { None },
            spent,
            spent_confirmed: spent,
            pot_binding: PotBinding::Chain,
            game_id_binding: PotBinding::Chain,
            verdict,
            settle_signers: Some("coop".into()),
            outcome,
            outcome_source: Some("chain+seatkey"),
            at_height: if spent == Some(true) { Some(900_100) } else { None },
            winner_hand: None,
            marker_hands: Default::default(),
            hands_source: None,
            committed_keys: None,
            money: MoneyFacts {
                funding: None,
                settle: if spent == Some(true) {
                    Some(TxMoneyFacts { txid: tx(0x03), size_bytes: Some(3577), fee_sats: Some(400), pay_a_sats: Some(39_200), pay_b_sats: Some(0) })
                } else {
                    None
                },
                hops: Vec::new(),
            },
            my_seat,
            stake_sats: my_seat.map(|_| 20_000),
        }
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
                SpenderOutput { vout: 0, pkh_hex: None, sats: 40_000, spent: None, pot_lock: true },
                // the change pays MY home: a pot-creating tx is still a JOIN, never a 190-sat "sweep" (NEW-3)
                SpenderOutput { vout: 1, pkh_hex: Some("cc".repeat(20)), sats: 190, spent: None, pot_lock: false },
            ],
        );
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "cc".repeat(20));
        // 1. the index pointer names the JOIN (status Spent)
        let hops = [hop(HopStatus::Spent, Some(&join), Some(10_000_000))];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &m;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].facts["spendKind"], "pot-unindexed");
        assert_eq!(rows[0].reason.as_deref(), Some(POT_UNINDEXED_REASON));
        assert!(rows[0].facts.get("claim").is_none_or(|c| c.is_null()));
        // 2. the chain rung names it (the index says unspent)
        let old = [hop(HopStatus::Unspent, None, Some(10_000_000))];
        let by_chain = chain_confirmed(&format!("{}:0", tx(0x07)), true, Some(true), Some(&join), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &m;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["spendKind"], "pot-unindexed");
        assert_eq!(rows[0].reason.as_deref(), Some(POT_UNINDEXED_REASON));
    }

    /// Fleet loop 11, the wave's batch 3 (`spec-admit-fast-join-refused`): the JOIN the network refused was EVICTED
    /// with the seats' party rows, so the results view holds no entry and the hop it spent read "cannot tell which
    /// committed home is yours". The ledger names the hop the JOIN spent (the UTXO), the twin holds the lock's
    /// decode, my VERIFIED hop marker names my settle key: the home follows and the hop's real spender is judged.
    #[test]
    fn a_refused_joins_committed_home_is_read_from_the_twin_keyed_on_the_released_hop_so_its_spender_is_judged() {
        use crate::results::CommittedKeys;
        let pub_a = format!("02{}", "aa".repeat(32));
        let pub_b = format!("03{}", "bb".repeat(32));
        let keys = CommittedKeys { pub_a: pub_a.clone(), pub_b: pub_b.clone(), pay_pkh_a: "cc".repeat(20), pay_pkh_b: "dd".repeat(20) };
        let join = tx(0x0c); // the refused JOIN (evicted: its pot row and the party rows live in the twins)
        let sweep = tx(0x0d); // what took the hop instead (courier-read bytes, pays elsewhere)
        let released_json = |hop_txid: &str| Some(format!(r#"[{{"table":"pot_records","txid":"{hop_txid}","vout":0}}]"#));
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
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        // without the twin's word nothing is named (a faulted read) and the pre-change sentence stands
        assert!(evicted_pot_homes(&released, &hops, &[]).is_empty());
        let none: HashMap<String, String> = HashMap::new();
        i.my_pkh_by_game = &none;
        assert_eq!(derive_owed_rows(&i)[0].reason.as_deref(), Some(HOME_UNKNOWN_REASON));
        // THE KEY IS THE RELEASED HOP UTXO, never a name: an eviction that released a stranger's hop names nothing,
        // and a malformed ledger row contributes nothing
        assert!(released_hops_by_eviction(&[(join.clone(), released_json(&tx(0x08)))], &hops).is_empty());
        assert!(released_hops_by_eviction(&[(join.clone(), None), ("zz".repeat(32), released_json(&tx(0x07))), (join.clone(), Some("nope".into()))], &hops).is_empty());
        // the seat must be PROVEN by the hop's own attested key
        let mut unverified = my_hop(&pub_a);
        unverified.marker_verified = MarkerVerification::Unverified;
        assert!(evicted_pot_homes(&released, &[unverified], &twin).is_empty());
        assert!(evicted_pot_homes(&released, &[my_hop(&format!("02{}", "ee".repeat(32)))], &twin).is_empty());
        assert!(evicted_pot_homes(&released, &[my_hop("")], &twin).is_empty());
        let both = [(join.clone(), 0u32, CommittedKeys { pub_b: pub_a.clone(), ..keys.clone() })];
        assert!(evicted_pot_homes(&released, &hops, &both).is_empty());
        let split = [twin[0].clone(), (join.clone(), 1u32, CommittedKeys { pay_pkh_a: "ee".repeat(20), ..keys.clone() })];
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
        assert_eq!(evicted_pot_homes(&released2, &two, &agree), vec![(tx(0x01), "cc".repeat(20))]);
        let disagree = [twin[0].clone(), (join2, 0u32, CommittedKeys { pay_pkh_a: "ee".repeat(20), ..keys.clone() })];
        assert!(evicted_pot_homes(&released2, &two, &disagree).is_empty());
        // seat B by the same rule
        assert_eq!(evicted_pot_homes(&released, &[my_hop(&pub_b)], &twin), vec![(tx(0x01), "dd".repeat(20))]);
    }

    /// The gate's M2: "pays none of your homes" needs a KNOWN home. A hop-only game (no pot committed a home) gets the
    /// spender's pay-to addresses and a sentence, never `spent-elsewhere`.
    #[test]
    fn spent_elsewhere_needs_a_known_home_else_the_addresses_are_served_for_the_device_to_match() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0d);
        let elsewhere = pays(&sweep, &[(0, &"99".repeat(20), 20_000, Some(false)), (1, &"88".repeat(20), 100, None)]);
        let hops = [hop(HopStatus::Spent, Some(&sweep), Some(10_000_000))];
        // home UNKNOWN (no results entry for the game): the addresses, no custody story
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].reason.as_deref(), Some(HOME_UNKNOWN_REASON));
        assert!(rows[0].facts.get("spendKind").is_none());
        assert_eq!(rows[0].facts["spenderPkhs"], json!(["88".repeat(20), "99".repeat(20)]));
        // home KNOWN and not paid: spent outside this game, as before
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "cc".repeat(20));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Payout, Some(20_000)));
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
        let rows = derive_owed_rows(&i);
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
            let rows = derive_owed_rows(&i);
            assert_eq!(rows[0].family, OwedFamily::Unbound, "courier_sourced={courier_sourced}");
            assert!(rows[0].reason.as_deref().unwrap().contains("could not judge"), "{:?}", rows[0].reason);
            assert!(rows[0].facts.get("spendKind").is_none());
        }
        // the same bytes from the INDEX: claimable, as before
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &mine;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["sweepSource"], "index-bytes");
        assert_eq!(rows[0].facts["claimable"], true);
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
        let i = inputs(&results, &refunds, &hops, &valid, &none, &pots, Some(900_200));
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].reason.as_deref(), Some("the spend is not classified yet (no verdict)"));
        assert!(rows[0].facts.get("chainWait").is_none());
    }

    /// Fleet loop 11's wave (2026-09-19): a pot whose spend the index HOLDS but the chain has not confirmed (the
    /// refund submitted here, seen, between two blocks) is an honest CHAIN wait, said by machine
    /// (`facts.chainWait = "block"`), never "not classified" — the verdict is computed at the landing bar.
    #[test]
    fn an_index_held_unconfirmed_spend_without_a_verdict_is_a_chain_wait_not_an_unclassified_story() {
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
        let i = inputs(&results, &refunds, &hops, &valid, &none, &pots, Some(900_200));
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "one story row");
        let r = &rows[0];
        assert_eq!(r.family, OwedFamily::Unbound);
        assert_eq!(r.reason.as_deref(), Some(SPEND_AWAITING_BLOCK_REASON));
        assert_eq!(r.facts["chainWait"], json!("block"));
        assert_eq!(r.facts["spendTxid"], json!(tx(0x03)));
        assert!(r.facts.get("claim").is_none_or(|c| c.is_null()), "no press before the block");

        // an unconfirmed PAYOUT (the verdict known, the block not yet): the press waits, the row says the chain wait
        let mut w = entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A));
        w.spent_confirmed = Some(false);
        w.at_height = None;
        let results_w = vec![w];
        let i_w = inputs(&results_w, &refunds, &hops, &valid, &none, &pots, Some(900_200));
        let rows_w = derive_owed_rows(&i_w);
        assert_eq!(rows_w.len(), 1);
        assert_eq!(rows_w[0].family, OwedFamily::Payout);
        assert_eq!(rows_w[0].facts["claimable"], false);
        assert_eq!(rows_w[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        assert_eq!(rows_w[0].facts["chainWait"], json!("block"));
        // the same spend CONFIRMED without a verdict keeps the old, honest word (a classification gap, not a wait)
        let mut c = entry(Some(true), None, Outcome::Refund, Some(SeatLetter::A));
        c.spent_confirmed = Some(true);
        let results2 = vec![c];
        let i2 = inputs(&results2, &refunds, &hops, &valid, &none, &pots, Some(900_200));
        let rows2 = derive_owed_rows(&i2);
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0].reason.as_deref(), Some("the spend is not classified yet (no verdict)"));
        assert!(rows2[0].facts.get("chainWait").is_none());
    }

    static NONE: std::sync::LazyLock<HashSet<String>> = std::sync::LazyLock::new(HashSet::new);
    fn inputs<'a>(
        results: &'a [ResultEntry],
        refunds: &'a [RefundEntry],
        hops: &'a [HopEntry],
        valid: &'a HashMap<String, ValidRefund>,
        collected_verified: &'a HashSet<String>,
        pots: &'a HashSet<String>,
        tip: Option<u64>,
    ) -> OwedInputs<'a> {
        OwedInputs {
            identity_lc: ME,
            tip,
            now_ms: 1_800_000_000_000,
            results,
            refunds,
            hops,
            valid_refunds: valid,
            collected_verified,
            collected_present: &NONE,
            pot_spenders: pots,
            pot_spenders_faulted: false,
            hop_chain: &NO_CHAIN,
            hop_sweeps: &NO_SWEEPS,
            evicted_pots: &NONE,
            evicted_hop_outpoints: &NONE,
            spender_outputs: &NO_SPENDERS,
            spender_inputs: &NO_INPUTS,
            courier_spenders: &NONE,
            my_pkh_by_game: &NO_PKHS,
        }
    }
    static NO_SPENDERS: std::sync::LazyLock<HashMap<String, Vec<SpenderOutput>>> = std::sync::LazyLock::new(HashMap::new);
    static NO_INPUTS: std::sync::LazyLock<HashMap<String, Vec<(String, u32)>>> = std::sync::LazyLock::new(HashMap::new);
    static NO_PKHS: std::sync::LazyLock<HashMap<String, String>> = std::sync::LazyLock::new(HashMap::new);
    fn pays(spender: &str, outs: &[(u32, &str, u64, Option<bool>)]) -> HashMap<String, Vec<SpenderOutput>> {
        let mut m = HashMap::new();
        m.insert(spender.to_string(), outs.iter().map(|(v, pkh, sats, spent)| SpenderOutput { vout: *v, pkh_hex: Some((*pkh).to_string()), sats: *sats, spent: *spent, pot_lock: false }).collect());
        m
    }
    static NO_CHAIN: std::sync::LazyLock<HashMap<String, HopChainWord>> = std::sync::LazyLock::new(HashMap::new);
    static NO_SWEEPS: std::sync::LazyLock<HashMap<String, crate::hopsweep::FiledHopSweep>> = std::sync::LazyLock::new(HashMap::new);
    fn chain(outpoint: &str, looked: bool, spent: Option<bool>, spender: Option<&str>) -> HashMap<String, HopChainWord> {
        chain_confirmed(outpoint, looked, spent, spender, None)
    }
    fn chain_confirmed(outpoint: &str, looked: bool, spent: Option<bool>, spender: Option<&str>, spent_confirmed: Option<bool>) -> HashMap<String, HopChainWord> {
        chain_confirmed_aged(outpoint, looked, spent, spender, spent_confirmed, None)
    }
    fn chain_confirmed_aged(outpoint: &str, looked: bool, spent: Option<bool>, spender: Option<&str>, spent_confirmed: Option<bool>, age_ms: Option<i64>) -> HashMap<String, HopChainWord> {
        let mut m = HashMap::new();
        m.insert(outpoint.to_string(), HopChainWord { looked, spent, spending_txid: spender.map(str::to_string), spent_confirmed, stale: false, age_ms });
        m
    }
    fn filed(outpoint: &str, sweep_txid: &str) -> HashMap<String, crate::hopsweep::FiledHopSweep> {
        let mut m = HashMap::new();
        m.insert(outpoint.to_string(), crate::hopsweep::FiledHopSweep { sweep_txid: sweep_txid.to_string(), raw_hex: "0100".repeat(20), pays_sats: Some(20_000), index_proven: false, index_proof_height: None });
        m
    }
    /// #517: the same filing, held PROVEN by the index (`transactions.has_proof = 1`, the bump's block when recorded).
    fn filed_proven(outpoint: &str, sweep_txid: &str, height: Option<u64>) -> HashMap<String, crate::hopsweep::FiledHopSweep> {
        let mut m = filed(outpoint, sweep_txid);
        if let Some(s) = m.get_mut(outpoint) {
            s.index_proven = true;
            s.index_proof_height = height;
        }
        m
    }

    #[test]
    fn a_won_confirmed_spend_paying_my_home_is_a_payout_row_with_my_sats() {
        let e = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(&e, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].sats, Some(39_200));
        assert_eq!(rows[0].outpoint, format!("{}:0", tx(0x02)));
        assert_eq!(rows[0].facts["claim"], "internalize");
        assert_eq!(rows[0].facts["creditBeef"], format!("/credit-beef/{}", tx(0x03)));
        assert_eq!(rows[0].at_height, Some(900_100));
    }

    #[test]
    fn a_decided_loss_is_not_a_row_and_a_verified_collected_payout_is_not_a_row_but_a_planted_one_stays() {
        let lost = [entry(Some(true), Some(PotVerdict::WinnerB), Outcome::Lost, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        assert!(derive_owed_rows(&inputs(&lost, &[], &[], &v, &c, &p, Some(900_200))).is_empty());
        let won = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        // the identity's OWN collected filing (signature verified under it): retired
        let verified: HashSet<String> = [tx(0x01)].into_iter().collect();
        assert!(derive_owed_rows(&inputs(&won, &[], &[], &v, &verified, &p, Some(900_200))).is_empty());
        // the gate's HIGH-1: a PRESENT collected row nobody verified (a stranger's dust marker naming the winner)
        // retires NOTHING — the row stays, with the presence named as provenance
        let present: HashSet<String> = [tx(0x01)].into_iter().collect();
        let mut i = inputs(&won, &[], &[], &v, &c, &p, Some(900_200));
        i.collected_present = &present;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["collectedMarkerPresent"], true);
        assert_eq!(rows[0].facts["collectedSigVerified"], false);
        assert_eq!(rows[0].facts["claimable"], true);
    }

    #[test]
    fn an_unconfirmed_payout_is_a_row_that_is_not_claimable_yet() {
        let mut e = entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A));
        e.spent_confirmed = Some(false);
        e.at_height = None; // unconfirmed: no verified mined height (the review's LOW-1 bar)
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(std::slice::from_ref(&e), &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
    }

    #[test]
    fn a_winner_verdict_that_contradicts_my_seat_sizes_nothing() {
        // NOTE-16: winner-a with my seat B (a self-signed claim contradicting the binding) never takes home A's amount
        let e = entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::B));
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(std::slice::from_ref(&e), &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].sats, None);
    }

    #[test]
    fn a_tie_and_a_refund_verdict_pay_my_seat_side() {
        let mut e = entry(Some(true), Some(PotVerdict::Tie), Outcome::Tie, Some(SeatLetter::B));
        e.money.settle.as_mut().unwrap().pay_a_sats = Some(19_600);
        e.money.settle.as_mut().unwrap().pay_b_sats = Some(19_600);
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(std::slice::from_ref(&e), &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Payout, Some(19_600)));
        // the same tie with the seat unknown: a payout the brain cannot size, never a guess
        let unknown = entry(Some(true), Some(PotVerdict::Tie), Outcome::Tie, None);
        let rows = derive_owed_rows(&inputs(std::slice::from_ref(&unknown), &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Payout, None));
    }

    #[test]
    fn a_spent_pot_the_brain_cannot_judge_is_unbound_with_its_sentence() {
        let no_verdict = [entry(Some(true), None, Outcome::Unresolved, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(&no_verdict, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("no verdict"));
        let no_seat = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Unresolved, None)];
        let rows = derive_owed_rows(&inputs(&no_seat, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("seat binding"));
        let no_word = [entry(None, None, Outcome::Unresolved, Some(SeatLetter::A))];
        let rows = derive_owed_rows(&inputs(&no_word, &[], &[], &v, &c, &p, Some(900_200)));
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
    fn an_unspent_pot_past_the_gate_with_a_valid_filed_refund_is_refund_due_sized_by_the_refunds_output() {
        let e = [entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A))];
        let r = [refund_entry(RefundStatus::GateOpen)];
        let mut valid = HashMap::new();
        valid.insert(format!("{}:0", tx(0x02)), ValidRefund { raw_hex: "00".into(), my_sats: Some(19_800) });
        let (c, p) = (HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &valid, &c, &p, Some(900_005)));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::RefundDue, Some(19_800)));
        assert_eq!(rows[0].facts["claim"], "present-refund");
        assert_eq!(rows[0].facts["claimable"], true);
        // LOW-15: a refund paying my home NOTHING is a sizing fault, said as such, never "0 sats"
        let mut zero = HashMap::new();
        zero.insert(format!("{}:0", tx(0x02)), ValidRefund { raw_hex: "00".into(), my_sats: Some(0) });
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &zero, &c, &p, Some(900_005)));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::RefundDue, None));
        assert!(rows[0].facts["refundSizing"].as_str().unwrap().starts_with("zero"));
        // the gate NOT open yet: the hand's window, a rejoin row sized by my stake
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &valid, &c, &p, Some(899_990)));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::InProgress, Some(20_000)));
        assert_eq!(rows[0].facts["claim"], "rejoin");
        // bsv-low #518 (loop 20's pre-flight, 2026-09-21): an EVICTED, never readmitted pot is never a refund-due row
        // (nor any row of its own), whatever its pot row says — a row that outran its eviction before #513 read
        // "unspent" and the brain offered a refund for a pot the network never held (SEEN_IN_ORPHAN_MEMPOOL on
        // every press). The JOIN never formed a pot: the seat's hop carries the money's story.
        let mut evicted = HashSet::new();
        evicted.insert(tx(0x02));
        let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
        i.evicted_pots = &evicted;
        let rows_ev = derive_owed_rows(&i);
        assert!(rows_ev.iter().all(|row| row.family != OwedFamily::RefundDue && row.family != OwedFamily::InProgress), "{rows_ev:?}");
        let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(899_990));
        i.evicted_pots = &evicted;
        let rows_ev = derive_owed_rows(&i);
        assert!(rows_ev.is_empty(), "{rows_ev:?}");
        assert_eq!(rows[0].facts["blocksToGate"], 10);
        // bsv-low #469 / fleet-loop-16 R1: the rejoin row CARRIES the served recovery gate as the
        // `recoveryHeight` FACT — the client's SECOND rejoin-safe recovery source (ORed beside this device's own
        // refund record in `RejoinWindowLine`), so a seat mid-hand shows "rejoin to finish" even before its local
        // refund record surfaces. It rides `facts.recoveryHeight`, never `at_height` (a future gate is not a mined
        // "at height": the owed list renders/sorts `atHeight` as one), which stays None on an unspent pot.
        assert_eq!(rows[0].facts["recoveryHeight"], 900_000);
        assert!(rows[0].at_height.is_none(), "an unspent in-progress pot has no mined height");
        // the gate open and NO valid filed refund: unbound, with the tower named as the path
        let none = HashMap::new();
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &none, &c, &p, Some(900_005)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("dead-man"));
        // an unknown tip cannot open the gate: in-progress with no blocks figure, never refund-due
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &valid, &c, &p, None));
        assert_eq!(rows[0].family, OwedFamily::InProgress);
        assert!(rows[0].facts["blocksToGate"].is_null());
        // MEDIUM-9: the gate height is the ONE shared predicate — a committed 0 falls back to the marker's height
        // (`/refund-view` serves gate-open for the same pot), a timestamp-range value is no height at all
        let mut cov_zero = entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A));
        cov_zero.cov_recovery_height = Some(0);
        let rows = derive_owed_rows(&inputs(std::slice::from_ref(&cov_zero), &r, &[], &valid, &c, &p, Some(900_005)));
        assert_eq!(rows[0].family, OwedFamily::RefundDue, "the marker height 900_000 gates, the committed 0 does not veto it");
        assert_eq!(rows[0].facts["recoveryHeight"], 900_000);
        let mut stamp = entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A));
        stamp.cov_recovery_height = Some(1_800_000_000);
        stamp.recovery_height = 0;
        let rows = derive_owed_rows(&inputs(std::slice::from_ref(&stamp), &[], &[], &valid, &c, &p, Some(900_005)));
        assert!(rows[0].facts["recoveryHeight"].is_null(), "a timestamp-range value is no gate height");
        assert_eq!(rows[0].family, OwedFamily::InProgress);
    }

    fn hop(status: HopStatus, spender: Option<&str>, created_ms_ago: Option<i64>) -> HopEntry {
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
        }
    }

    #[test]
    fn a_hop_unspent_past_the_window_is_stranded_with_a_sweep_claim_a_young_one_is_in_progress_with_rejoin() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        let key = format!("{}:0", tx(0x07));
        let unspent = chain(&key, true, Some(false), None);
        let mut i = inputs(&[], &[], &old, &v, &c, &p, Some(900_000));
        i.hop_chain = &unspent;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::HopStranded, Some(20_190)));
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["chainProbe"], "unspent");
        // a YOUNG unspent hop is an `in-progress` row with the felt's rejoin (the stake is funded, the hand has not
        // started), sized by the hop, never a sweep claim before the window
        let young = [hop(HopStatus::Unspent, None, Some(60_000))];
        let rows = derive_owed_rows(&inputs(&[], &[], &young, &v, &c, &p, Some(900_000)));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::InProgress, Some(20_190)));
        assert_eq!(rows[0].facts["claim"], "rejoin");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["stage"], "hop-funded");
        assert_eq!(rows[0].facts["strandedAfterMs"], HOP_STRANDED_AFTER_MS);
        assert_eq!(rows[0].reason.as_deref(), Some(YOUNG_HOP_REASON));
        assert!(rows[0].facts.get("sweepRawHex").is_none());
        // an age the brain cannot say, or a marker not yet verified: no row
        let ageless = [hop(HopStatus::Unspent, None, None)];
        assert!(derive_owed_rows(&inputs(&[], &[], &ageless, &v, &c, &p, Some(900_000))).is_empty());
        let mut unverified = hop(HopStatus::Unspent, None, Some(60_000));
        unverified.marker_verified = MarkerVerification::Unknown;
        assert!(derive_owed_rows(&inputs(&[], &[], &[unverified], &v, &c, &p, Some(900_000))).is_empty());
    }

    #[test]
    fn a_hop_spent_by_a_low_pot_is_no_row_a_hop_spent_by_an_unindexed_spender_is_unbound_never_a_custody_story() {
        let (v, c) = (HashMap::new(), HashSet::new());
        let join = tx(0x02);
        let by_pot = [hop(HopStatus::Spent, Some(&join), Some(10_000_000))];
        let pots: HashSet<String> = [join.clone()].into_iter().collect();
        assert!(derive_owed_rows(&inputs(&[], &[], &by_pot, &v, &c, &pots, Some(900_000))).is_empty());
        // MEDIUM-7: a spender absent from the index is NOT evidence of a wallet spend (an unindexed JOIN is the common
        // honest case): a sentence, no claim, never "spent elsewhere"
        let elsewhere = [hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000))];
        let rows = derive_owed_rows(&inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].facts["claim"].is_null());
        assert!(rows[0].reason.as_deref().unwrap().contains("could not judge"));
        // a FAULTED spender check says so
        let mut i = inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000));
        i.pot_spenders_faulted = true;
        let rows = derive_owed_rows(&i);
        assert!(rows[0].reason.as_deref().unwrap().contains("could not check"));
    }

    #[test]
    fn the_sweep_claim_rests_on_the_chain_rung_too() {
        // the index's "unspent" alone is a sentence; the chain's unspent is the claim; the chain's spent-by-a-pot is no
        // row; the chain's spent-by-another is could-not-judge
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let old = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        let key = format!("{}:0", tx(0x07));
        // no chain word yet
        let rows = derive_owed_rows(&inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].facts["chainProbe"], "pending");
        assert!(rows[0].reason.as_deref().unwrap().contains("not corroborated"));
        // the providers could not look
        let not_looked = chain(&key, false, None, None);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &not_looked;
        assert_eq!(derive_owed_rows(&i)[0].family, OwedFamily::Unbound);
        // spent by a POT the index holds (the index lagged): no row
        let join = tx(0x02);
        let pots: HashSet<String> = [join.clone()].into_iter().collect();
        let by_pot = chain(&key, true, Some(true), Some(&join));
        let mut i = inputs(&[], &[], &old, &v, &c, &pots, Some(900_000));
        i.hop_chain = &by_pot;
        assert!(derive_owed_rows(&i).is_empty());
        // spent by something the index does not hold: could not judge, the spender named in the facts
        let elsewhere = chain(&key, true, Some(true), Some(&tx(0x09)));
        let mut i = inputs(&[], &[], &old, &v, &c, &pots, Some(900_000));
        i.hop_chain = &elsewhere;
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&inputs(&[], &[], std::slice::from_ref(&planted), &v, &c, &p, Some(900_000)));
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Unbound, None));
        assert!(rows[0].facts["claim"].is_null());
        assert!(rows[0].reason.as_deref().unwrap().contains("does not verify"));
        let mut pending = hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1));
        pending.marker_verified = MarkerVerification::Unknown;
        let rows = derive_owed_rows(&inputs(&[], &[], std::slice::from_ref(&pending), &v, &c, &p, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("not verified yet"));
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
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Payout, Some(20_000)));
        assert_eq!(rows[0].facts["claim"], "internalize");
        assert_eq!(rows[0].facts["claimable"], false, "the sweep is seen, not mined: the credit lands with the block");
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
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["claimable"], true);
        // the CHAIN rung naming my sweep on an index-unspent hop is the same payout (a sweep broadcast direct to ARC)
        let old = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        i.hop_chain = &by_chain;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].facts["claimable"].as_bool()), (OwedFamily::Payout, Some(true)));
        // a spender that is NOT my filing (another tx) stays could-not-judge, as before
        let other = [hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000))];
        let mut i = inputs(&[], &[], &other, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        assert_eq!(derive_owed_rows(&i)[0].family, OwedFamily::Unbound);
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
        let hops = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        // the courier memo: spent by my sweep, NOT confirmed (an indexer lagging the block)
        let lagging = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(false));
        let proven = filed_proven(&key, &sweep, Some(967_696));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        i.hop_chain = &lagging;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Payout, Some(20_000)));
        assert_eq!(rows[0].facts["claimable"], true, "the index's verified proof outranks the courier's lag");
        assert_eq!(rows[0].facts["confirmedSource"], "index-proof");
        assert_eq!(rows[0].facts["sweepProofHeight"], 967_696);
        assert!(rows[0].facts.get("claimReason").is_none());
        // no courier word at all (never probed): the proof alone names the spender and confirms it
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].facts["claimable"].as_bool()), (OwedFamily::Payout, Some(true)));
        assert_eq!(rows[0].facts["confirmedSource"], "index-proof");
        // the same filing UNPROVEN by the index keeps the old word: the courier's lag is the row's wait
        let unproven = filed(&key, &sweep);
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &unproven;
        i.hop_chain = &lagging;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["claimable"], false);
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        // the proof is keyed on the FILED sweep: a hop the index names spent+confirmed by ANOTHER tx never turns an
        // unproven filing's payout claimable (the 2026-09-19 gate's LOW-3 shape stands)
        let mut other = hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000));
        other.spent_confirmed = Some(true);
        let hops_o = [other];
        let mut i = inputs(&[], &[], &hops_o, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &unproven;
        let rows = derive_owed_rows(&i);
        assert_ne!(rows[0].family, OwedFamily::Payout);
        // the row carries the sweep's proven block as its height (the gate's NIT-3)
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        assert_eq!(derive_owed_rows(&i)[0].at_height, Some(967_696));
        // an old latch without a recorded height (the gate's NIT-5): proven is proven, no height on the row
        let proven_no_height = filed_proven(&key, &sweep, None);
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven_no_height;
        i.hop_chain = &lagging;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].facts["claimable"].as_bool(), rows[0].at_height), (Some(true), None));
        assert!(rows[0].facts.get("sweepProofHeight").is_none());
        // the precedence: the hop row names the sweep spent+confirmed AND the index holds it proven: "index-proof"
        let mut named = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        named.spent_confirmed = Some(true);
        let hops_n = [named];
        let mut i = inputs(&[], &[], &hops_n, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        assert_eq!(derive_owed_rows(&i)[0].facts["confirmedSource"], "index-proof");
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
        let rows = derive_owed_rows(&i);
        assert_ne!(rows[0].family, OwedFamily::Payout, "the proof did not decide against the row's confirmed spender");
        assert!(rows[0].reason.as_deref().unwrap_or("").contains("could not judge"), "{:?}", rows[0].reason);
        assert_eq!(rows[0].facts["sweepProofContradicted"], true, "the operator sees WHICH hop");
        assert_eq!(count_sweep_proof_contradictions(&rows), 1);
        // the chain rung naming ANOTHER confirmed spender on an index-unspent hop: the same
        let unspent = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        let chain_other = chain_confirmed(&key, true, Some(true), Some(&tx(0x09)), Some(true));
        let mut i = inputs(&[], &[], &unspent, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        i.hop_chain = &chain_other;
        let rows = derive_owed_rows(&i);
        assert_ne!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["sweepProofContradicted"], true);
        assert_eq!(count_sweep_proof_contradictions(&rows), 1);
        // an UNCONFIRMED word for another tx does not contradict a proof (a stale pointer the block already settled)
        let mut stale = hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000));
        stale.spent_confirmed = Some(false);
        let hops_s = [stale];
        let mut i = inputs(&[], &[], &hops_s, &v, &c, &no_pots, Some(967_699));
        i.hop_sweeps = &proven;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].facts["confirmedSource"].as_str()), (OwedFamily::Payout, Some("index-proof")));
        assert!(rows[0].facts.get("sweepProofContradicted").is_none());
        assert_eq!(count_sweep_proof_contradictions(&rows), 0);
    }

    #[test]
    fn a_stranded_hop_carries_its_filed_sweep_or_says_sign_here_and_an_unindexed_hop_is_judged_by_the_chain_too() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let key = format!("{}:0", tx(0x07));
        let unspent = chain(&key, true, Some(false), None);
        let old = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        // without a filing: the press signs on the owning device
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &unspent;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["sweepSource"], "sign-here");
        assert!(rows[0].facts["sweepRawHex"].is_null());
        assert!(rows[0].facts["seatSettlePubkey"].is_string(), "the key the owning device matches its derivation against");
        // with a filing: the bytes ride the row
        let sweeps = filed(&key, &tx(0x0c));
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &unspent;
        i.hop_sweeps = &sweeps;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["sweepSource"], "hopsweep-filing");
        assert_eq!(rows[0].facts["sweepTxid"], tx(0x0c));
        assert_eq!(rows[0].facts["sweepRawHex"], "0100".repeat(20));
        // an UNKNOWN hop (the container never indexed) past the window with the chain's unspent: the same claim
        let mut unknown = hop(HopStatus::Unknown, None, Some(HOP_STRANDED_AFTER_MS + 1));
        unknown.spent = None;
        let hops = [unknown];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &unspent;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].facts["claim"].as_str()), (OwedFamily::HopStranded, Some("sweep-hop")));
        // …and without a chain word it is the sentence naming the missing index row
        let mut unknown = hop(HopStatus::Unknown, None, Some(HOP_STRANDED_AFTER_MS + 1));
        unknown.spent = None;
        let hops = [unknown];
        let rows = derive_owed_rows(&inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("not in the index"));
        // a recorded-but-unconfirmed spend on an Unknown hop keeps its own sentence
        let mut recorded = hop(HopStatus::Unknown, Some(&tx(0x09)), Some(HOP_STRANDED_AFTER_MS + 1));
        recorded.spent = Some(true);
        let hops = [recorded];
        let rows = derive_owed_rows(&inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000)));
        assert!(rows[0].reason.as_deref().unwrap().contains("recorded but not confirmed"));
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
        let old = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let no_pots: HashSet<String> = HashSet::new();
        // 1. the bytes pay my home, the home output unspent → a claimable payout (source index-bytes, no raw)
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, Some(false))]);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &mine;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::Payout, Some(20_000)));
        assert_eq!(rows[0].facts["sweepSource"], "index-bytes");
        assert_eq!(rows[0].facts["sweepTxid"], sweep);
        assert!(rows[0].facts["sweepRawHex"].is_null());
        assert_eq!(rows[0].facts["claimable"], true);
        // 2. the home output already SPENT (collected and moved on) → not a row
        let moved = pays(&sweep, &[(0, &my_pkh, 20_000, Some(true))]);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &moved;
        i.my_pkh_by_game = &pkhs;
        assert!(derive_owed_rows(&i).is_empty());
        // 3. the bytes pay ELSEWHERE → the spent-elsewhere story, no claim
        let elsewhere = pays(&sweep, &[(0, &"99".repeat(20), 20_000, Some(false))]);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        // 4. the bytes unknown (not read this pass) → could not judge, as before
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &by_chain;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert!(rows[0].reason.as_deref().unwrap().contains("could not judge the spender"));
        // 5. the same through the INDEX pointer (status Spent, the spender not a covenant pot)
        let mut spent = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        spent.spent_confirmed = Some(true);
        let hops = [spent];
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &mine;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].family, rows[0].facts["claimable"].as_bool()), (OwedFamily::Payout, Some(true)));
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        assert_eq!(derive_owed_rows(&i)[0].facts["spendKind"], "spent-elsewhere");
    }

    #[test]
    fn the_p2pkh_pkh_reader_takes_the_standard_shape_only() {
        let mut lock = vec![0x76, 0xa9, 0x14];
        lock.extend_from_slice(&[0x42; 20]);
        lock.extend_from_slice(&[0x88, 0xac]);
        assert_eq!(p2pkh_pkh_hex(&lock).as_deref(), Some("42".repeat(20).as_str()));
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
        chain.insert(key.clone(), HopChainWord { looked: true, spent: Some(false), spending_txid: None, spent_confirmed: None, stale: false, age_ms: None });
        let mut i = inputs(&e, &[], &young, &v, &c, &p, Some(900_000));
        i.hop_chain = &chain;
        let rows = derive_owed_rows(&i);
        assert!(rows.iter().any(|r| r.family == OwedFamily::InProgress && r.facts["claim"] == "rejoin"), "a young hop of a live game is in-progress");
        assert!(!rows.iter().any(|r| r.family == OwedFamily::HopStranded));
        // the eviction row as the overlay writes it: the pot txid + the released spends (this hop's outpoint)
        let released = format!("[{{\"table\":\"pot_records\",\"txid\":\"{}\",\"vout\":0}}]", tx(0x07));
        let refused = released_hop_outpoints(&[(tx(0x02), Some(released))]);
        assert_eq!(refused.len(), 1);
        assert!(refused.contains(&key));
        let evicted: HashSet<String> = [tx(0x02)].into_iter().collect();
        i.evicted_pots = &evicted;
        i.evicted_hop_outpoints = &refused;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "the pot is no row; the hop is the one row");
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["joinRefused"], true);
        assert_eq!(rows[0].reason.as_deref(), Some(JOIN_REFUSED_REASON));
        // HIGH-1: a PLANTED party row naming this identity, a live game and a stranger's own evicted txid releases the
        // STRANGER's hops, never this seat's: the young hop stays in-progress (a name is never the key)
        let planted = format!("[{{\"table\":\"pot_records\",\"txid\":\"{}\",\"vout\":0}}]", tx(0x09));
        let strangers = released_hop_outpoints(&[(tx(0x02), Some(planted))]);
        i.evicted_hop_outpoints = &strangers;
        let rows = derive_owed_rows(&i);
        assert!(rows.iter().any(|r| r.family == OwedFamily::InProgress && r.facts["claim"] == "rejoin"), "a plant strands nothing");
        assert!(!rows.iter().any(|r| r.family == OwedFamily::HopStranded));
        // the parser: NULL, malformed and non-hex entries contribute nothing (fail-safe)
        assert!(released_hop_outpoints(&[(tx(0x02), None)]).is_empty());
        assert!(released_hop_outpoints(&[(tx(0x02), Some("not json".into()))]).is_empty());
        assert!(released_hop_outpoints(&[(tx(0x02), Some("[{\"txid\":\"zz\",\"vout\":0}]".into()))]).is_empty());
    }

    /// The delta-verify's HIGH-A (2026-09-19): the refusal's candidate set must come from the eviction LEDGER —
    /// under the overlay's real eviction SQL the party row keyed by the pot leaves `potparty_records` (the results
    /// view has no entry to derive a candidate from), while the ledger's window still names the released hop; the
    /// derivation then strands the identity's young hop. Real SQLite, the shipped migrations, the overlay's own
    /// move statements (a dev-dependency), the app layer's own query.
    #[test]
    fn a_refused_join_is_found_through_the_eviction_ledger_after_the_overlay_moved_the_party_row_real_sqlite() {
        use bsv_overlay_cloudflare::admit_fast::{create_shadow_sql, move_sql, ColumnInfo, MOVED_TABLES};
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(e.to_string().to_ascii_lowercase().contains("duplicate column"), "migration failed under real SQLite: {e}");
            }
        }
        let me = ME.to_string();
        let pot = tx(0x02);
        let hop_txid = tx(0x07);
        conn.execute(
            "INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt) \
             VALUES (?1, 'opp', ?2, ?3, 0, 100, 'sig', ?4, 0, 1000)",
            rusqlite::params![me, tx(0x01), pot, tx(0x0a)],
        )
        .unwrap();
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 900)", rusqlite::params![pot]).unwrap();
        // the hop's own row, its spend pointer RELEASED by the eviction (the overlay's release SQL is its own pin)
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 800)", rusqlite::params![hop_txid]).unwrap();
        let evicted_at = 1_700_000_000_000i64;
        conn.execute(
            "INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES (?1, 'REJECTED (corroborated)', ?2, NULL, 2, ?3)",
            rusqlite::params![pot, evicted_at, format!("[{{\"table\":\"pot_records\",\"txid\":\"{hop_txid}\",\"vout\":0}}]")],
        )
        .unwrap();
        let cols = |conn: &rusqlite::Connection, t: &str| -> Vec<ColumnInfo> {
            let mut st = conn.prepare(&format!("PRAGMA table_info(\"{t}\")")).unwrap();
            st.query_map([], |r| Ok(ColumnInfo { name: r.get(1)?, ty: r.get::<_, String>(2).unwrap_or_default(), notnull: r.get::<_, i64>(3).unwrap_or(0), dflt_value: r.get::<_, Option<String>>(4).unwrap_or(None) })).unwrap().map(|r| r.unwrap()).collect()
        };
        // the overlay's eviction: every keyed row of the pot moves to its twin (the same SQL the D1 path runs)
        for (table, keys) in MOVED_TABLES {
            let c = cols(&conn, table);
            if c.is_empty() {
                continue;
            }
            for key in keys.iter() {
                if !c.iter().any(|x| x.name == *key) {
                    continue;
                }
                conn.execute_batch(&create_shadow_sql(table, &c)).unwrap();
                let (ins, del) = move_sql(table, key, &c);
                conn.execute(&ins, rusqlite::params![evicted_at, "REJECTED (corroborated)", &pot]).unwrap();
                conn.execute(&del, [&pot]).unwrap();
            }
        }
        let party_rows: i64 = conn.query_row("SELECT COUNT(*) FROM potparty_records WHERE identity = ?1", [&me], |r| r.get(0)).unwrap();
        assert_eq!(party_rows, 0, "the results view has NO entry to derive a candidate from after the eviction");
        // the app layer's own query on the ledger still names the released hop
        let mut st = conn.prepare(OWED_EVICTIONS_WINDOW_SQL).unwrap();
        let rows: Vec<(String, Option<String>)> = st
            .query_map([evicted_at - OWED_EVICTION_WINDOW_MS + 1], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        let released = released_hop_outpoints(&rows);
        let young = [hop(HopStatus::Unspent, None, Some(60_000))];
        let refused = refused_hop_outpoints_of(&released, &young);
        assert_eq!(refused.len(), 1);
        assert!(refused.contains(&format!("{hop_txid}:0")));
        // and the derivation strands the young hop (the pot itself is gone from the results, so no pot row at all)
        let key = format!("{hop_txid}:0");
        let mut chain: HashMap<String, HopChainWord> = HashMap::new();
        chain.insert(key, HopChainWord { looked: true, spent: Some(false), spending_txid: None, spent_confirmed: None, stale: false, age_ms: None });
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let mut i = inputs(&[], &[], &young, &v, &c, &p, Some(900_000));
        i.hop_chain = &chain;
        i.evicted_hop_outpoints = &refused;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["joinRefused"], true);
        // a window that excludes the eviction finds nothing (the bound is the window's whole meaning)
        let none: Vec<(String, Option<String>)> = conn
            .prepare(OWED_EVICTIONS_WINDOW_SQL)
            .unwrap()
            .query_map([evicted_at + 1], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(none.is_empty());
        // a stranger's eviction releases the stranger's hops: never a hop of ours
        let strangers: HashSet<String> = [format!("{}:0", tx(0x09))].into_iter().collect();
        assert!(refused_hop_outpoints_of(&strangers, &young).is_empty());
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
        let rows = derive_owed_rows(&i);
        let payout = rows.iter().find(|r| r.family == OwedFamily::Payout).expect("the sweep's payout row");
        assert_eq!(payout.facts["claimable"], false, "a different spender's confirmation is not the sweep's");
        // the chain naming the sweep, confirmed → claimable
        let by_sweep = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        i.hop_chain = &by_sweep;
        let rows = derive_owed_rows(&i);
        let payout = rows.iter().find(|r| r.family == OwedFamily::Payout).expect("the sweep's payout row");
        assert_eq!(payout.facts["claimable"], true);
    }

    #[test]
    fn a_coalesced_ask_remembers_the_latest_source_and_the_attribution_sql_names_the_marker_tables() {
        let mut m = HashMap::new();
        owed_rerun_note(&mut m, "ab", "pot-changed");
        owed_rerun_note(&mut m, "ab", "hop-changed");
        assert_eq!(m.get("ab").map(String::as_str), Some("hop-changed"));
        assert_eq!(m.len(), 1);
        // MEDIUM-1: the eviction trigger attributes through the seats' own markers, by the marker tables' real columns
        assert!(OWED_ATTRIBUTE_BY_POT_SQL.contains("FROM potparty_records WHERE potTxid = ?1 AND potVout = ?2"));
        assert!(OWED_ATTRIBUTE_BY_HOP_SQL.contains("FROM hopparty_records WHERE txid = ?1 AND hopVout = ?2"));
    }

    #[test]
    fn an_evicted_pot_is_not_a_row_the_hop_carries_the_story() {
        // the JOIN the network refused never formed a pot: its results entry (spent None) is skipped when the
        // eviction ledger names it; an unadmitted pot without an eviction record keeps its sentence
        let e = [entry(None, None, Outcome::Unresolved, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(&e, &[], &[], &v, &c, &p, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("no spend word"));
        let evicted: HashSet<String> = [tx(0x02)].into_iter().collect();
        let mut i = inputs(&e, &[], &[], &v, &c, &p, Some(900_000));
        i.evicted_pots = &evicted;
        assert!(derive_owed_rows(&i).is_empty());
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
        assert!(derive_owed_rows(&i).is_empty(), "collected: the sweep's credit is in the wallet");
        // the same game ALSO has a paid pot: two candidates, the marker cannot say which; both rows stay
        let paid = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let mut i = inputs(&paid, &[], &hops, &v, &verified, &no_pots, Some(900_200));
        i.hop_sweeps = &sweeps;
        assert_eq!(derive_owed_rows(&i).len(), 2);
    }

    #[test]
    fn one_collected_marker_never_hides_a_second_pots_payout_of_the_same_game() {
        // N10: two pots funded under one game id (a re-funding); one verified collected marker names the game
        let a = entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A));
        let mut b = entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A));
        b.pot_txid = tx(0x05);
        b.settle_txid = Some(tx(0x06));
        let both = [a, b];
        let verified: HashSet<String> = [tx(0x01)].into_iter().collect();
        let (v, p) = (HashMap::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(&both, &[], &[], &v, &verified, &p, Some(900_200)));
        assert_eq!(rows.len(), 2, "both payout rows stay; the marker cannot say which pot it meant");
        assert!(rows.iter().all(|r| r.facts["collectedSigVerified"] == true && r.facts["collectedMarkerPresent"] == true));
    }

    #[test]
    fn the_read_rule_recomputes_on_stale_on_the_gate_flip_on_an_aged_open_list_and_on_any_old_list() {
        // N5: the four arms of `should_recompute`, one predicate
        let e = [entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let in_progress = derive_owed_rows(&inputs(&e, &[], &[], &v, &c, &p, Some(899_990)));
        assert_eq!(in_progress[0].family, OwedFamily::InProgress);
        let paid = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let payout = derive_owed_rows(&inputs(&paid, &[], &[], &v, &c, &p, Some(900_200)));
        // stale wins whatever else
        assert!(should_recompute(true, 0, Some(1), Some(1), &[]));
        // the gate flip: the tip reached the in-progress row's recovery height since the compute
        assert!(should_recompute(false, 0, Some(899_990), Some(900_000), &in_progress));
        assert!(!should_recompute(false, 0, Some(899_990), Some(899_995), &in_progress));
        assert!(!should_recompute(false, 0, Some(900_000), Some(900_000), &in_progress), "no tip advance, no flip");
        assert!(should_recompute(false, 0, None, Some(900_000), &in_progress), "a compute without a tip heals on the next tipped read");
        // an OPEN list ages out at 5 minutes; a claimable-payout-only list at 15; an empty list at 15
        assert!(!should_recompute(false, 4 * 60_000, Some(1), Some(1), &in_progress));
        assert!(should_recompute(false, 6 * 60_000, Some(1), Some(1), &in_progress));
        assert!(!should_recompute(false, 6 * 60_000, Some(1), Some(1), &payout));
        assert!(should_recompute(false, 16 * 60_000, Some(1), Some(1), &payout));
        assert!(should_recompute(false, 16 * 60_000, Some(1), Some(1), &[]));
        assert!(!should_recompute(false, 14 * 60_000, Some(1), Some(1), &[]));
    }

    #[test]
    fn the_service_order_puts_the_actionable_families_first_then_the_newest_spend() {
        // N2b
        let mut rows = vec![
            OwedRow { identity: ME.into(), outpoint: "b".into(), family: OwedFamily::Unbound, game_id: String::new(), sats: None, opponent_identity: None, at_height: Some(5), facts: Value::Null, reason: None },
            OwedRow { identity: ME.into(), outpoint: "a".into(), family: OwedFamily::Payout, game_id: String::new(), sats: None, opponent_identity: None, at_height: Some(1), facts: Value::Null, reason: None },
            OwedRow { identity: ME.into(), outpoint: "c".into(), family: OwedFamily::Payout, game_id: String::new(), sats: None, opponent_identity: None, at_height: Some(9), facts: Value::Null, reason: None },
            OwedRow { identity: ME.into(), outpoint: "d".into(), family: OwedFamily::RefundDue, game_id: String::new(), sats: None, opponent_identity: None, at_height: None, facts: Value::Null, reason: None },
        ];
        sort_rows_for_service(&mut rows);
        assert_eq!(rows.iter().map(|r| r.outpoint.as_str()).collect::<Vec<_>>(), ["c", "a", "d", "b"]);
    }

    #[test]
    fn a_hop_the_view_could_not_judge_is_a_sentence_not_silence() {
        // MEDIUM-10: the hops view's Unknown covers the loudest stranded case (the container never indexed)
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let mut unknown = hop(HopStatus::Unknown, None, Some(10_000_000));
        unknown.spent = None;
        let rows = derive_owed_rows(&inputs(&[], &[], std::slice::from_ref(&unknown), &v, &c, &p, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("not in the index"));
        assert_eq!(rows[0].sats, Some(20_190));
        let mut pending = hop(HopStatus::Unknown, Some(&tx(0x09)), Some(10_000_000));
        pending.spent = Some(true);
        let rows = derive_owed_rows(&inputs(&[], &[], std::slice::from_ref(&pending), &v, &c, &p, Some(900_000)));
        assert!(rows[0].reason.as_deref().unwrap().contains("not confirmed"));
    }

    #[test]
    fn the_wire_body_is_versioned_and_carries_every_row_field() {
        let e = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(&e, &[], &[], &v, &c, &p, Some(900_200)));
        let body: Value = serde_json::from_str(&owed_body(ME, Some(900_200), &rows, 1_800_000_000_000, false)).unwrap();
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
        t.outputs.push(TransactionOutput { satoshis: Some(19_800), locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&pkh)).unwrap(), change: false });
        t.outputs.push(TransactionOutput { satoshis: Some(19_800), locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&other)).unwrap(), change: false });
        t.outputs.push(TransactionOutput { satoshis: Some(5), locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&pkh)).unwrap(), change: false });
        let raw = hex::encode(t.to_binary());
        assert_eq!(refund_output_sats(&raw, &hex::encode(pkh)), Some(19_805));
        assert_eq!(refund_output_sats(&raw, &hex::encode([0x33u8; 20])), Some(0));
        assert_eq!(refund_output_sats("zz", &hex::encode(pkh)), None);
        assert_eq!(refund_output_sats(&raw, "abcd"), None);
    }

    #[test]
    fn the_collected_lookup_covers_every_hop_game_beside_the_spent_pots() {
        let games = collected_lookup_games(["AA".repeat(32).as_str(), &"bb".repeat(32)].into_iter(), [&"cc".repeat(32)[..], &"bb".repeat(32)].into_iter());
        assert_eq!(games, vec!["aa".repeat(32), "bb".repeat(32), "cc".repeat(32)]);
        assert!(collected_lookup_games(std::iter::empty(), std::iter::empty()).is_empty());
    }

    #[test]
    fn a_filed_sweep_the_index_names_but_never_confirmed_is_claimable_once_the_chain_rung_confirms_it() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0c);
        let key = format!("{}:0", tx(0x07));
        let mut h = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        h.spent_confirmed = Some(false); // the index recorded the spend before its block and never re-checked
        let filed_sweeps = filed(&key, &sweep);
        let mut i = inputs(&[], &[], std::slice::from_ref(&h), &v, &c, &p, Some(900_000));
        i.hop_sweeps = &filed_sweeps;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["claimable"], false); // the index's word alone: not yet
        // the courier's word heals it: the same spender, confirmed
        let word = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        i.hop_chain = &word;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["sweepSource"], "hopsweep-filing");
    }

}
