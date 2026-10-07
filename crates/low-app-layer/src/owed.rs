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
//! filing; a payout whose paying transaction the identity's own held filing
//! names (bsv-low #492: the wallet already holds it); a written-off era pot (the views' era clause); a pot the identity
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
/// The door's refusal ledger AS THE WALKING IDENTITY OWNS IT (the lens fold's MEDIUM-1): driven from the identity's
/// own hop markers (`idx_hopparty_identity`), one primary-key probe of the ledger per marker, never a window over
/// every identity's refusals. The ledger holds one row per hop outpoint, so a stranger's refused variants of a
/// seat's input cost this read one row at most, and a refusal of a hop no marker of this identity names is never
/// read. A hop the index shows SPENT is skipped (a spend of it was admitted since: the refusal is stale, and the
/// door retires it). Binds: `?1` the identity (lowercase), `?2` now minus `OWED_EVICTION_WINDOW_MS`.
/// Cost: the identity's hop markers once more (the hops view walks the same index range), bounded rows out.
/// The delta fold's LOW-3 (2026-10-06): NEWEST refusal first (then the outpoint, so the cut is deterministic). A
/// seat with more than `OWED_DOOR_REFUSALS_MAX` unspent door-refused hops gets the newest 64 stranded at once, the
/// hop it just tried among them; the older ones keep the rejoin until the 30-minute age rule strands them, and
/// each enters the 64 as a newer one is swept (a spent hop is skipped). Before, the cut had no order.
pub const OWED_DOOR_REFUSALS_SQL: &str = "SELECT lower(r.hopTxid) AS hopTxid, r.hopVout AS hopVout, r.reason AS reason      FROM hopparty_records hp CROSS JOIN submit_refusals r ON r.hopTxid = hp.txid AND r.hopVout = hp.hopVout      WHERE hp.identity = ?1 AND r.refusedAt >= ?2        AND NOT EXISTS (SELECT 1 FROM pot_records p WHERE p.txid = r.hopTxid AND p.outputIndex = r.hopVout AND p.spent = 1)      GROUP BY r.hopTxid, r.hopVout ORDER BY r.refusedAt DESC, r.hopTxid ASC, r.hopVout ASC LIMIT 64";
const _: () = assert!(OWED_DOOR_REFUSALS_MAX == 64);

/// PURE: the identity's own hop outpoints (`txid:vout`, lowercase) the door's ledger names, with the arm that
/// named each. `rows` are `OWED_DOOR_REFUSALS_SQL`'s (hopTxid, hopVout, reason); the intersection with the walk's
/// OWN hops is kept as the belt (the SQL already joins the identity's markers): a row for an outpoint the walk does
/// not hold names nothing.
pub fn door_refused_hops(rows: &[(String, u32, String)], hops: &[HopEntry]) -> HashMap<String, DoorRefusal> {
    let mine: HashSet<String> = hops.iter().map(|h| outpoint_key(&h.hop_txid, h.hop_vout)).collect();
    let mut out: HashMap<String, DoorRefusal> = HashMap::new();
    for (txid, vout, reason) in rows {
        let key = outpoint_key(txid, *vout);
        if mine.contains(&key) {
            out.insert(key, DoorRefusal::of_reason(reason));
        }
    }
    out
}

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
/// The courier probes ONE recompute may buy for the home outputs of its courier-proven payouts (each is one
/// spent-any ladder, memoised like the hops', plus one tx-any read for a named spender's bytes).
pub const OWED_HOME_PROBES_PER_RECOMPUTE: usize = 2;
/// PURE (bsv-low #485): does `spender_raw` consume `home_txid:home_vout` with an unlocking script that VERIFIES
/// against the P2PKH lock of `home_pkh_hex` for `sats`? The interpreter runs here
/// (`overlay_discovery::pot::p2pkh_input_signed`: `bsv_rs::script::Spend`, the engine's own walk), over the sighash
/// of the bytes given: only the home key's holder can produce a `true`, whoever carried the bytes and whether or
/// not the spender is mined. Anything malformed, oversized or unverifiable is `false`.
pub fn home_output_spend_proven(spender_raw: &[u8], home_txid: &str, home_vout: u32, home_pkh_hex: &str, sats: u64) -> bool {
    let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(spender_raw) else { return false };
    let Some(pkh) = hex::decode(home_pkh_hex).ok().and_then(|b| <[u8; 20]>::try_from(b).ok()) else { return false };
    let Some(vin) = tx
        .inputs
        .iter()
        .position(|inp| inp.source_output_index == home_vout && inp.source_txid.as_deref().is_some_and(|t| t.eq_ignore_ascii_case(home_txid)))
    else {
        return false;
    };
    overlay_discovery::pot::p2pkh_input_signed(&tx, vin, &overlay_discovery::pot::p2pkh_lock(&pkh), sats)
}

/// PURE: the probe-memo target of a home output's LATCH (`read_probe_memos` and the latch write both key it through
/// `hops_view::probe_memo_key`, so the key written is the key read back: pinned on real SQLite).
pub fn home_latch_target(sweep_txid: &str, vout: u32) -> (String, u32) {
    (format!("{HOME_SPEND_LATCH_PREFIX}{}", sweep_txid.to_ascii_lowercase()), vout)
}

/// PURE (bsv-low #485, the lens fold's MEDIUM-2): the first rung of the home walk. A candidate whose latch memo
/// reads spent with a named spender is `Proven` with no network ask; every other candidate is a target of the
/// chain rung, in candidate order. A memo for a key no candidate has is ignored.
pub fn latched_home_words(
    candidates: &[CourierHomeOutput],
    latch_memos: &[crate::hops_view::ProbeMemo],
) -> (HashMap<String, HomeSpendWord>, Vec<(String, u32)>) {
    let latched: HashSet<&str> = latch_memos.iter().filter(|m| m.spent && m.spending_txid.is_some()).map(|m| m.outpoint.as_str()).collect();
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
            let Some(raw) = spender_raw.filter(|raw| home_output_spend_proven(raw, &home.sweep_txid, home.vout, &home.pkh_hex, home.sats)) else {
                return Some((HomeSpendWord::Unproven, None));
            };
            let proven_txid = bsv_rs::transaction::Transaction::from_binary(raw).ok()?.id().to_ascii_lowercase();
            let (latch_txid, vout) = home_latch_target(&home.sweep_txid, home.vout);
            Some((
                HomeSpendWord::Proven,
                Some(crate::hops_view::ProbeMemo {
                    outpoint: crate::hops_view::probe_memo_key(&latch_txid, vout),
                    probed_at_ms: now_ms,
                    spent: true,
                    spending_txid: Some(proven_txid),
                    spent_confirmed: probe.spent_confirmed,
                }),
            ))
        }
    }
}
/// The lens fold's LOW-4: stored-BEEF reads ONE recompute's home walk may make (one per chain-named spender, each
/// a D1 read plus the blob it points at), counted, and all inside the walk's wall-clock budget
/// (`OWED_RECOMPUTE_TIME_BUDGET_MS`). An output past the count or the clock reads `Unproven` this pass and is
/// looked at again on the next; a proven one latches and leaves the queue.
pub const OWED_HOME_STORED_READS_PER_RECOMPUTE: usize = 8;

/// The delta fold's LOW-1 (2026-10-06): the home outputs ONE pass looks at (one memo read holds the cursor, their
/// latches and their chain memos inside D1's bind limit).
pub const OWED_HOME_WINDOW: usize = crate::logic::D1_CHUNK_OUTPOINTS;
const _: () = assert!(1 + 2 * OWED_HOME_WINDOW < crate::logic::D1_MAX_BOUND_PARAMS);
/// The home walk's CURSOR: one `hop_chain_probes` row per identity under this key prefix
/// (`homecursor:<identity>.0`), written by this crate alone. Its `spendingTxid` column carries the memo key
/// (`<sweepTxid>.<vout>`) of the home output the NEXT pass starts at; it is a position in a ring, never a word
/// about a spend (`spent` is always 0, so no reader of the table takes it for a probe or a latch).
pub const HOME_WALK_CURSOR_PREFIX: &str = "homecursor:";

/// PURE: the probe-memo target of an identity's home-walk cursor.
pub fn home_cursor_target(identity_lc: &str) -> (String, u32) {
    (format!("{HOME_WALK_CURSOR_PREFIX}{}", identity_lc.to_ascii_lowercase()), 0)
}

/// PURE: the cursor the memo read returned for `identity_lc` (the memo key of the output to start at), if any.
pub fn home_cursor_of(memos: &[crate::hops_view::ProbeMemo], identity_lc: &str) -> Option<String> {
    let (txid, vout) = home_cursor_target(identity_lc);
    let key = crate::hops_view::probe_memo_key(&txid, vout);
    memos.iter().filter(|m| m.outpoint == key).max_by_key(|m| m.probed_at_ms).and_then(|m| m.spending_txid.clone())
}

/// PURE: the cursor row that makes `next` the start of the identity's next pass.
pub fn home_cursor_memo(identity_lc: &str, next: &CourierHomeOutput, now_ms: i64) -> crate::hops_view::ProbeMemo {
    let (txid, vout) = home_cursor_target(identity_lc);
    crate::hops_view::ProbeMemo {
        outpoint: crate::hops_view::probe_memo_key(&txid, vout),
        probed_at_ms: now_ms,
        spent: false,
        spending_txid: Some(crate::hops_view::probe_memo_key(&next.sweep_txid, next.vout)),
        spent_confirmed: None,
    }
}

/// PURE (the delta fold's LOW-1): THIS PASS'S WINDOW, the courier ladder's shape (a rotating start). `candidates`
/// are `courier_home_outputs`' (sorted by sweep txid and vout): a ring. The pass starts at the first candidate at
/// or after the cursor (a cursor naming an output that left the list still lands on its successor; none or a
/// malformed one starts at the head) and takes at most `window` of them, wrapping. Every cut the walk makes (the
/// window, the chain probes, the stored reads, the resolver asks) is taken in this order, and the cursor only
/// moves past an output the pass withheld nothing from (`home_cursor_after`), so no fixed head can hold a cap
/// against the tail: before, the three cuts shared one sorted order and an unprovable head starved the rest for
/// ever (bsv-low `CLAUDE.md`, the bounded-walk starvation lesson of 2026-09-19).
pub fn home_walk_window(candidates: &[CourierHomeOutput], cursor: Option<&str>, window: usize) -> Vec<CourierHomeOutput> {
    let n = candidates.len();
    let at = cursor.and_then(|c| c.rsplit_once('.')).and_then(|(txid, vout)| Some((txid.to_ascii_lowercase(), vout.parse::<u32>().ok()?)));
    let start = at
        .and_then(|(txid, vout)| candidates.iter().position(|c| (c.sweep_txid.to_ascii_lowercase(), c.vout) >= (txid.clone(), vout)))
        .unwrap_or(0);
    (0..n.min(window)).map(|k| candidates[(start + k) % n].clone()).collect()
}

/// PURE: where the NEXT pass starts. The first output of the window the pass WITHHELD something from (a cap or
/// the clock stood between it and a look it was owed); with nothing withheld, the candidate after the window. A
/// window that held every candidate and withheld nothing leaves the cursor where it is (`None`: no write).
pub fn home_cursor_after<'a>(candidates: &'a [CourierHomeOutput], window: &'a [CourierHomeOutput], first_withheld: Option<usize>) -> Option<&'a CourierHomeOutput> {
    if let Some(home) = first_withheld.and_then(|i| window.get(i)) {
        return Some(home);
    }
    if window.len() >= candidates.len() {
        return None;
    }
    let last = window.last()?;
    let at = candidates.iter().position(|c| c == last)?;
    candidates.get((at + 1) % candidates.len())
}

/// What one pass of the home walk can reach outside this crate's pure code: the clock, the chain rung, the bytes.
/// The route implements it over the Worker; the pins implement it over a table of answers and count every call.
#[allow(async_fn_in_trait)]
pub trait HomeWalkWorld {
    /// Is the recompute past its wall-clock budget?
    fn over_budget(&self) -> bool;
    /// One `/spent-any` ladder for a home output.
    async fn probe(&mut self, txid: &str, vout: u32) -> crate::hops_view::ChainSpendProbe;
    /// The spender's bytes from the index's stored BEEF (one D1 read plus the blob).
    async fn stored_spender(&mut self, spender_txid: &str) -> Option<Vec<u8>>;
    /// The isolate's held tx-any answer for the spender, when it holds one (`Some(None)`: held, no bytes). Free.
    fn cached_spender(&mut self, spender_txid: &str) -> Option<Option<Vec<u8>>>;
    /// One tx-any resolver ask for the spender's bytes.
    async fn resolve_spender(&mut self, spender_txid: &str) -> Option<Vec<u8>>;
}

/// What one pass established and spent.
#[derive(Debug, Default)]
pub struct HomePass {
    /// Per `<sweepTxid>:<vout>`: the latches' and this pass's words.
    pub words: HashMap<String, HomeSpendWord>,
    /// The memos to write: the fresh chain answers, then the new latches.
    pub memos: Vec<crate::hops_view::ProbeMemo>,
    /// The index (into the window) of the first output a cap or the clock withheld a look from.
    pub first_withheld: Option<usize>,
    pub probes: usize,
    pub stored_reads: usize,
    pub resolver_asks: usize,
}

/// ONE PASS of the home walk over its window, in the window's order (the delta fold's LOW-1 and LOW-2: the loop
/// the route ran inline, lifted so its caps EXECUTE under a pin). Per output: (1) a latch is `Proven`, no ask;
/// (2) the chain rung's word, a fresh memo or one of `OWED_HOME_PROBES_PER_RECOMPUTE` ladders; (3) for a named
/// spender its bytes, the index's stored BEEF first (`OWED_HOME_STORED_READS_PER_RECOMPUTE` reads), else the
/// isolate's held answer, else one of `OWED_HOME_PROBES_PER_RECOMPUTE` resolver asks; then `home_word` decides.
/// An output a cap or the clock kept a look from is WITHHELD (unless what it did get proved it): the first such
/// is where the next pass starts. Only `home_word` says `Proven`; a withheld or faulted look names nothing more
/// than it did before (the row stands).
pub async fn home_walk_pass<W: HomeWalkWorld>(
    world: &mut W,
    window: &[CourierHomeOutput],
    latch_memos: &[crate::hops_view::ProbeMemo],
    chain_memos: &[crate::hops_view::ProbeMemo],
    now_ms: i64,
) -> HomePass {
    let (words, targets) = latched_home_words(window, latch_memos);
    let (answered, _) = crate::hops_view::split_probe_targets_with(
        &targets,
        chain_memos,
        now_ms,
        crate::hops_view::PROBE_MEMO_MAX_AGE_MS,
        crate::hops_view::PROBE_MEMO_CONFIRMED_MAX_AGE_MS,
    );
    let mut answered: HashMap<String, crate::hops_view::ChainSpendProbe> = answered.into_iter().map(|(t, v, p)| (outpoint_key(&t, v), p)).collect();
    let mut pass = HomePass { words, ..HomePass::default() };
    let mut latches: Vec<crate::hops_view::ProbeMemo> = Vec::new();
    for (idx, home) in window.iter().enumerate() {
        let key = outpoint_key(&home.sweep_txid, home.vout);
        if pass.words.contains_key(&key) {
            continue;
        }
        let mut withheld = false;
        let probe = match answered.remove(&key) {
            Some(p) => Some(p),
            None if pass.probes < OWED_HOME_PROBES_PER_RECOMPUTE && !world.over_budget() => {
                pass.probes += 1;
                let p = world.probe(&home.sweep_txid, home.vout).await;
                pass.memos.extend(crate::hops_view::probe_memo_of(&home.sweep_txid, home.vout, &p, now_ms));
                Some(p)
            }
            None => {
                withheld = true;
                None
            }
        };
        if let Some(probe) = probe {
            let mut raw: Option<Vec<u8>> = None;
            if let (true, Some(true), Some(sp)) = (probe.known, probe.spent, probe.spending_txid.as_deref().map(str::to_ascii_lowercase)) {
                if pass.stored_reads < OWED_HOME_STORED_READS_PER_RECOMPUTE && !world.over_budget() {
                    pass.stored_reads += 1;
                    raw = world.stored_spender(&sp).await;
                } else {
                    withheld = true;
                }
                if raw.is_none() {
                    raw = match world.cached_spender(&sp) {
                        Some(held) => held,
                        None if pass.resolver_asks < OWED_HOME_PROBES_PER_RECOMPUTE && !world.over_budget() => {
                            pass.resolver_asks += 1;
                            world.resolve_spender(&sp).await
                        }
                        None => {
                            withheld = true;
                            None
                        }
                    };
                }
            }
            if let Some((word, latch)) = home_word(&probe, raw.as_deref(), home, now_ms) {
                if word == HomeSpendWord::Proven {
                    withheld = false;
                }
                latches.extend(latch);
                pass.words.insert(key, word);
            }
        }
        if withheld && pass.first_withheld.is_none() {
            pass.first_withheld = Some(idx);
        }
    }
    pass.memos.extend(latches);
    pass
}

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
    /// The sweep's outputs to the seat's home (vout, sats), when its bytes were read and the home is known.
    pub home_outputs: Vec<(u32, u64)>,
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
pub fn sole_home_vout<'a>(outputs: impl Iterator<Item = (u32, Option<&'a str>)>, pkh_hex: &str) -> Option<u32> {
    let mut mine = outputs.filter(|(_, p)| p.is_some_and(|p| p.eq_ignore_ascii_case(pkh_hex))).map(|(v, _)| v);
    let first = mine.next()?;
    mine.next().is_none().then_some(first)
}

/// PURE: [`sole_home_vout`] over a RAW transaction, which must hash to `txid` (the bytes are content-addressed:
/// whoever handed them over, an output layout read from bytes that hash to the paying txid is that transaction's).
pub fn sole_home_vout_of_raw(raw: &[u8], txid: &str, pkh_hex: &str) -> Option<u32> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    if !tx.id().eq_ignore_ascii_case(txid) {
        return None;
    }
    let pkhs: Vec<Option<String>> = tx.outputs.iter().map(|o| p2pkh_pkh_hex(&o.locking_script.to_binary())).collect();
    sole_home_vout(pkhs.iter().enumerate().map(|(v, p)| (v as u32, p.as_deref())), pkh_hex)
}

/// The served key of one (paying txid, home pkh) pair in the route's `payVout` map, lowercase.
pub fn pay_vout_key(pay_txid: &str, pkh_hex: &str) -> String {
    format!("{}:{}", pay_txid.to_ascii_lowercase(), pkh_hex.to_ascii_lowercase())
}

/// PURE (the B3 lens fold M2): the (paying txid, home pkh) of every derived payout row whose `payVout` the
/// derivation could not name from what it held (a pot's payout: the index stores the settle's sums per home,
/// never its output order, and the order depends on whether the spend carries a rake output). The route reads
/// those transactions' stored bytes and [`mark_pay_vouts`] writes the answer. Sorted, deduplicated.
pub fn pay_vout_wanted(rows: &[OwedRow]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = rows
        .iter()
        .filter(|r| r.family == OwedFamily::Payout && r.facts.get("payVout").is_some_and(Value::is_null))
        .filter_map(|r| Some((r.facts.get("payTxid")?.as_str()?.to_ascii_lowercase(), r.facts.get("payPkh")?.as_str()?.to_ascii_lowercase())))
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
    for r in rows.iter_mut().filter(|r| r.family == OwedFamily::Payout && r.facts.get("payVout").is_some_and(Value::is_null)) {
        let key = match (r.facts.get("payTxid").and_then(Value::as_str), r.facts.get("payPkh").and_then(Value::as_str)) {
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

/// The `payVout` read's OWN wall clock, counted from the step's start, never the recompute's (the B3 delta lens
/// D-L1): the step runs last, so behind the recompute's clock a heavy identity read `null` on every pass. The
/// first chunk is read whatever any clock says; a later chunk is read while the step is inside this budget.
pub const OWED_PAY_VOUT_READ_BUDGET_MS: i64 = 2_000;

/// What the `payVout` read reaches outside this crate's pure code: its own clock, the isolate's memo, the index's
/// stored bytes. The route implements it over the Worker; the pins over a table, counting every read.
#[allow(async_fn_in_trait)]
pub trait PayVoutWorld {
    /// Is the step past [`OWED_PAY_VOUT_READ_BUDGET_MS`] of its own start?
    fn over_own_budget(&self) -> bool;
    /// The isolate's held answer for one [`pay_vout_key`]. Free.
    fn memo_get(&self, key: &str) -> Option<u32>;
    fn memo_put(&mut self, key: &str, vout: u32);
    /// One indexed read: the raw transaction of each of `txids` the index stores, as (txid lowercase, raw).
    /// `Err` is a read fault (the chunk's rows serve `payVout: null` this pass).
    async fn stored_raws(&mut self, txids: &[&str]) -> Result<Vec<(String, Vec<u8>)>, String>;
}

/// What one `payVout` pass answered.
#[derive(Debug, Default)]
pub struct PayVoutPass {
    /// [`pay_vout_key`] to the sole home output, for [`mark_pay_vouts`].
    pub vouts: HashMap<String, u32>,
    /// A chunk was left unread at the step's own clock.
    pub cut: bool,
    pub reads: usize,
}

/// One pass of the `payVout` read over [`pay_vout_wanted`]'s pairs (the B3 delta lens D-L1). The memo answers
/// first; what it does not hold is read in chunks of `chunk`. The FIRST chunk read is never gated on a clock, and
/// the recompute's clock gates none: a `null` for want of time is a one-pass event, not a steady state. When the
/// step's own clock cuts a later chunk, the next pass does not start at the same head: an answer is memoised (a
/// warm isolate asks only for what is left), and the start chunk turns with `turn` (the route passes the
/// recompute's clock), so a cold isolate, or a head of rows the bytes never answer, cannot starve the tail.
pub async fn pay_vout_pass(world: &mut impl PayVoutWorld, wanted: &[(String, String)], chunk: usize, turn: u64) -> PayVoutPass {
    let mut pass = PayVoutPass::default();
    let mut to_read: Vec<&(String, String)> = Vec::new();
    for w in wanted {
        let key = pay_vout_key(&w.0, &w.1);
        match world.memo_get(&key) {
            Some(vout) => {
                pass.vouts.insert(key, vout);
            }
            None => to_read.push(w),
        }
    }
    let chunks: Vec<&[&(String, String)]> = to_read.chunks(chunk.max(1)).collect();
    for i in 0..chunks.len() {
        if i > 0 && world.over_own_budget() {
            pass.cut = true;
            break;
        }
        let this = chunks[(turn as usize % chunks.len() + i) % chunks.len()];
        let txids: Vec<&str> = this.iter().map(|(t, _)| t.as_str()).collect();
        pass.reads += 1;
        match world.stored_raws(&txids).await {
            Ok(raws) => {
                for (txid, raw) in raws {
                    for (_, pkh) in this.iter().filter(|(t, _)| *t == txid) {
                        if let Some(vout) = sole_home_vout_of_raw(&raw, &txid, pkh) {
                            let key = pay_vout_key(&txid, pkh);
                            world.memo_put(&key, vout);
                            pass.vouts.insert(key, vout);
                        }
                    }
                }
            }
            Err(_) => note_pay_vout_read_fault(),
        }
    }
    pass
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

/// The recompute's read of this identity's `collected_markers_v2` rows over one chunk of games (`marks` is the
/// chunk's `?, ?, ...`): the v1 markers and, since bsv-low #492, the held filings beside them (`txid` is the row's
/// own key, `payTxid` the paying transaction a held filing names; NULL on every other row). Binds: the identity,
/// then the game ids.
pub fn collected_rows_sql(marks: &str) -> String {
    format!("SELECT gameId, sigHex, txid, payTxid FROM collected_markers_v2 WHERE identity = ? AND gameId IN ({marks})")
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
    /// Fold one row (`gameId`, `sigHex`, `txid`, `payTxid` as [`collected_rows_sql`] reads them) of `identity_lc`.
    ///
    /// A row that names a paying transaction is a HELD FILING and nothing else. It retires that transaction's
    /// payout by STRING MATCH, on one condition, THE GATE: the row's own key is the content key the filing door
    /// derives from (game, identity, that txid) (`record_post::is_held_row_key`). The door is the only writer of
    /// such a key and of the column, and it writes both in one INSERT after it verified the identity's signature
    /// over the same three fields; so the signature is checked ONCE, at the door, and never again here. The gate
    /// keeps out every row the door did not write that way: a chain row (keyed by an outpoint; a stranger plants
    /// those free under lenient submit) and a row whose column and key disagree retire nothing, whatever they
    /// carry. No signature is spent on a held filing (it is not tried as the v1 marker either).
    pub fn row(&mut self, identity_lc: &str, game_id: &str, sig_hex: Option<&str>, txid: Option<&str>, pay_txid: Option<&str>) {
        let g = game_id.to_ascii_lowercase();
        self.present.insert(g.clone());
        if let Some(pay) = pay_txid {
            if txid.is_some_and(|key| crate::record_post::is_held_row_key(key, &g, identity_lc, pay)) {
                self.held.insert(held_key(&g, pay));
            } else {
                self.held_key_mismatches += 1;
            }
            return;
        }
        if sig_hex.is_some_and(|sig| crate::record_post::collected_sig_verifies(identity_lc, &g, sig)) {
            self.verified.insert(g);
        }
    }
}

/// The key of [`OwedInputs::held_verified`]: `<gameId>:<payTxid>`, lowercase.
pub fn held_key(game_id: &str, pay_txid: &str) -> String {
    format!("{}:{}", game_id.to_ascii_lowercase(), pay_txid.to_ascii_lowercase())
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
    let home_outputs_of = |spender: &str| -> Vec<&SpenderOutput> {
        let (Some(outs), Some(pkh)) = (i.spender_outputs.get(spender), my_pkh.as_deref()) else {
            return Vec::new();
        };
        outs.iter().filter(|o| o.pkh_hex.as_deref().is_some_and(|p| p.eq_ignore_ascii_case(pkh))).collect()
    };
    // bsv-low #485: a home output is SPENT by the index's own word, or by the home key's signature this crate
    // verified over its spender's bytes (`HomeSpendWord::Proven`); never by a courier's word of spent
    let home_proven = |spender: &str, o: &SpenderOutput| i.home_spends.get(&outpoint_key(spender, o.vout)) == Some(&HomeSpendWord::Proven);
    let home_spent = |spender: &str| -> bool {
        let mine = home_outputs_of(spender);
        !mine.is_empty() && mine.iter().all(|o| o.spent == Some(true) || home_proven(spender, o))
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
                home_outputs: home_outputs_of(&filed.sweep_txid).iter().map(|o| (o.vout, o.sats)).collect(),
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
        raw_hex: None,
        pays_sats: Some(sum),
        confirmed,
        index_proven: false,
        index_proof_height: None,
        source,
        output_spent: mine.iter().all(|o| o.spent == Some(true) || home_proven(&spender, o)),
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
        let Some(swept) = swept_home(i, h) else { continue };
        if swept.source != "courier-bytes" || !swept.confirmed || swept.output_spent {
            continue;
        }
        if i.held_verified.contains(&held_key(&game, &swept.sweep_txid)) {
            continue; // bsv-low #492: retired by the identity's held filing, nothing to walk
        }
        let Some(pkh) = i.my_pkh_by_game.get(&game) else { continue };
        for (vout, sats) in &swept.home_outputs {
            out.push(CourierHomeOutput { sweep_txid: swept.sweep_txid.to_ascii_lowercase(), vout: *vout, pkh_hex: pkh.to_ascii_lowercase(), sats: *sats });
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
                    // bsv-low #492 / #512: the identity's wallet ALREADY HOLDS this payout, by its own verified held
                    // filing naming the paying transaction (the live credit landed it and the closing page dropped
                    // the v1 filing; 118 presses of the two collect passes said "Collected" over such rows). Not a
                    // row: there is nothing to collect and so nothing to press. It names the TRANSACTION, so the
                    // candidate count (N10) does not matter, and a pot whose spend becomes another transaction is
                    // served again.
                    if e.settle_txid.as_deref().is_some_and(|t| i.held_verified.contains(&held_key(&game, t))) {
                        continue;
                    }
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
                    // bsv-low #492 / #512: WHAT a device asks its own wallet about before it offers the press (does
                    // the ledger already hold this transaction?) and what its held filing names when it does
                    facts["payTxid"] = json!(e.settle_txid.as_ref().map(|t| t.to_ascii_lowercase()));
                    facts["heldFiling"] = json!(HELD_FILING_TAG);
                    // the B3 lens fold M2: the OUTPUT the device must own before it files (`payTxid`:`payVout`, the
                    // spend's output to my committed home `payPkh`). The spend's output order is not in the index
                    // (a rake output may lead it), so the vout is `null` here and the route names it from the
                    // spend's stored bytes (`pay_vout_wanted`, `mark_pay_vouts`); `null` stays when it could not.
                    facts["payPkh"] = json!(my_committed_pay_pkh(e));
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
            // bsv-low #492 / #512: the wallet already holds this sweep's payout (the identity's held filing names
            // the sweep): not a row, whichever proof named the sweep (a courier-proven one included)
            if i.held_verified.contains(&held_key(&game, &swept.sweep_txid)) {
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
                // bsv-low #485: the row retires by a `collected` filing, or once every home output's spend is PROVEN
                // (`swept_home`'s `output_spent`: the home key's signature, verified here). Until then it says what the
                // chain rung's look at the home outputs found, when it looked: `unspent` (a corroborated absence: the
                // sats were seen at the home) or `unproven` (a spender is named, its bytes do not carry the proof yet).
                facts["creditKind"] = json!("courier-bytes");
                let words: Vec<Option<&HomeSpendWord>> = swept.home_outputs.iter().map(|(vout, _)| i.home_spends.get(&outpoint_key(&swept.sweep_txid, *vout))).collect();
                if words.iter().any(|w| matches!(w, Some(HomeSpendWord::Unproven))) {
                    facts["homeSpend"] = json!("unproven");
                } else if !words.is_empty() && words.iter().all(|w| matches!(w, Some(HomeSpendWord::Unspent | HomeSpendWord::Proven))) {
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
            // bsv-low #492 / #512: the paying transaction a device asks its wallet about and names in a held filing
            facts["payTxid"] = json!(swept.sweep_txid.to_ascii_lowercase());
            facts["heldFiling"] = json!(HELD_FILING_TAG);
            // the B3 lens fold M2: the sweep's output to my home, the OUTPUT the device must own before it files:
            // the home outputs the sweep's bytes showed, else the filed sweep's own raw (it hashes to `sweepTxid`),
            // and only when exactly one output pays the home (`null` otherwise: never a pick)
            let pay_pkh = i.my_pkh_by_game.get(&game).map(|p| p.to_ascii_lowercase());
            let pay_vout = match swept.home_outputs.as_slice() {
                [(vout, _)] => Some(*vout),
                [] => match (swept.raw_hex.as_deref().and_then(|h| hex::decode(h).ok()), pay_pkh.as_deref()) {
                    (Some(raw), Some(pkh)) => sole_home_vout_of_raw(&raw, &swept.sweep_txid, pkh),
                    _ => None,
                },
                _ => None,
            };
            facts["payPkh"] = json!(pay_pkh);
            facts["payVout"] = json!(pay_vout);
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
                // THE CHAIN RUNG: the index's "unspent" alone never offers the sweep (the client's bar since #451: a
                // read that could not look is never "unspent"). A chain word of unspent → the claim; spent by a pot →
                // no row (the JOIN took it); spent by something else → could not judge; no word yet → the claim waits.
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
                            reason: if evicted {
                                Some(JOIN_REFUSED_REASON.to_string())
                            } else {
                                door_refused.map(|door| door.sentence().to_string())
                            },
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

// bsv-low #487: THE WRITE IS MONOTONIC IN `computedAtMs` (the walk's own start stamp). The recompute lock is
// isolate-local and leased (`routes::OWED_IN_FLIGHT_STALE_MS`): a walk silent past its lease is taken over while
// still live, and a walk on ANOTHER isolate is never seen at all, so two walks of one identity can finish in either
// order. Each statement of the batch carries the same guard against the marker as it stood BEFORE the batch (the
// marker's own upsert runs last): a snapshot older than the one the marker carries deletes nothing, inserts nothing
// and stamps nothing, and the closing read-back tells the walk (`owed_write_landed`).
// Why the guard and not a token fence at the final write: the token lives in one isolate's memory, so a fence can
// only refuse the takeover's twin on that isolate, and it is checked BEFORE the batch's await; the guard is decided
// by D1 inside the batch, for every writer. The clocks are the isolates' own (`Date.now()`): a writer whose clock
// runs behind by the skew is refused for that long and leaves any stale mark standing, so the next read recomputes.
pub const OWED_ROWS_DELETE_SQL: &str =
    "DELETE FROM owed_rows WHERE identity = ?1 AND NOT EXISTS (SELECT 1 FROM owed_state WHERE identity = ?1 AND computedAtMs > ?2)";
/// `?9` is the row's `updatedAtMs`, which IS the walk's stamp (`owed_write_plan`): the guard reads it.
pub const OWED_ROW_INSERT_SQL: &str = "INSERT INTO owed_rows (identity, outpoint, family, gameId, sats, opponentIdentity, atHeight, facts, updatedAtMs, reason) SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10 WHERE NOT EXISTS (SELECT 1 FROM owed_state WHERE identity = ?1 AND computedAtMs > ?9)";
/// A refused stamp leaves the marker whole: its tip, its row count and its `stale` mark (a filing that marked the
/// newer snapshot stale is still owed its recompute).
pub const OWED_STATE_UPSERT_SQL: &str = "INSERT INTO owed_state (identity, computedAtMs, tip, rows, stale, truncated) VALUES (?1, ?2, ?3, ?4, 0, ?5) ON CONFLICT(identity) DO UPDATE SET computedAtMs = excluded.computedAtMs, tip = excluded.tip, rows = excluded.rows, stale = 0, truncated = excluded.truncated WHERE excluded.computedAtMs >= owed_state.computedAtMs";
/// One bind of the owed write, host-typed so the route (D1) and the real-SQLite pin run the SAME statements with the
/// SAME values (`owed_write_plan`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwedBind {
    Text(String),
    Int(i64),
    Null,
}
/// The read-back that closes the write batch: the stamp the identity's marker carries once the batch ran.
pub const OWED_STATE_STAMP_SQL: &str = "SELECT computedAtMs FROM owed_state WHERE identity = ?1";

/// PURE: the ONE batch that writes a snapshot (the identity's rows replaced, the marker stamped, the marker's stamp
/// read back), in order, all or nothing. The route binds it to D1; the pin runs it on real SQLite.
pub fn owed_write_plan(identity_lc: &str, rows: &[OwedRow], computed_at_ms: i64, tip: Option<u64>, truncated: bool) -> Vec<(&'static str, Vec<OwedBind>)> {
    let text = |s: &str| OwedBind::Text(s.to_string());
    let opt_text = |s: Option<&str>| s.map_or(OwedBind::Null, |s| OwedBind::Text(s.to_string()));
    let opt_int = |v: Option<u64>| v.map_or(OwedBind::Null, |v| OwedBind::Int(v as i64));
    let mut plan: Vec<(&'static str, Vec<OwedBind>)> = Vec::with_capacity(rows.len() + 3);
    plan.push((OWED_ROWS_DELETE_SQL, vec![text(identity_lc), OwedBind::Int(computed_at_ms)]));
    for r in rows {
        plan.push((
            OWED_ROW_INSERT_SQL,
            vec![
                text(&r.identity),
                text(&r.outpoint),
                text(r.family.as_str()),
                text(&r.game_id),
                opt_int(r.sats),
                opt_text(r.opponent_identity.as_deref()),
                opt_int(r.at_height),
                text(&r.facts.to_string()),
                OwedBind::Int(computed_at_ms),
                opt_text(r.reason.as_deref()),
            ],
        ));
    }
    plan.push((
        OWED_STATE_UPSERT_SQL,
        vec![text(identity_lc), OwedBind::Int(computed_at_ms), opt_int(tip), OwedBind::Int(rows.len() as i64), OwedBind::Int(i64::from(truncated))],
    ));
    plan.push((OWED_STATE_STAMP_SQL, vec![text(identity_lc)]));
    plan
}
/// PURE: did this walk's snapshot land? (the marker carries its own stamp after the batch)
pub fn owed_write_landed(stamp_after: Option<i64>, computed_at_ms: i64) -> bool {
    stamp_after == Some(computed_at_ms)
}
/// What a READ answers once its own compute returned (the lens fold's LOW-2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadAnswer {
    /// The walk's own rows: its snapshot is the one the table holds.
    Own,
    /// The rows the table holds: the write refused this walk's snapshot because a NEWER one stands (#487), and the
    /// reader is owed the newer rows, never the older ones the guard just declined to store.
    Stored,
}
/// PURE: which rows a read serves after its compute.
pub fn read_answer_after_compute(superseded: bool) -> ReadAnswer {
    if superseded {
        ReadAnswer::Stored
    } else {
        ReadAnswer::Own
    }
}

/// The delta fold's LOW-4 (2026-10-06): the word on a row served from a SUPERSEDED walk's own snapshot.
pub const SUPERSEDED_FALLBACK_REASON: &str = "this list is being refreshed and a newer one could not be read just now: nothing can be pressed from this copy; it is read again in a moment";
/// PURE (the delta fold's LOW-4): the rows a superseded read serves when the stored read ALSO faulted (two faults
/// in one request: a lost race and a failed D1 read). They are the older snapshot the monotonic guard just
/// refused, so a claimable word in them may be one the newer walk retired (a `hop-stranded` sweep press on a hop
/// it saw spent). For that one response NO row carries a claimable word: every `claimable: true` is cleared to
/// `false` with `claimReason` saying why and `supersededFallback: true` (additive); a row that was not claimable
/// keeps its own reason. Nothing is stored; the next read takes the table's rows.
pub fn superseded_fallback_rows(mut rows: Vec<OwedRow>) -> Vec<OwedRow> {
    for r in rows.iter_mut() {
        if r.facts.get("claimable") == Some(&Value::Bool(true)) {
            r.facts["claimable"] = json!(false);
            r.facts["claimReason"] = json!(SUPERSEDED_FALLBACK_REASON);
            r.facts["supersededFallback"] = json!(true);
        }
    }
    rows
}
/// One served snapshot: the rows, the tip, the walk's stamp, the cut bit.
pub type OwedSnapshot = (Vec<OwedRow>, Option<u64>, i64, bool);
/// PURE: what a SUPERSEDED read answers with: the table's snapshot (the newer walk's) when the stored read
/// returned one, else its own with every claimable word cleared (`superseded_fallback_rows`).
pub fn superseded_read_snapshot(stored: Option<OwedSnapshot>, own: OwedSnapshot) -> OwedSnapshot {
    stored.unwrap_or_else(|| (superseded_fallback_rows(own.0), own.1, own.2, own.3))
}

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
/// a hop crosses its window, a counterparty files). bsv-low #484 (delta fold, D-M1): so is a payout whose chain
/// word was a "confirmed" taken inside the reorg grace (`hops_view::mark_reorg_grace_rows`): asked again in five minutes.
pub fn row_is_open(r: &OwedRow) -> bool {
    r.family != OwedFamily::Payout || r.facts["claimable"] == Value::Bool(false) || r.facts[crate::hops_view::OWED_FACT_REORG_GRACE_WORD] == Value::Bool(true)
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
static HELD_FILINGS_KEY_MISMATCH: AtomicU64 = AtomicU64::new(0);
static HELD_FILINGS_MATCHED: AtomicU64 = AtomicU64::new(0);
/// bsv-low #492: one recompute's held filings: how many (game, txid) names the read matched, and how many rows
/// carried a `payTxid` their own key was not filed for (these retire nothing; nothing writes one).
pub fn note_held_filings(matched: usize, key_mismatches: usize) {
    HELD_FILINGS_MATCHED.fetch_add(matched as u64, Ordering::Relaxed);
    HELD_FILINGS_KEY_MISMATCH.fetch_add(key_mismatches as u64, Ordering::Relaxed);
}
static PAY_VOUT_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
/// bsv-low #512: a chunk of the `payVout` read faulted (its rows serve `payVout: null` that pass).
pub fn note_pay_vout_read_fault() {
    PAY_VOUT_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
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
/// The merged lens's LOW-3 (2026-10-06): a probe memo read faulted (`routes::read_probe_memos`, all or nothing: the
/// answer is empty). It was a `console_warn` only, and it is not free: the hop walk gets no memo and no reorg
/// tombstone for that pass (every candidate is "never probed", and a fresh "confirmed" taken inside a real grace
/// is not marked `chainWordInReorgGrace`), the home walk gets no latch (every retired courier-proven row is back
/// for the pass) and no cursor (the ring is walked from its head). Always the safe direction, now counted: here
/// for the isolate (`/health.owed.probeMemoReadFaults`) and in the overlay's `ops_counters` under
/// [`COUNTER_PROBE_MEMO_READ_FAULTS`], served on the overlay's `/health/invariants.counters`.
static PROBE_MEMO_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
pub fn note_probe_memo_read_fault() {
    PROBE_MEMO_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
}
/// The durable row of [`note_probe_memo_read_fault`] (the overlay seeds it at 0:
/// `bsv_overlay_cloudflare::hop_probe_memos::COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS`, pinned equal).
pub const COUNTER_PROBE_MEMO_READ_FAULTS: &str = "applayer_probe_memo_read_faults_total";
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
/// bsv-low #487: walks whose snapshot the write REFUSED because the marker already carried a newer one (a walk that
/// outlived its takeover, or the slower of two isolates).
static RECOMPUTE_WRITES_SUPERSEDED: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_write_superseded() {
    RECOMPUTE_WRITES_SUPERSEDED.fetch_add(1, Ordering::Relaxed);
}

/// bsv-low #499: the recomputes ONE identity may run on ONE isolate inside one window (the brain's per-identity
/// recompute, bounded). Past it a recompute is SHED: the hook's or the read's ask runs nothing, the reader is served
/// the snapshot `owed_rows` holds, and the shed is counted (`recomputeShed` on `/health`). Nothing new is persisted:
/// a hook marks the identity stale BEFORE it asks, and a shed walk never clears that mark, so the first read after
/// the window recomputes (the read's own staleness rule). What a shed costs, stated: a change that lands while its
/// identity is over the ceiling reaches the page at that identity's next read past the window, not by a push.
///
/// Why 12: one hand drives at most a handful per seat (the JOIN's `pot-changed` and `hop-changed`, the settle's
/// `pot-changed`, the seat's filings, a block's tip), the in-flight lock already folds twins, and a claim reruns
/// at most `OWED_RERUNS_PER_CLAIM` more; 12 a minute is a recompute every 5 s held for a minute, which no honest
/// table reaches and a hook storm (the 2026-09-01 callback flood, the loop-11 t=0 herd) does.
pub const OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW: u32 = 12;
/// The window of [`OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW`]: one minute, fixed from the first recompute in it.
pub const OWED_RECOMPUTE_RATE_WINDOW_MS: i64 = 60_000;
/// Identities tracked before the expired windows are pruned (the map is per isolate and bounded by its traffic).
const OWED_RECOMPUTE_RATE_PRUNE_AT: usize = 1024;

/// PURE (pinned): the per-identity recompute counter of one isolate.
#[derive(Debug, Default)]
pub struct RecomputeRate {
    /// identity (lowercase) -> (window start ms, recomputes run in it)
    windows: HashMap<String, (i64, u32)>,
}

impl RecomputeRate {
    /// May `identity_lc` recompute now? `true` counts the run. `sheddable = false` is the FIRST read of an identity
    /// (no snapshot exists to serve): it always runs, and it counts. `false` is a shed (the caller serves the
    /// snapshot and counts it with [`note_recompute_shed`]).
    pub fn admit(&mut self, identity_lc: &str, now_ms: i64, sheddable: bool) -> bool {
        if self.windows.len() >= OWED_RECOMPUTE_RATE_PRUNE_AT {
            self.windows.retain(|_, (start, _)| now_ms.saturating_sub(*start) < OWED_RECOMPUTE_RATE_WINDOW_MS);
        }
        let w = self.windows.entry(identity_lc.to_string()).or_insert((now_ms, 0));
        if now_ms.saturating_sub(w.0) >= OWED_RECOMPUTE_RATE_WINDOW_MS {
            *w = (now_ms, 0);
        }
        if sheddable && w.1 >= OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW {
            return false;
        }
        w.1 = w.1.saturating_add(1);
        true
    }
    /// The recomputes `identity_lc` has run in its current window (0 past it).
    pub fn in_window(&self, identity_lc: &str, now_ms: i64) -> u32 {
        match self.windows.get(identity_lc) {
            Some((start, n)) if now_ms.saturating_sub(*start) < OWED_RECOMPUTE_RATE_WINDOW_MS => *n,
            _ => 0,
        }
    }
    /// Identities at the ceiling right now.
    pub fn at_ceiling(&self, now_ms: i64) -> usize {
        self.windows
            .values()
            .filter(|(start, n)| now_ms.saturating_sub(*start) < OWED_RECOMPUTE_RATE_WINDOW_MS && *n >= OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW)
            .count()
    }
}

/// bsv-low #499: recomputes shed by the per-identity ceiling (the snapshot served instead).
static RECOMPUTE_SHED: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_shed() {
    RECOMPUTE_SHED.fetch_add(1, Ordering::Relaxed);
}
pub fn recompute_shed_total() -> u64 {
    RECOMPUTE_SHED.load(Ordering::Relaxed)
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
        "recomputeWritesSuperseded": RECOMPUTE_WRITES_SUPERSEDED.load(Ordering::Relaxed),
        "recomputeShed": RECOMPUTE_SHED.load(Ordering::Relaxed),
        "recomputesPerIdentityPerMinute": OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW,
        "collectedReadFaults": COLLECTED_READ_FAULTS.load(Ordering::Relaxed),
        "heldFilingsMatched": HELD_FILINGS_MATCHED.load(Ordering::Relaxed),
        "heldFilingsKeyMismatch": HELD_FILINGS_KEY_MISMATCH.load(Ordering::Relaxed),
        "payVoutReadFaults": PAY_VOUT_READ_FAULTS.load(Ordering::Relaxed),
        "potSpendersReadFaults": POT_SPENDERS_READ_FAULTS.load(Ordering::Relaxed),
        "hopSweepsReadFaults": HOP_SWEEPS_READ_FAULTS.load(Ordering::Relaxed),
        "sweepProofsReadFaults": SWEEP_PROOFS_READ_FAULTS.load(Ordering::Relaxed),
        "probeMemoReadFaults": PROBE_MEMO_READ_FAULTS.load(Ordering::Relaxed),
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
        }
    }
    static NO_SPENDERS: std::sync::LazyLock<HashMap<String, Vec<SpenderOutput>>> = std::sync::LazyLock::new(HashMap::new);
    static NO_DOOR: std::sync::LazyLock<HashMap<String, DoorRefusal>> = std::sync::LazyLock::new(HashMap::new);
    static NO_INPUTS: std::sync::LazyLock<HashMap<String, Vec<(String, u32)>>> = std::sync::LazyLock::new(HashMap::new);
    static NO_PKHS: std::sync::LazyLock<HashMap<String, String>> = std::sync::LazyLock::new(HashMap::new);
    fn pays(spender: &str, outs: &[(u32, &str, u64, Option<bool>)]) -> HashMap<String, Vec<SpenderOutput>> {
        let mut m = HashMap::new();
        m.insert(spender.to_string(), outs.iter().map(|(v, pkh, sats, spent)| SpenderOutput { vout: *v, pkh_hex: Some((*pkh).to_string()), sats: *sats, spent: *spent, pot_lock: false }).collect());
        m
    }
    static NO_CHAIN: std::sync::LazyLock<HashMap<String, HopChainWord>> = std::sync::LazyLock::new(HashMap::new);
    static NO_HOME_SPENDS: std::sync::LazyLock<HashMap<String, HomeSpendWord>> = std::sync::LazyLock::new(HashMap::new);
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

    /// bsv-low #492 / #512 (the collect pass's class: "collected N but the wallet read X to X"): a payout whose
    /// paying transaction the identity's wallet ALREADY HOLDS, said by its own verified held filing, is not a row,
    /// so there is nothing to press. The filing names the transaction: another transaction's payout stays.
    #[test]
    fn i492_a_payout_the_wallet_already_holds_is_not_served_and_the_filing_names_one_transaction_only() {
        let won = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        // before any filing: the row is served claimable, and it names what a device asks its wallet about
        let rows = derive_owed_rows(&inputs(&won, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["payTxid"], tx(0x03));
        assert_eq!(rows[0].facts["heldFiling"], HELD_FILING_TAG);
        // the wallet holds the settle (the live credit landed it): not a row
        let held: HashSet<String> = [held_key(&tx(0x01), &tx(0x03))].into_iter().collect();
        let mut i = inputs(&won, &[], &[], &v, &c, &p, Some(900_200));
        i.held_verified = &held;
        assert!(derive_owed_rows(&i).is_empty(), "held: nothing to collect, nothing to press");
        // a filing naming ANOTHER transaction (a spend of the pot that was replaced, another game's) retires nothing
        for other in [held_key(&tx(0x01), &tx(0x04)), held_key(&tx(0x09), &tx(0x03))] {
            let held: HashSet<String> = [other].into_iter().collect();
            let mut i = inputs(&won, &[], &[], &v, &c, &p, Some(900_200));
            i.held_verified = &held;
            let rows = derive_owed_rows(&i);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].facts["claimable"], true);
        }
        // a decided loss stays no row, an unspent pot is untouched by a filing naming a txid it does not have
        let open = [entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A))];
        let mut i = inputs(&open, &[], &[], &v, &c, &p, Some(899_000));
        i.held_verified = &held;
        assert_eq!(derive_owed_rows(&i).len(), 1);
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
        let paid = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let v1: HashSet<String> = [tx(0x01)].into_iter().collect();
        let rows_with = |held: &HashSet<String>| {
            let mut i = inputs(&paid, &[], &hops, &v, &v1, &no_pots, Some(900_200));
            i.hop_sweeps = &sweeps;
            i.held_verified = held;
            derive_owed_rows(&i)
        };
        assert_eq!(rows_with(&HashSet::new()).len(), 2, "the v1 marker alone cannot say which");
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
        assert!(derive_owed_rows(&i).is_empty());
    }

    /// The production schema on real SQLite (every overlay migration, the re-run "duplicate column" ignored).
    fn production_sqlite() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(e.to_string().to_ascii_lowercase().contains("duplicate column"), "production migration failed under real SQLite: {e}");
            }
        }
        conn
    }

    /// The recompute's read (`collected_rows_sql`, one game per call here) and fold of `me`'s rows, as the route
    /// runs them: the statement, the four columns, `CollectedFold::row`.
    fn fold_collected(conn: &rusqlite::Connection, me: &str, games: &[String]) -> CollectedFold {
        let mut fold = CollectedFold::default();
        let mut stmt = conn.prepare(&collected_rows_sql("?")).unwrap();
        for game in games {
            let rows = stmt
                .query_map(rusqlite::params![me, game], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, Option<String>>(2)?, r.get::<_, Option<String>>(3)?))
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
        use crate::record_post::{cap_queries, cap_refusal_for, collected_protocol, held_challenge, insert_wrote_a_row, verify_record_post, RecordKind, VerifiedRecord, COLLECTED_FILE_SQL, HELD_TAG};
        let (me, game) = (hex::encode(id), hex::encode(gid));
        let sig = sign(signer, collected_protocol(), &game, &held_challenge(&game, &me, &hex::encode(pay)));
        let verified = verify_record_post(RecordKind::Collected, &script(&[HELD_TAG, gid, id, pay, &sig]), &me)?;
        let VerifiedRecord::Held(r, pay_txid) = &verified else { panic!("a held filing") };
        let (rows_sql, binds, _, _) = cap_queries(&verified);
        let others: i64 = conn.query_row(rows_sql, rusqlite::params![binds[0], binds[1], binds[2]], |row| row.get(0)).unwrap();
        if let Some(refusal) = cap_refusal_for(&verified, others, 0) {
            return Err(refusal);
        }
        let changes = conn.execute(COLLECTED_FILE_SQL, rusqlite::params![r.identity, r.game_id, r.txid, 0i64, r.sig_hex, created_at, pay_txid]).unwrap();
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
                let mut e = entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A));
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
            derive_owed_rows(&i)
        };
        assert_eq!(served().len(), N, "nothing filed: every payout is served");
        for k in 0..N {
            let (_, written) = file_held(&conn, &w, &id, &b32(0xa0, k), &b32(0xc0, k), 1_000 + k as i64).unwrap();
            assert!(written);
        }
        assert_eq!(fold_collected(&conn, &me, &games).held.len(), N, "every filing is read as held: no cut at 256");
        // every recompute, not only the first: three passes, each from the table
        for pass in 0..3 {
            let rows = served();
            assert!(rows.is_empty(), "pass {pass}: {} of {N} held payouts are still served", rows.len());
        }
        // and the retire is exact at this scale too: a game whose spend is replaced is served again, alone
        let mut replaced = paid.clone();
        replaced[N - 1].settle_txid = Some(tx(0xee));
        let fold = fold_collected(&conn, &me, &games);
        let mut i = inputs(&replaced, &[], &[], &v, &fold.verified, &no_pots, Some(900_200));
        i.identity_lc = &me;
        i.held_verified = &fold.held;
        let rows = derive_owed_rows(&i);
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
        let won = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let served = || -> (Vec<OwedRow>, CollectedFold) {
            let fold = fold_collected(&conn, &me, std::slice::from_ref(&game));
            let mut i = inputs(&won, &[], &[], &v, &fold.verified, &no_pots, Some(900_200));
            i.identity_lc = &me;
            i.collected_present = &fold.present;
            i.held_verified = &fold.held;
            (derive_owed_rows(&i), fold)
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
        assert_eq!(rows.len(), 1, "planted rows retire nothing, whatever their column says");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["collectedMarkerPresent"], true, "they are provenance, as before");
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
        assert!(rows.is_empty(), "the identity's own filing is not crowded out");
        assert_eq!(fold.held, [held_key(&game, &tx(0x03))].into_iter().collect::<HashSet<String>>());
    }

    /// bsv-low #492 / #512 END TO END on real SQLite and real signatures: the door's verdict on a device's held
    /// filing, the production table's row with the txid the door verified, the recompute's read and fold, and
    /// the row gone. The shape is the collect pass's: the wallet holds the settle (the live credit), no v1 marker
    /// was ever filed, the row was served claimable and a press said "Collected". With it: the two caps counted
    /// APART (the B3 lens fold L1) and the re-file that writes nothing (L2).
    #[test]
    fn i492_a_devices_held_filing_retires_its_payout_through_the_door_the_table_and_the_read_on_real_sqlite() {
        use crate::record_post::tests::{identity, script, sign, wallet};
        use crate::record_post::{
            already_filed_body, cap_queries, cap_refusal_for, collected_challenge, collected_protocol, held_challenge, insert_wrote_a_row, verify_record_post, RecordKind,
            RecordRefusal, VerifiedRecord, COLLECTED_FILED_ROWS_SQL, COLLECTED_FILE_SQL, HELD_FILED_ROWS_SQL, HELD_FILINGS_PER_GAME,
        };
        let conn = production_sqlite();
        let w = wallet(21);
        let id = identity(&w);
        let me = hex::encode(&id);
        let (gid, settle, sweep_b) = ([0x01u8; 32], [0x03u8; 32], [0x0cu8; 32]);
        let (game, sweep) = (tx(0x01), tx(0x0c));
        // the game pays this seat twice: the pot's payout (settle 03..) and a swept hop's (sweep 0c..)
        let paid = [entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A))];
        let hop_key = format!("{}:0", tx(0x07));
        let sweeps = filed(&hop_key, &sweep);
        let mut mined = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        mined.spent_confirmed = Some(true);
        let hops = [mined];
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let file = |pay: &[u8; 32], signer: &bsv_rs::wallet::ProtoWallet, created_at: i64| file_held(&conn, signer, &id, &gid, pay, created_at);
        // the recompute's read and fold, then the derivation
        let served = || -> Vec<OwedRow> {
            let fold = fold_collected(&conn, &me, std::slice::from_ref(&game));
            let mut i = inputs(&paid, &[], &hops, &v, &fold.verified, &no_pots, Some(900_200));
            i.identity_lc = &me;
            i.collected_present = &fold.present;
            i.held_verified = &fold.held;
            i.hop_sweeps = &sweeps;
            derive_owed_rows(&i)
        };
        // RED's shape: nothing filed, both payouts served claimable (the press that said "Collected")
        let rows = served();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.family == OwedFamily::Payout && r.facts["claimable"] == true));
        // a stranger cannot retire it: its signature is refused at the door, and a row it plants through the
        // chain's byte-format admission (any signature bytes, a chain txid) is presence only
        assert_eq!(file(&settle, &wallet(22), 100).unwrap_err(), RecordRefusal::SignatureInvalid);
        let planted = hex::encode(sign(&wallet(22), collected_protocol(), &game, &held_challenge(&game, &me, &tx(0x03))));
        conn.execute(COLLECTED_FILE_SQL, rusqlite::params![me, game, tx(0xee), 0i64, planted, 100i64, Option::<String>::None]).unwrap();
        assert_eq!(served().len(), 2);
        // the device's wallet holds the settle: it files, and the pot's payout is gone; the sweep's stays
        let (key, written) = file(&settle, &w, 200).unwrap();
        assert!(written);
        let stored: Option<String> = conn.query_row("SELECT payTxid FROM collected_markers_v2 WHERE txid = ?1", rusqlite::params![key], |r| r.get(0)).unwrap();
        assert_eq!(stored, Some(tx(0x03)), "the door writes the txid it verified, in the row's own insert");
        let rows = served();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outpoint, hop_key);
        assert_eq!(rows[0].facts["payTxid"], sweep);
        assert_eq!(rows[0].facts["collectedMarkerPresent"], true, "the planted row is provenance, as before");
        // L2: filed again (a fresh signature, another device of the identity, a stranger's replay): the same key,
        // NO row written, so the door stales nothing, pushes nothing and counts no filing (`insert_wrote_a_row`
        // is the route's own test of the insert), and it answers 200 with the existing key and `filed: false`
        let (again, written) = file(&settle, &w, 300).unwrap();
        assert_eq!(again, key);
        assert!(!written, "a re-file is a no-op insert");
        assert!(insert_wrote_a_row(Some(1)) && insert_wrote_a_row(None) && !insert_wrote_a_row(Some(0)));
        let sig = sign(&w, collected_protocol(), &game, &held_challenge(&game, &me, &tx(0x03)));
        let refiled = verify_record_post(RecordKind::Collected, &script(&[crate::record_post::HELD_TAG, &gid, &id, &settle, &sig]), &me).unwrap();
        assert_eq!(already_filed_body(&refiled), json!({ "filed": false, "kind": "collected", "key": key, "alreadyFiled": true, "heldTxid": tx(0x03) }));
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
        assert_eq!(file(&[0xa0; 32], &w, 500).unwrap_err(), RecordRefusal::TooManyFiled, "a fifth held filing of the game");
        assert!(file(&settle, &w, 600).is_ok(), "a re-file of a held filing on file is never the cap's (its own key is excluded)");
        // L1, the other way (the lens's scenario): the game's four held rows do NOT refuse the identity's v1 marker
        let v1_sig = sign(&w, collected_protocol(), &game, &collected_challenge(&game, &me));
        let v1 = verify_record_post(RecordKind::Collected, &script(&[b"LOW/collected/v1", &gid, &id, &v1_sig]), &me).unwrap();
        let VerifiedRecord::Collected(r) = &v1 else { panic!("the v1 marker") };
        let (rows_sql, binds, _, _) = cap_queries(&v1);
        assert_eq!(rows_sql, COLLECTED_FILED_ROWS_SQL);
        let count = |sql: &str, key: &str| -> i64 { conn.query_row(sql, rusqlite::params![me, game, key], |row| row.get(0)).unwrap() };
        assert_eq!(count(rows_sql, &binds[2]), 0, "the v1 marker counts v1 rows only");
        assert_eq!(cap_refusal_for(&v1, 0, 0), None);
        assert_eq!(conn.execute(COLLECTED_FILE_SQL, rusqlite::params![r.identity, r.game_id, r.txid, 0i64, r.sig_hex, 700i64, Option::<String>::None]).unwrap(), 1);
        // and the v1 marker takes no held slot: the held count is still four, a second v1 content is the v1 cap's
        assert_eq!(count(HELD_FILED_ROWS_SQL, "filed:none"), HELD_FILINGS_PER_GAME);
        assert_eq!(count(COLLECTED_FILED_ROWS_SQL, "filed:none"), 1);
        assert_eq!(cap_refusal_for(&v1, 1, 0), Some(RecordRefusal::TooManyFiled));
        assert!(served().is_empty(), "filings that name no payout transaction of the game change nothing");
    }

    /// THE B3 LENS FOLD M2: every payout row names the OUTPUT a device must own before it files a held filing:
    /// `payTxid`:`payVout`, the paying transaction's output to the identity's committed home `payPkh`. The pot's
    /// payout gets it from the spend's own bytes (the index keeps sums per home, never the output order, and a
    /// rake output may lead); a swept hop's from the sweep's home outputs or the filed sweep's raw. Exactly one
    /// output to the home, or `null`: never a pick, and never from bytes that do not hash to the paying txid.
    /// The table behind the `payVout` pins: the stored bytes by txid, the step's own clock as one word, the
    /// isolate's memo (kept across passes by the caller, or dropped: a cold isolate), every read recorded.
    struct PayVoutTable {
        raws: HashMap<String, Vec<u8>>,
        memo: HashMap<String, u32>,
        /// `over_own_budget` answers true (the step's clock is spent the moment it is first asked).
        slow: bool,
        fault: bool,
        asked: Vec<Vec<String>>,
    }
    impl PayVoutWorld for PayVoutTable {
        fn over_own_budget(&self) -> bool {
            self.slow
        }
        fn memo_get(&self, key: &str) -> Option<u32> {
            self.memo.get(key).copied()
        }
        fn memo_put(&mut self, key: &str, vout: u32) {
            self.memo.insert(key.to_string(), vout);
        }
        async fn stored_raws(&mut self, txids: &[&str]) -> Result<Vec<(String, Vec<u8>)>, String> {
            self.asked.push(txids.iter().map(|t| t.to_string()).collect());
            if self.fault {
                return Err("d1 down".into());
            }
            Ok(txids.iter().filter_map(|t| Some((t.to_string(), self.raws.get(*t)?.clone()))).collect())
        }
    }

    /// bsv-low #512, the B3 delta lens D-L1: the `payVout` read is not behind the recompute's clock. A recompute
    /// that ran out of its budget before this step still reads the first chunk (the pass takes no recompute
    /// clock at all), and when the step's OWN clock cuts the rest, the next pass serves what the last one left:
    /// by the memo in a warm isolate, by the turning start chunk in a cold one. A `null` is a one-pass event.
    #[test]
    fn i512_dl1_a_payvout_left_null_by_the_clock_is_served_on_the_next_pass() {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Transaction, TransactionOutput};
        let run = |world: &mut PayVoutTable, wanted: &[(String, String)], chunk: usize, turn: u64| {
            tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(pay_vout_pass(world, wanted, chunk, turn))
        };
        let home = [0x11u8; 20];
        let home_hex = hex::encode(home);
        // seven paying transactions, each paying the home at vout 1 behind a distinct first output
        let mut raws = HashMap::new();
        let mut wanted: Vec<(String, String)> = Vec::new();
        for n in 0..7u64 {
            let mut t = Transaction::new();
            for (sats, pkh) in [(100 + n, [0x33u8; 20]), (39_000, home)] {
                t.outputs.push(TransactionOutput { satoshis: Some(sats), locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&pkh)).unwrap(), change: false });
            }
            raws.insert(t.id(), t.to_binary());
            wanted.push((t.id(), home_hex.clone()));
        }
        wanted.sort_unstable();
        let table = |slow: bool| PayVoutTable { raws: raws.clone(), memo: HashMap::new(), slow, fault: false, asked: Vec::new() };

        // the step's clock spent at once (the heaviest case): the first chunk is read all the same
        let mut world = table(true);
        let one = run(&mut world, &wanted, 3, 0);
        assert_eq!((one.vouts.len(), one.cut, one.reads), (3, true, 1), "pass one: one chunk whatever the clock says, the rest cut");
        assert!(one.vouts.values().all(|v| *v == 1));
        // WARM isolate, the clock spent on every pass, the SAME turn: the memo carries, the next chunk is the first
        let two = run(&mut world, &wanted, 3, 0);
        assert_eq!((two.vouts.len(), two.cut, two.reads), (6, true, 1), "pass two serves what pass one left");
        assert!(wanted[3..6].iter().all(|(t, p)| two.vouts.contains_key(&pay_vout_key(t, p))));
        let three = run(&mut world, &wanted, 3, 0);
        assert_eq!((three.vouts.len(), three.cut, three.reads), (7, false, 1));
        let four = run(&mut world, &wanted, 3, 0);
        assert_eq!((four.vouts.len(), four.cut, four.reads), (7, false, 0), "a warm isolate that holds every answer reads nothing");
        assert_eq!(world.asked.len(), 3);

        // COLD isolates (no memo carried), the clock spent on every pass: the start chunk turns, so three passes
        // cover the three chunks and no row is `null` on all of them
        let mut served: HashSet<String> = HashSet::new();
        let mut heads: HashSet<String> = HashSet::new();
        for turn in 100..103u64 {
            let mut cold = table(true);
            let pass = run(&mut cold, &wanted, 3, turn);
            assert_eq!(pass.reads, 1);
            heads.insert(cold.asked[0][0].clone());
            served.extend(pass.vouts.into_keys());
        }
        assert_eq!(heads.len(), 3, "each pass started at another chunk");
        assert_eq!(served.len(), 7, "every row was served on one of the three passes");

        // inside its own clock the step reads every chunk in one pass; a fault is counted and names nothing
        let mut quick = table(false);
        let all = run(&mut quick, &wanted, 3, 5);
        assert_eq!((all.vouts.len(), all.cut, all.reads), (7, false, 3));
        let mut down = table(false);
        down.fault = true;
        let before = PAY_VOUT_READ_FAULTS.load(Ordering::Relaxed);
        let none = run(&mut down, &wanted, 3, 0);
        assert!(none.vouts.is_empty() && !none.cut && down.memo.is_empty(), "a fault answers nothing and memoises nothing");
        assert!(PAY_VOUT_READ_FAULTS.load(Ordering::Relaxed) >= before + 3);
        // never wrong: bytes that do not hash to the asked txid name nothing
        let mut lying = table(false);
        let swapped = raws[&wanted[1].0].clone();
        lying.raws.insert(wanted[0].0.clone(), swapped);
        let pass = run(&mut lying, &wanted[..1], 3, 0);
        assert!(pass.vouts.is_empty() && lying.memo.is_empty());
        assert!(run(&mut table(true), &[], 3, 9).vouts.is_empty(), "nothing wanted: no read, no divide by zero");
    }

    #[test]
    fn i512_m2_a_payout_row_names_the_owned_paying_output() {
        use crate::results::CommittedKeys;
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Transaction, TransactionOutput};
        let (home, other, rake) = ([0x11u8; 20], [0x22u8; 20], [0x33u8; 20]);
        let out = |sats: u64, pkh: &[u8; 20]| TransactionOutput { satoshis: Some(sats), locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(pkh)).unwrap(), change: false };
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
        assert_eq!(sole_home_vout_of_raw(&claim_raw, &claim, &home_hex), Some(1));
        assert_eq!(sole_home_vout_of_raw(&bare_raw, &bare, &home_hex), Some(0), "no rake output: the order moves, so it is read, never assumed");
        assert_eq!(sole_home_vout_of_raw(&tie_raw, &tie, &home_hex), Some(2));
        assert_eq!(sole_home_vout_of_raw(&tie_raw, &tie, &hex::encode(other)), Some(1), "the tie's other seat: its own output");
        // never a pick: no output to the home, two outputs to the home, bytes of another transaction, junk
        let (twice, twice_raw) = raw_of(&[(1, &home), (2, &home)]);
        assert_eq!(sole_home_vout_of_raw(&claim_raw, &claim, &hex::encode([0x44u8; 20])), None);
        assert_eq!(sole_home_vout_of_raw(&twice_raw, &twice, &home_hex), None);
        assert_eq!(sole_home_vout_of_raw(&bare_raw, &claim, &home_hex), None, "the bytes must hash to the paying txid");
        assert_eq!(sole_home_vout_of_raw(&[0u8; 4], &claim, &home_hex), None);

        // THE POT'S ROW: `payPkh` is my committed home by my seat; `payVout` is null until the route read the bytes
        let mut won = entry(Some(true), Some(PotVerdict::WinnerA), Outcome::Won, Some(SeatLetter::A));
        won.settle_txid = Some(claim.clone());
        won.committed_keys = Some(CommittedKeys { pub_a: "02".repeat(33), pub_b: "03".repeat(33), pay_pkh_a: home_hex.clone(), pay_pkh_b: hex::encode(other) });
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let paid = [won.clone()];
        let mut rows = derive_owed_rows(&inputs(&paid, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows.len(), 1);
        assert_eq!((&rows[0].facts["payTxid"], &rows[0].facts["payPkh"]), (&json!(claim), &json!(home_hex)));
        assert!(rows[0].facts["payVout"].is_null());
        assert_eq!(pay_vout_wanted(&rows), vec![(claim.clone(), home_hex.clone())]);
        // the route's answer for that (txid, home), and only that: another home's answer marks nothing
        let mut vouts: HashMap<String, u32> = HashMap::new();
        vouts.insert(pay_vout_key(&claim, &hex::encode(other)), 7);
        assert_eq!(mark_pay_vouts(&mut rows, &vouts), 0);
        assert!(rows[0].facts["payVout"].is_null());
        vouts.insert(pay_vout_key(&claim, &home_hex), sole_home_vout_of_raw(&claim_raw, &claim, &home_hex).unwrap());
        assert_eq!(mark_pay_vouts(&mut rows, &vouts), 1);
        assert_eq!(rows[0].facts["payVout"], 1);
        assert!(pay_vout_wanted(&rows).is_empty(), "a named row is asked for no more");
        // seat B of the same pot is paid at ITS home; a seat the binding does not prove names no home and is not asked
        let mut tied = won.clone();
        (tied.verdict, tied.outcome, tied.my_seat) = (Some(PotVerdict::Tie), Outcome::Tie, Some(SeatLetter::B));
        let tied = [tied];
        let rows_b = derive_owed_rows(&inputs(&tied, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows_b[0].facts["payPkh"], hex::encode(other));
        let mut keyless = won.clone();
        keyless.committed_keys = None;
        let keyless = [keyless];
        let rows_k = derive_owed_rows(&inputs(&keyless, &[], &[], &v, &c, &p, Some(900_200)));
        assert!(rows_k[0].facts["payPkh"].is_null() && rows_k[0].facts["payVout"].is_null());
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
        let rows_s = derive_owed_rows(&i);
        assert_eq!(rows_s.len(), 1);
        assert_eq!((&rows_s[0].facts["payTxid"], &rows_s[0].facts["payPkh"], &rows_s[0].facts["payVout"]), (&json!(sweep), &json!(home_hex), &json!(0)));
        // a home this list does not know, or a raw that is not the sweep: `null`, and the route is not asked (a sweep is not in `pot_beefs`' contract)
        let mut i = inputs(&[], &[], &hops, &v, &NONE, &p, Some(900_000));
        i.hop_sweeps = &sweeps;
        let rows_n = derive_owed_rows(&i);
        assert!(rows_n[0].facts["payVout"].is_null() && rows_n[0].facts["payPkh"].is_null());
        assert!(pay_vout_wanted(&rows_n).is_empty());
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

    /// The owed write batch on real SQLite, as D1 runs it: every statement in order inside one transaction; the
    /// closing read-back's stamp is the answer (`owed_write_landed`).
    fn run_owed_write(conn: &mut rusqlite::Connection, plan: &[(&'static str, Vec<OwedBind>)]) -> Option<i64> {
        let txn = conn.transaction().expect("begin");
        let mut stamp: Option<i64> = None;
        for (n, (sql, binds)) in plan.iter().enumerate() {
            let vals: Vec<rusqlite::types::Value> = binds
                .iter()
                .map(|b| match b {
                    OwedBind::Text(s) => rusqlite::types::Value::Text(s.clone()),
                    OwedBind::Int(v) => rusqlite::types::Value::Integer(*v),
                    OwedBind::Null => rusqlite::types::Value::Null,
                })
                .collect();
            if n + 1 == plan.len() {
                stamp = txn.query_row(sql, rusqlite::params_from_iter(vals), |r| r.get(0)).ok();
            } else {
                txn.execute(sql, rusqlite::params_from_iter(vals)).unwrap_or_else(|e| panic!("statement {n} ({sql}): {e}"));
            }
        }
        txn.commit().expect("commit");
        stamp
    }
    fn served(conn: &rusqlite::Connection, identity: &str) -> (Vec<(String, String)>, i64, i64) {
        let mut st = conn.prepare("SELECT outpoint, family FROM owed_rows WHERE identity = ?1 ORDER BY outpoint").unwrap();
        let rows: Vec<(String, String)> = st.query_map([identity], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect();
        let (stamp, stale): (i64, i64) = conn.query_row("SELECT computedAtMs, stale FROM owed_state WHERE identity = ?1", [identity], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        (rows, stamp, stale)
    }

    /// bsv-low #487: a walk that stopped touching its lease for 60 s is taken over while still live; the takeover
    /// finishes and writes, then the FIRST walk finishes and wrote its OLDER snapshot over the newer one (the
    /// isolate-local lock cannot see a walk on another isolate at all). The write itself now refuses it: a snapshot
    /// stamped older than the one the marker carries changes no row and no marker, and the walk is told
    /// (`owed_write_landed`). Two walks, the older landing second, on real SQLite with the shipped statements.
    /// To red: drop the `NOT EXISTS` guard from `OWED_ROWS_DELETE_SQL` / `OWED_ROW_INSERT_SQL`, or the `WHERE` of
    /// `OWED_STATE_UPSERT_SQL`.
    #[test]
    fn an_older_walk_landing_after_a_newer_one_is_refused_by_the_write_itself_real_sqlite() {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        conn.execute_batch(OWED_ROWS_CREATE).unwrap();
        conn.execute_batch(OWED_STATE_CREATE).unwrap();
        let row = |outpoint: &str, family: OwedFamily| OwedRow {
            identity: ME.to_string(),
            outpoint: outpoint.to_string(),
            family,
            game_id: tx(0x01),
            sats: Some(20_190),
            opponent_identity: Some(OPP.to_string()),
            at_height: None,
            facts: json!({ "claim": "sweep-hop" }),
            reason: None,
        };
        // walk ONE began at t = 1 000 and saw the hop stranded; it stalls past the lease (60 s)
        let one_at = 1_000i64;
        let one = [row(&format!("{}:0", tx(0x07)), OwedFamily::HopStranded)];
        // walk TWO (the takeover) began at t = 71 000: the hop was swept meanwhile, so it sees the sweep's payout
        let two_at = 71_000i64;
        let two = [row(&format!("{}:0", tx(0x07)), OwedFamily::Payout), row(&format!("{}:1", tx(0x08)), OwedFamily::InProgress)];
        // TWO lands first
        let stamp = run_owed_write(&mut conn, &owed_write_plan(ME, &two, two_at, Some(900_001), false));
        assert!(owed_write_landed(stamp, two_at));
        let newer = served(&conn, ME);
        assert_eq!(newer.0.len(), 2);
        // a filing marks the newer snapshot stale before ONE lands: a refused write must not clear the mark
        conn.execute(OWED_STALE_FOR_IDENTITY_SQL, [ME]).unwrap();
        // ONE lands second, with its older snapshot: REFUSED, nothing moves
        let stamp = run_owed_write(&mut conn, &owed_write_plan(ME, &one, one_at, Some(900_000), false));
        assert!(!owed_write_landed(stamp, one_at), "the older walk is told its snapshot did not land");
        let after = served(&conn, ME);
        assert_eq!(after.0, newer.0, "the newer snapshot's rows stand, none replaced and none added");
        assert_eq!(after.1, two_at, "the marker keeps the newer stamp");
        assert_eq!(after.2, 1, "and its stale mark");
        let tip: i64 = conn.query_row("SELECT tip FROM owed_state WHERE identity = ?1", [ME], |r| r.get(0)).unwrap();
        assert_eq!(tip, 900_001);
        // the ordinary order still writes: a NEWER walk replaces the rows and clears the mark
        let three_at = 72_000i64;
        let stamp = run_owed_write(&mut conn, &owed_write_plan(ME, &one, three_at, Some(900_002), true));
        assert!(owed_write_landed(stamp, three_at));
        let after = served(&conn, ME);
        assert_eq!(after.0, vec![(format!("{}:0", tx(0x07)), "hop-stranded".to_string())]);
        assert_eq!((after.1, after.2), (three_at, 0));
        // the SAME stamp rewrites (one walk's batch retried is idempotent, never a refusal of itself)
        let stamp = run_owed_write(&mut conn, &owed_write_plan(ME, &two, three_at, Some(900_002), false));
        assert!(owed_write_landed(stamp, three_at));
        assert_eq!(served(&conn, ME).0.len(), 2);
        // the guard is per identity: another identity's newer marker refuses nothing of mine
        let other = OPP;
        let theirs = [OwedRow { identity: other.to_string(), ..row(&format!("{}:0", tx(0x09)), OwedFamily::Payout) }];
        assert!(owed_write_landed(run_owed_write(&mut conn, &owed_write_plan(other, &theirs, 999_000, None, false)), 999_000));
        let four_at = 73_000i64;
        assert!(owed_write_landed(run_owed_write(&mut conn, &owed_write_plan(ME, &one, four_at, None, false)), four_at));
        assert_eq!(served(&conn, ME).0.len(), 1);
        assert_eq!(served(&conn, other).0.len(), 1);
        // an EMPTY older snapshot (the false "nothing owed") is refused like any other
        let stamp = run_owed_write(&mut conn, &owed_write_plan(ME, &[], 2_000, None, false));
        assert!(!owed_write_landed(stamp, 2_000));
        assert_eq!(served(&conn, ME).0.len(), 1);
    }

    /// A real sweep paying `home_key`'s P2PKH `sats`, and a real spend of that home output signed by `signer`.
    fn sweep_and_home_spend(home_key: &bsv_rs::primitives::PrivateKey, signer: &bsv_rs::primitives::PrivateKey, sats: u64) -> (String, Vec<u8>) {
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
        sweep.outputs.push(TransactionOutput::new(sats, P2PKH::new().lock(&home_key.public_key().hash160()).unwrap()));
        let sweep_txid = sweep.id();
        let mut spend = Transaction::new();
        spend.add_input_from_tx(sweep, 0, P2PKH::unlock(signer, SignOutputs::All, false)).unwrap();
        spend.outputs.push(TransactionOutput::new(sats - 100, P2PKH::new().lock(&[0x11u8; 20]).unwrap()));
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(spend.sign()).expect("template signing");
        (sweep_txid, spend.to_binary())
    }

    /// bsv-low #485, the proof itself: the home output's spend is PROVEN only by the home key's own signature,
    /// executed here over the bytes; bytes a courier could fabricate (another key's signature, a signature over a
    /// different amount or outpoint, a damaged one) prove nothing.
    #[test]
    fn a_home_outputs_spend_is_proven_by_the_home_keys_signature_and_by_nothing_a_courier_can_fabricate() {
        let home = bsv_rs::primitives::PrivateKey::random();
        let stranger = bsv_rs::primitives::PrivateKey::random();
        let home_pkh = hex::encode(home.public_key().hash160());
        let (sweep_txid, spend_raw) = sweep_and_home_spend(&home, &home, 20_000);
        assert!(home_output_spend_proven(&spend_raw, &sweep_txid, 0, &home_pkh, 20_000));
        assert!(home_output_spend_proven(&spend_raw, &sweep_txid.to_ascii_uppercase(), 0, &home_pkh.to_ascii_uppercase(), 20_000));
        // a courier's fabrication: bytes that consume the home output under a key that is not the home's
        let (forged_sweep, forged_raw) = sweep_and_home_spend(&home, &stranger, 20_000);
        assert!(!home_output_spend_proven(&forged_raw, &forged_sweep, 0, &home_pkh, 20_000));
        // the signature commits to the amount and to the outpoint
        assert!(!home_output_spend_proven(&spend_raw, &sweep_txid, 0, &home_pkh, 20_001));
        assert!(!home_output_spend_proven(&spend_raw, &sweep_txid, 1, &home_pkh, 20_000));
        assert!(!home_output_spend_proven(&spend_raw, &tx(0x0e), 0, &home_pkh, 20_000));
        // another home, a damaged signature, junk
        assert!(!home_output_spend_proven(&spend_raw, &sweep_txid, 0, &"cc".repeat(20), 20_000));
        let mut damaged = spend_raw.clone();
        let at = damaged.len() / 3; // inside input 0's unlocking script (the DER signature)
        damaged[at] ^= 0x01;
        assert!(!home_output_spend_proven(&damaged, &sweep_txid, 0, &home_pkh, 20_000));
        assert!(!home_output_spend_proven(&[], &sweep_txid, 0, &home_pkh, 20_000));
        assert!(!home_output_spend_proven(&spend_raw, &sweep_txid, 0, "nothex", 20_000));
    }

    /// bsv-low #485: a courier-proven payout row (a sweep the index never held, courier outputs carry `spent: null`)
    /// never retired. It now retires when EVERY home output's spend is `Proven` (the home key's signature, verified
    /// by this crate over bytes any courier may carry), and on nothing weaker: a chain word of spent without the
    /// proof, a corroborated unspent, or no look at all leave the row standing, claimable false, saying what was seen.
    /// To red: drop the `home_proven` arm of `swept_home`'s `output_spent`.
    #[test]
    fn a_courier_proven_payout_retires_when_the_home_key_signed_the_sats_away_and_on_no_couriers_word() {
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
        let word = |w: Option<HomeSpendWord>| -> HashMap<String, HomeSpendWord> { w.map(|w| (home_key.clone(), w)).into_iter().collect() };
        let derive = |outs: &HashMap<String, Vec<SpenderOutput>>, words: &HashMap<String, HomeSpendWord>, courier: &HashSet<String>| {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.spender_outputs = outs;
            i.spender_inputs = &spends_it;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = courier;
            i.home_spends = words;
            (derive_owed_rows(&i), courier_home_outputs(&i))
        };
        // not looked yet: the row of 2026-09-20, unchanged, and the walk's one candidate
        let (rows, candidates) = derive(&mine, &word(None), &courier);
        assert_eq!((rows.len(), rows[0].family), (1, OwedFamily::Payout));
        assert_eq!(rows[0].facts["creditKind"], "courier-bytes");
        assert_eq!(rows[0].facts["claimable"], false);
        assert!(rows[0].facts.get("homeSpend").is_none());
        assert_eq!(candidates, vec![CourierHomeOutput { sweep_txid: sweep.clone(), vout: 0, pkh_hex: my_pkh.clone(), sats: 20_000 }]);
        // the chain rung's corroborated unspent: the row stands and says the sats were SEEN at the home
        let (rows, _) = derive(&mine, &word(Some(HomeSpendWord::Unspent)), &courier);
        assert_eq!((rows.len(), rows[0].facts["claimable"].clone(), rows[0].facts["homeSpend"].clone()), (1, json!(false), json!("unspent")));
        assert_eq!(rows[0].facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON);
        // a courier's word of spent WITHOUT the proof retires nothing
        let (rows, _) = derive(&mine, &word(Some(HomeSpendWord::Unproven)), &courier);
        assert_eq!((rows.len(), rows[0].facts["claimable"].clone(), rows[0].facts["homeSpend"].clone()), (1, json!(false), json!("unproven")));
        // THE RETIREMENT: the home key signed the output away
        let (rows, candidates) = derive(&mine, &word(Some(HomeSpendWord::Proven)), &courier);
        assert!(rows.is_empty(), "{rows:?}");
        assert!(candidates.is_empty(), "a retired output is not walked again inside the pass");
        // two home outputs: one proven is not enough
        let two = pays(&sweep, &[(0, &my_pkh, 20_000, None), (1, &my_pkh, 190, None)]);
        let (rows, candidates) = derive(&two, &word(Some(HomeSpendWord::Proven)), &courier);
        assert_eq!((rows.len(), rows[0].sats), (1, Some(20_190)));
        assert_eq!(candidates.len(), 2);
        let mut both = word(Some(HomeSpendWord::Proven));
        both.insert(format!("{sweep}:1"), HomeSpendWord::Proven);
        assert!(derive(&two, &both, &courier).0.is_empty());
        // a word for ANOTHER outpoint (a stranger's sweep, a planted key) retires nothing of mine
        let other: HashMap<String, HomeSpendWord> = [(format!("{}:0", tx(0x0f)), HomeSpendWord::Proven)].into_iter().collect();
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
        assert_eq!(derive_owed_rows(&i)[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
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
        assert_eq!(script, [(key.clone(), DoorRefusal::Script)].into_iter().collect::<HashMap<_, _>>());
        assert_eq!(network.get(&key), Some(&DoorRefusal::Network));
        let unspent = chain(&key, true, Some(false), None);
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let derive = |refused: &HashMap<String, DoorRefusal>, chain_word: &HashMap<String, HopChainWord>| {
            let mut i = inputs(&[], &[], &young, &v, &c, &p, Some(900_000));
            i.hop_chain = chain_word;
            i.door_refused_hop_outpoints = refused;
            derive_owed_rows(&i)
        };
        // THE FIX: stranded now, the sweep press (the Collect on a stranded hop), never the felt's rejoin
        let rows = derive(&script, &unspent);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!((rows[0].family, rows[0].sats), (OwedFamily::HopStranded, Some(20_190)));
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["joinRefused"], true);
        assert_eq!(rows[0].facts["joinRefusedBy"], "door");
        assert_eq!(rows[0].facts["doorRefusal"], "script");
        // LOW-1: the 400 arm says what one refused copy proves, and that the sweep returns the seat's own stake
        let said = rows[0].reason.as_deref().unwrap();
        assert_eq!(said, DOOR_SCRIPT_REFUSED_REASON);
        assert!(!said.contains("cannot start"), "one refused copy does not prove the hand cannot start");
        assert!(said.contains("refused at the door") && said.contains("swept back now") && said.contains("your own stake"));
        // the 422 arm (the network's definitive word on bytes whose scripts verified) keeps its sentence
        let rows = derive(&network, &unspent);
        assert_eq!((rows[0].family, rows[0].facts["doorRefusal"].clone(), rows[0].reason.as_deref()), (OwedFamily::HopStranded, json!("network"), Some(DOOR_REFUSED_REASON)));
        // the press still rests on the chain rung: no word yet is a waiting sentence that says why it will be sweepable
        let rows = derive(&script, &NO_CHAIN);
        assert_eq!((rows[0].family, rows[0].facts["joinRefused"].clone()), (OwedFamily::Unbound, json!(true)));
        assert!(rows[0].facts.get("claimable").is_none());
        // without the ledger's word: in progress, rejoin, and no door fact
        let rows = derive(&HashMap::new(), &unspent);
        assert_eq!((rows[0].family, rows[0].facts["claim"].clone()), (OwedFamily::InProgress, json!("rejoin")));
        assert!(rows[0].facts.get("doorRefusal").is_none());
        // a ledger row for an outpoint the walk does not hold names nothing (the belt behind the SQL's own join)
        assert!(door_refused_hops(&[(tx(0x09), 0, "script-refused".to_string())], &young).is_empty());
        assert!(door_refused_hops(&[(hop_txid.clone(), 1, "script-refused".to_string())], &young).is_empty());
        // an eviction's word keeps its own sentence when both ledgers name the hop
        let evicted: HashSet<String> = [key.clone()].into_iter().collect();
        let mut i = inputs(&[], &[], &young, &v, &c, &p, Some(900_000));
        i.hop_chain = &unspent;
        i.door_refused_hop_outpoints = &script;
        i.evicted_hop_outpoints = &evicted;
        let rows = derive_owed_rows(&i);
        assert_eq!((rows[0].facts["joinRefusedBy"].clone(), rows[0].reason.as_deref()), (json!("eviction"), Some(JOIN_REFUSED_REASON)));
        assert!(rows[0].facts.get("doorRefusal").is_none());
    }

    /// The lens fold's MEDIUM-2, the home walk's two decisions, EXECUTED, with the latch key round-tripped on real
    /// SQLite through the shipped memo statements (`PROBE_MEMO_UPSERT_SQL`, `probe_memo_read_sql`) and the shipped
    /// migrations: a proof writes a latch under the key the next pass reads; a latched output is `Proven` with no
    /// chain ask (it is not a target); an unknown or faulted probe names nothing; a word of spent without the home
    /// key's signature is `Unproven` and writes no latch.
    /// To red: have `home_word` latch on a bare word of spent, or key the latch read differently from its write.
    #[test]
    fn the_home_walk_latches_only_a_verified_proof_and_reads_its_own_latch_back_real_sqlite() {
        use crate::hops_view::{probe_memo_key, probe_memo_read_sql, ChainSpendProbe, ProbeMemo, PROBE_MEMO_UPSERT_SQL};
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(e.to_string().to_ascii_lowercase().contains("duplicate column"), "migration failed under real SQLite: {e}");
            }
        }
        let write = |m: &ProbeMemo| {
            conn.execute(PROBE_MEMO_UPSERT_SQL, rusqlite::params![m.outpoint, m.probed_at_ms, i64::from(m.spent), m.spending_txid, m.spent_confirmed.map(i64::from)]).unwrap();
        };
        // the route's `read_probe_memos`: one IN read keyed by `probe_memo_key` of each target
        let read = |targets: &[(String, u32)]| -> Vec<ProbeMemo> {
            let keys: Vec<String> = targets.iter().map(|(t, v)| probe_memo_key(t, *v)).collect();
            conn.prepare(&probe_memo_read_sql(keys.len()))
                .unwrap()
                .query_map(rusqlite::params_from_iter(keys.iter()), |r| {
                    Ok(ProbeMemo {
                        outpoint: r.get(0)?,
                        probed_at_ms: r.get(1)?,
                        spent: r.get::<_, i64>(2)? != 0,
                        spending_txid: r.get(3)?,
                        spent_confirmed: r.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                    })
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        let (home, stranger) = (bsv_rs::primitives::PrivateKey::random(), bsv_rs::primitives::PrivateKey::random());
        let pkh = hex::encode(home.public_key().hash160());
        let (sweep_a, spend_a) = sweep_and_home_spend(&home, &home, 20_000);
        let (sweep_b, forged_b) = sweep_and_home_spend(&home, &stranger, 30_000); // spent "by" bytes the home key never signed
        let cand = |sweep: &str, sats: u64| CourierHomeOutput { sweep_txid: sweep.to_string(), vout: 0, pkh_hex: pkh.clone(), sats };
        let candidates = [cand(&sweep_a, 20_000), cand(&sweep_b, 30_000)];
        let latch_targets: Vec<(String, u32)> = candidates.iter().map(|c| home_latch_target(&c.sweep_txid, c.vout)).collect();
        // pass 1: nothing latched, both outputs go to the chain rung
        let (words, targets) = latched_home_words(&candidates, &read(&latch_targets));
        assert!(words.is_empty());
        assert_eq!(targets, vec![(sweep_a.clone(), 0), (sweep_b.clone(), 0)]);
        let spent_by = |sp: &str| ChainSpendProbe { known: true, spent: Some(true), spending_txid: Some(sp.to_string()), spent_confirmed: Some(true) };
        let named = tx(0x5a); // the spender a courier NAMED; the proof is over the bytes
        // an unknown or faulted probe, and a known probe with no verdict: nothing established, the row stands
        assert_eq!(home_word(&ChainSpendProbe { known: false, spent: Some(true), spending_txid: Some(named.clone()), spent_confirmed: None }, Some(&spend_a), &candidates[0], 5_000), None);
        assert_eq!(home_word(&ChainSpendProbe { known: true, spent: None, spending_txid: None, spent_confirmed: None }, Some(&spend_a), &candidates[0], 5_000), None);
        // a corroborated absence
        assert_eq!(home_word(&ChainSpendProbe { known: true, spent: Some(false), spending_txid: None, spent_confirmed: None }, None, &candidates[0], 5_000), Some((HomeSpendWord::Unspent, None)));
        // a word of spent with no bytes, with bytes another key signed, with another output's proof: unproven, no latch
        assert_eq!(home_word(&spent_by(&named), None, &candidates[0], 5_000), Some((HomeSpendWord::Unproven, None)));
        assert_eq!(home_word(&spent_by(&named), Some(&forged_b), &candidates[1], 5_000), Some((HomeSpendWord::Unproven, None)));
        assert_eq!(home_word(&spent_by(&named), Some(&spend_a), &candidates[1], 5_000), Some((HomeSpendWord::Unproven, None)));
        // THE PROOF: the home key's signature over output A's spend
        let (word, latch) = home_word(&spent_by(&named), Some(&spend_a), &candidates[0], 5_000).expect("a word");
        let latch = latch.expect("a proof writes its latch");
        assert_eq!(word, HomeSpendWord::Proven);
        assert_eq!(latch.outpoint, format!("{HOME_SPEND_LATCH_PREFIX}{sweep_a}.0"));
        let proven_txid = bsv_rs::transaction::Transaction::from_binary(&spend_a).unwrap().id();
        assert_eq!(latch.spending_txid.as_deref(), Some(proven_txid.as_str()), "the latch names the transaction the proven bytes hash to");
        assert_ne!(proven_txid, named);
        write(&latch);
        // the chain rung's ordinary memo of output B (keyed `<txid>.<vout>`) is no latch
        write(&ProbeMemo { outpoint: probe_memo_key(&sweep_b, 0), probed_at_ms: 5_000, spent: true, spending_txid: Some(named.clone()), spent_confirmed: Some(true) });
        // pass 2: the key written is the key read; A is Proven with no ask, B is still a target
        let memos = read(&latch_targets);
        assert_eq!(memos.len(), 1);
        let (words, targets) = latched_home_words(&candidates, &memos);
        assert_eq!(words, [(outpoint_key(&sweep_a, 0), HomeSpendWord::Proven)].into_iter().collect::<HashMap<_, _>>());
        assert_eq!(targets, vec![(sweep_b.clone(), 0)]);
        // a candidate named in another case reads the same latch
        let upper = [cand(&sweep_a.to_ascii_uppercase(), 20_000)];
        let upper_targets: Vec<(String, u32)> = upper.iter().map(|c| home_latch_target(&c.sweep_txid, c.vout)).collect();
        assert_eq!(latched_home_words(&upper, &read(&upper_targets)).0.len(), 1);
        // a memo under a latch key that does not say spent-with-a-spender latches nothing
        let weak = ProbeMemo { outpoint: probe_memo_key(&latch_targets[1].0, 0), probed_at_ms: 5_000, spent: true, spending_txid: None, spent_confirmed: None };
        assert!(latched_home_words(&candidates[1..], &[weak]).0.is_empty());
        // the stored-read count sits inside the candidate cap
        const _: () = assert!(OWED_HOME_STORED_READS_PER_RECOMPUTE <= crate::logic::D1_CHUNK_OUTPOINTS);
    }

    /// The lens fold's LOW-2: a read whose own walk was SUPERSEDED (the #487 guard refused its older snapshot) is
    /// answered with the rows the table holds, the newer walk's, never its own. The decision is
    /// `read_answer_after_compute`; the rows are what the shipped read statements return after the two writes, on
    /// real SQLite.
    /// To red: have `read_answer_after_compute` answer `Own` for a superseded walk.
    #[test]
    fn a_superseded_read_is_served_the_newer_stored_rows_never_its_own_real_sqlite() {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        conn.execute_batch(OWED_ROWS_CREATE).unwrap();
        conn.execute_batch(OWED_STATE_CREATE).unwrap();
        let row = |outpoint: String, family: OwedFamily| OwedRow {
            identity: ME.to_string(),
            outpoint,
            family,
            game_id: tx(0x01),
            sats: Some(20_190),
            opponent_identity: Some(OPP.to_string()),
            at_height: None,
            facts: json!({ "claim": "sweep-hop" }),
            reason: None,
        };
        let older = [row(format!("{}:0", tx(0x07)), OwedFamily::HopStranded)];
        let newer = [row(format!("{}:0", tx(0x07)), OwedFamily::Payout), row(format!("{}:1", tx(0x08)), OwedFamily::InProgress)];
        // the route's stored read: the marker, then the rows, through the shipped statements
        let stored = |conn: &rusqlite::Connection| -> (Vec<(String, String)>, i64, Option<i64>, bool) {
            let (at, tip, cut): (i64, Option<i64>, i64) = conn.query_row(OWED_STATE_READ_SQL, [ME], |r| Ok((r.get(1)?, r.get(2)?, r.get(5)?))).unwrap();
            let rows = conn.prepare(OWED_ROWS_READ_SQL).unwrap().query_map([ME], |r| Ok((r.get(1)?, r.get(2)?))).unwrap().map(|r| r.unwrap()).collect();
            (rows, at, tip, cut != 0)
        };
        // the newer walk (began at 71 000) lands first; its own read serves its own rows
        let landed = owed_write_landed(run_owed_write(&mut conn, &owed_write_plan(ME, &newer, 71_000, Some(900_001), true)), 71_000);
        assert_eq!(read_answer_after_compute(!landed), ReadAnswer::Own);
        // the older walk (began at 1 000) lands second: refused, and its READER is owed the newer rows
        let landed = owed_write_landed(run_owed_write(&mut conn, &owed_write_plan(ME, &older, 1_000, Some(900_000), false)), 1_000);
        assert!(!landed);
        assert_eq!(read_answer_after_compute(!landed), ReadAnswer::Stored);
        let (rows, at, tip, cut) = stored(&conn);
        assert_eq!(rows, vec![(format!("{}:0", tx(0x07)), "payout".to_string()), (format!("{}:1", tx(0x08)), "in-progress".to_string())]);
        assert_ne!(rows.len(), older.len(), "never the older walk's own rows");
        assert_eq!((at, tip, cut), (71_000, Some(900_001), true), "with the newer snapshot's stamp, tip and cut");
    }

    // ---- the delta fold (2026-10-06): the home walk's ring, its caps, the superseded fallback ----

    /// A table of answers behind `HomeWalkWorld`, counting every call in order.
    #[derive(Default)]
    struct FakeCouriers {
        /// `<txid>:<vout>` to the chain rung's answer; an outpoint not named is one the couriers cannot answer.
        probes: HashMap<String, crate::hops_view::ChainSpendProbe>,
        stored: HashMap<String, Vec<u8>>,
        held: HashMap<String, Option<Vec<u8>>>,
        over_budget: bool,
        probed: Vec<String>,
        stored_asked: Vec<String>,
        resolver_asked: Vec<String>,
    }
    impl HomeWalkWorld for FakeCouriers {
        fn over_budget(&self) -> bool {
            self.over_budget
        }
        async fn probe(&mut self, txid: &str, vout: u32) -> crate::hops_view::ChainSpendProbe {
            let key = outpoint_key(txid, vout);
            self.probed.push(key.clone());
            self.probes.get(&key).cloned().unwrap_or(crate::hops_view::ChainSpendProbe { known: false, spent: None, spending_txid: None, spent_confirmed: None })
        }
        async fn stored_spender(&mut self, spender_txid: &str) -> Option<Vec<u8>> {
            self.stored_asked.push(spender_txid.to_string());
            self.stored.get(spender_txid).cloned()
        }
        fn cached_spender(&mut self, spender_txid: &str) -> Option<Option<Vec<u8>>> {
            self.held.get(spender_txid).cloned()
        }
        async fn resolve_spender(&mut self, spender_txid: &str) -> Option<Vec<u8>> {
            self.resolver_asked.push(spender_txid.to_string());
            None
        }
    }

    fn memo_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(e.to_string().to_ascii_lowercase().contains("duplicate column"), "migration failed under real SQLite: {e}");
            }
        }
        conn
    }
    fn write_memos(conn: &rusqlite::Connection, memos: &[crate::hops_view::ProbeMemo]) {
        for m in memos {
            conn.execute(crate::hops_view::PROBE_MEMO_UPSERT_SQL, rusqlite::params![m.outpoint, m.probed_at_ms, i64::from(m.spent), m.spending_txid, m.spent_confirmed.map(i64::from)]).unwrap();
        }
    }
    fn read_memos(conn: &rusqlite::Connection, targets: &[(String, u32)]) -> Vec<crate::hops_view::ProbeMemo> {
        let keys: Vec<String> = targets.iter().map(|(t, v)| crate::hops_view::probe_memo_key(t, *v)).collect();
        assert!(keys.len() < crate::logic::D1_MAX_BOUND_PARAMS, "one read stays inside D1's bind limit ({})", keys.len());
        conn.prepare(&crate::hops_view::probe_memo_read_sql(keys.len()))
            .unwrap()
            .query_map(rusqlite::params_from_iter(keys.iter()), |r| {
                Ok(crate::hops_view::ProbeMemo {
                    outpoint: r.get(0)?,
                    probed_at_ms: r.get(1)?,
                    spent: r.get::<_, i64>(2)? != 0,
                    spending_txid: r.get(3)?,
                    spent_confirmed: r.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                })
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }
    /// ONE PASS as `routes::owed_home_spend_walk` runs it, the Worker's D1 replaced by real SQLite over the shipped
    /// memo statements: the cursor and the window's memos read, `home_walk_pass`, the memos and the cursor written.
    /// Returns the pass and the window it walked.
    fn home_pass_on_sqlite(conn: &rusqlite::Connection, world: &mut FakeCouriers, candidates: &[CourierHomeOutput], now_ms: i64) -> (HomePass, Vec<CourierHomeOutput>) {
        let memo_targets = |window: &[CourierHomeOutput]| -> Vec<(String, u32)> {
            window.iter().map(|c| home_latch_target(&c.sweep_txid, c.vout)).chain(window.iter().map(|c| (c.sweep_txid.clone(), c.vout))).collect()
        };
        let (window, memos) = if candidates.len() <= OWED_HOME_WINDOW {
            let mut targets = vec![home_cursor_target(ME)];
            targets.extend(memo_targets(candidates));
            let memos = read_memos(conn, &targets);
            (home_walk_window(candidates, home_cursor_of(&memos, ME).as_deref(), OWED_HOME_WINDOW), memos)
        } else {
            let cursor = home_cursor_of(&read_memos(conn, &[home_cursor_target(ME)]), ME);
            let window = home_walk_window(candidates, cursor.as_deref(), OWED_HOME_WINDOW);
            let memos = read_memos(conn, &memo_targets(&window));
            (window, memos)
        };
        let pass = tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(home_walk_pass(world, &window, &memos, &memos, now_ms));
        write_memos(conn, &pass.memos);
        if let Some(next) = home_cursor_after(candidates, &window, pass.first_withheld) {
            write_memos(conn, &[home_cursor_memo(ME, next, now_ms)]);
        }
        (pass, window)
    }
    fn plain_candidates(n: usize) -> Vec<CourierHomeOutput> {
        (0..n).map(|i| CourierHomeOutput { sweep_txid: format!("{:064x}", 0x1000 + i), vout: 0, pkh_hex: "cc".repeat(20), sats: 20_000 }).collect()
    }
    fn unspent_probe() -> crate::hops_view::ChainSpendProbe {
        crate::hops_view::ChainSpendProbe { known: true, spent: Some(false), spending_txid: None, spent_confirmed: None }
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
            confirmation_refused, mark_refused_word_rows, probe_memo_of, set_aside_proofs_a_refused_word_contradicts, stale_memo_word, ChainSpendProbe, ProbeMemo,
            CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON, CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON, CHAIN_WORD_AWAITS_PROBE_REASON, OWED_CHAIN_WAIT_PROBE,
            OWED_FACT_CHAIN_WORD_AWAITS_PROBE,
        };
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let now: i64 = 1_800_000_000_000;
        let (clear, read_at) = (now - 7 * 60_000, now - 6 * 60_000); // the memo was read one minute into the grace
        let (sweep, rival, my_pkh) = (tx(0x0e), tx(0x9b), "cc".repeat(20));
        let key = outpoint_key(&tx(0x07), 0);
        let hops = [hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1))];
        let pkhs: HashMap<String, String> = [(tx(0x01), my_pkh.clone())].into_iter().collect();
        let memo_naming = |spender: &str| -> ProbeMemo {
            probe_memo_of(&tx(0x07), 0, &ChainSpendProbe { known: true, spent: Some(true), spending_txid: Some(spender.to_string()), spent_confirmed: Some(true) }, read_at).expect("a known answer is memoised")
        };
        // THE WALK past the budget, in the route's order: the stale word, the refusal remembered, the set-aside,
        // the derivation, the mark
        let walk = |memo: &ProbeMemo, spender: &str, courier: bool, filed: Option<&str>| -> OwedRow {
            let mut refused_words: HashMap<String, Option<String>> = HashMap::new();
            assert!(confirmation_refused(memo, Some(clear)));
            refused_words.insert(key.clone(), memo.spending_txid.as_deref().map(str::to_ascii_lowercase));
            let chain: HashMap<String, HopChainWord> = [(key.clone(), stale_memo_word(memo, now, Some(clear)))].into_iter().collect();
            let mut sweeps: HashMap<String, crate::hopsweep::FiledHopSweep> = filed
                .map(|f| (key.clone(), crate::hopsweep::FiledHopSweep { sweep_txid: f.to_string(), raw_hex: "0100".repeat(20), pays_sats: Some(20_000), index_proven: true, index_proof_height: Some(899_990) }))
                .into_iter()
                .collect();
            let set_aside = set_aside_proofs_a_refused_word_contradicts(&mut sweeps, &refused_words);
            let outs = pays(spender, &[(0, &my_pkh, 20_000, None)]);
            let ins: HashMap<String, Vec<(String, u32)>> = [(spender.to_string(), vec![(tx(0x07), 0)])].into_iter().collect();
            let couriers: HashSet<String> = if courier { [spender.to_string()].into_iter().collect() } else { HashSet::new() };
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.hop_chain = &chain;
            i.hop_sweeps = &sweeps;
            i.spender_outputs = &outs;
            i.spender_inputs = &ins;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = &couriers;
            let mut rows = derive_owed_rows(&i);
            assert_eq!(rows.len(), 1);
            assert_eq!((rows[0].family, rows[0].facts["claimable"].as_bool(), rows[0].facts["claimReason"].as_str()), (OwedFamily::Payout, Some(false), Some(UNCONFIRMED_PAYOUT_REASON)), "the row the mark renames");
            assert_eq!(rows[0].facts["chainWait"], "block");
            mark_refused_word_rows(&mut rows, &refused_words, &set_aside);
            rows.remove(0)
        };
        // 1. COURIER BYTES: no credit can be assembled here, and the row never says one is coming
        let row = walk(&memo_naming(&sweep), &sweep, true, None);
        assert_eq!(row.facts["creditKind"], "courier-bytes");
        assert_eq!(row.facts["claimReason"], CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON);
        assert_eq!(row.facts[OWED_FACT_CHAIN_WORD_AWAITS_PROBE], true);
        assert_eq!(row.facts["chainWait"], OWED_CHAIN_WAIT_PROBE);
        assert!(row.facts.get("sweepProofContradicted").is_none());
        //    the same row on a FRESH confirmed word: the sentence the wait now agrees with
        let fresh = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let outs = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let ins: HashMap<String, Vec<(String, u32)>> = [(sweep.clone(), vec![(tx(0x07), 0)])].into_iter().collect();
        let couriers: HashSet<String> = [sweep.clone()].into_iter().collect();
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &fresh;
        i.spender_outputs = &outs;
        i.spender_inputs = &ins;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &couriers;
        assert_eq!(derive_owed_rows(&i)[0].facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON);
        // 2. THE PROOF SET ASIDE: a proven filing, a refused word naming a rival whose bytes pay this home
        let row = walk(&memo_naming(&rival), &rival, false, Some(&sweep));
        assert_eq!(row.facts["sweepProofContradicted"], true);
        assert_eq!(row.facts["claimReason"], CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON);
        assert_eq!(row.facts[OWED_FACT_CHAIN_WORD_AWAITS_PROBE], true);
        assert_eq!(row.facts["chainWait"], OWED_CHAIN_WAIT_PROBE);
        //    and it outranks the courier-bytes word (the filed sweep's credit may yet stand)
        let row = walk(&memo_naming(&rival), &rival, true, Some(&sweep));
        assert_eq!((row.facts["creditKind"].as_str(), row.facts["claimReason"].as_str()), (Some("courier-bytes"), Some(CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON)));
        // 3. AN INDEX-HELD SWEEP, nothing set aside: the one row a credit can be offered on keeps the promise
        let row = walk(&memo_naming(&sweep), &sweep, false, None);
        assert!(row.facts.get("creditKind").is_none() && row.facts.get("sweepProofContradicted").is_none());
        assert_eq!(row.facts["claimReason"], CHAIN_WORD_AWAITS_PROBE_REASON);
        assert_eq!(row.facts["chainWait"], OWED_CHAIN_WAIT_PROBE);
        // the words: only the third speaks of a credit being offered; all three name the reorg and the re-ask
        assert!(CHAIN_WORD_AWAITS_PROBE_REASON.contains("the credit is offered"));
        for other in [CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON, CHAIN_WORD_AWAITS_PROBE_CONTRADICTED_REASON] {
            assert!(!other.contains("offered"), "{other}");
            assert!(other.contains("read just after a reorg") && other.contains("asked again in turn"), "{other}");
        }
        assert!(CHAIN_WORD_AWAITS_PROBE_NO_CREDIT_REASON.contains("no credit can be assembled here"));
        assert_ne!(OWED_CHAIN_WAIT_PROBE, "block");
    }

    /// The merged lens's LOW-3: A FAULTED PROBE MEMO READ IS COUNTED UNDER ITS OWN NAME, on the isolate's `/health`
    /// and as a durable row the overlay seeds and serves on `/health/invariants.counters`. The row is written with
    /// the shipped counter upsert over the shipped schema (real SQLite); the route's two fault arms call the
    /// counter before they answer empty (the source pin in `hops_view`, beside the all-or-nothing one).
    /// To red: drop `probe_memo_read_faulted` from either fault arm, or rename either constant alone.
    #[test]
    fn a_faulted_probe_memo_read_is_counted_on_health_and_in_the_overlays_counters_real_sqlite() {
        assert_eq!(COUNTER_PROBE_MEMO_READ_FAULTS, bsv_overlay_cloudflare::hop_probe_memos::COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS, "the overlay seeds the row this worker writes");
        let before = owed_health_json()["probeMemoReadFaults"].as_u64().expect("the count is on /health.owed");
        note_probe_memo_read_fault();
        assert_eq!(owed_health_json()["probeMemoReadFaults"].as_u64().unwrap(), before + 1);
        let conn = memo_db();
        for _ in 0..2 {
            conn.execute(crate::beef_guard::BUMP_COUNTER_SQL, rusqlite::params![COUNTER_PROBE_MEMO_READ_FAULTS, 1i64]).unwrap();
        }
        let held: i64 = conn.query_row("SELECT value FROM ops_counters WHERE name = ?1", [COUNTER_PROBE_MEMO_READ_FAULTS], |r| r.get(0)).unwrap();
        assert_eq!(held, 2, "one per faulted read");
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        assert!(routes.contains(&squash("crate::owed::note_probe_memo_read_fault(); let binds = [JsValue::from_str(crate::owed::COUNTER_PROBE_MEMO_READ_FAULTS), JsValue::from_f64(1.0)];")));
    }

    /// bsv-low #485 (merge fold, the merged lens's MEDIUM-1): THE OVERLAY'S TWO DELETES NEVER TAKE THE HOME LATCH
    /// OR THE HOME CURSOR. All three kinds of row live in `hop_chain_probes`, and the overlay's reorg clear
    /// (`DELETE ... WHERE spentConfirmed = 1`) and TTL sweep (`DELETE ... WHERE probedAtMs <= ?`) took them with
    /// the memos: a latch written from a confirmed probe went on the first reorg evidence, any latch and the
    /// cursor two hours and one block event after they were written, so a retired courier-proven row came back
    /// and was bought again from the couriers, two a recompute, and the ring started at its head for a seat that
    /// reads less often than two hours. Executed here on real SQLite over the shipped migrations: the latches are
    /// `home_word`'s own (a real signature, one probe confirmed and one not), the cursor is `home_cursor_memo`'s,
    /// all written through `PROBE_MEMO_UPSERT_SQL`; then the overlay's shipped statements run, far past every
    /// window; the latches are read back through the route's read and `latched_home_words`, the cursor through
    /// `home_cursor_of`. The chain memos beside them still go (the sweep and the clear keep their job) and the
    /// tombstone keeps its own exemption.
    /// To red: drop either prefix from either DELETE (on `7868785` the clear took the confirmed latch and the
    /// sweep took the other latch and the cursor).
    #[test]
    fn the_overlays_reorg_clear_and_ttl_sweep_leave_the_home_latch_and_the_home_cursor_real_sqlite() {
        use crate::hops_view::{probe_memo_key, ChainSpendProbe, ProbeMemo};
        use bsv_overlay_cloudflare::hop_probe_memos::{
            HOP_PROBE_MEMO_APP_LAYER_PREFIXES, HOP_PROBE_MEMO_EXPIRE_SQL, HOP_PROBE_MEMO_REORG_CLEAR_SQL, HOP_PROBE_MEMO_REORG_MARK, HOP_PROBE_MEMO_REORG_MARK_SQL,
        };
        assert_eq!(HOP_PROBE_MEMO_APP_LAYER_PREFIXES, [HOME_SPEND_LATCH_PREFIX, HOME_WALK_CURSOR_PREFIX], "the overlay spares exactly the prefixes this crate writes");
        let conn = memo_db();
        let t0: i64 = 1_800_000_000_000;
        let home = bsv_rs::primitives::PrivateKey::random();
        let pkh = hex::encode(home.public_key().hash160());
        let (sweep_a, spend_a) = sweep_and_home_spend(&home, &home, 20_000);
        let (sweep_b, spend_b) = sweep_and_home_spend(&home, &home, 30_000);
        let candidates = [
            CourierHomeOutput { sweep_txid: sweep_a.clone(), vout: 0, pkh_hex: pkh.clone(), sats: 20_000 },
            CourierHomeOutput { sweep_txid: sweep_b.clone(), vout: 0, pkh_hex: pkh.clone(), sats: 30_000 },
        ];
        let spent = |confirmed: Option<bool>| ChainSpendProbe { known: true, spent: Some(true), spending_txid: Some(tx(0x5a)), spent_confirmed: confirmed };
        // the latch of a CONFIRMED probe (`spentConfirmed = 1`: the reorg clear's own predicate) and of an unconfirmed one
        let (_, latch_a) = home_word(&spent(Some(true)), Some(&spend_a), &candidates[0], t0).expect("a word");
        let (_, latch_b) = home_word(&spent(None), Some(&spend_b), &candidates[1], t0).expect("a word");
        let (latch_a, latch_b) = (latch_a.expect("a proof latches"), latch_b.expect("a proof latches"));
        assert_eq!(latch_a.spent_confirmed, Some(true));
        let cursor = home_cursor_memo(ME, &candidates[1], t0);
        // the chain rung's own memos of the same two outputs, and the tombstone
        let chain = |sweep: &str, confirmed: Option<bool>| ProbeMemo { outpoint: probe_memo_key(sweep, 0), probed_at_ms: t0, spent: true, spending_txid: Some(tx(0x5a)), spent_confirmed: confirmed };
        write_memos(&conn, &[latch_a, latch_b, cursor, chain(&sweep_a, Some(true)), chain(&sweep_b, None)]);
        conn.execute(HOP_PROBE_MEMO_REORG_MARK_SQL, rusqlite::params![t0]).unwrap();
        let held = |conn: &rusqlite::Connection| -> usize { conn.query_row("SELECT COUNT(*) FROM hop_chain_probes", [], |r| r.get::<_, i64>(0)).unwrap() as usize };
        assert_eq!(held(&conn), 6);
        let latch_targets: Vec<(String, u32)> = candidates.iter().map(|c| home_latch_target(&c.sweep_txid, c.vout)).collect();
        let both_latched = |conn: &rusqlite::Connection| -> bool {
            let (words, targets) = latched_home_words(&candidates, &read_memos(conn, &latch_targets));
            targets.is_empty() && words.len() == 2 && words.values().all(|w| *w == HomeSpendWord::Proven)
        };
        let cursor_at_b = |conn: &rusqlite::Connection| -> bool { home_cursor_of(&read_memos(conn, &[home_cursor_target(ME)]), ME) == Some(probe_memo_key(&sweep_b, 0)) };
        // THE REORG CLEAR: the confirmed chain memo goes, nothing else (it took the confirmed latch too)
        assert_eq!(conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, []).unwrap(), 1, "the one confirmed chain memo: a signature proof is not a chain word, a reorg does not unmake it");
        assert!(both_latched(&conn), "the reorg clear took a latch");
        assert!(cursor_at_b(&conn));
        // THE TTL SWEEP, with a bound past every row: the other chain memo goes, nothing else (it took both latches and the cursor)
        assert_eq!(conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![i64::MAX]).unwrap(), 1, "the one chain memo left");
        assert!(both_latched(&conn), "the TTL sweep took a latch: the retired row is back and is bought again from the couriers");
        assert!(cursor_at_b(&conn), "the TTL sweep took the cursor: the ring starts at its head again");
        assert_eq!(read_memos(&conn, &[crate::hops_view::reorg_mark_target()]).len(), 1, "the tombstone keeps its own exemption");
        assert_eq!(held(&conn), 4, "two latches, the cursor, the tombstone");
        assert!(!HOP_PROBE_MEMO_APP_LAYER_PREFIXES.iter().any(|p| HOP_PROBE_MEMO_REORG_MARK.starts_with(p)));
    }

    /// The delta fold's LOW-1 (cut 1) and LOW-2 (the stored-read cap), EXECUTED on real SQLite: twelve home outputs
    /// the chain rung names spent, the first eight by spenders whose bytes nobody holds (they never prove), the
    /// last four by real spends the home key signed, sitting in the index's stored BEEF. Pass 1 spends exactly the
    /// cap on the head and moves the cursor to the ninth; pass 2 starts there and the four prove and latch. At
    /// `762e621` the ninth never got a stored read (`/tmp/b1-fold-2/red-item1-4-762e621.log`).
    /// To red: start every pass at the head (ignore the cursor), or set `OWED_HOME_STORED_READS_PER_RECOMPUTE` to 0.
    #[test]
    fn an_unprovable_head_cannot_hold_the_stored_reads_against_the_tail_real_sqlite() {
        use crate::hops_view::{probe_memo_key, ProbeMemo};
        let conn = memo_db();
        let home = bsv_rs::primitives::PrivateKey::random();
        let pkh = hex::encode(home.public_key().hash160());
        let mut made: Vec<(CourierHomeOutput, Vec<u8>)> = (0..12u64)
            .map(|i| {
                let (sweep, spend) = sweep_and_home_spend(&home, &home, 20_000 + i);
                (CourierHomeOutput { sweep_txid: sweep, vout: 0, pkh_hex: pkh.clone(), sats: 20_000 + i }, spend)
            })
            .collect();
        made.sort_by(|a, b| a.0.sweep_txid.cmp(&b.0.sweep_txid)); // `courier_home_outputs`' order
        let candidates: Vec<CourierHomeOutput> = made.iter().map(|(c, _)| c.clone()).collect();
        let spender_of = |i: usize| bsv_rs::transaction::Transaction::from_binary(&made[i].1).unwrap().id();
        let mut world = FakeCouriers::default();
        for (i, (c, spend)) in made.iter().enumerate() {
            // every output: a confirmed spend the chain rung already memoised (no probe is owed)
            write_memos(&conn, &[ProbeMemo { outpoint: probe_memo_key(&c.sweep_txid, 0), probed_at_ms: 1_000, spent: true, spending_txid: Some(spender_of(i)), spent_confirmed: Some(true) }]);
            if i < 8 {
                world.held.insert(spender_of(i), None); // the isolate holds the resolver's answer: no bytes
            } else {
                world.stored.insert(spender_of(i), spend.clone());
            }
        }
        assert!(candidates.len() > OWED_HOME_STORED_READS_PER_RECOMPUTE);

        // pass 1: the cap, honoured, on the head; nothing proves; the ninth is the first output withheld
        let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 2_000);
        assert_eq!(window, candidates);
        assert_eq!(pass.stored_reads, OWED_HOME_STORED_READS_PER_RECOMPUTE, "exactly the cap");
        assert_eq!(world.stored_asked, (0..8).map(spender_of).collect::<Vec<_>>());
        assert!(pass.words.values().all(|w| *w == HomeSpendWord::Unproven), "{:?}", pass.words);
        assert_eq!(pass.first_withheld, Some(8));
        assert_eq!(pass.resolver_asks, OWED_HOME_PROBES_PER_RECOMPUTE, "the resolver's cap, honoured: the ninth and tenth asked, no more");
        assert_eq!(world.resolver_asked, vec![spender_of(8), spender_of(9)]);
        // the cursor row, read back through the shipped statement under the key the next pass reads
        assert_eq!(home_cursor_of(&read_memos(&conn, &[home_cursor_target(ME)]), ME), Some(probe_memo_key(&candidates[8].sweep_txid, 0)));

        // pass 2: starts at the ninth; the tail's four spends are read, verified, latched
        world.stored_asked.clear();
        let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 3_000);
        assert_eq!(window[0], candidates[8], "the ring starts where the last pass was cut");
        assert_eq!(pass.stored_reads, OWED_HOME_STORED_READS_PER_RECOMPUTE);
        assert_eq!(world.stored_asked[..4], (8..12).map(spender_of).collect::<Vec<_>>()[..]);
        for c in &candidates[8..] {
            assert_eq!(pass.words.get(&outpoint_key(&c.sweep_txid, 0)), Some(&HomeSpendWord::Proven), "within two passes");
        }
        assert_eq!(pass.memos.iter().filter(|m| m.outpoint.starts_with(HOME_SPEND_LATCH_PREFIX)).count(), 4);

        // pass 3: the four are latched (no read, no ask); the eight that never prove are still looked at, none starved
        world.stored_asked.clear();
        let (pass, _) = home_pass_on_sqlite(&conn, &mut world, &candidates, 4_000);
        assert_eq!(pass.stored_reads, 8);
        assert_eq!(pass.first_withheld, None);
        assert_eq!(pass.words.values().filter(|w| **w == HomeSpendWord::Proven).count(), 4);
        assert_eq!(pass.words.values().filter(|w| **w == HomeSpendWord::Unproven).count(), 8);
    }

    /// The delta fold's LOW-1 (cut 2, the chain rung): two outputs the couriers can never answer leave no memo and,
    /// at the head of a fixed order, were asked on every pass with nothing behind them ever probed (the hop walk's
    /// gate M2 of 2026-09-19). On the ring every output is probed within ceil(n / cap) passes, the cap honoured
    /// on each.
    /// To red: start every pass at the head, or raise the per-pass probe count past its constant.
    #[test]
    fn two_unanswerable_outputs_at_the_head_cannot_hold_the_chain_probes_real_sqlite() {
        let conn = memo_db();
        let candidates = plain_candidates(5);
        let mut world = FakeCouriers::default();
        for c in &candidates[2..] {
            world.probes.insert(outpoint_key(&c.sweep_txid, 0), unspent_probe());
        }
        let key = |i: usize| outpoint_key(&candidates[i].sweep_txid, 0);
        let (pass, _) = home_pass_on_sqlite(&conn, &mut world, &candidates, 1_000);
        assert_eq!((pass.probes, pass.first_withheld), (OWED_HOME_PROBES_PER_RECOMPUTE, Some(2)));
        assert_eq!(world.probed, vec![key(0), key(1)]);
        assert!(pass.words.is_empty(), "an unanswered probe names nothing");
        let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 2_000);
        assert_eq!(window[0], candidates[2]);
        assert_eq!(pass.probes, OWED_HOME_PROBES_PER_RECOMPUTE);
        assert_eq!(world.probed[2..], [key(2), key(3)]);
        assert_eq!((pass.words.get(&key(2)), pass.words.get(&key(3))), (Some(&HomeSpendWord::Unspent), Some(&HomeSpendWord::Unspent)));
        let (pass, _) = home_pass_on_sqlite(&conn, &mut world, &candidates, 3_000);
        assert_eq!(pass.probes, OWED_HOME_PROBES_PER_RECOMPUTE);
        assert_eq!(world.probed[4..], [key(4), key(0)], "the fifth, then the ring wraps to the head");
        let seen: HashSet<&String> = world.probed.iter().collect();
        assert_eq!(seen.len(), 5, "every output probed within three passes of two");
        // a pass past its clock asks nothing and moves nothing
        let before = home_cursor_of(&read_memos(&conn, &[home_cursor_target(ME)]), ME);
        let asked = world.probed.len();
        world.over_budget = true;
        let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 400_000);
        assert_eq!((pass.probes, pass.stored_reads, pass.resolver_asks, world.probed.len()), (0, 0, 0, asked));
        assert_eq!(pass.first_withheld, Some(0));
        assert_eq!(home_cursor_of(&read_memos(&conn, &[home_cursor_target(ME)]), ME), before);
        assert_eq!(Some(crate::hops_view::probe_memo_key(&window[0].sweep_txid, 0)), before);
    }

    /// The delta fold's LOW-1 (cut 3, the window) and the bound itself: an identity with MORE home outputs than one
    /// pass looks at. Forty-five latched outputs at the head no longer hide the five behind them, and with fifty
    /// outputs no courier can ever answer (none ever proves), every one is tried within ceil(50 / 2) passes, no
    /// pass over its caps or its window.
    /// To red: cut the candidates to the first `OWED_HOME_WINDOW` (ignore the cursor).
    #[test]
    fn more_home_outputs_than_the_window_are_all_tried_within_a_bounded_number_of_passes_real_sqlite() {
        use crate::hops_view::{probe_memo_key, ProbeMemo};
        let candidates = plain_candidates(50);
        assert!(candidates.len() > OWED_HOME_WINDOW);
        let key = |i: usize| outpoint_key(&candidates[i].sweep_txid, 0);

        // (a) forty-five latched at the head, five the chain rung answers behind them
        let conn = memo_db();
        for c in &candidates[..45] {
            let (latch_txid, vout) = home_latch_target(&c.sweep_txid, c.vout);
            write_memos(&conn, &[ProbeMemo { outpoint: probe_memo_key(&latch_txid, vout), probed_at_ms: 500, spent: true, spending_txid: Some(tx(0x5a)), spent_confirmed: Some(true) }]);
        }
        let mut world = FakeCouriers::default();
        for c in &candidates[45..] {
            world.probes.insert(outpoint_key(&c.sweep_txid, 0), unspent_probe());
        }
        let mut words: HashMap<String, HomeSpendWord> = HashMap::new();
        for pass_no in 0..4i64 {
            let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 1_000 + pass_no);
            assert_eq!(window.len(), OWED_HOME_WINDOW);
            assert!(pass.probes <= OWED_HOME_PROBES_PER_RECOMPUTE);
            if pass_no == 0 {
                assert!(world.probed.is_empty() && pass.first_withheld.is_none(), "the head's window is all latches: the cursor moves past it");
            }
            words.extend(pass.words);
        }
        assert_eq!(world.probed, (45..50).map(key).collect::<Vec<_>>(), "the five behind the window, each probed once");
        assert_eq!(words.len(), 50, "every output has a word within four passes");
        assert_eq!(words.values().filter(|w| **w == HomeSpendWord::Unspent).count(), 5);

        // (b) fifty outputs, none answerable: two per pass, all fifty within twenty-five passes
        let conn = memo_db();
        let mut world = FakeCouriers::default();
        for pass_no in 0..25i64 {
            let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 1_000 + pass_no);
            assert_eq!((window.len(), pass.probes), (OWED_HOME_WINDOW, OWED_HOME_PROBES_PER_RECOMPUTE), "pass {pass_no}");
        }
        assert_eq!(world.probed.len(), 50);
        assert_eq!(world.probed.iter().collect::<HashSet<_>>().len(), 50, "every candidate tried within ceil(50 / 2) passes");
    }

    /// The ring's two pure ends: where a pass starts and where the next one will.
    #[test]
    fn the_home_walks_window_starts_at_the_cursor_and_wraps() {
        let c = plain_candidates(4);
        let k = |i: usize| crate::hops_view::probe_memo_key(&c[i].sweep_txid, 0);
        assert_eq!(home_walk_window(&c, None, 45), c);
        assert_eq!(home_walk_window(&c, Some(&k(2)), 45), vec![c[2].clone(), c[3].clone(), c[0].clone(), c[1].clone()]);
        assert_eq!(home_walk_window(&c, Some(&k(3).to_ascii_uppercase()), 2), vec![c[3].clone(), c[0].clone()]);
        // a cursor naming an output that left the list lands on its successor; past the end, or malformed: the head
        assert_eq!(home_walk_window(&c, Some(&format!("{:064x}.7", 0x1001)), 1), vec![c[2].clone()]);
        assert_eq!(home_walk_window(&c, Some(&format!("{}.0", "ff".repeat(32))), 1), vec![c[0].clone()]);
        assert_eq!(home_walk_window(&c, Some("not a cursor"), 1), vec![c[0].clone()]);
        assert!(home_walk_window(&[], Some(&k(0)), 45).is_empty());
        // the next start: the first withheld output; else the one after the window; a whole ring with nothing withheld stays
        let w = home_walk_window(&c, Some(&k(2)), 2);
        assert_eq!(home_cursor_after(&c, &w, Some(1)), Some(&c[3]));
        assert_eq!(home_cursor_after(&c, &w, None), Some(&c[0]));
        assert_eq!(home_cursor_after(&c, &home_walk_window(&c, Some(&k(2)), 45), None), None);
        // the cursor row is no latch and no chain word
        let memo = home_cursor_memo(ME, &c[3], 9_000);
        assert_eq!((memo.outpoint.as_str(), memo.spent, memo.spending_txid.as_deref()), (format!("{HOME_WALK_CURSOR_PREFIX}{ME}.0").as_str(), false, Some(k(3).as_str())));
        assert!(latched_home_words(&c, std::slice::from_ref(&memo)).0.is_empty());
    }

    /// The delta fold's LOW-4: a SUPERSEDED read whose stored read faults as well (here: no marker row, the shipped
    /// read statement on real SQLite returns nothing) is answered its own older rows with NO claimable word: the
    /// newer walk may have retired the press. A row that was not claimable keeps its own reason; a stored snapshot,
    /// when there is one, is served untouched.
    /// To red: have `superseded_read_snapshot` fall back to the walk's own rows as they are.
    #[test]
    fn a_superseded_read_whose_stored_read_faults_serves_no_claimable_word_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        conn.execute_batch(OWED_ROWS_CREATE).unwrap();
        conn.execute_batch(OWED_STATE_CREATE).unwrap();
        let row = |n: u8, family: OwedFamily, facts: Value| OwedRow {
            identity: ME.to_string(),
            outpoint: format!("{}:0", tx(n)),
            family,
            game_id: tx(0x01),
            sats: Some(20_190),
            opponent_identity: Some(OPP.to_string()),
            at_height: None,
            facts,
            reason: None,
        };
        let older = vec![
            row(0x07, OwedFamily::HopStranded, json!({ "claim": "sweep-hop", "claimable": true, "joinRefusedBy": "door" })),
            row(0x08, OwedFamily::Payout, json!({ "claim": "internalize", "claimable": true, "creditBeef": "/credit-beef/x" })),
            row(0x09, OwedFamily::Payout, json!({ "claim": "internalize", "claimable": false, "claimReason": UNCONFIRMED_PAYOUT_REASON })),
            row(0x0a, OwedFamily::Unbound, json!({ "claim": null })),
        ];
        // the route's stored read (`owed_read_stored`): the marker first; no marker = no snapshot
        let marker = conn.query_row(OWED_STATE_READ_SQL, [ME], |r| r.get::<_, i64>(1));
        assert!(matches!(marker, Err(rusqlite::Error::QueryReturnedNoRows)));
        assert_eq!(read_answer_after_compute(true), ReadAnswer::Stored);
        let (rows, tip, at, cut) = superseded_read_snapshot(None, (older.clone(), Some(900_000), 1_000, false));
        assert_eq!((rows.len(), tip, at, cut), (4, Some(900_000), 1_000, false));
        assert!(rows.iter().all(|r| r.facts.get("claimable") != Some(&json!(true))), "no claimable word on the double fault: {rows:?}");
        for r in &rows[..2] {
            assert_eq!((r.facts["claimable"].clone(), r.facts["claimReason"].clone(), r.facts["supersededFallback"].clone()), (json!(false), json!(SUPERSEDED_FALLBACK_REASON), json!(true)));
        }
        assert_eq!((rows[0].facts["claim"].clone(), rows[0].facts["joinRefusedBy"].clone(), rows[1].facts["creditBeef"].clone()), (json!("sweep-hop"), json!("door"), json!("/credit-beef/x")), "every other fact stands");
        assert_eq!((&rows[2], &rows[3]), (&older[2], &older[3]), "a row with no claimable word is served as it was");
        assert!(!owed_body(ME, tip, &rows, at, cut).contains("\"claimable\":true"));
        // with a stored snapshot the reader gets it, untouched
        let newer = vec![row(0x0b, OwedFamily::HopStranded, json!({ "claim": "sweep-hop", "claimable": true }))];
        assert_eq!(superseded_read_snapshot(Some((newer.clone(), Some(900_001), 71_000, true)), (older, Some(900_000), 1_000, false)), (newer, Some(900_001), 71_000, true));
    }

    /// bsv-low #499: THE CEILING SHEDS. Twelve recomputes of one identity inside a minute run; the thirteenth is
    /// shed (the caller serves the snapshot and counts it); another identity is untouched; the first read of an
    /// identity (no snapshot to serve) runs over the ceiling and still counts; the window past, the identity runs
    /// again. RED on `b1ad9a1`: there was no ceiling (every ask walked).
    #[test]
    fn the_thirteenth_recompute_of_one_identity_inside_a_minute_is_shed_to_the_snapshot() {
        let mut rate = RecomputeRate::default();
        let (me, other) = ("02".repeat(33), "03".repeat(33));
        let t0 = 1_790_000_000_000_i64;
        for i in 0..OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW {
            assert!(rate.admit(&me, t0 + i64::from(i) * 1_000, true), "recompute {} of the minute runs", i + 1);
        }
        assert_eq!(OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW, 12);
        assert_eq!(rate.at_ceiling(t0 + 12_000), 1);
        let shed_before = recompute_shed_total();
        // the 13th inside the minute: shed (the route counts it and serves the snapshot)
        assert!(!rate.admit(&me, t0 + 59_999, true), "the 13th recompute inside the minute must be shed");
        note_recompute_shed();
        assert!(recompute_shed_total() > shed_before, "a shed is counted on /health (recomputeShed)");
        assert_eq!(rate.in_window(&me, t0 + 59_999), 12, "a shed is not a run");
        // a stranger's storm is not mine, and mine is not theirs
        assert!(rate.admit(&other, t0 + 30_000, true));
        // a first read always runs (there is no snapshot to shed to) and counts
        assert!(rate.admit(&me, t0 + 59_999, false));
        assert_eq!(rate.in_window(&me, t0 + 59_999), 13);
        // the window past: runs again, counted from one
        assert!(rate.admit(&me, t0 + 60_000, true));
        assert_eq!(rate.in_window(&me, t0 + 60_000), 1);
        assert_eq!(rate.at_ceiling(t0 + 60_000), 0);
    }

    /// bsv-low #499: EVERY WALK ASKS THE CEILING FIRST. The pure counter above sheds nothing unless the route asks
    /// it before each `owed_recompute`: the hooks' and claims' one entry (`owed_recompute_and_push`), the read's
    /// inline arm, and the first read (which counts and never sheds). `owed_recompute(` is called at exactly the two
    /// sites below, so a new caller that skips the ask breaks the count here. To red: delete the ask in
    /// `owed_recompute_and_push` (the walk then runs past the ceiling).
    #[test]
    fn every_owed_recompute_caller_asks_the_ceiling_before_it_walks() {
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let code: String = include_str!("routes.rs")
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let routes = squash(&code);
        let push = routes.find(&squash("pub(crate) async fn owed_recompute_and_push(")).expect("the hooks' entry");
        let push_body = &routes[push..];
        let ask = push_body.find(&squash("if !owed_recompute_admit(identity_lc, source, true) { return; }")).expect("the ask");
        let walk = push_body.find(&squash("owed_recompute(env, db, identity_lc, source, tip_hint)")).expect("the walk");
        assert!(ask < walk, "owed_recompute_and_push must ask the ceiling before it walks");
        assert!(routes.contains(&squash("owed_recompute_admit(&identity, \"read-first\", false); match owed_compute_on_read(")));
        assert!(routes.contains(&squash("if !owed_recompute_admit(&identity, source, true) { (rows, prev_tip, st.computed_at_ms as i64, cut) } else { match owed_compute_on_read(&ctx.env, &db, &identity, source, tip_now)")));
        assert_eq!(routes.matches(&squash("owed_recompute(env, db, identity_lc,")).count(), 2, "the walk's two callers (the push and the read's compute)");
    }
}
