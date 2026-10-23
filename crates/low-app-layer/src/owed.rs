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
pub fn door_refused_hops(
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
/// The courier probes ONE recompute may buy for the home outputs of its courier-proven payouts (each is one
/// spent-any ladder, memoised like the hops', plus one tx-any read for a named spender's bytes).
pub const OWED_HOME_PROBES_PER_RECOMPUTE: usize = 2;
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
    (
        format!(
            "{HOME_WALK_CURSOR_PREFIX}{}",
            identity_lc.to_ascii_lowercase()
        ),
        0,
    )
}

/// PURE: the cursor the memo read returned for `identity_lc` (the memo key of the output to start at), if any.
pub fn home_cursor_of(memos: &[crate::hops_view::ProbeMemo], identity_lc: &str) -> Option<String> {
    let (txid, vout) = home_cursor_target(identity_lc);
    let key = crate::hops_view::probe_memo_key(&txid, vout);
    memos
        .iter()
        .filter(|m| m.outpoint == key)
        .max_by_key(|m| m.probed_at_ms)
        .and_then(|m| m.spending_txid.clone())
}

/// PURE: the cursor row that makes `next` the start of the identity's next pass.
pub fn home_cursor_memo(
    identity_lc: &str,
    next: &CourierHomeOutput,
    now_ms: i64,
) -> crate::hops_view::ProbeMemo {
    let (txid, vout) = home_cursor_target(identity_lc);
    crate::hops_view::ProbeMemo {
        outpoint: crate::hops_view::probe_memo_key(&txid, vout),
        probed_at_ms: now_ms,
        spent: false,
        spending_txid: Some(crate::hops_view::probe_memo_key(
            &next.sweep_txid,
            next.vout,
        )),
        spent_confirmed: None,
        tip_height: None,
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
pub fn home_walk_window(
    candidates: &[CourierHomeOutput],
    cursor: Option<&str>,
    window: usize,
) -> Vec<CourierHomeOutput> {
    let n = candidates.len();
    let at = cursor
        .and_then(|c| c.rsplit_once('.'))
        .and_then(|(txid, vout)| Some((txid.to_ascii_lowercase(), vout.parse::<u32>().ok()?)));
    let start = at
        .and_then(|(txid, vout)| {
            candidates
                .iter()
                .position(|c| (c.sweep_txid.to_ascii_lowercase(), c.vout) >= (txid.clone(), vout))
        })
        .unwrap_or(0);
    (0..n.min(window))
        .map(|k| candidates[(start + k) % n].clone())
        .collect()
}

/// PURE: where the NEXT pass starts. The first output of the window the pass WITHHELD something from (a cap or
/// the clock stood between it and a look it was owed); with nothing withheld, the candidate after the window. A
/// window that held every candidate and withheld nothing leaves the cursor where it is (`None`: no write).
pub fn home_cursor_after<'a>(
    candidates: &'a [CourierHomeOutput],
    window: &'a [CourierHomeOutput],
    first_withheld: Option<usize>,
) -> Option<&'a CourierHomeOutput> {
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
    let mut answered: HashMap<String, crate::hops_view::ChainSpendProbe> = answered
        .into_iter()
        .map(|(t, v, p)| (outpoint_key(&t, v), p))
        .collect();
    let mut pass = HomePass {
        words,
        ..HomePass::default()
    };
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
                pass.memos.extend(crate::hops_view::probe_memo_of(
                    &home.sweep_txid,
                    home.vout,
                    &p,
                    now_ms,
                ));
                Some(p)
            }
            None => {
                withheld = true;
                None
            }
        };
        if let Some(probe) = probe {
            let mut raw: Option<Vec<u8>> = None;
            if let (true, Some(true), Some(sp)) = (
                probe.known,
                probe.spent,
                probe.spending_txid.as_deref().map(str::to_ascii_lowercase),
            ) {
                if pass.stored_reads < OWED_HOME_STORED_READS_PER_RECOMPUTE && !world.over_budget()
                {
                    pass.stored_reads += 1;
                    raw = world.stored_spender(&sp).await;
                } else {
                    withheld = true;
                }
                if raw.is_none() {
                    raw = match world.cached_spender(&sp) {
                        Some(held) => held,
                        None if pass.resolver_asks < OWED_HOME_PROBES_PER_RECOMPUTE
                            && !world.over_budget() =>
                        {
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

/// bsv-low #669: the sweep claim of a `hop-stranded` row was computed against a chain probe taken at an EARLIER tip
/// than the one being served (a block has arrived since, or the present tip cannot be read at all). The chain probe
/// is no longer provably current, so the claim is no longer trustworthy: the served row degrades to a `pending`
/// chain-wait (`claimable:false`, no press), never a dead sweep. A fresh probe at the new tip re-points it to the
/// claim (still unspent) or to `spent-elsewhere` (a competitor took it).
pub const SWEEP_TIP_MOVED_REASON: &str =
    "the chain moved since this was judged; re-checking the hop against the new block";

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

// bsv-low #800 (FIX-800-LOCK): THE WALK'S OWN DEADLINE BOUNDS THE LOCK. The recompute runs in `wait_until`, which the
// platform cancels 30 s after the response; a walk cut there never ran its release, so the identity's lock stood
// until a later ask found it 60 s silent (loops 39 to 41: 106 to 675 s, a filing's row not on `/owed` in the cell's
// 90 to 120 s). Each walk now runs inside a SLICE of its invocation: an outward budget (the 12 s above, or half the
// slice when less is left), then a hard deadline at which the walk future is dropped by the claim itself (the drop the
// platform would make, made early, so the claim can release and mark the identity stale), and a reserve after it for
// the release, the stale mark and the push. A walk is not started on less than the minimum slice: the identity is
// marked stale and the next read walks it. No live claim then goes untouched longer than one hard deadline, so the
// take-over bound is the platform's own 30 s, and a take-over is only ever an abandoned future (counted).

/// The platform's `wait_until` ceiling: a detached recompute is cancelled this long after the response.
pub const OWED_PLATFORM_WAIT_UNTIL_MS: i64 = 30_000;
/// What a claim needs after its walk: the release (synchronous), one stale mark (one D1 statement) and the
/// `owed-changed` push (one actor fetch), or for a dropped walk the alarm's arm (one actor fetch). Kept out of every
/// slice. The FIX-800 delta lens's LOW-A, folded and stated: a claim now pushes ONE `owed-changed` (its last landed
/// walk's, `routes::owed_told_last`), so the tail after a walk is at most the arm, the mark, one push and a nudge
/// only when the arm failed. The mark is NOT bounded: a bounded mark can be dropped unlanded, which turns the next read
/// into a no-op and the page silent (MEDIUM-1's class), where an unbounded one only risks the trailing push past 30 s.
/// The band, stated: a mark slower than the reserve (the overload that dropped the walk) can cost that push; a
/// dropped walk arms the alarm BEFORE its mark, so its arm is not the one cut, and the alarm's walk pushes. The second
/// delta lens's LOW-A, kept: an overrun past 30 s that cuts even the ARM (one actor fetch, no D1) leaves the isolate's
/// map armed and handed with no alarm behind it: that window's sheds re-arm nothing and the page gets no push, and if no
/// earlier mark landed the reads serve the old rows as fresh until the 5 min age; the hand-off still ends at its
/// `until` (60 s past the drop when this claim armed). A shed hook (MEDIUM-B) adds one actor fetch (its note) before
/// its mark and a nudge after it, in the same reserve.
pub const OWED_RELEASE_RESERVE_MS: i64 = 3_000;
/// The hard deadline of ONE claimed walk, derived ONCE from the invocation's start (the FIX-800 lens's MEDIUM-2): the
/// platform's 30 s less the 3 s reserve, 27 s. Every entry captures its invocation's end (`owed_invocation_end`) where
/// its `wait_until` future starts, BEFORE the hook's tip read or the read's answer, so the start lag is already inside
/// that end; a walk's hard deadline is that end and nothing shorter (`owed_walk_slice`). As first built the walk was
/// also capped at 24 s from its OWN start "for 3 s of start lag", which counted the lag twice and dropped the 24 to
/// 27 s walks the base had landed (loop 39's row-delivering `1 row in 28443 ms` band). A walk started at the
/// invocation's start gets all 27 s; a later one gets what is left. The residual, stated: a walk of 27 s to about
/// 29 s that the platform would still have let finish is dropped (the reserve is what pays the release, the stale
/// mark and the push).
pub const OWED_WALK_HARD_DEADLINE_MS: i64 = OWED_PLATFORM_WAIT_UNTIL_MS - OWED_RELEASE_RESERVE_MS;
/// A walk is not started on a slice shorter than this (the hook's second identity behind a slow first one, a folded
/// rerun behind a slow walk): it would reach its outward cut at 4 s and its hard deadline soon after, writing little
/// or nothing. The identity is marked stale instead, so the next read walks it on a fresh invocation.
pub const OWED_WALK_MIN_SLICE_MS: i64 = 8_000;

/// The unix-ms by which a claim's last walk must have ended: the invocation's start plus the platform's ceiling,
/// less the reserve. PURE.
pub fn owed_invocation_end(invocation_start_ms: i64) -> i64 {
    invocation_start_ms
        .saturating_add(OWED_PLATFORM_WAIT_UNTIL_MS)
        .saturating_sub(OWED_RELEASE_RESERVE_MS)
}

/// One walk's clock (bsv-low #800).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwedWalkSlice {
    pub started_ms: i64,
    /// Past this the outward legs (the cursor's pages, the couriers, the stored reads) stop and the walk writes what
    /// it has (`budget cut`).
    pub outward_deadline_ms: i64,
    /// At this the walk future is dropped by its claim.
    pub hard_deadline_ms: i64,
}

impl OwedWalkSlice {
    /// Is the walk past its outward budget?
    pub fn over_budget(&self, now_ms: i64) -> bool {
        now_ms > self.outward_deadline_ms
    }
    /// How long the claim waits for the walk before dropping it.
    pub fn hard_ms(&self) -> u64 {
        self.hard_deadline_ms.saturating_sub(self.started_ms).max(0) as u64
    }
}

/// The slice a walk starting at `now_ms` may take of an invocation that must be done by `invocation_end_ms`
/// (`owed_invocation_end`); `None` when less than [`OWED_WALK_MIN_SLICE_MS`] is left. PURE (pinned).
///
/// The hard deadline is the invocation's end (MEDIUM-2: the start lag is counted once, inside that end); the cap at
/// [`OWED_WALK_HARD_DEADLINE_MS`] from `now_ms` binds only a clock read before the invocation's own start.
///
/// The outward budget is [`OWED_RECOMPUTE_TIME_BUDGET_MS`] or HALF the slice, whichever is less (the lens's LOW-4,
/// stated, not equalised): the walk's stored D1 reads, the pay-vout pass and the write batch are NOT gated by the
/// outward clock (skipping one would derive a different row, not a slower one), and loop 39's cut walks ran 22 to 28 s
/// with their outward legs stopped at 12 s, so the ungated tail is the other half. A first walk gets 12 s of a 27 s
/// slice; a folded rerun or the hook's next identity starting T s into the invocation gets min(12, (27 - T) / 2):
/// 12 s at T = 3, 9.5 s at 8, 8 s at 11, 5.5 s at 16, 4 s at 19, and none past 19 (under the 8 s minimum slice).
/// Equalising them at 12 s would push such a walk's ungated tail past its hard deadline, dropping it whole (nothing
/// written) where the half cuts it (a partial snapshot written). The cost: those walks are cut more often, and a cut
/// snapshot standing unmarked (`WrittenCut`, nothing folded) shows pending or withheld rows and honours unrechecked
/// latches under R-HIGH-1's rule until the 5 min open-row age; the rule is unchanged, it runs more often.
pub fn owed_walk_slice(now_ms: i64, invocation_end_ms: i64) -> Option<OwedWalkSlice> {
    let hard = now_ms
        .saturating_add(OWED_WALK_HARD_DEADLINE_MS)
        .min(invocation_end_ms);
    owed_slice_to(now_ms, hard, OWED_WALK_MIN_SLICE_MS)
}

fn owed_slice_to(now_ms: i64, hard: i64, min_len: i64) -> Option<OwedWalkSlice> {
    let len = hard.saturating_sub(now_ms);
    if len < min_len {
        return None;
    }
    Some(OwedWalkSlice {
        started_ms: now_ms,
        outward_deadline_ms: now_ms.saturating_add(OWED_RECOMPUTE_TIME_BUDGET_MS.min(len / 2)),
        hard_deadline_ms: hard,
    })
}

/// The deferred alarm's walk (bsv-low #648) runs in the Durable Object's ALARM handler, not in a `wait_until`: it has
/// no 30 s ceiling and holds no isolate lock, so the invocation's slice is not its bound (the FIX-800 lens's MEDIUM-3:
/// as first built it was dropped at 24 s, so an identity whose walk is steadily past 24 s, exactly the heavy identity
/// that reaches the alarm by a shed, could land by no path). Its own hard deadline is ONE #648 window,
/// [`OWED_RECOMPUTE_RATE_WINDOW_MS`] (60 s): the alarm's faulted retry is armed for the NEXT window's end
/// (`owed_fault_retry_due`), so a walk bounded by one window has ended before its own retry can be due, and two
/// deferred walks of one identity never overlap; it is also the most one reserved walk may take of the window it is
/// charged to. The platform's bound on an alarm handler's wall time is far above it (Cloudflare documents 15 minutes;
/// not measured here). The outward budget stays 12 s (half of 60 s is more, and the outward legs are the couriers
/// the rest of the walk must not wait on), so the 48 s past it are the ungated tail's (loop 39's longest: 16 s).
pub const OWED_DEFERRED_WALK_DEADLINE_MS: i64 = OWED_RECOMPUTE_RATE_WINDOW_MS;

/// The deferred alarm's slice from `now_ms`: [`OWED_DEFERRED_WALK_DEADLINE_MS`] long, its outward budget 12 s. PURE
/// (pinned).
pub fn owed_deferred_walk_slice(now_ms: i64) -> OwedWalkSlice {
    owed_slice_to(
        now_ms,
        now_ms.saturating_add(OWED_DEFERRED_WALK_DEADLINE_MS),
        0,
    )
    .expect("a positive deadline")
}

/// How one claimed walk ended, as the claim's next step reads it (bsv-low #800).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwedWalkEnd {
    /// The snapshot was written (or superseded) inside the outward budget.
    Written,
    /// The snapshot was written, cut at the outward budget (a partial snapshot).
    WrittenCut,
    /// The walk faulted (nothing written; the previous rows stand).
    Faulted,
    /// The claim dropped the walk at its hard deadline (nothing known written).
    PastDeadline,
    /// No slice was left to start it.
    NoSlice,
}

/// The claim's step after a walk: release the lock NOW (before any further await), and whether to mark the identity
/// stale on the way out, or run one folded ask again. PURE (pinned).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwedClaimStep {
    /// `nudge`: after the stale mark, push `owed-changed` with no rows (`owed_stale_event_body`): the walk was never
    /// started (no slice), and the page re-reads `/owed` only on an event (the FIX-800 lens's MEDIUM-1), so its read,
    /// served the stale rows, walks on a FRESH invocation with a whole slice (once: it lands or is dropped).
    ///
    /// `to_alarm`: the walk was DROPPED at its hard deadline. The identity is handed to the deferred alarm walk
    /// (`RecomputeRate::hand_to_alarm`, the window's one reserved walk, its own 60 s deadline: the one path that
    /// lands a heavy identity) and the page is NOT nudged (the FIX-800 delta lens's MEDIUM-A: a nudge there made the
    /// page re-read, which claimed another 27 s walk that was dropped again, one lap every ~28 s a tab, under the
    /// ceiling, so the alarm was never reached). The alarm's landed walk pushes the rows; a read meanwhile is shed
    /// with `retryAfterMs` past the alarm's landing. Only an arm that FAILED falls back to the nudge.
    Release {
        mark_stale: bool,
        nudge: bool,
        to_alarm: bool,
    },
    Rerun,
}

/// `folded`: an ask waits in the rerun map; `reruns_left`: the claim may still serve one; `next_slice`: a walk
/// started now would get a slice. A cut, a dropped or an unstarted walk never holds the lock into another walk: it
/// releases at once; a dropped or unstarted one (nothing written) marks the identity stale, a cut one only when an
/// ask is folded (its partial snapshot stands and the next pass continues it), so a folded ask is served by the next
/// read and the lock is never held across a walk the platform can cancel.
pub fn owed_claim_step(
    end: OwedWalkEnd,
    folded: bool,
    reruns_left: bool,
    next_slice: bool,
) -> OwedClaimStep {
    match end {
        // a partial snapshot stands (the next pass continues it, by the age rules); a folded ask marks it stale
        OwedWalkEnd::WrittenCut => OwedClaimStep::Release {
            mark_stale: folded,
            nudge: false,
            to_alarm: false,
        },
        // dropped at its deadline (nothing written): marked stale and handed to the alarm's 60 s walk, the page not
        // nudged into another walk that would be dropped the same way (the FIX-800 delta lens's MEDIUM-A)
        OwedWalkEnd::PastDeadline => OwedClaimStep::Release {
            mark_stale: true,
            nudge: false,
            to_alarm: true,
        },
        // never started (no slice): marked stale and the page told, so its next read walks on a fresh invocation
        OwedWalkEnd::NoSlice => OwedClaimStep::Release {
            mark_stale: true,
            nudge: true,
            to_alarm: false,
        },
        OwedWalkEnd::Written | OwedWalkEnd::Faulted if folded && reruns_left && next_slice => {
            OwedClaimStep::Rerun
        }
        OwedWalkEnd::Written | OwedWalkEnd::Faulted => OwedClaimStep::Release {
            mark_stale: folded,
            nudge: false,
            to_alarm: false,
        },
    }
}

/// A short id for one recompute (bsv-low #566's ask: every log line of one walk carries it). PURE.
pub fn owed_recompute_id(token: i64, nonce: u32) -> String {
    format!("r{:x}{:04x}", token.rem_euclid(1 << 24), nonce & 0xffff)
}

/// Everything one identity's recompute gathered (every one a served-view row).
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
    /// ([`FUNDING_HELD_NONE`], `owed_cursor::hydrate_party_pair_sql`'s `fundingHeld`). A refund-due pot named here
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
/// bsv-low #837: at most this many index-unspent hops are asked of their seat pots in one recompute (a hop whose JOIN
/// was admitted normally is index-SPENT and is never asked; an honest stranded hop has no seat pot and costs one
/// statement), and at most this many pot BEEFs are read for them.
pub const OWED_JOIN_SEAT_HOPS_PER_RECOMPUTE: usize = 8;
/// See [`OWED_JOIN_SEAT_HOPS_PER_RECOMPUTE`].
pub const OWED_JOIN_POT_READS_PER_RECOMPUTE: usize = 4;

/// PURE (bsv-low #837, the FIX-837 lens's LOW-2): the index-unspent hops the belt asks this pass, as indexes into
/// `hops` (`(outpoint_key, marker_created_at)`): a hop whose chain rung already names a spender is not asked (the
/// rung has its word), the newest hop keeps the first slot, and the other `OWED_JOIN_SEAT_HOPS_PER_RECOMPUTE - 1`
/// are taken from the rest (newest first) at the offset `rotate` (the recompute's minute), as
/// `hops_view::order_probe_targets` rotates the probes, so an identity with more than eight such hops reaches every
/// one of them over successive passes instead of asking the same newest eight for ever.
pub fn belt_window(
    hops: &[(String, Option<i64>)],
    chain_named: &HashSet<String>,
    rotate: usize,
) -> Vec<usize> {
    let mut order: Vec<usize> = (0..hops.len())
        .filter(|&i| !chain_named.contains(&hops[i].0))
        .collect();
    order.sort_by_key(|&i| std::cmp::Reverse(hops[i].1));
    if order.is_empty() {
        return order;
    }
    let leader = order.remove(0);
    let n = order.len();
    let take = (OWED_JOIN_SEAT_HOPS_PER_RECOMPUTE - 1).min(n);
    let start = if n == 0 { 0 } else { rotate % n };
    let mut window = vec![leader];
    window.extend((0..take).map(|k| order[(start + k) % n]));
    window
}

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
/// bsv-low #831 (the FIX-831 lens's L3): the durable `ops_counters` row (via `courier::note_presence_n`, the census
/// flush; the overlay seeds it at 0), bumped once per pot a recompute judged funding-not-held, so the class a strict
/// rule withholds for good (a mined funding the index never verified, with no latch and no `tx_words` row) is visible.
pub const COUNTER_REFUND_FUNDING_UNHELD: &str = "owed_refund_funding_unheld_total";
/// PURE: how many rows of one derivation are a refund-due pot judged funding-not-held (one row per pot).
pub fn count_refund_funding_unheld(rows: &[OwedRow]) -> u64 {
    rows.iter()
        .filter(|r| r.facts.get("claim").and_then(|v| v.as_str()) == Some(FUNDING_NOT_HELD_CLAIM))
        .count() as u64
}
/// bsv-low #831 (L3): the route counts the pass's funding-not-held pots ([`count_refund_funding_unheld`]).
pub fn note_refund_funding_unheld(n: u64) {
    crate::courier::note_presence_n(COUNTER_REFUND_FUNDING_UNHELD, n);
}

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

/// PURE: (my hop outpoint, the evicted txid that released it) — the same ledger rows and the same UTXO key as
/// [`released_hop_outpoints`], keeping WHICH eviction named the hop (the twin row to read). Sorted, deduplicated.
pub fn released_hops_by_eviction(
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

pub fn markerless_seats_sql(count: usize) -> String {
    let placeholders = (2..=count + 1)
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT lower(potTxid) AS potTxid, gameId, opponentIdentity, seatSettlePubkey FROM potparty_records_evicted WHERE identity = ?1 AND sigValid = 1 AND seatSettlePubkey IS NOT NULL AND lower(potTxid) IN ({placeholders}) UNION SELECT lower(potTxid), gameId, opponentIdentity, seatSettlePubkey FROM potparty_records WHERE identity = ?1 AND sigValid = 1 AND seatSettlePubkey IS NOT NULL AND lower(potTxid) IN ({placeholders})")
}
pub const MARKERLESS_KEYS_SQL: &str = "SELECT lower(txid) AS txid, outputIndex, pubA, pubB, payPkhA, payPkhB FROM pot_records_evicted WHERE lockKind = 'covenant' AND paramsDecoded = 1 AND txid = ?1";
pub const MARKERLESS_BEEF_SQL: &str = "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?1";

// H1/M4/L3: permanent, identity-owned candidates FIRST. Late filings remain live. The ledger's
// readmitted word ends a release; its age never does. COUNT is over this identity's distinct JOINs.
// N2: sigValid is the overlay/filing door's latched verdict under the single canonical-DER gate.
pub const MARKERLESS_CANDIDATES_SQL: &str = "WITH mine AS (SELECT lower(potTxid) AS txid FROM potparty_records_evicted WHERE identity = ?1 AND sigValid = 1 AND seatSettlePubkey IS NOT NULL UNION SELECT lower(potTxid) FROM potparty_records WHERE identity = ?1 AND sigValid = 1 AND seatSettlePubkey IS NOT NULL) SELECT e.txid, e.releasedSpends, e.evictedAt, COUNT(*) OVER () AS total FROM mine JOIN pot_evictions e ON e.txid = mine.txid WHERE e.readmittedAt IS NULL AND e.releasedSpends IS NOT NULL ORDER BY CASE WHEN e.txid > ?2 THEN 0 ELSE 1 END, e.txid LIMIT ?3";

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MarkerlessCandidate {
    pub txid: String,
    #[serde(rename = "releasedSpends")]
    pub released_spends: Option<String>,
    #[serde(rename = "evictedAt")]
    pub evicted_at: f64,
    pub total: f64,
}

/// The route uses this selection verbatim; the SQL already ordered the ring after the durable cursor.
pub fn markerless_plan(candidates: &[MarkerlessCandidate]) -> (Vec<MarkerlessCandidate>, usize) {
    let total = candidates.first().map_or(0, |r| r.total as usize);
    (
        candidates
            .iter()
            .take(OWED_MARKERLESS_JOINS_PER_RECOMPUTE)
            .cloned()
            .collect(),
        total,
    )
}

/// Read only the previous snapshot's JOINs, checking the same signed identity evidence and ledger end.
/// This does not enumerate an unbounded candidate tail; owed_rows is capped at OWED_MAX_ROWS.
pub const MARKERLESS_PREVIOUS_CANDIDATES_SQL: &str = "SELECT DISTINCT e.txid FROM owed_rows r JOIN pot_evictions e ON e.txid = json_extract(r.facts, '$.markerlessJoin') WHERE r.identity = ?1 AND e.readmittedAt IS NULL AND e.releasedSpends IS NOT NULL AND (EXISTS (SELECT 1 FROM potparty_records_evicted pp WHERE pp.identity = ?1 AND lower(pp.potTxid) = e.txid AND pp.sigValid = 1 AND pp.seatSettlePubkey IS NOT NULL) OR EXISTS (SELECT 1 FROM potparty_records pp WHERE pp.identity = ?1 AND lower(pp.potTxid) = e.txid AND pp.sigValid = 1 AND pp.seatSettlePubkey IS NOT NULL))";

/// A deferred row retires as soon as its JOIN leaves this identity's candidate set (#608).
pub fn markerless_should_carry(
    row: &OwedRow,
    current: &HashSet<String>,
    selected: &[MarkerlessCandidate],
    active: &HashSet<String>,
) -> bool {
    !current.contains(&row.outpoint)
        && row.facts["markerlessJoin"]
            .as_str()
            .is_some_and(|join| active.contains(join) && !selected.iter().any(|c| c.txid == join))
}

/// bsv-low #662: the previous snapshot's PAYOUT rows at the hop outpoints whose spender's bytes this pass WITHHELD
/// (one statement, the identity's own rows by their primary key: `(identity, outpoint)`, at most one row an
/// outpoint; the outpoints bound as one JSON array).
/// bsv-low #672 (RC-L30): the read also returns the identity's PREVIOUS `unbound` rows at the withheld outpoints, so
/// [`keep_withheld_elsewhere`] can carry a JUDGED `spent-elsewhere` row forward the same way (both callers filter the
/// family they want, so the broadened read is one statement for two carriers).
pub const OWED_WITHHELD_PREVIOUS_SQL: &str = "SELECT identity, outpoint, family, gameId, sats, opponentIdentity, atHeight, facts, updatedAtMs, reason FROM owed_rows WHERE identity = ?1 AND outpoint IN (SELECT value FROM json_each(?2)) AND family IN ('payout','unbound')";
/// The row word of a payout kept from the previous snapshot because this pass could not look at its spender.
pub const OWED_REFRESH_WITHHELD: &str = "withheld";
/// The L25-WITHHELD lens's MEDIUM-1 (1): how long a kept row may stand on its last judgement (`judgedAtMs`, the pass
/// that last read the spender's bytes). Sixty minutes, twelve open-list refreshes
/// (`OWED_RECOMPUTE_OPEN_AFTER_MS`, five): the durable rotation ([`spender_ask_order`]) buys every named spender
/// within ceil(16 / 2) = 8 passes (40 min) at the courier cap of two, so an honest rotation re-reads the bytes before
/// the bound; past it the rotation is not reaching the spender (every pass's batch lost, or more spenders than the
/// walk names) and the honest "could not judge the spender" row returns.
pub const OWED_WITHHELD_KEEP_MAX_MS: i64 = 60 * 60_000;
const _: () = assert!(OWED_WITHHELD_KEEP_MAX_MS >= 12 * OWED_RECOMPUTE_OPEN_AFTER_MS);

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

/// PURE (bsv-low L25): the distinct named spenders of this identity's SPENT hops (lowercase, sorted): the spenders
/// whose durable `homeproof:` latch the recompute reads byte-free ([`latched_retired_spenders`]) to retire a hop an
/// earlier pass already proved home-spent, whatever this pass's spender courier cap withheld.
pub fn hop_named_spenders(i: &OwedInputs) -> Vec<String> {
    let mut out: Vec<String> = i
        .hops
        .iter()
        .filter_map(|h| hop_named_spender(i, h))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// PURE (the L25-WITHHELD lens's LOW-2): `r` is THIS pass's spender judgement of the hop at `key`: an `unbound`
/// row whose spender fact (`chainSpender`, else `spendingTxid`) names `sp`, never a markerless evidence read-fault
/// row (`markerlessReadFault`: a faulted read keeps its fault row).
fn is_spender_judgement(r: &OwedRow, key: &str, sp: &str) -> bool {
    r.outpoint == key
        && r.family == OwedFamily::Unbound
        && r.facts["markerlessReadFault"] != true
        && r.facts["chainSpender"]
            .as_str()
            .or_else(|| r.facts["spendingTxid"].as_str())
            .is_some_and(|named| named.eq_ignore_ascii_case(sp))
}

/// PURE (bsv-low #662): the hop outpoints served `unbound` this pass ONLY because the spender's bytes were WITHHELD
/// (a cap or the clock kept the look; `withheld` names the spenders never looked at, lowercase): the row has no
/// bytes to judge from, and no read of this row faulted. A FAULTED read (a stored read or a courier ask that
/// answered badly, a cached fault, the pot spenders' read) is never in `withheld` and keeps its fault row.
pub fn withheld_hop_outpoints(
    i: &OwedInputs,
    rows: &[OwedRow],
    withheld: &HashSet<String>,
) -> Vec<String> {
    if withheld.is_empty() || i.pot_spenders_faulted {
        return Vec::new();
    }
    let mut out: Vec<String> = i
        .hops
        .iter()
        .filter_map(|h| {
            let sp = hop_named_spender(i, h)?;
            if !withheld.contains(&sp) || i.spender_outputs.contains_key(&sp) {
                return None;
            }
            let key = outpoint_key(&h.hop_txid, h.hop_vout);
            rows.iter()
                .any(|r| is_spender_judgement(r, &key, &sp))
                .then_some(key)
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// PURE (bsv-low #662): a judged courier-bytes payout does not flap to "could not judge" when a later pass's spender
/// courier cap (or its clock) WITHHELD the sweep's bytes. The home walk's rule (`home_walk_pass`): a withheld look
/// names nothing more than it did before, so the row stands. For each outpoint of [`withheld_hop_outpoints`], the
/// identity's PREVIOUS row (`previous`: the row and its `updatedAtMs`) replaces this pass's `unbound` row when it
/// was a courier-bytes payout of the SAME spender the hop still names, its game not retired by a verified
/// `collected` filing nor its sweep by a held one. The kept row is the identity's own previous evidence, served as
/// it was with `refresh: "withheld"` and `judgedAtMs` (the stamp of the pass that last read the bytes); it stays
/// unclaimable (a courier-bytes payout always is), so the read's open-list rule re-judges it. Returns the count.
pub fn keep_withheld_payouts(
    i: &OwedInputs,
    rows: &mut [OwedRow],
    previous: &[(OwedRow, i64)],
    withheld: &HashSet<String>,
) -> usize {
    let outpoints = withheld_hop_outpoints(i, rows, withheld);
    let mut kept = 0;
    for h in i.hops {
        let key = outpoint_key(&h.hop_txid, h.hop_vout);
        if !outpoints.contains(&key) {
            continue;
        }
        let Some(sp) = hop_named_spender(i, h) else {
            continue;
        };
        // the read binds the identity (`WHERE identity = ?1`, exact): no identity compare here (the lens's NOTE-2)
        let Some((prev, updated_at_ms)) = previous.iter().find(|(p, _)| {
            p.outpoint == key
                && p.family == OwedFamily::Payout
                && p.facts["creditKind"] == "courier-bytes"
                && p.facts["sweepTxid"]
                    .as_str()
                    .is_some_and(|s| s.eq_ignore_ascii_case(&sp))
        }) else {
            continue;
        };
        let game = prev.game_id.to_ascii_lowercase();
        if i.collected_verified.contains(&game) || i.held_verified.contains(&held_key(&game, &sp)) {
            continue;
        }
        let judged_at = prev.facts["judgedAtMs"].as_i64().unwrap_or(*updated_at_ms);
        // MEDIUM-1 (1): a judgement older than the bound is not kept; the could-not-judge row stands
        if i.now_ms.saturating_sub(judged_at) > OWED_WITHHELD_KEEP_MAX_MS {
            continue;
        }
        let Some(slot) = rows.iter_mut().find(|r| is_spender_judgement(r, &key, &sp)) else {
            continue;
        };
        let mut row = prev.clone();
        row.facts["claimable"] = json!(false);
        row.facts["refresh"] = json!(OWED_REFRESH_WITHHELD);
        row.facts["judgedAtMs"] = json!(judged_at);
        *slot = row;
        kept += 1;
    }
    kept
}

/// PURE (bsv-low #672, RC-L30-COULD-NOT-JUDGE): a hop JUDGED `spent-elsewhere` (the chain rung named a competitor
/// paying none of this identity's homes) does not flap to "could not judge the spender" when a later pass's spender
/// courier cap or its clock WITHHELD the competitor's bytes. The sibling of [`keep_withheld_payouts`] for the
/// `unbound` spender-judgement row, and the BELT behind the durable store ([`StoredSpenderBytes`]): once a pass
/// persists the competitor's bytes, later passes read them before the clock and judge FRESH, so this fires only for
/// a row JUDGED before the store held its spender (the transition, and any pass that genuinely has nothing). For each
/// outpoint of [`withheld_hop_outpoints`] the identity's PREVIOUS row replaces this pass's could-not-judge row when it
/// was a POSITIVE spender judgement (a `spendKind`, so `spent-elsewhere` or `pot-unindexed`, or `spenderPkhs` for the
/// home-unknown sentence) of the SAME spender the hop still names (keyed on the hop UTXO plus the content-addressed
/// spender txid: a DIFFERENT spender is NEW evidence and is never carried). The kept row is served as it was with
/// `refresh: "withheld"`, `carried: true` (the additive mark) and `judgedAtMs`, within the same
/// [`OWED_WITHHELD_KEEP_MAX_MS`] bound; it stays unclaimable (an `unbound` row always is), so the read re-judges it.
/// Returns the count.
pub fn keep_withheld_elsewhere(
    i: &OwedInputs,
    rows: &mut [OwedRow],
    previous: &[(OwedRow, i64)],
    withheld: &HashSet<String>,
) -> usize {
    let outpoints = withheld_hop_outpoints(i, rows, withheld);
    let mut kept = 0;
    for h in i.hops {
        let key = outpoint_key(&h.hop_txid, h.hop_vout);
        if !outpoints.contains(&key) {
            continue;
        }
        let Some(sp) = hop_named_spender(i, h) else {
            continue;
        };
        // the read binds the identity (`WHERE identity = ?1`): the PREVIOUS unbound row at this outpoint, a POSITIVE
        // spender judgement of the SAME spender (the content-addressed txid), never a could-not-judge or fault row
        let Some((prev, updated_at_ms)) = previous.iter().find(|(p, _)| {
            p.outpoint == key
                && p.family == OwedFamily::Unbound
                && prev_names_spender(p, &sp)
                && (p.facts.get("spendKind").is_some() || p.facts.get("spenderPkhs").is_some())
                && p.facts["markerlessReadFault"] != true
        }) else {
            continue;
        };
        let game = prev.game_id.to_ascii_lowercase();
        if i.collected_verified.contains(&game) || i.held_verified.contains(&held_key(&game, &sp)) {
            continue;
        }
        let judged_at = prev.facts["judgedAtMs"].as_i64().unwrap_or(*updated_at_ms);
        if i.now_ms.saturating_sub(judged_at) > OWED_WITHHELD_KEEP_MAX_MS {
            continue;
        }
        let Some(slot) = rows.iter_mut().find(|r| is_spender_judgement(r, &key, &sp)) else {
            continue;
        };
        let mut row = prev.clone();
        row.facts["claimable"] = json!(false);
        row.facts["refresh"] = json!(OWED_REFRESH_WITHHELD);
        row.facts["carried"] = json!(true);
        row.facts["judgedAtMs"] = json!(judged_at);
        *slot = row;
        kept += 1;
    }
    kept
}

/// bsv-low #837 (FIX-837): the row word of a row a BUDGET-CUT pass did not derive, carried from the previous snapshot.
pub const OWED_REFRESH_SHED: &str = "shed";
/// bsv-low #837: the `retryAfterMs` a carried shed row (and the answer serving one) names: the next pass is expected
/// within the read-aged window's own scale, never sooner than the shed floor of a second.
pub const OWED_SHED_RETRY_MS: i64 = 30_000;

/// PURE (bsv-low #837, FIX-837 fix 2): a pass the outward budget CUT never serves fewer rows than the snapshot it
/// replaces without saying so. Each row of `previous` (the identity's stored snapshot, `OWED_ROWS_READ_SQL`) whose
/// outpoint this pass did not derive is carried into `rows` as it was, marked `refresh: "shed"`, `stale: true`,
/// `carried: true`, `retryAfterMs`, its first judgement's `judgedAtMs` kept, and made NO PRESS (`claimable: false`,
/// the old word kept as `claimableBeforeShed`): a cut pass could not re-judge it, so it is shown, never pressed on a
/// stale word. Not carried: a row judged more than [`OWED_WITHHELD_KEEP_MAX_MS`] ago (a cut every pass cannot hold a
/// dead row up for good), a row of a game this identity filed verified-collected. Only on a cut pass; an uncut pass's
/// rows stand as derived (a row it did not derive is retired). The carry never takes `rows` past
/// [`OWED_MAX_ROWS`] (the FIX-837 lens's LOW-4: it runs after the pass's own truncation, so the write plan's bound,
/// rows + 3 statements, holds by construction and no carried row is the one the read handler drops). The count
/// carried.
pub fn carry_shed_rows(
    i: &OwedInputs,
    rows: &mut Vec<OwedRow>,
    previous: &[(OwedRow, i64)],
) -> usize {
    let current: HashSet<String> = rows.iter().map(|r| r.outpoint.clone()).collect();
    let mut carried = 0;
    for (prev, updated_at_ms) in previous {
        if rows.len() >= OWED_MAX_ROWS {
            break;
        }
        if current.contains(&prev.outpoint) {
            continue;
        }
        let judged_at = prev.facts["judgedAtMs"].as_i64().unwrap_or(*updated_at_ms);
        if i.now_ms.saturating_sub(judged_at) > OWED_WITHHELD_KEEP_MAX_MS {
            continue;
        }
        if i.collected_verified
            .contains(&prev.game_id.to_ascii_lowercase())
        {
            continue;
        }
        let mut row = prev.clone();
        let before = if prev.facts["refresh"] == OWED_REFRESH_SHED {
            prev.facts["claimableBeforeShed"].clone()
        } else {
            prev.facts["claimable"].clone()
        };
        row.facts["claimableBeforeShed"] = before;
        row.facts["claimable"] = json!(false);
        row.facts["refresh"] = json!(OWED_REFRESH_SHED);
        row.facts["stale"] = json!(true);
        row.facts["carried"] = json!(true);
        row.facts["judgedAtMs"] = json!(judged_at);
        row.facts["retryAfterMs"] = json!(OWED_SHED_RETRY_MS);
        rows.push(row);
        carried += 1;
    }
    carried
}

/// PURE (bsv-low #837): an answer that serves a carried shed row says so at the top (`refresh: "shed"`,
/// `retryAfterMs`), as a shed read does; an answer already marked keeps its own word.
pub fn mark_shed_rows_refresh(body: &mut Value) {
    if body.get("refresh").is_some_and(|r| !r.is_null()) {
        return;
    }
    let shed = body["rows"].as_array().is_some_and(|rows| {
        rows.iter()
            .any(|r| r["facts"]["refresh"] == OWED_REFRESH_SHED)
    });
    if shed {
        mark_shed_refresh(body, Some(OWED_SHED_RETRY_MS));
    }
}

/// PURE: the previous row's own spender fact (`chainSpender`, else `spendingTxid`) names `sp` (lowercase compare).
fn prev_names_spender(p: &OwedRow, sp: &str) -> bool {
    p.facts["chainSpender"]
        .as_str()
        .or_else(|| p.facts["spendingTxid"].as_str())
        .is_some_and(|named| named.eq_ignore_ascii_case(sp))
}

/// How a named spender's bytes were LOOKED at this pass (the L25-WITHHELD lens's LOW-1): a read was made, or an
/// answer already held was used, faulted or not. A looked spender is never withheld: its row (a judgement or a
/// fault) is this pass's word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpenderLooked {
    /// The index's stored BEEF read (`load_stored_beef`), whatever it answered: bytes, a fault, unparsable bytes.
    StoredRead,
    /// The isolate's parsed courier memo answered.
    Memo,
    /// The isolate's tx-any cache answered (a fault inside its TTL included).
    CachedAnswer,
    /// The tracker's compact row carried the hash-verified raw (or not: the row was the answer).
    TrackerRow,
    /// A courier was asked (`resolve_tx_any`), whatever it answered.
    CourierAsked,
    /// bsv-low #672 (RC-L30): the DURABLE spender-bytes store answered (a spender a prior pass hash-verified and
    /// persisted). A look, never a withhold: the bytes are in hand before the clock, so the budget cannot starve it.
    StoredDurable,
}

/// Why a named spender's bytes were NOT looked at this pass: the only causes of a WITHHELD spender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpenderWithheld {
    /// The spender courier cap (`OWED_SPENDER_COURIER_READS_PER_RECOMPUTE`).
    Cap,
    /// The recompute's clock (`OWED_RECOMPUTE_TIME_BUDGET_MS`).
    Clock,
    /// Past the stored-read count (`OWED_SPENDER_READS_PER_RECOMPUTE`): never reached this pass.
    Tail,
}

/// PURE (the L25-WITHHELD lens's LOW-1): the spender look ledger of one recompute. WITHHELD is positive evidence: a
/// spender is withheld only when a cap, the clock or the tail was NOTED for it and no look was. A path that notes
/// nothing leaves the spender looked-or-unknown, never withheld, so its row stands as derived (the pre-#662 word,
/// the honest could-not-judge): a read path added later without its note can never bring a stale payout over a
/// fault.
#[derive(Debug, Clone, Default)]
pub struct SpenderLook {
    looked: HashMap<String, SpenderLooked>,
    withheld: HashMap<String, SpenderWithheld>,
}

impl SpenderLook {
    pub fn look(&mut self, sp: &str, how: SpenderLooked) {
        self.looked.insert(sp.to_ascii_lowercase(), how);
    }
    pub fn withhold(&mut self, sp: &str, why: SpenderWithheld) {
        self.withheld.entry(sp.to_ascii_lowercase()).or_insert(why);
    }
    pub fn withhold_all<'a>(
        &mut self,
        sps: impl IntoIterator<Item = &'a String>,
        why: SpenderWithheld,
    ) {
        for sp in sps {
            self.withhold(sp, why);
        }
    }
    /// The spenders whose bytes this pass WITHHELD (lowercase): noted withheld and never looked at.
    pub fn withheld(&self) -> HashSet<String> {
        self.withheld
            .keys()
            .filter(|sp| !self.looked.contains_key(*sp))
            .cloned()
            .collect()
    }
}

/// The L25-WITHHELD lens's MEDIUM-1 (2): the durable ask stamp of a spender's courier read, one `hop_chain_probes`
/// row under this key prefix (`spenderask:<spender txid>.0`, the memo key's shape, so it rides the probe memo read).
/// Its `probedAtMs` is the last time a courier was asked for that spender's bytes; `spent` is 0 and
/// `spentConfirmed` NULL, so no reader takes it for a probe or a latch and the overlay's reorg clear (confirmed
/// memos only) never touches it. The overlay's TTL sweep (two hours) DOES delete it, and that is its lifecycle: a
/// stamp past two hours ranks as never asked, which is where the oldest ask goes anyway, and the rows never grow
/// past the spenders asked in two hours. Keyed by the SPENDER (the thing asked), not the hop: one spender can
/// serve two hops and the isolate's own stamp was keyed the same way.
pub const SPENDER_ASK_PREFIX: &str = "spenderask:";
/// MEDIUM-1 (2): stamp a spender's courier ask (binds: the memo key, now in ms). Written in the recompute's ONE
/// write batch, never alone on the read path; the stamp only grows.
pub const SPENDER_ASK_UPSERT_SQL: &str = "INSERT INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed) VALUES (?1, ?2, 0, NULL, NULL) \
     ON CONFLICT(outpoint) DO UPDATE SET probedAtMs = MAX(hop_chain_probes.probedAtMs, excluded.probedAtMs)";

/// PURE: the probe-memo target of a spender's durable ask stamp.
pub fn spender_ask_target(spender_lc: &str) -> (String, u32) {
    (
        format!("{SPENDER_ASK_PREFIX}{}", spender_lc.to_ascii_lowercase()),
        0,
    )
}

/// PURE: the durable ask stamps the memo read returned, by spender (lowercase).
pub fn spender_ask_stamps(memos: &[crate::hops_view::ProbeMemo]) -> HashMap<String, i64> {
    let mut out: HashMap<String, i64> = HashMap::new();
    for m in memos {
        let Some(sp) = m
            .outpoint
            .strip_prefix(SPENDER_ASK_PREFIX)
            .and_then(|k| k.strip_suffix(".0"))
        else {
            continue;
        };
        let at = out.entry(sp.to_ascii_lowercase()).or_insert(m.probed_at_ms);
        *at = (*at).max(m.probed_at_ms);
    }
    out
}

/// PURE (MEDIUM-1 (2)): the courier asks' order, NEVER-ASKED FIRST, then the oldest ask (the bounded-walk rule of
/// 2026-09-19), a tie in `wanted`'s order (newest hop first). An ask's stamp is the later of the DURABLE stamp (the
/// same on every isolate) and the isolate's own (`isolate`), so a cold isolate's pass buys the spenders the last
/// passes did not, and the oldest unasked spender is never withheld for ever.
pub fn spender_ask_order(
    wanted: Vec<String>,
    durable: &HashMap<String, i64>,
    isolate: impl Fn(&str) -> Option<i64>,
) -> Vec<String> {
    let mut keyed: Vec<(i64, String)> = wanted
        .into_iter()
        .map(|sp| {
            let at = durable
                .get(&sp.to_ascii_lowercase())
                .copied()
                .max(isolate(&sp))
                .unwrap_or(0);
            (at, sp)
        })
        .collect();
    keyed.sort_by_key(|(at, _)| *at);
    keyed.into_iter().map(|(_, sp)| sp).collect()
}

/// A count is progress, never a cut. Only known unheld tail evidence makes the list incomplete.
pub fn markerless_tail_unheld(
    total: usize,
    examined: usize,
    rebuilt: usize,
    carried: usize,
    ring_complete: bool,
) -> bool {
    total > examined && (carried > 0 || (rebuilt > 0 && !ring_complete))
}

/// Completion belongs to this ring's candidate count; a changed count restarts its coverage word.
pub fn markerless_ring_complete(
    previous: &str,
    cursor: &str,
    selected: &[MarkerlessCandidate],
    total: usize,
) -> bool {
    let held = serde_json::from_str::<Value>(previous).unwrap_or(Value::Null);
    total <= selected.len()
        || (held["complete"] == true && held["candidates"].as_u64() == Some(total as u64))
        || (!cursor.is_empty() && selected.iter().any(|r| r.txid.as_str() <= cursor))
}

/// Per-JOIN evidence reads shared by the Worker and the real-SQLite pins.
#[allow(async_fn_in_trait)]
pub trait MarkerlessEvidenceReads {
    async fn index(&mut self, txid: &str, vout: u32) -> Result<Option<MarkerlessIndex>, String>;
    async fn keys(
        &mut self,
        join: &str,
    ) -> Result<Vec<(String, u32, crate::results::CommittedKeys)>, String>;
    async fn beef(&mut self, join: &str) -> Result<Option<Vec<u8>>, String>;
}

#[derive(Debug, serde::Deserialize)]
pub struct MarkerlessIndex {
    pub spent: Option<f64>,
    #[serde(rename = "spendingTxid")]
    pub spending_txid: Option<String>,
    #[serde(rename = "spentConfirmed")]
    pub spent_confirmed: Option<f64>,
}

#[derive(Default)]
pub struct MarkerlessEvidence {
    pub keys: Vec<(String, u32, crate::results::CommittedKeys)>,
    pub outputs: HashMap<String, (Vec<u8>, u64)>,
    pub index: HashMap<String, MarkerlessIndex>,
    pub faults: Vec<OwedRow>,
}

/// Missing permanent bytes and failed per-JOIN reads fault that candidate, never the owed list.
/// A fault names the ledger's possible outpoints without asserting ownership, value or a press.
pub async fn read_markerless_candidate(
    reads: &mut impl MarkerlessEvidenceReads,
    identity: &str,
    candidate: &MarkerlessCandidate,
    outpoints: &[(String, u32)],
    seats: &[EvictedSeat],
    index_budget: &mut usize,
) -> MarkerlessEvidence {
    let mut evidence = MarkerlessEvidence::default();
    let attempt = async {
        for (txid, vout) in outpoints {
            if *index_budget == 0 {
                return Err("evidence-unheld: hop index read lies past the bound".to_owned());
            }
            *index_budget -= 1;
            if let Some(row) = reads.index(txid, *vout).await? {
                evidence.index.insert(outpoint_key(txid, *vout), row);
            }
        }
        evidence.keys = reads.keys(&candidate.txid).await?;
        let bytes = reads.beef(&candidate.txid).await?.ok_or_else(|| {
            "evidence-missing: the evicted JOIN's stored bytes are absent or undecodable".to_owned()
        })?;
        evidence.outputs = hop_outputs_from_join_beef(&bytes, outpoints);
        if evidence.outputs.len() != outpoints.len() {
            return Err(
                "evidence-missing: the evicted JOIN's stored outputs are absent or undecodable"
                    .to_owned(),
            );
        }
        Ok::<_, String>(())
    }
    .await;
    if let Err(reason) = attempt {
        note_markerless_read_fault();
        let seat = seats
            .iter()
            .find(|s| s.pot_txid.eq_ignore_ascii_case(&candidate.txid));
        evidence = MarkerlessEvidence::default();
        evidence.faults = outpoints
            .iter()
            .map(|(txid, vout)| OwedRow {
                identity: identity.to_owned(),
                outpoint: outpoint_key(txid, *vout),
                family: OwedFamily::Unbound,
                game_id: seat.map_or_else(String::new, |s| s.game_id.clone()),
                sats: None,
                opponent_identity: seat.map(|s| s.opponent_identity.clone()),
                at_height: None,
                facts: json!({"markerlessEvidence":true,"markerlessJoin":candidate.txid,
                "markerlessReadFault":true,"evidenceMissing":reason.starts_with("evidence-missing"),
                "hopTxid":txid,"hopVout":vout,"claim":null,"claimable":false,
                "markerCreatedAtMs":candidate.evicted_at as i64}),
                reason: Some(format!("could not judge: {reason}")),
            })
            .collect();
    }
    evidence
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

/// A failed inline refresh serves the older evidence as a could-not-judge sentence, never a fresh press.
pub fn markerless_faulted_rows(mut rows: Vec<OwedRow>) -> Vec<OwedRow> {
    for r in rows
        .iter_mut()
        .filter(|r| r.facts["markerlessEvidence"] == Value::Bool(true))
    {
        r.family = OwedFamily::Unbound;
        r.facts["claim"] = Value::Null;
        r.facts["claimable"] = Value::Bool(false);
        r.facts["markerlessReadFault"] = Value::Bool(true);
        r.reason = Some("could not judge this pass: markerless evidence read fault".into());
    }
    rows
}

/// The durable position and counted progress share the existing, spared cursor row (no schema change).
pub fn markerless_cursor_of(raw: &str) -> String {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|r| r["cursor"].as_str().map(str::to_owned))
        .unwrap_or_else(|| raw.to_owned())
}

/// The cursor shares the snapshot's monotonic guard, even when a newer snapshot wrote no cursor.
pub const MARKERLESS_CURSOR_UPSERT_SQL: &str = "INSERT INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed) SELECT ?1, ?2, 0, ?3, NULL WHERE NOT EXISTS (SELECT 1 FROM owed_state WHERE identity = ?4 AND computedAtMs > ?2) ON CONFLICT(outpoint) DO UPDATE SET probedAtMs = excluded.probedAtMs, spendingTxid = excluded.spendingTxid WHERE excluded.probedAtMs >= hop_chain_probes.probedAtMs";

/// Reuse the spared cursor namespace, with a disjoint key from the home-output walk.
pub fn markerless_cursor_target(identity: &str) -> (String, u32) {
    (format!("{HOME_WALK_CURSOR_PREFIX}markerless:{identity}"), 0)
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

/// How many evicted JOINs one recompute reads back for [`markerless_released_hops`] (each a twin read and one stored
/// BEEF). N3: a valid seat pays one candidate/cursor read, one seats read, two reads per JOIN,
/// and at most 128 indexed hop-row reads; at most eight JOINs per pass, counted and cursor-paged under #499.
pub const OWED_MARKERLESS_JOINS_PER_RECOMPUTE: usize = 8;
/// A malformed/large release list must not turn the eight-JOIN bound into unbounded index reads.
pub const OWED_MARKERLESS_INDEX_READS_PER_RECOMPUTE: usize = 128;

/// PURE: the released outpoints the hops view does not hold, by the evicted txid that released them (the
/// candidates [`markerless_released_hops`] judges; the route reads only these JOINs back).
pub fn unheld_released_by_eviction(
    evictions: &[(String, Option<String>)],
    hops: &[HopEntry],
) -> Vec<(String, Vec<(String, u32)>)> {
    let held: HashSet<String> = hops
        .iter()
        .filter(|h| h.marker_verified == MarkerVerification::Verified)
        .map(|h| outpoint_key(&h.hop_txid, h.hop_vout))
        .collect();
    let mut out: std::collections::BTreeMap<String, Vec<(String, u32)>> =
        std::collections::BTreeMap::new();
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
            let Ok(vout) = u32::try_from(vout) else {
                continue;
            };
            if txid.len() != 64
                || !txid.bytes().all(|b| b.is_ascii_hexdigit())
                || held.contains(&outpoint_key(txid, vout))
            {
                continue;
            }
            out.entry(evicted.to_ascii_lowercase())
                .or_default()
                .push((txid.to_ascii_lowercase(), vout));
        }
    }
    out.into_iter().collect()
}

/// PURE: the outputs at `outpoints` as the evicted JOIN's own stored BEEF carries them (each hop is the JOIN's
/// ancestor in that BEEF; the BEEF's txid index is computed by hashing, so a hit is hash-consistent). An outpoint
/// whose transaction is absent, txid-only or short of the vout is simply not named.
pub fn hop_outputs_from_join_beef(
    beef: &[u8],
    outpoints: &[(String, u32)],
) -> HashMap<String, (Vec<u8>, u64)> {
    let mut out = HashMap::new();
    let Ok(mut parsed) = overlay_engine::beef_limits::parse_beef(
        beef,
        &overlay_engine::beef_limits::APP_BEEF_LIMITS,
    ) else {
        return out;
    };
    for (txid, vout) in outpoints {
        let Some(raw) = parsed
            .find_txid_mut(&txid.to_ascii_lowercase())
            .and_then(|t| t.raw_tx_or_compute())
        else {
            continue;
        };
        let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(&raw) else {
            continue;
        };
        let Some(o) = tx.outputs.get(*vout as usize) else {
            continue;
        };
        let Some(sats) = o.satoshis else { continue };
        out.insert(
            outpoint_key(txid, *vout),
            (o.locking_script.to_binary(), sats),
        );
    }
    out
}

/// One of THIS identity's party rows in the permanent twin or a late live filing (the eviction moved it with its
/// pot): `sigValid = 1` (the identity's own signature and the seat signature under `seat_settle_pubkey`, both
/// verified at admission), read with `identity = ?` by the route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictedSeat {
    pub pot_txid: String,
    pub game_id: String,
    pub opponent_identity: String,
    pub seat_settle_pubkey: String,
}

/// PURE (loop 23's pair 13, bsv-low #610): the hops an eviction RELEASED that this identity's hops view does not hold,
/// rebuilt from the evicted JOIN itself so the derivation tells their story.
///
/// The hops view is fed by the seat's `LOW/hopparty/v1` marker, a best-effort submit: under the fleet's burst p28's
/// marker submit was refused 502 once (`tm_hopparty`, the corroboration ladder's STORED echoes) and never re-presented,
/// so the hop had NO row, `released_hops_by_eviction` intersected the ledger with nothing, and the owed list said
/// nothing for a hand the network refused. The released hop's owed row rests on the eviction ledger and the evicted
/// twin's party row; the seat's hopparty marker names the hop faster, never alone.
///
/// N1: the markerless sweep filing door still answers 425; the fire-and-forget press broadcasts unchanged.
/// Keyed on the UTXO the ledger names, never a name: a released outpoint is mine only when (1) my own party row of
/// that evicted JOIN (`seats`, `sigValid = 1`) names the settle key K, (2) the JOIN's twin lock committed K on exactly
/// one seat (`twin_keys`, every row of the JOIN agreeing), and (3) the outpoint's output, read from the evicted JOIN's
/// own stored BEEF (`hop_outputs`: the hop transaction carried as the JOIN's ancestor), is the standard P2PKH to
/// hash160(K). Only K spends that output, and only I signed for K. The rebuilt entry carries no spend word
/// (`Unknown`, `spent: None`): the eviction released the index's pointer, so the chain rung judges it, and the
/// marker attestation is the party row's (`Verified`; no marker txid: there is no hop marker to fetch). An outpoint
/// the hops view already holds is left to it; two of my seats in one JOIN, or two JOINs naming one outpoint for
/// different games or keys, name nothing (never a guess).
pub fn markerless_released_hops(
    evictions: &[(String, Option<String>)],
    hops: &[HopEntry],
    seats: &[EvictedSeat],
    twin_keys: &[(String, u32, crate::results::CommittedKeys)],
    hop_outputs: &HashMap<String, (Vec<u8>, u64)>,
) -> Vec<HopEntry> {
    let held: HashSet<String> = hops
        .iter()
        .filter(|h| h.marker_verified == MarkerVerification::Verified)
        .map(|h| outpoint_key(&h.hop_txid, h.hop_vout))
        .collect();
    let mut keys_by_txid: HashMap<String, Option<&crate::results::CommittedKeys>> = HashMap::new();
    for (txid, _vout, keys) in twin_keys {
        keys_by_txid
            .entry(txid.to_ascii_lowercase())
            .and_modify(|k| {
                if k.is_some_and(|x| x != keys) {
                    *k = None;
                }
            })
            .or_insert(Some(keys));
    }
    let mut seat_by_txid: HashMap<String, Option<&EvictedSeat>> = HashMap::new();
    for s in seats {
        seat_by_txid
            .entry(s.pot_txid.to_ascii_lowercase())
            .and_modify(|held| {
                if held.is_some_and(|x| {
                    !x.seat_settle_pubkey
                        .eq_ignore_ascii_case(&s.seat_settle_pubkey)
                        || !x.game_id.eq_ignore_ascii_case(&s.game_id)
                }) {
                    *held = None;
                }
            })
            .or_insert(Some(s));
    }
    let mut out: std::collections::BTreeMap<String, Option<HopEntry>> =
        std::collections::BTreeMap::new();
    for (evicted, released) in evictions {
        let evicted = evicted.to_ascii_lowercase();
        let Some(Some(seat)) = seat_by_txid.get(&evicted) else {
            continue;
        };
        let Some(Some(keys)) = keys_by_txid.get(&evicted) else {
            continue;
        };
        let my_key = seat.seat_settle_pubkey.to_ascii_lowercase();
        if (my_key == keys.pub_a) == (my_key == keys.pub_b) {
            continue; // neither committed key is mine, or both are: no seat
        }
        let Ok(key_bytes) = hex::decode(&my_key) else {
            continue;
        };
        if key_bytes.len() != 33 {
            continue;
        }
        let my_lock =
            overlay_discovery::pot::p2pkh_lock(&bsv_rs::primitives::hash::hash160(&key_bytes));
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
            let Ok(vout) = u32::try_from(vout) else {
                continue;
            };
            if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            let key = outpoint_key(txid, vout);
            if held.contains(&key) {
                continue;
            }
            let Some((lock, sats)) = hop_outputs.get(&key) else {
                continue;
            };
            if *lock != my_lock {
                continue; // the other seat's hop (or not a hop of K): not mine
            }
            let entry = HopEntry {
                game_id: seat.game_id.to_ascii_lowercase(),
                hop_txid: txid.to_ascii_lowercase(),
                hop_vout: vout,
                hop_sats: *sats,
                opponent_identity: seat.opponent_identity.clone(),
                seat_settle_pubkey: my_key.clone(),
                seat_sig_hex: String::new(),
                identity_sig_hex: String::new(),
                marker_txid: String::new(),
                marker_vout: 0,
                spent: None,
                spending_txid: None,
                spent_confirmed: None,
                status: HopStatus::Unknown,
                status_source: None,
                marker_verified: MarkerVerification::Verified,
                marker_created_at: None,
            };
            out.entry(key)
                .and_modify(|held| {
                    if held.as_ref().is_some_and(|x| {
                        x.game_id != entry.game_id
                            || x.seat_settle_pubkey != entry.seat_settle_pubkey
                    }) {
                        *held = None;
                    }
                })
                .or_insert(Some(entry));
        }
    }
    out.into_values().flatten().collect()
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
pub async fn pay_vout_pass(
    world: &mut impl PayVoutWorld,
    wanted: &[(String, String)],
    chunk: usize,
    turn: u64,
) -> PayVoutPass {
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

/// bsv-low #618: the recorded spenders (lowercase, sorted, deduplicated) whose words the owed path reads BEFORE
/// assembling its views: every hydrated row's `spending_txid`.
pub fn word_arm_subjects<'a>(spenders: impl IntoIterator<Item = Option<&'a str>>) -> Vec<String> {
    let ids: std::collections::BTreeSet<String> = spenders
        .into_iter()
        .flatten()
        .filter(|t| crate::logic::valid_txid(t))
        .map(str::to_ascii_lowercase)
        .collect();
    ids.into_iter().collect()
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

fn non_pot_spender(i: &OwedInputs, h: &HopEntry, outpoint: &str) -> Option<(String, bool)> {
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
                Some((s.clone(), confirmed))
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
    let confirmed = spender_mined(i, Some(&s), w.spent_confirmed == Some(true))
        || i.proof_mined_spenders.contains(&s);
    Some((s.clone(), confirmed))
}

/// The games whose `collected` filings the recompute must read: every SPENT pot the results name AND every hop the
/// hops view names (a swept hop's payout is retired by a `collected` filing for its GAME, and a hop-only game — the
/// stake swept before any JOIN — has no pot row at all; the collect pass of 2026-09-19 pressed five such payouts to
/// "already in your wallet", filed each, and the rows stood because this lookup read the pots' games only). Sorted,
/// deduped, lower-case: one chunked `IN (...)` read.
pub fn collected_lookup_games<'a>(
    spent_result_games: impl Iterator<Item = &'a str>,
    hop_games: impl Iterator<Item = &'a str>,
) -> Vec<String> {
    let mut games: Vec<String> = spent_result_games
        .chain(hop_games)
        .map(str::to_ascii_lowercase)
        .collect();
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
            let confirmed = index_word
                || tracker_word
                || index_proven_or_spender_mined(
                    i,
                    Some(&filed.sweep_txid),
                    index_latched,
                    index_word
                        || (named_by_index && h.spent_confirmed == Some(true))
                        || by_chain.and_then(|w| w.spent_confirmed) == Some(true),
                );
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
    Some(SweptHome {
        raw_hex: None,
        pays_sats: Some(sum),
        confirmed,
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
        None => {
            // bsv-low #608: a story that rests on no spender bytes is re-judged on the next read past a minute
            facts[OWED_FACT_SPENDER_UNREAD] = json!(true);
            unknown
        }
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
                    // bsv-low #736: `spentConfirmed` is the index's latch, a proof with or without its height
                    let confirmed = e.at_height.is_some()
                        || index_proven_or_spender_mined(
                            i,
                            e.settle_txid.as_deref(),
                            e.spent_confirmed == Some(true),
                            e.spent_confirmed == Some(true) || e.at_height.is_some(),
                        );
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
                    let awaiting_block = e.verdict.is_none()
                        && e.settle_txid.is_some()
                        && e.at_height.is_none()
                        && !index_proven_or_spender_mined(
                            i,
                            e.settle_txid.as_deref(),
                            e.spent_confirmed == Some(true),
                            e.spent_confirmed == Some(true) || e.at_height.is_some(),
                        );
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
                    facts[OWED_FACT_SPENDER_UNREAD] = json!(true);
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

/// bsv-low #669 MEDIUM-1 (the fold of the FIX-669 from-scratch lens): the two tips a served `/owed` body carries,
/// decoupled. Given the STORED snapshot tip (`owed_state.tip`, `prev_tip`, possibly `None` when that column is SQL
/// NULL) and the present tip this request read (`tip_now`, the 30-s isolate cache), this returns
/// `(display_tip, gate_tip)`:
///   - `display_tip = prev_tip.or(tip_now)` — the body's `tip` field, UNCHANGED: a NULL stored snapshot still shows
///     the present tip, so the client's "as of block N" line is never blank.
///   - `gate_tip = prev_tip` — the TRUE tip the snapshot's chain probe was taken at, fed to
///     [`degrade_stale_sweeps`]. A NULL stored tip reaches the gate as `None`, so the gate hits its fail-safe
///     `Degrade::Unknown` arm and degrades the sweep, instead of reading the collapsed display tip as tip-current.
///
/// Why the decoupling is load-bearing: feeding the gate the DISPLAY tip (`prev_tip.or(tip_now)`, the pre-fold route)
/// collapses a NULL stored tip to `tip_now`, so the serve reads `(Some(tn), Some(tn))` -> `Degrade::TipCurrent` and
/// the `Degrade::Unknown` arm is DEAD in production: an identity whose FIRST owed compute coincided with a
/// chaintracks blip (`owed_state.tip` written NULL) keeps offering a claimable (dead) sweep across a block. The
/// money is network-safe either way (consensus refuses the second spend), but the PRESS must not lie.
pub fn owed_served_tips(prev_tip: Option<u64>, tip_now: Option<u64>) -> (Option<u64>, Option<u64>) {
    (prev_tip.or(tip_now), prev_tip)
}

/// bsv-low #669: the READ-TIME freshness gate for a `hop-stranded` sweep claim (the design's §2a). The claimable
/// verdict in a served row rests on a chain probe taken when its snapshot was computed, at `snapshot_tip`. If the
/// chain has moved on since (`present_tip > snapshot_tip`), or the present tip cannot be read at all, that verdict
/// is no longer provably current and the row degrades, IN THE SERVED BODY ONLY, to a `pending` chain-wait:
/// `claim:null`, `claimable:false`, `chainWait:"block"`, so the client offers no press and the harness reads a block
/// wait. The write path still stores the honest probe-current verdict; this is read-time provenance, not stored
/// state, and the ONE place the rule lives. Fail-safe: an uncertain read never offers a dead sweep. A fresh compute
/// (snapshot_tip == present_tip) never degrades; the tip-triggered recompute re-points the row at the new tip.
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
        r.reason = Some(SWEEP_TIP_MOVED_REASON.to_string());
    }
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
/// bsv-low #566: the same upsert bumps the identity's `recomputeSeq` (1 on its first snapshot), so the sequence moves
/// once per LANDED write and a refused one (the `WHERE`) leaves it; a row written before the column reads NULL and
/// counts from 1.
pub const OWED_STATE_UPSERT_SQL: &str = "INSERT INTO owed_state (identity, computedAtMs, tip, rows, stale, truncated, recomputeSeq) VALUES (?1, ?2, ?3, ?4, 0, ?5, 1) ON CONFLICT(identity) DO UPDATE SET computedAtMs = excluded.computedAtMs, tip = excluded.tip, rows = excluded.rows, stale = 0, truncated = excluded.truncated, recomputeSeq = COALESCE(owed_state.recomputeSeq, 0) + 1 WHERE excluded.computedAtMs >= owed_state.computedAtMs";
/// bsv-low #566: the per-identity recompute sequence served on `/owed` (overlay migration 260, byte-identical; a cold
/// app-layer isolate adds it, `schema::LATCH_COLUMN_ALTERS`, because the snapshot's upsert and read name it).
pub const OWED_STATE_SEQ_ALTER: &str = "ALTER TABLE owed_state ADD COLUMN recomputeSeq INTEGER";
/// One bind of the owed write, host-typed so the route (D1) and the real-SQLite pin run the SAME statements with the
/// SAME values (`owed_write_plan`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwedBind {
    Text(String),
    Int(i64),
    Null,
}
/// The read-back that closes the write batch: the stamp the identity's marker carries once the batch ran.
pub const OWED_STATE_STAMP_SQL: &str =
    "SELECT computedAtMs, recomputeSeq FROM owed_state WHERE identity = ?1";

/// PURE: the ONE batch that writes a snapshot (the identity's rows replaced, the marker stamped, the marker's stamp
/// read back), in order, all or nothing. The route binds it to D1; the pin runs it on real SQLite.
pub fn owed_write_plan(
    identity_lc: &str,
    rows: &[OwedRow],
    computed_at_ms: i64,
    tip: Option<u64>,
    truncated: bool,
) -> Vec<(&'static str, Vec<OwedBind>)> {
    let text = |s: &str| OwedBind::Text(s.to_string());
    let opt_text = |s: Option<&str>| s.map_or(OwedBind::Null, |s| OwedBind::Text(s.to_string()));
    let opt_int = |v: Option<u64>| v.map_or(OwedBind::Null, |v| OwedBind::Int(v as i64));
    let mut plan: Vec<(&'static str, Vec<OwedBind>)> = Vec::with_capacity(rows.len() + 3);
    plan.push((
        OWED_ROWS_DELETE_SQL,
        vec![text(identity_lc), OwedBind::Int(computed_at_ms)],
    ));
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
        vec![
            text(identity_lc),
            OwedBind::Int(computed_at_ms),
            opt_int(tip),
            OwedBind::Int(rows.len() as i64),
            OwedBind::Int(i64::from(truncated)),
        ],
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
/// One served snapshot: the rows, the tip, the walk's stamp, the cut bit, the identity's recompute sequence (#566).
pub type OwedSnapshot = (Vec<OwedRow>, Option<u64>, i64, bool, Option<i64>);
/// PURE: what a SUPERSEDED read answers with: the table's snapshot (the newer walk's) when the stored read
/// returned one, else its own with every claimable word cleared (`superseded_fallback_rows`) and no sequence (the
/// walk's snapshot is not the one the row's sequence counts).
pub fn superseded_read_snapshot(stored: Option<OwedSnapshot>, own: OwedSnapshot) -> OwedSnapshot {
    stored.unwrap_or_else(|| (superseded_fallback_rows(own.0), own.1, own.2, own.3, None))
}

pub const OWED_STATE_READ_SQL: &str = "SELECT s.identity, s.computedAtMs, s.tip, s.rows, s.stale, s.truncated, c.spendingTxid AS markerlessProgress, s.recomputeSeq FROM owed_state s LEFT JOIN hop_chain_probes c ON c.outpoint = 'homecursor:markerless:' || s.identity || '.0' WHERE s.identity = ?1";
pub const OWED_ROWS_READ_SQL: &str = "SELECT identity, outpoint, family, gameId, sats, opponentIdentity, atHeight, facts, updatedAtMs, reason FROM owed_rows WHERE identity = ?1 ORDER BY CASE family WHEN 'payout' THEN 0 WHEN 'refund-due' THEN 1 WHEN 'hop-stranded' THEN 2 WHEN 'in-progress' THEN 3 ELSE 4 END, COALESCE(atHeight, 0) DESC, outpoint ASC LIMIT 501";
/// HIGH-4: the tip flips the gate — every identity party to an UNSPENT pot whose recovery height the new tip has
/// reached (a small band below it, so a missed block still lands) is marked STALE; its next read recomputes.
pub const OWED_STALE_ON_TIP_SQL: &str = "UPDATE owed_state SET stale = 1 WHERE identity IN (SELECT DISTINCT pp.identity FROM potparty_records pp JOIN pot_records p ON p.txid = pp.potTxid AND p.outputIndex = pp.potVout WHERE p.spent = 0 AND p.recoveryHeight IS NOT NULL AND p.recoveryHeight <= ?1 AND p.recoveryHeight >= ?1 - 6)";
/// bsv-low #669: the twin of [`OWED_STALE_ON_TIP_SQL`] for HOP-STRANDED rows. A non-LOW competitor's confirming
/// block fires no `pot-changed` / `hop-changed` / `/internal/tx-word` hook (LOW's door never saw the spend), so the
/// #469 pot gate above never names a stranded-hop identity and the sweep claim stays frozen at the compute tip. This
/// marks STALE exactly the identities that currently hold a LIVE claimable stranded-hop row (keyed on the derived
/// row, itself keyed on the hop OUTPOINT — never a name): the next read / the #648 deferred alarm re-probes the chain
/// at the new tip and re-points the hop to `spent-elsewhere` (or restores the claim if the hop is genuinely unspent).
/// It takes NO bind (the set is the derived rows, not a height band), and marks nothing but claimable stranded hops.
/// N (parity with the #469 twin's own notes): `family = 'hop-stranded'` is an exact match on the derived row's own
/// column (no `lower()` scan); `json_extract(facts, '$.claimable') = 1` is the same read the markerless fault path
/// already runs (`routes.rs` `markerlessEvidence`); the set is bounded by the rare stranded-hop case, never the
/// population, and the per-identity recompute ceiling + the shed/deferred mechanism cap the fan-out on a storm.
pub const OWED_STALE_ON_TIP_HOPS_SQL: &str = "UPDATE owed_state SET stale = 1 WHERE identity IN (SELECT DISTINCT identity FROM owed_rows WHERE family = 'hop-stranded' AND json_extract(facts, '$.claimable') = 1)";
/// HIGH-4: a filing on a pot marks BOTH parties stale (the counterparty's valid refund makes MY pot claimable).
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
pub const OWED_STALE_FOR_POT_SQL: &str = "UPDATE owed_state SET stale = 1 WHERE identity IN (SELECT DISTINCT identity FROM potparty_records WHERE potTxid = ?1 AND potVout = ?2)";
// N3: both stale marks join EXACTLY (`idx_potparty_pot`, the `pot_records` PK), as every sibling query in this crate
// does (`results_sql`: `r.txid = pp.potTxid`); a `lower()` on a join key would scan the table per block / per filing.
// N8: the filing mark is keyed on a CALLER-CHOSEN pot: a stranger's verified filing can mark both parties of any pot
// stale — bounded by the per-(poster, family, day) filing caps, and the cost is one recompute on the victim's next
// read, never a wrong row.
// N9: the pot-changed hook reaches the identities with a VERIFIED seat marker (since E649 L4 every attributed seat,
// no `/results` gather on the hook; it was those whose entry sat on their first results page); a hop-only seat is
// covered by the read's staleness rule below.
/// HIGH-4: a filing by an identity marks it stale (a `collected` retires a payout on its next read).
pub const OWED_STALE_FOR_IDENTITY_SQL: &str = "UPDATE owed_state SET stale = 1 WHERE identity = ?1";
/// MEDIUM-5: the probe before a first-read compute — an identity with no party row and no hop row has nothing to
/// derive and gets NO state row written (a public read must not write state keyed on a claimable name).
pub const OWED_IDENTITY_PROBE_SQL: &str = "SELECT 1 AS present FROM potparty_records WHERE identity = ?1 UNION ALL SELECT 1 FROM hopparty_records WHERE identity = ?1 UNION ALL SELECT 1 FROM potparty_records_evicted WHERE identity = ?1 LIMIT 1";
/// HIGH-4: the read's staleness rule — a list with an OPEN row (anything but a confirmed payout) is re-derived after
/// this age; any list after `OWED_RECOMPUTE_ANY_AFTER_MS` (a stranded hop crosses its window with no other trigger).
pub const OWED_RECOMPUTE_OPEN_AFTER_MS: i64 = 5 * 60 * 1000;
pub const OWED_RECOMPUTE_ANY_AFTER_MS: i64 = 15 * 60 * 1000;

/// A row is OPEN when it is anything but a claimable payout: it can change without a pot-changed event (a gate opens,
/// a hop crosses its window, a counterparty files). bsv-low #484 (delta fold, D-M1): so is a payout whose chain
/// word was a "confirmed" taken inside the reorg grace (`hops_view::mark_reorg_grace_rows`): asked again in five minutes.
pub fn row_is_open(r: &OwedRow) -> bool {
    r.family != OwedFamily::Payout
        || r.facts["claimable"] == Value::Bool(false)
        || r.facts[crate::hops_view::OWED_FACT_REORG_GRACE_WORD] == Value::Bool(true)
}

/// bsv-low #608: the fact a row carries when its "could not judge" sentence rests on NO spender bytes (the spender is
/// named, its bytes were not read this pass, or the pot-spenders read faulted): the bytes may arrive between passes
/// (#606 made a courier's mempool body and the chain rung's verified raw readable), so such a row is re-judged on
/// the next read once [`OWED_SPENDER_REJUDGE_AFTER_MS`] has passed, never waiting for the open list's 5 minutes.
pub const OWED_FACT_SPENDER_UNREAD: &str = "spenderUnread";
/// bsv-low #608: the bound of that re-judgement: at most one recompute a minute per identity on its reads (and inside
/// the identity's #499 ceiling), so a dead courier never makes every read recompute.
pub const OWED_SPENDER_REJUDGE_AFTER_MS: i64 = 60_000;
const _: () = assert!(OWED_SPENDER_REJUDGE_AFTER_MS < OWED_RECOMPUTE_OPEN_AFTER_MS);

/// The read's rule (N5, the ONE predicate): recompute when the marker says stale; when the tip advanced past an
/// in-progress row's recovery height since the compute; when an open list is older than 5 minutes; when any list is
/// older than 15 minutes (an empty list must not stay "nothing owed" forever; a stranded hop crosses its window with
/// no other trigger).
pub fn should_recompute(
    stale: bool,
    age_ms: i64,
    prev_tip: Option<u64>,
    tip_now: Option<u64>,
    rows: &[OwedRow],
) -> bool {
    recompute_before_unread(stale, age_ms, prev_tip, tip_now, rows)
        || spender_unread_due(age_ms, rows)
}

/// bsv-low #608 (the B-OWED-2 lens's LOW-1): a row still marked [`OWED_FACT_SPENDER_UNREAD`] in a snapshot older than
/// [`OWED_SPENDER_REJUDGE_AFTER_MS`].
fn spender_unread_due(age_ms: i64, rows: &[OwedRow]) -> bool {
    age_ms > OWED_SPENDER_REJUDGE_AFTER_MS
        && rows
            .iter()
            .any(|r| r.facts[OWED_FACT_SPENDER_UNREAD] == Value::Bool(true))
}

/// PURE (bsv-low #608, the B-OWED-2 lens's LOW-1): the read recomputes ONLY because of the unread-spender rule (no
/// other rule of [`should_recompute`] would have): the re-judgement #608 buys, counted
/// [`COUNTER_SPENDER_REJUDGE`] by the read.
pub fn spender_rejudge_only(
    stale: bool,
    age_ms: i64,
    prev_tip: Option<u64>,
    tip_now: Option<u64>,
    rows: &[OwedRow],
) -> bool {
    !recompute_before_unread(stale, age_ms, prev_tip, tip_now, rows)
        && spender_unread_due(age_ms, rows)
}

/// bsv-low #608 (the B-OWED-2 lens's LOW-1): the durable `ops_counters` row (the census flush; the overlay seeds it
/// at 0), bumped once per read whose recompute only the unread-spender rule asked for.
pub const COUNTER_SPENDER_REJUDGE: &str = "owed_spender_rejudge_total";
/// bsv-low #608 (LOW-1): count one re-judgement ([`spender_rejudge_only`]).
pub fn note_spender_rejudge() {
    crate::courier::note_presence(COUNTER_SPENDER_REJUDGE);
}

/// PURE (bsv-low #608, the B-OWED-2 lens's LOW-1): a spender this pass WITHHELD by its own bound (the courier cap,
/// the clock or the stored-read tail; `withheld`, lowercase, [`SpenderLook::withheld`]) is not an absence of bytes:
/// the rotation reaches it on a later pass at the open list's cadence, so its row's mark is taken off and the read
/// does not recompute every minute for it. The mark stays on a spender LOOKED at that gave no bytes (a courier with
/// nothing, a stored read that missed) and on every row of a pass whose pot-spenders read faulted (`faulted`: a
/// fault, never a bound). Returns the marks taken off.
pub fn unmark_withheld_spenders(
    rows: &mut [OwedRow],
    withheld: &HashSet<String>,
    faulted: bool,
) -> usize {
    if faulted {
        return 0;
    }
    let mut n = 0;
    for r in rows.iter_mut() {
        if r.facts[OWED_FACT_SPENDER_UNREAD] != Value::Bool(true) {
            continue;
        }
        let named = r.facts["chainSpender"]
            .as_str()
            .or_else(|| r.facts["spendingTxid"].as_str())
            .map(str::to_ascii_lowercase);
        if named.is_some_and(|sp| withheld.contains(&sp)) {
            if let Some(obj) = r.facts.as_object_mut() {
                obj.remove(OWED_FACT_SPENDER_UNREAD);
                n += 1;
            }
        }
    }
    n
}

fn recompute_before_unread(
    stale: bool,
    age_ms: i64,
    prev_tip: Option<u64>,
    tip_now: Option<u64>,
    rows: &[OwedRow],
) -> bool {
    if stale {
        return true;
    }
    let gate_flipped = match tip_now {
        Some(t) if prev_tip.is_none_or(|p| p < t) => rows.iter().any(|r| {
            // the refund gate opens when the tip reaches an in-progress row's recovery height; bsv-low #669: a
            // claimable hop-stranded sweep's chain probe is no longer tip-current, so a moved tip must re-probe it
            // (a non-LOW competitor's block fires no other hook) — the serve-then-refresh runs now, not in 5 minutes.
            (r.family == OwedFamily::InProgress
                && r.facts["recoveryHeight"].as_u64().is_some_and(|h| t >= h))
                || (r.family == OwedFamily::HopStranded
                    && r.facts["claimable"] == Value::Bool(true))
        }),
        _ => false,
    };
    if gate_flipped {
        return true;
    }
    // bsv-low #608: a "could not judge the spender" row whose bytes were not read is asked again past a minute
    // (`spender_unread_due`, in `should_recompute`)
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
pub const RECOMPUTE_SOURCES: [&str; 7] = [
    "pot-changed",
    "hop-changed",
    "filing",
    "read-first",
    "read-stale",
    "read-aged",
    "deferred",
];
static RECOMPUTE_BY_SOURCE: [AtomicU64; 7] = [
    AtomicU64::new(0),
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
/// bsv-low #628: every recompute this isolate finished ([`note_recompute`]), served on `/health.owed` as
/// `recomputesThisIsolate` (reset with the isolate); its durable twin [`COUNTER_RECOMPUTES`] is the fleet's, the
/// denominator of the owed fault rates. Not #566's sequence: that one is per identity, in `owed_state.recomputeSeq`.
static RECOMPUTES_THIS_ISOLATE: AtomicU64 = AtomicU64::new(0);
/// The durable recompute count (the overlay seeds it at 0).
pub const COUNTER_RECOMPUTES: &str = "applayer_owed_recomputes_total";
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
    names.push(COUNTER_RECOMPUTES.to_string());
    names.push(COUNTER_REFUND_FUNDING_UNHELD.to_string());
    names.push(COUNTER_SPENDER_REJUDGE.to_string());
    names.push("applayer_owed_read_faults_total".to_string());
    names.extend(
        OWED_READ_FAULT_KINDS
            .iter()
            .map(|k| format!("applayer_owed_read_faults_{k}_total")),
    );
    names
}
static COLLECTED_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
static POT_SPENDERS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
static HOP_SWEEPS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
/// #517: the filed sweeps' index-proof read faulted (the pass confirms swept payouts on the courier path alone).
static SWEEP_PROOFS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
/// #517 (the gate's LOW-1): a proven filed sweep met a CONTRADICTING confirmed spender (the hop row's or the chain
/// rung's) and the proof did not decide — a reorg the latch missed, for the operator to see.
static SWEEP_PROOF_CONTRADICTIONS: AtomicU64 = AtomicU64::new(0);
/// bsv-low #692 (RC-L34): a swept-payout row reached "home on chain" (confirmed) from a CHAINTRACKS-CHECKED mined
/// proof of the sweep ([`OwedInputs::proof_mined_spenders`]) rather than the spent-any ladder's `spent_confirmed`:
/// the row the only confirming courier (WoC `/0/spent`) could not confirm because it faulted.
static SWEPT_CONFIRMED_BY_PROOF: AtomicU64 = AtomicU64::new(0);
static SPENDER_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
pub fn note_spender_read_fault() {
    SPENDER_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
    mirror_read_fault("spender");
}
/// The L25-WITHHELD lens's LOW-3: the withheld payouts' previous-row read (`OWED_WITHHELD_PREVIOUS_SQL`) faulted
/// (that pass's rows stand as derived: could not judge).
static WITHHELD_PREVIOUS_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
pub fn note_withheld_previous_read_fault() {
    WITHHELD_PREVIOUS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
    mirror_read_fault("withheld_previous");
}
/// bsv-low #672 (RC-L30): the durable spender-bytes store's counters. HITS: a named spender served from the store
/// before the budget (no courier). PERSISTED: a hash-verified spender's bytes written for the first time. CARRIED:
/// a judged `spent-elsewhere` row kept by [`keep_withheld_elsewhere`] (the belt). READ FAULTS: the store read
/// faulted (the pass judges as before, nothing invented). SWEEP: a sweep pass ran.
static SPENDER_STORE_HITS: AtomicU64 = AtomicU64::new(0);
static SPENDER_STORE_PERSISTED: AtomicU64 = AtomicU64::new(0);
static SPENDER_STORE_CARRIED: AtomicU64 = AtomicU64::new(0);
static SPENDER_STORE_READ_FAULTS: AtomicU64 = AtomicU64::new(0);
static SPENDER_STORE_SWEEP_PASSES: AtomicU64 = AtomicU64::new(0);
pub fn note_spender_store_hits(n: u64) {
    SPENDER_STORE_HITS.fetch_add(n, Ordering::Relaxed);
}
pub fn note_spender_store_persisted(n: u64) {
    SPENDER_STORE_PERSISTED.fetch_add(n, Ordering::Relaxed);
}
pub fn note_spender_store_carried(n: u64) {
    SPENDER_STORE_CARRIED.fetch_add(n, Ordering::Relaxed);
}
pub fn note_spender_store_read_fault() {
    SPENDER_STORE_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
    mirror_read_fault("spender_store");
}
pub fn note_spender_store_sweep_pass() {
    SPENDER_STORE_SWEEP_PASSES.fetch_add(1, Ordering::Relaxed);
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
    mirror_read_fault("collected");
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
    mirror_read_fault("pay_vout");
}
pub fn note_pot_spenders_read_fault() {
    POT_SPENDERS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
    mirror_read_fault("pot_spenders");
}
pub fn note_hop_sweeps_read_fault() {
    HOP_SWEEPS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
    mirror_read_fault("hop_sweeps");
}
pub fn note_sweep_proofs_read_fault() {
    SWEEP_PROOFS_READ_FAULTS.fetch_add(1, Ordering::Relaxed);
    mirror_read_fault("sweep_proofs");
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
/// The route counts the rows that carry `sweepProofContradicted` (one per hop per recompute).
pub fn note_sweep_proof_contradictions(n: u64) {
    if n > 0 {
        SWEEP_PROOF_CONTRADICTIONS.fetch_add(n, Ordering::Relaxed);
        crate::courier::note_presence_n("applayer_owed_sweep_proof_contradictions_total", n);
    }
}
/// PURE: how many rows of one derivation carry the contradiction fact (the route's count, pinned).
pub fn count_sweep_proof_contradictions(rows: &[OwedRow]) -> u64 {
    rows.iter()
        .filter(|r| {
            r.facts
                .get("sweepProofContradicted")
                .and_then(|v| v.as_bool())
                == Some(true)
        })
        .count() as u64
}
/// bsv-low #692 (RC-L34): the route counts the confirmed swept-payout rows whose confirmation came from a
/// chaintracks-checked mined PROOF of the sweep (`confirmedSource == "proof"`), not the spent-any ladder.
pub fn note_swept_confirmed_by_proof(n: u64) {
    if n > 0 {
        SWEPT_CONFIRMED_BY_PROOF.fetch_add(n, Ordering::Relaxed);
    }
}
/// PURE: how many rows of one derivation reached "home on chain" from the proof (`confirmedSource == "proof"`).
pub fn count_swept_confirmed_by_proof(rows: &[OwedRow]) -> u64 {
    rows.iter()
        .filter(|r| r.facts.get("confirmedSource").and_then(|v| v.as_str()) == Some("proof"))
        .count() as u64
}

/// bsv-low #693 (the #692 lens's MEDIUM-1): the re-asks of a DURABLE spender (its bytes served from the #672 store,
/// so phase 1 and 2 never ask its `/tx-any`) whose confirmation is still unconfirmed: at most ONE a pass. It is taken
/// from the courier slots the never-asked spenders left (`OWED_SPENDER_COURIER_READS_PER_RECOMPUTE`, 2), never
/// beside them, and under the pass's clock, so a pass still makes at most 2 courier `/tx-any` asks (each with its
/// one `/proof/tsc`) and no new budget, table or D1 statement is added.
pub const OWED_DURABLE_REASKS_PER_RECOMPUTE: usize = 1;
/// The durable `ops_counters` rows of the re-ask (via `courier::note_presence`, the census flush): every re-ask made
/// (an answer the isolate held inside its TTL, or a fresh courier ask), then exactly one outcome each.
pub const COUNTER_DURABLE_REASK_ASKED: &str = "owed_durable_spender_reask_asked_total";
pub const COUNTER_DURABLE_REASK_MINED_BY_PROOF: &str =
    "owed_durable_spender_reask_mined_by_proof_total";
pub const COUNTER_DURABLE_REASK_STILL_UNCONFIRMED: &str =
    "owed_durable_spender_reask_still_unconfirmed_total";
pub const COUNTER_DURABLE_REASK_FAULT: &str = "owed_durable_spender_reask_fault_total";

/// The outcome of one re-ask of a durable spender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableReaskOutcome {
    /// A chaintracks-checked mined proof (`txany::proof_mined_word`): the spender joins `proof_mined_spenders`.
    MinedByProof,
    /// An answer with a word (present, raw bytes, or an absence) but no checked mined proof.
    StillUnconfirmed,
    /// No word at all (a courier fault, or a fault the isolate cached inside its TTL): could not look.
    Fault,
}

impl DurableReaskOutcome {
    pub fn of(a: &crate::txany::TxAnyAnswer) -> Self {
        if crate::txany::proof_mined_word(a) {
            DurableReaskOutcome::MinedByProof
        } else if a.present.is_none() && a.confirmed.is_none() && a.raw_hex.is_none() {
            DurableReaskOutcome::Fault
        } else {
            DurableReaskOutcome::StillUnconfirmed
        }
    }
    pub fn counter(self) -> &'static str {
        match self {
            DurableReaskOutcome::MinedByProof => COUNTER_DURABLE_REASK_MINED_BY_PROOF,
            DurableReaskOutcome::StillUnconfirmed => COUNTER_DURABLE_REASK_STILL_UNCONFIRMED,
            DurableReaskOutcome::Fault => COUNTER_DURABLE_REASK_FAULT,
        }
    }
}

/// One re-ask made: the spender, what it answered, and whether a courier was really asked (a slot) or the isolate's
/// cached answer served (free).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableReask {
    pub spender: String,
    pub outcome: DurableReaskOutcome,
    pub courier_asked: bool,
}

/// PURE (bsv-low #693): does a durable spender's row still need the proof? True when some hop names it through
/// [`non_pot_spender`]'s two arms (the index's recorded spend, else the chain rung's word of spent) and that arm's
/// confirmation is not already in hand: the tracker's word (`tracker_mined`: `Some(true)` mined, `Some(false)` a row
/// that is not mined, which [`spender_mined`] reads in place of the ladder; `None` no row) or the ladder's
/// `spent_confirmed`. A hop whose FILED sweep is this spender is skipped: the filed arm of [`swept_home`] judges it
/// and never reads the proof. A spender already confirmed is never re-asked.
pub fn durable_spender_needs_proof(
    spender: &str,
    hops: &[HopEntry],
    hop_chain: &HashMap<String, HopChainWord>,
    hop_sweeps: &HashMap<String, crate::hopsweep::FiledHopSweep>,
    tracker_mined: Option<bool>,
) -> bool {
    if tracker_mined == Some(true) {
        return false;
    }
    let unconfirmed = |ladder: Option<bool>| tracker_mined.is_some() || ladder != Some(true);
    hops.iter().any(|h| {
        let outpoint = outpoint_key(&h.hop_txid, h.hop_vout);
        if hop_sweeps
            .get(&outpoint)
            .is_some_and(|f| f.sweep_txid.eq_ignore_ascii_case(spender))
        {
            return false;
        }
        if h.spent == Some(true) {
            if let Some(s) = h.spending_txid.as_deref() {
                return s.eq_ignore_ascii_case(spender) && unconfirmed(h.spent_confirmed);
            }
        }
        hop_chain.get(&outpoint).is_some_and(|w| {
            w.looked
                && w.spent == Some(true)
                && w.spending_txid
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(spender))
                && unconfirmed(w.spent_confirmed)
        })
    })
}

/// bsv-low #693: the re-asks of one pass. `candidates` are the durable spenders that still need the proof, in the
/// courier phase's ask order; `courier_room` the slots the phase left; `stop` the pass's clock. At most
/// [`OWED_DURABLE_REASKS_PER_RECOMPUTE`] are made, none when no slot is left (a re-ask may need a fresh ask).
/// `ask` answers one spender: `(answer, courier_asked)`. Each re-ask is counted (asked, then its outcome).
pub async fn durable_reasks<A, Fut>(
    candidates: Vec<String>,
    courier_room: usize,
    stop: impl Fn() -> bool,
    mut ask: A,
) -> Vec<(DurableReask, crate::txany::TxAnyAnswer)>
where
    A: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = (crate::txany::TxAnyAnswer, bool)>,
{
    let mut out = Vec::new();
    for sp in candidates
        .into_iter()
        .take(OWED_DURABLE_REASKS_PER_RECOMPUTE.min(courier_room))
    {
        if stop() {
            break;
        }
        let (answer, courier_asked) = ask(sp.clone()).await;
        let outcome = DurableReaskOutcome::of(&answer);
        crate::courier::note_presence(COUNTER_DURABLE_REASK_ASKED);
        crate::courier::note_presence(outcome.counter());
        out.push((
            DurableReask {
                spender: sp,
                outcome,
                courier_asked,
            },
            answer,
        ));
    }
    out
}

/// PURE (bsv-low #693 fold, the W1-OWED-693 lens's LOW-2): the sweeps whose served row WAITS FOR THE BLOCK (a
/// courier-bytes payout, `chainWait == "block"`), lowercase: the only spenders phase 2b re-asks. A payout the
/// derivation retires (a verified `collected`, a held filing, the home output spent) or a pointer the bytes refute
/// serves no such row, so its durable spender is never re-asked.
pub fn sweeps_waiting_for_the_block(rows: &[OwedRow]) -> HashSet<String> {
    rows.iter()
        .filter(|r| {
            r.family == OwedFamily::Payout
                && r.facts["sweepSource"] == "courier-bytes"
                && r.facts["chainWait"] == "block"
        })
        .filter_map(|r| r.facts["sweepTxid"].as_str().map(str::to_ascii_lowercase))
        .collect()
}

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
/// The most proof latches one pass writes (its proofs come from at most two courier asks and one re-ask; the rest of
/// a pass's proofs, from answers the isolate already held, are latched by a later pass, which holds them too).
pub const OWED_PROOF_LATCH_WRITES_PER_RECOMPUTE: usize =
    OWED_SPENDER_COURIER_READS_PER_RECOMPUTE + OWED_DURABLE_REASKS_PER_RECOMPUTE;
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

/// PURE: the latch of one spender's `/tx-any` answer, ONLY for a chaintracks-checked mined word
/// ([`crate::txany::proof_mined_word`]: `confirmed` AND a proven `height`) and the canonical root at that height. A
/// courier's bare `confirmed`, a bare `claimed_height`, a SEEN or a fault writes nothing; nor does a missing root.
pub fn proof_latch_memo(
    spender: &str,
    answer: &crate::txany::TxAnyAnswer,
    canonical_root: Option<&str>,
    now_ms: i64,
) -> Option<crate::hops_view::ProbeMemo> {
    if !crate::txany::proof_mined_word(answer) {
        return None;
    }
    let height = u32::try_from(answer.height?).ok()?;
    let root = canonical_root.filter(|r| is_hex64(r))?;
    if !is_hex64(spender) {
        return None;
    }
    Some(crate::hops_view::ProbeMemo {
        outpoint: crate::hops_view::probe_memo_key(
            &format!("{PROOF_MINED_LATCH_PREFIX}{}", spender.to_ascii_lowercase()),
            height,
        ),
        probed_at_ms: now_ms,
        spent: true,
        spending_txid: Some(root.to_ascii_lowercase()),
        spent_confirmed: Some(true),
        tip_height: None,
    })
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

/// PURE: the latches a pass writes: each spender this pass holds a checked proof for (`proofs`: spender, answer,
/// the canonical root at its height) that no honoured latch already covers, the courier-bytes ones only (an
/// index-bytes spender is judged on the index's own proof), at most [`OWED_PROOF_LATCH_WRITES_PER_RECOMPUTE`].
pub fn proof_latches_to_write(
    proofs: &[(String, crate::txany::TxAnyAnswer, Option<String>)],
    honoured: &HashSet<String>,
    courier_spenders: &HashSet<String>,
    now_ms: i64,
) -> Vec<crate::hops_view::ProbeMemo> {
    proofs
        .iter()
        .filter(|(sp, _, _)| !honoured.contains(sp) && courier_spenders.contains(sp))
        .filter_map(|(sp, a, root)| proof_latch_memo(sp, a, root.as_deref(), now_ms))
        .take(OWED_PROOF_LATCH_WRITES_PER_RECOMPUTE)
        .collect()
}

pub fn note_recompute(source: &str, rows: &[OwedRow]) {
    RECOMPUTES_THIS_ISOLATE.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence(COUNTER_RECOMPUTES);
    if let Some(i) = RECOMPUTE_SOURCES.iter().position(|s| *s == source) {
        RECOMPUTE_BY_SOURCE[i].fetch_add(1, Ordering::Relaxed);
    }
    for r in rows {
        ROWS_BY_FAMILY[r.family.index()].fetch_add(1, Ordering::Relaxed);
    }
}
pub fn note_recompute_fault() {
    RECOMPUTE_FAULTS.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_faults_total");
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
/// bsv-low #800: the oldest mark this isolate took over (ms of silence), served beside the count.
static RECOMPUTE_LOCK_TAKEOVER_MAX_AGE_MS: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_lock_takeover(age_ms: i64) {
    RECOMPUTE_LOCK_TAKEOVERS.fetch_add(1, Ordering::Relaxed);
    RECOMPUTE_LOCK_TAKEOVER_MAX_AGE_MS.fetch_max(age_ms.max(0) as u64, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_lock_takeovers_total");
}
/// bsv-low #800: walks the claim dropped at their hard deadline, and walks not started for want of a slice.
static RECOMPUTE_WALK_DEADLINE: AtomicU64 = AtomicU64::new(0);
static RECOMPUTE_NO_SLICE: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_walk_deadline() {
    RECOMPUTE_WALK_DEADLINE.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_walk_deadline_total");
}
pub fn note_recompute_no_slice() {
    RECOMPUTE_NO_SLICE.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_no_slice_total");
}
/// bsv-low #487: walks whose snapshot the write REFUSED because the marker already carried a newer one (a walk that
/// outlived its takeover, or the slower of two isolates).
static RECOMPUTE_WRITES_SUPERSEDED: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_write_superseded() {
    RECOMPUTE_WRITES_SUPERSEDED.fetch_add(1, Ordering::Relaxed);
}

/// bsv-low #499: the recomputes ONE identity may run on ONE isolate; #648 keeps twelve ordinary walks per
/// fixed 60-second window. A shed
/// keeps and re-marks the stored snapshot stale (the #499 in-flight write race), and reserves ONE deferred walk
/// at the window's end. The durable alarm runs the SAME recompute and owed-changed push, without spending the
/// next window's ordinary admissions: ceiling + 1 walks charged to each identity/window, never one timer per ask.
/// Reads name an exhausted refresh as refresh:"shed" with retryAfterMs, so an old snapshot cannot pose as fresh.
/// The fleet bound scales with live isolates; this is a storm brake, not a fleet D1 budget. A first read without
/// a snapshot is counted too. Cross-isolate older writes still use the snapshot write guard and the age rules.
///
/// Why keep 12: the 2026-09-01 callback flood must not fan out into unbounded walks. The old honest-table premise
/// was false: the 2026-10-12 spec-identity-none 50-second hand drove 19 pushes and at least twelve counted walks
/// for its winner before the settle. Filing triggers amplified mounted-device reads; filings now schedule the
/// coalesced walk themselves. Honest traffic can reach the ceiling: a shed is a bounded delay, never silence.
pub const OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW: u32 = 12;
/// The window of [`OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW`]: one minute, fixed from the first recompute in it.
pub const OWED_RECOMPUTE_RATE_WINDOW_MS: i64 = 60_000;
/// Identities tracked before the expired windows are pruned (the map is per isolate and bounded by its traffic).
const OWED_RECOMPUTE_RATE_PRUNE_AT: usize = 1024;

/// What a dropped walk's hand-off decided (`RecomputeRate::hand_to_alarm`, the FIX-800 delta lenses' MEDIUM-A and
/// LOW-C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwedHandOff {
    /// `Some(end)`: arm the window's reserved alarm, keyed `end`; `None`: the window's alarm is armed already.
    pub arm: Option<i64>,
    /// When the alarm this call arms fires: the drop itself (`None` when nothing is armed here).
    pub fire_at: Option<i64>,
    /// Until when the identity's request-path walks are refused (the alarm's fire time plus its 60 s deadline).
    pub until: i64,
}

/// The second FIX-800 delta lens's MEDIUM-B: what the identity's deferred actor answers a hook SHED while that
/// actor's alarm is armed (`/owed-deferred-note`). PURE (pinned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwedAlarmNote {
    /// The alarm is armed and its walk has not started: it reads the tables after the hook's evidence and pushes.
    Pending,
    /// The alarm's walk is in flight: the actor keeps the note (`owed-dirty`), and the walk marks the identity stale
    /// AGAIN after its write, before its push (its write, stamped at its start, cleared the hook's mark).
    Noted,
    /// No alarm of this identity is armed or in flight (it ran, or none was): the hook's own mark stands.
    Idle,
}
impl OwedAlarmNote {
    pub fn word(self) -> &'static str {
        match self {
            OwedAlarmNote::Pending => "pending",
            OwedAlarmNote::Noted => "noted",
            OwedAlarmNote::Idle => "idle",
        }
    }
    pub fn from_word(word: &str) -> Option<Self> {
        [
            OwedAlarmNote::Pending,
            OwedAlarmNote::Noted,
            OwedAlarmNote::Idle,
        ]
        .into_iter()
        .find(|n| n.word() == word.trim())
    }
}
/// PURE (pinned): the actor's answer to a shed hook's note, from what it holds: an arm for this identity, and whether
/// its walk has started (`owed-started`, set durably before the walk).
pub fn owed_alarm_note(armed_for_identity: bool, started: bool) -> OwedAlarmNote {
    match (armed_for_identity, started) {
        (true, false) => OwedAlarmNote::Pending,
        (true, true) => OwedAlarmNote::Noted,
        (false, _) => OwedAlarmNote::Idle,
    }
}
/// PURE (pinned): does a shed hook tell the page after its mark? Always, unless the actor answered `Pending` (the
/// alarm's walk is still to start and pushes its rows). `None` is a note that faulted: the page is told.
pub fn owed_shed_nudges(answer: Option<OwedAlarmNote>) -> bool {
    answer != Some(OwedAlarmNote::Pending)
}

/// PURE (pinned): the per-identity recompute counter of one isolate.
#[derive(Debug, Default)]
pub struct RecomputeRate {
    /// identity (lowercase) -> (window start ms, recomputes run in it)
    windows: HashMap<String, (i64, u32)>,
    deferred: HashMap<String, i64>,
    /// identity (lowercase) -> the unix-ms until which its request-path walks are refused: it was handed to the
    /// deferred alarm walk by a DROPPED walk (`hand_to_alarm`, the FIX-800 delta lens's MEDIUM-A).
    handed: HashMap<String, i64>,
}

impl RecomputeRate {
    /// Count one admitted ordinary walk. The legacy sheddable flag no longer exempts repeated first-read faults:
    /// with no snapshot the caller consumes the shared durable reservation, rather than bypassing the ceiling.
    pub fn admit(&mut self, identity_lc: &str, now_ms: i64, _sheddable: bool) -> bool {
        if self.windows.len() >= OWED_RECOMPUTE_RATE_PRUNE_AT {
            self.windows.retain(|_, (start, _)| {
                now_ms.saturating_sub(*start) < OWED_RECOMPUTE_RATE_WINDOW_MS
            });
            self.deferred.retain(|_, end| *end > now_ms);
            self.handed.retain(|_, until| *until > now_ms);
        }
        // the FIX-800 delta lens's MEDIUM-A: an identity handed to the alarm walks no request-path walk until the
        // alarm's walk has had its whole deadline (a walk started here would be dropped as the last one was)
        if self.handed_until(identity_lc, now_ms).is_some() {
            return false;
        }
        let w = self
            .windows
            .entry(identity_lc.to_string())
            .or_insert((now_ms, 0));
        if now_ms.saturating_sub(w.0) >= OWED_RECOMPUTE_RATE_WINDOW_MS {
            *w = (now_ms, 0);
        }
        if w.1 >= OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW {
            return false;
        }
        w.1 = w.1.saturating_add(1);
        true
    }
    /// Arm at most one deferred walk for the exhausted window. The caller persists the alarm before returning.
    pub fn arm_deferred(&mut self, identity_lc: &str, _now_ms: i64) -> Option<i64> {
        // Called only after a shed decision. The clock crossing the end cannot revoke that window's reservation.
        let (start, n) = self.windows.get(identity_lc)?;
        if *n < OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW {
            return None;
        }
        let end = start.saturating_add(OWED_RECOMPUTE_RATE_WINDOW_MS);
        if self.deferred.get(identity_lc) == Some(&end) {
            return None;
        }
        self.deferred.insert(identity_lc.to_string(), end);
        Some(end)
    }
    /// The FIX-800 delta lens's MEDIUM-A: a claimed walk of `identity_lc` was DROPPED at its hard deadline. Hand the
    /// identity to the deferred alarm walk of the window that walk was charged to (its end, the same reservation a
    /// shed arms: ceiling + 1 walks a window kept), and refuse its request-path walks until that alarm's walk has
    /// had its whole deadline. `arm: Some(end)` when this call arms the window's alarm; `None` when it is armed
    /// already (a shed's, or an earlier drop's: a second drop in the window arms nothing new). PURE (pinned).
    ///
    /// The second delta lens's LOW-C: an alarm this call arms FIRES AT THE DROP (`fire_at`), while its reservation
    /// stays keyed to the window's end (the actor's `due`: one reserved walk a window, ceiling + 1 kept), so the
    /// hand-off lasts `now + OWED_DEFERRED_WALK_DEADLINE_MS`, 60 s past the drop, where it waited for the window's
    /// end first (up to 33 s idle, a landing up to 120 s after the dropped walk started, past identityNone's 90 s
    /// bar). An alarm armed already keeps its own fire time, which this call does not know: the hand-off then lasts
    /// `max(end, now) + OWED_DEFERRED_WALK_DEADLINE_MS` (60 to 93 s past the drop), as before; that is the case of a
    /// window a shed armed at the ceiling before the drop, whose alarm fires at the window's end (stated).
    ///
    /// The rate (per identity per isolate, the D1 overload band of loops 11 and 39 where the walk runs past 27 s):
    /// the identity costs ONE request-path walk (27 s) per hand-off, then the alarm's one (60 s, from the drop),
    /// whatever the number of open tabs: at most one request-path walk per 87 s (0.7 a minute) where fc64a957's
    /// nudge drove one every ~28 s per tab (2.2 a minute a tab, never reaching the ceiling of 12, so never the
    /// alarm). If the alarm's walk is dropped too it tells the page once and re-arms once (#648); the read that
    /// follows, past the hand-off, may start one more request-path walk, which hands off again, and that hand-off's
    /// arm REPLACES the faulted alarm's retry arm (`retry: false`), so under sustained overload #648's "re-arms
    /// once, then abandoned" never abandons: the bound is this rate, not a count (the second delta lens's NOTE-2).
    /// Each isolate keeps its own map, so the bound is per isolate (N isolates, N alarms a window: NOTE-1).
    pub fn hand_to_alarm(&mut self, identity_lc: &str, now_ms: i64) -> OwedHandOff {
        let start = self.windows.get(identity_lc).map_or(now_ms, |(s, _)| *s);
        let end = start.saturating_add(OWED_RECOMPUTE_RATE_WINDOW_MS);
        let arm = (self.deferred.get(identity_lc) != Some(&end)).then_some(end);
        let fire_at = arm.map(|_| now_ms);
        let until = fire_at
            .unwrap_or_else(|| end.max(now_ms))
            .saturating_add(OWED_DEFERRED_WALK_DEADLINE_MS);
        let h = self.handed.entry(identity_lc.to_string()).or_insert(until);
        *h = (*h).max(until);
        let until = *h;
        if arm.is_some() {
            self.deferred.insert(identity_lc.to_string(), end);
        }
        OwedHandOff {
            arm,
            fire_at,
            until,
        }
    }
    /// Until when `identity_lc` is handed to its alarm (`hand_to_alarm`), if it still is at `now_ms`.
    pub fn handed_until(&self, identity_lc: &str, now_ms: i64) -> Option<i64> {
        self.handed
            .get(identity_lc)
            .copied()
            .filter(|until| *until > now_ms)
    }
    /// Scheduling failed: allow the next shed to repair this window's missing alarm, and lift a hand-off that alarm
    /// was to serve (with no alarm the request path must walk the identity again).
    pub fn unarm_deferred(&mut self, identity_lc: &str, end: i64) {
        if self.deferred.get(identity_lc) == Some(&end) {
            self.deferred.remove(identity_lc);
            self.handed.remove(identity_lc);
        }
    }
    pub fn window_end(&self, identity_lc: &str) -> Option<i64> {
        self.windows
            .get(identity_lc)
            .map(|(start, _)| start.saturating_add(OWED_RECOMPUTE_RATE_WINDOW_MS))
    }
    /// Remainder of an exhausted window, or of a hand-off to the alarm (the FIX-800 delta lens's MEDIUM-A: past the
    /// alarm's expected landing, its due plus its 60 s deadline), whichever is later; reads can inspect this without
    /// spending another admission.
    pub fn retry_after_ms(&self, identity_lc: &str, now_ms: i64) -> Option<i64> {
        let exhausted = self.windows.get(identity_lc).and_then(|(start, n)| {
            let remaining = start
                .saturating_add(OWED_RECOMPUTE_RATE_WINDOW_MS)
                .saturating_sub(now_ms);
            (*n >= OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW && remaining > 0).then_some(remaining)
        });
        let handed = self
            .handed_until(identity_lc, now_ms)
            .map(|until| until.saturating_sub(now_ms));
        exhausted.max(handed)
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
            .filter(|(start, n)| {
                now_ms.saturating_sub(*start) < OWED_RECOMPUTE_RATE_WINDOW_MS
                    && *n >= OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW
            })
            .count()
    }
}

/// bsv-low #499 (lens fold M1): the statement a SHED runs, with its bind: the identity marked stale again, so the
/// first read past the window recomputes whatever a walk in flight cleared (see
/// [`OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW`]). PURE (pinned on real SQLite in the race's order).
pub fn owed_shed_plan(identity_lc: &str) -> (&'static str, String) {
    (OWED_STALE_FOR_IDENTITY_SQL, identity_lc.to_string())
}

/// bsv-low #499: recomputes shed by the per-identity ceiling (the snapshot served instead).
static RECOMPUTE_SHED: AtomicU64 = AtomicU64::new(0);
static RECOMPUTE_DEFERRED: AtomicU64 = AtomicU64::new(0);
static RECOMPUTE_ARM_FAILED: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_arm_failed() {
    RECOMPUTE_ARM_FAILED.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_arm_failed_total");
}
static RECOMPUTE_DEFERRED_ABANDONED: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_deferred_abandoned() {
    RECOMPUTE_DEFERRED_ABANDONED.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_deferred_abandoned_total");
}
static RECOMPUTE_DEFERRED_FAULTED: AtomicU64 = AtomicU64::new(0);
pub fn note_recompute_deferred_faulted() {
    RECOMPUTE_DEFERRED_FAULTED.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_deferred_faulted_total");
}
/// Retry only in a later window, even when a slow fault crossed several windows.
pub fn owed_fault_retry_due(due: i64, now: i64) -> i64 {
    let windows = now.saturating_sub(due).max(0) / OWED_RECOMPUTE_RATE_WINDOW_MS + 1;
    due.saturating_add(windows.saturating_mul(OWED_RECOMPUTE_RATE_WINDOW_MS))
}
/// A reserved walk that actually begins (alarm or first read; not an arm or an idempotent delivery).
pub fn note_recompute_deferred() {
    RECOMPUTE_DEFERRED.fetch_add(1, Ordering::Relaxed);
}
pub fn note_recompute_shed() {
    RECOMPUTE_SHED.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_owed_recompute_shed_total");
}
pub fn recompute_shed_total() -> u64 {
    RECOMPUTE_SHED.load(Ordering::Relaxed)
}

pub fn owed_health_json() -> Value {
    let shed = RECOMPUTE_SHED.load(Ordering::Relaxed);
    let mut by_source = serde_json::Map::new();
    for (i, s) in RECOMPUTE_SOURCES.iter().enumerate() {
        by_source.insert(
            (*s).to_string(),
            json!(RECOMPUTE_BY_SOURCE[i].load(Ordering::Relaxed)),
        );
    }
    let mut by_family = serde_json::Map::new();
    for f in OwedFamily::ALL {
        by_family.insert(
            f.as_str().to_string(),
            json!(ROWS_BY_FAMILY[f.index()].load(Ordering::Relaxed)),
        );
    }
    let mut v = json!({
        "countersScope": "isolate",
        "recomputeBySource": by_source,
        "rowsWrittenByFamily": by_family,
        "recomputeFaults": RECOMPUTE_FAULTS.load(Ordering::Relaxed),
        "recomputeCoalesced": RECOMPUTE_COALESCED.load(Ordering::Relaxed),
        "recomputeLockTakeovers": RECOMPUTE_LOCK_TAKEOVERS.load(Ordering::Relaxed),
        "recomputeWritesSuperseded": RECOMPUTE_WRITES_SUPERSEDED.load(Ordering::Relaxed),
        "recomputeShed": shed,
        "owed_recompute_shed_total": shed,
        "owed_recompute_deferred_total": RECOMPUTE_DEFERRED.load(Ordering::Relaxed),
        "owed_recompute_arm_failed_total": RECOMPUTE_ARM_FAILED.load(Ordering::Relaxed),
        "owed_recompute_deferred_abandoned_total": RECOMPUTE_DEFERRED_ABANDONED.load(Ordering::Relaxed),
        "owed_recompute_deferred_faulted_total": RECOMPUTE_DEFERRED_FAULTED.load(Ordering::Relaxed),
        "recomputesPerIdentityPerMinute": OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW,
        "recomputeCeilingScope": "per identity per isolate: the fleet bound is this times the live isolates",
        "collectedReadFaults": COLLECTED_READ_FAULTS.load(Ordering::Relaxed),
        "heldFilingsMatched": HELD_FILINGS_MATCHED.load(Ordering::Relaxed),
        "heldFilingsKeyMismatch": HELD_FILINGS_KEY_MISMATCH.load(Ordering::Relaxed),
        "payVoutReadFaults": PAY_VOUT_READ_FAULTS.load(Ordering::Relaxed),
        "potSpendersReadFaults": POT_SPENDERS_READ_FAULTS.load(Ordering::Relaxed),
        "hopSweepsReadFaults": HOP_SWEEPS_READ_FAULTS.load(Ordering::Relaxed),
        "sweepProofsReadFaults": SWEEP_PROOFS_READ_FAULTS.load(Ordering::Relaxed),
        "markerlessReadFaults": MARKERLESS_READ_FAULTS.load(Ordering::Relaxed),
        "markerlessIgnoredMarkers": MARKERLESS_IGNORED_MARKERS.load(Ordering::Relaxed),
        "probeMemoReadFaults": PROBE_MEMO_READ_FAULTS.load(Ordering::Relaxed),
        "sweepProofContradictions": SWEEP_PROOF_CONTRADICTIONS.load(Ordering::Relaxed),
        "owed_swept_confirmed_by_proof_total": SWEPT_CONFIRMED_BY_PROOF.load(Ordering::Relaxed),
        "spenderReadFaults": SPENDER_READ_FAULTS.load(Ordering::Relaxed),
        "withheldPreviousReadFaults": WITHHELD_PREVIOUS_READ_FAULTS.load(Ordering::Relaxed),
        "spenderStoreHits": SPENDER_STORE_HITS.load(Ordering::Relaxed),
        "spenderStorePersisted": SPENDER_STORE_PERSISTED.load(Ordering::Relaxed),
        "spenderStoreCarried": SPENDER_STORE_CARRIED.load(Ordering::Relaxed),
        "spenderStoreReadFaults": SPENDER_STORE_READ_FAULTS.load(Ordering::Relaxed),
        "spenderStoreSweepPasses": SPENDER_STORE_SWEEP_PASSES.load(Ordering::Relaxed),
        "spenderReadsPerRecompute": OWED_SPENDER_READS_PER_RECOMPUTE,
        "recomputeTimeBudgetMs": OWED_RECOMPUTE_TIME_BUDGET_MS,
        "readRefreshesKicked": READ_REFRESHES.load(Ordering::Relaxed),
        "readRefreshesSkippedInFlight": READ_REFRESHES_SKIPPED.load(Ordering::Relaxed),
        "hopStrandedAfterMs": HOP_STRANDED_AFTER_MS,
        "recomputeOpenAfterMs": OWED_RECOMPUTE_OPEN_AFTER_MS,
        "recomputeAnyAfterMs": OWED_RECOMPUTE_ANY_AFTER_MS,
    });
    // bsv-low #566 / #628 (outside the literal: `json!`'s recursion limit)
    v["recomputesThisIsolate"] = json!(RECOMPUTES_THIS_ISOLATE.load(Ordering::Relaxed));
    // bsv-low #800: the lock's take-over age, the walks dropped at their deadline or never started, the deadline
    v["recomputeLockTakeoverMaxAgeMs"] =
        json!(RECOMPUTE_LOCK_TAKEOVER_MAX_AGE_MS.load(Ordering::Relaxed));
    v["recomputeWalkDeadline"] = json!(RECOMPUTE_WALK_DEADLINE.load(Ordering::Relaxed));
    v["recomputeNoSlice"] = json!(RECOMPUTE_NO_SLICE.load(Ordering::Relaxed));
    v["recomputeWalkHardDeadlineMs"] = json!(OWED_WALK_HARD_DEADLINE_MS);
    // the FIX-800 lens's MEDIUM-3: the deferred alarm's own deadline
    v["recomputeDeferredWalkDeadlineMs"] = json!(OWED_DEFERRED_WALK_DEADLINE_MS);
    v
}

/// Additive v1 read word: a snapshot whose refresh hit the ceiling is served with its retry delay.
pub fn mark_shed_refresh(body: &mut Value, retry_after_ms: Option<i64>) {
    if let Some(ms) = retry_after_ms {
        body["refresh"] = json!("shed");
        body["retryAfterMs"] = json!(ms.max(1_000));
    }
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

/// bsv-low #800 (the FIX-800 lens's MEDIUM-1): the push of a walk that wrote NOTHING (dropped at its deadline, never
/// started): the identity was marked stale, the rows are the stored ones, so `rows` and `computedAtMs` are null
/// (the client's `lowEvents` parse reads both as null) and `stale` says why. The page re-reads `/owed` on it.
pub fn owed_stale_event_body(identity: &str, source: &str) -> Value {
    json!({
        "v": 1,
        "kind": "owed-changed",
        "identity": identity.to_ascii_lowercase(),
        "recipient": identity.to_ascii_lowercase(),
        "source": source,
        "rows": Value::Null,
        "computedAtMs": Value::Null,
        "stale": true,
    })
}

/// bsv-low #638: bounded stored-key lookup for decoded spender pots.
pub fn covenant_spenders_sql(n: usize) -> String {
    let placeholders = (1..=n)
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT DISTINCT txid AS txid FROM pot_records WHERE lockKind = 'covenant' AND txid IN ({placeholders})")
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
    use super::*;
    use crate::hops_view::MarkerVerification;
    use crate::refund_view::RefundStatus;
    use crate::results::{MoneyFacts, PotBinding, TxMoneyFacts};

    pub(crate) const ME: &str =
        "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OPP: &str = "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    pub(crate) fn tx(b: u8) -> String {
        hex::encode([b; 32])
    }

    fn entry(
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
            let mut row = serde_json::to_value(&derive_owed_rows(&i)[0]).unwrap();
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
                derive_owed_rows(&i)[0].facts["claimable"],
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
            derive_owed_rows(&i)[0].facts.clone()
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        // without the twin's word nothing is named (a faulted read) and the pre-change sentence stands
        assert!(evicted_pot_homes(&released, &hops, &[]).is_empty());
        let none: HashMap<String, String> = HashMap::new();
        i.my_pkh_by_game = &none;
        assert_eq!(
            derive_owed_rows(&i)[0].reason.as_deref(),
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

    /// Loop 23's pair 13 (bsv-low #610, `spec-admit-fast-join-refused`:752): p28's `tm_hopparty` submit was refused
    /// 502 once and never re-presented, so its hops view held NO entry for the game's hop; the JOIN was evicted, the
    /// competitor (p2's own spend of its hop to its wallet) mined, and `/owed` served no row for the game for six
    /// minutes. The captured shape: hops view empty for the game, the ledger naming both hops, p28's twin party row,
    /// the twin lock committing p28's key as seat B. RED at `18a6fd7`: the derivation over that hops view yields no row
    /// (asserted first below) and `markerless_released_hops` does not exist. GREEN: the hop is rebuilt from the
    /// evicted JOIN and its story is "spent outside this game".
    #[test]
    fn l23_r2_a_released_hop_with_no_marker_row_is_rebuilt_from_the_evicted_join_and_reads_spent_outside_this_game(
    ) {
        use crate::results::CommittedKeys;
        let pub_a = format!("02{}", "aa".repeat(32));
        let pub_b = format!("03{}", "bb".repeat(32)); // p28, seat B
        let keys = CommittedKeys {
            pub_a: pub_a.clone(),
            pub_b: pub_b.clone(),
            pay_pkh_a: "cc".repeat(20),
            pay_pkh_b: "dd".repeat(20),
        };
        let (join, competitor, my_hop, their_hop) = (tx(0x0c), tx(0x0d), tx(0x07), tx(0x08));
        let lock_of = |k: &str| {
            overlay_discovery::pot::p2pkh_lock(&bsv_rs::primitives::hash::hash160(
                &hex::decode(k).unwrap(),
            ))
        };
        let ledger = [(
            join.clone(),
            Some(format!(
                r#"[{{"table":"pot_records","txid":"{their_hop}","vout":0}},{{"table":"pot_records","txid":"{my_hop}","vout":0}}]"#
            )),
        )];
        let seat = EvictedSeat {
            pot_txid: join.to_ascii_uppercase(),
            game_id: tx(0x01),
            opponent_identity: OPP.to_string(),
            seat_settle_pubkey: pub_b.clone(),
        };
        let twin = [(join.clone(), 0u32, keys.clone())];
        let outputs: HashMap<String, (Vec<u8>, u64)> = [
            (format!("{my_hop}:0"), (lock_of(&pub_b), 20_190u64)),
            (format!("{their_hop}:0"), (lock_of(&pub_a), 20_190u64)),
        ]
        .into_iter()
        .collect();
        let mined = chain_confirmed(
            &format!("{my_hop}:0"),
            true,
            Some(true),
            Some(&competitor),
            Some(true),
        );
        let elsewhere = pays(&competitor, &[(0, &"99".repeat(20), 20_170, Some(false))]);
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());

        // the captured shape, before the fix: no hop entry, so no row at all (the story hidden)
        let before = derive_owed_rows(&inputs(&[], &[], &[], &v, &c, &no_pots, Some(970_754)));
        assert!(before.iter().all(|r| r.game_id != tx(0x01)), "{before:?}");

        // the fix: the ledger + my twin party row + the JOIN's own bytes rebuild MY hop only
        let hops =
            markerless_released_hops(&ledger, &[], std::slice::from_ref(&seat), &twin, &outputs);
        assert_eq!(hops.len(), 1, "{hops:?}");
        assert_eq!(
            (
                hops[0].hop_txid.as_str(),
                hops[0].hop_sats,
                hops[0].game_id.as_str()
            ),
            (my_hop.as_str(), 20_190, tx(0x01).as_str())
        );
        assert_eq!((hops[0].status, hops[0].spent), (HopStatus::Unknown, None));
        // the route's own downstream reads, unchanged, over the rebuilt view
        let refused = refused_hop_outpoints_of(&released_hop_outpoints(&ledger), &hops);
        let pkhs: HashMap<String, String> =
            evicted_pot_homes(&released_hops_by_eviction(&ledger, &hops), &hops, &twin)
                .into_iter()
                .collect();
        assert_eq!(
            pkhs.get(&tx(0x01)).map(String::as_str),
            Some("dd".repeat(20).as_str())
        );
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(970_754));
        i.evicted_hop_outpoints = &refused;
        i.hop_chain = &mined;
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].game_id, tx(0x01));
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));

        // the bindings, each necessary: a hop the view already holds is left to it
        let mut held = hop(HopStatus::Unspent, None, Some(60_000));
        held.hop_txid = my_hop.clone();
        assert!(markerless_released_hops(
            &ledger,
            &[held],
            std::slice::from_ref(&seat),
            &twin,
            &outputs
        )
        .is_empty());
        // no party row of mine for that JOIN (a stranger's eviction), or no twin lock: nothing
        assert!(markerless_released_hops(&ledger, &[], &[], &twin, &outputs).is_empty());
        assert!(
            markerless_released_hops(&ledger, &[], std::slice::from_ref(&seat), &[], &outputs)
                .is_empty()
        );
        // my party row naming a key the lock did not commit, or committed on both seats: nothing
        let stranger_key = EvictedSeat {
            seat_settle_pubkey: format!("02{}", "ee".repeat(32)),
            ..seat.clone()
        };
        assert!(
            markerless_released_hops(&ledger, &[], &[stranger_key], &twin, &outputs).is_empty()
        );
        let both = [(
            join.clone(),
            0u32,
            CommittedKeys {
                pub_a: pub_b.clone(),
                ..keys.clone()
            },
        )];
        assert!(markerless_released_hops(
            &ledger,
            &[],
            std::slice::from_ref(&seat),
            &both,
            &outputs
        )
        .is_empty());
        // the JOIN's bytes not read (no output known), or the output not paying hash160(K): nothing
        assert!(markerless_released_hops(
            &ledger,
            &[],
            std::slice::from_ref(&seat),
            &twin,
            &HashMap::new()
        )
        .is_empty());
        let swapped: HashMap<String, (Vec<u8>, u64)> =
            [(format!("{my_hop}:0"), (lock_of(&pub_a), 20_190u64))]
                .into_iter()
                .collect();
        assert!(markerless_released_hops(
            &ledger,
            &[],
            std::slice::from_ref(&seat),
            &twin,
            &swapped
        )
        .is_empty());
        // two of my party rows in one JOIN that disagree name nothing
        let other_game = EvictedSeat {
            game_id: tx(0x02),
            ..seat.clone()
        };
        assert!(markerless_released_hops(
            &ledger,
            &[],
            &[seat.clone(), other_game],
            &twin,
            &outputs
        )
        .is_empty());
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
            let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["sweepSource"], "courier-bytes");
        assert_eq!(rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON);
        assert_ne!(rows[0].facts["confirmedSource"], "proof");
        assert_eq!(count_swept_confirmed_by_proof(&rows), 0);
    }

    /// bsv-low #693: a courier's answer for a re-ask: a chaintracks-checked mined proof, or a present unmined hint.
    fn e693_answer(mined: bool) -> crate::txany::TxAnyAnswer {
        crate::txany::TxAnyAnswer {
            present: Some(true),
            confirmed: mined.then_some(true),
            height: mined.then_some(900_000),
            raw_hex: Some("00".into()),
            source: Some("external"),
            ..Default::default()
        }
    }

    /// bsv-low #693: the route's phase 2b over a fake courier that counts its asks: the durable spenders filtered by
    /// [`durable_spender_needs_proof`], then [`durable_reasks`] with the slots left. Returns the proof set and the asks.
    fn e693_pass(
        durable: &[String],
        hops: &[HopEntry],
        chain: &HashMap<String, HopChainWord>,
        tracker: Option<bool>,
        room: usize,
        mined: bool,
    ) -> (HashSet<String>, usize) {
        let asks = std::cell::Cell::new(0usize);
        let candidates: Vec<String> = durable
            .iter()
            .filter(|sp| durable_spender_needs_proof(sp, hops, chain, &HashMap::new(), tracker))
            .cloned()
            .collect();
        let reasks = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(durable_reasks(
                candidates,
                room,
                || false,
                |_sp| {
                    asks.set(asks.get() + 1);
                    async move { (e693_answer(mined), true) }
                },
            ));
        let proof = reasks
            .into_iter()
            .filter(|(r, _)| r.outcome == DurableReaskOutcome::MinedByProof)
            .map(|(r, _)| r.spender)
            .collect();
        (proof, asks.get())
    }

    /// bsv-low #693 (a): the #692 lens's MEDIUM-1 window. The e692 fixture (the chain rung names the sweep spent but
    /// UNCONFIRMED, WoC faulted) with the sweep's bytes served from the #672 DURABLE store, so phase 1 and 2 never
    /// ask its `/tx-any`. The re-ask serves a checked mined proof: the courier-bytes payout reaches "home on chain",
    /// `confirmedSource = "proof"`. RED on `b4ae1c6` (no re-ask: the proof set stays empty, `UNCONFIRMED_PAYOUT_REASON`;
    /// run as the mutant `durable_reasks` making no ask).
    #[test]
    fn e693_a_a_durable_unconfirmed_spender_is_re_asked_and_its_checked_proof_confirms_home_on_chain(
    ) {
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
        let (proof, asks) = e693_pass(
            std::slice::from_ref(&sweep),
            &hops,
            &chain,
            None,
            OWED_SPENDER_COURIER_READS_PER_RECOMPUTE,
            true,
        );
        assert_eq!(asks, 1, "the durable unconfirmed spender is re-asked once");
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        i.proof_mined_spenders = &proof;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].facts["sweepSource"], "courier-bytes");
        assert_eq!(
            rows[0].facts["claimable"], false,
            "a courier-bytes row is never claimable"
        );
        assert_eq!(rows[0].facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON);
        assert_eq!(rows[0].facts["confirmedSource"], "proof");
        // an unmined answer confirms nothing (never a courier's bare presence)
        let (proof, asks) = e693_pass(std::slice::from_ref(&sweep), &hops, &chain, None, 2, false);
        assert_eq!((proof.len(), asks), (0, 1));
        // the outcomes: a word-less answer is a fault, an unmined one still unconfirmed, a checked proof mined
        assert_eq!(
            DurableReaskOutcome::of(&crate::txany::TxAnyAnswer::default()),
            DurableReaskOutcome::Fault
        );
        assert_eq!(
            DurableReaskOutcome::of(&e693_answer(false)),
            DurableReaskOutcome::StillUnconfirmed
        );
        assert_eq!(
            DurableReaskOutcome::of(&e693_answer(true)),
            DurableReaskOutcome::MinedByProof
        );
        let bare_height = crate::txany::TxAnyAnswer {
            height: Some(900_000),
            ..e693_answer(false)
        };
        assert_eq!(
            DurableReaskOutcome::of(&bare_height),
            DurableReaskOutcome::StillUnconfirmed
        );
    }

    /// bsv-low #693 (b): a durable spender ALREADY CONFIRMED is never re-asked: by the chain rung's
    /// `spent_confirmed`, by the index's hop row, or by the tracker's mined word. The fake counts zero asks. A tracker
    /// row that is NOT mined outranks a confirmed ladder (as `spender_mined` reads it), so that spender IS re-asked.
    #[test]
    fn e693_b_a_durable_confirmed_spender_is_never_re_asked() {
        let sweep = tx(0x0e);
        let key = format!("{}:0", tx(0x07));
        let stranded = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let chain_ok = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let s = std::slice::from_ref(&sweep);
        assert_eq!(
            e693_pass(s, &stranded, &chain_ok, None, 2, true).1,
            0,
            "chain rung confirmed"
        );
        let mut by_index = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        by_index.spent_confirmed = Some(true);
        let indexed = [by_index];
        assert_eq!(
            e693_pass(s, &indexed, &HashMap::new(), None, 2, true).1,
            0,
            "index confirmed"
        );
        let chain_u = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(false));
        assert_eq!(
            e693_pass(s, &stranded, &chain_u, Some(true), 2, true).1,
            0,
            "tracker mined"
        );
        assert_eq!(
            e693_pass(s, &stranded, &chain_ok, Some(false), 2, true).1,
            1,
            "tracker not mined"
        );
        // a spender no hop names (or a filed sweep's own) is not a candidate either
        assert_eq!(
            e693_pass(&[tx(0x0f)], &stranded, &chain_u, None, 2, true).1,
            0
        );
        let mut filed = HashMap::new();
        filed.insert(
            key.clone(),
            crate::hopsweep::FiledHopSweep {
                sweep_txid: sweep.clone(),
                raw_hex: String::new(),
                pays_sats: None,
                index_proven: false,
                index_proof_height: None,
            },
        );
        assert!(!durable_spender_needs_proof(
            &sweep, &stranded, &chain_u, &filed, None
        ));
        assert!(durable_spender_needs_proof(
            &sweep,
            &stranded,
            &chain_u,
            &HashMap::new(),
            None
        ));
    }

    /// bsv-low #693 (c): THE BOUND. Five durable unconfirmed spenders, the fake courier counts at most
    /// `OWED_DURABLE_REASKS_PER_RECOMPUTE` (1) asks whatever slots are left, none when phase 2 spent both slots, none
    /// once the pass's clock is out; and the route takes the slots from what phase 2 left (source pin).
    #[test]
    fn e693_c_the_per_pass_re_ask_bound_holds_over_many_durable_unconfirmed_spenders() {
        assert_eq!(OWED_DURABLE_REASKS_PER_RECOMPUTE, 1);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let five: Vec<String> = (0x20u8..0x25).map(tx).collect();
        for (room, clock_out, want) in [
            (2usize, false, 1usize),
            (1, false, 1),
            (0, false, 0),
            (2, true, 0),
        ] {
            let asks = std::cell::Cell::new(0usize);
            let made = rt.block_on(durable_reasks(
                five.clone(),
                room,
                || clock_out,
                |_sp| {
                    asks.set(asks.get() + 1);
                    async move { (e693_answer(true), true) }
                },
            ));
            assert_eq!(
                (asks.get(), made.len()),
                (want, want),
                "room={room} clock_out={clock_out}"
            );
        }
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        for wired in [
            "letroom=crate::owed::OWED_SPENDER_COURIER_READS_PER_RECOMPUTE.saturating_sub(courier_reads);",
            "crate::owed::durable_reasks(candidates,room,&over_budget,",
            "ifr.courier_asked{courier_reads+=1;",
            "ifr.outcome==crate::owed::DurableReaskOutcome::MinedByProof{proof_answers.push((r.spender.clone(),a));proof_mined_spenders.insert(r.spender);}",
            "if!budget_cut&&!durable_unconfirmed.is_empty(){",
        ] {
            assert!(routes.contains(wired), "the route's phase 2b: {wired}");
        }
    }

    /// bsv-low #693 (d), the #692 lens's LOW-1, a STATED residual: the index-hop-row arm (`spent == true`) is not
    /// memo-upgraded because that arm confirms from the index's own `spent_confirmed` and never reads a chain-rung
    /// memo: a confirmed memo naming the sweep leaves its row unconfirmed (so the upgrade would change nothing). What
    /// THIS pin proves is ONE pass: the arm's durable spender IS a re-ask candidate while the index's word is
    /// unconfirmed, and that pass's checked proof confirms the row (`confirmedSource = "proof"`). Across passes the
    /// row is held by the proof LATCH (the W1-OWED-693 lens's HIGH-1: latched once checked, served from the latch on
    /// every later pass), pinned by `e693f_h1_*`. The route's filter and its stated reason stand (source pin).
    #[test]
    fn e693_d_the_index_arm_reads_no_memo_and_one_pass_s_re_ask_confirms_it() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0e);
        let my_pkh = "cc".repeat(20);
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        let mut spent = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        spent.spent_confirmed = Some(false);
        let hops = [spent];
        let key = format!("{}:0", tx(0x07));
        let memo = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let mut courier: HashSet<String> = HashSet::new();
        courier.insert(sweep.clone());
        let no_proof: HashSet<String> = HashSet::new();
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &memo;
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        i.proof_mined_spenders = &no_proof;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["spent"], true, "the index-hop-row arm");
        assert_eq!(
            rows[0].facts["claimReason"], UNCONFIRMED_PAYOUT_REASON,
            "a confirmed chain memo is not read on the index arm"
        );
        // the re-ask: the index arm's spender is a candidate (even beside that memo) and its proof confirms the row
        let (proof, asks) = e693_pass(std::slice::from_ref(&sweep), &hops, &memo, None, 2, true);
        assert_eq!(asks, 1);
        let no_chain = HashMap::new();
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &no_chain;
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        i.proof_mined_spenders = &proof;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON);
        assert_eq!(rows[0].facts["confirmedSource"], "proof");
        let routes = include_str!("routes.rs");
        assert!(routes.contains("bsv-low #693 (the #692 lens's LOW-1), a stated residual"));
        assert!(
            routes.contains("&& r.facts.get(\"spent\").and_then(|v| v.as_bool()) != Some(true)")
        );
    }

    /// bsv-low #693 fold (the W1-OWED-693 lens's MEDIUM-1): the re-ask's candidate predicate reads the tracker's
    /// word as the CONFIRMATION does, the RECHECKED view (`word == "mined"` AND `verifyPath` non-null), never the
    /// stored row. A stored `mined` row whose recheck could not check this pass (served `verifyPath: null`) leaves
    /// the row unconfirmed in `spender_mined` AND is a re-ask candidate; a checked one confirms the row and is never
    /// re-asked; no row falls to the ladder. The route passes `checked_tracker_mined` over the views it built before
    /// phase 2b (source pin). RED on `a789f9e` by the lens's mutant B shape (the route's tracker input from the
    /// stored row, or forced to `None`): the source assertions fail; the stored row answered `Some(true)` there and
    /// the spender was never re-asked.
    #[test]
    fn e693f_m1_the_candidates_read_the_rechecked_view_as_spender_mined_does() {
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
        let chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let mut spends: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let courier: HashSet<String> = [sweep.clone()].into();
        let view = |verify: Value| {
            let mut m = std::collections::BTreeMap::new();
            m.insert(
                sweep.clone(),
                serde_json::json!({"word": "mined", "verifyPath": verify}),
            );
            m
        };
        let unchecked = view(Value::Null);
        let checked = view(serde_json::json!(["merkle"]));
        let reason = |words: &std::collections::BTreeMap<String, Value>| -> Value {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.spender_words = Some(words);
            i.hop_chain = &chain;
            i.spender_outputs = &mine;
            i.spender_inputs = &spends;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = &courier;
            derive_owed_rows(&i)[0].facts["claimReason"].clone()
        };
        let s = std::slice::from_ref(&sweep);
        // the stored row says mined; the recheck could not check it: unconfirmed, and re-asked
        assert_eq!(checked_tracker_mined(Some(&unchecked), &sweep), Some(false));
        assert_eq!(reason(&unchecked), UNCONFIRMED_PAYOUT_REASON);
        let tracker = checked_tracker_mined(Some(&unchecked), &sweep);
        assert_eq!(
            e693_pass(s, &hops, &chain, tracker, 2, true).1,
            1,
            "re-asked"
        );
        // checked: confirmed, never re-asked
        assert_eq!(checked_tracker_mined(Some(&checked), &sweep), Some(true));
        assert_eq!(reason(&checked), COURIER_BYTES_NO_CREDIT_REASON);
        let tracker = checked_tracker_mined(Some(&checked), &sweep);
        assert_eq!(e693_pass(s, &hops, &chain, tracker, 2, true).1, 0);
        // no views, or no row: the ladder decides both
        assert_eq!(checked_tracker_mined(None, &sweep), None);
        assert_eq!(
            checked_tracker_mined(Some(&std::collections::BTreeMap::new()), &sweep),
            None
        );
        // the route: the views built before phase 2b, the candidates chosen after them, from the views
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        // bsv-low #618: the arm's subjects' views (3b) are merged into the rest's, both before phase 2b
        let views = "letmutspender_words=crate::tracker_host::runtime::views(env,&extra_rows).await;spender_words.extend(arm_views.unwrap_or_default());";
        let input = "lettracker=crate::owed::checked_tracker_mined(word_lookup_available.then_some(&spender_words),sp,);crate::owed::durable_spender_needs_proof(sp,&hops,&hop_chain,&hop_sweeps,tracker,)";
        let reask = "crate::owed::durable_reasks(candidates,room,&over_budget,";
        for wired in [views, input, reask] {
            assert!(routes.contains(wired), "the route's candidates: {wired}");
        }
        assert!(routes.find(views).unwrap() < routes.find(input).unwrap());
        assert!(routes.find(input).unwrap() < routes.find(reask).unwrap());
        assert!(
            !routes.contains("word_rows.get(sp.as_str()).map(|r|r.word==\"mined\")"),
            "never the stored row"
        );
    }

    /// bsv-low #693 fold (the W1-OWED-693 lens's LOW-2): RETIRED AND REFUTED ROWS ARE NOT RE-ASKED. The index-arm
    /// fixture (the index names the sweep spent, unconfirmed; its bytes durable) serves a payout waiting for the
    /// block, so its spender is a candidate; the same spender is NOT when the derivation retires the payout (a
    /// verified `collected`, a held filing naming the sweep, the home output spent) or the bytes refute the pointer
    /// (they do not spend the hop). The route filters phase 2b's candidates by the provisional derivation (source
    /// pin). RED on `6eb00a2` (no filter: each of the four was re-asked every pass; the source assertion fails).
    #[test]
    fn e693f_l2_a_retired_or_refuted_spender_is_not_re_asked() {
        let (v, no_pots) = (HashMap::new(), HashSet::new());
        let sweep = tx(0x0e);
        let game = tx(0x01);
        let my_pkh = "cc".repeat(20);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(game.clone(), my_pkh.clone());
        let mut h = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        h.spent_confirmed = Some(false);
        let hops = [h];
        let courier: HashSet<String> = [sweep.clone()].into();
        let spends_hop: HashMap<String, Vec<(String, u32)>> =
            [(sweep.clone(), vec![(tx(0x07), 0)])].into();
        let refuted: HashMap<String, Vec<(String, u32)>> =
            [(sweep.clone(), vec![(tx(0x09), 3)])].into();
        let unspent_home = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let spent_home = pays(&sweep, &[(0, &my_pkh, 20_000, Some(true))]);
        let none: HashSet<String> = HashSet::new();
        let collected: HashSet<String> = [game.clone()].into();
        let held: HashSet<String> = [held_key(&game, &sweep)].into();
        let waiting = |collected: &HashSet<String>,
                       held: &HashSet<String>,
                       inputs_of: &HashMap<String, Vec<(String, u32)>>,
                       outputs: &HashMap<String, Vec<SpenderOutput>>|
         -> bool {
            let mut i = inputs(&[], &[], &hops, &v, collected, &no_pots, Some(900_000));
            i.collected_present = collected;
            i.held_verified = held;
            i.spender_outputs = outputs;
            i.spender_inputs = inputs_of;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = &courier;
            sweeps_waiting_for_the_block(&derive_owed_rows(&i)).contains(&sweep)
        };
        assert!(
            waiting(&none, &none, &spends_hop, &unspent_home),
            "the waiting payout's spender is a candidate"
        );
        assert!(
            !waiting(&collected, &none, &spends_hop, &unspent_home),
            "collected"
        );
        assert!(
            !waiting(&none, &held, &spends_hop, &unspent_home),
            "a held filing"
        );
        assert!(
            !waiting(&none, &none, &spends_hop, &spent_home),
            "the home output spent"
        );
        assert!(
            !waiting(&none, &none, &refuted, &unspent_home),
            "the bytes refute the pointer"
        );
        // the route: the candidates filtered by the provisional derivation before the re-ask
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        let filter = ".into_iter().filter(|sp|waiting.contains(sp)).collect()";
        let derive =
            "letwaiting=crate::owed::sweeps_waiting_for_the_block(&derive_owed_rows(&OwedInputs{";
        let reask = "crate::owed::durable_reasks(candidates,room,&over_budget,";
        for wired in [derive, filter, reask] {
            assert!(routes.contains(wired), "the route's LOW-2 filter: {wired}");
        }
        assert!(routes.find(derive).unwrap() < routes.find(reask).unwrap());
    }

    /// bsv-low #693 fold: the latch range read over real SQLite, as the route's `read_proof_latches` makes it (the
    /// shipped `home_latch_range_read_sql` with `proof_latch_range_binds`).
    fn e693f_read_latches(
        conn: &rusqlite::Connection,
        spenders: &[String],
    ) -> Vec<crate::hops_view::ProbeMemo> {
        let binds = proof_latch_range_binds(spenders);
        let mut stmt = conn
            .prepare(&home_latch_range_read_sql(binds.len() / 2))
            .unwrap();
        stmt.query_map(rusqlite::params_from_iter(binds.iter()), |r| {
            Ok(crate::hops_view::ProbeMemo {
                outpoint: r.get(0)?,
                probed_at_ms: r.get::<_, i64>(1)?,
                spent: r.get::<_, i64>(2)? != 0,
                spending_txid: r.get(3)?,
                spent_confirmed: r.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                tip_height: None,
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    const E693F_ROOT: &str = "abababababababababababababababababababababababababababababababab";
    /// The clock of [`e693f_pass`]: the latch it writes is stamped at it, and judged at it.
    const E693F_NOW: i64 = 1_800_000_000_000;

    /// bsv-low #693 fold: ONE pass of the route's proof path over real SQLite, in the route's order: the durable
    /// spenders' latches read and honoured (the seed), the re-ask candidates (no honoured latch, still needing the
    /// proof), the re-ask under `room` unless `budget_cut`, then this pass's checked proofs latched through the
    /// shipped memo upsert. `mined` is what the courier's `/tx-any` would prove. Returns the proof set and the asks.
    #[allow(clippy::too_many_arguments)]
    fn e693f_pass(
        conn: &rusqlite::Connection,
        durable: &[String],
        hops: &[HopEntry],
        courier: &HashSet<String>,
        room: usize,
        budget_cut: bool,
        mined: &HashSet<String>,
        canonical: &HashMap<u64, Option<String>>,
    ) -> (HashSet<String>, Vec<String>) {
        e693f_pass_at(
            conn, durable, hops, courier, room, budget_cut, mined, canonical, E693F_NOW,
        )
    }

    /// [`e693f_pass`] at `now_ms`, with no tip (the newest latch stands in), the tombstone read with the latches as
    /// the route reads it.
    #[allow(clippy::too_many_arguments)]
    fn e693f_pass_at(
        conn: &rusqlite::Connection,
        durable: &[String],
        hops: &[HopEntry],
        courier: &HashSet<String>,
        room: usize,
        budget_cut: bool,
        mined: &HashSet<String>,
        canonical: &HashMap<u64, Option<String>>,
        now_ms: i64,
    ) -> (HashSet<String>, Vec<String>) {
        let memos = e693f_read_latches(conn, durable);
        let latches = proof_latches_of(durable, &memos);
        let latched = latched_proof_mined_spenders(
            &latches,
            canonical,
            None,
            crate::hops_view::reorg_clear_at(&memos),
            now_ms,
        );
        let mut proof = latched.clone();
        let candidates: Vec<String> = durable
            .iter()
            .filter(|sp| !latched.contains(*sp))
            .filter(|sp| {
                durable_spender_needs_proof(sp, hops, &HashMap::new(), &HashMap::new(), None)
            })
            .cloned()
            .collect();
        let asked = std::cell::RefCell::new(Vec::new());
        let mut answers = Vec::new();
        if !budget_cut {
            let reasks = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(durable_reasks(
                    candidates,
                    room,
                    || false,
                    |sp| {
                        asked.borrow_mut().push(sp.clone());
                        let m = mined.contains(&sp);
                        async move { (e693_answer(m), true) }
                    },
                ));
            for (r, a) in reasks {
                if r.outcome == DurableReaskOutcome::MinedByProof {
                    proof.insert(r.spender.clone());
                    answers.push((r.spender, a, Some(E693F_ROOT.to_string())));
                }
            }
        }
        let writes = proof_latches_to_write(&answers, &latched, courier, now_ms);
        write_memos(conn, &writes);
        (proof, asked.into_inner())
    }

    /// bsv-low #693 fold (the W1-OWED-693 lens's HIGH-1): LATCHED ONCE CHECKED, SERVED FROM THE LATCH ON EVERY LATER
    /// PASS. Two index-arm rows (the index names each hop's sweep spent, unconfirmed), both sweeps' bytes in the
    /// durable store. Pass N re-asks sweep A, whose `/tx-any` carries a checked mined proof: row A reads "home on
    /// chain" (`confirmedSource = "proof"`) and the proof is latched. Then the lens's three cases, each a later pass
    /// that makes no confirming ask of A: (a) the never-asked-first order puts B first and B's answer is unmined, (b)
    /// no slot (room 0), (c) `budget_cut`. Row A still reads "home on chain" from the latch, and A is never re-asked.
    /// The TTL sweep (the overlay's shipped statement) leaves the latch. RED on `e30e445` (no latch: the proof set of
    /// each later pass is empty and row A read `UNCONFIRMED_PAYOUT_REASON`; the API does not compile there, and the
    /// mutant `latched_proof_mined_spenders` answering empty is RED here).
    #[test]
    fn e693f_h1_a_checked_proof_is_latched_and_served_on_every_later_pass_real_sqlite() {
        use bsv_overlay_cloudflare::hop_probe_memos::{
            HOP_PROBE_MEMO_EXPIRE_SQL, HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES,
        };
        assert_eq!(
            HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES,
            [PROOF_MINED_LATCH_PREFIX]
        );
        let conn = memo_db();
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let (sweep_a, sweep_b) = (tx(0x0e), tx(0x0d));
        let my_pkh = "cc".repeat(20);
        let mut mine = pays(&sweep_a, &[(0, &my_pkh, 20_000, None)]);
        mine.extend(pays(&sweep_b, &[(0, &my_pkh, 20_000, None)]));
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        let mut hop_a = hop(HopStatus::Spent, Some(&sweep_a), Some(10_000_000));
        hop_a.spent_confirmed = Some(false);
        let mut hop_b = hop(HopStatus::Spent, Some(&sweep_b), Some(10_000_000));
        hop_b.spent_confirmed = Some(false);
        hop_b.hop_txid = tx(0x08);
        let hops = [hop_a, hop_b];
        let mut spends: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends.insert(sweep_a.clone(), vec![(tx(0x07), 0)]);
        spends.insert(sweep_b.clone(), vec![(tx(0x08), 0)]);
        let courier: HashSet<String> = [sweep_a.clone(), sweep_b.clone()].into();
        let row_a = |proof: &HashSet<String>| -> OwedRow {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.spender_outputs = &mine;
            i.spender_inputs = &spends;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = &courier;
            i.proof_mined_spenders = proof;
            let rows = derive_owed_rows(&i);
            let r = rows
                .into_iter()
                .find(|r| r.facts["sweepTxid"] == sweep_a.as_str())
                .expect("row A");
            assert_eq!(r.facts["spent"], true, "the index-hop-row arm");
            assert_eq!(
                r.facts["claimable"], false,
                "a courier-bytes row is never claimable"
            );
            r
        };
        let checked: HashMap<u64, Option<String>> =
            [(900_000u64, Some(E693F_ROOT.to_string()))].into();
        let a_mined: HashSet<String> = [sweep_a.clone()].into();
        let none: HashSet<String> = HashSet::new();
        // pass N: a slot, A re-asked, its proof confirms the row and is latched
        let (proof, asked) = e693f_pass(
            &conn,
            &[sweep_a.clone(), sweep_b.clone()],
            &hops,
            &courier,
            1,
            false,
            &a_mined,
            &checked,
        );
        assert_eq!(asked, vec![sweep_a.clone()]);
        assert_eq!(row_a(&proof).facts["confirmedSource"], "proof");
        let latches = proof_latches_of(
            std::slice::from_ref(&sweep_a),
            &e693f_read_latches(&conn, std::slice::from_ref(&sweep_a)),
        );
        assert_eq!(
            latches,
            vec![ProofLatch {
                spender: sweep_a.clone(),
                height: 900_000,
                root: E693F_ROOT.to_string(),
                written_at_ms: E693F_NOW,
            }],
            "the height and the root, never bytes"
        );
        // the overlay's TTL sweep, far past every window, leaves the latch
        conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![i64::MAX])
            .unwrap();
        for (case, order, room, cut) in [
            (
                "(a) B first, its answer unmined",
                [sweep_b.clone(), sweep_a.clone()],
                1usize,
                false,
            ),
            ("(b) no slot", [sweep_a.clone(), sweep_b.clone()], 0, false),
            (
                "(c) budget_cut",
                [sweep_a.clone(), sweep_b.clone()],
                2,
                true,
            ),
        ] {
            let (proof, asked) =
                e693f_pass(&conn, &order, &hops, &courier, room, cut, &none, &checked);
            assert!(
                !asked.contains(&sweep_a),
                "{case}: a latched spender is never re-asked"
            );
            let r = row_a(&proof);
            assert_eq!(r.facts["confirmedSource"], "proof", "{case}");
            assert_eq!(
                r.facts["claimReason"], COURIER_BYTES_NO_CREDIT_REASON,
                "{case}"
            );
            // a header read that faulted honours the latch once it is past the #484 grace with a clean clear history
            // (bsv-low #693 fold-2, the re-lens's R-HIGH-1 (2): a young latch waits for its recheck)
            let fault: HashMap<u64, Option<String>> = [(900_000u64, None)].into();
            let (proof, _) = e693f_pass_at(
                &conn,
                &order,
                &hops,
                &courier,
                room,
                cut,
                &none,
                &fault,
                E693F_NOW + crate::hops_view::PROBE_MEMO_REORG_GRACE_MS,
            );
            assert_eq!(
                row_a(&proof).facts["confirmedSource"],
                "proof",
                "{case}, header fault"
            );
        }
        // the route wiring: the seed before the candidates, the candidates without a latched spender, the latch in
        // the snapshot batch, the read through the shipped range read
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        let seed = "proof_latched=crate::owed::latched_proof_mined_spenders(&latches,&canonical,latch_tip,latch_clear,now_ms,);proof_mined_spenders.extend(proof_latched.iter().cloned());";
        let cands = ".filter(|sp|!proof_latched.contains(*sp))";
        let reask = "crate::owed::durable_reasks(candidates,room,&over_budget,";
        for wired in [
            seed,
            cands,
            reask,
            "proof_answers.push((r.spender.clone(),a));",
            "forlatchincrate::owed::proof_latches_to_write(&proofs_to_latch,&proof_latched,&courier_spenders,now_ms,){",
            ".prepare(crate::hops_view::PROBE_MEMO_UPSERT_SQL)",
            "read_latch_ranges(db,spenders,crate::owed::proof_latch_range_binds).await",
        ] {
            assert!(routes.contains(wired), "the route's latch: {wired}");
        }
        assert!(routes.find(seed).unwrap() < routes.find(cands).unwrap());
        assert!(routes.find(cands).unwrap() < routes.find(reask).unwrap());
    }

    /// bsv-low #693 fold (HIGH-1 (b)): A REORG DEMOTES THE LATCH. The overlay's shipped reorg clear deletes it (a mined
    /// proof is a chain word, never spared), and the next pass with no slot reads row A "waiting for the block" again
    /// until a new proof latches. A latch whose root is no longer chaintracks' root at its height is not honoured
    /// either (a reorg the clear has not yet run for). To red: spare `proofmined:` in the clear, or honour a moved root.
    #[test]
    fn e693f_h1_b_a_reorg_demotes_the_latch_and_the_row_waits_again_real_sqlite() {
        use bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_REORG_CLEAR_SQL;
        let conn = memo_db();
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0e);
        let my_pkh = "cc".repeat(20);
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        let mut h = hop(HopStatus::Spent, Some(&sweep), Some(10_000_000));
        h.spent_confirmed = Some(false);
        let hops = [h];
        let mut spends: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let courier: HashSet<String> = [sweep.clone()].into();
        let reason = |proof: &HashSet<String>| -> serde_json::Value {
            let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
            i.spender_outputs = &mine;
            i.spender_inputs = &spends;
            i.my_pkh_by_game = &pkhs;
            i.courier_spenders = &courier;
            i.proof_mined_spenders = proof;
            derive_owed_rows(&i)[0].facts["claimReason"].clone()
        };
        let checked: HashMap<u64, Option<String>> =
            [(900_000u64, Some(E693F_ROOT.to_string()))].into();
        let mined: HashSet<String> = [sweep.clone()].into();
        let none: HashSet<String> = HashSet::new();
        let s = std::slice::from_ref(&sweep);
        let (proof, _) = e693f_pass(&conn, s, &hops, &courier, 1, false, &mined, &checked);
        assert_eq!(reason(&proof), COURIER_BYTES_NO_CREDIT_REASON);
        let (proof, _) = e693f_pass(&conn, s, &hops, &courier, 0, false, &none, &checked);
        assert_eq!(
            reason(&proof),
            COURIER_BYTES_NO_CREDIT_REASON,
            "served from the latch"
        );
        // a moved root at the latch's height: not honoured
        let moved: HashMap<u64, Option<String>> = [(900_000u64, Some("cd".repeat(32)))].into();
        let (proof, _) = e693f_pass(&conn, s, &hops, &courier, 0, false, &none, &moved);
        assert_eq!(
            reason(&proof),
            UNCONFIRMED_PAYOUT_REASON,
            "a moved root is not honoured"
        );
        // the overlay's reorg clear takes the latch
        assert_eq!(conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, []).unwrap(), 1);
        assert!(e693f_read_latches(&conn, s).is_empty());
        let (proof, _) = e693f_pass(&conn, s, &hops, &courier, 0, false, &none, &checked);
        assert_eq!(
            reason(&proof),
            UNCONFIRMED_PAYOUT_REASON,
            "the row waits for the block again"
        );
        // the next checked proof latches again
        let (proof, asked) = e693f_pass(&conn, s, &hops, &courier, 1, false, &mined, &checked);
        assert_eq!(
            (asked.len(), reason(&proof)),
            (1, COURIER_BYTES_NO_CREDIT_REASON.into())
        );
        assert_eq!(proof_latches_of(s, &e693f_read_latches(&conn, s)).len(), 1);
    }

    /// bsv-low #693 fold (HIGH-1 (c)): A LATCH NEVER EXISTS WITHOUT A CHECKED PROOF. Only `proof_mined_word` (confirmed
    /// AND a proven height) and a canonical root write one: a courier's bare `confirmed`, a bare claimed height, a SEEN,
    /// a fault or a missing root writes nothing; nor does an index-bytes spender (not courier-bytes), nor one a
    /// latch already covers. A malformed row read back is no latch. To red: latch on `confirmed` alone.
    #[test]
    fn e693f_h1_c_a_latch_never_exists_without_a_checked_proof() {
        let sp = tx(0x0e);
        let root = Some(E693F_ROOT);
        assert!(proof_latch_memo(&sp, &e693_answer(true), root, 1).is_some());
        let bare_confirmed = crate::txany::TxAnyAnswer {
            confirmed: Some(true),
            height: None,
            ..e693_answer(false)
        };
        let bare_height = crate::txany::TxAnyAnswer {
            claimed_height: Some(900_000),
            ..e693_answer(false)
        };
        for (what, a) in [
            ("a bare confirmed", bare_confirmed),
            ("a bare claimed height", bare_height),
            ("a SEEN", e693_answer(false)),
            ("a fault", crate::txany::TxAnyAnswer::default()),
        ] {
            assert!(proof_latch_memo(&sp, &a, root, 1).is_none(), "{what}");
        }
        assert!(
            proof_latch_memo(&sp, &e693_answer(true), None, 1).is_none(),
            "no root"
        );
        assert!(
            proof_latch_memo(&sp, &e693_answer(true), Some("zz"), 1).is_none(),
            "a malformed root"
        );
        let courier: HashSet<String> = [sp.clone()].into();
        let proofs = vec![(sp.clone(), e693_answer(true), root.map(str::to_string))];
        assert_eq!(
            proof_latches_to_write(&proofs, &HashSet::new(), &courier, 1).len(),
            1
        );
        assert!(
            proof_latches_to_write(&proofs, &HashSet::new(), &HashSet::new(), 1).is_empty(),
            "index-bytes"
        );
        assert!(
            proof_latches_to_write(&proofs, &courier, &courier, 1).is_empty(),
            "already latched"
        );
        // the bound
        let many: Vec<_> = (0x30u8..0x38)
            .map(|b| (tx(b), e693_answer(true), root.map(str::to_string)))
            .collect();
        let all: HashSet<String> = many.iter().map(|(s, _, _)| s.clone()).collect();
        assert_eq!(
            proof_latches_to_write(&many, &HashSet::new(), &all, 1).len(),
            OWED_PROOF_LATCH_WRITES_PER_RECOMPUTE
        );
        // a row that is not a whole latch is no latch
        let key = |h: &str| format!("{PROOF_MINED_LATCH_PREFIX}{sp}.{h}");
        let memo =
            |k: String, confirmed: Option<bool>, root: Option<&str>| crate::hops_view::ProbeMemo {
                outpoint: k,
                probed_at_ms: 1,
                spent: true,
                spending_txid: root.map(str::to_string),
                spent_confirmed: confirmed,
                tip_height: None,
            };
        let s = std::slice::from_ref(&sp);
        for bad in [
            memo(key("900000"), None, root),
            memo(key("900000"), Some(true), None),
            memo(key("x"), Some(true), root),
            memo(format!("{HOME_SPEND_LATCH_PREFIX}{sp}.0"), Some(true), root),
        ] {
            assert!(
                proof_latches_of(s, std::slice::from_ref(&bad)).is_empty(),
                "{bad:?}"
            );
        }
        assert_eq!(
            proof_latches_of(s, &[memo(key("900000"), Some(true), root)]).len(),
            1
        );
        assert_eq!(proof_latch_recheck_heights(&[], None).len(), 0);
    }

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
        // the route reads the tip before the rechecks and hands it to the shipped choice
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        let tip = "letlatch_tip=tip;";
        let recheck = "forhincrate::owed::proof_latch_recheck_heights(&latches,latch_tip){";
        assert!(routes.contains(recheck), "the route's recheck: {recheck}");
        let at = routes.find(recheck).unwrap();
        assert!(
            routes[..at].rfind(tip).is_some_and(|t| at - t < 200),
            "the pass's tip handed to the recheck"
        );
    }

    /// bsv-low #693 fold-2 (the W1-OWED-693 re-lens's R-HIGH-1 (2), (3)): A LATCH NOT RE-READ THIS PASS IS HONOURED
    /// ONLY PAST THE #484 GRACE WITH A CLEAN CLEAR HISTORY, AND FOR TWO HOURS UNLESS REORG-PROOF. Over real SQLite,
    /// the tombstone written by the overlay's shipped statement and read in the latch read itself. (a) A latch written
    /// 5 s after a clear (a pass's proof checked before the reorg, committed after the clear) is not honoured on a
    /// skipped or faulted recheck at any age, nor once reorg-proof by depth, until a recheck matches. (b) A recheck
    /// skipped under `over_budget()`: a young latch is not honoured, one past the grace with no clear (or a clear
    /// long before it) is, until two hours. (c) A clear that FAULTED: with its tombstone written the latch, written
    /// before it, waits for a recheck; with no tombstone either it stands two hours, then its spender is re-asked and
    /// a new proof re-latches it. RED on `fb26a34` (every unrechecked latch honoured: (a) at its first assertion).
    #[test]
    fn e693f2_h2_an_unrechecked_latch_is_honoured_only_past_the_grace_with_a_clean_clear_history_real_sqlite(
    ) {
        use crate::hops_view::{reorg_clear_at, PROBE_MEMO_REORG_GRACE_MS as GRACE};
        use bsv_overlay_cloudflare::hop_probe_memos::{
            HOP_PROBE_MEMO_REORG_MARK_SQL, HOP_PROBE_MEMO_TTL_MS,
        };
        assert_eq!(PROOF_LATCH_UNRECHECKED_MAX_AGE_MS, HOP_PROBE_MEMO_TTL_MS);
        const MIN: i64 = 60_000;
        let t0 = E693F_NOW;
        let sp = tx(0x0e);
        let s = std::slice::from_ref(&sp);
        let latch_at = |conn: &rusqlite::Connection, at: i64| {
            write_memos(
                conn,
                &[proof_latch_memo(&sp, &e693_answer(true), Some(E693F_ROOT), at).unwrap()],
            )
        };
        let clear_at = |conn: &rusqlite::Connection, at: i64| {
            conn.execute(HOP_PROBE_MEMO_REORG_MARK_SQL, rusqlite::params![at])
                .unwrap();
        };
        let judge = |conn: &rusqlite::Connection,
                     canonical: &HashMap<u64, Option<String>>,
                     tip: Option<u64>,
                     now: i64|
         -> bool {
            let memos = e693f_read_latches(conn, s);
            latched_proof_mined_spenders(
                &proof_latches_of(s, &memos),
                canonical,
                tip,
                reorg_clear_at(&memos),
                now,
            )
            .contains(&sp)
        };
        let skipped: HashMap<u64, Option<String>> = HashMap::new();
        let fault: HashMap<u64, Option<String>> = [(900_000u64, None)].into();
        let checked: HashMap<u64, Option<String>> =
            [(900_000u64, Some(E693F_ROOT.to_string()))].into();
        let moved: HashMap<u64, Option<String>> = [(900_000u64, Some("cd".repeat(32)))].into();
        let deep = Some(900_000 + LATCH_RECHECK_DEPTH);

        // (a) written 5 s after a clear
        let conn = memo_db();
        clear_at(&conn, t0);
        latch_at(&conn, t0 + 5_000);
        assert_eq!(
            reorg_clear_at(&e693f_read_latches(&conn, s)),
            Some(t0),
            "the tombstone rides the latch read"
        );
        for now in [t0 + 6_000, t0 + GRACE + 5_000, t0 + 90 * MIN] {
            for (what, c) in [("skipped", &skipped), ("faulted", &fault)] {
                assert!(
                    !judge(&conn, c, None, now),
                    "(a) a {what} recheck, {} min on",
                    (now - t0) / MIN
                );
            }
        }
        assert!(
            !judge(&conn, &skipped, deep, t0 + 5 * 60 * MIN),
            "(a) reorg-proof by depth, still unrechecked"
        );
        assert!(
            judge(&conn, &checked, None, t0 + 6_000),
            "(a) a recheck that matches"
        );
        assert!(!judge(&conn, &moved, None, t0 + 6_000), "(a) a moved root");

        // (b) a recheck skipped under over_budget, no clear
        let conn = memo_db();
        latch_at(&conn, t0);
        assert!(!judge(&conn, &skipped, None, t0 + 10 * MIN), "(b) young");
        assert!(
            judge(&conn, &skipped, None, t0 + 40 * MIN),
            "(b) old, no clear"
        );
        clear_at(&conn, t0 - GRACE);
        assert!(
            judge(&conn, &skipped, None, t0 + 40 * MIN),
            "(b) old, the clear a grace before its write"
        );
        assert!(
            !judge(&conn, &skipped, None, t0 + 2 * 60 * MIN),
            "(b) two hours unrechecked"
        );
        assert!(
            judge(&conn, &skipped, deep, t0 + 5 * 60 * MIN),
            "(b) reorg-proof, clean history"
        );

        // (c) a clear that faulted, its tombstone written: the latch was written before it
        let conn = memo_db();
        latch_at(&conn, t0);
        clear_at(&conn, t0 + 60 * MIN);
        for now in [t0 + 61 * MIN, t0 + 3 * 60 * MIN] {
            assert!(
                !judge(&conn, &skipped, None, now),
                "(c) a clear after its write"
            );
            assert!(
                !judge(&conn, &skipped, deep, now),
                "(c) deep, a clear after its write"
            );
        }
        assert!(judge(&conn, &checked, None, t0 + 61 * MIN), "(c) rechecked");
        // both writes of the clear faulted (no tombstone): two hours, not for ever, then re-asked
        let conn = memo_db();
        latch_at(&conn, t0);
        assert!(judge(&conn, &fault, None, t0 + 60 * MIN), "(c) one hour");
        assert!(
            !judge(&conn, &fault, None, t0 + 2 * 60 * MIN),
            "(c) two hours"
        );
        let mut h = hop(HopStatus::Spent, Some(&sp), Some(10_000_000));
        h.spent_confirmed = Some(false);
        let hops = [h];
        let courier: HashSet<String> = [sp.clone()].into();
        let mined: HashSet<String> = [sp.clone()].into();
        let later = t0 + 2 * 60 * MIN;
        let (proof, asked) =
            e693f_pass_at(&conn, s, &hops, &courier, 1, false, &mined, &fault, later);
        assert_eq!(
            asked,
            vec![sp.clone()],
            "(c) the stale latch's spender is re-asked"
        );
        assert!(proof.contains(&sp));
        assert_eq!(
            proof_latches_of(s, &e693f_read_latches(&conn, s))[0].written_at_ms,
            later,
            "(c) re-latched at the new proof's time"
        );
        assert!(
            judge(&conn, &fault, None, later + GRACE),
            "(c) the new latch, past its grace"
        );
        // the read: one range per spender and the tombstone's
        let binds = proof_latch_range_binds(&[]);
        assert_eq!(
            binds,
            vec![
                crate::hops_view::PROBE_MEMO_REORG_MARK.to_string(),
                "reorg-clear.1".into()
            ]
        );
        // the route judges with the tombstone of the latch read and the pass's clock
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        for wired in [
            "letlatch_memos=read_proof_latches(db,&latch_spenders).await;letlatch_clear=crate::hops_view::reorg_clear_at(&latch_memos);",
            ".prepare(crate::owed::home_latch_range_read_sql(binds.len()/2))",
        ] {
            assert!(routes.contains(wired), "the route's latch judgement: {wired}");
        }
    }

    /// bsv-low #693 fold-3 (the W1-OWED-693 delta lens's R2-HIGH-1 (a)): A MOVED-ROOT REFUSAL IS DURABLE. Over real
    /// SQLite, the delta lens's reproduction: five latches at 900000..900004, the OLDEST's root moved by a reorg that
    /// NO detector recorded (no clear, no tombstone), judged 3 h after the write. Pass 1 at tip 900003 re-reads every
    /// height and refuses it, and its batch deletes that latch (the shipped statement, one bind, the other four kept).
    /// Pass 2 at tip 900010 re-reads only the four newest (900000 is 10 deep): the moved latch is NOT honoured
    /// (`honoured: false`; on `18b4226` the refusal was a filter only and the latch, now reorg-proof by depth, was
    /// honoured again). One latch alone, refused once, then its header read FAULTED at tip 900010: not honoured. A
    /// later checked proof at the new height latches afresh and is honoured. A fresh latch at the same key with
    /// another root is not taken by the delete. RED on `18b4226` (the API does not compile there; the mutant
    /// `moved_proof_latches` answering empty is RED here at pass 2).
    #[test]
    fn e693f3_h1_a_moved_root_refusal_is_durable_real_sqlite() {
        const MIN: i64 = 60_000;
        let t0 = E693F_NOW;
        let moved_root = "cd".repeat(32);
        let spenders: Vec<String> = (0..5u8).map(|i| tx(0x60 + i)).collect();
        let latch = |conn: &rusqlite::Connection, sp: &str, height: u64, root: &str, at: i64| {
            let mut a = e693_answer(true);
            a.height = Some(height);
            write_memos(conn, &[proof_latch_memo(sp, &a, Some(root), at).unwrap()]);
        };
        // one pass in the route's order: the latch read, the newest-first recheck against `chain`, the judgement,
        // then the batch's delete of the moved latches through the shipped statement
        let pass = |conn: &rusqlite::Connection,
                    s: &[String],
                    tip: u64,
                    chain: &dyn Fn(u64) -> Option<String>,
                    now: i64|
         -> (HashSet<String>, Vec<u64>, usize) {
            let memos = e693f_read_latches(conn, s);
            let latches = proof_latches_of(s, &memos);
            let heights = proof_latch_recheck_heights(&latches, Some(tip));
            let canonical: HashMap<u64, Option<String>> =
                heights.iter().map(|h| (*h, chain(*h))).collect();
            let honoured = latched_proof_mined_spenders(
                &latches,
                &canonical,
                Some(tip),
                crate::hops_view::reorg_clear_at(&memos),
                now,
            );
            let deleted =
                match moved_proof_latch_delete_bind(&moved_proof_latches(&latches, &canonical)) {
                    Some(bind) => conn
                        .execute(PROOF_LATCH_MOVED_DELETE_SQL, rusqlite::params![bind])
                        .unwrap(),
                    None => 0,
                };
            (honoured, heights, deleted)
        };
        let reorged = |h: u64| {
            Some(if h == 900_000 {
                moved_root.clone()
            } else {
                E693F_ROOT.to_string()
            })
        };

        // the lens's reproduction
        let conn = memo_db();
        for (i, sp) in spenders.iter().enumerate() {
            latch(&conn, sp, 900_000 + i as u64, E693F_ROOT, t0);
        }
        let (h1, heights1, deleted1) = pass(&conn, &spenders, 900_003, &reorged, t0 + 3 * 60 * MIN);
        assert_eq!(heights1, vec![900_004, 900_003, 900_002, 900_001, 900_000]);
        assert!(!h1.contains(&spenders[0]), "pass 1: the moved root refused");
        assert_eq!(h1.len(), 4);
        let (h2, heights2, deleted2) = pass(&conn, &spenders, 900_010, &reorged, t0 + 3 * 60 * MIN);
        assert_eq!(
            heights2,
            vec![900_004, 900_003, 900_002, 900_001],
            "pass 2: 900000 is reorg-proof by depth and not re-read"
        );
        assert!(
            !h2.contains(&spenders[0]),
            "pass 2: honoured: {} (the moved latch, 10 deep, unrechecked)",
            h2.contains(&spenders[0])
        );
        assert_eq!(deleted1, 1, "pass 1: the moved latch alone is deleted");
        assert_eq!((h2.len(), deleted2), (4, 0));

        // one latch alone: refused once, then its header read faulted, deep, no clear
        let conn = memo_db();
        let s = std::slice::from_ref(&spenders[0]);
        latch(&conn, &spenders[0], 900_000, E693F_ROOT, t0);
        let (h, _, d) = pass(&conn, s, 900_003, &reorged, t0 + 3 * 60 * MIN);
        assert!(!h.contains(&spenders[0]) && d == 1);
        let (h, _, _) = pass(&conn, s, 900_010, &|_| None, t0 + 3 * 60 * MIN);
        assert!(!h.contains(&spenders[0]), "a faulted header read, deep");
        // the spender is re-asked; the sweep re-mined at 900002: a fresh latch, honoured
        latch(&conn, &spenders[0], 900_002, E693F_ROOT, t0 + 4 * 60 * MIN);
        let (h, _, d) = pass(&conn, s, 900_010, &reorged, t0 + 4 * 60 * MIN + 1_000);
        assert!(h.contains(&spenders[0]) && d == 0, "the new height's latch");

        // the delete takes a row only while it holds the refused root
        let conn = memo_db();
        latch(&conn, &spenders[1], 900_000, &moved_root, t0);
        let stale = vec![(
            crate::hops_view::probe_memo_key(
                &format!("{PROOF_MINED_LATCH_PREFIX}{}", spenders[1]),
                900_000,
            ),
            E693F_ROOT.to_string(),
        )];
        let bind = moved_proof_latch_delete_bind(&stale).unwrap();
        assert_eq!(
            conn.execute(PROOF_LATCH_MOVED_DELETE_SQL, rusqlite::params![bind])
                .unwrap(),
            0
        );
        assert_eq!(
            proof_latches_of(&spenders[1..2], &e693f_read_latches(&conn, &spenders[1..2])).len(),
            1
        );
        assert_eq!(
            moved_proof_latch_delete_bind(&[]),
            None,
            "nothing moved: no statement"
        );
        assert!(
            moved_proof_latches(
                &proof_latches_of(&spenders[1..2], &e693f_read_latches(&conn, &spenders[1..2])),
                &[(900_000u64, None)].into()
            )
            .is_empty(),
            "a faulted header read deletes nothing"
        );

        // the route: the moved latches taken from the shipped judgement's recheck, deleted in the snapshot batch
        // before the latch writes, one statement
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        let collect = "proof_latches_moved=crate::owed::moved_proof_latches(&latches,&canonical);";
        let delete = ".prepare(crate::owed::PROOF_LATCH_MOVED_DELETE_SQL)";
        let writes = "forlatchincrate::owed::proof_latches_to_write(";
        assert!(
            routes.contains(collect),
            "the route keeps the moved latches"
        );
        let (d, w) = (routes.find(delete), routes.find(writes));
        assert!(
            d.is_some() && w.is_some() && d < w,
            "the moved latches' delete rides the batch before the latch writes"
        );
        assert_eq!(routes.matches(delete).count(), 1);
        assert!(routes[d.unwrap()..w.unwrap()].contains("stmts.insert(stmts.len()-1,moved_stmt);"));
    }

    /// bsv-low #662 (live 2026-10-14 00:35:35Z, pair 19, hop d4029675ccb65a4b:0, sweep 96448ef1fc3b): pass B judged
    /// the swept hop a courier-bytes payout; pass C's spender courier cap WITHHELD the sweep's bytes and the row
    /// flapped to `unbound`, "could not judge the spender". A withheld look names nothing more than it did before:
    /// the previous row stands, marked `refresh: "withheld"`. A FAULTED read keeps the fault row.
    #[test]
    fn i662_a_withheld_spender_read_keeps_the_judged_courier_bytes_payout_a_faulted_one_does_not() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let sweep = tx(0x0e);
        let my_pkh = "cc".repeat(20);
        let mine = pays(&sweep, &[(0, &my_pkh, 20_000, None)]);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh.clone());
        // the live shape: the index never held the hop's spend (swept around the overlay); the chain rung names the
        // sweep, confirmed; the JOIN was evicted
        let hops = [hop(
            HopStatus::Unknown,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let mut courier: HashSet<String> = HashSet::new();
        courier.insert(sweep.clone());
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(sweep.clone(), vec![(tx(0x07), 0)]);
        let no_bytes: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        let no_inputs: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        let none: HashSet<String> = HashSet::new();
        // pass B: the courier's bytes in hand, the row judged
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &mine;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        i.courier_spenders = &courier;
        let judged = derive_owed_rows(&i);
        assert_eq!(judged.len(), 1);
        assert_eq!(judged[0].outpoint, key);
        assert_eq!(judged[0].family, OwedFamily::Payout);
        assert_eq!(judged[0].facts["creditKind"], "courier-bytes");
        // the judging pass ten minutes before this one (the helper's `now_ms`), the first keep five minutes after it
        let judged_at = 1_800_000_000_000i64 - 10 * 60_000;
        let previous = vec![(judged[0].clone(), judged_at)];
        // pass C: the same hop, the same spender named, its bytes WITHHELD by the courier cap
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &no_bytes;
        i.spender_inputs = &no_inputs;
        i.my_pkh_by_game = &pkhs;
        let derived = derive_owed_rows(&i);
        assert_eq!(derived[0].family, OwedFamily::Unbound);
        assert!(derived[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not judge the spender"));
        let mut withheld: HashSet<String> = HashSet::new();
        withheld.insert(sweep.clone());
        let mut rows = derived.clone();
        assert_eq!(
            keep_withheld_payouts(&i, &mut rows, &previous, &withheld),
            1
        );
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(
            (r.family, r.sats, r.outpoint.as_str()),
            (OwedFamily::Payout, Some(20_000), key.as_str()),
            "a withheld read never turns a judged payout into could-not-judge: {r:?}"
        );
        assert_eq!(r.facts["creditKind"], "courier-bytes");
        assert_eq!(r.facts["sweepTxid"], json!(sweep));
        assert_eq!(r.facts["claimable"], false);
        assert_eq!(r.facts["refresh"], OWED_REFRESH_WITHHELD);
        assert_eq!(r.facts["judgedAtMs"], judged_at);
        assert!(
            row_is_open(r),
            "the kept row is open: the read re-judges it"
        );
        // a second withheld pass keeps the FIRST judgement's stamp
        let again = vec![(r.clone(), judged_at + 5 * 60_000)];
        let mut rows2 = derived.clone();
        assert_eq!(keep_withheld_payouts(&i, &mut rows2, &again, &withheld), 1);
        assert_eq!(rows2[0].facts["judgedAtMs"], judged_at);
        // the L25-WITHHELD lens's MEDIUM-1 (1): a third withheld pass AT the bound still keeps; one ms past it keeps
        // NOTHING, and the honest could-not-judge row returns
        let kept_twice = vec![(rows2[0].clone(), judged_at + 10 * 60_000)];
        let mut at_bound = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        at_bound.hop_chain = &chain;
        at_bound.spender_outputs = &no_bytes;
        at_bound.spender_inputs = &no_inputs;
        at_bound.my_pkh_by_game = &pkhs;
        at_bound.now_ms = judged_at + OWED_WITHHELD_KEEP_MAX_MS;
        let mut rows3 = derived.clone();
        assert_eq!(
            keep_withheld_payouts(&at_bound, &mut rows3, &kept_twice, &withheld),
            1,
            "at the bound the judgement still stands"
        );
        at_bound.now_ms = judged_at + OWED_WITHHELD_KEEP_MAX_MS + 1;
        let mut rows3 = derived.clone();
        assert_eq!(
            keep_withheld_payouts(&at_bound, &mut rows3, &kept_twice, &withheld),
            0,
            "a judgement older than OWED_WITHHELD_KEEP_MAX_MS is never kept"
        );
        assert_eq!(
            rows3, derived,
            "past the bound the could-not-judge row stands"
        );
        // the lens's LOW-2: the slot must be THIS pass's spender judgement. A markerless evidence READ-FAULT row at
        // the outpoint (`merge_markerless_faults` put it there) is never replaced, nor a spender row so marked
        let mut fault_row = derived[0].clone();
        fault_row.facts = json!({"markerlessEvidence": true, "markerlessReadFault": true, "hopTxid": tx(0x07),
            "hopVout": 0, "claim": null, "claimable": false});
        fault_row.reason = Some("could not judge: evidence-missing".into());
        let mut rows4 = vec![fault_row.clone()];
        assert_eq!(
            keep_withheld_payouts(&i, &mut rows4, &previous, &withheld),
            0
        );
        assert_eq!(rows4, vec![fault_row]);
        let mut marked = derived[0].clone();
        marked.facts["markerlessReadFault"] = json!(true);
        let mut rows4 = vec![marked.clone()];
        assert!(withheld_hop_outpoints(&i, &rows4, &withheld).is_empty());
        assert_eq!(
            keep_withheld_payouts(&i, &mut rows4, &previous, &withheld),
            0
        );
        assert_eq!(rows4, vec![marked]);
        // a FAULTED read (the spender looked at, the answer bad): never withheld, the fault row stands
        let mut rows = derived.clone();
        assert_eq!(keep_withheld_payouts(&i, &mut rows, &previous, &none), 0);
        assert_eq!(rows, derived);
        // the pot spenders' read faulted this pass: the fault row stands
        let mut faulted = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        faulted.hop_chain = &chain;
        faulted.spender_outputs = &no_bytes;
        faulted.my_pkh_by_game = &pkhs;
        faulted.pot_spenders_faulted = true;
        let mut rows = derived.clone();
        assert_eq!(
            keep_withheld_payouts(&faulted, &mut rows, &previous, &withheld),
            0
        );
        // the chain now names ANOTHER spender: the previous evidence is not about it
        let other = tx(0x0f);
        let chain_other = chain_confirmed(&key, true, Some(true), Some(&other), Some(true));
        let mut i2 = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i2.hop_chain = &chain_other;
        i2.spender_outputs = &no_bytes;
        i2.my_pkh_by_game = &pkhs;
        let derived_other = derive_owed_rows(&i2);
        let mut rows = derived_other.clone();
        let mut withheld_other: HashSet<String> = HashSet::new();
        withheld_other.insert(other.clone());
        assert_eq!(
            keep_withheld_payouts(&i2, &mut rows, &previous, &withheld_other),
            0
        );
        assert_eq!(rows, derived_other);
        // the previous row was not a courier-bytes payout: nothing to keep
        let mut index_bytes = judged[0].clone();
        index_bytes.facts["creditKind"] = Value::Null;
        let mut rows = derived.clone();
        assert_eq!(
            keep_withheld_payouts(&i, &mut rows, &[(index_bytes, 1)], &withheld),
            0
        );
        // a verified `collected` filing retired the game since: the row is not brought back
        let mut collected: HashSet<String> = HashSet::new();
        collected.insert(tx(0x01));
        let mut i3 = inputs(&[], &[], &hops, &v, &collected, &no_pots, Some(900_000));
        i3.hop_chain = &chain;
        i3.spender_outputs = &no_bytes;
        i3.my_pkh_by_game = &pkhs;
        let mut rows = derived.clone();
        assert_eq!(
            keep_withheld_payouts(&i3, &mut rows, &previous, &withheld),
            0
        );
        // the bytes in hand this pass: the derivation judges, nothing is kept
        let mut i4 = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i4.hop_chain = &chain;
        i4.spender_outputs = &mine;
        i4.spender_inputs = &spends_it;
        i4.my_pkh_by_game = &pkhs;
        i4.courier_spenders = &courier;
        let mut rows = derive_owed_rows(&i4);
        assert_eq!(
            keep_withheld_payouts(&i4, &mut rows, &previous, &withheld),
            0
        );
        assert!(rows[0].facts.get("refresh").is_none());
    }

    /// bsv-low #672 (RC-L30-COULD-NOT-JUDGE, loop 30's first red, pair 13, hop 16821e21…:0): pass A judged the hop
    /// `spent-elsewhere` (the chain rung named a competitor paying none of this identity's homes) and served "spent
    /// outside this game"; pass B's courier budget was cut before it re-read the competitor's bytes and the row
    /// REGRESSED to "could not judge the spender" — a completed recompute that knew LESS than the snapshot it
    /// replaced. The belt ([`keep_withheld_elsewhere`]) carries the judged word forward, keyed on the hop UTXO plus
    /// the content-addressed spender txid, within the same bound; past it the honest could-not-judge row returns; the
    /// bytes in hand this pass judge fresh and carry nothing. (Pin 1.)
    /// To RED: stub `keep_withheld_elsewhere` to `0` — the post-keep assertion fails on the derived could-not-judge
    /// row (the inline `contains("could not judge the spender")` assertion IS pass B's served loop-30 word).
    #[test]
    fn i672_a_a_withheld_spender_keeps_the_judged_spent_elsewhere_row_not_could_not_judge() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let competitor = tx(0x0d);
        let my_pkh = "cc".repeat(20);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), my_pkh); // this identity's committed home for the game
                                       // a competitor paying 20_170 to a pkh that is NOT any home of this identity: the spent-outside story
        let elsewhere = pays(&competitor, &[(0, &"99".repeat(20), 20_170, Some(false))]);
        let hops = [hop(
            HopStatus::Unknown,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let chain = chain_confirmed(&key, true, Some(true), Some(&competitor), Some(true));
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(competitor.clone(), vec![(tx(0x07), 0)]);
        let no_bytes: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        let no_inputs: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        // pass A: the competitor's bytes in hand, the row JUDGED spent-elsewhere (the good body at 15:05:38Z)
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &elsewhere;
        i.spender_inputs = &spends_it;
        i.my_pkh_by_game = &pkhs;
        let judged = derive_owed_rows(&i);
        assert_eq!(judged.len(), 1);
        assert_eq!(judged[0].outpoint, key);
        assert_eq!(judged[0].family, OwedFamily::Unbound);
        assert_eq!(judged[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(judged[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        let judged_at = 1_800_000_000_000i64 - 10 * 60_000;
        let previous = vec![(judged[0].clone(), judged_at)];
        // pass B (the loop-30 recompute, the budget cut): the SAME hop, the competitor's bytes WITHHELD
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &no_bytes;
        i.spender_inputs = &no_inputs;
        i.my_pkh_by_game = &pkhs;
        let derived = derive_owed_rows(&i);
        // the regression, in one line: derive alone serves the clobbered word (pass B's `9c422cba…` body)
        assert_eq!(derived[0].family, OwedFamily::Unbound);
        assert!(derived[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not judge the spender"));
        assert!(derived[0].facts.get("spendKind").is_none());
        let mut withheld: HashSet<String> = HashSet::new();
        withheld.insert(competitor.clone());
        let mut rows = derived.clone();
        assert_eq!(
            keep_withheld_elsewhere(&i, &mut rows, &previous, &withheld),
            1
        );
        let r = &rows[0];
        assert_eq!(
            r.facts["spendKind"], "spent-elsewhere",
            "a withheld read never clobbers a judged spent-elsewhere row: {r:?}"
        );
        assert_eq!(r.reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        assert_eq!(r.facts["refresh"], OWED_REFRESH_WITHHELD);
        assert_eq!(r.facts["carried"], true);
        assert_eq!(r.facts["judgedAtMs"], judged_at);
        assert_eq!(r.facts["claimable"], false);
        assert!(
            row_is_open(r),
            "the carried row is open: the read re-judges it"
        );
        // a second withheld pass keeps the FIRST judgement's stamp
        let again = vec![(r.clone(), judged_at + 5 * 60_000)];
        let mut rows2 = derived.clone();
        assert_eq!(
            keep_withheld_elsewhere(&i, &mut rows2, &again, &withheld),
            1
        );
        assert_eq!(rows2[0].facts["judgedAtMs"], judged_at);
        // past the bound the honest could-not-judge row returns
        let mut at_bound = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        at_bound.hop_chain = &chain;
        at_bound.spender_outputs = &no_bytes;
        at_bound.spender_inputs = &no_inputs;
        at_bound.my_pkh_by_game = &pkhs;
        at_bound.now_ms = judged_at + OWED_WITHHELD_KEEP_MAX_MS;
        let mut rows3 = derived.clone();
        assert_eq!(
            keep_withheld_elsewhere(&at_bound, &mut rows3, &previous, &withheld),
            1,
            "at the bound the judgement still stands"
        );
        at_bound.now_ms = judged_at + OWED_WITHHELD_KEEP_MAX_MS + 1;
        let mut rows3 = derived.clone();
        assert_eq!(
            keep_withheld_elsewhere(&at_bound, &mut rows3, &previous, &withheld),
            0,
            "a judgement older than OWED_WITHHELD_KEEP_MAX_MS is never kept"
        );
        assert_eq!(
            rows3, derived,
            "past the bound the could-not-judge row stands"
        );
        // the bytes in hand this pass: the derivation judges, nothing carried
        let mut i3 = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i3.hop_chain = &chain;
        i3.spender_outputs = &elsewhere;
        i3.spender_inputs = &spends_it;
        i3.my_pkh_by_game = &pkhs;
        let mut rows4 = derive_owed_rows(&i3);
        assert_eq!(
            keep_withheld_elsewhere(&i3, &mut rows4, &previous, &withheld),
            0
        );
        assert!(rows4[0].facts.get("carried").is_none());
    }

    /// bsv-low #672 (Pin 3): a spender the store lacks AND no prior judgement says could-not-judge HONESTLY. The belt
    /// carries nothing: a prior row that was itself could-not-judge (no `spendKind`) is not a positive judgement, an
    /// empty previous keeps nothing, and a FAULTED read (never withheld) keeps nothing.
    #[test]
    fn i672_b_no_prior_judgement_or_a_fault_stays_could_not_judge() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let competitor = tx(0x0d);
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "cc".repeat(20));
        let hops = [hop(
            HopStatus::Unknown,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let chain = chain_confirmed(&key, true, Some(true), Some(&competitor), Some(true));
        let no_bytes: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &chain;
        i.spender_outputs = &no_bytes;
        i.my_pkh_by_game = &pkhs;
        let derived = derive_owed_rows(&i);
        assert!(derived[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not judge the spender"));
        let judged_at = 1_800_000_000_000i64 - 10 * 60_000;
        let mut withheld: HashSet<String> = HashSet::new();
        withheld.insert(competitor.clone());
        // no previous at all
        let mut rows = derived.clone();
        assert_eq!(keep_withheld_elsewhere(&i, &mut rows, &[], &withheld), 0);
        assert_eq!(rows, derived, "no prior judgement: the honest word stands");
        // a previous row that is ITSELF could-not-judge (no spendKind, no spenderPkhs): not a positive judgement
        let prev_cnj = vec![(derived[0].clone(), judged_at)];
        let mut rows2 = derived.clone();
        assert_eq!(
            keep_withheld_elsewhere(&i, &mut rows2, &prev_cnj, &withheld),
            0
        );
        assert_eq!(rows2, derived);
        // a FAULTED read (the spender looked at, the answer bad): never withheld, the fault row stands
        let elsewhere = pays(&competitor, &[(0, &"99".repeat(20), 20_170, Some(false))]);
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(competitor.clone(), vec![(tx(0x07), 0)]);
        let mut ij = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        ij.hop_chain = &chain;
        ij.spender_outputs = &elsewhere;
        ij.spender_inputs = &spends_it;
        ij.my_pkh_by_game = &pkhs;
        let judged = derive_owed_rows(&ij);
        let previous = vec![(judged[0].clone(), judged_at)];
        let none: HashSet<String> = HashSet::new();
        let mut rows3 = derived.clone();
        assert_eq!(
            keep_withheld_elsewhere(&i, &mut rows3, &previous, &none),
            0,
            "a faulted read is never withheld; the fault row stands"
        );
    }

    /// bsv-low #672 (Pin 4): NEW evidence moves a carried word. A prior judged `spent-elsewhere` by competitor X is
    /// never carried onto a hop the chain now names spent by a DIFFERENT competitor Y (a second spend, a reorg): the
    /// carry is keyed on the hop UTXO PLUS the content-addressed spender txid, so a changed spender is new evidence
    /// the pass must judge from (or honestly could-not-judge).
    #[test]
    fn i672_c_a_different_spender_is_new_evidence_and_the_old_word_is_not_carried() {
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let (x, y) = (tx(0x0d), tx(0x0e));
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "cc".repeat(20));
        let elsewhere_x = pays(&x, &[(0, &"99".repeat(20), 20_170, Some(false))]);
        let hops = [hop(
            HopStatus::Unknown,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let key = format!("{}:0", tx(0x07));
        let mut spends_it: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        spends_it.insert(x.clone(), vec![(tx(0x07), 0)]);
        // the prior judged row names X
        let chain_x = chain_confirmed(&key, true, Some(true), Some(&x), Some(true));
        let mut ix = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        ix.hop_chain = &chain_x;
        ix.spender_outputs = &elsewhere_x;
        ix.spender_inputs = &spends_it;
        ix.my_pkh_by_game = &pkhs;
        let judged_x = derive_owed_rows(&ix);
        assert_eq!(judged_x[0].facts["chainSpender"], x);
        let previous_x = vec![(judged_x[0].clone(), 1_800_000_000_000i64 - 10 * 60_000)];
        // this pass: the chain names Y, Y withheld, no bytes
        let no_bytes: HashMap<String, Vec<SpenderOutput>> = HashMap::new();
        let chain_y = chain_confirmed(&key, true, Some(true), Some(&y), Some(true));
        let mut iy = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        iy.hop_chain = &chain_y;
        iy.spender_outputs = &no_bytes;
        iy.my_pkh_by_game = &pkhs;
        let derived_y = derive_owed_rows(&iy);
        let mut withheld_y: HashSet<String> = HashSet::new();
        withheld_y.insert(y.clone());
        let mut rows = derived_y.clone();
        assert_eq!(
            keep_withheld_elsewhere(&iy, &mut rows, &previous_x, &withheld_y),
            0
        );
        assert_eq!(
            rows, derived_y,
            "a different spender is new evidence; X's word is not carried onto Y"
        );
    }

    /// bsv-low #672 (Pin 2b): the store's bytes JUDGE, and a store hit is never withheld. A spender served from the
    /// durable store (`StoredSpenderBytes::to_outputs`) drives `derive_owed_rows` to the SAME `spent-elsewhere` word
    /// a fresh fetch gives, and with the spender in `spender_outputs` the pass's `withheld_hop_outpoints` is empty
    /// even when the spender is marked withheld — so the phases never ask a courier for it and the belt need not
    /// fire. The round-trip through the production SQL (and the bounds) is `owed_spender_store_sqlite.rs`.
    #[test]
    fn i672_store_bytes_judge_spent_elsewhere_and_a_store_hit_is_never_withheld() {
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
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
        assert_eq!(rows[0].reason.as_deref(), Some(SPENT_ELSEWHERE_REASON));
        // the spender is marked withheld this pass, but the store already filled its bytes: not withheld, no carry
        let mut withheld: HashSet<String> = HashSet::new();
        withheld.insert(competitor);
        assert!(
            withheld_hop_outpoints(&i, &rows, &withheld).is_empty(),
            "a store-filled spender is never withheld: the phases ask no courier for it"
        );
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
            derive_owed_rows(&i)
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
            derive_owed_rows(&i)
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

    /// The L25-WITHHELD lens's LOW-1: the withheld/looked ledger, per outcome. A cap break, the clock and the
    /// 16-read tail are WITHHELD; a stored read, a courier ask, a memo, a cached answer and a tracker row, faulted or
    /// not, are LOOKED, and a look wins over any withhold. A spender nothing was noted for is never withheld (a path
    /// that forgets its note keeps the honest could-not-judge row, never a stale payout over a fault).
    #[test]
    fn l25w_low1_the_spender_look_ledger_withholds_only_on_a_cap_the_clock_or_the_tail() {
        for why in [
            SpenderWithheld::Cap,
            SpenderWithheld::Clock,
            SpenderWithheld::Tail,
        ] {
            let mut look = SpenderLook::default();
            look.withhold("AB", why);
            assert_eq!(
                look.withheld(),
                HashSet::from(["ab".to_string()]),
                "{why:?} withholds"
            );
        }
        for how in [
            SpenderLooked::StoredRead,
            SpenderLooked::Memo,
            SpenderLooked::CachedAnswer,
            SpenderLooked::TrackerRow,
            SpenderLooked::CourierAsked,
            SpenderLooked::StoredDurable,
        ] {
            // looked first (phase 1), then a later cap names it (phase 2 never reaches it): looked
            let mut look = SpenderLook::default();
            look.look("ab", how);
            look.withhold("ab", SpenderWithheld::Cap);
            assert!(
                look.withheld().is_empty(),
                "{how:?} is a look, never withheld"
            );
            // the cap first, the look after (another identity's recompute bought it; order never matters)
            let mut look = SpenderLook::default();
            look.withhold("ab", SpenderWithheld::Clock);
            look.look("AB", how);
            assert!(
                look.withheld().is_empty(),
                "{how:?} is a look, never withheld"
            );
        }
        // named and never noted (a read path with no note): not withheld
        let look = SpenderLook::default();
        assert!(look.withheld().is_empty());
        // the route's shape: three wanted at a cap of two, the first two asked, the third withheld by the cap
        let wanted: Vec<String> = ["aa", "bb", "cc"].iter().map(|s| s.to_string()).collect();
        let mut look = SpenderLook::default();
        look.withhold_all(&wanted[3..], SpenderWithheld::Tail);
        for (at, sp) in wanted.iter().enumerate() {
            if at == OWED_SPENDER_COURIER_READS_PER_RECOMPUTE {
                look.withhold_all(&wanted[at..], SpenderWithheld::Cap);
                break;
            }
            look.look(sp, SpenderLooked::CourierAsked);
        }
        assert_eq!(look.withheld(), HashSet::from(["cc".to_string()]));
    }

    /// The L25-WITHHELD lens's MEDIUM-1 (2): the courier asks' rotation is DURABLE. Three named spenders (newest
    /// hop first: A, B, C) under the courier cap of two, two passes each on a COLD isolate (no isolate stamp): the
    /// first buys A and B and its write batch stamps them (`SPENDER_ASK_UPSERT_SQL`, real SQLite); the second reads
    /// the stamps back through the shipped probe memo read and buys C first. Before the fold the order was the
    /// isolate's alone and every cold pass bought A and B: C, the oldest hop, was withheld for ever. The stamp is
    /// spared by the overlay's reorg clear and expired by its TTL sweep (its lifecycle), and a reader of the memos
    /// never takes it for a probe of any outpoint.
    #[test]
    fn l25w_m1_two_cold_passes_over_three_named_spenders_buy_all_three_real_sqlite() {
        use bsv_overlay_cloudflare::hop_probe_memos::{
            HOP_PROBE_MEMO_EXPIRE_SQL, HOP_PROBE_MEMO_REORG_CLEAR_SQL,
        };
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        conn.execute_batch(
            bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS hop_chain_probes "))
                .unwrap(),
        )
        .unwrap();
        conn.execute_batch(bsv_overlay_cloudflare::d1::HOP_CHAIN_PROBES_TIP_ALTER)
            .unwrap();
        let (a, b, c) = (tx(0x0a), tx(0x0b), tx(0x0c));
        let wanted = vec![a.clone(), b.clone(), c.clone()];
        let cold = |_: &str| -> Option<i64> { None };
        let read_stamps = |conn: &rusqlite::Connection| -> HashMap<String, i64> {
            let targets: Vec<(String, u32)> =
                wanted.iter().map(|sp| spender_ask_target(sp)).collect();
            let keys = &crate::hops_view::probe_memo_read_chunks(&targets)[0];
            let mut stmt = conn
                .prepare(&crate::hops_view::probe_memo_read_sql(keys.len()))
                .unwrap();
            let memos: Vec<crate::hops_view::ProbeMemo> = stmt
                .query_map(rusqlite::params_from_iter(keys.iter()), |r| {
                    Ok(crate::hops_view::ProbeMemo {
                        outpoint: r.get(0)?,
                        probed_at_ms: r.get(1)?,
                        spent: r.get::<_, i64>(2)? != 0,
                        spending_txid: r.get(3)?,
                        spent_confirmed: r.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                        tip_height: None,
                    })
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert!(memos
                .iter()
                .all(|m| !m.spent && m.spent_confirmed.is_none()));
            spender_ask_stamps(&memos)
        };
        let stamp = |conn: &rusqlite::Connection, sp: &str, at: i64| {
            let (txid, vout) = spender_ask_target(sp);
            conn.execute(
                SPENDER_ASK_UPSERT_SQL,
                rusqlite::params![crate::hops_view::probe_memo_key(&txid, vout), at],
            )
            .unwrap();
        };
        let mut bought: HashSet<String> = HashSet::new();
        let t1 = 1_800_000_000_000i64;
        // pass 1, cold: no stamp anywhere
        let order = spender_ask_order(wanted.clone(), &read_stamps(&conn), cold);
        assert_eq!(order, wanted, "never asked: newest hop first");
        for sp in order.iter().take(OWED_SPENDER_COURIER_READS_PER_RECOMPUTE) {
            bought.insert(sp.clone());
            stamp(&conn, sp, t1);
        }
        // pass 2, five minutes later, ANOTHER cold isolate
        let t2 = t1 + 5 * 60_000;
        let order = spender_ask_order(wanted.clone(), &read_stamps(&conn), cold);
        assert_eq!(
            order[0], c,
            "the never-asked spender goes first on a cold isolate: {order:?}"
        );
        for sp in order.iter().take(OWED_SPENDER_COURIER_READS_PER_RECOMPUTE) {
            bought.insert(sp.clone());
            stamp(&conn, sp, t2);
        }
        assert_eq!(
            bought,
            HashSet::from([a.clone(), b.clone(), c.clone()]),
            "two cold passes buy all three named spenders"
        );
        // the stamp only grows (a late batch never moves it back)
        stamp(&conn, &c, t1);
        assert_eq!(read_stamps(&conn)[&c], t2);
        // the isolate's own stamp is merged: a warm isolate that asked A later than the durable stamp says
        let warm = |sp: &str| -> Option<i64> { (sp == a).then_some(t2 + 1) };
        let order = spender_ask_order(wanted.clone(), &read_stamps(&conn), warm);
        assert_eq!(order.last(), Some(&a));
        // the overlay's reorg clear spares the stamp (it is no confirmed memo); its TTL sweep expires it
        conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, []).unwrap();
        assert_eq!(read_stamps(&conn).len(), 3);
        conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![t1])
            .unwrap();
        let left = read_stamps(&conn);
        assert_eq!(
            left.len(),
            2,
            "the stamps of t1 expired, ranked never-asked again: {left:?}"
        );
        // the key never reads as a probe memo of an outpoint (the memo key is `<txid>.<vout>`)
        assert!(spender_ask_target(&a).0.starts_with(SPENDER_ASK_PREFIX));
        assert!(!crate::hops_view::probe_memo_key(&a, 0).starts_with(SPENDER_ASK_PREFIX));
    }

    /// The L25-WITHHELD lens's LOW-1, LOW-3 and MEDIUM-1 (2), the route's wiring (the house source-shape pin over
    /// the squashed route): each withhold is noted at its break (the cap, the clock in both phases, the tail), the
    /// withheld set is the ledger's, the courier order is the durable one (no isolate-only sort), each real ask is
    /// stamped in the SAME write batch, and the previous-row read's fault is counted and served on `/health`.
    #[test]
    fn l25w_the_route_notes_each_withhold_orders_by_the_durable_stamp_and_counts_the_read_fault() {
        let squash = |s: &str| {
            s.split_whitespace()
                .collect::<String>()
                .replace(",)", ")")
                .replace(",]", "]")
        };
        let code: String = include_str!("routes.rs")
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let routes = squash(&code);
        for shape in [
            "spender_look.withhold_all(&tail, crate::owed::SpenderWithheld::Tail);",
            "budget_cut = true; spender_look.withhold_all(&ordered[at..], crate::owed::SpenderWithheld::Clock); break;",
            "courier_cap_hit = true; spender_look.withhold_all(&wanted[at..], crate::owed::SpenderWithheld::Cap); break;",
            "budget_cut = true; spender_look.withhold_all(&wanted[at..], crate::owed::SpenderWithheld::Clock); break;",
            "let spenders_withheld = spender_look.withheld();",
            "let wanted = crate::owed::spender_ask_order(wanted, &durable_asks,",
            "owed_courier_note_asked(&sp, now_f); spenders_asked.push(sp.clone());",
            ".prepare(crate::owed::SPENDER_ASK_UPSERT_SQL)",
            "stmts.insert(stmts.len() - 1, ask_stmt);",
            "Err(e) => { crate::owed::note_withheld_previous_read_fault(); console_warn!(",
        ] {
            assert!(routes.contains(&squash(shape)), "the route carries: {shape}");
        }
        assert!(
            !routes.contains(&squash("wanted.sort_by_key(")),
            "the courier order is never the isolate's alone"
        );
        let stamped = routes
            .find(&squash("stmts.insert(stmts.len() - 1, ask_stmt);"))
            .unwrap();
        let written = routes
            .find(&squash(
                "let written = crate::d1_ledger::counted_batch(db, stmts)",
            ))
            .unwrap();
        assert!(
            stamped < written,
            "the ask stamps ride the snapshot's one batch"
        );
        assert!(owed_health_json()["withheldPreviousReadFaults"].is_u64());
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let i2 = inputs(
            &results2,
            &refunds,
            &hops,
            &valid,
            &none,
            &pots,
            Some(900_200),
        );
        let rows2 = derive_owed_rows(&i2);
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
        assert_eq!(
            word_arm_subjects([
                Some(tx(2).to_uppercase().as_str()),
                None,
                Some("zz"),
                Some(tx(1).as_str()),
                Some(tx(2).as_str())
            ]),
            vec![tx(1), tx(2)]
        );
    }

    /// bsv-low #618: the owed recompute applies the word arm to every hydrated row BEFORE it assembles the three
    /// views, and the later word read never re-reads the arm's subjects.
    #[test]
    fn l2_owed_recompute_assembles_after_the_word_arm() {
        let routes = include_str!("routes.rs");
        let body = &routes[routes.find("async fn owed_recompute_inner(").unwrap()
            ..routes
                .find("let mut rows = derive_owed_rows(&inputs);")
                .unwrap()];
        let arm = body
            .find("crate::owed::apply_word_arm(")
            .expect("the word arm is applied");
        for assembler in [
            "gather_result_rows(",
            "crate::refund_view::assemble_refund_view(",
            "crate::hops_view::assemble_hops_view(",
        ] {
            let at = body
                .find(assembler)
                .unwrap_or_else(|| panic!("{assembler} missing"));
            assert!(arm < at, "{assembler} runs before the word arm");
        }
        assert!(
            body.contains("arm_ids.binary_search(id).is_err()"),
            "the later read re-reads the arm's subjects"
        );
    }

    /// The W2-WORDS lens's LOW-2 (mutant M4): the route applies the word arm to the PARTY rows (results and
    /// refunds) and to the HOP rows, each by its own call, both before any assembler; a pin that found either call
    /// was satisfied by the hop rows' alone.
    #[test]
    fn l2_fold_the_route_applies_the_arm_to_party_and_hop_rows() {
        let routes = include_str!("routes.rs");
        let body = &routes[routes.find("async fn owed_recompute_inner(").unwrap()
            ..routes
                .find("let mut rows = derive_owed_rows(&inputs);")
                .unwrap()];
        let first_assembler = body.find("gather_result_rows(").unwrap();
        for call in [
            "crate::owed::apply_word_arm(result_rows, refund_rows, &mut [], views);",
            "crate::owed::apply_word_arm(&mut [], &mut [], rows, views);",
        ] {
            let at = body
                .find(call)
                .unwrap_or_else(|| panic!("the route does not call `{call}`"));
            assert!(at < first_assembler, "`{call}` runs after an assembler");
        }
        let party = body
            .find("for (result_rows, refund_rows) in &mut party_rows {")
            .unwrap();
        let hops = body.find("for rows in &mut hop_chunks {").unwrap();
        assert!(party < first_assembler && hops < first_assembler);
    }

    /// The W2-WORDS lens's LOW-3 (mutant M7): a word-store fault (`read_soft` answering `None`) keeps the latch's
    /// pair (NS-3: a fault serves the legacy answer). The arm's views are `None` on a fault and the arm runs only on
    /// `Some`; an empty map in their place would withhold every row's third arm, which this pins is not neutral.
    #[test]
    fn l2_fold_a_word_store_fault_keeps_the_legacy_pair() {
        let routes = include_str!("routes.rs");
        let body = &routes[routes
            .find("let arm_rows = crate::tracker_host::runtime::read_soft(")
            .unwrap()..];
        let body = &body[..body.find("let mut results = Vec::new();").unwrap()];
        let compact: String = body.split_whitespace().collect();
        assert!(
            compact.contains(
                "letarm_views=match&arm_rows{Some(rows)=>Some(crate::tracker_host::runtime::views(env,rows).await),None=>None,};"
            ),
            "a word-store fault must leave the arm's views None"
        );
        assert!(
            compact.contains("ifletSome(views)=&arm_views{"),
            "the arm must run only on views the store answered"
        );
        for empty in ["Default::default()", "BTreeMap::new()", "unwrap_or_default"] {
            assert!(
                !body.contains(empty),
                "the fault path builds an empty map ({empty})"
            );
        }
        // why an empty map is not the legacy answer: it withholds a latched, final spend
        let words = std::collections::BTreeMap::new();
        let t = "ab".repeat(32);
        assert_eq!(
            word_arm(Some(&t), Some(true), &words),
            (Some(false), Some(true))
        );
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
        let rows = derive_owed_rows(&inputs(&e, &[], &[], &v, &c, &p, Some(900_200)));
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
        assert!(derive_owed_rows(&inputs(&lost, &[], &[], &v, &c, &p, Some(900_200))).is_empty());
        let won = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        // the identity's OWN collected filing (signature verified under it): retired
        let verified: HashSet<String> = [tx(0x01)].into_iter().collect();
        assert!(
            derive_owed_rows(&inputs(&won, &[], &[], &v, &verified, &p, Some(900_200))).is_empty()
        );
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
        let mut e = entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        );
        e.spent_confirmed = Some(false);
        e.at_height = None; // unconfirmed: no verified mined height (the review's LOW-1 bar)
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(&no_verdict, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert!(rows[0].reason.as_deref().unwrap().contains("no verdict"));
        let no_seat = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Unresolved,
            None,
        )];
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
        let held = derive_owed_rows(&inputs(&e, &r, &[], &valid, &c, &p, Some(900_005)));
        assert_eq!(held[0].family, OwedFamily::RefundDue);
        assert_eq!(held[0].facts["claim"], "present-refund");
        assert_eq!(held[0].facts["claimable"], true);
        assert_eq!(held[0].reason, None);
        // the funding held by nothing: no press, a named reason
        let unheld: HashSet<String> = [tx(0x02)].into_iter().collect();
        let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
        i.funding_unheld = &unheld;
        let rows = derive_owed_rows(&i);
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
        assert_eq!(derive_owed_rows(&i)[0].family, OwedFamily::Unbound);
        // the tracker's CHECKED mined word for the funding: the network holds it, today's row byte for byte
        let mut checked = std::collections::BTreeMap::new();
        checked.insert(tx(0x02), json!({"word": "mined", "verifyPath": "header"}));
        i.spender_words = Some(&checked);
        let mined = derive_owed_rows(&i);
        let today = derive_owed_rows(&{
            let mut j = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
            j.spender_words = Some(&checked);
            j
        });
        assert_eq!(mined, today);
        assert_eq!(mined[0].facts["claim"], "present-refund");
        // the gate not open yet: the rejoin row is unchanged by the funding word (not a refund claim)
        let mut k = inputs(&e, &r, &[], &valid, &c, &p, Some(899_990));
        k.funding_unheld = &unheld;
        assert_eq!(derive_owed_rows(&k)[0].facts["claim"], "rejoin");
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
            derive_owed_rows(&i)
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

    /// bsv-low #831 (the FIX-831 lens's L3): `owed_refund_funding_unheld_total` moves by exactly ONE for fix831_a's
    /// pot judged funding-not-held, and by ZERO for the same pot with its funding held, through the shipped count and
    /// the census's pending deltas (`courier::take_pending_if_due`, this test thread's own map).
    #[test]
    fn fix831_l3_the_funding_unheld_counter_moves_once_per_withheld_pot() {
        let drained = || -> HashMap<String, u64> {
            crate::courier::take_pending_if_due(1_000_000.0)
                .into_iter()
                .collect()
        };
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
        drained();
        // held: no move
        let held = derive_owed_rows(&inputs(&e, &r, &[], &valid, &c, &p, Some(900_005)));
        assert_eq!(count_refund_funding_unheld(&held), 0);
        note_refund_funding_unheld(count_refund_funding_unheld(&held));
        assert_eq!(
            drained().get(COUNTER_REFUND_FUNDING_UNHELD),
            None,
            "a held funding moves nothing"
        );
        // unheld: exactly one
        let unheld: HashSet<String> = [tx(0x02)].into_iter().collect();
        let mut i = inputs(&e, &r, &[], &valid, &c, &p, Some(900_005));
        i.funding_unheld = &unheld;
        let rows = derive_owed_rows(&i);
        assert_eq!(count_refund_funding_unheld(&rows), 1);
        note_refund_funding_unheld(count_refund_funding_unheld(&rows));
        assert_eq!(
            drained().get(COUNTER_REFUND_FUNDING_UNHELD),
            Some(&1),
            "one pot judged funding-not-held is one increment"
        );
        assert!(owed_mirrored_counter_names()
            .iter()
            .any(|n| n == COUNTER_REFUND_FUNDING_UNHELD));
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
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &valid, &c, &p, Some(900_005)));
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
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &zero, &c, &p, Some(900_005)));
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::RefundDue, None)
        );
        assert!(rows[0].facts["refundSizing"]
            .as_str()
            .unwrap()
            .starts_with("zero"));
        // the gate NOT open yet: the hand's window, a rejoin row sized by my stake
        let rows = derive_owed_rows(&inputs(&e, &r, &[], &valid, &c, &p, Some(899_990)));
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
        let rows_ev = derive_owed_rows(&i);
        assert!(
            rows_ev
                .iter()
                .all(|row| row.family != OwedFamily::RefundDue
                    && row.family != OwedFamily::InProgress),
            "{rows_ev:?}"
        );
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
        assert!(
            rows[0].at_height.is_none(),
            "an unspent in-progress pot has no mined height"
        );
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&i);
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::HopStranded, Some(20_190))
        );
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["chainProbe"], "unspent");
        // a YOUNG unspent hop is an `in-progress` row with the felt's rejoin (the stake is funded, the hand has not
        // started), sized by the hop, never a sweep claim before the window
        let young = [hop(HopStatus::Unspent, None, Some(60_000))];
        let rows = derive_owed_rows(&inputs(&[], &[], &young, &v, &c, &p, Some(900_000)));
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
        assert!(
            derive_owed_rows(&inputs(&[], &[], &ageless, &v, &c, &p, Some(900_000))).is_empty()
        );
        let mut unverified = hop(HopStatus::Unspent, None, Some(60_000));
        unverified.marker_verified = MarkerVerification::Unknown;
        assert!(
            derive_owed_rows(&inputs(&[], &[], &[unverified], &v, &c, &p, Some(900_000)))
                .is_empty()
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
            derive_owed_rows(&inputs(&[], &[], &by_pot, &v, &c, &pots, Some(900_000))).is_empty()
        );
        // MEDIUM-7: a spender absent from the index is NOT evidence of a wallet spend (an unindexed JOIN is the common
        // honest case): a sentence, no claim, never "spent elsewhere"
        let elsewhere = [hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000))];
        let rows = derive_owed_rows(&inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000)));
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
        let rows = derive_owed_rows(&i);
        assert!(rows[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not check"));
    }

    /// bsv-low #608 (the E606 lens's deferred (a)): a "could not judge the spender" row whose story rests on NO spender
    /// bytes is re-judged on the NEXT read once a minute has passed (#606 made the bytes readable between passes), not
    /// only when the open list ages out at 5 minutes; a row whose bytes WERE read (a positive story) keeps the 5-minute
    /// rule, and inside the minute nothing recomputes (a dead courier never makes every read recompute). Written over
    /// the shipped API alone, so it runs on 45b83a32: RED there at "a could-not-judge row with no bytes is re-judged
    /// on the next read past a minute".
    #[test]
    fn fix608_a_could_not_judge_row_resting_on_no_spender_bytes_is_rejudged_on_the_next_read() {
        let (v, c) = (HashMap::new(), HashSet::new());
        let pots: HashSet<String> = HashSet::new();
        let elsewhere = [hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000))];
        let unread = derive_owed_rows(&inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000)));
        assert_eq!(unread.len(), 1);
        assert!(unread[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("could not judge"));
        let at = |age_ms: i64, rows: &[OwedRow]| {
            should_recompute(false, age_ms, Some(900_000), Some(900_000), rows)
        };
        assert!(
            !at(59_000, &unread),
            "inside the minute a read serves the snapshot (the bound)"
        );
        assert!(
            at(61_000, &unread),
            "a could-not-judge row with no bytes is re-judged on the next read past a minute"
        );
        // a faulted pot-spenders read is the same mark
        let mut i = inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000));
        i.pot_spenders_faulted = true;
        assert!(
            at(61_000, &derive_owed_rows(&i)),
            "a faulted read is re-judged too"
        );
        // the spender's BYTES read (they pay no home of mine): a positive story, the open list's 5-minute rule alone
        let outs = pays(&tx(0x09), &[(0, "ab", 1_000, None)]);
        let mut j = inputs(&[], &[], &elsewhere, &v, &c, &pots, Some(900_000));
        j.spender_outputs = &outs;
        let judged = derive_owed_rows(&j);
        assert!(
            !judged[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("could not judge"),
            "the bytes tell a story: {:?}",
            judged[0].reason
        );
        assert!(
            !at(61_000, &judged),
            "a story with its bytes waits for the 5-minute rule"
        );
        assert!(at(6 * 60_000, &judged));
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
        let rows = derive_owed_rows(&inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000)));
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        // the providers could not look (a fault): still no competitor known -> still claimable on the index
        let not_looked = chain(&key, false, None, None);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &not_looked;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        // the chain's UNSPENT word corroborates: the claim, with the courier-corroborated provenance
        let looked_unspent = chain(&key, true, Some(false), None);
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_chain = &looked_unspent;
        let rows = derive_owed_rows(&i);
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
        assert!(derive_owed_rows(&i).iter().all(|r| r.outpoint != key));
        // bsv-low #837 (amended): the results carrying no row for that pot, the game keeps one row, never a press
        let mut i = inputs(&[], &[], &old, &v, &c, &pots, Some(900_000));
        i.hop_chain = &by_pot;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].facts["joinSpentBy"], "chain");
        assert_eq!(rows[0].facts["claimable"], false);
        // spent by something the index does not hold (a KNOWN competitor): could not judge, the spender named
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(
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
            assert_eq!(derive_owed_rows(&i)[0].facts["claimable"], true);
        }
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        for (words, claimable) in [(&none, true), (&unchecked, false)] {
            let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
            i.hop_sweeps = &sweeps;
            i.hop_chain = &by_chain;
            i.spender_words = Some(words);
            assert_eq!(derive_owed_rows(&i)[0].facts["claimable"], claimable);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
        assert_eq!(rows[0].facts["claimable"], true);
        // the CHAIN rung naming my sweep on an index-unspent hop is the same payout (a sweep broadcast direct to ARC)
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        i.hop_chain = &by_chain;
        let rows = derive_owed_rows(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["claimable"].as_bool()),
            (OwedFamily::Payout, Some(true))
        );
        // a spender that is NOT my filing (another tx) stays could-not-judge, as before
        let other = [hop(HopStatus::Spent, Some(&tx(0x09)), Some(10_000_000))];
        let mut i = inputs(&[], &[], &other, &v, &c, &no_pots, Some(900_000));
        i.hop_sweeps = &sweeps;
        assert_eq!(derive_owed_rows(&i)[0].family, OwedFamily::Unbound);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
            derive_owed_rows(&i)[0].facts["confirmedSource"],
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        assert_eq!(
            (rows[0].family, rows[0].facts["claim"].as_str()),
            (OwedFamily::HopStranded, Some("sweep-hop"))
        );
        // …and without a chain word it is the sentence naming the missing index row
        let mut unknown = hop(HopStatus::Unknown, None, Some(HOP_STRANDED_AFTER_MS + 1));
        unknown.spent = None;
        let hops = [unknown];
        let rows = derive_owed_rows(&inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000)));
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
        let rows = derive_owed_rows(&inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000)));
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
        let rows = derive_owed_rows(&i);
        assert_eq!(
            (rows[0].family, rows[0].sats),
            (OwedFamily::Payout, Some(20_000))
        );
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
        let rows = derive_owed_rows(&i);
        assert_eq!(
            (rows[0].family, rows[0].facts["claimable"].as_bool()),
            (OwedFamily::Payout, Some(true))
        );
        let mut i = inputs(&[], &[], &hops, &v, &c, &no_pots, Some(900_000));
        i.spender_outputs = &elsewhere;
        i.my_pkh_by_game = &pkhs;
        assert_eq!(
            derive_owed_rows(&i)[0].facts["spendKind"],
            "spent-elsewhere"
        );
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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

    /// The delta-verify's HIGH-A (2026-09-19): the refusal's candidate set must come from the eviction LEDGER —
    /// under the overlay's real eviction SQL the party row keyed by the pot leaves `potparty_records` (the results
    /// view has no entry to derive a candidate from), while the ledger's window still names the released hop; the
    /// derivation then strands the identity's young hop. Real SQLite, the shipped migrations, the overlay's own
    /// move statements (a dev-dependency), the app layer's own query.
    #[test]
    fn a_refused_join_is_found_through_the_eviction_ledger_after_the_overlay_moved_the_party_row_real_sqlite(
    ) {
        use bsv_overlay_cloudflare::admit_fast::{
            create_shadow_sql, move_sql, ColumnInfo, MOVED_TABLES,
        };
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
        let me = ME.to_string();
        let pot = tx(0x02);
        let hop_txid = tx(0x07);
        conn.execute(
            "INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt) \
             VALUES (?1, 'opp', ?2, ?3, 0, 100, 'sig', ?4, 0, 1000)",
            rusqlite::params![me, tx(0x01), pot, tx(0x0a)],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 900)",
            rusqlite::params![pot],
        )
        .unwrap();
        // the hop's own row, its spend pointer RELEASED by the eviction (the overlay's release SQL is its own pin)
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 800)",
            rusqlite::params![hop_txid],
        )
        .unwrap();
        let evicted_at = 1_700_000_000_000i64;
        conn.execute(
            "INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES (?1, 'REJECTED (corroborated)', ?2, NULL, 2, ?3)",
            rusqlite::params![pot, evicted_at, format!("[{{\"table\":\"pot_records\",\"txid\":\"{hop_txid}\",\"vout\":0}}]")],
        )
        .unwrap();
        let cols = |conn: &rusqlite::Connection, t: &str| -> Vec<ColumnInfo> {
            let mut st = conn
                .prepare(&format!("PRAGMA table_info(\"{t}\")"))
                .unwrap();
            st.query_map([], |r| {
                Ok(ColumnInfo {
                    name: r.get(1)?,
                    ty: r.get::<_, String>(2).unwrap_or_default(),
                    notnull: r.get::<_, i64>(3).unwrap_or(0),
                    dflt_value: r.get::<_, Option<String>>(4).unwrap_or(None),
                })
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
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
                conn.execute(
                    &ins,
                    rusqlite::params![evicted_at, "REJECTED (corroborated)", &pot],
                )
                .unwrap();
                conn.execute(&del, [&pot]).unwrap();
            }
        }
        let party_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM potparty_records WHERE identity = ?1",
                [&me],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            party_rows, 0,
            "the results view has NO entry to derive a candidate from after the eviction"
        );
        // the app layer's own query on the ledger still names the released hop
        let mut st = conn.prepare(OWED_EVICTIONS_WINDOW_SQL).unwrap();
        let rows: Vec<(String, Option<String>)> = st
            .query_map([evicted_at - OWED_EVICTION_WINDOW_MS + 1], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
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
        chain.insert(
            key,
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
        let payout = rows
            .iter()
            .find(|r| r.family == OwedFamily::Payout)
            .expect("the sweep's payout row");
        assert_eq!(
            payout.facts["claimable"], false,
            "a different spender's confirmation is not the sweep's"
        );
        // the chain naming the sweep, confirmed → claimable
        let by_sweep = chain_confirmed(&key, true, Some(true), Some(&sweep), Some(true));
        i.hop_chain = &by_sweep;
        let rows = derive_owed_rows(&i);
        let payout = rows
            .iter()
            .find(|r| r.family == OwedFamily::Payout)
            .expect("the sweep's payout row");
        assert_eq!(payout.facts["claimable"], true);
    }

    #[test]
    fn a_coalesced_ask_remembers_the_latest_source_and_the_attribution_sql_names_the_marker_tables()
    {
        let mut m = HashMap::new();
        owed_rerun_note(&mut m, "ab", "pot-changed");
        owed_rerun_note(&mut m, "ab", "hop-changed");
        assert_eq!(m.get("ab").map(String::as_str), Some("hop-changed"));
        assert_eq!(m.len(), 1);
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
        assert!(
            derive_owed_rows(&i).is_empty(),
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
        assert_eq!(derive_owed_rows(&i).len(), 2);
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
        let rows = derive_owed_rows(&inputs(&won, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["payTxid"], tx(0x03));
        assert_eq!(rows[0].facts["heldFiling"], HELD_FILING_TAG);
        // the wallet holds the settle (the live credit landed it): not a row
        let held: HashSet<String> = [held_key(&tx(0x01), &tx(0x03))].into_iter().collect();
        let mut i = inputs(&won, &[], &[], &v, &c, &p, Some(900_200));
        i.held_verified = &held;
        assert!(
            derive_owed_rows(&i).is_empty(),
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
            let rows = derive_owed_rows(&i);
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
            derive_owed_rows(&i)
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
        assert!(derive_owed_rows(&i).is_empty());
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

    /// The recompute's read (`collected_rows_sql`, one game per call here) and fold of `me`'s rows, as the route
    /// runs them: the statement, the four columns, `CollectedFold::row`.
    fn fold_collected(conn: &rusqlite::Connection, me: &str, games: &[String]) -> CollectedFold {
        let mut fold = CollectedFold::default();
        let mut stmt = conn.prepare(&collected_rows_sql("?")).unwrap();
        for game in games {
            let rows = stmt
                .query_map(rusqlite::params![me, game], |r| {
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
            derive_owed_rows(&i)
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
            derive_owed_rows(&i)
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
        let derived = derive_owed_rows(&inputs(&paid, &[], &[], &v, &c, &p, Some(900_200)));
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
            self.asked
                .push(txids.iter().map(|t| t.to_string()).collect());
            if self.fault {
                return Err("d1 down".into());
            }
            Ok(txids
                .iter()
                .filter_map(|t| Some((t.to_string(), self.raws.get(*t)?.clone())))
                .collect())
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
        let run =
            |world: &mut PayVoutTable, wanted: &[(String, String)], chunk: usize, turn: u64| {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(pay_vout_pass(world, wanted, chunk, turn))
            };
        let home = [0x11u8; 20];
        let home_hex = hex::encode(home);
        // seven paying transactions, each paying the home at vout 1 behind a distinct first output
        let mut raws = HashMap::new();
        let mut wanted: Vec<(String, String)> = Vec::new();
        for n in 0..7u64 {
            let mut t = Transaction::new();
            for (sats, pkh) in [(100 + n, [0x33u8; 20]), (39_000, home)] {
                t.outputs.push(TransactionOutput {
                    satoshis: Some(sats),
                    locking_script: LockingScript::from_binary(
                        &overlay_discovery::pot::p2pkh_lock(&pkh),
                    )
                    .unwrap(),
                    change: false,
                });
            }
            raws.insert(t.id(), t.to_binary());
            wanted.push((t.id(), home_hex.clone()));
        }
        wanted.sort_unstable();
        let table = |slow: bool| PayVoutTable {
            raws: raws.clone(),
            memo: HashMap::new(),
            slow,
            fault: false,
            asked: Vec::new(),
        };

        // the step's clock spent at once (the heaviest case): the first chunk is read all the same
        let mut world = table(true);
        let one = run(&mut world, &wanted, 3, 0);
        assert_eq!(
            (one.vouts.len(), one.cut, one.reads),
            (3, true, 1),
            "pass one: one chunk whatever the clock says, the rest cut"
        );
        assert!(one.vouts.values().all(|v| *v == 1));
        // WARM isolate, the clock spent on every pass, the SAME turn: the memo carries, the next chunk is the first
        let two = run(&mut world, &wanted, 3, 0);
        assert_eq!(
            (two.vouts.len(), two.cut, two.reads),
            (6, true, 1),
            "pass two serves what pass one left"
        );
        assert!(wanted[3..6]
            .iter()
            .all(|(t, p)| two.vouts.contains_key(&pay_vout_key(t, p))));
        let three = run(&mut world, &wanted, 3, 0);
        assert_eq!((three.vouts.len(), three.cut, three.reads), (7, false, 1));
        let four = run(&mut world, &wanted, 3, 0);
        assert_eq!(
            (four.vouts.len(), four.cut, four.reads),
            (7, false, 0),
            "a warm isolate that holds every answer reads nothing"
        );
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
        assert_eq!(
            served.len(),
            7,
            "every row was served on one of the three passes"
        );

        // inside its own clock the step reads every chunk in one pass; a fault is counted and names nothing
        let mut quick = table(false);
        let all = run(&mut quick, &wanted, 3, 5);
        assert_eq!((all.vouts.len(), all.cut, all.reads), (7, false, 3));
        let mut down = table(false);
        down.fault = true;
        let before = PAY_VOUT_READ_FAULTS.load(Ordering::Relaxed);
        let none = run(&mut down, &wanted, 3, 0);
        assert!(
            none.vouts.is_empty() && !none.cut && down.memo.is_empty(),
            "a fault answers nothing and memoises nothing"
        );
        assert!(PAY_VOUT_READ_FAULTS.load(Ordering::Relaxed) >= before + 3);
        // never wrong: bytes that do not hash to the asked txid name nothing
        let mut lying = table(false);
        let swapped = raws[&wanted[1].0].clone();
        lying.raws.insert(wanted[0].0.clone(), swapped);
        let pass = run(&mut lying, &wanted[..1], 3, 0);
        assert!(pass.vouts.is_empty() && lying.memo.is_empty());
        assert!(
            run(&mut table(true), &[], 3, 9).vouts.is_empty(),
            "nothing wanted: no read, no divide by zero"
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
        let mut rows = derive_owed_rows(&inputs(&paid, &[], &[], &v, &c, &p, Some(900_200)));
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
        let rows_b = derive_owed_rows(&inputs(&tied, &[], &[], &v, &c, &p, Some(900_200)));
        assert_eq!(rows_b[0].facts["payPkh"], hex::encode(other));
        let mut keyless = won.clone();
        keyless.committed_keys = None;
        let keyless = [keyless];
        let rows_k = derive_owed_rows(&inputs(&keyless, &[], &[], &v, &c, &p, Some(900_200)));
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
        let rows_s = derive_owed_rows(&i);
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
        let rows_n = derive_owed_rows(&i);
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
            let mut rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&inputs(&both, &[], &[], &v, &verified, &p, Some(900_200)));
        assert_eq!(
            rows.len(),
            2,
            "both payout rows stay; the marker cannot say which pot it meant"
        );
        assert!(rows.iter().all(|r| r.facts["collectedSigVerified"] == true
            && r.facts["collectedMarkerPresent"] == true));
    }

    #[test]
    fn the_read_rule_recomputes_on_stale_on_the_gate_flip_on_an_aged_open_list_and_on_any_old_list()
    {
        // N5: the four arms of `should_recompute`, one predicate
        let e = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let in_progress = derive_owed_rows(&inputs(&e, &[], &[], &v, &c, &p, Some(899_990)));
        assert_eq!(in_progress[0].family, OwedFamily::InProgress);
        let paid = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let payout = derive_owed_rows(&inputs(&paid, &[], &[], &v, &c, &p, Some(900_200)));
        // stale wins whatever else
        assert!(should_recompute(true, 0, Some(1), Some(1), &[]));
        // the gate flip: the tip reached the in-progress row's recovery height since the compute
        assert!(should_recompute(
            false,
            0,
            Some(899_990),
            Some(900_000),
            &in_progress
        ));
        assert!(!should_recompute(
            false,
            0,
            Some(899_990),
            Some(899_995),
            &in_progress
        ));
        assert!(
            !should_recompute(false, 0, Some(900_000), Some(900_000), &in_progress),
            "no tip advance, no flip"
        );
        assert!(
            should_recompute(false, 0, None, Some(900_000), &in_progress),
            "a compute without a tip heals on the next tipped read"
        );
        // an OPEN list ages out at 5 minutes; a claimable-payout-only list at 15; an empty list at 15
        assert!(!should_recompute(
            false,
            4 * 60_000,
            Some(1),
            Some(1),
            &in_progress
        ));
        assert!(should_recompute(
            false,
            6 * 60_000,
            Some(1),
            Some(1),
            &in_progress
        ));
        assert!(!should_recompute(
            false,
            6 * 60_000,
            Some(1),
            Some(1),
            &payout
        ));
        assert!(should_recompute(
            false,
            16 * 60_000,
            Some(1),
            Some(1),
            &payout
        ));
        assert!(should_recompute(false, 16 * 60_000, Some(1), Some(1), &[]));
        assert!(!should_recompute(false, 14 * 60_000, Some(1), Some(1), &[]));
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(
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
        let rows = derive_owed_rows(&inputs(&e, &[], &[], &v, &c, &p, Some(900_200)));
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
    fn the_collected_lookup_covers_every_hop_game_beside_the_spent_pots() {
        let games = collected_lookup_games(
            ["AA".repeat(32).as_str(), &"bb".repeat(32)].into_iter(),
            [&"cc".repeat(32)[..], &"bb".repeat(32)].into_iter(),
        );
        assert_eq!(
            games,
            vec!["aa".repeat(32), "bb".repeat(32), "cc".repeat(32)]
        );
        assert!(collected_lookup_games(std::iter::empty(), std::iter::empty()).is_empty());
    }

    #[test]
    fn a_filed_sweep_the_index_names_but_never_confirmed_is_claimable_once_the_chain_rung_confirms_it(
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
    fn run_owed_write(
        conn: &mut rusqlite::Connection,
        plan: &[(&'static str, Vec<OwedBind>)],
    ) -> Option<i64> {
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
                stamp = txn
                    .query_row(sql, rusqlite::params_from_iter(vals), |r| r.get(0))
                    .ok();
            } else {
                txn.execute(sql, rusqlite::params_from_iter(vals))
                    .unwrap_or_else(|e| panic!("statement {n} ({sql}): {e}"));
            }
        }
        txn.commit().expect("commit");
        stamp
    }
    fn served(conn: &rusqlite::Connection, identity: &str) -> (Vec<(String, String)>, i64, i64) {
        let mut st = conn
            .prepare("SELECT outpoint, family FROM owed_rows WHERE identity = ?1 ORDER BY outpoint")
            .unwrap();
        let rows: Vec<(String, String)> = st
            .query_map([identity], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let (stamp, stale): (i64, i64) = conn
            .query_row(
                "SELECT computedAtMs, stale FROM owed_state WHERE identity = ?1",
                [identity],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
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
        conn.execute_batch(OWED_STATE_SEQ_ALTER).unwrap();
        conn.execute_batch(
            bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS hop_chain_probes "))
                .unwrap(),
        )
        .unwrap();
        conn.execute_batch(bsv_overlay_cloudflare::d1::HOP_CHAIN_PROBES_TIP_ALTER)
            .unwrap();
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
        let two = [
            row(&format!("{}:0", tx(0x07)), OwedFamily::Payout),
            row(&format!("{}:1", tx(0x08)), OwedFamily::InProgress),
        ];
        // TWO lands first
        let stamp = run_owed_write(
            &mut conn,
            &owed_write_plan(ME, &two, two_at, Some(900_001), false),
        );
        assert!(owed_write_landed(stamp, two_at));
        let newer = served(&conn, ME);
        assert_eq!(newer.0.len(), 2);
        // a filing marks the newer snapshot stale before ONE lands: a refused write must not clear the mark
        conn.execute(OWED_STALE_FOR_IDENTITY_SQL, [ME]).unwrap();
        // ONE lands second, with its older snapshot: REFUSED, nothing moves
        let stamp = run_owed_write(
            &mut conn,
            &owed_write_plan(ME, &one, one_at, Some(900_000), false),
        );
        assert!(
            !owed_write_landed(stamp, one_at),
            "the older walk is told its snapshot did not land"
        );
        let after = served(&conn, ME);
        assert_eq!(
            after.0, newer.0,
            "the newer snapshot's rows stand, none replaced and none added"
        );
        assert_eq!(after.1, two_at, "the marker keeps the newer stamp");
        assert_eq!(after.2, 1, "and its stale mark");
        let tip: i64 = conn
            .query_row(
                "SELECT tip FROM owed_state WHERE identity = ?1",
                [ME],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tip, 900_001);
        // the ordinary order still writes: a NEWER walk replaces the rows and clears the mark
        let three_at = 72_000i64;
        let stamp = run_owed_write(
            &mut conn,
            &owed_write_plan(ME, &one, three_at, Some(900_002), true),
        );
        assert!(owed_write_landed(stamp, three_at));
        let after = served(&conn, ME);
        assert_eq!(
            after.0,
            vec![(format!("{}:0", tx(0x07)), "hop-stranded".to_string())]
        );
        assert_eq!((after.1, after.2), (three_at, 0));
        // the SAME stamp rewrites (one walk's batch retried is idempotent, never a refusal of itself)
        let stamp = run_owed_write(
            &mut conn,
            &owed_write_plan(ME, &two, three_at, Some(900_002), false),
        );
        assert!(owed_write_landed(stamp, three_at));
        assert_eq!(served(&conn, ME).0.len(), 2);
        // the guard is per identity: another identity's newer marker refuses nothing of mine
        let other = OPP;
        let theirs = [OwedRow {
            identity: other.to_string(),
            ..row(&format!("{}:0", tx(0x09)), OwedFamily::Payout)
        }];
        assert!(owed_write_landed(
            run_owed_write(
                &mut conn,
                &owed_write_plan(other, &theirs, 999_000, None, false)
            ),
            999_000
        ));
        let four_at = 73_000i64;
        assert!(owed_write_landed(
            run_owed_write(&mut conn, &owed_write_plan(ME, &one, four_at, None, false)),
            four_at
        ));
        assert_eq!(served(&conn, ME).0.len(), 1);
        assert_eq!(served(&conn, other).0.len(), 1);
        // an EMPTY older snapshot (the false "nothing owed") is refused like any other
        let stamp = run_owed_write(&mut conn, &owed_write_plan(ME, &[], 2_000, None, false));
        assert!(!owed_write_landed(stamp, 2_000));
        assert_eq!(served(&conn, ME).0.len(), 1);
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
            (derive_owed_rows(&i), courier_home_outputs(&i))
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
            derive_owed_rows(&i)[0].facts["claimReason"],
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
            derive_owed_rows(&i)
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
        let rows = derive_owed_rows(&i);
        assert_eq!(
            (
                rows[0].facts["joinRefusedBy"].clone(),
                rows[0].reason.as_deref()
            ),
            (json!("eviction"), Some(JOIN_REFUSED_REASON))
        );
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
        use crate::hops_view::{
            probe_memo_key, probe_memo_read_sql, ChainSpendProbe, ProbeMemo, PROBE_MEMO_UPSERT_SQL,
        };
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
        let write = |m: &ProbeMemo| {
            conn.execute(
                PROBE_MEMO_UPSERT_SQL,
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
                        tip_height: None,
                    })
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        let (home, stranger) = (
            bsv_rs::primitives::PrivateKey::random(),
            bsv_rs::primitives::PrivateKey::random(),
        );
        let pkh = hex::encode(home.public_key().hash160());
        let (sweep_a, spend_a) = sweep_and_home_spend(&home, &home, 20_000);
        let (sweep_b, forged_b) = sweep_and_home_spend(&home, &stranger, 30_000); // spent "by" bytes the home key never signed
        let cand = |sweep: &str, sats: u64| CourierHomeOutput {
            sweep_txid: sweep.to_string(),
            vout: 0,
            pkh_hex: pkh.clone(),
            sats,
        };
        let candidates = [cand(&sweep_a, 20_000), cand(&sweep_b, 30_000)];
        let latch_targets: Vec<(String, u32)> = candidates
            .iter()
            .map(|c| home_latch_target(&c.sweep_txid, c.vout))
            .collect();
        // pass 1: nothing latched, both outputs go to the chain rung
        let (words, targets) = latched_home_words(&candidates, &read(&latch_targets));
        assert!(words.is_empty());
        assert_eq!(targets, vec![(sweep_a.clone(), 0), (sweep_b.clone(), 0)]);
        let spent_by = |sp: &str| ChainSpendProbe {
            known: true,
            spent: Some(true),
            spending_txid: Some(sp.to_string()),
            spent_confirmed: Some(true),
        };
        let named = tx(0x5a); // the spender a courier NAMED; the proof is over the bytes
                              // an unknown or faulted probe, and a known probe with no verdict: nothing established, the row stands
        assert_eq!(
            home_word(
                &ChainSpendProbe {
                    known: false,
                    spent: Some(true),
                    spending_txid: Some(named.clone()),
                    spent_confirmed: None
                },
                Some(&spend_a),
                &candidates[0],
                5_000
            ),
            None
        );
        assert_eq!(
            home_word(
                &ChainSpendProbe {
                    known: true,
                    spent: None,
                    spending_txid: None,
                    spent_confirmed: None
                },
                Some(&spend_a),
                &candidates[0],
                5_000
            ),
            None
        );
        // a corroborated absence
        assert_eq!(
            home_word(
                &ChainSpendProbe {
                    known: true,
                    spent: Some(false),
                    spending_txid: None,
                    spent_confirmed: None
                },
                None,
                &candidates[0],
                5_000
            ),
            Some((HomeSpendWord::Unspent, None))
        );
        // a word of spent with no bytes, with bytes another key signed, with another output's proof: unproven, no latch
        assert_eq!(
            home_word(&spent_by(&named), None, &candidates[0], 5_000),
            Some((HomeSpendWord::Unproven, None))
        );
        assert_eq!(
            home_word(&spent_by(&named), Some(&forged_b), &candidates[1], 5_000),
            Some((HomeSpendWord::Unproven, None))
        );
        assert_eq!(
            home_word(&spent_by(&named), Some(&spend_a), &candidates[1], 5_000),
            Some((HomeSpendWord::Unproven, None))
        );
        // THE PROOF: the home key's signature over output A's spend
        let (word, latch) =
            home_word(&spent_by(&named), Some(&spend_a), &candidates[0], 5_000).expect("a word");
        let latch = latch.expect("a proof writes its latch");
        assert_eq!(word, HomeSpendWord::Proven);
        assert_eq!(
            latch.outpoint,
            format!("{HOME_SPEND_LATCH_PREFIX}{sweep_a}.0")
        );
        let proven_txid = bsv_rs::transaction::Transaction::from_binary(&spend_a)
            .unwrap()
            .id();
        assert_eq!(
            latch.spending_txid.as_deref(),
            Some(proven_txid.as_str()),
            "the latch names the transaction the proven bytes hash to"
        );
        assert_ne!(proven_txid, named);
        write(&latch);
        // the chain rung's ordinary memo of output B (keyed `<txid>.<vout>`) is no latch
        write(&ProbeMemo {
            outpoint: probe_memo_key(&sweep_b, 0),
            probed_at_ms: 5_000,
            spent: true,
            spending_txid: Some(named.clone()),
            spent_confirmed: Some(true),
            tip_height: None,
        });
        // pass 2: the key written is the key read; A is Proven with no ask, B is still a target
        let memos = read(&latch_targets);
        assert_eq!(memos.len(), 1);
        let (words, targets) = latched_home_words(&candidates, &memos);
        assert_eq!(
            words,
            [(outpoint_key(&sweep_a, 0), HomeSpendWord::Proven)]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );
        assert_eq!(targets, vec![(sweep_b.clone(), 0)]);
        // a candidate named in another case reads the same latch
        let upper = [cand(&sweep_a.to_ascii_uppercase(), 20_000)];
        let upper_targets: Vec<(String, u32)> = upper
            .iter()
            .map(|c| home_latch_target(&c.sweep_txid, c.vout))
            .collect();
        assert_eq!(latched_home_words(&upper, &read(&upper_targets)).0.len(), 1);
        // a memo under a latch key that does not say spent-with-a-spender latches nothing
        let weak = ProbeMemo {
            outpoint: probe_memo_key(&latch_targets[1].0, 0),
            probed_at_ms: 5_000,
            spent: true,
            spending_txid: None,
            spent_confirmed: None,
            tip_height: None,
        };
        assert!(latched_home_words(&candidates[1..], &[weak]).0.is_empty());
        // the stored-read count sits inside the candidate cap
        const _: () =
            assert!(OWED_HOME_STORED_READS_PER_RECOMPUTE <= crate::logic::D1_CHUNK_OUTPOINTS);
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
        conn.execute_batch(OWED_STATE_SEQ_ALTER).unwrap();
        conn.execute_batch(
            bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS hop_chain_probes "))
                .unwrap(),
        )
        .unwrap();
        conn.execute_batch(bsv_overlay_cloudflare::d1::HOP_CHAIN_PROBES_TIP_ALTER)
            .unwrap();
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
        let newer = [
            row(format!("{}:0", tx(0x07)), OwedFamily::Payout),
            row(format!("{}:1", tx(0x08)), OwedFamily::InProgress),
        ];
        // the route's stored read: the marker, then the rows, through the shipped statements
        let stored =
            |conn: &rusqlite::Connection| -> (Vec<(String, String)>, i64, Option<i64>, bool) {
                let (at, tip, cut): (i64, Option<i64>, i64) = conn
                    .query_row(OWED_STATE_READ_SQL, [ME], |r| {
                        Ok((r.get(1)?, r.get(2)?, r.get(5)?))
                    })
                    .unwrap();
                let rows = conn
                    .prepare(OWED_ROWS_READ_SQL)
                    .unwrap()
                    .query_map([ME], |r| Ok((r.get(1)?, r.get(2)?)))
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();
                (rows, at, tip, cut != 0)
            };
        // the newer walk (began at 71 000) lands first; its own read serves its own rows
        let landed = owed_write_landed(
            run_owed_write(
                &mut conn,
                &owed_write_plan(ME, &newer, 71_000, Some(900_001), true),
            ),
            71_000,
        );
        assert_eq!(read_answer_after_compute(!landed), ReadAnswer::Own);
        // the older walk (began at 1 000) lands second: refused, and its READER is owed the newer rows
        let landed = owed_write_landed(
            run_owed_write(
                &mut conn,
                &owed_write_plan(ME, &older, 1_000, Some(900_000), false),
            ),
            1_000,
        );
        assert!(!landed);
        assert_eq!(read_answer_after_compute(!landed), ReadAnswer::Stored);
        let (rows, at, tip, cut) = stored(&conn);
        assert_eq!(
            rows,
            vec![
                (format!("{}:0", tx(0x07)), "payout".to_string()),
                (format!("{}:1", tx(0x08)), "in-progress".to_string())
            ]
        );
        assert_ne!(rows.len(), older.len(), "never the older walk's own rows");
        assert_eq!(
            (at, tip, cut),
            (71_000, Some(900_001), true),
            "with the newer snapshot's stamp, tip and cut"
        );
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
            self.probes
                .get(&key)
                .cloned()
                .unwrap_or(crate::hops_view::ChainSpendProbe {
                    known: false,
                    spent: None,
                    spending_txid: None,
                    spent_confirmed: None,
                })
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
    fn read_memos(
        conn: &rusqlite::Connection,
        targets: &[(String, u32)],
    ) -> Vec<crate::hops_view::ProbeMemo> {
        let keys: Vec<String> = targets
            .iter()
            .map(|(t, v)| crate::hops_view::probe_memo_key(t, *v))
            .collect();
        assert!(
            keys.len() < crate::logic::D1_MAX_BOUND_PARAMS,
            "one read stays inside D1's bind limit ({})",
            keys.len()
        );
        conn.prepare(&crate::hops_view::probe_memo_read_sql(keys.len()))
            .unwrap()
            .query_map(rusqlite::params_from_iter(keys.iter()), |r| {
                Ok(crate::hops_view::ProbeMemo {
                    outpoint: r.get(0)?,
                    probed_at_ms: r.get(1)?,
                    spent: r.get::<_, i64>(2)? != 0,
                    spending_txid: r.get(3)?,
                    spent_confirmed: r.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                    tip_height: None,
                })
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }
    /// ONE PASS as `routes::owed_home_spend_walk` runs it, the Worker's D1 replaced by real SQLite over the shipped
    /// memo statements: the cursor and the window's memos read, `home_walk_pass`, the memos and the cursor written.
    /// Returns the pass and the window it walked.
    fn home_pass_on_sqlite(
        conn: &rusqlite::Connection,
        world: &mut FakeCouriers,
        candidates: &[CourierHomeOutput],
        now_ms: i64,
    ) -> (HomePass, Vec<CourierHomeOutput>) {
        let memo_targets = |window: &[CourierHomeOutput]| -> Vec<(String, u32)> {
            window
                .iter()
                .map(|c| home_latch_target(&c.sweep_txid, c.vout))
                .chain(window.iter().map(|c| (c.sweep_txid.clone(), c.vout)))
                .collect()
        };
        let (window, memos) = if candidates.len() <= OWED_HOME_WINDOW {
            let mut targets = vec![home_cursor_target(ME)];
            targets.extend(memo_targets(candidates));
            let memos = read_memos(conn, &targets);
            (
                home_walk_window(
                    candidates,
                    home_cursor_of(&memos, ME).as_deref(),
                    OWED_HOME_WINDOW,
                ),
                memos,
            )
        } else {
            let cursor = home_cursor_of(&read_memos(conn, &[home_cursor_target(ME)]), ME);
            let window = home_walk_window(candidates, cursor.as_deref(), OWED_HOME_WINDOW);
            let memos = read_memos(conn, &memo_targets(&window));
            (window, memos)
        };
        let pass = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(home_walk_pass(world, &window, &memos, &memos, now_ms));
        write_memos(conn, &pass.memos);
        if let Some(next) = home_cursor_after(candidates, &window, pass.first_withheld) {
            write_memos(conn, &[home_cursor_memo(ME, next, now_ms)]);
        }
        (pass, window)
    }
    fn plain_candidates(n: usize) -> Vec<CourierHomeOutput> {
        (0..n)
            .map(|i| CourierHomeOutput {
                sweep_txid: format!("{:064x}", 0x1000 + i),
                vout: 0,
                pkh_hex: "cc".repeat(20),
                sats: 20_000,
            })
            .collect()
    }
    fn unspent_probe() -> crate::hops_view::ChainSpendProbe {
        crate::hops_view::ChainSpendProbe {
            known: true,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: None,
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
                let mut rows = derive_owed_rows(&i);
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
            derive_owed_rows(&i)[0].facts["claimReason"],
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
    fn the_overlays_reorg_clear_and_ttl_sweep_leave_the_home_latch_and_the_home_cursor_real_sqlite()
    {
        use crate::hops_view::{probe_memo_key, ChainSpendProbe, ProbeMemo};
        use bsv_overlay_cloudflare::hop_probe_memos::{
            HOP_PROBE_MEMO_APP_LAYER_PREFIXES, HOP_PROBE_MEMO_EXPIRE_SQL,
            HOP_PROBE_MEMO_REORG_CLEAR_SQL, HOP_PROBE_MEMO_REORG_MARK,
            HOP_PROBE_MEMO_REORG_MARK_SQL,
        };
        assert_eq!(
            HOP_PROBE_MEMO_APP_LAYER_PREFIXES,
            [HOME_SPEND_LATCH_PREFIX, HOME_WALK_CURSOR_PREFIX],
            "the overlay spares exactly the prefixes this crate writes"
        );
        let conn = memo_db();
        let t0: i64 = 1_800_000_000_000;
        let home = bsv_rs::primitives::PrivateKey::random();
        let pkh = hex::encode(home.public_key().hash160());
        let (sweep_a, spend_a) = sweep_and_home_spend(&home, &home, 20_000);
        let (sweep_b, spend_b) = sweep_and_home_spend(&home, &home, 30_000);
        let candidates = [
            CourierHomeOutput {
                sweep_txid: sweep_a.clone(),
                vout: 0,
                pkh_hex: pkh.clone(),
                sats: 20_000,
            },
            CourierHomeOutput {
                sweep_txid: sweep_b.clone(),
                vout: 0,
                pkh_hex: pkh.clone(),
                sats: 30_000,
            },
        ];
        let spent = |confirmed: Option<bool>| ChainSpendProbe {
            known: true,
            spent: Some(true),
            spending_txid: Some(tx(0x5a)),
            spent_confirmed: confirmed,
        };
        // the latch of a CONFIRMED probe (`spentConfirmed = 1`: the reorg clear's own predicate) and of an unconfirmed one
        let (_, latch_a) =
            home_word(&spent(Some(true)), Some(&spend_a), &candidates[0], t0).expect("a word");
        let (_, latch_b) =
            home_word(&spent(None), Some(&spend_b), &candidates[1], t0).expect("a word");
        let (latch_a, latch_b) = (
            latch_a.expect("a proof latches"),
            latch_b.expect("a proof latches"),
        );
        assert_eq!(latch_a.spent_confirmed, Some(true));
        let cursor = home_cursor_memo(ME, &candidates[1], t0);
        // the chain rung's own memos of the same two outputs, and the tombstone
        let chain = |sweep: &str, confirmed: Option<bool>| ProbeMemo {
            outpoint: probe_memo_key(sweep, 0),
            probed_at_ms: t0,
            spent: true,
            spending_txid: Some(tx(0x5a)),
            spent_confirmed: confirmed,
            tip_height: None,
        };
        write_memos(
            &conn,
            &[
                latch_a,
                latch_b,
                cursor,
                chain(&sweep_a, Some(true)),
                chain(&sweep_b, None),
            ],
        );
        conn.execute(HOP_PROBE_MEMO_REORG_MARK_SQL, rusqlite::params![t0])
            .unwrap();
        let held = |conn: &rusqlite::Connection| -> usize {
            conn.query_row("SELECT COUNT(*) FROM hop_chain_probes", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap() as usize
        };
        assert_eq!(held(&conn), 6);
        let latch_targets: Vec<(String, u32)> = candidates
            .iter()
            .map(|c| home_latch_target(&c.sweep_txid, c.vout))
            .collect();
        let both_latched = |conn: &rusqlite::Connection| -> bool {
            let (words, targets) =
                latched_home_words(&candidates, &read_memos(conn, &latch_targets));
            targets.is_empty()
                && words.len() == 2
                && words.values().all(|w| *w == HomeSpendWord::Proven)
        };
        let cursor_at_b = |conn: &rusqlite::Connection| -> bool {
            home_cursor_of(&read_memos(conn, &[home_cursor_target(ME)]), ME)
                == Some(probe_memo_key(&sweep_b, 0))
        };
        // THE REORG CLEAR: the confirmed chain memo goes, nothing else (it took the confirmed latch too)
        assert_eq!(conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, []).unwrap(), 1, "the one confirmed chain memo: a signature proof is not a chain word, a reorg does not unmake it");
        assert!(both_latched(&conn), "the reorg clear took a latch");
        assert!(cursor_at_b(&conn));
        // THE TTL SWEEP, with a bound past every row: the other chain memo goes, nothing else (it took both latches and the cursor)
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![i64::MAX])
                .unwrap(),
            1,
            "the one chain memo left"
        );
        assert!(both_latched(&conn), "the TTL sweep took a latch: the retired row is back and is bought again from the couriers");
        assert!(
            cursor_at_b(&conn),
            "the TTL sweep took the cursor: the ring starts at its head again"
        );
        assert_eq!(
            read_memos(&conn, &[crate::hops_view::reorg_mark_target()]).len(),
            1,
            "the tombstone keeps its own exemption"
        );
        assert_eq!(held(&conn), 4, "two latches, the cursor, the tombstone");
        assert!(!HOP_PROBE_MEMO_APP_LAYER_PREFIXES
            .iter()
            .any(|p| HOP_PROBE_MEMO_REORG_MARK.starts_with(p)));
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
                (
                    CourierHomeOutput {
                        sweep_txid: sweep,
                        vout: 0,
                        pkh_hex: pkh.clone(),
                        sats: 20_000 + i,
                    },
                    spend,
                )
            })
            .collect();
        made.sort_by(|a, b| a.0.sweep_txid.cmp(&b.0.sweep_txid)); // `courier_home_outputs`' order
        let candidates: Vec<CourierHomeOutput> = made.iter().map(|(c, _)| c.clone()).collect();
        let spender_of = |i: usize| {
            bsv_rs::transaction::Transaction::from_binary(&made[i].1)
                .unwrap()
                .id()
        };
        let mut world = FakeCouriers::default();
        for (i, (c, spend)) in made.iter().enumerate() {
            // every output: a confirmed spend the chain rung already memoised (no probe is owed)
            write_memos(
                &conn,
                &[ProbeMemo {
                    outpoint: probe_memo_key(&c.sweep_txid, 0),
                    probed_at_ms: 1_000,
                    spent: true,
                    spending_txid: Some(spender_of(i)),
                    spent_confirmed: Some(true),
                    tip_height: None,
                }],
            );
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
        assert_eq!(
            pass.stored_reads, OWED_HOME_STORED_READS_PER_RECOMPUTE,
            "exactly the cap"
        );
        assert_eq!(
            world.stored_asked,
            (0..8).map(spender_of).collect::<Vec<_>>()
        );
        assert!(
            pass.words.values().all(|w| *w == HomeSpendWord::Unproven),
            "{:?}",
            pass.words
        );
        assert_eq!(pass.first_withheld, Some(8));
        assert_eq!(
            pass.resolver_asks, OWED_HOME_PROBES_PER_RECOMPUTE,
            "the resolver's cap, honoured: the ninth and tenth asked, no more"
        );
        assert_eq!(world.resolver_asked, vec![spender_of(8), spender_of(9)]);
        // the cursor row, read back through the shipped statement under the key the next pass reads
        assert_eq!(
            home_cursor_of(&read_memos(&conn, &[home_cursor_target(ME)]), ME),
            Some(probe_memo_key(&candidates[8].sweep_txid, 0))
        );

        // pass 2: starts at the ninth; the tail's four spends are read, verified, latched
        world.stored_asked.clear();
        let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 3_000);
        assert_eq!(
            window[0], candidates[8],
            "the ring starts where the last pass was cut"
        );
        assert_eq!(pass.stored_reads, OWED_HOME_STORED_READS_PER_RECOMPUTE);
        assert_eq!(
            world.stored_asked[..4],
            (8..12).map(spender_of).collect::<Vec<_>>()[..]
        );
        for c in &candidates[8..] {
            assert_eq!(
                pass.words.get(&outpoint_key(&c.sweep_txid, 0)),
                Some(&HomeSpendWord::Proven),
                "within two passes"
            );
        }
        assert_eq!(
            pass.memos
                .iter()
                .filter(|m| m.outpoint.starts_with(HOME_SPEND_LATCH_PREFIX))
                .count(),
            4
        );

        // pass 3: the four are latched (no read, no ask); the eight that never prove are still looked at, none starved
        world.stored_asked.clear();
        let (pass, _) = home_pass_on_sqlite(&conn, &mut world, &candidates, 4_000);
        assert_eq!(pass.stored_reads, 8);
        assert_eq!(pass.first_withheld, None);
        assert_eq!(
            pass.words
                .values()
                .filter(|w| **w == HomeSpendWord::Proven)
                .count(),
            4
        );
        assert_eq!(
            pass.words
                .values()
                .filter(|w| **w == HomeSpendWord::Unproven)
                .count(),
            8
        );
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
            world
                .probes
                .insert(outpoint_key(&c.sweep_txid, 0), unspent_probe());
        }
        let key = |i: usize| outpoint_key(&candidates[i].sweep_txid, 0);
        let (pass, _) = home_pass_on_sqlite(&conn, &mut world, &candidates, 1_000);
        assert_eq!(
            (pass.probes, pass.first_withheld),
            (OWED_HOME_PROBES_PER_RECOMPUTE, Some(2))
        );
        assert_eq!(world.probed, vec![key(0), key(1)]);
        assert!(pass.words.is_empty(), "an unanswered probe names nothing");
        let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 2_000);
        assert_eq!(window[0], candidates[2]);
        assert_eq!(pass.probes, OWED_HOME_PROBES_PER_RECOMPUTE);
        assert_eq!(world.probed[2..], [key(2), key(3)]);
        assert_eq!(
            (pass.words.get(&key(2)), pass.words.get(&key(3))),
            (Some(&HomeSpendWord::Unspent), Some(&HomeSpendWord::Unspent))
        );
        let (pass, _) = home_pass_on_sqlite(&conn, &mut world, &candidates, 3_000);
        assert_eq!(pass.probes, OWED_HOME_PROBES_PER_RECOMPUTE);
        assert_eq!(
            world.probed[4..],
            [key(4), key(0)],
            "the fifth, then the ring wraps to the head"
        );
        let seen: HashSet<&String> = world.probed.iter().collect();
        assert_eq!(
            seen.len(),
            5,
            "every output probed within three passes of two"
        );
        // a pass past its clock asks nothing and moves nothing
        let before = home_cursor_of(&read_memos(&conn, &[home_cursor_target(ME)]), ME);
        let asked = world.probed.len();
        world.over_budget = true;
        let (pass, window) = home_pass_on_sqlite(&conn, &mut world, &candidates, 400_000);
        assert_eq!(
            (
                pass.probes,
                pass.stored_reads,
                pass.resolver_asks,
                world.probed.len()
            ),
            (0, 0, 0, asked)
        );
        assert_eq!(pass.first_withheld, Some(0));
        assert_eq!(
            home_cursor_of(&read_memos(&conn, &[home_cursor_target(ME)]), ME),
            before
        );
        assert_eq!(
            Some(crate::hops_view::probe_memo_key(&window[0].sweep_txid, 0)),
            before
        );
    }

    /// The delta fold's LOW-1 (cut 3, the window) and the bound itself: an identity with MORE home outputs than one
    /// pass looks at. Forty-five latched outputs at the head no longer hide the five behind them, and with fifty
    /// outputs no courier can ever answer (none ever proves), every one is tried within ceil(50 / 2) passes, no
    /// pass over its caps or its window.
    /// To red: cut the candidates to the first `OWED_HOME_WINDOW` (ignore the cursor).
    #[test]
    fn more_home_outputs_than_the_window_are_all_tried_within_a_bounded_number_of_passes_real_sqlite(
    ) {
        use crate::hops_view::{probe_memo_key, ProbeMemo};
        let candidates = plain_candidates(50);
        assert!(candidates.len() > OWED_HOME_WINDOW);
        let key = |i: usize| outpoint_key(&candidates[i].sweep_txid, 0);

        // (a) forty-five latched at the head, five the chain rung answers behind them
        let conn = memo_db();
        for c in &candidates[..45] {
            let (latch_txid, vout) = home_latch_target(&c.sweep_txid, c.vout);
            write_memos(
                &conn,
                &[ProbeMemo {
                    outpoint: probe_memo_key(&latch_txid, vout),
                    probed_at_ms: 500,
                    spent: true,
                    spending_txid: Some(tx(0x5a)),
                    spent_confirmed: Some(true),
                    tip_height: None,
                }],
            );
        }
        let mut world = FakeCouriers::default();
        for c in &candidates[45..] {
            world
                .probes
                .insert(outpoint_key(&c.sweep_txid, 0), unspent_probe());
        }
        let mut words: HashMap<String, HomeSpendWord> = HashMap::new();
        for pass_no in 0..4i64 {
            let (pass, window) =
                home_pass_on_sqlite(&conn, &mut world, &candidates, 1_000 + pass_no);
            assert_eq!(window.len(), OWED_HOME_WINDOW);
            assert!(pass.probes <= OWED_HOME_PROBES_PER_RECOMPUTE);
            if pass_no == 0 {
                assert!(
                    world.probed.is_empty() && pass.first_withheld.is_none(),
                    "the head's window is all latches: the cursor moves past it"
                );
            }
            words.extend(pass.words);
        }
        assert_eq!(
            world.probed,
            (45..50).map(key).collect::<Vec<_>>(),
            "the five behind the window, each probed once"
        );
        assert_eq!(
            words.len(),
            50,
            "every output has a word within four passes"
        );
        assert_eq!(
            words
                .values()
                .filter(|w| **w == HomeSpendWord::Unspent)
                .count(),
            5
        );

        // (b) fifty outputs, none answerable: two per pass, all fifty within twenty-five passes
        let conn = memo_db();
        let mut world = FakeCouriers::default();
        for pass_no in 0..25i64 {
            let (pass, window) =
                home_pass_on_sqlite(&conn, &mut world, &candidates, 1_000 + pass_no);
            assert_eq!(
                (window.len(), pass.probes),
                (OWED_HOME_WINDOW, OWED_HOME_PROBES_PER_RECOMPUTE),
                "pass {pass_no}"
            );
        }
        assert_eq!(world.probed.len(), 50);
        assert_eq!(
            world.probed.iter().collect::<HashSet<_>>().len(),
            50,
            "every candidate tried within ceil(50 / 2) passes"
        );
    }

    /// The ring's two pure ends: where a pass starts and where the next one will.
    #[test]
    fn the_home_walks_window_starts_at_the_cursor_and_wraps() {
        let c = plain_candidates(4);
        let k = |i: usize| crate::hops_view::probe_memo_key(&c[i].sweep_txid, 0);
        assert_eq!(home_walk_window(&c, None, 45), c);
        assert_eq!(
            home_walk_window(&c, Some(&k(2)), 45),
            vec![c[2].clone(), c[3].clone(), c[0].clone(), c[1].clone()]
        );
        assert_eq!(
            home_walk_window(&c, Some(&k(3).to_ascii_uppercase()), 2),
            vec![c[3].clone(), c[0].clone()]
        );
        // a cursor naming an output that left the list lands on its successor; past the end, or malformed: the head
        assert_eq!(
            home_walk_window(&c, Some(&format!("{:064x}.7", 0x1001)), 1),
            vec![c[2].clone()]
        );
        assert_eq!(
            home_walk_window(&c, Some(&format!("{}.0", "ff".repeat(32))), 1),
            vec![c[0].clone()]
        );
        assert_eq!(
            home_walk_window(&c, Some("not a cursor"), 1),
            vec![c[0].clone()]
        );
        assert!(home_walk_window(&[], Some(&k(0)), 45).is_empty());
        // the next start: the first withheld output; else the one after the window; a whole ring with nothing withheld stays
        let w = home_walk_window(&c, Some(&k(2)), 2);
        assert_eq!(home_cursor_after(&c, &w, Some(1)), Some(&c[3]));
        assert_eq!(home_cursor_after(&c, &w, None), Some(&c[0]));
        assert_eq!(
            home_cursor_after(&c, &home_walk_window(&c, Some(&k(2)), 45), None),
            None
        );
        // the cursor row is no latch and no chain word
        let memo = home_cursor_memo(ME, &c[3], 9_000);
        assert_eq!(
            (
                memo.outpoint.as_str(),
                memo.spent,
                memo.spending_txid.as_deref()
            ),
            (
                format!("{HOME_WALK_CURSOR_PREFIX}{ME}.0").as_str(),
                false,
                Some(k(3).as_str())
            )
        );
        assert!(latched_home_words(&c, std::slice::from_ref(&memo))
            .0
            .is_empty());
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
        conn.execute_batch(OWED_STATE_SEQ_ALTER).unwrap();
        conn.execute_batch(
            bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS hop_chain_probes "))
                .unwrap(),
        )
        .unwrap();
        conn.execute_batch(bsv_overlay_cloudflare::d1::HOP_CHAIN_PROBES_TIP_ALTER)
            .unwrap();
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
            row(
                0x07,
                OwedFamily::HopStranded,
                json!({ "claim": "sweep-hop", "claimable": true, "joinRefusedBy": "door" }),
            ),
            row(
                0x08,
                OwedFamily::Payout,
                json!({ "claim": "internalize", "claimable": true, "creditBeef": "/credit-beef/x" }),
            ),
            row(
                0x09,
                OwedFamily::Payout,
                json!({ "claim": "internalize", "claimable": false, "claimReason": UNCONFIRMED_PAYOUT_REASON }),
            ),
            row(0x0a, OwedFamily::Unbound, json!({ "claim": null })),
        ];
        // the route's stored read (`owed_read_stored`): the marker first; no marker = no snapshot
        let marker = conn.query_row(OWED_STATE_READ_SQL, [ME], |r| r.get::<_, i64>(1));
        assert!(matches!(marker, Err(rusqlite::Error::QueryReturnedNoRows)));
        assert_eq!(read_answer_after_compute(true), ReadAnswer::Stored);
        let (rows, tip, at, cut, seq) =
            superseded_read_snapshot(None, (older.clone(), Some(900_000), 1_000, false, Some(3)));
        assert_eq!((rows.len(), tip, at, cut), (4, Some(900_000), 1_000, false));
        assert_eq!(
            seq, None,
            "the fallback is not the snapshot the row's sequence counts"
        );
        assert!(
            rows.iter()
                .all(|r| r.facts.get("claimable") != Some(&json!(true))),
            "no claimable word on the double fault: {rows:?}"
        );
        for r in &rows[..2] {
            assert_eq!(
                (
                    r.facts["claimable"].clone(),
                    r.facts["claimReason"].clone(),
                    r.facts["supersededFallback"].clone()
                ),
                (json!(false), json!(SUPERSEDED_FALLBACK_REASON), json!(true))
            );
        }
        assert_eq!(
            (
                rows[0].facts["claim"].clone(),
                rows[0].facts["joinRefusedBy"].clone(),
                rows[1].facts["creditBeef"].clone()
            ),
            (json!("sweep-hop"), json!("door"), json!("/credit-beef/x")),
            "every other fact stands"
        );
        assert_eq!(
            (&rows[2], &rows[3]),
            (&older[2], &older[3]),
            "a row with no claimable word is served as it was"
        );
        assert!(!owed_body(ME, tip, &rows, at, cut, seq).contains("\"claimable\":true"));
        // with a stored snapshot the reader gets it, untouched
        let newer = vec![row(
            0x0b,
            OwedFamily::HopStranded,
            json!({ "claim": "sweep-hop", "claimable": true }),
        )];
        assert_eq!(
            superseded_read_snapshot(
                Some((newer.clone(), Some(900_001), 71_000, true, Some(4))),
                (older, Some(900_000), 1_000, false, Some(3))
            ),
            (newer, Some(900_001), 71_000, true, Some(4))
        );
    }

    /// bsv-low #499: THE CEILING SHEDS. Twelve recomputes of one identity inside a minute run; the thirteenth is
    /// shed (the caller serves the snapshot and counts it); another identity is untouched; the first read of an
    /// identity (no snapshot to serve) is also capped (#648); the window past, the identity runs
    /// again. RED on `b1ad9a1`: there was no ceiling (every ask walked).
    #[test]
    fn the_thirteenth_recompute_of_one_identity_inside_a_minute_is_shed_to_the_snapshot() {
        let mut rate = RecomputeRate::default();
        let (me, other) = ("02".repeat(33), "03".repeat(33));
        let t0 = 1_790_000_000_000_i64;
        for i in 0..OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW {
            assert!(
                rate.admit(&me, t0 + i64::from(i) * 1_000, true),
                "recompute {} of the minute runs",
                i + 1
            );
        }
        assert_eq!(OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW, 12);
        assert_eq!(rate.at_ceiling(t0 + 12_000), 1);
        let shed_before = recompute_shed_total();
        // the 13th inside the minute: shed (the route counts it and serves the snapshot)
        assert!(
            !rate.admit(&me, t0 + 59_999, true),
            "the 13th recompute inside the minute must be shed"
        );
        note_recompute_shed();
        assert!(
            recompute_shed_total() > shed_before,
            "a shed is counted on /health (recomputeShed)"
        );
        assert_eq!(rate.in_window(&me, t0 + 59_999), 12, "a shed is not a run");
        // a stranger's storm is not mine, and mine is not theirs
        assert!(rate.admit(&other, t0 + 30_000, true));
        // a repeated first read cannot bypass the bound when the previous walks faulted without a snapshot
        assert!(!rate.admit(&me, t0 + 59_999, false));
        assert_eq!(rate.in_window(&me, t0 + 59_999), 12);
        // the window past: runs again, counted from one
        assert!(rate.admit(&me, t0 + 60_000, true));
        assert_eq!(rate.in_window(&me, t0 + 60_000), 1);
        assert_eq!(rate.at_ceiling(t0 + 60_000), 0);
    }

    /// bsv-low #499 (lens fold M1): A SHED NEVER LOSES A REFRESH. The race, on real SQLite with the shipped
    /// statements: walk 12 of B begins (t = 10 000) and reads the store; the 13th hook marks B stale (its change
    /// landed after walk 12 read); walk 12 writes its snapshot (`stale = 0`, stamped at its start, which is still the
    /// newest stamp, so the write lands and clears the hook's mark); the claim pops the folded ask and the ceiling
    /// sheds it, and the shed runs [`owed_shed_plan`]. The next read must see the identity stale and recompute, at
    /// the age the walk left (a list of one claimable payout, which the age rule alone recomputes only after 15
    /// minutes). RED on `a0db9fb`: a shed wrote nothing (no `owed_shed_plan`), the read saw `stale = 0` and served
    /// walk 12's rows. To red: drop the plan's statement in `owed_recompute_and_push` (the source half below).
    #[test]
    fn a_shed_rerun_after_a_walk_cleared_the_hooks_mark_leaves_the_identity_stale_for_the_next_read(
    ) {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        conn.execute_batch(OWED_ROWS_CREATE).unwrap();
        conn.execute_batch(OWED_STATE_CREATE).unwrap();
        conn.execute_batch(OWED_STATE_SEQ_ALTER).unwrap();
        conn.execute_batch(
            bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS hop_chain_probes "))
                .unwrap(),
        )
        .unwrap();
        conn.execute_batch(bsv_overlay_cloudflare::d1::HOP_CHAIN_PROBES_TIP_ALTER)
            .unwrap();
        let payout = OwedRow {
            identity: ME.to_string(),
            outpoint: format!("{}:0", tx(0x21)),
            family: OwedFamily::Payout,
            game_id: tx(0x21),
            sats: Some(20_190),
            opponent_identity: Some(OPP.to_string()),
            at_height: None,
            facts: json!({ "claim": "payout", "claimable": true }),
            reason: None,
        };
        // an earlier snapshot, then twelve recomputes inside the minute (the counter at its ceiling)
        assert!(owed_write_landed(
            run_owed_write(
                &mut conn,
                &owed_write_plan(ME, &[], 1_000, Some(900_000), false)
            ),
            1_000
        ));
        let mut rate = RecomputeRate::default();
        let t0 = 10_000i64;
        for i in 0..OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW {
            assert!(rate.admit(ME, t0 + i64::from(i), true));
        }
        // walk 12 began at t0 (its stamp) and read the store; the 13th hook's change lands, the hook marks stale
        conn.execute(OWED_STALE_FOR_IDENTITY_SQL, [ME]).unwrap();
        assert_eq!(served(&conn, ME).2, 1, "the hook marked the identity stale");
        // walk 12 writes what it read (the payout, not the hook's change): stamped t0, newest, so it lands, stale = 0
        let stamp = run_owed_write(
            &mut conn,
            &owed_write_plan(ME, std::slice::from_ref(&payout), t0, Some(900_001), false),
        );
        assert!(owed_write_landed(stamp, t0));
        assert_eq!(
            served(&conn, ME).2,
            0,
            "the walk's write cleared the hook's mark (the race)"
        );
        // the claim pops the folded ask: the ceiling sheds it, and the shed runs its plan
        assert!(
            !rate.admit(ME, t0 + 30_000, true),
            "the folded rerun is the 13th: shed"
        );
        let (sql, bind) = owed_shed_plan(ME);
        conn.execute(sql, [bind.as_str()]).unwrap();
        // the next read: stale, so it recomputes at once (not 15 minutes later by age)
        let (rows, computed_at, stale) = served(&conn, ME);
        assert_eq!(
            (rows.len(), computed_at, stale),
            (1, t0, 1),
            "walk 12's rows stand, marked stale"
        );
        let read_at = t0 + 40_000;
        assert!(
            should_recompute(
                stale != 0,
                read_at - computed_at,
                Some(900_001),
                Some(900_001),
                std::slice::from_ref(&payout)
            ),
            "the next read recomputes"
        );
        assert!(
            !should_recompute(false, read_at - computed_at, Some(900_001), Some(900_001), std::slice::from_ref(&payout)),
            "without the mark the age rule alone would have served the old rows (the bug's 15 minutes)"
        );
        // past the window the read's recompute runs (it is not shed again)
        assert!(rate.admit(ME, t0 + OWED_RECOMPUTE_RATE_WINDOW_MS, true));
        // the source half: the hooks' and claims' entry runs the plan on a shed, before it returns
        let squash = |s: &str| {
            s.split_whitespace()
                .collect::<String>()
                .replace(",)", ")")
                .replace(",]", "]")
        };
        let code: String = include_str!("routes.rs")
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let routes = squash(&code);
        let push = routes
            .find(&squash("pub(crate) async fn owed_recompute_and_push("))
            .expect("the hooks' entry");
        assert!(
            routes[push..].starts_with(&squash(
                "pub(crate) async fn owed_recompute_and_push( env: &worker::Env, db: &worker::D1Database, identity_lc: &str, source: &str, tip_hint: Option<u64>, token: i64, invocation_end_ms: i64, admitted: bool, ) -> OwedClaimWalk { if !admitted && !owed_recompute_admit(identity_lc, source, true) { return OwedClaimWalk::Shed { deferred: owed_deferred_deadline(identity_lc), }; }"
            )),
            "a shed in owed_recompute_and_push captures its deferred deadline before any await and returns"
        );
        // the FIX-800 lens's LOW-2: the claim runs the re-mark and the deferred walk, once its lock is released (the
        // second FIX-800 delta lens's MEDIUM-B: after its note to an alarm armed already, `routes::fix800_k`)
        assert!(
            routes.contains(&squash(
                "OwedClaimWalk::Shed { deferred } => { owed_refresh_end(identity_lc, token); let note = match deferred { None => Some(owed_note_alarm(env, identity_lc).await), Some(_) => None, }; owed_shed_remark(db, identity_lc).await; owed_schedule_deferred(env, identity_lc, deferred).await;"
            )),
            "a shed claim releases, then re-marks the identity stale and arms its deferred walk"
        );
        let remark = routes
            .find(&squash("async fn owed_shed_remark("))
            .expect("the re-mark");
        assert!(
            routes[remark..].contains(&squash("crate::owed::owed_shed_plan(identity_lc)")),
            "the re-mark runs the pinned plan"
        );
    }

    /// bsv-low #499: EVERY WALK ASKS THE CEILING FIRST. The pure counter above sheds nothing unless the route asks
    /// it before each `owed_recompute`: the hooks' and claims' one entry (`owed_recompute_and_push`), the read's
    /// inline arm, and the first read (which counts and never sheds). `owed_recompute(` is called at exactly the two
    /// sites below, so a new caller that skips the ask breaks the count here. To red: delete the ask in
    /// `owed_recompute_and_push` (the walk then runs past the ceiling).
    #[test]
    fn every_owed_recompute_caller_asks_the_ceiling_before_it_walks() {
        let squash = |s: &str| {
            s.split_whitespace()
                .collect::<String>()
                .replace(",)", ")")
                .replace(",]", "]")
        };
        let code: String = include_str!("routes.rs")
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let routes = squash(&code);
        let push = routes
            .find(&squash("pub(crate) async fn owed_recompute_and_push("))
            .expect("the hooks' entry");
        let push_body = &routes[push..];
        // bsv-low #800: the read's refresh reserved before its answer (`admitted`); every other walk asks here
        let ask = push_body.find(&squash("if !admitted && !owed_recompute_admit(identity_lc, source, true) { return OwedClaimWalk::Shed { deferred: owed_deferred_deadline(identity_lc), }; }")).expect("the ask");
        let walk = push_body
            .find(&squash(
                "owed_recompute(env, db, identity_lc, source, tip_hint, walk)",
            ))
            .expect("the walk");
        assert!(
            ask < walk,
            "owed_recompute_and_push must ask the ceiling before it walks"
        );
        assert!(routes.contains(&squash(
            "if owed_recompute_admit(&identity, \"read-first\", false) {"
        )));
        assert!(routes.contains(&squash("if !owed_recompute_admit(&identity, source, true) { refresh_retry = Some(OWED_RATE.with(|r| { r.borrow().retry_after_ms(&identity, worker::Date::now().as_millis() as i64) }).unwrap_or(0)); let deferred = owed_deferred_deadline(&identity); owed_shed_remark(&db, &identity).await; owed_schedule_deferred(&ctx.env, &identity, deferred).await; (rows, prev_tip, st.computed_at_ms as i64, cut, stored_seq) } else { match owed_compute_on_read(&ctx.env, &db, &identity, source, tip_now)")));
        assert_eq!(
            routes
                .matches(&squash("owed_recompute(env, db, identity_lc,"))
                .count(),
            2,
            "the walk's two callers (the push and the read's compute)"
        );
    }
    #[test]
    fn ns2_shed_spent_pot_arms_a_deferred_walk_and_push() {
        let mut rate = RecomputeRate::default();
        for n in 0..12 {
            assert!(rate.admit(ME, 1_000 + n, true));
        }
        assert!(!rate.admit(ME, 20_000, true));
        assert!(
            include_str!("routes.rs")
                .contains("owed_schedule_deferred(env, identity_lc, deferred).await"),
            "spent-pot pot-changed must schedule a later walk and push"
        );
        assert!(include_str!("board_view.rs").contains("async fn alarm(&self)"));
        assert!(include_str!("board_view.rs").contains("owed_deferred_walk_and_push"));
    }

    #[test]
    fn ns2_one_deferred_walk_converges_the_spent_pot_at_the_window_end() {
        let mut rate = RecomputeRate::default();
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(OWED_ROWS_CREATE).unwrap();
        conn.execute_batch(OWED_STATE_CREATE).unwrap();
        conn.execute_batch(OWED_STATE_SEQ_ALTER).unwrap();
        conn.execute_batch(
            bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS hop_chain_probes "))
                .unwrap(),
        )
        .unwrap();
        conn.execute_batch(bsv_overlay_cloudflare::d1::HOP_CHAIN_PROBES_TIP_ALTER)
            .unwrap();
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let live = [entry(
            Some(false),
            None,
            Outcome::Unresolved,
            Some(SeatLetter::A),
        )];
        let before = derive_owed_rows(&inputs(&live, &[], &[], &v, &c, &p, Some(899_990)));
        assert_eq!(before[0].family, OwedFamily::InProgress);
        run_owed_write(
            &mut conn,
            &owed_write_plan(ME, &before, 1_000, Some(900_001), false),
        );
        for n in 0..12 {
            assert!(rate.admit(ME, 1_000 + n, true));
        }
        assert!(!rate.admit(ME, 20_000, true));
        let end = rate.arm_deferred(ME, 20_000).unwrap();
        assert_eq!(end, 61_000);
        for at in [21_000, 30_000, 60_999] {
            assert!(!rate.admit(ME, at, true));
            assert_eq!(rate.arm_deferred(ME, at), None);
        }
        assert_eq!(served(&conn, ME).0[0].1.as_str(), "in-progress");
        let spent = [entry(
            Some(true),
            Some(PotVerdict::WinnerA),
            Outcome::Won,
            Some(SeatLetter::A),
        )];
        let after = derive_owed_rows(&inputs(&spent, &[], &[], &v, &c, &p, Some(900_200)));
        run_owed_write(
            &mut conn,
            &owed_write_plan(ME, &after, end, Some(900_200), false),
        );
        assert_eq!(served(&conn, ME).0[0].1.as_str(), "payout");
        assert_eq!(after[0].sats, Some(39_200));
        let push = owed_changed_event_body(ME, "deferred", after.len(), end);
        assert_eq!(push["kind"], "owed-changed");
        assert_eq!(push["computedAtMs"], end);
        assert!(rate.admit(ME, end, true)); // the next window retains its ordinary twelve admits
        assert_eq!(rate.in_window(ME, end), 1);
    }

    #[test]
    fn ns2_shed_read_names_the_refresh_and_window_remainder() {
        let mut rate = RecomputeRate::default();
        for n in 0..12 {
            assert!(rate.admit(ME, 1_000 + n, true));
        }
        assert_eq!(rate.retry_after_ms(ME, 20_000), Some(41_000));
        assert_eq!(rate.retry_after_ms(ME, 60_999), Some(1));
        assert_eq!(rate.retry_after_ms(ME, 61_000), None);
        let mut body: Value =
            serde_json::from_str(&owed_body(ME, None, &[], 1_000, false, None)).unwrap();
        let original = body.clone();
        mark_shed_refresh(&mut body, None);
        assert_eq!(body, original);
        mark_shed_refresh(&mut body, rate.retry_after_ms(ME, 20_000));
        assert_eq!(body["refresh"], "shed");
        assert_eq!(body["retryAfterMs"], 41_000);
        assert_eq!(body["v"], 1);
        assert_eq!(body["computedAtMs"], 1_000);
        let code = include_str!("routes.rs");
        assert!(
            code.contains("crate::owed::mark_shed_refresh(&mut body, refresh_retry)"),
            "a shed read must mark its served snapshot"
        );
    }

    #[test]
    fn ns2_filing_runs_a_coalesced_walk_instead_of_amplifying_device_reads() {
        let code = include_str!("record_post.rs");
        let hook = &code[code.find("if !row_written {").unwrap()..];
        assert!(
            hook.contains("owed_recompute_and_push_coalesced"),
            "a filing must walk once before announcing its snapshot"
        );
        assert!(
            !hook.contains("owed_changed_event_body(id, \"filing\", 0"),
            "no bare trigger to every device"
        );
        assert!(
            !hook.contains("owed_identities_by_marker"),
            "SHED-M1: stranger filing must run no victim walk or push"
        );
        assert!(
            hook.contains("OWED_STALE_FOR_POT_SQL"),
            "the victim stays stale for its own read"
        );
        assert!(
            hook.contains("if let Some(id) = identity"),
            "only the filer owns the filing refresh"
        );
    }

    #[test]
    fn ns2_premise_records_the_honest_hand_and_the_bounded_delay() {
        let code = include_str!("owed.rs");
        let comment = &code[code
            .find("/// bsv-low #499: the recomputes ONE identity")
            .unwrap()
            ..code
                .find("pub const OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW")
                .unwrap()];
        assert!(
            comment.contains("19 pushes") && comment.contains("50-second"),
            "the measured honest hand replaces the false premise"
        );
        assert!(
            comment.contains("ceiling + 1") && comment.contains("alarm"),
            "state the bounded deferred rule"
        );
        assert!(!comment.contains("which no honest"));
    }

    #[test]
    fn ns2_shed_and_deferred_counters_name_every_event() {
        let health = owed_health_json();
        assert!(
            health["owed_recompute_shed_total"].is_u64(),
            "every shed has a named counter"
        );
        assert!(
            health["owed_recompute_deferred_total"].is_u64(),
            "every deferred run has a named counter"
        );
        let shed = health["owed_recompute_shed_total"].as_u64().unwrap();
        let deferred = health["owed_recompute_deferred_total"].as_u64().unwrap();
        note_recompute_shed();
        note_recompute_shed();
        note_recompute_deferred();
        let after = owed_health_json();
        assert!(after["owed_recompute_shed_total"].as_u64().unwrap() >= shed + 2);
        assert!(after["owed_recompute_deferred_total"].as_u64().unwrap() > deferred);
        assert_eq!(after["recomputeShed"], after["owed_recompute_shed_total"]);
        assert!(include_str!("routes.rs").contains("crate::owed::note_recompute_deferred();"));
    }

    #[test]
    fn ns2_first_read_cannot_bypass_the_ceiling_without_a_snapshot() {
        let mut rate = RecomputeRate::default();
        for n in 0..12 {
            assert!(rate.admit(ME, 1_000 + n, false));
        }
        assert!(
            !rate.admit(ME, 20_000, false),
            "repeated first-read faults must not bypass the storm bound"
        );
        assert_eq!(rate.in_window(ME, 20_000), 12);
    }

    #[test]
    fn ns2_a_shed_still_arms_when_the_clock_crosses_the_window_end() {
        let mut rate = RecomputeRate::default();
        for n in 0..12 {
            assert!(rate.admit(ME, 1_000 + n, true));
        }
        assert!(!rate.admit(ME, 60_999, true));
        assert_eq!(
            rate.arm_deferred(ME, 61_001),
            Some(61_000),
            "the shed owns the old deadline even when the clock just crossed it"
        );
        assert_eq!(rate.arm_deferred(ME, 61_002), None);
        assert!(rate.admit(ME, 61_003, true));
        assert_eq!(
            rate.arm_deferred(ME, 61_003),
            None,
            "a new unexhausted window has no deferred reservation"
        );
    }

    #[test]
    fn ns2_read_reserves_its_admission_before_sending_the_answer() {
        let code = include_str!("routes.rs");
        let read = &code[code.find("pub async fn owed(req:").unwrap()..];
        let claimed = &read[read.find("Some(token) => {").unwrap()..];
        let ask = claimed
            .find("owed_recompute_admit(&identity, source, true)")
            .unwrap();
        let detached = claimed.find("wait.wait_until").unwrap();
        assert!(ask < detached, "a later ask cannot turn the read's accepted refresh into an unmarked shed after its answer");
    }

    #[test]
    fn ns2_fault_retry_deadline_always_enters_a_later_window() {
        assert_eq!(owed_fault_retry_due(61_000, 61_001), 121_000);
        assert_eq!(owed_fault_retry_due(61_000, 120_999), 121_000);
        assert_eq!(owed_fault_retry_due(61_000, 121_000), 181_000);
    }

    #[test]
    fn ns2_server_floors_retry_after_at_one_second() {
        for delay in [0, 1, 999, 1_000, 41_000] {
            let mut body = json!({"v": 1});
            mark_shed_refresh(&mut body, Some(delay));
            assert_eq!(
                body["retryAfterMs"],
                delay.max(1_000),
                "NOTE-2: retryAfterMs must be at least 1000"
            );
        }
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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
        assert!(
            derive_owed_rows(&inputs(&[], &[], &spent, &v, &c, &pots, Some(972_190))).is_empty()
        );
        // (3) the CHAIN rung says a held pot spent the index-unspent hop (the refused sweep's eviction released it):
        // the game keeps a row, never a press
        let word = chain_confirmed(&key, true, Some(true), Some(&pot), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &pots, Some(972_190));
        i.hop_chain = &word;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].facts["joinSpentBy"], "chain");
        assert_eq!(rows[0].facts["claimable"], false);
        // (4) #793: an index-unspent hop whose chain rung holds a CONFIRMED competitor this index does not know reads
        // unbound (spent elsewhere or could not judge), never a press
        let word = chain_confirmed(&key, true, Some(true), Some(&tx(0x0a)), Some(true));
        let mut i = inputs(&[], &[], &old, &v, &c, &none, Some(972_190));
        i.hop_chain = &word;
        let rows = derive_owed_rows(&i);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].family, OwedFamily::Unbound);
        assert_ne!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["chainProbe"], "spent");
    }

    /// bsv-low #837 (FIX-837) pin (e), the route's wiring of (a2) and (c): the recompute reads the belt before it
    /// derives (each index-unspent hop's seat pots, then the pot's stored bytes, under their two bounds) and hands it to
    /// BOTH derivations; a CUT pass reads the previous snapshot and carries what it did not derive; the read answer
    /// names a carried row at the top. RED with any one of them removed.
    #[test]
    fn fix837_e_the_recompute_wires_the_belt_and_the_shed_carry() {
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = squash(include_str!("routes.rs"));
        for wired in [
            "db.prepare(crate::owed::JOIN_SEAT_POTS_SQL)",
            "letwindow=crate::owed::belt_window(&keys,&chain_named,(now_ms/60_000).max(0)asusize);",
            "ifbeefs.len()>=crate::owed::OWED_JOIN_POT_READS_PER_RECOMPUTE{continue;}",
            ".prepare(crate::owed::JOIN_POT_BEEF_SQL)",
            "crate::owed::join_spends_outpoint(b,&pot,&h.hop_txid,h.hop_vout)",
            "ifbudget_cut{letprevious=matchdb.prepare(crate::owed::OWED_ROWS_READ_SQL)",
            "letshed=crate::owed::carry_shed_rows(&inputs,&mutrows,&prev);",
            "crate::owed::mark_shed_rows_refresh(&mutbody);",
        ] {
            assert!(routes.contains(wired), "the recompute is wired: {wired}");
        }
        assert_eq!(
            routes.matches("join_spent_hops:&join_spent_hops,").count(),
            2,
            "both derivations read the belt"
        );
        let belt = routes
            .find("letmutjoin_spent_hops:HashMap<String,String>=HashMap::new();")
            .unwrap();
        let carry = routes
            .find("letshed=crate::owed::carry_shed_rows(")
            .unwrap();
        let write = routes
            .find("letplan=crate::owed::owed_write_plan(identity_lc,&rows,now_ms,tip,walk_cut);")
            .unwrap();
        assert!(
            belt < routes.find("join_spent_hops:&join_spent_hops,").unwrap(),
            "the belt is read before it is handed on"
        );
        assert!(
            carry < write,
            "the carried rows are written with the snapshot"
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

    /// bsv-low #837, the FIX-837 lens's LOW-2: the belt's window is the newest hop, then the others at a rotating
    /// offset (the recompute's minute), so an identity with more than eight index-unspent hops reaches every one over
    /// successive passes; a hop whose chain rung names a spender is never asked. RED on 9d380f8c: the window was the
    /// newest eight, inline (`belt_window` does not exist; `fix837_e` reds on the route's text).
    #[test]
    fn fix837f_l2_the_belts_window_rotates_and_skips_a_hop_the_chain_named() {
        let hops: Vec<(String, Option<i64>)> = (0..12i64)
            .map(|k| (format!("{}:0", tx(0x40 + k as u8)), Some(1_000 + k)))
            .collect();
        let named: HashSet<String> = [hops[5].0.clone()].into_iter().collect();
        let mut seen: HashSet<usize> = HashSet::new();
        for rotate in 0..12usize {
            let w = belt_window(&hops, &named, rotate);
            assert_eq!(
                w.len(),
                OWED_JOIN_SEAT_HOPS_PER_RECOMPUTE,
                "rotate {rotate}: {w:?}"
            );
            assert_eq!(w[0], 11, "the newest keeps the first slot");
            assert!(!w.contains(&5), "the chain-named hop is never asked");
            let distinct: HashSet<usize> = w.iter().copied().collect();
            assert_eq!(distinct.len(), w.len(), "no hop twice in one window");
            seen.extend(w);
        }
        assert_eq!(
            seen.len(),
            11,
            "every hop the chain did not name is reached: {seen:?}"
        );
        // a short list is asked whole, newest first; an empty one asks nothing
        let few = &hops[..3];
        assert_eq!(belt_window(few, &HashSet::new(), 0), vec![2, 1, 0]);
        assert_eq!(
            belt_window(few, &HashSet::new(), 7),
            vec![2, 0, 1],
            "the rest rotate"
        );
        assert!(belt_window(&[], &HashSet::new(), 0).is_empty());
    }

    /// bsv-low #837, the FIX-837 lens's LOW-4: the carry never takes the rows past `OWED_MAX_ROWS` (it runs after the
    /// pass's truncation), so the write plan's bound holds by construction. RED on 9d380f8c: 499 derived rows and ten
    /// carried were 509.
    #[test]
    fn fix837f_l4_the_carry_stops_at_the_row_cap() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let i = inputs(&[], &[], &[], &v, &c, &p, Some(972_190));
        let row = |k: usize| OwedRow {
            identity: ME.to_string(),
            outpoint: format!("{k:064x}:0"),
            family: OwedFamily::RefundDue,
            game_id: format!("{k:064x}"),
            sats: Some(19_800),
            opponent_identity: Some(OPP.to_string()),
            at_height: None,
            facts: json!({"claim": "present-refund", "claimable": true}),
            reason: None,
        };
        let mut rows: Vec<OwedRow> = (0..OWED_MAX_ROWS - 1).map(row).collect();
        let previous: Vec<(OwedRow, i64)> = (10_000..10_010)
            .map(|k| (row(k), i.now_ms - 1_000))
            .collect();
        let carried = carry_shed_rows(&i, &mut rows, &previous);
        assert_eq!(rows.len(), OWED_MAX_ROWS, "carried {carried}");
        assert_eq!(carried, 1);
        let mut full: Vec<OwedRow> = (0..OWED_MAX_ROWS).map(row).collect();
        assert_eq!(carry_shed_rows(&i, &mut full, &previous), 0);
        assert_eq!(full.len(), OWED_MAX_ROWS);
    }

    /// bsv-low #837 (FIX-837) pin (c): a pass the budget CUT never serves fewer rows than the snapshot it replaces
    /// without marking each missing row shed. Loop 44 pair 1: seven rows served, then every cut pass served six and the
    /// game's refund row was gone. The carried row is the previous one, `refresh: "shed"`, `stale`, `retryAfterMs`,
    /// no press (its word kept as `claimableBeforeShed`, through a second cut too); a row judged past the keep window
    /// or a verified-collected game's row is not carried; the answer serving one says `refresh: "shed"` at the top.
    #[test]
    fn fix837_d_a_budget_cut_pass_carries_each_row_it_did_not_derive_as_shed() {
        let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
        let i = inputs(&[], &[], &[], &v, &c, &p, Some(972_190));
        let row = |op: u8, game: u8, family: OwedFamily, claimable: bool| OwedRow {
            identity: ME.to_string(),
            outpoint: format!("{}:0", tx(op)),
            family,
            game_id: tx(game),
            sats: Some(19_800),
            opponent_identity: Some(OPP.to_string()),
            at_height: None,
            facts: json!({"claim": "present-refund", "claimable": claimable}),
            reason: None,
        };
        let now = i.now_ms;
        // the previous snapshot: seven rows, the game's refund row among them
        let previous: Vec<(OwedRow, i64)> = (0x20..0x27u8)
            .map(|b| (row(b, b, OwedFamily::RefundDue, true), now - 60_000))
            .collect();
        // the cut pass derived six of them (the newest game's refund row, 0x26, missing)
        let mut rows: Vec<OwedRow> = (0x20..0x26u8)
            .map(|b| row(b, b, OwedFamily::RefundDue, true))
            .collect();
        let carried = carry_shed_rows(&i, &mut rows, &previous);
        assert_eq!(carried, 1);
        assert_eq!(
            rows.len(),
            previous.len(),
            "never fewer rows than the last snapshot"
        );
        let shed = rows
            .iter()
            .find(|r| r.outpoint == format!("{}:0", tx(0x26)))
            .unwrap();
        assert_eq!(shed.family, OwedFamily::RefundDue, "the row as it was");
        assert_eq!(shed.facts["refresh"], OWED_REFRESH_SHED);
        assert_eq!(shed.facts["stale"], true);
        assert_eq!(
            shed.facts["claimable"], false,
            "a stale word is shown, never pressed"
        );
        assert_eq!(shed.facts["claimableBeforeShed"], true);
        assert_eq!(shed.facts["retryAfterMs"], OWED_SHED_RETRY_MS);
        assert_eq!(shed.facts["judgedAtMs"], now - 60_000);
        // a second cut pass carries it again with its first judgement and its first word
        let again: Vec<(OwedRow, i64)> = vec![(shed.clone(), now)];
        let mut rows2: Vec<OwedRow> = Vec::new();
        assert_eq!(carry_shed_rows(&i, &mut rows2, &again), 1);
        assert_eq!(rows2[0].facts["claimableBeforeShed"], true);
        assert_eq!(rows2[0].facts["judgedAtMs"], now - 60_000);
        // the answer says so at the top
        let mut body = json!({"rows": [{"facts": rows2[0].facts.clone()}]});
        mark_shed_rows_refresh(&mut body);
        assert_eq!(body["refresh"], "shed");
        assert_eq!(body["retryAfterMs"], OWED_SHED_RETRY_MS);
        // not carried: a row judged past the keep window, a verified-collected game's row
        let old = vec![(
            row(0x30, 0x30, OwedFamily::RefundDue, true),
            now - OWED_WITHHELD_KEEP_MAX_MS - 1,
        )];
        assert_eq!(carry_shed_rows(&i, &mut Vec::new(), &old), 0);
        let collected: HashSet<String> = [tx(0x31)].into_iter().collect();
        let mut j = inputs(&[], &[], &[], &v, &collected, &p, Some(972_190));
        j.now_ms = now;
        let done = vec![(row(0x31, 0x31, OwedFamily::Payout, true), now - 1_000)];
        assert_eq!(carry_shed_rows(&j, &mut Vec::new(), &done), 0);
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
        let read2 = derive_owed_rows(&i2);
        assert_eq!(read2[0].family, OwedFamily::Unbound);
        assert!(read2[0].facts["claim"].is_null());
        assert_eq!(read2[0].facts["chainProbe"], "spent");
        assert_eq!(read2[0].facts["chainSpender"], competitor);
    }

    /// PIN A4 (RED on the stub): the budget holds, and the read path reads the CACHED present tip. N identities each
    /// with a claimable stranded hop, one tip: each admits ≤ the ceiling per window + exactly one reserved deferred
    /// walk (the #648 ceiling+1), past which the read sheds. Nothing per request on the read path beyond the 30-s
    /// isolate-cached tip (`beef_guard::present_tip`), never the uncached `chaintracks_present_height`.
    ///
    /// bsv-low #669 NOTE-6 (the FIX-669 lens): the tip-cache part is now a BEHAVIORAL pin through the production
    /// reader (`beef_guard::tip_cache_hit`, the exact cache-hit decision `present_tip` makes), primed vs cold —
    /// replacing the brittle `include_str!` source-grep slice. A HARDENED structural belt (the owed read references
    /// `present_tip` + `HopStranded`) is kept, but its end-marker is now REQUIRED (`.expect`), so a renamed
    /// `gather_result_entries` fails loudly instead of silently widening the slice to the rest of the file.
    #[test]
    fn pin669_a4_the_budget_holds_and_the_read_reads_the_cached_tip() {
        let mut rate = RecomputeRate::default();
        let ids: Vec<String> = (0u8..8).map(|n| format!("id-stranded-{n}")).collect();
        for id in &ids {
            for k in 0..OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW {
                assert!(
                    rate.admit(id, 1_000 + k as i64, true),
                    "{id}: walk {k} within the ceiling"
                );
            }
            assert!(
                !rate.admit(id, 2_000, true),
                "{id}: past the ceiling the read sheds"
            );
            assert_eq!(
                rate.arm_deferred(id, 2_001),
                Some(61_000),
                "{id}: one reserved deferred walk (ceiling + 1)"
            );
            assert_eq!(
                rate.arm_deferred(id, 2_002),
                None,
                "{id}: no second reservation"
            );
            assert_eq!(
                rate.in_window(id, 2_000),
                OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW
            );
        }
        // NOTE-6 behavioral pin: the owed read's hop-stranded freshness-gate tip is `beef_guard::present_tip`, whose
        // per-isolate 30-s cache decides a HIT through `tip_cache_hit`. PRIMED (a latched, unexpired entry) returns
        // the held tip with ZERO subrequests; COLD (empty) or EXPIRED returns None and `present_tip` falls to
        // chaintracks (at most one such call per 30 s per isolate — strictly cheaper than a per-request uncached
        // read). TTL is 30 s (`beef_guard::PRESENT_TIP_TTL_MS`); `now` is an arbitrary isolate clock.
        let now = 1_792_000_000_000.0;
        assert_eq!(
            crate::beef_guard::tip_cache_hit(Some((971_115, now + 30_000.0)), now),
            Some(971_115),
            "a primed, unexpired isolate serves the owed read's tip from cache with zero chaintracks calls"
        );
        assert_eq!(
            crate::beef_guard::tip_cache_hit(Some((971_115, now - 1.0)), now),
            None,
            "an EXPIRED entry is a miss: present_tip re-reads chaintracks (self-heals to the new tip)"
        );
        assert_eq!(
            crate::beef_guard::tip_cache_hit(None, now),
            None,
            "a COLD isolate is a miss: present_tip reads chaintracks once, then caches"
        );
        // Hardened structural belt (NOT the behavioral proof — A1/A2/A3/D6/M1 are): the owed read still WIRES the
        // cached tip for a claimable hop-stranded row. The end-marker is REQUIRED, so a renamed `gather_result_
        // entries` is a loud RED here, never a silent widening of the slice to the rest of the file (the lens's
        // brittleness).
        let code = include_str!("routes.rs");
        let owed_start = code
            .find("pub async fn owed(req:")
            .expect("the owed read handler");
        let owed_end = code[owed_start..]
            .find("\nasync fn gather_result_entries")
            .expect("owed read end marker `gather_result_entries` renamed; re-anchor this pin");
        let read = &code[owed_start..owed_start + owed_end];
        assert!(
            read.contains("beef_guard::present_tip"),
            "the owed read's tip must be the 30-s isolate cache, not the uncached chaintracks_present_height"
        );
        assert!(
            read.contains("OwedFamily::HopStranded"),
            "the read must read the tip for a claimable hop-stranded row (needs_tip), to gate its sweep"
        );
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

    /// PIN D6 (RED on the base): `should_recompute` fires on a moved tip for a claimable hop-stranded row — the
    /// serve-then-refresh re-probe runs on the block, not only after the 5-minute open age. A tip that has not moved
    /// does not kick it (no churn). RED line: the first `assert!(should_recompute(…))`.
    #[test]
    fn pin669_d6_should_recompute_fires_on_a_moved_tip_for_a_claimable_hop_stranded_row() {
        let rows = stranded_sweep_rows(971_114);
        assert!(
            should_recompute(false, 10_000, Some(971_114), Some(971_115), &rows),
            "a block must kick the re-probe of a claimable stranded hop (gate_flipped), not wait 5 minutes"
        );
        assert!(
            !should_recompute(false, 10_000, Some(971_115), Some(971_115), &rows),
            "a tip that has not moved does not churn the recompute"
        );
    }

    /// PIN M1 (the FIX-669 lens's MEDIUM-1; RED on the pre-fold route wiring): a NULL stored `owed_state.tip` must
    /// reach the freshness gate as `None`, so a claimable stranded hop served across a block DEGRADES to pending —
    /// never a dead sweep. The route serves `prev_tip.or(tip_now)` as the BODY's display tip (unchanged: a NULL
    /// stored tip still shows the present tip), but `owed_served_tips` decouples the GATE's snapshot tip (`prev_tip`,
    /// here `None`) from it. The pre-fold route fed the gate the display tip, collapsing `None` to `tip_now`, so the
    /// serve read `(Some(tn), Some(tn))` -> `Degrade::TipCurrent` and the `Degrade::Unknown` fail-safe arm was DEAD.
    /// RED line (demonstrated by reverting `owed_served_tips` to the route's pre-fold `(prev_tip.or(tip_now),
    /// prev_tip.or(tip_now))`): `assert_eq!(gate_tip, None, ...)` gets `Some(971_115)`, and the degrade's
    /// `assert_eq!(rows[0].facts["claimable"], false)` gets `true`.
    #[test]
    fn pin669_m1_a_null_stored_tip_reaches_the_gate_as_none_and_degrades() {
        // the identity's FIRST owed compute coincided with a chaintracks blip: owed_state.tip is SQL NULL.
        let prev_tip: Option<u64> = None;
        let tip_now: Option<u64> = Some(971_115); // the present tip this request read (beef_guard::present_tip)
        let (display_tip, gate_tip) = owed_served_tips(prev_tip, tip_now);
        assert_eq!(
            display_tip,
            Some(971_115),
            "the body still shows the present tip for a NULL stored snapshot (display unchanged)"
        );
        assert_eq!(
            gate_tip, None,
            "the gate sees the TRUE stored snapshot tip (None), never the collapsed display tip"
        );
        // the serve path (snapshot not fresh) feeds the GATE tip, not the display tip:
        let mut rows = stranded_sweep_rows(971_114);
        degrade_stale_sweeps(&mut rows, gate_tip, tip_now);
        assert_eq!(
            rows[0].facts["claimable"], false,
            "a NULL stored tip cannot prove tip-current -> pending (never a dead sweep across a block)"
        );
        assert!(rows[0].facts["claim"].is_null());
        assert_eq!(rows[0].facts["chainWait"], "block");
        assert_eq!(rows[0].facts["tipUnknown"], true);
        // a stored tip equal to the present tip is still tip-current: the body unchanged, the claim stands.
        let (d2, g2) = owed_served_tips(Some(971_115), Some(971_115));
        assert_eq!((d2, g2), (Some(971_115), Some(971_115)));
        let mut rows = stranded_sweep_rows(971_115);
        degrade_stale_sweeps(&mut rows, g2, Some(971_115));
        assert_eq!(
            rows[0].facts["claimable"], true,
            "a tip-current stored snapshot is not degraded"
        );
    }

    /// PIN M1b (characterization of the self-heal rule MEDIUM-1 leans on; GREEN on the pre-fold tip — it states the
    /// EXISTING recompute/write behavior, unchanged by this fold). A recompute STORES the tip it computed at:
    ///   - chaintracks answered -> `owed_write_plan` binds `Some(height)` into `OWED_STATE_UPSERT_SQL` (the gate then
    ///     serves it tip-current, or degrades it on the next block);
    ///   - chaintracks did not answer AND a prior tip existed -> `owed_recompute_inner` bails BEFORE the write
    ///     (`routes.rs`: "the previous rows stand"), so a good tip is never overwritten with NULL;
    ///   - chaintracks did not answer AND no prior tip (a FIRST compute under a blip) -> the snapshot stores NULL,
    ///     which (a) the read-side gate degrades fail-safe (MEDIUM-1), and (b) `should_recompute` re-arms on the NEXT
    ///     readable tip (`gate_flipped` fires for a claimable hop-stranded row when `prev_tip` is None < `tip_now`),
    ///     so the next read with chaintracks back up stores a real tip. The NULL state is gone after one such read.
    #[test]
    fn pin669_m1b_a_recompute_stores_the_tip_it_computed_at_and_a_null_tip_self_heals() {
        let rows = stranded_sweep_rows(971_115);
        // chaintracks answered: the state upsert binds the computed height.
        let plan = owed_write_plan(ME, &rows, 1_792_000_000_000, Some(971_115), false);
        let binds = &plan
            .iter()
            .find(|(sql, _)| *sql == OWED_STATE_UPSERT_SQL)
            .expect("the state upsert is in the write plan")
            .1;
        assert_eq!(
            binds[2],
            OwedBind::Int(971_115),
            "a recompute stores the tip chaintracks answered"
        );
        // chaintracks did not answer on a first compute: the upsert binds NULL (fail-safe at read, re-armed next tip).
        let plan = owed_write_plan(ME, &rows, 1_792_000_000_000, None, false);
        let binds = &plan
            .iter()
            .find(|(sql, _)| *sql == OWED_STATE_UPSERT_SQL)
            .expect("the state upsert is in the write plan")
            .1;
        assert_eq!(
            binds[2],
            OwedBind::Null,
            "a tip-less first compute stores NULL (the degrade + the re-arm self-heal it)"
        );
        // the re-arm: a NULL prev_tip claimable hop-stranded snapshot recomputes the moment a tip is readable.
        assert!(
            should_recompute(false, 10_000, None, Some(971_115), &rows),
            "a NULL-tip claimable stranded snapshot re-arms on the next readable tip (it stores a real tip)"
        );
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
        derive_owed_rows(&i)
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
        let rows = derive_owed_rows(&i);
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
        let rows = derive_owed_rows(&i);
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

    /// PIN 669b-E (RED on `9330dbe6` via the precondition): the fail-safe (MEDIUM-1 parity) covers the new row too — a
    /// present tip that cannot be read degrades it; and a NULL-snapshot claimable index-unspent row re-arms on the
    /// next readable tip, so a chaintracks blip never holds an honest press forever.
    #[test]
    fn pin669b_e_a_cold_or_null_tip_degrades_and_rearms() {
        let mut rows = index_unspent_no_word_rows(971_132);
        degrade_stale_sweeps(&mut rows, Some(971_132), None); // present tip unknown (cold isolate / chaintracks down)
        assert_eq!(
            rows[0].facts["claimable"], false,
            "cannot prove tip-current -> pending (fail-safe)"
        );
        assert_eq!(rows[0].facts["chainWait"], "block");
        assert_eq!(rows[0].facts["tipUnknown"], true);
        // the re-arm: a NULL prev_tip claimable index-unspent stranded snapshot recomputes on the next readable tip.
        let rows2 = index_unspent_no_word_rows(971_132);
        assert!(
            should_recompute(false, 10_000, None, Some(971_132), &rows2),
            "a NULL-tip claimable index-unspent stranded snapshot re-arms (it does not hold past one re-probe cycle)"
        );
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
        let mut rows = derive_owed_rows(&i);
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

    /// PIN 808-A (RED on the base's rule: `memo_predates_tip` answering `false`, i.e. no tip rule): the loop-41 memo,
    /// "unspent" taken at 971993 and 277,950 ms old, read by a walk at 971994. Within the budget it is NOT an answer:
    /// the hop is asked again. Past the budget its word withholds the sweep: a pending chain-wait, never `claimable`,
    /// at a snapshot the read-time gate calls tip-current. A fresh probe at the served tip restores the press (still
    /// unspent) or re-points the hop to the competitor (spent).
    #[test]
    fn pin808_a_a_memo_taken_below_the_walks_tip_is_asked_again_and_never_offers_the_sweep() {
        use crate::hops_view::probe_memo_at_tip;
        let conn = memo_db();
        let (h, now) = (971_993u64, 10_000_000i64);
        let taken = now - 277_950;
        let memo = probe_memo_at_tip(&tx(0x07), 0, &unspent_probe808(), taken, Some(h)).unwrap();
        write_memos(&conn, &[memo]);
        // the same memo at its own tip answers (the five-minute window holds): the precondition of the incident
        let at_h = walk808(&conn, h, true, now).expect("a memo at the walk's tip answers");
        assert_eq!(
            served808(&at_h, h)[0].facts["claimable"],
            true,
            "precondition"
        );
        // within the budget, at H+1: asked again
        assert_eq!(
            walk808(&conn, h + 1, true, now),
            Err(()),
            "a memo taken at {h} is no answer at {}: the hop is re-probed within the walk",
            h + 1
        );
        // past the budget, at H+1, on an index-UNKNOWN hop: the old word withholds (nothing else says unspent)
        let past = walk808(&conn, h + 1, false, now).expect("the past-budget word");
        let rows = served808_as(&past, h + 1, HopStatus::Unknown);
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(
            rows[0].facts["claimable"], false,
            "a memo taken before the block never offers an index-unknown hop's sweep"
        );
        assert!(rows[0].facts["claim"].is_null());
        assert_eq!(rows[0].facts["chainWait"], "block");
        assert_eq!(rows[0].facts["tipMoved"], true);
        assert_eq!(rows[0].facts["chainProbe"], "unspent-predates-tip");
        assert_eq!(rows[0].facts["chainProbeTip"], h);
        assert_eq!(rows[0].facts["servedTip"], h + 1);
        assert_eq!(rows[0].facts["chainProbeAgeMs"], 277_950);
        assert_eq!(rows[0].reason.as_deref(), Some(SWEEP_TIP_MOVED_REASON));
        assert!(
            row_is_open(&rows[0]),
            "an open row: the next cadence re-derives it"
        );
        // past the budget on an index-UNSPENT hop (the lens's MEDIUM-1 (b)): served as silence is, its staleness stated
        let rows = served808(&past, h + 1);
        assert_eq!(rows[0].family, OwedFamily::HopStranded);
        assert_eq!(rows[0].facts["chainProbe"], "index-unspent");
        assert_eq!(rows[0].facts["claim"], "sweep-hop");
        assert_eq!(
            rows[0].facts["claimable"], true,
            "an out-of-date unspent is more evidence than none: the honest Collect is not held by the walk's budget"
        );
        assert_eq!(rows[0].facts["tipMoved"], true);
        assert_eq!(rows[0].facts["chainProbeTip"], h);
        assert_eq!(rows[0].facts["servedTip"], h + 1);
        assert_eq!(rows[0].facts["chainProbeAgeMs"], 277_950);
        // the fresh probe at the served tip, written back as the route writes it (bound to the walk's tip)
        let still = probe_memo_at_tip(&tx(0x07), 0, &unspent_probe808(), now, Some(h + 1)).unwrap();
        write_memos(&conn, &[still]);
        let fresh = walk808(&conn, h + 1, false, now + 1_000).expect("answered");
        let rows = served808(&fresh, h + 1);
        assert_eq!(
            rows[0].facts["claim"], "sweep-hop",
            "a fresh probe at the served tip restores the press"
        );
        assert_eq!(rows[0].facts["claimable"], true);
        assert_eq!(rows[0].facts["chainProbe"], "unspent");
        assert_eq!(rows[0].facts["chainProbeTip"], h + 1);
        let competitor = crate::hops_view::ChainSpendProbe {
            known: true,
            spent: Some(true),
            spending_txid: Some(tx(0x5d)),
            spent_confirmed: Some(true),
        };
        let spent = probe_memo_at_tip(&tx(0x07), 0, &competitor, now + 2_000, Some(h + 1)).unwrap();
        write_memos(&conn, &[spent]);
        let rows = served808(&walk808(&conn, h + 1, false, now + 3_000).unwrap(), h + 1);
        assert_eq!(
            rows[0].facts["chainProbe"], "spent",
            "the competitor's word re-points the hop"
        );
        assert_ne!(rows[0].facts["claimable"], true);
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

    /// PIN 808-D (the FIX-808 lens's MEDIUM-1, RED on `e487ee1f`: the two hops past the eighth slot, and every hop
    /// past the 12 s clock, were withheld): ten index-unspent stranded hops, every memo "unspent" at H, one block, a
    /// walk at H+1. The split sends all ten back; the first `OWED_PROBES_PER_RECOMPUTE` are asked FIRST (their fresh
    /// word at H+1 offers), the rest take the past-budget word and are offered as silence is (`index-unspent`,
    /// `tipMoved`). Past the clock (no ask at all) every hop is offered the same way. The same ten hops index-UNKNOWN:
    /// the asked ones offer on their fresh word, the past-budget ones stay pending.
    #[test]
    fn pin808_d_past_the_budget_every_index_unspent_hop_is_asked_or_offered() {
        use crate::hops_view::{
            probe_memo_at_tip, split_probe_targets_at_tip, stale_memo_word,
            PROBE_MEMO_CONFIRMED_MAX_AGE_MS, PROBE_MEMO_MAX_AGE_MS,
        };
        let (h, now) = (971_993u64, 10_000_000i64);
        let targets: Vec<(String, u32)> = (0..10u8).map(|b| (tx(0x60 + b), 0)).collect();
        let memos: Vec<_> = targets
            .iter()
            .map(|(t, v)| {
                probe_memo_at_tip(t, *v, &unspent_probe808(), now - 60_000, Some(h)).unwrap()
            })
            .collect();
        let (answered, to_probe) = split_probe_targets_at_tip(
            &targets,
            &memos,
            now,
            PROBE_MEMO_MAX_AGE_MS,
            PROBE_MEMO_CONFIRMED_MAX_AGE_MS,
            None,
            Some(h + 1),
        );
        assert!(answered.is_empty(), "a block: every unspent memo predates");
        assert_eq!(
            to_probe.len(),
            10,
            "within the budget each is asked again first"
        );
        // the walk's loop: `slots` fresh asks, the rest the past-budget word
        let walk = |slots: usize| -> HashMap<String, HopChainWord> {
            let mut chain = HashMap::new();
            for (n, (t, v)) in to_probe.iter().enumerate() {
                let key = format!("{t}:{v}");
                let word = if n < slots {
                    HopChainWord {
                        looked: true,
                        spent: Some(false),
                        spending_txid: None,
                        spent_confirmed: None,
                        stale: false,
                        age_ms: None,
                        probe_tip: Some(h + 1),
                        predates_tip: false,
                    }
                } else {
                    let m = memos
                        .iter()
                        .find(|m| m.outpoint == format!("{t}.{v}"))
                        .unwrap();
                    stale_memo_word(m, now, None, Some(h + 1))
                };
                chain.insert(key, word);
            }
            chain
        };
        let served = |chain: &HashMap<String, HopChainWord>, status: HopStatus| {
            let (v, c, p) = (HashMap::new(), HashSet::new(), HashSet::new());
            let hops: Vec<HopEntry> = targets
                .iter()
                .map(|(t, _)| {
                    let mut e = hop(HopStatus::Unspent, None, Some(HOP_STRANDED_AFTER_MS + 1));
                    e.hop_txid = t.clone();
                    if status == HopStatus::Unknown {
                        e.status = status;
                        e.spent = None;
                    }
                    e
                })
                .collect();
            let mut i = inputs(&[], &[], &hops, &v, &c, &p, Some(h + 1));
            i.hop_chain = chain;
            let mut rows = derive_owed_rows(&i);
            degrade_stale_sweeps(&mut rows, Some(h + 1), Some(h + 1));
            rows
        };
        for (slots, label) in [
            (OWED_PROBES_PER_RECOMPUTE, "the eighth slot"),
            (0, "the 12 s clock"),
        ] {
            let chain = walk(slots);
            let rows = served(&chain, HopStatus::Unspent);
            assert_eq!(rows.len(), 10);
            for r in &rows {
                assert_eq!(
                    r.facts["claimable"], true,
                    "past {label}: an index-unspent hop is asked or offered, never held by the budget: {:?}",
                    r.facts
                );
            }
            let offered_as_silence = rows
                .iter()
                .filter(|r| r.facts["chainProbe"] == "index-unspent" && r.facts["tipMoved"] == true)
                .count();
            assert_eq!(offered_as_silence, 10 - slots, "past {label}");
            let rows = served(&chain, HopStatus::Unknown);
            let pending = rows.iter().filter(|r| r.facts["claimable"] != true).count();
            assert_eq!(
                pending,
                10 - slots,
                "past {label}: an index-unknown hop's out-of-date word still withholds"
            );
        }
    }

    /// PIN 808-E (the FIX-808 lens's LOW-1, RED on `e487ee1f`: the re-ask took the isolate's 15 s cache): a target the
    /// tip rule sent back accepts no cached `/spent-any` answer; a never-probed target, a memo at the tip, a spent memo
    /// and a tipless walk keep the cache.
    #[test]
    fn pin808_e_the_re_ask_the_tip_rule_sends_back_takes_no_cached_word() {
        use crate::hops_view::{probe_memo_at_tip, reask_cache_max_age_ms};
        use crate::results::{spent_any_cache_fresh, SPENT_ANY_CACHE_TTL_MS};
        let (h, now) = (971_993u64, 10_000_000i64);
        let (t, t2) = (tx(0x07), tx(0x08));
        let below = probe_memo_at_tip(&t, 0, &unspent_probe808(), now - 60_000, Some(h)).unwrap();
        let max_age = reask_cache_max_age_ms(std::slice::from_ref(&below), &t, 0, Some(h + 1));
        assert_eq!(
            max_age, 0.0,
            "a memo below the walk's tip: the couriers are asked"
        );
        assert!(
            !spent_any_cache_fresh(now as f64 - 1.0, now as f64, max_age),
            "a word cached one millisecond before is not taken"
        );
        assert_eq!(
            reask_cache_max_age_ms(std::slice::from_ref(&below), &t2, 0, Some(h + 1)),
            SPENT_ANY_CACHE_TTL_MS,
            "a never-probed target keeps the cache"
        );
        assert_eq!(
            reask_cache_max_age_ms(std::slice::from_ref(&below), &t, 0, Some(h)),
            SPENT_ANY_CACHE_TTL_MS,
            "a memo at the walk's tip (re-asked by age) keeps the cache"
        );
        assert_eq!(
            reask_cache_max_age_ms(std::slice::from_ref(&below), &t, 0, None),
            SPENT_ANY_CACHE_TTL_MS,
            "no walk tip: no tip rule"
        );
        let spent = crate::hops_view::ChainSpendProbe {
            known: true,
            spent: Some(true),
            spending_txid: Some(tx(0x5d)),
            spent_confirmed: None,
        };
        let spent_below = probe_memo_at_tip(&t, 0, &spent, now - 60_000, Some(h)).unwrap();
        assert_eq!(
            reask_cache_max_age_ms(&[spent_below], &t, 0, Some(h + 1)),
            SPENT_ANY_CACHE_TTL_MS
        );
    }
}
