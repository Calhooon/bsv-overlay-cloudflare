//! `/hops-view` — the per-identity HOPS-IN-FLIGHT view (bsv-low #315, #252
//! stage 2b).
//!
//! The funding HOP (the staging P2PKH coin a seat pays to its own
//! `[2,'low settle']` key before the JOIN assembles the pot) previously had
//! NO identity-keyed server row: `tm_lowfund` indexes the bare outpoint and
//! `potparty_records` only fills at JOIN-assembly, so a seat that funded a
//! hop and died pre-JOIN was invisible to every per-identity view (the
//! #256 ~80.8k-sat class). Stage 2b adds the `LOW/hopparty/v1` marker,
//! which RIDES THE HOP TRANSACTION as a second output (indexed by
//! `tm_hopparty` → `hopparty_records`): the marker names its hop by VOUT
//! only and the containing tx supplies the txid, so the hop outpoint is
//! `(hopparty_records.txid, hopVout)`. This view joins those rows to the
//! `tm_lowfund`-indexed hop outpoint and answers, for one identity: which
//! hops did you mark, are they spent (JOIN / hop sweep) or still in
//! flight, and does each marker actually VERIFY.
//!
//! ## Posture (the `/results` / `/refund-view` template)
//!
//! One identity bind, one bounded D1 query, fail-safe-empty 200 on a bad
//! identity, 503 only on a D1 fault, `no-store`, tip in body, honesty
//! pairs, `unknown` first-class. Join key with `/live-view` semantics is
//! `identity` + `gameId` (each entry carries the marker's `gameId` so a
//! client can correlate a hop with a live hand).
//!
//! ## `status`/`statusSource` (the spent/unspent honesty pair)
//!
//! | facts                                                   | status    | source |
//! |---------------------------------------------------------|-----------|--------|
//! | `pot_records` row, spent = 0                            | `unspent` | chain  |
//! | row, spend recorded + confirmed (shared #323 bar)       | `spent`   | chain  |
//! | row, spend recorded but UNCONFIRMED (displaceable)      | `unknown` | null   |
//! | NO `pot_records` row (hop never indexed)                | `unknown` | null   |
//!
//! An absent `pot_records` row is `unknown`, NEVER asserted-unspent — the
//! overlay may simply never have seen the hop tx. `spent = 0` is likewise
//! the overlay's NON-observation of a spend on an indexed outpoint, not a
//! UTXO existence proof (the raw facts ride alongside either way). The
//! confirmation bar is the ONE shared bar
//! (`logic::is_confirmed_landing_with_proof` — `spentConfirmed` OR a
//! chaintracks-verified spender proof), so this view can never disagree
//! with `/refund-view`/`/results` on what counts as a landing.
//!
//! ## `markerVerified` — a LATCHED verdict, read (bsv-low #362)
//!
//! **This route does no cryptography.** It reads
//! `hopparty_records.markerValid`, a verdict the OVERLAY computed once, at
//! admission, when the whole container was in hand
//! (`overlay_discovery::hopparty::validity::record_marker_valid`), and it
//! sorts by it.
//!
//! It did not always. Until #362 this file verified two ECDSA signatures
//! plus a BRC-42 derivation plus the container bars **per row, per
//! request**, which cannot be unbounded, so it carried a rationing constant
//! (`HOPS_VIEW_VERIFY_BUDGET = 150`) and everything downstream of it: a
//! third verdict meaning "I did not get to check this", a body bit to report
//! it, a consumer obliged to handle it, and a client display that counted
//! only `verified` rows so an unchecked row rendered identically to an
//! ABSENT one. **A budget on a correctness check means the check is on the
//! wrong side of the system** (epoch Rule 25) — reads scale with readers,
//! admissions scale with writers, and under read-time verification a junk
//! row an attacker files ONCE makes every honest reader pay the ECDSA to
//! reject it, forever. Moving it to admission puts the cost on the
//! submitter, beside the on-chain fee they already paid, and five of those
//! six artifacts stop being representable rather than getting fixed.
//!
//! MEASURED, on the frozen golden marker, release build, native: a full
//! 150-row budget cost **~47 ms of CPU per request**; the latched read costs
//! **~107 ns** for the same 150 rows. That is ~4.4e5x, and it was per
//! request, per reader, on a public unauthenticated route — the #314
//! MEDIUM-B read-time CPU class, closed rather than bounded.
//!
//! The three bars the latch decided — all against facts the CONTAINER
//! itself supplies, none of them a claim — are documented at
//! `overlay_discovery::hopparty::validity::marker_valid`: the hop LOCK
//! (`P2PKH(hash160(seatSettlePubkey))` at `hopVout`, hash-bound by the
//! txid), the hop VALUE, and BOTH signatures.
//!
//! Three-valued still, and every value is served and LABELLED:
//!
//! - `verified`   — `markerValid = 1`: every bar cleared at admission;
//! - `unverified` — `markerValid = 0`: at least one bar FAILED. Still
//!   served, never dropped — a refuted row's existence is the reader's to
//!   know (epoch Rule 23);
//! - `unknown`    — `markerValid IS NULL`: the row was admitted BEFORE the
//!   #362 migration and was never evaluated. This is now the ONLY producer
//!   of the word: it means "this row predates the latch", never "the budget
//!   ran out", and it is not per-request, not attacker-inducible, and not
//!   ambiguous about what happened.
//!
//! **The legacy tier has exactly ONE retirement path — the re-latch pass
//! (bsv-low#367, `bsv_overlay_cloudflare::relatch`, on the overlay's cron).**
//! It does not self-heal and must never be described as doing so: a hopparty
//! marker must ride the hop transaction, and that transaction is already on
//! chain, so no republish can re-latch an old row. The pass is bounded per
//! tick, so a row reads `unknown` until a sweep REACHES it — "the pass
//! exists" is not "this row is latched".
//!
//! The verdict is a **leading SORT KEY and never a `WHERE`**. Both
//! signatures ride back in the body so the client re-verifies; the server's
//! label is display, never an authority.
//!
//! ### What the container buys, and the one residual it leaves
//!
//! Because the marker rides the hop tx, `SIGHASH_ALL` makes the funding
//! wallet's signature commit the marker bytes: **the entity that funded
//! the hop provably authored the marker.** Neither digest can bind the
//! containing txid (a tx cannot embed its own txid), so what remains is a
//! PAID REPLAY, analysed at [`assemble_hops_view`].
//!
//! ## The window (the #281 shape, SUPERSET per outpoint)
//!
//! `hopparty_records.identity` is attacker-writable (byte-format
//! admission), so the SQL reuses the identity-window discipline: `limit`
//! counts HOP OUTPOINTS ([`HOPS_VIEW_MAX_OUTPOINTS`], keyed
//! `(txid, hopVout)`), a hop-existence tier against `pot_records` demotes
//! markers naming never-indexed hops
//! (with [`HOPS_VIEW_UNKNOWN_HOP_QUOTA`] promoted slots so a genuinely
//! fresh hop whose `tm_lowfund` admission is in flight is not hidden), and
//! each outpoint serves up to [`HOPS_VIEW_ROWS_PER_OUTPOINT`] OLDEST rows —
//! a SUPERSET, never a one-row collapse, so a forged marker cannot evict
//! the honest one from its own outpoint (the potparty rn=1 lesson,
//! executed in the #281 gate). The window fetches ONE outpoint beyond the
//! page so the body's `truncated` bit is honest (a flood is DETECTABLE,
//! never a complete-looking partial answer).
//!
//! Since #362 the window's LEADING key is the latched verdict, aggregated
//! per outpoint — which is what makes verification happen BEFORE paging,
//! and is the thing the pre-#362 design structurally could not do (see
//! [`assemble_hops_view`] for the re-priced residual it retires).

use serde_json::json;

/// Rows served per hop OUTPOINT — the overlay window constant, imported so
/// the two crates cannot drift (Rule 16: share the constant, not the
/// convention).
pub use overlay_discovery::hopparty::storage::HOPSFOR_ROWS_PER_OUTPOINT as HOPS_VIEW_ROWS_PER_OUTPOINT;

/// Hard bound on `/hops-view` DISTINCT HOP OUTPOINTS per request — same cap
/// + rationale as [`crate::refund_view::REFUND_VIEW_MAX_ROWS`].
pub const HOPS_VIEW_MAX_OUTPOINTS: usize = 100;

/// How many of the newest hops ABSENT from `pot_records` are promoted into
/// the main tier — same reservation + rationale as
/// [`crate::refund_view::REFUND_VIEW_UNKNOWN_POT_QUOTA`] (a fresh hop whose
/// `tm_lowfund` admission is in flight is exactly the hop this view exists
/// to show).
pub const HOPS_VIEW_UNKNOWN_HOP_QUOTA: usize = 10;

/// Freshness window for promoting a hop the overlay has NOT yet indexed
/// under `tm_lowfund` (bsv-low #283a semantics, adopted at the gate's
/// MEDIUM-5): an unknown hop competes for a promoted slot only while its
/// first marker is younger than this, and slots go OLDEST-first — the one
/// order an attacker cannot jump by publishing MORE after seeing the
/// victim's funding. MUST equal the overlay's
/// `UNKNOWN_POT_PROMOTION_MAX_AGE_SECS`, which the sibling identity
/// windows use; a test asserts that equality across the crate boundary
/// (the runtime dependency does not exist, so the PIN carries it —
/// Rule 16).
pub const HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS: u64 = 3600;

/// `verifyBudgetExhausted` on the wire, RETIRED (bsv-low #362) — always
/// `false`, and there is no machinery left that could make it anything else.
///
/// The bit reported that `HOPS_VIEW_VERIFY_BUDGET` had run out before some
/// rows were checked. There is no budget, because there is no read-time
/// verification, because the verdict is latched at admission — so the bit
/// has nothing to report.
///
/// It is still EMITTED, deliberately. The client's parser
/// (`app/src/lib/chainReads.ts::parseHopsView`) is strict: a non-boolean
/// `verifyBudgetExhausted` makes the WHOLE view `null`, which blanks the
/// hops surface. Removing a required field from the wire before every
/// client tolerates its absence would turn a retired feature into an outage
/// — and "dead in production" describes the default build, never what is
/// executing (epoch Rule 14: stale tabs and cached bundles run code you no
/// longer ship). So the field stays, with an honest value, and the
/// machinery behind it is gone. Retiring the FIELD is a separate,
/// client-first change.
pub const VERIFY_BUDGET_EXHAUSTED_WIRE: bool = false;

const _: () = assert!(HOPS_VIEW_UNKNOWN_HOP_QUOTA < HOPS_VIEW_MAX_OUTPOINTS);

/// The single `/hops-view` SQL. Binds: `?1` the lowercase identity, and —
/// when `scoped_to_game` — `?2` a lowercase gameId.
///
/// # Ranking: on the LATCHED VERDICT first (bsv-low #362)
///
/// The leading key is `outpointMarkerRank DESC` — the best
/// `markerValid` verdict among the rows served for that hop outpoint,
/// through the shared
/// [`overlay_discovery::hopparty::validity::marker_rank_expr`] (2 =
/// verified, 1 = legacy/never-evaluated, 0 = refuted). Within an outpoint
/// the per-row `markerRank DESC` orders the superset.
///
/// **It is a SORT KEY and never a `WHERE`.** A 0-latched row is served and
/// labelled `unverified`; hiding it would recreate the invisible-money class
/// (#358, epoch Rule 23). SQL cannot compute which row's signatures verify —
/// but it can `ORDER BY` the answer the moment somebody stores it, and that
/// is the whole change.
///
/// Why the OUTPOINT aggregate rather than the raw per-row rank as the
/// leading key: `finalRank` is a `DENSE_RANK` that must count OUTPOINTS
/// (the page bound is in outpoints). A key that differs between two rows of
/// the SAME outpoint splits it across ranks. `outpointMarkerRank` is
/// `MAX(markerRank)` over the served rows of the outpoint, so it is constant
/// per outpoint by construction, and it answers the question the page
/// actually asks: *does this hop have a marker that verifies?*
///
/// The keys below it are unchanged and still earn their place — they order
/// the legacy and refuted tiers, where the verdict discriminates nothing.
///
/// # The keys below: on what the chain settled, never on arrival (gate HIGH-1)
///
/// `hopparty_records.identity` is attacker-writable by design, and the hop
/// outpoint is `(container txid, hopVout)` — so ONE transaction with K dust
/// P2PKH outputs plus K markers naming `hopVout = 0..K-1` mints K distinct
/// hop outpoints, and `tm_lowfund` admits every P2PKH output of an
/// explicitly-submitted tx, putting them all in the existence tier beside
/// the victim's real hop. Ranking those by RECENCY (the first draft) let
/// ~3,600 sats in one transaction permanently outrank an 80,800-sat hop:
/// measured k=99 honest survives, **k=100 erased**.
///
/// The ranking is therefore led by a fact the attacker must PAY for, read
/// off the container itself:
///
/// ```text
/// paidTier = 0 when hopLockHex IS NOT NULL AND hopSatsOnChain = hopSats
/// ```
///
/// i.e. the container really does pay the claimed value to the claimed
/// settle key — the same predicate `markerVerified`'s bars 1+2 use.
///
/// **What actually prices the attack is the next key, `hopSatsOnChain
/// DESC`** — the value the CHAIN records for that output, never the value
/// the marker claims. Displacing an honest hop therefore requires an
/// output of at least its value per attacker outpoint.
///
/// That is a real bar against DUST, and it is weaker than it first looks
/// against a funded attacker: the value is paid to the attacker's OWN key,
/// and `hopSatsOnChain` is decoded once at admission and never re-read, so
/// a single coin CHAINED through k transactions satisfies it k times at
/// ~one hop of peak, fully recoverable, capital. See the re-priced
/// residual at [`assemble_hops_view`] — do not restate this key as a cost
/// multiplier without reading it. This was verified by injection — neutering `paidTier` alone
/// does NOT re-open the flood, because the value key already demotes dust,
/// so the ordering doc must not credit the tier with work it does not do
/// (Rule 10).
///
/// `paidTier` earns its place for a narrower, real reason: it demotes rows
/// whose container has NO output at `hopVout` (or a mismatched one)
/// DETERMINISTICALLY, instead of leaning on SQLite's convention that NULLs
/// sort last under `DESC` — an engine detail this money-visible ordering
/// should not depend on.
///
/// Ties on value break **OLDEST-first** (#283a again): a REACTIVE flood
/// necessarily arrives after the hop it targets, and `createdAt` is
/// server-stamped at admission, so an attacker can always be newer but can
/// never backdate. (Paying oneself is cheap in BURN terms, which is exactly
/// why this was a cost multiplier and not a closure. The closure is the
/// leading verdict key above: the identity signature — the one input an
/// attacker cannot forge — now decides which outpoints reach the page, not
/// just how the page is arranged afterwards.)
///
/// The existence tier follows, with unknown-hop promotion on the #283a
/// semantics the sibling windows use (gate MEDIUM-5): only hops whose
/// first marker is younger than
/// [`HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS`] compete, and slots go
/// OLDEST-first — a newest-first quota handed all 10 promoted slots to an
/// attacker's just-published ghosts, which is precisely the case where the
/// honest hop's own `tm_lowfund` admission is still in flight.
///
/// **This is a SECURITY order, not a display preference** — it decides
/// which rows survive the page bound under flood, nothing more. Clients
/// should sort the returned entries however their screen wants.
///
/// `finalRank <= MAX+1` fetches one outpoint beyond the page so the
/// `truncated` bit is honest; `scoped_to_game` narrows the window to one
/// game, which helps a truncated caller **unless the flood names that same
/// gameId** (measured: it does not help then — see the residual at
/// [`assemble_hops_view`]).
///
/// The verification facts need NO join: `hopLockHex`, `hopSatsOnChain` and
/// `containerOutputs` are typed columns the overlay decoded from the
/// marker's own container at admission (#310), so this query touches no
/// BLOB, no `outputs` row, and no second topic.
///
/// # #375 era write-off
///
/// `written_off_before_ms` set ⇒ the innermost scan drops rows whose era
/// anchor pre-dates the cutoff, before the per-outpoint windows / quota /
/// `DENSE_RANK` run. Anchor: `COALESCE(r.createdAt, hp.createdAt)` — the
/// hop container output's own `pot_records` admission stamp when indexed,
/// else the hop marker's admission stamp (both server-written unix seconds
/// via `current_unix_seconds_i64`; `hopparty_records.createdAt` is an
/// INTEGER seconds column, same as every marker table here). This query
/// uses NUMBERED binds, so the cutoff placeholder is `?2` unscoped / `?3`
/// scoped — always the LAST bind. `None` ⇒ byte-identical to the pre-#375
/// query.
pub fn hops_view_sql(scoped_to_game: bool, written_off_before_ms: Option<i64>) -> String {
    let game_filter = if scoped_to_game {
        " AND hp.gameId = ?2"
    } else {
        ""
    };
    let era_placeholder = if scoped_to_game { "?3" } else { "?2" };
    format!(
        "SELECT w.identity AS identity, w.gameId AS gameId, w.hopTxid AS hopTxid, \
                w.hopVout AS hopVout, w.hopSats AS hopSats, \
                w.opponentIdentity AS opponentIdentity, \
                w.seatSettlePubkey AS seatSettlePubkey, w.seatSigHex AS seatSigHex, \
                w.identitySigHex AS identitySigHex, w.markerTxid AS markerTxid, \
                w.markerVout AS markerVout, \
                w.hopLockHex AS hopLockHex, w.hopSatsOnChain AS hopSatsOnChain, \
                w.containerOutputs AS containerOutputs, w.markerValid AS markerValid, \
                w.spent AS spent, w.spendingTxid AS spendingTxid, \
                w.spentConfirmed AS spentConfirmed, \
                sb.proof_verified AS spenderProofVerified, \
                w.spenderFinal AS spenderFinal, \
                ns.txid IS NOT NULL AS spenderSeen \
         FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                  seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                  hopLockHex, hopSatsOnChain, containerOutputs, markerValid, \
                  spent, spendingTxid, spentConfirmed, spenderFinal, \
                  markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                  markerRank, outpointMarkerRank, paidTier, tier \
           FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                    seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                    hopLockHex, hopSatsOnChain, containerOutputs, markerValid, \
                    spent, spendingTxid, spentConfirmed, spenderFinal, \
                    markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                    markerRank, outpointMarkerRank, paidTier, tier, \
                    DENSE_RANK() OVER (ORDER BY outpointMarkerRank DESC, \
                                                paidTier ASC, tier ASC, \
                                                hopSatsOnChain DESC, \
                                                COALESCE(potCreatedAt, firstMarkerAt) ASC, \
                                                hopTxid ASC, hopVout ASC) AS finalRank \
           FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                    seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                    hopLockHex, hopSatsOnChain, containerOutputs, markerValid, \
                    spent, spendingTxid, spentConfirmed, spenderFinal, \
                    markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                    markerRank, outpointMarkerRank, paidTier, unknownHop, \
                    CASE WHEN unknownHop = 0 \
                         OR (freshUnknown = 1 AND hopRank <= {quota}) \
                         THEN 0 ELSE 1 END AS tier \
             FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                      seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                      hopLockHex, hopSatsOnChain, containerOutputs, markerValid, \
                      spent, spendingTxid, spentConfirmed, spenderFinal, \
                      markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                      markerRank, paidTier, unknownHop, freshUnknown, \
                      MAX(markerRank) OVER (PARTITION BY hopTxid, hopVout) \
                          AS outpointMarkerRank, \
                      DENSE_RANK() OVER (PARTITION BY unknownHop, freshUnknown \
                                         ORDER BY COALESCE(firstMarkerAt, 0) ASC, \
                                                  hopTxid ASC, hopVout ASC) AS hopRank \
               FROM (SELECT hp.identity AS identity, hp.gameId AS gameId, \
                        hp.txid AS hopTxid, hp.hopVout AS hopVout, \
                        hp.hopSats AS hopSats, \
                        hp.opponentIdentity AS opponentIdentity, \
                        hp.seatSettlePubkey AS seatSettlePubkey, \
                        hp.seatSigHex AS seatSigHex, \
                        hp.identitySigHex AS identitySigHex, \
                        hp.txid AS markerTxid, hp.outputIndex AS markerVout, \
                        hp.hopLockHex AS hopLockHex, \
                        hp.hopSatsOnChain AS hopSatsOnChain, \
                        hp.containerOutputs AS containerOutputs, \
                        hp.markerValid AS markerValid, \
                        {marker_rank} AS markerRank, \
                        r.spent AS spent, r.spendingTxid AS spendingTxid, \
                        r.spentConfirmed AS spentConfirmed, \
                        r.spenderFinal AS spenderFinal, \
                        hp.createdAt AS markerCreatedAt, hp.rowid AS markerRowid, \
                        r.createdAt AS potCreatedAt, \
                        MIN(hp.createdAt) OVER (PARTITION BY hp.txid, hp.hopVout) \
                            AS firstMarkerAt, \
                        CASE WHEN hp.hopLockHex IS NOT NULL \
                                  AND hp.hopSatsOnChain = hp.hopSats \
                             THEN 0 ELSE 1 END AS paidTier, \
                        CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownHop, \
                        CASE WHEN r.txid IS NULL \
                                  AND COALESCE(MIN(hp.createdAt) OVER \
                                        (PARTITION BY hp.txid, hp.hopVout), 0) \
                                      >= unixepoch() - {fresh_secs} \
                             THEN 1 ELSE 0 END AS freshUnknown, \
                        ROW_NUMBER() OVER (PARTITION BY hp.txid, hp.hopVout \
                                           ORDER BY hp.createdAt ASC, hp.rowid ASC) AS rn \
                 FROM hopparty_records hp \
                 LEFT JOIN pot_records r \
                        ON r.txid = hp.txid AND r.outputIndex = hp.hopVout \
                 WHERE hp.identity = ?1{game_filter}{era}) \
               WHERE rn <= {per_outpoint}))) \
           WHERE finalRank <= {rank_cap} \
           ORDER BY outpointMarkerRank DESC, paidTier ASC, tier ASC, hopSatsOnChain DESC, \
                    COALESCE(potCreatedAt, firstMarkerAt) ASC, \
                    hopTxid ASC, hopVout ASC, markerRank DESC, \
                    markerCreatedAt ASC, markerRowid ASC \
           LIMIT {row_cap}) w \
         LEFT JOIN pot_beefs sb ON w.spendingTxid IS NOT NULL \
              AND sb.txid = lower(w.spendingTxid) \
         LEFT JOIN network_seen ns ON w.spendingTxid IS NOT NULL \
              AND ns.txid = lower(w.spendingTxid) \
         ORDER BY w.outpointMarkerRank DESC, w.paidTier ASC, w.tier ASC, \
                  w.hopSatsOnChain DESC, \
                  COALESCE(w.potCreatedAt, w.firstMarkerAt) ASC, \
                  w.hopTxid ASC, w.hopVout ASC, w.markerRank DESC, \
                  w.markerCreatedAt ASC, w.markerRowid ASC",
        quota = HOPS_VIEW_UNKNOWN_HOP_QUOTA,
        fresh_secs = HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS,
        per_outpoint = HOPS_VIEW_ROWS_PER_OUTPOINT,
        rank_cap = HOPS_VIEW_MAX_OUTPOINTS + 1,
        row_cap = (HOPS_VIEW_MAX_OUTPOINTS + 1) * HOPS_VIEW_ROWS_PER_OUTPOINT,
        game_filter = game_filter,
        // The SHARED rank expression — the column name and its three tiers
        // live in the overlay crate that WRITES them (epoch Rule 16), so a
        // rename cannot leave this query silently reading nothing.
        marker_rank = overlay_discovery::hopparty::validity::marker_rank_expr("hp."),
        era = crate::logic::era_filter_sql(
            "COALESCE(r.createdAt, hp.createdAt)",
            era_placeholder,
            written_off_before_ms
        ),
    )
}

/// One `/hops-view` joined row, host-typed (the `hops_view_sql` shape).
/// Pot-side fields are `Option` because the `pot_records` join can MISS —
/// a marker whose hop the overlay never indexed yields NULL columns
/// (fail-safe: never asserted unspent).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HopsViewRow {
    /// The marker's identity (== the bound caller identity by SQL).
    pub identity: String,
    pub game_id: String,
    pub hop_txid: String,
    pub hop_vout: u32,
    pub hop_sats: u64,
    pub opponent_identity: String,
    pub seat_settle_pubkey: String,
    pub seat_sig_hex: String,
    pub identity_sig_hex: String,
    /// The txid carrying the marker OP_RETURN — the SAME tx as the hop
    /// (the marker rides it), so this equals `hop_txid` by construction.
    pub marker_txid: String,
    /// The marker output's index within that tx.
    pub marker_vout: u32,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// `pot_beefs.proof_verified` for the recorded spender — the second
    /// accepted confirmation signal (#323 shared bar). `None` = no row.
    pub spender_proof_verified: Option<bool>,
    /// #371: the overlay's OWN network witness for the recorded spender
    /// (`network_seen` join) + the spender bytes-finality latch
    /// (`pot_records.spenderFinal`) — the shared bar's third arm. A hop
    /// SWEEP is final by construction, so a swept hop shows `spent` at the
    /// SEEN bar instead of waiting out the merkle proof.
    pub spender_seen: Option<bool>,
    pub spender_final: Option<bool>,
    /// The CONTAINER's own output at `hop_vout`: its locking script
    /// (lowercase hex), decoded at admission (#310). `None` IFF the
    /// container has no such output — a PROVEN absence (see
    /// `container_outputs`) that REFUTES the marker.
    pub hop_lock_hex: Option<String>,
    /// That output's satoshi value as the chain records it. `None` in the
    /// same absent case.
    pub hop_sats_on_chain: Option<u64>,
    /// How many outputs the container has — makes the absence above
    /// provable rather than ambiguous.
    pub container_outputs: u32,
    /// THE LATCHED VERDICT (bsv-low #362): `hopparty_records.markerValid`,
    /// decided once at admission by
    /// `overlay_discovery::hopparty::validity::record_marker_valid`.
    /// `Some(true)` = every bar cleared, `Some(false)` = REFUTED,
    /// `None` = the row predates the migration and the #367 re-latch sweep
    /// has not reached it yet. Nothing else can clear it: this table cannot
    /// self-heal by republish.
    pub marker_valid: Option<bool>,
}

/// The three-valued marker verdict — `unknown` first-class.
///
/// Since #362 `Unknown` has exactly ONE producer: a `markerValid IS NULL`
/// row, i.e. one admitted before the latch existed. It no longer means "the
/// per-request budget ran out before I looked at this row", which was a
/// state an attacker could induce for every reader at will.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerVerification {
    Verified,
    Unverified,
    Unknown,
}

impl MarkerVerification {
    pub fn as_str(self) -> &'static str {
        match self {
            MarkerVerification::Verified => "verified",
            MarkerVerification::Unverified => "unverified",
            MarkerVerification::Unknown => "unknown",
        }
    }
}

/// The P2PKH lock a hop paying `seat_settle_pubkey` MUST carry:
/// `OP_DUP OP_HASH160 <hash160(pubkey)> OP_EQUALVERIFY OP_CHECKSIG`.
/// `None` when the pubkey hex is malformed (not 33 bytes) — an unbuildable
/// expectation can never verify (fail-safe).
///
/// A hex-decoding ADAPTER over the one implementation, which lives beside
/// the predicate that uses it in the overlay crate (epoch Rule 10: the
/// durable fix for "these two must agree" is deleting one of them, not
/// testing both). Kept public because the real-SQLite cells build
/// production containers with it.
pub fn expected_hop_lock_hex(seat_settle_pubkey_hex: &str) -> Option<String> {
    let pk = hex::decode(seat_settle_pubkey_hex).ok()?;
    overlay_discovery::hopparty::validity::expected_hop_lock_hex(&pk)
}

/// Read the LATCHED verdict (bsv-low #362). No cryptography, no budget, no
/// per-row work — one `Option<bool>` to one label.
///
/// # This signature is the bar, not a convenience
///
/// It takes the column and NOTHING else, deliberately. The pre-#362 version
/// took the whole `&HopsViewRow` and had every input a full verification
/// needs, which is exactly how read-time crypto grew here in the first
/// place. A function that cannot see the signatures cannot verify them, and
/// making the wrong thing UNREPRESENTABLE is the only tier that is
/// axis-independent (epoch Rule 12a addendum) — a source scan for `verify(`
/// would be one rename wide.
///
/// # The residual this does NOT change: a PAID REPLAY
///
/// Neither digest can bind the containing txid (a tx cannot embed its own
/// txid), so the marker bytes are portable. An attacker can copy an admitted
/// marker verbatim into a transaction of their own. `seatSettlePubkey` is
/// public, so they CAN construct the lock — but the value bar means the
/// replay only latches `1` if that output pays **`hopSats` to the victim's
/// own settle key**: the attacker must GIVE THE VICTIM the full hop amount
/// (~80,800 sats in the #256 class, plus fees) per row, and the victim can
/// spend it — they hold the settle key. Bounded, self-defeating, and it
/// fails in the honest direction (an extra `verified` row in the victim's
/// own view, never a lost or altered one).
///
/// **No app-layer signature is invented to close it** (Rule 4 / the epoch
/// doc's recorded mistake). The only bar that would close it must bind the
/// container's INPUTS to the identity, and that is not available: the hop is
/// funded by arbitrary BRC-42 wallet children.
///
/// # Legacy limit, stated plainly
///
/// Hops created BEFORE the marker shipped carry none and never will — the
/// marker must ride the hop tx, and that tx is already on chain. Those hops
/// stay invisible to `/hops-view` forever. Same acknowledged limit #319
/// carries for retrofitting potparty markers.
pub fn derive_marker_verification(marker_valid: Option<bool>) -> MarkerVerification {
    match marker_valid {
        Some(true) => MarkerVerification::Verified,
        Some(false) => MarkerVerification::Unverified,
        // The row predates the latch and the #367 re-latch sweep has not
        // reached it yet. NOT "we could not look" — nothing per-request
        // decides this, and no republish can clear it.
        None => MarkerVerification::Unknown,
    }
}

/// The `/hops-view` status enum (module-docs table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopStatus {
    Unspent,
    Spent,
    Unknown,
}

impl HopStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            HopStatus::Unspent => "unspent",
            HopStatus::Spent => "spent",
            HopStatus::Unknown => "unknown",
        }
    }
}

/// Derive `(status, statusSource)` from the hop's chain facts — never a
/// guess: an absent `pot_records` row and an unconfirmed spend pointer are
/// both (`Unknown`, `None`). The confirmation bar is the ONE shared
/// `logic::is_confirmed_landing_with_proof` (#323).
pub fn derive_hop_status(
    spent: Option<bool>,
    spent_confirmed: Option<bool>,
    spender_proof_verified: Option<bool>,
    spender_seen: Option<bool>,
    spender_final: Option<bool>,
) -> (HopStatus, Option<&'static str>) {
    match spent {
        // Non-observation of a spend on an INDEXED hop (module docs).
        Some(false) => (HopStatus::Unspent, Some("chain")),
        Some(true) => {
            if crate::logic::is_confirmed_landing_with_proof(
                spent_confirmed,
                spender_proof_verified,
                spender_seen,
                spender_final,
            ) {
                (HopStatus::Spent, Some("chain"))
            } else {
                // Recorded-but-unconfirmed: a displaceable intent.
                (HopStatus::Unknown, None)
            }
        }
        // No pot_records row: genuinely unknown, never asserted unspent.
        None => (HopStatus::Unknown, None),
    }
}

/// One `/hops-view` response entry, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopEntry {
    pub game_id: String,
    pub hop_txid: String,
    pub hop_vout: u32,
    pub hop_sats: u64,
    pub opponent_identity: String,
    pub seat_settle_pubkey: String,
    /// Both signatures ride back verbatim so the CLIENT re-verifies —
    /// the server's filter is display labeling, never an authority.
    pub seat_sig_hex: String,
    pub identity_sig_hex: String,
    /// The marker's containing tx = the hop tx, and the marker's own vout
    /// within it (so a client can fetch the exact OP_RETURN back).
    pub marker_txid: String,
    pub marker_vout: u32,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    pub status: HopStatus,
    pub status_source: Option<&'static str>,
    pub marker_verified: MarkerVerification,
}

/// Assemble the joined rows into response entries + the one honesty bit:
/// `truncated` (more than [`HOPS_VIEW_MAX_OUTPOINTS`] distinct outpoints
/// survived — the SQL fetches one outpoint beyond the page on purpose).
///
/// # VERIFY-BEFORE-PAGE, and why the second sort is gone (bsv-low #362)
///
/// The property an attacker cannot forge is the **identity signature**: they
/// can mint a marker naming the victim's identity, can pay the claimed
/// value, and can even mint a valid `seatSig` over the victim's identity
/// with their own settle key — bars 1, 2 and 3a all pass — yet they can
/// never produce bar 3b, the BRC-42 signature under the victim's identity
/// key.
///
/// Before #362 that property was only available AFTER paging, because
/// verification happened here, in Rust, on rows SQL had already chosen. So
/// this function re-sorted the page verified-first — which reordered what
/// the caller saw and did nothing about which outpoints reached it. An
/// attacker who could crowd the candidate set still evicted the honest row.
///
/// Now the verdict is a column, so the SQL leads its `ORDER BY` with it
/// (`outpointMarkerRank DESC`) and the served order IS the verified-first
/// order, before the page bound is applied. A second sort here would be a
/// duplicate source of ordering truth that can only drift — so there is
/// one, and it is the SQL's (epoch Rule 1: a check that is dominated is
/// re-derivation; delete it, don't cover it). The row order this function
/// receives is the order it serves.
///
/// # The residual, RE-PRICED — measured, not asserted
///
/// | shape (k = 100–400 outpoints)                | honest row | was |
/// |----------------------------------------------|-----------|-----|
/// | dust, reactive or pre-dated                  | position 0, Verified | position 0 |
/// | paid at EXACTLY the hop value                | position 0, Verified | position 0 |
/// | paid at hop value + 1, REACTIVE              | position 0, Verified | **absent** |
/// | one coin CHAINED through k txs               | position 0, Verified | **absent** |
/// | paid REPLAY of the victim's marker, reactive | position 0, Verified | absent |
/// | paid REPLAY **admitted before** the victim's | **absent**, `truncated:true` | absent |
///
/// The two bold "was" cells are the evictions this change retires: ~5.2k
/// sats burned and ~86k of fully RECOVERABLE peak capital used to erase an
/// 80,800-sat hop. Outranking a verified row now requires a verified row,
/// and paying MORE does not produce one — the latch requires the container
/// to pay EXACTLY `hopSats`, so an attacker's row can only ever TIE on the
/// value key and then loses the oldest-first tie-break.
///
/// **What is left is a SUBMIT RACE, not a payment.** An attacker who
/// watches the hop transaction in the mempool can replay the marker bytes
/// (they are portable — no digest binds the containing txid) into containers
/// of their own and get them ADMITTED before the victim's own marker,
/// winning the `createdAt` tie-break. Each such container must pay `hopSats`
/// to the VICTIM'S OWN settle key: a gift the victim can spend and the
/// attacker cannot, which is also why it cannot be chained the way the old
/// shape could.
///
/// All six rows are measured through the real producer chain in
/// `tests/hops_view_sqlite.rs` — the last one from the UNSAFE side, in
/// `a_paid_replay_flood_is_the_remaining_residual`, whose two legs pin the
/// reactive case winning and the raced case losing. Closure direction is
/// unchanged: #318's per-identity auth + quota. The `?gameId=` scope is not
/// an escape (the gameId is one of the marker's own nine pushes).
///
/// What did NOT change: rows are never dropped server-side, `truncated` is
/// always honest, and both signatures ride back for client re-verification.
pub fn assemble_hops_view(rows: Vec<HopsViewRow>) -> (Vec<HopEntry>, bool) {
    // Distinct outpoints in served order; drop rows beyond the page.
    let mut outpoints: Vec<(String, u32)> = Vec::new();
    for r in &rows {
        let key = (r.hop_txid.to_ascii_lowercase(), r.hop_vout);
        if !outpoints.contains(&key) {
            outpoints.push(key);
        }
    }
    let truncated = outpoints.len() > HOPS_VIEW_MAX_OUTPOINTS;
    outpoints.truncate(HOPS_VIEW_MAX_OUTPOINTS);

    let entries: Vec<HopEntry> = rows
        .into_iter()
        .filter(|r| outpoints.contains(&(r.hop_txid.to_ascii_lowercase(), r.hop_vout)))
        .map(|r| {
            let marker_verified = derive_marker_verification(r.marker_valid);
            let (status, status_source) = derive_hop_status(
                r.spent,
                r.spent_confirmed,
                r.spender_proof_verified,
                r.spender_seen,
                r.spender_final,
            );
            HopEntry {
                game_id: r.game_id,
                hop_txid: r.hop_txid,
                hop_vout: r.hop_vout,
                hop_sats: r.hop_sats,
                opponent_identity: r.opponent_identity,
                seat_settle_pubkey: r.seat_settle_pubkey,
                seat_sig_hex: r.seat_sig_hex,
                identity_sig_hex: r.identity_sig_hex,
                marker_txid: r.marker_txid,
                marker_vout: r.marker_vout,
                spent: r.spent,
                spending_txid: r.spending_txid,
                spent_confirmed: r.spent_confirmed,
                status,
                status_source,
                marker_verified,
            }
        })
        .collect();
    (entries, truncated)
}

/// Assemble the `/hops-view` wire body:
/// `{"identity","tip":<height|null>,"truncated":<bool>,
///   "verifyBudgetExhausted":<bool>,"hops":[{gameId,hopTxid,hopVout,
///   hopSats,opponentIdentity,seatSettlePubkey,seatSigHex,identitySigHex,
///   markerTxid,spent,spendingTxid,spentConfirmed,status,statusSource,
///   markerVerified}]}`.
/// `tip` mirrors the sibling views (`null` on a chaintracks fault — the D1
/// facts still serve).
///
/// `verifyBudgetExhausted` is emitted as the constant
/// [`VERIFY_BUDGET_EXHAUSTED_WIRE`] — see there for why a retired field is
/// still on the wire.
pub fn hops_view_body(
    identity: &str,
    tip: Option<u64>,
    entries: &[HopEntry],
    truncated: bool,
) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            json!({
                "gameId": e.game_id,
                "hopTxid": e.hop_txid,
                "hopVout": e.hop_vout,
                "hopSats": e.hop_sats,
                "opponentIdentity": e.opponent_identity,
                "seatSettlePubkey": e.seat_settle_pubkey,
                "seatSigHex": e.seat_sig_hex,
                "identitySigHex": e.identity_sig_hex,
                "markerTxid": e.marker_txid,
                "markerVout": e.marker_vout,
                "spent": e.spent,
                "spendingTxid": e.spending_txid,
                "spentConfirmed": e.spent_confirmed,
                "status": e.status.as_str(),
                "statusSource": e.status_source,
                "markerVerified": e.marker_verified.as_str(),
            })
        })
        .collect();
    json!({
        "identity": identity,
        "tip": tip,
        "truncated": truncated,
        "verifyBudgetExhausted": VERIFY_BUDGET_EXHAUSTED_WIRE,
        "hops": arr,
    })
    .to_string()
}

// ============================================================================
// Tests (pure logic; the real-SQLite end-to-end cells live in
// tests/hops_view_sqlite.rs)
//
// The CRYPTO matrix that used to live here — the golden positive control,
// the per-field tamper table, the canonical-DER refusals, the F1 bar, the
// potparty/hopparty domain separation — moved WITH the predicate, to
// `overlay_discovery::hopparty::validity` (bsv-low #362). It did not shrink;
// it is now measured where the work happens, against the frozen client
// golden, once per admitted marker instead of once per row per request.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn h64(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }

    /// A served row carrying a latched verdict. The signature/pubkey fields
    /// are opaque here BY DESIGN: this file no longer has anything that
    /// could look at them, which is the change.
    fn row(hop_txid_byte: u8, marker_valid: Option<bool>) -> HopsViewRow {
        HopsViewRow {
            identity: format!("02{}", "a1".repeat(32)),
            game_id: h64(0x11),
            hop_txid: h64(hop_txid_byte),
            hop_vout: 0,
            hop_sats: 80_800,
            opponent_identity: format!("02{}", "b0".repeat(32)),
            seat_settle_pubkey: format!("03{}", "c4".repeat(32)),
            seat_sig_hex: format!("3045{}", "ab".repeat(69)),
            identity_sig_hex: format!("3045{}", "cd".repeat(69)),
            marker_txid: h64(hop_txid_byte),
            marker_vout: 1,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: Some(false),
            spender_proof_verified: None,
            spender_seen: None,
            spender_final: None,
            hop_lock_hex: Some(format!("76a914{}88ac", "d4".repeat(20))),
            hop_sats_on_chain: Some(80_800),
            container_outputs: 2,
            marker_valid,
        }
    }

    // ── the verdict mapping ──────────────────────────────────────────────

    /// The whole of `/hops-view`'s verification, post-#362: one column, three
    /// labels, exhaustively.
    #[test]
    fn the_latched_column_maps_to_exactly_three_labels() {
        assert_eq!(
            derive_marker_verification(Some(true)),
            MarkerVerification::Verified
        );
        assert_eq!(
            derive_marker_verification(Some(false)),
            MarkerVerification::Unverified
        );
        assert_eq!(
            derive_marker_verification(None),
            MarkerVerification::Unknown,
            "a legacy row is UNKNOWN — never silently relabelled as refuted, \
             which would assert something about it that nobody measured"
        );
        for (v, s) in [
            (MarkerVerification::Verified, "verified"),
            (MarkerVerification::Unverified, "unverified"),
            (MarkerVerification::Unknown, "unknown"),
        ] {
            assert_eq!(v.as_str(), s, "the wire word is frozen — clients parse it");
        }
    }

    /// The RETIRED bit is still on the wire, and it is honestly `false`.
    ///
    /// The client's `parseHopsView` is STRICT: a non-boolean
    /// `verifyBudgetExhausted` nulls the whole view and blanks the surface.
    /// Removing the field would therefore be an outage for every client that
    /// has not shipped tolerance for its absence — and "dead in production"
    /// describes the default build, never what is executing (epoch Rule 14).
    #[test]
    fn the_retired_budget_bit_is_emitted_as_an_honest_false() {
        // Read through a runtime binding: the constant IS `false`, and a
        // `assert!(!CONST)` would be a clippy-flagged tautology rather than
        // an observation. What is actually pinned is the BODY below.
        let wire: bool = VERIFY_BUDGET_EXHAUSTED_WIRE;
        assert!(!wire, "there is no budget left that could exhaust");
        // Emitted on a populated body…
        let (entries, truncated) = assemble_hops_view(vec![row(0xaa, Some(true))]);
        let v: serde_json::Value =
            serde_json::from_str(&hops_view_body("id", Some(1), &entries, truncated)).unwrap();
        assert_eq!(v["verifyBudgetExhausted"], json!(false));
        assert!(
            v["verifyBudgetExhausted"].is_boolean(),
            "a non-boolean nulls the client's whole view"
        );
        // …and on the empty one, which is the fail-safe path the route takes
        // for an invalid identity.
        let v: serde_json::Value =
            serde_json::from_str(&hops_view_body("nope", None, &[], false)).unwrap();
        assert_eq!(v["verifyBudgetExhausted"], json!(false));
        assert!(v["verifyBudgetExhausted"].is_boolean());
    }

    // ── SERVE AND LABEL: never a WHERE, never a drop ─────────────────────

    /// Every verdict class is SERVED, in the order the SQL handed them over,
    /// with its own label. A refuted or legacy row being dropped — or
    /// reordered here, behind the SQL's back — is the invisible-money class
    /// (#358, epoch Rule 23).
    #[test]
    fn every_verdict_class_is_served_labelled_and_in_the_sql_order() {
        // Deliberately handed over in the WORST order (refuted first): this
        // function must not "fix" it, because the SQL already leads on the
        // latched rank and a second sort here would be a rival source of
        // ordering truth (epoch Rule 1).
        let rows = vec![
            row(0x01, Some(false)),
            row(0x02, None),
            row(0x03, Some(true)),
        ];
        let (entries, truncated) = assemble_hops_view(rows);
        assert!(!truncated);
        assert_eq!(entries.len(), 3, "no class is dropped server-side");
        assert_eq!(
            entries
                .iter()
                .map(|e| (e.hop_txid.clone(), e.marker_verified))
                .collect::<Vec<_>>(),
            vec![
                (h64(0x01), MarkerVerification::Unverified),
                (h64(0x02), MarkerVerification::Unknown),
                (h64(0x03), MarkerVerification::Verified),
            ],
            "served order is the SQL's, verbatim"
        );
        // …and the signatures ride back on every class, so the client can
        // re-verify a row the server labelled `unverified`.
        for e in &entries {
            assert!(!e.seat_sig_hex.is_empty() && !e.identity_sig_hex.is_empty());
        }
    }

    // ── paging ───────────────────────────────────────────────────────────

    #[test]
    fn truncation_bit_is_honest_and_the_page_is_outpoint_counted() {
        // MAX+1 distinct outpoints (as the SQL's rank_cap admits): the page
        // keeps MAX and reports truncated.
        let rows: Vec<HopsViewRow> = (0..=HOPS_VIEW_MAX_OUTPOINTS)
            .map(|i| {
                let mut r = row(0xaa, Some(true));
                r.hop_txid = format!("{i:064x}");
                r
            })
            .collect();
        let (entries, truncated) = assemble_hops_view(rows.clone());
        assert_eq!(entries.len(), HOPS_VIEW_MAX_OUTPOINTS);
        assert!(truncated, "one outpoint past the page ⇒ truncated");
        // Exactly MAX outpoints: complete.
        let (entries, truncated) = assemble_hops_view(rows[..HOPS_VIEW_MAX_OUTPOINTS].to_vec());
        assert_eq!(entries.len(), HOPS_VIEW_MAX_OUTPOINTS);
        assert!(!truncated);
        // Multiple rows of ONE outpoint never count as multiple outpoints.
        let (entries, truncated) = assemble_hops_view(vec![
            row(0xaa, Some(true)),
            row(0xaa, Some(false)),
            row(0xaa, None),
        ]);
        assert_eq!(entries.len(), 3, "superset rows all served");
        assert!(!truncated);
    }

    /// The page bound is on OUTPOINTS and it is applied to the SQL's order,
    /// which #362 makes verified-first. A row past the bound is CUT, not
    /// relabelled — and `truncated` says so.
    #[test]
    fn the_page_cuts_the_tail_of_the_sql_order_and_says_so() {
        let mut rows: Vec<HopsViewRow> = (0..HOPS_VIEW_MAX_OUTPOINTS)
            .map(|i| {
                let mut r = row(0x00, Some(true));
                r.hop_txid = format!("{i:064x}");
                r
            })
            .collect();
        let mut tail = row(0x00, Some(false));
        tail.hop_txid = "ff".repeat(32);
        rows.push(tail.clone());
        let (entries, truncated) = assemble_hops_view(rows);
        assert!(truncated);
        assert_eq!(entries.len(), HOPS_VIEW_MAX_OUTPOINTS);
        assert!(
            !entries.iter().any(|e| e.hop_txid == tail.hop_txid),
            "the outpoint past the page is absent, and `truncated` is the \
             only honest way the caller learns that"
        );
    }

    // ── status derivation table ──────────────────────────────────────────

    #[test]
    fn hop_status_table() {
        // Indexed, unspent.
        assert_eq!(
            derive_hop_status(Some(false), Some(false), None, None, None),
            (HopStatus::Unspent, Some("chain"))
        );
        // Confirmed spend (flag).
        assert_eq!(
            derive_hop_status(Some(true), Some(true), None, None, None),
            (HopStatus::Spent, Some("chain"))
        );
        // Confirmed via the verified spender proof (the #323 second signal).
        assert_eq!(
            derive_hop_status(Some(true), Some(false), Some(true), None, None),
            (HopStatus::Spent, Some("chain"))
        );
        // #371 third arm: the overlay's own network witness + final bytes —
        // a swept hop is `spent` at the SEEN bar, no merkle wait.
        assert_eq!(
            derive_hop_status(Some(true), Some(false), None, Some(true), Some(true)),
            (HopStatus::Spent, Some("chain"))
        );
        // SEEN without finality, and finality without the witness, are each
        // NOT a landing (the two halves of the third arm are conjunctive).
        assert_eq!(
            derive_hop_status(Some(true), Some(false), None, Some(true), Some(false)),
            (HopStatus::Unknown, None)
        );
        assert_eq!(
            derive_hop_status(Some(true), Some(false), None, None, Some(true)),
            (HopStatus::Unknown, None)
        );
        // Recorded-but-unconfirmed: a displaceable intent — unknown.
        assert_eq!(
            derive_hop_status(Some(true), Some(false), None, None, None),
            (HopStatus::Unknown, None)
        );
        assert_eq!(
            derive_hop_status(Some(true), None, Some(false), None, None),
            (HopStatus::Unknown, None)
        );
        // No pot_records row: never asserted unspent.
        assert_eq!(
            derive_hop_status(None, None, None, None, None),
            (HopStatus::Unknown, None)
        );
    }

    // ── body shape ───────────────────────────────────────────────────────

    #[test]
    fn body_shape_and_labels() {
        let verified = row(0xaa, Some(true));
        let mut junk = row(0xbb, Some(false));
        junk.spent = None;
        junk.spent_confirmed = None;
        let (entries, truncated) = assemble_hops_view(vec![verified.clone(), junk]);
        let body = hops_view_body(&verified.identity, Some(960_000), &entries, truncated);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["identity"], json!(verified.identity));
        assert_eq!(v["tip"], json!(960_000));
        assert_eq!(v["truncated"], json!(false));
        assert_eq!(v["verifyBudgetExhausted"], json!(false));
        let e0 = &v["hops"][0];
        assert_eq!(e0["hopTxid"], json!(verified.hop_txid));
        assert_eq!(e0["hopVout"], json!(0));
        assert_eq!(e0["hopSats"], json!(80_800));
        assert_eq!(e0["status"], json!("unspent"));
        assert_eq!(e0["statusSource"], json!("chain"));
        assert_eq!(e0["markerVerified"], json!("verified"));
        assert_eq!(e0["seatSettlePubkey"], json!(verified.seat_settle_pubkey));
        assert_eq!(
            e0["markerTxid"],
            json!(verified.hop_txid),
            "the marker rides the hop tx"
        );
        assert_eq!(e0["markerVout"], json!(1));
        // The refuted row is SERVED, labeled — never dropped.
        let e1 = &v["hops"][1];
        assert_eq!(e1["markerVerified"], json!("unverified"));
        assert_eq!(e1["status"], json!("unknown"));
        assert!(e1["statusSource"].is_null());
        // The sigs ride back for client re-verification.
        assert!(e0["seatSigHex"].is_string() && e0["identitySigHex"].is_string());
    }

    #[test]
    fn empty_body_and_null_tip() {
        let v: serde_json::Value =
            serde_json::from_str(&hops_view_body("nope", None, &[], false)).unwrap();
        assert_eq!(v["identity"], json!("nope"));
        assert!(v["tip"].is_null());
        assert_eq!(v["hops"], json!([]));
        assert_eq!(v["truncated"], json!(false));
    }

    /// The lock-derivation adapter still derives the same script the overlay
    /// predicate expects — the two crates must agree on what a hop lock IS,
    /// and here that is structural (one implementation) rather than asserted.
    #[test]
    fn the_lock_adapter_delegates_to_the_one_implementation() {
        let pk_hex = "03d3e37fc9edbd1c225d703873b45f66368e86c633cb613252b3254ffe0b8ad5ee";
        assert_eq!(
            expected_hop_lock_hex(pk_hex),
            overlay_discovery::hopparty::validity::expected_hop_lock_hex(
                &hex::decode(pk_hex).unwrap()
            )
        );
        // Malformed input is None on both sides (fail-safe).
        for bad in ["", "zz", "02abcd", &"aa".repeat(34)] {
            assert_eq!(expected_hop_lock_hex(bad), None, "{bad}");
        }
    }

    // ── SQL structure pins ───────────────────────────────────────────────

    #[test]
    fn hops_view_sql_shape() {
        let sql = hops_view_sql(false, None);
        assert_eq!(sql.matches('?').count(), 1, "one identity bind");
        assert!(
            sql.contains("ROW_NUMBER() OVER (PARTITION BY hp.txid, hp.hopVout"),
            "the hop outpoint is (containing txid, hopVout)"
        );
        assert!(
            sql.contains(&format!("rn <= {HOPS_VIEW_ROWS_PER_OUTPOINT}")),
            "bounded SUPERSET per outpoint — never rn = 1"
        );
        assert!(sql.contains(&format!("hopRank <= {HOPS_VIEW_UNKNOWN_HOP_QUOTA}")));
        assert!(
            sql.contains(&format!("finalRank <= {}", HOPS_VIEW_MAX_OUTPOINTS + 1)),
            "one outpoint beyond the page — the honest truncated bit"
        );
        // The verification facts are TYPED COLUMNS decoded at admission —
        // no `outputs` join, no topic dependency, no BLOB. POSITIVE counts,
        // so a rename or a dropped nesting level fails loudly rather than
        // vacuously. The numbers are the per-level occurrence counts (one
        // per SELECT level, plus the aliasing projection, plus — for
        // hopSatsOnChain — the paid-tier CASE and the two ORDER BYs);
        // re-derive them deliberately if the nesting changes, never by
        // pasting whatever the failure prints.
        assert_eq!(sql.matches("hopLockHex").count(), 9);
        assert_eq!(sql.matches("hopSatsOnChain").count(), 12);
        assert_eq!(sql.matches("containerOutputs").count(), 8);
        // The ranking leads on chain-settled facts, never on arrival.
        assert!(sql.contains("CASE WHEN hp.hopLockHex IS NOT NULL"));
        // #283a semantics: freshness-gated, OLDEST-first promotion.
        assert!(sql.contains(&format!(
            "unixepoch() - {HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS}"
        )));
        assert!(sql.contains("ORDER BY COALESCE(firstMarkerAt, 0) ASC"));
        let banned_join = ["tm_low", "fund'"].concat(); // split so it never matches itself
        assert!(
            !sql.contains(&banned_join),
            "the container supplies the hop facts — no outputs/tm_lowfund join"
        );
        // The shared confirmation bar's latch is fetched.
        assert!(sql.contains("sb.proof_verified AS spenderProofVerified"));
        // No BEEF blob is ever hauled (25-byte P2PKH scripts only).
        assert!(!sql.contains("hex(sb.beef)") && !sql.contains("hex(hb.beef)"));
    }

    /// #375 — the era filter on `/hops-view`: exactly one shared fragment,
    /// at the innermost identity scan, anchored `COALESCE(r.createdAt,
    /// hp.createdAt)` (the hop container's own admission stamp when indexed,
    /// else the hop marker's — both server-written unix seconds). This
    /// query uses NUMBERED binds, so the cutoff placeholder is `?2`
    /// unscoped / `?3` scoped — always the LAST bind. Stripping the
    /// fragment restores the `None` arm byte-for-byte in BOTH arities.
    #[test]
    fn hops_view_sql_era_filter_shape_and_none_identity() {
        let cutoff = Some(1_754_500_000_000i64);
        for (scoped, placeholder, where_prefix) in [
            (false, "?2", "WHERE hp.identity = ?1"),
            (true, "?3", "WHERE hp.identity = ?1 AND hp.gameId = ?2"),
        ] {
            let frag = crate::logic::era_filter_sql(
                "COALESCE(r.createdAt, hp.createdAt)",
                placeholder,
                cutoff,
            );
            let with = hops_view_sql(scoped, cutoff);
            let without = hops_view_sql(scoped, None);
            assert_eq!(
                with.matches(&frag).count(),
                1,
                "exactly one era fragment (scoped={scoped})"
            );
            assert_eq!(
                with.matches(&format!("{where_prefix}{frag})")).count(),
                1,
                "the era filter rides the innermost identity scan (scoped={scoped})"
            );
            assert_eq!(
                with.replace(&frag, ""),
                without,
                "None must stay byte-identical to the pre-#375 query (scoped={scoped})"
            );
        }
    }

    /// THE #362 SHAPE PINS: the latched verdict is a LEADING SORT KEY, built
    /// from the OVERLAY's shared expression, and it is NEVER a filter.
    ///
    /// # This cell is a BELT. The bar is behavioural. (Measured, Rule 12a)
    ///
    /// An earlier revision of this doc claimed the `WHERE` count meant "a
    /// fourth `WHERE` — however it is spelled — fails here". **RED-verified
    /// false**: folding `AND COALESCE(hp.markerValid, 1) <> 0` into the
    /// EXISTING identity `WHERE` compiles, hides every refuted row, and
    /// leaves this cell GREEN. One `AND` defeated it. That is the rule
    /// itself — you cannot enumerate your way to a property — recorded here
    /// rather than papered over.
    ///
    /// What DOES observe that injection is the real-SQLite behaviour, which
    /// runs the shipped query against rows of every verdict class and counts
    /// what comes back:
    /// `hops_view_sqlite.rs::junk_sig_marker_is_served_labeled_unverified_never_verified`
    /// (RED-confirmed against exactly that injection) and
    /// `…::a_legacy_row_is_served_labelled_unknown_and_ranks_between_the_two_verdicts`.
    /// Row conservation is a property; a clause count is a spelling.
    ///
    /// The counts below are kept because they are cheap and they DO see a
    /// whole new clause, a demoted sort key, or a rank expression that
    /// stopped being the overlay's. They are blind to an `AND`, and now say
    /// so.
    #[test]
    fn the_latched_verdict_leads_every_order_and_filters_nothing() {
        for scoped in [false, true] {
            let sql = hops_view_sql(scoped, None);
            // Built from the SHARED expression, not a local copy.
            assert!(
                sql.contains(&overlay_discovery::hopparty::validity::marker_rank_expr(
                    "hp."
                )),
                "the rank CASE is the overlay's, verbatim (scoped={scoped})"
            );
            // LEADING in the DENSE_RANK that allocates the page…
            assert_eq!(
                sql.matches("DENSE_RANK() OVER (ORDER BY outpointMarkerRank DESC, ")
                    .count(),
                1,
                "the page-allocating rank leads on the latched verdict"
            );
            // …and leading in BOTH unaliased orderings: the DENSE_RANK's own
            // (counted above) and the LIMITed ORDER BY that fills the page.
            assert_eq!(
                sql.matches("ORDER BY outpointMarkerRank DESC, paidTier ASC")
                    .count(),
                2
            );
            assert_eq!(
                sql.matches("ORDER BY w.outpointMarkerRank DESC, w.paidTier ASC")
                    .count(),
                1
            );
            // The outpoint aggregate is what keeps finalRank counting
            // OUTPOINTS: a key that differs between two rows of one outpoint
            // would split it across ranks.
            assert!(sql.contains(
                "MAX(markerRank) OVER (PARTITION BY hopTxid, hopVout) \
                          AS outpointMarkerRank"
            ));
            // Exactly the three clauses the window has always had (identity
            // scope, rn <= superset, finalRank <= page). BLIND to a term
            // ANDed into one of them — see the doc above; the behavioural
            // cells are what cover that.
            assert_eq!(
                sql.matches("WHERE ").count(),
                3,
                "a whole new WHERE clause appeared: {sql}"
            );
            // The three names a filter would have to reach for, counted where
            // they legitimately appear. Any EXTRA use — in a WHERE, a HAVING,
            // a CASE that nulls a row out, or an ANDed term — moves one of
            // these. Still a count, still a belt: re-derive them deliberately
            // if the nesting changes, never by pasting a failure's output.
            //
            //   markerValid = 10: 2 in the rank CASE, 2 in L0's alias, 4
            //     carried through L1..L4, 2 in the outer projection.
            //   markerRank = 8: L0's alias, L1's carry + its MAX(), L2/L3/L4
            //     carries, the L4 ORDER BY, the outer ORDER BY. (It does NOT
            //     overlap `outpointMarkerRank` — that one capitalises the M.)
            //   outpointMarkerRank = 7: L1's alias, L2/L3/L4 carries, the
            //     DENSE_RANK ordering, the L4 ORDER BY, the outer ORDER BY.
            // The `?gameId=` scope adds no rank identifier, so both shapes
            // must agree — which is why this runs inside the `scoped` loop.
            assert_eq!(sql.matches("markerValid").count(), 10, "{sql}");
            assert_eq!(sql.matches("markerRank").count(), 8, "{sql}");
            assert_eq!(sql.matches("outpointMarkerRank").count(), 7, "{sql}");
            assert!(sql.contains("WHERE hp.identity = ?1"));
            assert!(sql.contains(&format!("WHERE rn <= {HOPS_VIEW_ROWS_PER_OUTPOINT}")));
            assert!(sql.contains(&format!(
                "WHERE finalRank <= {}",
                HOPS_VIEW_MAX_OUTPOINTS + 1
            )));
            // The column is also SELECTED, so the reader can label the row
            // rather than infer the label from its position.
            assert!(sql.contains("w.markerValid AS markerValid"));
        }
    }

    /// The `?gameId=` scope adds exactly one bind and one AND — it must not
    /// become a second filter dimension by accident.
    #[test]
    fn the_game_scope_adds_one_bind_and_one_and() {
        let unscoped = hops_view_sql(false, None);
        let scoped = hops_view_sql(true, None);
        assert_eq!(scoped.matches('?').count(), 2);
        assert!(scoped.contains("WHERE hp.identity = ?1 AND hp.gameId = ?2"));
        assert_eq!(
            scoped.len() - unscoped.len(),
            " AND hp.gameId = ?2".len(),
            "the scope is exactly that clause and nothing else"
        );
    }
}
