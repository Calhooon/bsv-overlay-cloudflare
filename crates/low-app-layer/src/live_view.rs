//! `/live-view` — the per-identity LIVE-HAND view (bsv-low #252 stage 2a
//! step 3).
//!
//! For one identity, every pot it is a party to whose spend the overlay has
//! NOT confirmed — the pots a Home surface must keep in front of the user
//! because money may still be in motion (an unspent pot, a displaceable
//! unconfirmed spend, or a pot the overlay has not indexed at all). A pot
//! with a CONFIRMED spend is NOT live and is excluded here — its story is
//! `/refund-view`'s and `/results`' to narrate.
//!
//! ## Liveness (who is in the list)
//!
//! The SQL keeps a caller's pot when `NOT (spent AND spentConfirmed)` on the
//! joined `pot_records` row, NULL-safe via COALESCE so a JOIN MISS (a party
//! marker whose pot the overlay never indexed) is INCLUDED with
//! `spent: null` — spend status genuinely unknown is possibly-live, and the
//! error direction is safe: showing a pot the user may still need to act on
//! costs a glance; hiding one can hide recoverable money. The converse rule
//! is the honesty bar: this route never asserts "not live" from a fault — a
//! D1 fault is a 503, never an empty list.
//!
//! An UNCONFIRMED recorded spend stays live on purpose: the pointer is a
//! displaceable intent (the parked-refund lesson — an indexer reports a
//! parked non-final tx as the mempool spender), so until the spend confirms
//! the pot can still change hands.
//!
//! ONE-WAY-LATCH DEPENDENCY (documented, accepted): the exclusion rides
//! `pot_records.spentConfirmed`, and its producer
//! (`overlay-cloudflare/src/d1_discovery.rs::mark_spent_sql`) only ever sets
//! that latch 0→1 — nothing resets it. A reorg that ORPHANS a confirmed
//! spend would therefore keep the pot hidden from this view even while it is
//! momentarily actionable again. The exposure is narrow and self-healing —
//! the orphaned spend normally re-mines from the mempool within a block or
//! two (and if a DIFFERENT spend of the pot confirms instead, the pointer-
//! guarded writer records it) — and `/refund-view` plus the client's own
//! per-pot polls are independent surfaces; but a pot vanishing from
//! `/live-view` is a LATCH fact, not fresh chain truth. Revisit if the
//! overlay ever grows reorg-rollback for `spentConfirmed`.
//!
//! ## The window is the `/refund-view` window (bsv-low #281)
//!
//! `tm_potparty` admission is byte-format-only, so the same dust-DoS window
//! applies verbatim: per-POT-OUTPOINT collapse (the OLDEST marker is the
//! representative — the only order an attacker cannot win by publishing
//! later), pot-existence tier with the newest
//! [`LIVE_VIEW_UNKNOWN_POT_QUOTA`] unknown pots promoted, LIMIT
//! [`LIVE_VIEW_MAX_ROWS`]. Same accepted residuals as `refund_view_sql`
//! (real-txid window flood; oldest-marker display-field front-run — which is
//! why the served recovery height prefers the covenant-COMMITTED column).
//! `potVout` is part of the key; the CLIENT only honors vout-0 pots (the pot
//! is always the funding tx's vout 0) — other vouts are served as data, not
//! asserted meaningful.
//!
//! **The quota bounds PROMOTION, not PRESENCE.** Only the newest
//! [`LIVE_VIEW_UNKNOWN_POT_QUOTA`] unknown pots are promoted into tier 0;
//! the demoted rest still compete for the remaining `LIMIT` slots, so a dust
//! flood can occupy up to `LIVE_VIEW_MAX_ROWS - 1` of the page (proven: 1
//! honest pot + 300 ghosts ⇒ 100 rows, 99 of them ghosts, the honest pot
//! still present). Inherited from the gate-passed `refund_view_sql` and
//! harmless for the row list itself — but it is the multiplier behind the
//! per-request verification cost, which is why the verify budget below is
//! sized against a 99-ghost page and spent in QUALITY order.
//!
//! ## Marker corroboration (what a row's marker fields may claim)
//!
//! Two of a row's fields are ATTACKER-WRITABLE marker claims this view
//! previously served bare: `gameId` (the tower-case join key) and
//! `opponentIdentity`. They are corroborated per POT — never from the window
//! representative — because **production publishes the v1 marker BEFORE the
//! v2 seat-binding one** (bsv-low `app/src/lib/overlay.ts`: v2 is published
//! "ALONGSIDE v1, never instead of it"; `potPartyRepublish.ts` awaits the v1
//! half first), `createdAt` is server-side with second granularity, and the
//! representative is the OLDEST row (`pp.rowid ASC` breaks a same-second tie
//! the same way) — so the representative IS the v1 row, and a
//! representative-only check could never see the genuine v2 marker sitting
//! in the same table.
//!
//! The architecture is `/results`' (`results.rs`: "The fix is not a better
//! sort: it is to fetch seat markers under the pot's OWN COMMITTED KEYS"):
//!
//! 1. a SECOND bounded query gathers the pot's v2 CANDIDATES —
//!    [`crate::results::seat_markers_sql`] verbatim (per-`(pot, committed
//!    key)` slot window, membership enforced IN SQL) for pots whose covenant
//!    params the overlay decoded, and [`keyless_candidates_sql`] (per-
//!    outpoint window) for join-miss/bare pots that have no committed keys
//!    to bind. The representative row's own marker joins the pool when it
//!    happens to be v2 (strictly additive — every candidate is re-verified);
//! 2. only candidates whose `identity` IS THE CALLER are considered (this
//!    view relays the caller's OWN signed claim, never the counterparty's);
//! 3. cheap rejections first — outpoint match, and lock membership
//!    (`seatSettlePubkey ∈ {pubA, pubB}`) WHERE the decoded columns exist,
//!    which is exactly the pre-filter `attribute_seats` applies before
//!    spending curve time;
//! 4. the FIRST surviving candidate that VERIFIES — both
//!    [`crate::results::verify_seat_marker`] (settle-key seat signature over
//!    the {gameId, potOutpoint, identity} preimage) and
//!    [`crate::results::verify_identity_binding`] (the caller's own identity
//!    signature over the full v2 challenge, which binds `gameId`,
//!    `opponentIdentity`, the outpoint, the height hint and the settle key)
//!    — supplies that pot's `gameId`, `opponentIdentity` and fan-out
//!    eligibility. Only the identity holder can mint the identity signature,
//!    so a corroborated row's claims are the CALLER'S OWN: a third party
//!    cannot steer them.
//!
//! A pot with no verifying candidate keeps the representative's UNVERIFIED
//! values and says so (`markerSource: "marker-unverified"`,
//! `caseSource: "marker-unverified"`, no case fetch) — silence over speech,
//! and the row is still SERVED, never hidden. A candidate-query FAULT is
//! labeled `"corroboration-unavailable"` instead: "we could not look" is not
//! "there is nothing to find".
//!
//! Two properties of the candidate window worth recording (R2-3):
//! `seat_markers_sql` windows EIGHT rows deep per `(pot, committed key)`, so
//! an honest marker crowded behind junk is normally still IN the fetched
//! list — what can drop it is this view's own
//! [`LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT`] slot cap, i.e. an allocation
//! choice, not a data limitation (raising it trades CPU ceiling for crowding
//! depth). And the crowding RACE WINDOW is the v1→v2 publication gap: the
//! victim's v1 marker lands first and already publishes identity, potTxid,
//! gameId and opponent, while `pubA` is public in the funding lock — so a
//! watching attacker has that whole interval to land pre-built markers under
//! the committed key. Measured: `cap - 1` junk rows ahead of the honest
//! marker still corroborate, `cap` suppress it (fail-safe: no case, never a
//! wrong claim). Round-robin improves the economics — those junk rows now
//! only push the pot into later passes, and every other pot keeps its pass-0
//! attempt.
//!
//! Sorting is deliberately NOT used to prefer v2-shaped rows: admission is
//! byte-format-only, so an attacker could publish a v2-SHAPED junk marker
//! with an earlier `createdAt` and reclaim the representative slot — one
//! marker instead of ten. Only "verify candidates, first verifier wins" is
//! sound.
//!
//! ### Verification budget (attacker-forced curve work)
//!
//! Verification is ECDSA + BRC-42 derivation, and an attacker can force the
//! expensive half: crafting their OWN settle key over a seat preimage that
//! embeds the VICTIM's identity passes `verify_seat_marker` (which never
//! asks whose key it is), so the identity check runs before failing. On a
//! public, unauthenticated, uncached endpoint callable for any identity that
//! must be BOUNDED, not merely correct:
//!
//! - at most [`LIVE_VIEW_VERIFY_BUDGET`] candidate ATTEMPTS per request
//!   (each ≤ 2 ECDSA verifies + 1 BRC-42 derivation), and at most
//!   [`LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT`] per pot;
//! - allocated ROUND-ROBIN BY CANDIDATE DEPTH across the page's pots, with
//!   QUALITY order (known pots before ghosts, window order within a class)
//!   as the tie-break WITHIN a depth pass. What that guarantees EXACTLY:
//!   since an honest marker is its pot's depth-0 candidate, pass 0 spends one
//!   attempt on each pot in order, so **on a page of at most
//!   [`LIVE_VIEW_VERIFY_BUDGET`] pots no amount of CROWDING on other pots
//!   can take a pot's first attempt** — crowding only delays the crowded pot
//!   to a later pass. (A depth-FIRST allotment had no such property: 8
//!   attacker-funded KNOWN, newer pots × 4 junk candidates drained the whole
//!   budget before an older victim pot's single honest attempt, with no race
//!   against the victim's marker — the R2-1 finding.) What it does NOT
//!   promise: a page with MORE than the budget's worth of pots still leaves a
//!   tail uncorroborated (below) — pushing a victim there now costs the
//!   attacker 32 separately FUNDED, admitted pots rather than 8 crowded ones;
//! - rows the budget never reached, and rows whose slots all failed, are
//!   `markerSource: "marker-unverified"` — honest and fail-safe:
//!   "unverified" means "not corroborated", never "forged". A candidate
//!   QUERY FAULT is the distinct `"corroboration-unavailable"` (we could not
//!   look, see the provenance table).
//!
//! Sizing (ONE decision with the candidate slots): the budget is exactly
//! `LIVE_VIEW_CASE_FANOUT_CAP × LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT`
//! (8 × 4 = 32), compile-time pinned — so a full fan-out's worth of pots can
//! always be corroborated EVEN IF every one of them is crowded to its
//! per-pot attempt cap, and no more. The reviewer measured ~0.29 ms per full
//! attempt natively (29.3 ms for an UNBOUNDED 100-row hostile page ⇒
//! ~60–120 ms in wasm); 32 attempts is ~9 ms native, i.e. ~19–37 ms of wasm
//! CPU worst case, and it cannot grow with page size or ghost count. The
//! per-pot cap 4 is the crowding depth an attacker must out-file to suppress
//! ONE pot's corroboration (half of `/results`' 8-deep
//! [`crate::results::SEAT_MARKERS_PER_KEY`] bar — the trade for a hard CPU
//! ceiling on an unauthenticated endpoint; the effect is display-tier,
//! fail-safe, and never a wrong claim). Honest traffic spends ONE attempt
//! per live pot (the caller's single genuine marker, membership-prefiltered).
//!
//! ## Gate math
//!
//! `recoveryHeight`/`blocksToGate`/`gatePassed` are exactly `/refund-view`'s
//! (shared helper [`crate::refund_view::served_recovery_height`]):
//! covenant-committed height preferred over the byte-format-admitted marker
//! hint, both range-checked (nonsense serves `null`, never a fake
//! countdown); `gatePassed` is `false` whenever either height or tip is
//! unknown (a gate is never reported open on missing data).
//!
//! ## The `case` half (bounded, corroborated watchtower fan-out)
//!
//! At most [`LIVE_VIEW_CASE_FANOUT_CAP`] DISTINCT corroborated gameIds get a
//! fetch of the tower's public `GET /case/:gameId` through the `TOWER`
//! service binding, each fetch raced against a
//! [`CASE_FETCH_TIMEOUT_MS`]-millisecond `worker::Delay` (the tower's own
//! #272 `with_timeout` idiom) and byte-bounded BEFORE buffering
//! (Content-Length pre-gate + a hard [`CASE_BODY_MAX_BYTES`] budget on the
//! streamed read via [`BodyAccumulator`] — a wedged or compromised tower can
//! never balloon this Worker's memory). The body is shaped by
//! [`parse_case_body`] into the VALIDATED subset `{status, epoch,
//! deadlineMs, accused}` — only fields the tower actually serves
//! (`case.rs::view`), each bounded/enum-mapped/range-checked; nothing is
//! ever invented.
//!
//! **Fan-out targets are chosen by QUALITY, not window position**
//! ([`fanout_targets`]): promoted ghost/unknown-pot rows sort into tier 0
//! AHEAD of known pots (they are newest), and the cap is not larger than the
//! unknown-pot quota (compile-time pinned below), so taking "the first cap
//! rows" would let quota-many dust markers burn every fetch slot and starve
//! a victim's real live pot's case. Instead: only CORROBORATED pots are
//! eligible at all, KNOWN pots (`spent.is_some()` — exactly the
//! `pot_records` join hit, since `pot_records.spent` is `INTEGER NOT NULL
//! DEFAULT 0`) come first, and window order breaks ties within each class.
//!
//! **The case↔pot binding is NOT verified — and this surface says so.** The
//! join key is the pot's corroborated `gameId`; even as the caller's own
//! signed claim it does not bind the ANSWER to this pot: the tower's public
//! case view is per-GAME, not per-outpoint — it serves the game's PRIMARY
//! case and its body carries no pot outpoint — so two live pots sharing one
//! gameId (the funding-retry shape) both receive the SAME answer, and at
//! most one can be right. This view therefore never vouches for the
//! binding: on success the provenance tag is `"tower-by-gameid-unverified"`
//! — meaning EXACTLY "the tower's answer for THIS gameId", never "this case
//! belongs to this pot" — and `caseGameId` carries the 64-hex join key
//! actually fetched.
//!
//! What `caseGameId` DOES defend against: a SUBSTITUTED key (the client
//! compares it against its own game truth) and a SHARED key (two rows of
//! one response carrying it). What it does NOT defend against: a gameId
//! REUSED ACROSS GAMES. The host mints the invite code (bsv-low
//! `app/src/pages/Table.tsx`), so a hostile host can reuse a gameId from an
//! earlier game that already has a TERMINAL case; the victim's honest client
//! signs a marker naming it, the row corroborates, and `GET /case/:gameId`
//! returns the EARLIER game's terminal record (the tower's `primary_case`
//! prefers a terminal one). `caseGameId` then equals the row's own gameId,
//! so a single-row client cannot detect it — only the non-vouching tag
//! carries the honesty load. The CROSS-REPO FOLLOW-UP (deliberately not in
//! this commit) is the ONLY real closure: extend the tower's public case
//! view with `potTxid`/`potVout` (its `CaseRecord` knows the pot outpoint)
//! and REJECT in [`shape_case`] unless it equals the row's outpoint; only
//! then may the tag ever vouch.
//!
//! **Provenance is four-valued** ([`CaseProvenance`]) so `case: null` never
//! conflates its reasons — but in EVERY non-success state `case: null` keeps
//! meaning UNKNOWN, never "no case exists":
//!
//! | `caseSource`                    | meaning                                                                                   |
//! |---------------------------------|-------------------------------------------------------------------------------------------|
//! | `"tower-by-gameid-unverified"`  | fetch succeeded; the tower's answer FOR THIS gameId                                        |
//! | `"tower-unavailable"`           | we asked (or should have) and got no valid answer                                          |
//! | `"not-fetched"`                 | eligible but not selected (fan-out cap) / unfetchable gameId                               |
//! | `"marker-unverified"`           | no candidate verified, OR the verify budget was spent before this pot (>32-pot page tail)  |
//! | `"corroboration-unavailable"`   | the candidate QUERY faulted — we could not look at all                                     |
//!
//! `markerSource` carries the same three-valued corroboration bit for the
//! row's `gameId`/`opponentIdentity`: `"seat-signed"` /
//! `"marker-unverified"` (nothing verified, or budget-exhausted tail) /
//! `"corroboration-unavailable"` (query fault).
//!
//! ANY fault — binding absent, timeout, non-200 (including the tower's 404
//! "no case"), oversized or malformed body, unrecognized status — serves
//! `case: null` and NEVER a 5xx (the `/results` claims-fault posture: a
//! tower blip must not take down the D1 half, which must always serve).

use serde_json::json;

use crate::refund_view::served_recovery_height;
use crate::results::{SeatMarkerBind, SeatMarkerRow};

/// Hard bound on `/live-view` rows per request — same cap + rationale as
/// [`crate::refund_view::REFUND_VIEW_MAX_ROWS`] (a cap on DISTINCT POTS
/// since the window is per-pot-outpoint).
pub const LIVE_VIEW_MAX_ROWS: usize = 100;

/// How many of the newest pots ABSENT from `pot_records` are PROMOTED into
/// the main tier — same reservation + rationale as
/// [`crate::refund_view::REFUND_VIEW_UNKNOWN_POT_QUOTA`] (a fresh pot whose
/// `tm_pot` admission is in flight is exactly the pot a LIVE view must not
/// hide). It bounds PROMOTION only: demoted ghosts still compete for the
/// remaining [`LIVE_VIEW_MAX_ROWS`] slots (module docs).
pub const LIVE_VIEW_UNKNOWN_POT_QUOTA: usize = 10;

const _: () = assert!(LIVE_VIEW_UNKNOWN_POT_QUOTA < LIVE_VIEW_MAX_ROWS);

/// The tower case fan-out cap: at most this many DISTINCT gameIds get a
/// `GET /case/:gameId` fetch per request. A CAP, not an assertion — pots
/// whose gameId is not fetched serve `case: null` with an honest
/// [`CaseProvenance`] tag. 8 bounds the worst case to 8 service-binding
/// subrequests per view request (well under Workers' subrequest budget)
/// while covering every realistic Home surface (one identity rarely has
/// more than a couple of live pots; more than 8 is already a dust-window
/// story).
pub const LIVE_VIEW_CASE_FANOUT_CAP: usize = 8;

// The cap does not exceed the unknown-pot quota, so an attacker's
// quota-many promoted ghost rows CAN occupy every head-of-window slot —
// which is exactly why [`fanout_targets`] selects by QUALITY (corroborated
// only, known pots before unknown) and never by raw window position. If this
// relationship ever flips (cap > quota), position-based selection would
// still be wrong (a flood of REAL newest pots outranks the victim's), so
// re-review `fanout_targets` rather than "simplifying" it back to
// `take(CAP)`.
const _: () = assert!(LIVE_VIEW_CASE_FANOUT_CAP <= LIVE_VIEW_UNKNOWN_POT_QUOTA);

/// Per-request ceiling on candidate VERIFICATION ATTEMPTS (each ≤ 2 ECDSA
/// verifies + 1 BRC-42 derivation) — the attacker-forced-curve-work bound.
/// Spent in [`quality_order`]; pots beyond it are honestly
/// `marker-unverified`. Sizing rationale: module docs.
pub const LIVE_VIEW_VERIFY_BUDGET: usize = 32;

/// Per-POT ceiling on candidate attempts. Bounds what crowding ONE pot's
/// candidate window can cost the request (and therefore what it can steal
/// from other pots). A pot whose honest marker sits deeper than this keeps
/// `marker-unverified` — fail-safe (corroboration omitted, never wrong), the
/// same residual class as `/results`'
/// [`crate::results::SEAT_MARKERS_PER_KEY`] eviction bar (bsv-low #283c).
pub const LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT: usize = 4;

// A full fan-out's worth of pots can always be corroborated even if every
// one of them is crowded to its per-pot attempt cap — and the budget is not
// larger than that (the CPU ceiling is the point). Change one of the three
// constants and this pin forces the other two to be re-decided.
const _: () = assert!(
    LIVE_VIEW_VERIFY_BUDGET == LIVE_VIEW_CASE_FANOUT_CAP * LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT
);

/// Pots per [`keyless_candidates_sql`] chunk: 1 identity bind + 2 per pot
/// (`potTxid`, `potVout`) = 49 binds at 24 pots, well under D1's cap.
pub const LIVE_VIEW_CANDIDATE_CHUNK_POTS: usize = 24;

/// Binds a full [`keyless_candidates_sql`] chunk issues: the identity plus
/// `(potTxid, potVout)` per pot.
pub const fn keyless_chunk_binds(pots: usize) -> usize {
    1 + pots * 2
}

const _: () = assert!(
    keyless_chunk_binds(LIVE_VIEW_CANDIDATE_CHUNK_POTS) <= crate::logic::D1_MAX_BOUND_PARAMS
);

/// Per-case-fetch timeout (ms). The fetches run CONCURRENTLY, so this also
/// bounds the whole fan-out's added latency. 3 s is generous for a
/// same-account service-binding hop to a DO-backed GET, and small enough
/// that a wedged tower degrades this route by seconds, not minutes.
pub const CASE_FETCH_TIMEOUT_MS: u64 = 3_000;

/// Timeout (ms) for the chaintracks tip hop — same idiom as the case
/// fetches, and the tip fetch runs CONCURRENTLY with them, so the route's
/// added latency is `max(tip, cases)`, not `tip + cases`. A timeout
/// degrades to `tip: null` (gate fields `null`/`false`), never an error.
pub const TIP_FETCH_TIMEOUT_MS: u64 = 3_000;

/// Hard bound on an accepted `/case` response body. The tower's case view
/// carries the re-serve envelope chain + `J`, which can reach tens of KB for
/// a real dispute; 512 KB accommodates any genuine case while refusing to
/// buffer an adversarial flood. Enforced BEFORE buffering (Content-Length
/// pre-gate + [`BodyAccumulator`]'s streamed budget) and again at parse time
/// ([`parse_case_body`], belt). Larger ⇒ `case: null` (fail-closed).
pub const CASE_BODY_MAX_BYTES: usize = 512 * 1024;

/// Integer acceptance ceiling for tower-served numbers: JavaScript's
/// `Number.MAX_SAFE_INTEGER` (2^53 − 1). Anything above (or negative, or
/// fractional) is served `null`, never a rounded lie.
pub const CASE_MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// The single `/live-view` SQL (ONE bind: the lowercase identity). The #281
/// window over the caller's `potparty_records` rows LEFT-JOINed to
/// `pot_records`, with the LIVENESS filter applied INSIDE the window's
/// innermost scan: a pot is kept unless its joined row shows a CONFIRMED
/// spend. COALESCE makes the predicate NULL-safe — a join miss (r.spent
/// NULL) or an unconfirmed spend (spentConfirmed 0/NULL) stays live; plain
/// `NOT (r.spent = 1 AND r.spentConfirmed = 1)` would evaluate NULL for a
/// join miss and silently drop exactly the rows this view must not hide.
/// The representative marker's v2 columns ride along (it is ONE candidate
/// among the pot's markers — never the sole corroboration source, module
/// docs), as do the pot's DECODED committed settle keys (`pubA`/`pubB`,
/// #284) which drive the free membership pre-filter and the keyed candidate
/// query. No BLOB is ever touched.
///
/// # #375 era write-off
///
/// `written_off_before_ms` set ⇒ the innermost scan additionally drops rows
/// whose era anchor (`COALESCE(r.createdAt, pp.createdAt)` — the pot's
/// admission stamp when indexed, else the marker's; both server-written
/// unix seconds) pre-dates the cutoff: a written-off pre-launch pot is
/// never a LIVE hand, however its liveness columns read. ONE extra bind
/// (the cutoff, after the identity). `None` ⇒ byte-identical to the
/// pre-#375 query.
pub fn live_view_sql(written_off_before_ms: Option<i64>) -> String {
    format!(
        "SELECT w.identity AS identity, w.gameId AS gameId, \
                w.potTxid AS potTxid, w.potVout AS potVout, \
                w.opponentIdentity AS opponentIdentity, \
                w.recoveryHeight AS recoveryHeight, \
                w.covRecoveryHeight AS covRecoveryHeight, \
                w.sigHex AS sigHex, w.seatSettlePubkey AS seatSettlePubkey, \
                w.seatSigHex AS seatSigHex, \
                w.covPubA AS covPubA, w.covPubB AS covPubB, \
                w.spent AS spent, w.spendingTxid AS spendingTxid, \
                w.spentConfirmed AS spentConfirmed \
         FROM (SELECT identity, gameId, potTxid, potVout, opponentIdentity, recoveryHeight, \
                  covRecoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                  covPubA, covPubB, spent, spendingTxid, spentConfirmed, \
                  markerCreatedAt, markerRowid, potCreatedAt, potBestSigRank, \
                  CASE WHEN unknownPot = 0 OR potRank <= {quota} THEN 0 ELSE 1 END AS tier \
           FROM (SELECT identity, gameId, potTxid, potVout, opponentIdentity, recoveryHeight, \
                    covRecoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                    covPubA, covPubB, spent, spendingTxid, spentConfirmed, \
                    markerCreatedAt, markerRowid, potCreatedAt, unknownPot, \
                    potBestSigRank, \
                    ROW_NUMBER() OVER (PARTITION BY unknownPot \
                                       ORDER BY potBestSigRank DESC, \
                                                COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                                                markerCreatedAt DESC, markerRowid DESC) AS potRank \
             FROM (SELECT pp.identity AS identity, pp.gameId AS gameId, pp.potTxid AS potTxid, \
                      pp.potVout AS potVout, pp.opponentIdentity AS opponentIdentity, \
                      pp.recoveryHeight AS recoveryHeight, \
                      r.recoveryHeight AS covRecoveryHeight, \
                      pp.sigHex AS sigHex, pp.seatSettlePubkey AS seatSettlePubkey, \
                      pp.seatSigHex AS seatSigHex, \
                      r.pubA AS covPubA, r.pubB AS covPubB, \
                      r.spent AS spent, r.spendingTxid AS spendingTxid, \
                      r.spentConfirmed AS spentConfirmed, \
                      pp.createdAt AS markerCreatedAt, pp.rowid AS markerRowid, \
                      r.createdAt AS potCreatedAt, \
                      CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                      MAX({rank}) OVER (PARTITION BY pp.potTxid, pp.potVout) \
                          AS potBestSigRank, \
                      ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout \
                                         ORDER BY {rank} DESC, \
                                                  pp.createdAt ASC, pp.rowid ASC) AS rn \
               FROM potparty_records pp \
               LEFT JOIN pot_records r \
                      ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
               WHERE pp.identity = ? \
                 AND (COALESCE(r.spent, 0) = 0 OR COALESCE(r.spentConfirmed, 0) = 0){era}) \
             WHERE rn = 1) \
           ORDER BY potBestSigRank DESC, tier ASC, \
                    COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                    markerCreatedAt DESC, markerRowid DESC \
           LIMIT {rows}) w \
         ORDER BY w.potBestSigRank DESC, w.tier ASC, \
                  COALESCE(w.potCreatedAt, w.markerCreatedAt) DESC, \
                  w.markerCreatedAt DESC, w.markerRowid DESC",
        quota = LIVE_VIEW_UNKNOWN_POT_QUOTA,
        rows = LIVE_VIEW_MAX_ROWS,
        rank = overlay_discovery::potparty::validity::sig_rank_expr("pp."),
        era = crate::logic::era_filter_sql(
            "COALESCE(r.createdAt, pp.createdAt)",
            "?",
            written_off_before_ms
        ),
    )
}

/// The KEYLESS candidate query for `n` pots the overlay has NO decoded
/// committed keys for (a join miss, or a bare/legacy lock): the caller's own
/// v2-shaped markers for those outpoints, windowed per OUTPOINT to
/// [`LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT`] rows. Binds: the identity, then
/// `(potTxid, potVout)` per pot.
///
/// Pots WITH decoded keys use [`crate::results::seat_markers_sql`] verbatim
/// instead — there the committed keys are bound and membership is enforced
/// in SQL, which is strictly stronger. This query exists only so a join-miss
/// row (a fresh pot whose `tm_pot` admission is in flight — exactly what a
/// LIVE view must not hide) keeps the pre-existing semantics: verifiable
/// without membership.
///
/// The residual this used to carry — "an attacker who knows the pot txid can
/// file [`LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT`] v2-SHAPED junk rows stamped
/// before the honest marker and evict it; SQL cannot pick the row that
/// verifies; only admission-side pricing would close it" — is closed the same
/// way [`crate::results::seat_markers_sql`]'s is (bsv-low #283): SQL does not
/// have to PICK the verifying row, only to sort by a verdict latched at
/// admission. `{rank}` is `potparty_records.sigValid` and it leads the
/// window. Pricing was never on the table anyway (bsv-low#347: a marker is
/// free, not dust-priced).
///
/// This window is BOTH identity- and outpoint-scoped, so a junk row must name
/// the caller AND cannot carry the caller's identity signature — rank 0 at
/// any volume or stamp.
///
/// Unchanged: the fail direction (corroboration OMITTED, never wrong), and
/// the LEGACY tier (`sigValid IS NULL`, pre-migration rows) which orders
/// exactly as before. That tier cannot grow and does not drain by any CLIENT
/// behaviour; what drains it — and repairs a rank-0 row a transient predicate
/// fault produced — is the overlay's re-latch sweep
/// (`bsv_overlay_cloudflare::relatch`, bsv-low#355), a re-latch of EVERY row
/// rather than a `NULL`-only backfill. It is bounded per tick, so a row stays
/// legacy until a sweep reaches it.
pub fn keyless_candidates_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let per_pot = vec!["(potTxid = ? AND potVout = ?)"; n].join(" OR ");
    format!(
        "SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                recoveryHeight, seatSettlePubkey, seatSigHex, sigHex, sigValid \
         FROM (SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                      recoveryHeight, seatSettlePubkey, seatSigHex, sigHex, sigValid, \
                      ROW_NUMBER() OVER (PARTITION BY potTxid, potVout \
                                         ORDER BY {rank} DESC, \
                                                  createdAt ASC, rowid ASC) AS rn \
               FROM potparty_records \
               WHERE identity = ? AND seatSettlePubkey IS NOT NULL AND ({per_pot})) \
         WHERE rn <= {cap} \
         ORDER BY potTxid ASC, potVout ASC, rn ASC",
        cap = LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT,
        rank = overlay_discovery::potparty::validity::sig_rank_expr(""),
    )
}

/// One `/live-view` joined row, host-typed (the `live_view_sql` shape). The
/// pot-side fields are `Option` because the `pot_records` join can MISS — a
/// party marker whose pot the overlay never indexed yields NULL columns
/// (fail-safe: never asserted unspent, and INCLUDED as possibly-live).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveViewRow {
    /// The marker row's OWN identity column (the SQL binds the caller, but
    /// verification takes the identity from the ROW — the #230 F8
    /// discipline: a future SQL change can't silently decouple them).
    pub identity: String,
    /// The REPRESENTATIVE marker's gameId — byte-format-admitted; served
    /// only when the pot has NO verifying candidate (and labeled so).
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    /// The representative marker's opponent claim — byte-format-admitted.
    /// Served only when identity-shaped (66 hex) and ALWAYS with a
    /// provenance tag — see [`assemble_live_view`].
    pub opponent_identity: Option<String>,
    /// The caller's OWN potparty marker height — byte-format-admitted, a
    /// HINT (used only when no covenant-committed height exists).
    pub marker_recovery_height: u32,
    /// The COVENANT-COMMITTED recoveryHeight decoded from the admitted
    /// funding lock (#284) — chain truth; `None` for bare/legacy rows.
    pub cov_recovery_height: Option<u64>,
    /// The representative marker's IDENTITY signature push (`sigHex`).
    pub identity_sig_hex: Option<String>,
    /// The representative marker's v2 settle pubkey — `None` on a v1 marker
    /// (which production files FIRST, so in practice this IS `None`; the
    /// pot's real v2 marker arrives through the candidate query).
    pub seat_settle_pubkey: Option<String>,
    /// The representative marker's v2 seat signature.
    pub seat_sig_hex: Option<String>,
    /// The pot's DECODED committed settle keys (`pot_records.pubA`/`pubB`,
    /// #284) — chain truth an attacker cannot file. `None` for join-miss and
    /// bare/legacy pots.
    pub cov_pub_a: Option<String>,
    pub cov_pub_b: Option<String>,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
}

impl LiveViewRow {
    /// The pot outpoint key (lowercased txid + vout).
    pub fn key(&self) -> (String, u32) {
        (self.pot_txid.to_ascii_lowercase(), self.pot_vout)
    }

    /// Whether the `pot_records` join HIT — exactly `spent.is_some()`, since
    /// `pot_records.spent` is `INTEGER NOT NULL DEFAULT 0` (so a NULL can
    /// only mean "no row"). Drives quality ordering everywhere.
    pub fn pot_known(&self) -> bool {
        self.spent.is_some()
    }

    /// The representative row's OWN marker as a candidate, when it happens
    /// to carry the full v2 field set. Strictly additive to the fetched
    /// candidates (every candidate is re-verified) — the same discipline
    /// `/results` applies to its rows' markers.
    pub fn own_marker(&self) -> Option<SeatMarkerRow> {
        let (Some(pk), Some(seat_sig), Some(id_sig), Some(opp)) = (
            self.seat_settle_pubkey.as_deref(),
            self.seat_sig_hex.as_deref(),
            self.identity_sig_hex.as_deref(),
            self.opponent_identity.as_deref(),
        ) else {
            return None;
        };
        Some(SeatMarkerRow {
            identity: self.identity.to_ascii_lowercase(),
            opponent_identity: opp.to_ascii_lowercase(),
            game_id: self.game_id.to_ascii_lowercase(),
            pot_txid: self.pot_txid.to_ascii_lowercase(),
            pot_vout: self.pot_vout,
            recovery_height: self.marker_recovery_height,
            seat_settle_pubkey: pk.to_ascii_lowercase(),
            seat_sig_hex: seat_sig.to_ascii_lowercase(),
            identity_sig_hex: id_sig.to_ascii_lowercase(),
            sig_valid: None, // additive candidate from a non-latch source — compute
        })
    }
}

/// Row indices in QUALITY order: KNOWN pots (join hit) first, then unknown
/// ones, window order preserved within each class. The SINGLE ordering used
/// by the candidate plan, the verification budget AND the fan-out selection,
/// so dust that owns the window head can starve none of them.
pub fn quality_order(rows: &[LiveViewRow]) -> Vec<usize> {
    let mut out: Vec<usize> = Vec::with_capacity(rows.len());
    for want_known in [true, false] {
        for (i, r) in rows.iter().enumerate() {
            if r.pot_known() == want_known {
                out.push(i);
            }
        }
    }
    out
}

/// The bounded CANDIDATE-fetch plan for a page, split by whether the pot's
/// committed keys are known, and chunked so every query stays under D1's
/// bound-param ceiling. Capped at [`LIVE_VIEW_VERIFY_BUDGET`] pots in
/// [`quality_order`] — beyond that the verify budget is spent anyway, so
/// fetching more candidates would be work nobody can use. Pure, so the
/// delivery path is testable without a Worker (the #230 re-gate lesson: this
/// whole fetch could be deleted with no test failing).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CandidatePlan {
    /// Pots WITH decoded committed keys — one chunk per
    /// [`crate::results::seat_markers_sql`] query (membership in SQL).
    pub keyed: Vec<Vec<SeatMarkerBind>>,
    /// Pots WITHOUT decoded keys — one chunk per [`keyless_candidates_sql`].
    pub keyless: Vec<Vec<(String, u32)>>,
}

/// Build the [`CandidatePlan`] for a page (deterministic: quality order,
/// deduped by outpoint).
///
/// The committed-key binds are LOWERCASED here, and `seat_markers_sql`
/// compares them with SQLite `IN (?, ?)` — a case-SENSITIVE TEXT compare. The
/// host-side membership pre-filter in [`corroborate_rows`] therefore
/// lowercases and compares EXACTLY too, so the two layers cannot disagree
/// about which keys are committed (R2-5). Both producers (`#284` decode and
/// the marker admission) `hex::encode` lowercase today; this keeps that from
/// being load-bearing in two different ways.
pub fn candidate_plan(rows: &[LiveViewRow]) -> CandidatePlan {
    let mut keyed: Vec<SeatMarkerBind> = Vec::new();
    let mut keyless: Vec<(String, u32)> = Vec::new();
    let mut seen: std::collections::HashSet<(String, u32)> = std::collections::HashSet::new();
    for i in quality_order(rows) {
        if seen.len() >= LIVE_VIEW_VERIFY_BUDGET {
            break;
        }
        let r = &rows[i];
        let key = r.key();
        if !seen.insert(key.clone()) {
            continue;
        }
        match (r.cov_pub_a.as_deref(), r.cov_pub_b.as_deref()) {
            (Some(a), Some(b)) => keyed.push(SeatMarkerBind {
                pot_txid: key.0,
                pot_vout: key.1,
                pub_a_hex: a.to_ascii_lowercase(),
                pub_b_hex: b.to_ascii_lowercase(),
            }),
            _ => keyless.push(key),
        }
    }
    CandidatePlan {
        keyed: keyed
            .chunks(crate::results::SEAT_MARKERS_CHUNK_POTS)
            .map(<[SeatMarkerBind]>::to_vec)
            .collect(),
        keyless: keyless
            .chunks(LIVE_VIEW_CANDIDATE_CHUNK_POTS)
            .map(<[(String, u32)]>::to_vec)
            .collect(),
    }
}

/// One pot's CORROBORATED claim: the values a VERIFYING v2 marker of the
/// CALLER's own supplies. `gameId` and `opponentIdentity` are then the
/// caller's own signed claims (the identity signature binds both), which is
/// what makes them safe to use as the tower-case join key and as a labeled
/// display fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedClaim {
    pub game_id: String,
    pub opponent_identity: String,
    /// The settle key that carried the seat signature (lowercase hex) — kept
    /// for diagnostics; never served (this view attributes no seats).
    pub seat_settle_pubkey: String,
}

/// The corroboration result for a page: one `Option<VerifiedClaim>` per row
/// (index-aligned), the number of verification ATTEMPTS actually spent (the
/// budget, asserted behaviourally by the tests), and whether the candidate
/// FETCH itself faulted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Corroborated {
    pub claims: Vec<Option<VerifiedClaim>>,
    pub attempts: usize,
    /// A candidate query (or bind) faulted, so this page's corroboration is
    /// "we could not LOOK", not "there is nothing to find" (R2-2). Rows
    /// without a claim then carry the distinct
    /// `corroboration-unavailable` labels instead of `marker-unverified`.
    /// Set by the caller (`routes::live_view`) — the pure corroborator never
    /// touches D1.
    pub unavailable: bool,
}

/// One pot's corroboration work-item: the outpoint, the row indices that
/// share it, and its cheap-filtered/deduped candidate SLOTS (≤ the per-pot
/// attempt cap) in fetch order — depth `d` is `slots[d]`.
struct PotSlots {
    key: (String, u32),
    rows: Vec<usize>,
    slots: Vec<SeatMarkerRow>,
}

/// Corroborate each row's POT (never the window representative — module
/// docs): try the pot's candidate markers and let the FIRST one that VERIFIES
/// supply the claim.
///
/// **Allocation is ROUND-ROBIN BY CANDIDATE DEPTH** across the page's pots
/// (R2-1), not depth-first per pot: pass `d` spends one attempt on each
/// still-unresolved pot that has a `d`-th candidate, pots visited in
/// [`quality_order`] within a pass, until [`LIVE_VIEW_VERIFY_BUDGET`] is
/// exhausted or [`LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT`] passes are done. An
/// honest marker sits at depth 0 (one candidate per pot, measured), so pass 0
/// corroborates EVERY uncrowded pot regardless of how many crowded pots
/// precede it — a depth-first allotment let an attacker who funds their own
/// KNOWN, NEWER pots and crowds each to the per-pot cap drain the whole
/// budget before the victim's older pot got its single attempt, with no race
/// against the victim's marker at all. Crowding now costs the attacker DEPTH
/// (it only delays a pot to a later pass) instead of a flat per-pot block.
/// The worst case is unchanged: `LIVE_VIEW_VERIFY_BUDGET` attempts.
///
/// **SCOPE OF THE GUARANTEE (R3-1) — state it exactly, it is not "every pot".**
/// Pass 0 reaches the FIRST [`LIVE_VIEW_VERIFY_BUDGET`] pots **in
/// [`quality_order`]**, which puts KNOWN (join-hit) pots ahead of unknown ones.
/// So:
/// - A **KNOWN** pot is safe unless the attacker manufactures BUDGET-many known
///   pots, which costs a real funding tx each — the reported cost.
/// - A pot the overlay has **not yet admitted** (`tm_pot` still pending, so the
///   row is a join miss) sits in the UNKNOWN class *alongside ghosts, which are
///   free*: an invented `potTxid` is a join miss, and one v2-shaped marker per
///   ghost serves as both the window row and its single depth-0 candidate. So
///   BUDGET-many OP_RETURN markers — no funding, no race against the victim's
///   marker — can exhaust pass 0 ahead of a genuinely fresh pot. Past the
///   BUDGET-pot plan cap ([`candidate_plan`]) such a pot is not even planned,
///   so no candidate query is issued for it.
///
/// **Corroboration for a not-yet-admitted pot is therefore BEST-EFFORT**, and
/// that is irreducible rather than a missing guard: pre-admission the overlay
/// genuinely cannot distinguish an invented pot from a real in-flight one (the
/// same indistinguishability the #281 unknown-pot quota exists to bound).
/// Capping the unknown class's budget share does not help — the newest ghosts
/// still win the smaller pool; ordering the unknown class by age does not help
/// either, since an attacker can be newer OR older at will. It **self-heals**
/// the moment `tm_pot` admits the pot, which promotes it into the KNOWN class
/// ahead of every ghost. The fail direction is safe throughout:
/// `marker-unverified`, no case fetched, and the pot itself is still served
/// with full gate math, `recoveryHeight` and spend status — nothing hidden,
/// nothing falsely claimed.
///
/// Cheap rejections happen BEFORE any curve work (and before a candidate
/// occupies a depth slot): a candidate must be the CALLER's own
/// (`identity == identity_lc`), must name this row's outpoint, and — where
/// the pot's committed keys are decoded — must carry one of them (the
/// `attribute_seats` pre-filter, compared exactly as the keyed candidate SQL
/// compares it: see [`candidate_plan`]). Identical duplicates are free.
pub fn corroborate_rows(
    identity_lc: &str,
    rows: &[LiveViewRow],
    candidates: &std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>>,
) -> Corroborated {
    let mut out = Corroborated {
        claims: vec![None; rows.len()],
        attempts: 0,
        unavailable: false,
    };
    // One entry per DISTINCT pot, in quality order (see `PotSlots`). Building
    // this costs string compares only — no curve work.
    let mut pots: Vec<PotSlots> = Vec::new();
    for i in quality_order(rows) {
        let r = &rows[i];
        let key = r.key();
        if let Some(p) = pots.iter_mut().find(|p| p.key == key) {
            p.rows.push(i);
            continue;
        }
        let mut slots: Vec<SeatMarkerRow> = Vec::new();
        let pool = r.own_marker().into_iter().chain(
            candidates
                .get(&key)
                .map(|l| l.iter().cloned())
                .into_iter()
                .flatten(),
        );
        for m in pool {
            if slots.len() >= LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT {
                break;
            }
            if !m.identity.eq_ignore_ascii_case(identity_lc) {
                continue; // only the CALLER's own signed claim is relayed
            }
            if !m.pot_txid.eq_ignore_ascii_case(&key.0) || m.pot_vout != key.1 {
                continue; // a marker for another pot corroborates nothing here
            }
            if let (Some(a), Some(b)) = (r.cov_pub_a.as_deref(), r.cov_pub_b.as_deref()) {
                // EXACT compare on lowercase, byte-for-byte what the keyed
                // candidate SQL's `seatSettlePubkey IN (?, ?)` does with the
                // lowercased binds `candidate_plan` builds (R2-5).
                let pk = m.seat_settle_pubkey.to_ascii_lowercase();
                if pk != a.to_ascii_lowercase() && pk != b.to_ascii_lowercase() {
                    continue; // key not committed by THIS pot's lock — free reject
                }
            }
            if slots.contains(&m) {
                continue; // idempotent republish duplicates are free
            }
            slots.push(m);
        }
        pots.push(PotSlots {
            key,
            rows: vec![i],
            slots,
        });
    }
    // Round-robin by depth.
    let mut claim_by_pot: Vec<Option<VerifiedClaim>> = vec![None; pots.len()];
    'passes: for depth in 0..LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT {
        for (p, PotSlots { slots, .. }) in pots.iter().enumerate() {
            if out.attempts >= LIVE_VIEW_VERIFY_BUDGET {
                break 'passes; // budget spent — honestly uncorroborated
            }
            if claim_by_pot[p].is_some() {
                continue;
            }
            let Some(m) = slots.get(depth) else {
                continue;
            };
            out.attempts += 1;
            // Dual-arm (gate F2, brain-cutover M1): the admission latch
            // covers exactly these two checks for a v2 row — consult it
            // first (same contract as `attribute_seats`), compute on `None`.
            // Keeps this surface's verdict consistent with `/results`'
            // attribution for the same row.
            let verified = match m.sig_valid {
                Some(v) => v,
                None => {
                    crate::results::verify_seat_marker(m)
                        && crate::results::verify_identity_binding(m)
                }
            };
            if verified {
                claim_by_pot[p] = Some(VerifiedClaim {
                    game_id: m.game_id.to_ascii_lowercase(),
                    opponent_identity: m.opponent_identity.to_ascii_lowercase(),
                    seat_settle_pubkey: m.seat_settle_pubkey.to_ascii_lowercase(),
                });
            }
        }
    }
    for (p, PotSlots { rows: idxs, .. }) in pots.iter().enumerate() {
        for i in idxs {
            out.claims[*i] = claim_by_pot[p].clone();
        }
    }
    out
}

/// The tower case status enum — EXACTLY the five strings
/// `case.rs::CaseRecord.status` can hold (`ST_PENDING` /
/// `ST_ADJUDICATED` / `ST_CONTINUE` / `ST_CONCEDE` / `ST_REFUSE`). An
/// unrecognized string rejects the WHOLE case (fail-closed — a status this
/// view cannot vouch for must not ride out under a success tag); a future
/// tower status therefore degrades to `case: null` here until this enum
/// learns it, which is the honest direction.
///
/// CROSS-REPO CONTRACT (bsv-low `workers/low-watchtower/src/case.rs`, the
/// `ST_*` constants — a matching note lives there): these five wire strings
/// are FROZEN between the tower and this view. Renaming one tower-side does
/// not break anything loudly — it silently degrades every case of that
/// status to `case: null` here (fail-closed, but a product regression).
/// Change them only in lockstep, tower first, this enum in the same
/// campaign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseStatus {
    Pending,
    ResolvedAdjudicated,
    ResolvedContinue,
    FinalizedConcede,
    FinalizedRefuse,
}

impl CaseStatus {
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(CaseStatus::Pending),
            "resolved_adjudicated" => Some(CaseStatus::ResolvedAdjudicated),
            "resolved_continue" => Some(CaseStatus::ResolvedContinue),
            "finalized_concede" => Some(CaseStatus::FinalizedConcede),
            "finalized_refuse" => Some(CaseStatus::FinalizedRefuse),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            CaseStatus::Pending => "pending",
            CaseStatus::ResolvedAdjudicated => "resolved_adjudicated",
            CaseStatus::ResolvedContinue => "resolved_continue",
            CaseStatus::FinalizedConcede => "finalized_concede",
            CaseStatus::FinalizedRefuse => "finalized_refuse",
        }
    }
}

/// Case provenance — the four-valued honesty tag next to `case` (module
/// docs table). `case` is `Some` iff the tag is
/// [`CaseProvenance::TowerByGameIdUnverified`]; every other value keeps
/// `case: null`, and `case: null` always means UNKNOWN — never "no case".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseProvenance {
    /// The fetch for this pot succeeded.
    ///
    /// SINCE 2026-08-12 THE FETCH IS OUTPOINT-SCOPED: we ask
    /// `/case/:gameId/:txid/:vout` with the pot outpoint this row is
    /// displaying (server-held, from `pot_records` — never a caller claim), so
    /// the tower's answer IS bound to this pot by construction. The previous
    /// by-name fetch had two problems: it could not vouch for the case↔pot
    /// binding, and — since the co-signer DOs became outpoint-scoped — the
    /// tower's game-level view 404s for every case actually opened, so this
    /// tag was effectively unreachable and every row degraded to
    /// `TowerUnavailable`.
    ///
    /// THE NAME AND WIRE VALUE (`"tower-by-gameid-unverified"`) ARE RETAINED
    /// DELIBERATELY and now UNDER-CLAIM. They are a published contract that
    /// `chainReads.ts` whitelists; emitting a new value before the client
    /// knows it would make the client withdraw the case (its documented
    /// degrade-on-unknown-tag rule), i.e. the server improvement would delete
    /// the surface it just fixed. Renaming to an outpoint-bound tag is a
    /// COORDINATED client-then-server change — exactly the "FUTURE caseSource
    /// value" that client's own docs anticipate.
    TowerByGameIdUnverified,
    /// This pot's gameId was an EFFECTIVE fan-out target but no valid answer
    /// arrived — binding absent, timeout, non-200 (including the tower's
    /// genuine 404), oversized/malformed body, unrecognized status. We asked
    /// (or should have) and failed; the case is unknown.
    TowerUnavailable,
    /// Not asked: the pot was eligible but not selected (fan-out cap), or
    /// its corroborated gameId is not 64-hex (unfetchable — `/case` requires
    /// a txid shape). The case is unknown.
    NotFetched,
    /// Not asked: the pot has NO verifying candidate marker among the slots
    /// this request examined — because none verified, or because the verify
    /// BUDGET was spent before this pot's depth (R2-4: a page with more than
    /// `LIVE_VIEW_VERIFY_BUDGET` pots leaves the tail here even with perfect
    /// markers). Its gameId is an unverified — potentially attacker-chosen —
    /// join key this view refuses to fan out on. The case is unknown.
    MarkerUnverified,
    /// Not asked: the CANDIDATE QUERY faulted, so corroboration could not be
    /// attempted at all — "we could not LOOK", never a claim about the data
    /// (R2-2; the distinction `MarkerUnverified` would have conflated). The
    /// case is unknown.
    CorroborationUnavailable,
}

impl CaseProvenance {
    pub fn as_str(self) -> &'static str {
        match self {
            CaseProvenance::TowerByGameIdUnverified => "tower-by-gameid-unverified",
            CaseProvenance::TowerUnavailable => "tower-unavailable",
            CaseProvenance::NotFetched => "not-fetched",
            CaseProvenance::MarkerUnverified => "marker-unverified",
            CaseProvenance::CorroborationUnavailable => "corroboration-unavailable",
        }
    }
}

/// The `markerSource` wire value for a row: the corroboration bit, with the
/// "we could not look" case kept DISTINCT from "nothing verified" (R2-2).
pub const MARKER_SOURCE_SEAT_SIGNED: &str = "seat-signed";
pub const MARKER_SOURCE_UNVERIFIED: &str = "marker-unverified";
pub const MARKER_SOURCE_UNAVAILABLE: &str = "corroboration-unavailable";

/// The shaped, VALIDATED subset of the tower's `/case/:gameId` view served
/// on a live row. Only fields the tower actually serves (`case.rs::view`:
/// `status`, `epoch`, `deadline`, `accused` — the rest of its body is not
/// this surface's to relay); every field bounded/mapped, never verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseView {
    /// Enum-mapped — load-bearing (an unmappable status rejects the case).
    pub status: CaseStatus,
    /// `epoch` — a non-negative integer ≤ 2^53−1, else `null`.
    pub epoch: Option<u64>,
    /// The tower's `deadline` field (epoch ms). Positive integer ≤ 2^53−1,
    /// else `null` — the tower stores `0.0` on resolved records as a
    /// no-deadline sentinel, which maps to `null` here, never a 1970 date.
    pub deadline_ms: Option<u64>,
    /// Seat label `"A"` / `"B"` verbatim from that two-value alphabet;
    /// anything else is `null` (never an unvalidated string).
    pub accused: Option<&'static str>,
}

/// Range-check a tower-served number into a non-negative integer ≤ 2^53−1.
/// `min_exclusive_zero` additionally maps `0` to `None` (the tower's
/// resolved-record `deadline: 0.0` sentinel).
fn checked_u64(v: Option<f64>, min_exclusive_zero: bool) -> Option<u64> {
    let x = v?;
    if !x.is_finite() || !(0.0..=CASE_MAX_SAFE_INTEGER).contains(&x) || x.fract() != 0.0 {
        return None;
    }
    if min_exclusive_zero && x == 0.0 {
        return None;
    }
    Some(x as u64)
}

/// Shape a parsed tower `/case` 200 body into a [`CaseView`]. `None` when
/// the body is not an object, has no string `status`, or the status is
/// outside the known enum (fail-closed). Field-level junk (out-of-range
/// epoch/deadline, non-A/B accused) degrades that FIELD to `null`, not the
/// case — those fields are advisory, the status is load-bearing.
pub fn shape_case(v: &serde_json::Value) -> Option<CaseView> {
    let status_str = v.get("status")?.as_str()?;
    // Bound before comparing — a hostile megabyte "status" string never even
    // reaches the enum match. (The longest real status is 20 chars.)
    if status_str.len() > 40 {
        return None;
    }
    let status = CaseStatus::from_wire(status_str)?;
    let accused = match v.get("accused").and_then(|a| a.as_str()) {
        Some("A") => Some("A"),
        Some("B") => Some("B"),
        _ => None,
    };
    Some(CaseView {
        status,
        epoch: checked_u64(v.get("epoch").and_then(|e| e.as_f64()), false),
        deadline_ms: checked_u64(v.get("deadline").and_then(|d| d.as_f64()), true),
        accused,
    })
}

/// Turn a raw tower `/case/:gameId` response into `Option<CaseView>`. `None`
/// — meaning `case: null` under a non-success [`CaseProvenance`] — for
/// ANYTHING but an HTTP 200 with a bounded, well-formed, known-status JSON
/// body. This includes the tower's own 404 ("no case for this game"): a
/// non-200 is a NON-ANSWER here, so this surface never asserts case-absence
/// (module docs).
pub fn parse_case_body(status_code: u16, body: &str) -> Option<CaseView> {
    if status_code != 200 || body.len() > CASE_BODY_MAX_BYTES {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    shape_case(&v)
}

/// MEDIUM-3 pre-buffer gate: does a `Content-Length` header already prove
/// the body exceeds [`CASE_BODY_MAX_BYTES`]? Only a PARSEABLE over-budget
/// value answers `true` (reject before reading a single body byte). Absent,
/// unparseable, or in-budget headers answer `false` — the header is a cheap
/// early exit, never the enforcement: the streamed read's hard budget
/// ([`BodyAccumulator`]) is the bound a lying/chunked response cannot dodge.
pub fn content_length_over_budget(header: Option<&str>) -> bool {
    header
        .and_then(|h| h.trim().parse::<u64>().ok())
        .is_some_and(|n| n > CASE_BODY_MAX_BYTES as u64)
}

/// Append `chunk` to `buf` only if the total stays ≤ `max`. `false` (buf
/// untouched) means the response blew the budget and the read must abort. A
/// body of exactly `max` bytes still accumulates (the parse-side
/// exact-ceiling edge is preserved).
pub fn push_bounded(buf: &mut Vec<u8>, chunk: &[u8], max: usize) -> bool {
    if buf.len().saturating_add(chunk.len()) > max {
        return false;
    }
    buf.extend_from_slice(chunk);
    true
}

/// The `/case` body READER as an injectable seam: bytes in →
/// `Option<CaseView>` out. `routes::tower_case_fetch` owns only the stream
/// plumbing (and the `worker::Delay` race); every decision — the hard byte
/// budget, the abort, the UTF-8 decode, the status/shape gate — lives here.
///
/// UTF-8 is decoded ONCE at the end, never per chunk, so a multi-byte
/// character split across two chunks is not a fault (a per-chunk decode
/// would have turned that into a silent `case: null`).
#[derive(Debug, Clone)]
pub struct BodyAccumulator {
    buf: Vec<u8>,
    max: usize,
    aborted: bool,
}

impl BodyAccumulator {
    pub fn new(max: usize) -> Self {
        Self {
            buf: Vec::new(),
            max,
            aborted: false,
        }
    }

    /// Accumulate one chunk. `false` ⇒ the budget is blown: the caller must
    /// stop reading (drop the stream) and treat the case as unknown.
    pub fn push(&mut self, chunk: &[u8]) -> bool {
        if self.aborted {
            return false;
        }
        if !push_bounded(&mut self.buf, chunk, self.max) {
            self.aborted = true;
            self.buf = Vec::new(); // never keep holding an over-budget body
            return false;
        }
        true
    }

    pub fn aborted(&self) -> bool {
        self.aborted
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Finish: `None` on an abort, on non-UTF-8 bytes, or on anything
    /// [`parse_case_body`] refuses (non-200, oversized, malformed, unknown
    /// status).
    pub fn finish(self, status_code: u16) -> Option<CaseView> {
        if self.aborted {
            return None;
        }
        let body = String::from_utf8(self.buf).ok()?;
        parse_case_body(status_code, &body)
    }
}

/// Drive a whole chunk sequence through a [`BodyAccumulator`] — the exact
/// loop `routes::tower_case_fetch` runs, minus the stream. `Err(())` models
/// a mid-stream transport fault (⇒ `None`, never a partial parse).
pub fn read_case_body<I>(status_code: u16, chunks: I) -> Option<CaseView>
where
    I: IntoIterator<Item = std::result::Result<Vec<u8>, ()>>,
{
    let mut acc = BodyAccumulator::new(CASE_BODY_MAX_BYTES);
    for chunk in chunks {
        match chunk {
            Ok(bytes) => {
                if !acc.push(&bytes) {
                    return None;
                }
            }
            Err(()) => return None,
        }
    }
    acc.finish(status_code)
}

/// The fan-out targets: at most [`LIVE_VIEW_CASE_FANOUT_CAP`] DISTINCT,
/// well-formed (64-hex, lowercased) CORROBORATED gameIds chosen by QUALITY,
/// not window position (module docs — position-based selection let
/// quota-many dust markers starve every slot):
///
/// 1. only pots with a [`VerifiedClaim`] are eligible — an uncorroborated
///    gameId is never used as a join key;
/// 2. KNOWN pots first, then unknown, window order within each class
///    ([`quality_order`]);
/// 3. duplicates are fetched once and shared; a non-64-hex gameId is
///    skipped (it can never match a tower case — `/case` requires a txid
///    shape).
///
/// `claims` is index-aligned with `rows`; a missing entry counts as
/// UNCORROBORATED (fail-safe).
pub fn fanout_targets(rows: &[LiveViewRow], claims: &[Option<VerifiedClaim>]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for i in quality_order(rows) {
        if out.len() >= LIVE_VIEW_CASE_FANOUT_CAP {
            break;
        }
        let Some(Some(claim)) = claims.get(i) else {
            continue;
        };
        let g = claim.game_id.to_ascii_lowercase();
        if crate::logic::valid_txid(&g) && !out.contains(&g) {
            out.push(g);
        }
    }
    out
}

/// Run the fan-out through an INJECTABLE fetcher seam: one `fetch_one`
/// future per target (at most [`LIVE_VIEW_CASE_FANOUT_CAP`] — a defensive
/// re-cap; [`fanout_targets`] already bounds it), all awaited CONCURRENTLY,
/// `None` results (any transport/shape fault) simply absent from the map.
///
/// Returns the EFFECTIVE (capped) target list alongside the map, so
/// [`apply_cases`] can only ever label targets that were really asked — a
/// dropped target must never be reported as "asked and failed" (LOW-F).
pub async fn run_fanout<F, Fut>(
    targets: &[String],
    fetch_one: F,
) -> (Vec<String>, std::collections::HashMap<String, CaseView>)
where
    F: Fn(String) -> Fut,
    Fut: core::future::Future<Output = Option<CaseView>>,
{
    let capped: Vec<String> = targets
        .iter()
        .take(LIVE_VIEW_CASE_FANOUT_CAP)
        .cloned()
        .collect();
    let futs: Vec<Fut> = capped.iter().map(|g| fetch_one(g.clone())).collect();
    let results = futures_util::future::join_all(futs).await;
    let fetched = capped
        .iter()
        .cloned()
        .zip(results)
        .filter_map(|(g, r)| r.map(|cv| (g, cv)))
        .collect();
    (capped, fetched)
}

/// Fold fetched cases back onto the entries and stamp EVERY row's
/// [`CaseProvenance`] (LOW-9: the tag is never left ambiguous):
///
/// - uncorroborated pot ⇒ `MarkerUnverified` (never fetched, whatever the
///   maps say — an uncorroborated row must not receive a case even if its
///   gameId string collides with a fetched one);
/// - corroborated + gameId fetched ⇒ the case, `TowerByGameIdUnverified`,
///   and `caseGameId` = the join key actually fetched (HIGH-1a). Rows
///   SHARING a fetched gameId share the answer — honestly labeled, since
///   the tag vouches only "the tower's answer for this gameId";
/// - corroborated + gameId was an EFFECTIVE target but nothing valid
///   arrived ⇒ `TowerUnavailable`;
/// - corroborated + never asked (cap overflow / malformed gameId) ⇒
///   `NotFetched`.
///
/// `targets` must be the EFFECTIVE list ([`run_fanout`]'s first return
/// value, or the selected list when the binding itself was absent — "we
/// should have asked and could not" is the same honest unknown). Pure — the
/// whole attribution decision is testable without a Worker. Call it
/// UNCONDITIONALLY (even with empty targets/fetched) so the tags are always
/// stamped.
pub fn apply_cases(
    entries: &mut [LiveEntry],
    targets: &[String],
    fetched: &std::collections::HashMap<String, CaseView>,
) {
    for e in entries.iter_mut() {
        if !e.marker_verified {
            // Keep the tag `assemble_live_view` stamped — `MarkerUnverified`
            // or `CorroborationUnavailable` — never flatten the two (R2-2).
            e.case = None;
            e.case_game_id = None;
            continue;
        }
        let g = e.game_id.to_ascii_lowercase();
        if let Some(cv) = fetched.get(&g) {
            e.case = Some(cv.clone());
            e.case_game_id = Some(g);
            e.case_source = CaseProvenance::TowerByGameIdUnverified;
        } else if targets.contains(&g) {
            e.case_source = CaseProvenance::TowerUnavailable;
        } else {
            e.case_source = CaseProvenance::NotFetched;
        }
    }
}

/// One `/live-view` response entry, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveEntry {
    /// The pot's CORROBORATED gameId when one exists, else the
    /// representative marker's unverified value — see `marker_source`.
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    /// Opponent identity: the corroborated claim when the pot corroborated,
    /// else the representative's value when identity-shaped, else `None`.
    pub opponent_identity: Option<String>,
    /// `"seat-signed"` / `"marker-unverified"` for the SERVED opponent
    /// value, `None` when no opponent is served.
    pub opponent_identity_source: Option<&'static str>,
    /// The row-level corroboration bit as a wire tag: `"seat-signed"` when a
    /// v2 marker of the caller's own VERIFIED for this pot (then `gameId` AND
    /// `opponentIdentity` are the caller's own signed claims),
    /// `"marker-unverified"` when nothing verified or the verify budget was
    /// spent before this pot (never "forged"), or
    /// `"corroboration-unavailable"` when the candidate query faulted (we
    /// could not look).
    pub marker_source: &'static str,
    /// Whether this pot corroborated — drives fan-out eligibility and both
    /// source tags.
    pub marker_verified: bool,
    /// The served recovery height (see
    /// [`crate::refund_view::served_recovery_height`]).
    pub recovery_height: Option<u64>,
    /// `max(0, recoveryHeight - tip)` when both known, else `None`.
    pub blocks_to_gate: Option<u64>,
    /// `tip >= recoveryHeight` when both known, else `false` (fail-safe).
    pub gate_passed: bool,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// The shaped tower case — `None` = UNKNOWN (see `case_source`), NEVER
    /// "no case exists".
    pub case: Option<CaseView>,
    /// The 64-hex gameId the case was actually fetched under — `Some` iff
    /// `case` is `Some` (HIGH-1a: the client's substitution/sharing check;
    /// it cannot expose a REUSED gameId — module docs).
    pub case_game_id: Option<String>,
    /// The four-valued provenance tag (module docs table). Set together
    /// with `case` in [`apply_cases`].
    pub case_source: CaseProvenance,
}

/// Assemble the joined rows + per-row corroboration + chain tip into
/// response entries. `case` starts `null` with the honest pre-fan-out tag
/// (`MarkerUnverified`, or `CorroborationUnavailable` when
/// [`Corroborated::unavailable`] says the candidate query faulted);
/// [`apply_cases`] finalizes it. Order is preserved (the SQL already returns
/// tiered-newest-first). `corr.claims` is index-aligned; a missing entry
/// counts as uncorroborated (fail-safe). Pure — every branch is
/// unit-testable without D1.
pub fn assemble_live_view(
    rows: Vec<LiveViewRow>,
    corr: &Corroborated,
    tip: Option<u64>,
) -> Vec<LiveEntry> {
    let claims = &corr.claims;
    rows.into_iter()
        .enumerate()
        .map(|(i, r)| {
            let claim = claims.get(i).and_then(|c| c.as_ref());
            let marker_verified = claim.is_some();
            let recovery_height =
                served_recovery_height(r.cov_recovery_height, r.marker_recovery_height);
            let (blocks_to_gate, gate_passed) = match (recovery_height, tip) {
                (Some(rh), Some(t)) => (Some(rh.saturating_sub(t)), t >= rh),
                _ => (None, false),
            };
            // gameId: the CORROBORATED value when the pot corroborated (the
            // caller's own signed claim), else the representative marker's
            // byte-format-admitted value — labeled either way.
            let game_id = match claim {
                Some(c) => c.game_id.clone(),
                None => r.game_id,
            };
            // The opponent claim is only ever served when identity-SHAPED,
            // and ALWAYS with its provenance tag (a forgeable claim never
            // rides out looking like a verified fact).
            let opponent_identity = match claim {
                Some(c) => Some(c.opponent_identity.to_ascii_lowercase()),
                None => r.opponent_identity.map(|o| o.to_ascii_lowercase()),
            }
            .filter(|o| crate::logic::valid_identity(o));
            let marker_source = match (marker_verified, corr.unavailable) {
                (true, _) => MARKER_SOURCE_SEAT_SIGNED,
                // "we could not LOOK" is NOT "nothing to find" (R2-2).
                (false, true) => MARKER_SOURCE_UNAVAILABLE,
                (false, false) => MARKER_SOURCE_UNVERIFIED,
            };
            LiveEntry {
                game_id,
                pot_txid: r.pot_txid,
                pot_vout: r.pot_vout,
                opponent_identity_source: opponent_identity.as_ref().map(|_| marker_source),
                opponent_identity,
                marker_source,
                marker_verified,
                recovery_height,
                blocks_to_gate,
                gate_passed,
                spent: r.spent,
                spending_txid: r.spending_txid,
                spent_confirmed: r.spent_confirmed,
                case: None,
                case_game_id: None,
                case_source: match (marker_verified, corr.unavailable) {
                    (true, _) => CaseProvenance::NotFetched,
                    (false, true) => CaseProvenance::CorroborationUnavailable,
                    (false, false) => CaseProvenance::MarkerUnverified,
                },
            }
        })
        .collect()
}

/// Assemble the `/live-view` wire body:
/// `{"identity","tip":<height|null>,"live":[{gameId,potTxid,potVout,
/// opponentIdentity,opponentIdentitySource,markerSource,recoveryHeight,
/// blocksToGate,gatePassed,spent,spendingTxid,spentConfirmed,case,
/// caseGameId,caseSource}]}` with
/// `case = {status,epoch,deadlineMs,accused} | null`, `caseGameId` the
/// fetched join key (`null` unless `case` is served) and `caseSource`
/// ALWAYS one of the four [`CaseProvenance`] strings. `tip` mirrors
/// `/refund-view` (`null` on a chaintracks fault — the D1 facts still
/// serve; the gate fields then degrade to `null`/`false`).
pub fn live_view_body(identity: &str, tip: Option<u64>, entries: &[LiveEntry]) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            let case = e.case.as_ref().map(|c| {
                json!({
                    "status": c.status.as_str(),
                    "epoch": c.epoch,
                    "deadlineMs": c.deadline_ms,
                    "accused": c.accused,
                })
            });
            json!({
                "gameId": e.game_id,
                "potTxid": e.pot_txid,
                "potVout": e.pot_vout,
                "opponentIdentity": e.opponent_identity,
                "opponentIdentitySource": e.opponent_identity_source,
                "markerSource": e.marker_source,
                "recoveryHeight": e.recovery_height,
                "blocksToGate": e.blocks_to_gate,
                "gatePassed": e.gate_passed,
                "spent": e.spent,
                "spendingTxid": e.spending_txid,
                "spentConfirmed": e.spent_confirmed,
                "case": case,
                "caseGameId": e.case_game_id,
                "caseSource": e.case_source.as_str(),
            })
        })
        .collect();
    json!({ "identity": identity, "tip": tip, "live": arr }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, ProtoWallet};

    fn h64(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }

    fn h66(seed: u8) -> String {
        format!("02{}", format!("{seed:02x}").repeat(32))
    }

    fn base_row() -> LiveViewRow {
        LiveViewRow {
            identity: h66(0xa1),
            game_id: h64(0x11),
            pot_txid: h64(0xaa),
            pot_vout: 0,
            opponent_identity: Some(h66(0xbb)),
            marker_recovery_height: 900_123,
            cov_recovery_height: None,
            identity_sig_hex: None,
            seat_settle_pubkey: None,
            seat_sig_hex: None,
            cov_pub_a: None,
            cov_pub_b: None,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: Some(false),
        }
    }

    /// A synthetic corroborated claim for a row — the crypto path is proven
    /// separately by the real-key round-trips below, so selection and
    /// attribution are tested as the pure functions they are.
    fn claim_for(r: &LiveViewRow) -> Option<VerifiedClaim> {
        Some(VerifiedClaim {
            game_id: r.game_id.to_ascii_lowercase(),
            opponent_identity: r.opponent_identity.clone().unwrap_or_else(|| h66(0xbb)),
            seat_settle_pubkey: h66(0x51),
        })
    }

    /// Wrap claims as a [`Corroborated`] (no fault) for the pure assemblers.
    fn corr_of(claims: Vec<Option<VerifiedClaim>>) -> Corroborated {
        Corroborated {
            claims,
            attempts: 0,
            unavailable: false,
        }
    }

    /// An entry from a base row with the corroboration state injected.
    fn entry(game_seed: u8, verified: bool) -> LiveEntry {
        let mut r = base_row();
        r.game_id = h64(game_seed);
        let claim = if verified { claim_for(&r) } else { None };
        assemble_live_view(vec![r], &corr_of(vec![claim]), None)
            .pop()
            .unwrap()
    }

    /// The exact shape `case.rs::view` serves for a pending case (fields
    /// this view consumes, values realistic).
    fn tower_case_body() -> serde_json::Value {
        serde_json::json!({
            "status": "pending",
            "caseId": "aa:1",
            "epoch": 1,
            "openAt": { "ms": 1_700_000_000_000.0f64, "height": 900_100 },
            "windowMs": 600_000,
            "deadline": 1_700_000_600_000.0f64,
            "accused": "B",
            "claimant": "A",
            "expectedKind": "reveal_hand",
            "canOpen": true,
            "forfeitPostPeek": false,
            "reServe": ["aabb"],
            "mode": null,
            "J": null,
        })
    }

    // ── real-crypto corroboration (REAL secp256k1 + REAL BRC-42 identity
    //    signatures, never a mocked verify) ────────────────────────────────

    fn wallet_of(seed: u8) -> ProtoWallet {
        let key = bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{seed:064x}")).unwrap();
        ProtoWallet::new(Some(key))
    }

    fn identity_of(w: &ProtoWallet) -> String {
        w.identity_key_hex().to_ascii_lowercase()
    }

    fn settle_key(seed: u8) -> (bsv_rs::primitives::ec::PrivateKey, String) {
        let k = bsv_rs::primitives::ec::PrivateKey::from_bytes(&{
            let mut b = [0u8; 32];
            b[31] = seed;
            b
        })
        .unwrap();
        let pk = k.public_key().to_hex().to_ascii_lowercase();
        (k, pk)
    }

    /// A FULLY REAL v2 seat-binding marker: genuine settle-key ECDSA over
    /// the exact cross-repo preimage + genuine BRC-42 'anyone' identity
    /// signature over the exact v2 challenge.
    fn real_marker(
        w: &ProtoWallet,
        settle_seed: u8,
        game_id: &str,
        pot_txid: &str,
        opponent: &str,
    ) -> SeatMarkerRow {
        let identity = identity_of(w);
        let (sk, settle_pub) = settle_key(settle_seed);
        let preimage = crate::results::seatsig_preimage(game_id, pot_txid, 0, &identity).unwrap();
        let seat_sig = sk
            .sign(&bsv_rs::primitives::hash::sha256(&preimage))
            .unwrap();
        let mut m = SeatMarkerRow {
            identity,
            opponent_identity: opponent.to_string(),
            game_id: game_id.to_string(),
            pot_txid: pot_txid.to_string(),
            pot_vout: 0,
            recovery_height: 900_123,
            seat_settle_pubkey: settle_pub,
            seat_sig_hex: hex::encode(seat_sig.to_der()),
            identity_sig_hex: String::new(),
        sig_valid: None, // fixture: the compute arm
        };
        let challenge = crate::results::potparty_v2_challenge(&m).unwrap();
        let sig = w
            .create_signature(CreateSignatureArgs {
                data: Some(challenge),
                hash_to_directly_sign: None,
                protocol_id: bsv_rs::wallet::Protocol::new(
                    bsv_rs::wallet::SecurityLevel::App,
                    "low potparty",
                ),
                key_id: game_id.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();
        m.identity_sig_hex = hex::encode(sig.signature);
        m
    }

    /// The attacker shape (the #230 F1 shape, reviewer probe C): the
    /// attacker's OWN settle key signs a seat preimage embedding the
    /// VICTIM's identity, so `verify_seat_marker` PASSES and the expensive
    /// identity check runs before failing.
    fn hostile_marker(victim_lc: &str, game_id: &str, pot_txid: &str) -> SeatMarkerRow {
        let (sk, pk) = settle_key(0x99);
        let preimage = crate::results::seatsig_preimage(game_id, pot_txid, 0, victim_lc).unwrap();
        let sig = sk
            .sign(&bsv_rs::primitives::hash::sha256(&preimage))
            .unwrap();
        SeatMarkerRow {
            identity: victim_lc.to_string(),
            opponent_identity: h66(0xbb),
            game_id: game_id.to_string(),
            pot_txid: pot_txid.to_string(),
            pot_vout: 0,
            recovery_height: 900_123,
            seat_settle_pubkey: pk,
            seat_sig_hex: hex::encode(sig.to_der()),
            identity_sig_hex: hex::encode(sig.to_der()), // well-formed DER, wrong signer
            sig_valid: None,                             // fixture: the compute arm
        }
    }

    fn candidates(
        list: Vec<SeatMarkerRow>,
    ) -> std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>> {
        let mut out: std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>> =
            std::collections::HashMap::new();
        for m in list {
            out.entry((m.pot_txid.to_ascii_lowercase(), m.pot_vout))
                .or_default()
                .push(m);
        }
        out
    }

    /// HIGH-A in pure form: the row is the V1 representative (no v2
    /// columns), the genuine v2 marker arrives as a CANDIDATE — and the pot
    /// corroborates, supplying the gameId. Before the fix this shape (the
    /// only one production produces) was structurally unverifiable.
    #[test]
    fn the_pots_v2_candidate_corroborates_a_v1_representative_row() {
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let game = h64(0x21);
        let pot = h64(0xaa);
        let mut row = base_row();
        row.identity = me.clone();
        row.game_id = h64(0x77); // the v1 representative's (unverified) gameId
        row.pot_txid = pot.clone();
        let m = real_marker(&w, 0x51, &game, &pot, &h66(0xbb));
        let c = corroborate_rows(&me, &[row.clone()], &candidates(vec![m]));
        let claim = c.claims[0]
            .as_ref()
            .expect("the pot's v2 candidate corroborates");
        assert_eq!(
            claim.game_id, game,
            "the CORROBORATED gameId, not the v1 row's"
        );
        assert_eq!(claim.opponent_identity, h66(0xbb));
        assert_eq!(c.attempts, 1, "one candidate, one attempt");
        // The served entry carries the corroborated values + the tag.
        let e = &assemble_live_view(vec![row.clone()], &c, None)[0];
        assert_eq!(e.game_id, game);
        assert_eq!(e.marker_source, "seat-signed");

        // With NO candidate (v1 only — the pre-fix reality) the pot stays
        // honestly uncorroborated and the row's own value is served.
        let c = corroborate_rows(&me, &[row.clone()], &Default::default());
        assert!(c.claims[0].is_none());
        assert_eq!(c.attempts, 0, "a v1-only pot spends no curve time");
        let e = &assemble_live_view(vec![row], &c, None)[0];
        assert_eq!(
            e.game_id,
            h64(0x77),
            "the unverified representative value, labeled"
        );
        assert_eq!(e.marker_source, "marker-unverified");
    }

    #[test]
    fn corroboration_refuses_every_tampered_or_foreign_candidate() {
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let game = h64(0x21);
        let pot = h64(0xaa);
        let mut row = base_row();
        row.identity = me.clone();
        row.pot_txid = pot.clone();
        let good = real_marker(&w, 0x51, &game, &pot, &h66(0xbb));
        assert!(
            corroborate_rows(&me, &[row.clone()], &candidates(vec![good.clone()])).claims[0]
                .is_some()
        );

        // gameId steered (the HIGH-1 attack): both signatures bind it.
        let mut m = good.clone();
        m.game_id = h64(0x66);
        assert!(corroborate_rows(&me, &[row.clone()], &candidates(vec![m])).claims[0].is_none());
        // opponent swapped: the v2 challenge binds it.
        let mut m = good.clone();
        m.opponent_identity = h66(0xcc);
        assert!(corroborate_rows(&me, &[row.clone()], &candidates(vec![m])).claims[0].is_none());
        // recoveryHeight tampered: the v2 challenge binds that too.
        let mut m = good.clone();
        m.recovery_height = 1;
        assert!(corroborate_rows(&me, &[row.clone()], &candidates(vec![m])).claims[0].is_none());
        // A candidate for ANOTHER outpoint never corroborates this pot — and
        // costs no curve work (rejected before verification).
        let mut m = good.clone();
        m.pot_txid = h64(0xab);
        let mut hand_fed = std::collections::HashMap::new();
        hand_fed.insert((pot.clone(), 0u32), vec![m]);
        let c = corroborate_rows(&me, &[row.clone()], &hand_fed);
        assert!(c.claims[0].is_none());
        assert_eq!(c.attempts, 0, "outpoint mismatch is a free reject");
        // The COUNTERPARTY's own genuine marker is not relayed as ours (free
        // reject: identity mismatch).
        let w2 = wallet_of(0x43);
        let other = real_marker(&w2, 0x52, &game, &pot, &me);
        let c = corroborate_rows(&me, &[row.clone()], &candidates(vec![other]));
        assert!(c.claims[0].is_none());
        assert_eq!(c.attempts, 0, "identity mismatch is a free reject");
        // Bit-flipped seat signature refuses (and DOES cost an attempt).
        let mut m = good.clone();
        let mut sig = hex::decode(&m.seat_sig_hex).unwrap();
        let last = sig.len() - 1;
        sig[last] ^= 0x01;
        m.seat_sig_hex = hex::encode(sig);
        let c = corroborate_rows(&me, &[row.clone()], &candidates(vec![m]));
        assert!(c.claims[0].is_none());
        assert_eq!(c.attempts, 1);
        // Junk hex never panics.
        let mut m = good;
        m.seat_settle_pubkey = "zz".into();
        assert!(corroborate_rows(&me, &[row], &candidates(vec![m])).claims[0].is_none());
    }

    #[test]
    fn the_first_verifying_candidate_wins_past_earlier_junk() {
        // The candidate window can carry junk stamped BEFORE the honest
        // marker (byte-format admission): the honest one still wins, as long
        // as it is inside the per-pot attempt cap.
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let game = h64(0x21);
        let pot = h64(0xaa);
        let mut row = base_row();
        row.identity = me.clone();
        row.pot_txid = pot.clone();
        let mut pool: Vec<SeatMarkerRow> = Vec::new();
        for i in 0..(LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT - 1) {
            let mut junk = hostile_marker(&me, &game, &pot);
            junk.recovery_height = 100 + i as u32; // distinct rows, all junk
            pool.push(junk);
        }
        pool.push(real_marker(&w, 0x51, &game, &pot, &h66(0xbb)));
        let c = corroborate_rows(&me, &[row], &candidates(pool));
        assert_eq!(c.claims[0].as_ref().map(|x| x.game_id.clone()), Some(game));
        assert_eq!(c.attempts, LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT);
    }

    #[test]
    fn lock_membership_is_a_free_prefilter_where_the_columns_exist() {
        // MEDIUM-B: on a KNOWN covenant pot a candidate under a key the lock
        // never committed is rejected with ZERO curve work — the
        // `attribute_seats` pre-filter, restored.
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let pot = h64(0xaa);
        let (_, honest_pk) = settle_key(0x51);
        let mut row = base_row();
        row.identity = me.clone();
        row.pot_txid = pot.clone();
        row.cov_pub_a = Some(honest_pk.clone());
        row.cov_pub_b = Some(h66(0x0b));

        let hostile = hostile_marker(&me, &h64(0x21), &pot);
        let c = corroborate_rows(&me, &[row.clone()], &candidates(vec![hostile.clone()]));
        assert!(c.claims[0].is_none());
        assert_eq!(c.attempts, 0, "foreign key ⇒ free reject, no ECDSA");

        // The same hostile marker on a pot with NO decoded keys (join miss)
        // keeps the pre-existing semantics: it IS verified (and refused).
        let mut keyless = row.clone();
        keyless.cov_pub_a = None;
        keyless.cov_pub_b = None;
        keyless.spent = None;
        keyless.spent_confirmed = None;
        let c = corroborate_rows(&me, &[keyless], &candidates(vec![hostile]));
        assert!(c.claims[0].is_none());
        assert_eq!(
            c.attempts, 1,
            "no committed keys ⇒ the signature bars decide"
        );

        // The honest marker under a COMMITTED key survives the pre-filter.
        let good = real_marker(&w, 0x51, &h64(0x21), &pot, &h66(0xbb));
        assert_eq!(good.seat_settle_pubkey, honest_pk);
        let c = corroborate_rows(&me, &[row], &candidates(vec![good]));
        assert!(c.claims[0].is_some());
        assert_eq!(c.attempts, 1);
    }

    #[test]
    fn the_verify_budget_bounds_a_full_hostile_page() {
        // MEDIUM-B behaviourally: a whole page of the attacker shape on
        // KEYLESS (ghost) pots — the worst case, since membership cannot
        // pre-filter them — consumes no more than the budget.
        let me = h66(0xa1);
        let mut rows: Vec<LiveViewRow> = Vec::new();
        let mut pool: Vec<SeatMarkerRow> = Vec::new();
        for i in 0..LIVE_VIEW_MAX_ROWS {
            let pot = format!("{:064x}", 0xbeef_0000_u64 + i as u64);
            let game = format!("{:064x}", 0xdead_0000_u64 + i as u64);
            let mut r = base_row();
            r.identity = me.clone();
            r.game_id = game.clone();
            r.pot_txid = pot.clone();
            r.spent = None; // ghost: join miss
            r.spent_confirmed = None;
            rows.push(r);
            // Each ghost pot carries a FULL per-pot candidate window of
            // DISTINCT hostile markers (identical clones would be free).
            for k in 0..LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT {
                let mut m = hostile_marker(&me, &game, &pot);
                m.recovery_height = 900_000 + k as u32;
                pool.push(m);
            }
        }
        let c = corroborate_rows(&me, &rows, &candidates(pool));
        assert!(
            c.claims.iter().all(|x| x.is_none()),
            "nothing hostile corroborates"
        );
        assert_eq!(
            c.attempts, LIVE_VIEW_VERIFY_BUDGET,
            "the budget is the ceiling AND it is fully spendable"
        );
        // Every row is still SERVED, honestly labeled.
        let entries = assemble_live_view(rows, &c, Some(900_000));
        assert_eq!(entries.len(), LIVE_VIEW_MAX_ROWS);
        assert!(entries
            .iter()
            .all(|e| e.marker_source == "marker-unverified"));
    }

    #[test]
    fn identical_duplicate_candidates_are_free() {
        let me = h66(0xa1);
        let mut r = base_row();
        r.identity = me.clone();
        r.spent = None;
        r.spent_confirmed = None;
        let m = hostile_marker(&me, &h64(0x21), &r.pot_txid.clone());
        let c = corroborate_rows(&me, &[r], &candidates(vec![m.clone(), m.clone(), m]));
        assert_eq!(
            c.attempts, 1,
            "the sweep's content-idempotent republish costs once"
        );
    }

    #[test]
    fn crowding_other_pots_can_never_take_a_pots_first_attempt() {
        // R2-1: round-robin by depth. A budget-full page of hostile pots —
        // HALF of them KNOWN (join hit) and crowded to the per-pot cap, the
        // shape depth-first allotment lost to; half join-miss dust — still
        // corroborates the victim's pot, which sorts LAST.
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let real_pot = h64(0xaa);
        let real_game = h64(0x21);
        let mut rows: Vec<LiveViewRow> = Vec::new();
        let mut pool: Vec<SeatMarkerRow> = Vec::new();
        for i in 0..(LIVE_VIEW_VERIFY_BUDGET - 1) {
            let pot = format!("{:064x}", 0xbeef_0000_u64 + i as u64);
            let game = format!("{:064x}", 0xdead_0000_u64 + i as u64);
            let mut r = base_row();
            r.identity = me.clone();
            r.game_id = game.clone();
            r.pot_txid = pot.clone();
            if i % 2 == 0 {
                // ATTACKER-FUNDED KNOWN pot (spent Some ⇒ join hit), its own
                // committed key, crowded to the cap with distinct junk.
                let (_, atk_pk) = settle_key(0xa0 + (i % 32) as u8);
                r.cov_pub_a = Some(atk_pk.clone());
                r.cov_pub_b = Some(h66(0x0b));
                for k in 0..LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT {
                    let mut m = hostile_marker(&me, &game, &pot);
                    m.seat_settle_pubkey = atk_pk.clone(); // passes membership
                    m.recovery_height = 800_000 + k as u32;
                    pool.push(m);
                }
            } else {
                r.spent = None; // join-miss dust
                r.spent_confirmed = None;
                pool.push(hostile_marker(&me, &game, &pot));
            }
            rows.push(r);
        }
        // The victim's REAL known pot lands LAST in window order.
        let (_, pk) = settle_key(0x51);
        let mut real = base_row();
        real.identity = me.clone();
        real.game_id = h64(0x77); // the v1 representative value
        real.pot_txid = real_pot.clone();
        real.cov_pub_a = Some(pk);
        real.cov_pub_b = Some(h66(0x0b));
        rows.push(real);
        pool.push(real_marker(&w, 0x51, &real_game, &real_pot, &h66(0xbb)));

        let c = corroborate_rows(&me, &rows, &candidates(pool));
        let last = rows.len() - 1;
        assert_eq!(
            c.claims[last].as_ref().map(|x| x.game_id.clone()),
            Some(real_game.clone()),
            "the victim's pot gets its depth-0 attempt however many crowded pots precede it"
        );
        assert!(c.attempts <= LIVE_VIEW_VERIFY_BUDGET);
        // …and it is the fan-out target.
        assert_eq!(fanout_targets(&rows, &c.claims), vec![real_game]);
    }

    /// R3-1, the MIRROR of `crowding_other_pots_can_never_take_a_pots_first_attempt`:
    /// that cell pins the boundary from the SAFE side (`BUDGET - 1` hostile pots
    /// plus the victim = exactly BUDGET planned pots, victim corroborates).
    /// This one pins it from the UNSAFE side, so the documented limit cannot
    /// silently drift into a claim we do not honour.
    ///
    /// Shape: a NOT-YET-ADMITTED (join-miss) victim pot behind BUDGET-many FREE
    /// ghost pots — invented `potTxid`s, one v2-shaped marker each, no funding
    /// and no race against the victim's marker. Pass 0 is exhausted before the
    /// victim is reached, so corroboration is genuinely best-effort here. What
    /// this cell guarantees is the FAIL DIRECTION: the pot is still served, and
    /// it is honestly labelled rather than falsely claimed. Self-heals at
    /// `tm_pot` admission (the pot then sorts into the KNOWN class ahead of
    /// every ghost) — see the SCOPE OF THE GUARANTEE note on `corroborate_rows`.
    #[test]
    fn a_not_yet_admitted_pot_behind_a_full_pass_of_free_ghosts_is_honest_not_corroborated() {
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let real_pot = h64(0xaa);
        let real_game = h64(0x21);
        let mut rows: Vec<LiveViewRow> = Vec::new();
        let mut pool: Vec<SeatMarkerRow> = Vec::new();
        // BUDGET-many FREE ghosts: invented pot, join miss, ONE depth-0
        // candidate each — enough to consume the whole pass-0 allocation.
        for i in 0..LIVE_VIEW_VERIFY_BUDGET {
            let pot = format!("{:064x}", 0xbeef_0000_u64 + i as u64);
            let game = format!("{:064x}", 0xdead_0000_u64 + i as u64);
            let mut r = base_row();
            r.identity = me.clone();
            r.game_id = game.clone();
            r.pot_txid = pot.clone();
            r.spent = None; // never indexed ⇒ join miss ⇒ unknown class
            r.spent_confirmed = None;
            rows.push(r);
            pool.push(hostile_marker(&me, &game, &pot));
        }
        // The victim's pot is REAL and its marker is GENUINE, but the pot is
        // not yet admitted, so it shares the unknown class with the ghosts.
        let mut real = base_row();
        real.identity = me.clone();
        real.game_id = h64(0x77); // the v1 representative value
        real.pot_txid = real_pot.clone();
        real.spent = None;
        real.spent_confirmed = None;
        rows.push(real);
        pool.push(real_marker(&w, 0x51, &real_game, &real_pot, &h66(0xbb)));

        let c = corroborate_rows(&me, &rows, &candidates(pool));
        let last = rows.len() - 1;
        // The documented limit: pass 0 was spent before this pot was reached.
        assert!(
            c.claims[last].is_none(),
            "documented R3-1 limit: a not-yet-admitted pot can be crowded out of pass 0 by free ghosts"
        );
        // The CEILING still holds — the crowd cannot buy extra curve work.
        assert_eq!(c.attempts, LIVE_VIEW_VERIFY_BUDGET);
        // The FAIL DIRECTION is what matters: honestly labelled, never a claim,
        // and no case is fetched for an uncorroborated row.
        assert!(!c.unavailable, "a spent budget is NOT a lookup fault");
        let entries = assemble_live_view(rows.clone(), &c, Some(900_000));
        assert_eq!(entries.len(), rows.len(), "every pot is still served");
        assert_eq!(entries[last].marker_source, MARKER_SOURCE_UNVERIFIED);
        assert!(entries[last].case.is_none());
        assert!(
            fanout_targets(&rows, &c.claims).is_empty(),
            "an uncorroborated row is never a fan-out target"
        );
    }

    #[test]
    fn crowding_boundary_under_round_robin_is_the_per_pot_slot_cap() {
        // Deviation-4 trade, pinned: with ONE pot, junk stamped ahead of the
        // honest marker delays it by that many PASSES — it survives at
        // cap - 1 junk rows and is suppressed at cap (the slots are full).
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let game = h64(0x21);
        let pot = h64(0xaa);
        let mut row = base_row();
        row.identity = me.clone();
        row.pot_txid = pot.clone();
        for (junk_n, want) in [
            (LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT - 1, true),
            (LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT, false),
        ] {
            let mut pool: Vec<SeatMarkerRow> = Vec::new();
            for k in 0..junk_n {
                let mut m = hostile_marker(&me, &game, &pot);
                m.recovery_height = 100 + k as u32;
                pool.push(m);
            }
            pool.push(real_marker(&w, 0x51, &game, &pot, &h66(0xbb)));
            let c = corroborate_rows(&me, std::slice::from_ref(&row), &candidates(pool));
            assert_eq!(
                c.claims[0].is_some(),
                want,
                "{junk_n} junk rows ahead of the honest marker (cap {LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT})"
            );
            assert!(c.attempts <= LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT);
        }
    }

    #[test]
    fn a_page_past_the_budget_leaves_its_tail_marker_unverified() {
        // R2-4: more live pots than the budget ⇒ the tail is honestly
        // uncorroborated even with PERFECT markers (and is still served).
        let w = wallet_of(0x42);
        let me = identity_of(&w);
        let n = LIVE_VIEW_VERIFY_BUDGET + 5;
        let mut rows: Vec<LiveViewRow> = Vec::new();
        let mut pool: Vec<SeatMarkerRow> = Vec::new();
        let (_, pk) = settle_key(0x51);
        for i in 0..n {
            let pot = format!("{:064x}", 0xaa00_u64 + i as u64);
            let game = format!("{:064x}", 0x2100_u64 + i as u64);
            let mut r = base_row();
            r.identity = me.clone();
            r.game_id = game.clone();
            r.pot_txid = pot.clone();
            r.cov_pub_a = Some(pk.clone());
            r.cov_pub_b = Some(h66(0x0b));
            rows.push(r);
            pool.push(real_marker(&w, 0x51, &game, &pot, &h66(0xbb)));
        }
        let c = corroborate_rows(&me, &rows, &candidates(pool));
        assert_eq!(c.attempts, LIVE_VIEW_VERIFY_BUDGET);
        assert_eq!(
            c.claims.iter().filter(|x| x.is_some()).count(),
            LIVE_VIEW_VERIFY_BUDGET,
            "one attempt each, honest markers all verify — until the budget ends"
        );
        let entries = assemble_live_view(rows, &c, Some(900_000));
        assert_eq!(entries.len(), n, "every pot is still SERVED");
        for e in &entries[LIVE_VIEW_VERIFY_BUDGET..] {
            assert_eq!(e.marker_source, MARKER_SOURCE_UNVERIFIED);
            assert_eq!(e.case_source, CaseProvenance::MarkerUnverified);
            assert!(e.recovery_height.is_some(), "gate math still serves");
        }
    }

    #[test]
    fn a_candidate_query_fault_is_labeled_we_could_not_look() {
        // R2-2: the fault flag produces DISTINCT labels — never
        // "marker-unverified", which claims something about the data.
        let mut r = base_row();
        r.spent = Some(false);
        let faulted = Corroborated {
            claims: vec![None],
            attempts: 0,
            unavailable: true,
        };
        let mut entries = assemble_live_view(vec![r.clone()], &faulted, Some(900_078));
        // Nothing is fetched (no corroborated pot), and the tag survives
        // apply_cases untouched.
        assert_eq!(
            fanout_targets(std::slice::from_ref(&r), &faulted.claims),
            Vec::<String>::new()
        );
        apply_cases(&mut entries, &[], &std::collections::HashMap::new());
        let e = &entries[0];
        assert_eq!(e.marker_source, MARKER_SOURCE_UNAVAILABLE);
        assert_eq!(e.case_source, CaseProvenance::CorroborationUnavailable);
        assert_eq!(e.opponent_identity_source, Some(MARKER_SOURCE_UNAVAILABLE));
        // The row is STILL served with its full D1 facts + gate math (a fault
        // never empties the list and never 5xxs — routes logs and continues).
        assert_eq!(e.recovery_height, Some(900_123));
        assert_eq!(e.blocks_to_gate, Some(45));
        assert_eq!(e.spent, Some(false));
        assert!(e.case.is_none(), "case:null still means UNKNOWN");
        let v: serde_json::Value =
            serde_json::from_str(&live_view_body(&h66(0xa1), Some(900_078), &entries)).unwrap();
        assert_eq!(
            v["live"][0]["markerSource"],
            serde_json::json!("corroboration-unavailable")
        );
        assert_eq!(
            v["live"][0]["caseSource"],
            serde_json::json!("corroboration-unavailable")
        );
        // A corroborated row on the same (faulted) page keeps its real tags.
        let claim = claim_for(&r);
        let mixed = Corroborated {
            claims: vec![claim],
            attempts: 1,
            unavailable: true,
        };
        let e = &assemble_live_view(vec![r], &mixed, None)[0];
        assert_eq!(e.marker_source, MARKER_SOURCE_SEAT_SIGNED);
        assert_eq!(e.case_source, CaseProvenance::NotFetched);
    }

    #[test]
    fn candidate_plan_splits_keyed_and_keyless_and_stays_under_the_bind_cap() {
        let mut rows: Vec<LiveViewRow> = Vec::new();
        for i in 0..40u64 {
            let mut r = base_row();
            r.pot_txid = format!("{:064x}", 0x1000_u64 + i);
            if i % 2 == 0 {
                r.cov_pub_a = Some(h66(0x0a));
                r.cov_pub_b = Some(h66(0x0b));
            } else {
                r.spent = None; // keyless ghost
                r.spent_confirmed = None;
            }
            rows.push(r);
        }
        let plan = candidate_plan(&rows);
        let keyed: usize = plan.keyed.iter().map(Vec::len).sum();
        let keyless: usize = plan.keyless.iter().map(Vec::len).sum();
        assert_eq!(
            keyed + keyless,
            LIVE_VIEW_VERIFY_BUDGET,
            "plan only what the budget can spend"
        );
        // KNOWN pots come first in quality order, so the plan is keyed-heavy
        // (all 20 known/keyed pots, then the budget's remainder in ghosts).
        assert_eq!(keyed, 20);
        assert_eq!(keyless, LIVE_VIEW_VERIFY_BUDGET - 20);
        for chunk in &plan.keyed {
            assert!(
                chunk.len() * crate::results::SEAT_MARKERS_BINDS_PER_POT
                    <= crate::logic::D1_MAX_BOUND_PARAMS
            );
        }
        for chunk in &plan.keyless {
            assert!(keyless_chunk_binds(chunk.len()) <= crate::logic::D1_MAX_BOUND_PARAMS);
        }
        // Deterministic: same rows ⇒ same plan.
        assert_eq!(candidate_plan(&rows), plan);
        // Every planned pot appears EXACTLY once (no pot silently dropped at
        // a chunk boundary — the #230 re-gate invariant).
        let mut all: Vec<(String, u32)> = plan
            .keyed
            .iter()
            .flatten()
            .map(|b| (b.pot_txid.clone(), b.pot_vout))
            .chain(plan.keyless.iter().flatten().cloned())
            .collect();
        let n = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), n);
    }

    #[test]
    fn keyless_candidates_sql_shape() {
        let sql = keyless_candidates_sql(3);
        assert_eq!(sql.matches('?').count(), 1 + 3 * 2, "identity + 2 per pot");
        assert!(sql.contains("identity = ?"));
        assert!(sql.contains("seatSettlePubkey IS NOT NULL"));
        assert!(sql.contains("PARTITION BY potTxid, potVout"));
        assert!(sql.contains(&format!("rn <= {LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT}")));
        assert!(!sql.contains("hex("));
    }

    // ── opponent / marker provenance ────────────────────────────────────────

    #[test]
    fn provenance_tags_are_honest() {
        let mut r = base_row();
        r.opponent_identity = Some(h66(0xbb).to_ascii_uppercase());
        let claim = claim_for(&r);
        let e = &assemble_live_view(vec![r.clone()], &corr_of(vec![claim]), None)[0];
        assert_eq!(
            e.opponent_identity,
            Some(h66(0xbb)),
            "the corroborated claim"
        );
        assert_eq!(e.opponent_identity_source, Some("seat-signed"));
        assert_eq!(e.marker_source, "seat-signed");

        let e = &assemble_live_view(vec![r], &corr_of(vec![None]), None)[0];
        assert_eq!(
            e.opponent_identity,
            Some(h66(0xbb)),
            "lowercased representative value"
        );
        assert_eq!(e.opponent_identity_source, Some("marker-unverified"));
        assert_eq!(e.marker_source, "marker-unverified");

        // No servable opponent ⇒ value AND tag null (never a dangling tag),
        // while markerSource still states the row's corroboration bit.
        for junk in [None, Some("banana".to_string()), Some(h64(0xaa))] {
            let mut r = base_row();
            r.opponent_identity = junk;
            let e = &assemble_live_view(vec![r], &corr_of(vec![None]), None)[0];
            assert_eq!(e.opponent_identity, None);
            assert_eq!(e.opponent_identity_source, None);
            assert_eq!(e.marker_source, "marker-unverified");
        }
        // A missing claims entry counts as uncorroborated (fail-safe).
        let e = &assemble_live_view(vec![base_row()], &corr_of(vec![]), None)[0];
        assert!(!e.marker_verified);
        assert_eq!(e.case_source, CaseProvenance::MarkerUnverified);
    }

    // ── case shaping ────────────────────────────────────────────────────────

    #[test]
    fn shapes_the_real_tower_case_body() {
        let cv = shape_case(&tower_case_body()).expect("valid case");
        assert_eq!(cv.status, CaseStatus::Pending);
        assert_eq!(cv.epoch, Some(1));
        assert_eq!(cv.deadline_ms, Some(1_700_000_600_000));
        assert_eq!(cv.accused, Some("B"));
    }

    #[test]
    fn every_stored_status_string_maps_and_nothing_else() {
        for (s, want) in [
            ("pending", CaseStatus::Pending),
            ("resolved_adjudicated", CaseStatus::ResolvedAdjudicated),
            ("resolved_continue", CaseStatus::ResolvedContinue),
            ("finalized_concede", CaseStatus::FinalizedConcede),
            ("finalized_refuse", CaseStatus::FinalizedRefuse),
        ] {
            assert_eq!(CaseStatus::from_wire(s), Some(want));
            assert_eq!(want.as_str(), s, "wire round-trip");
        }
        // Unknown / hostile statuses reject the WHOLE case (fail-closed) —
        // including the settled-lock 409's "settled" (that arrives on a 409,
        // but even a hypothetical 200 carrying it must not pass unmapped).
        for s in ["settled", "refused", "PENDING", "", "pending "] {
            let mut v = tower_case_body();
            v["status"] = serde_json::json!(s);
            assert_eq!(shape_case(&v), None, "status {s:?} must reject");
        }
        // A megabyte status string is bounded out before comparison.
        let mut v = tower_case_body();
        v["status"] = serde_json::json!("x".repeat(1_000_000));
        assert_eq!(shape_case(&v), None);
    }

    #[test]
    fn malformed_bodies_reject() {
        for v in [
            serde_json::json!(null),
            serde_json::json!([]),
            serde_json::json!("pending"),
            serde_json::json!({}),              // no status
            serde_json::json!({ "status": 7 }), // non-string status
            serde_json::json!({ "error": "no case for this game" }), // the 404 body shape
        ] {
            assert_eq!(shape_case(&v), None);
        }
    }

    #[test]
    fn hostile_numeric_fields_degrade_to_null_not_a_lie() {
        let mut v = tower_case_body();
        v["epoch"] = serde_json::json!(-1);
        v["deadline"] = serde_json::json!(1.0e300);
        let cv = shape_case(&v).expect("status still valid");
        assert_eq!(cv.epoch, None);
        assert_eq!(cv.deadline_ms, None);

        // Fractional / above 2^53−1 / missing all serve null.
        let mut v = tower_case_body();
        v["epoch"] = serde_json::json!(1.5);
        v["deadline"] = serde_json::json!(9_007_199_254_740_992.0f64); // 2^53
        let cv = shape_case(&v).unwrap();
        assert_eq!(cv.epoch, None);
        assert_eq!(cv.deadline_ms, None);
        let mut v = tower_case_body();
        v.as_object_mut().unwrap().remove("epoch");
        v.as_object_mut().unwrap().remove("deadline");
        let cv = shape_case(&v).unwrap();
        assert_eq!(cv.epoch, None);
        assert_eq!(cv.deadline_ms, None);

        // The exact ceiling is accepted; epoch 0 is a real value (deadline 0
        // is the tower's resolved-record sentinel → null).
        let mut v = tower_case_body();
        v["epoch"] = serde_json::json!(0);
        v["deadline"] = serde_json::json!(9_007_199_254_740_991.0f64);
        let cv = shape_case(&v).unwrap();
        assert_eq!(cv.epoch, Some(0));
        assert_eq!(cv.deadline_ms, Some(9_007_199_254_740_991));
        let mut v = tower_case_body();
        v["deadline"] = serde_json::json!(0.0);
        assert_eq!(shape_case(&v).unwrap().deadline_ms, None);
    }

    #[test]
    fn accused_maps_only_the_seat_alphabet() {
        for (raw, want) in [
            (serde_json::json!("A"), Some("A")),
            (serde_json::json!("B"), Some("B")),
            (serde_json::json!("C"), None),
            (serde_json::json!("AB"), None),
            (serde_json::json!("a"), None),
            (serde_json::json!(0), None),
            (serde_json::json!(null), None),
            (serde_json::json!("A".repeat(100_000)), None),
        ] {
            let mut v = tower_case_body();
            v["accused"] = raw;
            assert_eq!(shape_case(&v).unwrap().accused, want);
        }
    }

    #[test]
    fn parse_case_body_gates_on_status_and_size() {
        let body = tower_case_body().to_string();
        assert!(parse_case_body(200, &body).is_some());
        // Non-200 — including the tower's genuine 404 — is a NON-ANSWER.
        for code in [404, 409, 500, 302, 0] {
            assert_eq!(parse_case_body(code, &body), None, "HTTP {code}");
        }
        // Oversized / malformed bodies reject.
        let huge = format!(
            "{{\"status\":\"pending\",\"pad\":\"{}\"}}",
            "x".repeat(CASE_BODY_MAX_BYTES)
        );
        assert_eq!(parse_case_body(200, &huge), None);
        assert_eq!(parse_case_body(200, "not json"), None);
        assert_eq!(parse_case_body(200, ""), None);
    }

    // ── MEDIUM-3: the pre-buffer bounds + the reader seam ───────────────────

    #[test]
    fn content_length_gate_rejects_only_a_provable_over_budget_body() {
        // Over budget: reject before a single body byte is read.
        assert!(content_length_over_budget(Some(
            &(CASE_BODY_MAX_BYTES as u64 + 1).to_string()
        )));
        assert!(content_length_over_budget(Some("999999999999")));
        // Exactly at the ceiling / below: pass through to the budgeted read.
        assert!(!content_length_over_budget(Some(
            &CASE_BODY_MAX_BYTES.to_string()
        )));
        assert!(!content_length_over_budget(Some("0")));
        // Absent / unparseable / lying-shaped headers never REJECT (the
        // streamed budget is the enforcement, the header only an early exit)
        // and never PASS anything either.
        assert!(!content_length_over_budget(None));
        assert!(!content_length_over_budget(Some("")));
        assert!(!content_length_over_budget(Some("banana")));
        assert!(!content_length_over_budget(Some("-5")));
        assert!(!content_length_over_budget(Some("1e99")));
    }

    #[test]
    fn push_bounded_enforces_the_hard_byte_budget() {
        // Exactly max accumulates (the parse-side exact-ceiling edge holds).
        let mut buf = Vec::new();
        assert!(push_bounded(&mut buf, &[0u8; 10], 10));
        assert_eq!(buf.len(), 10);
        // One more byte aborts, buf untouched.
        assert!(!push_bounded(&mut buf, &[0u8], 10));
        assert_eq!(buf.len(), 10);
        // A single oversized chunk aborts immediately.
        let mut buf = Vec::new();
        assert!(!push_bounded(&mut buf, &[0u8; 11], 10));
        assert!(buf.is_empty());
        // Accumulation across chunks is what's bounded, not chunk size.
        let mut buf = Vec::new();
        assert!(push_bounded(&mut buf, &[0u8; 6], 10));
        assert!(!push_bounded(&mut buf, &[0u8; 5], 10));
        assert_eq!(buf.len(), 6);
        assert!(!push_bounded(&mut Vec::new(), &[0u8; 1], 0));
    }

    #[test]
    fn the_body_reader_seam_handles_every_stream_shape() {
        let body = tower_case_body().to_string();
        // Chunked delivery reassembles.
        let chunks: Vec<std::result::Result<Vec<u8>, ()>> =
            body.as_bytes().chunks(7).map(|c| Ok(c.to_vec())).collect();
        assert!(read_case_body(200, chunks).is_some());
        // A mid-stream transport fault is a NON-ANSWER, never a partial parse.
        assert_eq!(
            read_case_body(
                200,
                vec![
                    Ok(body.as_bytes()[..20].to_vec()),
                    Err(()),
                    Ok(body.clone().into_bytes())
                ]
            ),
            None
        );
        // Over-budget ACROSS chunks aborts (and holds nothing).
        let half = vec![0u8; CASE_BODY_MAX_BYTES / 2 + 1];
        assert_eq!(read_case_body(200, vec![Ok(half.clone()), Ok(half)]), None);
        // Exactly the ceiling is still accepted, chunked.
        let pad = CASE_BODY_MAX_BYTES - "{\"status\":\"pending\",\"pad\":\"\"}".len();
        let exact = format!("{{\"status\":\"pending\",\"pad\":\"{}\"}}", "x".repeat(pad));
        assert_eq!(exact.len(), CASE_BODY_MAX_BYTES);
        assert!(
            read_case_body(200, exact.as_bytes().chunks(4096).map(|c| Ok(c.to_vec()))).is_some()
        );
        // A multi-byte UTF-8 char SPLIT across chunks must not fault (the
        // decode happens once, at the end).
        let s = "{\"status\":\"pending\",\"pad\":\"é\"}";
        let bytes = s.as_bytes().to_vec();
        let cut = bytes.len() - 3; // splits the 2-byte 'é'
        assert!(
            read_case_body(
                200,
                vec![Ok(bytes[..cut].to_vec()), Ok(bytes[cut..].to_vec())]
            )
            .is_some(),
            "a split UTF-8 sequence is not a fault"
        );
        // Truly invalid UTF-8 is a non-answer, not a panic.
        assert_eq!(read_case_body(200, vec![Ok(vec![0xff, 0xfe])]), None);
        // Non-200 with a perfect body is still a non-answer.
        assert_eq!(read_case_body(404, vec![Ok(body.into_bytes())]), None);
        // The accumulator's abort is sticky and drops what it held.
        let mut acc = BodyAccumulator::new(4);
        assert!(acc.push(b"ab"));
        assert!(!acc.push(b"cde"));
        assert!(acc.aborted() && acc.is_empty());
        assert!(!acc.push(b"x"), "abort is sticky");
        assert_eq!(acc.finish(200), None);
        // An empty stream is a non-answer, not an empty "no case".
        assert_eq!(
            read_case_body(200, Vec::<std::result::Result<Vec<u8>, ()>>::new()),
            None
        );
    }

    // ── fan-out target selection ────────────────────────────────────────────

    fn row(game_seed: u8, pot_seed: u8, known: bool) -> LiveViewRow {
        let mut r = base_row();
        r.game_id = h64(game_seed);
        r.pot_txid = h64(pot_seed);
        if !known {
            r.spent = None;
            r.spent_confirmed = None;
        }
        r
    }

    #[test]
    fn fanout_prefers_known_pots_over_a_dust_filled_window_head() {
        // The MEDIUM-2 shape: quota-many unknown rows occupy the window head
        // (they sort tier-0 newest-first), the caller's real KNOWN pot sits
        // at position 10 — beyond the old positional cap. All corroborated
        // (the worst case for selection).
        let mut rows: Vec<LiveViewRow> = (0..LIVE_VIEW_UNKNOWN_POT_QUOTA as u8)
            .map(|i| row(0x20 + i, 0x60 + i, false))
            .collect();
        rows.push(row(0x11, 0xaa, true)); // the real pot, position 10
        let claims: Vec<Option<VerifiedClaim>> = rows.iter().map(claim_for).collect();
        let targets = fanout_targets(&rows, &claims);
        assert_eq!(
            targets[0],
            h64(0x11),
            "the KNOWN pot's gameId is fetched FIRST despite sitting beyond the positional cap"
        );
        assert_eq!(
            targets.len(),
            LIVE_VIEW_CASE_FANOUT_CAP,
            "remaining slots fill from unknowns"
        );
        assert_eq!(
            targets[1],
            h64(0x20),
            "window order within the unknown class"
        );
    }

    #[test]
    fn fanout_never_targets_an_uncorroborated_row() {
        let rows = vec![
            row(0x21, 0x61, true),
            row(0x22, 0x62, false),
            row(0x23, 0x63, true),
        ];
        let claims = vec![None, claim_for(&rows[1]), claim_for(&rows[2])];
        assert_eq!(
            fanout_targets(&rows, &claims),
            vec![h64(0x23), h64(0x22)],
            "only corroborated pots, known first"
        );
        // Missing entries count as uncorroborated (fail-safe), never a panic.
        assert_eq!(fanout_targets(&rows, &[]), Vec::<String>::new());
        assert_eq!(
            fanout_targets(&rows, &[None, None, None]),
            Vec::<String>::new()
        );
    }

    #[test]
    fn fanout_caps_dedupes_lowercases_and_skips_malformed() {
        let mut rows: Vec<LiveViewRow> = (0..12u8).map(|i| row(0x20 + i, 0x60 + i, true)).collect();
        rows[2].game_id = rows[0].game_id.to_ascii_uppercase(); // dup, case-folded
        rows[3].game_id = "not-a-game-id".into(); // unfetchable
        let claims: Vec<Option<VerifiedClaim>> = rows.iter().map(claim_for).collect();
        let targets = fanout_targets(&rows, &claims);
        assert_eq!(
            targets.len(),
            LIVE_VIEW_CASE_FANOUT_CAP,
            "cap counts DISTINCT fetches"
        );
        assert_eq!(targets[0], h64(0x20));
        assert!(!targets.contains(&"not-a-game-id".to_string()));
        assert_eq!(
            targets.iter().filter(|g| **g == h64(0x20)).count(),
            1,
            "dup fetched once"
        );
        for t in &targets {
            assert_eq!(*t, t.to_ascii_lowercase(), "tower route wants lowercase");
        }
    }

    // ── the injectable transport seam ───────────────────────────────────────

    #[test]
    fn run_fanout_caps_fetches_returns_the_effective_list_and_omits_faults() {
        use futures_util::FutureExt;
        let targets: Vec<String> = (0..12u8).map(|i| h64(0x20 + i)).collect();
        let calls = std::cell::RefCell::new(Vec::<String>::new());
        let ok = shape_case(&tower_case_body()).unwrap();
        // The fake fetcher: succeeds only for target 0x21; every other
        // outcome (timeout / non-200 / body-read failure / oversize /
        // malformed) is the same `None` the real transport returns.
        let (effective, fetched) = run_fanout(&targets, |g: String| {
            calls.borrow_mut().push(g.clone());
            let ok = ok.clone();
            async move { (g == h64(0x21)).then_some(ok) }
        })
        .now_or_never()
        .expect("ready futures");
        // Defensive re-cap: never more than CAP fetches even if a caller
        // hands an overlong target list…
        assert_eq!(calls.borrow().len(), LIVE_VIEW_CASE_FANOUT_CAP);
        assert_eq!(calls.borrow()[0], h64(0x20));
        // …and the EFFECTIVE list is exactly what was asked (LOW-F).
        assert_eq!(effective, targets[..LIVE_VIEW_CASE_FANOUT_CAP].to_vec());
        assert_eq!(fetched.len(), 1, "faulted fetches are simply absent");
        assert!(fetched.contains_key(&h64(0x21)));

        // LOW-F end to end: a row whose target was DROPPED by the re-cap is
        // "not-fetched", never "tower-unavailable" (asked and failed).
        let mut entries: Vec<LiveEntry> = (0..12u8).map(|i| entry(0x20 + i, true)).collect();
        apply_cases(&mut entries, &effective, &fetched);
        assert_eq!(
            entries[1].case_source,
            CaseProvenance::TowerByGameIdUnverified
        );
        assert_eq!(
            entries[0].case_source,
            CaseProvenance::TowerUnavailable,
            "asked, no answer"
        );
        assert_eq!(
            entries[9].case_source,
            CaseProvenance::NotFetched,
            "never asked"
        );
    }

    // ── attribution: the four-valued provenance ─────────────────────────────

    #[test]
    fn apply_cases_stamps_the_four_provenances_and_the_join_key() {
        let mut entries = vec![
            entry(0x21, true),  // fetched OK
            entry(0x22, true),  // targeted, fetch faulted
            entry(0x23, true),  // never targeted (cap overflow)
            entry(0x24, false), // uncorroborated — never asked
        ];
        let cv = shape_case(&tower_case_body()).unwrap();
        let targets = vec![h64(0x21), h64(0x22)];
        let fetched: std::collections::HashMap<String, CaseView> = [(h64(0x21), cv.clone())].into();
        apply_cases(&mut entries, &targets, &fetched);

        assert_eq!(entries[0].case.as_ref(), Some(&cv));
        assert_eq!(
            entries[0].case_source,
            CaseProvenance::TowerByGameIdUnverified
        );
        assert_eq!(
            entries[0].case_game_id,
            Some(h64(0x21)),
            "the join key is served (HIGH-1a)"
        );

        assert_eq!(entries[1].case, None, "asked and failed: still UNKNOWN");
        assert_eq!(entries[1].case_source, CaseProvenance::TowerUnavailable);
        assert_eq!(entries[1].case_game_id, None);

        assert_eq!(
            entries[2].case, None,
            "beyond the cap: honestly not-fetched"
        );
        assert_eq!(entries[2].case_source, CaseProvenance::NotFetched);

        assert_eq!(entries[3].case, None);
        assert_eq!(entries[3].case_source, CaseProvenance::MarkerUnverified);

        // The wire strings are exactly the documented four.
        assert_eq!(
            CaseProvenance::TowerByGameIdUnverified.as_str(),
            "tower-by-gameid-unverified"
        );
        assert_eq!(
            CaseProvenance::TowerUnavailable.as_str(),
            "tower-unavailable"
        );
        assert_eq!(CaseProvenance::NotFetched.as_str(), "not-fetched");
        assert_eq!(
            CaseProvenance::MarkerUnverified.as_str(),
            "marker-unverified"
        );
    }

    #[test]
    fn apply_cases_never_cases_an_uncorroborated_row_even_on_a_gameid_collision() {
        let mut entries = vec![entry(0x21, true), entry(0x21, false)];
        let cv = shape_case(&tower_case_body()).unwrap();
        let targets = vec![h64(0x21)];
        let fetched = [(h64(0x21), cv.clone())].into();
        apply_cases(&mut entries, &targets, &fetched);
        assert_eq!(entries[0].case.as_ref(), Some(&cv));
        assert_eq!(entries[1].case, None, "uncorroborated row stays caseless");
        assert_eq!(entries[1].case_source, CaseProvenance::MarkerUnverified);

        // Two CORROBORATED rows sharing one gameId share the answer —
        // honestly, since the tag only vouches "the tower's answer for this
        // gameId" and both serve the same caseGameId for the client to judge.
        let mut entries = vec![entry(0x21, true), entry(0x21, true)];
        apply_cases(&mut entries, &targets, &fetched);
        for e in &entries {
            assert_eq!(e.case.as_ref(), Some(&cv));
            assert_eq!(e.case_game_id, Some(h64(0x21)));
            assert_eq!(e.case_source, CaseProvenance::TowerByGameIdUnverified);
        }
    }

    #[test]
    fn binding_absent_labels_selected_targets_tower_unavailable() {
        // LOW-E: with the TOWER binding missing the route passes the SELECTED
        // targets and an empty map — "we should have asked and could not" is
        // the same honest unknown, never "no case".
        let mut entries = vec![entry(0x21, true), entry(0x22, false)];
        apply_cases(
            &mut entries,
            &[h64(0x21)],
            &std::collections::HashMap::new(),
        );
        assert_eq!(entries[0].case_source, CaseProvenance::TowerUnavailable);
        assert_eq!(entries[0].case, None);
        assert_eq!(entries[1].case_source, CaseProvenance::MarkerUnverified);
    }

    // ── gate math + assembly ────────────────────────────────────────────────

    #[test]
    fn gate_math_matches_refund_view_semantics() {
        let rows = vec![base_row()]; // marker height 900_123, no covenant
        let e = &assemble_live_view(rows.clone(), &corr_of(vec![None]), Some(900_078))[0];
        assert_eq!(e.recovery_height, Some(900_123));
        assert_eq!(e.blocks_to_gate, Some(45));
        assert!(!e.gate_passed);
        let e = &assemble_live_view(rows.clone(), &corr_of(vec![None]), Some(900_123))[0];
        assert_eq!(e.blocks_to_gate, Some(0));
        assert!(e.gate_passed);
        // No tip: degrade, never guess.
        let e = &assemble_live_view(rows, &corr_of(vec![None]), None)[0];
        assert_eq!(e.blocks_to_gate, None);
        assert!(!e.gate_passed);
        // Covenant-committed height beats the marker hint.
        let mut r = base_row();
        r.cov_recovery_height = Some(900_200);
        let e = &assemble_live_view(vec![r], &corr_of(vec![None]), Some(900_200))[0];
        assert_eq!(e.recovery_height, Some(900_200));
        assert!(e.gate_passed);
        // Nonsense heights serve null (no fake countdown).
        let mut r = base_row();
        r.marker_recovery_height = 0;
        let e = &assemble_live_view(vec![r], &corr_of(vec![None]), Some(900_200))[0];
        assert_eq!(e.recovery_height, None);
        assert!(!e.gate_passed);
    }

    // ── wire body ───────────────────────────────────────────────────────────

    #[test]
    fn live_view_body_shape() {
        let me = h66(0xa1);
        let row = base_row();
        let claim = claim_for(&row);
        let mut entries = assemble_live_view(vec![row], &corr_of(vec![claim]), Some(900_078));
        apply_cases(&mut entries, &[], &std::collections::HashMap::new());
        let v: serde_json::Value =
            serde_json::from_str(&live_view_body(&me, Some(900_078), &entries)).unwrap();
        assert_eq!(v["identity"], serde_json::json!(me));
        assert_eq!(v["tip"], serde_json::json!(900_078));
        let e = &v["live"][0];
        assert_eq!(e["gameId"], serde_json::json!(h64(0x11)));
        assert_eq!(e["potTxid"], serde_json::json!(h64(0xaa)));
        assert_eq!(e["potVout"], serde_json::json!(0));
        assert_eq!(e["opponentIdentity"], serde_json::json!(h66(0xbb)));
        assert_eq!(
            e["opponentIdentitySource"],
            serde_json::json!("seat-signed")
        );
        assert_eq!(e["markerSource"], serde_json::json!("seat-signed"));
        assert_eq!(e["recoveryHeight"], serde_json::json!(900_123));
        assert_eq!(e["blocksToGate"], serde_json::json!(45));
        assert_eq!(e["gatePassed"], serde_json::json!(false));
        assert_eq!(e["spent"], serde_json::json!(false));
        assert!(e["spendingTxid"].is_null());
        assert_eq!(e["spentConfirmed"], serde_json::json!(false));
        assert!(e["case"].is_null(), "case null without a successful fetch");
        assert!(
            e["caseGameId"].is_null(),
            "no join key without a fetched case"
        );
        assert_eq!(e["caseSource"], serde_json::json!("not-fetched"));

        // With a fetched case: the shaped subset, the NON-VOUCHING source
        // tag, and the served join key.
        let cv = shape_case(&tower_case_body()).unwrap();
        let targets = vec![h64(0x11)];
        let fetched = [(h64(0x11), cv)].into();
        apply_cases(&mut entries, &targets, &fetched);
        let v: serde_json::Value =
            serde_json::from_str(&live_view_body(&me, Some(900_078), &entries)).unwrap();
        let c = &v["live"][0]["case"];
        assert_eq!(c["status"], serde_json::json!("pending"));
        assert_eq!(c["epoch"], serde_json::json!(1));
        assert_eq!(c["deadlineMs"], serde_json::json!(1_700_000_600_000u64));
        assert_eq!(c["accused"], serde_json::json!("B"));
        assert_eq!(c.as_object().unwrap().len(), 4, "exactly the shaped subset");
        assert_eq!(v["live"][0]["caseGameId"], serde_json::json!(h64(0x11)));
        assert_eq!(
            v["live"][0]["caseSource"],
            serde_json::json!("tower-by-gameid-unverified"),
            "success never vouches for the case↔pot binding"
        );
        assert_ne!(v["live"][0]["caseSource"], serde_json::json!("tower"));

        // An uncorroborated row's body carries the honest tags.
        let mut entries = assemble_live_view(vec![base_row()], &corr_of(vec![None]), None);
        apply_cases(&mut entries, &[], &std::collections::HashMap::new());
        let v: serde_json::Value =
            serde_json::from_str(&live_view_body(&me, None, &entries)).unwrap();
        assert_eq!(
            v["live"][0]["caseSource"],
            serde_json::json!("marker-unverified")
        );
        assert_eq!(
            v["live"][0]["markerSource"],
            serde_json::json!("marker-unverified")
        );
        assert_eq!(
            v["live"][0]["opponentIdentitySource"],
            serde_json::json!("marker-unverified")
        );
    }

    #[test]
    fn live_view_body_empty_and_null_tip() {
        let v: serde_json::Value =
            serde_json::from_str(&live_view_body("nope", None, &[])).unwrap();
        assert_eq!(v["identity"], serde_json::json!("nope"));
        assert!(v["tip"].is_null());
        assert_eq!(v["live"], serde_json::json!([]));
    }

    // ── SQL structure pins ──────────────────────────────────────────────────

    #[test]
    fn live_view_sql_shape() {
        let sql = live_view_sql(None);
        assert_eq!(sql.matches('?').count(), 1, "one identity bind");
        assert!(sql.contains(&format!("LIMIT {LIVE_VIEW_MAX_ROWS}")));
        assert!(sql.contains("PARTITION BY pp.potTxid, pp.potVout"));
        assert!(sql.contains(&format!("potRank <= {LIVE_VIEW_UNKNOWN_POT_QUOTA}")));
        // The NULL-safe liveness predicate — COALESCE on BOTH columns, so a
        // join miss / unconfirmed spend stays live and only a confirmed
        // spend is excluded.
        assert!(sql.contains("COALESCE(r.spent, 0) = 0 OR COALESCE(r.spentConfirmed, 0) = 0"));
        // The corroboration columns ride along: the representative marker's
        // own v2 fields AND the pot's DECODED committed keys.
        for col in [
            "pp.identity",
            "pp.sigHex",
            "pp.seatSettlePubkey",
            "pp.seatSigHex",
            "r.pubA AS covPubA",
            "r.pubB AS covPubB",
        ] {
            assert!(sql.contains(col), "missing {col}");
        }
        // …but no BLOB is ever transferred; no backup bytes belong here.
        assert!(!sql.contains("hex("));
        assert!(!sql.contains("refundRawHex"));
        assert!(!sql.contains("potrefund_records"));
    }

    /// #375 — the era filter on `/live-view`: exactly one shared fragment,
    /// at the innermost identity+liveness scan, anchored
    /// `COALESCE(r.createdAt, pp.createdAt)`; stripping it restores the
    /// `None` arm byte-for-byte, and the cutoff is exactly one extra bind.
    #[test]
    fn live_view_sql_era_filter_shape_and_none_identity() {
        let cutoff = Some(1_754_500_000_000i64);
        let frag = crate::logic::era_filter_sql("COALESCE(r.createdAt, pp.createdAt)", "?", cutoff);
        let with = live_view_sql(cutoff);
        let without = live_view_sql(None);
        assert_eq!(with.matches(&frag).count(), 1, "exactly one era fragment");
        assert_eq!(
            with.matches(&format!(
                "(COALESCE(r.spent, 0) = 0 OR COALESCE(r.spentConfirmed, 0) = 0){frag})"
            ))
            .count(),
            1,
            "the era filter rides the innermost scan, beside the liveness predicate"
        );
        assert_eq!(with.matches('?').count(), 2, "identity + cutoff binds");
        assert_eq!(
            with.replace(&frag, ""),
            without,
            "None must stay byte-identical to the pre-#375 query"
        );
    }
}
