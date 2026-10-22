//! Observability for the BEEF proof-completion pipeline (#192/#193, P4).
//!
//! zanaadu's single most expensive omission was an unobserved completion pass:
//! a dead pass hid for WEEKS. These primitives make a dead pass surface in a
//! DAY, not weeks:
//!
//! - `ops_heartbeat` — a singleton (`id = 0`) upserted at the end of every cron
//!   completion pass. `last_tick_ms` is the wall-clock of the last live pass;
//!   `tick_count` a monotonic pass count.
//! - `ops_counters` — persistent monotonic counters (`proofs_completed_total`,
//!   `fetch_failed_total`, `pot_beefs_compacted_total`, `spends_confirmed_total`),
//!   bumped every tick.
//! - `proofless_watch` — a first-seen ledger for proofless txids; a tx still
//!   proofless after 24h is flagged (the signal that a proof genuinely is not
//!   landing, vs. merely not-yet-mined).
//! - `GET /health/invariants?strict=1` — 503 when the completion pass has been
//!   dead longer than the staleness budget (the alarm surface).
//!
//! Schema lives in `d1::OVERLAY_MIGRATIONS`. Every write here is BEST-EFFORT:
//! an observability fault logs and is swallowed — it must never break a
//! completion pass or a request.

use serde::Deserialize;
use serde_json::json;
use worker::{D1Database, Env, Response};

use crate::d1::Query;

/// Counter names (persisted in `ops_counters`).
pub const COUNTER_PROOFS_COMPLETED: &str = "proofs_completed_total";
/// Proof fetches that failed.
pub const COUNTER_FETCH_FAILED: &str = "fetch_failed_total";
/// Pot BEEFs compacted once their proof landed.
pub const COUNTER_POT_BEEFS_COMPACTED: &str = "pot_beefs_compacted_total";
/// Pot spends UPGRADED to `spentConfirmed = 1` by the spend-confirmation
/// chaser (#186). Name-keyed → additive, no schema change.
pub const COUNTER_SPENDS_CONFIRMED: &str = "spends_confirmed_total";
/// Verified merkle proofs that arrived via the `/arc-ingest` PUSH (#228 — the
/// primary proof source). Compare against `proofs_completed_total` (the poll
/// backstop) to see the push/poll split. Name-keyed → additive.
pub const COUNTER_ARC_INGEST_PUSHED: &str = "arc_ingest_pushed_total";

// ── S2 queue-durable admission (bsv-low 2026-08-29) ──────────────────────
/// `/submit` admissions whose Phase-3 writes did NOT all land (the engine's
/// `MutationReport` carried ≥1 fault). Every one of these was, pre-S2, an
/// ack over a dropped write.
pub const COUNTER_SUBMIT_MUTATION_FAULT: &str = "submit_mutation_fault_total";
/// …of which the replay was QUEUED before the ack (the durable-ack path).
pub const COUNTER_SUBMIT_MUTATION_QUEUED: &str = "submit_mutation_queued_total";
/// …of which the replay could NOT be queued and the submit was REFUSED
/// (502 retryable). Sustained non-zero = the queue itself is unhealthy.
pub const COUNTER_SUBMIT_MUTATION_REFUSED: &str = "submit_mutation_refused_total";
/// Queue consumer: replays whose every write landed (acked).
pub const COUNTER_QUEUE_MUTATION_APPLIED: &str = "queue_mutation_applied_total";
/// Queue consumer: replays that still faulted (or errored) and were handed
/// back for the platform's retry/backoff. After `max_retries` the message
/// dead-letters — a growing DLQ is an operator page, never a silent drop.
pub const COUNTER_QUEUE_MUTATION_RETRIED: &str = "queue_mutation_retried_total";
// ── bsv-low W-A / #437 step 2: the gated door's script walk (2026-09-09) ──
/// Network-gated submits REFUSED at the door on the interpreter's verdict
/// (`Engine::verify_scripts_only`: a spend whose unlocking script the
/// interpreter refused — the network would have refused it too; nothing was
/// broadcast). Sustained non-zero on a healthy client population is an
/// interpreter divergence to investigate, never a silent drop: the client
/// falls to its own direct broadcast and the network's own verdict.
pub const COUNTER_SUBMIT_SCRIPT_REFUSED: &str = "submit_script_refused_total";
/// Network-gated submits whose door walk ended INCONCLUSIVE before the
/// SUBJECT was judged (a structural fault on the subject itself: a source the
/// completed BEEF still lacks, a parse fault) — logged, counted, and handed
/// to the network gate unchanged. Not a refusal class: the interpreter never
/// reached a verdict on the money transaction.
pub const COUNTER_SUBMIT_SCRIPT_WALK_INCONCLUSIVE: &str = "submit_script_walk_inconclusive_total";
/// …whose door walk judged the SUBJECT (every input executed, none refused)
/// and then faulted structurally on an ANCESTOR (the routine shape of an
/// F-D-completed or loop-2 incomplete ancestry). The money tx was judged.
pub const COUNTER_SUBMIT_SCRIPT_WALK_ANCESTOR_INCONCLUSIVE: &str =
    "submit_script_walk_ancestor_inconclusive_total";
/// …whose door walk exceeded the door's OWN bound (`DoorBudget`: the static
/// work estimate or the memory limit) — the door's verdict, never the
/// network's; the request proceeds to the network gate. Sustained non-zero on
/// honest traffic means the budget is too tight for a real shape.
pub const COUNTER_SUBMIT_SCRIPT_WALK_OVER_BUDGET: &str = "submit_script_walk_over_budget_total";
/// …whose door walk was not made because it would hold more MEMORY than the
/// door's budget beside the body (`DoorBudget::max_memory_bytes`, bsv-low
/// #585: the estimate is made from the frame's lengths and counts before the
/// stream is opened: a BUMP of tens of thousands of leaves, a hundred
/// thousand tiny transactions) — the door's verdict, never the network's;
/// the request proceeds to the network gate.
pub const COUNTER_SUBMIT_SCRIPT_WALK_OVER_MEMORY: &str = "submit_script_walk_over_memory_total";

/// Submits whose ENGINE walk could not run within the engine's budget
/// (bsv-low #592: `Engine::submit`'s own script walk, every mode but
/// `historical-tx-no-spv`, the queue's `historical-tx` replay of every gated
/// submit and the ungated modes): the static work estimate, or the
/// interpreter's own memory limit tripping. NOT a refusal: "not now" on every
/// door where nothing stands in for the walk (the E592 lens fold), and on the
/// queue's replay of a gated submission (the network's accept on record) the
/// submit went on as under `historical-tx-no-spv`. Sustained non-zero on
/// honest traffic means the budget is too tight for a real shape.
pub const COUNTER_SUBMIT_ENGINE_WALK_OVER_BUDGET: &str = "submit_engine_walk_over_budget_total";
/// …whose engine walk was not made because it would hold more MEMORY than
/// the budget's limb beside the body (the 1.9 MB push-only unproven parent
/// that killed the consumer's isolate on every redelivery before #592).
pub const COUNTER_SUBMIT_ENGINE_WALK_OVER_MEMORY: &str = "submit_engine_walk_over_memory_total";

/// The counter of an engine walk that could not run, by the limb it passed.
pub fn engine_walk_over_counter(limb: overlay_engine::engine::WalkLimb) -> &'static str {
    match limb.door_limb() {
        overlay_engine::engine::DoorLimb::Work => COUNTER_SUBMIT_ENGINE_WALK_OVER_BUDGET,
        overlay_engine::engine::DoorLimb::Memory => COUNTER_SUBMIT_ENGINE_WALK_OVER_MEMORY,
    }
}

/// Count and log the engine walks of a submit that could not run (bsv-low #592) and WENT ON (the gated replay,
/// `WalkBreachPolicy::NetworkAccepted`), and those of its carried predecessors' landings (the E592 lens fold, L3),
/// at `door` (`POST /submit`, `Queue`, `POST /admin/readmit`, `peer-crawl`). Nothing when every walk ran.
pub async fn note_engine_walk(
    db: &D1Database,
    report: &overlay_engine::engine::MutationReport,
    door: &str,
) {
    if let Some(stop) = &report.walk_could_not_run {
        worker::console_log!(
            "{door}: the engine's script walk could not run for {} ({}: at {}, subject judged: {}; {}): the network accepted these bytes, admitted as historical-tx-no-spv",
            stop.subject_txid,
            stop.limb.as_str(),
            stop.at_txid,
            stop.subject_judged,
            stop.what
        );
        bump_counter(db, engine_walk_over_counter(stop.limb), 1).await;
    }
    for stop in &report.landed_walks_could_not_run {
        worker::console_log!(
            "{door}: a carried predecessor's walk could not run ({} for {}; {}): counted with its landing",
            stop.limb.as_str(),
            stop.subject_txid,
            stop.what
        );
        bump_counter(db, engine_walk_over_counter(stop.limb), 1).await;
    }
}

/// Count and log a submit whose engine walk could not run and was answered NOT NOW (the E592 lens fold: nothing on
/// record stands in for the walk), at `door`.
pub async fn note_engine_walk_not_now(
    db: &D1Database,
    stop: &overlay_engine::engine::WalkCouldNotRun,
    door: &str,
) {
    worker::console_log!("{}", engine_walk_not_now_line(stop, door));
    bump_counter(db, engine_walk_over_counter(stop.limb), 1).await;
}

/// The detail line of a NOT NOW walk at `door`, logged by every door whether its counter is written at once
/// ([`note_engine_walk_not_now`]) or rides `/submit`'s request batch.
pub fn engine_walk_not_now_line(
    stop: &overlay_engine::engine::WalkCouldNotRun,
    door: &str,
) -> String {
    format!(
        "{door}: the engine's script walk could not run for {} ({}: at {}, subject judged: {}; {}): not now, nothing admitted; present it proven",
        stop.subject_txid,
        stop.limb.as_str(),
        stop.at_txid,
        stop.subject_judged,
        stop.what
    )
}

/// The counter of a door walk past its own bound, by the limb it passed.
pub fn script_walk_over_counter(limb: overlay_engine::engine::DoorLimb) -> &'static str {
    match limb {
        overlay_engine::engine::DoorLimb::Work => COUNTER_SUBMIT_SCRIPT_WALK_OVER_BUDGET,
        overlay_engine::engine::DoorLimb::Memory => COUNTER_SUBMIT_SCRIPT_WALK_OVER_MEMORY,
    }
}
/// Non-MINED `/arc-ingest` status callbacks (X-FullStatusUpdates bodies with
/// no merklePath) acknowledged-and-ignored (#228). A count here is NORMAL
/// operation, not an error — it proves the webhook stream is alive.
pub const COUNTER_ARC_INGEST_STATUS_IGNORED: &str = "arc_ingest_status_ignored_total";
/// `/arc-ingest` callbacks REFUSED 401 because NO bearer token was presented in
/// either accepted header. Diagnosis: a CONTRACT or CONFIG problem — the
/// courier is not authenticating the way we read it — or unauthenticated noise.
///
/// This counter exists because its absence hid a month-long outage: the 401 arm
/// bumped nothing, and `arc_ingest_status_ignored_total` is only reachable
/// AFTER auth, so `{pushed: 0, statusIgnored: 0}` read identically for "nobody
/// is calling us" and "every caller is being refused" (epoch Rule 13). The
/// wrong one was believed.
pub const COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN: &str = "arc_ingest_unauthorized_no_token_total";
/// `/arc-ingest` callbacks REFUSED 401 because a token WAS presented and did
/// not equal the subject txid. Diagnosis: a stale registration, or a prober.
pub const COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN: &str = "arc_ingest_unauthorized_bad_token_total";
/// #640: ancestor legs left unregistered by the 32/request window or the
/// unused portion of the consumer's shared 256-leg budget, plus matching
/// TERMINAL ancestors skipped without a re-feed. Counted per rung-3 pass,
/// so a later run can count the same unregistered ancestor again.
/// Their proofs still arrive through the backstop poll and couriers.
pub const COUNTER_ARC_REGISTRATION_SKIPPED: &str = "arc_registration_legs_skipped_total";
/// bsv-low loop 6 (2026-09-07): MINED callbacks for a txid NO topic admitted
/// (a `/submit` that admitted 0 outputs still registered the webhook). Since
/// #640 this also counts ancestors' own registered MINED proof pushes when
/// no topic holds their txid (hops and strangers' ancestors) — nobody holds it, nothing is owed; acknowledged 200 and COUNTED (a 5xx made Arcade
/// retry the same callback a second later, twice per txid in the beta log).
pub const COUNTER_ARC_INGEST_UNKNOWN_TXID: &str = "arc_ingest_unknown_txid_total";
/// bsv-low loop 6: block-event spend-confirmation passes run (`/internal/tip-changed`).
pub const COUNTER_TIP_PASS_TOTAL: &str = "tip_pass_total";
// ── bsv-low M19 R2 (2026-09-08): the reorg reconcile (#425, #429) ──────────
/// Reorgs the block-event pass DETECTED (a hash change at a held height, or a
/// lower announce whose hash we did not hold). Loop 8 saw two in 35 minutes.
pub const COUNTER_CHAIN_REORGS_DETECTED: &str = "chain_reorgs_detected_total";
/// Confirmed pot rows DEMOTED back to SEEN by a detected reorg (bulk, from the
/// reorg height) or by Arcade's `reorg_unmined`: the confirm arm re-upgrades
/// them on the next good proof.
pub const COUNTER_REORG_DEMOTED: &str = "reorg_demoted_total";
/// Confirmed rows the revalidation sweep re-verified against the current
/// header source (the last 3 heights, every block-event pass).
pub const COUNTER_REORG_REVERIFIED: &str = "reorg_reverified_total";
/// Rows whose STORED spender proof the current header source REFUTES (#429):
/// zero on a healthy stream; every one is a confirmation that rested on an
/// orphan and was demoted. Surfaced on `/health/invariants`.
pub const COUNTER_REORG_STALE_PROOFS: &str = "reorg_stale_proofs_total";
/// Sweep rows whose re-verification could not READ the header source (a
/// transport / subrequest fault): not a verdict, retried next pass.
pub const COUNTER_REORG_TRACKER_FAULTS: &str = "reorg_tracker_faults_total";
/// `/arc-ingest` callbacks carrying an Arcade reorg marker (`extraInfo:
/// reorg_reanchor` / `reorg_unmined`, Arcade issue #279).
pub const COUNTER_ARC_INGEST_REORG_EVENTS: &str = "arc_ingest_reorg_events_total";
/// Confirmed rows whose `spentHeight` MOVED because a pushed, verified proof
/// named a different block than the stored confirmation (a re-anchor is a
/// REPLACEMENT, never a `cas_missed`).
pub const COUNTER_ARC_INGEST_REANCHORED: &str = "arc_ingest_reanchored_total";
/// admit-fast (2026-09-15): a SEEN+ callback latched `network_seen` (the witness by push).
pub const COUNTER_ARC_INGEST_SEEN_LATCHED: &str = "arc_ingest_seen_latched_total";
/// admit-fast: a txid evicted everywhere (a double spend at once, or a corroborated refusal).
pub const COUNTER_ARC_INGEST_EVICTED: &str = "arc_ingest_evicted_total";
/// admit-fast: an evicted txid readmitted on a pushed proof (the chain overruled a courier) — or, since loop 18,
/// by the operator's `/admin/readmit` (the same readmission, the operator's word).
pub const COUNTER_ARC_INGEST_READMITTED: &str = "arc_ingest_readmitted_total";
/// admit-fast: a refusal callback the evidence check did NOT corroborate (kept; a plant or a stale word).
pub const COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED: &str =
    "arc_ingest_refusal_uncorroborated_total";
/// admit-fast: a SEEN+ push Arcade did NOT confirm live (kept unlatched; a plant or a stale word).
pub const COUNTER_ARC_INGEST_PUSH_UNVERIFIED: &str = "arc_ingest_push_unverified_total";
/// Stored Reached POST echoes, including refused witnessed duplicates (#637).
pub const COUNTER_SUBMIT_DUPLICATE_IN_FLIGHT: &str = "submit_duplicate_in_flight_total";
/// Pending fast admissions from stored Reached POST echoes, separate from new accepts.
pub const COUNTER_SUBMIT_FAST_ADMITTED_ECHO: &str = "submit_fast_admitted_echo_total";
/// Pending admission classes have disjoint counters.
pub fn pending_admission_counter(fast: bool, duplicate: bool) -> &'static str {
    if fast && duplicate {
        COUNTER_SUBMIT_FAST_ADMITTED_ECHO
    } else if fast {
        COUNTER_SUBMIT_FAST_ADMITTED
    } else {
        COUNTER_SUBMIT_PENDING_ADMITTED
    }
}

/// admit-fast step 3 (2026-09-15): gated submits ANSWERED on Arcade's
/// synchronous accept (`ADMIT_FAST`), admitted PENDING (the witness in the
/// background).
pub const COUNTER_SUBMIT_FAST_ADMITTED: &str = "submit_fast_admitted_total";
/// #397 pending admissions in the WITNESSED mode (a lagging tracker + an
/// inconclusive corroborator on a proven-ancestry single leg).
pub const COUNTER_SUBMIT_PENDING_ADMITTED: &str = "submit_pending_admitted_total";
/// Pending admissions the pending watch saw SEEN (the latch), and their
/// summed admission→SEEN milliseconds (the mean SEEN latency is the ratio).
pub const COUNTER_SUBMIT_PENDING_SEEN_LATCHED: &str = "submit_pending_seen_latched_total";
/// The summed admission-to-SEEN milliseconds of those admissions.
pub const COUNTER_SUBMIT_PENDING_SEEN_MS: &str = "submit_pending_seen_ms_total";
/// Pending admissions the watch EVICTED on a corroborated refusal.
pub const COUNTER_SUBMIT_PENDING_EVICTED: &str = "submit_pending_evicted_total";
/// A fatal look the evidence check did not corroborate (kept; the #214 class).
pub const COUNTER_SUBMIT_PENDING_REFUSAL_UNCORROBORATED: &str =
    "submit_pending_refusal_uncorroborated_total";
/// Unwitnessed at the end of the watch: silent (Arcade's last look unknown or below SEEN) AND the subject's
/// `network_seen` still UNLATCHED (bsv-low #822: a subject another path latched is
/// [`COUNTER_SUBMIT_PENDING_LATCHED_ELSEWHERE`], never silent).
pub const COUNTER_SUBMIT_PENDING_SILENT: &str = "submit_pending_silent_total";
/// Unwitnessed at the end of the watch: still an orphan view, and `network_seen` still UNLATCHED (bsv-low #822).
pub const COUNTER_SUBMIT_PENDING_ORPHAN: &str = "submit_pending_orphan_total";
/// bsv-low #822 (loop 42, pair 3): the watch saw no SEEN from Arcade, but the subject's `network_seen` was latched
/// by another path by the watch's end (the #413 dual push's TAAL or GorillaPool SEEN, a callback, the door's own
/// witness): Arcade's rank can sit at ACCEPTED_BY_NETWORK for 30 min after the network said SEEN. Never silent.
pub const COUNTER_SUBMIT_PENDING_LATCHED_ELSEWHERE: &str = "submit_pending_latched_elsewhere_total";
/// bsv-low loop 18 (2026-09-21, pair 11): an UNGATED write (the operator's historical modes; the lenient
/// window) refused under an OPEN eviction (nothing written; a MINED proof or the operator's readmit opens it).
pub const COUNTER_SUBMIT_REFUSED_EVICTED: &str = "submit_refused_evicted_total";
/// loop 18 (the gate's HIGH-2): a gated subject under an open eviction that the network ACCEPTED again —
/// readmitted at the door (the fresh word overrules the ledger's stale one; a wrong eviction heals on the next
/// accepted re-present, never permanently).
pub const COUNTER_SUBMIT_READMITTED_BY_FRESH_ACCEPT: &str =
    "submit_readmitted_by_fresh_accept_total";
/// bsv-low #519 (loop 20, 2026-09-21): an Arcade-terminal subject judged at the ladder by the chain's indexers
/// (never a broadcaster's echo): held by an indexer (admitted), absent past the age floor (the two-source refusal),
/// absent but young (the retryable word), the indexers inconclusive, or this isolate's courier budget spent.
pub const COUNTER_SUBMIT_TERMINAL_RACE: &str = "submit_terminal_judged_race_total";
/// Judged by the indexers: held by one (admitted).
pub const COUNTER_SUBMIT_TERMINAL_PRESENT: &str = "submit_terminal_judged_present_total";
/// Judged: absent past the age floor (the two-source refusal).
pub const COUNTER_SUBMIT_TERMINAL_ABSENT: &str = "submit_terminal_judged_absent_total";
/// Judged: absent but young (the retryable word).
pub const COUNTER_SUBMIT_TERMINAL_YOUNG: &str = "submit_terminal_judged_young_total";
/// Judged: the indexers inconclusive.
pub const COUNTER_SUBMIT_TERMINAL_INCONCLUSIVE: &str = "submit_terminal_judged_inconclusive_total";
/// Judged: this isolate's courier budget spent.
pub const COUNTER_SUBMIT_TERMINAL_CAPPED: &str = "submit_terminal_judged_capped_total";
/// loop 18: an admission write that outran its own eviction's table loop — re-evicted after the write, the
/// submit answered as the refusal it is.
pub const COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE: &str = "admit_fast_reevicted_after_write_total";
/// bsv-low #575 (the E1D delta-2 fold, L2): a carried predecessor the engine was about to land first from a
/// successor's BEEF, refused BEFORE its write because it is under an OPEN eviction (the successor's topic waits).
pub const COUNTER_LANDING_REFUSED_EVICTED: &str = "landing_refused_evicted_total";
/// bsv-low #576: dead letters PARKED in `mutation_dead_letters` by the DLQ consumer (a re-park of a re-driven letter
/// included; a DLQ redelivery of a parked letter is not counted twice).
pub const COUNTER_DEAD_LETTERS_PARKED: &str = "dead_letters_parked_total";
/// bsv-low #576: parked letters the operator's lever re-enqueued (`POST /internal/redrive-dead-letters`).
pub const COUNTER_DEAD_LETTERS_REDRIVEN: &str = "dead_letters_redriven_total";
/// bsv-low #576: re-driven letters that dead-lettered again and were parked again.
pub const COUNTER_DEAD_LETTERS_STILL_FAILING: &str = "dead_letters_still_failing_total";
/// bsv-low #576 (lens fold, H1): a dead letter whose park faulted on its LAST DLQ delivery: the platform drops it
/// (logged `[dead-letters] LOST` with its key and the sha256 of its bytes). Best effort: D1 is usually what faulted.
/// Since the delta-3 fold (D3-L3) a FAULT letter only, or one whose class was not read: the honest loss to alarm on.
pub const COUNTER_DEAD_LETTERS_LOST: &str = "dead_letters_lost_total";
/// bsv-low #576 (the delta-3 fold, D3-L3): a "not now" letter LOST (deferred at a bound or at the ceiling for its
/// ~48 h, or its park faulting): the designed fate of a stranger's flood's tail, counted apart from
/// [`COUNTER_DEAD_LETTERS_LOST`].
pub const COUNTER_DEAD_LETTERS_LOST_NOT_NOW: &str = "dead_letters_lost_not_now_total";
/// bsv-low #576 (lens fold, M2): a new LETTER not parked because the table holds the ceiling of letters with bytes
/// ([`crate::dead_letters::PARKED_ROWS_CEILING`]); it is handed back to the DLQ with backoff, never acked. Counted
/// once per letter, on its first DLQ delivery (`attempts == 1`; the delta fold, D-L1: it counted every delivery).
pub const COUNTER_DEAD_LETTERS_CEILING_DEFERRED: &str = "dead_letters_ceiling_deferred_total";
/// bsv-low #576 (the delta fold, D-L1): every DLQ DELIVERY deferred at the ceiling (up to 1 + 100 per letter).
pub const COUNTER_DEAD_LETTERS_CEILING_DEFERRALS: &str = "dead_letters_ceiling_deferrals_total";
/// bsv-low #576 (the delta fold, D-M1): parked letters the operator discarded by key
/// (`POST /internal/discard-dead-letters`) to make room under the ceiling.
pub const COUNTER_DEAD_LETTERS_DISCARDED: &str = "dead_letters_discarded_total";
/// bsv-low #576 (the delta-2 fold, D2-M1): every DLQ DELIVERY of a "not now" letter deferred at one of its class's
/// bounds (the not-now share, its txid's bound, the day's bound), apart from the whole ceiling's deferrals.
pub const COUNTER_DEAD_LETTERS_NOT_NOW_DEFERRALS: &str = "dead_letters_not_now_deferrals_total";
/// bsv-low #576 (lens fold, M1): re-drives claimed longer than [`crate::dead_letters::STALE_REDRIVE_MS`] ago and
/// never resolved nor parked again, returned to the parked set by the lever.
pub const COUNTER_DEAD_LETTERS_STALE_RETURNED: &str = "dead_letters_stale_returned_total";
/// bsv-low #576 (lens fold, M2): parked or re-driven letters whose bytes later landed (the row is deleted).
pub const COUNTER_DEAD_LETTERS_RESOLVED: &str = "dead_letters_resolved_total";
/// bsv-low #585 (door 3): a queued replay's BEEF written to R2 (`BEEF_BLOBS`) because its inline message would pass
/// the room (`queue::QUEUE_MESSAGE_ROOM`); the write comes before the enqueue.
pub const COUNTER_BEEF_BLOBS_WRITTEN: &str = "beef_blobs_written_total";
/// bsv-low #585 (door 3): R2 objects deleted by the deletion rule (the consumer's ack, a LOST or dropped dead
/// letter, the operator's discard).
pub const COUNTER_BEEF_BLOBS_DELETED: &str = "beef_blobs_deleted_total";
/// bsv-low #585 (door 3): a delete of the rule that faulted: the object stays and nothing names it (its key is in
/// the `[beef-blobs]` log line).
pub const COUNTER_BEEF_BLOBS_DELETE_FAULTS: &str = "beef_blobs_delete_faults_total";
/// bsv-low #585 (door 3): a replay whose R2 object was MISSING at the consumer's read (a fault letter).
pub const COUNTER_BEEF_BLOBS_MISSING: &str = "beef_blobs_missing_total";
/// bsv-low #585 (the d3 fold): a replay whose R2 object was MISSING and whose subject's applied rows show its
/// bytes landed (a twin of an acked message): acked as a dupe, no letter.
pub const COUNTER_QUEUE_R2_TWIN_ACKED: &str = "queue_r2_twin_acked_total";
/// bsv-low #585 (the d3 fold): a replay whose R2 object was MISSING and whose subject is NOT shown landed: the
/// replay's fault (a fault letter). `beef_blobs_missing_total` counts every missing read, these two its verdicts
/// (the rest were acked under an open eviction).
pub const COUNTER_QUEUE_R2_MISSING_FAULT: &str = "queue_r2_missing_fault_total";
/// bsv-low #585 (door 3's fold): R2 objects the orphan sweep deleted (past the window, named by no dead letter),
/// their bytes, and the sweep's reads and deletes that faulted (the object stays for the next round).
pub const COUNTER_QUEUE_R2_ORPHANS_SWEPT: &str = "queue_r2_orphans_swept_total";
/// The bytes of the R2 objects the orphan sweep deleted.
pub const COUNTER_QUEUE_R2_ORPHANS_SWEPT_BYTES: &str = "queue_r2_orphans_swept_bytes_total";
/// The orphan sweep's reads and deletes that faulted.
pub const COUNTER_QUEUE_R2_ORPHAN_SWEEP_FAULTS: &str = "queue_r2_orphan_sweep_faults_total";
/// bsv-low #585 (door 3's fold-4, the delta-2 lens's N3): listed objects the sweep could not read (a key or an
/// `uploaded` date that does not read), skipped and never swept; each pass that meets one counts it again.
pub const COUNTER_QUEUE_R2_ORPHAN_SWEEP_UNREADABLE: &str = "queue_r2_orphan_sweep_unreadable_total";
/// loop 18: an eviction pass that could not prove every table clean (a faulted read, a survivor after the
/// second move) — the open marker stands; the write-side guard and the next eviction converge.
pub const COUNTER_ADMIT_FAST_EVICT_INCOMPLETE: &str = "admit_fast_evict_incomplete_total";
/// loop 18: the eviction ledger could not be read where a guard asked (named, never a silent "none").
pub const COUNTER_ADMIT_FAST_LEDGER_UNREADABLE: &str = "admit_fast_ledger_unreadable_total";
/// loop 18: a queued replay skipped because its subject is under an open eviction (a replay never
/// resurrects an evicted pot).
pub const COUNTER_QUEUE_REPLAY_SKIPPED_EVICTED: &str = "queue_replay_skipped_evicted_total";
/// loop 18, round 2 of the gate (NEW-1): an eviction pass overtaken by a READMISSION (the chain's word landed
/// under the pass) — the pass's own moves restored again, the ledger left readmitted.
pub const COUNTER_ADMIT_FAST_EVICT_YIELDED: &str = "admit_fast_evict_yielded_total";
/// bsv-low #649: a background flush of an admit-fast job (the pending watch, the `/arc-ingest` refusal and
/// witness jobs) SKIPPED because less than `pot_changes::FLUSH_WINDOW_FLOOR_MS` of the invocation's `waitUntil`
/// window was left, counted per job. Its pot and hop notes are PARKED in D1 for the cron (`change_park.rs`): a
/// successful park takes them out of the isolate's sets (`change_parked_total`); a failed park
/// (`change_park_faults_total`) or a full table (`change_park_full_total`) leaves them in the sets for the
/// isolate's next flush. The tracker frames it withheld stay in the isolate's memory.
pub const COUNTER_ADMIT_FAST_FLUSH_SKIPPED_NO_WINDOW: &str =
    "admit_fast_flush_skipped_no_window_total";
/// loop 18, round 2 (NEW-2): a readmission whose restore faulted on a table — the row left OPEN (never a
/// closed ledger over rows still in a twin); the next accept or proof runs it again. Also bumped by an eviction
/// pass whose yield-restore faulted with twin rows left (re-opened), by the readmission's belt on the same, and
/// by a readmission whose stamp UPDATE failed (round 4).
pub const COUNTER_ADMIT_FAST_READMIT_INCOMPLETE: &str = "admit_fast_readmit_incomplete_total";
/// bsv-low #602 fold (MEDIUM-1): the conflict pass's live unverified pots re-checked, the pots it evicted on a
/// bound confirmed competitor, and the pots whose re-check faulted (a candidate read fault counts one).
pub const COUNTER_ADMIT_FAST_CONFLICT_SCANNED: &str = "admit_fast_conflict_scanned_total";
/// Pots the conflict pass evicted on a bound confirmed competitor.
pub const COUNTER_ADMIT_FAST_CONFLICT_EVICTED: &str = "admit_fast_conflict_evicted_total";
/// Conflict re-checks that faulted.
pub const COUNTER_ADMIT_FAST_CONFLICT_FAULTS: &str = "admit_fast_conflict_faults_total";
/// bsv-low #831 half 2: retired rows (either store's arm, since the FIX-831-B lens's MEDIUM-1) whose live pot the
/// retire pass then evicted (`proof_fetcher::end_retired_pot`).
pub const COUNTER_RETIRE_EVICTED: &str = "retire_evicted_total";
/// bsv-low #831 half 2: pots latched unprovable before the retire pass evicted, looked at again by the backlog pass.
pub const COUNTER_RETIRE_BACKLOG_SCANNED: &str = "retire_backlog_scanned_total";
/// Backlog pots the fresh look and the verdict evicted.
pub const COUNTER_RETIRE_BACKLOG_EVICTED: &str = "retire_backlog_evicted_total";
/// Backlog pots the fresh look found held or mined: left alone, their latch cleared.
pub const COUNTER_RETIRE_BACKLOG_LEFT: &str = "retire_backlog_left_total";
/// Retire-pass end steps that faulted (an end-state read, an eviction or one left incomplete, a latch or its clear,
/// the backlog's candidate read).
pub const COUNTER_RETIRE_EVICT_FAULTS: &str = "retire_evict_faults_total";
/// bsv-low #831 (the FIX-831-B lens's MEDIUM-2): endings the retire pass's statement budget did not cover, and
/// backlog candidates left unlooked for it; each pot is latched and live, ended by a later pass.
pub const COUNTER_RETIRE_END_DEFERRED: &str = "retire_end_deferred_total";
/// fleet loop 15 (2026-09-20): the pending watch confirmed a spend by its OWN proof read at its end — a tx that mined
/// before the index saw it (Arcade's stored echo never says MINED, no callback is registered for an already-known tx).
pub const COUNTER_SUBMIT_PENDING_CONFIRMED_NOW: &str = "submit_pending_confirmed_now_total";
/// fleet loop 15: the watch's end ASKED the couriers for a proof (the txid names an unconfirmed pot spender) — the
/// courier spend of the watch-end confirmation, visible beside the confirms it produced.
pub const COUNTER_SUBMIT_PENDING_CONFIRM_ASKED: &str = "submit_pending_confirm_asked_total";
/// bsv-low M19 round 2 (review H2): an Arcade `reorg_unmined` marker whose
/// spender's stored proof chaintracks still HOLDS: the hint changed nothing
/// (a planted marker, or a stale one). Non-zero on a healthy stream is a
/// courier or an attacker talking; either way visible.
pub const COUNTER_REORG_UNMINED_UNCORROBORATED: &str = "reorg_unmined_uncorroborated_total";
/// bsv-low M19 round 2 (review H3): a tip announce BELOW the highest held
/// header (an old header announced late, or two webhook tasks landing out
/// of order): ignored, never a reorg, counted.
pub const COUNTER_TIP_ANNOUNCE_OLD: &str = "tip_announce_old_total";
/// bsv-low M19 R2 round 3 (review L2): the OPERATOR's manual reorg demotion
/// (`POST /internal/reorg`), counted apart from `chain_reorgs_detected_total`
/// so "zero detected on a healthy stream" stays a true invariant while the
/// operator heals a past event by hand.
pub const COUNTER_OPERATOR_REORG: &str = "operator_reorg_total";
/// bsv-low M19 R2 round 3 (review MED-4): courier-confirmed rows the routine
/// sweep re-asked the ladder for and HEALED (stitched an agreeing proof into
/// the store); and the sweep's L1 re-anchors (a standing bump at a new height).
pub const COUNTER_REORG_HEALED_FROM_COURIER: &str = "reorg_healed_from_courier_total";
/// The routine sweep's re-anchors (a standing bump at a new height).
pub const COUNTER_REORG_REANCHORED: &str = "reorg_reanchored_total";
/// bsv-low M19 round 2 (review M4): the sweep's second and third legs, the
/// pots' own proofs and the engine's transactions (hops) whose stitched
/// bump chaintracks refutes: unlatched for the completion passes to
/// re-prove. Zero on a healthy stream (#429).
pub const COUNTER_REORG_STALE_POT_PROOFS: &str = "reorg_stale_pot_proofs_total";
/// Transactions (hops) whose stitched bump chaintracks refutes, unlatched to be re-proved.
pub const COUNTER_REORG_STALE_TX_PROOFS: &str = "reorg_stale_tx_proofs_total";
// ── bsv-low M19B-G1 (2026-09-08): the Arcade reorg-event consumer ─────────
/// Arcade orphan events the consumer FINISHED (applied in full, or skipped
/// as uncorroborated): the cursor moved past each one exactly once.
pub const COUNTER_ARCADE_REORG_EVENTS: &str = "arcade_reorg_events_total";
/// Confirmed rows whose anchor MOVED on an event without a demotion: the
/// ladder (Arcade first) served a chaintracks-verified proof for another
/// block and the stored bump + height were replaced in place.
pub const COUNTER_ARCADE_REORG_REANCHORED: &str = "arcade_reorg_reanchored_total";
/// Confirmed rows an event DEMOTED to SEEN: chaintracks refuted the stored
/// proof and no courier served a canonical one (the confirm arm re-judges).
pub const COUNTER_ARCADE_REORG_DEMOTED: &str = "arcade_reorg_demoted_total";
/// Arcade orphan events whose "orphaned" hash chaintracks holds as CANONICAL
/// deep below its tip: Arcade's row is wrong for our header source (the
/// 2026-09-07 965773 row); skipped, nothing re-verified on its account.
pub const COUNTER_ARCADE_REORG_UNCORROBORATED: &str = "arcade_reorg_uncorroborated_total";
/// Consumer passes STOPPED by a read fault (the Arcade feed, the header
/// source, the tracker inside a leg) or a storage fault, with the cursor
/// where it was. Non-zero and climbing = a source is down; the cursor on
/// `/health/invariants.arcadeReorg` says where it is stuck.
pub const COUNTER_ARCADE_REORG_FAULTS: &str = "arcade_reorg_faults_total";
/// Round 2 (review MED-3): events RELEASED unresolved from the head of the
/// queue (held or faulting past the ceiling, or by the operator's
/// `skipPending`): the cursor moved past them without an apply. Their rows
/// are the OPERATOR's (round 3, review MED): the routine sweep never walks
/// a height older than its window again, so each release is recorded on
/// `/health/invariants.arcadeReorg.unresolved` with the `/internal/reorg`
/// body that heals it.
pub const COUNTER_ARCADE_REORG_UNRESOLVED: &str = "arcade_reorg_unresolved_total";
/// Round 2 (review LOW-3): passes held because our header source was more
/// than the tolerance behind Arcade's own listing (not an event verdict).
pub const COUNTER_ARCADE_REORG_TRACKER_LAGGING: &str = "arcade_reorg_tracker_lagging_total";
/// Round 2 (review LOW-2): passes whose state write lost its compare-and-set
/// to another isolate (nothing of that pass's progress written).
pub const COUNTER_ARCADE_REORG_CONTENDED: &str = "arcade_reorg_contended_total";
/// Round 2 (review MED-2): spenders-leg pages stopped because the ladder's
/// budget was spent (resumed next pass; nothing judged blind).
pub const COUNTER_ARCADE_REORG_BUDGET_STOPS: &str = "arcade_reorg_budget_stops_total";
// The app-layer's `/beef` read-side guard writes three more rows into this
// table directly (it shares OVERLAY_DB): `beef_guard_stripped_total` (a
// refuted bump stripped, served raw with ancestry), `beef_guard_refused_total`
// (a refuted trimmed row answered 503) and `beef_guard_unchecked_total` (a
// recent bump served UNCHECKED because no tip or header was in hand: the
// stated fail-open, review L3). All surface on /health/invariants.
/// bsv-low (2026-09-04) — the missing-spend DISCOVERY pass (`proof_fetcher::
/// discover_missing_spends`), lifetime totals: candidates scanned, spends
/// discovered (an unconfirmed pointer written), courier-ladder faults, and
/// discoveries on NEW-era rows (admitted after the pointer heal shipped — a
/// growing value is the page: a live pot's re-present never reached us).
pub const COUNTER_MISSING_SPEND_SCANNED: &str = "missing_spend_scanned_total";
/// Missing spends discovered (an unconfirmed pointer written).
pub const COUNTER_MISSING_SPEND_DISCOVERED: &str = "missing_spend_discovered_total";
/// Courier-ladder faults of the discovery pass.
pub const COUNTER_MISSING_SPEND_FAULTS: &str = "missing_spend_faults_total";
/// Discoveries on new-era rows.
pub const COUNTER_MISSING_SPEND_DISCOVERED_NEW_ERA: &str = "missing_spend_discovered_new_era_total";
/// bsv-low #837 (the FIX-837 lens's MEDIUM-1): JOIN inputs the discovery pass's hop-spend write left past a JOIN's
/// bound (`JOIN_HOP_INPUTS_MAX`), and pot candidates whose JOIN its per-pass read budget (`JOIN_HOP_READS_PER_PASS`)
/// left unread: each is read by a later pass, never dropped. A rising value is a JOIN wider than an honest one.
pub const COUNTER_MISSING_SPEND_JOIN_HOP_INPUTS_DEFERRED: &str =
    "missing_spend_join_hop_inputs_deferred_total";
/// Pot candidates whose JOIN the per-pass budget left unread (above).
pub const COUNTER_MISSING_SPEND_JOIN_HOP_POTS_DEFERRED: &str =
    "missing_spend_join_hop_pots_deferred_total";
/// bsv-low (2026-09-04): per-courier-rung lifetime tallies —
/// `courier_<rung>_{ok,fault,skipped}_total` for every rung in
/// [`crate::proof_fetcher::COURIER_RUNGS`] ("skipped" = the rung's circuit
/// was open). The health surface folds them into `indexJanitor.couriers`.
pub fn courier_counter_name(rung: &str, kind: &str) -> String {
    format!("courier_{rung}_{kind}_total")
}

/// Default staleness budget for `/health/invariants?strict=1`: 6 hours. The
/// completion cron runs every 15 min (`wrangler.toml crons`), so 6h ≈ 24 dead
/// ticks — well inside the "surface in a day" bar, while tolerant of a couple
/// of skipped/slow ticks. Override with `OPS_INVARIANTS_MAX_STALE_MS`.
pub const DEFAULT_MAX_STALE_MS: i64 = 6 * 60 * 60 * 1000;

/// Proofless-ledger flag threshold: a tx proofless longer than this is flagged.
pub use overlay_engine::proof_window::PROOFLESS_FLAG_MS;

/// Per-tick cap on how many proofless txids we newly enrol into the watch
/// ledger from each store (keeps the write bounded; the ledger self-heals over
/// ticks and is GC'd as txs prove).
const WATCH_ENROLL_LIMIT: u32 = 500;

fn now_ms() -> i64 {
    js_sys::Date::now() as i64
}

// ── row shapes (D1 returns numeric columns as f64 — codebase convention) ────

#[derive(Deserialize)]
struct HeartbeatRow {
    last_tick_ms: f64,
    tick_count: f64,
}

#[derive(Deserialize)]
struct CounterRow {
    name: String,
    value: f64,
}

#[derive(Deserialize)]
struct CountRow {
    c: f64,
}

/// Record one live completion pass: upsert the heartbeat singleton and bump the
/// persistent counters. Best-effort — logs and swallows any D1 error.
pub async fn record_completion_tick(
    db: &D1Database,
    proofs_completed: u64,
    fetch_failed: u64,
    pot_beefs_compacted: u64,
    spends_confirmed: u64,
) {
    let ts = now_ms();

    // Heartbeat singleton (id = 0): stamp the wall-clock, increment the count.
    let hb = Query::new(
        "INSERT INTO ops_heartbeat (id, last_tick_ms, tick_count) VALUES (0, ?, 1) \
         ON CONFLICT(id) DO UPDATE SET \
             last_tick_ms = excluded.last_tick_ms, \
             tick_count = ops_heartbeat.tick_count + 1",
    )
    .bind(ts);
    if let Err(e) = hb.execute(db).await {
        worker::console_log!("[ops] heartbeat upsert failed: {e}");
    }

    record_completion_results(
        db,
        proofs_completed,
        fetch_failed,
        pot_beefs_compacted,
        spends_confirmed,
    )
    .await;
}

/// Add results from later maintenance without stamping a second tick heartbeat.
pub async fn record_completion_results(
    db: &D1Database,
    proofs_completed: u64,
    fetch_failed: u64,
    pot_beefs_compacted: u64,
    spends_confirmed: u64,
) {
    // Persistent monotonic counters (additive upsert).
    for (name, delta) in [
        (COUNTER_PROOFS_COMPLETED, proofs_completed),
        (COUNTER_FETCH_FAILED, fetch_failed),
        (COUNTER_POT_BEEFS_COMPACTED, pot_beefs_compacted),
        (COUNTER_SPENDS_CONFIRMED, spends_confirmed),
    ] {
        bump_counter(db, name, delta).await;
    }
}

/// bsv-low (2026-09-04): one discovery pass's totals → the lifetime counters.
pub async fn record_missing_spend_pass(
    db: &D1Database,
    scanned: u64,
    discovered: u64,
    faults: u64,
    discovered_new_era: u64,
    join_hop_deferred: (u64, u64),
) {
    for (name, delta) in [
        (COUNTER_MISSING_SPEND_SCANNED, scanned),
        (COUNTER_MISSING_SPEND_DISCOVERED, discovered),
        (COUNTER_MISSING_SPEND_FAULTS, faults),
        (COUNTER_MISSING_SPEND_DISCOVERED_NEW_ERA, discovered_new_era),
        (
            COUNTER_MISSING_SPEND_JOIN_HOP_INPUTS_DEFERRED,
            join_hop_deferred.0,
        ),
        (
            COUNTER_MISSING_SPEND_JOIN_HOP_POTS_DEFERRED,
            join_hop_deferred.1,
        ),
    ] {
        if delta > 0 {
            bump_counter(db, name, delta).await;
        }
    }
}

/// bsv-low (2026-09-04): one tick's courier tallies → the lifetime counters.
pub async fn record_courier_tallies(
    db: &D1Database,
    stats: &std::collections::BTreeMap<&'static str, crate::proof_fetcher::RungTally>,
) {
    for (rung, t) in stats {
        for (kind, delta) in [("ok", t.ok), ("fault", t.fault), ("skipped", t.skipped)] {
            if delta > 0 {
                bump_counter(db, &courier_counter_name(rung, kind), u64::from(delta)).await;
            }
        }
    }
}

/// PURE: the health surface's `couriers` view — `{rung: {ok, fault,
/// skipped}}` from the flat counters object (absent = 0).
pub fn couriers_view(counters: &serde_json::Value) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for rung in crate::proof_fetcher::COURIER_RUNGS {
        let get = |kind: &str| {
            counters
                .get(courier_counter_name(rung, kind))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        out.insert(
            rung.to_string(),
            json!({ "ok": get("ok"), "fault": get("fault"), "skipped": get("skipped") }),
        );
    }
    serde_json::Value::Object(out)
}

/// bsv-low #668 (the observability bar): a histogram quantile over `(upper_bound_ms, count)` buckets sorted
/// ascending (`u64::MAX` is the `+Inf` bucket). Returns the upper bound of the bucket the q-th sample falls in
/// (Prometheus-style). A quantile that lands in the `+Inf` bucket reports the largest FINITE bound as a lower
/// estimate (the real value is unbounded above it). Empty or zero-total reports 0.
pub fn push_latency_quantile(buckets: &[(u64, u64)], q: f64) -> u64 {
    let total: u64 = buckets.iter().map(|(_, c)| c).sum();
    if total == 0 {
        return 0;
    }
    let target = (q * total as f64).ceil().max(1.0) as u64;
    let mut cum = 0u64;
    for &(bound, c) in buckets {
        cum += c;
        if cum >= target {
            if bound == u64::MAX {
                return buckets
                    .iter()
                    .rev()
                    .map(|(b, _)| *b)
                    .find(|b| *b != u64::MAX)
                    .unwrap_or(0);
            }
            return bound;
        }
    }
    0
}

/// bsv-low #668 (step 0 / the observability bar): the pots-room push counters and the push-latency p50/p95
/// derived from the `pots_push_latency_ms_le_<bound>_total` bucket counters the app layer emits. The bucket
/// bounds are read FROM the counter names (the overlay never needs to know the app layer's bucket list), so the
/// two crates stay decoupled. `latencyMsP50`/`_P95` are app-layer-measured (the hook's answer to the relay's 2xx).
pub fn pots_push_view(counters: &serde_json::Value) -> serde_json::Value {
    let get = |k: &str| {
        counters
            .get(k)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    let mut buckets: Vec<(u64, u64)> = Vec::new();
    if let Some(obj) = counters.as_object() {
        for (name, val) in obj {
            if let Some(rest) = name
                .strip_prefix("pots_push_latency_ms_le_")
                .and_then(|r| r.strip_suffix("_total"))
            {
                let bound = if rest == "inf" {
                    u64::MAX
                } else {
                    match rest.parse::<u64>() {
                        Ok(n) => n,
                        Err(_) => continue,
                    }
                };
                buckets.push((bound, val.as_u64().unwrap_or(0)));
            }
        }
    }
    buckets.sort_by_key(|(b, _)| *b);
    let samples: u64 = buckets.iter().map(|(_, c)| c).sum();
    json!({
        "sentTotal": get("pots_push_sent_total"),
        "walledTotal": get("pots_push_walled_total"),
        "redrivenTotal": get("pots_push_redriven_total"),
        "latencyMsP50": push_latency_quantile(&buckets, 0.50),
        "latencyMsP95": push_latency_quantile(&buckets, 0.95),
        "latencySamples": samples,
    })
}

#[derive(serde::Deserialize)]
struct KindCountRow {
    #[serde(rename = "lockKind")]
    lock_kind: Option<String>,
    c: f64,
}

/// The class a `pot_records.lockKind` is served under by [`index_janitor_backlog`] (`covenant`, `p2pkh`, every
/// other value and NULL `other`), as SQL over `{col}`: the one spelling the count's triggers, its backfill and the
/// read share.
macro_rules! lock_class {
    ($col:literal) => {
        concat!(
            "CASE ",
            $col,
            " WHEN 'covenant' THEN 'covenant' WHEN 'p2pkh' THEN 'p2pkh' ELSE 'other' END"
        )
    };
}
pub(crate) use lock_class;

/// NS-COST-BAR-1: the index janitor's backlog was a `GROUP BY lockKind` over every unspent `pot_records` row on
/// every `/health/invariants` read (14,313 rows a read on beta, 5.2 M a day). The backlog is the unspent rows
/// minus the YOUNG ones (`createdAt` in the last hour, the exact complement of `createdAt IS NULL OR createdAt <=
/// unixepoch() - 3600` among unspent rows): the unspent rows per class are one row each in
/// `count_pot_unspent_kind`, kept by `pot_records`' insert, update and delete triggers (no writer of `pot_records`
/// uses `INSERT OR REPLACE`, #638's rule, so a +1/-1 count is exact), and the young ones an index range over the
/// last hour (`idx_pot_unspent_created`, partial on `spent = 0`, named by `INDEXED BY`: without statistics the
/// planner prefers the `spent = ?` equality of `idx_pot_spent_unconfirmed`, every unspent pot). Three rows, one
/// per class. Before the migrations both the table and the index are absent: the read errs and serves `-1`.
pub const INDEX_JANITOR_BACKLOG_SQL: &str = concat!(
    "SELECT k.kind AS lockKind, k.n - COALESCE(y.c, 0) AS c FROM count_pot_unspent_kind k LEFT JOIN (SELECT ",
    lock_class!("lockKind"),
    " AS kind, COUNT(*) AS c FROM pot_records INDEXED BY idx_pot_unspent_created WHERE spent = 0 AND createdAt > unixepoch() - 3600 GROUP BY 1) y ON y.kind = k.kind"
);

/// NS-COST-BAR-1: the class counts' one-time backfill, behind the table's rows in the OUTER derived table.
pub const UNSPENT_KIND_BACKFILL: &str = concat!(
    "INSERT OR IGNORE INTO count_pot_unspent_kind (kind, n) SELECT k.kind, (SELECT COUNT(*) FROM pot_records p WHERE p.spent = 0 AND ",
    lock_class!("p.lockKind"),
    " = k.kind) FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM count_pot_unspent_kind)) g CROSS JOIN (SELECT 'covenant' AS kind UNION ALL SELECT 'p2pkh' UNION ALL SELECT 'other') k"
);

/// bsv-low (2026-09-04): the index janitor's live backlog, by row kind —
/// `spent = 0` rows at least an hour old (the discovery pass's own candidate
/// rule). `covenant` = real pots (the money), `p2pkh` = `tm_lowfund` hop /
/// change rows (indexed for the hop's landing proof, never a pot), `other` =
/// bare / undecoded. -1 = table unreadable (a pre-migration isolate).
pub async fn index_janitor_backlog(db: &D1Database) -> serde_json::Value {
    let rows: Result<Vec<KindCountRow>, _> =
        Query::new(INDEX_JANITOR_BACKLOG_SQL).fetch_all(db).await;
    match rows {
        Ok(rows) => {
            let (mut covenant, mut p2pkh, mut other) = (0u64, 0u64, 0u64);
            for r in rows {
                let n = r.c.max(0.0) as u64;
                match r.lock_kind.as_deref() {
                    Some("covenant") => covenant += n,
                    Some("p2pkh") => p2pkh += n,
                    _ => other += n,
                }
            }
            json!({ "staleCovenant": covenant, "staleP2pkh": p2pkh, "staleOther": other,
                    "note": "spent = 0 rows ≥ 1 h old; the discovery pass drains covenant (pot) rows first, ≤ 20 per 15-min tick; staleCovenant is the number that matters" })
        }
        Err(_) => json!({ "staleCovenant": -1, "staleP2pkh": -1, "staleOther": -1 }),
    }
}

/// bsv-low M19B-G1: the consumer's state row for `/health/invariants`.
/// `{readable, everRan, cursor, pending, updatedAtMs}`: the cursor is the
/// last Arcade orphan event finished (`{orphanedAt, height, hash}`), the
/// pending event the one in progress with its `heldPasses`.
pub async fn arcade_reorg_view(db: &D1Database) -> serde_json::Value {
    #[derive(serde::Deserialize)]
    struct Row {
        state: String,
        #[serde(default)]
        version: Option<f64>,
        #[serde(rename = "updatedAt", default)]
        updated_at: Option<f64>,
    }
    let row: Result<Option<Row>, _> =
        Query::new(crate::d1_discovery::arcade_reorg_state_view_sql())
            .bind(crate::arcade_reorg::ARCADE_STATE_NAME)
            .fetch_optional(db)
            .await;
    match row {
        Err(_) => json!({ "readable": false, "everRan": false }),
        Ok(None) => json!({ "readable": true, "everRan": false, "cursor": null, "pending": null }),
        Ok(Some(r)) => {
            match overlay_discovery::pot::arcade_events::ConsumerState::from_json(&r.state) {
                Ok(state) => arcade_reorg_state_json(
                    &state,
                    r.updated_at.map(|v| v.max(0.0) as i64),
                    r.version.map(|v| v.max(0.0) as u64),
                ),
                Err(e) => json!({ "readable": false, "everRan": true, "error": e }),
            }
        }
    }
}

/// PURE (bsv-low #484): the reorg view carries how many CONFIRMED hop probe memos the reorg passes have cleared
/// (`hop_probe_memos`; the lifetime counter, 0 until the first), beside the cursor that says where the judge is.
pub fn with_probe_memos_cleared(
    mut view: serde_json::Value,
    counters: &serde_json::Value,
) -> serde_json::Value {
    if view.is_object() {
        view["probeMemosCleared"] = json!(counters
            .get(crate::hop_probe_memos::COUNTER_HOP_PROBE_MEMOS_CLEARED)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0));
    }
    view
}

/// PURE (round 3, review MED): the released events on record, each with the
/// `/internal/reorg` body that heals its height, newest last.
pub fn unresolved_json(
    unresolved: &[overlay_discovery::pot::arcade_events::UnresolvedEvent],
) -> serde_json::Value {
    let why = |w: overlay_discovery::pot::arcade_events::ReleaseReason| match w {
        overlay_discovery::pot::arcade_events::ReleaseReason::HeldCeiling => "held-ceiling",
        overlay_discovery::pot::arcade_events::ReleaseReason::FaultCeiling => "fault-ceiling",
        overlay_discovery::pot::arcade_events::ReleaseReason::Operator => "operator",
    };
    serde_json::Value::Array(
        unresolved
            .iter()
            .map(|u| {
                json!({
                    "height": u.key.height,
                    "hash": u.key.hash,
                    "orphanedAt": u.key.orphaned_at,
                    "why": why(u.why),
                    "heal": format!("POST /internal/reorg {{\"fromHeight\": {}, \"toHeight\": {}}}", u.key.height, u.key.height),
                })
            })
            .collect(),
    )
}

/// PURE: the health surface's shape of a consumer state document.
pub fn arcade_reorg_state_json(
    state: &overlay_discovery::pot::arcade_events::ConsumerState,
    updated_at_ms: Option<i64>,
    version: Option<u64>,
) -> serde_json::Value {
    let key = |k: &overlay_discovery::pot::arcade_events::EventKey| json!({ "orphanedAt": k.orphaned_at, "height": k.height, "hash": k.hash });
    json!({
        "readable": true,
        "everRan": true,
        "cursor": state.cursor.as_ref().map(key),
        "pending": state.pending.as_ref().map(|p| json!({
            "event": key(&p.event.key()),
            "heldPasses": p.held_passes,
            "faultPasses": p.fault_passes,
            "spendersExhausted": p.spenders.exhausted,
            "potBeefsExhausted": p.pot_beefs.exhausted,
            "transactionsExhausted": p.transactions.exhausted,
        })),
        // round 3 (review MED): the released events, the operator's heals
        "unresolved": unresolved_json(&state.unresolved),
        "version": version,
        "updatedAtMs": updated_at_ms,
    })
}

/// Bump one persistent monotonic counter by `delta` (additive upsert).
/// Best-effort — logs and swallows any D1 error (observability must never
/// break the request that carries it).
/// bsv-low #371: the shipped `network_seen` latch write — the overlay's OWN
/// witness that the network accepted `txid`. Write-once (`INSERT OR IGNORE`;
/// a seen fact never changes), keyed on the lowercased txid, stamped by
/// SQLite (`unixepoch()`) so no clock bind is needed. A const so the
/// real-SQLite tier executes the production string against the production
/// schema.
///
/// bsv-low #621 (L5): the latch also names WHO said SEEN and WHEN (`seenSource`, `sourceAtMs`): the tracker's `seen` is a
/// hint written by a named broadcaster at a time, never evidence. `sourceAtMs` is the broadcaster's own stamp in ms
/// when its answer carries one (Arcade's `GET /tx` `timestamp`), else the overlay's clock in ms at the answer. The
/// first writer's pair stands with its row (`INSERT OR IGNORE`). A row latched before the migration carries
/// neither (served `source: null`, its time `seenAt`).
pub const NETWORK_SEEN_INSERT_SQL: &str =
    "INSERT OR IGNORE INTO network_seen (txid, seenAt, seenSource, sourceAtMs) VALUES (lower(?1), unixepoch(), ?2, ?3)";

/// bsv-low #621 (L5): the one writer class of `network_seen`: a BROADCASTER whose answer said SEEN or better for a
/// transaction. A courier's mempool presence is the tracker's `announced` (#602's ruling, the words map's S11), never
/// `seen`, so there is no courier variant and no caller can latch one: every latch call names one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeenSource {
    /// Arcade: its live `GET /tx` at SEEN or better, its stored echo or pre-flight probe in the witnessed mode, or
    /// the gated ladder's poll.
    Arcade,
    /// TAAL's ARC as the #214 corroborator or the #413 dual push.
    Taal,
    /// GorillaPool's ARC as the #214 corroborator or the #413 dual push.
    GorillaPool,
}

impl SeenSource {
    /// The served name (`networkSeenBy.source`, the tracker hint's source).
    pub const fn wire(self) -> &'static str {
        match self {
            SeenSource::Arcade => "arcade",
            SeenSource::Taal => "taal",
            SeenSource::GorillaPool => "gorillapool",
        }
    }
}

/// The `/health/invariants.networkSeenTotal` read (bsv-low #371): the one row the table's triggers keep equal to
/// its `COUNT(*)` (bsv-low #645), never a count of the table. No row (an isolate before the migrations) is the
/// served `-1`, as an unreadable table was.
pub const NETWORK_SEEN_TOTAL_SQL: &str = "SELECT n AS c FROM count_network_seen WHERE id = 1";

/// bsv-low #645: the count's one-time backfill. Its guard is the row itself, in the OUTER derived table, so a
/// re-run of the list never counts the table (#638's rule).
pub const NETWORK_SEEN_COUNT_BACKFILL: &str = "INSERT OR IGNORE INTO count_network_seen (id, n) SELECT 1, c.n FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM count_network_seen WHERE id = 1)) g CROSS JOIN (SELECT COUNT(*) AS n FROM network_seen) c";

/// NS-COST-BAR-1: `/health/invariants.retiredUnprovableTotal` (#2b) was a `COUNT(*)` of `pot_beefs` on every read
/// (20,837 rows a read on beta, 10.4 M a day). It reads one row, kept equal to `COUNT(*) FROM pot_beefs WHERE
/// structurally_unprovable = 1` by triggers inside every write. `pot_beefs` is written by `INSERT OR REPLACE`
/// (`d1_discovery`), whose implicit delete fires no delete trigger unless `recursive_triggers` is on, so a +1/-1
/// counter on `pot_beefs` itself could drift: the triggers keep a MEMBERSHIP set
/// (`pot_beefs_unprovable_set`, the txids of the latched rows), written idempotently on every insert, update and
/// delete (delete the key, then insert it if the row is latched), and the count is kept by the SET's own insert
/// and delete triggers, which no writer but those triggers touches. A row whose txid is NULL is not counted (no
/// writer writes one). No row (an isolate before the migrations) is the served `-1`, as an unreadable table was.
pub const RETIRED_UNPROVABLE_TOTAL_SQL: &str =
    "SELECT n AS c FROM count_pot_beefs_unprovable WHERE id = 1";

/// NS-COST-BAR-1: the set's one-time backfill, behind the count's row in the OUTER derived table (#638's rule): a
/// re-run of the list never reads `pot_beefs`. Runs after the triggers, so a write landing before it is in the set.
pub const UNPROVABLE_SET_BACKFILL: &str = "INSERT OR IGNORE INTO pot_beefs_unprovable_set (txid) SELECT b.txid FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM count_pot_beefs_unprovable WHERE id = 1)) g CROSS JOIN pot_beefs b WHERE b.structurally_unprovable = 1 AND b.txid IS NOT NULL";

/// NS-COST-BAR-1: the count's one-time backfill, the set counted once behind its own row.
pub const UNPROVABLE_COUNT_BACKFILL: &str = "INSERT OR IGNORE INTO count_pot_beefs_unprovable (id, n) SELECT 1, c.n FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM count_pot_beefs_unprovable WHERE id = 1)) g CROSS JOIN (SELECT COUNT(*) AS n FROM pot_beefs_unprovable_set) c";

/// NS-COST-BAR-1: `/health/invariants.txRetiredTotal` (INCIDENT D1-CALLBACK-FLOOD 2026-09-01) was a `COUNT(*)` of
/// `transactions` on every read (21,724 rows a read on beta, 7.5 M a day). It reads one row, kept equal to `COUNT(*)
/// FROM transactions WHERE retired_ms IS NOT NULL` the way [`RETIRED_UNPROVABLE_TOTAL_SQL`] is (`transactions` is
/// written by `INSERT OR REPLACE` too, `d1_storage`): a membership set (`transactions_retired_set`, each retired
/// row's txid and whether its reason is `expired-unproven`) kept idempotently by `transactions`' triggers, the
/// count row by the set's own. A row whose txid is NULL is not counted (no writer writes one).
pub const TX_RETIRED_TOTAL_SQL: &str =
    "SELECT retired AS c FROM count_transactions_retired WHERE id = 1";

/// NS-COST-BAR-1: the retired set's one-time backfill, behind the count's row in the OUTER derived table.
pub const RETIRED_SET_BACKFILL: &str = "INSERT OR IGNORE INTO transactions_retired_set (txid, expired) SELECT t.txid, t.retired_reason IS 'expired-unproven' FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM count_transactions_retired WHERE id = 1)) g CROSS JOIN transactions t WHERE t.retired_ms IS NOT NULL AND t.txid IS NOT NULL";

/// NS-COST-BAR-1: the retired count's one-time backfill, the set counted once behind its own row.
pub const RETIRED_COUNT_BACKFILL: &str = "INSERT OR IGNORE INTO count_transactions_retired (id, retired, expired) SELECT 1, c.n, c.e FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM count_transactions_retired WHERE id = 1)) g CROSS JOIN (SELECT COUNT(*) AS n, COALESCE(SUM(expired), 0) AS e FROM transactions_retired_set) c";

/// Every trigger-kept count's stamped backfill, in the list's order (bsv-low #645, NS-COST-BAR-1): what a database
/// loaded from a schema dump (no count row) runs after its rows, as the budget tier's seed does.
pub const COUNT_ROW_BACKFILLS: &[&str] = &[
    NETWORK_SEEN_COUNT_BACKFILL,
    UNPROVABLE_SET_BACKFILL,
    UNPROVABLE_COUNT_BACKFILL,
    RETIRED_SET_BACKFILL,
    RETIRED_COUNT_BACKFILL,
    UNSPENT_KIND_BACKFILL,
];

/// bsv-low #822: is `txid`'s `network_seen` latched? One primary-key read (the latch writes `lower(?)`).
pub const NETWORK_SEEN_LATCHED_SQL: &str =
    "SELECT 1 AS one FROM network_seen WHERE txid = lower(?1)";

#[derive(serde::Deserialize)]
struct LatchedRow {
    #[allow(dead_code)]
    one: i64,
}

/// bsv-low #822: [`NETWORK_SEEN_LATCHED_SQL`] for `txid`: `Ok(true)` latched, `Ok(false)` not, `Err` the read
/// faulted (the caller's to name; never a silent "unlatched").
pub async fn network_seen_latched(db: &D1Database, txid: &str) -> Result<bool, String> {
    Query::new(NETWORK_SEEN_LATCHED_SQL)
        .bind(txid)
        .fetch_optional::<LatchedRow>(db)
        .await
        .map(|row| row.is_some())
        .map_err(|e| e.to_string())
}

/// Latch [`NETWORK_SEEN_INSERT_SQL`] for `txid`. Best-effort: the latch only
/// ACCELERATES verdict publication (the #371 third arm); a lost write leaves
/// the row behind the merkle bar, which is the pre-#371 behaviour — so a D1
/// fault is logged, never propagated.
///
/// bsv-low #621 (L5): `source` names the broadcaster that said SEEN and `at_ms` its own stamp when its answer
/// carried one (`None`: the overlay's clock now). The tracker's `seen` hint carries the same pair.
pub async fn latch_network_seen(db: &D1Database, txid: &str, source: SeenSource, at_ms: Option<i64>) {
    let at_ms = at_ms.unwrap_or_else(|| worker::Date::now().as_millis() as i64);
    let q = Query::new(NETWORK_SEEN_INSERT_SQL)
        .bind(txid)
        .bind(source.wire())
        .bind(at_ms);
    if let Err(e) = q.execute(db).await {
        worker::console_log!("[#371] network_seen latch failed for {txid}: {e}");
        return;
    }
    crate::tracker_events::note(
        txid,
        "seen",
        Some(serde_json::json!({"source": source.wire(), "at": at_ms})),
    );
    // admit-fast step 4: the witness is a pot fact the felt renders
    // ("awaiting the network" → "seen"), so every pot row the txid FUNDS is
    // NOTED for the pots-room push (`ls_pot spentStatus.networkSeen`), and so
    // is every pot row it SPENDS (bsv-low #660): a settle, refund or sweep
    // funds no pot, but this latch publishes the spent pot's verdict (the
    // app layer's #371 third arm, `spender_seen AND spender_final`), so its
    // note is what makes `/internal/pot-changed` recompute the winner's owed
    // row after the witness. The caller ships the notes: the route's
    // end-of-request flush for an in-request latch, `pot_changes::flush_inline`
    // from a background job. A txid that funds and spends no pot notes nothing.
    // Best effort: a faulted read notes nothing (the latch above stays
    // committed) and logs one line, so the operator sees why no note followed.
    let funded: Vec<PotVoutRow> = match Query::new(NETWORK_SEEN_POT_ROWS_SQL)
        .bind(txid)
        .fetch_all(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            worker::console_log!("[#660] funded-pot read failed for {txid}: {e}");
            Vec::new()
        }
    };
    let spent: Vec<PotOutpointRow> = match Query::new(NETWORK_SEEN_SPENT_POT_ROWS_SQL)
        .bind(txid)
        .fetch_all(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            worker::console_log!("[#660] spent-pot read failed for {txid}: {e}");
            Vec::new()
        }
    };
    note_witnessed_pots(txid, &funded, &spent);
}

/// The notes of one latch: each pot `txid` funds and each pot it spends.
fn note_witnessed_pots(txid: &str, funded: &[PotVoutRow], spent: &[PotOutpointRow]) {
    for row in funded {
        crate::pot_changes::note(txid, row.output_index);
    }
    for row in spent {
        crate::pot_changes::note(&row.txid, row.output_index);
    }
}

/// The pot rows a witnessed txid funds (admit-fast step 4; pinned).
pub const NETWORK_SEEN_POT_ROWS_SQL: &str =
    "SELECT outputIndex FROM pot_records WHERE txid = lower(?)";

/// The pot rows a witnessed txid spends (bsv-low #660; one read by
/// `idx_pot_spending`, pinned).
pub const NETWORK_SEEN_SPENT_POT_ROWS_SQL: &str =
    "SELECT txid, outputIndex FROM pot_records WHERE spendingTxid = lower(?)";

#[derive(Deserialize)]
struct PotVoutRow {
    #[serde(rename = "outputIndex")]
    output_index: u32,
}

#[derive(Deserialize)]
struct PotOutpointRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: u32,
}

/// The one bump statement (factored so the real-SQLite tier runs it as shipped).
pub const BUMP_COUNTER_SQL: &str = "INSERT INTO ops_counters (name, value) VALUES (?, ?) \
     ON CONFLICT(name) DO UPDATE SET value = ops_counters.value + excluded.value";

/// Flush one request or pass as one D1 batch. Failure loses telemetry only.
pub(crate) async fn bump_counters(
    db: &D1Database,
    deltas: std::collections::BTreeMap<String, u64>,
) {
    let statements = deltas
        .into_iter()
        .filter(|(_, delta)| *delta > 0)
        .map(|(name, delta)| Query::new(BUMP_COUNTER_SQL).bind(name).bind(delta).prepare(db))
        .collect::<Result<Vec<_>, _>>();
    match statements {
        Ok(statements) if statements.is_empty() => {}
        Ok(statements) => {
            if let Err(e) = crate::d1_ledger::counted_batch(db, statements).await {
                worker::console_log!("[ops] counter batch failed: {e}");
            }
        }
        Err(e) => worker::console_log!("[ops] counter batch could not be prepared: {e}"),
    }
}

/// NS-COST-BAR-1 (the owner's cost word of 2026-10-18): the pass families whose D1 rows read are counted durably,
/// each as `d1_rows_read_<family>_total` (D1's own `meta.rows_read`, summed by `d1_ledger`): the maintenance tick's
/// steps, labelled in `tick.rs` (`setup` is the engine's build and everything before the first label), and
/// `/health/invariants`, the fleet's most-polled read. A closed set: the tick's labels are pinned to it.
pub const D1_ROWS_READ_FAMILIES: &[&str] = &[
    "setup",
    "proof_window",
    "tx_completion",
    "ad_sync",
    "beef_blob_sweep",
    "gasp_deferred_sweep",
    "ef_deferred",
    "change_park",
    "gasp_sync",
    "peer_crawl",
    "arcade_reorg",
    "pot_maintenance",
    "missing_spends",
    "backfills",
    "relatch",
    "retire",
    "rebroadcast",
    "advert_lifecycle",
    "proofless_watch",
    "janitor",
    "health_invariants",
];

/// The durable counter of one family's D1 rows read (NS-COST-BAR-1).
pub fn d1_rows_read_counter(family: &str) -> String {
    format!("d1_rows_read_{family}_total")
}

/// PURE (NS-COST-BAR-1): the counter deltas of one tick's per-pass tallies. A label outside
/// [`D1_ROWS_READ_FAMILIES`] is folded into `setup` (it cannot happen: the labels are pinned), so no name is minted.
pub fn d1_rows_read_deltas(
    by_pass: &std::collections::BTreeMap<&'static str, crate::d1_ledger::Tally>,
) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    for (pass, t) in by_pass {
        let family = if D1_ROWS_READ_FAMILIES.contains(pass) {
            pass
        } else {
            "setup"
        };
        *out.entry(d1_rows_read_counter(family)).or_insert(0) += t.reads;
    }
    out
}

/// NS-COST-BAR-1: add one tick's rows read by pass to the durable counters, one batch. Best-effort (telemetry).
pub async fn record_d1_rows_read(
    db: &D1Database,
    by_pass: &std::collections::BTreeMap<&'static str, crate::d1_ledger::Tally>,
) {
    bump_counters(db, d1_rows_read_deltas(by_pass)).await;
}

/// Add `delta` to the counter `name`; a fault is logged and swallowed.
pub async fn bump_counter(db: &D1Database, name: &str, delta: u64) {
    if delta == 0 {
        return;
    }
    let q = Query::new(BUMP_COUNTER_SQL).bind(name).bind(delta);
    if let Err(e) = q.execute(db).await {
        worker::console_log!("[ops] counter {name} bump failed: {e}");
    }
}

/// INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — how many status-ignored callbacks
/// accumulate per isolate before one billed counter write flushes them.
///
/// The per-callback `bump_counter` was the billing site: every webhook did one
/// D1 UPSERT on the same `ops_counters` row (~370/s sustained ≈ $28/day of
/// rows-written for a CHATTER counter whose whole job is "the stream is
/// alive"). Batching trades exactness for cost: up to `BATCH-1` events per
/// isolate are uncounted at eviction, which the counter's consumers
/// (`arc_ingest_push_health`, an operator eyeballing deltas) never needed.
/// The first [`STATUS_IGNORED_EXACT_HEAD`] events per isolate still flush
/// IMMEDIATELY — the route tier pins "an authorized callback visibly counts"
/// (Rule 13: accepted and silently-dropped must never look alike), and at
/// trickle volume (the normal state) exactness is what an operator actually
/// reads. Batching engages only once one isolate has seen flood-scale
/// volume, which is precisely when per-event exactness stops meaning
/// anything and starts costing $28/day. The 401 counters and
/// `arc_ingest_pushed_total` stay EXACT — they are rare and diagnostic.
pub const STATUS_IGNORED_FLUSH_BATCH: u32 = 256;
/// Events per isolate counted exactly (one write each) before batching.
pub const STATUS_IGNORED_EXACT_HEAD: u32 = 8;

thread_local! {
    /// Workers isolates are single-threaded. `(seen, pending)`: `seen` =
    /// lifetime events in this isolate (saturating), `pending` = events not
    /// yet flushed.
    static STATUS_IGNORED_STATE: std::cell::Cell<(u32, u32)> =
        const { std::cell::Cell::new((0, 0)) };
}

/// PURE fold for one ignored-status event: `(seen, pending)` before →
/// `((seen, pending) after, rows_to_flush_now)`. The first
/// [`STATUS_IGNORED_EXACT_HEAD`] events each flush 1 (exact); past the head
/// every [`STATUS_IGNORED_FLUSH_BATCH`]th accumulated event flushes the
/// accumulation.
pub fn status_ignored_step(state: (u32, u32)) -> ((u32, u32), u64) {
    let (seen, pending) = state;
    let seen = seen.saturating_add(1);
    if seen <= STATUS_IGNORED_EXACT_HEAD {
        return ((seen, pending), 1);
    }
    let p = pending + 1;
    if p >= STATUS_IGNORED_FLUSH_BATCH {
        ((seen, 0), u64::from(p))
    } else {
        ((seen, p), 0)
    }
}

/// Count one ignored status callback (see [`STATUS_IGNORED_FLUSH_BATCH`]):
/// exact for the first [`STATUS_IGNORED_EXACT_HEAD`] events per isolate,
/// batched past that.
pub async fn bump_status_ignored_batched(db: &D1Database) {
    let flush = STATUS_IGNORED_STATE.with(|c| {
        let (next, flush) = status_ignored_step(c.get());
        c.set(next);
        flush
    });
    if flush > 0 {
        bump_counter(db, COUNTER_ARC_INGEST_STATUS_IGNORED, flush).await;
    }
}

/// INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — record a courier-delivered TERMINAL
/// broadcast verdict (REJECTED / DOUBLE_SPEND_ATTEMPTED) as evidence.
///
/// Write-once per txid (INSERT OR IGNORE: the first verdict wins, and the
/// ~7/s repeat deliveries for a wedged txid write ZERO rows — an ignored
/// insert is not a billed row write). This is EVIDENCE for the retire
/// classifier, never a verdict by itself: #214 stands — an async REJECTED is
/// authoritative only once corroborated (both-indexer definitive absence,
/// `proof_fetcher::retire_verdict`).
pub async fn record_arc_terminal(db: &D1Database, txid: &str, status: &str, extra: Option<&str>) {
    let q = Query::new(
        "INSERT OR IGNORE INTO arc_terminal (txid, status, extra, first_ms) VALUES (?, ?, ?, ?)",
    )
    .bind(txid)
    .bind(status)
    .bind(extra.unwrap_or(""))
    .bind(now_ms());
    if let Err(e) = q.execute(db).await {
        worker::console_log!("[ops] arc_terminal record failed for {txid}: {e}");
    }
}

/// Refresh the proofless first-seen ledger and return the count of txids
/// flagged (proofless > 24h). Best-effort throughout.
///
/// Each tick: (1) enrol currently-proofless txids from both stores with a
/// first-seen stamp (`INSERT OR IGNORE`, so an existing first-seen is never
/// overwritten — the age is real), (2) GC txids that have since proven, (3)
/// count those older than the flag threshold.
pub async fn refresh_proofless_watch(db: &D1Database) -> u64 {
    let ts = now_ms();

    // 1. Enrol proofless txids from both stores (bounded). First-seen is only
    //    set on the FIRST sighting (INSERT OR IGNORE keeps the original stamp).
    for table in ["pot_beefs", "transactions"] {
        let sql = proofless_watch_enrol_sql(table);
        if let Err(e) = Query::new(sql).bind(ts).execute(db).await {
            worker::console_log!("[ops] proofless_watch enrol ({table}) failed: {e}");
        }
    }

    // 2. GC: drop any txid that has since proven in either store — or was
    //    RETIRED structurally unprovable (#2b: residue must leave the watch,
    //    or `prooflessOver24h` keeps conflating it with genuine backlog).
    if let Err(e) = Query::new(PROOFLESS_WATCH_GC_SQL).execute(db).await {
        worker::console_log!("[ops] proofless_watch GC failed: {e}");
    }

    // 3. Flag: count txids proofless longer than the threshold.
    let cutoff = ts - PROOFLESS_FLAG_MS;
    let flagged = count_flagged(db, cutoff).await;
    if flagged > 0 {
        worker::console_log!(
            "[ops] proofless_watch: {flagged} tx(s) proofless > 24h (proof not landing)"
        );
    }
    flagged
}

/// SHIPPED proofless-watch enrol page for one store (factored so the
/// real-SQLite cell executes the production strings).
///
/// ORDER BY RANDOM(): the enrol page is bounded, so with a >500 backlog a
/// fixed (insertion) order would keep sampling the same head every tick
/// and UNDERCOUNT the dead-pass signal (never-mineable rows deeper in the
/// backlog would go unwatched). Random sampling makes every proofless row
/// eventually visible to the flag. (INSERT OR IGNORE still preserves each
/// row's original first-seen stamp, so the age stays real.)
///
/// #2b: the `pot_beefs` page excludes LATCHED structurally-unprovable rows
/// (`IS NOT 1` — NULL/pre-migration rows still enrol, fail-safe toward
/// watching MORE): a retired row is RESIDUE, not backlog, and enrolling it
/// would keep `prooflessOver24h` unable to tell the two apart. The
/// `transactions` page excludes `retired_ms`-latched rows for the same
/// reason (INCIDENT D1-CALLBACK-FLOOD 2026-09-01: that store's retirement
/// twin — corroborated network-dead, e.g. a UTXO_SPENT double-spend).
pub(crate) fn proofless_watch_enrol_sql(table: &str) -> String {
    if table == "transactions" {
        return format!("INSERT OR IGNORE INTO proofless_watch (txid, first_seen_ms) SELECT txid, COALESCE(created_at * 1000, ?) FROM transactions WHERE has_proof = 0 AND retired_ms IS NULL ORDER BY RANDOM() LIMIT {WATCH_ENROLL_LIMIT}");
    }
    let unprovable_clause = if table == "pot_beefs" {
        " AND structurally_unprovable IS NOT 1"
    } else {
        " AND retired_ms IS NULL"
    };
    format!(
        "INSERT OR IGNORE INTO proofless_watch (txid, first_seen_ms) \
         SELECT txid, ? FROM {table} WHERE has_proof = 0{unprovable_clause} \
         ORDER BY RANDOM() LIMIT {WATCH_ENROLL_LIMIT}"
    )
}

/// SHIPPED proofless-watch GC: drop txids that have since PROVEN in either
/// store, or were RETIRED (structurally unprovable in `pot_beefs` — #2b —
/// or corroborated network-dead in `transactions` — INCIDENT
/// D1-CALLBACK-FLOOD 2026-09-01), or are ORPHANS — rows with NO backing row
/// in EITHER store. The orphan arm closes the incident's zombie class: 9 of
/// beta's 27 flagged rows pointed at store rows that displacement/cleanup
/// had deleted, and the old GC (proven-only) could never release them, so
/// `prooflessOver24h` counted ghosts forever. All four arms are "no longer
/// genuine backlog", which is the only thing the 24h flag may count.
///
/// NS-COST-BAR-1 (bsv-low, the owner's cost word of 2026-10-18): the four store arms are primary-key probes of
/// each WATCHED txid (`EXISTS ... WHERE b.txid = proofless_watch.txid`), never a list built over the whole store:
/// the `txid IN (SELECT txid FROM pot_beefs WHERE has_proof = 1)` shape materialised all four filtered lists every
/// tick (84,852 rows a run on beta, 30.8 M a day). Same rows deleted: inside the `OR` chain a list's NULL is a
/// false, and an `EXISTS` over the key is true exactly when the `IN` was. The orphan arm already probes the key.
pub const PROOFLESS_WATCH_GC_SQL: &str = "DELETE FROM proofless_watch WHERE \
     EXISTS (SELECT 1 FROM pot_beefs b WHERE b.txid = proofless_watch.txid \
             AND (b.has_proof = 1 OR b.structurally_unprovable = 1)) OR \
     EXISTS (SELECT 1 FROM transactions t WHERE t.txid = proofless_watch.txid \
             AND (t.has_proof = 1 OR t.retired_ms IS NOT NULL)) OR \
     (txid NOT IN (SELECT txid FROM pot_beefs) AND \
      txid NOT IN (SELECT txid FROM transactions))";

/// Count proofless_watch rows first seen before `cutoff_ms`.
async fn count_flagged(db: &D1Database, cutoff_ms: i64) -> u64 {
    let row: Option<CountRow> =
        Query::new("SELECT COUNT(*) AS c FROM proofless_watch WHERE first_seen_ms < ?")
            .bind(cutoff_ms)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    row.map(|r| r.c.max(0.0) as u64).unwrap_or(0)
}

/// The three change webhooks' flush accounts (bsv-low #436): each set's five counters are seeded 0.
pub const CHANGE_FLUSH_SETS: [&crate::change_flush::FlushCounters; 3] = [
    &crate::pot_changes::COUNTERS,
    &crate::hop_changes::COUNTERS,
    &crate::lobby_changes::COUNTERS,
];

/// PURE (the delta lens's D-L2): each set's note-backs NEVER RE-SENT, derived from the served counters
/// (`retried + deferred - resent`, [`crate::change_flush::noted_back_unresent`]): `(name, value)` per set.
/// `*_undelivered_total = 0` proves delivery only together with this figure at its recorded baseline: the totals
/// are cumulative across isolates, so after one loss the figure never reads 0 again (D2-L2, `change_flush`'s doc).
pub fn change_flush_unresent(counters: &serde_json::Value) -> Vec<(&'static str, u64)> {
    let read = |name: &str| counters[name].as_u64().unwrap_or(0);
    CHANGE_FLUSH_SETS
        .iter()
        .map(|set| {
            (
                set.noted_back_unresent,
                crate::change_flush::noted_back_unresent(
                    read(set.retried),
                    read(set.deferred),
                    read(set.resent),
                ),
            )
        })
        .collect()
}

/// Read the persistent counters into a JSON object (missing ⇒ 0).
///
/// The #366 census rows share `ops_counters` but are EXCLUDED here — they are
/// served structured under `submitReadinessCensus` (see [`census_json`]), and
/// reporting the same numbers twice under two shapes invites a reader to
/// depend on the flat spelling this module never promised. (Safe filter: no
/// `submit_census_` row existed before #366, so nothing a reader saw is
/// removed.)
#[rustfmt::skip] // Preserve main's strict producer wiring pin.
async fn read_counters(db: &D1Database) -> serde_json::Value {
    let rows: Vec<CounterRow> =
        Query::new("SELECT name, value FROM ops_counters WHERE name NOT LIKE 'submit_census_%'")
            .fetch_all(db)
            .await
            .unwrap_or_default();
    let mut obj = json!({
        COUNTER_PROOFS_COMPLETED: 0,
        COUNTER_FETCH_FAILED: 0,
        COUNTER_POT_BEEFS_COMPACTED: 0,
        COUNTER_SPENDS_CONFIRMED: 0,
        COUNTER_ARC_INGEST_PUSHED: 0,
        COUNTER_ARC_INGEST_STATUS_IGNORED: 0,
        COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN: 0,
        COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN: 0,
        COUNTER_ARC_INGEST_UNKNOWN_TXID: 0,
        COUNTER_ARC_REGISTRATION_SKIPPED: 0,
        COUNTER_TIP_PASS_TOTAL: 0,
        // S2 (2026-08-29): seeded so a never-bumped counter reads an explicit
        // 0 on the surface — an ABSENT key is an unknown, and an unknown must
        // not read as fine.
        COUNTER_SUBMIT_MUTATION_FAULT: 0,
        COUNTER_SUBMIT_MUTATION_QUEUED: 0,
        COUNTER_SUBMIT_MUTATION_REFUSED: 0,
        // bsv-low W-A (2026-09-09): the gated door's script walk — seeded so a
        // door that never refused reads an explicit 0.
        COUNTER_SUBMIT_SCRIPT_REFUSED: 0,
        COUNTER_SUBMIT_SCRIPT_WALK_INCONCLUSIVE: 0,
        COUNTER_SUBMIT_SCRIPT_WALK_ANCESTOR_INCONCLUSIVE: 0,
        COUNTER_SUBMIT_SCRIPT_WALK_OVER_BUDGET: 0,
        COUNTER_SUBMIT_SCRIPT_WALK_OVER_MEMORY: 0,
        // bsv-low #592: the engine's own walk under the same two limbs.
        COUNTER_SUBMIT_ENGINE_WALK_OVER_BUDGET: 0,
        COUNTER_SUBMIT_ENGINE_WALK_OVER_MEMORY: 0,
        COUNTER_QUEUE_MUTATION_APPLIED: 0,
        COUNTER_QUEUE_MUTATION_RETRIED: 0,
        // 2026-09-04: the discovery pass — seeded to 0 for the same reason.
        COUNTER_MISSING_SPEND_SCANNED: 0,
        COUNTER_MISSING_SPEND_DISCOVERED: 0,
        COUNTER_MISSING_SPEND_FAULTS: 0,
        COUNTER_MISSING_SPEND_DISCOVERED_NEW_ERA: 0,
        COUNTER_MISSING_SPEND_JOIN_HOP_INPUTS_DEFERRED: 0,
        COUNTER_MISSING_SPEND_JOIN_HOP_POTS_DEFERRED: 0,
        // bsv-low M19B-G1: the Arcade reorg-event consumer: seeded to 0 so
        // "never ran" reads as an explicit 0, never as an absent key.
        COUNTER_ARCADE_REORG_EVENTS: 0,
        COUNTER_ARCADE_REORG_REANCHORED: 0,
        COUNTER_ARCADE_REORG_DEMOTED: 0,
        COUNTER_ARCADE_REORG_UNCORROBORATED: 0,
        COUNTER_ARCADE_REORG_FAULTS: 0,
        COUNTER_ARCADE_REORG_UNRESOLVED: 0,
        COUNTER_ARCADE_REORG_TRACKER_LAGGING: 0,
        COUNTER_ARCADE_REORG_CONTENDED: 0,
        COUNTER_ARCADE_REORG_BUDGET_STOPS: 0,
    });
    // admit-fast (2026-09-15): the callback-trigger and pending-watch counters
    // are served as 0 before their first bump (an absent key reads as an
    // unknown, never as fine) — added here rather than in the literal above,
    // which is at serde_json's `json!` recursion limit.
    for name in [
        COUNTER_DEAD_LETTERS_PARKED,
        COUNTER_DEAD_LETTERS_REDRIVEN,
        COUNTER_DEAD_LETTERS_STILL_FAILING,
        COUNTER_DEAD_LETTERS_LOST,
        COUNTER_DEAD_LETTERS_LOST_NOT_NOW,
        COUNTER_DEAD_LETTERS_CEILING_DEFERRED,
        COUNTER_DEAD_LETTERS_CEILING_DEFERRALS,
        COUNTER_DEAD_LETTERS_DISCARDED,
        COUNTER_DEAD_LETTERS_NOT_NOW_DEFERRALS,
        COUNTER_DEAD_LETTERS_STALE_RETURNED,
        COUNTER_DEAD_LETTERS_RESOLVED,
        COUNTER_BEEF_BLOBS_WRITTEN,
        COUNTER_BEEF_BLOBS_DELETED,
        COUNTER_BEEF_BLOBS_DELETE_FAULTS,
        COUNTER_BEEF_BLOBS_MISSING,
        COUNTER_QUEUE_R2_TWIN_ACKED,
        COUNTER_QUEUE_R2_MISSING_FAULT,
        COUNTER_QUEUE_R2_ORPHANS_SWEPT,
        COUNTER_QUEUE_R2_ORPHANS_SWEPT_BYTES,
        COUNTER_QUEUE_R2_ORPHAN_SWEEP_FAULTS,
        COUNTER_QUEUE_R2_ORPHAN_SWEEP_UNREADABLE,
        COUNTER_ARC_INGEST_SEEN_LATCHED,
        COUNTER_ARC_INGEST_EVICTED,
        COUNTER_ARC_INGEST_READMITTED,
        COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED,
        COUNTER_ARC_INGEST_PUSH_UNVERIFIED,
        COUNTER_SUBMIT_FAST_ADMITTED,
        COUNTER_SUBMIT_DUPLICATE_IN_FLIGHT,
        COUNTER_SUBMIT_FAST_ADMITTED_ECHO,
        COUNTER_SUBMIT_PENDING_ADMITTED,
        COUNTER_SUBMIT_PENDING_SEEN_LATCHED,
        COUNTER_SUBMIT_PENDING_SEEN_MS,
        COUNTER_SUBMIT_PENDING_EVICTED,
        COUNTER_SUBMIT_PENDING_REFUSAL_UNCORROBORATED,
        COUNTER_SUBMIT_PENDING_SILENT,
        COUNTER_SUBMIT_PENDING_ORPHAN,
        COUNTER_SUBMIT_PENDING_LATCHED_ELSEWHERE,
        COUNTER_SUBMIT_PENDING_CONFIRMED_NOW,
        COUNTER_SUBMIT_PENDING_CONFIRM_ASKED,
        // bsv-low loop 18 (2026-09-21): the write-side guard's counters read 0 until they fire.
        COUNTER_SUBMIT_REFUSED_EVICTED,
        COUNTER_SUBMIT_READMITTED_BY_FRESH_ACCEPT,
        // bsv-low #519 (2026-09-21): the terminal judgement's counters read 0 until they fire.
        COUNTER_SUBMIT_TERMINAL_PRESENT,
        COUNTER_SUBMIT_TERMINAL_RACE,
        COUNTER_SUBMIT_TERMINAL_ABSENT,
        COUNTER_SUBMIT_TERMINAL_YOUNG,
        COUNTER_SUBMIT_TERMINAL_INCONCLUSIVE,
        COUNTER_SUBMIT_TERMINAL_CAPPED,
        COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE,
        COUNTER_LANDING_REFUSED_EVICTED,
        COUNTER_ADMIT_FAST_EVICT_INCOMPLETE,
        COUNTER_ADMIT_FAST_LEDGER_UNREADABLE,
        COUNTER_QUEUE_REPLAY_SKIPPED_EVICTED,
        COUNTER_ADMIT_FAST_EVICT_YIELDED,
        COUNTER_ADMIT_FAST_FLUSH_SKIPPED_NO_WINDOW,
        crate::change_park::COUNTER_ADMIT_FAST_FLUSH_WITHHELD,
        crate::change_park::COUNTER_CHANGE_PARKED,
        crate::change_park::COUNTER_CHANGE_PARK_FAULTS,
        crate::change_park::COUNTER_CHANGE_UNPARKED,
        crate::change_park::COUNTER_CHANGE_PARK_FULL,
        crate::change_park::COUNTER_CHANGE_PARK_REFUSED,
        crate::change_park::COUNTER_CHANGE_PARK_EXHAUSTED,
        crate::change_park::COUNTER_CHANGE_PARK_SWEPT,
        crate::change_park::COUNTER_CHANGE_PARK_DRAIN_TICK,
        crate::change_park::COUNTER_CHANGE_PARK_DRAIN_COMPLETE_PROOFS,
        COUNTER_ADMIT_FAST_READMIT_INCOMPLETE,
        COUNTER_ADMIT_FAST_CONFLICT_SCANNED,
        COUNTER_ADMIT_FAST_CONFLICT_EVICTED,
        COUNTER_ADMIT_FAST_CONFLICT_FAULTS,
        // bsv-low #831 half 2: the retire pass's evictions and its backlog pass
        COUNTER_RETIRE_EVICTED,
        COUNTER_RETIRE_BACKLOG_SCANNED,
        COUNTER_RETIRE_BACKLOG_EVICTED,
        COUNTER_RETIRE_BACKLOG_LEFT,
        COUNTER_RETIRE_EVICT_FAULTS,
        COUNTER_RETIRE_END_DEFERRED,
        // bsv-low #484: the probe-memo invalidation reads 0 until a reorg (or the TTL) clears one.
        crate::hop_probe_memos::COUNTER_HOP_PROBE_MEMOS_CLEARED,
        crate::hop_probe_memos::COUNTER_HOP_PROBE_MEMOS_EXPIRED,
        // the merged lens's LOW-3: the app layer's faulted memo reads (it writes the row)
        crate::hop_probe_memos::COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS,
        // bsv-low #436: the app layer's own counts of what a body carried past its cap (it writes these rows);
        // the flushes' own accounts are seeded below (`CHANGE_FLUSH_SETS`)
        crate::pot_changes::COUNTER_POT_CHANGED_DROPPED,
        crate::hop_changes::COUNTER_HOP_CHANGED_DROPPED,
        crate::lobby_changes::COUNTER_LOBBY_CHANGED_DROPPED,
    ] {
        obj[name] = json!(0);
    }
    // bsv-low #555: the deferred GASP graphs' counters (by reason too) read 0 until they fire.
    // NS-COST-BAR-1: each pass family's rows read reads 0 until its first tick.
    for family in D1_ROWS_READ_FAMILIES {
        obj[d1_rows_read_counter(family)] = json!(0);
    }
    for name in crate::gasp_deferred::counter_names() {
        obj[name] = json!(0);
    }
    // bsv-low #625, #626, #628: the app layer's observability families and this worker's memo writes read 0
    // until they fire (the app layer writes the `applayer_*` and `courier_applayer_*` rows).
    for name in crate::applayer_counters::seeded_names() {
        obj[name] = json!(0);
    }
    // 2026-09-04: every courier rung reads an explicit 0 until it is called.
    for rung in crate::proof_fetcher::COURIER_RUNGS {
        for kind in ["ok", "fault", "skipped"] {
            obj[courier_counter_name(rung, kind)] = json!(0);
        }
    }
    for set in CHANGE_FLUSH_SETS {
        for name in [set.undelivered, set.retry_failed, set.deferred, set.retried, set.resent] {
            obj[name] = json!(0);
        }
    }
    for name in crate::submit_counters::all_names().into_iter().chain(crate::courier_counters::all_names()) {
        obj[name] = json!(0);
    }
    for r in rows {
        obj[r.name] = json!(r.value.max(0.0) as u64);
    }
    // the delta lens's D-L2: the note-backs never re-sent, derived from the rows just read
    for (name, value) in change_flush_unresent(&obj) {
        obj[name] = json!(value);
    }
    obj
}

/// Derive the `/arc-ingest` push-path verdict from the four arc-ingest counters
/// — the ONE distinction the health surface could not previously express.
///
/// * `"flowing"` — at least one callback got past auth (a proof push or an
///   acknowledged status callback). The push path is live.
/// * `"refusing"` — callbacks ARE arriving and every one of them is being
///   401'd. This is the state that hid for a month: the primary proof source is
///   down, everything is falling through to the ~30-min poll backstop, and the
///   fix is a contract/config one (see the two `unauthorized_*` counters for
///   which). **This is the pageable state.**
/// * `"silent"` — no callback has ever arrived at all. Either nothing has been
///   broadcast, or `X-CallbackUrl` is not being registered.
///
/// MODELLING BOUNDARY, stated here because a reader will otherwise assume more
/// (epoch Rule 17): these are MONOTONIC LIFETIME totals, so this verdict is
/// decisive only until the first accepted callback — after that it latches
/// `"flowing"` forever. A later regression that starts 401ing everything is
/// visible in the DELTA of the `unauthorized_*` counters between two reads
/// (same reader contract as the #366 census), NOT in this field. It is a
/// standing-start diagnosis, not a continuous alarm; wiring a pager to the
/// delta is the follow-on.
///
/// It reports no number the `counters` object already reports — a second
/// spelling of the same value is what `read_counters` deliberately avoids.
fn arc_ingest_push_health(counters: &serde_json::Value) -> &'static str {
    let v = |name: &str| counters.get(name).and_then(|x| x.as_u64()).unwrap_or(0);
    let admitted = v(COUNTER_ARC_INGEST_PUSHED)
        + v(COUNTER_ARC_INGEST_STATUS_IGNORED)
        + v(COUNTER_ARC_INGEST_UNKNOWN_TXID);
    let refused = v(COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN) + v(COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN);
    match (admitted, refused) {
        (0, 0) => "silent",
        (0, _) => "refusing",
        _ => "flowing",
    }
}

/// The #366 broadcast-gated readiness census, shaped for `/health/invariants`.
///
/// Reads the durable `submit_census_*` rows (written by the `/submit` route —
/// `routes.rs`, `ProceedWithoutGate` arm; names owned by
/// [`crate::submit_census`]) and serves them as monotonic totals per
/// (mode, population, state) plus the global reason breakdown.
///
/// READER CONTRACT, stated in the body itself because the reader is a human
/// with `curl` (or a poller diffing two reads):
/// * "the last N" = the DELTA between two reads of these monotonic totals.
/// * a window whose `observed` delta is 0 is NO EVIDENCE — it carries a
///   streak, it never credits one (bsv-low #341's `None`-not-`0` posture).
///   The flip criterion is "client `wouldHaveFailed` delta is 0 across a
///   window whose client `observed` delta is MEANINGFULLY POSITIVE, and the
///   `couldNotEvaluate` bucket is understood" — never "the endpoint read 0".
/// * arrive-only residual: these count only submits the overlay SERVED. A
///   client that could not reach the overlay at all is invisible here, and an
///   overlay outage is exactly when the client is least ready — that slice
///   stays with the client-side warn (bsv-low #351).
pub async fn census_json(db: &D1Database) -> serde_json::Value {
    let rows: Vec<CounterRow> =
        Query::new("SELECT name, value FROM ops_counters WHERE name LIKE 'submit_census_%'")
            .fetch_all(db)
            .await
            .unwrap_or_default();
    let value_of = |name: &str| -> u64 {
        rows.iter()
            .find(|r| r.name == name)
            .map(|r| r.value.max(0.0) as u64)
            .unwrap_or(0)
    };

    // byMode.{mode}.{population} = {gatedReady, wouldHaveFailed,
    // couldNotEvaluate, observed} — driven off the ONE table the writer uses
    // (`CENSUS_STATE_COUNTERS`), so a name cannot drift between write and
    // read. Accumulated in a plain map first (no panic-capable JSON indexing
    // on a request path).
    let mut cells: std::collections::BTreeMap<(&str, &str), (u64, u64, u64)> =
        std::collections::BTreeMap::new();
    for (name, mode, population, state) in crate::submit_census::CENSUS_STATE_COUNTERS {
        let v = value_of(name);
        let cell = cells.entry((mode, population)).or_insert((0, 0, 0));
        match state {
            "ready" => cell.0 += v,
            "would_fail" => cell.1 += v,
            _ => cell.2 += v,
        }
    }
    let mut mode_maps: std::collections::BTreeMap<
        &str,
        serde_json::Map<String, serde_json::Value>,
    > = std::collections::BTreeMap::new();
    for ((mode, population), (ready, fail, uneval)) in cells {
        mode_maps.entry(mode).or_default().insert(
            population.to_string(),
            json!({
                "gatedReady": ready,
                "wouldHaveFailed": fail,
                "couldNotEvaluate": uneval,
                "observed": ready + fail + uneval,
            }),
        );
    }
    let mut by_mode = serde_json::Map::new();
    for (mode, populations) in mode_maps {
        by_mode.insert(mode.to_string(), serde_json::Value::Object(populations));
    }

    let mut reasons = serde_json::Map::new();
    for (name, key) in crate::submit_census::CENSUS_REASON_COUNTERS {
        reasons.insert(key.to_string(), json!(value_of(name)));
    }

    json!({
        // Rule 13: the three states are served distinctly; `couldNotEvaluate`
        // is never folded into either decided state.
        "byMode": serde_json::Value::Object(by_mode),
        "wouldFailAndUnevalReasons": serde_json::Value::Object(reasons),
        // Monotonic totals — durable across isolate recycling (D1), unlike
        // the per-isolate submitAdmission soak counters.
        "semantics": "monotonic totals; 'last N' is a delta between reads",
        "emptyWindowRule": "an observed delta of 0 across a window is NO EVIDENCE — it carries the streak, it never credits it (bsv-low #341)",
        "arriveOnlyResidual": "counts only submits the overlay SERVED; an unreachable overlay is exactly when the client is least ready — that slice stays with the client-side warn (bsv-low #351)",
    })
}

/// `GET /health/invariants[?strict=1]` — the proof-completion liveness surface.
///
/// Reports the heartbeat (last tick wall-clock + monotonic count), the
/// persistent counters, the proofless-watch flagged count, and a computed
/// `dead` verdict: the completion pass is DEAD when it has never run, or its
/// last tick is older than the staleness budget (`OPS_INVARIANTS_MAX_STALE_MS`,
/// default 6h).
///
/// - `strict=1` (or `strict=true`) → HTTP **503** when dead (the alarm can page
///   on the status alone); 200 otherwise.
/// - default (non-strict) → always HTTP 200 with the same JSON body (a probe
///   that reports the verdict without flapping the endpoint's own health).
pub async fn health_invariants(
    db: &D1Database,
    env: &Env,
    strict: bool,
) -> worker::Result<Response> {
    let now = now_ms();

    let hb: Option<HeartbeatRow> =
        Query::new("SELECT last_tick_ms, tick_count FROM ops_heartbeat WHERE id = 0")
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    let (last_tick_ms, tick_count) = hb
        .map(|h| (h.last_tick_ms.max(0.0) as i64, h.tick_count.max(0.0) as i64))
        .unwrap_or((0, 0));

    let max_stale_ms = env
        .var("OPS_INVARIANTS_MAX_STALE_MS")
        .ok()
        .and_then(|v| v.to_string().trim().parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_MAX_STALE_MS);

    // Never-run (last_tick_ms == 0) is dead; otherwise dead iff too stale.
    let never_ran = last_tick_ms == 0;
    let staleness_ms: i64 = if never_ran {
        -1
    } else {
        (now - last_tick_ms).max(0)
    };
    let dead = never_ran || staleness_ms > max_stale_ms;

    let counters = read_counters(db).await;
    let flagged = count_flagged(db, now - PROOFLESS_FLAG_MS).await;
    let held_by_window = Query::new(crate::proof_window::held_count_sql())
        .bind(now)
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c as i64);
    let expired_unproven = Query::new(crate::proof_window::EXPIRED_COUNT_SQL)
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c as i64);

    let census = census_json(db).await;
    // #371: total network_seen latches — the observable the ci-route tier
    // asserts a submit MOVES (gate MEDIUM-1: both latch call sites are
    // wasm-route-only, so without a served count the feature could be
    // deleted with every native tier green). -1 = the table is unreadable
    // (pre-migration isolate), distinct from a real 0 (Rule 13).
    let network_seen_total: i64 = Query::new(NETWORK_SEEN_TOTAL_SQL)
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c.max(0.0) as i64);
    // #2b: rows RETIRED from the proof-poll pools as structurally
    // unprovable (superseded competing spends of a confirmed pot outpoint,
    // dominated by superseded pre-signed refunds). The Rule 13 surfaced
    // number: `prooflessOver24h` above now counts GENUINE backlog only, and
    // this is where the residue went — a LIVE count (a reorg-cleared latch
    // honestly decrements), -1 = unreadable (pre-migration isolate),
    // distinct from a real 0.
    let retired_unprovable_total: i64 = Query::new(RETIRED_UNPROVABLE_TOTAL_SQL)
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c.max(0.0) as i64);
    // INCIDENT D1-CALLBACK-FLOOD 2026-09-01: the two new Rule-13 surfaces.
    // `arcTerminalTotal` = distinct txids with a courier-delivered terminal
    // verdict on record (evidence intake is ALIVE); `txRetiredTotal` = the
    // `transactions` rows retired on corroborated evidence (where the
    // incident's retry-forever population went). -1 = table unreadable
    // (pre-migration isolate), distinct from a real 0.
    let arc_terminal_total: i64 = Query::new("SELECT COUNT(*) AS c FROM arc_terminal")
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c.max(0.0) as i64);
    let tx_retired_total: i64 = Query::new(TX_RETIRED_TOTAL_SQL)
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c.max(0.0) as i64);

    let status = if strict && dead { 503 } else { 200 };
    let arcade_reorg = with_probe_memos_cleared(arcade_reorg_view(db).await, &counters);
    let mut index_janitor = index_janitor_backlog(db).await;
    let dead_letters = crate::dead_letters::health_json(db, env).await;
    let change_parked = crate::change_park::health_json(db).await;
    let queue = crate::beef_blob_sweep::health_json(db, env).await;
    let tick = crate::tick_alarm::health_json(db, env).await;
    let deferred_graphs = crate::gasp_deferred::health_json(
        db,
        crate::gasp_deferred::graph_budget_limbs_from_env(env),
    )
    .await;
    // 2026-09-04: the courier rungs' lifetime ok/fault/skipped, at a glance.
    index_janitor["couriers"] = couriers_view(&counters);
    // bsv-low #668: computed before `counters` is moved into the body; assigned after (the big `json!` is at the
    // macro recursion limit). A UX/observability figure; the money never rests on this push.
    let pots_push = pots_push_view(&counters);
    let gasp_load = crate::gasp_cost::view(db).await;
    let gasp_speed = crate::gasp_speed::view(db).await;
    let mut body = json!({
        "ok": !dead,
        "service": "low-overlay",
        "check": "proof-completion",
        "build": bsv_worker_build_info::build_json!(),
        "strict": strict,
        // bsv-low (2026-09-04): the index janitor — the live backlog by row
        // kind; its pass totals are the `missing_spend_*` counters below.
        "indexJanitor": index_janitor,
        "tick": tick,
        "completionPass": {
            "dead": dead,
            "neverRan": never_ran,
            "lastTickMs": last_tick_ms,
            "tickCount": tick_count,
            "stalenessMs": staleness_ms,
            "maxStaleMs": max_stale_ms,
        },
        // Rule 13: "nobody is calling us" vs "everybody is being refused" are
        // now distinguishable. See `arc_ingest_push_health` for the lifetime-
        // total boundary — the continuous instrument is the DELTA of the two
        // `arc_ingest_unauthorized_*_total` counters above.
        "arcIngestPushHealth": arc_ingest_push_health(&counters),
        // bsv-low M19B-G1: the Arcade reorg-event consumer's cursor (the
        // last event finished), the event in progress (with its held count)
        // and the row's stamp; `readable: false` = the row is unreadable
        // (pre-migration isolate or a newer document), distinct from "never ran".
        "arcadeReorg": arcade_reorg,
        "counters": counters,
        // #2b: GENUINE backlog only — retired (structurally-unprovable)
        // rows are excluded at enrol AND dropped by the watch GC.
        "prooflessOver24h": flagged,
        "heldByWindow": held_by_window,
        "gaspLastTick": gasp_load["lastTick"],
        "gaspCallsOut24h": gasp_load["totals24h"]["calls_out"],
        "gaspStorageReads24h": gasp_load["totals24h"]["storage_reads"],
        "gaspStorageWrites24h": gasp_load["totals24h"]["storage_writes"],
        "gaspSyncMs24h": gasp_load["totals24h"]["ms"],
        "gaspLoadFault": gasp_load["fault"],
        "gaspRootsProven": counters.get(crate::gasp_deferred::COUNTER_GASP_ROOTS_PROVEN).and_then(serde_json::Value::as_u64).unwrap_or(0),
        "gaspRootsUnproven": counters.get(crate::gasp_deferred::COUNTER_GASP_ROOTS_UNPROVEN).and_then(serde_json::Value::as_u64).unwrap_or(0),
        "crawlExpiredRefused": counters.get(crate::peer_crawler::COUNTER_CRAWL_EXPIRED_REFUSED).and_then(serde_json::Value::as_u64).unwrap_or(0),
        "gaspExpiredRefused": counters.get(crate::gasp_deferred::COUNTER_GASP_EXPIRED_REFUSED).and_then(serde_json::Value::as_u64).unwrap_or(0),
        // Engine 0.1.9: roots recorded unanchorable (the cursor passed them).
        "gaspUnanchorable": counters.get(crate::gasp_deferred::COUNTER_GASP_UNANCHORABLE).and_then(serde_json::Value::as_u64).unwrap_or(0),
        // Engine 0.1.10: strikes not written (the root stays a gap, named).
        "gaspStrikesRefused": counters.get(crate::gasp_deferred::COUNTER_GASP_STRIKES_REFUSED).and_then(serde_json::Value::as_u64).unwrap_or(0),
        "peerProofRequests": counters.get("peer_proof_requests_total").and_then(serde_json::Value::as_u64).unwrap_or(0),
        "peerProofsLanded": counters.get("peer_proofs_landed_total").and_then(serde_json::Value::as_u64).unwrap_or(0),
        "retiredExpiredUnprovenTotal": expired_unproven,
        // #2b: where the residue went (live count; -1 = table unreadable).
        "retiredUnprovableTotal": retired_unprovable_total,
        // INCIDENT D1-CALLBACK-FLOOD 2026-09-01 (both -1 = unreadable):
        "arcTerminalTotal": arc_terminal_total,
        "txRetiredTotal": tx_retired_total,
        // #371: lifetime network_seen latch count (-1 = table unreadable).
        "networkSeenTotal": network_seen_total,
        // #347 soak signal (Rule 6c closure criterion 3): `unauthenticatedUngated`
        // must reach ~0 before SUBMIT_ENFORCE is flipped to true. Per-isolate and
        // therefore lossy — a soak signal, never an audit log.
        "submitAdmission": crate::submit_gate::counters_json(),
        "trackerEvents": crate::tracker_events::health_json(),
        // #366: would every honest CLIENT submit survive `broadcast-gated`?
        // The flip-criterion instrument for #347 criterion 1 — durable D1
        // totals, three states (Rule 13), reader contract in `census_json`.
        "submitReadinessCensus": census,
        // bsv-low #499: this isolate's D1 rows ledger, the per-route running maxima since boot (what
        // `scripts/d1-census.py` reads before a promotion; the request that serves this body is not in it yet).
        "d1Budget": crate::d1_ledger::budget_json(),
        // bsv-low #576: the parked dead letters (count by status, the oldest parked, the last re-drive, the letters
        // past the re-drive ceiling); `readable: false` = the table is unreadable, distinct from none.
        "deadLetters": dead_letters,
        // bsv-low #649 M1: the pot and hop notes a background flush withheld, parked in D1 until the cron delivers
        // them (by set; `null` = unreadable).
        "changeParked": change_parked,
        // bsv-low #585 (door 3's fold): the queue's R2 objects at rest as the orphan sweep's listing counted them
        // (the last complete round and the one in progress) and the sweep's window, bounds and last pass.
        "queue": queue,
        // bsv-low #555: the GASP graphs deferred past their per-graph budget (count, the oldest, each with its
        // topic, peer, outpoint, nodes, pending, calls, passes, reason, bytes, age); `readable: false` = unreadable.
        "gasp": { "deferredGraphs": deferred_graphs },
    });

    body["gaspLastSync"] = gasp_speed["lastSync"].clone();
    body["gaspWalk24h"] = gasp_speed["totals24h"].clone();
    body["gaspWalkByTopicPeer24h"] = gasp_speed["byTopicPeer24h"].clone();
    body["gaspSpeedFault"] = gasp_speed["fault"].clone();
    // bsv-low #668: the pots-room push totals + p50/p95 latency, beside the raw `pots_push_*` counters.
    body["potsPush"] = pots_push;
    body["gaspDead"] = counters
        .get(crate::gasp_deferred::COUNTER_GASP_DEAD)
        .cloned()
        .unwrap_or(json!(0));
    body["gaspFaultExpired"] = counters
        .get(crate::gasp_deferred::COUNTER_GASP_FAULT_EXPIRED)
        .cloned()
        .unwrap_or(json!(0));
    match crate::gasp_fault_memo::health(db).await {
        Ok(rows) => {
            body["gaspFaultMemoRows"] = json!(rows);
        }
        Err(error) => {
            body["gaspFaultMemoRows"] = serde_json::Value::Null;
            body["gaspFaultMemoFault"] = json!(error);
        }
    }
    // Engine 0.1.15 (#56): the absences the peers' word recorded in a day.
    body["gaspPeerAbsent24h"] = match crate::gasp_fault_memo::peer_absent_24h(db).await {
        Ok(absent) => absent,
        Err(error) => json!({"fault": error}),
    };
    // Engine 0.2.0 (#57): the route faults of a day, none counted against a peer.
    body["gaspRouteFaults24h"] = crate::gasp_route::view(db).await;
    body["gaspFaultMemoFolded"] = counters
        .get("gasp_fault_memo_folded_total")
        .cloned()
        .unwrap_or(json!(0));
    body["d1RowsRead"] = d1_rows_read_view(&body["counters"]);
    // NS-COST-BAR-1: this read's own rows (every statement above), durable, so the fleet can judge the poll's cost
    // per day. One statement more, not in its own figure.
    if let Some(t) = crate::d1_ledger::current() {
        bump_counter(db, &d1_rows_read_counter("health_invariants"), t.reads).await;
    }
    let mut resp = Response::from_json(&body)?.with_status(status);
    crate::routes::add_cors_headers(&mut resp);
    let _ = resp.headers_mut().set("Cache-Control", "no-store");
    Ok(resp)
}

/// PURE (NS-COST-BAR-1): `/health/invariants.d1RowsRead`, the families' durable rows-read counters in one place
/// (the same values as `counters.d1_rows_read_<family>_total`), for the invariants cell and the daily census to
/// judge against the daily budget in `tools/lane-499/ceilings.json` (`d1RowsReadPerDay`).
pub fn d1_rows_read_view(counters: &serde_json::Value) -> serde_json::Value {
    let mut by = serde_json::Map::new();
    let mut total = 0u64;
    for family in D1_ROWS_READ_FAMILIES {
        let n = counters[d1_rows_read_counter(family)].as_u64().unwrap_or(0);
        total = total.saturating_add(n);
        by.insert((*family).to_string(), json!(n));
    }
    json!({
        "unit": "D1 meta.rows_read, summed since each counter's first bump (a delta over a day is the day's figure)",
        "byFamily": by,
        "total": total,
    })
}

#[cfg(test)]
mod tests {
    /// NS-COST-BAR-1: one tick's per-pass tallies become one delta per family; the view serves the counters.
    #[test]
    fn ns_cost_rows_read_deltas_and_view() {
        use crate::d1_ledger::Tally;
        let t = |reads| Tally {
            reads,
            writes: 1,
            stmts: 1,
            max_reads: reads,
        };
        let by_pass = std::collections::BTreeMap::from([
            ("setup", t(3)),
            ("proofless_watch", t(389)),
            ("relatch", t(0)),
            ("not_a_family", t(7)),
        ]);
        let deltas = d1_rows_read_deltas(&by_pass);
        assert_eq!(
            deltas,
            std::collections::BTreeMap::from([
                ("d1_rows_read_proofless_watch_total".to_string(), 389),
                ("d1_rows_read_relatch_total".to_string(), 0),
                ("d1_rows_read_setup_total".to_string(), 10),
            ]),
            "an unknown label folds into setup; no name is minted"
        );
        let counters = json!({ "d1_rows_read_proofless_watch_total": 389, "d1_rows_read_health_invariants_total": 11 });
        let view = d1_rows_read_view(&counters);
        assert_eq!(view["byFamily"]["proofless_watch"], 389);
        assert_eq!(view["byFamily"]["janitor"], 0);
        assert_eq!(view["total"], 400);
        assert_eq!(
            view["byFamily"].as_object().unwrap().len(),
            D1_ROWS_READ_FAMILIES.len()
        );
        // Every family's counter is seeded 0 by `read_counters`, and the health read bumps its own.
        let src = include_str!("ops.rs");
        let read = &src[src.find("async fn read_counters(").unwrap()..];
        assert!(read[..read.find("\n}\n").unwrap()].contains("for family in D1_ROWS_READ_FAMILIES"));
        assert!(src.contains(
            "bump_counter(db, &d1_rows_read_counter(\"health_invariants\"), t.reads).await"
        ));
    }

    /// NS-COST-BAR-1: a migrated SQLite connection with `recursive_triggers` as given (D1's setting is not
    /// documented, and an `INSERT OR REPLACE` fires the delete trigger only when it is on).
    pub(crate) fn ns_cost_migrated(recursive_triggers: bool) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    e.to_string().contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        conn.pragma_update(None, "recursive_triggers", recursive_triggers)
            .unwrap();
        conn
    }

    /// bsv-low #794: hold SQLite's `unixepoch()` on `conn` to the value in the returned cell, so every statement a
    /// pin runs (the shipped backlog, its old twin, a writer's `spentAt = unixepoch()`, a fixture's `createdAt`)
    /// reads ONE `now` until the pin moves it. An application function overrides the built-in of its name and
    /// arity; `unixepoch(time, ..)` with arguments stays SQLite's.
    pub(crate) fn ns_cost_pin_clock(
        conn: &rusqlite::Connection,
        now: i64,
    ) -> std::sync::Arc<std::sync::atomic::AtomicI64> {
        let clock = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(now));
        let held = clock.clone();
        conn.create_scalar_function(
            "unixepoch",
            0,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            move |_| Ok(held.load(std::sync::atomic::Ordering::SeqCst)),
        )
        .unwrap();
        clock
    }

    /// NS-COST-BAR-1: the backlog before the count row (`index_janitor_backlog`'s old statement).
    pub(crate) const INDEX_JANITOR_BACKLOG_BEFORE_SQL: &str =
        "SELECT lockKind, COUNT(*) AS c FROM pot_records \
         WHERE spent = 0 AND (createdAt IS NULL OR createdAt <= unixepoch() - 3600) \
         GROUP BY lockKind";

    /// The three served figures, folded the way [`index_janitor_backlog`] folds its rows.
    pub(crate) fn ns_cost_backlog_fold(conn: &rusqlite::Connection, sql: &str) -> (i64, i64, i64) {
        let mut stmt = conn.prepare(sql).unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?))
            })
            .unwrap();
        let (mut covenant, mut p2pkh, mut other) = (0, 0, 0);
        for row in rows {
            let (kind, n) = row.unwrap();
            let n = n.max(0);
            match kind.as_deref() {
                Some("covenant") => covenant += n,
                Some("p2pkh") => p2pkh += n,
                _ => other += n,
            }
        }
        (covenant, p2pkh, other)
    }

    /// NS-COST-BAR-1: the index janitor's backlog equals the old `GROUP BY` after every SHIPPED `pot_records` writer
    /// of `spent` or `lockKind` (the #284 upsert's insert and its conflict fill, the four spend marks, both CASes,
    /// an eviction's delete and restore), its class counts equal a count of the unspent rows, and the hour boundary
    /// is the old one to the second. The clock is held (bsv-low #794): read from the wall, a second boundary between
    /// the young fixtures' writes and a later check aged y3 past the edge (`(1, 0, 1)`, about one run in three).
    #[test]
    fn ns_cost_unspent_kind_counts_are_exact_under_every_shipped_writer() {
        use crate::d1_discovery as d;
        use rusqlite::types::Value;
        use std::sync::atomic::Ordering;
        let conn = ns_cost_migrated(false);
        let now = 1_800_000_000i64;
        let clock = ns_cost_pin_clock(&conn, now);
        let sql_now = |conn: &rusqlite::Connection| -> i64 {
            conn.query_row("SELECT unixepoch()", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(sql_now(&conn), now, "the held clock overrides SQLite's");
        let check = |what: &str| {
            let new = ns_cost_backlog_fold(&conn, INDEX_JANITOR_BACKLOG_SQL);
            assert_eq!(
                new,
                ns_cost_backlog_fold(&conn, INDEX_JANITOR_BACKLOG_BEFORE_SQL),
                "{what}: the served backlog"
            );
            let counted = ns_cost_backlog_fold(
                &conn,
                "SELECT lockKind, COUNT(*) FROM pot_records WHERE spent = 0 GROUP BY lockKind",
            );
            let held = ns_cost_backlog_fold(&conn, "SELECT kind, n FROM count_pot_unspent_kind");
            assert_eq!(held, counted, "{what}: the class counts");
            new
        };
        let store = |txid: &str, spent: i64, created: Value, kind: Value| {
            let mut binds = vec![Value::Null; 19];
            binds[0] = Value::Text(txid.into());
            binds[1] = Value::Integer(0);
            binds[2] = Value::Integer(spent);
            binds[4] = Value::Integer(0);
            binds[5] = created;
            binds[6] = kind;
            binds[18] = Value::Integer(0);
            conn.execute(d::store_record_sql(), rusqlite::params_from_iter(binds))
                .unwrap();
        };
        let at_end = |sql: &str, txid: &str| {
            let n = conn.prepare(sql).unwrap().parameter_count();
            let mut binds = vec![Value::Null; n - 2];
            binds.push(Value::Text(txid.into()));
            binds.push(Value::Integer(0));
            conn.execute(sql, rusqlite::params_from_iter(binds))
                .unwrap();
        };
        // The two CASes end in `RETURNING`: stepped to their end.
        let returning = |sql: &str, binds: &[&dyn rusqlite::ToSql]| {
            conn.prepare(sql)
                .unwrap()
                .query_map(binds, |_| Ok(()))
                .unwrap()
                .for_each(|r| r.unwrap());
        };
        let old = Value::Integer(1_700_000_000);
        let t = |s: &str| Value::Text(s.into());
        assert_eq!(check("empty"), (0, 0, 0));
        store("c1", 0, old.clone(), t("covenant"));
        store("c2", 0, Value::Null, t("covenant"));
        store("p1", 0, old.clone(), t("p2pkh"));
        store("o1", 0, old.clone(), Value::Null);
        store("o2", 0, old.clone(), t("bare"));
        store("s1", 1, old.clone(), t("covenant"));
        assert_eq!(check("the #284 upsert's inserts"), (2, 1, 2));
        store("o1", 0, old.clone(), t("covenant"));
        assert_eq!(check("the conflict fill decodes o1"), (3, 1, 1));
        store("o1", 0, old.clone(), t("p2pkh"));
        assert_eq!(check("stored-wins: o1 stays covenant"), (3, 1, 1));
        for (i, (confirmed, verdict)) in
            [(true, true), (true, false), (false, true), (false, false)]
                .into_iter()
                .enumerate()
        {
            let txid = ["c1", "c2", "p1", "o2"][i];
            at_end(d::mark_spent_sql(confirmed, verdict), txid);
        }
        assert_eq!(check("the four spend marks"), (1, 0, 0));
        returning(
            d::confirm_spend_cas_sql(),
            rusqlite::params![Option::<i64>::None, "o1", 0i64, Option::<String>::None],
        );
        assert_eq!(check("a confirm CAS that misses"), (1, 0, 0));
        store("y1", 0, Value::Null, t("p2pkh"));
        conn.execute(
            "UPDATE pot_records SET createdAt = unixepoch() WHERE txid = 'y1'",
            [],
        )
        .unwrap();
        store("y2", 0, Value::Null, t("covenant"));
        conn.execute(
            "UPDATE pot_records SET createdAt = unixepoch() - 3600 WHERE txid = 'y2'",
            [],
        )
        .unwrap();
        store("y3", 0, Value::Null, Value::Null);
        conn.execute(
            "UPDATE pot_records SET createdAt = unixepoch() - 3599 WHERE txid = 'y3'",
            [],
        )
        .unwrap();
        assert_eq!(
            check("a young row is not backlog; the hour's edge is the old one"),
            (2, 0, 0)
        );
        conn.execute("DELETE FROM pot_records WHERE txid = 'o1'", [])
            .unwrap();
        assert_eq!(check("the eviction's delete"), (1, 0, 0));
        for _ in 0..2 {
            conn.execute(
                "INSERT OR IGNORE INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES ('o1', 0, 0, 5, 'covenant')",
                [],
            )
            .unwrap();
            assert_eq!(check("the restore, twice"), (2, 0, 0));
        }
        returning(
            d::displace_spend_cas_sql(),
            rusqlite::params!["s9", 1i64, 1i64, "o1", 0i64, "nobody"],
        );
        assert_eq!(check("a displace CAS that misses"), (2, 0, 0));
        conn.execute(
            "UPDATE pot_records SET spendingTxid = 'x' WHERE txid = 'o1'",
            [],
        )
        .unwrap();
        returning(
            d::displace_spend_cas_sql(),
            rusqlite::params!["s9", 1i64, 1i64, "o1", 0i64, "x"],
        );
        assert_eq!(check("a displace CAS that lands"), (1, 0, 0));
        assert_eq!(
            conn.query_row(
                "SELECT spentAt FROM pot_records WHERE txid = 'o1'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            now,
            "the CAS's spentAt reads the held now"
        );
        // The edge, moved by the pin alone: y2 sits at exactly now - 3600 (backlog), y3 at now - 3599 (young) until
        // one second passes, when both statements count it at once.
        assert_eq!(
            conn.query_row(
                "SELECT createdAt FROM pot_records WHERE txid = 'y2'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            now - 3600
        );
        clock.store(now + 1, Ordering::SeqCst);
        assert_eq!(sql_now(&conn), now + 1);
        assert_eq!(check("one second on: y3 is backlog"), (1, 0, 1));
    }

    /// NS-COST-BAR-1: `txRetiredTotal` and the expired-unproven total equal their `COUNT(*)`s after every SHIPPED
    /// `transactions` writer, the three `INSERT OR REPLACE` writes included, with `recursive_triggers` off and on.
    #[test]
    fn ns_cost_retired_counts_are_exact_under_every_shipped_writer() {
        use crate::d1_storage as s;
        for recursive in [false, true] {
            let conn = ns_cost_migrated(recursive);
            let one = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
            let check = |what: &str| -> (i64, i64) {
                let counted = (
                    one("SELECT COUNT(*) FROM transactions WHERE retired_ms IS NOT NULL"),
                    one("SELECT COUNT(*) FROM transactions WHERE retired_ms IS NOT NULL AND retired_reason = 'expired-unproven'"),
                );
                assert_eq!(
                    (
                        one(TX_RETIRED_TOTAL_SQL),
                        one(crate::proof_window::EXPIRED_COUNT_SQL)
                    ),
                    counted,
                    "{what} (recursive_triggers {recursive})"
                );
                counted
            };
            let retire = |txid: &str, reason: &str| {
                conn.execute(
                    crate::proof_fetcher::RETIRE_TRANSACTIONS_SQL,
                    rusqlite::params![5i64, reason, txid],
                )
                .unwrap();
            };
            assert_eq!(check("empty"), (0, 0));
            for t in ["a", "b", "c"] {
                conn.execute(s::INSERT_OUTPUT_TX_SQL, rusqlite::params![t, vec![1u8]])
                    .unwrap();
            }
            conn.execute(
                s::INSERT_OUTPUT_TX_GASP_SQL,
                rusqlite::params!["d", vec![1u8]],
            )
            .unwrap();
            retire("a", "expired-unproven");
            retire("b", "utxo-spent");
            retire("d", "expired-unproven");
            assert_eq!(check("the retire pass"), (3, 2));
            retire("a", "utxo-spent");
            assert_eq!(check("a retired row is never re-retired"), (3, 2));
            conn.execute(s::INSERT_OUTPUT_TX_SQL, rusqlite::params!["a", vec![2u8]])
                .unwrap();
            conn.execute(
                s::INSERT_OUTPUT_TX_GASP_SQL,
                rusqlite::params!["d", vec![2u8]],
            )
            .unwrap();
            assert_eq!(check("the REPLACE writes keep the latches"), (3, 2));
            conn.execute(
                s::UPDATE_TX_BEEF_SQL,
                rusqlite::params!["b", vec![3u8], 0i64, Option::<i64>::None],
            )
            .unwrap();
            assert_eq!(check("an unproven beef REPLACE keeps b's latch"), (3, 2));
            conn.execute(
                s::UPDATE_TX_BEEF_SQL,
                rusqlite::params!["a", vec![3u8], 1i64, 9i64],
            )
            .unwrap();
            assert_eq!(check("a proven beef REPLACE clears a"), (2, 1));
            conn.execute(s::MARK_TX_PROVEN_AT_SQL, rusqlite::params![9i64, "d"])
                .unwrap();
            assert_eq!(check("the anchored flip clears d"), (1, 0));
            conn.execute(
                "UPDATE transactions SET has_proof = 1, retired_ms = NULL, retired_reason = NULL WHERE txid = ?",
                ["b"],
            )
            .unwrap();
            assert_eq!(check("the stored-proof flip clears b"), (0, 0));
            conn.execute(s::TRANSACTION_UNPROVE_SQL, ["b"]).unwrap();
            retire("b", "expired-unproven");
            retire("c", "expired-unproven");
            assert_eq!(check("unproved, then retired"), (2, 2));
            conn.execute(
                "UPDATE transactions SET retired_reason = 'utxo-spent' WHERE txid = 'c'",
                [],
            )
            .unwrap();
            assert_eq!(check("the reason moves alone"), (2, 1));
            conn.execute("DELETE FROM transactions WHERE txid = ?", ["c"])
                .unwrap();
            assert_eq!(check("the deep delete"), (1, 1));
            for _ in 0..2 {
                conn.execute(
                    "INSERT OR IGNORE INTO transactions (txid, beef, retired_ms, retired_reason) VALUES ('c', x'00', 1, 'expired-unproven')",
                    [],
                )
                .unwrap();
                assert_eq!(check("an eviction's restore, twice"), (2, 2));
            }
            conn.execute(
                "UPDATE transactions SET txid = 'e', retired_reason = NULL WHERE txid = 'c'",
                [],
            )
            .unwrap();
            assert_eq!(check("a new key with no reason"), (2, 1));
        }
    }

    /// NS-COST-BAR-1: `retiredUnprovableTotal` equals `COUNT(*)` after every SHIPPED `pot_beefs` writer, the two
    /// `INSERT OR REPLACE` writes included, whether or not the REPLACE fires the delete trigger.
    #[test]
    fn ns_cost_unprovable_count_is_exact_under_every_shipped_writer() {
        use crate::d1_discovery as d;
        for recursive in [false, true] {
            let conn = ns_cost_migrated(recursive);
            let check = |what: &str| {
                let served: i64 = conn
                    .query_row(RETIRED_UNPROVABLE_TOTAL_SQL, [], |r| r.get(0))
                    .unwrap();
                let count: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM pot_beefs WHERE structurally_unprovable = 1",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(served, count, "{what} (recursive_triggers {recursive})");
                count
            };
            let admit = |txid: &str| {
                conn.execute(
                    d::POT_BEEF_ADMIT_WRITE_SQL,
                    rusqlite::params![txid, vec![1u8], txid, 1i64, 0i64, txid],
                )
                .unwrap();
            };
            assert_eq!(check("empty"), 0);
            for t in ["a", "b", "c", "d"] {
                admit(t);
            }
            conn.execute(crate::proof_fetcher::RETIRE_POT_BEEFS_SQL, ["a"])
                .unwrap();
            assert_eq!(check("the retire pass latches a"), 1);
            admit("a");
            assert_eq!(check("an admit REPLACE keeps a's latch"), 1);
            conn.execute(&d::pot_beef_retire_sql(3), ["b", "c", "d"])
                .unwrap();
            assert_eq!(check("the sibling latch of three"), 4);
            conn.execute(&d::pot_beef_retire_sql(2), ["b", "c"])
                .unwrap();
            assert_eq!(check("a latch that changes nothing"), 4);
            conn.execute(
                d::POT_BEEF_VERIFIED_WRITE_SQL,
                rusqlite::params!["b", vec![2u8], 1i64, 7i64],
            )
            .unwrap();
            assert_eq!(check("a verified REPLACE clears b"), 3);
            conn.execute(d::POT_BEEF_MARK_PROVEN_SQL, ["c"]).unwrap();
            assert_eq!(check("mark proven clears c"), 2);
            conn.execute(d::POT_BEEF_CLEAR_UNPROVABLE_SQL, ["a"])
                .unwrap();
            assert_eq!(check("the confirm clear of a"), 1);
            conn.execute(crate::proof_fetcher::RETIRE_POT_BEEFS_SQL, ["a"])
                .unwrap();
            conn.execute(&d::pot_beef_mark_proven_batch_sql(2), ["d", "zz"])
                .unwrap();
            assert_eq!(check("the batched flip clears d"), 1);
            conn.execute(d::POT_BEEF_MARK_PROVEN_AT_SQL, rusqlite::params![9i64, "a"])
                .unwrap();
            assert_eq!(check("the anchored flip clears a"), 0);
            conn.execute(crate::proof_fetcher::RETIRE_POT_BEEFS_SQL, ["d"])
                .unwrap();
            assert_eq!(check("a proven row is not retired"), 0);
            admit("e");
            conn.execute(crate::proof_fetcher::RETIRE_POT_BEEFS_SQL, ["e"])
                .unwrap();
            conn.execute(
                &d::pot_beef_mark_proven_at_batch_sql(1),
                rusqlite::params!["e", 3i64, "e"],
            )
            .unwrap();
            assert_eq!(check("the batched anchored flip clears e"), 0);
            // An eviction's move and restore (plain DELETE, INSERT OR IGNORE), a latch value that is not 1, and a
            // raw 2.
            admit("f");
            conn.execute(crate::proof_fetcher::RETIRE_POT_BEEFS_SQL, ["f"])
                .unwrap();
            assert_eq!(check("f latched"), 1);
            conn.execute("DELETE FROM pot_beefs WHERE txid = 'f'", [])
                .unwrap();
            assert_eq!(check("the eviction's delete"), 0);
            for _ in 0..2 {
                conn.execute(
                    "INSERT OR IGNORE INTO pot_beefs (txid, beef, structurally_unprovable) VALUES ('f', x'00', 1)",
                    [],
                )
                .unwrap();
                assert_eq!(check("the restore, twice"), 1);
            }
            conn.execute(
                "UPDATE pot_beefs SET structurally_unprovable = 2 WHERE txid = 'f'",
                [],
            )
            .unwrap();
            assert_eq!(check("a latch of 2 is not 1"), 0);
            conn.execute(
                "UPDATE pot_beefs SET structurally_unprovable = 1.0, txid = 'g' WHERE txid = 'f'",
                [],
            )
            .unwrap();
            assert_eq!(check("1.0 under a new key"), 1);
        }
    }

    use super::*;

    /// Build the counters object the way `read_counters` does (all four
    /// arc-ingest names present, defaulting to 0), then set the values.
    fn counters(
        pushed: u64,
        status_ignored: u64,
        no_token: u64,
        bad_token: u64,
    ) -> serde_json::Value {
        json!({
            COUNTER_ARC_INGEST_PUSHED: pushed,
            COUNTER_ARC_INGEST_STATUS_IGNORED: status_ignored,
            COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN: no_token,
            COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN: bad_token,
        })
    }

    #[test]
    fn copy_expired_counter_persists_real_sqlite_deltas() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            let _ = db.execute_batch(sql);
        }
        let topic: overlay_engine::engine::TopicSyncResult = serde_json::from_value(json!({
            "peers": [], "sync_type": "peers", "errors": [], "expired_refused": 2
        }))
        .unwrap();
        let topics = std::collections::HashMap::from([("tm_uhrp".to_string(), topic)]);
        for (name, delta) in crate::gasp_deferred::counter_deltas(&topics) {
            db.execute(BUMP_COUNTER_SQL, rusqlite::params![name, delta])
                .unwrap();
        }
        let count: u64 = db
            .query_row(
                "SELECT value FROM ops_counters WHERE name = ?",
                [crate::gasp_deferred::COUNTER_GASP_EXPIRED_REFUSED],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert!(include_str!("ops.rs").contains("\"gaspExpiredRefused\": counters.get(crate::gasp_deferred::COUNTER_GASP_EXPIRED_REFUSED)"));
    }

    #[test]
    fn ns2_l2_duplicate_counter_is_distinct_and_route_uses_it() {
        assert_eq!(
            pending_admission_counter(true, true),
            "submit_fast_admitted_echo_total"
        );
        assert_eq!(
            pending_admission_counter(true, false),
            "submit_fast_admitted_total"
        );
        assert_eq!(
            pending_admission_counter(false, true),
            COUNTER_SUBMIT_PENDING_ADMITTED
        );
        let route = include_str!("routes.rs");
        assert!(route.contains("pending_admission_counter(admit_fast, duplicate_echo)"));
        assert!(route.contains("COUNTER_SUBMIT_DUPLICATE_IN_FLIGHT"));
        assert!(route.contains("\"fast-echo\""));
    }

    /// bsv-low #668: `/health/invariants.potsPush` folds the app layer's `pots_push_*` counters into the
    /// sent/walled/redriven totals and derives p50/p95 from the latency bucket counters (bounds read from the
    /// names). RED without the view (the counters would be flat under `.counters` with no derived quantile).
    #[test]
    fn pots_push_view_derives_the_totals_and_latency_quantiles() {
        let c = json!({
            "pots_push_sent_total": 42,
            "pots_push_walled_total": 5,
            "pots_push_redriven_total": 4,
            "pots_push_latency_ms_le_250_total": 1,
            "pots_push_latency_ms_le_500_total": 2,
            "pots_push_latency_ms_le_1000_total": 4,
            "pots_push_latency_ms_le_2000_total": 2,
            "pots_push_latency_ms_le_inf_total": 1,
        });
        let v = pots_push_view(&c);
        assert_eq!(v["sentTotal"], 42);
        assert_eq!(v["walledTotal"], 5);
        assert_eq!(v["redrivenTotal"], 4);
        assert_eq!(v["latencySamples"], 10);
        // cumulative 250→1, 500→3, 1000→7, 2000→9, inf→10: p50 (target 5) in the le_1000 bucket; p95 (target 10)
        // in the +Inf bucket → the largest FINITE bound (2000) as a lower estimate.
        assert_eq!(v["latencyMsP50"], 1000);
        assert_eq!(v["latencyMsP95"], 2000);
        // empty counters: zeros, no panic.
        let z = pots_push_view(&json!({}));
        assert_eq!(z["latencyMsP50"], 0);
        assert_eq!(z["latencySamples"], 0);
        // the quantile helper's boundaries.
        assert_eq!(
            push_latency_quantile(&[(250, 0), (500, 0)], 0.5),
            0,
            "no samples → 0"
        );
        assert_eq!(push_latency_quantile(&[(250, 1), (500, 1)], 0.5), 250);
        assert_eq!(push_latency_quantile(&[(250, 1), (500, 1)], 0.95), 500);
    }

    #[test]
    fn couriers_view_folds_the_flat_counters_per_rung_with_explicit_zeroes() {
        let c = json!({ "courier_woc_ok_total": 7, "courier_woc_fault_total": 2, "courier_bitails_tx_skipped_total": 1 });
        let v = couriers_view(&c);
        assert_eq!(v["woc"]["ok"], 7);
        assert_eq!(v["woc"]["fault"], 2);
        assert_eq!(v["bitails_tx"]["skipped"], 1);
        assert_eq!(
            v["bananablocks"]["ok"], 0,
            "never called reads an explicit 0"
        );
        assert_eq!(
            courier_counter_name("woc_history", "fault"),
            "courier_woc_history_fault_total"
        );
    }

    /// The delta lens's D-L2 on the surface: each webhook's note-backs never re-sent are served under their own
    /// name, derived from the three counters `read_counters` serves; a set whose retried and deferred entries
    /// were all POSTed again reads 0, one that lost eight with an isolate reads 8 while its `undelivered` is 0.
    /// To red: serve the counters without the derived figure, or derive it without `resent`.
    #[test]
    fn the_note_backs_never_resent_are_served_by_name_beside_an_undelivered_of_zero() {
        let c = json!({
            "pot_changed_undelivered_total": 0, "pot_changed_retried_total": 16, "pot_changed_deferred_total": 3, "pot_changed_resent_total": 11,
            "hop_changed_retried_total": 4, "hop_changed_deferred_total": 0, "hop_changed_resent_total": 4,
        });
        assert_eq!(
            change_flush_unresent(&c),
            vec![
                ("pot_changed_noted_back_unresent", 8),
                ("hop_changed_noted_back_unresent", 0),
                ("lobby_changed_noted_back_unresent", 0)
            ]
        );
        let names: Vec<&str> = CHANGE_FLUSH_SETS
            .iter()
            .flat_map(|s| {
                [
                    s.undelivered,
                    s.retry_failed,
                    s.deferred,
                    s.retried,
                    s.resent,
                ]
            })
            .collect();
        assert_eq!(
            names,
            [
                "pot_changed_undelivered_total",
                "pot_changed_retry_failed_total",
                "pot_changed_deferred_total",
                "pot_changed_retried_total",
                "pot_changed_resent_total",
                "hop_changed_undelivered_total",
                "hop_changed_retry_failed_total",
                "hop_changed_deferred_total",
                "hop_changed_retried_total",
                "hop_changed_resent_total",
                "lobby_changed_undelivered_total",
                "lobby_changed_retry_failed_total",
                "lobby_changed_deferred_total",
                "lobby_changed_retried_total",
                "lobby_changed_resent_total",
            ]
        );
        // the read seeds every one at 0 and derives the figure after the rows (source)
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let src = include_str!("ops.rs");
        let start = src.find("async fn read_counters(").expect("read_counters");
        let body = squash(&src[start..start + src[start..].find("\n}\n").expect("its end")]);
        assert!(body.contains(&squash("for set in CHANGE_FLUSH_SETS { for name in [set.undelivered, set.retry_failed, set.deferred, set.retried, set.resent] { obj[name] = json!(0); } }")));
        let rows_at = body.find(&squash("for r in rows {")).expect("the rows");
        let derive_at = body
            .find(&squash(
                "for (name, value) in change_flush_unresent(&obj) { obj[name] = json!(value); }",
            ))
            .expect("the derived figure");
        assert!(derive_at > rows_at, "derived from the rows just read");
    }

    /// bsv-low M19B-G1: the consumer's state on the health surface: the
    /// cursor and the pending event with its held count, `everRan` and
    /// `readable` explicit (an absent key is an unknown, never fine).
    #[test]
    fn arcade_reorg_state_json_shows_the_cursor_and_the_pending_event() {
        use overlay_discovery::pot::arcade_events::{ConsumerState, OrphanEvent};
        let mut state = ConsumerState::default();
        let v = arcade_reorg_state_json(&state, None, None);
        assert_eq!(v["readable"], true);
        assert_eq!(v["everRan"], true);
        assert!(v["cursor"].is_null() && v["pending"].is_null());
        let ev = OrphanEvent {
            orphaned_at: "2026-09-07T22:45:22.316Z".into(),
            height: 965771,
            hash: "ab".repeat(32),
        };
        state.start(ev.clone());
        state.hold_pending();
        state.note_fault_on_pending();
        let v = arcade_reorg_state_json(&state, Some(1_700_000_000_000), Some(7));
        assert_eq!(v["pending"]["event"]["height"], 965771);
        assert_eq!(
            v["pending"]["event"]["orphanedAt"],
            "2026-09-07T22:45:22.316Z"
        );
        assert_eq!(v["pending"]["heldPasses"], 1);
        assert_eq!(v["pending"]["faultPasses"], 1);
        assert_eq!(v["pending"]["spendersExhausted"], false);
        assert_eq!(v["updatedAtMs"], 1_700_000_000_000i64);
        assert_eq!(v["version"], 7);
        state.advance_past_pending();
        let v = arcade_reorg_state_json(&state, Some(1), Some(8));
        assert_eq!(v["cursor"]["hash"], "ab".repeat(32));
        assert!(v["pending"].is_null());
        assert_eq!(
            v["unresolved"].as_array().map(Vec::len),
            Some(0),
            "nothing released: an empty list, never absent"
        );
        // round 3 (review MED): a released event is on the surface with its height, hash, reason and heal
        state.start(OrphanEvent {
            orphaned_at: "2026-09-07T22:48:27.809Z".into(),
            height: 965773,
            hash: "cd".repeat(32),
        });
        state.release_pending(overlay_discovery::pot::arcade_events::ReleaseReason::HeldCeiling);
        let v = arcade_reorg_state_json(&state, Some(1), Some(9));
        assert_eq!(v["unresolved"][0]["height"], 965773);
        assert_eq!(v["unresolved"][0]["hash"], "cd".repeat(32));
        assert_eq!(v["unresolved"][0]["orphanedAt"], "2026-09-07T22:48:27.809Z");
        assert_eq!(v["unresolved"][0]["why"], "held-ceiling");
        assert_eq!(
            v["unresolved"][0]["heal"],
            "POST /internal/reorg {\"fromHeight\": 965773, \"toHeight\": 965773}"
        );
        // every counter the consumer bumps is seeded on the surface
        let obj = json!({
            COUNTER_ARCADE_REORG_EVENTS: 0,
            COUNTER_ARCADE_REORG_REANCHORED: 0,
            COUNTER_ARCADE_REORG_DEMOTED: 0,
            COUNTER_ARCADE_REORG_UNCORROBORATED: 0,
            COUNTER_ARCADE_REORG_FAULTS: 0,
            COUNTER_ARCADE_REORG_UNRESOLVED: 0,
            COUNTER_ARCADE_REORG_TRACKER_LAGGING: 0,
            COUNTER_ARCADE_REORG_CONTENDED: 0,
            COUNTER_ARCADE_REORG_BUDGET_STOPS: 0,
        });
        for name in [
            "arcade_reorg_events_total",
            "arcade_reorg_reanchored_total",
            "arcade_reorg_demoted_total",
            "arcade_reorg_uncorroborated_total",
            "arcade_reorg_faults_total",
            "arcade_reorg_unresolved_total",
            "arcade_reorg_tracker_lagging_total",
            "arcade_reorg_contended_total",
            "arcade_reorg_budget_stops_total",
        ] {
            assert_eq!(obj[name], 0, "{name}");
        }
    }

    #[test]
    fn arc_ingest_push_health_separates_silent_from_refusing() {
        // THE defect this counter pair exists for: before it, these two rows
        // were the SAME observation (`{pushed: 0, statusIgnored: 0}`) and the
        // wrong one was believed for a month.
        assert_eq!(arc_ingest_push_health(&counters(0, 0, 0, 0)), "silent");
        assert_eq!(arc_ingest_push_health(&counters(0, 0, 9, 0)), "refusing");
        assert_eq!(arc_ingest_push_health(&counters(0, 0, 0, 9)), "refusing");
        assert_ne!(
            arc_ingest_push_health(&counters(0, 0, 0, 0)),
            arc_ingest_push_health(&counters(0, 0, 9, 0)),
        );
    }

    #[test]
    fn arc_ingest_push_health_is_flowing_once_anything_gets_past_auth() {
        // A verified proof push, an acknowledged status callback, or a mix with
        // some refusals — all "flowing": the push path works.
        assert_eq!(arc_ingest_push_health(&counters(1, 0, 0, 0)), "flowing");
        assert_eq!(arc_ingest_push_health(&counters(0, 1, 0, 0)), "flowing");
        assert_eq!(arc_ingest_push_health(&counters(5, 7, 3, 2)), "flowing");
    }

    /// bsv-low #660, real SQLite over the SHIPPED strings: latching a SETTLE
    /// txid (a pot row whose `spendingTxid` is that txid, whose own `txid`
    /// differs) notes the pot it SPENDS, so the app layer recomputes the
    /// winner's owed row after the witness; latching a FUNDING txid still
    /// notes the pot it funds. The spent read is one indexed lookup. To red:
    /// drop the spent read from the latch, or its notes.
    /// bsv-low #621 (L5): the writers of `network_seen` and the source each names. Every latch call in the crate
    /// passes a `SeenSource`, a type with BROADCASTER variants only (a courier's mempool presence is `announced`,
    /// never `seen`, #602), so the census is the five calls below and nothing else; a new writer, a moved one or a
    /// changed source reds here. The tracker event carries the same pair.
    #[test]
    fn i621_every_seen_latch_names_its_broadcaster() {
        // production code only: the text before the file's test module
        let squash = |s: &str| -> String {
            s[..s.find("\n#[cfg(test)]\nmod tests").unwrap_or(s.len())]
                .lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<String>()
                .split_whitespace()
                .collect()
        };
        let routes = squash(include_str!("routes.rs"));
        let jobs = squash(include_str!("admit_fast.rs"));
        let census: [(&str, &str, usize); 4] = [
            // the #413 dual push: the corroborator whose answer accepted the subject (TAAL, else GorillaPool)
            (routes.as_str(), "crate::ops::latch_network_seen(&dual_db,&dual_txid,by,None)", 1),
            // the gated accept: the broadcaster that said SEEN in this request (Arcade's poll, echo or probe; a corroborator)
            (routes.as_str(), "(Ok(db),Some((by,at_ms)))=>{crate::ops::latch_network_seen(&db,&subject_txid,by,at_ms).await}", 1),
            // the ungated arm's background look: Arcade's live GET, at its stamp
            (routes.as_str(), "crate::ops::latch_network_seen(&seen_db,&subject,crate::ops::SeenSource::Arcade,at_ms)", 1),
            // the callback's witness job and the pending watch: Arcade's live GET, at its stamp
            (jobs.as_str(), "crate::ops::SeenSource::Arcade,", 2),
        ];
        for (src, needle, n) in census {
            assert_eq!(src.matches(needle).count(), n, "{needle}");
        }
        let calls = routes.matches("crate::ops::latch_network_seen(").count()
            + jobs.matches("crate::ops::latch_network_seen(").count();
        assert_eq!(calls, 5, "five writers: three in routes.rs, two in admit_fast.rs");
        // the gated accept's witness is noted at every Accepted origin (poll, stored echo, probe, corroborators)
        let b = squash(include_str!("broadcaster.rs"));
        assert_eq!(b.matches("self.note_witness(Some(crate::ops::SeenSource::Arcade),").count(), 3);
        assert_eq!(b.matches("self.note_witness(by,None);").count(), 2);
        assert_eq!(b.matches("self.note_witness(last_by.get(),None);").count(), 1);
        // broadcasters only, by name
        let all = [SeenSource::Arcade, SeenSource::Taal, SeenSource::GorillaPool];
        assert_eq!(all.map(SeenSource::wire), ["arcade", "taal", "gorillapool"]);
        for s in all {
            match s {
                SeenSource::Arcade | SeenSource::Taal | SeenSource::GorillaPool => {}
            }
        }
        let latch = squash(include_str!("ops.rs"));
        assert!(latch.contains(
            "crate::tracker_events::note(txid,\"seen\",Some(serde_json::json!({\"source\":source.wire(),\"at\":at_ms})),);"
        ));
    }

    #[test]
    fn l25_the_seen_latch_notes_the_pots_the_txid_spends_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        let pot = "e160e530".repeat(8);
        let settle = "0e37aca0".repeat(8);
        let funding = "f0f0f0f0".repeat(8);
        let other = "a1a1a1a1".repeat(8);
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid) VALUES (?1, 0, 1, ?2)",
            rusqlite::params![pot, settle],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid) VALUES (?1, 0, 1, ?2)",
            rusqlite::params![other, "b2".repeat(32)],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent) VALUES (?1, 1, 0)",
            rusqlite::params![funding],
        )
        .unwrap();
        // The latch's two reads, as shipped, then its notes.
        let latch = |txid: &str| -> Vec<(String, u32)> {
            let funded: Vec<PotVoutRow> = conn
                .prepare(NETWORK_SEEN_POT_ROWS_SQL)
                .unwrap()
                .query_map(rusqlite::params![txid], |r| {
                    Ok(PotVoutRow {
                        output_index: r.get(0)?,
                    })
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            let spent: Vec<PotOutpointRow> = conn
                .prepare(NETWORK_SEEN_SPENT_POT_ROWS_SQL)
                .unwrap()
                .query_map(rusqlite::params![txid], |r| {
                    Ok(PotOutpointRow {
                        txid: r.get(0)?,
                        output_index: r.get(1)?,
                    })
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            crate::pot_changes::drain();
            note_witnessed_pots(txid, &funded, &spent);
            crate::pot_changes::drain()
        };
        assert_eq!(
            latch(&settle.to_ascii_uppercase()),
            vec![(pot.clone(), 0)],
            "a settle's SEEN latch notes the pot it spends (bsv-low #660)"
        );
        assert_eq!(
            latch(&funding),
            vec![(funding.clone(), 1)],
            "a funding's SEEN latch still notes the pot it funds"
        );
        assert!(
            latch(&"c3".repeat(32)).is_empty(),
            "a txid that funds and spends no pot notes nothing"
        );
        // One indexed read by idx_pot_spending itself, never a scan of
        // pot_records and never its sibling idx_pot_spending_lower (the
        // index name is matched with its opening parenthesis).
        let plan: Vec<String> = conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {NETWORK_SEEN_SPENT_POT_ROWS_SQL}"
            ))
            .unwrap()
            .query_map(rusqlite::params![settle], |r| r.get::<_, String>(3))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(
            !plan.is_empty()
                && plan.iter().all(|d| {
                    d.contains("USING INDEX idx_pot_spending (")
                        || d.contains("USING COVERING INDEX idx_pot_spending (")
                }),
            "the spent read uses idx_pot_spending: {plan:?}"
        );
        // The shipped latch makes both reads and notes both sets.
        let src = include_str!("ops.rs");
        let latch_fn = &src[src.find("pub async fn latch_network_seen(").unwrap()..];
        let latch_fn = &latch_fn[..latch_fn.find("\n}\n").unwrap()];
        for needle in [
            "Query::new(NETWORK_SEEN_POT_ROWS_SQL)",
            "Query::new(NETWORK_SEEN_SPENT_POT_ROWS_SQL)",
            "note_witnessed_pots(txid, &funded, &spent);",
            "[#660] funded-pot read failed for {txid}: {e}",
            "[#660] spent-pot read failed for {txid}: {e}",
        ] {
            assert!(latch_fn.contains(needle), "the latch: {needle}");
        }
    }

    /// bsv-low handoff #2b, real SQLite over the SHIPPED strings: the
    /// proofless watch neither ENROLS a retired (structurally-unprovable)
    /// row nor keeps one it already holds — `prooflessOver24h` counts
    /// GENUINE backlog only, while NULL (never-examined) rows keep
    /// enrolling (fail-safe: watch MORE).
    #[test]
    fn proofless_watch_excludes_retired_rows_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        conn.execute_batch(
            "INSERT INTO pot_beefs (txid, beef, has_proof, structurally_unprovable) \
             VALUES ('retired', x'beef', 0, 1); \
             INSERT INTO pot_beefs (txid, beef, has_proof) VALUES ('backlog', x'beef', 0); \
             INSERT INTO transactions (txid, beef, has_proof) VALUES ('enginetx', x'beef', 0);",
        )
        .unwrap();

        // Enrol pages (the shipped per-table strings): the retired row is
        // excluded; the NULL-latch row and the engine row enrol.
        for table in ["pot_beefs", "transactions"] {
            conn.execute(
                &proofless_watch_enrol_sql(table),
                rusqlite::params![1_000i64],
            )
            .unwrap();
        }
        let watched = |txid: &str| -> bool {
            conn.query_row(
                "SELECT COUNT(*) FROM proofless_watch WHERE txid = ?",
                rusqlite::params![txid],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
                > 0
        };
        assert!(!watched("retired"), "a retired row is residue, not backlog");
        assert!(watched("backlog"), "a NULL-latch row still enrols");
        assert!(watched("enginetx"));

        // A row retired AFTER enrolment is dropped by the GC (or the 24h
        // flag would keep conflating residue with backlog forever).
        conn.execute(
            "UPDATE pot_beefs SET structurally_unprovable = 1 WHERE txid = 'backlog'",
            [],
        )
        .unwrap();
        conn.execute(PROOFLESS_WATCH_GC_SQL, []).unwrap();
        assert!(!watched("backlog"), "the GC drops retired rows");
        assert!(watched("enginetx"), "genuine backlog survives the GC");

        // INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — the two new arms.
        //
        // (1) A `transactions` row retired on corroborated network-death
        // (`retired_ms`) stops enrolling AND is dropped if already watched —
        // the 11 UTXO_SPENT double-spends sat flagged for weeks.
        conn.execute(
            "UPDATE transactions SET retired_ms = 1, retired_reason = 'test' \
             WHERE txid = 'enginetx'",
            [],
        )
        .unwrap();
        conn.execute(PROOFLESS_WATCH_GC_SQL, []).unwrap();
        assert!(
            !watched("enginetx"),
            "a retired transactions row is residue, not backlog"
        );
        conn.execute(
            &proofless_watch_enrol_sql("transactions"),
            rusqlite::params![2_000i64],
        )
        .unwrap();
        assert!(
            !watched("enginetx"),
            "a retired transactions row never re-enrols"
        );
        // (2) ORPHANS: a watch row whose store row was deleted (displacement
        // / cleanup) could NEVER leave under the proven-only GC — 9 of beta's
        // 27 flagged rows were exactly this zombie class. The orphan arm
        // drops it; rows with a live store row are untouched.
        conn.execute_batch(
            "INSERT INTO proofless_watch (txid, first_seen_ms) VALUES ('ghost', 1); \
             INSERT INTO pot_beefs (txid, beef, has_proof) VALUES ('alive', x'beef', 0); \
             INSERT INTO proofless_watch (txid, first_seen_ms) VALUES ('alive', 1);",
        )
        .unwrap();
        conn.execute(PROOFLESS_WATCH_GC_SQL, []).unwrap();
        assert!(!watched("ghost"), "an orphan watch row is GC'd");
        assert!(
            watched("alive"),
            "a store-backed row survives the orphan arm"
        );
    }

    /// INCIDENT D1-CALLBACK-FLOOD 2026-09-01: the status-ignored counter's
    /// exact-head-then-batch fold — the first [`STATUS_IGNORED_EXACT_HEAD`]
    /// events each write (the route tier's Rule-13 pin: an authorized
    /// callback visibly counts), then writes happen once per
    /// [`STATUS_IGNORED_FLUSH_BATCH`] with no events lost between flushes.
    #[test]
    fn status_ignored_fold_exact_head_then_batch() {
        let mut state = (0u32, 0u32);
        let mut written = 0u64;
        // Head: every event flushes exactly 1.
        for i in 1..=STATUS_IGNORED_EXACT_HEAD {
            let (next, flush) = status_ignored_step(state);
            state = next;
            assert_eq!(flush, 1, "head event {i} must count exactly");
            written += flush;
        }
        assert_eq!(written, u64::from(STATUS_IGNORED_EXACT_HEAD));
        // Past the head: silence until the batch fills, then one flush of the
        // full accumulation — nothing lost.
        let mut post_head_events = 0u64;
        let mut flushed_after_head = 0u64;
        loop {
            let (next, flush) = status_ignored_step(state);
            state = next;
            post_head_events += 1;
            flushed_after_head += flush;
            if flush > 0 {
                assert_eq!(
                    flush,
                    u64::from(STATUS_IGNORED_FLUSH_BATCH),
                    "the batch flush carries the whole accumulation"
                );
                break;
            }
        }
        assert_eq!(post_head_events, u64::from(STATUS_IGNORED_FLUSH_BATCH));
        assert_eq!(flushed_after_head, post_head_events, "no event is lost");
        // Saturation safety at absurd lifetimes.
        let (_, flush) = status_ignored_step((u32::MAX, 0));
        assert_eq!(flush, 0);
    }

    #[test]
    fn arc_ingest_push_health_reads_the_names_read_counters_serves() {
        // Rule 16 (pin the boundary, not the sides): the verdict is computed
        // off the object `read_counters` builds. A name that is ABSENT reads as
        // 0 and can therefore only ever produce "silent" — the exact way this
        // field could rot back into the pre-fix ambiguity. Asserted, not
        // assumed, so a rename that misses one side goes red here.
        let mut c = counters(0, 0, 4, 0);
        assert_eq!(arc_ingest_push_health(&c), "refusing");
        c.as_object_mut()
            .unwrap()
            .remove(COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN);
        assert_eq!(arc_ingest_push_health(&c), "silent");
    }
}
