//! The schema statements this read-only Worker issues, and why a read-only
//! Worker issues any at all (bsv-low #283, extended by #362).
//!
//! # The outage this exists to remove
//!
//! `low-app-layer` is a SEPARATE Cloudflare Worker from the overlay, over the
//! SAME D1 database, and it has never run migrations — `OVERLAY_MIGRATIONS`
//! appears nowhere in this crate outside `tests/`. The two latch columns
//! (`potparty_records.sigValid`, the #283 latch; `hopparty_records.markerValid`,
//! the #362 one) are applied by the OVERLAY worker's
//! `ensure_overlay_migrations`, and that runs **on cold start, not at
//! deploy**.
//!
//! So there is a window with no operator error in it at all: deploy this
//! worker first, or deploy both and have a request reach this one before any
//! request has warmed the overlay, and every query referencing `pp.sigValid`
//! or `hp.markerValid` fails to PREPARE with `no such column`. That is
//! `/results`, `/recovery-view`, `/refund-view`, `/live-view` and
//! `/hops-view` — every recovery surface — returning 500 at once. Strictly
//! worse than the marker-flood eviction the columns defend against (epoch
//! Rule 6: never trade a self-healing failure for a total one), and a
//! deploy-order NOTE would not have fixed it, because the hazard is a cold
//! isolate rather than an ordering an operator controls.
//!
//! # The fix, and why it is the additive ALTER rather than a probe
//!
//! Three shapes were available:
//!
//! 1. **A capability probe + a fallback rank expression** — read-both /
//!    write-new (epoch Rule 14). Correct, and it makes eight pure SQL-builder
//!    functions read process-global state, which is how a suite becomes
//!    order-dependent and a builder becomes untestable in isolation.
//! 2. **A deploy-order runbook note.** Does not survive a cold isolate.
//! 3. **This.** Issue the same additive, idempotent statements the overlay's
//!    own migration list issues, once per isolate, and IGNORE the
//!    duplicate-column error exactly as `run_migrations` does.
//!
//! (3) is chosen because the failure modes are not close. If it runs first,
//! the column exists and the overlay's migration is the no-op. If the overlay
//! ran first, this is the no-op. If BOTH run concurrently, one gets
//! "duplicate column" and ignores it — the same race the overlay already
//! tolerates across its own isolates. There is no ordering in which a column
//! is dropped, renamed or re-typed, because `ALTER TABLE … ADD COLUMN` is the
//! only DDL here and D1 migrations in this project are append-only.
//!
//! **Scope, stated so it does not creep:** this is not a migration runner and
//! must not become one. It is ONE STATEMENT PER COLUMN (or table) this worker
//! READS and cannot serve without — the [`LATCH_COLUMN_ALTERS`] and
//! [`CREATE_TABLE_CATCHUPS`] lists, six ALTERs + three CREATEs as of
//! brain-cutover M1 — and nothing else.
//!
//! The membership test is "does a shipped query in this crate NAME the
//! column", not "is it a verdict latch": #217's `firstSpentAt` is a plain
//! timestamp rather than a latch, but `/refund-view` selects it, and a
//! `SELECT` naming an absent column fails to PREPARE exactly as
//! `pp.sigValid` did — same outage, same remedy, so the list is named for
//! the hazard rather than for the two columns that happened to arrive first.
//! There is no ordering, no
//! versioning and no failure bookkeeping here; every statement is an
//! independent `ALTER TABLE … ADD COLUMN` whose only outcomes are "added" and
//! "already there". Anything richer belongs in the overlay's
//! `OVERLAY_MIGRATIONS`, which owns the schema.
//!
//! # Why the call site is a COMPILE error to remove (gate round 2, MED-3)
//!
//! Round 1 landed the fix and left its own call site unobservable, which is
//! epoch Rule 22 in its purest form: the second gate wrapped
//! `ensure_sig_valid_column` in `if false { … }`, it compiled, and the suite
//! stayed green (`42 passed`, `3 passed`) while production would have served
//! exactly the outage described above — `/results`, `/recovery-view`,
//! `/refund-view` and `/live-view` all 500 on a cold app-layer isolate.
//! "Deploy order closed in code" was true of the code and untrue of the
//! coverage.
//!
//! A source-scanning pin cannot see that injection: `if false { call(); }`
//! leaves every needle matching. So the fix removes the CHOICE instead
//! (Rule 15), the same move the sibling bsv-low#347 lane used for its route:
//! [`ensure_latch_columns`] returns a [`LatchColumnsEnsured`], the type
//! has a private field and exactly one constructor, and `router()` demands
//! one. Deleting the call leaves the router without an argument it cannot
//! obtain any other way; `if false { … }` leaves the binding unassigned.
//! Both are build failures, which is strictly stronger than a failing test.
//!
//! **Boundary, stated at the pin (Rule 22):** this makes the CALL mandatory
//! on every request. It does not make the D1 round-trip observable natively
//! — nothing in this crate can, `D1Database` has no native constructor. What
//! IS pinned natively is the statement's byte-identity with the migration
//! that owns it and the exact error class this treats as benign.

use std::sync::atomic::{AtomicBool, Ordering};

/// The #283 potparty latch column — byte-identical to its migration in the
/// overlay's `OVERLAY_MIGRATIONS`. Pinned equal by
/// `every_app_layer_alter_is_byte_identical_to_its_overlay_migration`, so the
/// two cannot drift into a column this worker adds and the overlay never
/// writes (epoch Rule 16: share the artifact, not the convention).
pub const SIG_VALID_ALTER: &str = "ALTER TABLE potparty_records ADD COLUMN sigValid INTEGER";

/// The #362 hopparty latch column, same contract. `/hops-view` reads
/// `hp.markerValid` in its LEADING sort key, so without this statement a
/// cold app-layer isolate 503s that route outright (epoch Rule 24: a column
/// one service READS must be added by a statement that service can also
/// ISSUE).
pub const MARKER_VALID_ALTER: &str = "ALTER TABLE hopparty_records ADD COLUMN markerValid INTEGER";

/// The #217 durable hand-END anchor, same contract. NOT a latch — it is the
/// write-once unix-second stamp of the first accepted spend pointer, which
/// `/refund-view` serves in its `timeline` block. It is here for the identical
/// reason the two latches are: `refund_view_sql` names `r.firstSpentAt`, so a
/// cold app-layer isolate against a schema the overlay has not warmed fails to
/// PREPARE and 503s the route outright (epoch Rule 24: a column one service
/// READS must be added by a statement that service can also ISSUE).
pub const FIRST_SPENT_AT_ALTER: &str = "ALTER TABLE pot_records ADD COLUMN firstSpentAt INTEGER";

/// The bsv-low #371 spender bytes-finality column, same contract: the three
/// money views name `r.spenderFinal` in the verdict-gate third arm, so a cold
/// app-layer isolate against an unwarmed schema fails to PREPARE without it
/// (epoch Rule 24).
pub const SPENDER_FINAL_ALTER: &str = "ALTER TABLE pot_records ADD COLUMN spenderFinal INTEGER";

/// The brain-cutover M1 result-claim TIER latch, same Rule-24 contract:
/// `/results` and `/leaderboard` read `claimValid` in the dual-arm
/// `verified_claim` (latch when present, compute when NULL), so a cold
/// app-layer isolate against an unwarmed schema fails to PREPARE without it.
/// NULL = unswept (compute-at-serve), 0 = invalid, 1 = winner-valid, 2 =
/// countersigned — the tier the overlay's `result::validity::claim_tier`
/// latches at admission and the relatch sweep repairs.
pub const CLAIM_VALID_ALTER: &str = "ALTER TABLE result_markers_v2 ADD COLUMN claimValid INTEGER";

/// The brain-cutover M1 hand-marker latch, same contract — read by the M2
/// `/results` hands join (`hand::validity::row_valid`'s verdict).
pub const ROW_VALID_ALTER: &str = "ALTER TABLE hand_markers ADD COLUMN rowValid INTEGER";

/// Every ALTER this module issues. Each is independently idempotent; the ONE
/// ordering rule is module-level (gate F5): the CREATE catch-ups run first,
/// because `ROW_VALID_ALTER` targets a table `HAND_MARKERS_CREATE` may be
/// creating on the same cold isolate.
pub const LATCH_COLUMN_ALTERS: &[&str] = &[
    SIG_VALID_ALTER,
    MARKER_VALID_ALTER,
    FIRST_SPENT_AT_ALTER,
    SPENDER_FINAL_ALTER,
    CLAIM_VALID_ALTER,
    ROW_VALID_ALTER,
];

/// The bsv-low #371 `network_seen` TABLE, same Rule-24 contract as the
/// columns above but a different statement class: the three money views
/// LEFT JOIN `network_seen`, and a `SELECT` naming an absent TABLE fails to
/// PREPARE exactly as an absent column does. `CREATE TABLE IF NOT EXISTS` is
/// idempotent BY CONSTRUCTION (re-running it against an existing table
/// succeeds — no benign-error predicate is needed on either side), additive
/// (no DROP/RENAME/re-type), and byte-identical to the owning overlay
/// migration (pinned by `the_create_table_catchup_is_byte_identical…`).
/// The membership rule is unchanged: a shipped query in this crate names it.
pub const NETWORK_SEEN_CREATE: &str =
    "CREATE TABLE IF NOT EXISTS network_seen (txid TEXT PRIMARY KEY, seenAt INTEGER)";

/// The #252 stage-A `collected_markers_v2` catch-up, same Rule-24 contract as
/// [`NETWORK_SEEN_CREATE`]: `/recovery-view`'s `collected` presence query
/// names the table, and a `SELECT` naming an absent table fails to PREPARE on
/// a cold app-layer isolate against an unwarmed schema. (That leg is
/// BEST-EFFORT — it would serve `collected: null` rather than 503 — but
/// Rule 24's bar is by CONSTRUCTION, not by luck of the caller's posture,
/// and the module's membership rule is "a shipped query names it".)
/// Byte-identical to the owning overlay migration, pinned below. NOTE the
/// column defs carry `NOT NULL` — inert in a CREATE-class statement (`IF NOT
/// EXISTS` no-ops against an existing table and a CREATE never mutates
/// existing rows), unlike an `ADD COLUMN NOT NULL`, which is why the
/// ALTER-class ban on `NOT NULL` does not apply here.
pub const COLLECTED_MARKERS_V2_CREATE: &str = "CREATE TABLE IF NOT EXISTS collected_markers_v2 (
        identity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        sigHex TEXT,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )";

/// The #382 `hand_markers` catch-up (brain-cutover M1) — the owning overlay
/// migration's comment predicted this exact statement: "if /results later
/// joins this table, the app-layer must issue this same statement". The M2
/// hands join names it, and [`ROW_VALID_ALTER`] above adds its latch column
/// (CREATE-before-ALTER ordered by `apply` (gate F5): the ALTER's duplicate-column outcome is benign and a
/// CREATE against an existing table is `IF NOT EXISTS`-idempotent).
/// Byte-identical to the overlay migration, pinned below.
pub const HAND_MARKERS_CREATE: &str = "CREATE TABLE IF NOT EXISTS hand_markers (
        gameId TEXT NOT NULL,
        identity TEXT NOT NULL,
        potTxid TEXT NOT NULL,
        cardsHex TEXT NOT NULL,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        sigHex TEXT,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )";

/// Every CREATE-class catch-up this module issues. `IF NOT EXISTS` makes
/// success the only benign outcome — any error is real and defers the latch.
pub const CREATE_TABLE_CATCHUPS: &[&str] = &[
    NETWORK_SEEN_CREATE,
    COLLECTED_MARKERS_V2_CREATE,
    HAND_MARKERS_CREATE,
];

/// Set once THIS isolate has issued (or knowingly skipped) EVERY statement.
///
/// All-or-nothing on purpose: a partial success must retry the whole (tiny)
/// list rather than book-keep per statement, because a statement that
/// already exists is a no-op and re-issuing it costs one round trip on a
/// cold isolate only.
static APPLIED: AtomicBool = AtomicBool::new(false);

/// Is a D1 error the benign "this ALTER already ran" case?
///
/// Kept as a pure predicate so it is testable without a D1 binding — the same
/// reason `potparty_insert_query` exists on the writer side. D1 surfaces
/// SQLite's message; matching is case-insensitive and substring-based because
/// the wrapper's prefix has changed across `worker` versions.
///
/// # Narrowed to DUPLICATE COLUMN only (gate round 2, LOW-4)
///
/// This used to also bless anything containing `"already exists"`, which is
/// broader than the overlay's `d1::migration_error_is_benign` and wrong in a
/// specific way: `"table potparty_records already exists"` says nothing about
/// the COLUMN, and swallowing it would latch [`APPLIED`] on an isolate where
/// `sigValid` is still absent — turning a self-healing retry into a permanent
/// 500 for that isolate (epoch Rule 6). The only statement this module issues
/// is [`SIG_VALID_ALTER`], so "duplicate column" is the ONLY benign outcome,
/// and the predicate is now pinned to agree with the overlay's on a shared
/// message table (`the_two_benign_error_predicates_agree`).
pub fn is_duplicate_column_error(msg: &str) -> bool {
    msg.to_ascii_lowercase().contains("duplicate column")
}

/// Proof that THIS isolate has issued (or knowingly skipped) the catch-up.
///
/// Private field, one constructor: the only way to obtain a value is to call
/// [`ensure_latch_columns`]. `router()` takes one, so deleting the call —
/// or wrapping it in `if false { … }`, the exact injection the second gate
/// used — is a BUILD failure rather than a green suite. See the module doc.
pub struct LatchColumnsEnsured(());

/// Ensure every column in [`LATCH_COLUMN_ALTERS`] exists, at most once per
/// isolate.
///
/// Takes the whole `Env` rather than a `D1Database` so the binding lookup is
/// inside the mandatory call: a caller cannot satisfy the router by skipping
/// the lookup and passing nothing.
///
/// NEVER fails the request. A genuine error is logged and the flag is left
/// unset so the next request retries: the caller is a recovery surface, and a
/// recovery surface must degrade toward working (epoch Rule 4b). If the
/// column genuinely cannot be added, the query below it fails on its own and
/// says so — this call does not get to decide that. A missing `OVERLAY_DB`
/// binding is likewise not this function's problem to report; every route
/// that needs the database says so itself.
pub async fn ensure_latch_columns(env: &worker::Env) -> LatchColumnsEnsured {
    if !APPLIED.load(Ordering::Acquire) {
        if let Ok(db) = env.d1("OVERLAY_DB") {
            apply(&db).await;
        }
    }
    LatchColumnsEnsured(())
}

/// Issue every [`LATCH_COLUMN_ALTERS`] statement once, ignoring only the
/// duplicate-column report.
///
/// `prepare(...).run()` — NOT `exec` (gate round 2, LOW-4). The overlay's
/// migration runner is `Query::execute`, which is `db.prepare(sql).run()`, so
/// the two services now issue the same statement through the same D1 API and
/// the byte-identity pin covers the EXECUTION PATH as well as the statement
/// (epoch Rule 24). The two crates still pin different `worker` majors (0.8
/// here, 0.7.5 there), so the error TEXT can differ across versions — which
/// is why the predicates are pinned to agree rather than assumed to.
async fn apply(db: &worker::D1Database) {
    let mut all_ok = true;
    // CREATEs FIRST (gate F5): `ROW_VALID_ALTER` targets `hand_markers`,
    // which `HAND_MARKERS_CREATE` may be the first to create on this D1 — an
    // ALTER against a missing table fails not-benign ("no such table"), which
    // self-heals on the next request but is one avoidable deferred cycle.
    // This is the module's first in-list dependency; the per-statement
    // idempotence claims below are per-statement, not cross-statement.
    for stmt in CREATE_TABLE_CATCHUPS {
        if let Err(e) = db.prepare(*stmt).run().await {
            all_ok = false;
            worker::console_log!("[schema] CREATE deferred: {stmt} — {e}");
        }
    }
    for stmt in LATCH_COLUMN_ALTERS {
        match db.prepare(*stmt).run().await {
            Ok(_) => {}
            Err(e) => {
                let msg = e.to_string();
                if !is_duplicate_column_error(&msg) {
                    // Not benign: leave the flag unset so the NEXT request
                    // retries the whole list. A recovery surface must degrade
                    // toward working (epoch Rule 4b), and latching APPLIED on
                    // a real failure would turn one transient error into a
                    // permanent 500 for this isolate (Rule 6).
                    all_ok = false;
                    worker::console_log!("[schema] ALTER deferred: {stmt} — {msg}");
                }
            }
        }
    }
    if all_ok {
        APPLIED.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each app-layer statement must be EXACTLY the overlay migration that
    /// owns its column. A drift here would have this worker add a column the
    /// overlay never writes — the latch would read NULL forever and the
    /// change would be silently inoperative in production while every test
    /// stayed green.
    ///
    /// Driven off [`LATCH_COLUMN_ALTERS`] rather than a hand-listed pair, so
    /// a third column added to that list without a matching migration fails
    /// here instead of shipping (epoch Rule 24).
    #[test]
    fn every_app_layer_alter_is_byte_identical_to_its_overlay_migration() {
        assert_eq!(
            LATCH_COLUMN_ALTERS.len(),
            6,
            "sigValid (#283), markerValid (#362), firstSpentAt (#217), \
             spenderFinal (#371), claimValid + rowValid (brain-cutover M1)"
        );
        for stmt in LATCH_COLUMN_ALTERS {
            let column = stmt
                .rsplit("ADD COLUMN ")
                .next()
                .and_then(|t| t.split_whitespace().next())
                .expect("every statement is an ADD COLUMN");
            let hits: Vec<&&str> = bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .filter(|m| m.contains(column))
                .collect();
            assert_eq!(hits.len(), 1, "exactly one migration owns {column}");
            assert_eq!(
                *hits[0], *stmt,
                "the app-layer ALTER and the overlay migration must be the \
                 same statement for {column}"
            );
        }
    }

    /// ADDITIVE ONLY, asserted on the CONSTRUCT. D1 migrations re-execute on
    /// every cold start, so a DROP/RENAME/re-type here would run repeatedly
    /// against live data. `ADD COLUMN` is the only shape that survives being
    /// run an unbounded number of times in an unknown order, and the columns
    /// must stay NULLABLE — a pre-migration NULL has to remain observable
    /// because it means "never evaluated", not "refuted".
    #[test]
    fn every_alter_is_additive_and_nullable() {
        for stmt in LATCH_COLUMN_ALTERS {
            assert!(stmt.starts_with("ALTER TABLE "), "{stmt}");
            assert!(stmt.contains(" ADD COLUMN "), "{stmt}");
            let upper = stmt.to_ascii_uppercase();
            for banned in ["DROP", "RENAME", "NOT NULL", "DEFAULT", ";"] {
                assert!(!upper.contains(banned), "{stmt} must not contain {banned}");
            }
        }
    }

    /// The CREATE-class catch-ups (#371 `network_seen`, #252
    /// `collected_markers_v2`): each byte-identical to the owning overlay
    /// migration (epoch Rule 24 — share the artifact, not the convention),
    /// idempotent by construction (`IF NOT EXISTS`), and additive (no
    /// DROP/RENAME, single statement). Driven off [`CREATE_TABLE_CATCHUPS`]
    /// so a third table added without a matching migration fails here.
    ///
    /// The owning migration is matched as "the migration that CREATEs this
    /// table" — `collected_markers_v2` is also NAMED by its carry-INSERT and
    /// index migrations, so a bare substring filter would match three.
    ///
    /// `NOT NULL` is deliberately NOT banned here (unlike the ALTER class):
    /// inside `CREATE TABLE IF NOT EXISTS` it defines new-table columns and
    /// never touches existing rows — the statement no-ops when the table
    /// exists. `DEFAULT` likewise. The additive bans that DO matter for a
    /// re-run-forever statement (DROP/RENAME/multi-statement) are asserted.
    #[test]
    fn every_create_table_catchup_is_byte_identical_to_its_overlay_migration() {
        assert_eq!(
            CREATE_TABLE_CATCHUPS.len(),
            3,
            "network_seen (#371), collected_markers_v2 (#252 stage A), \
             hand_markers (#382, mirrored for the brain-cutover M2 join)"
        );
        for stmt in CREATE_TABLE_CATCHUPS {
            assert!(stmt.starts_with("CREATE TABLE IF NOT EXISTS "), "{stmt}");
            let table = stmt["CREATE TABLE IF NOT EXISTS ".len()..]
                .split([' ', '('])
                .next()
                .expect("a table name follows the prefix");
            let hits: Vec<&&str> = bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
                .iter()
                .filter(|m| m.starts_with(&format!("CREATE TABLE IF NOT EXISTS {table}")))
                .collect();
            assert_eq!(hits.len(), 1, "exactly one migration CREATEs {table}");
            assert_eq!(
                *hits[0], *stmt,
                "the app-layer CREATE and the overlay migration must be the \
                 same statement for {table}"
            );
            let upper = stmt.to_ascii_uppercase();
            for banned in ["DROP", "RENAME", ";"] {
                assert!(!upper.contains(banned), "{stmt} must not contain {banned}");
            }
        }
    }

    /// Every message either side of the boundary might see, in one place, so
    /// the two predicates below are measured against the SAME table.
    const BENIGN: &[&str] = &[
        "duplicate column name: sigValid",
        "duplicate column name: markerValid",
        "D1_ERROR: duplicate column name: sigValid",
        "Duplicate Column Name",
    ];

    /// `"table potparty_records already exists"` sits here deliberately: it
    /// is the message the old, broader predicate blessed, and it does NOT
    /// mean the column arrived.
    const NOT_BENIGN: &[&str] = &[
        "no such table: potparty_records",
        "table potparty_records already exists",
        "D1_ERROR: network error",
        "database is locked",
        "",
    ];

    /// Only the benign case is benign. A real failure must NOT latch
    /// `APPLIED`, or one transient error would permanently stop the retry.
    #[test]
    fn duplicate_column_is_benign_and_nothing_else_is() {
        for msg in BENIGN {
            assert!(is_duplicate_column_error(msg), "{msg}");
        }
        for msg in NOT_BENIGN {
            assert!(!is_duplicate_column_error(msg), "{msg}");
        }
    }

    /// PIN THE BOUNDARY, not just its two sides (epoch Rule 16 / Rule 24).
    ///
    /// The byte-identity cell above pins the STATEMENT. It says nothing about
    /// the two hand-written predicates that decide, on each side, whether a
    /// re-run of that statement failed benignly. The second gate found the
    /// app-layer's was strictly broader — it blessed
    /// `"table potparty_records already exists"`, which does not mean the
    /// column is present and would have latched `APPLIED` wrongly.
    ///
    /// Not a live defect at the time (each side accepted the other's
    /// duplicate-column report, checked both directions), but two
    /// hand-maintained copies of one decision drift — the whole reason Rule
    /// 24 demands the equality be asserted rather than reviewed. Driven
    /// through the overlay's REAL predicate with the REAL statement.
    #[test]
    fn the_two_benign_error_predicates_agree() {
        use bsv_overlay_cloudflare::d1::migration_error_is_benign;
        // Across EVERY statement this module issues, not just the first —
        // the overlay's predicate takes the statement as an argument, so a
        // pin scoped to one of them proves nothing about the others.
        for stmt in LATCH_COLUMN_ALTERS {
            for msg in BENIGN.iter().chain(NOT_BENIGN.iter()) {
                assert_eq!(
                    is_duplicate_column_error(msg),
                    migration_error_is_benign(stmt, msg),
                    "the app-layer and the overlay must classify {msg:?} the same \
                     way for {stmt} — they issue the same statement against the \
                     same D1"
                );
            }
        }
        // …and the shared table is not vacuous in either direction.
        assert!(
            BENIGN.iter().any(|m| is_duplicate_column_error(m)),
            "positive control: at least one message is benign"
        );
        assert!(
            NOT_BENIGN.iter().any(|m| !is_duplicate_column_error(m)),
            "negative control: at least one message is not"
        );
    }
}
