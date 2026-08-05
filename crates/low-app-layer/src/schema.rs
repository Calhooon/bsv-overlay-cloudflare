//! The ONE schema statement this read-only Worker issues, and why a read-only
//! Worker issues one at all (bsv-low #283).
//!
//! # The outage this exists to remove
//!
//! `low-app-layer` is a SEPARATE Cloudflare Worker from the overlay, over the
//! SAME D1 database, and it has never run migrations — `OVERLAY_MIGRATIONS`
//! appears nowhere in this crate outside `tests/`. Migration 98
//! (`potparty_records.sigValid`, the #283 latch) is applied by the OVERLAY
//! worker's `ensure_overlay_migrations`, and that runs **on cold start, not at
//! deploy**.
//!
//! So there is a window with no operator error in it at all: deploy this
//! worker first, or deploy both and have a request reach this one before any
//! request has warmed the overlay, and every recovery query referencing
//! `pp.sigValid` fails to PREPARE with `no such column`. That is `/results`,
//! `/recovery-view`, `/refund-view` and `/live-view` — every recovery surface
//! — returning 500 at once. Strictly worse than the marker-flood eviction the
//! column defends against (epoch Rule 6: never trade a self-healing failure
//! for a total one), and a deploy-order NOTE would not have fixed it, because
//! the hazard is a cold isolate rather than an ordering an operator controls.
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
//! 3. **This.** Issue the same additive, idempotent statement the overlay's
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
//! must not become one. It is one statement, for the one column this worker
//! reads and cannot serve without. Anything else belongs in the overlay's
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
//! [`ensure_sig_valid_column`] returns a [`SigValidColumnEnsured`], the type
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

/// The statement — byte-identical to migration 98 in the overlay's
/// `OVERLAY_MIGRATIONS`. Pinned equal by
/// `the_app_layer_alter_is_byte_identical_to_the_overlay_migration`, so the
/// two cannot drift into a column this worker adds and the overlay never
/// writes (epoch Rule 16: share the artifact, not the convention).
pub const SIG_VALID_ALTER: &str = "ALTER TABLE potparty_records ADD COLUMN sigValid INTEGER";

/// Set once THIS isolate has issued (or knowingly skipped) the statement.
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
/// [`ensure_sig_valid_column`]. `router()` takes one, so deleting the call —
/// or wrapping it in `if false { … }`, the exact injection the second gate
/// used — is a BUILD failure rather than a green suite. See the module doc.
pub struct SigValidColumnEnsured(());

/// Ensure `potparty_records.sigValid` exists, at most once per isolate.
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
pub async fn ensure_sig_valid_column(env: &worker::Env) -> SigValidColumnEnsured {
    if !APPLIED.load(Ordering::Acquire) {
        if let Ok(db) = env.d1("OVERLAY_DB") {
            apply(&db).await;
        }
    }
    SigValidColumnEnsured(())
}

/// Issue [`SIG_VALID_ALTER`] once, ignoring only the duplicate-column report.
///
/// `prepare(...).run()` — NOT `exec` (gate round 2, LOW-4). The overlay's
/// migration runner is `Query::execute`, which is `db.prepare(sql).run()`, so
/// the two services now issue the same statement through the same D1 API and
/// the byte-identity pin covers the EXECUTION PATH as well as the statement
/// (epoch Rule 24). The two crates still pin different `worker` majors (0.8
/// here, 0.7.5 there), so the error TEXT can differ across versions — which
/// is why the predicates are pinned to agree rather than assumed to.
async fn apply(db: &worker::D1Database) {
    match db.prepare(SIG_VALID_ALTER).run().await {
        Ok(_) => {
            APPLIED.store(true, Ordering::Release);
        }
        Err(e) => {
            let msg = e.to_string();
            if is_duplicate_column_error(&msg) {
                APPLIED.store(true, Ordering::Release);
            } else {
                worker::console_log!("[schema] sigValid ALTER deferred: {msg}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The app-layer's statement must be EXACTLY the overlay's migration 98.
    /// A drift here would have this worker add a column the overlay never
    /// writes — the latch would read NULL forever and #283 would be silently
    /// inoperative in production while every test stayed green.
    #[test]
    fn the_app_layer_alter_is_byte_identical_to_the_overlay_migration() {
        let hits: Vec<&&str> = bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
            .iter()
            .filter(|m| m.contains("sigValid"))
            .collect();
        assert_eq!(hits.len(), 1, "exactly one migration owns the latch column");
        assert_eq!(
            *hits[0], SIG_VALID_ALTER,
            "the app-layer ALTER and the overlay migration must be the same statement"
        );
    }

    /// Every message either side of the boundary might see, in one place, so
    /// the two predicates below are measured against the SAME table.
    const BENIGN: &[&str] = &[
        "duplicate column name: sigValid",
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
        for msg in BENIGN.iter().chain(NOT_BENIGN.iter()) {
            assert_eq!(
                is_duplicate_column_error(msg),
                migration_error_is_benign(SIG_VALID_ALTER, msg),
                "the app-layer and the overlay must classify {msg:?} the same \
                 way — they issue the same statement against the same D1"
            );
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
