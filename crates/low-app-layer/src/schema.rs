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

use std::sync::atomic::{AtomicBool, Ordering};

/// The statement — byte-identical to migration 98 in the overlay's
/// `OVERLAY_MIGRATIONS`. Pinned equal by
/// `the_app_layer_alter_is_byte_identical_to_the_overlay_migration`, so the
/// two cannot drift into a column this worker adds and the overlay never
/// writes (epoch Rule 16: share the artifact, not the convention).
pub const SIG_VALID_ALTER: &str = "ALTER TABLE potparty_records ADD COLUMN sigValid INTEGER";

/// Set once THIS isolate has issued (or knowingly skipped) the statement.
static APPLIED: AtomicBool = AtomicBool::new(false);

/// Is a D1 error the benign "the column is already there" case?
///
/// Kept as a pure predicate so it is testable without a D1 binding — the same
/// reason `potparty_insert_query` exists on the writer side. D1 surfaces
/// SQLite's message; matching is case-insensitive and substring-based because
/// the wrapper's prefix has changed across `worker` versions.
pub fn is_duplicate_column_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("duplicate column") || m.contains("already exists")
}

/// Ensure `potparty_records.sigValid` exists, at most once per isolate.
///
/// NEVER fails the request. A genuine error is logged and the flag is left
/// unset so the next request retries: the caller is a recovery surface, and a
/// recovery surface must degrade toward working (epoch Rule 4b). If the
/// column genuinely cannot be added, the query below it fails on its own and
/// says so — this call does not get to decide that.
pub async fn ensure_sig_valid_column(db: &worker::D1Database) {
    if APPLIED.load(Ordering::Acquire) {
        return;
    }
    match db.exec(SIG_VALID_ALTER).await {
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

    /// Only the benign case is benign. A real failure must NOT latch
    /// `APPLIED`, or one transient error would permanently stop the retry.
    #[test]
    fn duplicate_column_is_benign_and_nothing_else_is() {
        for benign in [
            "duplicate column name: sigValid",
            "D1_ERROR: duplicate column name: sigValid",
            "Duplicate Column Name",
            "table potparty_records already exists",
        ] {
            assert!(is_duplicate_column_error(benign), "{benign}");
        }
        for real in [
            "no such table: potparty_records",
            "D1_ERROR: network error",
            "database is locked",
            "",
        ] {
            assert!(!is_duplicate_column_error(real), "{real}");
        }
    }
}
