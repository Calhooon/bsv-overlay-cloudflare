//! EVERY SQL-producing function in this crate must PREPARE against the real
//! production schema (bsv-low #323, written after a production outage).
//!
//! # Why
//!
//! `recovery_view_sql(, 0)` shipped syntactically invalid — a column carried
//! through three of four projection tiers and dropped by the middle one — and
//! took `/recovery-view` down for every request with a valid identity. It
//! passed the whole suite because the only pins on it were
//! `sql.contains(…)` assertions, which are true of strings no database will
//! accept. **A `contains` assertion on a query must never be the only thing
//! standing behind it.**
//!
//! A sweep then found four MORE query builders that no test had ever
//! executed: `batch_where_sql`, `pots_view_join_sql`, `decoded_pots_sql` and
//! `claims_sql`. Each was the same defect waiting. This file closes the class
//! crate-wide: a cheap, total backstop that every builder is at least VALID
//! SQL against the real migrations, whatever else does or does not cover it.
//!
//! Richer per-view behaviour (ordering, tiers, windows, joins returning real
//! rows) lives in the dedicated suites — `recovery_view_sqlite.rs`,
//! `refund_view_sqlite.rs`, `live_view_sqlite.rs`, `results_window_sqlite.rs`.
//! This file deliberately asserts the ONE property all of them share, so a
//! new builder added tomorrow is covered the moment it is listed here.

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::hops_view::hops_view_sql;
use low_app_layer::live_view::{keyless_candidates_sql, live_view_sql};
use low_app_layer::logic::{
    batch_where_sql, leaderboard_markers_sql, pots_view_join_sql, proof_pointers_sql,
    recovery_view_sql,
};
use low_app_layer::refund_view::refund_view_sql;
use low_app_layer::results::{claims_sql, decoded_pots_sql, results_sql, seat_markers_sql};
use rusqlite::Connection;

/// A fresh in-memory SQLite carrying the REAL production schema.
fn production_schema_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            let msg = e.to_string().to_ascii_lowercase();
            assert!(
                msg.contains("duplicate column"),
                "production migration failed under real SQLite: {e}\n{sql}"
            );
        }
    }
    conn
}

fn assert_prepares(conn: &Connection, name: &str, sql: &str) {
    conn.prepare(sql).unwrap_or_else(|e| {
        panic!("{name} is not valid SQL against the production schema: {e}\n{sql}")
    });
}

/// Every fixed-shape query builder — BOTH #375 era arms: the inert `None`
/// (production default) and a set cutoff, since either arm shipping invalid
/// is the same outage class.
#[test]
fn every_fixed_query_prepares_against_the_production_schema() {
    let conn = production_schema_db();
    const ERA: Option<i64> = Some(1_754_500_000_000);
    assert_prepares(&conn, "recovery_view_sql", &recovery_view_sql(None, 0));
    assert_prepares(
        &conn,
        "recovery_view_sql(era, 0)",
        &recovery_view_sql(ERA, 0),
    );
    assert_prepares(&conn, "refund_view_sql", &refund_view_sql(None, 0));
    assert_prepares(&conn, "refund_view_sql(era, 0)", &refund_view_sql(ERA, 0));
    assert_prepares(&conn, "hops_view_sql", &hops_view_sql(false, None, 0));
    assert_prepares(&conn, "hops_view_sql(era)", &hops_view_sql(false, ERA, 0));
    // Both arities of the gameId-scoped escape hatch must parse.
    assert_prepares(
        &conn,
        "hops_view_sql(scoped, None)",
        &hops_view_sql(true, None, 0),
    );
    assert_prepares(
        &conn,
        "hops_view_sql(scoped, era)",
        &hops_view_sql(true, ERA, 0),
    );
    assert_prepares(&conn, "live_view_sql", &live_view_sql(None, 0));
    assert_prepares(&conn, "live_view_sql(era, 0)", &live_view_sql(ERA, 0));
    assert_prepares(&conn, "results_sql", &results_sql(None, 0));
    assert_prepares(&conn, "results_sql(era)", &results_sql(ERA, 0));
    // #332 — the /leaderboard anti-flood marker window.
    assert_prepares(
        &conn,
        "leaderboard_markers_sql",
        &leaderboard_markers_sql(None),
    );
    assert_prepares(
        &conn,
        "leaderboard_markers_sql(era)",
        &leaderboard_markers_sql(ERA),
    );
}

/// #375 — the FIXED queries' bind arity, both era arms: the cutoff is
/// exactly ONE extra parameter when set (for the `?N` queries,
/// `parameter_count()` is the max index, which is exactly what the routes'
/// positional bind vectors must fill).
#[test]
fn fixed_queries_declare_the_parameter_count_per_era_arm() {
    let conn = production_schema_db();
    const ERA: Option<i64> = Some(1_754_500_000_000);
    for (name, sql, expected) in [
        ("recovery_view_sql(None, 0)", recovery_view_sql(None, 0), 1),
        ("recovery_view_sql(era, 0)", recovery_view_sql(ERA, 0), 2),
        ("refund_view_sql(None, 0)", refund_view_sql(None, 0), 1),
        ("refund_view_sql(era, 0)", refund_view_sql(ERA, 0), 2),
        ("live_view_sql(None, 0)", live_view_sql(None, 0), 1),
        ("live_view_sql(era, 0)", live_view_sql(ERA, 0), 2),
        ("results_sql(None, 0)", results_sql(None, 0), 1),
        ("results_sql(era)", results_sql(ERA, 0), 2),
        (
            "hops_view_sql(false, None, 0)",
            hops_view_sql(false, None, 0),
            1,
        ),
        ("hops_view_sql(false, era)", hops_view_sql(false, ERA, 0), 2),
        (
            "hops_view_sql(true, None, 0)",
            hops_view_sql(true, None, 0),
            2,
        ),
        ("hops_view_sql(true, era)", hops_view_sql(true, ERA, 0), 3),
        (
            "leaderboard_markers_sql(None)",
            leaderboard_markers_sql(None),
            3,
        ),
        (
            "leaderboard_markers_sql(era)",
            leaderboard_markers_sql(ERA),
            4,
        ),
    ] {
        let stmt = conn
            .prepare(&sql)
            .unwrap_or_else(|e| panic!("{name} did not prepare: {e}"));
        assert_eq!(
            stmt.parameter_count(),
            expected,
            "{name} must bind {expected} parameters"
        );
    }
}

/// Every `n`-parameterised builder, at the boundaries that matter: one
/// outpoint, a couple, and a batch. An `n`-dependent builder can be valid at
/// n=1 and malformed at n>1 (a missing separator, a stray comma), so a single
/// arity would not close the class.
#[test]
fn every_batched_query_prepares_at_representative_arities() {
    let conn = production_schema_db();
    for n in [1usize, 2, 5, 20] {
        assert_prepares(&conn, &format!("batch_where_sql({n})"), &batch_where_sql(n));
        assert_prepares(
            &conn,
            &format!("pots_view_join_sql({n})"),
            &pots_view_join_sql(n, None),
        );
        // #375 — the era arm of the one n-parameterised era-filtered builder.
        assert_prepares(
            &conn,
            &format!("pots_view_join_sql({n}, era)"),
            &pots_view_join_sql(n, Some(1_754_500_000_000)),
        );
        assert_prepares(
            &conn,
            &format!("decoded_pots_sql({n})"),
            &decoded_pots_sql(n),
        );
        assert_prepares(
            &conn,
            &format!("seat_markers_sql({n})"),
            &seat_markers_sql(n, low_app_layer::results::SEAT_MARKERS_PER_KEY),
        );
        assert_prepares(&conn, &format!("claims_sql({n})"), &claims_sql(n));
        assert_prepares(
            &conn,
            &format!("proof_pointers_sql({n})"),
            &proof_pointers_sql(n),
        );
        assert_prepares(
            &conn,
            &format!("keyless_candidates_sql({n})"),
            &keyless_candidates_sql(n),
        );
    }
}

/// The bind ARITY must match the placeholders the builder emits — a query can
/// PREPARE and still fail at execution when the caller binds the wrong count,
/// which is the other half of this failure class.
#[test]
fn batched_queries_declare_the_parameter_count_they_emit() {
    let conn = production_schema_db();
    for n in [1usize, 2, 5, 20] {
        for (name, sql, expected) in [
            ("batch_where_sql", batch_where_sql(n), n * 2),
            ("pots_view_join_sql", pots_view_join_sql(n, None), n * 2),
            // #375: the cutoff is exactly one extra bind, appended LAST.
            (
                "pots_view_join_sql(era)",
                pots_view_join_sql(n, Some(1_754_500_000_000)),
                n * 2 + 1,
            ),
            ("decoded_pots_sql", decoded_pots_sql(n), n * 2),
            // FOUR per pot: the outpoint pair plus the two COMMITTED settle
            // keys read from that pot's own funding lock.
            (
                "seat_markers_sql",
                seat_markers_sql(n, low_app_layer::results::SEAT_MARKERS_PER_KEY),
                n * 4,
            ),
            // #332: one (gameId, winner) PAIR per key.
            ("proof_pointers_sql", proof_pointers_sql(n), n * 2),
        ] {
            let stmt = conn
                .prepare(&sql)
                .unwrap_or_else(|e| panic!("{name}({n}) did not prepare: {e}"));
            assert_eq!(
                stmt.parameter_count(),
                expected,
                "{name}({n}) must bind {expected} parameters (one pair per outpoint)"
            );
        }
    }
}
