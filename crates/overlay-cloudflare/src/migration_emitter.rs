//! Native out-of-band overlay D1 migration emitter.
//!
//! bsv-low `docs/PROMOTION-REPAIR-2026-10-15.md` remedy B, Option 1 (the #430
//! shape). Renders the overlay migration statements `from+1..=count` plus the
//! certify rows as `.sql` files for `wrangler d1 execute --remote --file`,
//! byte-exact and fingerprint-certified, so the gap is applied to prod D1
//! BEFORE the overlay version moves. The first cold start's version gate
//! ([`crate::d1::ensure_overlay_migrations`]) then finds the persisted count +
//! fingerprint current and the self-apply is a no-op (no request wall, no
//! scale-up burst replay).
//!
//! The statements are compile-time `&[&str]` constants
//! ([`crate::d1::OVERLAY_MIGRATIONS`]), some written inline and some as named
//! `const` references (e.g. `TX_WORDS_CREATE`); the emitter resolves every
//! element uniformly, which is why a Rust emitter succeeds where the report's
//! text extraction could not. It is byte-exact with what the worker would
//! apply (the same array) and fingerprint-certified with the same
//! [`crate::d1::migration_list_fingerprint`] the worker certifies with.
//!
//! Native-only host tooling: gated off the wasm32 Worker build in `lib.rs`.
//! The entry point is `examples/emit-migrations.rs`.

use std::path::{Path, PathBuf};

use crate::d1::{
    migration_list_fingerprint, migration_statement_is_additive, MIGRATION_CERTIFY_SQL,
    OVERLAY_MIGRATIONS, OVERLAY_MIGRATION_COUNT,
};

/// Default statements per emitted part file.
///
/// `wrangler d1 execute --file` has a per-invocation statement/size ceiling;
/// the gap (~99 statements) splits into a few `--file` runs. Each run is
/// idempotent, so a re-run of a chunk is safe (B.1 "Chunking").
pub const DEFAULT_CHUNK: usize = 25;

/// A named refusal. The emitter writes NOTHING on any of these (B.1 / the
/// lane's refusal list).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitError {
    /// `--count` is not the compiled-in [`OVERLAY_MIGRATION_COUNT`]. The
    /// emitter must run from the PROMOTED sha's tree; a different count means
    /// the wrong checkout, so it refuses rather than emit a mismatched list.
    CountNotBuiltin {
        /// The `--count` the caller passed.
        requested: usize,
        /// The compiled-in [`OVERLAY_MIGRATION_COUNT`].
        builtin: usize,
    },
    /// `--from` is >= `--count`: an inverted or empty range (nothing to emit).
    FromNotBelowCount {
        /// The `--from` the caller passed.
        from: usize,
        /// The target `--count`.
        count: usize,
    },
    /// `--chunk` is zero (a part file must hold at least one statement).
    ZeroChunk,
    /// A statement in `[from..count]` is not additive/idempotent per
    /// [`migration_statement_is_additive`], the one predicate the pinned
    /// `migrations_are_valid_sql` test uses. The emitter refuses to render a
    /// statement the shipped runner would not treat as additive, so the
    /// out-of-band `--file` step can never carry a data-destroying statement.
    NonAdditiveStatement {
        /// The statement's absolute index into [`OVERLAY_MIGRATIONS`].
        index: usize,
        /// The first 40 characters of the offending statement, for the message.
        head: String,
    },
}

impl std::fmt::Display for EmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmitError::CountNotBuiltin { requested, builtin } => write!(
                f,
                "refuse: --count {requested} is not the built-in OVERLAY_MIGRATION_COUNT \
                 {builtin} (run the emitter from the promoted sha's tree; a different \
                 count means the wrong checkout)"
            ),
            EmitError::FromNotBelowCount { from, count } => write!(
                f,
                "refuse: --from {from} must be below --count {count} (nothing to emit)"
            ),
            EmitError::ZeroChunk => {
                write!(f, "refuse: --chunk must be at least 1")
            }
            EmitError::NonAdditiveStatement { index, head } => write!(
                f,
                "refuse: migration statement {index} is not additive (must be a CREATE, an \
                 additive ALTER, an INSERT OR IGNORE, or a DROP INDEX IF EXISTS), got: {head}"
            ),
        }
    }
}

impl std::error::Error for EmitError {}

/// A refusal ([`EmitError`]) or an IO fault while writing. Returned by
/// [`emit_to_dir`]; a refusal writes nothing.
#[derive(Debug)]
pub enum EmitFailure {
    /// A named refusal: the args or a statement were rejected before any write.
    Refused(EmitError),
    /// An IO fault creating the directory or writing a file.
    Io {
        /// The path being created or written when the fault occurred, if known.
        path: Option<PathBuf>,
        /// The underlying IO error.
        source: std::io::Error,
    },
}

impl std::fmt::Display for EmitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmitFailure::Refused(e) => write!(f, "{e}"),
            EmitFailure::Io {
                path: Some(p),
                source,
            } => write!(f, "io error at {}: {source}", p.display()),
            EmitFailure::Io { path: None, source } => write!(f, "io error: {source}"),
        }
    }
}

impl std::error::Error for EmitFailure {}

/// Terminate a statement for a `--file` script: the statement VERBATIM (no
/// reformatting) followed by `;` and a newline. This is the B.1 join form
/// (`OVERLAY_MIGRATIONS[..].join(";\n") + ";\n"`), rendered statement by
/// statement so the terminator is the only byte the emitter ever adds.
fn render_statement(sql: &str) -> String {
    format!("{sql};\n")
}

/// The body of one part file over `statements` (a chunk of the range). The
/// statements are emitted in order, verbatim, each terminated `;` on its own
/// line (no reformatting).
pub fn part_body(statements: &[&str]) -> String {
    statements.iter().map(|s| render_statement(s)).collect()
}

/// The `certify.sql` body: [`MIGRATION_CERTIFY_SQL`] with `?1` -> `count` and
/// `?2` -> `fp`, inlined as integer literals (the shipped `ops_counters.value`
/// column is `INTEGER`; the worker binds the same two numbers). Applied LAST,
/// it writes the two version-gate rows the cold start reads so the self-apply
/// is skipped.
pub fn certify_body(count: usize, fp: u32) -> String {
    let sql = MIGRATION_CERTIFY_SQL
        .replace("?1", &count.to_string())
        .replace("?2", &fp.to_string());
    render_statement(&sql)
}

/// The file name for part `part_no` (1-based) of the range `from+1..=count`:
/// `migrations-<from+1>-<count>-partNN.sql`.
pub fn part_file_name(from: usize, count: usize, part_no: usize) -> String {
    format!("migrations-{}-{}-part{:02}.sql", from + 1, count, part_no)
}

/// Scan a statement range for the FIRST non-additive statement, reporting its
/// ABSOLUTE index (`base + offset`). This is the emitter's additive guard,
/// factored so a pin can drive it over a hand-built slice. The built-in list
/// is all-additive (pinned by `migrations_are_valid_sql`), so the refusal is
/// otherwise unreachable from the built-ins. It routes through the SAME
/// [`migration_statement_is_additive`] predicate the shipped test uses.
pub fn first_non_additive(range: &[&str], base: usize) -> Option<EmitError> {
    range.iter().enumerate().find_map(|(offset, sql)| {
        if migration_statement_is_additive(sql) {
            None
        } else {
            Some(EmitError::NonAdditiveStatement {
                index: base + offset,
                head: sql.trim().chars().take(40).collect(),
            })
        }
    })
}

/// Check the args against the built-in list and the additive rule, returning
/// the statement slice `[from..count]` to emit. The three refusals (B.1):
/// `--count` not the built-in count, `--from` >= `--count`, and any
/// non-additive statement in range (plus a zero `--chunk` guard).
pub fn validate(
    from: usize,
    count: usize,
    chunk: usize,
) -> Result<&'static [&'static str], EmitError> {
    if count != OVERLAY_MIGRATION_COUNT {
        return Err(EmitError::CountNotBuiltin {
            requested: count,
            builtin: OVERLAY_MIGRATION_COUNT,
        });
    }
    if from >= count {
        return Err(EmitError::FromNotBelowCount { from, count });
    }
    if chunk == 0 {
        return Err(EmitError::ZeroChunk);
    }
    // `count == OVERLAY_MIGRATION_COUNT == OVERLAY_MIGRATIONS.len()` (pinned by
    // `migrations_are_valid_sql`) and `from < count`, so the slice is in bounds.
    let range = &OVERLAY_MIGRATIONS[from..count];
    if let Some(e) = first_non_additive(range, from) {
        return Err(e);
    }
    Ok(range)
}

/// One emitted file: its name and its full body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmitFile {
    /// The file's name (no directory), e.g. `migrations-161-259-part01.sql`.
    pub name: String,
    /// The file's full body (statements terminated `;\n`, verbatim).
    pub body: String,
}

/// The full set of files the emitter would write for `[from..count]` at
/// `chunk` statements per part, plus the final `certify.sql`. PURE (no IO), so
/// a pin can assert byte-for-byte what lands on disk.
pub fn plan_files(from: usize, count: usize, chunk: usize) -> Result<Vec<EmitFile>, EmitError> {
    let range = validate(from, count, chunk)?;
    let mut files: Vec<EmitFile> = range
        .chunks(chunk)
        .enumerate()
        .map(|(part_idx, group)| EmitFile {
            name: part_file_name(from, count, part_idx + 1),
            body: part_body(group),
        })
        .collect();
    files.push(EmitFile {
        name: "certify.sql".to_string(),
        // The fingerprint of the WHOLE list through `count`: the same
        // function the worker certifies with.
        body: certify_body(count, migration_list_fingerprint()),
    });
    Ok(files)
}

/// What [`emit_to_dir`] wrote.
#[derive(Debug, Clone)]
pub struct EmitOutcome {
    /// The `--from` (the persisted count the operator read).
    pub from: usize,
    /// The target count (the built-in [`OVERLAY_MIGRATION_COUNT`]).
    pub count: usize,
    /// The statements-per-part the emitter used.
    pub chunk: usize,
    /// The fingerprint written into `certify.sql`.
    pub fingerprint: u32,
    /// The number of migration statements emitted (`count - from`).
    pub statements: usize,
    /// The paths written, in apply order (parts first, `certify.sql` last).
    pub files: Vec<PathBuf>,
}

/// Render and WRITE every file into `out` (created if absent). Returns the
/// outcome, or a named refusal (nothing written) / an IO fault. The written
/// order IS the apply order: the part files, then `certify.sql` LAST.
pub fn emit_to_dir(
    from: usize,
    count: usize,
    chunk: usize,
    out: &Path,
) -> Result<EmitOutcome, EmitFailure> {
    let files = plan_files(from, count, chunk).map_err(EmitFailure::Refused)?;
    std::fs::create_dir_all(out).map_err(|source| EmitFailure::Io {
        path: Some(out.to_path_buf()),
        source,
    })?;
    let mut written = Vec::with_capacity(files.len());
    for file in &files {
        let path = out.join(&file.name);
        std::fs::write(&path, file.body.as_bytes()).map_err(|source| EmitFailure::Io {
            path: Some(path.clone()),
            source,
        })?;
        written.push(path);
    }
    Ok(EmitOutcome {
        from,
        count,
        chunk,
        fingerprint: migration_list_fingerprint(),
        statements: count - from,
        files: written,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::d1::{migration_count_matches, migration_error_is_benign};

    // The realistic prod case: prod `low-overlay-db` is at count 160, the
    // build is at `OVERLAY_MIGRATION_COUNT`. (`--from` is the persisted count.)
    const PROD_FROM: usize = 160;

    /// Apply a statement list the way the shipped `run_migrations` does: one
    /// statement at a time, propagating every error except a benign re-run
    /// duplicate-column on an ALTER (`migration_error_is_benign`).
    fn apply_like_runner(conn: &rusqlite::Connection, statements: &[&str]) {
        for sql in statements {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string();
                assert!(
                    migration_error_is_benign(sql, &msg),
                    "runner statement failed non-benignly: {sql}\n{msg}"
                );
            }
        }
    }

    fn schema_rows(conn: &rusqlite::Connection) -> Vec<(String, String, String)> {
        let mut stmt = conn
            .prepare(
                "SELECT type, name, sql FROM sqlite_master \
                 WHERE sql IS NOT NULL ORDER BY type, name",
            )
            .unwrap();
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    /// Pin (a): the emitted statements, concatenated, are byte-identical to
    /// `OVERLAY_MIGRATIONS[from..count]`: the ONLY byte the emitter adds is
    /// the mandated `;\n` terminator after each statement. Proved on disk.
    #[test]
    fn a_emitted_statements_are_byte_identical_to_the_builtin_slice() {
        let count = OVERLAY_MIGRATION_COUNT;
        let dir = tempdir();
        let outcome = emit_to_dir(PROD_FROM, count, DEFAULT_CHUNK, &dir).unwrap();

        // Every file but the last is a part; the last is certify.sql.
        let (parts, certify) = outcome.files.split_at(outcome.files.len() - 1);
        assert_eq!(certify[0].file_name().unwrap(), "certify.sql");

        // Chunking: 99 statements at 25/part -> 25, 25, 25, 24 across 4 parts.
        assert_eq!(outcome.statements, count - PROD_FROM);
        let expected_parts = (count - PROD_FROM).div_ceil(DEFAULT_CHUNK);
        assert_eq!(parts.len(), expected_parts);

        // The part files, concatenated in apply order, reproduce the slice
        // exactly, each statement verbatim + the terminator.
        let on_disk: String = parts
            .iter()
            .map(|p| std::fs::read_to_string(p).unwrap())
            .collect();
        let expected: String = OVERLAY_MIGRATIONS[PROD_FROM..count]
            .iter()
            .map(|s| format!("{s};\n"))
            .collect();
        assert_eq!(on_disk, expected, "part bodies are not the verbatim slice");

        // And each source statement appears verbatim, in order, so there is no
        // reformatting of any multi-line CREATE.
        let mut cursor = 0usize;
        for s in &OVERLAY_MIGRATIONS[PROD_FROM..count] {
            let at = on_disk[cursor..]
                .find(s)
                .expect("each statement appears verbatim, in order");
            cursor += at + s.len();
        }

        // File names carry the overall range and the part number.
        assert_eq!(
            parts[0].file_name().unwrap().to_str().unwrap(),
            format!("migrations-{}-{}-part01.sql", PROD_FROM + 1, count)
        );
    }

    /// Pin (b): the certify fingerprint equals `migration_list_fingerprint()`
    /// of the built-in list, and the count is the built-in count, read back
    /// out of the emitted `certify.sql`, not trusted by construction.
    #[test]
    fn b_certify_fingerprint_equals_the_builtin() {
        let count = OVERLAY_MIGRATION_COUNT;
        let files = plan_files(PROD_FROM, count, DEFAULT_CHUNK).unwrap();
        let certify = &files.last().unwrap().body;

        let parsed_count = value_after(certify, "overlay_migration_count");
        let parsed_fp = value_after(certify, "overlay_migration_fp");
        assert_eq!(parsed_count, count as u64);
        assert_eq!(parsed_fp, migration_list_fingerprint() as u64);

        // It is the two-row upsert the worker certifies with.
        assert!(certify.contains("ON CONFLICT(name)"));
    }

    /// Extract the integer bound to `name` in a certify statement of the shape
    /// `('<name>', <value>)`.
    fn value_after(sql: &str, name: &str) -> u64 {
        let needle = format!("'{name}',");
        let rest = &sql[sql.find(&needle).expect("name present") + needle.len()..];
        let digits: String = rest
            .chars()
            .skip_while(|c| c.is_whitespace())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        digits.parse().expect("an integer value")
    }

    /// Pin (c): each refusal is reported as its named variant, and nothing is
    /// written. Driven through the pure `plan_files` / `first_non_additive`.
    #[test]
    fn c_refusals_are_named() {
        let builtin = OVERLAY_MIGRATION_COUNT;

        // --count is not the built-in count (wrong checkout), both directions.
        assert_eq!(
            plan_files(PROD_FROM, builtin + 1, DEFAULT_CHUNK).unwrap_err(),
            EmitError::CountNotBuiltin {
                requested: builtin + 1,
                builtin
            }
        );
        assert_eq!(
            plan_files(PROD_FROM, builtin - 1, DEFAULT_CHUNK).unwrap_err(),
            EmitError::CountNotBuiltin {
                requested: builtin - 1,
                builtin
            }
        );

        // --from >= --count: equal, and strictly greater.
        assert_eq!(
            plan_files(builtin, builtin, DEFAULT_CHUNK).unwrap_err(),
            EmitError::FromNotBelowCount {
                from: builtin,
                count: builtin
            }
        );
        assert_eq!(
            plan_files(builtin + 5, builtin, DEFAULT_CHUNK).unwrap_err(),
            EmitError::FromNotBelowCount {
                from: builtin + 5,
                count: builtin
            }
        );

        // A zero chunk.
        assert_eq!(
            plan_files(PROD_FROM, builtin, 0).unwrap_err(),
            EmitError::ZeroChunk
        );

        // A non-additive statement in range, reported at its ABSOLUTE index.
        // (Driven over a hand-built slice: the built-in list is all-additive,
        // so this refusal is otherwise unreachable.)
        assert_eq!(
            first_non_additive(&["DROP TABLE pot_records"], 161),
            Some(EmitError::NonAdditiveStatement {
                index: 161,
                head: "DROP TABLE pot_records".to_string(),
            })
        );
        assert_eq!(
            first_non_additive(&["CREATE TABLE IF NOT EXISTS a (x INTEGER)"], 0),
            None
        );
    }

    /// Pin (d): the emitter's additive guard IS the shipped predicate, one
    /// predicate, two callers. `first_non_additive([sql]).is_none()` equals
    /// `d1::migration_statement_is_additive(sql)` for every representative
    /// shape, and the same predicate is what `migrations_are_valid_sql`
    /// asserts over the whole list (see `d1/mod.rs`).
    #[test]
    fn d_additive_predicate_is_the_shipped_one() {
        let additive = [
            "CREATE TABLE IF NOT EXISTS t (x INTEGER)",
            "CREATE INDEX IF NOT EXISTS i ON t(x)",
            "ALTER TABLE t ADD COLUMN y INTEGER",
            "INSERT OR IGNORE INTO t (x) VALUES (1)",
            "DROP INDEX IF EXISTS i",
            "DROP TRIGGER IF EXISTS owed_dirty_pot_records_update",
            "  create table if not exists lower (x integer)",
        ];
        let banned = [
            "DROP TABLE t",
            "DROP INDEX i",
            "INSERT INTO t (x) VALUES (1)",
            "DELETE FROM t",
            "UPDATE t SET x = 1",
            "DROP TRIGGER owed_dirty_pot_records_update",
        ];
        for sql in additive {
            assert!(migration_statement_is_additive(sql), "additive: {sql}");
            assert!(
                first_non_additive(&[sql], 0).is_none(),
                "guard additive: {sql}"
            );
        }
        for sql in banned {
            assert!(!migration_statement_is_additive(sql), "banned: {sql}");
            assert!(
                first_non_additive(&[sql], 0).is_some(),
                "guard banned: {sql}"
            );
        }
        // The whole built-in list is additive (the same verdict
        // `migrations_are_valid_sql` relies on).
        assert_eq!(first_non_additive(OVERLAY_MIGRATIONS, 0), None);
    }

    /// Pin (e): round trip. A database migrated to `from` then brought forward
    /// by APPLYING THE EMITTED FILES (parts, then certify.sql) has the same
    /// schema as a database migrated through the whole list by the worker's
    /// own runner; and the emitted certify rows make the version gate read
    /// count + fp current (it would skip the self-apply).
    #[test]
    fn e_round_trip_schema_matches_the_runner_and_the_gate_skips() {
        let count = OVERLAY_MIGRATION_COUNT;
        // A realistic midpoint that is PAST the ops_counters CREATE, so the
        // emitted certify.sql has its table on the from-migrated database.
        let ops_idx = OVERLAY_MIGRATIONS
            .iter()
            .position(|s| s.contains("CREATE TABLE IF NOT EXISTS ops_counters"))
            .expect("ops_counters is created by a migration");
        let from = ops_idx + 1;

        // DB_A: migrated to `from` by the runner, then the EMITTED files.
        let db_a = rusqlite::Connection::open_in_memory().unwrap();
        apply_like_runner(&db_a, &OVERLAY_MIGRATIONS[0..from]);
        let dir = tempdir();
        let outcome = emit_to_dir(from, count, DEFAULT_CHUNK, &dir).unwrap();
        for path in &outcome.files {
            let script = std::fs::read_to_string(path).unwrap();
            db_a.execute_batch(&script)
                .unwrap_or_else(|e| panic!("applying {}: {e}", path.display()));
        }

        // DB_B: migrated through the WHOLE list by the runner.
        let db_b = rusqlite::Connection::open_in_memory().unwrap();
        apply_like_runner(&db_b, &OVERLAY_MIGRATIONS[0..count]);

        assert_eq!(
            schema_rows(&db_a),
            schema_rows(&db_b),
            "the emitted parts yield the runner's schema"
        );

        // The version gate on DB_A: read count + fp, both current -> skip.
        let persisted_count: f64 = db_a
            .query_row(
                "SELECT value FROM ops_counters WHERE name = 'overlay_migration_count'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let persisted_fp: f64 = db_a
            .query_row(
                "SELECT value FROM ops_counters WHERE name = 'overlay_migration_fp'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            migration_count_matches(persisted_count),
            "the gate's count check reads current"
        );
        assert_eq!(
            persisted_fp,
            migration_list_fingerprint() as f64,
            "the gate's fp check reads current"
        );
    }

    fn tempdir() -> PathBuf {
        let mut p = std::env::temp_dir();
        let unique = format!(
            "overlay-emitter-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p.push(unique);
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
