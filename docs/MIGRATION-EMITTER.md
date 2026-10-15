# MIGRATION-EMITTER: the out-of-band prod D1 step (remedy B, Option 1)

Written 2026-10-15T19:29:30Z. Stamps from `date -u`. Names only, no secret
values. This is the operator procedure for bsv-low
`docs/PROMOTION-REPAIR-2026-10-15.md` section B, Option 1 (the #430 shape):
apply the overlay migration gap to prod `low-overlay-db` out of band, BEFORE
the overlay version moves, so the first cold start's version gate
(`ensure_overlay_migrations`) finds the count and fingerprint current and the
self-apply is a no-op.

The tool is `crates/overlay-cloudflare/examples/emit-migrations.rs` (logic in
`src/migration_emitter.rs`), native host tooling gated off the wasm32 Worker
build.

## Steps

1. Read the persisted state from prod (the row names are those in
   `MIGRATION_CERTIFY_SQL`):
   `wrangler d1 execute low-overlay-db --remote --command "SELECT name, value FROM ops_counters WHERE name IN ('overlay_migration_count','overlay_migration_fp')"`.
   Record the count (this is `--from`).
2. Check out the PROMOTED sha and emit from THAT tree (the emitter refuses when
   `--count` is not the built-in `OVERLAY_MIGRATION_COUNT`, which catches a
   wrong checkout):
   `cargo run -p bsv-overlay-cloudflare --example emit-migrations -- --from <persisted count> --out <dir>`
   (`--count` defaults to the built-in; `--chunk` defaults to 25). It writes
   `migrations-<from+1>-<count>-partNN.sql` plus `certify.sql`.
3. Apply each part IN ORDER through the vault wrapper (the Cloudflare API token
   and account id injected from the vault, never inline):
   `wrangler d1 execute low-overlay-db --remote --file <partNN.sql>`. Each run
   is idempotent, so a re-run of a part is safe.
4. Apply `certify.sql` LAST (it writes the two version-gate rows).
5. Read back count and fp (step 1's query); both must equal the promoted
   build's `OVERLAY_MIGRATION_COUNT` and `migration_list_fingerprint()` (the
   O10 pre-deploy sub-leg). Only then deploy the overlay version.

## Rollback

D1 has no rollback: a statement applied is kept. This is SAFE because every
statement `161..<count>` is additive and idempotent, ENFORCED by the pinned
test `migrations_are_valid_sql` (`d1/mod.rs`), which the emitter reuses through
the one shared predicate `migration_statement_is_additive` (one predicate, two
callers): only `CREATE` (IF NOT EXISTS), `ALTER TABLE` (additive), `INSERT OR
IGNORE`, or `DROP INDEX IF EXISTS` are permitted; no DROP TABLE, DROP COLUMN or
RENAME. New tables, columns and indexes are simply unused by older code, so the
PREVIOUS overlay version keeps working against prod D1 at `<count>` (its
rollback anchor stands). The emitter never emits a non-additive statement: it
refuses (nonzero, named reason) rather than render one.
