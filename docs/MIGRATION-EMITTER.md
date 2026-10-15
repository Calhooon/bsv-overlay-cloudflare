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
   wrong checkout). The `overlay-cloudflare` crate is EXCLUDED from the root
   workspace (it is a member of `workers/Cargo.toml`), so `cargo run -p
   bsv-overlay-cloudflare --example emit-migrations ...` from the repo root
   FAILS with "package `bsv-overlay-cloudflare` not found in workspace". Run it
   from the crate directory instead:
   ```
   cd crates/overlay-cloudflare
   cargo run --example emit-migrations -- --from <persisted count> --out <dir>
   ```
   or, from the repo root, name the manifest:
   `cargo run --manifest-path crates/overlay-cloudflare/Cargo.toml --example emit-migrations -- --from <persisted count> --out <dir>`.
   (`--count` defaults to the built-in; `--chunk` defaults to 25.) Both forms
   were run against this PR's tree with `--from 160`: each emitted 99 statements
   `161..259` at 25 per file (fp 1780566194 on this tree), writing
   `migrations-<from+1>-<count>-partNN.sql` plus `certify.sql`.
3. Apply each part IN ORDER through the vault wrapper (the Cloudflare API token
   and account id injected from the vault, never inline):
   `wrangler d1 execute low-overlay-db --remote --file <partNN.sql>`. Each run
   is idempotent, so a re-run of a part is safe.
4. Apply `certify.sql` LAST (it writes the two version-gate rows).
5. Read back count and fp (step 1's query); both must equal the promoted
   build's `OVERLAY_MIGRATION_COUNT` and `migration_list_fingerprint()` (the
   O10 pre-deploy sub-leg). Only then deploy the overlay version.

## The `wrangler d1 execute --file` splitter (triggers)

Step 3 applies each part with `wrangler d1 execute --remote --file`, and
wrangler splits a `.sql` file into statements ITSELF with a
compound-statement-aware tokenizer (`splitSqlQuery` in wrangler's
`src/d1/splitter.ts`). It carries a `CREATE -> TRIGGER -> SEMI -> END` state so
that a `;` INSIDE a `CREATE TRIGGER ... BEGIN ...; END` body does not end the
statement; a statement ends only on the `;` that follows the real `END`. 11 of
the emitted statements in `161..259` are exactly this shape with embedded
semicolons (the nine `derived_hops` triggers in `src/derived_hops.rs`, each with
inner `;` before its `END`, and the two `gasp_fault_memo` triggers in
`src/gasp_fault_memo.rs`), and the emitter terminates each statement with `;\n`
(the trigger consts end at `END`, so the file carries `END;\n`), which is what
that state machine needs. The safety of the trigger output therefore RESTS on
this splitter.

The fleet's pinned deploy wrangler is 4.148.0, whose splitter carries that
state; the lens ran 4.148's splitter over the real emitted files (99 statements
plus certify) and every one of the 11 triggers stayed whole, zero mis-splits.
Before the prod run, confirm `wrangler --version` is at least 4.148 (or pin
`wrangler@4.148`). An OLDER wrangler that predates the compound-statement-aware
splitter would cut a trigger at its first inner `;`; D1 would then error on the
incomplete `CREATE TRIGGER` fragment and roll back that `--file` chunk's
transaction. That is a LOUD failure, not a silent corruption, but the version
check prevents the confusing prod-window error. Each chunk holds whole
statements, so a trigger is never split across two part files.

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
