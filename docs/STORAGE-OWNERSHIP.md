# Storage ownership (bsv-low #474)

Who may write and who may read every persistent store of the LOW stack, with each table's rebuild class and its never-wipe flag. The machine-readable twin is `storage-ownership.json` at the repository root; `scripts/check-storage-ownership.py` (run by `make ci`) holds every SQL statement of `crates/overlay-cloudflare`, `crates/overlay-discovery` and `crates/low-app-layer` to it. This page and the JSON change together, in the same commit as the code that needs the change.

The columns are those of bsv-low `docs/DECISION-470-DATA-WIPE-2026-09-19.md` section 2, kept current from now on. The never-wipe set is the owner's ruling of 2026-10-07 (bsv-low `docs/STATE.md`, "Owner decisions RULED 2026-10-07"): a never-wipe set exists, and any wipe moves LOW's own era cutoff only.

## The model

- **The shared D1.** The overlay and the app layer share ONE D1 per environment, binding `OVERLAY_DB`. The overlay owns the schema: its `OVERLAY_MIGRATIONS` (`crates/overlay-cloudflare/src/d1/mod.rs`) create every table. The app layer issues additive catch-up DDL for the columns and tables it reads (`crates/low-app-layer/src/schema.rs`, `LATCH_COLUMN_ALTERS` and `CREATE_TABLE_CATCHUPS`), each one byte-identical to an overlay migration; those are its CREATE and ALTER grants below.
- **owner**: the crate whose statements own the rows; it may issue any statement on its table but the never-wipe rule's. **further writers**: every other write grant, by crate and statement. **readers**: read grants (an INSERT, UPDATE, DELETE or REPLACE grant implies a read; a DDL grant does not). The schema owner (`overlay-cloudflare`) may CREATE, ALTER and DROP every table of the shared D1 that is not never-wipe.
- **rebuild class** (#470 section 2): `chain` (the bytes are on chain; a re-index, a courier or a GASP peer can rebuild the row), `filings` (only the app layer's filed rows hold it), `transient` (a restart or the next read recomputes it), `lost` (nothing else holds it).
- **never-wipe**: a table whose loss cannot be rebuilt from the chain, or whose rows are money evidence. A wipe of the shared D1 leaves every such table untouched: a wipe moves LOW's era cutoff (#470 option A), it never deletes from a never-wipe table. The check enforces it on every crate, the owner and the schema owner included: no DROP, TRUNCATE, ALTER that drops a column or renames, or DELETE with no WHERE; a DELETE with a WHERE only where the row's `delete_scope` grants that crate, file and statement (below).
- **Crates and workers.** `overlay-cloudflare` is the `low-overlay` / `low-overlay-beta` worker (`wrangler.low.toml`) and the generic `bsv-overlay-cloudflare` (`wrangler.toml`). `low-app-layer` is `low-app-layer` / `low-app-layer-beta`. `overlay-discovery` is a library linked into both and issues no SQL of its own.

## The databases

| database | environments | owner | writers | readers | rebuild class | never-wipe | why |
|---|---|---|---|---|---|---|---|
| `low-overlay-db` | prod `low-overlay-db`, beta `low-overlay-db-beta` | low-overlay (the schema) | low-overlay, low-app-layer | low-overlay, low-app-layer | per table | **partial**: the 22 table rows marked below | the shared D1 (#470 section 2.1) |
| `low-identity-db` (binding `DB`) | prod `low-identity-db`, beta `low-identity-db-beta` | `low-identity-node` | `low-identity-node` | `low-identity-resolver` (M29-3a, not built yet); LOW's app layer through a service binding (never the database) | GASP resync from Zanaadu's peer | **yes, the whole database, with the node's R2 bucket, KV and queue** | M29-2's identity node (bsv-low `workers/low-identity-node`), a separate D1 per environment; the owner's ruling of 2026-10-07 |
| the operator's own (`wrangler.toml`) | any | bsv-overlay-cloudflare | bsv-overlay-cloudflare | bsv-overlay-cloudflare | chain | no | the generic, non-LOW deployment of the same schema |

`low-identity-db`'s tables are the identity node's (M29-2) and are listed in that node's repository; this manifest carries the database row, the node's other stores (below) and Chaintracks' headers so the never-wipe set is in one place.

### The identity node's other stores (`workers/low-identity-node`, M29-2)

The node's stores are kept or lost together, never one alone: all are in the never-wipe set with its D1.

| store | owner | holds | rebuild class | never-wipe | why |
|---|---|---|---|---|---|
| R2 `low-identity-beefs` / `low-identity-beefs-beta` (binding `BEEF`) | low-identity-node | each admitted transaction's BEEF (`beef/<txid>.bin`) and ancestry (`beef-ancestry/<txid>.beef`), Zanaadu's overlay-lib | lost while the D1 is kept | **yes** | wiped alone, the mirror cannot be rebuilt: the D1 says every row is held, so GASP never re-fetches its BEEF |
| KV `low-identity-kv` / `low-identity-kv-beta` (binding `KV`) | low-identity-node | the GASP rotation mark (`GASP_ROTATION_KEY`) | transient | **yes** | in the set with the node (CAP, lane 474-fold); in itself a cursor |
| queue `low-identity-events` / `low-identity-events-beta` (producer `EVENTS_OUT`) | low-identity-node | the pf lookup services' admitted envelopes, kept 4 days | lost after 4 days | **yes** | a purge drops envelopes the resolver (M29-3a, no consumer yet) has not read; nothing re-emits them |

The resolver (`workers/low-identity-resolver`, lane M29-3a) reads the D1. What it writes is to be filled by M29-3a: Zanaadu's `app-layer-lib/src/storage` at `2de902a` writes `chain_state`, `identity_events`, `identity_contests`, `identity_evictions`, `identity_number_spends`, `identity_preferences`, `xana_user_proofs`, `xana_evictions`, `xana_posts`, `pf_names`, `pf_registry_pictures`, `pf_content`, `pf_content_kill` and the engagement tables (a dynamic `UPDATE {table}`); which of them the resolver writes, and into which D1, is not known here.

## The tables of `low-overlay-db`

56 rows: 51 owned by `overlay-cloudflare`, 5 by `low-app-layer` (`hopsweep_records`, `proof_posts`, `owed_rows`, `owed_state`, `hop_chain_probes`). 23 are never-wipe: the money tables and filings (`pot_records`, `potparty_records`, `potrefund_records`, `hopsweep_records`, `hopparty_records`, `result_markers_v2`, `collected_markers_v2`, `proof_posts`), the eviction ledger (`pot_evictions` and its 11 `_evicted` twins), the dead v1 `collected_markers` (the copy source of v2) `banned_hosts` (the operator's word, held nowhere else) and `mutation_dead_letters` (the parked dead letters, #576: the only copy once the DLQ acks).

The filed rows of `potparty_records`, `potrefund_records`, `result_markers_v2`, `collected_markers_v2` and `lb_marker_rows` are written today only by the app layer's `/record` (`crates/low-app-layer/src/record_post.rs`); the overlay is still named their owner, as it holds the schema and its topic managers wrote the old era's rows.

The app layer's write set (the comment in `crates/low-app-layer/wrangler.toml` repeats it): INSERT `potparty_records`, INSERT and UPDATE `potrefund_records`, INSERT `hopsweep_records`, `result_markers_v2`, `collected_markers_v2`, `proof_posts`, `lb_marker_rows`, `tx_any_verdicts`, `hop_chain_probes`, `ops_counters`, INSERT and DELETE `owed_rows`, INSERT and UPDATE `owed_state`; and the catch-up DDL (ALTER on `pot_records`, `potparty_records`, `hopparty_records`, `result_markers_v2`, `hand_markers`, `collected_markers_v2`; CREATE IF NOT EXISTS on `network_seen`, `collected_markers_v2`, `hand_markers`, `proof_posts`, `hopsweep_records`, `owed_rows`, `owed_state`).

| table | owner | further writers | readers | rebuild class | never-wipe | why |
|---|---|---|---|---|---|---|
| `outputs` | overlay-cloudflare | none | none | chain | no | re-admission or a GASP peer re-feeds it; chain bytes |
| `transactions` | overlay-cloudflare | none | low-app-layer | chain | no | the BEEF ancestry `/beef` and `/credit-beef` serve; a wipe is a re-fetch, not a loss |
| `applied_transactions` | overlay-cloudflare | none | none | chain | no | dedup marks, re-derived on re-admission; the successor rule reads a row as landed (bsv-low #575), so never wiped without `outputs` |
| `low_records` | overlay-cloudflare | none | none | chain | no | lobby ads ride on chain |
| `reveal_records` | overlay-cloudflare | none | none | chain | no | on chain; the tower's reveal scan falls back to the couriers, a wipe degrades, never concedes |
| `pot_records` | overlay-cloudflare | low-app-layer: ALTER | low-app-layer | chain | **yes** | THE landing proof every credit reads (#470 section 3 item 7); every column re-derives from chain bytes only with a re-scan nobody has |
| `pot_beefs` | overlay-cloudflare | none | low-app-layer | chain | no | liveness, not custody: the wallet's ancestry is re-fetchable |
| `potparty_records` | overlay-cloudflare | low-app-layer: INSERT, ALTER | low-app-layer | filings | **yes** | filings since D11 (`filed:` rows never touch the chain): identity to pot, a fresh device's enumeration and `/owed`'s attribution (#470 section 3 item 4) |
| `potrefund_records` | overlay-cloudflare | low-app-layer: INSERT, UPDATE | low-app-layer | filings | **yes** | the PRE-SIGNED refund raws, filed before the JOIN; `/internal/armed-pots` rebuilds the tower's alarm population from them (#470 section 3 item 3) |
| `hopsweep_records` | low-app-layer | none | none | filings | **yes** | the seat's pre-signed sweep of its own funding hop, never on chain: the one claim path for a stranded hop from a new device (#470 section 3 item 5) |
| `hopparty_records` | overlay-cloudflare | low-app-layer: ALTER | low-app-layer | chain | **yes** | money evidence: `/hops-view` and `/owed`'s hop-stranded / in-progress families read it; chain in principle, nothing re-scans for it (#470 section 3 item 6) |
| `result_markers_v2` | overlay-cloudflare | low-app-layer: INSERT, ALTER | low-app-layer | filings | **yes** | filings since D11: the results' claims cannot be rebuilt from the chain |
| `hand_markers` | overlay-cloudflare | low-app-layer: CREATE, ALTER | low-app-layer | chain | no | on chain (tm_hand); display only |
| `proof_markers` | overlay-cloudflare | none | low-app-layer | chain | no | on chain; display only |
| `collected_markers_v2` | overlay-cloudflare | low-app-layer: INSERT, CREATE, ALTER | low-app-layer | filings | **yes** | filings since D11 and money evidence: the "already collected" mark that suppresses a duplicate payout offer |
| `proof_posts` | low-app-layer | none | none | filings | **yes** | the filed LOW/proof/v1 bundle; the winner's device held it once, nothing else does |
| `lb_marker_rows` | overlay-cloudflare | low-app-layer: INSERT | low-app-layer | transient | no | the windowed query backfills it |
| `network_seen` | overlay-cloudflare | low-app-layer: CREATE | low-app-layer | chain | no | re-ask Arcade; bar-lowering only |
| `arc_terminal` | overlay-cloudflare | none | none | chain | no | re-askable |
| `tx_any_verdicts` | overlay-cloudflare | low-app-layer: INSERT | low-app-layer | chain | no | re-askable negative memos (courier calls to rebuild) |
| `hop_chain_probes` | low-app-layer | overlay-cloudflare: INSERT, DELETE | none | chain | no | courier memos, re-askable; the overlay expires them and marks them on a reorg. It also holds `homeproof:` rows (the durable latch of a verified Proven word, `owed.rs`) and `homecursor:` rows (the home walk's cursor), which the expiry spares: a wipe loses no money, every latch becomes a courier re-probe again, 2 per recompute (`OWED_HOME_PROBES_PER_RECOMPUTE`) |
| `pot_evictions` | overlay-cloudflare | none | low-app-layer | lost | **yes** | admit-fast's eviction ledger: a readmission re-marks released spends FROM this row; nothing else holds it (#470 section 3 item 8) |
| `owed_rows` | low-app-layer | none | none | transient | no | a pure derivation over the money tables; the next read recomputes |
| `owed_state` | low-app-layer | none | none | transient | no | recomputed on the next read |
| `chain_headers_seen` | overlay-cloudflare | none | none | transient | no | a cursor |
| `reorg_sweep_state` | overlay-cloudflare | none | none | transient | no | a cursor |
| `arcade_reorg_state` | overlay-cloudflare | none | none | transient | no | a cursor |
| `relatch_cursors` | overlay-cloudflare | none | none | transient | no | a cursor |
| `rebroadcast_state` | overlay-cloudflare | none | none | transient | no | a watch list |
| `proofless_watch` | overlay-cloudflare | none | none | transient | no | a watch list, re-enrolled from transactions / pot_beefs |
| `host_sync_state` | overlay-cloudflare | none | none | transient | no | a GASP cursor; the next sync walks again |
| `gasp_peer_health` | overlay-cloudflare | none | none | transient | no | counters |
| `gasp_deferred_graphs` | overlay-cloudflare | none | none | transient | no | bsv-low #555: the partial walks of GASP graphs deferred past their per-graph budget, one row per graph, replaced on each deferral; a lost row costs the walk again from its root |
| `ops_counters` | overlay-cloudflare | low-app-layer: INSERT | none | transient | no | operator counters |
| `ops_heartbeat` | overlay-cloudflare | none | none | transient | no | a heartbeat |
| `submit_refusals` | overlay-cloudflare | none | low-app-layer | transient | no | a census window (#366) |
| `mutation_dead_letters` | overlay-cloudflare | none | none | lost | **yes** | bsv-low #576: the parked dead letters; once the DLQ consumer acks a letter this row is its only copy, and the operator's lever re-drives from it. The lens fold (M2): a scoped DELETE, `dead_letters::resolve` when a replay of the key is acked (its bytes landed, or were refused under an open eviction; the row copies nothing); the delta fold (D-M1): a second, `dead_letters::internal_discard` (`POST /internal/discard-dead-letters`, bearer `INTERNAL_TOKEN`), the operator's discard of named PARKED letters (at most 50 keys and, since the delta-2 fold D2-L1, 50 rows per call, oldest parked first, each logged with the sha256 of its bytes), so the ceiling can drain; at most 2000 letters hold bytes, of which at most 1000 are "not now" letters (column `class`, the delta-2 fold D2-M1: one per txid, 200 new a day), a row holds one queue message and 20 history entries |
| `banned_hosts` | overlay-cloudflare | none | none | lost | **yes** | the operator's bans: nothing else holds them (export first) |
| `ship_records` | overlay-cloudflare | none | none | chain | no | other overlays' adverts; tm_ship is not registered on LOW (empty) |
| `slap_records` | overlay-cloudflare | none | none | chain | no | as ship_records |
| `uhrp_records` | overlay-cloudflare | none | none | chain | no | not registered on LOW (empty) |
| `agent_records` | overlay-cloudflare | none | none | chain | no | not registered on LOW (empty) |
| `agent_capabilities` | overlay-cloudflare | none | none | chain | no | not registered on LOW (empty) |
| `dm_delegation_records` | overlay-cloudflare | none | none | chain | no | not registered on LOW (empty) |
| `result_markers` | overlay-cloudflare | none | none | chain | no | dead v1 shape, write-frozen, old-era rows on chain; display only |
| `collected_markers` | overlay-cloudflare | none | none | chain | **yes** | dead v1 shape, write-frozen; the copy source of `collected_markers_v2` (the carry migration re-runs on every start); its NULL-txid rows were never carried and exist nowhere else. The duplicate-offer fence is v2's |
| `pot_records_evicted` | overlay-cloudflare | none | low-app-layer | lost | **yes** | admit-fast's shadow twin of `pot_records`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `outputs_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `outputs`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `applied_transactions_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `applied_transactions`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `transactions_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `transactions`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `low_records_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `low_records`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `potparty_records_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `potparty_records`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `potrefund_records_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `potrefund_records`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `result_markers_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `result_markers`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `result_markers_v2_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `result_markers_v2`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `hand_markers_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `hand_markers`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
| `lb_marker_rows_evicted` | overlay-cloudflare | none | none | lost | **yes** | admit-fast's shadow twin of `lb_marker_rows`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |

### Where this differs from the #470 write-up

- #470 section 2.1 names "the app layer's record path" as a writer of `pot_records`. No production statement of `crates/low-app-layer` writes `pot_records` (its only statement there is the catch-up ALTER); the block-event pass the app layer drives reaches the overlay through the `OVERLAY` service binding, and the overlay writes the row.
- `submit_refusals` (the #366 census window) is not in #470's inventory; it is listed here as `transient`.
- `mutation_dead_letters` (#576, after #470) is not in #470's inventory; it is listed here as never-wipe, `lost`.
- `hop_chain_probes` is owned here by the app layer (it writes the memos); the overlay holds INSERT and DELETE on it (the reorg mark and the expiry, `hop_probe_memos.rs`).
- The never-wipe set adds to #470 section 3 (each under the ruling's "plus what the storage manifest #474 names"): `collected_markers` v1 (#470: "n/a, no"); `banned_hosts` (#470: "lost (export first)", not in section 3); `proof_posts`, `result_markers_v2` and `collected_markers_v2` (not in section 3); the 11 `_evicted` twins never-wipe unconditionally, where #470 item 8 says "while any eviction is unresolved"; the identity node's D1, R2 bucket, KV and queue.

## Dynamic sites (string-built table names)

A statement whose table name is not in the same string literal as its keyword (`FROM {table}`, `INSERT INTO "{}"`, a literal ending in `FROM ` or `UPDATE `) cannot be resolved by the check. Each such site is pinned in `storage-ownership.json` `dynamic_sites` by file and op, each SITE named by its statement (the literal, whitespace collapsed, at most 120 characters) and the count, with the tables it reaches named by hand and granted like any statement. A new dynamic site, a changed one or one gone is a red at its file:line until the pin is re-read.

| file | statement | count | tables reached |
|---|---|---|---|
| `crates/overlay-cloudflare/src/admit_fast.rs` | CREATE, ALTER | 1, 1 | the 11 `_evicted` twins |
| `crates/overlay-cloudflare/src/admit_fast.rs` | INSERT, DELETE, SELECT | 2, 2, 5 | `MOVED_TABLES` (11) and their twins |
| `crates/overlay-cloudflare/src/d1/mod.rs` | ALTER | 1 | none: `migration_error_is_benign`'s prefix test |
| `crates/overlay-cloudflare/src/ops.rs` | SELECT | 1 | `pot_beefs`, `transactions` (`proofless_watch_enrol_sql`) |
| `crates/low-app-layer/src/txany.rs` | SELECT | 1 | `pot_beefs`, `transactions` (`tx_any_index_leg_batch_sql`) |
| `crates/low-app-layer/src/{logic,refund_backups,refund_view,results}.rs` | SELECT | 1 each | `potparty_records`, `hopparty_records`, `pot_records` (`FROM {party}`, the `party_candidates_sql` subquery) |

## Never-wipe DELETE grants (`delete_scope`)

What production needs today, each granted by crate, file and statement in the table's row:

| tables | statement | file | why |
|---|---|---|---|
| `pot_records`, `potparty_records`, `potrefund_records`, `result_markers_v2` | `DELETE FROM "{table}" WHERE "{key}" = ?` | `crates/overlay-cloudflare/src/admit_fast.rs` (`move_sql`) | the eviction move: the row is copied into its `_evicted` twin first, the `pot_evictions` ledger holds the move |
| the 11 `_evicted` twins | `DELETE FROM "{}" WHERE "{key}" = ?` | `crates/overlay-cloudflare/src/admit_fast.rs` (`restore_sql`) | the readmission: the row is copied back into its source first |
| `banned_hosts` | `DELETE FROM banned_hosts WHERE type = ? AND value = ?` | `crates/overlay-cloudflare/src/ban_storage.rs` | `/admin/unban`: the operator's own word takes one ban back |

## The check and its limits

`python3 scripts/check-storage-ownership.py` reads every Rust string literal of the three crates (production code only: `tests/`, `examples/`, `benches/` and `#[cfg(test)]` items are skipped) and is red, naming the file, the line and the table, on: a write to a table the crate holds no grant for; a read the manifest does not grant; a table the manifest does not list (so a migration that adds a table must add its row here and in the JSON); an unpinned or miscounted dynamic site; a manifest row nothing creates; a non-owner grant no statement exercises; a table row this page does not name. One statement may be allowed in place by `// storage-ok(<table>): <reason>` on the literal's first line or the line above it. It is red as well on any never-wipe breach above (an in-place allow does not lift it), and on a CTE named like a manifest table. `--self-test` (also in `make ci`) plants a write into a table the crate only reads (red) and the same write under an allow (green), the never-wipe rows (the owner's DROP and bare DELETE red, a granted scoped DELETE green, an ungranted one red), the split and aliased UPDATEs, and pins each limit below.

1. **String-built SQL.** Only SQL inside a string literal is seen; a table name built from a const or an argument is a dynamic site (above), never resolved. A pin names the site's text, not the table a caller passes: a new caller of a pinned helper reaching another table through the same statement stays green, so the pin's tables are read by hand.
2. **Case.** Reads are seen by the UPPERCASE keywords `FROM` and `JOIN` only (prose in messages would flood a case-blind match); a lowercase read is unseen. Writes are matched case-blind but need their full shape (`INSERT INTO t`, `DELETE FROM t`, `UPDATE t SET`, `UPDATE t AS x SET`, `UPDATE t x SET`). An UPDATE is also seen when its literal ends at `UPDATE` (a dynamic site), or, UPPERCASE, at `UPDATE t` (resolved, the `concat!` split) or `UPDATE {t}` (dynamic); a lowercase `update t` split from its SET is unseen.
3. **Joins and CTEs.** A comma join is followed; a CTE name (`name AS (`) defined anywhere in the same file is not a table for a read (a `{cte}` is spliced across literals), never hides a write, and is a red if it is named like a manifest table; an upsert's `DO UPDATE SET` is not an UPDATE. A `not_tables` word is skipped only in its own crate and ops.
4. **Files.** Only `.rs` and `.sql` files are read; SQL received at run time (none today) is out of reach.
5. **Tests.** `#[cfg(test)]` is recognised on an item whose body is a brace block or ends at `;`; a test-only helper outside such an item is scanned as production code (which errs toward red).
6. **Never-wipe.** A DELETE's WHERE is looked for in its own literal only (one in another literal reads as none, a red); what the WHERE selects is not judged, the `delete_scope` grant is the reviewed word for it. SQL an operator runs by hand (`wrangler d1 execute`) is out of reach: this page is the rule there. In-place allows (`// storage-ok`) are noted, not pinned by count (none on the tree).

## The other stores (owners only)

No cross-worker access exists today: each store below is read and written only by its owning worker; any other worker reaches it through that worker's routes or a service binding, never the store itself.

### The app layer (`low-app-layer`)

| store | owner | holds |
|---|---|---|
| `BoardView` DO | low-app-layer | an in-memory cache of the board and the results bodies (no `state.storage` call) |
| `AuthSessionStore` DO + `AUTH_SESSIONS` KV | low-app-layer | BRC-103 sessions and the origin lanes |
| `IDENTITY_KILL` KV, keys `identity-kill:<imageHash>` (bsv-low #532) | low-app-layer | the operator's picture kill list, `{reason, killedAtMs}` per killed hash; written only by `POST /internal/identity/kill` (bearer `INTERNAL_TOKEN`), read by the identity views. **Never-wipe**: a wipe re-exposes every face the operator removed |

### The tower (bsv-low `workers/low-watchtower`)

| store | owner | holds | never-wipe |
|---|---|---|---|
| `LowTower` DO key `record` + its storage alarm | low-watchtower | the parked pre-signed refund, lock height, watched outpoint, give-up height, state | **yes** |
| `LowTower` DO key `candidates` | low-watchtower | the validated candidate set | **yes** |
| `CoSigner` DO keys `case:{txid}:{vout}` (legacy `case`, `legacyCaseMigrated`) | low-watchtower | open and finalized cases | **yes** |
| `CoSigner` DO keys `settledPot:{txid}:{vout}` | low-watchtower | the permanent already-co-signed fence | **yes** |
| `CoSigner` DO keys `justification`, `cosignSeq`, `pending:{deadline}:{txid}:{vout}`, `pendingIndexMigrated`, `rateCount`, `rateWindowStart`, `nonceSeen:`, `nonceExp:`, `idSeen:`, `metric429Bucket:` | low-watchtower | the emitted J, the pending index, rate, nonce and identity housekeeping | **yes** (the class is never deleted) |
| `MONITOR_KV` prefixes `pot:` (30-day TTL), `legacyFallback:`, `metric429:` | low-watchtower (read by `low-monitor`) | armed-pot breadcrumbs, legacy-fallback marks, 429 metrics | no (`pot:` is rebuilt from `potrefund_records` by `/internal/armed-pots`) |
| `AuthSessionStore` DO + `AUTH_SESSIONS` KV | low-watchtower | BRC-103 sessions | no |

### Chaintracks (`rust-chaintracks`, the account's shared header service)

| store | owner | holds | never-wipe |
|---|---|---|---|
| the header store behind the `CHAINTRACKS` service binding (the overlay's and the identity node's) | rust-chaintracks | the block headers every merkle proof is checked against (#470 section 3 item 9) | **yes** |

### The relay (`~/bsv/rust-message-box`, `low-relay`)

| store | owner | holds |
|---|---|---|
| D1 `low-relay-prod` / `low-relay-beta`: `messages`, `message_boxes` | low-relay | the hand transcripts (`low_game_*` retained 14 days), the events boxes |
| D1: `transcript_tombstones`, `message_permissions`, `server_fees`, `device_registrations` | low-relay | purge marks, per-sender config, the fee table, push registrations |
| `MessageHub`, `EngineIoSession`, `BroadcastRegistry` DOs; `AUTH_SESSIONS` KV; R2 `BEEF_BLOBS` (unused) | low-relay | socket, session and presence state |

## Changing this

A new table: its CREATE in `OVERLAY_MIGRATIONS`, its row in `storage-ownership.json` and in the table above, in one commit; the check is red until all three agree. A new cross-crate write or read: the grant in the JSON with the reason in the row's `why`, reviewed like code. A table that leaves the never-wipe set needs the owner's word.
