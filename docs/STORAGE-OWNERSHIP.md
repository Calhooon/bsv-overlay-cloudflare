# Storage ownership (bsv-low #474)

Who may write and who may read every persistent store of the LOW stack, with each table's rebuild class and its never-wipe flag. The machine-readable twin is `storage-ownership.json` at the repository root; `scripts/check-storage-ownership.py` (run by `make ci`) holds every SQL statement of `crates/overlay-cloudflare`, `crates/overlay-discovery` and `crates/low-app-layer` to it. This page and the JSON change together, in the same commit as the code that needs the change.

The columns are those of bsv-low `docs/DECISION-470-DATA-WIPE-2026-09-19.md` section 2, kept current from now on. The never-wipe set is the owner's ruling of 2026-10-07 (bsv-low `docs/STATE.md`, "Owner decisions RULED 2026-10-07"): a never-wipe set exists, and any wipe moves LOW's own era cutoff only.

## The model

- **The shared D1.** The overlay and the app layer share ONE D1 per environment, binding `OVERLAY_DB`. The overlay owns the schema: its `OVERLAY_MIGRATIONS` (`crates/overlay-cloudflare/src/d1/mod.rs`) create every table. The app layer issues additive catch-up DDL for the columns and tables it reads (`crates/low-app-layer/src/schema.rs`, `LATCH_COLUMN_ALTERS` and `CREATE_TABLE_CATCHUPS`), each one byte-identical to an overlay migration; those are its CREATE and ALTER grants below.
- **owner**: the crate whose statements own the rows; it may issue any statement on its table but the never-wipe rule's. **further writers**: every other write grant, by crate and statement. **readers**: read grants (an INSERT, UPDATE, DELETE or REPLACE grant implies a read; a DDL grant does not). The schema owner (`overlay-cloudflare`) may CREATE, ALTER and DROP every table of the shared D1 that is not never-wipe.
- **rebuild class** (#470 section 2): `chain` (the bytes are on chain; a re-index, a courier or a GASP peer can rebuild the row), `filings` (only the app layer's filed rows hold it), `transient` (a restart or the next read recomputes it), `lost` (nothing else holds it).
- **never-wipe**: a table whose loss cannot be rebuilt from the chain, or whose rows are money evidence. A wipe of the shared D1 leaves every such table untouched: a wipe moves LOW's era cutoff (#470 option A), it never deletes from a never-wipe table. The check enforces it on every crate, the owner and the schema owner included: no DROP, TRUNCATE, ALTER that drops a column or renames, or DELETE with no WHERE; a DELETE with a WHERE only where the row's `delete_scope` grants that crate, file and statement (below).
- **Crates and workers.** `overlay-cloudflare` is the `low-overlay` / `low-overlay-beta` worker (`wrangler.low.toml`) and the generic `bsv-overlay-cloudflare` (`wrangler.toml`). `low-app-layer` is `low-app-layer` / `low-app-layer-beta`. `overlay-discovery` is a library linked into both and issues no SQL of its own; it writes one statement's text, the read of `pot_evictions` (`pot::storage::evictions_by_txid_sql`, E649 L6), which both workers issue, hence its read grant.

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

65 rows: 57 owned by `overlay-cloudflare`, 8 by `low-app-layer` (`hopsweep_records`, `proof_posts`, `owed_rows`, `owed_state`, `owed_spender_bytes`, `hop_chain_probes`, `pot_announce_parked`, `pot_announce_seq`). 23 are never-wipe: the money tables and filings (`pot_records`, `potparty_records`, `potrefund_records`, `hopsweep_records`, `hopparty_records`, `result_markers_v2`, `collected_markers_v2`, `proof_posts`), the eviction ledger (`pot_evictions` and its 11 `_evicted` twins), the dead v1 `collected_markers` (the copy source of v2) `banned_hosts` (the operator's word, held nowhere else) and `mutation_dead_letters` (the parked dead letters, #576: the only copy once the DLQ acks).

The filed rows of `potparty_records`, `potrefund_records`, `result_markers_v2`, `collected_markers_v2` and `lb_marker_rows` are written today only by the app layer's `/record` (`crates/low-app-layer/src/record_post.rs`); the overlay is still named their owner, as it holds the schema and its topic managers wrote the old era's rows.

The app layer's write set (the comment in `crates/low-app-layer/wrangler.toml` repeats it): INSERT `potparty_records`, INSERT and UPDATE `potrefund_records`, INSERT `hopsweep_records`, `result_markers_v2`, `collected_markers_v2`, `proof_posts`, `lb_marker_rows`, `tx_any_verdicts`, `hop_chain_probes`, `ops_counters`, INSERT and DELETE `owed_rows`, INSERT and UPDATE `owed_state`, INSERT and DELETE `owed_spender_bytes`, INSERT, UPDATE and DELETE `pot_announce_parked`, INSERT and UPDATE `pot_announce_seq`, UPDATE `board_era` (#674, the cutoff); and the catch-up DDL (ALTER on `pot_records`, `potparty_records`, `hopparty_records`, `result_markers_v2`, `hand_markers`, `collected_markers_v2`, `owed_state` (#566's `recomputeSeq`), `hop_chain_probes` (#808's `tipHeight`); CREATE IF NOT EXISTS on `network_seen`, `collected_markers_v2`, `hand_markers`, `proof_posts`, `hopsweep_records`, `owed_rows`, `owed_state`, `owed_spender_bytes`, `pot_announce_parked`, `pot_announce_seq`).

| table | owner | further writers | readers | rebuild class | never-wipe | why |
|---|---|---|---|---|---|---|
| `outputs` | overlay-cloudflare | none | none | chain | no | re-admission or a GASP peer re-feeds it; chain bytes |
| `transactions` | overlay-cloudflare | none | low-app-layer | chain | no | the BEEF ancestry `/beef` and `/credit-beef` serve; a wipe is a re-fetch, not a loss |
| `applied_transactions` | overlay-cloudflare | none | none | chain | no | dedup marks, re-derived on re-admission; the successor rule reads a row as landed (bsv-low #575), and so does the queue consumer's twin rule for a message whose R2 object is MISSING (bsv-low #585 door 3, `TWIN_APPLIED_SQL`), so never wiped without `outputs` |
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
| `hop_chain_probes` | low-app-layer | overlay-cloudflare: INSERT, DELETE | none | chain | no | courier memos, re-askable; the overlay expires them and marks them on a reorg. It also holds `homeproof:` rows (the durable latch of a verified Proven word, `owed.rs`) and `homecursor:` rows (the home walk's cursor), which the expiry spares, and `spenderask:` rows (the durable courier ask stamp per spender, `owed.rs` `SPENDER_ASK_PREFIX`), which the expiry takes after two hours: a wipe loses no money (a lost ask stamp ranks its spender never-asked), every latch becomes a courier re-probe again, 2 per recompute (`OWED_HOME_PROBES_PER_RECOMPUTE`) |
| `pot_evictions` | overlay-cloudflare | none | low-app-layer, overlay-discovery | lost | **yes** | admit-fast's eviction ledger: a readmission re-marks released spends FROM this row; nothing else holds it (#470 section 3 item 8) |
| `tx_words` | low-app-layer | low-app-layer: CREATE | none | re-askable | no | compact tracker projection; eight-day stranger sweep; public reads create zero rows |
| `owed_rows` | low-app-layer | none | none | transient | no | a pure derivation over the money tables; the next read recomputes |
| `owed_state` | low-app-layer | none | none | transient | no | recomputed on the next read |
| `owed_spender_bytes` | low-app-layer | none | none | transient | no | bsv-low #672 (RC-L30): a cache of a hop spender's immutable outputs + input outpoints, content-addressed by spender txid, read before the owed recompute's budget so a budget cut never clobbers a judged spent-elsewhere row; a sweep ages it and a lost row is re-fetched from a courier when its spender is asked again |
| `pot_announce_parked` | low-app-layer | none | none | transient | no | a wake-up signal only (E649 L4 H1): the seat's own read serves every fact, and the next pot change announces again |
| `pot_announce_seq` | low-app-layer | none | none | transient | no | a counter only (E649 L4 N1): the park's tokens and its batch room; a wipe re-seeds it from the clock (times 1,000) at the next park |
| `chain_headers_seen` | overlay-cloudflare | none | none | transient | no | a cursor |
| `reorg_sweep_state` | overlay-cloudflare | none | none | transient | no | a cursor |
| `arcade_reorg_state` | overlay-cloudflare | none | none | transient | no | a cursor |
| `relatch_cursors` | overlay-cloudflare | none | none | transient | no | a cursor |
| `rebroadcast_state` | overlay-cloudflare | none | none | transient | no | a watch list |
| `proofless_watch` | overlay-cloudflare | none | none | transient | no | a watch list, re-enrolled from transactions / pot_beefs |
| `host_sync_state` | overlay-cloudflare | none | none | transient | no | a GASP cursor; the next sync walks again |
| `gasp_sync_cost` | overlay-cloudflare | none | none | transient | no | one day of sync cost readings and the latest tick; no admission state |
| `gasp_sync_speed` | overlay-cloudflare | none | none | transient | no | one day of total, topic and peer speed readings and the latest sync; no admission state |
| `gasp_route_faults` | overlay-cloudflare | none | none | transient | no | one day of route faults by topic, peer and class (engine 0.2.0, #57), counted apart from the peers' health; no admission state |
| `gasp_peer_health` | overlay-cloudflare | none | none | transient | no | counters |
| `ef_deferred_jobs` | overlay-cloudflare | none | none | transient | no | NL-6c: one row per broadcast-gated submission whose EF work was past one request's budget (its reference, where its bytes rest, its state, its runs, the corroboration's cursor since NL-6d, the arm's answer); swept by the cron seven days after it settles; a lost row loses the caller's poll and the queued run |
| `ef_deferred_chunks` | overlay-cloudflare | none | none | transient | no | NL-6c: a deferred submission's bytes at rest in D1, a chunk of at most 1,000,000 bytes a row, when no `BEEF_BLOBS` binding exists; deleted when the job settles |
| `gasp_deferred_graphs` | overlay-cloudflare | none | none | transient | no | bsv-low #555: the partial walks of GASP graphs deferred past their per-graph budget, one row per graph, replaced on each deferral, at most 256 rows and 64 MiB over every peer (the upsert's ceiling) and swept by the cron 30 h after its last write (the lens fold's M3); a lost row costs the walk again from its root; a record has no byte bound (bsv-low #585): the row holds its first 1 MiB and `bytes` the whole record's size |
| `gasp_deferred_graph_chunks` | overlay-cloudflare | none | none | transient | no | bsv-low #585: the further parts of a deferred graph's record past one row's room, 1 MiB a row, keyed by the graph, the generation its `gasp_deferred_graphs` row names and a sequence; written before the head row and deleted with it, a part no head names swept by the cron after an hour; a lost part drops its record (the walk again from its root) |
| `gasp_fault_memo` | overlay-cloudflare | none | none | transient | no | compact fault facts with retry count and expiry; 20,000 per topic with oldest eviction, independent of partial graphs |
| `gasp_fault_memo_size` | overlay-cloudflare | none | none | transient | no | transactional memo row counts for capacity eviction; no admission authority |
| `conflict_fault_memo` | overlay-cloudflare | none | none | transient | no | bsv-low #825: the conflict pass's per-pot fault memo; a pot past three consecutive faulted re-checks is held out of the draw for 24 h (never one the index holds SEEN or funded); a clean re-check deletes its row, a bounded sweep (64 a pass) the week-old ones; a lost row costs three more faulted draws |
| `gasp_expired_outpoints` | overlay-cloudflare | none | none | transient | no | engine 0.1.8: the GASP roots the walk refused because their own advert had expired, one row per (topic, outpoint), kept until `max(expiry, refusal) + 7 days` and at most 20,000 rows a topic (the cron's sweeps); a later sync skips each with no node fetch; a lost row costs one node fetch again |
| `beef_blob_sweep` | overlay-cloudflare | none | none | transient | no | bsv-low #585 (door 3's fold): the one-row state of the orphan sweep over the queue's R2 objects (`BEEF_BLOBS`): the last key a pass handled, the round in progress, the last complete count of the objects at rest, the last pass and the entries it could not read (the fold-4; a pass the cron or the operator's lever `POST /internal/beef-blob-sweep` runs); a lost row restarts the round at the first key |
| `change_parked` | overlay-cloudflare | none | none | transient | no | bsv-low #649 M1: the pot and hop change notes a background flush of an admit-fast job withheld for want of its `waitUntil` window, and (bsv-low #560, the outbox) every flush's new note-backs of the pot, hop and lobby sets, one row per (set, entry); the cron delivers the oldest 128 a set a tick and deletes only the rows of accepted POSTs (served at `/health/invariants.changeParked`); a lost row costs one push the felt's own reads recover |
| `change_parked_seq` | overlay-cloudflare | none | none | transient | no | a counter only (bsv-low #649 D3-M1): the change park's tokens, one per park batch, never reused; a wipe re-seeds it from the clock (times 2,048, one second ahead) at the next park |
| `ef_deferred_sweep` | overlay-cloudflare | none | none | transient | no | N5 (align-engine-050, LOW's E585 land2 lens): the one-row state of the cron's pass over the deferred EF jobs' R2 objects (`BEEF_BLOBS` under `ef-deferred/`): the last key a pass handled, the round in progress, the last complete count of the objects and bytes at rest (served at `/health/invariants.queue.r2.efDeferred`), the last pass; a lost row restarts the round at the first key |
| `ops_counters` | overlay-cloudflare | low-app-layer: INSERT | low-app-layer (its `tracker_chain_events_*` rows, #777, #778) | transient | no | operator counters |
| `ops_lease` | overlay-cloudflare | none | none | transient | no | tick ownership with bounded expiry |
| `ops_tick_events` | overlay-cloudflare | none | none | transient | no | indexed one-day tick activity and lease skips |
| `ops_heartbeat` | overlay-cloudflare | none | none | transient | no | a heartbeat |
| `worker_cost_daily` | overlay-cloudflare | none (the app layer writes its rows through the shared `d1_ledger.rs`, seen as the overlay's) | none | transient | no | bsv-low #635, the cost bar: requests and D1 rows per UTC day and Worker, served on `/health/invariants.costBar` |
| `submit_refusals` | overlay-cloudflare | none | low-app-layer | transient | no | a census window (#366) |
| `mutation_dead_letters` | overlay-cloudflare | none | none | lost | **yes** | bsv-low #576: the parked dead letters; once the DLQ consumer acks a letter this row is its only copy, and the operator's lever re-drives from it. The lens fold (M2): a scoped DELETE, `dead_letters::resolve` when a replay of the key is acked (its bytes landed, or were refused under an open eviction; the row copies nothing); the delta fold (D-M1): a second, `dead_letters::internal_discard` (`POST /internal/discard-dead-letters`, bearer `INTERNAL_TOKEN`), the operator's discard of named PARKED letters (at most 50 keys and, since the delta-2 fold D2-L1, 50 rows per call, oldest parked first, each logged with the sha256 of its bytes), so the ceiling can drain; at most 2000 letters hold bytes, of which at most 1000 are "not now" letters (column `class`, the delta-2 fold D2-M1: one per txid, 200 new a day), a row holds one queue message and 20 history entries; since bsv-low #585 door 3 a letter whose BEEF is past the queue's inline room holds its R2 key (`r2_key`, `r2_bytes`; the bytes are in the `BEEF_BLOBS` bucket, deleted by the worker at the letter's ack, LOST line, dropped copy or discard, never by an expiry rule) |
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
| `potparty_records_evicted` | overlay-cloudflare | none | low-app-layer | lost | **yes** | admit-fast's shadow twin of `potparty_records`: an evicted row lives ONLY here until readmitted; part of the eviction ledger (#470 section 3 item 8) |
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

### The overlay (`overlay-cloudflare`)

| store | owner | holds | never-wipe |
|---|---|---|---|
| R2 `BEEF_BLOBS` (`low-overlay-beefs`, `low-overlay-beefs-beta`; `overlay-beefs` for `wrangler.toml`), keys `mutations/<sha256>/<scope>` (bsv-low #585 door 3) | overlay-cloudflare | the queued BEEF of a replay past the queue's inline room: the ONLY copy of a parked keyed dead letter's bytes (`mutation_dead_letters.r2_key`). Deleted only by the deletion rule (an ack, a LOST deferral, the copy rule, the operator's discard) and the orphan sweep (past 8 days, named by no row); no bucket expiry rule | **yes** (a wipe turns every parked keyed letter into a fault letter with no bytes) |

### The relay (`~/bsv/rust-message-box`, `low-relay`)

| store | owner | holds |
|---|---|---|
| D1 `low-relay-prod` / `low-relay-beta`: `messages`, `message_boxes` | low-relay | the hand transcripts (`low_game_*` retained 14 days), the events boxes |
| D1: `transcript_tombstones`, `message_permissions`, `server_fees`, `device_registrations` | low-relay | purge marks, per-sender config, the fee table, push registrations |
| `MessageHub`, `EngineIoSession`, `BroadcastRegistry` DOs; `AUTH_SESSIONS` KV; R2 `BEEF_BLOBS` (unused) | low-relay | socket, session and presence state |

## Changing this

A new table: its CREATE in `OVERLAY_MIGRATIONS`, its row in `storage-ownership.json` and in the table above, in one commit; the check is red until all three agree. A new cross-crate write or read: the grant in the JSON with the reason in the row's `why`, reviewed like code. A table that leaves the never-wipe set needs the owner's word.

#638 adds `hop_pot_links`, `pot_hop_seats` and `view_derivation_state`: overlay-owned derived lookup rows, read by the app layer. Admission, relatching, spend reversal and eviction maintain them in the source transaction; the guarded backfill runs once.

| `hop_pot_links` | overlay-cloudflare | derived | no | App-layer identity lookup, source-write triggers. |
| `pot_hop_seats` | overlay-cloudflare | derived | no | App-layer outpoint lookup, source-write triggers. |
| `view_derivation_state` | overlay-cloudflare | derived | no | Guarded one-time legacy backfill. |
| `count_network_seen` | overlay-cloudflare | derived | no | bsv-low #645: one row, the count of `network_seen` kept by its insert and delete triggers; guarded one-time backfill. |
| `pot_beefs_unprovable_set` | overlay-cloudflare | derived | no | NS-COST-BAR-1: the txids of the latched `structurally_unprovable = 1` rows, kept by `pot_beefs`' triggers (idempotent under `INSERT OR REPLACE`); wiped only with its count row. |
| `count_pot_beefs_unprovable` | overlay-cloudflare | derived | no | NS-COST-BAR-1: one row, the count of `pot_beefs_unprovable_set` kept by its insert and delete triggers; guarded one-time backfill. |
| `transactions_retired_set` | overlay-cloudflare | derived | no | NS-COST-BAR-1: the txids of the retired `transactions` rows and whether each is `expired-unproven`, kept by `transactions`' triggers (idempotent under `INSERT OR REPLACE`); wiped only with its count row. |
| `count_transactions_retired` | overlay-cloudflare | derived | no | NS-COST-BAR-1: one row, the retired and expired counts of `transactions_retired_set` kept by its insert and delete triggers; guarded one-time backfill. |
| `count_pot_unspent_kind` | overlay-cloudflare | derived | no | NS-COST-BAR-1: three rows, the unspent `pot_records` per lock class kept by `pot_records`' triggers; the index janitor's backlog is these less the hour's young rows; guarded one-time backfill. |

#674 adds `board_wins`, `board_owners`, `board_hands` and `board_total`: the leaderboard derived at the write, overlay-owned, read by the app layer's `/leaderboard`. Triggers in the settle hooks' own statements (pot, witness, seat, claim; the eviction's shadow move and the readmission) maintain them; the backfill behind the `board-v1` stamp in `view_derivation_state` runs once. The lens fold (MEDIUM-1) adds each win's `frag` (the rows the page reads for it) and each owner's `card` (its wins and frags), so the page reads `board_owners` alone; the proof pointer (`proof_markers`) and posted bundle (`proof_posts`, read by the overlay's triggers) writes refresh the frags they feed.

| `board_wins` | overlay-cloudflare | derived | no | One row per counted win and its frag; source-write triggers. |
| `board_owners` | overlay-cloudflare | derived | no | Counted wins per owner and their card; `board_wins` triggers. |
| `board_hands` | overlay-cloudflare | derived | no | Each identity-owned win's hand; `board_wins` and marker triggers. |
| `board_total` | overlay-cloudflare | derived | no | The count of every counted win, one row. |
| `board_era` | overlay-cloudflare | transient | no | #674, the cutoff: the era cutoff the board is derived under, one row; the app layer's scheduled pass UPDATEs it from its `WRITTEN_OFF_BEFORE_MS` and `board_era_update` re-derives the board in that statement; `board_win_era` refuses a win before it. |
| `tracker_backfill` | low-app-layer | re-askable | Bounded, resumed proof-only deploy cursor; a lost cursor restarts the pass. |
| `overlay_chain_events` | overlay-cloudflare | re-askable | #620: the overlay's own cursor on chaintracks' typed chain-event feed (its reorg word), one versioned row; a lost row is re-seeded from the overlay's `CHAIN_EVENTS_START_CURSOR` (unset: it reads nothing and Arcade's orphan listing is the input again) or set by the overlay's lever `POST /internal/chain-events-cursor`. |
| `tracker_chain_events` | low-app-layer | re-askable | #771: the chaintracks feed cursor, one row; a lost row is re-seeded from `CHAIN_EVENTS_START_CURSOR` (unset: the intake pulls nothing) or set by the operator's lever `POST /internal/chain-events-cursor` (FIX-771-L1). |
| `tracker_reasks` | low-app-layer | re-askable | #771: the re-ask queue a chain event writes in its cursor's batch, drained eight a pass; a lost entry is a mined row rechecked at its next read or event. |
| `applayer_scheduled_pass` | low-app-layer | re-askable | #781, #778: the app layer's last scheduled pass, one row (its wall and each phase's wall), read by the overlay's `/health/invariants.appLayerScheduledPass`; the next pass writes it again. |
