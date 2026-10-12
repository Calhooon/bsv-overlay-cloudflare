# NS-2-D1BOUND readiness, 2026-10-12
Status: code and measured fleet budget GREEN; independent money lens and next-loop live census pending. Route tier is owed under the ten-minute command cap.
Branch `lane/ns-2-d1bound`, base `4634090`. No deploy or spend. Locks unchanged.

## Commits and cursor rule
Prior steps: `e82c6b6` fleet fixture/plan pin; `2e03d38` stored-key reads and canonical writer; `777c9b5` transactional derived links/seats; `272ecda` initial readiness; `dbc628f` #639 terminal flags.
Follow-up: `0fe820d` createdAt cursor and D1 page pin; `77a7f7d` shared bounded party hydration; `891e66b` sibling-route measurement and cursor read-ownership pins.
Files: `src/owed_cursor.rs`, `routes::owed_party_cursor`, `routes::owed_recompute`, `tests/d1_fleet_plans.rs`, `tests/support/cursor_measure.rs`, `examples/d1_budget_seed.rs` (under `crates/low-app-layer`), `tools/lane-499/cursor_pin.mjs` and `storage-ownership.json`.
Each source walks its own `(identity,createdAt,rowid)` index. The initial party/link statement takes at most 16 rows per source; subsequent source pages and hydration chunks take 32. NULL stamps are retained. Equal stamps use an equality and rowid seek, then a separate next-stamp seek. The tuple-range control revisited equal stamps and exceeded 287 (288 reads on page eight); the corrected workerd pin measures a maximum of 160 reads across 177 source/hydration pages, each at or below 287.
`created_at_cursor_union_equals_unpaged_ranked_views_byte_for_byte` compares the full ordered rows and each separate production projection, including NULL stamps, all validity tiers and millisecond-era boundaries. Selection keeps the same representative rank/oldest marker, global unknown quota, dense ranks, probes, oldest-hop-row cap and 20-page walk cap. Cursor order governs retrieval, not serving order. Results still derive in the original 100-row groups. A cursor time-budget failure preserves the previous snapshot.
Results/refunds share candidate metadata and one hydration statement, with separate proof joins/flags. The strengthened fixture exposed 30 owed statements against 28; shared initial/hydration reads brought the request back to 28 without raising any ceiling.

## Fleet measurement
Fixture: 22,268 pot/hop outpoints, 14,719 parties, 7,549 hop markers, 7,209 backups and sweeps, 19,005 proven transactions and 150 evictions; whale 1,033 parties and 554 hops, including 54 hop-only candidates. A/B retain their 8/2 pots. A now has inert synthetic decoded columns and a latched seat marker, exercising the real hop-seat and valid-backup legs. No transaction is broadcast by the fixture.
BEFORE: a reversible source copy uses main `4634090`'s party union, hop-seat helper, recompute rank walk and key predicates, seeded with migrations 1 to 189 and the same fleet fixture. `D1_BUDGET_MEASURE=1 make ci-d1-budget` records real Server-Timing/D1 counters; ceilings are deliberately not enforced on this RED control. Sources were restored byte for byte by a finally block.
AFTER: ordinary `make ci-d1-budget`, all unchanged request, statement, write and statement-count ceilings enforced. Private bindgen CLI 0.2.126 at `$HOME/bsv/targets/wbg-0.2.126/bin/wasm-bindgen`, matching `workers/Cargo.lock`; global 0.2.129 was not used and the lock was not bumped.
| Route / scenario | BEFORE reads | AFTER reads | Bar / context |
| --- | ---: | ---: | --- |
| `/utxo-status`, A 8 | 8 | 8 | 12 |
| `/pots-view`, A 8 | 8 | 8 | 12 |
| `/results:view`, A compute | 7,805 | 256 | 287; 8 statements, zero writes |
| `/results`, forwarding | 0 | 0 | 4 |
| `/owed`, A first read/recompute | 15,323 | 311 | 528; 28 statements, 18 writes |
| `/owed`, served read | 17 | 17 | 26 |
| `/submit`, one local collected admission | 0 | 0 | 4; 13 writes, 8 statements |
| `/refund-view`, A | 143 | 143 | measured sibling, no invented request bar |
| `/recovery-view`, A | 7,950 | 401 | measured sibling, no invented request bar |
| `/refund-backups`, A | 10 | 10 | measured sibling, no invented request bar |
| `owed-recompute`, B/opponent hook maximum | 15,041 | 203 | 24 detached walks |
| `/internal`, hook maximum | 6 | 6 | 13 announcements, 2 sheds |
These are measured A/B HTTP scenarios with fleet noise. Whale source/hydration pages are separately measured on local D1, not held against the fixed A/B whole-request ceilings; a full whale HTTP request measurement remains outside this tier.
`hot_routes_never_scan_fleet_tables`: main RED (fleet SCANs and quadratic identity-index exclusions), shipped hot-route fleet SCAN list empty. Current pins include stored-key proof/spender/backup reads and legacy indexed hop-seat lookup. `d1_ledger::Tally::max_reads` is enforced by the route harness beside per-request reads.
The local wrangler CLI discards rows_read in its JSON output. The page pin therefore uses the same local Miniflare D1 backend directly, compares each result with native SQLite's ordered rows, and checks real meta.rows_read; it never substitutes result length for reads.

## Byte identity and #639
`routes::ns2_d1bound_compat::fleet_served_bodies_are_byte_identical_before_and_after_derivation` pins five production bodies: `/results`, `/refund-view`, `/recovery-view`, `/refund-backups` and the owed judgement body. Original/derived SQL feed production D1 mappers, assemblers and body builders, at offsets 0/100/1000, with fixed side evidence. The cursor additionally pins identical ordered production inputs and separate results/refund proof flags. This is fixture byte identity, not a live-state or live-census claim. Real hop money facts retain their existing end-to-end pin.
#639 app-layer half (`dbc628f`): `/tx-any` (single and batch) and `/spent-any` carry additive `terminal`. Definitive memo/ladder kinds set true; young or one-source `Absent` sets false. Removing the additive field preserves the previous response bytes. The monitor half is outside this lane.

## Validation and remaining work
Native app-layer suite: 655 tests, exit 0. App-layer all-target clippy: exit 0. Storage ownership: 794 SQL references in 3 crates match the manifest, exit 0; self-test exit 0. JS syntax, config placeholder checks and diff checks: exit 0. New files and changed Rust hunks are formatted; whole-crate formatting still has the previously documented baseline drift.
BEFORE budget control: exit 0, launch load 17.83. AFTER `make ci-d1-budget`: exit 0, launch load 17.88. Page pin: 177 pages, maximum 160/287 reads. Root engine/discovery wasm32: exit 0, launch load 18.03. Final overlay/app-layer release wasm32: exit 0, launch load 9.64.
Route tier: owed. The separately bounded recipe also reached the 580-second foreground cap, exit 124, during the large-body route harness after the script-door checks; the remaining route expectations are unverified. Full `make ci-route` attempt at load 19.72 hit the 580-second foreground cap in its budget prerequisite, exit 124. The budget prerequisite independently passed unchanged; `make -o ci-d1-budget ci-route` runs the remaining recipe as its own bounded leg (launch load 7.85), not an omitted test or a raised ceiling. That recipe ran its startup and multiple route harnesses but did not finish; it is not reported GREEN.
One superseded diagnostic launched at load 28.19 before the fresh uptime guard was added and hit the cap. Final heavy invocations use the immediate uptime guard (<24). A shared `/tmp` runner was overwritten by another lane; its unexpected invocation rejected the unknown argument before running a cell. This lane's runner was moved into its private target tree. No deploy or spend occurred.
Logs: `/tmp/ns-2-d1bound-2-budget-before.log`, `/tmp/ns-2-d1bound-2-budget-after-pass.log`, `/tmp/ns-2-d1bound-2-native-pair-final.log`, `/tmp/ns-2-d1bound-2-clippy-final.log`, `/tmp/ns-2-d1bound-2-route.log`, `/tmp/ns-2-d1bound-2-route-recipe.log`, `/tmp/ns-2-d1bound-2-wasm-workers-final.log`.
Unverified: independent from-scratch money lens; live #634 census after the next loop/deployment; full whale HTTP request totals. Next: the independent lens and the owed route tier, then the captain's land and next-loop census. This lane does not deploy.
Also-seen titles, unchanged: Leaderboard: page the whole-board read (80,189); Tip hook: bound the unspent-pot read per block (6,622); Overlay: bound the two pot_records scans per pass; Invariants: materialize network_seen count instead of COUNT(*).

### Why migrations 190 to 211 are 22 entries
These are 22 independently replayable, additive SQL statements: one read index, two derived tables, six supporting indexes, one guard table, nine source triggers, two guarded backfills and one completion stamp. Cold starts replay CREATE IF NOT EXISTS and guard the backfills. No DROP or RENAME.
190: `idx_hopparty_seat`, stored settle-key lookup for legacy undecoded pots.
191: `hop_pot_links`, stored identity-to-pot link rows.
192: `idx_hop_pot_links_identity_created`, identity cursor source.
193: `idx_hop_pot_links_pot`, party suppression and pot changes.
194: `pot_hop_seats`, stored pot-to-hop-seat lookup rows.
195: `idx_pot_hop_seats_marker`, source-marker removal/relatching.
196: `idx_pot_pub_a`, find pots when seat A's marker arrives.
197: `idx_pot_pub_b`, find pots when seat B's marker arrives.
198: `view_derivation_state`, guarded legacy-backfill completion.
199: `idx_pot_spending_lower`, find hop spenders for pot-first admission.
200: `derive_hop_insert`, marker-first admission links and seats.
201: `derive_pot_insert`, pot-first admission links and seats.
202: `derive_party_insert`, suppress synthetic links when a party exists.
203: `derive_hop_update`, relatch/re-key links and seats.
204: `derive_pot_update`, spend reversal, decoding and pointer changes.
205: `derive_party_update`, maintain suppression after a party changes.
206: `derive_hop_delete`, remove a marker's derived rows.
207: `derive_pot_delete`, remove an evicted pot/hop's derived rows.
208: `derive_party_delete`, restore links when the last party disappears.
209: guarded link backfill for pre-existing rows.
210: guarded seat backfill for pre-existing rows.
211: completion stamp, only after both backfills.
The adjacent CREATE statements (190 to 199) and source-trigger statements (200 to 208) could be grouped into migration batches if the runner were changed to execute batches. With the current one-statement runner they stay separate. Backfills 209/210 must precede 211; folding them would require transactional batch execution. No migration folding is necessary for correctness and none is done here.

Merge addendum (D1BD-L1, main `17418b1`): register the triggers as `derived_hops::HOP_INSERT` through `PARTY_DELETE`, followed by `LINKS_BACKFILL`, `SEATS_BACKFILL`, and `STATE_DONE`; at the merge onto main's 196 these occupy 207–215, 216–217, and 218. The numbered list above records the original branch, not stable migration identities.
