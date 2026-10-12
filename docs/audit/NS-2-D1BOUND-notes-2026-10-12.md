# NS-2-D1BOUND readiness, 2026-10-12
Status: WALLED, partial implementation for review, not a completed #638 land.
Branch `lane/ns-2-d1bound`, base `4634090`; launched at load 6.81. No deploy or spend.
Step 1: `e82c6b6`, fleet fixture and plan pin. Main was RED: three fleet SCANs plus identity-index exclusion in four views.
Step 2: `2e03d38`, stored-key reads, canonical hop writer, unary-plus proof filter and pot-index exclusion; plan GREEN.
Step 3 (partial): `777c9b5`, transactional derived links and pot-seat lookups; the createdAt recompute cursor is NOT implemented.
Files: `examples/d1_budget_seed.rs`, `tests/support/d1_fleet.rs`, `tests/d1_fleet_plans.rs`, `tests/write_derived_reads.rs`.
Read functions: `logic::party_candidates_sql`, `results::{hop_seat_markers_sql,legacy_hop_seat_markers_sql}`, `routes::results_hop_seat_markers`, `owed::{covenant_spenders_sql,valid_refunds_sql}`, `hopsweep::sweep_proofs_sql`.
Writes: `overlay-cloudflare/src/derived_hops.rs` source-table triggers cover either admission order, relatching, party suppression, spend reversal and eviction; legacy backfill is guarded by `view_derivation_state`.
Migrations: 190 seat-key index; 191 to 211 derived schema, supporting indexes, triggers and guarded backfill. Additive, no DROP or RENAME.
Fixture noise: 22,268 pot/hop outpoints, 14,719 parties, 7,549 hop markers, 7,209 backups and sweeps, 19,005 proven transactions, 150 evictions; whale 1,033 parties and 554 hops, 54 hop-only candidates.
The original A/B 8/2-pot ceiling scenarios remain; the whale is exercised by native pins, not yet a measured route-tier scenario. Fleet rows use disjoint synthetic keys and inert bytes.
`d1_ledger::Tally::max_reads` exposes the largest statement on timing and health; `tools/lane-499` enforces it. Request and statement bars remain 287/528.
| Route | Fixture before reads | Fixture after reads | Verification |
| --- | --- | --- | --- |
| `/results:view` | unmeasured | unmeasured | plan and production body pins GREEN |
| owed recompute | unmeasured | unmeasured | source queries and production owed judgement body GREEN; cursor outstanding |
| `/recovery-view` | unmeasured | unmeasured | plan and production body pins GREEN |
| `/refund-backups` | unmeasured | unmeasured | plan and production body pins GREEN |
Byte identity: `routes::ns2_d1bound_compat::fleet_served_bodies_are_byte_identical_before_and_after_derivation` uses original SQL, production D1 row readers, assemblers and five body builders at cursors 0/100/1000; side evidence is fixed. Real hop money facts retain their existing end-to-end pin. Legacy undecoded pots retain indexed seat-key reads.
#639: `txany::{answer_from_verdict_memo,tx_any_value,spent_any_body}` and `routes::spent_any` add terminal true for definitive kinds, false for Absent; single, batch and outpoint pins GREEN, existing bytes preserved when the additive field is removed.
Validation: 654 app-layer tests GREEN; both-worker all-target clippy GREEN; storage ownership, JS syntax and diff checks GREEN. Changed-hunk rustfmt converged; whole-crate cargo fmt --check still reports pre-existing formatting, left outside this lane's hunks. Native validation saw shared loads above 100.
D1 tier: launched below 24, failed before any route measurement: required shared wasm-bindgen binary 0.2.129 disagrees with locked schema 0.2.126. Logs `/tmp/ns-2-d1bound-budget-before.log`, `/tmp/ns-2-d1bound-final-tests.log`, `/tmp/ns-2-d1bound-final-clippy.log`.
Also-seen issue titles: Leaderboard: page the whole-board read (80,189); Tip hook: bound the unspent-pot read per block (6,622); Overlay: bound the two pot_records scans per pass; Invariants: materialize network_seen count instead of COUNT(*).
Next: finish the createdAt cursor while preserving the existing rank-window selection and anti-flood rules; resolve bindgen compatibility, measure both generations on the fixture and enforce unchanged ceilings; route tier and wasm32 at land; live census after the next loop; independent money review. These remain unverified.
