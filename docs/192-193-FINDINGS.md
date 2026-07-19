# #192/#193 BEEF compaction — swarm build, review findings + remaining work (2026-07-19)

Built by an orchestrated 5-phase swarm against `docs/BEEF-COMPACTION-DESIGN.md`.
**Status: APPROVE-WITH-FINDINGS.** The core invariant HOLDS — every merkle BUMP
newly stitched by the cron pull (`complete_missing_proofs`, `complete_pot_beef_proofs`)
is chaintracks-verified (`ChainProofFetcher`) before it lands. Workspace + wasm build
green; `cargo test --workspace` green (733/133/110/… suites). **NOT yet deployed.**

## What landed (this branch, overlay repo)
- **P0** Arcade V2 sole broadcaster (`broadcaster.rs` `ArcadeBroadcaster`, EF-only, registers
  `X-CallbackUrl=/arc-ingest` + `X-CallbackToken` + `X-FullStatusUpdates`; SEEN gate via bounded
  poll). Old TAAL/GorillaPool retained as fallback. `/submit` admits on subject-SEEN.
- **P1** `proof_fetcher.rs` `ChainProofFetcher: AncestorFetcher` — Arcade→WoC-TSC→Bitails-TSC,
  every bump re-verified via `compute_root` + `is_valid_root_for_height`, fail-closed.
- **P2** `has_proof` migration + `find_transactions_for_proof_check` (RANDOM/limit) +
  `mark_transaction_proven`; `set_ancestor_fetcher` + cron `complete_missing_proofs(40)`.
- **P2.5** `/arc-ingest` Arcade callback: constant-time `X-CallbackToken` + chaintracks re-verify
  BEFORE stitch.
- **P3** `pot_beefs` `has_proof` + `compact_pot_beef` (overwrite-when-proven, BYPASSES longer-wins)
  + a pot-store completion tick in the same cron.
- **P4** serve-time `compact_beef` = `trim_known_proven` (passthrough-on-failure) in low-app-layer
  `/beef`; `ops.rs` heartbeat/counters + `/health/invariants`.

## Findings to CLEAR before deploy
- **MEDIUM — transactions-store `has_proof` latched on STRUCTURAL bump, no chaintracks re-verify**
  (`d1_storage.rs:140 beef_has_proof` on insert / `:305 mark_transaction_proven`). Serve-time
  `compact_beef` then trims trusting it. Safe only if admit-time SPV gated every bump-bearing BEEF;
  a legacy row admitted via the OLD unauthenticated/unverified `/arc-ingest` could be latched-proven
  and trimmed on. Fail-closed (a bad trim yields an invalid BEEF the CONSUMER rejects → recovery
  liveness, never theft). **Fix:** stop trusting a structural bump at admit — set `has_proof=0` on
  insert so the VERIFYING cron pass is the sole latch; OR gate serve-time trim on a separately-stored
  `verified` flag; OR re-verify in `mark_transaction_proven`. The pot_beefs pass already re-verifies.
- **MEDIUM — completion hard-depends on free-tier WoC raw-tx fetch FIRST** (`proof_fetcher.rs:209,240`,
  no `WOC_API_KEY`). ~60 raw + ~60 height probes/tick vs ~3 req/s → 429 → completion stalls even when
  Arcade has the proof. **Fix:** reuse the raw already in the stored BEEF (`cand.beef` / pot `stored_beef`)
  and/or wire `WOC_API_KEY`.
- **LOW** `/arc-ingest` bearer token is the PUBLIC subject txid (not real auth; merklePath is
  independently chaintracks-verified so no false compaction — minor DoS only). Consider a real shared secret.
- **LOW** no `MemoryPotStorage` unit test for `compact_pot_beef` (longer-wins bypass / proofless no-op /
  candidate filter). Add a red/green cell.
- **LOW** `proofless_watch` enrol uses `LIMIT 500` with no `ORDER BY RANDOM()` → a >500 backlog undercounts
  the dead-pass signal. Use RANDOM() or raise the cap.

## Remaining work (NOT in this diff)
- **#193 tower leg (bsv-low repo):** route `workers/low-watchtower/src/broadcast.rs` through the overlay
  `/submit` (keep the pre-signed-refund dead-man's own broadcast as last resort). The overlay side
  (Arcade sole broadcaster) is done here; the tower still broadcasts directly.
- **Client overlay-first:** verify `broadcastPotTxOverlayFirst` covers every money tx (already shipped;
  audit only).
- **Deploy (batched with the #66 + #169 cutover):** real D1 id injected, both workers deployed, then
  PROVE on mainnet — a mined tx's stored BEEF gains its BUMP + shrinks (via callback AND cron);
  `/health/invariants` green.
