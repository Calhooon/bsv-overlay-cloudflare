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

---

# 2026-08-06 — `/arc-ingest` 401'd EVERY Arcade proof callback since #228 (fixed, NOT yet deployed)

## The defect: one wrong header name

`arc_ingest` read the bearer token from exactly one header (`routes.rs`,
`req.headers().get("x-callbacktoken")`) and 401'd when it was absent. **Arcade never sends
that header on the webhook.** Arcade V2's published contract is that `X-CallbackToken` is
"an opaque bearer token, sent on every outbound webhook as `Authorization: Bearer <token>`" —
the header name is the *registration* spelling, not the *delivery* spelling.

Arcade's own delivery record for settle `ee37b606…`:

```json
{ "callbackUrl": "https://low-overlay.dev-a3e.workers.dev/arc-ingest",
  "lastDeliveredStatus": "MINED", "attempts": 10,
  "lastAttemptAt": "2026-08-06T03:51:29.445425Z", "lastResult": "status 401" }
```

**Measured consequence:** a verdict that should publish ~6 min after the block took **~53 min**,
of which block time was only 12.6 min. With the push path dead, everything falls through to the
poll backstop — `PUSH_BACKSTOP_MIN_AGE_SECS = 30*60`, then `ORDER BY RANDOM() LIMIT 20` every
~2–3 min against a ~190-row pool ≈ 24 min drain.

## The fix

1. **Read the token where the sender actually puts it.** `classify_arc_callback_auth` accepts
   `Authorization: Bearer <token>` (RFC 7235 — case-insensitive scheme) **in addition to**
   `X-CallbackToken`, by ENUMERATE-AND-FILTER rather than header precedence: a candidate from
   either header is admitted iff it equals the subject txid. First-header-wins would refuse a
   courier that spends `Authorization` on a proxy credential.
   **Nothing else moved.** The constant-time compare against the body's subject txid
   (`constant_time_eq`) and the chaintracks re-verification of the merklePath
   (`proof_fetcher::verify_bump`, fail-closed → 422) are byte-identical. The courier is still
   never believed.
2. **Count the refusal (epoch Rule 13) —** and this is why the outage hid for a month. The 401
   arm bumped no counter, and `arc_ingest_status_ignored_total` is only reachable *after* auth.
   So `{arc_ingest_pushed_total: 0, arc_ingest_status_ignored_total: 0}` read **identically**
   for "nobody is calling us" and "everybody is being refused". #303's closing note blamed an
   unfunded ad wallet, which was never the cause. Two new counters make the two states
   distinguishable, and their *diagnoses* differ:
   - `arc_ingest_unauthorized_no_token_total` — no credential presented ⇒ a CONTRACT/CONFIG
     problem (exactly this bug's signature).
   - `arc_ingest_unauthorized_bad_token_total` — a credential presented that did not match ⇒ a
     stale registration, or a prober.
   Both surface in `GET /health/invariants` → `counters`, alongside a derived
   `arcIngestPushHealth` ∈ {`silent`, `refusing`, `flowing`} — the one distinction the health
   surface previously could not express. **Boundary, stated (Rule 17):** those are monotonic
   lifetime totals, so the derived verdict is a standing-start diagnosis; the continuous
   instrument is the DELTA of the two `unauthorized_*` counters between two reads.

## Follow-ons — RECORDED, deliberately NOT fixed here

### 1. The verdict gate asks for more than the doctrine allows (bsv-low, `low-app-layer`)

Owner ruling: **SEEN_ON_NETWORK is the finality bar; proofs arrive later and must not gate money
or its display.** `is_confirmed_landing_with_proof` (`crates/low-app-layer/src/logic.rs:1316`)
requires `spentConfirmed == true` OR `spender_proof_verified == true` — **both merkle-class**, so
a settle that the network has already accepted is invisible until a proof lands.

Its recorded justification (#323) is entirely about **non-final parked refunds** — and that is a
property the spender's own bytes already carry: `lockTime > 0 && any sequence < 0xffffffff`. The
client already tests exactly that (`app/src/lib/stake.ts:7688-7700`). A cooperative covenant
settle is FINAL and **cannot** be parked, so the merkle-class requirement is buying nothing on the
path it most delays. This is the network-enforcement instinct: the payload already carries the
proof the check was invented to substitute for (epoch Rule 4).

Fixing it belongs in `low-app-layer`, not here. **It is also the larger half of the latency
story** — this repo's fix restores the ~6 min push, but the display gate can still hold a verdict
behind a proof it does not need.

### 2. `ORDER BY RANDOM()` pools never retire structurally-unprovable rows

A superseded pre-signed refund never mines *by design* (~64% of them, per #369), so it stays in the
proofless sample forever and dilutes it. Poll latency is `(pool / 20) × tick` and `pool` grows
**monotonically with lifetime hands played**: ~24 min at today's ~190 rows, ~50 min at 400, with no
alarm — `prooflessOver24h` cannot distinguish a real backlog from permanent residue either (it is
the same Rule 13 collapse as the 401 counter, one layer down). Recorded, not fixed: the shape of the
fix is to retire rows that are structurally unprovable (a spend displaced by a landed conflicting
spend of the same outpoint) rather than to raise the cap.
