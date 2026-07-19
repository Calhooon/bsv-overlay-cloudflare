# Overlay BEEF compaction / proof-completion — god-tier design (2026-07-19)

**Trigger:** the LOW overlay stores proofless BEEFs at submit and never re-fetches the
merkle BUMP when the tx mines, so stored/served BEEFs stay proofless and bloat forever
(bsv-low #192). References: `~/bsv/zanaadu/overlay` (proven dual-trigger pipeline on the
IDENTICAL engine crate) + `~/bsv/btc-relay-rs/docs/phantom-hardening-architecture.md` (the
verify-every-courier discipline). This design merges both.

## The invariant

> A stored BEEF carries a merkle BUMP as soon as its tx is mined, and that BUMP is a FACT
> only once its computed merkle root is verified against our PoW-anchored header source
> (chaintracks). No BEEF is ever compacted on a broadcaster's word. A proof is only as good
> as the header root — every courier (ARC, WoC, Bitails) is re-verified.

## The engine already has the machinery (shared `overlay-engine` crate)

- `AncestorFetcher::fetch_ancestor(txid) -> {raw_tx, proof: Option<bump_hex>}` (`gasp.rs:144`).
- `Engine::complete_missing_proofs(limit)` — bounded cron scan; **no-op without a fetcher** (`engine.rs:1307`).
- `Engine::handle_new_merkle_proof(txid, proof_hex, height)` — the idempotent stitch:
  `from_beef → set merkle_path → to_beef(true) → update_transaction_beef`, **recursing the
  consumed-by chain** so descendant BEEFs re-stitch (`engine.rs:1194`).

LOW wired NONE of the legs (no fetcher, no `has_proof` column / candidate query, no cron
call, no callback). zanaadu's `WocAncestorFetcher` (`~/bsv/zanaadu/overlay/src/ancestor_fetcher.rs`)
is a wasm-clean drop-in against this exact trait.

## The god-tier fetcher — courier ladder + chaintracks verify

`ChainProofFetcher: AncestorFetcher` — per txid, first VERIFIED wins, FAIL-CLOSED:

1. **ARC** (primary, LOW's own proxy): `GET /arc/v1/tx/{txid}` → if `txStatus == "MINED"`
   and `merklePath` present, it's a ready BRC-74 BUMP (height inline). (Mirror
   `broadcast.ts arcTxMinedProof` server-side.)
2. **WoC** (fallback): `GET /tx/{txid}/proof/tsc` (TSC JSON) + height from
   `GET /tx/hash/{txid}.blockheight` → `tsc_json_to_bump_hex(json, height)`
   (`rust-wallet-toolbox/src/tsc_proof.rs:38`).
3. **Bitails** (tertiary): `GET /tx/{txid}/proof/tsc` (identical TSC shape) → same converter.

**Verify before returning ANY bump** (the btc-relay-rs discipline, already LOW's
`bump_verifies` — `overlay-discovery/src/pot/lookup_service.rs:100`):
```
let root = bump.compute_root(Some(txid))?;
CHAINTRACKS.is_valid_root_for_height(&root, bump.block_height).await? == true
```
Any hiccup (no tracker / compute err / tracker err / false) → treat as UNMINED
(`proof: None`), never a positive. Content-address: verify `raw_tx` hashes to `txid`
(trait mandate). Unmined at every tier → `proof: None` (retry next tick), NOT an error.
Per-tick fetch budget (`DEFAULT_FETCH_BUDGET = 40`) under the CF subrequest cap; WoC/Bitails
tick only on ARC miss (audit-only posture: worker cron, never the browser).

## Two stores — BOTH must compact

1. **Engine `transactions` store** (general, GASP-synced). The zanaadu wiring covers it:
   - Migration: `ALTER TABLE transactions ADD COLUMN has_proof INTEGER NOT NULL DEFAULT 0` +
     index (append-only, IF NOT EXISTS, never DROP/RENAME).
   - `D1Storage`: `find_transactions_for_proof_check` = `... WHERE has_proof=0 ORDER BY
     RANDOM() LIMIT n` (RANDOM defeats head-of-queue starvation — zanaadu prod incident) +
     `mark_transaction_proven`; bind `has_proof` in `update_transaction_beef`/`insert_output`.
   - `set_ancestor_fetcher(ChainProofFetcher)` in `build_engine_with_storage`.
   - Cron `scheduled`: `engine.complete_missing_proofs(budget).await` + log summary.

2. **`pot_beefs` store** (LOW-specific — the RECOVERY surface `/pots-view`/`/recovery-view`/
   `/beef` serve). The engine does NOT touch it. Needs a PARALLEL pass:
   - Add `has_proof` to `pot_beefs` + a proofless-candidate query (same RANDOM/limit shape).
   - A pot-store proof-completion tick (in the same cron): per proofless pot_beef, run the
     SAME fetcher → verify → stitch the bump → **trim proven ancestry** (`Beef::trim_known_proven`)
     → write back. **The write MUST bypass the "longer-wins" guard** (`PotStorage::store_beef`,
     `storage.rs:41`) for a compaction write — a bumped BEEF is authoritative even when
     SHORTER. Add a dedicated `compact_pot_beef(txid, new_beef)` that overwrites when the new
     BEEF proves the tx (has_proof) regardless of length. Self-containment guard preserved
     (never store a BEEF missing its own txid).

## Serve-time compaction (the actual shrink)

`compact_beef(subject_txid, beef)` = `Beef::trim_known_proven()` — BFS from tips, STOP
descending at any tx carrying a BUMP, drop now-unreachable ancestor raw-tx bytes, GC orphan
BUMPs. STRICTLY passthrough-on-failure (return original bytes on any error; verify
`verify_valid(true)` before substitution; preserve Atomic-vs-plain format). Only pays off
AFTER proofs are stitched, so it runs after the completion pass (or at serve time in
`low-app-layer`).

## Observability (zanaadu's most expensive omission — a dead pass hid for WEEKS)

- `ops_heartbeat` singleton upserted every tick; persistent counters
  `proofs_completed_total` / `fetch_failed_total` / `pot_beefs_compacted_total`.
- `proofless_watch` first-seen ledger → flag any tx proofless > 24h.
- `GET /health/invariants?strict=1` → 503 when a completion pass has been dead > N ticks
  (nightly cron / alarm). **A dead completion pass must surface in a day, not weeks.**

## Robustness (copy verbatim)

Idempotent (stitch overwrites; a callback for an already-proven tx logs + 200s). Bounded
page per tick. Skip-don't-error on unmined + parse/fetch/stitch failures (counted, retried).
Proofless-cache defeat: a 0-conf cached blob carries no BUMP → treat as fallback, re-ask
(budget-bounded). No negative caching of the proofless answer (self-upgrades). Once-per-txid
success latch (mirror `low_pot_pointer_healed`).

## Optional push path (lower latency, later)

Register `X-CallbackUrl: {HOSTING_URL}/arc-ingest` + `X-CallbackToken` at ARC broadcast
(`broadcaster.rs post_arc_tx`); the existing `/arc-ingest` route (`routes.rs:801`) already
calls `handle_new_merkle_proof`. Bearer-auth the callback (constant-time). The cron pull is
the proven baseline and works alone; the callback is a latency optimization, not required.

## Phased plan

- **P1 — the fetcher** (`ChainProofFetcher`: ARC→WoC→Bitails + chaintracks verify), unit-tested
  (ARC hex path, TSC→BUMP, verify rejects a forged root, unmined → None).
- **P2 — transactions store**: migration + candidate query + `set_ancestor_fetcher` + cron call.
- **P3 — pot_beefs store**: migration + candidate query + `compact_pot_beef` (bypass longer-wins) + cron tick.
- **P4 — serve-time `trim_known_proven`** + observability (heartbeat/invariants).
- **P5 — deploy + PROVE**: a stored BEEF for a mined tx gains its BUMP + shrinks; a
  proofless one for an unmined tx is retried; `/health/invariants` green.
- **P6 (optional) — ARC callback push.**

Gate: this is money-adjacent infra (recovery reads these BEEFs). `cargo test --workspace` +
wasm build green; deploy both `low-overlay` + `low-app-layer`; prove compaction on mainnet.
