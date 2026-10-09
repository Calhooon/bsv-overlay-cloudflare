#!/usr/bin/env node
/**
 * NL-6 route-level witness, part of `make ci-route`: the `/submit` route does
 * not refuse a valid BEEF for its size (the charter "a BEEF of any size",
 * bsv-stack-lean docs/charters/beef-of-any-size.md).
 *
 * Until NL-6 the route answered 413 "BEEF too large (max 10MB)" to any body
 * over 10,000,000 bytes before a reader saw it. No native cell reaches that
 * line (`submit()` takes a `worker::Request`), so this drives the route.
 *
 * The body is a proofless V1 BEEF of one transaction whose one output is an
 * OP_FALSE OP_RETURN of padding: valid bytes, 10,000,001 bytes and more, and
 * nothing `tm_collected` admits, so no row is written whatever the size.
 *
 * Expects the strict worker of `make ci-route` (SUBMIT_ENFORCE=true,
 * SUBMIT_OPERATOR_TOKEN=ci-submit-tok). No network is needed.
 *
 * MODELLING BOUNDARY: the worker is `wrangler dev --local`. What the deployed
 * platform does with a request body over its own plan limit is the
 * platform's and is not asserted here.
 */
import { randomFillSync } from 'node:crypto'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8791'
const FORMER_CAP = 10_000_000

const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
function varint(n) {
  if (n < 0xfd) return Buffer.from([n])
  if (n <= 0xffff) { const b = Buffer.alloc(3); b[0] = 0xfd; b.writeUInt16LE(n, 1); return b }
  return Buffer.concat([Buffer.from([0xfe]), u32(n)])
}

/** A proofless V1 BEEF of exactly `size` bytes: one tx, one padded OP_RETURN. */
function paddedBeef(size) {
  const build = (pad) => {
    const prev = Buffer.alloc(32)
    randomFillSync(prev) // a fresh txid every run
    const script = Buffer.concat([Buffer.from([0x00, 0x6a, 0x4e]), u32(pad), Buffer.alloc(pad, 0x42)])
    const raw = Buffer.concat([
      u32(1),
      varint(1), prev, u32(0), varint(0), Buffer.from([0xff, 0xff, 0xff, 0xff]),
      varint(1), u64(0), varint(script.length), script,
      u32(0),
    ])
    return Buffer.concat([Buffer.from([0x01, 0x00, 0xbe, 0xef]), varint(0), varint(1), raw, Buffer.from([0x00])])
  }
  const overhead = build(100_000).length - 100_000
  const beef = build(size - overhead)
  if (beef.length !== size) throw new Error(`built ${beef.length} bytes, wanted ${size}`)
  return beef
}

async function submit(body, { token } = {}) {
  const headers = {
    'Content-Type': 'application/octet-stream',
    'x-topics': JSON.stringify(['tm_collected']),
    'x-submit-mode': 'historical-tx-no-spv',
  }
  if (token) headers['Authorization'] = `Bearer ${token}`
  const res = await fetch(`${BASE}/submit`, { method: 'POST', headers, body })
  return { status: res.status, text: await res.text() }
}

let failures = 0
function expect(label, ok, got) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}  → ${got}`)
  if (!ok) failures++
}

// The control: the same shape under the former cap is read (200, nothing admitted).
const under = await submit(paddedBeef(100_000), { token: 'ci-submit-tok' })
expect('operator, 100,000 bytes: read', under.status === 200, `status=${under.status}`)

// One byte over the former cap, and well over it: read as the control was.
for (const size of [FORMER_CAP + 1, 12 * 1024 * 1024]) {
  const r = await submit(paddedBeef(size), { token: 'ci-submit-tok' })
  expect(
    `operator, ${size.toLocaleString('en-US')} bytes: read, never 413`,
    r.status === 200 && !/too large/i.test(r.text),
    `status=${r.status} ${r.text.slice(0, 120)}`,
  )
}

// Size is no bar and no pass: an unauthenticated body over the former cap
// meets the same gate as a small one (401), not a size refusal.
const unauth = await submit(paddedBeef(FORMER_CAP + 1))
expect('unauthenticated, 10,000,001 bytes: the gate answers (401), not a size refusal', unauth.status === 401, `status=${unauth.status}`)

if (failures) {
  console.error(`\nNL-6 submit-any-size route witness: ${failures} expectation(s) FAILED.`)
  process.exit(1)
}
console.log('\nNL-6 submit-any-size route witness: every expectation held.')
