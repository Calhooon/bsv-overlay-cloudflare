#!/usr/bin/env node
/**
 * #371 route-level ASSERTING harness for the `network_seen` latch.
 * Part of `make ci-route` (Rule 22 / gate MEDIUM-1: both latch producers are
 * wasm-route-only — the gated arm's synchronous latch and the ungated arm's
 * backgrounded Arcade corroboration — so every native tier stays green with
 * either call site deleted. This is the executable bar that says the third
 * arm's feed is ALIVE, observed through `/health/invariants.networkSeenTotal`.)
 *
 * Runs its own FIXTURE Arcade (this process, `FIXTURE_PORT`) so no leg ever
 * touches the real network:
 *   POST /tx        → 200 {"txStatus":"SEEN_ON_NETWORK"} (the gated arm's
 *                     submit; echo-less body — the txid the route trusts is
 *                     the one IT computed from the beef)
 *   GET  /tx/:txid  → allowlisted ⇒ 200 {"txid":<path>,"txStatus":
 *                     "SEEN_ON_NETWORK"} (path-echo satisfies the L4 echo
 *                     bar); else 404 (Arcade does not know the tx)
 *
 * Expectations:
 *  1. UNGATED admitted: an operator `historical-tx-no-spv` submit whose
 *     subject the fixture knows moves `networkSeenTotal` by 1 (the
 *     backgrounded corroboration — polled, since `wait_until` runs after
 *     the response), AND the fixture logged a GET for that exact subject.
 *  2. UNGATED unknown: same shape, subject NOT allowlisted — the fixture
 *     answers 404, the count must NOT move, and the GET must have been
 *     ATTEMPTED (distinguishing "corroborated and refused" from
 *     "corroboration never ran" — Rule 13).
 *  3. REFUSED body: an unparseable submit (engine 400) fires NO fixture GET
 *     at all (gate MEDIUM-2: corroboration only after `engine.submit`
 *     succeeds — no free Arcade fan-out for garbage).
 *
 * MODELLING BOUNDARY, stated per Rule 22: this cell pins the UNGATED
 * producer end-to-end. The GATED arm's latch is NOT behaviorally driven
 * here — every Arcade accept claim is corroborated against the hardcoded
 * TAAL/GorillaPool hosts (`gate_accept_claim_with`), so a hermetic gated
 * leg would need a corroborator-host config knob added to the MONEY
 * broadcast path for CI's sake, which we decline. The gated latch is
 * covered by (a) the Rule-9 positive source pin
 * `routes_call_the_network_seen_latch_from_both_arms` (native), and (b)
 * the deploy-runbook live check: `networkSeenTotal` must move on the first
 * real gated settle after deploy.
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { createServer } from 'node:http'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8796'
const FIXTURE_PORT = Number(process.env.FIXTURE_PORT ?? '8795')
const OP_TOKEN = process.env.CENSUS_OP_TOKEN ?? 'ci-submit-tok'

let failures = 0
const results = []
const pass = (label) => results.push(`PASS  ${label}`)
const fail = (label, why) => {
  failures++
  results.push(`FAIL  ${label}`)
  results.push(`      ${why}`)
}

// ── minimal raw-tx / BEEF builders (lane-366 conventions) ──────────────────
const varint = (n) => {
  if (n < 0xfd) return Buffer.from([n])
  const b = Buffer.alloc(3)
  b[0] = 0xfd
  b.writeUInt16LE(n, 1)
  return b
}
const u32 = (n) => {
  const b = Buffer.alloc(4)
  b.writeUInt32LE(n)
  return b
}
const u64 = (n) => {
  const b = Buffer.alloc(8)
  b.writeBigUInt64LE(BigInt(n))
  return b
}
const sha256d = (buf) =>
  createHash('sha256').update(createHash('sha256').update(buf).digest()).digest()

/** Zero-input funded "parent" — complete, EF-trivial (no sources to inline).
 *  A random nonce output keeps each run's txid distinct. */
function zeroInputParent() {
  const nonce = Buffer.alloc(8)
  randomFillSync(nonce)
  const nonceScript = Buffer.concat([Buffer.from([0x00, 0x6a]), Buffer.from([8]), nonce])
  return Buffer.concat([
    u32(1),
    varint(0),
    varint(2),
    u64(5000), Buffer.from([1, 0x51]),
    u64(0), varint(nonceScript.length), nonceScript,
    u32(0),
  ])
}
/** Display-order txid hex of a raw tx (reversed sha256d). */
const txidOf = (raw) => Buffer.from(sha256d(raw)).reverse().toString('hex')

/** Proofless V1 BEEF over N raw txs. */
function beefV1(...rawTxs) {
  return Buffer.concat([
    Buffer.from([0x01, 0x00, 0xbe, 0xef]),
    varint(0),
    varint(rawTxs.length),
    ...rawTxs.flatMap((raw) => [raw, Buffer.from([0x00])]),
  ])
}

// ── the fixture Arcade ─────────────────────────────────────────────────────
const allowlist = new Set()
const getLog = [] // every txid asked of GET /tx/:txid
const fixture = createServer((req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${FIXTURE_PORT}`)
  if (req.method === 'GET' && url.pathname.startsWith('/tx/')) {
    const txid = url.pathname.slice('/tx/'.length).toLowerCase()
    getLog.push(txid)
    if (allowlist.has(txid)) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ txid, txStatus: 'SEEN_ON_NETWORK' }))
    } else {
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: 'not found' }))
    }
    return
  }
  if (req.method === 'POST' && (url.pathname === '/tx' || url.pathname === '/txs')) {
    // Drain the body, answer SEEN. The route computes its own subject txid
    // from the beef; the response txid is deliberately absent (the gated
    // arm's classification reads txStatus).
    req.resume()
    req.on('end', () => {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ txStatus: 'SEEN_ON_NETWORK' }))
    })
    return
  }
  res.writeHead(404)
  res.end()
})

async function seenTotal() {
  const res = await fetch(`${BASE}/health/invariants`)
  if (!res.ok) throw new Error(`GET /health/invariants -> ${res.status}`)
  const j = await res.json()
  if (typeof j.networkSeenTotal !== 'number')
    throw new Error('networkSeenTotal missing from /health/invariants')
  return j.networkSeenTotal
}

async function postSubmit(body, { mode, token } = {}) {
  const headers = {
    'Content-Type': 'application/octet-stream',
    'x-topics': JSON.stringify(['tm_collected']),
  }
  if (mode) headers['x-submit-mode'] = mode
  if (token) headers['Authorization'] = `Bearer ${token}`
  const res = await fetch(`${BASE}/submit`, { method: 'POST', headers, body })
  return { status: res.status, text: await res.text() }
}

/** Poll until `predicate()` is true or ~timeoutMs elapsed. */
async function pollFor(predicate, timeoutMs = 15_000, stepMs = 500) {
  const t0 = Date.now()
  for (;;) {
    if (await predicate()) return true
    if (Date.now() - t0 > timeoutMs) return false
    await new Promise((r) => setTimeout(r, stepMs))
  }
}

await new Promise((resolve, reject) => {
  fixture.once('error', reject)
  fixture.listen(FIXTURE_PORT, '127.0.0.1', resolve)
})

try {
  const base0 = await seenTotal()
  if (base0 < 0) {
    fail('invariants serve networkSeenTotal', `got ${base0} (table unreadable)`)
  } else {
    pass(`networkSeenTotal served (${base0})`)
  }

  const afterGated = base0

  // ── 1. UNGATED admitted + fixture-known: backgrounded latch, +1. ──
  const knownParent = zeroInputParent()
  const knownTxid = txidOf(knownParent)
  allowlist.add(knownTxid)
  const u = await postSubmit(beefV1(knownParent), { mode: 'historical-tx-no-spv', token: OP_TOKEN })
  if (u.status !== 200) {
    fail('ungated submit accepted', `POST /submit(historical-tx-no-spv) -> ${u.status}: ${u.text.slice(0, 200)}`)
  }
  const latched = await pollFor(async () => (await seenTotal()) >= afterGated + 1)
  if (latched && getLog.includes(knownTxid)) {
    pass('UNGATED corroboration latches a fixture-known subject (+1, GET observed)')
  } else {
    fail(
      'UNGATED corroboration latches a fixture-known subject',
      `latched=${latched} fixtureGETs=${JSON.stringify(getLog.slice(-5))}`
    )
  }
  const afterKnown = await seenTotal()

  // ── 3. UNGATED unknown subject: corroborated, refused, count holds. ──
  const unknownParent = zeroInputParent()
  const unknownTxid = txidOf(unknownParent) // deliberately NOT allowlisted
  const u2 = await postSubmit(beefV1(unknownParent), { mode: 'historical-tx-no-spv', token: OP_TOKEN })
  if (u2.status !== 200) {
    fail('unknown-subject submit accepted', `-> ${u2.status}`)
  }
  const asked = await pollFor(async () => getLog.includes(unknownTxid))
  const afterUnknown = await seenTotal()
  if (asked && afterUnknown === afterKnown) {
    pass('UNGATED unknown subject: corroboration FIRED and refused (count unchanged)')
  } else {
    fail(
      'UNGATED unknown subject refused without a latch',
      `asked=${asked} count ${afterKnown} -> ${afterUnknown}`
    )
  }

  // ── 4. REFUSED body: no corroboration at all (MEDIUM-2). ──
  const getsBefore = getLog.length
  const junk = Buffer.from('deadbeef', 'hex')
  const r = await postSubmit(junk, { mode: 'historical-tx-no-spv', token: OP_TOKEN })
  if (r.status >= 200 && r.status < 300) {
    fail('junk body refused', `expected a 4xx/5xx, got ${r.status}`)
  }
  await new Promise((s) => setTimeout(s, 2_000))
  if (getLog.length === getsBefore) {
    pass('REFUSED body fires no Arcade corroboration (post-engine.submit only)')
  } else {
    fail('REFUSED body fires no Arcade corroboration', `fixture GETs grew: ${JSON.stringify(getLog.slice(getsBefore))}`)
  }
} finally {
  fixture.close()
}

console.log('')
for (const line of results) console.log(line)
console.log('')
if (failures > 0) {
  console.log(`✗ /submit network_seen route harness: ${failures} expectation(s) FAILED.`)
  process.exit(1)
}
console.log(`/submit network_seen route harness: all ${results.length} expectations held.`)
