#!/usr/bin/env node
/**
 * #366 route-level ASSERTING harness for the broadcast-gated readiness census.
 * Part of `make ci-route` (Rule 22: the census's wiring — route → classify →
 * durable counter → /health/invariants — is invisible to every native cell,
 * because `submit()` takes a `worker::Request` and only runs on wasm).
 *
 * Drives the REAL `/submit` route with real bodies and asserts:
 *
 *  1. counted, durably: each shape moves exactly its (mode, population,
 *     state) cell and its reason bucket on `/health/invariants`;
 *  2. the THREE states are distinct on the wire (Rule 13) — would-fail /
 *     gated-ready / could-not-evaluate each move a DIFFERENT field, and
 *     `observed` is always their sum;
 *  3. measurement never gates: a census-classified submit's admission
 *     outcome is exactly what #347 dictates (200 for a served operator
 *     submit, 400 for a body the ENGINE refuses — counted either way);
 *  4. EMPIRICAL agreement with the gated arm (the Rule 16 boundary, checked
 *     through the real producer rather than a second derivation): the same
 *     bytes POSTed with `broadcast-gated` get the route's own structural
 *     verdict — 400 where the census said would-fail(subject-ef), and
 *     NOT-400/429 where the census said gated-ready.
 *
 * Expects the ci-route strict worker on argv[2] (:8791) and, when
 * CENSUS_LENIENT_BASE is set, a LENIENT worker (SUBMIT_ENFORCE unset) for the
 * CLIENT-population leg — the exact population the #347 flip criterion reads.
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { readFileSync } from 'node:fs'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8791'
const LENIENT_BASE = process.env.CENSUS_LENIENT_BASE ?? ''
const OP_TOKEN = process.env.CENSUS_OP_TOKEN ?? 'ci-submit-tok'

let failures = 0
const results = []
const pass = (label) => results.push(`PASS  ${label}`)
const fail = (label, why) => {
  failures++
  results.push(`FAIL  ${label}`)
  results.push(`      ${why}`)
}

// ── minimal raw-tx / BEEF builders (same conventions as lane-347's script) ──
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
const sha256d = (buf) => createHash('sha256').update(createHash('sha256').update(buf).digest()).digest()

/** Zero-input funded "parent" (complete: nothing for the sort to miss). A
 *  random nonce output keeps each run's txid distinct (no dedupe). */
function zeroInputParent() {
  const nonce = Buffer.alloc(8)
  randomFillSync(nonce)
  const nonceScript = Buffer.concat([Buffer.from([0x00, 0x6a]), Buffer.from([8]), nonce])
  return Buffer.concat([
    u32(1),
    varint(0), // zero inputs
    varint(2),
    u64(5000), Buffer.from([1, 0x51]), // 5000 sats, OP_TRUE
    u64(0), varint(nonceScript.length), nonceScript,
    u32(0),
  ])
}

/** A child spending `parentRaw`'s vout 0 (unsigned — structure is what the
 *  census reads; the network leg is deliberately out of scope for ci). */
function childOf(parentRaw) {
  const prev = sha256d(parentRaw) // natural (LE) order, as raw txs carry it
  return Buffer.concat([
    u32(1),
    varint(1),
    prev, u32(0), varint(1), Buffer.from([0x00]), Buffer.from([0xff, 0xff, 0xff, 0xff]),
    varint(1),
    u64(4900), Buffer.from([1, 0x52]),
    u32(0),
  ])
}

/** A tx with one junk input whose source is ABSENT (the #351 client shape). */
function junkInputTx() {
  const prev = Buffer.alloc(32)
  randomFillSync(prev)
  return Buffer.concat([
    u32(1),
    varint(1),
    prev, u32(0), varint(0), Buffer.from([0xff, 0xff, 0xff, 0xff]),
    varint(1),
    u64(0), Buffer.from([2, 0x00, 0x6a]),
    u32(0),
  ])
}

/** Proofless V1 BEEF over N raw txs (BEEF_V1 = 0xEFBE0001 → LE 01 00 BE EF). */
function beefV1(...rawTxs) {
  return Buffer.concat([
    Buffer.from([0x01, 0x00, 0xbe, 0xef]),
    varint(0), // nBUMPs
    varint(rawTxs.length),
    ...rawTxs.flatMap((raw) => [raw, Buffer.from([0x00])]), // hasBump = false
  ])
}

// The committed all-proven fixture (real mainnet tx + real bump) — the
// mined-claim shape whose gated fate is a network corroboration the census
// must not perform: the THIRD state.
const MINED_CLAIM = Buffer.from(
  readFileSync(new URL('../../crates/overlay-cloudflare/tests/fixtures/ef/parent_a7d76588_beef.hex', import.meta.url), 'utf8').trim(),
  'hex',
)

// ── census read/diff helpers ──────────────────────────────────────────────
async function censusOf(base) {
  const res = await fetch(`${base}/health/invariants`)
  if (!res.ok) throw new Error(`GET ${base}/health/invariants -> ${res.status}`)
  const j = await res.json()
  if (!j.submitReadinessCensus) throw new Error('submitReadinessCensus missing from /health/invariants')
  return j.submitReadinessCensus
}

const cell = (c, mode, pop) =>
  c.byMode?.[mode]?.[pop] ?? { gatedReady: 0, wouldHaveFailed: 0, couldNotEvaluate: 0, observed: 0 }

async function postSubmit(base, body, { mode, token } = {}) {
  const headers = {
    'Content-Type': 'application/octet-stream',
    'x-topics': JSON.stringify(['tm_collected']),
  }
  if (mode) headers['x-submit-mode'] = mode
  if (token) headers['Authorization'] = `Bearer ${token}`
  const res = await fetch(`${base}/submit`, { method: 'POST', headers, body })
  return { status: res.status, text: await res.text() }
}

/** Poll until `check(censusDelta)` holds or ~20s elapse (the counter bump is
 *  a `wait_until` background write — post-response by design). */
async function awaitDelta(base, before, check) {
  let last
  for (let i = 0; i < 20; i++) {
    last = await censusOf(base)
    if (check(last, before)) return { ok: true, last }
    await new Promise((r) => setTimeout(r, 1000))
  }
  return { ok: false, last }
}

const d = (afterC, beforeC, mode, pop, field) => cell(afterC, mode, pop)[field] - cell(beforeC, mode, pop)[field]
const dr = (afterC, beforeC, key) =>
  (afterC.wouldFailAndUnevalReasons?.[key] ?? 0) - (beforeC.wouldFailAndUnevalReasons?.[key] ?? 0)

/** One census expectation: POST a body, await the exact cell+reason delta,
 *  and verify the observed invariant. */
async function expectCensus({ base, label, body, mode, token, wantStatus, cellMode, pop, state, reason }) {
  const before = await censusOf(base)
  const res = await postSubmit(base, body, { mode, token })
  if (!wantStatus(res.status)) {
    fail(label, `unexpected /submit status ${res.status}: ${res.text.slice(0, 200)}`)
    return
  }
  const got = await awaitDelta(base, before, (a, b) => d(a, b, cellMode, pop, state) >= 1)
  if (!got.ok) {
    fail(label, `census cell ${cellMode}.${pop}.${state} never moved (status was ${res.status})`)
    return
  }
  const after = got.last
  // The OTHER two states of this cell must not have absorbed the event
  // (Rule 13: three distinct states, never collapsed).
  const states = ['gatedReady', 'wouldHaveFailed', 'couldNotEvaluate']
  for (const s of states.filter((s) => s !== state)) {
    if (d(after, before, cellMode, pop, s) > 0) {
      fail(label, `state ${s} ALSO moved — the states are not distinct`)
      return
    }
  }
  if (reason && dr(after, before, reason) < 1) {
    fail(label, `reason bucket ${reason} did not move`)
    return
  }
  const c = cell(after, cellMode, pop)
  if (c.observed !== c.gatedReady + c.wouldHaveFailed + c.couldNotEvaluate) {
    fail(label, `observed (${c.observed}) != sum of states`)
    return
  }
  pass(label)
}

// ── the run ───────────────────────────────────────────────────────────────
const parentRaw = zeroInputParent()
const ancestryBody = beefV1(parentRaw, childOf(parentRaw))
const noAncestryBody = beefV1(junkInputTx())
const garbage = Buffer.from([0xde, 0xad, 0xbe, 0xef])

// 1. OPERATOR population on the strict worker — the three states.
await expectCensus({
  base: BASE,
  label: 'operator no-ancestry → would-fail(subject-ef), served 200',
  body: noAncestryBody,
  mode: 'historical-tx-no-spv',
  token: OP_TOKEN,
  wantStatus: (s) => s === 200, // measurement never gates (#347 outcome intact)
  cellMode: 'historical-tx-no-spv',
  pop: 'operator',
  state: 'wouldHaveFailed',
  reason: 'subjectEf',
})
await expectCensus({
  base: BASE,
  label: 'operator ancestry-carrying → gated-ready, served 200',
  body: ancestryBody,
  mode: 'historical-tx-no-spv',
  token: OP_TOKEN,
  wantStatus: (s) => s === 200,
  cellMode: 'historical-tx-no-spv',
  pop: 'operator',
  state: 'gatedReady',
})
await expectCensus({
  base: BASE,
  label: 'operator mined-claim → could-not-evaluate (the third state, on the wire)',
  body: MINED_CLAIM,
  mode: 'historical-tx-no-spv',
  token: OP_TOKEN,
  wantStatus: (s) => s === 200,
  cellMode: 'historical-tx-no-spv',
  pop: 'operator',
  state: 'couldNotEvaluate',
  reason: 'minedClaimUnverified',
})
// 2. Counting is independent of the ADMISSION outcome: a body the engine
//    itself refuses (400) is still an ARRIVAL and still counted.
await expectCensus({
  base: BASE,
  label: 'operator garbage → would-fail(parse), counted despite engine 400',
  body: garbage,
  mode: 'historical-tx-no-spv',
  token: OP_TOKEN,
  wantStatus: (s) => s === 400,
  cellMode: 'historical-tx-no-spv',
  pop: 'operator',
  state: 'wouldHaveFailed',
  reason: 'parse',
})

// 3. EMPIRICAL agreement with the gated arm (Rule 16 through the real route,
//    not a second derivation): the same bytes on `broadcast-gated`.
{
  const res = await postSubmit(BASE, noAncestryBody, { mode: 'broadcast-gated' })
  if (res.status === 400 && /broadcast-gated/.test(res.text)) {
    pass('gated arm agrees: census would-fail body → structural 400 before any broadcast')
  } else {
    fail(
      'gated arm agrees: census would-fail body → structural 400 before any broadcast',
      `status=${res.status} body=${res.text.slice(0, 200)}`,
    )
  }
}
{
  const res = await postSubmit(BASE, ancestryBody, { mode: 'broadcast-gated' })
  // Past the structural checks the verdict belongs to the NETWORK: 422
  // online (fabrication rejected), 502 offline. Never the structural 400/429,
  // never a refusal by US (401), and never an admit of a fabrication (200).
  if (![400, 429, 401, 200].includes(res.status)) {
    pass('gated arm agrees: census gated-ready body gets PAST the structural checks')
  } else {
    fail(
      'gated arm agrees: census gated-ready body gets PAST the structural checks',
      `status=${res.status} body=${res.text.slice(0, 200)}`,
    )
  }
}

// 4. CLIENT population (the #347 flip criterion's population) — needs the
//    lenient worker, where an unauthenticated ungated submit is SERVED.
if (LENIENT_BASE) {
  await expectCensus({
    base: LENIENT_BASE,
    label: 'client (lenient, unauth) no-ancestry → client.would-fail — the flip-criterion number',
    body: noAncestryBody,
    mode: 'historical-tx-no-spv',
    wantStatus: (s) => s === 200,
    cellMode: 'historical-tx-no-spv',
    pop: 'client',
    state: 'wouldHaveFailed',
    reason: 'subjectEf',
  })
  await expectCensus({
    base: LENIENT_BASE,
    label: 'client (lenient, unauth) ancestry-carrying → client.gated-ready',
    body: ancestryBody,
    mode: 'historical-tx-no-spv',
    wantStatus: (s) => s === 200,
    cellMode: 'historical-tx-no-spv',
    pop: 'client',
    state: 'gatedReady',
  })
} else {
  results.push('SKIP  client-population legs (set CENSUS_LENIENT_BASE)')
}

console.log(results.join('\n'))
if (failures) {
  console.error(`\n#366 census route harness: ${failures} expectation(s) FAILED.`)
  process.exit(1)
}
console.log(`\n#366 census route harness: all ${results.length} expectations held.`)
