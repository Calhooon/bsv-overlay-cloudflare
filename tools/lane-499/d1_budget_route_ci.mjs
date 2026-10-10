#!/usr/bin/env node
/**
 * bsv-low #499: the D1 BUDGET tier. Part of `make ci-route` (through `make ci-d1-budget`).
 *
 * Every hot route answers what it cost D1 (`Server-Timing: d1;desc="reads=N writes=M stmts=K"`, the sum of
 * every statement's `meta.rows_read` / `rows_written` under the request's ledger, `d1_ledger.rs`). This harness
 * drives each scenario once against workers on the fixture D1 (real SQLite under `wrangler dev --local`; the app
 * layer's rows are `crates/low-app-layer/examples/d1_budget_seed.rs`) and REDS BY NAME when a figure passes its
 * ceiling (`ceilings.json`). A planted N+1 (a loop that reads per row) passes the statement ceiling of its route
 * at once and the rows ceiling as soon as the per-row read scans. The doctrine is the courier census's: count a
 * D1 read or it did not happen.
 *
 *   node tools/lane-499/d1_budget_route_ci.mjs <app-layer base> <overlay base>
 *   D1_BUDGET_MEASURE=1 ...   prints the measured table and enforces nothing (how a ceiling is set: from the
 *                             measured figure plus the margin `ceilings.json` states, never from a guess)
 *
 * Legs:
 *  1. app layer: /utxo-status and /pots-view over identity A's 8 pots, /results (the view actor's compute, served
 *     as `d1view`, and the forwarding request itself), /owed's first read (the compute on read) and a served read.
 *  2. overlay: an operator `historical-tx-no-spv` /submit that ADMITS one tm_collected output (the admission
 *     writes); the overlay's ARCADE_URL points at a closed local port, so nothing leaves the machine.
 *  3. the health surfaces: `/health.d1Budget` (app) and `/health/invariants.d1Budget` (overlay) carry each route
 *     driven, with a running maximum at least the figure its answer carried.
 *  4. the brain's recompute ceiling: identity B's pot is announced 13 times inside one minute through
 *     `/internal/pot-changed` (each waited out, so none is folded into an in-flight one). The announcement names
 *     BOTH seats (B and B's opponent), so each ask is two recomputes, one per identity, and each identity's 13th
 *     is shed: `recomputeBySource.pot-changed` moves by 24 and `/health.owed.recomputeShed` by exactly 2. Each
 *     hook answer carries its `d1` segment, the hooks are keyed `/internal` and every walk after the answer
 *     `owed-recompute` on `d1Budget` (the lens fold's M2 and M3).
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { readFileSync } from 'node:fs'

const APP = process.argv[2] ?? 'http://127.0.0.1:8800'
const OVERLAY = process.argv[3] ?? 'http://127.0.0.1:8801'
const OP_TOKEN = process.env.D1_BUDGET_OP_TOKEN ?? 'ci-submit-tok'
const INTERNAL_TOKEN = process.env.D1_BUDGET_INTERNAL_TOKEN ?? 'ci-internal-tok'
const MEASURE = process.env.D1_BUDGET_MEASURE === '1'
const CEILINGS = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url), 'utf8'))

const ID_A = '02' + 'a1'.repeat(32)
const ID_B = '02' + 'b2'.repeat(32)
const potTxid = (owner, i) => owner + owner + i.toString(16).padStart(62, '0')
const A_OUTPOINTS = Array.from({ length: 8 }, (_, i) => `${potTxid('a', i)}.0`).join(',')

let failures = 0
const lines = []
const table = []
const pass = (label) => lines.push(`PASS  ${label}`)
const fail = (label, why) => {
  failures++
  lines.push(`FAIL  ${label}`)
  lines.push(`      ${why}`)
}

/** The figures of segment `name` in a Server-Timing value (the shape `d1_ledger::named_segment` writes). */
function segment(header, name) {
  for (const seg of (header ?? '').split(',')) {
    const m = seg.trim().match(new RegExp(`^${name};desc="reads=(\\d+) writes=(\\d+) stmts=(\\d+)"$`))
    if (m) return { reads: Number(m[1]), writes: Number(m[2]), stmts: Number(m[3]) }
  }
  return null
}

async function get(base, path, init) {
  const res = await fetch(base + path, init)
  const text = await res.text()
  return { status: res.status, timing: res.headers.get('server-timing'), text }
}

/** Hold one measured figure against its ceiling, by the scenario's name. */
function check(name, got) {
  const c = CEILINGS.scenarios[name]
  if (!got) {
    fail(`${name}: the answer carries its D1 figures`, 'no d1 segment on Server-Timing')
    return
  }
  table.push({ name, got, ceiling: c?.ceiling })
  if (MEASURE) return
  if (!c) {
    fail(`${name}: has a ceiling`, `no entry in ceilings.json (measured ${JSON.stringify(got)})`)
    return
  }
  const over = ['reads', 'writes', 'stmts'].filter((k) => got[k] > c.ceiling[k])
  if (over.length === 0) pass(`${name}: reads ${got.reads}/${c.ceiling.reads}, writes ${got.writes}/${c.ceiling.writes}, stmts ${got.stmts}/${c.ceiling.stmts}`)
  else fail(`${name}: within its D1 ceiling`, over.map((k) => `${k} ${got[k]} > ${c.ceiling[k]}`).join(', ') + ` (${c.route})`)
}

// ── minimal raw-tx / BEEF builders (the lane-371 conventions) ──────────────
const varint = (n) => (n < 0xfd ? Buffer.from([n]) : Buffer.from([0xfd, n & 0xff, n >> 8]))
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
const sha256d = (buf) => createHash('sha256').update(createHash('sha256').update(buf).digest()).digest()
// The tm_collected golden marker (`overlay-discovery` collected::GOLDEN_MARKER_HEX): byte-format admitted.
const COLLECTED_MARKER = Buffer.from(
  '006a104c4f572f636f6c6c65637465642f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab',
  'hex',
)
/** A transaction spending a coinbase's null outpoint (a transaction with no input is refused at the door since
 *  bsv-rs 0.4.1, NL-8 W6), with one collected marker (admitted) and a nonce output (a fresh txid per run). */
function collectedTx() {
  const nonce = Buffer.alloc(8)
  randomFillSync(nonce)
  const nonceScript = Buffer.concat([Buffer.from([0x00, 0x6a, 8]), nonce])
  return Buffer.concat([
    u32(1),
    varint(1), Buffer.alloc(32), u32(0xffffffff), varint(0), u32(0xffffffff),
    varint(2),
    u64(0), varint(COLLECTED_MARKER.length), COLLECTED_MARKER,
    u64(0), varint(nonceScript.length), nonceScript,
    u32(0),
  ])
}
const beefV1 = (raw) => Buffer.concat([Buffer.from([0x01, 0x00, 0xbe, 0xef]), varint(0), varint(1), raw, Buffer.from([0x00])])
const txidOf = (raw) => Buffer.from(sha256d(raw)).reverse().toString('hex')

async function appHealth() {
  const r = await get(APP, '/health')
  if (r.status !== 200) throw new Error(`GET /health -> ${r.status}`)
  return JSON.parse(r.text)
}
async function pollFor(predicate, timeoutMs = 20_000, stepMs = 100) {
  const t0 = Date.now()
  for (;;) {
    if (await predicate()) return true
    if (Date.now() - t0 > timeoutMs) return false
    await new Promise((r) => setTimeout(r, stepMs))
  }
}

const measured = {}

// ── 1. the app layer's hot routes ──────────────────────────────────────────
{
  const u = await get(APP, `/utxo-status?outpoints=${A_OUTPOINTS}`)
  if (u.status !== 200) fail('/utxo-status answers', `${u.status}: ${u.text.slice(0, 200)}`)
  check('utxo-status-8', (measured['/utxo-status'] = segment(u.timing, 'd1')))

  const p = await get(APP, `/pots-view?outpoints=${A_OUTPOINTS}`)
  if (p.status !== 200) fail('/pots-view answers', `${p.status}: ${p.text.slice(0, 200)}`)
  check('pots-view-8', (measured['/pots-view'] = segment(p.timing, 'd1')))

  const r = await get(APP, `/results?identity=${ID_A}`)
  if (r.status !== 200) fail('/results answers', `${r.status}: ${r.text.slice(0, 200)}`)
  else if (JSON.parse(r.text).results?.length !== 8) fail('/results serves A\'s 8 pots', r.text.slice(0, 200))
  check('results-view-compute', (measured['/results:view'] = segment(r.timing, 'd1view')))
  check('results-forward', (measured['/results'] = segment(r.timing, 'd1')))

  const o1 = await get(APP, `/owed?identity=${ID_A}`)
  if (o1.status !== 200) fail('/owed first read answers', `${o1.status}: ${o1.text.slice(0, 200)}`)
  else if (JSON.parse(o1.text).rows?.length !== 8) fail('/owed computes A\'s 8 rows', o1.text.slice(0, 200))
  const first = segment(o1.timing, 'd1')
  check('owed-first-read', first)
  const o2 = await get(APP, `/owed?identity=${ID_A}`)
  if (o2.status !== 200) fail('/owed served read answers', `${o2.status}: ${o2.text.slice(0, 200)}`)
  const served = segment(o2.timing, 'd1')
  check('owed-served-read', served)
  measured['/owed'] = first && served ? { reads: Math.max(first.reads, served.reads), writes: Math.max(first.writes, served.writes), stmts: Math.max(first.stmts, served.stmts) } : null
}

// ── 2. the overlay's admission writes ──────────────────────────────────────
{
  const raw = collectedTx()
  const res = await get(OVERLAY, '/submit', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'x-topics': JSON.stringify(['tm_collected']),
      'x-submit-mode': 'historical-tx-no-spv',
      Authorization: `Bearer ${OP_TOKEN}`,
    },
    body: beefV1(raw),
  })
  let admitted = false
  try {
    admitted = (JSON.parse(res.text)?.tm_collected?.outputsToAdmit ?? []).includes(0)
  } catch {}
  if (res.status !== 200 || !admitted) fail('the operator submit admits the collected marker', `${res.status}: ${res.text.slice(0, 300)} (txid ${txidOf(raw)})`)
  else pass(`the operator submit admits the collected marker (${txidOf(raw).slice(0, 12)}…)`)
  check('submit-admit-1', (measured['/submit'] = segment(res.timing, 'd1')))
}

// ── 3. the health surfaces carry the running maxima ────────────────────────
{
  const app = (await appHealth()).d1Budget
  const ov = JSON.parse((await get(OVERLAY, '/health/invariants')).text).d1Budget
  const surfaces = { '/utxo-status': app, '/pots-view': app, '/results': app, '/results:view': app, '/owed': app, '/submit': ov }
  for (const [route, budget] of Object.entries(surfaces)) {
    const m = measured[route]
    const b = budget?.routes?.[route]
    if (!m) continue
    if (!b) fail(`d1Budget carries ${route}`, `routes: ${Object.keys(budget?.routes ?? {}).join(' ')}`)
    else if (b.max.reads < m.reads || b.max.writes < m.writes || b.max.stmts < m.stmts) fail(`d1Budget's ${route} maximum covers its answer`, `${JSON.stringify(b.max)} < ${JSON.stringify(m)}`)
    else pass(`d1Budget carries ${route} (max reads ${b.max.reads}, writes ${b.max.writes}, stmts ${b.max.stmts}; ${b.requests} request(s))`)
  }
}

// ── 4. the brain's recompute ceiling sheds ─────────────────────────────────
{
  const h0 = (await appHealth()).owed
  const ceiling = h0.recomputesPerIdentityPerMinute
  if (ceiling !== 12) fail('/health serves the recompute ceiling', `recomputesPerIdentityPerMinute = ${ceiling}`)
  const t0 = Date.now()
  const asks = (ceiling ?? 12) + 1
  const SEATS = 2 // the announcement names both seats of B's pot: one recompute per identity per ask
  const b0 = (await appHealth()).d1Budget
  let hookUnstamped = 0 // the lens fold's M2: a hook's own answer carries its D1 figures too
  for (let i = 0; i < asks; i++) {
    const r = await get(APP, '/internal/pot-changed', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${INTERNAL_TOKEN}` },
      body: JSON.stringify({ outpoints: [{ txid: potTxid('b', 0), vout: 0 }] }),
    })
    if (r.status !== 200) {
      fail(`pot-changed ask ${i + 1} answers`, `${r.status}: ${r.text.slice(0, 200)}`)
      break
    }
    if (!segment(r.timing, 'd1')) hookUnstamped++
    // the recompute runs after the answer: wait it out, so the next ask is a recompute and not a folded one
    const settled = await pollFor(async () => {
      const o = (await appHealth()).owed
      return o.inFlight.count === 0 && (o.recomputeBySource['pot-changed'] + o.recomputeShed) >= (h0.recomputeBySource['pot-changed'] + h0.recomputeShed) + SEATS * (i + 1)
    })
    if (!settled) {
      fail(`pot-changed ask ${i + 1} settles`, 'the recompute neither ran nor shed inside 20 s')
      break
    }
  }
  const h1 = (await appHealth()).owed
  const elapsed = Date.now() - t0
  const ran = h1.recomputeBySource['pot-changed'] - h0.recomputeBySource['pot-changed']
  const shed = h1.recomputeShed - h0.recomputeShed
  if (elapsed >= 60_000) fail('the 13 asks fall inside one minute', `${elapsed} ms`)
  else if (ran === SEATS * ceiling && shed === SEATS && h1.recomputeFaults === h0.recomputeFaults && h1.recomputeAtCeiling >= SEATS)
    pass(`the ceiling sheds: ${asks} announcements of B's pot in ${elapsed} ms, ${ran} recomputes ran (${ceiling} per seat), each seat's ${asks}th shed (recomputeShed +${shed}, recomputeAtCeiling ${h1.recomputeAtCeiling})`)
  else fail('the ceiling sheds the 13th recompute of each identity inside a minute', `ran ${ran} (want ${SEATS * ceiling}), shed ${shed} (want ${SEATS}), atCeiling ${h1.recomputeAtCeiling}, faults +${h1.recomputeFaults - h0.recomputeFaults}`)
  // the lens fold's M2 and M3: the hooks run under the ledger (`/internal`), and each walk after the answer is
  // keyed `owed-recompute` (before, both landed only in `unscoped`)
  if (hookUnstamped === 0) pass(`every /internal/pot-changed answer carries d1;desc= (${asks} of ${asks})`)
  else fail('a hook route answers with its D1 figures', `${hookUnstamped} of ${asks} answers carry no d1 segment`)
  const b1 = (await appHealth()).d1Budget
  const moved = (route) => (b1?.routes?.[route]?.requests ?? 0) - (b0?.routes?.[route]?.requests ?? 0)
  if (moved('/internal') >= asks) pass(`d1Budget keys the hooks under /internal (+${moved('/internal')} requests)`)
  else fail('d1Budget keys the hooks under /internal', `+${moved('/internal')} requests (want at least ${asks})`)
  if (moved('owed-recompute') >= ran && ran > 0) pass(`d1Budget keys each detached walk under owed-recompute (+${moved('owed-recompute')}, max reads ${b1.routes['owed-recompute'].max.reads})`)
  else fail('d1Budget keys each detached walk under owed-recompute', `+${moved('owed-recompute')} (want at least ${ran})`)
}

console.log('\n── bsv-low #499 D1 budget tier ──')
console.log('scenario                 reads (ceiling)   writes (ceiling)  stmts (ceiling)')
for (const { name, got, ceiling } of table) {
  const col = (k) => `${String(got[k]).padStart(5)} (${ceiling ? String(ceiling[k]).padStart(5) : '    -'})`
  console.log(`${name.padEnd(24)} ${col('reads')}     ${col('writes')}      ${col('stmts')}`)
}
console.log('')
for (const l of lines) console.log(l)
if (MEASURE) {
  console.log('\nD1_BUDGET_MEASURE=1: measured only, no ceiling enforced')
  process.exit(failures === 0 ? 0 : 1)
}
console.log(failures === 0 ? '\n✅ D1 budget tier green' : `\n✗ D1 budget tier: ${failures} failure(s)`)
process.exit(failures === 0 ? 0 : 1)
