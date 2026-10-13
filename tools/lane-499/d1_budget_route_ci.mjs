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
 *     `owed-recompute` on `d1Budget` (the lens fold's M2 and M3). E649 L4: the first ask's own figures are held to
 *     `pot-changed-hook-1` (one attribution batch, one seat-marker batch, no walk) and it answers `hook;dur=`.
 *  5. E649 L4 M1: the hook's maximum bodies (eight decoded outpoints; the marker fallback; the 8-outpoint /
 *     19-identity body with existing owed-state rows) and one pass of the park's drain, each held to its ceiling.
 *
 * Exit 0 = every expectation held.
 */
import { assertBuildInfo } from '../assert-build-info.mjs'
import { createHash, randomFillSync } from 'node:crypto'
import { provedBeefV1, blockCoinbaseInput } from '../fixtures/proven_beef.mjs'
import { readFileSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

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
    const m = seg.trim().match(new RegExp(`^${name};desc="reads=(\\d+) writes=(\\d+) stmts=(\\d+)(?: maxReads=(\\d+))?"$`))
    if (m) return { reads: Number(m[1]), writes: Number(m[2]), stmts: Number(m[3]), maxReads: m[4] === undefined ? null : Number(m[4]) }
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
  if (got.maxReads === null) fail(`${name}: per-statement rows-read figure`, 'maxReads absent')
  else if (got.maxReads > (c.ceiling.maxReads ?? c.ceiling.reads)) fail(`${name}: per-statement rows-read ceiling`, `largest statement ${got.maxReads} > ${c.ceiling.maxReads ?? c.ceiling.reads}`)
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
/** A synthetic coinbase with one collected marker and a nonce output, proved by beefV1. */
function collectedTx() {
  const nonce = Buffer.alloc(8)
  randomFillSync(nonce)
  const nonceScript = Buffer.concat([Buffer.from([0x00, 0x6a, 8]), nonce])
  return Buffer.concat([
    u32(1),
    varint(1), blockCoinbaseInput(),
    varint(2),
    u64(0), varint(COLLECTED_MARKER.length), COLLECTED_MARKER,
    u64(0), varint(nonceScript.length), nonceScript,
    u32(0),
  ])
}
// The marker is a coinbase in a one-transaction synthetic block.
const beefV1 = (raw) => provedBeefV1([raw])
const txidOf = (raw) => Buffer.from(sha256d(raw)).reverse().toString('hex')

async function appHealth() {
  const r = await get(APP, '/health')
  if (r.status !== 200) throw new Error(`GET /health -> ${r.status}`)
  const body = JSON.parse(r.text)
  assertBuildInfo(body)
  return body
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

// L23R2-H1/M2/M3/M4: the real /owed route over captured, markerless, twin-only 25h evidence.
{
  const p28 = '02f9f80a92a92677d97fce5bb0a03d0321425b918868b3ef5773120d9cf5364e49'
  const r = await get(APP, `/owed?identity=${p28}`)
  const body = JSON.parse(r.text)
  const row = body.rows?.find(r => r.gameId === '9d88aaaa348acae127efd08e133822201ecae56513831c37124837f3ef9ae17f')
  if (r.status === 200 && row?.facts?.spendKind === 'spent-elsewhere' && row.facts.claim == null && row.facts.markerlessCandidates === 1)
    pass('L23R2 route-selection: captured ninth eviction, twin-only seat, planted marker, 25h story')
  else fail('L23R2 route-selection: captured ninth eviction, twin-only seat, planted marker, 25h story', r.text.slice(0, 2000))
  const tally = segment(r.timing, 'd1')
  if (tally && tally.reads <= 256) pass('L23R2-H1 captured read stays in the 256-read #499 class under twin noise')
  else fail('L23R2-H1 captured read stays in the 256-read #499 class under twin noise', JSON.stringify(tally))
  const h = await appHealth()
  if (h.owed.markerlessIgnoredMarkers >= 1) pass('L23R2-M2 planted marker counted')
  else fail('L23R2-M2 planted marker counted', JSON.stringify(h.owed))
}

// M3 independently: the other captured identity has only permanent twin party rows, no hopparty row.
{
  const other = '030d9e9fbe045a8130f190b103609c8a6bf13b1887d5c4f15e2f918d68aca8ad67'
  const r = await get(APP, `/owed?identity=${other}`)
  const row = JSON.parse(r.text).rows?.find(r => r.outpoint === '9b238eed0177c714b1f9a608243e8f1bf79e35e9597110f83cefa1b658785c62:0')
  if (r.status === 200 && row?.facts?.markerlessCandidates === 1) pass('L23R2-M3 twin-only captured seat computes through owed route')
  else fail('L23R2-M3 twin-only captured seat computes through owed route', `${r.status} ${r.text}`)
}

// L23R2D-M1: absent permanent bytes fault this candidate; the first read serves its row with 200.
{
  const before = (await appHealth()).owed.markerlessReadFaults
  const r = await get(APP, `/owed?identity=02${'f1'.repeat(32)}`)
  const after = (await appHealth()).owed.markerlessReadFaults
  const body = JSON.parse(r.text)
  const fault = body.rows?.find(row => row.facts?.evidenceMissing)
  if (r.status === 200 && fault?.reason?.includes('evidence-missing') && fault.facts.claim == null && body.rows.length === 2 && body.rows.some(row => row.gameId === 'healthy-game') && body.truncated && after > before)
    pass('L23R2-L2 markerless read fault counted, could not judge served')
  else fail('L23R2-L2 markerless read fault counted, could not judge served', `${r.status} ${r.text} faults ${before}->${after}`)
}

// The detached fault must also mark a previously served snapshot; its old press is withheld.
{
  const url = `/owed?identity=02${'f2'.repeat(32)}`
  let r = await get(APP, url)
  for (let i = 0; i < 30 && !JSON.parse(r.text).rows?.[0]?.facts?.markerlessReadFault; i++) {
    await new Promise(resolve => setTimeout(resolve, 100))
    r = await get(APP, url)
  }
  const body = JSON.parse(r.text)
  const row = body.rows?.[0]
  if (r.status === 200 && body.truncated && body.judgement?.includes('could not judge') && row?.facts?.markerlessReadFault && row.facts.claim == null)
    pass('L23R2-L2 cached snapshot tells could-not-judge and withholds its press')
  else fail('L23R2-L2 cached snapshot tells could-not-judge and withholds its press', `${r.status} ${r.text}`)
}

// A real signed stranger filing: all request statements join /record's measured ceiling.
// M1 removed the three party enumeration reads; the remaining filing work is still counted.
{
  const seed = readFileSync(process.env.D1_BUDGET_SEED, 'utf8')
  const fixture = JSON.parse(seed.split('\n').find((line) => line.startsWith('-- NS2_RECORD_FIXTURE ')).slice('-- NS2_RECORD_FIXTURE '.length))
  const before = (await appHealth()).owed.recomputeBySource.filing
  const filed = await get(APP, `/record?kind=potparty&identity=${fixture.identity}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ scriptHex: fixture.scriptHex }),
  })
  if (filed.status === 200 && JSON.parse(filed.text).filed) pass('NS2 stranger potparty filing admitted')
  else fail('NS2 stranger potparty filing admitted', `${filed.status}: ${filed.text}`)
  check('record-potparty-filing', (measured['/record'] = segment(filed.timing, 'd1')))
  const ran = await pollFor(async () => (await appHealth()).owed.recomputeBySource.filing > before)
  const after = (await appHealth()).owed.recomputeBySource.filing
  if (ran && after === before + 1) pass('SHED-M1 filing walks and pushes only its filer; no victim walk or push')
  else fail('SHED-M1 filing walks only its filer', `filing walks +${after - before} (want 1)`)
  const staleBefore = (await appHealth()).owed.recomputeBySource['read-stale']
  await get(APP, `/owed?identity=${ID_A}`)
  const repaired = await pollFor(async () => (await appHealth()).owed.recomputeBySource['read-stale'] > staleBefore)
  const staleAfter = (await appHealth()).owed.recomputeBySource['read-stale']
  if (repaired && staleAfter === staleBefore + 1) pass('SHED-M1 victim own next read consumes its stale mark with one walk')
  else fail('SHED-M1 victim own next read recomputes once', `read-stale walks +${staleAfter - staleBefore}`)
}

// NOTE-3: twelve real failed writes leave no snapshot. The next read spends the reserved walk once.
{
  const identity = '02' + (299).toString(16).padStart(64, '0')
  const crate = fileURLToPath(new URL('../../crates/low-app-layer/', import.meta.url))
  const sql = (command) => execFileSync('npx', ['wrangler', 'd1', 'execute', 'low-overlay-db', '--local', '--persist-to', process.env.D1_BUDGET_STATE, '--command', command], { cwd: crate, timeout: 30_000, stdio: 'pipe' })
  sql(`CREATE TRIGGER ns2_first_read_fault BEFORE INSERT ON owed_rows WHEN NEW.identity='${identity}' BEGIN SELECT RAISE(FAIL,'NS2 pinned write fault'); END;`)
  try {
    let faulted = 0
    for (let n = 0; n < 12; n++) {
      const r = await get(APP, `/owed?identity=${identity}`)
      if (r.status === 503 && !JSON.parse(r.text).refresh) faulted++
    }
    if (faulted === 12) pass('NOTE-3 twelve ordinary first-read writes faulted with no snapshot')
    else fail('NOTE-3 twelve first reads fault', `${faulted}/12 real write faults`)
  } finally { sql('DROP TRIGGER ns2_first_read_fault;') }
  const before = (await appHealth()).owed
  const read = await get(APP, `/owed?identity=${identity}`)
  const after = (await appHealth()).owed
  if (read.status === 200 && JSON.parse(read.text).rows?.length === 1 && after.owed_recompute_deferred_total === before.owed_recompute_deferred_total + 1)
    pass('NOTE-3 snapshotless read past the ceiling runs one reserved walk and answers 200')
  else fail('NOTE-3 snapshotless first read walks instead of shedding 503', `${read.status} ${read.text} deferred ${before.owed_recompute_deferred_total}->${after.owed_recompute_deferred_total}`)
  const second = await get(APP, `/owed?identity=${identity}`)
  const served = (await appHealth()).owed
  if (second.status === 200 && JSON.parse(second.text).rows?.length === 1 && served.owed_recompute_deferred_total === after.owed_recompute_deferred_total && served.recomputeBySource['read-first'] === after.recomputeBySource['read-first'])
    pass('NOTE-3 a second read serves the snapshot without a second reserved walk')
  else fail('NOTE-3 one reserved walk per window', JSON.stringify(served))
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

// #638: measure each additional hot read on the same fleet fixture, with no invented request bars.
for (const path of ['/refund-view', '/recovery-view', '/refund-backups']) {
  const r = await get(APP, `${path}?identity=${ID_A}`)
  if (r.status !== 200) fail(`${path} answers`, `${r.status}: ${r.text.slice(0, 200)}`)
  const got = segment(r.timing, 'd1')
  if (!got) fail(`${path} carries D1 figures`, 'no d1 segment')
  else table.push({ name: path, got, ceiling: null })
}

// ── 3. the health surfaces carry the running maxima ────────────────────────
{
  const app = (await appHealth()).d1Budget
  const overlayHealth = JSON.parse((await get(OVERLAY, '/health/invariants')).text)
  assertBuildInfo(overlayHealth)
  for (const route of ['/health', '/health/live', '/health/ready']) {
    const response = await get(OVERLAY, route)
    if (response.status !== 200) throw new Error(`${route} -> ${response.status}`)
    assertBuildInfo(JSON.parse(response.text))
  }
  const ov = overlayHealth.d1Budget
  const surfaces = { '/utxo-status': app, '/pots-view': app, '/results': app, '/results:view': app, '/owed': app, '/submit': ov }
  for (const [route, budget] of Object.entries(surfaces)) {
    const m = measured[route]
    const b = budget?.routes?.[route]
    if (!m) continue
    if (!b) fail(`d1Budget carries ${route}`, `routes: ${Object.keys(budget?.routes ?? {}).join(' ')}`)
    else if (b.max.reads < m.reads || b.max.writes < m.writes || b.max.stmts < m.stmts) fail(`d1Budget's ${route} maximum covers its answer`, `${JSON.stringify(b.max)} < ${JSON.stringify(m)}`)
    else pass(`d1Budget carries ${route} (max reads ${b.max.reads}, writes ${b.max.writes}, stmts ${b.max.stmts}; ${b.requests} request(s))`)
    if (route === '/owed' && b?.max.stmts > CEILINGS.scenarios['owed-first-read'].ceiling.stmts)
      fail('owed surface maximum within named ceiling', `${b.max.stmts} > ${CEILINGS.scenarios['owed-first-read'].ceiling.stmts}`)
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
    // E649 L4: the hook is a write. Its first answer is held to a named ceiling (the attribution batch and the seat
    // markers' batch, no statement per outpoint, no walk) and carries its own timing (`hook;dur=`).
    if (i === 0) {
      check('pot-changed-hook-1', segment(r.timing, 'd1'))
      const dur = (r.timing ?? '').match(/(?:^|,\s*)hook;dur=(\d+)/)
      if (!dur) fail('E649 L4: the hook answers Server-Timing hook;dur=', `Server-Timing: ${r.timing}`)
      else if (Number(dur[1]) >= 1000) fail('E649 L4: the hook answers in under a second', `hook;dur=${dur[1]}`)
      else pass(`E649 L4: the hook answers hook;dur=${dur[1]} (under 1000 ms), announce detached`)
      const body = JSON.parse(r.text)
      if (body.announce === 'detached') pass('E649 L4: the answer says the announce is detached')
      else fail('E649 L4: the answer says the announce is detached', r.text.slice(0, 200))
    }
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
  const maximum = (await appHealth()).d1Budget?.routes?.['owed-recompute']?.max
  const owedCeiling = CEILINGS.scenarios['owed-first-read'].ceiling.stmts
  if (maximum) {
    console.log(`OWED MAXIMUM reads ${maximum.reads} writes ${maximum.writes} stmts ${maximum.stmts}; ceiling ${owedCeiling}`)
    if (maximum.stmts > owedCeiling) fail('owed maximum within named ceiling', `${maximum.stmts} > ${owedCeiling}`)
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
  // #648: a stale mount read names its shed. No owed read during the delay: only the alarm can refresh it.
  const stale = await get(APP, `/owed?identity=${ID_B}`)
  const old = JSON.parse(stale.text)
  if (stale.status === 200 && old.refresh === 'shed' && old.retryAfterMs > 0 && old.retryAfterMs <= 60_000)
    pass(`NS2 shed read marked: refresh=shed, retryAfterMs=${old.retryAfterMs}`)
  else fail('NS2 shed read carries its refresh word and window remainder', stale.text)
  const marked = (await appHealth()).owed
  if (marked.owed_recompute_shed_total === marked.recomputeShed && marked.owed_recompute_shed_total >= h1.recomputeShed + 1)
    pass('NS2 the shed mount read is counted')
  else fail('NS2 every shed is counted', JSON.stringify(marked))
  await new Promise((resolve) => setTimeout(resolve, Math.min(Math.max(old.retryAfterMs ?? 60_000, 0), 60_000)))
  const ranDeferred = await pollFor(async () => (await appHealth()).owed.owed_recompute_deferred_total >= (h1.owed_recompute_deferred_total ?? 0) + SEATS, 20_000)
  if (ranDeferred && (await appHealth()).owed.owed_recompute_deferred_total === (h1.owed_recompute_deferred_total ?? 0) + SEATS) pass('NS2 exactly one reserved alarm walk per seat runs without an owed poll')
  else fail('NS2 deferred walks converge at the window end', JSON.stringify((await appHealth()).owed))
  const refreshed = await get(APP, `/owed?identity=${ID_B}`)
  const fresh = JSON.parse(refreshed.text)
  if (refreshed.status === 200 && fresh.computedAtMs > old.computedAtMs && fresh.refresh !== 'shed')
    pass(`NS2 snapshot advanced without client poll: ${old.computedAtMs} -> ${fresh.computedAtMs}`)
  else fail('NS2 the alarm writes a fresh owed snapshot', refreshed.text)

}

// ── 5. E649 L4 M1: the pot hook's maximum bodies and its park's drain, each against a named ceiling ──────────
// (the fixture's `e649_hook_rows`). The hook PARKS every wake-up in its write batch before the answer (H1), so each
// hook scenario's writes include its parks; the tier's deploy has no BROADCAST_TOKEN, so the detached announce
// settles each as not configured and the drain deletes the seeded due rows.
{
  const hook = async (outpoints) => get(APP, '/internal/pot-changed', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${INTERNAL_TOKEN}` },
    body: JSON.stringify({ outpoints }),
  })
  const e649 = (i) => 'e649' + i.toString(16).padStart(60, '0')
  const bodies = [
    // A's eight decoded pots, one latched seat each: the decoded maximum body (eight outpoints)
    ['pot-changed-hook-decoded-8', Array.from({ length: 8 }, (_, i) => ({ txid: potTxid('a', i), vout: 0 })), 8],
    // the marker fallback alone: the undecoded pot whose party and refund-backup markers name five identities
    ['pot-changed-hook-marker', [{ txid: e649(0xff), vout: 0 }], 0],
    // the review's H2 body: seven decoded pots with two latched seats each, the marker pot naming five; 19 identities,
    // each with an existing owed-state row
    ['pot-changed-hook-8x19', [...Array.from({ length: 7 }, (_, i) => ({ txid: e649(i), vout: 0 })), { txid: e649(0xff), vout: 0 }], 14],
  ]
  for (const [name, outpoints, seats] of bodies) {
    const r = await hook(outpoints)
    if (r.status !== 200) {
      fail(`${name} answers`, `${r.status}: ${r.text.slice(0, 200)}`)
      continue
    }
    const body = JSON.parse(r.text)
    if (body.filed.length === seats && body.parked === outpoints.length + seats) pass(`${name}: ${seats} seat(s) filed, ${body.parked} wake-up(s) parked before the answer`)
    else fail(`${name}: files its seats and parks every wake-up`, r.text.slice(0, 300))
    check(name, segment(r.timing, 'd1'))
  }
  const d = await get(APP, '/internal/pot-announce-drain', { method: 'POST', headers: { Authorization: `Bearer ${INTERNAL_TOKEN}` } })
  if (d.status !== 200) fail('pot-announce-drain answers', `${d.status}: ${d.text.slice(0, 200)}`)
  else {
    const t = JSON.parse(d.text)
    if (t.due === 24 && t.skipped === 24 && t.swept === 6 && t.faults === 0) pass(`pot-announce-drain: 24 due settled (not configured here), 6 exhausted swept`)
    else fail('pot-announce-drain reads its bound and sweeps', d.text)
    check('pot-announce-drain', segment(d.timing, 'd1'))
  }
  const unauth = await get(APP, '/internal/pot-announce-drain', { method: 'POST' })
  if (unauth.status === 401) pass('pot-announce-drain refuses without the bearer')
  else fail('pot-announce-drain refuses without the bearer', `${unauth.status}`)
}

// NS-3: the real Worker reads compact rows with zero courier calls and
// keeps the legacy confirmation beside the tracker hint-tier disagreement.
{
  const parent = 'f1'.repeat(32), seen = 'f2'.repeat(32), mined = 'f3'.repeat(32)
  const checkWord = (label, response, word) => {
    if (response.status === 200 && word?.word === 'seen' && word.tier === 'hint' && word.verifyPath === null) pass(label)
    else fail(label, `${response.status}: ${response.text.slice(0, 300)}`)
  }
  const tx = await get(APP, `/tx-any/${seen}`)
  const txBody = JSON.parse(tx.text)
  checkWord('NS-3 tx-any lookup serves seen beside legacy confirmed', tx, txBody.tracker)
  if (txBody.tier !== 'unknown' || txBody.present !== null) fail('NS-3 preserves main foreign unknown fields', tx.text)
  const batch = await get(APP, `/tx-any?txids=${seen}`)
  const batchBody = JSON.parse(batch.text)
  const legacy = ({ tracker, ...body }) => body
  if (batch.status === 200 && JSON.stringify(legacy(batchBody.answers?.[seen] ?? {})) === JSON.stringify(legacy(txBody))) pass('NS-3 batch keeps main cached single body for a stored foreign word')
  else fail('NS-3 batch changed main cached foreign body', batch.text)
  const utxo = await get(APP, `/utxo-status?outpoints=${parent}.0`)
  checkWord('NS-3 utxo-status uses the spender subject', utxo, JSON.parse(utxo.text)?.[0]?.spenderWord)
  const spent = await get(APP, `/spent-any?outpoints=${parent}.0`)
  if (spent.status === 200 && JSON.parse(spent.text)?.[0]?.spendingTxid !== seen) pass('NS-3 an expired memo is re-asked even when its spender is tracked')
  else fail('NS-3 expired memo was reused', spent.text)
  const chain = await get(APP, `/tx-any/${mined}`)
  const chainBody = JSON.parse(chain.text)
  if (chain.status === 200 && chainBody.tracker?.verifyPath === null && chainBody.tracker?.cannotCheck) pass('NS-3 unavailable headers cannot serve a verify path')
  else fail('NS-3 mined lookup fails closed on unavailable headers', chain.text)
}

console.log('\n── bsv-low #499 D1 budget tier ──')
console.log('scenario                 reads (ceiling)   writes (ceiling)  stmts (ceiling)  maxReads (ceiling)')
for (const { name, got, ceiling } of table) {
  const col = (k) => `${String(got[k]).padStart(5)} (${ceiling ? String(ceiling[k] ?? (k === 'maxReads' ? ceiling.reads : '-')).padStart(5) : '    -'})`
  console.log(`${name.padEnd(24)} ${col('reads')}     ${col('writes')}      ${col('stmts')}     ${col('maxReads')}`)
}
console.log('')
for (const l of lines) console.log(l)
if (MEASURE) {
  console.log('\nD1_BUDGET_MEASURE=1: measured only, no ceiling enforced')
  process.exit(failures === 0 ? 0 : 1)
}
console.log(failures === 0 ? '\n✅ D1 budget tier green' : `\n✗ D1 budget tier: ${failures} failure(s)`)
process.exit(failures === 0 ? 0 : 1)
