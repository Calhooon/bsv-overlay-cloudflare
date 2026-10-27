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
 *     as `d1view`, and the forwarding request itself), /owed's first read (pending, one wanted mark: the read never
 *     computes since bsv-low #893) and, after the targeted judge (`/internal/hop-changed`), a served read.
 *  2. overlay: an operator `historical-tx-no-spv` /submit that ADMITS one tm_collected output (the admission
 *     writes); the overlay's ARCADE_URL points at a closed local port, so nothing leaves the machine.
 *  3. the health surfaces: `/health.d1Budget` (app) and `/health/invariants.d1Budget` (overlay) carry each route
 *     driven, with a running maximum at least the figure its answer carried.
 *  4. the pot hook: identity B's pot announced three times through `/internal/pot-changed`; each answer carries
 *     its `d1` segment (the judge inside it, bsv-low #893), the hooks are keyed `/internal` on `d1Budget`, and the
 *     first ask is held to `pot-changed-hook-1` and answers `hook;dur=`. (The recompute ceiling, its shed and the
 *     deferred walk are the retired walk's: D2-OWED-FOLD-8 dropped their cells.)
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
  const out = { status: res.status, timing: res.headers.get('server-timing'), text }
  // the port guard's record: every request of more than 50 statements, with the loopback count right after it
  if (((segment(out.timing, 'd1')?.stmts ?? 0) > 50 || path.startsWith('/internal') || path.startsWith('/record')) && typeof noteRequest === 'function') noteRequest(`${init?.method ?? 'GET'} ${path.split('?')[0]}`, out)
  return out
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

/** `POST /internal/hop-changed {"identities":[..]}`: the targeted judge (at most 16 dirty subjects, then the answer). */
const hopChanged = (identities) => get(APP, '/internal/hop-changed', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${INTERNAL_TOKEN}` },
  body: JSON.stringify({ identities }),
})
/** The owed-v2 backfill's lever: `''` one pass (the first arms), `'?evidence=1'` its evidence phase alone. */
const backfillLever = (query = '') => get(APP, `/internal/owed-backfill-pass${query}`, { method: 'POST', headers: { Authorization: `Bearer ${INTERNAL_TOKEN}` } })

// D2-OWED-FOLD-8, the port guard: wrangler dev's local D1 shim leaves about two loopback connections per D1 statement
// for about ten seconds, so a hook of 433 statements holds about 2,500 by itself. Before each heavy cell the harness
// waits for the loopback count to fall under 500 (at most 30 s) and stops the run past 2,000; the peak it saw is
// printed at the end.
function loopbackConnections() {
  const cmd = process.platform === 'darwin'
    ? "netstat -an -p tcp | grep ESTABLISHED | grep -c '127\\.0\\.0\\.1' || true"
    : "ss -Htn state established 2>/dev/null | grep -c '127\\.0\\.0\\.1' || true"
  try { return Number(execFileSync('sh', ['-c', cmd]).toString().trim()) || 0 } catch { return 0 }
}
let portPeak = 0
/** Each heavy request's own figures and the loopback count right after it (the port guard's record). */
function noteRequest(label, r) {
  const n = loopbackConnections()
  portPeak = Math.max(portPeak, n)
  console.log(`REQUEST ${label}: d1 ${JSON.stringify(segment(r.timing, 'd1'))}, loopback after ${n}`)
}
async function portGuard(label) {
  let n = 0
  for (let i = 0; i < 60; i++) {
    n = loopbackConnections()
    portPeak = Math.max(portPeak, n)
    if (n < 500) return
    await new Promise((r) => setTimeout(r, 500))
  }
  if (n > 2000) throw new Error(`port guard: ${n} loopback connections before ${label} (past 2,000): the run stops`)
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

  // D2-OWED-FOLD-8: the read never computes (bsv-low #893, the READ lane). A's first read finds no stored state, so it
  // answers `pending: true` and writes ONE wanted mark (five physical rows, the captain's ruling) and nothing else.
  // The judge writes the rows at the write path: A's seed rows carry their dirty marks (the seed loads after the
  // triggers), so `/internal/hop-changed` (the targeted judge) judges A's games, and ONE read then serves them.
  const o1 = await get(APP, `/owed?identity=${ID_A}`)
  let b1 = {}
  try { b1 = JSON.parse(o1.text) } catch {}
  if (o1.status === 200 && b1.pending === true && (b1.rows ?? []).length === 0) pass('/owed first read answers pending: true and computes nothing')
  else fail('/owed first read answers pending: true and computes nothing', `${o1.status}: ${o1.text.slice(0, 200)}`)
  const first = segment(o1.timing, 'd1')
  check('owed-first-read', first)
  await portGuard('hop-changed A')
  const judgedA = await hopChanged([ID_A])
  if (judgedA.status !== 200) fail('/internal/hop-changed judges A', `${judgedA.status}: ${judgedA.text.slice(0, 200)}`)
  const o2 = await get(APP, `/owed?identity=${ID_A}`)
  if (o2.status !== 200) fail('/owed served read answers', `${o2.status}: ${o2.text.slice(0, 200)}`)
  else if (JSON.parse(o2.text).rows?.length !== 9) fail('/owed serves A\'s 9 judged rows (8 pots and the swept hop)', o2.text.slice(0, 300))
  else pass('/owed serves A\'s 9 judged rows (8 pots and the swept hop) after the targeted judge')
  const served = segment(o2.timing, 'd1')
  check('owed-served-read', served)
  measured['/owed'] = first && served ? { reads: Math.max(first.reads, served.reads), writes: Math.max(first.writes, served.writes), stmts: Math.max(first.stmts, served.stmts) } : null
}

// bsv-low #693 fold-2: A's swept hop (the seed's `identity_a_durable_spender_rows`): its sweep is a DURABLE spender
// with a `proofmined:` latch past the grace, so the first read above made the latch range read and served the row
// from the latch (owed-first-read carries it).
{
  const sweep = 'a8'.repeat(32)
  const o = await get(APP, `/owed?identity=${ID_A}`)
  const row = JSON.parse(o.text).rows?.find((r) => r.facts?.sweepTxid === sweep)
  if (o.status === 200 && row?.facts?.confirmedSource === 'proof') pass('#693 A\'s durable sweep is served from its proof latch')
  else fail('#693 A\'s durable sweep is served from its proof latch', `${o.status} ${JSON.stringify(row ?? JSON.parse(o.text).rows?.filter((r) => r.outpoint?.startsWith('a7')))}`)
}

// bsv-low #723: /spent-any's index-proof read (`hopsweep::sweep_proofs_sql`) at its most outpoints. Twenty fresh
// probe memos name twenty of the fleet's PROVEN transactions as spenders with no confirmed word, so every row is a
// known spend and the one statement reads the index's proofs (`spentConfirmedSource: "index-proof"`).
{
  const crate = fileURLToPath(new URL('../../crates/low-app-layer/', import.meta.url))
  const wr = (args) => execFileSync('npx', ['wrangler', 'd1', 'execute', 'low-overlay-db', '--local', '--persist-to', process.env.D1_BUDGET_STATE, ...args], { cwd: crate, timeout: 30_000, stdio: 'pipe' })
  const proven = JSON.parse(wr(['--json', '--command', 'SELECT lower(txid) AS txid FROM transactions WHERE has_proof = 1 ORDER BY txid LIMIT 20']).toString())[0].results.map((r) => r.txid)
  const subject = (i) => 'e7' + i.toString(16).padStart(62, '0')
  const now = Date.now()
  wr(['--command', proven.map((sp, i) => `INSERT OR REPLACE INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed) VALUES ('${subject(i)}.0', ${now}, 1, '${sp}', NULL);`).join(' ')])
  const ops = proven.map((_, i) => `${subject(i)}.0`).join(',')
  const r = await get(APP, `/spent-any?outpoints=${ops}`)
  let sourced = 0
  try { sourced = JSON.parse(r.text).filter((e) => e.spentConfirmedSource === 'index-proof' && e.spentConfirmed === true).length } catch {}
  if (r.status === 200 && proven.length === 20 && sourced === 20) pass('#723 /spent-any confirms 20 known spends from the index proof read')
  else fail('#723 /spent-any confirms 20 known spends from the index proof read', `${r.status} proven=${proven.length} sourced=${sourced} ${r.text.slice(0, 300)}`)
  check('spent-any-20', (measured['/spent-any'] = segment(r.timing, 'd1')))
}

// L23R2 and D2-OWED-FOLD-8: the captured beta pair 13 (an eviction made BEFORE the eviction writer, so no evidence row)
// and its synthetic missing-bytes JOIN. The owed-v2 backfill's evidence phase writes their evidence through the
// writer's own statements (`?evidence=1`, after the arming pass), each evidence row marking its seat's game; the
// targeted judge then judges the four seats and ONE read each serves the old walk's rows (bsv-low
// D2-OWED-FOLD-7-FINDING; `old_walk_native::pins::fold8_a` is the native oracle, byte for byte).
// Dropped, by design: the old ring's cursor facts (`markerlessCandidates`: the ring is replaced by the evidence row)
// and the walk's per-pass cut (`truncated`, `judgement`: a missing JOIN's could-not-judge row is a complete judgement).
// The twin-only seat's hop memo is stamped by the cell's clock (overlay #85), so the judgement asks no courier.
{
  const p28 = '02f9f80a92a92677d97fce5bb0a03d0321425b918868b3ef5773120d9cf5364e49'
  const other = '030d9e9fbe045a8130f190b103609c8a6bf13b1887d5c4f15e2f918d68aca8ad67'
  const f1 = '02' + 'f1'.repeat(32), f2 = '02' + 'f2'.repeat(32)
  const otherHop = '9b238eed0177c714b1f9a608243e8f1bf79e35e9597110f83cefa1b658785c62'
  const crate = fileURLToPath(new URL('../../crates/low-app-layer/', import.meta.url))
  execFileSync('npx', ['wrangler', 'd1', 'execute', 'low-overlay-db', '--local', '--persist-to', process.env.D1_BUDGET_STATE, '--command',
    `INSERT OR REPLACE INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed) VALUES ('${otherHop}.0',${Date.now()},0,NULL,0)`],
  { cwd: crate, timeout: 30_000, stdio: 'pipe' })
  // the app layer's D1 here is the seed's, which the overlay never certified: write its certification row (the backfill
  // gate reads it), as the backfill tier does
  execFileSync('npx', ['wrangler', 'd1', 'execute', 'low-overlay-db', '--local', '--persist-to', process.env.D1_BUDGET_STATE, '--command',
    "INSERT INTO ops_counters(name,value) VALUES ('overlay_migration_count',409) ON CONFLICT(name) DO UPDATE SET value=excluded.value"],
  { cwd: crate, timeout: 30_000, stdio: 'pipe' })
  await portGuard('owed-backfill evidence')
  const armed = await backfillLever()
  const evidence = await backfillLever('?evidence=1')
  let ev = {}
  try { ev = JSON.parse(evidence.text) } catch {}
  if (armed.status === 200 && evidence.status === 200 && ev.stop === 'Page' && ev.evictions >= 2 && ev.evidenceCandidates >= 2)
    pass(`D2-OWED-FOLD-8 the evidence phase writes the pre-writer evictions' evidence (${ev.evictions} open evictions, ${ev.evidenceCandidates} hop outputs, ${ev.missingOutpoints} unreadable outpoints)`)
  else fail('D2-OWED-FOLD-8 the evidence phase writes the pre-writer evictions\' evidence', `${armed.status} ${evidence.status}: ${evidence.text.slice(0, 300)}`)
  const h0 = (await appHealth()).owed
  await portGuard('hop-changed pair 13')
  const judged = await hopChanged([p28, other, f1, f2])
  if (judged.status !== 200) fail('/internal/hop-changed judges pair 13', `${judged.status}: ${judged.text.slice(0, 200)}`)
  const h1 = (await appHealth()).owed
  const read = async (id) => {
    const r = await get(APP, `/owed?identity=${id}`)
    let body = {}
    try { body = JSON.parse(r.text) } catch {}
    return { r, body, rows: body.rows ?? [] }
  }
  const p = await read(p28)
  const prow = p.rows.find((x) => x.gameId === '9d88aaaa348acae127efd08e133822201ecae56513831c37124837f3ef9ae17f')
  if (p.r.status === 200 && prow?.facts?.spendKind === 'spent-elsewhere' && prow.facts.claim == null && prow.facts.markerVerified === 'verified')
    pass('L23R2 route-selection: captured eviction, twin evidence over the planted marker, 25h story (spent elsewhere)')
  else fail('L23R2 route-selection: captured eviction, twin evidence over the planted marker, 25h story', p.r.text.slice(0, 2000))
  const tally = segment(p.r.timing, 'd1')
  if (tally && tally.reads <= 256) pass('L23R2-H1 captured read stays in the 256-read #499 class under twin noise')
  else fail('L23R2-H1 captured read stays in the 256-read #499 class under twin noise', JSON.stringify(tally))
  if (h1.markerlessIgnoredMarkers > h0.markerlessIgnoredMarkers) pass('L23R2-M2 planted marker counted (the judge replaced it with the twin evidence)')
  else fail('L23R2-M2 planted marker counted', `${h0.markerlessIgnoredMarkers} -> ${h1.markerlessIgnoredMarkers}`)
  const o = await read(other)
  const orow = o.rows.find((x) => x.outpoint === `${otherHop}:0`)
  if (o.r.status === 200 && orow?.family === 'hop-stranded' && orow.facts?.joinRefusedBy === 'eviction' && orow.facts.markerVerified === 'verified')
    pass('L23R2-M3 twin-only captured seat: its released hop judged from the evidence the backfill wrote')
  else fail('L23R2-M3 twin-only captured seat judged', `${o.r.status} ${o.r.text.slice(0, 1500)}`)
  const a = await read(f1)
  const fault = a.rows.find((x) => x.facts?.evidenceMissing)
  if (a.r.status === 200 && fault?.reason?.includes('evidence-missing') && fault.facts.claim == null && fault.facts.claimable === false && a.rows.length === 2 && a.rows.some((x) => x.gameId === 'healthy-game') && h1.markerlessReadFaults > h0.markerlessReadFaults)
    pass('L23R2-L2 the missing JOIN is judged could-not-judge beside the healthy game, counted')
  else fail('L23R2-L2 the missing JOIN is judged could-not-judge beside the healthy game, counted', `${a.r.status} ${a.r.text.slice(0, 1500)} faults ${h0.markerlessReadFaults}->${h1.markerlessReadFaults}`)
  const c = await read(f2)
  if (c.r.status === 200 && c.rows.length > 0 && c.rows.every((x) => x.facts?.markerlessReadFault && x.facts.claim == null && x.facts.claimable === false))
    pass('L23R2-L2 the old snapshot\'s press is judged away: could-not-judge, no press')
  else fail('L23R2-L2 the old snapshot\'s press is judged away: could-not-judge, no press', `${c.r.status} ${c.r.text.slice(0, 1500)}`)
}

// A real signed stranger filing: all request statements join /record's measured ceiling.
// M1 removed the three party enumeration reads; the remaining filing work is still counted.
{
  const seed = readFileSync(process.env.D1_BUDGET_SEED, 'utf8')
  const fixture = JSON.parse(seed.split('\n').find((line) => line.startsWith('-- NS2_RECORD_FIXTURE ')).slice('-- NS2_RECORD_FIXTURE '.length))
  const victimBefore = JSON.parse((await get(APP, `/owed?identity=${ID_A}`)).text)
  await portGuard('record filing')
  const filed = await get(APP, `/record?kind=potparty&identity=${fixture.identity}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ scriptHex: fixture.scriptHex }),
  })
  if (filed.status === 200 && JSON.parse(filed.text).filed) pass('NS2 stranger potparty filing admitted')
  else fail('NS2 stranger potparty filing admitted', `${filed.status}: ${filed.text}`)
  // #908: only verification, caps and the source transaction belong to the request.
  check('record-potparty-filing', (measured['/record'] = segment(filed.timing, 'd1')))
  const victimAfter = JSON.parse((await get(APP, `/owed?identity=${ID_A}`)).text)
  if (victimAfter.computedAtMs === victimBefore.computedAtMs && victimAfter.rows?.length === victimBefore.rows?.length) pass('SHED-M1 a stranger\'s filing judges only its filer: A\'s judgement untouched')
  else fail('SHED-M1 a stranger\'s filing judges only its filer', `A computedAtMs ${victimBefore.computedAtMs} -> ${victimAfter.computedAtMs}, rows ${victimBefore.rows?.length} -> ${victimAfter.rows?.length}`)
  let filer
  const judged = await pollFor(async () => {
    filer = JSON.parse((await get(APP, `/owed?identity=${fixture.identity}`)).text)
    return filer.computedAtMs > 0 && filer.pending !== true
  })
  if (judged) pass(`SHED-M1 the filer's read serves its detached judgement (${filer.rows?.length} row(s))`)
  else fail('SHED-M1 the filer\'s detached judgement lands', JSON.stringify(filer).slice(0, 400))
}

// NOTE-3 (twelve failed first-read writes, then one reserved walk): DROPPED by design. The read never walks and the
// reserved deferred walk is retired (bsv-low #893, the READ and BELT lanes); a write fault is the judge's, under the
// drain's and the filing's own fault rules (pinned natively: `owed893_app_filing_atomic_on_row_fault`, the drain's).

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

// bsv-low #837 (FIX-837, fix 2), re-pointed by D2-OWED-FOLD-8: identity C holds 300 results (the seed's
// `fix837_c_rows`; loop 44 pair 1's A held 264). The read never computes, so C's 300 games are JUDGED NATIVELY by the
// seed (`d1_budget_seed`, the shipped judge 16 games a batch, its writes in the seed: the backfill's shape), and the
// read serves them: the NEWEST game's row (game 299) and every game, from ONE read of stored rows, measured
// (`owed-read-c`). A second read serves the same rows. It runs AFTER section 3, whose `/owed` maximum is A's.
{
  const ID_C = '02' + 'c3'.repeat(32)
  const newestGame = '837d' + (299).toString(16).padStart(60, '0')
  const t0 = Date.now()
  const o = await get(APP, `/owed?identity=${ID_C}`)
  const ms = Date.now() - t0
  let body = {}
  try { body = JSON.parse(o.text) } catch {}
  const rows = body.rows ?? []
  const games = new Set(rows.map((r) => r.gameId))
  const d1 = segment(o.timing, 'd1')
  console.log(`#837 C's read: ${ms} ms, ${rows.length} rows over ${games.size} games, truncated=${body.truncated ?? null}, d1 ${JSON.stringify(d1)}`)
  if (o.status === 200 && rows.some((r) => r.gameId === newestGame)) pass(`#837 a 300-result identity's newest game is served (${ms} ms)`)
  else fail('#837 a 300-result identity\'s newest game is served', `${o.status} ${ms} ms rows=${rows.length} ${o.text.slice(0, 300)}`)
  if (games.size >= 300) pass(`#837 C's list holds every game (${games.size})`)
  else fail('#837 C\'s list holds every game', `games=${games.size}`)
  check('owed-read-c', d1)
  const o2 = await get(APP, `/owed?identity=${ID_C}`)
  let rows2 = []
  try { rows2 = JSON.parse(o2.text).rows ?? [] } catch {}
  if (o2.status === 200 && rows2.some((r) => r.gameId === newestGame) && rows2.length === rows.length) pass('#837 C\'s second read serves the same rows')
  else fail('#837 C\'s second read serves the same rows', `${o2.status} ${rows2.length} vs ${rows.length}`)
}

// ── 3b. bsv-low #838: the harness's settle of /health/invariants on the fleet fixture ─────────────────────
// Loop 44 lost two cells to this read (each needs two agreeing answers inside 20 s, `settled(.., budgetMs: 20000)`),
// not to /submit. Its reads are issued together and its own rows-read bump is after the answer, so the answer
// WRITES nothing (a write put back before the answer reds `writes`), and two answers 250 ms apart, the cells'
// shape, are held to a wall-time ceiling far under the settle (measured on the fixture plus the margin).
{
  const t0 = Date.now()
  const first = await get(OVERLAY, '/health/invariants')
  await new Promise((r) => setTimeout(r, 250))
  const second = await get(OVERLAY, '/health/invariants')
  const ms = Date.now() - t0
  if (first.status !== 200 || second.status !== 200) fail('health-invariants: two answers', `${first.status}, ${second.status}`)
  check('health-invariants', segment(second.timing, 'd1'))
  const c = CEILINGS.scenarios['health-invariants']
  if (MEASURE) lines.push(`MEASURE health-invariants settle: two answers in ${ms} ms`)
  else if (!c?.settleCeilingMs) fail('health-invariants: a settle ceiling', 'no settleCeilingMs in ceilings.json')
  else if (ms > c.settleCeilingMs) fail('health-invariants: two answers inside the settle ceiling', `${ms} ms > ${c.settleCeilingMs} ms (the harness's settle is 20000)`)
  else pass(`health-invariants: two answers in ${ms} ms (ceiling ${c.settleCeilingMs}, the harness's settle 20000)`)
}

// ── 4. the pot hook answers with its own figures ────────────────────────────
// D2-OWED-FOLD-8: the recompute ceiling, its shed, the deferred alarm walk and `owed-recompute` are the retired walk's
// machinery (bsv-low #893: the judge runs at the write, the read is a lookup, the belt looks in the background), so
// their cells are DROPPED: "pot-changed ask N settles", "the ceiling sheds the 13th recompute", "d1Budget keys each
// detached walk under owed-recompute", "owed maximum within named ceiling" (the owed-recompute maximum), and NS2's
// shed read, its count, the deferred walks converging and the alarm's fresh snapshot. What stays: the hook is a write
// whose answer carries its D1 figures (the judge inside it, the design's cost at the write) and its `hook;dur=`, is
// keyed under `/internal`, and answers each of three asks on B's pot.
{
  const asks = 3
  const b0 = (await appHealth()).d1Budget
  let hookUnstamped = 0
  for (let i = 0; i < asks; i++) {
    await portGuard(`pot-changed ask ${i + 1}`)
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
  }
  if (hookUnstamped === 0) pass(`every /internal/pot-changed answer carries d1;desc= (${asks} of ${asks})`)
  else fail('a hook route answers with its D1 figures', `${hookUnstamped} of ${asks} answers carry no d1 segment`)
  const b1 = (await appHealth()).d1Budget
  const moved = (route) => (b1?.routes?.[route]?.requests ?? 0) - (b0?.routes?.[route]?.requests ?? 0)
  if (moved('/internal') >= asks) pass(`d1Budget keys the hooks under /internal (+${moved('/internal')} requests)`)
  else fail('d1Budget keys the hooks under /internal', `+${moved('/internal')} requests (want at least ${asks})`)
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
    await portGuard(name)
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
  // E649 L4 N3: the seed's backlog of 120 due rows. The first pass runs its four rounds of 24 (96 settled, not
  // configured here) and sweeps the 6 exhausted; the second takes the 24 left and reads one more round of none.
  const drain = async () => get(APP, '/internal/pot-announce-drain', { method: 'POST', headers: { Authorization: `Bearer ${INTERNAL_TOKEN}` } })
  await portGuard('pot-announce-drain')
  for (const [name, want] of [
    ['pot-announce-drain-backlog', { rounds: 4, due: 96, skipped: 96, swept: 6 }],
    ['pot-announce-drain', { rounds: 2, due: 24, skipped: 24, swept: 0 }],
  ]) {
    const d = await drain()
    if (d.status !== 200) {
      fail(`${name} answers`, `${d.status}: ${d.text.slice(0, 200)}`)
      continue
    }
    const t = JSON.parse(d.text)
    if (t.rounds === want.rounds && t.due === want.due && t.skipped === want.skipped && t.swept === want.swept && t.aged === 0 && t.faults === 0) pass(`${name}: ${t.rounds} round(s), ${t.due} due settled (not configured here), ${t.swept} exhausted swept`)
    else fail(`${name} reads its rounds and sweeps`, d.text)
    check(name, segment(d.timing, 'd1'))
  }
  const unauth = await get(APP, '/internal/pot-announce-drain', { method: 'POST' })
  if (unauth.status === 401) pass('pot-announce-drain refuses without the bearer')
  else fail('pot-announce-drain refuses without the bearer', `${unauth.status}`)
  // E649 L4 N6 (fold-3): the ceiling held for the batch WHOLE. The table filled to 2,048 rows of live wake-ups that
  // failed no delivery yet (none may be displaced): a body of eight new rooms is refused whole (503, retryable,
  // nothing written). Eight rows then made exhausted: the same body parks all eight, the room made of them.
  const crate = fileURLToPath(new URL('../../crates/low-app-layer/', import.meta.url))
  const sql = (command) => execFileSync('npx', ['wrangler', 'd1', 'execute', 'low-overlay-db', '--local', '--persist-to', process.env.D1_BUDGET_STATE, '--command', command], { cwd: crate, timeout: 30_000, stdio: 'pipe' })
  sql("WITH RECURSIVE k(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM k WHERE i + 1 < 2048) INSERT INTO pot_announce_parked (outpoint, kind, recipient, at, attempts, next_attempt_at_ms, generation, exhausted_at_ms) SELECT printf('e64f%060x:0', i), 'room', '', 1, 0, 9000000000000, 1, NULL FROM k WHERE i < 2048 - (SELECT COUNT(*) FROM pot_announce_parked)")
  const fresh = Array.from({ length: 8 }, (_, i) => ({ txid: 'e64e' + i.toString(16).padStart(60, '0'), vout: 0 }))
  await portGuard('pot-changed-hook-full-refused')
  const refused = await hook(fresh)
  const rb = refused.status === 503 ? JSON.parse(refused.text) : null
  if (rb && rb.retryable === true && rb.parked === 0 && rb.parkFull === 8 && rb.announce === 'none') pass('pot-changed-hook-full-refused: refused whole at 2,048 rows, 503 retryable, nothing announced')
  else fail('pot-changed-hook-full-refused: refused whole', `${refused.status}: ${refused.text.slice(0, 300)}`)
  check('pot-changed-hook-full-refused', segment(refused.timing, 'd1'))
  // E649 L4 N10 (the third delta review): D1 answers the failed batch with no figures, so the reads above (16) are
  // NOT the refusal's total: the batch's counter scan (at most 2,048 + 8 rows) and its guard, which with no row that
  // may make room VISITS EVERY ROW (about 2,059, by the review's D1 binding), are real and unreported here. The
  // local wrangler CLI reports no rows read, so that total is stated (docs/D1-BUDGETS.md), bounded by the scans the
  // tail scenario below measures, not measured here.
  sql("UPDATE pot_announce_parked SET exhausted_at_ms = 1 WHERE rowid IN (SELECT rowid FROM pot_announce_parked WHERE outpoint LIKE 'e64f%' ORDER BY outpoint LIMIT 8)")
  await portGuard('pot-changed-hook-full-room')
  const room = await hook(fresh)
  const ob = room.status === 200 ? JSON.parse(room.text) : null
  // the detached announce (not configured here) settles the eight as delivered at once: what is left to read is that
  // the eight exhausted rows made the room and the table never passed its ceiling
  const one = (q) => Number(Object.values(JSON.parse(execFileSync('npx', ['wrangler', 'd1', 'execute', 'low-overlay-db', '--local', '--persist-to', process.env.D1_BUDGET_STATE, '--json', '--command', q], { cwd: crate, timeout: 30_000, stdio: 'pipe' }).toString())[0].results[0])[0])
  const exhaustedLeft = one("SELECT COUNT(*) AS n FROM pot_announce_parked WHERE exhausted_at_ms IS NOT NULL AND outpoint LIKE 'e64f%'")
  const count = one('SELECT COUNT(*) AS n FROM pot_announce_parked')
  if (ob && ob.parked === 8 && ob.parkFull === 0 && exhaustedLeft === 0 && count <= 2048) pass(`pot-changed-hook-full-room: eight exhausted rows make the room, all eight parked, the table at ${count} <= 2,048`)
  else fail('pot-changed-hook-full-room: room made of exhausted rows', `${room.status} exhaustedLeft=${exhaustedLeft} count=${count}: ${room.text.slice(0, 300)}`)
  check('pot-changed-hook-full-room', segment(room.timing, 'd1'))
  // E649 L4 N10: the guard's LIMIT is of ELIGIBLE rows, not of rows visited: with the rows that may make room at the
  // TAIL of the table's scan order, the guard visits the whole table to find them (and the delete's ORDER BY reads it
  // as ever). Wait for the detached announce to settle the eight (the table at 2,040), refill to 2,048 at the tail,
  // make the LAST eight rows by rowid exhausted, then a fresh body of eight rooms.
  let settled = 0
  for (let i = 0; i < 40 && (settled = one('SELECT COUNT(*) AS n FROM pot_announce_parked')) > 2040; i++) await new Promise((r) => setTimeout(r, 250))
  // the room hook's detached work (its settle and counters) is let finish before this cell writes the local D1 again
  await new Promise((r) => setTimeout(r, 3000))
  const alive = await get(APP, '/health')
  if (alive.status !== 200) fail('pot-changed-hook-full-room-tail: the app layer answers before the refill', `${alive.status}`)
  sql("WITH RECURSIVE k(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM k WHERE i + 1 < 2048) INSERT INTO pot_announce_parked (outpoint, kind, recipient, at, attempts, next_attempt_at_ms, generation, exhausted_at_ms) SELECT printf('e64d%060x:0', i), 'room', '', 1, 0, 9000000000000, 1, NULL FROM k WHERE i < 2048 - (SELECT COUNT(*) FROM pot_announce_parked)")
  sql('UPDATE pot_announce_parked SET exhausted_at_ms = 1 WHERE rowid IN (SELECT rowid FROM pot_announce_parked ORDER BY rowid DESC LIMIT 8)')
  const before = one('SELECT COUNT(*) AS n FROM pot_announce_parked')
  const tailFresh = Array.from({ length: 8 }, (_, i) => ({ txid: 'e64c' + i.toString(16).padStart(60, '0'), vout: 0 }))
  await portGuard('pot-changed-hook-full-room-tail')
  const tail = await hook(tailFresh)
  const tb = tail.status === 200 ? JSON.parse(tail.text) : null
  const tailCount = one('SELECT COUNT(*) AS n FROM pot_announce_parked')
  if (settled === 2040 && before === 2048 && tb && tb.parked === 8 && tb.parkFull === 0 && tailCount <= 2048) pass(`pot-changed-hook-full-room-tail: eight exhausted rows at the tail make the room, all eight parked, the table at ${tailCount} <= 2,048`)
  else fail('pot-changed-hook-full-room-tail: room made of exhausted rows at the tail', `${tail.status} settled=${settled} before=${before} count=${tailCount}: ${tail.text.slice(0, 300)}`)
  check('pot-changed-hook-full-room-tail', segment(tail.timing, 'd1'))
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
console.log(`PORT GUARD the loopback connections the guard saw: peak ${portPeak}`)
if (MEASURE) {
  console.log('\nD1_BUDGET_MEASURE=1: measured only, no ceiling enforced')
  process.exit(failures === 0 ? 0 : 1)
}
console.log(failures === 0 ? '\n✅ D1 budget tier green' : `\n✗ D1 budget tier: ${failures} failure(s)`)
process.exit(failures === 0 ? 0 : 1)
