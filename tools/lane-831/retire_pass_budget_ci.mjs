#!/usr/bin/env node
/**
 * bsv-low #831 (the FIX-831-B lens's MEDIUM-2): the RETIRE PASS's worst tick in the budget tier (`make ci-d1-budget`).
 *
 * The lens found the pass's new D1 cost unmeasured: the tier's overlay D1 held no retire candidate, so its one pass
 * read nothing, while one ending is about 75 statements and the base could end 16 a tick (about 1,200 statements,
 * above the per-invocation query ceiling the overlay calls unverified). This cell runs the pass on a POPULATED D1,
 * on its own overlay (`make ci-d1-budget`: LANE_BASE+16, given PROOF_SOURCES and a COURIER_SINK that is this
 * cell's loopback stand-in on LANE_BASE+18, so every courier ask is answered 404 here and none leaves the machine),
 * through `POST /internal/retire-pass` (bearer INTERNAL_TOKEN), the tick's own function:
 *
 *  0. the lever's bearer (401) and body (400); one pass on the empty D1 (`retire-pass-empty`).
 *  1. the fixture: 24 watched ghost JOINs (two hops each, an engine row and a stored BEEF, 49 h old), 16 pots
 *     already latched with a live row, 8 pots the engine arm retired before the fold (off the watch, unlatched:
 *     the lens's MEDIUM-1 population), every one absent at Arcade and both indexers (the stand-in's 404).
 *  2. the worst tick: one pass on that D1, its figures held to `retire-pass-worst-tick` (`ceilings.json`, by name),
 *     the pass's own count of its statements held to its budget plus its three tail statements, some endings made
 *     and the rest DEFERRED.
 *  3. the drain: passes until every ghost pot is ended (each pass under the same ceiling), each ended pot under the
 *     retire reason in the ledger, its rows shadow-moved, its hops released.
 * On `ef9186df` there is no lever (404): RED. With the budget lifted (the base's endings) the worst tick reds on
 * the ceiling.
 *
 *   node tools/lane-831/retire_pass_budget_ci.mjs <retire overlay base> <its --persist-to dir> <stand-in port>
 *   D1_BUDGET_MEASURE=1 ...   prints the measured figures and enforces nothing
 *
 * Exit 0 = every expectation held.
 */
import { execFileSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const OVERLAY = process.argv[2]
const STATE = process.argv[3]
const SINK_PORT = Number(process.argv[4])
const TOKEN = process.env.D1_BUDGET_INTERNAL_TOKEN ?? 'ci-internal-tok'
const MEASURE = process.env.D1_BUDGET_MEASURE === '1'
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
const CEILINGS = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url), 'utf8'))
if (!OVERLAY || !STATE || !SINK_PORT) {
  console.error('usage: retire_pass_budget_ci.mjs <retire overlay base> <its --persist-to dir> <stand-in port>')
  process.exit(2)
}

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}

// The couriers' stand-in: every GET is 404 (Arcade's "not carried", both indexers' "not known"), counted by path.
const asked = []
const sink = createServer((req, res) => {
  asked.push(`${req.method} ${req.url}`)
  res.writeHead(404, { 'content-type': 'text/plain' })
  res.end('not found')
})
await new Promise((ok, no) => sink.listen(SINK_PORT, '127.0.0.1', ok).on('error', no))

const scratch = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), 'lane831-'))
function d1(sql, file = false) {
  const args = ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json']
  let path = null
  if (file) {
    path = join(scratch, `seed-${Date.now()}.sql`)
    writeFileSync(path, sql)
    args.push('--file', path)
  } else args.push('--command', sql)
  for (let attempt = 1; ; attempt++) {
    try {
      const out = execFileSync('npx', args, { cwd: CRATE, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
      return JSON.parse(out)[0]?.results ?? []
    } catch (e) {
      if (attempt >= 4) throw new Error(`wrangler d1 execute failed: ${e.stderr ?? e.message}`)
    }
  }
}

function segment(header) {
  for (const seg of (header ?? '').split(',')) {
    const m = seg.trim().match(/^d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
    if (m) return { reads: Number(m[1]), writes: Number(m[2]), stmts: Number(m[3]), maxReads: m[4] === undefined ? null : Number(m[4]) }
  }
  return null
}

async function lever(body = '', token = TOKEN) {
  const r = await fetch(OVERLAY + '/internal/retire-pass', {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', Connection: 'close' },
    body,
  })
  const text = await r.text()
  let json = null
  try {
    json = JSON.parse(text)
  } catch {}
  return { status: r.status, d1: segment(r.headers.get('server-timing')), json, text }
}

const measured = {}
function check(name, got) {
  const c = CEILINGS.scenarios[name]
  measured[name] = got
  if (MEASURE) return lines.push(`MEASURE ${name}: ${JSON.stringify(got)}`)
  if (!got) return expect(false, `${name}: the answer carries its d1 segment`, 'no d1 Server-Timing segment')
  if (!c) return expect(false, `${name}: has a ceiling`, `no entry in ceilings.json (measured ${JSON.stringify(got)})`)
  const over = ['reads', 'writes', 'stmts'].filter((k) => got[k] > c.ceiling[k])
  if (got.maxReads !== null && got.maxReads > (c.ceiling.maxReads ?? c.ceiling.reads)) over.push(`maxReads ${got.maxReads}`)
  expect(over.length === 0, `${name}: ${JSON.stringify(got)} under ${JSON.stringify(c.ceiling)}`, `over on ${over.join(', ')} (${c.route})`)
}

try {
  // 0. The bearer, the body, an empty pass (and the migrations, run by the lever).
  const unauth = await lever('', 'wrong')
  expect(unauth.status === 401, 'the lever refuses a wrong bearer (401)', `got ${unauth.status}: ${unauth.text}`)
  const bad = await lever('{"x":1}')
  expect(bad.status === 400, 'the lever refuses a body (400)', `got ${bad.status}: ${bad.text}`)
  const empty = await lever('{}')
  expect(empty.status === 200 && empty.json?.scanned === 0, 'one pass on the empty D1 reads nothing', `${empty.status} ${empty.text.slice(0, 300)}`)
  const budget = empty.json?.budget ?? {}
  expect(budget.statements === 480 && budget.endReserve === 96 && budget.backlogPage === 32 && budget.backlogLimit === 8,
    'the lever serves the pass budget (480 statements, 96 a step, a page of 32, 8 a pass)', JSON.stringify(budget))
  check('retire-pass-empty', empty.d1)

  // 1. The fixture.
  const now = Date.now()
  const at = Math.floor((now - 49 * 3600_000) / 1000)
  const hex = () => randomBytes(32).toString('hex')
  const ghosts = { watched: [], latched: [], escaped: [] }
  const hops = []
  const sql = []
  const seed = (join, kind) => {
    const hs = kind === 'watched' ? [hex(), hex()] : [hex()]
    hops.push(...hs)
    sql.push(`INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind, pubA, stakeA) VALUES ('${join}', 0, 0, ${at}, 'covenant', '02aa', 9900);`)
    for (const h of hs)
      sql.push(`INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, spentAt, createdAt) VALUES ('${h}', 0, 1, '${join}', 0, ${at}, ${at - 60});`)
    sql.push(`INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES ('${join}', 0, X'51', 'tm_pot', 19800, 0);`)
    sql.push(`INSERT INTO applied_transactions (txid, topic) VALUES ('${join}', 'tm_pot'), ('${join}', 'tm_lowfund');`)
    const retired = kind === 'escaped' ? `${(now - 3600_000)}, 'network-absent 176400s: arcade 404 + both indexers 404'` : 'NULL, NULL'
    sql.push(`INSERT INTO transactions (txid, beef, has_proof, created_at, retired_ms, retired_reason) VALUES ('${join}', X'00', 0, ${at}, ${retired});`)
    sql.push(`INSERT INTO pot_beefs (txid, beef, createdAt, has_proof, structurally_unprovable) VALUES ('${join}', X'0100beef', ${at}, 0, ${kind === 'latched' ? 1 : 'NULL'});`)
    if (kind === 'watched') sql.push(`INSERT INTO proofless_watch (txid, first_seen_ms) VALUES ('${join}', ${at * 1000});`)
    ghosts[kind].push(join)
  }
  for (let i = 0; i < 24; i++) seed(hex(), 'watched')
  for (let i = 0; i < 16; i++) seed(hex(), 'latched')
  for (let i = 0; i < 8; i++) seed(hex(), 'escaped')
  d1(sql.join('\n'), true)
  const all = [...ghosts.watched, ...ghosts.latched, ...ghosts.escaped]
  const live = () => Number(d1(`SELECT COUNT(*) AS n FROM pot_records WHERE txid IN (${all.map((t) => `'${t}'`).join(',')})`)[0]?.n)
  expect(live() === 48, 'the fixture holds 48 live ghost pots', `got ${live()}`)

  // 2. The worst tick.
  const p1 = await lever()
  check('retire-pass-worst-tick', p1.d1)
  const s1 = p1.json ?? {}
  const ended1 = (s1.evictedOnRetire ?? 0) + (s1.backlog?.evicted ?? 0)
  const deferred1 = (s1.endDeferred ?? 0) + (s1.backlog?.deferred ?? 0)
  lines.push(`INFO  worst tick: ${JSON.stringify({ d1: p1.d1, statements: s1.statements, ended: ended1, deferred: deferred1, scanned: s1.scanned, backlog: s1.backlog, conflict: s1.conflict })}`)
  expect(p1.status === 200 && ended1 > 0 && deferred1 > 0, 'the worst tick ends some pots and DEFERS the rest', p1.text.slice(0, 400))
  expect(s1.statements <= 480 + 3, `the pass's own statements (${s1.statements}) stay within its budget (480) and its three tail statements`, JSON.stringify(s1))
  expect(p1.d1 && s1.d1 && s1.d1.stmts <= p1.d1.stmts, "the pass's own ledger is inside the request's", `${JSON.stringify(s1.d1)} vs ${JSON.stringify(p1.d1)}`)

  // 3. The drain: every ghost ends, each pass under the same ceiling.
  let passes = 1
  let worst = p1.d1
  while (live() > 0 && passes < 80) {
    const p = await lever()
    passes++
    if (p.status !== 200) {
      expect(false, `drain pass ${passes} answers`, `${p.status} ${p.text.slice(0, 200)}`)
      break
    }
    if (p.d1 && worst && p.d1.stmts > worst.stmts) worst = p.d1
    if (p.json?.statements > 480 + 3) expect(false, `drain pass ${passes} within its budget`, JSON.stringify(p.json))
  }
  lines.push(`INFO  drained in ${passes} pass(es); the largest pass ${JSON.stringify(worst)}; ${asked.length} courier ask(s) answered here`)
  expect(live() === 0, `every ghost pot is ended (${passes} passes)`, `${live()} still live`)
  check('retire-pass-drain-max', worst)
  const ledger = d1(`SELECT txid, reason, readmittedAt FROM pot_evictions WHERE txid IN (${all.map((t) => `'${t}'`).join(',')})`)
  expect(ledger.length === 48 && ledger.every((r) => r.reason.startsWith('retired unprovable: ') && (r.readmittedAt === null || r.readmittedAt === 'null')),
    'each ended pot is under the retire reason in the ledger, open', `${ledger.length} rows: ${JSON.stringify(ledger.slice(0, 2))}`)
  const moved = Number(d1(`SELECT COUNT(*) AS n FROM pot_records_evicted WHERE txid IN (${all.map((t) => `'${t}'`).join(',')})`)[0]?.n)
  expect(moved === 48, 'every ended pot row is shadow-moved', `${moved}`)
  const pointing = Number(d1(`SELECT COUNT(*) AS n FROM pot_records WHERE spendingTxid IN (${all.map((t) => `'${t}'`).join(',')})`)[0]?.n)
  expect(pointing === 0, "no hop names an ended pot (the spends released)", `${pointing}`)
  expect(asked.length > 0 && asked.every((a) => a.startsWith('GET /')), 'the couriers asked the loopback stand-in only', asked.slice(0, 3).join(' | '))
} catch (e) {
  expect(false, 'the cell ran', e.stack ?? String(e))
} finally {
  sink.close()
  rmSync(scratch, { recursive: true, force: true })
}

console.log(lines.join('\n'))
if (MEASURE) console.log(`MEASURED ${JSON.stringify(measured)}`)
if (failures) {
  console.log(`✗ retire-pass budget: ${failures} failure(s)`)
  process.exit(1)
}
console.log(MEASURE ? '✓ retire-pass budget: measured (nothing enforced)' : '✓ retire-pass budget: every expectation held')
