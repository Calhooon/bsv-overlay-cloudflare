#!/usr/bin/env node
/**
 * bsv-low #837 (the FIX-837 lens's MEDIUM-1): the MISSING-SPEND DISCOVERY PASS under a flood of a stranger's wide JOINs,
 * in the budget tier (`make ci-d1-budget`).
 *
 * The lens found the shared hop-spend write (`record_join_hop_spends`) reading one D1 row per JOIN input with no
 * bound, on the scheduled discovery pass whose pot candidates a stranger's admitted covenant-shaped body can shape.
 * The fold bounds it: at most `JOIN_HOP_INPUTS_MAX` (16) input rows a JOIN and `JOIN_HOP_READS_PER_PASS` (64) a pass,
 * two reads held back for each pot candidate still to come (an honest JOIN's two hops), every input past the bound
 * COUNTED and read by a later window. This cell runs the pass on the #831 cell's overlay after that cell (its
 * drained D1, its couriers sunk to this cell's loopback 404 stand-in) through `POST /internal/missing-spend-pass`
 * (bearer INTERNAL_TOKEN), the tick's own function:
 *
 *  0. the lever's bearer (401) and body (400); the budget it serves.
 *  1. the fixture (`examples/fix837_join_fixture.rs`): five stranger JOINs of 64 carried P2PKH inputs (320 hop rows)
 *     and eight honest two-hop JOINs, every pot held, older than every other row so the pass reads these first.
 *  2. one pass: its figures held to `missing-spend-pass-join-flood` (`ceilings.json`, by name); its JOIN input reads
 *     at most 64; every unread input counted; every honest JOIN's two hops marked spent by it (the reserve).
 * On 9d380f8c there is no lever (404): RED. With the bound lifted the pass reads 336 input rows and reds on the
 * ceiling.
 *
 *   node tools/lane-837/join_hop_pass_ci.mjs <overlay base> <its --persist-to dir> <stand-in port>
 *   D1_BUDGET_MEASURE=1 ...   prints the measured figures and enforces nothing
 *
 * Exit 0 = every expectation held.
 */
import { execFileSync } from 'node:child_process'
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
const ROOT = fileURLToPath(new URL('../../', import.meta.url))
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
const CEILINGS = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url), 'utf8'))
if (!OVERLAY || !STATE || !SINK_PORT) {
  console.error('usage: join_hop_pass_ci.mjs <overlay base> <its --persist-to dir> <stand-in port>')
  process.exit(2)
}
const WIDE = 5
const WIDTH = 64
const HONEST = 8
const PER_JOIN = 16
const PER_PASS = 64

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}

const asked = []
const sink = createServer((req, res) => {
  asked.push(`${req.method} ${req.url}`)
  res.writeHead(404, { 'content-type': 'text/plain' })
  res.end('not found')
})
await new Promise((ok, no) => sink.listen(SINK_PORT, '127.0.0.1', ok).on('error', no))

const scratch = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), 'lane837-'))
function d1(sql, file = false) {
  const args = ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json']
  if (file) {
    const path = join(scratch, `seed-${Date.now()}.sql`)
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
  const r = await fetch(OVERLAY + '/internal/missing-spend-pass', {
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
  // 0. The bearer, the body, the budget.
  const unauth = await lever('', 'wrong')
  expect(unauth.status === 401, 'the lever refuses a wrong bearer (401)', `got ${unauth.status}: ${unauth.text.slice(0, 200)}`)
  const bad = await lever('{"x":1}')
  expect(bad.status === 400, 'the lever refuses a body (400)', `got ${bad.status}: ${bad.text.slice(0, 200)}`)

  // 1. The fixture.
  const sql = execFileSync(
    'cargo',
    ['run', '-q', '--manifest-path', 'workers/Cargo.toml', '-p', 'bsv-overlay-cloudflare', '--example', 'fix837_join_fixture', '--', String(WIDE), String(WIDTH), String(HONEST)],
    { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], maxBuffer: 64 << 20 },
  )
  const joins = sql.split('\n').filter((l) => l.startsWith('-- JOIN ')).map((l) => {
    const [, , kind, txid, funding, inputs] = l.split(' ')
    return { kind, txid, funding, inputs: Number(inputs) }
  })
  expect(joins.length === WIDE + HONEST, `the fixture names ${WIDE + HONEST} JOINs`, `${joins.length}`)
  d1(sql, true)
  const total = joins.reduce((n, j) => n + j.inputs, 0)

  // 2. One pass.
  const p = await lever()
  const s = p.json ?? {}
  lines.push(`INFO  the pass: ${JSON.stringify({ d1: p.d1, scanned: s.scanned, joinHopsMarked: s.joinHopsMarked, joinHopReads: s.joinHopReads, joinHopInputsDeferred: s.joinHopInputsDeferred, joinHopPotsDeferred: s.joinHopPotsDeferred, courierAsks: asked.length })}`)
  expect(p.status === 200, 'the lever answers one pass', `${p.status} ${p.text.slice(0, 300)}`)
  const budget = s.budget ?? {}
  expect(budget.joinInputsPerJoin === PER_JOIN && budget.joinReadsPerPass === PER_PASS && budget.candidates === 20,
    `the lever serves the bound (${PER_JOIN} a JOIN, ${PER_PASS} a pass, 20 candidates)`, JSON.stringify(budget))
  check('missing-spend-pass-join-flood', p.d1)
  expect(s.joinHopReads > 0 && s.joinHopReads <= PER_PASS, `the pass's JOIN input reads (${s.joinHopReads}) are at most ${PER_PASS}`, JSON.stringify(s))
  expect(s.joinHopInputsDeferred + s.joinHopReads === total && s.joinHopPotsDeferred === 0,
    `every input the pass did not read is counted (${s.joinHopInputsDeferred} of ${total})`, JSON.stringify(s))
  const marked = (j) => Number(d1(`SELECT COUNT(*) AS n FROM pot_records WHERE txid = '${j.funding}' AND spent = 1 AND spendingTxid = '${j.txid}' AND spentConfirmed = 0`)[0]?.n)
  const honest = joins.filter((j) => j.kind === 'honest')
  const honestMarked = honest.map(marked)
  expect(honestMarked.every((n) => n === 2), 'every honest JOIN has both hops marked spent by it, unconfirmed (the reserve)', JSON.stringify(honestMarked))
  const wideMarked = joins.filter((j) => j.kind === 'wide').map(marked)
  expect(wideMarked.every((n) => n <= PER_JOIN), `no stranger JOIN had more than ${PER_JOIN} inputs read`, JSON.stringify(wideMarked))
  lines.push(`INFO  marked per stranger JOIN ${JSON.stringify(wideMarked)}`)
  expect(asked.every((a) => a.startsWith('GET /')), 'the couriers asked the loopback stand-in only', asked.slice(0, 3).join(' | '))
} catch (e) {
  expect(false, 'the cell ran', e.stack ?? String(e))
} finally {
  sink.close()
  rmSync(scratch, { recursive: true, force: true })
}

console.log(lines.join('\n'))
if (MEASURE) console.log(`MEASURED ${JSON.stringify(measured)}`)
if (failures) {
  console.log(`✗ missing-spend pass budget (#837): ${failures} failure(s)`)
  process.exit(1)
}
console.log(MEASURE ? '✓ missing-spend pass budget (#837): measured (nothing enforced)' : '✓ missing-spend pass budget (#837): every expectation held')
