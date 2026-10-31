#!/usr/bin/env node
/**
 * bsv-low #934 (FIX-934-FOLD, the READ-934 lens's "What the tier must exercise"): `/internal/tx-word` under a drain
 * pass's burst, in the budget tier (`make ci-d1-budget`, after the #936 cell). UNRUN until the land's Linux tier
 * (this lane checked it with `node --check` only).
 *
 * The cell runs its OWN app layer (`wrangler dev` over the shim the tier's app layer already built,
 * `crates/low-app-layer/build/worker/shim.mjs`, never a rebuild under it) on a minimal config, beside a second worker
 * in the same dev session bound as `CHAINTRACKS` (`chaintracks_proxy.mjs`), which forwards each call to this cell's
 * loopback stand-in. The stand-in answers the verified header tip SLOWLY (`SLOW_MS`, 1.5 s a call) and counts every
 * call: the judge's first statement is its tip read (`judge_words`), so the stand-in's tip count is the number of
 * judge RUNS (a run and its one re-run each read the tip once). `COURIER_SINK` is the stand-in too. Every host the
 * worker can reach is a loopback stand-in; its debug log is tapped (`tools/lane-731/network_tap.mjs`) and handed to
 * the egress guard by the Makefile.
 *
 * Seed (the production schema from `examples/owed_cadence_schema.rs`, then 128 subjects): 128 word txids, each held
 * live (a `transactions` row, `stored::LIVE_HOLD`), each named by one owed row of its own game (`owed_rows` +
 * `owed_row_words`, the shape of the native pin `read934f_l1_*`), `owed_dirty` emptied.
 *
 * Legs:
 *  A. 128 frames in the producer's shape (`tracker_events::note(txid, "seen", seen_body(..))`, the frame the
 *     overlay's change-park drain POSTs), POSTed at once (the drain's `DRAIN_MAX_ROWS`): every answer 200
 *     `{"ok":true,"held":true}` (what `tracker_events::answered_held` reads) inside 2 s of its send; every word's
 *     `tx_words` row written; at least 112 subjects still marked when the last answer arrived.
 *  B. Exactly one detached judge (the lens's LOW-1 fold): after the judge's wall (17 s, waited 20 s) the stand-in
 *     counted at most 2 tip reads (the one judge and its one re-run), and at most 16 subjects were judged in all
 *     (`owed_dirty` at least 112).
 *  C. The marks drain (the alarm drain is not driven locally; each remaining frame re-POSTed alone, the stand-in
 *     fast, waiting for its subject's mark to go): `owed_dirty` reaches 0, so no mark was lost behind the gate.
 *
 * NOT exercised: the platform's spread of a pass's 128 POSTs over several isolates (local dev is one isolate: the
 * bound is per isolate, k isolates start k judges), the belt actor's alarm drain, and the cost bar's late window
 * (LOW-4: the detached statements reach `worker_cost_daily` only through a later request's take).
 *
 *   node tools/lane-934/tx_word_burst_route_ci.mjs <port base> <worker log path>
 *
 * Ports: base (the app layer), base+1 (its inspector), base+2 (the stand-in). Exit 0 = every expectation held.
 */
import { execFileSync, spawn } from 'node:child_process'
import { existsSync, mkdtempSync, openSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const BASE_PORT = Number(process.argv[2])
const LOG = process.argv[3]
if (!BASE_PORT || !LOG) {
  console.error('usage: tx_word_burst_route_ci.mjs <port base> <worker log path>')
  process.exit(2)
}
const PORT = BASE_PORT
const INSPECTOR = BASE_PORT + 1
const STANDIN = BASE_PORT + 2
const APP = `http://127.0.0.1:${PORT}`
const ROOT = fileURLToPath(new URL('../../', import.meta.url))
const SHIM = join(ROOT, 'crates/low-app-layer/build/worker/shim.mjs')
const TOKEN = 'ci-lane-934'
const FRAMES = 128
const SLOW_MS = 1_500
const ANSWER_MS = 2_000
const JUDGE_WALL_WAIT_MS = 20_000
const HEIGHT = 900000

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}
const sleep = (ms) => new Promise((ok) => setTimeout(ok, ms))
const word = (n) => `f${n.toString(16).padStart(63, '0')}`
const pot = (n) => n.toString(16).padStart(64, '0')

// ── the stand-in: the slow CHAINTRACKS behind the proxy, and the courier sink ───────────────────────────────────
let slow = true
const calls = { tip: 0, other: 0 }
const header = (height) => ({
  status: 'success',
  value: { height, hash: 'ab'.repeat(32), merkleRoot: 'cd'.repeat(32), time: Math.floor(Date.now() / 1000) },
})
const standin = createServer((req, res) => {
  req.resume()
  req.on('end', async () => {
    const url = new URL(req.url, 'http://standin')
    const route = url.pathname.replace(/^\/chaintracks/, '')
    const json = (status, body) => {
      res.writeHead(status, { 'content-type': 'application/json' })
      res.end(JSON.stringify(body))
    }
    if (route === '/findChainTipHeaderHex' || route === '/getPresentHeight') {
      calls.tip++
      if (slow) await sleep(SLOW_MS)
      return route === '/getPresentHeight' ? json(200, { status: 'success', value: HEIGHT }) : json(200, header(HEIGHT))
    }
    calls.other++
    if (route === '/findHeaderHexForHeight' && Number(url.searchParams.get('height')) === HEIGHT) {
      if (slow) await sleep(SLOW_MS)
      return json(200, header(HEIGHT))
    }
    json(404, {})
  })
})

// ── the worker ────────────────────────────────────────────────────────────────────────────────────────────────────
const scratch = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), 'lane934-'))
const STATE = join(scratch, 'state')
const CONFIG = join(scratch, 'wrangler.toml')
const PROXY_CONFIG = join(scratch, 'chaintracks.toml')
function writeConfigs() {
  if (!existsSync(SHIM)) throw new Error(`no app-layer shim at ${SHIM} (the tier's app layer builds it first)`)
  writeFileSync(CONFIG, `name = "lane934-app"
main = ${JSON.stringify(SHIM)}
compatibility_date = "2024-12-01"
[vars]
AUTH_ENFORCE = "false"
SESSION_LANE = "false"
COURIER_SINK = "http://127.0.0.1:${STANDIN}"
INTERNAL_TOKEN = "${TOKEN}"
OWED_FLEET_COMPARE_PER_TICK = "0"
[[services]]
binding = "CHAINTRACKS"
service = "lane934-chaintracks"
[[d1_databases]]
binding = "OVERLAY_DB"
database_name = "low-overlay-db"
database_id = "00000000-0000-0000-0000-000000000000"
`)
  writeFileSync(join(scratch, 'chaintracks_proxy.mjs'), `export default {
  async fetch(request) {
    const { pathname, search } = new URL(request.url)
    return fetch('http://127.0.0.1:${STANDIN}/chaintracks' + pathname + search)
  },
}
`)
  writeFileSync(PROXY_CONFIG, `name = "lane934-chaintracks"
main = ${JSON.stringify(join(scratch, 'chaintracks_proxy.mjs'))}
compatibility_date = "2024-12-01"
`)
}
const children = []
function startWorker() {
  const fd = openSync(LOG, 'a')
  const args = ['wrangler', 'dev', '--config', CONFIG, '--config', PROXY_CONFIG, '--local', '--log-level', 'debug',
    '--port', String(PORT), '--inspector-port', String(INSPECTOR), '--ip', '127.0.0.1', '--persist-to', STATE]
  const w = spawn('npx', args, { cwd: scratch, stdio: ['ignore', fd, fd], detached: true, env: { ...process.env, WRANGLER_SEND_METRICS: 'false' } })
  children.push(w)
  const tapLog = openSync(LOG.replace(/\.log$/, '') + '-tap.log', 'a')
  const tap = spawn(process.execPath, [join(ROOT, 'tools/lane-731/network_tap.mjs'), String(INSPECTOR), 'lane934-app-layer'], { stdio: ['ignore', tapLog, tapLog], detached: true })
  children.push(tap)
}
function stopAll() {
  for (const c of children) {
    try { process.kill(-c.pid, 'SIGTERM') } catch { /* gone */ }
  }
  standin.close()
}
async function up(url, ms = 180_000) {
  const t0 = Date.now()
  for (;;) {
    try { const r = await fetch(url, { headers: { Connection: 'close' } }); await r.text(); return true } catch { /* not yet */ }
    if (Date.now() - t0 > ms) return false
    await sleep(1_000)
  }
}

// ── D1: the schema and the seed through wrangler, reads straight from the file ───────────────────────────────────
function d1File(path) {
  const args = ['wrangler', 'd1', 'execute', 'low-overlay-db', '--config', CONFIG, '--local', '--persist-to', STATE, '--json', '--file', path]
  for (let attempt = 1; ; attempt++) {
    try {
      return execFileSync('npx', args, { cwd: scratch, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
    } catch (e) {
      if (attempt >= 4) throw new Error(`wrangler d1 execute failed: ${e.stderr ?? e.message}`)
    }
  }
}
function seed() {
  const schema = JSON.parse(execFileSync('cargo', ['run', '-q', '--manifest-path', join(ROOT, 'workers/Cargo.toml'),
    '-p', 'low-app-layer', '--example', 'owed_cadence_schema'], { cwd: ROOT, encoding: 'utf8', maxBuffer: 64 << 20 }))
  const schemaPath = join(scratch, 'schema.sql')
  writeFileSync(schemaPath, schema.map((s) => `${s};`).join('\n'))
  execFileSync('python3', [join(ROOT, 'scripts/d1-load-chunked.py'), scratch, 'low-overlay-db', STATE, schemaPath, '200'],
    { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] })
  const rows = []
  for (let n = 0; n < FRAMES; n++) {
    const game = `g${String(n).padStart(3, '0')}`
    rows.push(
      `INSERT INTO transactions(txid) VALUES ('${word(n)}');`,
      `INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES ('owner','other','${game}','${pot(n)}',0,1,'00','${pot(n)}',1,1,1);`,
      `INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) VALUES ('owner','${pot(n)}:0','unbound','${game}','{}',0);`,
      `INSERT INTO owed_row_words(identity,outpoint,role,txid) VALUES ('owner','${pot(n)}:0','settle','${word(n)}');`,
    )
  }
  rows.push('DELETE FROM owed_dirty;')
  const seedPath = join(scratch, 'seed.sql')
  writeFileSync(seedPath, rows.join('\n'))
  d1File(seedPath)
}
let FILE = null
function findFile() {
  const files = []
  const walk = (d) => {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      if (e.isDirectory()) walk(join(d, e.name))
      else if (e.name.endsWith('.sqlite')) files.push(join(d, e.name))
    }
  }
  try { walk(join(STATE, 'v3/d1')) } catch { /* not yet */ }
  for (const f of files) {
    try {
      const out = execFileSync('sqlite3', ['-cmd', '.timeout 5000', f, "SELECT name FROM sqlite_master WHERE name = 'owed_dirty'"], { encoding: 'utf8' })
      if (out.trim() === 'owed_dirty') return f
    } catch { /* another database */ }
  }
  return null
}
function count(sql) {
  for (;;) {
    try {
      return Number(execFileSync('sqlite3', ['-cmd', '.timeout 2000', FILE, sql], { encoding: 'utf8' }).trim())
    } catch (e) {
      if (!/locked|SQLITE_BUSY/i.test(`${e.stderr ?? ''}${e.message ?? ''}`)) throw e
    }
  }
}
const marked = () => count('SELECT COUNT(*) FROM owed_dirty')

/** The overlay's frame (`tracker_events::note(txid, "seen", seen_body("arcade", at))`, as its drain POSTs it). */
const frame = (n) => ({ txid: word(n), kind: 'seen', body: { source: 'arcade', at: Date.now() } })
async function post(n) {
  const sent = Date.now()
  const r = await fetch(APP + '/internal/tx-word', {
    method: 'POST',
    headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json', Connection: 'close' },
    body: JSON.stringify(frame(n)),
  })
  const text = await r.text()
  let held = false
  try { held = JSON.parse(text)?.held === true } catch { /* not json */ }
  return { n, status: r.status, held, ms: Date.now() - sent, text }
}

async function main() {
  await new Promise((ok) => standin.listen(STANDIN, '127.0.0.1', ok))
  writeConfigs()
  seed()
  startWorker()
  if (!(await up(APP + '/health'))) throw new Error(`the cell's app layer never came up (see ${LOG})`)
  FILE = findFile()
  if (!FILE) throw new Error('the D1 file was not found')
  expect(marked() === 0, 'seed: no mark pending before the burst', `owed_dirty ${marked()}`)
  calls.tip = 0

  // A. the burst
  const answers = await Promise.all(Array.from({ length: FRAMES }, (_, n) => post(n)))
  const atAnswers = marked()
  const slowest = Math.max(...answers.map((a) => a.ms))
  const bad = answers.filter((a) => a.status !== 200 || !a.held)
  expect(bad.length === 0, `A: all ${FRAMES} frames answered 200 held`, `${bad.length} not: ${bad.slice(0, 3).map((a) => `${a.n} ${a.status} ${a.text}`).join(' | ')}`)
  expect(slowest <= ANSWER_MS, `A: every frame answered inside ${ANSWER_MS} ms (slowest ${slowest} ms)`, `slowest ${slowest} ms`)
  const words = count(`SELECT COUNT(*) FROM tx_words WHERE txid GLOB 'f*' AND length(txid) = 64`)
  expect(words === FRAMES, `A: ${FRAMES} tx_words rows written before their answers`, `${words}`)
  expect(atAnswers >= FRAMES - 16, `A: at least ${FRAMES - 16} subjects marked at the last answer (${atAnswers})`, `${atAnswers}`)

  // B. one detached judge
  await sleep(JUDGE_WALL_WAIT_MS)
  const afterWall = marked()
  expect(calls.tip <= 2, `B: one detached judge (tip reads ${calls.tip}, at most 2: the run and its one re-run)`, `${calls.tip} tip reads`)
  expect(afterWall >= FRAMES - 16, `B: at most 16 subjects judged by the burst (${FRAMES - afterWall})`, `owed_dirty ${afterWall}`)
  lines.push(`INFO  B: judged by the burst's judge ${FRAMES - afterWall}, tip reads ${calls.tip}, other CHAINTRACKS ${calls.other}`)

  // C. the marks drain
  slow = false
  const t0 = Date.now()
  for (let n = 0; n < FRAMES && marked() > 0; n++) {
    const owner = `g${String(n).padStart(3, '0')}`
    if (count(`SELECT COUNT(*) FROM owed_dirty WHERE gameId = '${owner}'`) === 0) continue
    await post(n)
    for (let i = 0; i < 100 && count(`SELECT COUNT(*) FROM owed_dirty WHERE gameId = '${owner}'`) > 0; i++) await sleep(100)
  }
  expect(marked() === 0, `C: every mark drained (${Date.now() - t0} ms)`, `owed_dirty ${marked()}`)
}

let crashed = null
try {
  await main()
} catch (e) {
  crashed = e
} finally {
  stopAll()
  await sleep(500)
  try { rmSync(scratch, { recursive: true, force: true }) } catch { /* best effort */ }
}
console.log('── #934 /internal/tx-word burst (FIX-934-FOLD) ──')
for (const l of lines) console.log(l)
if (crashed) {
  console.log(`FAIL  the cell crashed: ${crashed.stack ?? crashed}`)
  process.exit(1)
}
console.log(failures ? `✗ ${failures} expectation(s) failed` : '✓ every expectation held')
process.exit(failures ? 1 : 0)
