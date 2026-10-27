#!/usr/bin/env node
/**
 * bsv-low #936 (FIX-936-FOLD, the FIX-936 lens's section "What the tier must exercise"): a broadcast-gated JOIN
 * whose client goes away DURING its engine write, in the budget tier (`make ci-d1-budget`, after the #834 cell).
 *
 * The cell runs its OWN overlay (`wrangler dev` over the shim the tier's overlays already built, never a rebuild
 * under them), from a copy of `crates/overlay-cloudflare/wrangler.toml` (with its `compatibility_flags =
 * ["enable_request_signal"]`), every host it can reach a loopback stand-in on this cell's port: `ARCADE_URL` and
 * `CORROBORATOR_URL` (every status probe and every broadcast SEEN_ON_NETWORK, so the gated ladder stops at its
 * pre-flight probe), `COURIER_SINK`, `CHAIN_TRACKER_URL`, `DUAL_BROADCAST=off`, `MAINNET_FANOUT=false`. Its debug
 * log is tapped (`tools/lane-731/network_tap.mjs`) and handed to the egress guard by the Makefile.
 *
 * The write is SLOWED on local D1 (Rule: never a guess at a timing): a spin table and two triggers on the subject's
 * own rows, the `outputs` insert (the write's first statement, `insert_output`) spinning about 1.5 s and the
 * `pot_records` insert (the lookup service's `output_admitted_by_topic`) about 4 s, each sized against this host's
 * sqlite3 before the legs. The cell reads the D1 file (sqlite3, never through the worker) until the subject's
 * `outputs` row is there and aborts the client's fetch at that moment: the cancel falls between two statements of
 * the write, the S4 class (loop 52's `fad01028…`).
 *
 * Legs (each a fresh JOIN: a proofless V1 BEEF, an `OP_TRUE` parent and the JOIN spending it into a
 * `Poc5TemplatePot` covenant output built from the frozen template, `tm_pot` / `ls_pot`):
 *  A. the abort mid-write: (a) `outputs`, `transactions`, `pot_records`, `pot_beefs` and the `tm_pot` applied row
 *     all present after the task; (b) the task's own line `<S> request-gone write landed` and
 *     `submit_write_outlived_request_total` +1; (d) the deferred job at rest (`ef_deferred_jobs`, the cover put on
 *     the abort, shape (a) of the fold) and its line.
 *  B. the abort mid-write with an OPEN eviction written DURING the write (the `outputs` trigger writes the
 *     `pot_evictions` row after the coin and before the lookup services' rows): (c) the task's guard re-evicts:
 *     no live `outputs`, `pot_records` or applied row, the rows in the twins, the line `<S> request-gone write
 *     outran its eviction`.
 *  C. the control (Rule 6), no abort: 200, the line `<S> in-request write landed`, the counter unchanged, no job.
 *
 * NOT exercised here: the platform's own cut of the task (`wait_until`'s cap after a cancel) and the job's run
 * landing a cut write (the job's message is delayed 45 s; the local tier's queue is not the platform's). Whether
 * `wrangler dev`'s proxy hands a client's disconnect to the runtime's `Request.signal` is what leg A finds first:
 * an `in-request` line with the client aborted is named as that, not as the fold's failure.
 *
 *   node tools/lane-936/request_gone_route_ci.mjs <port base> <worker log path>
 *
 * Ports: base (the worker), base+1 (its inspector), base+2 (the stand-in). Exit 0 = every expectation held.
 */
import { execFileSync, spawn } from 'node:child_process'
import { createHash, randomFillSync } from 'node:crypto'
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, openSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const BASE_PORT = Number(process.argv[2])
const LOG = process.argv[3]
if (!BASE_PORT || !LOG) {
  console.error('usage: request_gone_route_ci.mjs <port base> <worker log path>')
  process.exit(2)
}
const PORT = BASE_PORT
const INSPECTOR = BASE_PORT + 1
const STANDIN = BASE_PORT + 2
const OVERLAY = `http://127.0.0.1:${PORT}`
const ROOT = fileURLToPath(new URL('../../', import.meta.url))
const CRATE = join(ROOT, 'crates/overlay-cloudflare')
const SHIM = join(CRATE, 'build/worker/shim.mjs')
const COUNTER = 'submit_write_outlived_request_total'
const SPIN_MS = { outputs: 1_500, pot_records: 4_000 }

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}
const sleep = (ms) => new Promise((ok) => setTimeout(ok, ms))

// ── the JOIN ──────────────────────────────────────────────────────────────────
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
function varint(n) {
  if (n < 0xfd) return Buffer.from([n])
  if (n <= 0xffff) { const b = Buffer.alloc(3); b[0] = 0xfd; b.writeUInt16LE(n, 1); return b }
  return Buffer.concat([Buffer.from([0xfe]), u32(n)])
}
const sha256 = (b) => createHash('sha256').update(b).digest()
const txidLE = (raw) => sha256(sha256(raw))
const txidHex = (raw) => Buffer.from(txidLE(raw)).reverse().toString('hex')
/** A data push of `b` (each param here is under 76 bytes). */
const push = (b) => Buffer.concat([Buffer.from([b.length]), b])
/** A minimal script number push (positive). */
function num(n) {
  if (n === 0) return Buffer.from([0x00])
  if (n >= 1 && n <= 16) return Buffer.from([0x50 + n])
  const bytes = []
  while (n > 0) { bytes.push(n & 0xff); n = Math.floor(n / 256) }
  if (bytes[bytes.length - 1] & 0x80) bytes.push(0)
  return push(Buffer.from(bytes))
}
function covenantLock() {
  const template = readFileSync(join(ROOT, 'crates/overlay-discovery/src/pot/poc5_template.hex'), 'utf8').trim()
  const head = Buffer.from(template.slice(0, template.indexOf('<')), 'hex')
  const tail = Buffer.from(template.slice(template.lastIndexOf('>') + 1), 'hex')
  const vectors = JSON.parse(readFileSync(join(ROOT, 'crates/overlay-discovery/vectors/money-templates-v1.json'), 'utf8'))
  const f = vectors.fixtures
  const c = vectors.cases[0]
  const h = (x) => Buffer.from(x, 'hex')
  const mid = Buffer.concat([
    push(h(f.pubA)), push(h(f.pubB)), push(h(f.pubTower)),
    push(h(c.payPkhA)), push(h(c.payPkhB)), push(h(c.rakePkh)),
    num(c.stakeA), num(c.stakeB), num(c.feeSats), num(c.recoveryHeight),
  ])
  return { lock: Buffer.concat([head, mid, tail]), sats: c.stakeA + c.stakeB }
}
const COVENANT = covenantLock()
function rawTx(prev, vout, outputs) {
  return Buffer.concat([
    u32(1),
    varint(1), prev, u32(vout), varint(0), Buffer.from([0xff, 0xff, 0xff, 0xff]),
    varint(outputs.length),
    ...outputs.flatMap(([sats, script]) => [u64(sats), varint(script.length), script]),
    u32(0),
  ])
}
function join936() {
  const prev = Buffer.alloc(32); randomFillSync(prev)
  const parent = rawTx(prev, 0, [[20_000, Buffer.from([0x51])]])
  const subject = rawTx(txidLE(parent), 0, [[COVENANT.sats, COVENANT.lock]])
  const beef = Buffer.concat([
    Buffer.from([0x01, 0x00, 0xbe, 0xef]), varint(0), varint(2),
    parent, Buffer.from([0x00]), subject, Buffer.from([0x00]),
  ])
  return { beef, subject: txidHex(subject) }
}

// ── the stand-in: Arcade, the corroborator, the couriers, the chain tracker ─────
const asked = []
const standin = createServer((req, res) => {
  req.resume()
  req.on('end', () => {
    asked.push(`${req.method} ${req.url}`)
    const probed = req.url.match(/^\/tx\/([0-9a-fA-F]{64})$/)
    if (req.method === 'GET' && probed) {
      res.writeHead(200, { 'content-type': 'application/json' })
      return res.end(JSON.stringify({ txid: probed[1].toLowerCase(), txStatus: 'SEEN_ON_NETWORK' }))
    }
    if (req.method === 'POST' && ['/tx', '/txs', '/v1/tx'].includes(req.url)) {
      res.writeHead(200, { 'content-type': 'application/json' })
      return res.end(JSON.stringify({ txStatus: 'SEEN_ON_NETWORK' }))
    }
    res.writeHead(404, { 'content-type': 'text/plain' })
    res.end('not found')
  })
})

// ── the worker ────────────────────────────────────────────────────────────────
const scratch = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), 'lane936-'))
const STATE = join(scratch, 'state')
const CONFIG = join(scratch, 'wrangler.toml')
function writeConfig() {
  let toml = readFileSync(join(CRATE, 'wrangler.toml'), 'utf8')
  // the shim the tier's overlays built; no [build]: this session never rebuilds under the others
  toml = toml.replace(/^main = .*$/m, `main = ${JSON.stringify(SHIM)}`)
  toml = toml.replace(/^\[build\]\n(?:(?!^\[).*\n)*/m, '')
  if (!/^compatibility_flags = \["enable_request_signal"\]$/m.test(toml)) throw new Error('the overlay config carries no enable_request_signal')
  writeFileSync(CONFIG, toml)
}
const children = []
function startWorker() {
  const fd = openSync(LOG, 'a')
  const vars = {
    TOPIC_MANAGERS: 'tm_pot',
    LOOKUP_SERVICES: 'ls_pot',
    SUBMIT_ENFORCE: 'true',
    SUBMIT_OPERATOR_TOKEN: 'ci-lane-936',
    ENABLE_EXTENSIONS: 'true',
    ARCADE_URL: `http://127.0.0.1:${STANDIN}`,
    CORROBORATOR_URL: `http://127.0.0.1:${STANDIN}`,
    COURIER_SINK: `http://127.0.0.1:${STANDIN}`,
    CHAIN_TRACKER_URL: `http://127.0.0.1:${STANDIN}/chaintracks`,
    HOSTING_URL: `http://127.0.0.1:${PORT}`,
    DUAL_BROADCAST: 'off',
    MAINNET_FANOUT: 'false',
    INTERNAL_TOKEN: 'ci-internal-tok',
  }
  const args = ['wrangler', 'dev', '--config', CONFIG, '--local', '--log-level', 'debug', '--port', String(PORT),
    '--inspector-port', String(INSPECTOR), '--ip', '127.0.0.1', '--persist-to', STATE,
    ...Object.entries(vars).flatMap(([k, v]) => ['--var', `${k}:${v}`])]
  const w = spawn('npx', args, { cwd: scratch, stdio: ['ignore', fd, fd], detached: true, env: { ...process.env, WRANGLER_SEND_METRICS: 'false' } })
  children.push(w)
  const tapLog = openSync(LOG.replace(/\.log$/, '') + '-tap.log', 'a')
  const tap = spawn(process.execPath, [join(ROOT, 'tools/lane-731/network_tap.mjs'), String(INSPECTOR), 'lane936-overlay'], { stdio: ['ignore', tapLog, tapLog], detached: true })
  children.push(tap)
}
function stopAll() {
  for (const c of children) {
    try { process.kill(-c.pid, 'SIGTERM') } catch { /* gone */ }
  }
}
async function up(url, ms = 180_000) {
  const t0 = Date.now()
  for (;;) {
    try { const r = await fetch(url, { headers: { Connection: 'close' } }); await r.text(); return true } catch { /* not yet */ }
    if (Date.now() - t0 > ms) return false
    await sleep(1_000)
  }
}

// ── D1: writes through wrangler (the sibling cells' way), reads straight from the file ──
let seq = 0
function d1(sql) {
  const path = join(scratch, `q-${seq++}.sql`)
  writeFileSync(path, sql)
  const args = ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--config', CONFIG, '--local', '--persist-to', STATE, '--json', '--file', path]
  for (let attempt = 1; ; attempt++) {
    try {
      const out = execFileSync('npx', args, { cwd: scratch, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
      const all = JSON.parse(out)
      return all[all.length - 1]?.results ?? []
    } catch (e) {
      if (attempt >= 4) throw new Error(`wrangler d1 execute failed: ${e.stderr ?? e.message}`)
    }
  }
}
function d1File() {
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
      const out = execFileSync('sqlite3', ['-cmd', '.timeout 5000', f, "SELECT name FROM sqlite_master WHERE name = 'outputs'"], { encoding: 'utf8' })
      if (out.trim() === 'outputs') return f
    } catch { /* another database */ }
  }
  return null
}
let FILE = null
/** One read of the D1 file; undefined when busy (the next read is made). */
function read(sql) {
  try {
    const out = execFileSync('sqlite3', ['-json', '-cmd', '.timeout 2000', FILE, sql], { encoding: 'utf8' })
    return out.trim() ? JSON.parse(out) : []
  } catch (e) {
    if (/locked|SQLITE_BUSY/i.test(`${e.stderr ?? ''}${e.message ?? ''}`)) return undefined
    throw e
  }
}
const one = (sql) => { for (;;) { const r = read(sql); if (r !== undefined) return r[0] ?? null } }

/** The spin's units per millisecond on this host's sqlite3 (the runtime's sqlite is of the same build class). */
function calibrate() {
  const db = join(scratch, 'calibrate.sqlite')
  const spin = 'CREATE TABLE s (x INTEGER); WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000) INSERT INTO s SELECT x FROM c;'
  execFileSync('sqlite3', [db, spin])
  const t0 = process.hrtime.bigint()
  execFileSync('sqlite3', [db, 'SELECT COUNT(*) FROM s a, s b, s c WHERE c.x <= 20;'])
  const ms = Number(process.hrtime.bigint() - t0) / 1e6
  return Math.max(0.05, 20 / Math.max(1, ms - 5)) // units (of 10^6 rows) per ms, the process start discounted
}
function plantSpin(perMs) {
  const n = (t) => Math.max(1, Math.round(SPIN_MS[t] * perMs))
  d1(`
CREATE TABLE IF NOT EXISTS lane936_spin (x INTEGER);
DELETE FROM lane936_spin;
WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000) INSERT INTO lane936_spin SELECT x FROM c;
CREATE TABLE IF NOT EXISTS lane936_spin_n (t TEXT PRIMARY KEY, n INTEGER NOT NULL);
INSERT OR REPLACE INTO lane936_spin_n (t, n) VALUES ('outputs', ${n('outputs')}), ('pot_records', ${n('pot_records')});
CREATE TABLE IF NOT EXISTS lane936_subjects (txid TEXT PRIMARY KEY, evict INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS lane936_slow_outputs AFTER INSERT ON outputs
  WHEN NEW.txid IN (SELECT txid FROM lane936_subjects)
BEGIN
  SELECT COUNT(*) FROM lane936_spin a, lane936_spin b, lane936_spin c WHERE c.x <= (SELECT n FROM lane936_spin_n WHERE t = 'outputs');
  INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends)
    SELECT NEW.txid, 'REJECTED (lane-936: an eviction opened during the write)', CAST(strftime('%s', 'now') AS INTEGER) * 1000, NULL, 0, '[]'
    WHERE NEW.txid IN (SELECT txid FROM lane936_subjects WHERE evict = 1)
      AND NOT EXISTS (SELECT 1 FROM pot_evictions WHERE txid = NEW.txid);
END;
CREATE TRIGGER IF NOT EXISTS lane936_slow_pot_records AFTER INSERT ON pot_records
  WHEN NEW.txid IN (SELECT txid FROM lane936_subjects)
BEGIN
  SELECT COUNT(*) FROM lane936_spin a, lane936_spin b, lane936_spin c WHERE c.x <= (SELECT n FROM lane936_spin_n WHERE t = 'pot_records');
END;
`)
  return { outputs: n('outputs'), pot_records: n('pot_records') }
}

async function counter() {
  const r = await fetch(OVERLAY + '/health/invariants', { headers: { Connection: 'close' } })
  return Number((await r.json())?.counters?.[COUNTER] ?? NaN)
}
const logText = () => (existsSync(LOG) ? readFileSync(LOG, 'utf8') : '')
async function logLine(re, ms = 45_000) {
  const t0 = Date.now()
  for (;;) {
    const m = logText().match(re)
    if (m) return m[0]
    if (Date.now() - t0 > ms) return null
    await sleep(500)
  }
}
function rows(S) {
  return one(`SELECT
    (SELECT COUNT(*) FROM outputs WHERE txid = '${S}') AS outputs,
    (SELECT COUNT(*) FROM transactions WHERE txid = '${S}') AS transactions,
    (SELECT COUNT(*) FROM pot_records WHERE txid = '${S}') AS pot_records,
    (SELECT COUNT(*) FROM pot_beefs WHERE txid = '${S}') AS pot_beefs,
    (SELECT COUNT(*) FROM applied_transactions WHERE txid = '${S}' AND topic = 'tm_pot') AS applied`)
}
/** The deferred jobs naming the subject (the table is created by the first deferral). */
function jobs(S) {
  const t = one("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'ef_deferred_jobs'")
  if (!Number(t?.n)) return 0
  return Number(one(`SELECT COUNT(*) AS n FROM ef_deferred_jobs WHERE subject_txid = '${S}'`)?.n ?? 0)
}
function twins(S) {
  const names = (read("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('outputs_evicted','transactions_evicted','pot_records_evicted','pot_beefs_evicted','applied_transactions_evicted')") ?? []).map((r) => r.name)
  let n = 0
  for (const t of names) n += Number(one(`SELECT COUNT(*) AS n FROM "${t}" WHERE txid = '${S}'`)?.n ?? 0)
  return n
}

/** A gated submit of a fresh JOIN; `abortMidWrite` aborts the client once the coin's row is in the file. */
async function present({ evict, abortMidWrite }) {
  const { beef, subject: S } = join936()
  d1(`INSERT INTO lane936_subjects (txid, evict) VALUES ('${S}', ${evict ? 1 : 0});`)
  const ctl = new AbortController()
  const sent = Date.now()
  const answer = fetch(OVERLAY + '/submit', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'x-topics': JSON.stringify(['tm_pot']),
      'x-submit-mode': 'broadcast-gated',
      Connection: 'close',
    },
    body: beef,
    signal: ctl.signal,
  }).then(async (r) => ({ status: r.status, text: await r.text() }), (e) => ({ status: 0, text: String(e?.name ?? e) }))
  let abortedAt = null
  if (abortMidWrite) {
    let settled = false
    answer.then(() => { settled = true })
    while (!settled && Date.now() - sent < 30_000) {
      const r = read(`SELECT COUNT(*) AS n FROM outputs WHERE txid = '${S}'`)
      if (r && Number(r[0]?.n) > 0) {
        ctl.abort()
        abortedAt = Date.now() - sent
        break
      }
      await sleep(25)
    }
  }
  const got = await answer
  return { S, got, abortedAt }
}

try {
  if (!existsSync(SHIM)) throw new Error(`no built overlay shim at ${SHIM} (the tier's overlays build it first)`)
  await new Promise((ok, no) => standin.listen(STANDIN, '127.0.0.1', ok).on('error', no))
  writeConfig()
  startWorker()
  if (!(await up(OVERLAY + '/listTopicManagers'))) throw new Error('the lane-936 overlay never answered')
  const tapLog = LOG.replace(/\.log$/, '') + '-tap.log'
  let tapped = false
  for (let i = 0; i < 60 && !tapped; i++) {
    tapped = existsSync(tapLog) && /TAP ATTACHED/.test(readFileSync(tapLog, 'utf8'))
    if (!tapped) await sleep(1_000)
  }
  expect(tapped, 'the network tap attached (the egress guard reads every subrequest in the worker log)', tapLog)
  const c0 = await counter() // boots the migrations
  expect(Number.isFinite(c0), `the counter ${COUNTER} is served`, String(c0))
  const units = plantSpin(calibrate())
  lines.push(`INFO  spin units: ${JSON.stringify(units)} (target ${JSON.stringify(SPIN_MS)} ms)`)
  FILE = d1File()
  if (!FILE) throw new Error('the D1 file was not found under the persist dir')

  // A. the abort mid-write
  const a = await present({ evict: false, abortMidWrite: true })
  lines.push(`INFO  leg A: ${a.S} aborted at ${a.abortedAt} ms; the client saw ${a.got.status} ${a.got.text.slice(0, 120)}`)
  expect(a.abortedAt !== null && a.got.status === 0, 'leg A: the client aborted mid-write (after the coin, before the answer)', JSON.stringify(a))
  const lineA = await logLine(new RegExp(`${a.S} (request-gone|in-request|past-deadline) write (landed|answered an error)`))
  if (lineA && / in-request /.test(lineA)) {
    expect(false, "leg A: the runtime delivered the client's abort to Request.signal", `the task's line says in-request: ${lineA} (wrangler dev's proxy did not cancel the runtime's request, or the flag is not in force)`)
  }
  expect(lineA && / request-gone write landed/.test(lineA), "leg A (b): the task's own after-write line, request-gone", String(lineA))
  const rA = rows(a.S)
  expect(rA && rA.outputs >= 1 && rA.transactions === 1 && rA.pot_records >= 1 && rA.pot_beefs === 1 && rA.applied === 1, 'leg A (a): the write whole: outputs, transactions, pot_records, pot_beefs, the applied row', JSON.stringify(rA))
  const cA = await counter()
  expect(cA - c0 === 1, `leg A (b): ${COUNTER} +1`, `${c0} -> ${cA}`)
  expect(jobs(a.S) === 1, 'leg A (d): the deferred job at rest, put on the abort', `jobs ${jobs(a.S)}`)
  expect(
    new RegExp(`${a.S}'s request went away during its engine write; the write goes on in its own task, the submission deferred as [0-9a-f]{64}`).test(logText()),
    "leg A (d): the cover's line",
    'no deferral line for the subject',
  )

  // B. the abort mid-write, an eviction opened during the write
  const b = await present({ evict: true, abortMidWrite: true })
  lines.push(`INFO  leg B: ${b.S} aborted at ${b.abortedAt} ms; the client saw ${b.got.status}`)
  expect(b.abortedAt !== null && b.got.status === 0, 'leg B: the client aborted mid-write', JSON.stringify(b))
  const lineB = await logLine(new RegExp(`${b.S} request-gone write outran its eviction \\(evicted at \\d+ ms: [^)]*\\): re-evicted \\d+ row`))
  expect(lineB !== null, "leg B (c): the task's guard re-evicted the write that outran its eviction", String(lineB))
  const rB = rows(b.S)
  const tB = twins(b.S)
  expect(rB && rB.outputs === 0 && rB.pot_records === 0 && rB.applied === 0 && tB >= 2, 'leg B (c): no live rows, the rows in the twins', JSON.stringify({ rows: rB, twins: tB }))
  const cB = await counter()
  expect(cB - cA === 1, `leg B: ${COUNTER} +1`, `${cA} -> ${cB}`)

  // C. the control: no abort
  const c = await present({ evict: false, abortMidWrite: false })
  lines.push(`INFO  leg C: ${c.S} answered ${c.got.status} ${c.got.text.slice(0, 120)}`)
  expect(c.got.status === 200, 'leg C: the gated JOIN answered 200 in the request', `${c.got.status} ${c.got.text.slice(0, 300)}`)
  const lineC = await logLine(new RegExp(`${c.S} (request-gone|in-request|past-deadline) write landed`))
  expect(lineC && / in-request write landed/.test(lineC), "leg C: the task's line says in-request", String(lineC))
  const rC = rows(c.S)
  expect(rC && rC.pot_records >= 1 && rC.pot_beefs === 1 && rC.applied === 1 && jobs(c.S) === 0, 'leg C: the write whole, no job', JSON.stringify({ rows: rC, jobs: jobs(c.S) }))
  const cC = await counter()
  expect(cC === cB, `leg C: ${COUNTER} unchanged`, `${cB} -> ${cC}`)
} catch (e) {
  expect(false, 'the cell ran', e.stack ?? String(e))
} finally {
  stopAll()
  standin.close()
  rmSync(scratch, { recursive: true, force: true })
}

lines.push(`INFO  stand-in asked: ${JSON.stringify(asked.slice(0, 40))}`)
console.log(lines.join('\n'))
if (failures) {
  console.log(`✗ lane-936 request gone: ${failures} failure(s)`)
  process.exit(1)
}
console.log('✓ lane-936 request gone: every expectation held')
