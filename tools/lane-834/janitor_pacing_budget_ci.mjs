#!/usr/bin/env node
/**
 * bsv-low #834 (FIX-834): the index janitor's COURIER PACING in the budget tier (`make ci-d1-budget`).
 *
 * Loop 44 (89.5 min, 33 pairs) measured the janitor's WoC at 36.6 percent faults and its Bitails at 37.5 percent
 * while the app layer's own reads of both answered: the janitor's asks fault, not the couriers. This cell runs the
 * janitor's pass (`POST /admin/complete-proofs`, the tick's own `run_pot_maintenance` on paced counters) on the
 * retire overlay of the #831 cell (`make ci-d1-budget`: LANE_BASE+16, its couriers sunk to the loopback stand-in
 * on LANE_BASE+18, run after the #831 cell has closed it), with a stand-in that answers WoC 429 and every other
 * courier 404, so nothing leaves the machine:
 *
 *  1. the fixture: 40 proofless pot BEEFs two hours old (past the 30 min push backstop, short of the retire floor).
 *  2. the first pass: WoC is asked exactly COURIER_BACKOFF_FAULTS (3) times, backs off, and is skipped for the rest
 *     of the pass; pruned Bitails' 404s are "could not look", never a fault, and skip it by construction after 3;
 *     Arcade keeps asking; the pass's courier subrequests are held to `janitor-pass-woc-429` (`ceilings.json`).
 *  3. the second pass, inside the cool-down: WoC is not asked at all (`janitor-pass-woc-cooled`).
 *  4. `/health/invariants.indexJanitor.courierPacing`: the back-off counted, Bitails' could-not-look beside its
 *     asks (outside them), the bounds served.
 * On `00a5e613` there is no pacing: WoC is asked on every candidate of every pass (40 a pass here) and Bitails'
 * 404s are faults (RED on both ceilings and on the served view, which does not exist there).
 *
 *   node tools/lane-834/janitor_pacing_budget_ci.mjs <retire overlay base> <its --persist-to dir> <stand-in port>
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
const ADMIN = process.env.D1_BUDGET_ADMIN_TOKEN ?? 'ci-admin-tok'
const MEASURE = process.env.D1_BUDGET_MEASURE === '1'
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
// The pruned-Bitails questions the pass can ask on this overlay (FIX-834-MERGE): the proof ladder's and the conflict
// pass's per-output spend.
const BITAILS_QUESTIONS = 2
const CEILINGS = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url), 'utf8'))
if (!OVERLAY || !STATE || !SINK_PORT) {
  console.error('usage: janitor_pacing_budget_ci.mjs <retire overlay base> <its --persist-to dir> <stand-in port>')
  process.exit(2)
}

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}

// The provider a stand-in path belongs to: the sink keeps each courier URL's own path (`courier_target`), and the
// overlay's ARCADE_URL carries an `/arcade` prefix here so Arcade's `/tx/{txid}` is told from Bitails'.
const providerOf = (url) =>
  url.startsWith('/v1/bsv/main/') ? 'woc' : url.startsWith('/arcade/') ? 'arcade' : url.startsWith('/api/v1/') ? 'bananablocks' : 'bitails'
let asked = []
const sink = createServer((req, res) => {
  const provider = providerOf(req.url)
  asked.push(provider)
  if (provider === 'woc') {
    res.writeHead(429, { 'content-type': 'text/plain' })
    return res.end('Too Many Requests')
  }
  res.writeHead(404, { 'content-type': 'text/plain' })
  res.end('not found')
})
await new Promise((ok, no) => sink.listen(SINK_PORT, '127.0.0.1', ok).on('error', no))
const tally = () => asked.reduce((t, p) => ({ ...t, [p]: (t[p] ?? 0) + 1 }), { arcade: 0, bitails: 0, woc: 0, bananablocks: 0 })

const scratch = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), 'lane834-'))
function d1(sql) {
  const path = join(scratch, `seed-${Date.now()}.sql`)
  writeFileSync(path, sql)
  const args = ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json', '--file', path]
  for (let attempt = 1; ; attempt++) {
    try {
      const out = execFileSync('npx', args, { cwd: CRATE, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
      return JSON.parse(out)[0]?.results ?? []
    } catch (e) {
      if (attempt >= 4) throw new Error(`wrangler d1 execute failed: ${e.stderr ?? e.message}`)
    }
  }
}

async function pass() {
  asked = []
  const r = await fetch(OVERLAY + '/admin/complete-proofs', {
    method: 'POST',
    headers: { authorization: `Bearer ${ADMIN}`, Connection: 'close' },
  })
  const text = await r.text()
  let json = null
  try {
    json = JSON.parse(text)
  } catch {}
  return { status: r.status, json, text, asked: tally(), total: asked.length }
}

const measured = {}
function check(name, got) {
  const c = CEILINGS.scenarios[name]
  measured[name] = got
  if (MEASURE) return lines.push(`MEASURE ${name}: ${JSON.stringify(got)}`)
  if (!c) return expect(false, `${name}: has a ceiling`, `no entry in ceilings.json (measured ${JSON.stringify(got)})`)
  const over = Object.keys(c.ceiling).filter((k) => (got[k] ?? 0) > c.ceiling[k])
  expect(over.length === 0, `${name}: ${JSON.stringify(got)} under ${JSON.stringify(c.ceiling)}`, `over on ${over.join(', ')} (${c.route})`)
}

try {
  // 1. The fixture.
  const at = Math.floor(Date.now() / 1000) - 7200
  const pots = Array.from({ length: 40 }, () => randomBytes(32).toString('hex'))
  d1(pots.map((t) => `INSERT INTO pot_beefs (txid, beef, createdAt, has_proof) VALUES ('${t}', X'00', ${at}, 0);`).join('\n'))

  // 2. The first pass: WoC answers 429.
  const p1 = await pass()
  const v1 = p1.json?.courier_pacing ?? {}
  lines.push(`INFO  pass 1: ${JSON.stringify({ status: p1.status, asked: p1.asked, total: p1.total, pacing: v1 })}`)
  expect(p1.status === 200, 'the janitor pass answers 200', `${p1.status} ${p1.text.slice(0, 300)}`)
  expect(p1.asked.woc === 3, 'WoC is asked exactly 3 times (its third 429 backs it off)', JSON.stringify(p1.asked))
  expect(v1.providers?.woc?.backedOff === true && v1.providers?.woc?.skipped > 0, 'WoC is backed off and SKIPPED for the rest of the pass', JSON.stringify(v1.providers?.woc))
  // FIX-834-MERGE: the cap is per QUESTION (rung), not per provider, so Bitails stays every other ladder's fallback. On
  // this overlay (the #837 cell's JOINs before this cell) the pass asks pruned Bitails two questions: the proof
  // ladder's (`bitails`, its proofless pots) and the retire pass's conflict pass's per-output spend (`bitails_tx`,
  // the JOINs' inputs), each skipped by construction after its 3rd non-answer.
  expect(p1.asked.bitails <= 3 * BITAILS_QUESTIONS && v1.providers?.bitails?.couldNotLook === p1.asked.bitails,
    "pruned Bitails' 404s are could-not-look, skipped by construction after 3 per question", JSON.stringify(v1.providers?.bitails))
  expect(p1.asked.arcade >= 37, 'Arcade (the next rung) keeps asking', JSON.stringify(p1.asked))
  // FIX-834-MERGE: EVERY courier GET of the pass goes through its one pacer (on 45b83a32 the retire pass's conflict
  // pass asked past it: stand-in 70, paced 46).
  expect(Number.isInteger(v1.asks) && v1.asks > 0 && v1.asks === p1.total,
    "every subrequest the stand-in saw is one the pass's pacer let go (no unpaced caller)", `${v1.asks} vs ${p1.total}`)
  check('janitor-pass-woc-429', { total: p1.total, paced: v1.asks ?? null, ...p1.asked })

  // 3. The second pass inside the cool-down.
  const p2 = await pass()
  const v2 = p2.json?.courier_pacing ?? {}
  lines.push(`INFO  pass 2: ${JSON.stringify({ status: p2.status, asked: p2.asked, total: p2.total, woc: v2.providers?.woc })}`)
  expect(p2.status === 200 && p2.asked.woc === 0, 'inside the cool-down WoC is not asked at all', JSON.stringify(p2.asked))
  expect(v2.asks === p2.total, 'the cooled pass makes no unpaced subrequest either', `${v2.asks} vs ${p2.total}`)
  expect(Number.isFinite(v2.providers?.woc?.cooledUntilMs), 'the pass names the cool-down it started under', JSON.stringify(v2.providers?.woc))
  check('janitor-pass-woc-cooled', { total: p2.total, paced: v2.asks ?? null, ...p2.asked })

  // 4. The served view (the counters ride the answer's wait_until: poll briefly).
  let view = null
  for (let i = 0; i < 20; i++) {
    const h = await (await fetch(OVERLAY + '/health/invariants', { headers: { Connection: 'close' } })).json()
    view = h?.indexJanitor?.courierPacing
    if (view?.providers?.woc?.backoffs >= 1 && view?.providers?.bitails?.couldNotLook >= 1) break
    await new Promise((ok) => setTimeout(ok, 250))
  }
  lines.push(`INFO  served: ${JSON.stringify(view)}`)
  expect(view?.providers?.woc?.backoffs >= 1, 'the back-off is counted on /health/invariants', JSON.stringify(view?.providers?.woc))
  expect(view?.providers?.bitails?.couldNotLook >= 1 && view?.providers?.bitails?.faults === 0,
    "Bitails' could-not-look is served beside its asks, and is no fault", JSON.stringify(view?.providers?.bitails))
  const w = view?.providers?.woc ?? {}
  expect(w.asks > 0 && Math.abs(w.faultRate - w.faults / w.asks) < 1e-9, 'the served rate is faults over the asks the rung could answer', JSON.stringify(w))
  expect(view?.backoffAfterFaults === 3 && view?.cooldownSecs === 1200 && view?.budgetPerTick?.woc === 64,
    'the pacing bounds are served (3 faults, 1200 s, 64 a tick)', JSON.stringify(view))
} catch (e) {
  expect(false, 'the cell ran', e.stack ?? String(e))
} finally {
  sink.close()
  rmSync(scratch, { recursive: true, force: true })
}

console.log(lines.join('\n'))
if (MEASURE) console.log(`MEASURED ${JSON.stringify(measured)}`)
if (failures) {
  console.log(`✗ janitor pacing: ${failures} failure(s)`)
  process.exit(1)
}
console.log(MEASURE ? '✓ janitor pacing: measured (nothing enforced)' : '✓ janitor pacing: every expectation held')
