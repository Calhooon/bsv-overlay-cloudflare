// bsv-low #986 (lane FIX-986-COMPARE): the fleet compare's ring step on local D1 (the budget tier's
// `owed-compare-ring` cell), inside the owed belt tier's dev session (the READ lane's fleet seed, the lived-in identity
// of 270 games and its opponents). Each step judges every game of up to 16 identities FRESH (reads only) and writes
// one row and the cursor per identity; this worker binds no OwedActor, so each row names the actor's error (the cost
// measured is the D1 side: the fresh judgement and the writes). Then the summary route and the hand lever.
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
const [base, token] = process.argv.slice(2)
const ceilings = JSON.parse(readFileSync(new URL('../lane-499/ceilings.json', import.meta.url))).scenarios
const auth = { authorization: `Bearer ${token}`, 'content-type': 'application/json' }
const cost = response => {
  const timing = response.headers.get('server-timing')
  const match = timing?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
  assert.ok(match, timing)
  const [reads, writes, stmts, maxReads] = match.slice(1).map(Number)
  return { reads, writes, stmts, maxReads }
}
assert.equal((await fetch(`${base}/internal/owed-actor-compare`)).status, 401, 'the summary is bearer-gated')
assert.equal((await fetch(`${base}/internal/owed-actor-compare`, { method: 'POST', body: '{"ring":true}' })).status, 401, 'the levers are bearer-gated')
const step = async () => {
  const started = Date.now()
  const response = await fetch(`${base}/internal/owed-actor-compare`, { method: 'POST', headers: auth, body: '{"ring":true}' })
  assert.equal(response.status, 200, await response.clone().text())
  return { report: await response.json(), cost: cost(response), wallMs: Date.now() - started }
}
const runs = []
for (let n = 0; n < 40; n++) {
  const run = await step()
  runs.push(run)
  assert.ok(!run.report.ungated, 'the certified count is 474')
  console.log('owed-compare-ring', JSON.stringify({ ...run.cost, wallMs: run.wallMs, compared: run.report.compared.length, games: run.report.games, wrapped: run.report.wrapped, stoppedEarly: run.report.stoppedEarly }))
  if (run.report.wrapped) break
}
assert.ok(runs.at(-1).report.wrapped, 'the ring wraps')
const compared = runs.flatMap(r => r.report.compared)
assert.equal(new Set(compared).size, compared.length, 'each identity once a revolution')
const summaryResponse = await fetch(`${base}/internal/owed-actor-compare`, { headers: auth })
assert.equal(summaryResponse.status, 200)
const summary = await summaryResponse.json()
console.log('owed-compare-summary', JSON.stringify({ ...cost(summaryResponse), identities: summary.identities, errors: summary.errors, rows: summary.rows.length }))
for (const k of ['identities', 'comparedWithinMs', 'diffs', 'heldByLag', 'errors', 'ringCursor', 'revolution', 'lastRevolutionMs', 'lastTickAt', 'rows', 'next']) assert.ok(k in summary, k)
assert.equal(summary.identities, compared.length)
assert.equal(summary.revolution, 1)
const whale = runs.reduce((a, r) => (r.report.games > a.report.games ? r : a))
const worst = key => Math.max(...runs.map(r => r.cost[key]))
const measured = { reads: worst('reads'), writes: worst('writes'), stmts: worst('stmts'), maxReads: worst('maxReads'), wallMs: Math.max(...runs.map(r => r.wallMs)) }
console.log('owed-compare-ring worst step', JSON.stringify({ ...measured, whaleGames: whale.report.games }))
const c = ceilings['owed-compare-ring']?.ceiling
if (!c) console.log('NO CEILING owed-compare-ring (measure first)')
else for (const key of ['reads', 'writes', 'stmts', 'maxReads']) assert.ok(measured[key] <= c[key], `owed-compare-ring ${key}: ${measured[key]} > ${c[key]}`)
console.log('PASS owed compare ring: every identity once, a row each, the summary served, under its ceilings')
