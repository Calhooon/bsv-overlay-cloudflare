import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
const [base, standin, token] = process.argv.slice(2)
const ceilings = JSON.parse(readFileSync(new URL('../lane-499/ceilings.json', import.meta.url))).scenarios
const pass = async () => {
  const response = await fetch(`${base}/internal/owed-backfill-pass`, { method: 'POST', headers: { authorization: `Bearer ${token}` } })
  assert.equal(response.status, 200, await response.clone().text())
  const timing = response.headers.get('server-timing')
  const match = timing?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
  assert.ok(match, timing)
  const [reads, writes, stmts, maxReads] = match.slice(1).map(Number)
  return { report: await response.json(), cost: { reads, writes, stmts, maxReads } }
}
const refused = await fetch(`${base}/internal/owed-backfill-pass`, { method: 'POST' })
assert.equal(refused.status, 401, 'the lever is bearer-gated')
const armed = await pass()
console.log('owed-backfill-arm', JSON.stringify(armed))
assert.equal(armed.report.stop, 'Armed')
assert.equal(armed.report.identities, 0)
// The backfill: two identities (the lived-in seat, 270 games, and the one-game wanted identity). A pass the wall cuts
// resumes; the cost is summed over the passes it took.
const work = { reads: 0, writes: 0, stmts: 0, maxReads: 0 }
let identities = 0, rows = 0, batches = 0, passes = 0, last
do {
  last = await pass()
  passes += 1
  console.log('owed-backfill-pass', JSON.stringify(last))
  for (const k of ['reads', 'writes', 'stmts']) work[k] += last.cost[k]
  work.maxReads = Math.max(work.maxReads, last.cost.maxReads ?? 0)
  identities += last.report.identities; rows += last.report.rows; batches += last.report.batches
} while (last.report.stop === 'Wall' && passes < 8)
assert.equal(last.report.stop, 'Finished')
assert.equal(identities, 2)
assert.equal(batches, 18, '270 games in 17 batches of 16, one game in one')
assert.ok(rows >= 500, `rows ${rows}: the seat at its 500-row ceiling and the wanted identity`)
const perBatch = Math.round(work.reads / batches)
console.log('owed-backfill-pass-total', JSON.stringify({ ...work, passes, identities, rows, batches, readsPerBatch: perBatch }))
const stamped = await pass()
console.log('owed-backfill-stamped', JSON.stringify(stamped))
assert.equal(stamped.report.stop, 'Stamped')
assert.equal(stamped.cost.stmts, 1, 'a stamped replay is one statement')
assert.equal(stamped.cost.reads, 1, 'and one row read')
assert.equal(stamped.cost.writes, 0)
const calls = await (await fetch(`${standin}/__calls`)).json()
assert.equal(calls.length, 0, 'the backfill asks no courier')
for (const [name, run] of [['owed-backfill-pass', { cost: work }], ['owed-backfill-stamped', stamped]]) {
  const c = ceilings[name]?.ceiling
  if (!c) { console.log(`NO CEILING ${name} (measure first)`); continue }
  for (const key of ['reads', 'writes', 'stmts', 'maxReads']) assert.ok(run.cost[key] <= c[key], `${name} ${key}: ${run.cost[key]} > ${c[key]}`)
}
console.log('PASS owed backfill: armed one tick, every game judged 16 a batch, stamped, then one read')
