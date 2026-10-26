import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
// The delta lens's D-H1: the tip handler (`owed-tip-mark`, `POST /internal/tip-changed`) over the belt tier's seed
// plus the lens's shape planted by `owed_belt_tick.sh`: 1,000 strangers' index-unspent claimable sweeps and 40
// courier-dependent ones, all judged at the seed's tip. A block marks the gate window, dates due now only the 40
// (an index-unspent sweep keeps its cadence), re-marks the tipless rows and judges at most 16 marked subjects; no
// courier is asked. Argument 4 is the count of rows the mark dated (read back by the shell).
const [base, standin, token, phase, dated] = process.argv.slice(2)
const ceilings = JSON.parse(readFileSync(new URL('../lane-499/ceilings.json', import.meta.url))).scenarios
if (phase === 'mark') {
  await fetch(`${standin}/__reset`)
  const r = await fetch(`${base}/internal/tip-changed`, { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify({ height: 900001 }) })
  assert.equal(r.status, 200, await r.clone().text())
  const m = r.headers.get('server-timing')?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
  assert.ok(m, r.headers.get('server-timing'))
  const [reads, writes, stmts, maxReads] = m.slice(1).map(Number)
  const calls = (await (await fetch(`${standin}/__calls`)).json()).length
  assert.equal(calls, 0, 'the tip handler asks no courier')
  const cost = { reads, writes, stmts, maxReads, calls }
  console.log('owed-tip-mark', JSON.stringify({ ...cost, report: await r.json() }))
  const c = ceilings['owed-tip-mark']?.ceiling
  if (!c) console.log('NO CEILING owed-tip-mark (measure first)')
  else for (const key of ['reads', 'writes', 'stmts', 'maxReads', 'calls']) assert.ok(cost[key] <= c[key], `owed-tip-mark ${key}: ${cost[key]} > ${c[key]}`)
} else {
  assert.equal(Number(dated), 40, `the block dates the 40 courier-dependent sweeps alone, not the strangers': ${dated}`)
  console.log('PASS owed tip mark: no courier asked, the 40 courier-dependent sweeps dated, the 1,000 index-unspent ones not')
}
