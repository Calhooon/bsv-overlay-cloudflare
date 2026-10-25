import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
const [base, standin, token] = process.argv.slice(2)
const ceilings = JSON.parse(readFileSync(new URL('../lane-499/ceilings.json', import.meta.url))).scenarios
const tick = async () => {
  const response = await fetch(`${base}/internal/owed-belt-tick`, { method: 'POST', headers: { authorization: `Bearer ${token}` } })
  assert.equal(response.status, 200, await response.clone().text())
  const timing = response.headers.get('server-timing')
  const match = timing?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
  assert.ok(match, timing)
  const [reads, writes, stmts, maxReads] = match.slice(1).map(Number)
  return { report: await response.json(), cost: { reads, writes, stmts, maxReads } }
}
const calls = async () => (await fetch(`${standin}/__calls`)).json()
const outpoints = list => new Set(list.map(u => u.match(/([0-9a-f]{64})\/(\d+)\/spen/)).filter(Boolean).map(m => `${m[1]}.${m[2]}`))
const refused = await fetch(`${base}/internal/owed-belt-tick`, { method: 'POST' })
assert.equal(refused.status, 401, 'the lever is bearer-gated')
const first = await tick()
const firstCalls = await calls()
console.log('owed-belt-tick', JSON.stringify({ ...first.cost, calls: firstCalls.length, report: first.report }))
assert.equal(first.report.candidates, 8)
assert.ok(first.report.asked <= 8)
assert.ok(firstCalls.length <= 24, `calls ${firstCalls.length}`)
assert.equal(first.report.calls, firstCalls.length, 'every call the gate let through reached the stand-in')
assert.equal(first.report.writeFaults, 0, 'every find writes')
assert.equal(first.report.found, first.report.asked, 'a corroborated unspent is a find')
await fetch(`${standin}/__reset`)
const second = await tick()
const secondCalls = await calls()
console.log('owed-belt-tick-next', JSON.stringify({ ...second.cost, calls: secondCalls.length, report: second.report }))
assert.ok(secondCalls.length <= 24)
const a = outpoints(firstCalls), b = outpoints(secondCalls)
assert.ok(a.size > 0 && b.size > 0)
assert.equal([...a].filter(x => b.has(x)).length, 0, 'the next tick asks the next rows by due time')
await fetch(`${standin}/__reset`)
await fetch(`${standin}/__mode/429`)
const limited = await tick()
const limitedCalls = await calls()
console.log('owed-belt-tick-429', JSON.stringify({ ...limited.cost, calls: limitedCalls.length, report: limited.report }))
assert.equal(limited.report.found, 0)
assert.equal(limited.report.rearmed, limited.report.candidates)
assert.ok(limitedCalls.length <= 4, `a 429 ends each rung's turn: ${limitedCalls.length} calls`)
for (const [name, run, n] of [['owed-belt-tick', first, firstCalls.length], ['owed-belt-tick', second, secondCalls.length], ['owed-belt-tick-429', limited, limitedCalls.length]]) {
  const c = ceilings[name]?.ceiling
  if (!c) { console.log(`NO CEILING ${name} (measure first)`); continue }
  for (const key of ['reads', 'writes', 'stmts', 'maxReads']) assert.ok(run.cost[key] <= c[key], `${name} ${key}: ${run.cost[key]} > ${c[key]}`)
  assert.ok(n <= c.calls, `${name} calls: ${n} > ${c.calls}`)
}
console.log('PASS owed belt tick: eight outpoints, at most 24 calls, the next tick the next rows, a 429 re-arms')
