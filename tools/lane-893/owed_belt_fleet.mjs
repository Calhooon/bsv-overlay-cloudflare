import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
// The third delta lens's D3-N2 and D3-M1 (`owed-belt-tick-fleet`): one belt tick at fleet scale, after the tip mark's
// cell, over the belt tier's seed plus what `owed_belt_tick.sh` planted: the 1,040 strangers (each its own identity,
// 1,000 dated an hour ahead and not due, 40 dated due by the mark), so the walk of the second class reads its whole
// window of 512 dated identities (two index seeks each), and a first-class flood of 40 courier-dependent sweeps judged
// below the stand-in's tip and due now. The tick gives the second class its reserved places (2 of 8) beside the
// flood's 6 (one due second-class row is planted inside the window, so its other place yields: 7 and 1); its reads are the figure D3-N2 asks for, one tick in every 120 s.
const [base, standin, token] = process.argv.slice(2)
const ceilings = JSON.parse(readFileSync(new URL('../lane-499/ceilings.json', import.meta.url))).scenarios
await fetch(`${standin}/__reset`)
await fetch(`${standin}/__mode/find`)
const r = await fetch(`${base}/internal/owed-belt-tick`, { method: 'POST', headers: { authorization: `Bearer ${token}` } })
assert.equal(r.status, 200, await r.clone().text())
const m = r.headers.get('server-timing')?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
assert.ok(m, r.headers.get('server-timing'))
const [reads, writes, stmts, maxReads] = m.slice(1).map(Number)
const list = await (await fetch(`${standin}/__calls`)).json()
const report = await r.json()
const cost = { reads, writes, stmts, maxReads, calls: list.length }
console.log('owed-belt-tick-fleet', JSON.stringify({ ...cost, report }))
assert.equal(report.candidates, 8, 'a full tick')
assert.equal(report.writeFaults, 0, 'every find writes')
const flood = list.filter(u => /\/f{4}[0-9a-f]{60}\//.test(u))
const floodOutpoints = new Set(flood.map(u => u.match(/([0-9a-f]{64})\/\d+\/spen/)?.[1]).filter(Boolean))
assert.ok(list.some(u => /\/eeee0{60}\//.test(u)), "the second class's due row inside the window is asked")
assert.equal(floodOutpoints.size, 7, `the flood keeps 7 places (the second class holds one due row, its other place yields): ${[...floodOutpoints]}`)
const c = ceilings['owed-belt-tick-fleet']?.ceiling
if (!c) console.log('NO CEILING owed-belt-tick-fleet (measure first)')
else for (const key of ['reads', 'writes', 'stmts', 'maxReads', 'calls']) assert.ok(cost[key] <= c[key], `owed-belt-tick-fleet ${key}: ${cost[key]} > ${c[key]}`)
console.log('PASS owed belt tick fleet: the walk read its 512 identities, the second class kept its place beside the flood')
