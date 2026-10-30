// FIX-986-COMPARE-2 (bsv-low #986, READ-986 items 1 and 4): the standing fresh compare on local D1, after the belt's
// cells. `POST /internal/owed-actor-compare {"tick": true}` runs one tick of the ring (16 identities a tick, under its
// statement budget: each identity a FRESH D1 judgement of its games, nothing written but the compare row); `{"identity"}`
// re-compares the lived-in seat alone; `GET` serves the rows. The tier binds no owed actor, so the ring and the POST record
// each identity's fault ("no owed actor bound") and make no judgement; the fresh judgement's D1 cost, what each ring
// identity pays in production, is measured on the operator's route over the lived-in seat
// (`GET /internal/owed-actor-rows`, `owed-actor-rows-fresh`: 64 games at most, `partial` past them).
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
const [base, token, seat] = process.argv.slice(2)
const ceilings = JSON.parse(readFileSync(new URL('../lane-499/ceilings.json', import.meta.url))).scenarios
const call = async (method, body) => {
  const response = await fetch(`${base}/internal/owed-actor-compare`, {
    method,
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  })
  assert.equal(response.status, 200, await response.clone().text())
  const timing = response.headers.get('server-timing')
  const match = timing?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
  assert.ok(match, timing)
  const [reads, writes, stmts, maxReads] = match.slice(1).map(Number)
  return { body: await response.json(), cost: { reads, writes, stmts, maxReads } }
}
for (const method of ['GET', 'POST']) {
  const refused = await fetch(`${base}/internal/owed-actor-compare`, { method, body: method === 'POST' ? '{}' : undefined })
  assert.equal(refused.status, 401, `${method}: the lever is bearer-gated`)
}
// FIX-998 (bsv-low #998): the ring runs in shards, each its own actor's tick (its own invocation); one tick of each
// shard, the cell's cost the heaviest shard's tick.
const first = await call('POST', { tick: true, shard: 0 })
const ticks = [first]
for (let shard = 1; shard < (first.body.shards ?? 1); shard++) ticks.push(await call('POST', { tick: true, shard }))
const tick = ticks.reduce((a, b) => (b.cost.reads > a.cost.reads ? b : a))
console.log('owed-fleet-compare-tick', JSON.stringify({ ...tick.cost, shards: ticks.map((t) => t.body) }))
assert.ok(ticks.reduce((n, t) => n + t.body.compared, 0) >= 1, 'the ring compared')
for (const t of ticks) assert.ok(t.body.statements <= 800, `the tick's own budget: ${t.body.statements}`)
const one = await call('POST', { identity: seat })
console.log('owed-fleet-compare-one', JSON.stringify({ ...one.cost, partial: one.body.partial, fault: one.body.fault }))
assert.equal(one.body.identity, seat)
const rowsResponse = await fetch(`${base}/internal/owed-actor-rows?identity=${seat}`, { headers: { authorization: `Bearer ${token}` } })
assert.equal(rowsResponse.status, 200, await rowsResponse.clone().text())
const rowsTiming = rowsResponse.headers.get('server-timing').match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
const [rr, rw, rs, rm] = rowsTiming.slice(1).map(Number)
const rowsBody = await rowsResponse.json()
const fresh = { body: rowsBody, cost: { reads: rr, writes: rw, stmts: rs, maxReads: rm } }
console.log('owed-actor-rows-fresh', JSON.stringify({ ...fresh.cost, games: rowsBody.d1.games, rows: rowsBody.d1.rows.length, stored: rowsBody.d1Stored.rows.length, staleOutpoints: rowsBody.d1Stored.staleOutpoints.length }))
assert.equal(rowsBody.d1.fresh, true, 'the route judges D1 fresh')
assert.equal(fresh.cost.writes, 0, 'the fresh judgement writes nothing')
assert.ok(fresh.cost.stmts + 80 <= 800, `one identity fits the ring's tick: ${fresh.cost.stmts}`)
const read = await call('GET')
console.log('owed-fleet-compare-read', JSON.stringify({ ...read.cost, identities: read.body.identities, faultCount: read.body.faultCount, lastTickAt: read.body.lastTickAt }))
assert.ok(read.body.identities >= 1)
assert.ok(Number.isInteger(read.body.lastTickAt))
for (const [name, run] of [['owed-fleet-compare-tick', tick], ['owed-fleet-compare-one', one], ['owed-actor-rows-fresh', fresh], ['owed-fleet-compare-read', read]]) {
  const c = ceilings[name]?.ceiling
  if (!c) { console.log(`NO CEILING ${name} (measure first)`); continue }
  for (const key of ['reads', 'writes', 'stmts', 'maxReads']) assert.ok(run.cost[key] <= c[key], `${name} ${key}: ${run.cost[key]} > ${c[key]}`)
}
console.log('PASS owed fleet compare: a ring tick under its budget, one identity re-compared, the rows served')
