import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
const base = process.argv[2]
const ceilings = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url))).scenarios
const get = async identity => {
  const response = await fetch(`${base}/owed?identity=${identity}`)
  assert.equal(response.status, 200)
  const body = await response.json()
  const timing = response.headers.get('server-timing')
  const match = timing?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
  assert.ok(match, timing)
  const [reads, writes, stmts, maxReads] = match.slice(1).map(Number)
  return { body, cost: { reads, writes, stmts, maxReads } }
}
const health = async () => (await (await fetch(`${base}/health`)).json()).owed
const before = await health()
const fleet = await get(`02${'ee'.repeat(32)}`)
console.log('owed-fleet-read', JSON.stringify(fleet.cost))
const wanted = await get(`02${'ab'.repeat(32)}`)
console.log('owed-wanted-read', JSON.stringify(wanted.cost))
const empty = await get(`02${'ef'.repeat(32)}`)
console.log('owed-empty-read', JSON.stringify(empty.cost))
assert.equal(fleet.body.pending, 0)
assert.equal(fleet.body.rows.length, 500)
assert.equal(fleet.body.markerlessProgress, null)
assert.ok(Number.isInteger(fleet.body.recomputeSeq) && fleet.body.recomputeSeq > 0)
assert.ok(fleet.body.rows.every(row => row.facts.beltNextAtMs === null))
assert.equal(fleet.cost.stmts, 3)
for (const [key, limit] of Object.entries(ceilings['owed-fleet-read'].ceiling)) assert.ok(fleet.cost[key] <= limit, `${key}: ${fleet.cost[key]} > ${limit}`)
assert.deepEqual(wanted.body.rows, [])
assert.equal(wanted.body.pending, true)
assert.equal(wanted.body.retryAfterMs, 1500)
assert.equal(wanted.cost.stmts, 5)
assert.deepEqual(empty.body.rows, [])
assert.equal(empty.body.pending, 0)
assert.equal(empty.cost.writes, 0)
const after = await health()
assert.deepEqual(after, before, 'reads schedule no walk and do not change its counters')
const probe = process.argv[3]
const source = readFileSync(new URL('../../crates/low-app-layer/src/owed_read.rs', import.meta.url), 'utf8')
const names = ['ROWS_SQL', 'STATE_SQL', 'PENDING_SQL']
const statements = names.map(name => source.match(new RegExp(`pub const ${name}: &str =\\s*"([^"]+)"`))[1])
const metas = await (await fetch(probe, { method: 'POST', body: JSON.stringify({ statements, identity: `02${'ee'.repeat(32)}` }) })).json()
metas.forEach((result, index) => console.log(names[index], JSON.stringify(result.meta)))
const wantedCheck = await (await fetch(probe, { method: 'POST', body: JSON.stringify({
  statements: ["SELECT (SELECT COUNT(*) FROM owed_dirty WHERE identity=?1 AND source='wanted' AND gameId='') AS marks,(SELECT COUNT(*) FROM owed_state WHERE identity=?1) AS states,(SELECT COUNT(*) FROM owed_rows WHERE identity=?1) AS rows"],
  identity: `02${'ab'.repeat(32)}`,
}) })).json()
assert.deepEqual(wantedCheck[0].rows, [{ marks: 1, states: 0, rows: 0 }])
console.log('wanted application rows', JSON.stringify(wantedCheck[0].rows[0]))
assert.equal(wanted.cost.writes, 1, 'wanted physical-write ceiling')
console.log('PASS owed reads: three statements, no work, one wanted mark only')
