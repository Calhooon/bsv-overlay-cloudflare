import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
const base = process.argv[2]
const ceilings = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url))).scenarios
const inspector = new WebSocket(`ws://127.0.0.1:${process.argv[4]}/ws`, { headers: { Origin: 'http://localhost' } })
const requests = new Map()
const replies = new Map()
let serial = 0
let observed = null
inspector.addEventListener('message', event => {
  const message = JSON.parse(String(event.data))
  if (message.id) {
    const reply = replies.get(message.id)
    if (!reply) return // The proxy also sends replies to its own setup commands.
    replies.delete(message.id)
    if (message.error) reply.reject(new Error(JSON.stringify(message.error)))
    else reply.resolve(message.result)
    return
  }
  if (!observed) return
  const params = message.params
  if (message.method === 'Network.requestWillBeSent') {
    const url = new URL(params.request.url)
    requests.set(params.requestId, url)
    if (url.hostname !== 'd1') observed.courierCalls++
  }
  if (message.method === 'Network.loadingFinished') {
    const url = requests.get(params.requestId)
    requests.delete(params.requestId)
    if (url?.hostname !== 'd1') return
    const response = params.cfResponse
    const body = response.base64Encoded ? Buffer.from(response.body, 'base64').toString() : response.body
    const results = JSON.parse(body)
    for (const result of Array.isArray(results) ? results : [results]) {
      assert.equal(result.success, true, 'D1 statement succeeded')
      assert.ok(result.meta, 'D1 statement metadata present')
      observed.statements.push(result.meta)
    }
  }
})
const command = (method, params = {}) => new Promise((resolve, reject) => {
  const id = ++serial
  const timer = setTimeout(() => { replies.delete(id); reject(new Error(`inspector timeout: ${method}`)) }, 10_000)
  replies.set(id, {
    resolve: result => { clearTimeout(timer); resolve(result) },
    reject: error => { clearTimeout(timer); reject(error) },
  })
  inspector.send(JSON.stringify({ id, method, params }))
})
const barrier = () => command('Runtime.enable')
await new Promise((resolve, reject) => {
  inspector.addEventListener('open', resolve, { once: true })
  inspector.addEventListener('error', reject, { once: true })
})
await command('Network.enable')
try {
  const get = async identity => {
    await barrier()
    requests.clear()
    observed = { statements: [], courierCalls: 0 }
    const response = await fetch(`${base}/owed?identity=${identity}`)
    assert.equal(response.status, 200)
    const body = await response.json()
    const timing = response.headers.get('server-timing')
    const match = timing?.match(/d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)(?: maxReads=(\d+))?"/)
    assert.ok(match, timing)
    const [reads, writes, stmts, maxReads] = match.slice(1).map(Number)
    await barrier()
    assert.equal(observed.statements.length, stmts, 'inspector counts every D1 statement')
    assert.equal(observed.statements.reduce((sum, meta) => sum + meta.rows_read, 0), reads)
    assert.equal(observed.statements.reduce((sum, meta) => sum + meta.rows_written, 0), writes)
    const backgroundTasks = response.headers.get('x-owed-background-tasks')
    assert.notEqual(backgroundTasks, null, 'test wrapper counts background task registrations')
    const captured = observed
    observed = null
    return { body, statements: captured.statements, cost: { reads, writes, stmts, maxReads, courierCalls: captured.courierCalls, backgroundTasks: Number(backgroundTasks) } }
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
  // the belt's date (lane D2-OWED-BELT): a young in-progress hop at its marker + 30 min, every other row of the seed none
  for (const row of fleet.body.rows) {
    const young = row.family === 'in-progress' && typeof row.facts.hopTxid === 'string'
    assert.equal(row.facts.beltNextAtMs, young ? row.facts.markerCreatedAtMs + 1_800_000 : null, row.outpoint)
  }
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
  function owed_wanted_read_one_insert_at_most_5_physical_rows() {
    const statements = wanted.statements.filter(meta => meta.rows_written > 0)
    const cost = {
      stmts: statements.length,
      writes: statements.reduce((sum, meta) => sum + meta.rows_written, 0),
      courierCalls: wanted.cost.courierCalls,
      backgroundTasks: wanted.cost.backgroundTasks,
    }
    const ceiling = ceilings['owed-wanted-read'].ceiling
    assert.equal(cost.stmts, 1, 'exactly one wanted INSERT')
    assert.equal(cost.stmts, ceiling.stmts)
    assert.equal(cost.writes, wanted.cost.writes, 'all writes belong to the wanted INSERT')
    // one wanted mark: one INSERT, five physical rows as D1 counts them
    for (const [key, limit] of Object.entries(ceiling)) assert.ok(cost[key] <= limit, `${key}: ${cost[key]} > ${limit}`)
    console.log('owed-wanted-insert', JSON.stringify(cost))
    console.log('PASS owed-wanted-read: one INSERT, at most 5 physical rows, no courier calls or background tasks')
  }
  owed_wanted_read_one_insert_at_most_5_physical_rows()
  console.log('PASS owed reads: three statements, no work, one wanted mark only')
} finally {
  inspector.close()
}
// workerd can leave the inspector close handshake open after all assertions pass.
await new Promise(resolve => process.stdout.write('', resolve))
process.exit(0)
