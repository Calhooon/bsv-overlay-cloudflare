// #908: the production record doors, real workerd/D1, and a 10 s evidence-read fake.
// bsv-low #911 (D2-ACTOR-A): the door publishes its owed events to the `owed-events` queue and answers; the judge
// runs in the queue's consumer (the same Worker here, its producer and consumer bound as wrangler.toml binds them).
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { delimiter, dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
// Miniflare's dispose can close its loopback server twice (node 20 raises ERR_SERVER_NOT_RUNNING on the second
// close, after the first instance is gone): benign for this cell, which starts a second instance on the copied D1.
process.on('uncaughtException', e => { if (e?.code === 'ERR_SERVER_NOT_RUNNING') return; console.error(e); process.exit(1) })
const root = fileURLToPath(new URL('../../', import.meta.url))
const fixturePath = process.env.RECORD_DOOR_FIXTURE
assert(fixturePath, 'RECORD_DOOR_FIXTURE names the exported fleet fixture')
const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'))
const before = process.env.RECORD_DOOR_BEFORE === '1'
const build = process.env.NS2_APP_BUILD ?? resolve(root, 'crates/low-app-layer/build')
const wrangler = process.env.NS2_WRANGLER_PACKAGE ?? process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'use the tier’s pinned wrangler/Miniflare')
const { Miniflare, convertV4MiniflareOptions } = createRequire(wrangler)('miniflare')
const scratch = mkdtempSync(resolve(tmpdir(), 'record-door-'))
const state = resolve(scratch, 'state')
const paths = ['tools/lane-908/record_door_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm']
const options = {
  // `defaultPersistRoot` is the name a Miniflare without `convertV4MiniflareOptions` (4.86) reads; the older reads the other.
  name: 'record-door', resourcePersistencePath: state, defaultPersistRoot: state, modulesRoot: root,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', build) : root + path) })),
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'RecordBelt', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore' },
  d1Databases: { OVERLAY_DB: '00000000-0000-0000-0000-000000000908' },
  queueProducers: { OWED_EVENTS: { queueName: 'low-owed-events' } },
  queueConsumers: { 'low-owed-events': { maxBatchSize: 1, maxBatchTimeout: 0, maxRetries: 3, deadLetterQueue: 'low-owed-events-dlq' }, 'low-owed-events-dlq': { maxBatchSize: 10, maxRetries: 100 } },
  bindings: { AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: 'ci-internal-tok', COURIER_SINK: 'http://127.0.0.1:9' },
  serviceBindings: { CHAINTRACKS: async request => {
    const path = new URL(request.url).pathname
    if (path === '/getPresentHeight') return Response.json({ status: 'success', value: 900000 })
    if (path === '/findChainTipHeaderHex' || path === '/findHeaderHexForHeight') return Response.json({ status: 'success', value: { height: 900000, hash: 'ab'.repeat(32), merkleRoot: 'cd'.repeat(32), time: Math.floor(Date.now() / 1000) } })
    return new Response('{}', { status: 404 })
  } },
}
const start = () => new Miniflare((convertV4MiniflareOptions ?? (v => v))(options))
const files = path => readdirSync(path, { withFileTypes: true }).flatMap(e => e.isDirectory() ? files(resolve(path, e.name)) : [resolve(path, e.name)])
let mf = start()
const measured = []
try {
  // Let Miniflare choose its D1 filename, then install the native-exported fleet SQLite snapshot while stopped.
  await (await mf.getD1Database('OVERLAY_DB')).prepare('SELECT 1').first()
  await mf.dispose()
  const dbFile = files(state).find(p => p.includes('D1Database') && p.endsWith('.sqlite'))
  assert(dbFile, 'the local D1 database exists')
  cpSync(fixture.sqlite, dbFile)
  for (const suffix of ['-wal', '-shm']) rmSync(dbFile + suffix, { force: true })
  mf = start()
  const db = await mf.getD1Database('OVERLAY_DB')
  const one = (sql, ...binds) => db.prepare(sql).bind(...binds).first()
  assert((await one('SELECT COUNT(*) AS n FROM pot_records')).n >= 14719, 'the shared fleet fixture is loaded')
  const call = (path, init) => mf.dispatchFetch('http://app' + path, init)
  const poll = async (fn, ms = 22000) => {
    const end = Date.now() + ms
    do { if (await fn()) return; await new Promise(r => setTimeout(r, 25)) } while (Date.now() < end)
    assert.fail('timed out waiting for the queue consumer\u2019s judge')
  }
  await call('/health') // Cold schema work belongs to boot, not a warm door sample.
  const run = async (scenario, delay = 10000) => {
    await call('/pin/delay?ms=' + delay)
    const t0 = performance.now()
    const response = await call(`/record?kind=${scenario.kind}&identity=${scenario.identity}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(scenario.body) })
    const body = await response.json()
    const ms = performance.now() - t0
    assert.equal(response.status, 200, JSON.stringify(body))
    assert.deepEqual(body, scenario.answer, 'the door’s answer body is unchanged')
    const timing = response.headers.get('server-timing')
    measured.push({ name: scenario.name, ms, timing })
    console.log(JSON.stringify(measured.at(-1)))
    const durable = await one(scenario.read, ...scenario.binds)
    assert(durable.n > 0, 'the filed rows are readable as soon as the door answers')
    if (before) assert(ms >= 10000, 'the old door paid for the slow judge')
    else {
      assert(ms < 300, `${scenario.name} took ${ms} ms with a 10 s judge`)
      assert.match(timing, /write;dur=\d+, enqueue;dur=\d+/)
      assert.doesNotMatch(timing, /judge;desc=detached/, 'no judge rides the door')
      const got = Object.fromEntries([...timing.matchAll(/(reads|writes|stmts|maxReads)=(\d+)/g)].map(m => [m[1], Number(m[2])]))
      const ceiling = JSON.parse(readFileSync(new URL('../lane-499/ceilings.json', import.meta.url), 'utf8')).scenarios['record-door-' + scenario.name]?.ceiling
      if (process.env.D1_BUDGET_MEASURE !== '1') assert(ceiling, 'each record door has a write-only ceiling')
      if (process.env.D1_BUDGET_MEASURE !== '1') for (const [key, limit] of Object.entries(ceiling)) assert(got[key] <= limit, `${scenario.name} ${key}: ${got[key]} > ${limit}`)
      assert((await one('SELECT COUNT(*) AS n FROM owed_dirty WHERE identity=?', scenario.identity)).n > 0, 'the source transaction durably marked the identity before answering')
      if (scenario.name === 'handfile') assert.equal((await one('SELECT COUNT(*) AS n FROM owed_state WHERE identity=?', scenario.identity)).n, 0, 'no judgement landed before the first door answered')
    }
  }
  if (before) await run(fixture.scenarios[0])
  else {
    for (const [index, scenario] of fixture.scenarios.entries()) {
      await run(scenario)
      // the consumer's judge takes the injected delay (one at a time: a queue consumer's batches run in order)
      await poll(async () => (await (await call('/pin/judge')).json()).delayed >= index + 1, 40000)
    }
    // The collected/held filings use the handfile identity while its first judgement sleeps.
    assert.equal(fixture.scenarios[0].identity, fixture.scenarios.find(s => s.name === 'held').identity)
    await poll(async () => (await one('SELECT COUNT(*) AS n FROM owed_dirty')).n === 0, 60000)
    assert((await one('SELECT COUNT(*) AS n FROM owed_state WHERE identity=? AND recomputeSeq>0', fixture.scenarios[0].identity)).n > 0, 'judgement landed after the answer')
    const health = await (await call('/health')).json()
    assert(health.record.answerSamples >= 7)
    assert(health.record.answerMsP50 < 300 && health.record.answerMsP95 < 300, JSON.stringify(health.record))
    assert(health.record.enqueueMsP50 !== null && health.record.enqueueMsP95 !== null, JSON.stringify(health.record))
    assert(health.owedEvents.consumedTotal >= 7 && health.owedEvents.ackedMarks > 0, JSON.stringify(health.owedEvents))
    assert(health.owedEvents.oldestEnqueueAgeMs !== null, 'the consumer wrote its batch row')
    console.log('PASS all seven doors: write-only budgets, <300 ms answers with the publish, immediate reads, the queue consumer\u2019s judgement, concurrent filing marks drained, /health.record and /health.owedEvents served')
    // HIGH-2's second belt: a mark no consumer acknowledged within one tick is re-published by the sweep on the belt's
    // alarm and judged by the consumer.
    const party = fixture.scenarios.find(s => s.name === 'potparty')
    await db.prepare("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'pin','wall',? FROM potparty_records WHERE txid=?").bind(Date.now() - 600000, party.binds[0]).run()
    assert.equal((await one("SELECT COUNT(*) AS n FROM owed_dirty WHERE source='pin'")).n, 1, 'the planted mark is un-acked')
    const ns = await mf.getDurableObjectNamespace('BOARD_VIEW')
    const response = await ns.get(ns.idFromName('owed-belt:v1')).fetch('http://pin/pin/alarm')
    assert.equal(response.status, 200, await response.clone().text())
    await poll(async () => (await one("SELECT COUNT(*) AS n FROM owed_dirty WHERE source='pin'")).n === 0)
    console.log('PASS the un-acked sweep on the belt alarm re-publishes a mark older than a tick; the consumer acknowledges it')
  }
  writeFileSync(resolve(dirname(fixturePath), before ? 'before.json' : 'after.json'), JSON.stringify(measured, null, 2) + '\n')
} finally { await mf.dispose(); rmSync(scratch, { recursive: true, force: true }) }
