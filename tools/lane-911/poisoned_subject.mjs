// bsv-low #911 (D2-ACTOR-A-FOLD-3, the second delta lens's NEW2-M1 and section 6): ONE POISONED SUBJECT ON A SHARED
// PAIR, through the production consumer on real workerd D1 and Miniflare's local `owed-events` queue.
//   1. `age`/`age` holds three honest subjects and one whose every judge faults (the worker's D1 boundary): one
//      message lands the three, keeps the poisoned mark, notes the SUBJECT's memo (never the pair's).
//   2. Its redeliveries fault it to its hold (the third fault); the delivery inside the hold meets only held subjects:
//      acked, counted `owed_events_all_held_total`, no continuation.
//   3. The next delivery of that pair judges a subject marked since; the poisoned mark still stands.
// Every host is sealed (ARCADE_URL, COURIER_SINK, DUAL_BROADCAST:off, CORROBORATOR_URL point at a closed port).
// Reads the fleet fixture `record_door.mjs` exports (RECORD_DOOR_FIXTURE): its migrations, its marks cleared first.
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { delimiter, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
process.on('uncaughtException', e => { if (e?.code === 'ERR_SERVER_NOT_RUNNING') return; console.error(e); process.exit(1) })
const root = fileURLToPath(new URL('../../', import.meta.url))
const fixturePath = process.env.RECORD_DOOR_FIXTURE
assert(fixturePath, 'RECORD_DOOR_FIXTURE names the exported fleet fixture')
const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'))
const build = process.env.NS2_APP_BUILD ?? resolve(root, 'crates/low-app-layer/build')
const wrangler = process.env.NS2_WRANGLER_PACKAGE ?? process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'use the tier’s pinned wrangler/Miniflare')
const { Miniflare, convertV4MiniflareOptions } = createRequire(wrangler)('miniflare')
const scratch = mkdtempSync(resolve(process.env.TMPDIR ?? tmpdir(), 'poisoned-subject-'))
const state = resolve(scratch, 'state')
const SEALED = 'http://127.0.0.1:9'
const POISON = 'poison-911'
const paths = ['tools/lane-911/poisoned_subject_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm']
const options = {
  name: 'poisoned-subject', resourcePersistencePath: state, defaultPersistRoot: state, modulesRoot: root,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', build) : root + path) })),
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'BoardView', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore' },
  d1Databases: { OVERLAY_DB: '00000000-0000-0000-0000-000000000911' },
  queueProducers: { OWED_EVENTS: { queueName: 'low-owed-events' } },
  queueConsumers: { 'low-owed-events': { maxBatchSize: 1, maxBatchTimeout: 0, maxRetries: 3, deadLetterQueue: 'low-owed-events-dlq' }, 'low-owed-events-dlq': { maxBatchSize: 10, maxRetries: 100 } },
  bindings: { AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: 'ci-internal-tok', COURIER_SINK: SEALED, ARCADE_URL: SEALED, CORROBORATOR_URL: SEALED, DUAL_BROADCAST: 'off' },
  serviceBindings: { CHAINTRACKS: async request => {
    const path = new URL(request.url).pathname
    if (path === '/getPresentHeight') return Response.json({ status: 'success', value: 900000 })
    if (path === '/findChainTipHeaderHex' || path === '/findHeaderHexForHeight') return Response.json({ status: 'success', value: { height: 900000, hash: 'ab'.repeat(32), merkleRoot: 'cd'.repeat(32), time: Math.floor(Date.now() / 1000) } })
    return new Response('{}', { status: 404 })
  } },
}
const start = () => new Miniflare((convertV4MiniflareOptions ?? (v => v))(options))
const files = path => readdirSync(path, { withFileTypes: true }).flatMap(e => e.isDirectory() ? files(resolve(path, e.name)) : [resolve(path, e.name)])
const id = n => '02' + n.repeat(32)
const poisoned = id('9a')
const honest = [id('91'), id('92'), id('93')]
let mf = start()
try {
  await (await mf.getD1Database('OVERLAY_DB')).prepare('SELECT 1').first()
  await mf.dispose()
  const dbFile = files(state).find(p => p.includes('D1Database') && p.endsWith('.sqlite'))
  assert(dbFile, 'the local D1 database exists')
  cpSync(fixture.sqlite, dbFile)
  for (const suffix of ['-wal', '-shm']) rmSync(dbFile + suffix, { force: true })
  mf = start()
  const db = await mf.getD1Database('OVERLAY_DB')
  const one = (sql, ...binds) => db.prepare(sql).bind(...binds).first()
  const call = path => mf.dispatchFetch('http://app' + path)
  const counter = async name => (await one('SELECT value FROM ops_counters WHERE name=?', name))?.value ?? 0
  const poll = async (what, fn, ms = 60000) => {
    const end = Date.now() + ms
    do { if (await fn()) return; await new Promise(r => setTimeout(r, 50)) } while (Date.now() < end)
    assert.fail('timed out: ' + what)
  }
  await call('/health')
  await db.prepare('DELETE FROM owed_dirty').run()
  const mark = (identity, game) => db.prepare("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?,?,'age','age',?)").bind(identity, game, Date.now()).run()
  const marksOf = async (identity, game) => (await one('SELECT COUNT(*) AS n FROM owed_dirty WHERE identity=? AND gameId=?', identity, game)).n
  const producer = await mf.getQueueProducer('OWED_EVENTS')
  let seq = 0
  const send = () => producer.send(JSON.stringify({ v: 1, seq: ++seq, source: 'age', sourceKey: 'age', facts: null, atMs: Date.now() }))
  const subjectMemo = `owed-events:fault:subject ${poisoned}/${POISON}`
  const memo = async name => {
    const row = await one('SELECT cursor FROM owed_belt_state WHERE name=?', name)
    return row ? JSON.parse(row.cursor) : null
  }

  // 1. three honest subjects land beside the poisoned one; the memo is the subject's
  await mark(poisoned, POISON)
  for (const h of honest) await mark(h, 'live-1')
  await send()
  await poll('the honest subjects judged', async () => (await Promise.all(honest.map(h => marksOf(h, 'live-1')))).every(n => n === 0))
  assert.equal(await marksOf(poisoned, POISON), 1, 'the poisoned mark stands')
  assert(await memo(subjectMemo), 'the poisoned subject’s own memo')
  console.log('PASS 1: one message landed the three honest subjects of age/age beside the poisoned one; its memo is the subject’s')

  // 2. the redeliveries fault it to its hold; the delivery inside the hold is all-held and ends
  const continued = await counter('owed_events_continued_total')
  await send()
  await poll('the poisoned subject held', async () => ((await memo(subjectMemo))?.heldUntilMs ?? 0) > Date.now())
  await poll('an all-held message counted', async () => (await counter('owed_events_all_held_total')) >= 1)
  assert.equal(await marksOf(poisoned, POISON), 1, 'the held mark stands')
  assert.equal(await counter('owed_events_continued_total'), continued, 'no continuation from an all-held message')
  console.log(`PASS 2: the poisoned subject held (faults ${(await memo(subjectMemo)).faults}); the all-held delivery acked, no continuation`)

  // 3. the next delivery of the pair judges a subject marked since
  await mark(honest[0], 'live-2')
  await send()
  await poll('the new subject judged inside the hold', async () => (await marksOf(honest[0], 'live-2')) === 0)
  assert.equal(await marksOf(poisoned, POISON), 1, 'the poisoned mark still stands')
  assert.equal(await counter('owed_events_continued_total'), continued, 'no continuation')
  const faulted = (await (await call('/pin/faulted')).json()).faulted
  const health = await (await call('/health')).json()
  console.log(JSON.stringify({ faulted, owedEvents: health.owedEvents }))
  console.log('PASS 3: inside the hold the pair’s next delivery judged a subject marked since; the held one never took its place')
} finally { await mf.dispose(); rmSync(scratch, { recursive: true, force: true }) }
