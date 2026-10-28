// bsv-low #952 (lane D2-ACTOR-D): d_a, A FLOOD OF 32 FRAMES FOR 32 IDENTITIES EACH REACHES FIRST VISIBILITY UNDER 1 S
// WITH THE DIRECT CALL AND THE QUEUE BEHIND. On real workerd (Miniflare, the tier's pinned wrangler) with the production
// app layer and the production OwedActor (`owed_actor_alarm_worker.mjs`, the read-only wrapper: `/pin/state` reads the
// actor's `meta`), the `owed-events` queue bound as a PRODUCER only (its consumer never runs here: the backstop behind).
//   (1) 32 seats, one hop marker each (their dirty triggers write their marks), each actor bootstrapped (one page);
//   (2) the flood: the hop hook (`POST /internal/hop-changed`, bearer, 8 identities a body as the overlay chunks it)
//       four bodies at once, 32 identity events published to the queue, the published set handed to each actor
//       directly after the answer (`owed_events::schedule_direct`);
//   (3) each actor's judgement time (`meta.judgedAtMs`) moves within 1 s of the flood's start, the queue untouched
//       (`owed_events_published_total` up by 32; no consumer), `owed_actor_direct_calls_total` 32.
// RED on `046b721e`: the hook published and called no actor: no `judgedAtMs` moved ("timed out: the 32 actors' first
// visibility").
// Reads the tier's exported fleet fixture (RECORD_DOOR_FIXTURE, as the #911 cell does). Every host is sealed.
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { delimiter, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
const root = fileURLToPath(new URL('../../', import.meta.url))
const fixturePath = process.env.RECORD_DOOR_FIXTURE
assert(fixturePath, 'RECORD_DOOR_FIXTURE names the exported fleet fixture')
const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'))
const build = process.env.NS2_APP_BUILD ?? resolve(root, 'crates/low-app-layer/build')
const wrangler = process.env.NS2_WRANGLER_PACKAGE ?? process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'use the tier’s pinned wrangler/Miniflare')
const { Miniflare, convertV4MiniflareOptions } = createRequire(wrangler)('miniflare')
const scratch = mkdtempSync(resolve(process.env.TMPDIR ?? tmpdir(), 'owed-direct-flood-'))
const state = resolve(scratch, 'state')
const SEALED = 'http://127.0.0.1:9'
const TOKEN = 'ci-internal-tok'
const paths = ['tools/lane-912/owed_actor_alarm_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm']
const options = {
  name: 'owed-direct-flood', resourcePersistencePath: state, defaultPersistRoot: state, modulesRoot: root,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', build) : root + path) })),
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'BoardView', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore', OWED_ACTOR: { className: 'AlarmPin', useSQLite: true } },
  d1Databases: { OVERLAY_DB: '00000000-0000-0000-0000-000000000952' },
  kvNamespaces: { OWED_COPY: 'owed-copy-952' },
  // the queue BEHIND: a producer, no consumer here (the direct call is the only judge of this flood)
  queueProducers: { OWED_EVENTS: { queueName: 'low-owed-events' } },
  bindings: { AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: TOKEN, COURIER_SINK: SEALED, ARCADE_URL: SEALED, CORROBORATOR_URL: SEALED, RELAY_URL: SEALED, DUAL_BROADCAST: 'off', OWED_D1_JUDGE: 'true', OWED_ACTOR_READ: 'false' },
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
try {
  await (await mf.getD1Database('OVERLAY_DB')).prepare('SELECT 1').first()
  await mf.dispose()
  const dbFile = files(state).find(p => p.includes('D1Database') && p.endsWith('.sqlite'))
  assert(dbFile, 'the local D1 database exists')
  cpSync(fixture.sqlite, dbFile)
  for (const suffix of ['-wal', '-shm']) rmSync(dbFile + suffix, { force: true })
  mf = start()
  const db = await mf.getD1Database('OVERLAY_DB')
  const ns = await mf.getDurableObjectNamespace('OWED_ACTOR')
  const actor = id => ns.get(ns.idFromName(id))
  const pinState = async id => (await actor(id).fetch('http://owed-actor/pin/state')).json()
  const poll = async (what, fn, ms = 60000) => {
    const end = Date.now() + ms
    do { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)) } while (Date.now() < end)
    assert.fail('timed out: ' + what)
  }
  const counter = async name => (await db.prepare('SELECT value FROM ops_counters WHERE name=?').bind(name).first())?.value ?? 0
  // (1) 32 one-game seats: a copy of one fixture hop marker each, under a fresh identity, game and outpoint.
  const hex = (prefix, n, len) => prefix + n.toString(16).padStart(len - prefix.length, '0')
  const seats = Array.from({ length: 32 }, (_, i) => ({ identity: hex('02fe', i, 66), game: hex('fe', i, 64), txid: hex('fd', i, 64) }))
  const template = await db.prepare('SELECT * FROM hopparty_records LIMIT 1').first()
  assert(template, 'the fixture holds a hop marker')
  const cols = Object.keys(template)
  for (const s of seats) {
    const row = { ...template, identity: s.identity, gameId: s.game, txid: s.txid, outputIndex: 0, createdAt: Math.floor(Date.now() / 1000) }
    await db.prepare(`INSERT INTO hopparty_records (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`).bind(...cols.map(c => row[c])).run()
  }
  const marks = (await db.prepare("SELECT COUNT(DISTINCT identity) AS n FROM owed_dirty WHERE identity LIKE '02fe%'").first()).n
  assert.equal(marks, 32, 'each seat\'s marker wrote its mark (the dirty trigger)')
  const tb = Date.now()
  await Promise.all(seats.map(s => actor(s.identity).fetch('http://owed-actor/bootstrap', { method: 'POST', body: JSON.stringify({ identity: s.identity }) })))
  await poll('the 32 bootstraps', async () => (await Promise.all(seats.map(s => pinState(s.identity)))).every(p => p.meta.bootstrapped))
  console.log(`d_a: 32 seats bootstrapped in ${Date.now() - tb} ms`)
  const judgedBefore = new Map()
  for (const s of seats) judgedBefore.set(s.identity, Number((await pinState(s.identity)).meta.judgedAtMs ?? 0))
  const publishedBefore = await counter('owed_events_published_total')
  const callsBefore = await counter('owed_actor_direct_calls_total')
  // (2) the flood: four hook bodies at once, eight identities each
  const t0 = Date.now()
  const answers = await Promise.all([0, 1, 2, 3].map(k => mf.dispatchFetch('http://app/internal/hop-changed', {
    method: 'POST', headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' },
    body: JSON.stringify({ identities: seats.slice(8 * k, 8 * k + 8).map(s => s.identity) }),
  })))
  const answeredMs = Date.now() - t0
  for (const a of answers) assert.equal(a.status, 200, `the hook answered: ${await a.text()}`)
  // (3) each actor's first visibility
  const seen = new Map()
  await poll('the 32 actors\' first visibility', async () => {
    await Promise.all(seats.filter(s => !seen.has(s.identity)).map(async s => {
      const j = Number((await pinState(s.identity)).meta.judgedAtMs ?? 0)
      if (j > judgedBefore.get(s.identity)) seen.set(s.identity, j - t0)
    }))
    return seen.size === 32
  }, 20000)
  const ms = [...seen.values()].sort((a, b) => a - b)
  console.log(`d_a: the hooks answered in ${answeredMs} ms; first visibility of 32 identities: p50 ${ms[15]} ms, max ${ms[31]} ms`)
  assert(ms[31] < 1000, `each identity's rows visible under 1 s of its write: max ${ms[31]} ms (${JSON.stringify(ms)})`)
  // the queue behind: every event published (no consumer ran), every call counted
  await poll('the counters', async () => (await counter('owed_events_published_total')) - publishedBefore >= 32 && (await counter('owed_actor_direct_calls_total')) - callsBefore >= 32, 10000)
  console.log(`PASS d_a: 32 frames for 32 identities, each visible under 1 s (max ${ms[31]} ms) by the direct call; the queue behind (+${(await counter('owed_events_published_total')) - publishedBefore} published, no consumer); +${(await counter('owed_actor_direct_calls_total')) - callsBefore} direct calls`)
} finally {
  await Promise.race([mf.dispose(), new Promise(r => setTimeout(r, 15000))])
  rmSync(scratch, { recursive: true, force: true })
}
process.exit(0)
