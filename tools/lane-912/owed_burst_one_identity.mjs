// bsv-low #954 (lane D2-ACTOR-E): e_b, A BURST OF 64 FILINGS FOR ONE IDENTITY EACH REACHES FIRST VISIBILITY UNDER 1 S
// WITH THE DIRECT CALL. On real workerd (Miniflare, the tier's pinned wrangler) with the production app layer and the
// production OwedActor (`owed_actor_alarm_worker.mjs`: `/pin/state` the actor's `meta`, `/pin/applied` its `applied`
// rows), the `owed-events` queue bound as a PRODUCER only (the backstop behind, no consumer here).
//   (1) one seat of 64 games (64 hop markers of one identity, their dirty triggers mark them), bootstrapped;
//   (2) the burst: 64 writes at once, each a relatch of one game's marker (its trigger marks `hop txid:vout`) handed
//       to the app layer as the overlay hands its published events (`POST /internal/owed-direct`, one event a body);
//   (3) every event's `(kind, seq)` recorded applied by the actor within 1 s of its write; the judge passes the
//       object's burst took (`/bars` `burst`) far fewer than 64.
// RED on `e99472091` (reasoned, the measured shape of #954): each of the 64 calls queues in the object behind the one
// before it at the apply's full cost (one record statement a family, 24, and a judge pass each), so the later events'
// first visibility is past 1 s (beta: 64 of 164 direct calls walled at 2 s, `rowsVisibleDirectMsP95` 9.6 s).
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
const scratch = mkdtempSync(resolve(process.env.TMPDIR ?? tmpdir(), 'owed-burst-one-'))
const state = resolve(scratch, 'state')
const SEALED = 'http://127.0.0.1:9'
const TOKEN = 'ci-internal-tok'
const N = 64
const paths = ['tools/lane-912/owed_actor_alarm_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm']
const options = {
  name: 'owed-burst-one', resourcePersistencePath: state, defaultPersistRoot: state, modulesRoot: root,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', build) : root + path) })),
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'BoardView', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore', OWED_ACTOR: { className: 'AlarmPin', useSQLite: true } },
  d1Databases: { OVERLAY_DB: '00000000-0000-0000-0000-000000000954' },
  kvNamespaces: { OWED_COPY: 'owed-copy-954' },
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
  const hex = (prefix, n, len) => prefix + n.toString(16).padStart(len - prefix.length, '0')
  const identity = hex('02fd', 1, 66)
  const actor = ns.get(ns.idFromName(identity))
  const pinState = async () => (await actor.fetch('http://owed-actor/pin/state')).json()
  const poll = async (what, fn, ms = 60000) => {
    const end = Date.now() + ms
    do { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)) } while (Date.now() < end)
    assert.fail('timed out: ' + what)
  }
  // (1) one seat of 64 games: a copy of one fixture hop marker each, one identity, its own game and outpoint
  const games = Array.from({ length: N }, (_, i) => ({ game: hex('fb', i, 64), txid: hex('fa', i, 64) }))
  const template = await db.prepare('SELECT * FROM hopparty_records LIMIT 1').first()
  assert(template, 'the fixture holds a hop marker')
  const cols = Object.keys(template)
  for (const g of games) {
    const row = { ...template, identity, gameId: g.game, txid: g.txid, outputIndex: 0, createdAt: Math.floor(Date.now() / 1000) }
    await db.prepare(`INSERT INTO hopparty_records (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`).bind(...cols.map(c => row[c])).run()
  }
  const tb = Date.now()
  await actor.fetch('http://owed-actor/bootstrap', { method: 'POST', body: JSON.stringify({ identity }) })
  await poll('the bootstrap', async () => (await pinState()).meta.bootstrapped, 120000)
  console.log(`e_b: one seat of ${N} games bootstrapped in ${Date.now() - tb} ms`)
  const barsBefore = (await (await actor.fetch('http://owed-actor/bars')).json()).burst ?? { passes: 0, events: 0 }
  // (2) the burst: 64 writes at once, each handed to the app layer as the overlay hands its published event
  const seq0 = Date.now() * 1024
  const wrote = new Map()
  const answers = await Promise.all(games.map(async (g, i) => {
    await db.prepare('UPDATE hopparty_records SET hopSats=hopSats+1 WHERE txid=? AND identity=?').bind(g.txid, identity).run()
    const at = Date.now()
    const seq = seq0 + i
    wrote.set(seq, at)
    const ev = { v: 1, seq, source: 'hop', sourceKey: `${g.txid}:${template.hopVout}`, facts: { marks: 1, row: null }, atMs: at }
    return mf.dispatchFetch('http://app/internal/owed-direct', {
      method: 'POST', headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' },
      body: JSON.stringify({ events: [JSON.stringify(ev)] }),
    })
  }))
  for (const a of answers) assert.equal(a.status, 202, `the hand-off answered: ${await a.text()}`)
  // (3) every event recorded applied by the actor, each within 1 s of its write
  const applied = await poll('the 64 events applied', async () => {
    const rows = (await (await actor.fetch('http://owed-actor/pin/applied?kind=hop')).json()).rows.filter(r => wrote.has(Number(r.seq)))
    const first = new Map()
    for (const r of rows) { const s = Number(r.seq); first.set(s, Math.min(first.get(s) ?? Infinity, Number(r.atMs))) }
    return first.size === N ? first : null
  }, 30000)
  const ms = [...applied.entries()].map(([s, at]) => at - wrote.get(s)).sort((a, b) => a - b)
  const bars = (await (await actor.fetch('http://owed-actor/bars')).json()).burst
  const passes = bars.passes - barsBefore.passes
  console.log(`e_b: ${N} writes for one identity: first visibility p50 ${ms[N >> 1]} ms, max ${ms[N - 1]} ms; ${passes} judge passes for ${bars.events - barsBefore.events} events`)
  assert(ms[N - 1] < 1000, `each write visible under 1 s: max ${ms[N - 1]} ms (${JSON.stringify(ms)})`)
  assert(passes < N / 4, `a burst is a few judge passes: ${passes}`)
  console.log(`PASS e_b: ${N} writes for one identity, each visible under 1 s (max ${ms[N - 1]} ms) by the direct call, ${passes} judge passes`)
} finally {
  await Promise.race([mf.dispose(), new Promise(r => setTimeout(r, 15000))])
  rmSync(scratch, { recursive: true, force: true })
}
process.exit(0)
