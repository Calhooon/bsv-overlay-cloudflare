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
// FIX-952 (lane FIX-952-PHASE-C, bsv-low #952): fix952_a, THE SAME FLOOD IN PHASE C (`OWED_D1_JUDGE=false`,
// `OWED_ACTOR_READ=true`, the actor the one writer) on the same persisted state with 32 fresh seats: each visible
// under 1 s by the direct call, which in Phase C also ACKNOWLEDGES the marks it applied (`owed_dirty` empty for the
// 32, `owed_events_direct_acked_total` up by 32), the queue behind (+32 published, no consumer), and NO `owed_rows`
// written for them (nothing writes D1's rows in Phase C). Each leg prints its figures; the cell FAILS on either.
// Reads the tier's exported fleet fixture (RECORD_DOOR_FIXTURE, as the #911 cell does). Every host is sealed.
// FIX-984 (bsv-low #984): THE CELL OWNS ITS PHASE AND ITS BOOTSTRAP BOUND. The committed render's owed flags
// (`crates/low-app-layer/wrangler.toml`, top level and `[env.beta.vars]`) are read and printed at the start and never
// used: each half binds its own (B, then C), so the cell is the same under a committed Phase A, B or C render. The
// bootstraps take tokens of the fleet budget (`owed-actor:boot-budget`, BOOTSTRAP_PAGES_PER_MINUTE 60 a wall-clock
// minute): the two halves' 64 pages can fall in one minute, and a page past the 60th waits for the next minute plus
// its identity's spread (under 30 s), up to 90 s from the request, past the old fixed 60 s poll (the refused land of
// PR 233, 28 of the second half's 32 pages run). The poll's bound is now read off the budget row before the
// bootstraps: the pages this minute still holds run at once, the rest at the next minute's edge plus the spread.
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
const options = phase => ({
  name: 'owed-direct-flood', resourcePersistencePath: state, defaultPersistRoot: state, modulesRoot: root,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', build) : root + path) })),
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'BoardView', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore', OWED_ACTOR: { className: 'AlarmPin', useSQLite: true } },
  d1Databases: { OVERLAY_DB: '00000000-0000-0000-0000-000000000952' },
  kvNamespaces: { OWED_COPY: 'owed-copy-952' },
  // the queue BEHIND: a producer, no consumer here (the direct call is the only judge of this flood)
  queueProducers: { OWED_EVENTS: { queueName: 'low-owed-events' } },
  // Phase B (the D1 judge writes `owed_rows`, the actor's rows served) or Phase C (the actor the one writer)
  bindings: { AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: TOKEN, COURIER_SINK: SEALED, ARCADE_URL: SEALED, CORROBORATOR_URL: SEALED, RELAY_URL: SEALED, DUAL_BROADCAST: 'off', OWED_D1_JUDGE: phase === 'C' ? 'false' : 'true', OWED_ACTOR_READ: 'true', OWED_CONSUMER_CONCURRENCY: phase === 'C' ? '4' : '1' },
  serviceBindings: { CHAINTRACKS: async request => {
    const path = new URL(request.url).pathname
    if (path === '/getPresentHeight') return Response.json({ status: 'success', value: 900000 })
    if (path === '/findChainTipHeaderHex' || path === '/findHeaderHexForHeight') return Response.json({ status: 'success', value: { height: 900000, hash: 'ab'.repeat(32), merkleRoot: 'cd'.repeat(32), time: Math.floor(Date.now() / 1000) } })
    return new Response('{}', { status: 404 })
  } },
})
const start = phase => new Miniflare((convertV4MiniflareOptions ?? (v => v))(options(phase)))
// the committed render's owed flags, printed and not used (the halves bind their own)
const renderFlags = (() => {
  const toml = readFileSync(resolve(root, 'crates/low-app-layer/wrangler.toml'), 'utf8')
  const out = {}
  let section = ''
  for (const line of toml.split('\n')) {
    const h = line.match(/^\s*\[([^\]]+)\]/)
    if (h) { section = h[1]; continue }
    const m = line.match(/^\s*(OWED_ACTOR_READ|OWED_D1_JUDGE|OWED_CONSUMER_CONCURRENCY)\s*=\s*"([^"]*)"/)
    if (m && (section === 'vars' || section === 'env.beta.vars')) out[`${section}.${m[1]}`] = m[2]
  }
  return out
})()
console.log(`owed_direct_flood: the committed render's owed flags ${JSON.stringify(renderFlags)}; each half binds its own (d_a Phase B, fix952_a Phase C)`)
// FIX-984: the fleet budget the bootstraps take from (owed_actor.rs), and the bound of a poll over N one-page bootstraps
const PAGES_PER_MINUTE = 60
const SPREAD_MS = 30000
const BOOT_SLACK_MS = 30000
const bootBound = async (db, pages) => {
  const now = Date.now()
  const row = await db.prepare("SELECT cursor, tickedAtMs FROM owed_belt_state WHERE name='owed-actor:boot-budget'").first()
  const taken = row && Math.trunc(Number(row.tickedAtMs)) === Math.floor(now / 60000) ? Number(row.cursor) : 0
  const spill = Math.max(0, taken + pages - PAGES_PER_MINUTE)
  const minutes = Math.ceil(spill / PAGES_PER_MINUTE)
  const ms = (spill ? (Math.floor(now / 60000) + minutes) * 60000 - now + SPREAD_MS : 0) + BOOT_SLACK_MS
  return { taken, spill, ms }
}
const files = path => readdirSync(path, { withFileTypes: true }).flatMap(e => e.isDirectory() ? files(resolve(path, e.name)) : [resolve(path, e.name)])
let mf = start('B')
try {
  await (await mf.getD1Database('OVERLAY_DB')).prepare('SELECT 1').first()
  await mf.dispose()
  const dbFile = files(state).find(p => p.includes('D1Database') && p.endsWith('.sqlite'))
  assert(dbFile, 'the local D1 database exists')
  cpSync(fixture.sqlite, dbFile)
  for (const suffix of ['-wal', '-shm']) rmSync(dbFile + suffix, { force: true })
  const hex = (prefix, n, len) => prefix + n.toString(16).padStart(len - prefix.length, '0')
  // ONE FLOOD under one phase: 32 fresh one-game seats (a copy of one fixture hop marker each, under a fresh identity,
  // game and outpoint), their actors bootstrapped, four hook bodies at once, each identity's first visibility.
  const flood = async (tag, phase, prefix) => {
    mf = start(phase)
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
    const seats = Array.from({ length: 32 }, (_, i) => ({ identity: hex(prefix, i, 66), game: hex(prefix.slice(2), i, 64), txid: hex(prefix.slice(2) === 'fe' ? 'fd' : 'fb', i, 64) }))
    const template = await db.prepare('SELECT * FROM hopparty_records LIMIT 1').first()
    assert(template, 'the fixture holds a hop marker')
    const cols = Object.keys(template)
    for (const s of seats) {
      const row = { ...template, identity: s.identity, gameId: s.game, txid: s.txid, outputIndex: 0, createdAt: Math.floor(Date.now() / 1000) }
      await db.prepare(`INSERT INTO hopparty_records (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`).bind(...cols.map(c => row[c])).run()
    }
    const like = prefix + '%'
    const marks = (await db.prepare('SELECT COUNT(DISTINCT identity) AS n FROM owed_dirty WHERE identity LIKE ?').bind(like).first()).n
    assert.equal(marks, 32, `${tag}: each seat's marker wrote its mark (the dirty trigger)`)
    const bound = await bootBound(db, seats.length)
    const tb = Date.now()
    await Promise.all(seats.map(s => actor(s.identity).fetch('http://owed-actor/bootstrap', { method: 'POST', body: JSON.stringify({ identity: s.identity }) })))
    await poll(`${tag}: the 32 bootstraps (${bound.taken} tokens of this minute taken before, ${bound.spill} pages past the budget, bound ${bound.ms} ms)`, async () => (await Promise.all(seats.map(s => pinState(s.identity)))).every(p => p.meta.bootstrapped), bound.ms)
    console.log(`${tag}: 32 seats bootstrapped in ${Date.now() - tb} ms (phase ${phase}; ${bound.taken} tokens of the minute taken before, ${bound.spill} pages waited for the next minute; bound ${bound.ms} ms)`)
    const judgedBefore = new Map()
    for (const s of seats) judgedBefore.set(s.identity, Number((await pinState(s.identity)).meta.judgedAtMs ?? 0))
    const publishedBefore = await counter('owed_events_published_total')
    const callsBefore = await counter('owed_actor_direct_calls_total')
    const ackedBefore = await counter('owed_events_direct_acked_total')
    // (2) the flood: four hook bodies at once, eight identities each
    const t0 = Date.now()
    const answers = await Promise.all([0, 1, 2, 3].map(k => mf.dispatchFetch('http://app/internal/hop-changed', {
      method: 'POST', headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' },
      body: JSON.stringify({ identities: seats.slice(8 * k, 8 * k + 8).map(s => s.identity) }),
    })))
    const answeredMs = Date.now() - t0
    for (const a of answers) assert.equal(a.status, 200, `${tag}: the hook answered: ${await a.text()}`)
    // (3) each actor's first visibility
    const seen = new Map()
    await poll(`${tag}: the 32 actors' first visibility`, async () => {
      await Promise.all(seats.filter(s => !seen.has(s.identity)).map(async s => {
        const j = Number((await pinState(s.identity)).meta.judgedAtMs ?? 0)
        if (j > judgedBefore.get(s.identity)) seen.set(s.identity, j - t0)
      }))
      return seen.size === 32
    }, 20000)
    const ms = [...seen.values()].sort((a, b) => a - b)
    console.log(`${tag}: the hooks answered in ${answeredMs} ms; first visibility of 32 identities: p50 ${ms[15]} ms, max ${ms[31]} ms`)
    assert(ms[31] < 1000, `${tag}: each identity's rows visible under 1 s of its write: max ${ms[31]} ms (${JSON.stringify(ms)})`)
    // the queue behind: every event published (no consumer ran), every call counted
    await poll(`${tag}: the counters`, async () => (await counter('owed_events_published_total')) - publishedBefore >= 32 && (await counter('owed_actor_direct_calls_total')) - callsBefore >= 32, 10000)
    const published = (await counter('owed_events_published_total')) - publishedBefore
    const calls = (await counter('owed_actor_direct_calls_total')) - callsBefore
    if (phase === 'C') {
      // FIX-952 (fix952_a): in Phase C the direct call is the judgement: it acknowledges the marks it applied, and
      // nothing writes D1's owed_rows for these seats (the actor is the one writer)
      await poll(`${tag}: the marks acknowledged by the direct call`, async () => (await db.prepare('SELECT COUNT(*) AS n FROM owed_dirty WHERE identity LIKE ?').bind(like).first()).n === 0, 10000)
      const acked = (await counter('owed_events_direct_acked_total')) - ackedBefore
      assert(acked >= 32, `${tag}: the direct call acknowledged the 32 marks: +${acked}`)
      const rows = (await db.prepare('SELECT COUNT(*) AS n FROM owed_rows WHERE identity LIKE ?').bind(like).first()).n
      assert.equal(rows, 0, `${tag}: no owed_rows written in Phase C (the actor is the one writer)`)
      console.log(`PASS fix952_a: Phase C, 32 frames for 32 identities, each visible under 1 s (max ${ms[31]} ms) by the direct call, which acknowledged their marks (+${acked}); the queue behind (+${published} published, no consumer); no owed_rows written; +${calls} direct calls`)
    } else {
      console.log(`PASS d_a: 32 frames for 32 identities, each visible under 1 s (max ${ms[31]} ms) by the direct call; the queue behind (+${published} published, no consumer); +${calls} direct calls`)
    }
    await Promise.race([mf.dispose(), new Promise(r => setTimeout(r, 15000))])
    mf = null
  }
  await flood('d_a', 'B', '02fe')
  await flood('fix952_a', 'C', '02fc')
} finally {
  if (mf) await Promise.race([mf.dispose(), new Promise(r => setTimeout(r, 15000))])
  rmSync(scratch, { recursive: true, force: true })
}
process.exit(0)
