// bsv-low #912 (D2-ACTOR-B-FOLD-7): fold7_h, THE OWED ACTOR'S ALARMS FIRE (the captain's item 11). workers-rs 0.8.7
// reads an `i64` alarm as an OFFSET from now; until the fold every OwedActor site passed an epoch stamp, so each alarm
// landed about 56 years out and none fired on beta (bootstraps 0, the budget row never written). On real workerd
// (Miniflare, the tier's pinned wrangler), the production actor (`owed_actor_alarm_worker.mjs`, a read-only wrapper):
//   (a) an armed bootstrap's alarm is stored within now + 2 s and FIRES: its first page runs (the fleet's budget row
//       written in D1) and a seat under 16 games is bootstrapped within seconds;
//   (b) a seat over 16 games: after the first page the stored alarm is within now + BOOTSTRAP_PAGE_DELAY_MS + 2 s and
//       the next page fires;
//   (i) the captain's item 12, fold7_i: an actor ARMED UNDER A DEAD ALARM (its cursor set, its alarm set the way the old
//       build set it, about 56 years out): one `/bootstrap` call stores an alarm within now + 2 s (`rearmed`), and it
//       fires and pages; a second such actor's first `/apply` is held (202) and its alarm too is within now + 2 s and
//       fires. RED on `d910ad5d`: `arm_bootstrap` answered `(false, false)` and kept the dead alarm.
//   (c) an unseen identity's (a seat over one page) first `/apply` is HELD (202, `bootstrapArmed`), its alarm fires, its bootstrap ends and
//       the held row is gone; then two applies inside a second: the KV copy's alarm within now + 1 s + 2 s.
// RED on `d910ad5d`: the stored alarm reads about 56 years out and `fired` stays 0.
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
const scratch = mkdtempSync(resolve(process.env.TMPDIR ?? tmpdir(), 'owed-actor-alarm-'))
const state = resolve(scratch, 'state')
const SEALED = 'http://127.0.0.1:9'
const paths = ['tools/lane-912/owed_actor_alarm_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm']
const options = {
  name: 'owed-actor-alarm', resourcePersistencePath: state, defaultPersistRoot: state, modulesRoot: root,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', build) : root + path) })),
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'BoardView', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore', OWED_ACTOR: { className: 'AlarmPin', useSQLite: true } },
  d1Databases: { OVERLAY_DB: '00000000-0000-0000-0000-0000000f0107' },
  kvNamespaces: { OWED_COPY: 'owed-copy-fold7' },
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
  const poll = async (what, fn, ms = 30000) => {
    const end = Date.now() + ms
    do { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 100)) } while (Date.now() < end)
    assert.fail('timed out: ' + what)
  }
  // Seats by their game count (the identity union the bootstrap pages), 66-hex keys only.
  const seats = (await db.prepare("SELECT identity,COUNT(DISTINCT gameId) AS n FROM (SELECT identity,gameId FROM potparty_records UNION SELECT identity,gameId FROM hopparty_records) WHERE length(identity)=66 GROUP BY identity ORDER BY n").all()).results
  const small = seats.find(s => s.n >= 1 && s.n < 16)
  const bigs = seats.filter(s => s.n > 16)
  assert(small && bigs.length >= 2, `the fixture holds a seat under one page and two over it: ${JSON.stringify(seats.slice(0, 3))} .. ${seats.length}`)
  const big = bigs[0]
  const actor = id => ns.get(ns.idFromName(id))
  const pinState = async id => (await actor(id).fetch('http://owed-actor/pin/state')).json()
  // An alarm already fired reads null (an offset of 0 fires at once): then it must have run.
  const within = (s, offset, what) => {
    if (s.alarm === null) { assert(s.fired > 0, `${what}: no alarm set and none fired`); return }
    assert(s.alarm <= s.now + offset + 2000, `${what}: the alarm ${s.alarm} is within now ${s.now} + ${offset} + 2000 (it is ${Math.round((s.alarm - s.now) / 86400000 / 365)} years out)`)
  }
  // FOLD7_ONLY=i runs fold7_i alone (its RED on `d910ad5d` fails at (a) otherwise).
  if (process.env.FOLD7_ONLY !== 'i') {
  // (a) an armed bootstrap fires.
  const a = small.identity
  const armed = await (await actor(a).fetch('http://owed-actor/bootstrap', { method: 'POST', body: JSON.stringify({ identity: a }) })).json()
  assert.equal(armed.armed, true, 'the bootstrap armed')
  within(await pinState(a), 0, '(a) the armed bootstrap')
  const t0 = Date.now()
  const done = await poll('(a) the alarm fires and the bootstrap ends', async () => { const s = await pinState(a); return s.fired > 0 && s.meta.bootstrapped ? s : null })
  console.log(`PASS fold7_h (a): ${a.slice(0, 10)}.. (${small.n} games) armed, the alarm fired ${done.fired}x, bootstrapped in ${Date.now() - t0} ms`)
  const budget = await db.prepare("SELECT cursor FROM owed_belt_state WHERE name='owed-actor:boot-budget'").first()
  assert(budget, 'the page took a token of the fleet budget (owed-actor:boot-budget written)')
  // (b) a seat over one page: the next page's alarm is the page delay.
  const b = big.identity
  await actor(b).fetch('http://owed-actor/bootstrap', { method: 'POST', body: JSON.stringify({ identity: b }) })
  const paged = await poll('(b) the first page ran', async () => { const s = await pinState(b); return s.fired >= 1 && s.meta.bootCursor && s.meta.bootCursor.startsWith('[') && !s.meta.bootCursor.startsWith('[9223372036854775807') ? s : null })
  within(paged, 1000, '(b) the next page')
  const t1 = Date.now()
  const doneB = await poll('(b) every page fired', async () => { const s = await pinState(b); return s.meta.bootstrapped ? s : null }, 90000)
  console.log(`PASS fold7_h (b): ${b.slice(0, 10)}.. (${big.n} games) paged by its own alarms (${doneB.fired} fired), bootstrapped ${Date.now() - t1} ms after the first page`)
  // (c) an unseen identity's first event is held, its alarm fires, the held event is applied.
  const c = bigs[1].identity
  const games = (await db.prepare('SELECT gameId FROM potparty_records WHERE identity=? UNION SELECT gameId FROM hopparty_records WHERE identity=? LIMIT 1').bind(c, c).all()).results.map(r => r.gameId)
  const apply = () => actor(c).fetch('http://owed-actor/apply', { method: 'POST', body: JSON.stringify({ identity: c, games, kind: 'pot', seq: 7, atMs: Date.now(), tip: 900000 }) })
  const tc = Date.now()
  const held = await apply()
  assert.equal(held.status, 202, 'the first event is held')
  const h = await held.json()
  assert.equal(h.held, true)
  assert.equal(h.bootstrapArmed, true)
  within(await pinState(c), 0, '(c) the held event\'s bootstrap')
  const doneC = await poll('(c) the held event applied', async () => { const s = await pinState(c); return s.meta.bootstrapped && !Object.keys(s.meta).some(k => k.startsWith('held:')) ? s : null }, 90000)
  console.log(`PASS fold7_h (c): ${c.slice(0, 10)}.. (${bigs[1].n} games) held (202), bootstrapArmed, the alarm fired ${doneC.fired}x over its pages, the held row applied ${Date.now() - tc} ms after the event (the cold time, pace as it stands)`)
  const first = await apply()
  assert.equal(first.status, 200, 'a bootstrapped actor applies')
  await apply()
  within(await pinState(c), 1000, '(c) the KV copy\'s coalesced alarm')
  console.log('PASS fold7_h (c): the KV copy\'s alarm within the second')
  }
  // (i) fold7_i: armed under a dead alarm, re-armed by the lever and by a held event.
  assert(bigs.length >= 4, 'two more seats over one page for fold7_i')
  for (const [n, how] of [[2, 'lever'], [3, 'held event']]) {
    const d = bigs[n].identity
    const dead = await (await actor(d).fetch('http://owed-actor/pin/dead-arm', { method: 'POST', body: JSON.stringify({ identity: d }) })).json()
    assert(dead.alarm > Date.now() + 365 * 86400000 * 50, `the dead arm is decades out: ${dead.alarm}`)
    const dgames = (await db.prepare('SELECT gameId FROM potparty_records WHERE identity=? UNION SELECT gameId FROM hopparty_records WHERE identity=? LIMIT 1').bind(d, d).all()).results.map(r => r.gameId)
    const td = Date.now()
    if (how === 'lever') {
      const r = await (await actor(d).fetch('http://owed-actor/bootstrap', { method: 'POST', body: JSON.stringify({ identity: d }) })).json()
      assert.equal(r.rearmed, true, `the lever re-armed: ${JSON.stringify(r)}`)
      assert.equal(r.armed, true)
    } else {
      const r = await actor(d).fetch('http://owed-actor/apply', { method: 'POST', body: JSON.stringify({ identity: d, games: dgames, kind: 'pot', seq: 11, atMs: Date.now(), tip: 900000 }) })
      assert.equal(r.status, 202, 'the event is held')
      assert.equal((await r.json()).bootstrapArmed, true)
    }
    within(await pinState(d), 0, `(i) the ${how}'s re-arm`)
    const doneD = await poll(`(i) the ${how}'s re-armed alarm fires and pages`, async () => { const s = await pinState(d); return s.meta.bootstrapped && !Object.keys(s.meta).some(k => k.startsWith('held:')) ? s : null }, 90000)
    console.log(`PASS fold7_i (${how}): ${d.slice(0, 10)}.. armed under a dead alarm, re-armed, ${doneD.fired} alarms fired, bootstrapped ${Date.now() - td} ms later`)
  }
  const rearmed = await db.prepare("SELECT value FROM ops_counters WHERE name='owed_actor_alarm_rearmed_total'").first()
  assert.equal(rearmed?.value, 2, 'counted owed_actor_alarm_rearmed_total')
} finally {
  // Under the tier's seal (`MINIFLARE_WORKERD_PATH`, workerd under sandbox-exec) `dispose()` never resolved here though
  // the runtime had exited (measured twice: the last PASS printed, node held by Miniflare's loopback listener). Every
  // assertion has run by now; the dispose is bounded and the process ends with the cell's verdict.
  await Promise.race([mf.dispose(), new Promise(r => setTimeout(r, 15000))])
  rmSync(scratch, { recursive: true, force: true })
}
process.exit(0)
