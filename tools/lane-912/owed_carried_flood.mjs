// bsv-low #959 (lane D2-ACTOR-F-FOLD, the lens's LOW-2): f_cell, A FLOOD OF 32 NEW FILINGS FOR 32 IDENTITIES IS APPLIED
// FROM THE ROWS THE FRAME CARRIES, WITH NO D1 READ. On real workerd (Miniflare, the tier's pinned wrangler) with the
// production app layer, its records door and the production OwedActor (`owed_actor_alarm_worker.mjs`, the read-only
// wrapper: `/pin/state` reads the actor's `meta`, `/pin/answers` the answers of its `/apply` as the actor gave them),
// the `owed-events` queue bound as a PRODUCER only (its consumer never runs here: the direct call is the only road).
//   (1) 32 seats (the fixture's `carriedSeats`, each its own identity and game, real signed filings): each files a
//       first `collected` marker through the door (`POST /record`); its direct call reaches an actor not yet
//       bootstrapped, which holds it and bootstraps by its own alarms, then applies the held event (a cold full read:
//       the game is then WARM and closed);
//   (2) the flood: the 32 seats' `held` filings at once, each a NEW filing (a marker txid and a payout txid no row of
//       the record named before; seat 0's payout holds a stored BEEF of about 20 KB, the rest none): the door writes
//       and publishes (the carrier statement carries the row, never a closure: D2-ACTOR-F-FOLD-2), answers, and its
//       direct call after the answer reads the CLOSURE in its own batch (the record's rows of every mirrored family
//       for the txids the row names, at one high-water, a BEEF as its DECODED facts) and hands both to the actor;
//   (3) each actor's apply of its held filing answers `carried: true`, `recordCalls: 0`, `readMs: 0` (no D1 call:
//       the payout's keys came with the closure, `closureRounds` at least 1), and its stages; the cell prints the
//       door's `enqueue;dur` (its `Server-Timing`, the publish inside `publish_before_answer`) and answer times, the
//       apply and first visibility figures each answer gave;
//   (4) (the delta lens's LOW-2) it waits out the bars' minute, files seat 1's extra NEW filing (its warm actor's
//       apply flushes the isolate's pending samples, the flood's among them, and the health snapshot in their batch),
//       and asserts `/health.owedActor` SERVES non-null p95s
//       (`stages.{read,wait,judge,write}MsP95`, `classes.direct` with samples), printed beside the answers' figures.
// RED on 3223be64 (no closure carried): every held filing's apply makes one record call ("recordCalls 1").
// Reads the tier's exported fleet fixture (RECORD_DOOR_FIXTURE, as the #911 cell does). Every host is sealed.
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
assert.equal(fixture.carriedSeats?.length, 32, 'the fixture holds the 32 carried seats (export_record_door_fixture)')
const build = process.env.NS2_APP_BUILD ?? resolve(root, 'crates/low-app-layer/build')
const wrangler = process.env.NS2_WRANGLER_PACKAGE ?? process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'use the tier’s pinned wrangler/Miniflare')
const { Miniflare, convertV4MiniflareOptions } = createRequire(wrangler)('miniflare')
const scratch = mkdtempSync(resolve(process.env.TMPDIR ?? tmpdir(), 'owed-carried-flood-'))
const state = resolve(scratch, 'state')
const SEALED = 'http://127.0.0.1:9'
const TOKEN = 'ci-internal-tok'
const paths = ['tools/lane-912/owed_actor_alarm_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm']
const options = {
  name: 'owed-carried-flood', resourcePersistencePath: state, defaultPersistRoot: state, modulesRoot: root,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', build) : root + path) })),
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'BoardView', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore', OWED_ACTOR: { className: 'AlarmPin', useSQLite: true } },
  d1Databases: { OVERLAY_DB: '00000000-0000-0000-0000-000000000959' },
  kvNamespaces: { OWED_COPY: 'owed-copy-959' },
  // the queue BEHIND: a producer, no consumer here (the direct call is the only road of this flood)
  queueProducers: { OWED_EVENTS: { queueName: 'low-owed-events' } },
  // Phase B (the D1 judge on, the actor's rows served): the cell binds its own phase, whatever the committed render's
  bindings: { AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: TOKEN, COURIER_SINK: SEALED, ARCADE_URL: SEALED, CORROBORATOR_URL: SEALED, RELAY_URL: SEALED, DUAL_BROADCAST: 'off', OWED_D1_JUDGE: 'true', OWED_ACTOR_READ: 'true', OWED_CONSUMER_CONCURRENCY: '1', OWED_FLEET_COMPARE_PER_TICK: '0' },
  serviceBindings: { CHAINTRACKS: async request => {
    const path = new URL(request.url).pathname
    if (path === '/getPresentHeight') return Response.json({ status: 'success', value: 900000 })
    if (path === '/findChainTipHeaderHex' || path === '/findHeaderHexForHeight') return Response.json({ status: 'success', value: { height: 900000, hash: 'ab'.repeat(32), merkleRoot: 'cd'.repeat(32), time: Math.floor(Date.now() / 1000) } })
    return new Response('{}', { status: 404 })
  } },
}
const files = path => readdirSync(path, { withFileTypes: true }).flatMap(e => e.isDirectory() ? files(resolve(path, e.name)) : [resolve(path, e.name)])
const q = (xs, p) => { const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.ceil(p * s.length) - 1)] }
const fig = xs => `p50 ${q(xs, 0.5)} p95 ${q(xs, 0.95)} max ${Math.max(...xs)}`
const start = () => new Miniflare((convertV4MiniflareOptions ?? (v => v))(options))
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
  const actor = id => ns.get(ns.idFromName(id.toLowerCase()))
  const pinState = async id => (await actor(id).fetch('http://owed-actor/pin/state')).json()
  const answers = async id => (await (await actor(id).fetch('http://owed-actor/pin/answers')).json()).answers
  const poll = async (what, fn, ms) => {
    const end = Date.now() + ms
    do { const v = await fn(); if (v) return v; await new Promise(r => setTimeout(r, 25)) } while (Date.now() < end)
    assert.fail('timed out: ' + what)
  }
  const call = (path, init) => mf.dispatchFetch('http://app' + path, init)
  await call('/health') // cold schema work belongs to boot
  const file = async s => {
    const t = Date.now()
    const r = await call(`/record?kind=${s.kind}&identity=${s.identity}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(s.body) })
    const body = await r.json()
    assert.equal(r.status, 200, JSON.stringify(body))
    assert.deepEqual(body, s.answer, 'the door’s answer body is unchanged')
    const timing = r.headers.get('server-timing') ?? ''
    const enqueue = Number((timing.match(/enqueue;dur=(\d+)/) ?? [])[1])
    return { answerMs: Date.now() - t, enqueue, timing }
  }
  const seats = fixture.carriedSeats
  // (1) the first filings, one after another (each a cold apply, its actor bootstrapping by its own alarms)
  for (const s of seats) await file(s.first)
  const tb = Date.now()
  await poll('the 32 actors bootstrapped and their first filings applied', async () => {
    const all = await Promise.all(seats.map(async s => ({ st: await pinState(s.identity), a: await answers(s.identity) })))
    return all.every(x => x.st.meta.bootstrapped && Number(x.st.meta.judgedAtMs ?? 0) > 0)
  }, 150000)
  console.log(`f_cell: 32 seats bootstrapped and their first filings applied in ${Date.now() - tb} ms`)
  const before = new Map()
  for (const s of seats) before.set(s.identity, (await answers(s.identity)).length)
  // (2) the flood: the 32 held filings at once
  assert(seats.some(s => s.payBeefBytes > 15000), 'one seat’s payout holds a realistic stored BEEF')
  const t0 = Date.now()
  const doors = await Promise.all(seats.map(s => file(s.held)))
  const answeredMs = Date.now() - t0
  const enq = doors.map(d => d.enqueue)
  assert(enq.every(Number.isFinite), `every door answer carries enqueue;dur: ${JSON.stringify(doors.map(d => d.timing))}`)
  console.log(`f_cell: the door under the flood: enqueue;dur ms ${fig(enq)}; answer ms ${fig(doors.map(d => d.answerMs))}`)
  // (3) each actor's apply of its held filing
  const got = await poll('the 32 held filings applied', async () => {
    const out = await Promise.all(seats.map(async s => (await answers(s.identity)).slice(before.get(s.identity)).find(a => a.applied && (a.appliedGames ?? []).includes(s.game))))
    return out.every(Boolean) && out
  }, 20000)
  const carried = got.filter(a => a.carried).length
  const calls = got.map(a => a.recordCalls ?? -1)
  const read = got.map(a => a.readMs), wait = got.map(a => a.waitMs ?? 0), judge = got.map(a => a.judgeMs), write = got.map(a => a.writeMs), outside = got.map(a => a.outsideMs ?? 0)
  const apply = got.map(a => a.readMs + (a.waitMs ?? 0) + a.judgeMs + a.writeMs + (a.outsideMs ?? 0))
  const visible = got.map(a => a.at - t0)
  console.log(`f_cell: the doors answered the 32 held filings in ${answeredMs} ms; applies carried ${carried}/32, record calls ${JSON.stringify([...new Set(calls)])}, closure rounds ${JSON.stringify([...new Set(got.map(a => a.closureRounds))])}`)
  console.log(`f_cell: stages ms: read ${fig(read)}; wait ${fig(wait)}; judge ${fig(judge)}; write ${fig(write)}; outside ${fig(outside)}`)
  console.log(`f_cell: apply ms ${fig(apply)}; first visibility ms (the answer's time less the flood's start) ${fig(visible)}`)
  assert.equal(carried, 32, `every held filing took the carried path: ${JSON.stringify(got)}`)
  assert(calls.every(c => c === 0), `no apply made a record call: ${JSON.stringify(calls)}`)
  assert(read.every(r => r === 0), `readMs 0 on the warm path: ${JSON.stringify(read)}`)
  assert(got.every(a => a.closureRounds >= 1), 'each new filing’s keys came with its closure')
  // the payout's keys are in each mirror at the closure's high-water (its per-key bound)
  for (const s of seats.slice(0, 4)) {
    const rows = await (await actor(s.identity).fetch('http://owed-actor/pin/applied?kind=read-key')).json()
    assert(rows.rows.some(r => r.game === s.pay), `${s.identity}: the payout read at the closure's high-water`)
  }
  // (4) the served stages (the delta lens's LOW-2): the fleet's bars are flushed at most once a minute an isolate and
  // the health snapshot at most once a minute fleet-wide, so the cell waits the minute out, files one more filing (its
  // direct pass flushes the isolate's pending samples, the flood's among them, and the snapshot in their batch), and
  // asserts the SERVED figures are numbers
  const extra = seats.find(s => s.extra)?.extra
  assert(extra, 'the fixture holds seat 1’s extra filing')
  await new Promise(r => setTimeout(r, 62000))
  await file(extra)
  // the actor's flush writes the class rows and, in their batch, the health snapshot (at most once a minute
  // fleet-wide: the last was the bootstrap's, a minute ago)
  await poll('the direct class’s bars flushed', async () => {
    const r = await db.prepare("SELECT cursor FROM owed_belt_state WHERE name='owed-actor:bars:direct'").first()
    return r && JSON.parse(r.cursor).apply?.length > 0
  }, 20000)
  let last = null
  const health = await poll('/health.owedActor serves the flood’s stages', async () => {
    const h = await (await call('/health')).json()
    last = h.owedActor
    const st = h.owedActor?.stages ?? {}
    const d = h.owedActor?.classes?.direct ?? {}
    return ['readMsP95', 'waitMsP95', 'judgeMsP95', 'writeMsP95'].every(k => typeof st[k] === 'number' && typeof d[k] === 'number') && d.applySamples > 0 && h
  }, 70000).catch(e => { console.log(`f_cell: last /health.owedActor ${JSON.stringify(last)}`); throw e })
  const st = health.owedActor.stages
  console.log(`f_cell: SERVED /health.owedActor.stages ${JSON.stringify(st)}; classes.direct ${JSON.stringify(health.owedActor.classes.direct)}; record ${JSON.stringify({ enqueueMsP50: health.record?.enqueueMsP50, enqueueMsP95: health.record?.enqueueMsP95 })}`)
  console.log(`PASS f_cell: 32 new filings for 32 identities (one payout a ~20 KB stored BEEF) applied from their carried rows and closures with no D1 read (record calls 0, readMs 0); door enqueue ${fig(enq)} ms; apply ${fig(apply)} ms, first visibility ${fig(visible)} ms; served stages read p95 ${st.readMsP95}, judge p95 ${st.judgeMsP95}`)
} finally {
  if (mf) await Promise.race([mf.dispose(), new Promise(r => setTimeout(r, 15000))])
  rmSync(scratch, { recursive: true, force: true })
}
process.exit(0)
