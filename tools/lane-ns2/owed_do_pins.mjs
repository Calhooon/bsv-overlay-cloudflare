import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { delimiter, resolve } from 'node:path'
import { existsSync, readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
// Use the same Miniflare installed alongside the tier's wrangler, no second runtime/version.
const wrangler = process.env.NS2_WRANGLER_PACKAGE ?? process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'set NS2_WRANGLER_PACKAGE (the tier does) or put the tier’s wrangler on PATH, so the pin uses its Miniflare')
const require = createRequire(wrangler)
const { Miniflare, convertV4MiniflareOptions } = require('miniflare')
// Miniflare 5 (wrangler 4.148) takes v4 options only through `convertV4MiniflareOptions`, which DROPS `d1Persist`
// silently (an empty D1: "no such table"); it persists every resource under `resourcePersistencePath`, the `v3`
// directory itself. Miniflare 4 (wrangler 4.54) has neither and takes `d1Persist`, the `v3/d1` directory.
const persist = dir => convertV4MiniflareOptions ? {resourcePersistencePath: dir} : {d1Persist: resolve(dir, 'd1')}
const asMiniflare = convertV4MiniflareOptions ?? (v => v)
const root = fileURLToPath(new URL('../../', import.meta.url))
const compatibilityDate = readFileSync(root + 'crates/low-app-layer/wrangler.toml', 'utf8').match(/^compatibility_date = "([^"]+)"/m)[1]
assert.equal(compatibilityDate, '2024-12-01', 'NOTE-C compatibility date must match the register evidence')
// The tier's fixture D1 as the seed loaded it (`D1_BUDGET_SEED_V3`, a copy of its `v3` taken before any worker ran),
// under the app layer's own database id. Loading the seed again here, in this one workerd, ran its SQLite out of
// memory under the board's triggers (lane W1-LB-674-NOMEM: Miniflare holds each statement's compiled program, its
// triggers' programs inside it, until its isolate's GC, and 102,000 statements in one process pass the heap).
const seedV3 = process.env.D1_BUDGET_SEED_V3
assert(seedV3 && existsSync(resolve(seedV3, 'd1')), 'D1_BUDGET_SEED_V3 names the copy of the tier\'s loaded v3')
const overlayDb = readFileSync(root + 'crates/low-app-layer/wrangler.toml', 'utf8').match(/binding = "OVERLAY_DB"[\s\S]*?database_id = "([^"]+)"/)[1]
const mf = new Miniflare(asMiniflare({name: "pin", ...persist(seedV3),
  modules: ['tools/lane-ns2/owed_do_pin_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm'].map(path => ({path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(root + path)})),
  modulesRoot: root,
  compatibilityDate, compatibilityFlags: ['nodejs_compat'],
  durableObjects: {BOARD_VIEW: {className: 'OwedPin', useSQLite: true}, AUTH_SESSIONS: 'AuthSessionStore'},
  d1Databases: {OVERLAY_DB: overlayDb},
  bindings: {AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: 'ci-internal-tok'},
}))
try {
  const db = await mf.getD1Database('OVERLAY_DB')
  const ns = await mf.getDurableObjectNamespace('BOARD_VIEW')
  const cleanup = ns.get(ns.idFromName('ns2-delete-all-compatibility'))
  const cleared = await (await cleanup.fetch('http://pin/pin/delete-all')).json()
  assert.deepEqual(cleared.keys, {}, 'NOTE-C deleteAll removes all keys')
  assert(cleared.alarm > Date.now(), 'NOTE-C deleteAll preserves the alarm at configured 2024-12-01 date')
  console.log('PASS NOTE-C real SQLite DO deleteAll: keys=0, alarm preserved (2024-12-01)')

  const identity = '02' + (299).toString(16).padStart(64, '0')
  const stub = ns.get(ns.idFromName(`owed-deferred:pin:${identity}`))
  const call = async path => stub.fetch('http://board-view' + path)
  const state = async () => (await call('/pin/state')).json()
  const arm = due => call(`/owed-deferred?identity=${identity}&due=${due}`)
  await db.prepare(`CREATE TRIGGER ns2_alarm_fault BEFORE INSERT ON owed_rows WHEN NEW.identity='${identity}' BEGIN SELECT RAISE(FAIL,'NS2 deterministic write fault'); END;`).run()
  assert.equal((await arm(Date.now() + 30000)).status, 200)
  const once = await (await call('/pin/alarm')).json()
  assert.equal(once.keys['owed-retry'], true, 'SHEDD-M1 faulted alarm carries a durable once latch')
  assert(once.alarm > Date.now(), 'SHED-L1 faulted alarm rearms next window')
  assert.equal(once.keys['owed-started'], undefined, 'SHED-L1 consumed claim cleared')
  assert.equal((await (await mf.dispatchFetch('http://app/health')).json()).owed.owed_recompute_deferred_faulted_total, 1, 'SHED-L1 fault counted once')
  // The next exhausted window lands on the same object/slot, retaining the retry latch.
  assert.equal((await arm(once.keys['owed-due'])).status, 200)
  const twice = await (await call('/pin/alarm')).json()
  assert.equal(twice.keys['owed-identity'], undefined, 'SHEDD-M1 faulted retry arms NOTHING; two faulted windows cannot stack chains')
  const duplicate = await (await call('/pin/duplicate-alarm')).json()
  assert.equal(duplicate.keys['owed-consumed-due'], twice.keys['owed-consumed-due'], 'NOTE-C duplicate old alarm preserves the consumed tombstone until its expiry')
  assert.equal(duplicate.keys['owed-identity'], undefined)
  const health = await (await mf.dispatchFetch('http://app/health')).json()
  assert.equal(health.owed.owed_recompute_deferred_abandoned_total, 1, 'SHEDD-M1 abandoned retry counted once')
  console.log('PASS SHEDD-M1 two faulted windows: one live retry then zero; abandoned=1')
  await db.exec('DROP TRIGGER ns2_alarm_fault;')
  const readFault = '02' + (296).toString(16).padStart(64, '0')
  const readStub = ns.get(ns.idFromName(`owed-deferred:pin:${readFault}`))
  const readCall = path => readStub.fetch('http://board-view' + path)
  await db.prepare(`CREATE TRIGGER ns2_reserved_read_fault BEFORE INSERT ON owed_rows WHEN NEW.identity='${readFault}' BEGIN SELECT RAISE(FAIL,'NS2 reserved read fault'); END;`).run()
  assert.equal((await readCall(`/owed-deferred?identity=${readFault}&due=${Date.now() + 30000}&read=1`)).status, 503)
  const readRetry = await (await readCall('/pin/state')).json()
  assert.equal(readRetry.keys['owed-retry'], true, 'read=1 fault arms one retry')
  assert.equal((await readCall(`/owed-deferred?identity=${readFault}&due=${readRetry.keys['owed-due']}&read=1`)).status, 503)
  const readStopped = await (await readCall('/pin/state')).json()
  assert.equal(readStopped.keys['owed-identity'], undefined, 'read=1 retry fault arms nothing')
  assert.equal((await (await mf.dispatchFetch('http://app/health')).json()).owed.owed_recompute_deferred_abandoned_total, 2, 'read=1 retry abandonment counted')
  await db.exec('DROP TRIGGER ns2_reserved_read_fault;')
  console.log('PASS SHEDD-L1 read=1 fault: one retry; retry fault: no arm, abandonment counted')

  // SHEDD-L1: production route -> DO -> a REAL refused storage.put, counted by the scheduling caller.
  const refused = '02' + (298).toString(16).padStart(64, '0')
  const request = path => mf.dispatchFetch('http://app' + path)
  const owedHealth = async () => (await (await request('/health')).json()).owed
  await db.prepare(`CREATE TRIGGER ns2_refused_arm_fault BEFORE INSERT ON owed_rows WHEN NEW.identity='${refused}' BEGIN SELECT RAISE(FAIL,'NS2 no snapshot'); END;`).run()
  for (let n = 0; n < 12; n++) assert.equal((await request(`/owed?identity=${refused}`)).status, 503, 'twelve ordinary walks fault before any snapshot')
  const armBefore = await owedHealth()
  await request(`/pin/refuse?identity=${refused}`)
  assert.equal((await mf.dispatchFetch('http://app/internal/pot-changed', {
    method: 'POST', headers: {'Content-Type': 'application/json', Authorization: 'Bearer ci-internal-tok'},
    body: JSON.stringify({outpoints: [{txid: 'cc' + (298).toString(16).padStart(62, '0'), vout: 0}]})
  })).status, 200)
  // The scheduler runs after the hook answer. Only health is polled, never another shed.
  let armAfter
  for (let n = 0; n < 100; n++) {
    armAfter = await owedHealth()
    if (armAfter.owed_recompute_arm_failed_total > armBefore.owed_recompute_arm_failed_total) break
    await new Promise(resolve => setTimeout(resolve, 20))
  }
  assert.equal(armAfter.owed_recompute_arm_failed_total, armBefore.owed_recompute_arm_failed_total + 1, 'SHED-L2 refused storage arm counted once')
  await db.exec('DROP TRIGGER ns2_refused_arm_fault;')
  await request('/pin/advance-clock')
  const repaired = await request(`/owed?identity=${refused}`)
  assert.equal(repaired.status, 200, 'SHED-L2 the last shed had no snapshot: own next-window read walks')
  assert.equal((await repaired.json()).rows.length, 1)
  assert.equal((await owedHealth()).recomputeBySource['read-first'], armAfter.recomputeBySource['read-first'] + 1, 'SHED-L2 next read runs exactly one walk')
  console.log('PASS SHEDD-L1 refused storage arm counted once; snapshotless own next-window read walks once')

  // NOTE-3: first read and alarm share the durable claim, even after twelve real D1 faults.
  const first = '02' + (297).toString(16).padStart(64, '0')
  await db.prepare(`CREATE TRIGGER ns2_first_read_fault BEFORE INSERT ON owed_rows WHEN NEW.identity='${first}' BEGIN SELECT RAISE(FAIL,'NS2 first read'); END;`).run()
  for (let n = 0; n < 12; n++) assert.equal((await request(`/owed?identity=${first}`)).status, 503)
  await db.exec('DROP TRIGGER ns2_first_read_fault;')
  const readBefore = await owedHealth()
  const reservedRead = await request(`/owed?identity=${first}`)
  assert.equal(reservedRead.status, 200, 'NOTE-3 snapshotless read spends the DO reservation')
  assert.equal((await reservedRead.json()).rows.length, 1)
  const readAfter = await owedHealth()
  assert.equal(readAfter.owed_recompute_deferred_total, readBefore.owed_recompute_deferred_total + 1, 'NOTE-3 reserved read walked once')
  assert.equal((await request(`/owed?identity=${first}`)).status, 200)
  assert.equal((await owedHealth()).owed_recompute_deferred_total, readAfter.owed_recompute_deferred_total, 'NOTE-3 second read spends no second reservation')
  await request(`/pin/route-alarm?identity=${first}`)
  assert.equal((await owedHealth()).owed_recompute_deferred_total, readAfter.owed_recompute_deferred_total, 'NOTE-3 alarm cannot spend the first read’s claim twice')
  console.log('PASS SHEDD-L1 snapshotless first read: reserved walk=1; second read=0; alarm=0')
  // Two genuinely exhausted ROUTE windows under a deterministic write fault must resolve one live actor.
  const stacked = '02' + (295).toString(16).padStart(64, '0')
  await db.prepare(`CREATE TRIGGER ns2_two_window_fault BEFORE INSERT ON owed_rows WHEN NEW.identity='${stacked}' BEGIN SELECT RAISE(FAIL,'NS2 two-window fault'); END;`).run()
  for (let window = 0; window < 2; window++) {
    for (let n = 0; n < 13; n++) assert.equal((await request(`/owed?identity=${stacked}`)).status, 503)
    const ids = await (await request(`/pin/object-ids?identity=${stacked}`)).json()
    assert.equal(ids.length, 1, 'SHEDD-M1 two exhausted route windows share ONE live actor, never window-named chains')
    if (window === 0) await request('/pin/advance-clock')
  }
  const abandonedBefore = (await owedHealth()).owed_recompute_deferred_abandoned_total
  await request(`/pin/route-alarm?identity=${stacked}`)
  assert.equal((await owedHealth()).owed_recompute_deferred_abandoned_total, abandonedBefore + 1, 'SHEDD-M1 the sole faulted carried retry is abandoned')
  await db.exec('DROP TRIGGER ns2_two_window_fault;')
  console.log('PASS SHEDD-M1 2 exhausted route windows (26 faults): live actors=1, faulted retry abandoned=1')


} finally { await mf.dispose() }
