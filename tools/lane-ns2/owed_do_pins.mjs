import { execFileSync } from 'node:child_process'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { delimiter, resolve } from 'node:path'
import { existsSync, readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
// Use the same Miniflare installed alongside the tier's wrangler, no second runtime/version.
const wrangler = process.env.NS2_WRANGLER_PACKAGE ?? process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'run through npx --package wrangler -c so the tier uses wrangler’s Miniflare')
const require = createRequire(wrangler)
const { Miniflare, convertV4MiniflareOptions = v => v } = require('miniflare')
const root = fileURLToPath(new URL('../../', import.meta.url))
const mf = new Miniflare(convertV4MiniflareOptions({name: "pin",
  modules: ['tools/lane-ns2/owed_do_pin_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm'].map(path => ({path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(root + path)})),
  modulesRoot: root,
  compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: {BOARD_VIEW: {className: 'OwedPin', useSQLite: true}, AUTH_SESSIONS: 'AuthSessionStore'},
  d1Databases: {OVERLAY_DB: 'ns2-pin'},
  bindings: {AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: 'ci-internal-tok'},
}))
try {
  const db = await mf.getD1Database('OVERLAY_DB')
  const statements = JSON.parse(execFileSync('python3', ['-c', `
import sqlite3,json,sys
buf=''; out=[]
for line in open(sys.argv[1]):
 if line.startswith('--'): continue
 buf+=line
 if sqlite3.complete_statement(buf): out.append(buf); buf=''
assert not buf.strip()
print(json.dumps(out))
`, process.env.D1_BUDGET_SEED], {maxBuffer: 64 * 1024 * 1024, timeout: 30000}))
  for (let n = 0; n < statements.length; n += 100) await db.batch(statements.slice(n, n + 100).map(s => db.prepare(s)))
  const ns = await mf.getDurableObjectNamespace('BOARD_VIEW')
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

} finally { await mf.dispose() }
