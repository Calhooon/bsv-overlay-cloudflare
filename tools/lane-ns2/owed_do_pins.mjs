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
  // The next exhausted window lands on the same object/slot, retaining the retry latch.
  assert.equal((await arm(once.keys['owed-due'])).status, 200)
  const twice = await (await call('/pin/alarm')).json()
  assert.equal(twice.keys['owed-identity'], undefined, 'SHEDD-M1 faulted retry arms NOTHING; two faulted windows cannot stack chains')
  const health = await (await mf.dispatchFetch('http://app/health')).json()
  assert.equal(health.owed.owed_recompute_deferred_abandoned_total, 1, 'SHEDD-M1 abandoned retry counted once')
  console.log('PASS SHEDD-M1 two faulted windows: one live retry then zero; abandoned=1')
  await db.exec('DROP TRIGGER ns2_alarm_fault;')
} finally { await mf.dispose() }
