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

  // D2-OWED-FOLD-8: the deferred owed walk is retired (bsv-low #893: the read is one lookup, the judge runs at the
  // write, the belt asks the chain in the background), so its pins are DROPPED: SHEDD-M1 and SHED-L1 (a faulted alarm's
  // one retry, two faulted windows), SHEDD-L1 (the read=1 retry, a refused storage arm), NOTE-3 (the reserved
  // first-read walk) and the two exhausted route windows. What stays of it: a stale caller of the retired routes is
  // answered 410 and served nothing, and an actor the retired walk armed before the deploy clears its storage at its
  // alarm and arms nothing again.
  const identity = '02' + (299).toString(16).padStart(64, '0')
  const stub = ns.get(ns.idFromName(`owed-deferred:pin:${identity}`))
  const call = async path => stub.fetch('http://board-view' + path)
  for (const path of [`/owed-deferred?identity=${identity}&due=${Date.now() + 30000}`, `/owed-deferred-note?identity=${identity}`]) {
    assert.equal((await call(path)).status, 410, `${path}: the retired walk answers 410`)
  }
  const before = Date.now()
  const old = await (await call(`/pin/old-actor?identity=${identity}`)).json()
  assert.deepEqual(old.keys, {}, 'an old actor\'s alarm clears its storage')
  // deleteAll keeps an alarm at this compatibility date (NOTE-C, above): what stays is the one the old walk set
  // (30 s out), never a new arm; its next firing finds empty storage and clears again
  assert(old.alarm === null || old.alarm <= before + 31000, `and arms nothing new: alarm ${old.alarm}`)
  console.log('PASS D2-OWED-FOLD-8 the retired deferred walk: 410 to a stale caller; an old actor clears at its alarm')

} finally { await mf.dispose() }
