/** #906: production WASM hooks and the singleton's real alarm on workerd D1. */
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { delimiter, resolve } from 'node:path'
import { cpSync, existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
const root = fileURLToPath(new URL('../../', import.meta.url))
const wrangler = process.env.NS2_WRANGLER_PACKAGE ?? process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'use the tier’s pinned wrangler/Miniflare')
const { Miniflare, convertV4MiniflareOptions } = createRequire(wrangler)('miniflare')
const appBuild = process.env.NS2_APP_BUILD ?? root + 'crates/low-app-layer/build'
const paths = ['tools/lane-ns2/owed_do_pin_worker.mjs', 'crates/low-app-layer/build/worker/shim.mjs', 'crates/low-app-layer/build/index.js', 'crates/low-app-layer/build/index_bg.wasm']
const seed = process.env.D1_BUDGET_SEED_V3
const scratch = mkdtempSync(resolve(tmpdir(), 'owed-cadence-'))
const state = resolve(scratch, 'state')
if (seed) cpSync(seed, state, { recursive: true })
const persist = seed ? (convertV4MiniflareOptions ? { resourcePersistencePath: state } : { d1Persist: resolve(state, 'd1') }) : {}
const mf = new Miniflare((convertV4MiniflareOptions ?? (v => v))({
  name: 'cadence', ...persist,
  modules: paths.map(path => ({ path: root + path, type: path.endsWith('.wasm') ? 'CompiledWasm' : 'ESModule', contents: readFileSync(path.includes('/build/') ? path.replace('crates/low-app-layer/build', appBuild) : root + path) })),
  modulesRoot: root, compatibilityDate: '2024-12-01', compatibilityFlags: ['nodejs_compat'],
  durableObjects: { BOARD_VIEW: { className: 'OwedPin', useSQLite: true }, AUTH_SESSIONS: 'AuthSessionStore' },
  d1Databases: { OVERLAY_DB: readFileSync(root + 'crates/low-app-layer/wrangler.toml', 'utf8').match(/binding = "OVERLAY_DB"[\s\S]*?database_id = "([^"]+)"/)[1] },
  bindings: { AUTH_ENFORCE: 'false', SESSION_LANE: 'false', INTERNAL_TOKEN: 'ci-internal-tok' },
  serviceBindings: { CHAINTRACKS: async request => {
    const url = new URL(request.url)
    if (url.pathname === '/findChainTipHeaderHex' || url.pathname === '/findHeaderHexForHeight') return Response.json({ status: 'success', value: { height: 900000, hash: 'ab'.repeat(32), merkleRoot: 'cd'.repeat(32), time: Math.floor(Date.now() / 1000) } })
    if (url.pathname === '/getPresentHeight') return Response.json({ status: 'success', value: 900000 })
    return new Response('{}', { status: 404 })
  } },
}))
try {
  const db = await mf.getD1Database('OVERLAY_DB')
  if (process.env.OWED_CADENCE_SCHEMA) {
    for (const sql of JSON.parse(readFileSync(process.env.OWED_CADENCE_SCHEMA, 'utf8'))) await db.prepare(sql).run()
  }
  const run = (sql, ...binds) => db.prepare(sql).bind(...binds).run()
  const one = (sql, ...binds) => db.prepare(sql).bind(...binds).first()
  const id = '02' + '91'.repeat(32), other = '03' + '92'.repeat(32), pot = '93'.repeat(32), spend = '94'.repeat(32), game = '95'.repeat(32)
  const now = Date.now()
  await run(`INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt,lockKind,paramsDecoded,pubA,pubB,pubTower,payPkhA,payPkhB,rakePkh,stakeA,stakeB,feeSats,recoveryHeight,potSats,verdict,verdictTxid,settleSigners,spenderFinal) VALUES (?,0,1,?,0,?,'covenant',1,?,?,?, ?,?,?,1000,1000,1,899900,2000,'winner-a',?,'coop',1)`, pot, spend, Math.floor(now / 1000), id, other, id, '11'.repeat(20), '22'.repeat(20), '33'.repeat(20), spend)
  await run(`INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,seatSettlePubkey,seatSigHex,sigValid) VALUES (?,?,?,?,0,899900,'00',?,0,?,?,'00',1)`, id, other, game, pot, '96'.repeat(32), Math.floor(now / 1000), id)
  await run('DELETE FROM owed_dirty')
  const hook = async () => {
    const response = await mf.dispatchFetch('http://app/internal/pot-changed', { method: 'POST', headers: { authorization: 'Bearer ci-internal-tok', 'content-type': 'application/json' }, body: JSON.stringify({ outpoints: [{ txid: pot, vout: 0 }] }) })
    assert.equal(response.status, 200, await response.clone().text())
    assert.equal((await response.json()).announce, 'detached')
  }
  // Start with the unconfirmed source write, and let its own hook produce the first stored judgement.
  await run('UPDATE pot_records SET spentConfirmed=0 WHERE txid=?', pot)
  await hook()
  const before = await one('SELECT family,facts,reason,claimable,judgedAtMs FROM owed_rows WHERE identity=? AND gameId=?', id, game)
  assert(before, 'the attributed seat is judged')
  assert.notEqual(before.claimable, 1, JSON.stringify(before))
  assert.equal(JSON.parse(before.facts).spentConfirmed, false)
  await run('UPDATE pot_records SET spentConfirmed=1,spentHeight=899999 WHERE txid=?', pot)
  assert((await one('SELECT COUNT(*) AS n FROM owed_dirty WHERE identity=?', id)).n > 0, 'overlay latch raised its trigger mark')
  await hook()
  const after = await one('SELECT family,facts,reason,claimable,judgedAtMs FROM owed_rows WHERE identity=? AND gameId=?', id, game)
  assert.equal(after.claimable, 1, 'the latch is judged within this one pot-changed hook, without a word frame: ' + JSON.stringify(after))
  assert(after.judgedAtMs >= before.judgedAtMs)
  assert.equal((await one('SELECT COUNT(*) AS n FROM owed_dirty WHERE identity=?', id)).n, 0)
  console.log('PASS #906 overlay spentConfirmed trigger judged in one pot-changed hook; announce detached')

  await run('DELETE FROM owed_dirty')
  await run('UPDATE owed_rows SET beltDueAtMs=NULL')
  const plant = async () => run(`WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<499) INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT printf('cadence%04d',i),printf('game%04d',i),'pin','pin',? FROM n`, now - 301000)
  await plant()
  const health = await (await mf.dispatchFetch('http://app/health')).json()
  assert.equal(health.owed.dirty.count, 500)
  assert(health.owed.dirty.oldestMarkAgeMs >= 301000)
  const ns = await mf.getDurableObjectNamespace('BOARD_VIEW')
  const stub = ns.get(ns.idFromName('owed-belt:v1'))
  const alarm = async () => {
    const r = await stub.fetch('http://pin/pin/belt-alarm')
    assert.equal(r.status, 200, await r.clone().text())
    const b = await r.json()
    assert.equal(b.drain.error, undefined, JSON.stringify(b))
    assert(b.drain.subjects <= 64)
    assert(Number.isFinite(b.phases['owed.drain']) && Number.isFinite(b.phases['owed.belt_tick']))
    assert(b.wallMs < 30000)
    return b.drain.subjects
  }
  let drained = 0
  for (let tick = 1; tick <= 8; tick++) {
    drained += await alarm()
    const left = (await one("SELECT COUNT(*) AS n FROM owed_dirty WHERE source='pin'")).n
    assert.equal(left, Math.max(500 - 64 * tick, 0), `tick ${tick}: bounded progress`)
  }
  assert.equal(drained, 500)
  console.log('PASS #906 500 marks drained in eight 120 s alarm ticks (64/tick), alarm drain and belt phases metered')
  // Force an alarm and scheduled pass to contend for the same marks on the same actor.
  await run("DELETE FROM owed_state WHERE identity LIKE 'cadence%'")
  await plant()
  const pair = await Promise.all([alarm(), stub.fetch('http://pin/owed-drain').then(async r => { assert.equal(r.status, 200); return (await r.json()).subjects })])
  assert.equal(pair[0] + pair[1], 128)
  assert.equal((await one("SELECT MAX(recomputeSeq) AS n FROM owed_state WHERE identity LIKE 'cadence%'")).n, 1, 'one judgement per subject despite overlapping pass and alarm')
  assert.equal((await one("SELECT COUNT(*) AS n FROM owed_dirty WHERE source='pin'")).n, 372)
  console.log('PASS #906 scheduled drain and alarm serialize: 128 distinct subjects, no double judgement')
} finally { await mf.dispose(); rmSync(scratch, { recursive: true, force: true }) }
