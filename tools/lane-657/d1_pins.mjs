import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { delimiter, resolve } from 'node:path'
import { existsSync, readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
// Same runtime resolution as tools/lane-ns2/owed_do_pins.mjs.
const wrangler = process.env.PATH.split(delimiter).map(p => resolve(p, '../wrangler/package.json')).find(existsSync)
assert(wrangler, 'run: npx --package wrangler@4.150.0 -c "node tools/lane-657/d1_pins.mjs"')
const require = createRequire(wrangler)
const { Miniflare, convertV4MiniflareOptions = v => v } = require('miniflare')
const mf = new Miniflare(convertV4MiniflareOptions({
  modules: true, script: readFileSync(fileURLToPath(new URL('./d1_worker.mjs', import.meta.url)), 'utf8'),
  compatibilityDate: '2024-12-01', d1Databases: { DB: 'atomic-spike-fixture' },
}))
const batch = async statements => (await mf.dispatchFetch('http://pin/', {
  method: 'POST', body: JSON.stringify({ statements }),
})).json()
const ok = async statements => {
  const result = await batch(statements)
  assert.equal(result.error, undefined, JSON.stringify(result.error))
  return result.results
}
try {
  console.log('runtime', require('miniflare/package.json').version, 'wrangler', require(wrangler).version)
  if (process.argv.includes('--budget') || process.argv.includes('--red-budget')) {
    for (const budget of [{ batches: 1, width: 1001 }, { batches: 11, width: 100 }, { batches: 1001, width: 1 }]) {
      const result = await (await mf.dispatchFetch('http://pin/', {
        method: 'POST', body: JSON.stringify({ budget }),
      })).json()
      console.log(JSON.stringify({ budget, result }))
      if (process.argv.includes('--red-budget')) assert(result.error, 'deliberately expected local enforcement at 1000 queries')
      assert.equal(result.error, null, 'local behavior changed: inspect accounting before changing this pin')
      assert.equal(result.completedStatements, budget.batches * budget.width)
      assert.equal(result.completedBatches, budget.batches)
    }
    console.log('PASS local limit NOT enforced; hosted 1 versus N remains UNVERIFIED')
  } else {
    await ok([
      'CREATE TABLE effects (id INTEGER PRIMARY KEY)',
      'CREATE TABLE held (id INTEGER PRIMARY KEY, spent INTEGER NOT NULL)',
      'CREATE TABLE admission_guard (ok INTEGER NOT NULL CONSTRAINT atomic_admission_precondition CHECK (ok = 1))',
      'CREATE TABLE trigger_guard (ok INTEGER NOT NULL)',
      "CREATE TRIGGER require_held BEFORE INSERT ON trigger_guard WHEN NEW.ok != 1 BEGIN SELECT RAISE(ABORT, 'atomic_admission_precondition'); END",
    ])
    const effects = async () => (await ok(['SELECT * FROM effects ORDER BY id']))[0].results
    const failedBatch = async (guard, marker = /CHECK constraint failed: atomic_admission_precondition/) => {
      const result = await batch(['INSERT INTO effects VALUES (1)', guard, 'INSERT INTO effects VALUES (3)'])
      assert(result.error, 'the guard must FAIL, not merely change zero rows')
      assert.equal(result.error.name, 'Error')
      assert.match(result.error.message, marker)
      assert.equal(result.error.cause.message, result.error.message.replace(/^D1_ERROR: /, ''))
      console.log(JSON.stringify(result.error))
      const rows = await effects()
      if (process.argv.includes('--red-rollback')) assert.deepEqual(rows, [{ id: 1 }], 'deliberately wrong: first statement survived')
      assert.deepEqual(rows, [], 'neither statement one nor three may remain')
      for (const table of ['admission_guard', 'trigger_guard']) assert.deepEqual((await ok([`SELECT * FROM ${table}`]))[0].results, [])
    }
    await failedBatch('INSERT INTO admission_guard VALUES (0)')
    // WHERE false inserts nothing: neither CHECK nor NOT NULL sees a row.
    await ok(['INSERT INTO effects VALUES (1)',
      'INSERT INTO admission_guard SELECT 0 WHERE EXISTS (SELECT 1 FROM held WHERE id = 7 AND spent = 0)',
      'INSERT INTO effects VALUES (3)'])
    assert.deepEqual(await effects(), [{ id: 1 }, { id: 3 }])
    await ok(['DELETE FROM effects WHERE id IN (1, 3)'])
    const check = 'INSERT INTO admission_guard SELECT EXISTS (SELECT 1 FROM held WHERE id = 7 AND spent = 0)'
    const scalar = 'INSERT INTO admission_guard VALUES ((SELECT 1 FROM held WHERE id = 7 AND spent = 0))'
    const trigger = 'INSERT INTO trigger_guard SELECT EXISTS (SELECT 1 FROM held WHERE id = 7 AND spent = 0)'
    const guards = [
      [check, /CHECK constraint failed: atomic_admission_precondition/],
      [scalar, /NOT NULL constraint failed: admission_guard.ok/],
      [trigger, /atomic_admission_precondition: SQLITE_CONSTRAINT/],
    ]
    for (const args of guards) await failedBatch(...args)
    await ok(['INSERT INTO held VALUES (7, 1)'])
    for (const args of guards) await failedBatch(...args)
    const outside = await batch(["SELECT RAISE(ABORT, 'atomic_admission_precondition')"])
    assert(outside.error)
    console.log('RAISE outside trigger', JSON.stringify(outside.error))
    await ok(['UPDATE held SET spent = 0 WHERE id = 7'])
    for (const guard of [check, scalar, trigger]) {
      await ok(['INSERT INTO effects VALUES (1)', guard, 'INSERT INTO effects VALUES (3)'])
      assert.deepEqual(await effects(), [{ id: 1 }, { id: 3 }])
      await ok(['DELETE FROM effects WHERE id IN (1, 3)', 'DELETE FROM admission_guard WHERE ok = 1', 'DELETE FROM trigger_guard WHERE ok = 1'])
    }
    console.log('PASS rollback, zero-row non-guard, CHECK/NOT NULL/trigger false and true guards')
  }
} finally {
  await mf.dispose()
}
