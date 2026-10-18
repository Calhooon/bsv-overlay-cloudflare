// #638: execute exact native-pinned pages through D1, retaining rows_read (wrangler's local CLI drops it).
import { readFileSync, realpathSync } from 'node:fs'
import { createRequire } from 'node:module'
import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import assert from 'node:assert/strict'
const expected = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const require = createRequire(import.meta.url)
// Allow the deploy CLI's bundle to be checked independently of today's npx default.
const wranglerPackage = process.env.CURSOR_PIN_WRANGLER || 'wrangler'
let miniflarePath
try {
  if (process.env.CURSOR_PIN_WRANGLER) throw new Error('explicit wrangler bundle')
  miniflarePath = require.resolve('miniflare')
}
catch {
  const root = execFileSync('npm', ['root', '-g'], { encoding: 'utf8' }).trim()
  try {
    if (process.env.CURSOR_PIN_WRANGLER) throw new Error('explicit wrangler bundle')
    miniflarePath = createRequire(join(root, 'wrangler/package.json')).resolve('miniflare')
  }
  catch {
    // The same npx-only setup supported by the Makefile, when there is no global install.
    const cli = execFileSync('npm', ['exec', '--package', wranglerPackage, '--', 'which', 'wrangler'], { encoding: 'utf8' }).trim()
    miniflarePath = createRequire(realpathSync(cli)).resolve('miniflare')
  }
}
const { Miniflare, convertV4MiniflareOptions } = await import(miniflarePath)
const config = readFileSync(new URL('../../crates/low-app-layer/wrangler.toml', import.meta.url), 'utf8')
const id = config.match(/binding = "OVERLAY_DB"[\s\S]*?database_id = "([^"]+)"/)[1]
const script = 'export default { fetch() { return new Response("cursor pin") } }'
const mf = new Miniflare(convertV4MiniflareOptions
  ? convertV4MiniflareOptions({ resourcePersistencePath: join(process.argv[3], 'v3'), workers: [{ name: 'cursor-pin', modules: true, script, d1Databases: { DB: id } }] })
  : { modules: true, script, d1Databases: { DB: id }, d1Persist: join(process.argv[3], 'v3/d1') })
try {
  const db = await mf.getD1Database('DB')
  let max = 0
  for (const e of expected) {
    const a = await db.prepare(e.sql).all()
    assert.equal(a.success, true, e.name)
    // W1.6: a RANDOM-ordered read is compared as a set.
    // NS-COST-BAR-1: each row's keys sorted too (the native side's map is alphabetical, D1's in column order).
    const key = (rows) => e.unordered ? rows.map((r) => JSON.stringify(Object.fromEntries(Object.entries(r).sort()))).sort() : rows
    assert.deepEqual(key(a.results), key(e.rows), `${e.name}: identical ${e.unordered ? 'row set' : 'ordered rows'}`)
    assert.ok(Number.isInteger(a.meta.rows_read), `${e.name}: rows_read exists`)
    if (e.bound) console.log(`  ${e.name}: ${a.meta.rows_read} reads (ceiling ${e.ceiling})`)
    assert.ok(a.meta.rows_read <= e.ceiling, `${e.name}: ${a.meta.rows_read} reads > ${e.ceiling}`)
    if (!e.bound) max = Math.max(max, a.meta.rows_read)
  }
  console.log(`PASS createdAt cursor and the W1.6 bounds: ${expected.length} statements, identical rows, max reads ${max}/287`)
} finally { await mf.dispose() }
