// #638: execute exact native-pinned pages through D1, retaining rows_read (wrangler's local CLI drops it).
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import assert from 'node:assert/strict'
const expected = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const require = createRequire(import.meta.url)
let miniflarePath
try { miniflarePath = require.resolve('miniflare') }
catch {
  const root = execFileSync('npm', ['root', '-g'], { encoding: 'utf8' }).trim()
  miniflarePath = createRequire(join(root, 'wrangler/package.json')).resolve('miniflare')
}
const { Miniflare } = await import(miniflarePath)
const config = readFileSync(new URL('../../crates/low-app-layer/wrangler.toml', import.meta.url), 'utf8')
const id = config.match(/binding = "OVERLAY_DB"[\s\S]*?database_id = "([^"]+)"/)[1]
const mf = new Miniflare({ modules: true, script: 'export default { fetch() { return new Response("cursor pin") } }', d1Databases: { DB: id }, d1Persist: join(process.argv[3], 'v3/d1') })
try {
  const db = await mf.getD1Database('DB')
  let max = 0
  for (const e of expected) {
    const a = await db.prepare(e.sql).all()
    assert.equal(a.success, true, e.name)
    assert.deepEqual(a.results, e.rows, `${e.name}: identical ordered rows`)
    assert.ok(Number.isInteger(a.meta.rows_read), `${e.name}: rows_read exists`)
    assert.ok(a.meta.rows_read <= e.ceiling, `${e.name}: ${a.meta.rows_read} reads > ${e.ceiling}`)
    max = Math.max(max, a.meta.rows_read)
  }
  console.log(`PASS createdAt cursor: ${expected.length} pages, identical rows, max reads ${max}/287`)
} finally { await mf.dispose() }
