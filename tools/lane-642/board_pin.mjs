// bsv-low #642: replay every statement a /leaderboard:view page request runs (the shipped pipeline's, emitted by
// `lb642_emit_the_d1_cell`) through local D1 and hold its rows_read: identical ordered rows, each statement under its
// ceiling, each request under the per-request ceiling. The whole-board read's statements are replayed for the record.
// bsv-low #674 fold (MEDIUM-1): a request marked `flat` must read the same rows on every board the run replays; the
// third argument names the file that carries the first board's figure to the next.
// Usage: node board_pin.mjs <expected.json> <persisted D1 dir> [<flat figures file>]
import { existsSync, readFileSync, realpathSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import assert from 'node:assert/strict'
const expected = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const require = createRequire(import.meta.url)
let miniflarePath
try {
  miniflarePath = require.resolve('miniflare')
} catch {
  const root = execFileSync('npm', ['root', '-g'], { encoding: 'utf8' }).trim()
  try {
    miniflarePath = createRequire(join(root, 'wrangler/package.json')).resolve('miniflare')
  } catch {
    const cli = execFileSync('npm', ['exec', '--package', 'wrangler', '--', 'which', 'wrangler'], { encoding: 'utf8' }).trim()
    miniflarePath = createRequire(realpathSync(cli)).resolve('miniflare')
  }
}
const { Miniflare, convertV4MiniflareOptions } = await import(miniflarePath)
const config = readFileSync(new URL('../../crates/low-app-layer/wrangler.toml', import.meta.url), 'utf8')
const id = config.match(/binding = "OVERLAY_DB"[\s\S]*?database_id = "([^"]+)"/)[1]
const script = 'export default { fetch() { return new Response("board pin") } }'
const mf = new Miniflare(convertV4MiniflareOptions
  ? convertV4MiniflareOptions({ resourcePersistencePath: join(process.argv[3], 'v3'), workers: [{ name: 'board-pin', modules: true, script, d1Databases: { DB: id } }] })
  : { modules: true, script, d1Databases: { DB: id }, d1Persist: join(process.argv[3], 'v3/d1') })
try {
  const db = await mf.getD1Database('DB')
  for (const req of expected.requests) {
    let total = 0
    let max = 0
    for (const [i, s] of req.statements.entries()) {
      const a = await db.prepare(s.sql).all()
      assert.equal(a.success, true, `${req.name} #${i}`)
      assert.deepEqual(a.results, s.rows, `${req.name} #${i}: identical ordered rows`)
      assert.ok(Number.isInteger(a.meta.rows_read), `${req.name} #${i}: rows_read exists`)
      if (req.ceiling !== null) {
        assert.ok(a.meta.rows_read <= req.statementCeiling, `${req.name} #${i}: ${a.meta.rows_read} reads > ${req.statementCeiling}: ${s.sql.slice(0, 120)}`)
      }
      total += a.meta.rows_read
      max = Math.max(max, a.meta.rows_read)
    }
    if (req.ceiling !== null) {
      assert.ok(total <= req.ceiling, `${req.name}: ${total} reads > ${req.ceiling}`)
    }
    if (req.flat && process.argv[4]) {
      const flat = existsSync(process.argv[4]) ? JSON.parse(readFileSync(process.argv[4], 'utf8')) : {}
      if (flat[req.name] === undefined) {
        flat[req.name] = { wins: expected.wins, reads: total }
        writeFileSync(process.argv[4], JSON.stringify(flat))
      } else {
        assert.equal(total, flat[req.name].reads, `${req.name}: ${total} reads at ${expected.wins} wins, ${flat[req.name].reads} at ${flat[req.name].wins}: not flat`)
        console.log(`FLAT ${req.name}: ${total} reads at ${flat[req.name].wins} and ${expected.wins} wins`)
      }
    }
    // The whole-board read is the RED side: past the page ceiling on the same board.
    if (req.mustExceed !== undefined) {
      assert.ok(total > req.mustExceed, `${req.name}: ${total} reads, expected past ${req.mustExceed}`)
    }
    console.log(`${req.ceiling === null ? 'RECORD' : 'PASS'} ${expected.wins} wins, ${req.name}: ${req.statements.length} statements, reads ${total} (largest statement ${max})${req.ceiling === null ? '' : `, ceilings ${req.ceiling} / ${req.statementCeiling}`}`)
  }
} finally { await mf.dispose() }
