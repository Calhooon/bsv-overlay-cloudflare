/** Replay the real file_verified SQL batches on local D1, including the source triggers. */
import { createRequire } from 'node:module'
import { readFileSync, writeFileSync } from 'node:fs'
import { resolve, dirname, join } from 'node:path'
import { tmpdir } from 'node:os'

const fixturePath = process.argv[2]
if (!fixturePath) throw new Error('usage: node owed_judge_filing.mjs <exported-fixture.json>')
const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'))
const require = createRequire(process.env.D1_BUDGET_WRANGLER_PACKAGE ?? join(process.cwd(), 'node_modules/wrangler/package.json'))
const { Miniflare, convertV4MiniflareOptions } = require('miniflare')
const state = process.env.D1_BUDGET_STATE ?? join(tmpdir(), 'owed-judge-filing')
const mf = new Miniflare(convertV4MiniflareOptions({ resourcePersistencePath: state, workers: [{
  name: 'owed-judge-filing', modules: true, script: 'export default { fetch() { return new Response("local D1 budget") } }',
  d1Databases: { DB: '00000000-0000-0000-0000-000000000893' },
}] }))
try {
  const db = await mf.getD1Database('DB')
  for (const sql of fixture.schema) await db.prepare(sql).run()
  for (let at = 0; at < fixture.seed.length; at += 100) await db.batch(fixture.seed.slice(at, at + 100).map(sql => db.prepare(sql)))
  const rows = []
  for (const batch of fixture.batches) {
    const landed = await db.batch(batch.map(({sql, binds}) => db.prepare(sql).bind(...binds)))
    rows.push(...landed.map((result, at) => ({sql:batch[at].sql,...result.meta})))
  }
  for (const row of rows) if (![row.rows_read, row.rows_written].every(Number.isFinite)) throw new Error('D1 did not return numeric row accounting')
  const got = {reads:rows.reduce((n,r)=>n+r.rows_read,0),writes:rows.reduce((n,r)=>n+r.rows_written,0),stmts:rows.length,maxReads:Math.max(...rows.map(r=>r.rows_read))}
  const stateRows = await db.prepare('SELECT COUNT(*) AS n FROM owed_rows WHERE identity IN (SELECT identity FROM potparty_records WHERE potTxid IN (SELECT potTxid FROM potparty_records WHERE identity=?1))').bind(fixture.identity).first()
  if (stateRows.n !== fixture.expectedRows) throw new Error(`filing did not judge both seats: ${JSON.stringify(stateRows)}`)
  const output = {scenario:'owed-judge-filing',got,rows}
  writeFileSync(join(dirname(resolve(fixturePath)), 'owed-judge-filing-measured.json'), JSON.stringify(output,null,2)+'\n')
  console.log(JSON.stringify({scenario:output.scenario,got}))
  if (process.env.D1_BUDGET_MEASURE !== '1') {
    const { ceiling } = JSON.parse(readFileSync(new URL('./ceilings.json',import.meta.url),'utf8')).scenarios[output.scenario]
    for (const [key,value] of Object.entries(got)) if (value > ceiling[key]) throw new Error(`${key} ${value} exceeds ${ceiling[key]}`)
  }
} finally { await mf.dispose() }
