#!/usr/bin/env node
/**
 * bsv-low #649 N3 (the delta lens): the parked set's D1 work on a POPULATED table, in the budget tier
 * (`make ci-d1-budget`). The overlay's local D1 is seeded with 10,000 parked pot rows; then the shipped code runs
 * one step at a time through `POST /internal/change-park` (bearer INTERNAL_TOKEN), and each answer's
 * `Server-Timing: d1;desc="reads=N writes=M stmts=K"` is held against its ceiling (`ceilings.json`, by name):
 *
 *  1. `health`: the gauge, each count stopped at its cap (256).
 *  2. `park` of a NEW key on a full table (10,000 rows past the 2,048 ceiling): refused, kept in memory; the
 *     ceiling's read stops at the ceiling.
 *  3. `park` of a HELD key: it re-parks (a new intent token for its generation).
 *  4. `drain`: 128 eligible rows read through the drain's index (no APP_LAYER_URL here: no POST leaves, each
 *     read row is refused and backed off), the exhausted sweep.
 *  5. The drain's statement, read verbatim out of the Rust source, under EXPLAIN QUERY PLAN on the same local
 *     D1: the drain's index, no temp B-tree.
 * On `1516ed5` there is no lever (404) and the drain's plan is a temp B-tree: RED.
 *
 *   node tools/lane-e649/change_park_budget_ci.mjs <overlay base> <overlay --persist-to dir>
 *   D1_BUDGET_MEASURE=1 ...   prints the measured figures and enforces nothing
 *
 * Exit 0 = every expectation held.
 */
import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const OVERLAY = process.argv[2] ?? 'http://127.0.0.1:8801'
const STATE = process.argv[3]
const TOKEN = process.env.D1_BUDGET_INTERNAL_TOKEN ?? 'ci-internal-tok'
const MEASURE = process.env.D1_BUDGET_MEASURE === '1'
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
const CEILINGS = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url), 'utf8'))
if (!STATE) {
  console.error('usage: change_park_budget_ci.mjs <overlay base> <overlay --persist-to dir>')
  process.exit(2)
}

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}

function d1(sql) {
  for (let attempt = 1; ; attempt++) {
    try {
      const out = execFileSync(
        'npx',
        ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json', '--command', sql],
        { cwd: CRATE, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
      )
      return JSON.parse(out)[0]?.results ?? []
    } catch (e) {
      if (attempt >= 4) throw new Error(`wrangler d1 execute failed: ${e.stderr ?? e.message}`)
    }
  }
}

/** A `pub const NAME: &str = "...";` of the Rust source, as the binary holds it. */
function shipped(name) {
  const src = readFileSync(CRATE + 'src/change_park.rs', 'utf8')
  const at = src.indexOf(`pub const ${name}: &str =`)
  if (at < 0) throw new Error(`no ${name} in change_park.rs`)
  const open = src.indexOf('"', at)
  let i = open + 1
  while (!(src[i] === '"' && src[i - 1] !== '\\')) i++
  return src.slice(open + 1, i).replace(/\\\n\s*/g, '').replace(/\\"/g, '"')
}

function segment(header) {
  for (const seg of (header ?? '').split(',')) {
    const m = seg.trim().match(/^d1;desc="reads=(\d+) writes=(\d+) stmts=(\d+)/)
    if (m) return { reads: Number(m[1]), writes: Number(m[2]), stmts: Number(m[3]) }
  }
  return null
}

async function lever(body, token = TOKEN) {
  const r = await fetch(OVERLAY + '/internal/change-park', {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', Connection: 'close' },
    body: JSON.stringify(body),
  })
  const text = await r.text()
  let json = null
  try {
    json = JSON.parse(text)
  } catch {}
  return { status: r.status, d1: segment(r.headers.get('server-timing')), json, text }
}

function check(name, got) {
  const c = CEILINGS.scenarios[name]
  if (MEASURE) return lines.push(`MEASURE ${name}: ${JSON.stringify(got)}`)
  if (!got) return expect(false, `${name}: the answer carries its d1 segment`, 'no d1 Server-Timing segment')
  const over = ['reads', 'writes', 'stmts'].filter((k) => got[k] > c.ceiling[k])
  expect(over.length === 0, `${name}: ${JSON.stringify(got)} under ${JSON.stringify(c.ceiling)}`, `over on ${over.join(', ')}`)
}

// 0. The bearer.
const unauth = await lever({ op: 'health' }, 'wrong')
expect(unauth.status === 401, 'the lever refuses a wrong bearer (401)', `got ${unauth.status}: ${unauth.text}`)
// the table exists (the lever runs the migrations)
const h0 = await lever({ op: 'health' })
expect(h0.status === 200 && h0.json?.pot === 0, 'the parked set is served empty', `${h0.status} ${h0.text}`)

// The fixture: 10,000 parked pot rows, eligible, oldest first.
d1('DELETE FROM change_parked')
d1(
  "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 9999) " +
    "INSERT INTO change_parked (kind, item, parked_at_ms, next_attempt_at_ms) " +
    "SELECT 'pot', printf('%064x:0', i + 1), 1000 + i, 1000 + i FROM n",
)
const seeded = d1('SELECT COUNT(*) AS n FROM change_parked')[0]?.n
expect(seeded === 10000, 'the fixture holds 10,000 parked rows', `got ${seeded}`)

// 1. The gauge.
const h = await lever({ op: 'health' })
expect(h.json?.pot === 256 && h.json?.capped === 256, 'the gauge stops at its cap (256)', h.text)
check('change-park-health-10000', h.d1)

// 2. A NEW key on a full table.
const fresh = 'ee'.repeat(32) + ':0'
const p1 = await lever({ op: 'park', items: [fresh] })
expect(p1.status === 200 && p1.json?.keptInMemory === 1, 'a new key on a full table is refused and kept in memory', p1.text)
expect(d1(`SELECT COUNT(*) AS n FROM change_parked WHERE item = '${fresh}'`)[0]?.n === 0, 'and not parked', 'parked past the ceiling')
check('change-park-park-new-full', p1.d1)

// 3. A HELD key re-parks.
const held = '0'.repeat(63) + '1:0'
const p2 = await lever({ op: 'park', items: [held] })
const gen = d1(`SELECT generation AS g FROM change_parked WHERE item = '${held}'`)[0]?.g
expect(p2.status === 200 && gen > 0, 'a held key re-parks on a full table (a new intent token, D2-M1)', `${p2.text} generation=${gen}`)
check('change-park-park-held', p2.d1)

// 4. The drain.
const dr = await lever({ op: 'drain' })
expect(dr.status === 200 && dr.json?.read === 128 && dr.json?.refused === 128, 'the drain reads 128 eligible rows and backs them off (no transport here)', dr.text)
const backed = d1('SELECT COUNT(*) AS n FROM change_parked WHERE attempts = 1')[0]?.n
expect(backed === 128, 'the 128 rows are backed off, the rest untouched', `attempts=1 on ${backed}`)
check('change-park-drain-10000', dr.d1)

// 5. The drain's plan on local D1.
const read = shipped('PARKED_READ_SQL').replace('?1', "'pot'").replace('?2', '9999999999999').replace('?3', '128')
const plan = d1(`EXPLAIN QUERY PLAN ${read}`).map((r) => r.detail).join(' | ')
expect(plan.includes('change_parked_drain') && !plan.includes('TEMP B-TREE'), "the drain's plan is its index, no temp B-tree", plan)

d1('DELETE FROM change_parked')
for (const l of lines) console.log(l)
if (failures) {
  console.log(`✗ change-park budget: ${failures} failure(s)`)
  process.exit(1)
}
console.log(MEASURE ? '✓ change-park budget: measured (nothing enforced)' : '✓ change-park budget: every expectation held')
