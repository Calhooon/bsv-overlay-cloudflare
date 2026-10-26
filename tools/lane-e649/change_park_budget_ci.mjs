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
 *  6. bsv-low #649 D2-L2 (the second delta lens): the ACCEPTED worker delivery, on a second overlay whose
 *     APP_LAYER_URL is a mock app layer served here. (a) A pot and a hop row parked, the mock answers 200: both
 *     rows are deleted (unparked 2). (b) A pot and a hop row parked, the mock RE-PARKS what each POST carries
 *     (through the lever) before it answers 200: both rows stay, each at a NEW generation, the drain answers
 *     unparked 0 and kept 2; the next drain takes them. The worker's own `WorkerUnpark::unpark` binds the
 *     generation here, so a mutant binding any other generation (the lens's: generation 1) is RED at (a) or (b).
 *
 *   node tools/lane-e649/change_park_budget_ci.mjs <overlay base> <overlay --persist-to dir> \
 *        <delivering overlay base> <its --persist-to dir> <mock app-layer port>
 *   All five arguments are required (the D3-L2 fold): legs 6 and 7 run on the delivering overlay
 *   (`make ci-d1-budget`: LANE_BASE+11, given APP_LAYER_URL and ADMIN_TOKEN) and its mock app layer (LANE_BASE+12).
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
const ADMIN = process.env.D1_BUDGET_ADMIN_TOKEN ?? 'ci-admin-tok'
const MEASURE = process.env.D1_BUDGET_MEASURE === '1'
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
const CEILINGS = JSON.parse(readFileSync(new URL('./ceilings.json', import.meta.url), 'utf8'))
if (process.argv.length < 7) {
  console.error(
    'usage: change_park_budget_ci.mjs <overlay base> <overlay --persist-to dir> <delivering overlay base> <its --persist-to dir> <mock app-layer port>',
  )
  process.exit(2)
}

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}

function d1(sql, state = STATE) {
  for (let attempt = 1; ; attempt++) {
    try {
      const out = execFileSync(
        'npx',
        ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', state, '--json', '--command', sql],
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

async function lever(body, token = TOKEN, base = OVERLAY) {
  const r = await fetch(base + '/internal/change-park', {
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

// 6. D2-L2: the accepted worker delivery, a re-park in both sets during its POST.
const [DELIVERING, STATE2, MOCK_PORT] = process.argv.slice(4)
if (DELIVERING && STATE2 && MOCK_PORT) {
  const { createServer } = await import('node:http')
  let mode = 'plain'
  const posts = []
  const mock = createServer(async (req, res) => {
    let raw = ''
    for await (const c of req) raw += c
    posts.push({ path: req.url, body: raw })
    if (mode === 'repark') {
      const b = JSON.parse(raw)
      const items = (b.outpoints ?? []).map((o) => `${o.txid}:${o.vout}`)
      const hops = b.identities ?? []
      const r = await lever({ op: 'park', items, hops }, TOKEN, DELIVERING)
      if (r.status !== 200) {
        res.writeHead(500).end(`re-park failed: ${r.status} ${r.text}`)
        return
      }
    }
    res.writeHead(200, { 'content-type': 'application/json' }).end(mode === 'unheld' ? '{"held":false}' : '{}')
  })
  await new Promise((ok) => mock.listen(Number(MOCK_PORT), '127.0.0.1', ok))
  const rows2 = () => d1('SELECT kind, item, generation FROM change_parked ORDER BY kind, item', STATE2)
  try {
    const h2 = await lever({ op: 'health' }, TOKEN, DELIVERING)
    expect(h2.status === 200, 'the delivering overlay serves the lever', `${h2.status} ${h2.text}`)
    d1('DELETE FROM change_parked', STATE2)
    // (a) accepted, nothing re-parked: both rows deleted at the generation read
    const potA = 'a1'.repeat(32) + ':0'
    const hopA = '02' + 'a1'.repeat(32)
    const pa = await lever({ op: 'park', items: [potA], hops: [hopA] }, TOKEN, DELIVERING)
    expect(pa.status === 200 && rows2().length === 2, '(a) a pot and a hop row parked on the delivering overlay', `${pa.text} rows=${JSON.stringify(rows2())}`)
    mode = 'plain'
    const da = await lever({ op: 'drain' }, TOKEN, DELIVERING)
    expect(
      da.json?.read === 2 && da.json?.unparked === 2 && da.json?.kept === 0 && rows2().length === 0,
      '(a) the accepted worker delivery deletes both rows at the generation it read',
      `${da.text} rows=${JSON.stringify(rows2())} posts=${JSON.stringify(posts)}`,
    )
    // (b) accepted, each POST's rows re-parked during it: both stay, each a new intent
    const potB = 'b2'.repeat(32) + ':1'
    const hopB = '03' + 'b2'.repeat(32)
    await lever({ op: 'park', items: [potB], hops: [hopB] }, TOKEN, DELIVERING)
    const before = rows2()
    mode = 'repark'
    posts.length = 0
    const db = await lever({ op: 'drain' }, TOKEN, DELIVERING)
    const after = rows2()
    const moved = after.length === 2 && after.every((r) => before.some((b) => b.item === r.item && b.generation !== r.generation))
    expect(
      db.json?.read === 2 && db.json?.unparked === 0 && db.json?.kept === 2 && moved && posts.length === 2,
      '(b) a re-park in both sets during the accepted POST survives the delivery, counted kept',
      `${db.text} before=${JSON.stringify(before)} after=${JSON.stringify(after)} posts=${posts.length}`,
    )
    mode = 'plain'
    const dc = await lever({ op: 'drain' }, TOKEN, DELIVERING)
    expect(dc.json?.unparked === 2 && rows2().length === 0, '(b) the next delivery takes the new intents', `${dc.text} rows=${JSON.stringify(rows2())}`)
    // 7. D3-H1 (the third delta lens), D4-L1 (the fourth): the ADDITIONAL trigger drains. The `*/15` scheduled
    //    event is the observed one; the authenticated admin path `POST /admin/complete-proofs` drains too when
    //    invoked. A pot and a hop row
    //    parked, then that route alone (no lever drain, no tick): both delivered through the mock and deleted, the
    //    pass answered in its body. On `1bf8e66` the route never reads `change_parked`: both rows stay.
    const potC = 'c3'.repeat(32) + ':2'
    const hopC = '02' + 'c3'.repeat(32)
    await lever({ op: 'park', items: [potC], hops: [hopC] }, TOKEN, DELIVERING)
    posts.length = 0
    const parkedC = rows2().length
    const wrong = await fetch(DELIVERING + '/admin/complete-proofs', { method: 'POST', headers: { authorization: 'Bearer wrong', Connection: 'close' } })
    await wrong.text()
    expect(wrong.status === 403 && rows2().length === 2, '7. /admin/complete-proofs refuses a wrong bearer (the admin gate: 403) and drains nothing', `${wrong.status} rows=${JSON.stringify(rows2())}`)
    const cp = await fetch(DELIVERING + '/admin/complete-proofs', { method: 'POST', headers: { authorization: `Bearer ${ADMIN}`, Connection: 'close' } })
    const cpText = await cp.text()
    let cpJson = null
    try {
      cpJson = JSON.parse(cpText)
    } catch {}
    expect(
      parkedC === 2 && cp.status === 200 && cpJson?.change_parked?.unparked === 2 && rows2().length === 0 && posts.length === 2,
      '7. D3-H1: the fallback trigger (/admin/complete-proofs) drains the parked pot and hop rows',
      `parked=${parkedC} ${cp.status} change_parked=${JSON.stringify(cpJson?.change_parked)} rows=${JSON.stringify(rows2())} posts=${posts.length} ${cpJson ? '' : cpText.slice(0, 300)}`,
    )

    // FIX-CADENCE-GAUGES: actual workerd D1 + the shipped transport. A full page of
    // word frames is attempted, including 2xx {held:false}; no old row monopolizes a tick.
    for (const outcome of ['plain', 'unheld']) {
      mode = outcome
      posts.length = 0
      d1("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<500) INSERT INTO change_parked(kind,item,parked_at_ms,next_attempt_at_ms) SELECT 'word',json_object('txid',printf('%064x',i),'kind','refresh'),1000,0 FROM n", STATE2)
      const ticks = outcome === 'plain' ? 4 : 32
      let removed = 0, exhausted = 0
      for (let tick = 1; tick <= ticks; tick++) {
        const result = await lever({ op: 'drain' }, TOKEN, DELIVERING)
        expect(result.status === 200 && result.json?.read <= 128, `cadence ${outcome} tick ${tick} bounded`, result.text)
        removed += result.json?.unparked ?? 0
        exhausted += result.json?.exhausted ?? 0
      }
      const rows = d1("SELECT COUNT(*) AS active FROM change_parked WHERE kind='word' AND exhausted=0", STATE2)
      expect(rows[0]?.active === 0 && (outcome === 'plain' ? removed === 500 : exhausted === 500),
        `cadence 500 word frames ${outcome === 'plain' ? 'delivered in 4' : 'exhausted in 32'} ticks`,
        JSON.stringify({ rows, removed, exhausted, posts: posts.length }))
      expect(posts.length === (outcome === 'plain' ? 500 : 4000), `cadence ${outcome} every attempt reaches the transport`, `${posts.length} POSTs`)
      const health = await lever({ op: 'health' }, TOKEN, DELIVERING)
      expect(health.json?.oldestParkedFrameAgeMs === null, 'cadence active age clears after delivery/exhaustion', health.text)
      if (outcome === 'unheld') expect(health.json?.oldestExhaustedFrameAgeMs > 0, 'cadence exhausted age is served separately', health.text)
      d1('DELETE FROM change_parked', STATE2)
    }
  } finally {
    mock.close()
  }
} else {
  expect(false, 'D2-L2: the delivering overlay and the mock port are given', 'usage: ... <delivering base> <its state> <mock port>')
}

for (const l of lines) console.log(l)
if (failures) {
  console.log(`✗ change-park budget: ${failures} failure(s)`)
  process.exit(1)
}
console.log(MEASURE ? '✓ change-park budget: measured (nothing enforced)' : '✓ change-park budget: every expectation held')
