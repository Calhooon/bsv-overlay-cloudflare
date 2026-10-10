/**
 * bsv-low #555 (lane E555): the deferred GASP graphs' health block over a REAL local D1 (`make ci-d1-budget`).
 *
 *  1. `/health/invariants.gasp.deferredGraphs` is served (`readable`: the migration made the table), with the
 *     budget the worker runs under (100 calls, 15 s, 60 passes, 1 MiB, 16 per peer and topic), and every
 *     `gasp_graph_*_total` counter, the per-reason drops included, is served from 0.
 *  2. A deferred graph's row (as the engine's upsert writes it, aged five minutes) is listed: the count, the
 *     oldest, and its {topic, peer, outpoint, nodes, pending, calls, passes, reason, bytes, ageSecs}.
 *  3. Its delete (the converge / drop) empties the block again.
 * On the base (`cf933e8`) there is no `gasp` block and no table: RED.
 * The lens fold (bsv-low #555, M3): the block names `totalBytes` and the global ceiling (256 rows, 64 MiB) and the
 * stale sweep's age (30 h); `gasp_graph_dropped_stale_total` and `..._no_progress_total` are served from 0. RED on
 * `03e1e17` (no `totalBytes`, no ceiling, neither counter).
 * The delta fold (bsv-low #555, D-M1 and D-M2): the block names the per-host share of the ceiling (32 rows, 8 MiB),
 * and the migrations gave `gasp_peer_health` its `yieldless_syncs` column. RED on `0974be5` (neither).
 * The delta-2 fold (bsv-low #555, D2-M1 and D2-M2): the block names the discovered peers' half (128 rows, 32 MiB);
 * `gasp_graph_dropped_idle_faults_total` is served; the migrations gave `gasp_peer_health` its streak stamps and
 * `gasp_deferred_graphs` its `origin` and `configured`; and the two SHIPPED statements, read verbatim out of the Rust
 * source with their binds as literals, run on local D1: the upsert under small bounds (two spellings of one origin
 * share one share, the discovered half refuses a new host, a configured peer saves to the global bound) and the
 * yield upsert (count, age, a yield). RED on `ef423da` (no fields, no columns, the statements' binds absent).
 *
 *   node tools/lane-e555/deferred_graphs_route_ci.mjs <overlay base> <overlay --persist-to dir>
 *
 * Exit 0 = every expectation held.
 */
import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const OVERLAY = process.argv[2] ?? 'http://127.0.0.1:8801'
const STATE = process.argv[3]
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
if (!STATE) {
  console.error('usage: deferred_graphs_route_ci.mjs <overlay base> <overlay --persist-to dir>')
  process.exit(2)
}

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function d1(sql) {
  const out = execFileSync(
    'npx',
    ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json', '--command', sql],
    { cwd: CRATE, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  )
  return JSON.parse(out)[0]?.results ?? []
}
async function health() {
  for (let attempt = 1; ; attempt++) {
    try {
      const r = await fetch(OVERLAY + '/health/invariants', { headers: { Connection: 'close' } })
      return JSON.parse(await r.text())
    } catch (e) {
      if (attempt >= 3) throw e
      await sleep(500)
    }
  }
}

// 1. Served, empty, with its budget and its counters at 0.
const h0 = await health()
const block0 = h0.gasp?.deferredGraphs
expect(block0?.readable === true, 'gasp.deferredGraphs is served (the table exists)', JSON.stringify(h0.gasp))
expect(block0?.count === 0 && block0?.oldest === null, 'no deferred graph yet', JSON.stringify(block0))
expect(
  block0?.budget?.calls === 100 && block0?.budget?.ms === 15000 && block0?.budget?.maxPasses === 60 &&
    block0?.budget?.maxBytes === undefined && block0?.budget?.chunkBytes === 1048576 &&
    block0?.budget?.perPeerTopic === 16,
  'the budget the worker runs under: 100 calls, 15 s, 60 passes, no record cap (1 MiB a row, #585), 16 per peer and topic',
  JSON.stringify(block0?.budget),
)
expect(
  block0?.totalBytes === 0 && block0?.budget?.maxRows === 256 && block0?.budget?.maxTotalBytes === 67108864 &&
    block0?.budget?.staleSecs === 108000,
  'the global ceiling (256 rows, 64 MiB), the stale age (30 h) and totalBytes 0',
  JSON.stringify(block0),
)
expect(
  block0?.budget?.maxRowsPerHost === 32 && block0?.budget?.maxBytesPerHost === 8388608,
  'the per-host share of the ceiling (32 rows, 8 MiB)',
  JSON.stringify(block0?.budget),
)
const yieldless = d1(`SELECT COUNT(*) AS n FROM pragma_table_info('gasp_peer_health') WHERE name = 'yieldless_syncs'`)
expect(yieldless[0]?.n === 1, 'gasp_peer_health.yieldless_syncs exists (migration 171)', JSON.stringify(yieldless))
const names = [
  'gasp_graph_deferred_total', 'gasp_graph_resumed_total', 'gasp_graph_converged_total', 'gasp_graph_dropped_total',
  ...['max_passes', 'too_many', 'store_fault', 'not_served', 'held', 'not_held', 'root_proven', 'refused',
    'no_progress', 'idle_faults', 'stale']
    .map((r) => `gasp_graph_dropped_${r}_total`),
]
const missing = names.filter((n) => typeof h0.counters?.[n] !== 'number')
expect(missing.length === 0, `every gasp_graph_*_total counter is served (${names.length})`, `missing: ${missing}`)

// The delta-2 fold (D2-M1, D2-M2).
expect(
  block0?.budget?.discoveredMaxRows === 128 && block0?.budget?.discoveredMaxBytes === 33554432,
  "the discovered peers' half of the ceiling (128 rows, 32 MiB)",
  JSON.stringify(block0?.budget),
)
const columns = (table) => d1(`SELECT name FROM pragma_table_info('${table}')`).map((r) => r.name)
const health_cols = columns('gasp_peer_health')
const graph_cols = columns('gasp_deferred_graphs')
expect(
  health_cols.includes('first_yieldless_at') && health_cols.includes('last_yieldless_at') &&
    graph_cols.includes('origin') && graph_cols.includes('configured'),
  'migrations 172-175: the streak stamps, origin and configured',
  JSON.stringify({ health_cols, graph_cols }),
)
// The shipped statements, verbatim from the Rust source, binds as literals.
function shipped(file, name) {
  const src = readFileSync(new URL(`../../crates/overlay-cloudflare/src/${file}`, import.meta.url), 'utf8')
  const m = src.match(new RegExp(`const ${name}: &str =\\s*"((?:[^"\\\\]|\\\\.)*)"`, 's'))
  if (!m) throw new Error(`${name} not found in ${file}`)
  return m[1].replace(/\\\n\s*/g, '')
}
const lit = (v) => (typeof v === 'string' ? `'${v.replaceAll("'", "''")}'` : String(v))
const bound = (sql, binds) => binds.reduceRight((q, v, i) => q.replaceAll(`?${i + 1}`, lit(v)), sql)
const UPSERT = shipped('gasp_deferred.rs', 'DEFERRED_GRAPH_UPSERT_SQL')
const T = 'tm_e555d2'
// Bounds: 4 rows, 2 discovered, 1 per discovered origin; bytes far above.
const save = (host, origin, outpoint, configured) =>
  d1(bound(UPSERT, [host, T, outpoint, 1, 1, 1, 1, 1, 'calls', 10, '{}', 4, 1e9, 1, 1e9, origin, configured ? 1 : 0, 2,
    1e9, 0, ''])).length === 1
d1(`DELETE FROM gasp_deferred_graphs WHERE topic = '${T}'`)
const saves = [
  save('https://evil.example/?1', 'evil.example', 'q1.0', false),
  save('https://evil.example/?2', 'evil.example', 'q2.0', false),
  save('https://a.evil.example', 'a.evil.example', 'a1.0', false),
  save('https://b.evil.example', 'b.evil.example', 'b1.0', false),
  save('https://configured.example', 'configured.example', 'c1.0', true),
  save('https://configured.example', 'configured.example', 'c2.0', true),
  save('https://configured.example', 'configured.example', 'c3.0', true),
]
expect(
  JSON.stringify(saves) === JSON.stringify([true, false, true, false, true, true, false]),
  'the shipped upsert on local D1: one share per origin, the discovered half, configured to the global bound',
  JSON.stringify(saves),
)
d1(`DELETE FROM gasp_deferred_graphs WHERE topic = '${T}'`)
const YIELD = shipped('d1_storage.rs', 'PEER_YIELD_UPSERT_SQL')
d1(`DELETE FROM gasp_peer_health WHERE topic = '${T}'`)
const y = (yielded) => d1(bound(YIELD, ['evil.example', T, yielded ? 1 : 0, 21600]))[0]
const y1 = y(false)
d1(`UPDATE gasp_peer_health SET first_yieldless_at = first_yieldless_at - 3600, last_yieldless_at = last_yieldless_at - 60 WHERE topic = '${T}'`)
const y2 = y(false)
const y3 = y(true)
expect(
  y1?.yieldless_syncs === 1 && y1?.secsSinceFirst === 0 && y2?.yieldless_syncs === 2 && y2?.secsSinceFirst >= 3600 &&
    // `wrangler d1 execute --json` renders every SQL NULL as the string "null" (`SELECT NULL` included).
    y3?.yieldless_syncs === 0 && (y3?.secsSinceFirst === null || y3?.secsSinceFirst === 'null'),
  'the shipped yield upsert on local D1: the count, the age from the first, a yield ends it',
  JSON.stringify([y1, y2, y3]),
)
d1(`DELETE FROM gasp_peer_health WHERE topic = '${T}'`)

// 2. A deferred graph's row, aged five minutes.
const OUTPOINT = 'e555'.repeat(16) + '.0'
const record = JSON.stringify({ peer: 'https://peer.example', topic: 'tm_collected', outpoint: OUTPOINT, score: 7,
  nodes: [], pending: [], calls: 120, passes: 3, reason: 'time' })
d1(`DELETE FROM gasp_deferred_graphs WHERE outpoint = '${OUTPOINT}'`)
d1(`INSERT INTO gasp_deferred_graphs (host, topic, outpoint, score, nodes, pending, calls, passes, reason, bytes, record, created_at, updated_at)
    VALUES ('https://peer.example', 'tm_collected', '${OUTPOINT}', 7, 40, 3, 120, 3, 'time', 51000, '${record}', unixepoch() - 300, unixepoch())`)
const h1 = await health()
const block1 = h1.gasp?.deferredGraphs
const g = block1?.graphs?.[0]
expect(block1?.count === 1 && block1?.oldest?.outpoint === OUTPOINT, 'the row is counted and is the oldest', JSON.stringify(block1))
expect(block1?.totalBytes === 51000, 'totalBytes sums the rows', JSON.stringify(block1?.totalBytes))
expect(
  g?.topic === 'tm_collected' && g?.peer === 'https://peer.example' && g?.nodes === 40 && g?.pending === 3 &&
    g?.calls === 120 && g?.passes === 3 && g?.reason === 'time' && g?.bytes === 51000 && g?.ageSecs >= 300,
  'each graph: topic, peer, outpoint, nodes, pending, calls, passes, reason, bytes, ageSecs',
  JSON.stringify(g),
)

// 3. Deleted: the block is empty again.
d1(`DELETE FROM gasp_deferred_graphs WHERE outpoint = '${OUTPOINT}'`)
const h2 = await health()
expect(h2.gasp?.deferredGraphs?.count === 0, 'the delete empties the block', JSON.stringify(h2.gasp))

console.log(lines.join('\n'))
console.log(failures ? `✗ deferred graphs route: ${failures} failed` : '✓ deferred graphs route: every expectation held')
process.exit(failures ? 1 : 0)
