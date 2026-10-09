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
 *
 *   node tools/lane-e555/deferred_graphs_route_ci.mjs <overlay base> <overlay --persist-to dir>
 *
 * Exit 0 = every expectation held.
 */
import { execFileSync } from 'node:child_process'
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
    block0?.budget?.maxBytes === 1048576 && block0?.budget?.perPeerTopic === 16,
  'the budget the worker runs under: 100 calls, 15 s, 60 passes, 1 MiB, 16 per peer and topic',
  JSON.stringify(block0?.budget),
)
const names = [
  'gasp_graph_deferred_total', 'gasp_graph_resumed_total', 'gasp_graph_converged_total', 'gasp_graph_dropped_total',
  ...['max_passes', 'too_big', 'too_many', 'store_fault', 'not_served', 'held', 'not_held', 'root_proven', 'refused']
    .map((r) => `gasp_graph_dropped_${r}_total`),
]
const missing = names.filter((n) => typeof h0.counters?.[n] !== 'number')
expect(missing.length === 0, `every gasp_graph_*_total counter is served (${names.length})`, `missing: ${missing}`)

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
