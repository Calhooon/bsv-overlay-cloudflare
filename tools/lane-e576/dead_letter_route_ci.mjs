#!/usr/bin/env node
/**
 * bsv-low #576 (lane E576): the dead letters PARKED in D1 and the operator's bounded re-drive, at the route, on a
 * real (local) D1 and a real (local) queue. Part of `make ci-route` (through `make ci-d1-budget`, after the landing
 * guard cell, on the same overlay worker: its `--persist-to` state is read and seeded with `wrangler d1 execute
 * --local`; the worker runs `wrangler.toml`, whose mutations queue dead-letters to `overlay-mutations-dlq`, consumed
 * by the same worker).
 *
 *  1. The bearer: no token and a wrong token are 401; nothing moves.
 *  2. A real dead letter: S spends P, P spends G's held coin and P is under an OPEN eviction, so the door never
 *     lands P and S is "not now" (queued) at the door and at every replay (the e1d class: a predecessor that never
 *     lands). After the replays S is dead-lettered and the worker PARKS it: one `mutation_dead_letters` row, status
 *     `parked`, its fault the replay's own text, one history entry; `dead_letters_parked_total` moved.
 *  3. The lever over seeded letters (four parked at known times, one past the ceiling): `{"limit": 2}` moves the two
 *     OLDEST and no more; a second call moves the next two; a call naming a moved letter moves nothing (one
 *     enqueue per claim); the exhausted letter is never moved.
 *  4. A re-driven letter that fails again (its bytes do not parse) parks AGAIN on the same row, with its history.
 *  5. `/health/invariants.deadLetters`: the counts, the oldest parked, the last re-drive, the exhausted list.
 * On the base (`835b80c`) there is no route (the dispatch's 404) and no table: RED.
 *
 *   node tools/lane-e576/dead_letter_route_ci.mjs <overlay base> <overlay --persist-to dir>
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const OVERLAY = process.argv[2] ?? 'http://127.0.0.1:8801'
const STATE = process.argv[3]
const OP_TOKEN = process.env.D1_BUDGET_OP_TOKEN ?? 'ci-submit-tok'
const INTERNAL = process.env.DEAD_LETTER_INTERNAL_TOKEN ?? 'ci-internal-tok'
const WAIT_MS = Number(process.env.DEAD_LETTER_WAIT_MS ?? 120_000)
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
if (!STATE) {
  console.error('usage: dead_letter_route_ci.mjs <overlay base> <overlay --persist-to dir>')
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

// ── the lane-499 / E1D raw-tx and BEEF builders ──────────────────────────────
const varint = (n) => (n < 0xfd ? Buffer.from([n]) : Buffer.from([0xfd, n & 0xff, n >> 8]))
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
const sha256d = (buf) => createHash('sha256').update(createHash('sha256').update(buf).digest()).digest()
const txidOf = (raw) => Buffer.from(sha256d(raw)).reverse().toString('hex')
const COLLECTED_MARKER = Buffer.from(
  '006a104c4f572f636f6c6c65637465642f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab',
  'hex',
)
const nonceScript = () => {
  const nonce = Buffer.alloc(8)
  randomFillSync(nonce)
  return Buffer.concat([Buffer.from([0x00, 0x6a, 8]), nonce])
}
const output = (script) => Buffer.concat([u64(0), varint(script.length), script])
const input = (txid, vout) => Buffer.concat([Buffer.from(txid, 'hex').reverse(), u32(vout), varint(0), u32(0xffffffff)])
const tx = (inputs, outputs) =>
  Buffer.concat([u32(1), varint(inputs.length), ...inputs, varint(outputs.length), ...outputs, u32(0)])
const beefV1 = (...raws) =>
  Buffer.concat([Buffer.from([0x01, 0x00, 0xbe, 0xef]), varint(0), varint(raws.length), ...raws.flatMap((r) => [r, Buffer.from([0x00])])])

async function submit(beef) {
  const res = await fetch(OVERLAY + '/submit', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'x-topics': JSON.stringify(['tm_collected']),
      'x-submit-mode': 'historical-tx-no-spv',
      Authorization: `Bearer ${OP_TOKEN}`,
      Connection: 'close',
    },
    body: beef,
  })
  return { status: res.status, mutation: res.headers.get('x-overlay-mutation'), text: await res.text() }
}
async function lever(body, token = INTERNAL) {
  const headers = { 'Content-Type': 'application/json', Connection: 'close' }
  if (token !== null) headers.Authorization = `Bearer ${token}`
  const res = await fetch(OVERLAY + '/internal/redrive-dead-letters', { method: 'POST', headers, body: JSON.stringify(body) })
  const text = await res.text()
  let json = null
  try { json = JSON.parse(text) } catch {}
  return { status: res.status, text, json }
}
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
const letter = (txid) => {
  try {
    return d1(`SELECT * FROM mutation_dead_letters WHERE txid = '${txid}'`)[0]
  } catch (e) {
    if (`${e.stdout ?? ''}${e.stderr ?? ''}`.includes('no such table')) return undefined
    throw e
  }
}
async function until(what, pred) {
  const t0 = Date.now()
  for (;;) {
    const v = pred()
    if (v) return v
    if (Date.now() - t0 > WAIT_MS) return null
    await sleep(2_000)
  }
}

// 1. The bearer.
const none = await lever({ limit: 200 }, null)
const wrong = await lever({ limit: 200 }, 'not-the-token')
expect(none.status === 401 && wrong.status === 401, 'the lever refuses no bearer and a wrong bearer (401)', `${none.status} / ${wrong.status}: ${wrong.text.slice(0, 200)}`)
const h0 = await health()
expect(h0.deadLetters?.readable === true, '/health/invariants.deadLetters is served (the table exists)', JSON.stringify(h0.deadLetters))
const parked0 = h0.counters?.dead_letters_parked_total ?? NaN

// 2. A real dead letter of the e1d class.
const G = tx([], [output(COLLECTED_MARKER), output(nonceScript())])
const g = txidOf(G)
const P = tx([input(g, 0)], [output(COLLECTED_MARKER), output(nonceScript())])
const p = txidOf(P)
const S = tx([input(p, 0)], [output(nonceScript())])
const s = txidOf(S)
const rg = await submit(beefV1(G))
expect(rg.status === 200, `G (${g.slice(0, 12)}…) is admitted`, `${rg.status}: ${rg.text.slice(0, 200)}`)
d1(`INSERT INTO pot_evictions (txid, reason, evictedAt) VALUES ('${p}', 'REJECTED (lane E576, route tier)', ${Date.now()})`)
const rs = await submit(beefV1(G, P, S))
expect(rs.status === 200 && rs.mutation === 'queued', `S (${s.slice(0, 12)}…) is "not now": queued`, `${rs.status} ${rs.mutation}: ${rs.text.slice(0, 200)}`)
const sParked = await until('S parked', () => {
  const r = letter(s)
  return r && r.status === 'parked' ? r : null
})
expect(!!sParked, `S was dead-lettered after its replays and PARKED (within ${WAIT_MS / 1000} s)`, JSON.stringify(letter(s) ?? 'no row'))
if (sParked) {
  const hist = JSON.parse(sParked.history)
  expect(
    sParked.topics === 'tm_collected' && Number(sParked.attempts) >= 1 && !!sParked.fault && !sParked.fault.startsWith('dead-lettered;') && hist.length === 1,
    "the parked row: its topics, its replays' attempts, the replay's own fault text, one history entry",
    JSON.stringify({ ...sParked, message: '…' }),
  )
  const m = JSON.parse(sParked.message)
  expect(Buffer.from(m.beef_b64, 'base64').equals(beefV1(G, P, S)), 'the parked message is the queued bytes as they were', m.beef_b64.slice(0, 40))
}
const h1 = await health()
expect((h1.counters?.dead_letters_parked_total ?? NaN) >= parked0 + 1, 'dead_letters_parked_total moved', `${parked0} -> ${h1.counters?.dead_letters_parked_total}`)
// S's own letter is not re-driven below: it would only fail again (P stays evicted). Take it out of the lever's way.
if (sParked) d1(`UPDATE mutation_dead_letters SET parked_at = 9000000000000 WHERE txid = '${s}'`)

// 3. The lever over seeded letters: junk bytes (they will fail again, step 4).
try {
const msg = JSON.stringify({ beef_b64: 'AAAA', topics: ['tm_collected'], mode: 'historical-tx', reason: 'phase3-fault' }).replaceAll("'", "''")
const seeded = ['e576a', 'e576b', 'e576c', 'e576d']
seeded.forEach((t, i) =>
  d1(`INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, history) VALUES ('${t}', 'tm_collected', '${msg}', 'seeded ${i}', 4, 'parked', 0, ${1000 + i}, ${1000 + i}, '[]')`),
)
d1(`INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, history) VALUES ('e576x', 'tm_collected', '${msg}', 'past the ceiling', 4, 'parked', 3, 1, 1, '[]')`)
const redriven0 = (await health()).counters?.dead_letters_redriven_total ?? NaN
const a = await lever({ limit: 2 })
const aKeys = (a.json?.redriven ?? []).map((r) => r.txid)
expect(a.status === 200 && JSON.stringify(aKeys) === JSON.stringify(['e576a', 'e576b']), '{"limit": 2} re-drives the two OLDEST and no more', `${a.status}: ${a.text.slice(0, 300)}`)
const b = await lever({ limit: 2 })
const bKeys = (b.json?.redriven ?? []).map((r) => r.txid)
expect(JSON.stringify(bKeys) === JSON.stringify(['e576c', 'e576d']), 'the next call re-drives the next two (the exhausted letter is never selected)', b.text.slice(0, 300))
const again = await lever({ txid: 'E576A' })
expect(again.status === 200 && again.json?.read === 0 && (again.json?.redriven ?? []).length === 0, 'naming a letter already re-driven moves nothing (one enqueue)', again.text.slice(0, 300))
const rows = d1(`SELECT txid, status, redrives FROM mutation_dead_letters WHERE txid IN ('e576a','e576b','e576c','e576d','e576x') ORDER BY txid`)
expect(
  rows.filter((r) => r.txid !== 'e576x').every((r) => Number(r.redrives) === 1) && rows.find((r) => r.txid === 'e576x')?.status === 'parked',
  'each moved letter counts one re-drive; the exhausted one stays parked',
  JSON.stringify(rows),
)
const h2 = await health()
expect((h2.counters?.dead_letters_redriven_total ?? NaN) === redriven0 + 4, 'dead_letters_redriven_total moved by four', `${redriven0} -> ${h2.counters?.dead_letters_redriven_total}`)

// 4. Each re-driven letter fails again (its bytes do not parse) and parks again, on its row, with its history.
const reparked = await until('e576a parked again', () => {
  const r = letter('e576a')
  return r && r.status === 'parked' ? r : null
})
expect(!!reparked, `a re-driven letter that fails again parks AGAIN on its own row (within ${WAIT_MS / 1000} s)`, JSON.stringify(letter('e576a') ?? 'no row'))
if (reparked) {
  const hist = JSON.parse(reparked.history)
  expect(
    Number(reparked.redrives) === 1 && hist.length === 1 && hist[0].redrive === 1 && /subject could not be derived/.test(hist[0].fault ?? ''),
    "its history names the re-drive and the re-driven replay's fault",
    JSON.stringify({ ...reparked, message: '…' }),
  )
}
const h3 = await health()
expect((h3.counters?.dead_letters_still_failing_total ?? 0) >= 1, 'dead_letters_still_failing_total moved', JSON.stringify(h3.counters?.dead_letters_still_failing_total))

// 5. The health block.
const dl = h3.deadLetters ?? {}
expect(
  dl.readable === true && dl.parked >= 2 && dl.oldestParked?.txid === 'e576x' && Number(dl.lastRedrive?.at) > 0 && (dl.exhausted ?? []).some((e) => e.txid === 'e576x' && e.redrives === 3),
  '/health/invariants.deadLetters: the count, the oldest parked, the last re-drive, the exhausted letter',
  JSON.stringify(dl),
)

} catch (e) {
  expect(false, 'the seeded legs ran (the table and the lever exist)', `${e.message ?? e}`.split('\n')[0])
}

console.log('── lane E576: the dead letters parked and re-driven at the route ──')
for (const l of lines) console.log(l)
console.log(failures === 0 ? 'dead letter route tier: OK' : `dead letter route tier: ${failures} FAILED`)
process.exit(failures === 0 ? 0 : 1)
