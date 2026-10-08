#!/usr/bin/env node
/**
 * bsv-low #575 (lane E1D, the delta-2 fold, L2 and L3): the door's LANDING of a carried predecessor is asked of the
 * eviction ledger BEFORE its write. Part of `make ci-route` (through `make ci-d1-budget`, whose overlay worker runs
 * on a `--persist-to` state this cell can read and seed with `wrangler d1 execute --local`; it runs after that
 * tier's own legs, so their figures are untouched).
 *
 * The delta fold's pin (`admit_fast::tests::e1d_delta_l2_…`) read the source and executed no eviction. This drives
 * the real route on a real (local) D1:
 *  1. G: a zero-input transaction with one tm_collected marker, submitted by the operator (`historical-tx-no-spv`):
 *     its output is a coin the topic holds.
 *  2. P spends G's coin (its own marker); it is NEVER submitted. An OPEN eviction row is written for P in
 *     `pot_evictions` (as a corroborated refusal of P leaves it).
 *  3. S spends P and admits nothing in tm_collected; its BEEF carries G and P. S found no coin and P is an
 *     unlanded predecessor whose body the BEEF carries and which spends a held coin (case (a)): the door would land
 *     P first.
 * Expected: S is "not now" (`X-Overlay-Mutation: queued`, or the retryable 502 when the queue cannot take it); P
 * was never written (no row of P in `outputs`, `applied_transactions` or their `_evicted` twins); G's coin is still
 * held unspent; S is not recorded; `landing_refused_evicted_total` moved. On `f057acc` (the guard after the write
 * only) P was written and then moved to the twins, and S was recorded over it with a 200: RED.
 *
 *   node tools/lane-e1d/landing_guard_route_ci.mjs <overlay base> <overlay --persist-to dir>
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const OVERLAY = process.argv[2] ?? 'http://127.0.0.1:8801'
const STATE = process.argv[3]
const OP_TOKEN = process.env.D1_BUDGET_OP_TOKEN ?? 'ci-submit-tok'
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
if (!STATE) {
  console.error('usage: landing_guard_route_ci.mjs <overlay base> <overlay --persist-to dir>')
  process.exit(2)
}

let failures = 0
const lines = []
const pass = (label) => lines.push(`PASS  ${label}`)
const fail = (label, why) => {
  failures++
  lines.push(`FAIL  ${label}`)
  lines.push(`      ${why}`)
}
const expect = (ok, label, why) => (ok ? pass(label) : fail(label, why))

// ── minimal raw-tx / BEEF builders (the lane-499 conventions) ──────────────
const varint = (n) => (n < 0xfd ? Buffer.from([n]) : Buffer.from([0xfd, n & 0xff, n >> 8]))
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
const sha256d = (buf) => createHash('sha256').update(createHash('sha256').update(buf).digest()).digest()
const txidOf = (raw) => Buffer.from(sha256d(raw)).reverse().toString('hex')
// The tm_collected golden marker (`overlay-discovery` collected::GOLDEN_MARKER_HEX): byte-format admitted.
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
const input = (txid, vout) =>
  Buffer.concat([Buffer.from(txid, 'hex').reverse(), u32(vout), varint(0), u32(0xffffffff)])
const tx = (inputs, outputs) =>
  Buffer.concat([u32(1), varint(inputs.length), ...inputs, varint(outputs.length), ...outputs, u32(0)])
/** BEEF V1, no bumps, parents first. */
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
    },
    body: beef,
  })
  return { status: res.status, mutation: res.headers.get('x-overlay-mutation'), text: await res.text() }
}
/** One statement against the worker's own local D1 (its `--persist-to` state). */
function d1(sql) {
  const out = execFileSync(
    'npx',
    ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json', '--command', sql],
    { cwd: CRATE, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  )
  return JSON.parse(out)[0]?.results ?? []
}
/** Rows of `table` for `txid`; a twin that was never created holds none. */
function rowsOf(table, txid) {
  try {
    return d1(`SELECT * FROM ${table} WHERE txid = '${txid}'`)
  } catch (e) {
    if (`${e.stdout ?? ''}${e.stderr ?? ''}`.includes('no such table')) return []
    throw e
  }
}
/** A counter off `/health/invariants`; a reset keep-alive socket (seen after the `wrangler d1 execute` calls) is
 * asked again on a fresh connection, three times at most. */
async function counter(name) {
  for (let attempt = 1; ; attempt++) {
    try {
      const r = await fetch(OVERLAY + '/health/invariants', { headers: { Connection: 'close' } })
      return JSON.parse(await r.text()).counters?.[name]
    } catch (e) {
      if (attempt >= 3) throw e
      await new Promise((r) => setTimeout(r, 500))
    }
  }
}

const G = tx([], [output(COLLECTED_MARKER), output(nonceScript())])
const g = txidOf(G)
const P = tx([input(g, 0)], [output(COLLECTED_MARKER), output(nonceScript())])
const p = txidOf(P)
const S = tx([input(p, 0)], [output(nonceScript())])
const s = txidOf(S)

// 1. G's coin is held.
const rg = await submit(beefV1(G))
let gAdmitted = false
try { gAdmitted = (JSON.parse(rg.text)?.tm_collected?.outputsToAdmit ?? []).includes(0) } catch {}
expect(rg.status === 200 && gAdmitted, `G (${g.slice(0, 12)}…) admits its marker: a coin the topic holds`, `${rg.status}: ${rg.text.slice(0, 300)}`)

// 2. P under an OPEN eviction, never written.
d1(`INSERT INTO pot_evictions (txid, reason, evictedAt) VALUES ('${p}', 'REJECTED (lane E1D delta-2 fold, route tier)', ${Date.now()})`)
const open = d1(`SELECT txid FROM pot_evictions WHERE txid = '${p}' AND readmittedAt IS NULL`)
expect(open.length === 1, `P (${p.slice(0, 12)}…) is under an OPEN eviction in the ledger`, JSON.stringify(open))

// 3. S carries P.
const refused0 = (await counter('landing_refused_evicted_total')) ?? NaN
const rs = await submit(beefV1(G, P, S))
const notNow = (rs.status === 200 && rs.mutation === 'queued') || (rs.status === 502 && rs.text.includes('not durable'))
expect(notNow, `S (${s.slice(0, 12)}…) is "not now" (queued, or the retryable 502)`, `${rs.status} X-Overlay-Mutation=${rs.mutation}: ${rs.text.slice(0, 300)}`)
const pLive = rowsOf('outputs', p).length + rowsOf('applied_transactions', p).length
const pTwins = rowsOf('outputs_evicted', p).length + rowsOf('applied_transactions_evicted', p).length
expect(pLive === 0 && pTwins === 0, 'P was never written (no row live, none in the twins)', `live ${pLive}, twins ${pTwins}`)
const gRows = rowsOf('outputs', g)
expect(gRows.length === 1 && Number(gRows[0].spent) === 0, "G's coin is still held, unspent", JSON.stringify(gRows))
const sApplied = rowsOf('applied_transactions', s).length
expect(sApplied === 0, 'S is not recorded over P', `${sApplied} applied row(s) for S`)
const refused1 = await counter('landing_refused_evicted_total')
expect(refused1 >= refused0 + 1, 'landing_refused_evicted_total moved', `${refused0} -> ${refused1}`)

console.log('── lane E1D delta-2 fold: the landing guard at the route ──')
for (const l of lines) console.log(l)
console.log(failures === 0 ? 'landing guard route tier: OK' : `landing guard route tier: ${failures} FAILED`)
process.exit(failures === 0 ? 0 : 1)
