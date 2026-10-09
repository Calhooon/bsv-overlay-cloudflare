#!/usr/bin/env node
/**
 * bsv-low #585, door 3 (lane E585-d3): a queued replay whose message would pass the queue's inline room rides BY
 * KEY, its BEEF in R2 (`BEEF_BLOBS`), at the route, on a real (local) queue, D1 and R2. Part of `make ci-route`
 * (through `make ci-d1-budget`, after the dead-letter cell, on the same overlay worker, which is started with
 * `MUTATION_QUEUE_INLINE_ROOM:4096` so an 8 KB body takes the R2 path under the consumer's policy as it stands).
 *
 *  0. `/health/invariants.deadLetters.r2`: the bucket bound, the room in force, the consumer's policy.
 *  1. Three "not now" successors of ~8 KB (each over an evicted predecessor, the e1d class) are acked `queued`; each
 *     is written to R2 before its enqueue, dead-letters after its replays and PARKS ITS KEY: the row holds
 *     `r2_key` / `r2_bytes` and a message with no body, and the object is the submitted BEEF byte for byte. The
 *     health block shows the bytes at rest.
 *  2. A: its predecessor's eviction lifted, the lever re-drives the KEY, the consumer re-reads R2 and the letter
 *     LANDS (the row is gone, the subject is applied); its object is gone after the ack.
 *  3. B: its object deleted out from under it, the re-driven replay finds it MISSING and the letter parks again as
 *     a FAULT letter (never "not now"), `beef_blobs_missing_total` moved.
 *  4. C: the operator's discard deletes the row AND the object (`r2Key`, `r2Deleted`).
 *  5. A 500 KB VALID submission. When the consumer's policy admits it (`r2.replayMaxBytes`, NL-6's
 *     `QUEUE_BEEF_LIMITS`): acked `queued`, parked by key, re-driven from R2, landed, its object gone. While that
 *     policy still stops at 90,000 bytes the door refuses it (502 naming the policy, nothing written): this leg
 *     then says so and passes on the refusal; it turns into the full leg, unedited, when the policy lifts.
 * On the base (`6926f2c`): no `r2` health block, no bucket, and every body past 90,000 bytes is the door's 502.
 *
 *   node tools/lane-e585/beef_blobs_route_ci.mjs <overlay base> <overlay --persist-to dir>
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
const BUCKET = process.env.BEEF_BLOBS_BUCKET ?? 'overlay-beefs'
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
if (!STATE) {
  console.error('usage: beef_blobs_route_ci.mjs <overlay base> <overlay --persist-to dir>')
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

// ── the lane-499 / E1D raw-tx and BEEF builders (as lane E576's) ─────────────
const varint = (n) => {
  if (n < 0xfd) return Buffer.from([n])
  if (n <= 0xffff) return Buffer.from([0xfd, n & 0xff, n >> 8])
  const b = Buffer.alloc(5)
  b[0] = 0xfe
  b.writeUInt32LE(n, 1)
  return b
}
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
const sha256 = (buf) => createHash('sha256').update(buf).digest()
const txidOf = (raw) => Buffer.from(sha256(sha256(raw))).reverse().toString('hex')
const COLLECTED_MARKER = Buffer.from(
  '006a104c4f572f636f6c6c65637465642f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab',
  'hex',
)
/** OP_FALSE OP_RETURN PUSHDATA4 <n random bytes>: a nonce that also sets the body's size. */
const padScript = (n) => {
  const data = Buffer.alloc(n)
  randomFillSync(data)
  return Buffer.concat([Buffer.from([0x00, 0x6a, 0x4e]), u32(n), data])
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
async function lever(body, route = '/internal/redrive-dead-letters') {
  const res = await fetch(OVERLAY + route, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Connection: 'close', Authorization: `Bearer ${INTERNAL}` },
    body: JSON.stringify(body),
  })
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
/** The local bucket's object under `key`, or null when it holds none. */
function r2get(key) {
  try {
    return execFileSync('npx', ['wrangler', 'r2', 'object', 'get', `${BUCKET}/${key}`, '--local', '--persist-to', STATE, '--pipe'], {
      cwd: CRATE,
      stdio: ['ignore', 'pipe', 'pipe'],
      maxBuffer: 64 * 1024 * 1024,
    })
  } catch {
    return null
  }
}
function r2del(key) {
  execFileSync('npx', ['wrangler', 'r2', 'object', 'delete', `${BUCKET}/${key}`, '--local', '--persist-to', STATE], {
    cwd: CRATE,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
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
const letter = (txid) => d1(`SELECT * FROM mutation_dead_letters WHERE txid = '${txid}'`)[0]
const brief = (row) => JSON.stringify(row ? { ...row, history: '…' } : 'no row')
async function until(pred) {
  const t0 = Date.now()
  for (;;) {
    const v = pred()
    if (v) return v
    if (Date.now() - t0 > WAIT_MS) return null
    await sleep(2_000)
  }
}
/** G (admitted), P (spends G, under an OPEN eviction), S (spends P, padded to about `pad` bytes): S is "not now". */
async function chain(pad, why) {
  const G = tx([], [output(COLLECTED_MARKER), output(padScript(8))])
  const g = txidOf(G)
  const P = tx([input(g, 0)], [output(COLLECTED_MARKER), output(padScript(8))])
  const p = txidOf(P)
  const S = tx([input(p, 0)], [output(padScript(pad))])
  const s = txidOf(S)
  const rg = await submit(beefV1(G))
  d1(`INSERT INTO pot_evictions (txid, reason, evictedAt) VALUES ('${p}', 'REJECTED (lane E585-d3, ${why})', ${Date.now()})`)
  const beef = beefV1(G, P, S)
  return { g, p, s, beef, sha: sha256(beef).toString('hex'), rg, rs: await submit(beef) }
}

try {
// 0. The health block.
const h0 = await health()
const r2h = h0.deadLetters?.r2
expect(
  r2h?.bound === true && r2h?.inlineRoom === 4096 && typeof r2h?.replayMaxBytes === 'number' && typeof r2h?.letters === 'number' && typeof r2h?.bytes === 'number',
  '/health/invariants.deadLetters.r2: the bucket bound, the room in force (the var), the consumer\'s policy, the bytes at rest',
  JSON.stringify(h0.deadLetters?.r2 ?? h0.deadLetters),
)
const c0 = h0.counters ?? {}

// 1. Three ~8 KB "not now" letters, by key.
const [A, B, C] = [await chain(8_000, 'A'), await chain(8_000, 'B'), await chain(8_000, 'C')]
expect(
  [A, B, C].every((x) => x.rg.status === 200 && x.rs.status === 200 && x.rs.mutation === 'queued'),
  'three ~8 KB "not now" successors past the room are acked: queued',
  [A, B, C].map((x) => `${x.rg.status}/${x.rs.status} ${x.rs.mutation}: ${x.rs.text.slice(0, 120)}`).join(' | '),
)
expect(
  ((await health()).counters?.beef_blobs_written_total ?? NaN) === (c0.beef_blobs_written_total ?? NaN) + 3,
  'each was written to R2 before its ack (beef_blobs_written_total moved by three)',
  `${c0.beef_blobs_written_total} -> ${(await health()).counters?.beef_blobs_written_total}`,
)
for (const x of [A, B, C]) {
  x.row = await until(() => {
    const r = letter(x.s)
    return r && r.status === 'parked' ? r : null
  })
}
expect([A, B, C].every((x) => !!x.row), `each dead-letters after its replays and PARKS (within ${WAIT_MS / 1000} s)`, [A, B, C].map((x) => brief(letter(x.s))).join(' | '))
for (const x of [A, B, C]) {
  if (!x.row) continue
  const m = JSON.parse(x.row.message)
  x.key = x.row.r2_key
  const obj = x.key ? r2get(x.key) : null
  expect(
    typeof x.key === 'string' && x.key.startsWith(`mutations/${x.sha}/`) && Number(x.row.r2_bytes) === x.beef.length && x.row.class === 'not_now' &&
      !('beef_b64' in m) && m.r2?.beefR2Key === x.key && m.r2?.sha256 === x.sha && m.r2?.bytes === x.beef.length && m.r2?.txid === x.s && x.row.message.length < 600,
    `${x.s.slice(0, 12)}…: the row parks the KEY (r2_key, r2_bytes, a message with no body), class not_now`,
    brief(x.row),
  )
  expect(!!obj && obj.equals(x.beef), `${x.s.slice(0, 12)}…: the object is the submitted BEEF, byte for byte (${x.beef.length} B)`, obj ? `${obj.length} B differ` : 'no object')
}
const h1 = await health()
expect(
  h1.deadLetters?.r2?.letters >= 3 && h1.deadLetters?.r2?.bytes >= A.beef.length + B.beef.length + C.beef.length,
  'the health block shows the bytes at rest in R2',
  JSON.stringify(h1.deadLetters?.r2),
)

// 2. A: re-driven from R2, lands, its object gone after the ack.
if (A.row) {
  d1(`DELETE FROM pot_evictions WHERE txid = '${A.p}'`)
  const ra = await lever({ txid: A.s })
  expect(ra.status === 200 && (ra.json?.redriven ?? []).length === 1, 'A: its predecessor readmitted, the lever re-drives the key', ra.text.slice(0, 300))
  const landed = await until(() => (letter(A.s) === undefined ? true : null))
  const applied = d1(`SELECT COUNT(*) AS c FROM applied_transactions WHERE txid = '${A.s}'`)[0]?.c
  expect(!!landed && Number(applied) >= 1, `A: the consumer re-read R2 and the letter LANDED (row gone, subject applied; within ${WAIT_MS / 1000} s)`, `${brief(letter(A.s))} applied=${applied}`)
  const gone = await until(() => (r2get(A.key) === null ? true : null))
  const h2 = await health()
  expect(
    !!gone && (h2.counters?.beef_blobs_deleted_total ?? 0) >= (c0.beef_blobs_deleted_total ?? 0) + 1,
    'A: its object is GONE after the ack (beef_blobs_deleted_total moved)',
    `object ${gone ? 'gone' : 'still there'}; deleted ${c0.beef_blobs_deleted_total} -> ${h2.counters?.beef_blobs_deleted_total}`,
  )
}

// 3. B: its object missing, the re-driven replay is a FAULT letter.
if (B.row) {
  r2del(B.key)
  const rb = await lever({ txid: B.s })
  expect((rb.json?.redriven ?? []).length === 1, 'B: its object deleted out from under it, the lever re-drives the key', rb.text.slice(0, 300))
  const again = await until(() => {
    const r = letter(B.s)
    return r && r.status === 'parked' && Number(r.redrives) === 1 ? r : null
  })
  const h3 = await health()
  expect(
    !!again && again.class === 'fault' && /MISSING/.test(again.fault ?? '') && again.r2_key === B.key &&
      (h3.counters?.beef_blobs_missing_total ?? 0) >= (c0.beef_blobs_missing_total ?? 0) + 1,
    `B: a missing object is a FAULT letter, parked again with its key (never "not now"; within ${WAIT_MS / 1000} s)`,
    `${brief(letter(B.s))} missing=${h3.counters?.beef_blobs_missing_total}`,
  )
  await lever({ letters: [{ txid: B.s }] }, '/internal/discard-dead-letters')
}

// 4. C: the operator's discard deletes the object.
if (C.row) {
  const dc = await lever({ letters: [{ txid: C.s, topics: 'tm_collected' }] }, '/internal/discard-dead-letters')
  const d = dc.json?.discarded?.[0]
  expect(
    dc.status === 200 && d?.r2Key === C.key && d?.r2Bytes === C.beef.length && d?.r2Deleted === true && letter(C.s) === undefined && r2get(C.key) === null,
    'C: the discard deletes the row AND its object (r2Key, r2Bytes, r2Deleted)',
    dc.text.slice(0, 400),
  )
}

// 5. 500 KB.
const max = r2h?.replayMaxBytes ?? 0
const written0 = (await health()).counters?.beef_blobs_written_total ?? NaN
const D = await chain(500_000, 'D')
if (D.beef.length <= max) {
  expect(D.rg.status === 200 && D.rs.status === 200 && D.rs.mutation === 'queued', `a ${D.beef.length} B "not now" submission is acked: queued`, `${D.rs.status} ${D.rs.mutation}: ${D.rs.text.slice(0, 200)}`)
  const row = await until(() => {
    const r = letter(D.s)
    return r && r.status === 'parked' ? r : null
  })
  const obj = row?.r2_key ? r2get(row.r2_key) : null
  expect(!!row && Number(row.r2_bytes) === D.beef.length && !!obj && obj.equals(D.beef), 'it parks with its key; its object is the 500 KB, byte for byte', brief(row))
  if (row) {
    d1(`DELETE FROM pot_evictions WHERE txid = '${D.p}'`)
    const rd = await lever({ txid: D.s })
    const landed = await until(() => (letter(D.s) === undefined ? true : null))
    const applied = d1(`SELECT COUNT(*) AS c FROM applied_transactions WHERE txid = '${D.s}'`)[0]?.c
    const gone = await until(() => (r2get(row.r2_key) === null ? true : null))
    expect((rd.json?.redriven ?? []).length === 1 && !!landed && Number(applied) >= 1 && !!gone, 're-driven from R2 it LANDS, and its object is gone after the ack', `${rd.text.slice(0, 200)} ${brief(letter(D.s))} applied=${applied}`)
  }
} else {
  const written = (await health()).counters?.beef_blobs_written_total ?? NaN
  expect(
    D.rs.status === 502 && /QUEUE_BEEF_LIMITS/.test(D.rs.text) && written === written0 && letter(D.s) === undefined,
    `a ${D.beef.length} B submission: the consumer's policy stops at ${max} B, so the door refuses it (502 naming the policy, nothing written, nothing acked)`,
    `${D.rs.status}: ${D.rs.text.slice(0, 300)}; written ${written0} -> ${written}`,
  )
  lines.push(`NOTE  the 500 KB leg passed on the REFUSAL: QUEUE_BEEF_LIMITS.max_bytes is ${max}. It becomes the full leg (acked, parked by key, re-driven, landed, object gone) when that policy lifts (NL-6).`)
}
} catch (e) {
  expect(false, 'the cell ran to its end', `${e.message ?? e}`.split('\n')[0])
}

console.log('── lane E585-d3: a queued BEEF past the room rides by key, in R2 ──')
for (const l of lines) console.log(l)
console.log(failures === 0 ? 'beef blobs route tier: OK' : `beef blobs route tier: ${failures} FAILED`)
process.exit(failures === 0 ? 0 : 1)
