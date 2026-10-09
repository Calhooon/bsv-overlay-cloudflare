#!/usr/bin/env node
/**
 * bsv-low #585, door 3 (lane E585-d3): a queued replay whose message would pass the queue's inline room rides BY
 * KEY, its BEEF in R2 (`BEEF_BLOBS`), at the route, on a real (local) queue, D1 and R2. Part of `make ci-route`
 * (through `make ci-d1-budget`, the LAST cell, after the dead-letter and deferred-graph cells, on the same overlay worker, which is started with
 * `MUTATION_QUEUE_INLINE_ROOM:4096` so an 8 KB body takes the R2 path under the consumer's policy as it stands).
 *
 *  0. `/health/invariants.deadLetters.r2`: the bucket bound, the room in force, the consumer's policy.
 *  1. Three "not now" successors of ~8 KB (each over an evicted predecessor, the e1d class) are acked `queued`; each
 *     is written to R2 before its enqueue, dead-letters after its replays and PARKS ITS KEY: the row holds
 *     `r2_key` / `r2_bytes` and a message with no body, and the object is the submitted BEEF byte for byte. The
 *     health block shows the bytes at rest.
 *  2. A: its predecessor's eviction lifted, the lever re-drives the KEY, the consumer re-reads R2 and the letter
 *     LANDS (the row is gone, the subject is applied); its object is gone after the ack.
 *  3. B: its object deleted out from under it, the re-driven replay finds it MISSING and the letter parks again,
 *     KEEPING its class (`not_now`: the fold-2, E585-D3-L4; a missing object never promotes a letter), its fault
 *     naming the MISSING object, `beef_blobs_missing_total` moved.
 *  4. C: the operator's discard deletes the row AND the object (`r2Key`, `r2Deleted`).
 *  5. A 500 KB VALID submission (the fold-2, E585-D3-M1, #568: the door refuses no body for its size): acked
 *     `queued`, parked by key, re-driven from R2, landed, its object gone. On `bc32851` it was the door's 502.
 *  6. E, the TWIN ack (the fold-2, E585-D3-L3): a parked keyed letter whose subject then LANDS by another road (its
 *     predecessor readmitted, its bytes presented again at the door, durable), its object deleted, re-driven: the
 *     consumer finds the object MISSING and the subject applied, ACKS it as a twin: no letter,
 *     `queue_r2_twin_acked_total` moved.
 *  7. The SWEEP under `wrangler dev --test-scheduled` (`/__scheduled`): a young object is untouched, the
 *     `beef_blob_sweep` row advanced (`last_pass_at`), `/health/invariants.queue.r2.atRest` served.
 *     The tick runs WHOLE and goes on in the background past the leg (its GASP step syncs the worker's hard-coded
 *     peers over the network and defers real graphs), so this cell is the LAST of `ci-d1-budget` (the d3 fold-3).
 *     Its object is put with NO custom metadata on purpose (E585-D3-DELTA-M1).
 *  8. A put TWICE (N2 (b)): the same "not now" bytes presented twice write one key twice, and its
 *     `customMetadata.touched` stamp MOVED (read from the local bucket's own store); whether `uploaded` moved too is
 *     printed as a NOTE (the sweep no longer depends on it).
 * On the base (`6926f2c`): no `r2` health block, no bucket, and every body past 90,000 bytes is the door's 502.
 *
 *   node tools/lane-e585/beef_blobs_route_ci.mjs <overlay base> <overlay --persist-to dir>
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { execFileSync, spawnSync } from 'node:child_process'
import { readdirSync, writeFileSync } from 'node:fs'
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
/**
 * One `npx wrangler ...` call (the d3 fold-3, E585-D3-DELTA-M2). Each call starts a second Miniflare over the running
 * dev server's persist dir, which can fail on its own (a lock, a slow start): the call is made up to `CLI_TRIES`
 * times with a growing pause, and on the last failure it THROWS with the child's status and its stderr IN FULL, so
 * the FAIL line names the cause (the fold-2's cell piped stderr away and printed one line of `e.message`).
 * `isNotFound(result)` lets a caller take a definite answer (r2get's "no such object") as a result, never a retry.
 */
const CLI_TRIES = Number(process.env.E585_CLI_TRIES ?? 4)
const sleepSync = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
function cli(args, { isNotFound } = {}) {
  let last
  for (let attempt = 1; attempt <= CLI_TRIES; attempt++) {
    const r = spawnSync('npx', ['wrangler', ...args], { cwd: CRATE, maxBuffer: 64 * 1024 * 1024 })
    const out = { status: r.status, stdout: r.stdout ?? Buffer.alloc(0), stderr: `${r.stderr ?? ''}`, error: r.error }
    if (!r.error && r.status === 0) return out
    if (!r.error && isNotFound?.(out)) return { ...out, notFound: true }
    last = out
    if (attempt < CLI_TRIES) sleepSync(750 * attempt)
  }
  throw new Error(
    `npx wrangler ${args.join(' ')} failed ${CLI_TRIES} times; last: status ${last.status}${last.error ? ` (${last.error.message})` : ''}\n` +
      `--- its stderr ---\n${last.stderr}--- its stdout ---\n${last.stdout.toString().slice(0, 2000)}`,
  )
}
function d1(sql) {
  const out = cli(['d1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json', '--command', sql])
  return JSON.parse(out.stdout.toString('utf8'))[0]?.results ?? []
}
/**
 * The local bucket's object under `key`, or null when it holds NONE: wrangler's own not-found answer ("The specified
 * key does not exist.", its local `get`'s `UserError`). Any other failure is a failed call and THROWS (a FAIL), so
 * legs 2, 4 and 5 assert a deletion only on a true not-found, never on a CLI that did not answer.
 */
const R2_NOT_FOUND = /The specified key does not exist/
function r2get(key) {
  const r = cli(['r2', 'object', 'get', `${BUCKET}/${key}`, '--local', '--persist-to', STATE, '--pipe'], {
    isNotFound: (o) => R2_NOT_FOUND.test(o.stderr) || R2_NOT_FOUND.test(o.stdout.toString('utf8')),
  })
  return r.notFound ? null : r.stdout
}
function r2del(key) {
  cli(['r2', 'object', 'delete', `${BUCKET}/${key}`, '--local', '--persist-to', STATE])
}
/** Writes `bytes` under `key` with NO custom metadata (see leg 7). */
function r2put(key, bytes) {
  const file = `${STATE}/e585f2-put.bin`
  writeFileSync(file, bytes)
  cli(['r2', 'object', 'put', `${BUCKET}/${key}`, '--file', file, '--local', '--persist-to', STATE])
}
/** The local bucket's own row of the object whose key starts with `prefix` (miniflare's `_mf_objects`). */
function r2meta(prefix) {
  const files = []
  const walk = (d) => {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      if (e.isDirectory()) walk(`${d}/${e.name}`)
      else if (e.name.endsWith('.sqlite')) files.push(`${d}/${e.name}`)
    }
  }
  try { walk(`${STATE}/v3/r2`) } catch { return null }
  for (const f of files) {
    try {
      const out = execFileSync('sqlite3', ['-json', f, `SELECT key, uploaded, custom_metadata FROM _mf_objects WHERE key LIKE '${prefix}%'`], { encoding: 'utf8' })
      const row = out.trim() ? JSON.parse(out)[0] : null
      if (row) return { key: row.key, uploaded: Number(row.uploaded), custom: JSON.parse(row.custom_metadata || '{}') }
    } catch {}
  }
  return null
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

// 1. Four ~8 KB "not now" letters, by key (E is leg 6's twin).
const [A, B, C, E] = [await chain(8_000, 'A'), await chain(8_000, 'B'), await chain(8_000, 'C'), await chain(8_000, 'E')]
expect(
  [A, B, C, E].every((x) => x.rg.status === 200 && x.rs.status === 200 && x.rs.mutation === 'queued'),
  'four ~8 KB "not now" successors past the room are acked: queued',
  [A, B, C, E].map((x) => `${x.rg.status}/${x.rs.status} ${x.rs.mutation}: ${x.rs.text.slice(0, 120)}`).join(' | '),
)
expect(
  ((await health()).counters?.beef_blobs_written_total ?? NaN) === (c0.beef_blobs_written_total ?? NaN) + 4,
  'each was written to R2 before its ack (beef_blobs_written_total moved by four)',
  `${c0.beef_blobs_written_total} -> ${(await health()).counters?.beef_blobs_written_total}`,
)
for (const x of [A, B, C, E]) {
  x.row = await until(() => {
    const r = letter(x.s)
    return r && r.status === 'parked' ? r : null
  })
}
expect([A, B, C, E].every((x) => !!x.row), `each dead-letters after its replays and PARKS (within ${WAIT_MS / 1000} s)`, [A, B, C, E].map((x) => brief(letter(x.s))).join(' | '))
for (const x of [A, B, C, E]) {
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
    !!again && again.class === 'not_now' && /MISSING/.test(again.fault ?? '') && again.r2_key === B.key &&
      (h3.counters?.beef_blobs_missing_total ?? 0) >= (c0.beef_blobs_missing_total ?? 0) + 1,
    `B: a missing object is the replay's fault, parked again with its key and its class KEPT (not_now; within ${WAIT_MS / 1000} s)`,
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
  expect(false, `a ${D.beef.length} B submission is carried (the door refuses no body for its size, #568)`, `replayMaxBytes is ${max}; ${D.rs.status}: ${D.rs.text.slice(0, 300)}; written0 ${written0}`)
}

// 6. E: the twin ack. Its subject lands by another road, its object goes, its re-drive is acked as a twin.
if (E.row) {
  d1(`DELETE FROM pot_evictions WHERE txid = '${E.p}'`)
  const again = await submit(E.beef)
  const applied = Number(d1(`SELECT COUNT(*) AS c FROM applied_transactions WHERE txid = '${E.s}'`)[0]?.c ?? 0)
  expect(again.status === 200 && again.mutation === null && applied >= 1, 'E: its predecessor readmitted, its bytes presented again LAND at the door (durable, no queue)', `${again.status} ${again.mutation}: ${again.text.slice(0, 200)} applied=${applied}`)
  r2del(E.key)
  const twin0 = (await health()).counters?.queue_r2_twin_acked_total ?? 0
  const re = await lever({ txid: E.s })
  expect((re.json?.redriven ?? []).length === 1, 'E: its object deleted, the lever re-drives the key', re.text.slice(0, 300))
  const gone = await until(() => (letter(E.s) === undefined ? true : null))
  const h6 = await health()
  expect(
    !!gone && (h6.counters?.queue_r2_twin_acked_total ?? 0) >= twin0 + 1,
    `E: MISSING and landed, the replay is ACKED as a twin: no letter, queue_r2_twin_acked_total moved (within ${WAIT_MS / 1000} s)`,
    `${brief(letter(E.s))}; twin ${twin0} -> ${h6.counters?.queue_r2_twin_acked_total}`,
  )
}

// 7. The sweep, through the scheduled event (`wrangler dev --test-scheduled`).
{
  // The object is put WITHOUT custom metadata ON PURPOSE (the d3 fold-3, E585-D3-DELTA-M1): an unstamped object (an
  // operator's CLI put, one written before the stamp) is the one whose metadata read could throw through wasm and
  // wedge the scheduled tick before its GASP step. Locally miniflare answers `{}` for it, so this leg shows the pass
  // runs over it and ages it by `uploaded`; the platform's own answer is the beta check of the fold-3 REPORT.
  const young = `mutations/${'e5'.repeat(32)}/${'0'.repeat(32)}`
  r2put(young, Buffer.from('a young object nothing names, with no customMetadata'))
  const before = d1('SELECT last_pass_at FROM beef_blob_sweep WHERE id = 1')[0]?.last_pass_at ?? 0
  const t0 = Date.now()
  fetch(OVERLAY + '/__scheduled?cron=' + encodeURIComponent('*/15 * * * *'), { headers: { Connection: 'close' }, signal: AbortSignal.timeout(WAIT_MS) }).catch(() => {})
  const row = await until(() => {
    const r = d1('SELECT last_pass_at, last_listed, last_swept, full_objects FROM beef_blob_sweep WHERE id = 1')[0]
    return r && Number(r.last_pass_at) > Number(before) && Number(r.last_pass_at) >= t0 - 60_000 ? r : null
  })
  const h7 = await health()
  const at = h7.queue?.r2
  expect(
    !!row && Number(row.last_listed) >= 1 && Number(row.last_swept) === 0 && r2get(young) !== null,
    `the scheduled tick ran one sweep pass: the beef_blob_sweep row advanced, the young object untouched (within ${WAIT_MS / 1000} s)`,
    `${JSON.stringify(row)}; young ${r2get(young) ? 'kept' : 'GONE'}`,
  )
  expect(
    at?.bound === true && at?.readable === true && typeof at?.atRest?.objects === 'number' && at.atRest.objects >= 1 && typeof at?.sweep?.lastPassAt === 'number',
    '/health/invariants.queue.r2 serves the objects at rest (atRest) and the pass',
    JSON.stringify(at),
  )
  r2del(young)
}

// 8. A put twice: the touched stamp moves.
{
  const F = await chain(8_000, 'F')
  const key = `mutations/${F.sha}/`
  const first = r2meta(key)
  await sleep(1_100)
  const again = await submit(F.beef)
  const second = r2meta(key)
  const t1 = Number(first?.custom?.touched), t2 = Number(second?.custom?.touched)
  expect(
    F.rs.mutation === 'queued' && again.mutation === 'queued' && first && second && first.key === second.key && t2 > t1,
    'the same "not now" bytes presented twice write one key twice, and its customMetadata.touched MOVED',
    `${F.rs.status} ${F.rs.mutation} / ${again.status} ${again.mutation}; ${JSON.stringify(first)} -> ${JSON.stringify(second)}`,
  )
  if (first && second) {
    lines.push(`NOTE  N2 (b), local R2: on the re-put \`uploaded\` ${second.uploaded > first.uploaded ? 'MOVED' : 'did NOT move'} (${first.uploaded} -> ${second.uploaded}); the sweep reads the later of it and touched`)
  }
  const parked = await until(() => {
    const r = letter(F.s)
    return r && r.status === 'parked' ? r : null
  })
  if (parked) await lever({ letters: [{ txid: F.s }] }, '/internal/discard-dead-letters')
}
} catch (e) {
  // the whole message: a CLI failure carries its stderr in full (the d3 fold-3, E585-D3-DELTA-M2)
  expect(false, 'the cell ran to its end', `${e.message ?? e}`.split('\n').join('\n      '))
}

console.log('── lane E585-d3: a queued BEEF past the room rides by key, in R2 ──')
for (const l of lines) console.log(l)
console.log(failures === 0 ? 'beef blobs route tier: OK' : `beef blobs route tier: ${failures} FAILED`)
process.exit(failures === 0 ? 0 : 1)
