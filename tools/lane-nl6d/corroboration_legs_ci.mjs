#!/usr/bin/env node
/**
 * NL-6d route-level witness, part of `make ci-route`: a corroboration of any
 * number of legs is read one leg at a time, finished in the request where the
 * budget allows and otherwise deferred, never answered 502 for its count (the
 * charter "a BEEF of any size", bsv-stack-lean docs/charters/beef-of-any-size.md;
 * bsv-stack-lean #60).
 *
 * Until NL-6d the broadcaster refused to corroborate a batch of more than 32
 * EF legs (`MAX_CORROBORATION_LEGS` in `broadcaster.rs`): the corroboration was
 * inconclusive, the route answered 502 and nothing was admitted; a job the
 * NL-6c deferral had queued answered the same 502 on every run and ended
 * `failed` after five.
 *
 * Three bodies, each a valid proofless V1 BEEF: a chain whose first
 * transaction spends an absent source (so it is no EF leg) and whose every
 * later transaction spends the one before it; the subject is the tip.
 *   - 33 legs (32 ancestors and the subject);
 *   - 1,000 legs (999 ancestors and the subject);
 *   - 40 legs whose first leg carries 2,200,000 bytes and more, so the batch's
 *     EF is past the NL-6c byte budget and the request defers it at once.
 * `tm_collected` admits none of their outputs, so no row is written.
 *
 * This process is both hosts (`FIXTURE_PORT`):
 *   - the Arcade (`ARCADE_URL`): an unknown subject to every status probe, and
 *     SEEN_ON_NETWORK echoed to every submit, so the gated ladder takes the
 *     accept claim of a subject with unproven ancestry, which the #267 rule
 *     corroborates WITH its ancestry before it may admit;
 *   - the corroborating hosts (`CORROBORATOR_URL`, `/v1/tx`): an ancestor is
 *     answered SEEN_ON_NETWORK; the subject is answered an ORPHAN view until
 *     every one of its ancestors has been posted, then SEEN_ON_NETWORK. So the
 *     subject is accepted only after every leg was read, and the log counts
 *     how often each leg was posted.
 * The worker runs with `DUAL_BROADCAST=off`. No network is needed and nothing
 * is broadcast.
 *
 * Expectations:
 *  1. 33 legs: 200 in the request (the budget allows it), never 502; every
 *     ancestor posted to the corroborator exactly once;
 *  2. 1,000 legs: never 502; 202 with a reference; the reference reaches
 *     `done` with the arm's 200; every ancestor posted exactly once over the
 *     request and every run (a run resumes from the leg the last one reached,
 *     none starts over), and the subject presented in more than two runs;
 *  3. 40 legs past the byte budget: 202 with a reference; the job's run
 *     reads every ancestor exactly once and the corroborator accepts the
 *     subject; the job's answer is never the leg count's refusal.
 * In every case the corroborator accepted the subject only after every one of
 * its ancestors was posted.
 *
 * NOT THIS WITNESS'S, said so it is not lost: the 40-leg body's admission
 * meets a different ceiling after the corroboration. `tm_collected` admits
 * nothing over an unproven chain deeper than the engine's predecessor
 * question reads (16), so Phase 3 is "not now" and the S2 replay is queued
 * with the BEEF's bytes in the message; a BEEF over `QUEUE_BEEF_SIZE_LIMIT`
 * (90,000 B) cannot be queued and the arm answers 502 "admission not
 * durable". The witness prints that answer as a NOTE; it is not the count.
 *
 * MODELLING BOUNDARY: `wrangler dev --local` with its local queue and D1; the
 * platform's own limits are not exercised. The fixture answers at once, so the
 * witness measures the leg count, never a host's latency.
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { createServer } from 'node:http'

const BASE = process.argv[2] ?? 'http://127.0.0.1:9812'
const FIXTURE_PORT = Number(process.env.FIXTURE_PORT ?? '9813')

const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
function varint(n) {
  if (n < 0xfd) return Buffer.from([n])
  if (n <= 0xffff) { const b = Buffer.alloc(3); b[0] = 0xfd; b.writeUInt16LE(n, 1); return b }
  return Buffer.concat([Buffer.from([0xfe]), u32(n)])
}
function readVarint(buf, at) {
  const b = buf[at]
  if (b < 0xfd) return [b, at + 1]
  if (b === 0xfd) return [buf.readUInt16LE(at + 1), at + 3]
  if (b === 0xfe) return [buf.readUInt32LE(at + 1), at + 5]
  return [Number(buf.readBigUInt64LE(at + 1)), at + 9]
}
const sha256 = (b) => createHash('sha256').update(b).digest()
const txidLE = (raw) => sha256(sha256(raw))
const txidHex = (raw) => Buffer.from(txidLE(raw)).reverse().toString('hex')

const OP_TRUE = Buffer.from([0x51])
const opReturn = (pad) => Buffer.concat([Buffer.from([0x00, 0x6a, 0x4e]), u32(pad), Buffer.alloc(pad, 0x42)])

function rawTx(prev, vout, outputs) {
  return Buffer.concat([
    u32(1),
    varint(1), prev, u32(vout), varint(0), Buffer.from([0xff, 0xff, 0xff, 0xff]),
    varint(outputs.length),
    ...outputs.flatMap(([sats, script]) => [u64(sats), varint(script.length), script]),
    u32(0),
  ])
}
const freshPrev = () => { const b = Buffer.alloc(32); randomFillSync(b); return b }
const beefOf = (raws) =>
  Buffer.concat([Buffer.from([0x01, 0x00, 0xbe, 0xef]), varint(0), varint(raws.length), ...raws.flatMap((r) => [r, Buffer.from([0x00])])])

/** The raw transaction inside an Extended Format body (BRC-30): the marker and each input's source output dropped. */
function txidOfEf(ef) {
  let at = 4 + 6
  const parts = [ef.subarray(0, 4)]
  let n
  let start = at
  ;[n, at] = readVarint(ef, at)
  parts.push(ef.subarray(start, at))
  for (let i = 0; i < n; i++) {
    start = at
    at += 36
    let len
    ;[len, at] = readVarint(ef, at)
    at += len + 4
    parts.push(ef.subarray(start, at))
    at += 8
    ;[len, at] = readVarint(ef, at)
    at += len
  }
  parts.push(ef.subarray(at))
  return txidHex(Buffer.concat(parts))
}

/**
 * A chain of `legs + 1` transactions: the first spends an absent source (no EF
 * leg), each later one spends the one before; `bigFirstLeg` pads the first EF
 * leg past the NL-6c batch budget.
 */
function chain(legs, bigFirstLeg = 0) {
  const raws = []
  let prev = freshPrev()
  for (let i = 0; i <= legs; i++) {
    const last = i === legs
    const outputs = last
      ? [[0, opReturn(64)]]
      : [[1_000_000 - i, OP_TRUE], ...(i === 1 && bigFirstLeg ? [[0, opReturn(bigFirstLeg)]] : [])]
    const raw = rawTx(prev, 0, outputs)
    raws.push(raw)
    prev = txidLE(raw)
  }
  const txids = raws.map(txidHex)
  return { beef: beefOf(raws), subject: txids[legs], ancestors: txids.slice(1, legs) }
}

// ── the fixture: the Arcade and the corroborating hosts ────────────────────────
const posts = new Map() // txid → corroborator posts
const subjects = new Map() // subject txid → its ancestors
const arcadeSubmits = { count: 0 }
const acceptedAfterAll = new Set() // subjects the corroborator accepted, each after every ancestor
const fixture = createServer((req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${FIXTURE_PORT}`)
  const chunks = []
  req.on('data', (c) => chunks.push(c))
  req.on('end', () => {
    const json = (status, body) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)) }
    if (req.method === 'GET' && url.pathname.startsWith('/tx/')) return json(404, { error: 'not found' })
    if (req.method === 'POST' && (url.pathname === '/tx' || url.pathname === '/txs')) {
      arcadeSubmits.count++
      return json(200, { txStatus: 'SEEN_ON_NETWORK' })
    }
    if (req.method === 'POST' && url.pathname === '/v1/tx') {
      let txid = ''
      try { txid = txidOfEf(Buffer.from(JSON.parse(Buffer.concat(chunks).toString()).rawTx, 'hex')) } catch { return json(400, { error: 'unreadable' }) }
      posts.set(txid, (posts.get(txid) ?? 0) + 1)
      const ancestors = subjects.get(txid)
      if (ancestors && !ancestors.every((a) => posts.has(a))) {
        return json(200, { txid, txStatus: 'SEEN_IN_ORPHAN_MEMPOOL', extraInfo: 'ORPHAN: missing inputs' })
      }
      if (ancestors) acceptedAfterAll.add(txid)
      return json(200, { txid, txStatus: 'SEEN_ON_NETWORK' })
    }
    res.writeHead(404)
    res.end()
  })
})
await new Promise((resolve, reject) => {
  fixture.once('error', reject)
  fixture.listen(FIXTURE_PORT, '127.0.0.1', resolve)
})

async function submit(body) {
  const headers = {
    'Content-Type': 'application/octet-stream',
    'x-topics': JSON.stringify(['tm_collected']),
    'x-submit-mode': 'broadcast-gated',
  }
  // the dev server's own reload answers (see tools/lane-nl6c): sent again
  for (let attempt = 1; ; attempt++) {
    let res
    try {
      res = await fetch(`${BASE}/submit`, { method: 'POST', headers, body })
    } catch (e) {
      const code = e && e.cause && e.cause.code
      if (attempt < 6 && /^(ECONNRESET|EPIPE|ECONNREFUSED|UND_ERR_SOCKET)$/.test(String(code))) {
        console.log(`      (wrangler dev dropped the connection, ${code}; sending the ${body.length}-byte body again after a pause)`)
        await new Promise((r) => setTimeout(r, 1500 * attempt))
        continue
      }
      throw e
    }
    const text = await res.text()
    if (res.status === 503 && /restarted mid-request/.test(text) && attempt < 6) {
      console.log(`      (wrangler dev reloaded the worker; sending the ${body.length}-byte body again)`)
      await new Promise((r) => setTimeout(r, 1000))
      continue
    }
    let json = null
    try { json = JSON.parse(text) } catch { /* not JSON */ }
    return { status: res.status, text, json }
  }
}

async function pollDone(path, timeoutMs = 180_000) {
  const t0 = Date.now()
  let last = null
  for (;;) {
    const res = await fetch(`${BASE}${path}`)
    const text = await res.text()
    try { last = { status: res.status, json: JSON.parse(text) } } catch { last = { status: res.status, text } }
    const state = last.json?.state
    if (state === 'done' || state === 'failed') return last
    // a run's answer that is no progress (not a 202 naming the next leg): the job waits for the cron's hand-back
    const answered = last.json?.answer?.status
    if (state === 'queued' && answered !== undefined && answered !== 202) return last
    if (Date.now() - t0 > timeoutMs) return last
    await new Promise((r) => setTimeout(r, 1000))
  }
}

let failures = 0
function expect(label, ok, got) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}  → ${got}`)
  if (!ok) failures++
}
/** Every ancestor posted to the corroborator exactly once. */
function eachLegOnce(built) {
  const counts = built.ancestors.map((a) => posts.get(a) ?? 0)
  const once = counts.filter((c) => c === 1).length
  const never = counts.filter((c) => c === 0).length
  const twice = counts.filter((c) => c > 1).length
  return { ok: once === built.ancestors.length, desc: `${once} once, ${never} never, ${twice} more than once, of ${built.ancestors.length}` }
}

try {
  // 1. 33 legs: the request's budget allows it
  const b33 = chain(33)
  subjects.set(b33.subject, b33.ancestors)
  const r33 = await submit(b33.beef)
  expect(
    `33 legs (${b33.beef.length.toLocaleString('en-US')} bytes): answered in the request (200), never 502`,
    r33.status === 200 && !r33.json?.deferred,
    `status=${r33.status} ${r33.text.slice(0, 200)}`,
  )
  const o33 = eachLegOnce(b33)
  expect('33 legs: every ancestor read by the corroborator exactly once', o33.ok, o33.desc)
  expect('33 legs: the corroborator accepted the subject after every ancestor', acceptedAfterAll.has(b33.subject), `${acceptedAfterAll.has(b33.subject)}`)

  for (const [label, built] of [
    ['1,000 legs', chain(1000)],
    ['40 legs past the byte budget', chain(40, 2_200_000)],
  ]) {
    subjects.set(built.subject, built.ancestors)
    const submitsBefore = arcadeSubmits.count
    const r = await submit(built.beef)
    const j = r.json ?? {}
    expect(
      `${label} (${built.beef.length.toLocaleString('en-US')} bytes): 202 accepted with a reference, never 502`,
      r.status === 202 && j.accepted === true && j.deferred === true &&
        typeof j.reference === 'string' && j.reference.length === 64 &&
        j.poll === `/submit-deferred/${j.reference}` && j.subjectTxid === built.subject,
      `status=${r.status} ${r.text.slice(0, 240)}`,
    )
    if (r.status !== 202 || typeof j.poll !== 'string') continue
    const done = await pollDone(j.poll)
    const d = done.json ?? {}
    const answerText = JSON.stringify(d.answer ?? null)
    if (label === '1,000 legs') {
      expect(
        `${label}: the reference reaches done with the arm's own answer (200)`,
        done.status === 200 && d.state === 'done' && d.answer?.status === 200 && d.subjectTxid === built.subject,
        `status=${done.status} ${JSON.stringify(d).slice(0, 300)}`,
      )
    } else {
      expect(
        `${label}: the job's run answered, and never with the leg count's refusal`,
        done.status === 200 && d.answer && !/leg cap|EF legs >|corroboration/i.test(answerText),
        `state=${d.state} legsFrom=${d.legsFrom} answer ${answerText.slice(0, 160)}`,
      )
      if (d.answer?.status !== 200) {
        console.log(`NOTE  ${label}: the admission after the corroboration answered ${d.answer?.status}: ${answerText.slice(0, 200)} (not the count: the S2 replay's queue message bound, see the header)`)
      }
    }
    const once = eachLegOnce(built)
    expect(`${label}: every ancestor read by the corroborator exactly once (no run starts over)`, once.ok, once.desc)
    expect(`${label}: the corroborator accepted the subject after every ancestor`, acceptedAfterAll.has(built.subject), `${acceptedAfterAll.has(built.subject)}`)
    if (label === '1,000 legs') {
      const runs = arcadeSubmits.count - submitsBefore
      expect(
        `${label}: the work ran in more than two invocations (the request and the runs that resumed it)`,
        runs > 2,
        `${runs} presentation(s) of the subject to the Arcade`,
      )
    }
  }
} finally {
  fixture.close()
}

if (failures) {
  console.error(`\nNL-6d corroboration legs route witness: ${failures} expectation(s) FAILED.`)
  process.exit(1)
}
console.log('\nNL-6d corroboration legs route witness: every expectation held.')
process.exit(0)
