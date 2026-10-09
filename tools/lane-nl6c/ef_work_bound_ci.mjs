#!/usr/bin/env node
/**
 * NL-6c route-level witness, part of `make ci-route`: the broadcast-gated arm
 * does not refuse a valid submission for the size of its Extended Format work
 * (the charter "a BEEF of any size", bsv-stack-lean
 * docs/charters/beef-of-any-size.md).
 *
 * Until NL-6c the arm answered 429 "EF too large ... retry via fallback" when
 * the subject's EF passed 256 KiB or the batch's passed 2 MiB
 * (`MAX_SUBJECT_EF_BYTES`, `MAX_BATCH_EF_BYTES` in `routes.rs`). Now the
 * request keeps a work budget and routes past it: the work that fits is done
 * in the request, as before; the rest is answered 202 with a reference, its
 * bytes rest in D1 (R2 when the binding exists), and the queue consumer
 * finishes it. The caller polls the reference for the outcome.
 *
 * Three bodies, each a valid proofless V1 BEEF whose subject is the tip of
 * its own ancestry (no courier is asked for a source):
 *   - the control: a small subject; answered in the request, as before;
 *   - over the subject budget: one subject whose EF is 300,000 bytes and more;
 *   - over the batch budget: an unproven parent of 2,200,000 bytes and more
 *     under a small subject (two EF legs).
 * `tm_collected` admits none of their outputs, so no row is written.
 *
 * The broadcaster is THIS process (`FIXTURE_PORT`): a fixture Arcade that
 * answers every status probe SEEN_ON_NETWORK, so the gated ladder stops at
 * its pre-flight probe and no rung, no corroborator and no real host is ever
 * asked. The worker runs with `DUAL_BROADCAST=off`, so nothing is pushed to
 * TAAL or GorillaPool either. No network is needed and nothing is broadcast.
 *
 * Expects `wrangler dev --local` on the given base with
 *   SUBMIT_ENFORCE=true, ENABLE_EXTENSIONS=true, DUAL_BROADCAST=off,
 *   ARCADE_URL=http://127.0.0.1:<FIXTURE_PORT>, TOPIC_MANAGERS=tm_collected,...
 *
 * Expectations:
 *  1. the control: 200 in the request, the probe asked for its subject;
 *  2. each over-budget body: never 429, never 413; 202 with `accepted: true`,
 *     `deferred: true`, a `reference` and a `poll` path naming it, and the
 *     subject's txid;
 *  3. each reference polled: the job reaches `done` with the outcome the
 *     request would have answered (200), and the fixture saw the deferred
 *     work's probe for the subject AFTER the 202 (the consumer did the work);
 *  4. the same body sent again while its job is open names the same reference.
 *
 * MODELLING BOUNDARY: the worker is `wrangler dev --local` with its local
 * queue and its local D1. The platform's own limits (the request body, the
 * isolate's memory, the consumer's CPU slice) are not exercised here.
 *
 * Exit 0 = every expectation held.
 */
import { createHash, randomFillSync } from 'node:crypto'
import { createServer } from 'node:http'

const BASE = process.argv[2] ?? 'http://127.0.0.1:9612'
const FIXTURE_PORT = Number(process.env.FIXTURE_PORT ?? '9613')
const SUBJECT_BUDGET = 256 * 1024
const BATCH_BUDGET = 2 * 1024 * 1024

const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
function varint(n) {
  if (n < 0xfd) return Buffer.from([n])
  if (n <= 0xffff) { const b = Buffer.alloc(3); b[0] = 0xfd; b.writeUInt16LE(n, 1); return b }
  return Buffer.concat([Buffer.from([0xfe]), u32(n)])
}
const sha256 = (b) => createHash('sha256').update(b).digest()
/** The txid as the wire names it in an outpoint (internal order). */
const txidLE = (raw) => sha256(sha256(raw))
const txidHex = (raw) => Buffer.from(txidLE(raw)).reverse().toString('hex')

const OP_TRUE = Buffer.from([0x51])
/** OP_FALSE OP_RETURN OP_PUSHDATA4 <pad>. */
const opReturn = (pad) => Buffer.concat([Buffer.from([0x00, 0x6a, 0x4e]), u32(pad), Buffer.alloc(pad, 0x42)])

/** A raw transaction: one input at `prev` (32 bytes, internal order) : `vout`, the outputs as given. */
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
/** A proofless V1 BEEF of the raws, parents first. */
const beefOf = (raws) =>
  Buffer.concat([Buffer.from([0x01, 0x00, 0xbe, 0xef]), varint(0), varint(raws.length), ...raws.flatMap((r) => [r, Buffer.from([0x00])])])

/** A subject whose own EF passes the subject budget; its parent is not an EF leg (its source is absent). */
function overSubject() {
  const parent = rawTx(freshPrev(), 0, [[1000, OP_TRUE]])
  const subject = rawTx(txidLE(parent), 0, [[0, opReturn(300_000)]])
  return { beef: beefOf([parent, subject]), subject: txidHex(subject) }
}
/** A small subject over an unproven parent that is itself an EF leg of 2,200,000 bytes and more. */
function overBatch() {
  const grand = rawTx(freshPrev(), 0, [[2000, OP_TRUE]])
  const parent = rawTx(txidLE(grand), 0, [[1000, OP_TRUE], [0, opReturn(2_200_000)]])
  const subject = rawTx(txidLE(parent), 0, [[0, opReturn(64)]])
  return { beef: beefOf([grand, parent, subject]), subject: txidHex(subject) }
}
function control() {
  const parent = rawTx(freshPrev(), 0, [[1000, OP_TRUE]])
  const subject = rawTx(txidLE(parent), 0, [[0, opReturn(64)]])
  return { beef: beefOf([parent, subject]), subject: txidHex(subject) }
}

// ── the fixture Arcade: every probe is SEEN; every request is logged ──────────
const log = [] // { method, path, at }
const fixture = createServer((req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${FIXTURE_PORT}`)
  const chunks = []
  req.on('data', (c) => chunks.push(c))
  req.on('end', () => {
    log.push({ method: req.method, path: url.pathname, at: Date.now() })
    if (req.method === 'GET' && url.pathname.startsWith('/tx/')) {
      const txid = url.pathname.slice('/tx/'.length).toLowerCase()
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ txid, txStatus: 'SEEN_ON_NETWORK' }))
      return
    }
    if (req.method === 'POST' && (url.pathname === '/tx' || url.pathname === '/txs')) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ txStatus: 'SEEN_ON_NETWORK' }))
      return
    }
    res.writeHead(404)
    res.end()
  })
})
await new Promise((resolve, reject) => {
  fixture.once('error', reject)
  fixture.listen(FIXTURE_PORT, '127.0.0.1', resolve)
})
const probedSince = (txid, since) => log.some((e) => e.method === 'GET' && e.path === `/tx/${txid}` && e.at >= since)

async function submit(body) {
  const headers = {
    'Content-Type': 'application/octet-stream',
    'x-topics': JSON.stringify(['tm_collected']),
    'x-submit-mode': 'broadcast-gated',
  }
  // `wrangler dev`'s own proxy answers 503 "Your worker restarted mid-request"
  // when it reloads the worker and retries only GET and HEAD; that answer is
  // the dev server's, not the route's, so a POST that meets it is sent again.
  for (let attempt = 1; ; attempt++) {
    let res
    try {
      res = await fetch(`${BASE}/submit`, { method: 'POST', headers, body })
    } catch (e) {
      // The dev server drops the connection while it reloads the worker after
      // a large body (ECONNRESET, EPIPE, a refused connect); that is the dev
      // server's, not the route's, so the body is sent again after a pause.
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

async function pollDone(path, timeoutMs = 90_000) {
  const t0 = Date.now()
  let last = null
  for (;;) {
    const res = await fetch(`${BASE}${path}`)
    const text = await res.text()
    try { last = { status: res.status, json: JSON.parse(text) } } catch { last = { status: res.status, text } }
    const state = last.json?.state
    if (state === 'done' || state === 'failed') return last
    if (Date.now() - t0 > timeoutMs) return last
    await new Promise((r) => setTimeout(r, 1000))
  }
}

let failures = 0
function expect(label, ok, got) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}  → ${got}`)
  if (!ok) failures++
}

try {
  // 1. the control: answered in the request, as before
  const c = control()
  const c0 = Date.now()
  const rc = await submit(c.beef)
  expect(
    `control, ${c.beef.length} bytes: answered in the request (200), the broadcaster asked`,
    rc.status === 200 && !rc.json?.deferred && probedSince(c.subject, c0),
    `status=${rc.status} ${rc.text.slice(0, 120)}`,
  )

  for (const [label, built, efLabel] of [
    ['over the subject budget', overSubject(), `subject EF > ${SUBJECT_BUDGET}`],
    ['over the batch budget', overBatch(), `batch EF > ${BATCH_BUDGET}`],
  ]) {
    // 2. accepted with a reference, never refused for its size
    const r = await submit(built.beef)
    const sentAt = Date.now()
    const j = r.json ?? {}
    expect(
      `${label} (${built.beef.length.toLocaleString('en-US')} bytes, ${efLabel}): 202 accepted with a reference, never 429 or 413`,
      r.status === 202 && j.accepted === true && j.deferred === true &&
        typeof j.reference === 'string' && j.reference.length === 64 &&
        j.poll === `/submit-deferred/${j.reference}` && j.subjectTxid === built.subject,
      `status=${r.status} ${r.text.slice(0, 200)}`,
    )
    if (r.status !== 202 || typeof j.poll !== 'string') continue

    // 4. the same body again, while its job is open: the same reference
    const again = await submit(built.beef)
    expect(
      `${label}: sent again while open, the same reference`,
      again.status === 202 && again.json?.reference === j.reference,
      `status=${again.status} ${again.text.slice(0, 160)}`,
    )

    // 3. the consumer finishes the work and the reference says so
    const done = await pollDone(j.poll)
    const d = done.json ?? {}
    expect(
      `${label}: the reference reaches done with the request's own answer (200)`,
      done.status === 200 && d.state === 'done' && d.answer?.status === 200 && d.subjectTxid === built.subject,
      `status=${done.status} ${JSON.stringify(d).slice(0, 240)}`,
    )
    expect(
      `${label}: the deferred work asked the broadcaster for the subject after the 202`,
      probedSince(built.subject, sentAt - 50),
      `fixture log: ${log.filter((e) => e.path.includes(built.subject)).length} request(s) for the subject`,
    )
  }

  // an unknown reference is 404, never a 5xx
  const unknown = await fetch(`${BASE}/submit-deferred/${'0'.repeat(64)}`)
  expect('an unknown reference: 404', unknown.status === 404, `status=${unknown.status}`)
} finally {
  fixture.close()
}

if (failures) {
  console.error(`\nNL-6c EF work bound route witness: ${failures} expectation(s) FAILED.`)
  process.exit(1)
}
console.log('\nNL-6c EF work bound route witness: every expectation held.')
process.exit(0)
