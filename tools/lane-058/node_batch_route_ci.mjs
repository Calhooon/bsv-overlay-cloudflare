#!/usr/bin/env node
/**
 * Engine 0.2.1 route-level witness (#58), part of `make ci-route`: the batched
 * node request `POST /requestForeignGASPNodes` answers as the single route
 * `/requestForeignGASPNode` does, entry by entry, and refuses a request over
 * its cap with the cap in the body.
 *
 * Expects the strict worker of `make ci-route`. No network is needed.
 *
 * MODELLING BOUNDARY: the worker is `wrangler dev --local` with an empty
 * index, so every outpoint here is one it does not hold. That a held output
 * is served with its node and proof as the single route serves it is the
 * engine's witness `batch_021_the_provider_answers_each_outpoint_as_the_single_route`.
 */
import { randomBytes } from 'node:crypto'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8791'
const CAP = 100

let failures = 0
function expect(ok, label, detail) {
  if (ok) {
    console.log(`  PASS ${label}`)
  } else {
    failures += 1
    console.log(`  FAIL ${label}: ${detail}`)
  }
}

async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  })
  const text = await res.text()
  let json = null
  try { json = JSON.parse(text) } catch { /* not JSON */ }
  return { status: res.status, json, text }
}

const txid = () => randomBytes(32).toString('hex')

console.log(`→ node batch route witness on ${BASE}`)

// 1. Over the cap: 400, the cap named.
{
  const outpoints = Array.from({ length: CAP + 1 }, () => ({ txid: txid(), vout: 0 }))
  const r = await post('/requestForeignGASPNodes', { outpoints })
  expect(r.status === 400, 'a request over the cap is answered 400', `HTTP ${r.status} ${r.text}`)
  expect(r.json?.maxOutpoints === CAP, 'the 400 names the cap', r.text)
}

// 2. Outpoints the peer does not hold: one not_found entry each, in order,
// each naming its outpoint; a repeat answered once.
const a = txid()
const b = txid()
{
  const r = await post('/requestForeignGASPNodes', {
    outpoints: [{ txid: a, vout: 0 }, { txid: b, vout: 3 }, { txid: a, vout: 0 }],
  })
  expect(r.status === 200, 'a request at or under the cap is answered 200', `HTTP ${r.status} ${r.text}`)
  const nodes = r.json?.nodes ?? []
  expect(nodes.length === 2, 'one entry per outpoint, a repeat answered once', r.text)
  expect(
    nodes[0]?.txid === a && nodes[0]?.vout === 0 && nodes[0]?.refused === 'not_found' && !('node' in nodes[0]),
    'the entry names its outpoint and the refusal not_found',
    JSON.stringify(nodes[0]),
  )
  expect(
    nodes[1]?.txid === b && nodes[1]?.vout === 3 && nodes[1]?.refused === 'not_found',
    'the entries keep the asked order',
    JSON.stringify(nodes[1]),
  )
}

// 3. The single route's word for the same outpoint: HTTP 400, the peer's
// "I do not hold it" (the class the batch's not_found stands for).
{
  const r = await post('/requestForeignGASPNode', { graphID: `${a}.0`, txid: a, outputIndex: 0 })
  expect(r.status === 400, 'the single route answers the same outpoint 400', `HTTP ${r.status} ${r.text}`)
}

// 4. A body that is not the batch's shape: 400, nothing read.
{
  const r = await post('/requestForeignGASPNodes', { outpoints: [], extra: 1 })
  expect(r.status === 400, 'an unknown field is refused 400', `HTTP ${r.status} ${r.text}`)
  const empty = await post('/requestForeignGASPNodes', { outpoints: [] })
  expect(
    empty.status === 200 && Array.isArray(empty.json?.nodes) && empty.json.nodes.length === 0,
    'an empty request is an empty answer',
    `HTTP ${empty.status} ${empty.text}`,
  )
}

if (failures > 0) {
  console.log(`✗ node batch route witness: ${failures} failure(s)`)
  process.exit(1)
}
console.log('node batch route witness: every expectation held.')
