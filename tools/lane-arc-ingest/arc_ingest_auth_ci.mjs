#!/usr/bin/env node
/**
 * Route-level ASSERTING harness for `/arc-ingest` bearer-auth.
 * Part of `make ci-route`.
 *
 * WHY THIS TIER EXISTS (Rule 6b / Rule 22). The defect being fixed here was a
 * HEADER NAME: the handler read only `x-callbacktoken`, and Arcade V2 sends the
 * callback token as `Authorization: Bearer <token>` — so every proof callback
 * since #228 was 401'd and the primary proof path silently fell through to the
 * ~30-min poll backstop. **No native cell can see that class**: `cargo test`
 * cannot build a `worker::Request`, so it can pin the classifier's logic and
 * still be blind to which headers the ROUTE hands it. The bug lived precisely
 * in the wiring the native tier cannot reach.
 *
 * So this drives the REAL handler over HTTP with the REAL header Arcade sets:
 *
 *  1. POSITIVE CONTROL (Rule 9's "the code under test is never reached"):
 *     `Authorization: Bearer <subject-txid>` with NO `x-callbacktoken` is
 *     ACCEPTED — 200, with the status-callback acknowledgement body, and the
 *     post-auth counter moves. A 404 (route not mounted) or a 400 (body died
 *     earlier) fails this leg, so the refusal legs below cannot pass vacuously.
 *  2. a WRONG bearer still 401s, and bumps `unauthorized_bad_token`;
 *  3. NO token at all still 401s, and bumps `unauthorized_no_token`;
 *  4. the legacy `X-CallbackToken` header form is still accepted (not traded
 *     away for the new one);
 *  5. the counters are DISTINCT (Rule 13): a refusal never moves a post-auth
 *     counter and an acceptance never moves an unauthorized counter — the
 *     collapse of exactly this distinction is what hid the outage.
 *
 * MODELLING BOUNDARY, stated at the harness as well as at the cells (Rule 17):
 * every body here is a STATUS-ONLY callback (no merklePath). That is deliberate
 * — it reaches the auth gate and the acknowledgement path without requiring a
 * real mined transaction or a live chaintracks lookup, so `make ci` needs no
 * network. This tier therefore proves NOTHING about the merklePath →
 * chaintracks-verify → stitch path, which is unchanged by this fix and is
 * covered by the native `verify_bump` cells and the proof_fetcher suite.
 *
 * Expects a wrangler-dev worker WITH TAAL_API_KEY set (the route is only
 * mounted when it is — `lib.rs`) on argv[2], default :8794.
 *
 * Exit 0 = every expectation held.
 */
import { randomBytes } from 'node:crypto'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8794'

const C_PUSHED = 'arc_ingest_pushed_total'
const C_STATUS = 'arc_ingest_status_ignored_total'
const C_NO_TOKEN = 'arc_ingest_unauthorized_no_token_total'
const C_BAD_TOKEN = 'arc_ingest_unauthorized_bad_token_total'
const ALL_COUNTERS = [C_PUSHED, C_STATUS, C_NO_TOKEN, C_BAD_TOKEN]

let failures = 0
const results = []
const pass = (label) => results.push(`PASS  ${label}`)
const fail = (label, why) => {
  failures++
  results.push(`FAIL  ${label}`)
  results.push(`      ${why}`)
}

const newTxid = () => randomBytes(32).toString('hex')

async function health() {
  const res = await fetch(`${BASE}/health/invariants`)
  if (!res.ok) throw new Error(`GET ${BASE}/health/invariants -> ${res.status}`)
  return res.json()
}

async function counters() {
  const j = await health()
  if (!j.counters) throw new Error('counters missing from /health/invariants')
  return j.counters
}

/** Every counter this harness reads must be SERVED by name, or a delta of 0 is
 *  indistinguishable from a name that does not exist (the writer/reader
 *  boundary — Rule 16). */
function assertCountersServed(c) {
  const missing = ALL_COUNTERS.filter((n) => typeof c[n] !== 'number')
  if (missing.length) {
    throw new Error(
      `/health/invariants does not serve ${missing.join(', ')} — every delta below would read 0 vacuously`,
    )
  }
}

async function postIngest(body, headers) {
  const res = await fetch(`${BASE}/arc-ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  })
  return { status: res.status, text: await res.text() }
}

/** Counters are bumped with an inline `.await` before the response is returned,
 *  so one read after the response should already see it. The bounded retry is
 *  belt-and-braces against local-D1 read latency, never a substitute for the
 *  assertion. */
async function awaitCounters(before, name) {
  let after
  for (let i = 0; i < 10; i++) {
    after = await counters()
    if ((after[name] ?? 0) - (before[name] ?? 0) >= 1) return { ok: true, after }
    await new Promise((r) => setTimeout(r, 300))
  }
  return { ok: false, after }
}

/**
 * One expectation: POST a status callback with `headers`, assert the HTTP
 * status, assert `wantCounter` moved, and assert NO OTHER arc-ingest counter
 * moved (the Rule 13 distinctness that did not exist before this fix).
 */
async function expectIngest({ label, headers, txid, wantStatus, wantBodyRe, wantCounter }) {
  const before = await counters()
  assertCountersServed(before)
  const res = await postIngest({ txid, txStatus: 'SEEN_ON_NETWORK' }, headers)
  if (res.status !== wantStatus) {
    fail(label, `expected HTTP ${wantStatus}, got ${res.status}: ${res.text.slice(0, 200)}`)
    return
  }
  if (wantBodyRe && !wantBodyRe.test(res.text)) {
    fail(label, `body did not match ${wantBodyRe}: ${res.text.slice(0, 200)}`)
    return
  }
  const got = await awaitCounters(before, wantCounter)
  if (!got.ok) {
    fail(label, `counter ${wantCounter} never moved (HTTP ${res.status})`)
    return
  }
  for (const other of ALL_COUNTERS.filter((n) => n !== wantCounter)) {
    const delta = (got.after[other] ?? 0) - (before[other] ?? 0)
    if (delta > 0) {
      fail(label, `counter ${other} ALSO moved by ${delta} — the outcomes are not distinct`)
      return
    }
  }
  pass(label)
}

// ── 1. THE FIX, at the producer: the header Arcade actually sets ────────────
// Arcade V2's contract: "X-CallbackToken is an opaque bearer token, sent on
// every outbound webhook as `Authorization: Bearer <token>`." No
// `x-callbacktoken` header is present on the webhook. This is the exact request
// shape that was 401'd on every delivery attempt for a month.
{
  const txid = newTxid()
  await expectIngest({
    label: 'Authorization: Bearer <subject-txid>, NO x-callbacktoken → ACCEPTED (200)',
    headers: { Authorization: `Bearer ${txid}` },
    txid,
    wantStatus: 200,
    // Positive control: this exact body proves the request reached the
    // post-auth acknowledgement arm, not merely "did not 401".
    wantBodyRe: /Status update acknowledged/,
    wantCounter: C_STATUS,
  })
}

// RFC 7235: the scheme is case-insensitive on the wire.
{
  const txid = newTxid()
  await expectIngest({
    label: 'authorization: bearer <subject-txid> (lowercase scheme) → ACCEPTED (200)',
    headers: { authorization: `bearer ${txid}` },
    txid,
    wantStatus: 200,
    wantBodyRe: /Status update acknowledged/,
    wantCounter: C_STATUS,
  })
}

// ── 2. Nothing else was weakened: a WRONG bearer is still refused ───────────
{
  const txid = newTxid()
  await expectIngest({
    label: 'Authorization: Bearer <WRONG token> → 401 + unauthorized_bad_token',
    headers: { Authorization: `Bearer ${newTxid()}` },
    txid,
    wantStatus: 401,
    wantBodyRe: /Unauthorized/,
    wantCounter: C_BAD_TOKEN,
  })
}

// ── 3. No credential at all is still refused — and is a DISTINCT counter ────
{
  const txid = newTxid()
  await expectIngest({
    label: 'no Authorization, no x-callbacktoken → 401 + unauthorized_no_token',
    headers: {},
    txid,
    wantStatus: 401,
    wantBodyRe: /Unauthorized/,
    wantCounter: C_NO_TOKEN,
  })
}

// A non-Bearer scheme carries no credential — same class as sending nothing.
{
  const txid = newTxid()
  await expectIngest({
    label: 'Authorization: Basic … (not a Bearer challenge) → 401 + unauthorized_no_token',
    headers: { Authorization: 'Basic Zm9vOmJhcg==' },
    txid,
    wantStatus: 401,
    wantBodyRe: /Unauthorized/,
    wantCounter: C_NO_TOKEN,
  })
}

// ── 4. The legacy header form is KEPT, not traded away ──────────────────────
{
  const txid = newTxid()
  await expectIngest({
    label: 'X-CallbackToken: <subject-txid> (legacy form) → still ACCEPTED (200)',
    headers: { 'X-CallbackToken': txid },
    txid,
    wantStatus: 200,
    wantBodyRe: /Status update acknowledged/,
    wantCounter: C_STATUS,
  })
}

// Enumerate-and-filter, not header precedence: a courier that spends
// `Authorization` on a proxy credential and carries the callback token in
// `X-CallbackToken` must still be admitted.
{
  const txid = newTxid()
  await expectIngest({
    label: 'Authorization: Basic … + X-CallbackToken: <subject-txid> → ACCEPTED (200)',
    headers: { Authorization: 'Basic Zm9vOmJhcg==', 'X-CallbackToken': txid },
    txid,
    wantStatus: 200,
    wantBodyRe: /Status update acknowledged/,
    wantCounter: C_STATUS,
  })
}

// ── 5. The health surface can now tell "refused" from "silent" ─────────────
// By this point callbacks have been both accepted and refused on this worker,
// so the lifetime-total verdict must read "flowing". (The silent/refusing
// discrimination itself is pinned natively in `ops::tests` — it is a function
// of the counters, and a route tier cannot control lifetime totals it inherits
// from a persisted local D1.)
{
  const label = 'arcIngestPushHealth is served and reads "flowing" after an accepted callback'
  try {
    const j = await health()
    if (j.arcIngestPushHealth === 'flowing') pass(label)
    else fail(label, `arcIngestPushHealth = ${JSON.stringify(j.arcIngestPushHealth)} (expected "flowing")`)
  } catch (e) {
    fail(label, String(e))
  }
}

console.log(results.join('\n'))
if (failures) {
  console.error(`\n/arc-ingest auth route harness: ${failures} expectation(s) FAILED.`)
  process.exit(1)
}
console.log(`\n/arc-ingest auth route harness: all ${results.length} expectations held.`)
