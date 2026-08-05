#!/usr/bin/env node
/**
 * #347 route-level ASSERTING harness — the executable half of `make ci-route`.
 *
 * `submit_gate_attack.mjs` PRINTS what happened; this ASSERTS it, so the
 * attack is coverage rather than recorded prose (Rule 17: a demonstrated
 * attack that lives only in a comment protects nothing, and the next refactor
 * has no way to learn it existed).
 *
 * Covers the one thing no native cell can reach: the ROUTE's use of the
 * decision seam. `make ci` cannot drive it — `submit()` takes a
 * `worker::Request` and only runs on wasm — which is exactly how a re-gate
 * disabled the sole public bar with all 1826 tests green (Rule 22).
 *
 * Expects `wrangler dev --local` on the given base with:
 *   SUBMIT_ENFORCE=true, ENABLE_EXTENSIONS=true,
 *   SUBMIT_OPERATOR_TOKEN=ci-submit-tok,
 *   TOPIC_MANAGERS=tm_collected,tm_potparty
 *
 * Exit 0 = every expectation held. Non-zero = the gate regressed.
 */
import { randomBytes } from 'node:crypto'
import { execFileSync } from 'node:child_process'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8791'
const SCRIPT = new URL('./submit_gate_attack.mjs', import.meta.url).pathname

let failures = 0
const results = []

/** Drive the real attack script and return {status, admitted, present}. */
function probe({ mode, shape, topic, token, label }) {
  const identity = '02' + randomBytes(32).toString('hex').slice(0, 64)
  const env = { ...process.env, SHAPE: shape ?? 'junk-input' }
  if (topic) env.TOPIC = topic
  if (token) env.OP_TOKEN = token
  const out = execFileSync('node', [SCRIPT, BASE, mode, identity], {
    env,
    encoding: 'utf8',
  })
  const status = Number(/status=(\d+)/.exec(out)?.[1] ?? -1)
  return { label, status, admitted: /outputsToAdmit/.test(out), present: /"present":true/.test(out) }
}

function expect(spec, predicate, why) {
  const r = probe(spec)
  const ok = predicate(r)
  results.push(`${ok ? 'PASS' : 'FAIL'}  ${r.label}  → status=${r.status} admitted=${r.admitted}`)
  if (!ok) {
    failures++
    results.push(`      expected: ${why}`)
  }
  return r
}

const refused = (r) => r.status === 401 && !r.admitted && !r.present

// ── The CRITICAL: SPV is not a bar. Every unbarred path, every spelling. ──
for (const [mode, shape] of [
  ['none', 'zero-input'],
  ['none', 'junk-input'],
  ['historical-tx', 'zero-input'],
  ['historical-tx-no-spv', 'zero-input'],
  ['HISTORICAL-TX', 'zero-input'],
  ['historical-tx-no-spv', 'junk-input'],
]) {
  expect(
    { mode, shape, label: `unauth ${mode} (${shape})` },
    refused,
    '401 — an unbarred path must never admit an unauthenticated fabrication',
  )
}

// The money topic specifically (the enumeration-starvation escalation).
expect(
  { mode: 'none', shape: 'zero-input', topic: 'tm_potparty', label: 'unauth tm_potparty (zero-input)' },
  refused,
  '401 — victim-named potparty markers must not be admittable for free',
)

// ── The one public path is REFUSED BY THE NETWORK, never by us. ──
expect(
  { mode: 'broadcast-gated', shape: 'zero-input', label: 'broadcast-gated (zero-input)' },
  (r) => r.status === 422 && !r.admitted,
  '422 — the real network rejects it (bad-txns-vin-empty); never a 401, and never admitted',
)

// ── A legitimate operator still works (peer sync / migration must not break). ──
expect(
  {
    mode: 'historical-tx-no-spv',
    shape: 'junk-input',
    token: 'ci-submit-tok',
    label: 'operator historical-tx-no-spv',
  },
  (r) => r.status === 200 && r.admitted,
  '200 — an authenticated operator retains the ungated path',
)

// ── A wrong credential fails CLOSED. ──
expect(
  {
    mode: 'historical-tx-no-spv',
    shape: 'junk-input',
    token: 'not-the-token',
    label: 'wrong operator token',
  },
  refused,
  '401 — a bad credential must never be treated as absent-but-allowed',
)

console.log(results.join('\n'))
if (failures) {
  console.error(`\n#347 route gate: ${failures} expectation(s) FAILED — the admission gate regressed.`)
  process.exit(1)
}
console.log(`\n#347 route gate: all ${results.length} expectations held.`)
