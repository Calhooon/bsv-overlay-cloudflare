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
 *  6-9. The lens fold (lane E576-f): the health's exhaustedCount / ceiling / stale fields; a stale re-drive returned
 *     and re-driven (M1); an exhausted letter forced by txid (L5); a bad-base64 replay dead-lettered and parked (N4).
 * 10. The delta fold (lane E576-f2, D-M1): the table filled to the ceiling (`ceiling.full`, `near`); a REAL dead letter
 *     is deferred (not parked; both deferral counters move); the discard lever's bearer and body; a discard of one
 *     parked letter by key (its hash returned, a missing key `notFound`, `dead_letters_discarded_total`); the
 *     deferred letter then PARKED on its next DLQ delivery.
 * 11. The delta-2 fold (lane E576-f3, D2-M1): the real e1d letters are class `not_now`; the not-now share filled
 *     (1000 seeded, a stranger's flood); `classes` in the health; a REAL "not now" dead letter is deferred at the
 *     share (its own counter, not the ceiling's); the same key as a FAULT letter is then parked, the share still full.
 * On the base (`835b80c`) there is no route (the dispatch's 404) and no table: RED. On `f8b5525` legs 6-9: RED.
 * On `28f5d0b` leg 10: RED (no discard route, no `near`, the ceiling never drains). On `66d069f` leg 11: RED (no
 * `class` column: the seeding fails; no `classes`, the not-now letter parks).
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
async function lever(body, token = INTERNAL, route = '/internal/redrive-dead-letters') {
  const headers = { 'Content-Type': 'application/json', Connection: 'close' }
  if (token !== null) headers.Authorization = `Bearer ${token}`
  const res = await fetch(OVERLAY + route, { method: 'POST', headers, body: JSON.stringify(body) })
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
    // The fixture's bytes are refused by the replay's BEEF reader: before P0-5f (bsv-rs 0.3.35, the bounded
    // stranger readers, overlay a8a8d73) they parsed to a BEEF whose subject could not be derived; since, the
    // bounded reader refuses them first ("reader underflow"). Since NL-6 (bsv-rs 0.4.0, the streaming door) the
    // refusal names the invalid bytes: the fixture is three zero bytes, so the stream ends inside the four-byte
    // version word ("invalid BEEF at byte 0: Truncated"). Each is the re-driven replay's own fault, and the
    // history must carry the fault the row carries.
    Number(reparked.redrives) === 1 && hist.length === 1 && hist[0].redrive === 1 && typeof hist[0].fault === 'string' && hist[0].fault.length > 0 && hist[0].fault === reparked.fault && /invalid BEEF at byte 0: Truncated/.test(hist[0].fault),
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

// ── bsv-low #576's lens fold (lane E576-f) ──────────────────────────────────
// 6. The health block's new fields (M1, M2, L4).
expect(
  typeof dl.exhaustedCount === 'number' && dl.exhaustedCount >= 1 && dl.ceiling?.max === 2000 && typeof dl.ceiling?.held === 'number' &&
    typeof dl.staleRedriven === 'number' && dl.staleAfterMs === 3_600_000 && typeof dl.parkedLast24h === 'number' && !('resolved' in dl),
  '/health/invariants.deadLetters: exhaustedCount, the ceiling, the stale re-drives, the last 24 h',
  JSON.stringify(dl),
)
// 7. M1: a re-drive claimed two hours ago that never came back is returned to the parked set and re-driven.
d1(`INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, redriven_at, history) VALUES ('e576s', 'tm_collected', '${msg}', 'its send never left', 0, 'redriven', 1, 500, 500, ${Date.now() - 2 * 3_600_000}, '[]')`)
const hs = await health()
expect((hs.deadLetters?.staleRedriven ?? 0) >= 1 && hs.deadLetters?.oldestRedriven?.txid === 'e576s', 'the health shows the stale re-drive meanwhile', JSON.stringify(hs.deadLetters))
const stale0 = hs.counters?.dead_letters_stale_returned_total ?? NaN
const st = await lever({ txid: 'e576s' })
expect(
  st.status === 200 && (st.json?.staleReturned ?? []).some((r) => r.txid === 'e576s') && (st.json?.redriven ?? []).some((r) => r.txid === 'e576s' && r.redrive === 2),
  'the lever returns the stale re-drive to the parked set and re-drives it (its spent re-drive stays spent)',
  st.text.slice(0, 400),
)
expect(((await health()).counters?.dead_letters_stale_returned_total ?? NaN) >= stale0 + 1, 'dead_letters_stale_returned_total moved', `${stale0}`)
// 8. L5: the exhausted letter is re-driven once more only with force, by txid.
const nf = await lever({ txid: 'e576x' })
const fx = await lever({ txid: 'e576x', force: true })
const bad = await lever({ force: true })
expect(
  nf.json?.read === 0 && (fx.json?.redriven ?? []).some((r) => r.txid === 'e576x' && r.redrive === 4 && r.forced === true) && bad.status === 400,
  'an exhausted letter moves only with {"txid", "force": true} (a force with no txid is 400)',
  `${nf.text.slice(0, 120)} | ${fx.text.slice(0, 200)} | ${bad.status}`,
)
// 9. N4: a body whose BEEF is not base64 is no longer acked silently: it dead-letters and parks again.
const badMsg = JSON.stringify({ beef_b64: '!!!!', topics: ['tm_collected'], mode: 'historical-tx', reason: 'phase3-fault' }).replaceAll("'", "''")
d1(`INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, history) VALUES ('e576n', 'tm_collected', '${badMsg}', 'seeded', 0, 'parked', 0, 400, 400, '[]')`)
const nb = await lever({ txid: 'e576n' })
expect((nb.json?.redriven ?? []).length === 1, 'the bad-base64 letter is re-driven', nb.text.slice(0, 200))
const nbParked = await until('e576n parked again', () => {
  const r = letter('e576n')
  return r && r.status === 'parked' && /invalid base64/.test(r.fault ?? '') ? r : null
})
expect(!!nbParked, `a bad-base64 replay is handed back, dead-letters and parks again with its fault (within ${WAIT_MS / 1000} s)`, JSON.stringify({ ...(letter('e576n') ?? {}), message: '…' }))

// ── bsv-low #576's delta fold (lane E576-f2) ────────────────────────────────
// 10. D-M1: the ceiling drains by the operator's discard.
const discard = (body, token = INTERNAL) => lever(body, token, '/internal/discard-dead-letters')
const heldNow = Number(d1(`SELECT COUNT(*) AS c FROM mutation_dead_letters WHERE status IN ('parked', 'redriven')`)[0]?.c ?? 0)
const fill = 2000 - heldNow
if (fill > 0) {
  d1(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${fill}) INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, history) SELECT 'e576fill' || i, 'tm_collected', '{}', 'filler (never lands)', 0, 'parked', 3, 2, 2, '[]' FROM n`)
}
const hf = await health()
expect(
  hf.deadLetters?.ceiling?.full === true && hf.deadLetters?.ceiling?.near === true && hf.deadLetters?.ceiling?.nearAt === 1600 && hf.deadLetters?.ceiling?.room === 0,
  'the table at the ceiling: health says full, near (from 1600), no room',
  JSON.stringify(hf.deadLetters?.ceiling),
)
const deferred0 = hf.counters?.dead_letters_ceiling_deferred_total ?? NaN
const deferrals0 = hf.counters?.dead_letters_ceiling_deferrals_total ?? NaN
const discarded0 = hf.counters?.dead_letters_discarded_total ?? NaN
const G2 = tx([], [output(COLLECTED_MARKER), output(nonceScript())])
const g2 = txidOf(G2)
const P2 = tx([input(g2, 0)], [output(COLLECTED_MARKER), output(nonceScript())])
const p2 = txidOf(P2)
const S2 = tx([input(p2, 0)], [output(nonceScript())])
const s2 = txidOf(S2)
const rg2 = await submit(beefV1(G2))
d1(`INSERT INTO pot_evictions (txid, reason, evictedAt) VALUES ('${p2}', 'REJECTED (lane E576-f2, route tier)', ${Date.now()})`)
const rs2 = await submit(beefV1(G2, P2, S2))
expect(rg2.status === 200 && rs2.status === 200 && rs2.mutation === 'queued', `S2 (${s2.slice(0, 12)}…) is "not now": queued`, `${rg2.status} / ${rs2.status} ${rs2.mutation}`)
let hd = null
for (const t0 = Date.now(); Date.now() - t0 < WAIT_MS; await sleep(2_000)) {
  const h = await health()
  if ((h.counters?.dead_letters_ceiling_deferrals_total ?? 0) > deferrals0) { hd = h; break }
}
expect(
  !!hd && (letter(s2)?.status ?? 'none') !== 'parked',
  `S2 dead-lettered at the full ceiling is DEFERRED, not parked (within ${WAIT_MS / 1000} s)`,
  JSON.stringify({ row: { ...(letter(s2) ?? {}), message: '…' }, counters: hd?.counters ? Object.fromEntries(Object.entries(hd.counters).filter(([k]) => k.startsWith('dead_letters'))) : null }),
)
expect(
  !!hd && hd.counters.dead_letters_ceiling_deferred_total === deferred0 + 1,
  'its first DLQ delivery counts ONE deferred letter (attempts == 1) and one deferral (D-L1)',
  `${deferred0} -> ${hd?.counters?.dead_letters_ceiling_deferred_total}; deferrals ${deferrals0} -> ${hd?.counters?.dead_letters_ceiling_deferrals_total}`,
)
const dNone = await discard({ letters: [{ txid: 'e576fill1' }] }, null)
const dWrong = await discard({ letters: [{ txid: 'e576fill1' }] }, 'not-the-token')
const dEmpty = await discard({ letters: [] })
const dOver = await discard({ letters: Array.from({ length: 51 }, (_, i) => ({ txid: `e576fill${i + 1}` })) })
expect(
  dNone.status === 401 && dWrong.status === 401 && dEmpty.status === 400 && dOver.status === 400 && letter('e576fill1')?.status === 'parked',
  'the discard lever: 401 without the bearer, 400 on no letter and on 51; nothing moved',
  `${dNone.status} ${dWrong.status} ${dEmpty.status} ${dOver.status}`,
)
const fillKey = fill > 0 ? 'e576fill1' : 'e576x'
const dk = await discard({ letters: [{ txid: fillKey.toUpperCase(), topics: 'tm_collected' }, { txid: 'e576nope' }] })
const gone = dk.json?.discarded ?? []
expect(
  dk.status === 200 && gone.length === 1 && gone[0].txid === fillKey && /^[0-9a-f]{64}$/.test(gone[0].sha256 ?? '') &&
    (dk.json?.notFound ?? []).some((n) => n.txid === 'e576nope') && letter(fillKey) === undefined,
  'a discard by key deletes that parked letter (its bytes hashed), a missing key is notFound',
  dk.text.slice(0, 400),
)
const hx = await health()
expect(
  hx.counters?.dead_letters_discarded_total === discarded0 + 1 && hx.deadLetters?.ceiling?.full === false && hx.deadLetters?.ceiling?.room === 1,
  'dead_letters_discarded_total moved; the ceiling has room for one',
  JSON.stringify({ discarded: hx.counters?.dead_letters_discarded_total, ceiling: hx.deadLetters?.ceiling }),
)
const s2Parked = await until('S2 parked after the discard', () => {
  const r = letter(s2)
  return r && r.status === 'parked' ? r : null
})
expect(!!s2Parked, `the deferred letter is PARKED on its next DLQ delivery, now that there is room (within ${WAIT_MS / 1000} s)`, JSON.stringify({ ...(letter(s2) ?? {}), message: '…' }))
d1(`DELETE FROM mutation_dead_letters WHERE txid LIKE 'e576fill%'`)

// ── bsv-low #576's delta-2 fold (lane E576-f3) ──────────────────────────────
// 11. D2-M1: a stranger's "not now" letters hold at most half the ceiling; a fault letter still parks.
const classOf = (txid) => letter(txid)?.class
expect(
  classOf(s) === 'not_now' && classOf(s2) === 'not_now',
  'the real e1d letters (S, S2) are noted and parked as class not_now by the main consumer',
  `${classOf(s)} / ${classOf(s2)}`,
)
const notNowNow = Number(d1(`SELECT COUNT(*) AS c FROM mutation_dead_letters WHERE class = 'not_now' AND status IN ('parked', 'redriven')`)[0]?.c ?? 0)
const flood = 1000 - notNowNow
if (flood > 0) {
  d1(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${flood}) INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, history, class) SELECT 'e576flood' || i, 'tm_collected', '{}', 'a stranger''s not now', 4, 'parked', 0, 2, 2, '[]', 'not_now' FROM n`)
}
const hc = await health()
const cls = hc.deadLetters?.classes
expect(
  cls?.notNow?.held === 1000 && cls?.notNow?.max === 1000 && cls?.notNow?.full === true && cls?.fault?.kept === 1000 && cls?.fault?.room > 0 &&
    hc.deadLetters?.ceiling?.full === false && cls?.notNow?.perTxid === 1 && cls?.notNow?.perDay === 200,
  'the health names the classes apart: the not-now share full, room left for fault letters',
  JSON.stringify(cls),
)
const nnDeferrals0 = hc.counters?.dead_letters_not_now_deferrals_total ?? NaN
const ceilDeferrals0 = hc.counters?.dead_letters_ceiling_deferrals_total ?? NaN
const G3 = tx([], [output(COLLECTED_MARKER), output(nonceScript())])
const g3 = txidOf(G3)
const P3 = tx([input(g3, 0)], [output(COLLECTED_MARKER), output(nonceScript())])
const p3 = txidOf(P3)
const S3 = tx([input(p3, 0)], [output(nonceScript())])
const s3 = txidOf(S3)
const rg3 = await submit(beefV1(G3))
d1(`INSERT INTO pot_evictions (txid, reason, evictedAt) VALUES ('${p3}', 'REJECTED (lane E576-f3, route tier)', ${Date.now()})`)
const rs3 = await submit(beefV1(G3, P3, S3))
expect(rg3.status === 200 && rs3.status === 200 && rs3.mutation === 'queued', `S3 (${s3.slice(0, 12)}…) is "not now": queued`, `${rg3.status} / ${rs3.status} ${rs3.mutation}`)
let hn = null
for (const t0 = Date.now(); Date.now() - t0 < WAIT_MS; await sleep(2_000)) {
  const h = await health()
  if ((h.counters?.dead_letters_not_now_deferrals_total ?? 0) > nnDeferrals0) { hn = h; break }
}
expect(
  !!hn && (letter(s3)?.status ?? 'none') === 'failing' && classOf(s3) === 'not_now' && hn.counters.dead_letters_ceiling_deferrals_total === ceilDeferrals0,
  `S3 dead-lettered over the full not-now share is DEFERRED (counted apart from the ceiling's), not parked (within ${WAIT_MS / 1000} s)`,
  JSON.stringify({ row: { ...(letter(s3) ?? {}), message: '…' }, nn: hn?.counters?.dead_letters_not_now_deferrals_total, ceil: hn?.counters?.dead_letters_ceiling_deferrals_total }),
)
// The same key as a FAULT letter (a storage refusal of a real admission: here its note's class set by hand, the
// only way the route tier can make a D1 fault) is PARKED on its next DLQ delivery, the not-now share still full.
d1(`UPDATE mutation_dead_letters SET class = 'fault' WHERE txid = '${s3}' AND status = 'failing'`)
const s3Parked = await until('S3 parked as a fault letter', () => {
  const r = letter(s3)
  return r && r.status === 'parked' ? r : null
})
const hp = await health()
expect(
  !!s3Parked && hp.deadLetters?.classes?.notNow?.held === 1000 && hp.deadLetters?.classes?.fault?.held >= 1,
  `a fault letter is PARKED while a flood holds the whole not-now share (within ${WAIT_MS / 1000} s)`,
  JSON.stringify({ row: { ...(letter(s3) ?? {}), message: '…' }, classes: hp.deadLetters?.classes }),
)
d1(`DELETE FROM mutation_dead_letters WHERE txid LIKE 'e576flood%'`)

} catch (e) {
  expect(false, 'the seeded legs ran (the table and the lever exist)', `${e.message ?? e}`.split('\n')[0])
}

console.log('── lane E576: the dead letters parked and re-driven at the route ──')
for (const l of lines) console.log(l)
console.log(failures === 0 ? 'dead letter route tier: OK' : `dead letter route tier: ${failures} FAILED`)
process.exit(failures === 0 ? 0 : 1)
