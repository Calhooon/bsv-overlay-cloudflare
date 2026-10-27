#!/usr/bin/env node
/**
 * bsv-low #920 (FIX-920, the FIX-920 lens's section 5): a refusal in flight completes the eviction at the
 * admission's landing, in the budget tier (`make ci-d1-budget`, after the #834 cell, on the retire overlay of the
 * #831 and #834 cells, its couriers sunk to this cell's stand-in on the same port).
 *
 * The stand-in answers Arcade's own `GET /arcade/tx/{S}` with a terminal REJECTED for the subject S (the valid
 * P2PKH spend of `tools/lane-script/fixtures`; its POST never reaches Arcade), WoC's presence 404, BananaBlocks'
 * recorded not-found body and every other courier 404, so nothing leaves the machine. S's rows are planted as a
 * landing leaves them (`outputs`, `pot_records`, `applied_transactions`, all young), and:
 *
 *  1. THE RE-PRESENT UNDER AN OPEN EVICTION (arm 3, the Rejected site of arm 1): beside an OPEN `pot_evictions` row
 *     and no memo, the broadcast-gated re-present answers 422 on Arcade's own word, young as it is; the rows are in
 *     the twins; `tx_any_verdicts` holds `refused` with the ledger's reason; `txany_memo_write_refused_total` +1;
 *     nothing was POSTed to Arcade.
 *  2. THE CONTROL (Rule 6): the same subject with no open row and a fresh `outputs.score` answers the retryable 502;
 *     the rows stay live, no memo is written, no eviction is opened.
 *  3. THE PUSH AFTER THE LANDING (today's path): an `/arc-ingest` REJECTED push on that admission counts
 *     `arc_ingest_evicted_total` +1 and ONE memo write; a following re-present answers 422 and its re-run writes no
 *     second memo (the memo-write counter unchanged).
 *
 * The past-deadline landing itself is not driven: the arm's 12 s deadline is a constant and local D1 does not hold a
 * write past it (the lens, section 5); its settle is pinned natively (`--lib fix920`).
 *
 *   node tools/lane-920/eviction_landing_route_ci.mjs <retire overlay base> <its --persist-to dir> <stand-in port>
 *
 * Exit 0 = every expectation held.
 */
import { execFileSync } from 'node:child_process'
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const OVERLAY = process.argv[2]
const STATE = process.argv[3]
const SINK_PORT = Number(process.argv[4])
const CRATE = fileURLToPath(new URL('../../crates/overlay-cloudflare/', import.meta.url))
const FIX = new URL('../lane-script/fixtures/', import.meta.url)
if (!OVERLAY || !STATE || !SINK_PORT) {
  console.error('usage: eviction_landing_route_ci.mjs <retire overlay base> <its --persist-to dir> <stand-in port>')
  process.exit(2)
}

let failures = 0
const lines = []
const expect = (ok, label, why) => {
  if (ok) return lines.push(`PASS  ${label}`)
  failures++
  lines.push(`FAIL  ${label}`, `      ${why}`)
}

const manifest = JSON.parse(readFileSync(new URL('manifest.json', FIX), 'utf8'))
const BEEF = Buffer.from(readFileSync(new URL(manifest.valid.file, FIX), 'utf8').trim(), 'hex')
const S = manifest.valid.subject_txid
const EXTRA = 'UTXO_SPENT (70): lane-920'
const LEDGER_REASON = `REJECTED (arcade live REJECTED: ${EXTRA}; both indexers absent)`
const C_MEMO = 'txany_memo_write_refused_total'
const C_PUSH = 'arc_ingest_evicted_total'

// The couriers' stand-in. Arcade's GET names S REJECTED (its stored terminal word); a POST is logged and refused.
const asked = []
const arcadePosts = []
const sink = createServer((req, res) => {
  asked.push(`${req.method} ${req.url}`)
  if (req.method === 'POST' && req.url.startsWith('/arcade/')) {
    arcadePosts.push(req.url)
    res.writeHead(503, { 'content-type': 'text/plain' })
    return res.end('lane-920: no broadcast expected')
  }
  const arcade = req.url.match(/^\/arcade\/tx\/([0-9a-f]{64})$/)
  if (arcade && arcade[1] === S) {
    res.writeHead(200, { 'content-type': 'application/json' })
    return res.end(JSON.stringify({ txid: S, txStatus: 'REJECTED', extraInfo: EXTRA, timestamp: new Date().toISOString() }))
  }
  // bsv-low #868: BananaBlocks' native `/api/v1/tx/{txid}` answers its recorded not-found body
  if (/^\/api\/v1\/tx\/[0-9a-f]{64}$/.test(req.url)) {
    res.writeHead(404, { 'content-type': 'application/json' })
    return res.end('{"error":"Transaction not found"}')
  }
  res.writeHead(404, { 'content-type': 'text/plain' })
  res.end('not found')
})
await new Promise((ok, no) => sink.listen(SINK_PORT, '127.0.0.1', ok).on('error', no))

const scratch = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), 'lane920-'))
let seq = 0
function d1(sql) {
  const path = join(scratch, `q-${Date.now()}-${seq++}.sql`)
  writeFileSync(path, sql)
  const args = ['wrangler', 'd1', 'execute', 'OVERLAY_DB', '--local', '--persist-to', STATE, '--json', '--file', path]
  for (let attempt = 1; ; attempt++) {
    try {
      const out = execFileSync('npx', args, { cwd: CRATE, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
      const all = JSON.parse(out)
      return all[all.length - 1]?.results ?? []
    } catch (e) {
      if (attempt >= 4) throw new Error(`wrangler d1 execute failed: ${e.stderr ?? e.message}`)
    }
  }
}
// wrangler's JSON renders a NULL as the string "null" (its CLI's shape)
const nul = (v) => v === null || v === undefined || v === 'null'

const twins = () =>
  d1("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('outputs_evicted','pot_records_evicted','applied_transactions_evicted')").map(
    (r) => r.name,
  )
function rows() {
  const live = d1(
    `SELECT (SELECT COUNT(*) FROM outputs WHERE txid = '${S}') AS o, (SELECT COUNT(*) FROM pot_records WHERE txid = '${S}') AS p, (SELECT COUNT(*) FROM applied_transactions WHERE txid = '${S}') AS a`,
  )[0]
  const out = { live: Number(live.o) + Number(live.p) + Number(live.a), twins: 0 }
  for (const t of twins()) out.twins += Number(d1(`SELECT COUNT(*) AS n FROM "${t}" WHERE txid = '${S}'`)[0].n)
  return out
}
const memo = () => d1(`SELECT kind, evidence FROM tx_any_verdicts WHERE txid = '${S}'`)[0] ?? null
const ledger = () => d1(`SELECT reason, evictedAt, readmittedAt FROM pot_evictions WHERE txid = '${S}'`)[0] ?? null

function clean() {
  const sql = [
    `DELETE FROM outputs WHERE txid = '${S}';`,
    `DELETE FROM pot_records WHERE txid = '${S}';`,
    `DELETE FROM applied_transactions WHERE txid = '${S}';`,
    `DELETE FROM pot_evictions WHERE txid = '${S}';`,
    `DELETE FROM tx_any_verdicts WHERE txid = '${S}';`,
    ...twins().map((t) => `DELETE FROM "${t}" WHERE txid = '${S}';`),
  ]
  d1(sql.join('\n'))
}
// The rows an admission's landing leaves, stamped now (young: Rule 6's floor is ten minutes).
function land() {
  const nowMs = Date.now()
  d1(
    [
      `INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent, score) VALUES ('${S}', 0, X'51', 'tm_pot', 9000, 0, ${nowMs});`,
      `INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES ('${S}', 0, 0, ${Math.floor(nowMs / 1000)}, 'covenant');`,
      `INSERT INTO applied_transactions (txid, topic) VALUES ('${S}', 'tm_pot');`,
    ].join('\n'),
  )
}

async function counters() {
  const r = await fetch(OVERLAY + '/health/invariants', { headers: { Connection: 'close' } })
  return (await r.json())?.counters ?? {}
}
async function poll(pred, ms = 20_000) {
  const t0 = Date.now()
  for (;;) {
    const v = await pred()
    if (v) return v
    if (Date.now() - t0 > ms) return null
    await new Promise((ok) => setTimeout(ok, 400))
  }
}
let topic = 'tm_pot'
async function present() {
  const r = await fetch(OVERLAY + '/submit', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'x-topics': JSON.stringify([topic]),
      'x-submit-mode': 'broadcast-gated',
      Connection: 'close',
    },
    body: BEEF,
  })
  return { status: r.status, text: await r.text() }
}

try {
  const listed = await (await fetch(OVERLAY + '/listTopicManagers', { headers: { Connection: 'close' } })).json()
  const names = Array.isArray(listed) ? listed : Object.keys(listed ?? {})
  if (!names.includes(topic) && names.length) topic = names[0]
  const c0 = await counters()
  expect(typeof c0[C_MEMO] === 'number' && typeof c0[C_PUSH] === 'number', 'the counters the legs read are served', JSON.stringify({ [C_MEMO]: c0[C_MEMO], [C_PUSH]: c0[C_PUSH] }))

  // 1. The re-present under an open eviction: the push's marker stands, its job wrote no memo.
  clean()
  land()
  d1(
    `INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES ('${S}', '${LEDGER_REASON}', ${Date.now() - 5_000}, NULL, 0, '[]');`,
  )
  const r1 = await present()
  lines.push(`INFO  leg 1: ${r1.status} ${r1.text.slice(0, 200)}`)
  expect(r1.status === 422 && /network rejected/.test(r1.text), 'leg 1: the young re-present under an open eviction answers 422 on Arcade\'s own word', `${r1.status} ${r1.text.slice(0, 300)}`)
  const rows1 = rows()
  expect(rows1.live === 0 && rows1.twins >= 2, 'leg 1: the landed rows are in the twins', JSON.stringify(rows1))
  const m1 = memo()
  const l1 = ledger()
  expect(m1?.kind === 'refused' && m1?.evidence === l1?.reason && nul(l1?.readmittedAt), "leg 1: the memo is `refused` with the ledger's reason", JSON.stringify({ memo: m1, ledger: l1 }))
  const c1 = await counters()
  expect(c1[C_MEMO] - c0[C_MEMO] === 1, 'leg 1: one memo write counted', `${c0[C_MEMO]} -> ${c1[C_MEMO]}`)
  expect(arcadePosts.length === 0, 'leg 1: nothing was broadcast', JSON.stringify(arcadePosts))

  // 2. The control: no open row, a fresh admission: Rule 6's floor stands.
  clean()
  land()
  const r2 = await present()
  lines.push(`INFO  leg 2: ${r2.status} ${r2.text.slice(0, 200)}`)
  expect(r2.status === 502, 'leg 2: the young first presentation with no open row answers the retryable 502', `${r2.status} ${r2.text.slice(0, 300)}`)
  const rows2 = rows()
  expect(rows2.live === 3 && memo() === null && ledger() === null, 'leg 2: the rows stay live, no memo, no eviction', JSON.stringify({ rows: rows2, memo: memo(), ledger: ledger() }))
  const c2 = await counters()
  expect(c2[C_MEMO] === c1[C_MEMO], 'leg 2: no memo write counted', `${c1[C_MEMO]} -> ${c2[C_MEMO]}`)

  // 3. The push after the landing (today's path), then a re-present.
  const push = await fetch(OVERLAY + '/arc-ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', authorization: `Bearer ${S}`, Connection: 'close' },
    body: JSON.stringify({ txid: S, txStatus: 'REJECTED', extraInfo: EXTRA }),
  })
  const pushText = await push.text()
  lines.push(`INFO  leg 3 push: ${push.status} ${pushText.slice(0, 200)}`)
  expect(push.status === 200, 'leg 3: the REJECTED push is acknowledged', `${push.status} ${pushText.slice(0, 300)}`)
  const c3 = await poll(async () => {
    const c = await counters()
    return c[C_PUSH] - c2[C_PUSH] >= 1 && c[C_MEMO] - c2[C_MEMO] >= 1 ? c : null
  })
  expect(c3 !== null && c3[C_PUSH] - c2[C_PUSH] === 1 && c3[C_MEMO] - c2[C_MEMO] === 1, 'leg 3: the push evicts (arc_ingest_evicted_total +1) and writes one memo', JSON.stringify({ before: c2, after: c3 && { [C_PUSH]: c3[C_PUSH], [C_MEMO]: c3[C_MEMO] } }))
  const rows3 = rows()
  expect(rows3.live === 0 && memo()?.kind === 'refused', 'leg 3: the rows are in the twins and the memo is `refused`', JSON.stringify({ rows: rows3, memo: memo() }))
  const r3 = await present()
  lines.push(`INFO  leg 3 re-present: ${r3.status} ${r3.text.slice(0, 200)}`)
  expect(r3.status === 422, 'leg 3: the re-present answers 422', `${r3.status} ${r3.text.slice(0, 300)}`)
  const c4 = await counters()
  expect(c3 && c4[C_MEMO] === c3[C_MEMO], "leg 3: the re-present's re-run writes no second memo", `${c3?.[C_MEMO]} -> ${c4[C_MEMO]}`)
  expect(arcadePosts.length === 0, 'legs 2 and 3: nothing was broadcast', JSON.stringify(arcadePosts))
  clean()
} catch (e) {
  expect(false, 'the cell ran', e.stack ?? String(e))
} finally {
  sink.close()
  rmSync(scratch, { recursive: true, force: true })
}

lines.push(`INFO  stand-in asked: ${JSON.stringify(asked.slice(0, 40))}`)
console.log(lines.join('\n'))
if (failures) {
  console.log(`✗ lane-920 eviction landing: ${failures} failure(s)`)
  process.exit(1)
}
console.log('✓ lane-920 eviction landing: every expectation held')
