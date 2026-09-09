#!/usr/bin/env node
/**
 * bsv-low W-A / #437 step 2 (2026-09-09) — route-level ASSERTING harness for
 * the gated door's SCRIPT WALK. Part of `make ci-route`.
 *
 * Every native tier stays green with the door deleted (the walk lives in the
 * wasm-only `/submit` route, like the #347 gate), so this drives the REAL
 * route against a FIXTURE Arcade (this process, `FIXTURE_PORT`) that LOGS every
 * broadcast POST — the one observation that says "the refusal happened BEFORE
 * any broadcast" and "the valid sibling was broadcast".
 *
 * Expects `wrangler dev --local` on the given base with:
 *   SUBMIT_ENFORCE=true, ENABLE_EXTENSIONS=true,
 *   SCRIPT_VERIFY_NETWORK_GATED=true, ARCADE_URL=http://127.0.0.1:<FIXTURE_PORT>,
 *   TOPIC_MANAGERS=tm_collected,tm_potparty
 *
 * Fixtures: `fixtures/{valid,corrupted}.beef.hex` + `manifest.json`, PRODUCED
 * by the engine's own test producer (`emit_lane_script_fixtures`, fixed keys)
 * — a signed P2PKH spend of a proven funding output, and the same spend with
 * one DER `r` byte flipped after signing. Never retyped here.
 *
 * Expectations:
 *  1. `/health/invariants` serves `submit_script_refused_total` (a number;
 *     an absent key is an unknown, never fine).
 *  2. CORRUPTED, broadcast-gated → 400, body `code == "script-refused"`, the
 *     message names the corrupted SUBJECT txid; the fixture Arcade saw NO
 *     POST (nothing broadcast); `submit_script_refused_total` moved by +1
 *     (polled — the bump is backgrounded) and the `Server-Timing` header
 *     carries a `script-verify` segment.
 *  3. JUNK body (not a BEEF), broadcast-gated → a 4xx BEFORE the door (the
 *     parse/EF refusal), no POST, and NEITHER door counter moved: the door
 *     counts only its own verdicts.
 *  4. VALID, broadcast-gated → the fixture Arcade RECEIVED a POST for it (the
 *     walk passed and the broadcast ran; the final status is the network
 *     leg's and is NOT asserted beyond "not a script refusal");
 *     `submit_script_refused_total` unchanged.
 *
 * Boundary, stated: the valid leg's accept claim is corroborated against the
 * hardcoded TAAL/GorillaPool hosts (lane-371's stated boundary; lane-347's
 * gated probes already reach the real couriers from CI), so its final status
 * is not hermetic; the ORDER (walk, then POST) is what this cell pins.
 *
 * `EXPECT_DOOR=off` (gate MED-4): the KILL-SWITCH leg against a worker WITHOUT
 * `SCRIPT_VERIFY_NETWORK_GATED` — the corrupted spend must NOT be
 * script-refused, MUST reach the fixture Arcade (its POST body carries the
 * corrupted subject's own bytes), and the door counters must not move. A
 * regression that leaves the door executing with the var absent fails here
 * and nowhere else.
 *
 * Every POST is identified (gate LOW-3): the fixture parses `{rawTx}` /
 * `[{rawTx},…]` bodies and the cell asserts a window of the EXPECTED subject's
 * own SIGNATURE (carried verbatim by the extended format; the one byte the
 * two fixtures differ by lies inside it) is in the body.
 *
 * Exit 0 = every expectation held.
 */
import { readFileSync } from 'node:fs'
import { createServer } from 'node:http'

const BASE = process.argv[2] ?? 'http://127.0.0.1:8797'
const FIXTURE_PORT = Number(process.env.FIXTURE_PORT ?? '8798')
const DOOR_ON = (process.env.EXPECT_DOOR ?? 'on') !== 'off'
const FIX = new URL('./fixtures/', import.meta.url)

let failures = 0
const results = []
const pass = (label) => results.push(`PASS  ${label}`)
const fail = (label, why) => {
  failures++
  results.push(`FAIL  ${label}`)
  results.push(`      ${why}`)
}

const manifest = JSON.parse(readFileSync(new URL('manifest.json', FIX), 'utf8'))
const beefOf = (entry) => Buffer.from(readFileSync(new URL(entry.file, FIX), 'utf8').trim(), 'hex')
const VALID = beefOf(manifest.valid)
const CORRUPTED = beefOf(manifest.corrupted)

// ── the fixture Arcade: logs every broadcast POST; a txid is SEEN only AFTER
// a POST (the gated arm PRE-FLIGHTS `GET /tx/{txid}` and skips the whole
// ladder when Arcade already reports ≥SEEN — a fixture that says SEEN to any
// GET never receives the POST this cell exists to observe; found on the first
// run, 2026-09-09). ──────────────────────────────────────────────────────────
const postLog = [] // one entry per POST /tx | /txs: { path, bytes, rawTxs: [hex…] }
const fixture = createServer((req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${FIXTURE_PORT}`)
  if (req.method === 'POST' && (url.pathname === '/tx' || url.pathname === '/txs')) {
    const chunks = []
    req.on('data', (c) => chunks.push(c))
    req.on('end', () => {
      const body = Buffer.concat(chunks)
      let rawTxs = []
      try {
        const j = JSON.parse(body.toString('utf8'))
        rawTxs = (Array.isArray(j) ? j : [j]).map((e) => String(e?.rawTx ?? '')).filter(Boolean)
      } catch {
        rawTxs = [body.toString('hex')] // an octet-stream body: keep its hex
      }
      postLog.push({ path: url.pathname, bytes: body.length, rawTxs, at: Date.now() })
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ txStatus: 'SEEN_ON_NETWORK' }))
    })
    return
  }
  if (req.method === 'GET' && url.pathname.startsWith('/tx/')) {
    const txid = url.pathname.slice('/tx/'.length).toLowerCase()
    if (postLog.length === 0) {
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: 'not found' }))
      return
    }
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ txid, txStatus: 'SEEN_ON_NETWORK' }))
    return
  }
  res.writeHead(404)
  res.end()
})

async function counters() {
  const res = await fetch(`${BASE}/health/invariants`)
  if (!res.ok) throw new Error(`GET /health/invariants -> ${res.status}`)
  const j = await res.json()
  const c = j.counters ?? {}
  return {
    refused: c.submit_script_refused_total,
    inconclusive: c.submit_script_walk_inconclusive_total,
  }
}

async function postSubmit(body) {
  const res = await fetch(`${BASE}/submit`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'x-topics': JSON.stringify(['tm_collected']),
      'x-submit-mode': 'broadcast-gated',
    },
    body,
  })
  const text = await res.text()
  let json = null
  try {
    json = JSON.parse(text)
  } catch {
    /* not JSON */
  }
  return { status: res.status, text, json, serverTiming: res.headers.get('server-timing') ?? '' }
}

async function pollFor(predicate, timeoutMs = 15_000, stepMs = 500) {
  const t0 = Date.now()
  for (;;) {
    if (await predicate()) return true
    if (Date.now() - t0 > timeoutMs) return false
    await new Promise((r) => setTimeout(r, stepMs))
  }
}

await new Promise((resolve, reject) => {
  fixture.once('error', reject)
  fixture.listen(FIXTURE_PORT, '127.0.0.1', resolve)
})

/** Was `subjectRawHex`'s tail carried by any POST logged since `from`? The
 * gated arm POSTs EXTENDED FORMAT (octet-stream): each input gains its
 * previous output's value + script AFTER its sequence, so only the bytes
 * after the last input — the outputs and the locktime — are contiguous with
 * the raw. A P2PKH output + locktime is 38 bytes; 35 bytes (70 hex) of tail
 * lie wholly inside that region for both fixtures. */
const postedSubject = (subjectRawHex, from) => {
  // The gated arm POSTs EXTENDED FORMAT: the input's unlocking script is
  // carried verbatim, and the two fixtures pay the same output to the same
  // key while differing by ONE DER byte of the signature — so the SIGNATURE
  // is the identity (gate LOW-3). A P2PKH spend's unlocking script starts at
  // raw byte 42 (version 4 · vin 1 · outpoint 36 · length 1); 40 bytes from
  // byte 43 sit inside the signature on both sides of the flipped byte.
  const sigWindow = subjectRawHex.slice(86, 86 + 80).toLowerCase()
  return postLog.slice(from).some((p) => p.rawTxs.some((h) => h.toLowerCase().includes(sigWindow)))
}

try {
  if (DOOR_ON) {
    // 1. the counters are served
    const c0 = await counters()
    if (typeof c0.refused !== 'number' || typeof c0.inconclusive !== 'number') {
      fail('door counters served', `submit_script_refused_total=${c0.refused} submit_script_walk_inconclusive_total=${c0.inconclusive}`)
    } else {
      pass(`door counters served (refused ${c0.refused}, inconclusive ${c0.inconclusive})`)
    }

    // 2. the corrupted spend is refused at the door, nothing broadcast
    const postsBefore = postLog.length
    const r = await postSubmit(CORRUPTED)
    const named = typeof r.json?.message === 'string' && r.json.message.includes(manifest.corrupted.subject_txid)
    if (r.status === 400 && r.json?.code === 'ERR_SCRIPT_REFUSED' && named) {
      pass(`corrupted spend refused 400 ERR_SCRIPT_REFUSED, naming ${manifest.corrupted.subject_txid.slice(0, 12)}…`)
    } else {
      fail('corrupted spend refused at the door', `status=${r.status} body=${r.text.slice(0, 240)}`)
    }
    if (/script-verify;dur=/.test(r.serverTiming)) pass(`Server-Timing carries the walk (${r.serverTiming})`)
    else fail('Server-Timing carries script-verify', `got ${JSON.stringify(r.serverTiming)}`)
    await new Promise((s) => setTimeout(s, 1_500))
    if (postLog.length === postsBefore) pass('the refused spend was NEVER broadcast (0 fixture POSTs)')
    else fail('the refused spend was never broadcast', `fixture POSTs grew: ${JSON.stringify(postLog.slice(postsBefore).map((p) => p.bytes))}`)
    const bumped = await pollFor(async () => (await counters()).refused === c0.refused + 1)
    if (bumped) pass('submit_script_refused_total +1')
    else fail('submit_script_refused_total +1', `still ${(await counters()).refused} (was ${c0.refused})`)
    const c1 = await counters()

    // 3. a junk body refuses BEFORE the door: no POST, no door counter moves
    const postsBeforeJunk = postLog.length
    const junk = await postSubmit(Buffer.from('deadbeef', 'hex'))
    if (junk.status >= 400 && junk.status < 500 && junk.json?.code !== 'ERR_SCRIPT_REFUSED') {
      pass(`junk body refused before the door (${junk.status})`)
    } else {
      fail('junk body refused before the door', `status=${junk.status} body=${junk.text.slice(0, 160)}`)
    }
    await new Promise((s) => setTimeout(s, 1_500))
    const c2 = await counters()
    if (postLog.length === postsBeforeJunk && c2.refused === c1.refused && c2.inconclusive === c1.inconclusive) {
      pass('junk body: no POST, no door counter moved (the door counts only its own verdicts)')
    } else {
      fail('junk body leaves the door untouched', `posts ${postLog.length - postsBeforeJunk} refused ${c1.refused}->${c2.refused} inconclusive ${c1.inconclusive}->${c2.inconclusive}`)
    }

    // 4. the valid spend passes the door and IS broadcast — THAT spend
    const postsBeforeValid = postLog.length
    const v = await postSubmit(VALID)
    const posted = await pollFor(async () => postedSubject(manifest.valid.subject_raw_hex, postsBeforeValid), 20_000)
    if (posted) pass(`valid spend passed the door and was broadcast (its own bytes in a fixture POST; status ${v.status})`)
    else fail('valid spend passed the door and was broadcast', `no fixture POST carrying the valid subject; posts=${JSON.stringify(postLog.slice(postsBeforeValid).map((p) => p.bytes))} status=${v.status} body=${v.text.slice(0, 200)}`)
    if (v.status === 400 && v.json?.code === 'ERR_SCRIPT_REFUSED') fail('valid spend is not script-refused', v.text.slice(0, 200))
    else pass('valid spend is not script-refused')
    if (/script-walk;desc=/.test(v.serverTiming)) pass(`the walk's stats ride Server-Timing (${v.serverTiming.match(/script-walk;desc="[^"]*"/)?.[0]})`)
    else fail('the walk stats ride Server-Timing', `got ${JSON.stringify(v.serverTiming)}`)
    await new Promise((s) => setTimeout(s, 1_500))
    const c3 = await counters()
    if (c3.refused === c2.refused) pass('submit_script_refused_total unchanged by the valid spend')
    else fail('submit_script_refused_total unchanged by the valid spend', `${c2.refused} -> ${c3.refused}`)
  } else {
    // ── THE KILL SWITCH (EXPECT_DOOR=off): the door executes NOTHING. ──
    const c0 = await counters()
    const postsBefore = postLog.length
    const r = await postSubmit(CORRUPTED)
    if (r.status === 400 && r.json?.code === 'ERR_SCRIPT_REFUSED') {
      fail('kill switch: the corrupted spend is NOT script-refused', `the door refused with the var absent: ${r.text.slice(0, 200)}`)
    } else {
      pass(`kill switch: the corrupted spend was not script-refused (status ${r.status})`)
    }
    const posted = await pollFor(async () => postedSubject(manifest.corrupted.subject_raw_hex, postsBefore), 20_000)
    if (posted) pass('kill switch: the corrupted spend REACHED the fixture Arcade (its own bytes in a POST)')
    else fail('kill switch: the corrupted spend reached the fixture Arcade', `no fixture POST carrying it; posts=${JSON.stringify(postLog.slice(postsBefore).map((p) => p.bytes))}`)
    // `skipped` is reserved for the kill switch: a door that RAN and ended
    // inconclusive writes its own desc, so this cannot pass on a stuck-on door.
    if (/script-walk;desc="skipped"/.test(r.serverTiming)) pass('kill switch: the walk desc is exactly "skipped" (the door did not run)')
    else fail('kill switch: the walk desc is exactly "skipped"', `got ${JSON.stringify(r.serverTiming)}`)
    await new Promise((s) => setTimeout(s, 1_500))
    const c1 = await counters()
    if ((c1.refused ?? 0) === (c0.refused ?? 0) && (c1.inconclusive ?? 0) === (c0.inconclusive ?? 0)) pass('kill switch: no door counter moved')
    else fail('kill switch: no door counter moved', `refused ${c0.refused}->${c1.refused} inconclusive ${c0.inconclusive}->${c1.inconclusive}`)
  }
} finally {
  fixture.close()
}

console.log('')
for (const line of results) console.log(line)
console.log('')
if (failures > 0) {
  console.log(`✗ /submit script-door route harness: ${failures} expectation(s) FAILED.`)
  process.exit(1)
}
console.log(`/submit script-door route harness: all ${results.length} expectations held.`)
