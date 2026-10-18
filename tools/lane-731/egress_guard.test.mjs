#!/usr/bin/env node
// bsv-low #731: the egress guard's own pins, on fixture logs shaped as wrangler 4.54's `--log-level debug` prints
// them. Run first in `make ci-d1-budget` (milliseconds), so a guard that stopped seeing is a red before the tier.
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { isEgress } from './egress_guard.mjs'

const guard = fileURLToPath(new URL('./egress_guard.mjs', import.meta.url))
const event = (url) => `[InspectorProxyWorker] RUNTIME INCOMING MESSAGE {
  method: 'Network.requestWillBeSent',
  params: {
    requestId: '0',
    loaderId: '',
    request: {
      url: '${url}',
      method: 'GET',
      headers: {},
    },
    type: 'Fetch'
  }
}
`
const dir = mkdtempSync(join(process.env.TMPDIR || tmpdir(), 'egress-guard-'))
const run = (text) => {
  const log = join(dir, 'w.log')
  writeFileSync(log, text)
  try {
    return { rc: 0, out: execFileSync('node', [guard, `w=${log}`], { encoding: 'utf8', stdio: 'pipe' }) }
  } catch (e) {
    return { rc: e.status, out: String(e.stderr) }
  }
}
try {
  for (const h of ['127.0.0.1', 'localhost', 'd1', 'chaintracks', 'board-view', 'tick.invalid', 'x.test', 'a.localhost'])
    assert.equal(isEgress(h), false, `${h} is not the network`)
  for (const h of ['api.whatsonchain.com', 'bananablocks.com', 'overlay-us-1.bsvb.tech', '10.0.0.1', '[2001:db8::1]'])
    assert.equal(isEgress(h), true, `${h} is the network`)

  const loop = event('http://127.0.0.1:46782/internal/pot-changed')
  let r = run(loop + event('http://d1/query?resultsFormat=ROWS_AND_COLUMNS') + event('https://chaintracks/getPresentHeight'))
  assert.equal(r.rc, 0, `loopback and binding calls pass: ${r.out}`)

  r = run(loop + event('https://api.whatsonchain.com/v1/bsv/main/tx/ab/0/spent'))
  assert.equal(r.rc, 1, 'a courier ask reds')
  assert.match(r.out, /EGRESS w: https:\/\/api\.whatsonchain\.com\/v1\/bsv\/main\/tx\/ab\/0\/spent/)

  r = run(event('http://d1/query'))
  assert.equal(r.rc, 1, 'no loopback subrequest at all is a blind guard')
  assert.match(r.out, /BLIND/)

  r = run('')
  assert.equal(r.rc, 1, 'an empty log is a blind guard')
  console.log('egress guard self-test: ok')
} finally {
  rmSync(dir, { recursive: true, force: true })
}
