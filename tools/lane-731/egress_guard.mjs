#!/usr/bin/env node
/**
 * bsv-low #731: the D1 budget tier's EGRESS GUARD. Every cell of `make ci-d1-budget` reads only what it seeded;
 * a subrequest that leaves the loopback is a red BY NAME (the worker, the URL), never a pass that rested on what
 * mainnet answered that minute (#85's L23R2-M3: a seed memo aged past its window and the owed walk asked mainnet
 * WhatsOnChain; a 429 passed the cell, a 200 failed it).
 *
 *   node tools/lane-731/egress_guard.mjs <label>=<wrangler dev log> [...]
 *
 * The tier runs each `wrangler dev` at `--log-level debug`, where wrangler's inspector proxy prints the runtime's
 * `Network.requestWillBeSent` for EVERY subrequest the worker makes (its `fetch`, workers-rs's `Fetch::Url` and
 * `Fetch::Request`), with the request's URL. Since wrangler 4.148 the proxy enables the Network domain only while
 * a DevTools client is attached, so the tier attaches `network_tap.mjs` to each session's inspector port; the line
 * shape is then unchanged from 4.54 (probed on 4.148.0, a worker's `fetch` of a loopback and a network URL):
 *   [InspectorProxyWorker] RUNTIME INCOMING MESSAGE {
 *     method: 'Network.requestWillBeSent',
 *     params: { requestId: '0', loaderId: '', request: { url: 'http://127.0.0.1:48712/loop-probe', method: 'GET', ...
 * (the same event is echoed as `SEND TO DEVTOOLS {"method":"Network.requestWillBeSent",...}` JSON, which the
 * pattern below, keyed on the single-quoted form, does not count twice). Without the tap 4.148's log holds no
 * subrequest at all and the positive control below reds the guard as BLIND. This guard reads those URLs and reds on any host that is not the
 * network. The Network domain also reports the runtime's BINDING calls (`http://d1/...`, an unbound service
 * binding's `https://chaintracks/...`, a Durable Object stub's `https://board-view/...`, an alarm's self-call to
 * `https://tick.invalid/...`): a host with no dot, or under a TLD reserved never to resolve (RFC 2606/6761:
 * `.invalid`, `.localhost`, `.test`, `.example`), is a binding name, never the network. Every other host (a dotted
 * name or an IP literal) that is not the loopback is EGRESS. The seal beside it (`loopback-only.sb`, macOS `sandbox-exec`) makes such a request fail closed, so a
 * cell can never pass or fail on a mainnet answer; the guard makes the attempt itself a red.
 *
 * A positive control keeps the guard honest: at least one loopback subrequest must be seen across the logs (the
 * e649 cell's delivering overlay posts to its mock app layer on 127.0.0.1), else the log format moved under the
 * guard and it would see nothing; that is a red too (`D1_EGRESS_MIN_SEEN`, default 1).
 *
 * Exit 0 = no egress and the control seen; 1 = egress or a blind guard; 2 = usage.
 */
import { readFileSync, realpathSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const MIN_SEEN = Number(process.env.D1_EGRESS_MIN_SEEN ?? '1')
const LOOPBACK = new Set(['127.0.0.1', 'localhost', '[::1]', '::1'])
const RESERVED_TLDS = ['invalid', 'localhost', 'test', 'example']
// PURE: is this host the network (not the loopback, not a binding's name)?
export const isEgress = (host) => {
  const h = host.toLowerCase()
  if (LOOPBACK.has(h) || /^127\./.test(h)) return false
  if (h.startsWith('[')) return true // an IPv6 literal other than ::1
  if (!h.includes('.')) return false // a binding / Durable Object name: `d1`, `chaintracks`, `board-view`
  return !RESERVED_TLDS.includes(h.split('.').pop())
}

function main() {
  const args = process.argv.slice(2)
  if (args.length === 0 || args.some(a => !a.includes('='))) {
    console.error('usage: egress_guard.mjs <label>=<log> [...]')
    process.exit(2)
  }
  let seen = 0
  let loopback = 0
  const egress = []
  const counts = {}
  for (const arg of args) {
    const i = arg.indexOf('=')
    const label = arg.slice(0, i)
    const path = arg.slice(i + 1)
    let text
    try {
      text = readFileSync(path, 'utf8')
    } catch (e) {
      console.error(`✗ egress guard: cannot read the ${label} log ${path}: ${e.message}`)
      process.exit(1)
    }
    // One event per `Network.requestWillBeSent`; its request's `url:` is the first after the method line.
    const re = /Network\.requestWillBeSent'[\s\S]*?\burl: '([^']*)'/g
    let m
    while ((m = re.exec(text)) !== null) {
      seen++
      const url = m[1]
      let host
      try {
        host = new URL(url).hostname
      } catch {
        host = `(unparsed) ${url}`
      }
      counts[`${label} ${host}`] = (counts[`${label} ${host}`] ?? 0) + 1
      if (isEgress(host)) egress.push(`${label}: ${url}`)
      else if (LOOPBACK.has(host) || /^127\./.test(host)) loopback++
    }
  }

  const summary = Object.entries(counts).map(([k, n]) => `${k} x${n}`).join(', ')
  if (egress.length > 0) {
    const uniq = [...new Set(egress)]
    console.error(`✗ egress guard: ${egress.length} subrequest(s) LEFT THE LOOPBACK (a cell must read only what it seeded; bsv-low #731):`)
    for (const e of uniq.slice(0, 40)) console.error(`  EGRESS ${e}`)
    if (uniq.length > 40) console.error(`  … and ${uniq.length - 40} more distinct`)
    console.error(`  hosts seen: ${summary}`)
    process.exit(1)
  }
  if (loopback < MIN_SEEN) {
    console.error(`✗ egress guard: BLIND, ${loopback} loopback subrequest(s) seen of ${seen} (at least ${MIN_SEEN} expected: the e649 delivering overlay's loopback post); the debug log format moved, the guard proves nothing`)
    process.exit(1)
  }
  console.log(`✅ egress guard: ${seen} subrequest(s), none left the machine (${loopback} on the loopback, the rest binding calls: ${summary})`)
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) main()
