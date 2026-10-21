#!/usr/bin/env node
/**
 * bsv-low #731 under wrangler 4.148 (FIX-TIERS-4148): the egress guard's TAP. The guard reads the runtime's
 * `Network.requestWillBeSent` out of each `wrangler dev --log-level debug` log, which wrangler's inspector proxy
 * prints as `RUNTIME INCOMING MESSAGE`. Up to 4.54 the proxy sent `Network.enable` to the runtime on every
 * connection; since (at least) 4.148 it sends it only while a DevTools client is attached
 * (`InspectorProxyWorker.handleRuntimeWebSocketOpen`), so with none attached the log holds no subrequest at all
 * and the guard is BLIND. This tap is that client: it attaches to the dev session's inspector port, asks
 * `Network.enable` itself (forwarded to the runtime) and holds the socket for the session's life, so the debug
 * log carries every subrequest again, in the same line shape as 4.54's. Harmless under 4.54 (enabled twice).
 *
 *   node tools/lane-731/network_tap.mjs <inspector port> <label>
 *
 * It prints `TAP ATTACHED <label>` once `Network.enable` is answered, re-attaches if the socket drops (a reload),
 * and exits with its parent's kill (the tier's cleanup kills its job's process tree). Exit 1 if it never attaches
 * within 60 s. Its log is `/tmp/lane499-d1-<LANE_BASE>-tap-<label>.log`, beside the tier's worker logs.
 */
import { spawn } from 'node:child_process'
const [port, label = 'worker'] = process.argv.slice(2)
if (!port) { console.error('usage: network_tap.mjs <inspector port> <label>'); process.exit(2) }
if (typeof WebSocket === 'undefined') {
  // node 20 (the Studio's 4.54 tiers) has the global WebSocket behind a flag; node 22 (the .nvmrc) has it on.
  // The child is killed with this process (a plain `kill <pid>` of the tap must not leave it attached).
  const child = spawn(process.execPath, ['--experimental-websocket', ...process.argv.slice(1)], { stdio: 'inherit' })
  for (const sig of ['SIGTERM', 'SIGINT', 'SIGHUP']) process.on(sig, () => { child.kill(sig); process.exit(143) })
  child.on('exit', (code) => process.exit(code ?? 1))
} else {
  const deadline = Date.now() + 60_000
  let attached = false
  let everAttached = false
  function attach() {
    // Node's WebSocket (undici) takes headers: the proxy wants a loopback Host and Origin.
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, { headers: { Origin: 'http://localhost' } })
    ws.addEventListener('open', () => ws.send(JSON.stringify({ id: 1, method: 'Network.enable' })))
    ws.addEventListener('message', (e) => {
      let m
      try { m = JSON.parse(String(e.data)) } catch { return }
      if (m.id === 1 && !attached) { attached = everAttached = true; console.log(`TAP ATTACHED ${label} :${port}`) }
    })
    let retried = false
    const again = () => {
      if (retried) return
      retried = true
      // The 60 s bound is for the FIRST attach; after a drop (a reload) it retries for the session's life.
      if (!everAttached && Date.now() > deadline) { console.error(`✗ network tap: ${label} never attached on :${port}`); process.exit(1) }
      attached = false
      setTimeout(attach, 500)
    }
    ws.addEventListener('close', again)
    // node 20's undici fires `error` and no `close` on a refused connect (node 22 fires both): retry on either, once.
    ws.addEventListener('error', again)
  }
  attach()
}
