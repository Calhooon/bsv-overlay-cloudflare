import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

// Used by the existing local-Worker route tier. No network or servers here.
export function assertBuildInfo(body) {
  const b = body.build
  assert.ok(b && typeof b === 'object', 'health carries build metadata')
  assert.deepEqual(Object.keys(b).sort(), ['built_at', 'crates', 'dirty', 'engine_rev', 'lib_rev'])
  for (const key of ['engine_rev', 'lib_rev']) assert.match(b[key], /^(?:[0-9a-fA-F]{40}|unknown)$/)
  assert.equal(typeof b.dirty, 'boolean')
  assert.match(b.built_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/)
  assert.ok(Number.isFinite(Date.parse(b.built_at)))
  // The rule: the live build object names the exact versions of the workers
  // lock it was built from. Read the lock, never a literal (a re-pin moved
  // bsv-rs and a literal here went stale: the route tier failed on 0.4.5).
  assert.deepEqual(b.crates, lockVersions(['bsv-rs', 'bsv-middleware-cloudflare', 'bsv-middleware-core']))
}

function lockVersions(names) {
  const lock = readFileSync(resolve(dirname(fileURLToPath(import.meta.url)), '..', 'workers', 'Cargo.lock'), 'utf8')
  const out = {}
  for (const name of names) {
    const entries = lock.split('[[package]]').filter((p) => new RegExp(`^name = "${name}"$`, 'm').test(p))
    assert.equal(entries.length, 1, `exactly one ${name} in workers/Cargo.lock`)
    out[name] = entries[0].match(/^version = "([^"]+)"$/m)[1]
  }
  return out
}
