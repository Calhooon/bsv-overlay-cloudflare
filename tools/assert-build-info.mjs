import assert from 'node:assert/strict'

// Used by the existing local-Worker route tier. No network or servers here.
export function assertBuildInfo(body) {
  const b = body.build
  assert.ok(b && typeof b === 'object', 'health carries build metadata')
  assert.deepEqual(Object.keys(b).sort(), ['built_at', 'crates', 'dirty', 'engine_rev', 'lib_rev'])
  for (const key of ['engine_rev', 'lib_rev']) assert.match(b[key], /^(?:[0-9a-fA-F]{40}|unknown)$/)
  assert.equal(typeof b.dirty, 'boolean')
  assert.match(b.built_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/)
  assert.ok(Number.isFinite(Date.parse(b.built_at)))
  assert.deepEqual(b.crates, {
    'bsv-rs': '0.4.3',
    'bsv-middleware-cloudflare': '0.5.1',
    'bsv-middleware-core': '0.2.1',
  })
}
