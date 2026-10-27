// Test-only boundary around the production WASM worker and real workerd D1.
// A slow judge is injected after its generation snapshot, never into a write.
import Worker, { AuthSessionStore, BoardView } from '../../crates/low-app-layer/build/worker/shim.mjs'
export { AuthSessionStore, BoardView }
export class RecordBelt {
  constructor(state, env) { this.state = state; this.inner = new BoardView(state, env) }
  async fetch(req) {
    if (new URL(req.url).pathname === '/pin/alarm') {
      await this.state.storage.put('owed-belt', true)
      return this.inner.alarm()
    }
    return this.inner.fetch(req)
  }
  alarm() { return this.inner.alarm() }
}
let delayMs = 0
let delayed = 0
let waiting = 0
const statements = new WeakMap()
function database(db) {
  const statement = (inner, sql) => {
    const proxy = new Proxy(inner, { get(target, key) {
      if (key === 'constructor') return target.constructor
      if (key === 'bind') return (...args) => statement(target.bind(...args), sql)
      const value = Reflect.get(target, key)
      return typeof value === 'function' ? value.bind(target) : value
    } })
    statements.set(proxy, { inner, sql })
    return proxy
  }
  return new Proxy(db, { get(target, key) {
    if (key === 'constructor') return target.constructor
    if (key === 'prepare') return sql => statement(target.prepare(sql), sql)
    if (key === 'batch') return async batch => {
      // PARTIES_SQL is the first evidence read after MARKS_SQL captured the generation.
      if (delayMs && batch.some(s => statements.get(s)?.sql.includes('FROM potparty_records pp INDEXED BY idx_potparty_game_identity'))) {
        const ms = delayMs
        delayMs = 0
        delayed++
        waiting++
        try { await new Promise(resolve => setTimeout(resolve, ms)) } finally { waiting-- }
      }
      return target.batch(batch.map(s => statements.get(s)?.inner ?? s))
    }
    const value = Reflect.get(target, key)
    return typeof value === 'function' ? value.bind(target) : value
  } })
}
export default class RecordDoorPin extends Worker {
  constructor(ctx, env) { super(ctx, { ...env, OVERLAY_DB: database(env.OVERLAY_DB) }) }
  fetch(req) {
    const url = new URL(req.url)
    if (url.pathname === '/pin/delay') {
      delayMs = Number(url.searchParams.get('ms'))
      return Response.json({ delayMs })
    }
    if (url.pathname === '/pin/judge') return Response.json({ delayed, waiting })
    return super.fetch(req)
  }
}
