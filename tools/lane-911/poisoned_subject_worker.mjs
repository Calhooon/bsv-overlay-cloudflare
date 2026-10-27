// bsv-low #911 (D2-ACTOR-A-FOLD-3): test-only boundary around the production WASM worker and real workerd D1.
// Every batch holding a statement that binds the poisoned game faults, except the fault memo's own rows
// (`owed_belt_state`), so the consumer's judge of that ONE subject faults for good and its memo is still written.
import Worker, { AuthSessionStore, BoardView } from '../../crates/low-app-layer/build/worker/shim.mjs'
export { AuthSessionStore, BoardView }
export const POISON = 'poison-911'
let faulted = 0
const statements = new WeakMap()
function database(db) {
  const statement = (inner, sql, binds) => {
    const proxy = new Proxy(inner, { get(target, key) {
      if (key === 'constructor') return target.constructor
      if (key === 'bind') return (...args) => statement(target.bind(...args), sql, args)
      const value = Reflect.get(target, key)
      return typeof value === 'function' ? value.bind(target) : value
    } })
    statements.set(proxy, { inner, sql, binds })
    return proxy
  }
  const poisoned = s => {
    const meta = statements.get(s)
    return meta && !meta.sql.includes('owed_belt_state') && (meta.binds ?? []).some(b => typeof b === 'string' && b.includes(POISON))
  }
  return new Proxy(db, { get(target, key) {
    if (key === 'constructor') return target.constructor
    if (key === 'prepare') return sql => statement(target.prepare(sql), sql, [])
    if (key === 'batch') return async batch => {
      if (batch.some(poisoned)) { faulted++; throw new Error('injected fault: the poisoned subject') }
      return target.batch(batch.map(s => statements.get(s)?.inner ?? s))
    }
    const value = Reflect.get(target, key)
    return typeof value === 'function' ? value.bind(target) : value
  } })
}
export default class PoisonedSubjectPin extends Worker {
  constructor(ctx, env) { super(ctx, { ...env, OVERLAY_DB: database(env.OVERLAY_DB) }) }
  fetch(req) {
    if (new URL(req.url).pathname === '/pin/faulted') return Response.json({ faulted })
    return super.fetch(req)
  }
}
