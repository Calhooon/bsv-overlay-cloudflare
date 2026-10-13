// Test-only wrapper: the production WASM BoardView receives real workerd storage and D1.
// Only the clock and one refused storage.put are controlled; no production route exposes these levers.
import worker, { BoardView, AuthSessionStore } from '../../crates/low-app-layer/build/worker/shim.mjs'
export { AuthSessionStore }
export default worker
export class OwedPin {
  constructor(state, env) {
    this.state = state
    this.refuse = false
    const storage = new Proxy(state.storage, { get: (target, key) => {
      if (key === 'put') return (...args) => {
        if (this.refuse) { this.refuse = false; throw new Error('pinned storage write refused') }
        return target.put(...args)
      }
      const value = Reflect.get(target, key)
      return typeof value === 'function' ? value.bind(target) : value
    } })
    const wrapped = new Proxy(state, { get: (target, key) => {
      if (key === 'storage') return storage
      const value = Reflect.get(target, key)
      return typeof value === 'function' ? value.bind(target) : value
    } })
    this.inner = new BoardView(wrapped, env)
  }
  async fetch(req) {
    const url = new URL(req.url)
    if (url.pathname === '/pin/state') return Response.json({keys: Object.fromEntries(await this.state.storage.list()), alarm: await this.state.storage.getAlarm()})
    if (url.pathname === '/pin/alarm') {
      // Drive the real alarm handler at its stored due, without waiting a minute per fault.
      await this.state.storage.put('owed-due', Date.now() - 1)
      await this.inner.alarm()
      return this.fetch(new Request('http://pin/pin/state'))
    }
    if (url.pathname === '/pin/refuse') { this.refuse = true; return new Response('next put refused') }
    if (url.pathname === '/pin/delete-all') {
      await this.state.storage.setAlarm(Date.now() + 120000)
      await this.state.storage.put('probe', true)
      await this.state.storage.deleteAll()
      return this.fetch(new Request('http://pin/pin/state'))
    }
    return this.inner.fetch(req)
  }
  alarm() { return this.inner.alarm() }
}
