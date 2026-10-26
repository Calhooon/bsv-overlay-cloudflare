// Test-only wrapper: the production WASM BoardView receives real workerd storage and D1.
// Only the clock and one refused storage.put are controlled; no production route exposes these levers.
import worker, { BoardView, AuthSessionStore } from '../../crates/low-app-layer/build/worker/shim.mjs'
export { AuthSessionStore }
const refusals = new Set()
const objects = new Map()
const NativeDate = Date
let clockOffset = 0
globalThis.Date = class extends NativeDate {
  constructor(...args) { super(...(args.length ? args : [NativeDate.now() + clockOffset])) }
  static now() { return NativeDate.now() + clockOffset }
}
export default class PinWorker extends worker {
  fetch(req) {
    const url = new URL(req.url)
    if (url.pathname === '/pin/refuse') { refusals.add(url.searchParams.get('identity')); return new Response('next arm write refused') }
    if (url.pathname === '/pin/object-ids') return Response.json([...objects.get(url.searchParams.get('identity'))])
    if (url.pathname === '/pin/route-alarm') {
      const id = [...objects.get(url.searchParams.get('identity'))].at(-1)
      return this.env.BOARD_VIEW.get(this.env.BOARD_VIEW.idFromString(id)).fetch('http://pin/pin/alarm')
    }
    if (url.pathname === '/pin/advance-clock') { clockOffset += 61000; return new Response('next window') }
    return super.fetch(req)
  }
}
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
    if (url.pathname === '/pin/belt-alarm') {
      await this.state.storage.put('owed-belt', true)
      return this.inner.alarm()
    }
    if (url.pathname === '/pin/state') return Response.json({keys: Object.fromEntries(await this.state.storage.list()), alarm: await this.state.storage.getAlarm()})
    if (url.pathname === '/pin/duplicate-alarm') {
      await this.inner.alarm()
      return this.fetch(new Request('http://pin/pin/state'))
    }
    if (url.pathname === '/pin/alarm') {
      // Drive the real alarm handler at its stored due, without waiting a minute per fault.
      await this.state.storage.put('owed-due', Date.now() - 1)
      await this.inner.alarm()
      return this.fetch(new Request('http://pin/pin/state'))
    }
    if (url.pathname === '/pin/refuse') { this.refuse = true; return new Response('next put refused') }
    if (url.pathname === '/pin/old-actor') {
      // the storage the retired deferred walk left (its identity, due and an armed alarm), then its alarm fires
      await this.state.storage.put('owed-identity', url.searchParams.get('identity'))
      await this.state.storage.put('owed-due', Date.now() - 1)
      await this.state.storage.setAlarm(Date.now() + 30000)
      await this.inner.alarm()
      return this.fetch(new Request('http://pin/pin/state'))
    }
    if (url.pathname === '/pin/delete-all') {
      await this.state.storage.setAlarm(Date.now() + 120000)
      await this.state.storage.put('probe', true)
      await this.state.storage.deleteAll()
      return this.fetch(new Request('http://pin/pin/state'))
    }
    if (url.pathname === '/owed-deferred') {
      const identity = url.searchParams.get('identity')
      if (!objects.has(identity)) objects.set(identity, new Set())
      objects.get(identity).add(this.state.id.toString())
    }
    if (url.pathname === '/owed-deferred' && refusals.delete(url.searchParams.get('identity'))) this.refuse = true
    return this.inner.fetch(req)
  }
  alarm() { return this.inner.alarm() }
}
