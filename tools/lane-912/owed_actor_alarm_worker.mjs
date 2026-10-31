// bsv-low #912 (D2-ACTOR-B-FOLD-7, the captain's item 11): test-only wrapper around the production WASM OwedActor on
// real workerd storage. It changes nothing the actor does: it forwards every fetch and every ALARM to the actor, and
// adds one read (`/pin/state`: the stored alarm, the actor's `meta`, how many alarms workerd fired). No production
// route reaches it; workerd itself fires the alarm (never a direct call of `alarm()` from the pin).
import worker, { OwedActor, BoardView, AuthSessionStore } from '../../crates/low-app-layer/build/worker/shim.mjs'
export { BoardView, AuthSessionStore }
export default worker
export class AlarmPin {
  constructor(state, env) {
    this.state = state
    this.fired = 0
    this.inner = new OwedActor(state, env)
    this.answers = []
  }
  async fetch(req) {
    if (new URL(req.url).pathname === '/apply' && req.method === 'POST') {
      // D2-ACTOR-F-FOLD (bsv-low #959): each apply's answer kept as the actor gave it (its stages, its record calls),
      // the response itself unchanged
      const res = await this.inner.fetch(req)
      try {
        const a = await res.clone().json()
        this.answers.push({ at: Date.now(), carried: a.carried, recordCalls: a.recordCalls, closureRounds: a.closureRounds, readMs: a.readMs, waitMs: a.waitMs, judgeMs: a.judgeMs, writeMs: a.writeMs, outsideMs: a.outsideMs, rounds: a.rounds, applied: a.applied, firstApply: a.firstApply, visibleMs: a.visibleMs, appliedGames: a.appliedGames })
      } catch {}
      return res
    }
    if (new URL(req.url).pathname === '/pin/answers') return Response.json({ answers: this.answers })
    if (new URL(req.url).pathname === '/pin/state') {
      let meta = {}
      try { meta = Object.fromEntries(this.state.storage.sql.exec('SELECT name,value FROM meta').toArray().map(r => [r.name, r.value])) } catch {}
      return Response.json({ alarm: await this.state.storage.getAlarm(), now: Date.now(), fired: this.fired, meta })
    }
    if (new URL(req.url).pathname === '/pin/applied') {
      // D2-ACTOR-E (bsv-low #954): the events this actor recorded applied, and when (its own clock).
      const kind = new URL(req.url).searchParams.get('kind') ?? ''
      let rows = []
      try { rows = this.state.storage.sql.exec('SELECT kind,seq,game,atMs FROM applied WHERE kind=?', kind).toArray() } catch {}
      return Response.json({ rows })
    }
    if (new URL(req.url).pathname === '/pin/dead-arm') {
      // The state the old build left on beta (the captain's item 12): the bootstrap's cursor set and its alarm handed
      // an epoch stamp as the offset (`set_alarm(Date::now().as_millis() as i64)`), so about 56 years out.
      const { identity } = await req.json()
      this.state.storage.sql.exec('CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT)')
      this.state.storage.sql.exec("INSERT OR REPLACE INTO meta(name,value) VALUES ('identity',?),('bootCursor',?)", identity, '[9223372036854775807,""]')
      await this.state.storage.setAlarm(Date.now() + Date.now())
      return Response.json({ alarm: await this.state.storage.getAlarm() })
    }
    return this.inner.fetch(req)
  }
  async alarm() {
    this.fired++
    return this.inner.alarm()
  }
}
