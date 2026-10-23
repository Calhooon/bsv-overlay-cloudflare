/**
 * The deferred EF job's OWN state, read from the worker's local D1 file, never through the worker
 * (bsv-low #776, #763). Shared by the NL-6c and NL-6d route witnesses.
 *
 * Why not the poll route. Measured on the local tier (`wrangler dev --local`, 2026-10-22, the
 * reproduction kept under /Volumes/Lanes/out/wt-overlay-B-FLAKES-OVERLAY/nl6c-repro): a request
 * the worker serves WHILE a deferred run of the same isolate is in flight, the run then waiting
 * some seconds on a subrequest, leaves the next request, or the run's queue dispatch, never
 * completing (3 of 3 with the run's broadcaster probe held 4 s; 0 of 2 with no request served
 * during the run). Miniflare's queue hands one batch at a time and only after the last returned,
 * so a dispatch that never returns stops every later job of that worker: `queued`, attempts 0, for
 * good (#763: both NL-6d jobs, after the NL-6c cell's runs). A witness that polls the route every
 * second during a run builds that shape itself whenever the host is loaded. So the witness reads
 * the job's row from the D1 file and asks the worker only once the job has SETTLED.
 *
 * The verdict follows the job, not a wall: each read that shows the job moving (its state, its
 * attempts, its cursor, its touch) restarts the clock. A job `queued` with no run started within
 * `dispatchMs` is NOT DISPATCHED (the local queue: no run ever took it); a job that stops moving
 * mid-run for `runMs` is STALLED; either names its last two reads.
 *
 * The bounds come from the job's measured walls on the local tier at host loads of 6 to 42: the
 * queue dispatched a job within milliseconds of an idle queue, and the slowest run of the cells'
 * jobs (the 2.2 MB batch) took 6.4 s (`QUEUE overlay-mutations 1/1 (6425ms)`). The run bound is
 * ten times that; the dispatch bound covers the S2 replays NL-6d queues ahead of a job (four
 * deliveries each, measured at 28 to 334 ms) many times over.
 */
import { execFileSync } from 'node:child_process'
import { readdirSync } from 'node:fs'

export const DISPATCH_MS = Number(process.env.EF_JOB_DISPATCH_MS ?? 30_000)
export const RUN_MS = Number(process.env.EF_JOB_RUN_MS ?? 64_000)

function d1Files(stateDir) {
  const files = []
  const walk = (d) => {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      if (e.isDirectory()) walk(`${d}/${e.name}`)
      else if (e.name.endsWith('.sqlite')) files.push(`${d}/${e.name}`)
    }
  }
  try { walk(`${stateDir}/v3/d1`) } catch { /* not created yet */ }
  return files
}

/**
 * The job's row (`state`, `attempts`, `legs_from`, `status`, `updated_at`), null when no file holds it, undefined
 * when the file was busy past its timeout (no read made). Opened read-write: `-readonly` cannot open a WAL database
 * whose `-wal` and `-shm` files are absent (SQLITE_CANTOPEN), which is how miniflare leaves it between writes.
 */
export function jobRow(stateDir, reference) {
  if (!/^[0-9a-f]{64}$/.test(reference)) throw new Error(`not a reference: ${reference}`)
  for (const f of d1Files(stateDir)) {
    try {
      const out = execFileSync(
        'sqlite3',
        ['-json', '-cmd', '.timeout 5000', f,
          `SELECT state, attempts, legs_from, status, updated_at FROM ef_deferred_jobs WHERE reference = '${reference}'`],
        { encoding: 'utf8' },
      )
      const row = out.trim() ? JSON.parse(out)[0] : null
      if (row) return row
    } catch (e) {
      // not this database, or the table is not there yet; any other error is the reader's, never "no row"
      const why = `${e.stderr ?? ''}${e.message ?? ''}`
      if (/database is locked|SQLITE_BUSY/i.test(why)) return undefined // this read is not made; the next one is
      if (!/no such table/.test(why)) throw e
    }
  }
  return null
}

const brief = (r) => (r ? `${r.state} attempts=${r.attempts} legsFrom=${r.legs_from} status=${r.status}` : 'no row')

/**
 * Read the job until it settles: `done` or `failed`, or `queued` with a run's answer that is no
 * progress (a status other than the 202 naming the next leg: the job waits for the cron's
 * hand-back, which no local tier fires). Answers `{ settled, verdict, reads }`; `verdict` is null
 * when settled, else `NOT DISPATCHED (...)` or `STALLED (...)` with the last two reads.
 */
export async function waitSettled(stateDir, reference, { dispatchMs = DISPATCH_MS, runMs = RUN_MS } = {}) {
  const reads = []
  let key = null
  let movedAt = Date.now()
  let started = false
  for (;;) {
    const r = jobRow(stateDir, reference)
    if (r !== undefined) {
      reads.push(r)
      if (reads.length > 2) reads.shift()
    }
    if (r) {
      if (r.state === 'done' || r.state === 'failed') return { settled: true, verdict: null, reads }
      if (r.state === 'queued' && r.status !== null && r.status !== 202) return { settled: true, verdict: null, reads }
      if (r.state === 'running' || r.attempts > 0 || r.status !== null) started = true
      const k = `${r.state}|${r.attempts}|${r.legs_from}|${r.updated_at}`
      if (k !== key) { key = k; movedAt = Date.now() }
    }
    const still = Date.now() - movedAt
    if (!started && still > dispatchMs) {
      return { settled: false, verdict: `NOT DISPATCHED (the local queue started no run in ${dispatchMs} ms; last reads: ${reads.map(brief).join(' | ')})`, reads }
    }
    if (started && still > runMs) {
      return { settled: false, verdict: `STALLED (the job did not move for ${runMs} ms; last reads: ${reads.map(brief).join(' | ')})`, reads }
    }
    await new Promise((res) => setTimeout(res, 500))
  }
}

/**
 * The run's queue event has ENDED (bsv-low #763): its D1 row settles before the event returns (the run still
 * releases its bytes at rest and flushes its background work), and a request served inside that tail is the shape
 * above. Miniflare logs `QUEUE <queue> <acked>/<total>` when a dispatch returns and hands one batch at a time, so the
 * first such line after the run's own settle line (`NL-6c: job <reference> done|FAILED|answered`) is that run's end.
 * Answers null once seen, else `EVENT NOT ENDED (...)` after `runMs` with no such line. Without a log path (a
 * caller outside the tier) there is nothing to read and the answer is null.
 */
export async function waitRunEnded(logPath, reference, { runMs = RUN_MS } = {}) {
  if (!logPath) return null
  const { readFileSync } = await import('node:fs')
  const t0 = Date.now()
  for (;;) {
    let text = ''
    try { text = readFileSync(logPath, 'utf8') } catch { /* not written yet */ }
    const lines = text.split('\n')
    let settledAt = -1
    lines.forEach((l, i) => { if (l.includes(`NL-6c: job ${reference} `) && /\b(done|FAILED|answered)\b/.test(l)) settledAt = i })
    if (settledAt >= 0 && lines.slice(settledAt + 1).some((l) => /QUEUE overlay-mutations \d+\/\d+/.test(l))) return null
    if (Date.now() - t0 > runMs) {
      return `EVENT NOT ENDED (the run of ${reference.slice(0, 12)}… settled ${settledAt >= 0 ? 'and logged it' : 'but logged no settle line'}, and no queue dispatch result followed in ${runMs} ms: its queue event never returned)`
    }
    await new Promise((res) => setTimeout(res, 250))
  }
}
