#!/usr/bin/env node
/**
 * bsv-low #594's pin: `node tools/lane-e585/redrive_verdict_test.mjs` (run by `make ci-d1-budget` before the cell). A
 * re-driven row no delivery touched reads NOT-DELIVERED, never FAIL; a row a delivery touched (an attempt counted,
 * re-parked, re-claimed) reads FAIL. On the base (`45b83a32`'s cell) both read FAIL alike (`de29a49`'s run 1, leg E).
 */
import { parkVerdict, redriveVerdict, undelivered } from './redrive_verdict.mjs'

let bad = 0
const check = (ok, what) => {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${what}`)
  if (!ok) bad++
}
const claimed = { status: 'redriven', attempts: 0, redrives: 1, redriven_at: 1_792_000_000_000 }

check(undelivered(claimed, claimed.redriven_at), 'a row still as the lever claimed it was never consumed')
check(redriveVerdict(claimed, claimed.redriven_at).startsWith('NOT-DELIVERED'), 'its verdict is NOT-DELIVERED, never FAIL')
check(undelivered(claimed), 'the claim time unread: the claim shape alone decides')
check(redriveVerdict({ ...claimed, attempts: 1 }, claimed.redriven_at) === 'FAIL', 'a delivery that failed counted an attempt: FAIL (a wrong answer)')
check(redriveVerdict({ ...claimed, status: 'parked' }, claimed.redriven_at) === 'FAIL', 're-parked after its replays: FAIL')
check(redriveVerdict(undefined, claimed.redriven_at) === 'FAIL', 'no row (an ack the leg did not expect): FAIL')
check(redriveVerdict({ ...claimed, redriven_at: claimed.redriven_at + 5 }, claimed.redriven_at) === 'FAIL', 're-claimed since: another re-drive, FAIL')
check(redriveVerdict({ ...claimed, attempts: '0', redriven_at: String(claimed.redriven_at) }, claimed.redriven_at).startsWith('NOT-DELIVERED'), 'the CLI renders numbers as text: read alike')
check(parkVerdict({ status: 'failing', attempts: 4 }).startsWith('NOT-DELIVERED'), 'a letter that used its deliveries and never parked: NOT-DELIVERED (the dead letter queue)')
check(parkVerdict({ status: 'failing', attempts: 2 }).startsWith('NOT-DELIVERED'), 'a letter whose retries stopped: NOT-DELIVERED (the main queue)')
check(parkVerdict({ ...claimed }).startsWith('NOT-DELIVERED'), 'a re-driven letter never consumed: NOT-DELIVERED')
check(parkVerdict(undefined) === 'FAIL', 'no row (acked, not dead-lettered): FAIL')
check(parkVerdict({ status: 'parked', attempts: 4 }) === 'FAIL', 'parked but the leg wanted other fields: FAIL')

console.log(bad === 0 ? 'redrive verdict pin: OK' : `redrive verdict pin: ${bad} FAILED`)
process.exit(bad === 0 ? 0 : 1)
