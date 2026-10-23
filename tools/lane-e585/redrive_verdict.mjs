/**
 * bsv-low #594: what a re-driven dead letter's OWN row says about its message, read from the local D1, so a
 * re-drive leg tells "the local queue never delivered it" apart from "the consumer answered wrong".
 *
 * The lever's claim (`dead_letters::CLAIM_SQL`) writes `status = 'redriven'`, `attempts = 0`, `redriven_at = now`,
 * and sends ONE message. Every delivery of that message touches the row: an ack DELETES it (`RESOLVE_SQL`), a
 * failed replay counts an attempt on it (`NOTE_FAILING_SQL`: `attempts + 1`, its status kept), a death after the
 * replays re-PARKS it. So a row still `redriven`, `attempts` 0, `redriven_at` the claim's, was never consumed: no
 * delivery reached the consumer. That is the local queue's (miniflare's) verdict, NOT DELIVERED, never a wrong
 * answer. Until #594 the leg read only "row gone / counters moved" and a message the harness's queue did not
 * deliver read the same as a consumer that answered wrong (`de29a49`'s run 1, leg E: 120 s, no
 * `QUEUE overlay-mutations` line after the lever's `redriven=1`).
 *
 * `claimedAt` is the `redriven_at` the leg saw right after its lever call (null when not read): a row re-claimed
 * since is another re-drive, not this one.
 */
export function undelivered(row, claimedAt = null) {
  if (!row || row.status !== 'redriven' || Number(row.attempts) !== 0) return false
  return claimedAt === null || Number(row.redriven_at) === Number(claimedAt)
}

/** The verdict line's head for a re-drive leg that did not reach its expectation. */
export function redriveVerdict(row, claimedAt = null) {
  return undelivered(row, claimedAt)
    ? 'NOT-DELIVERED (the local queue: the re-driven message was never consumed, its row still redriven, attempts 0)'
    : 'FAIL'
}

/**
 * A leg that waits for a letter to PARK (its replays fail, the message dead-letters, the DLQ consumer parks it). A
 * row still in flight at the end of the wait, `failing` (the main queue's deliveries noted) or `redriven` (a claim),
 * is a message whose life the local queue did not finish: either the main queue stopped handing it back or the dead
 * letter queue never delivered it. NOT DELIVERED, never FAIL. A row gone, or parked with the wrong fields, is a FAIL.
 */
export function parkVerdict(row) {
  return row && (row.status === 'failing' || row.status === 'redriven')
    ? `NOT-DELIVERED (the local queue: the letter is still ${row.status}, attempts ${row.attempts}; its deliveries or its dead-letter delivery never came)`
    : 'FAIL'
}
