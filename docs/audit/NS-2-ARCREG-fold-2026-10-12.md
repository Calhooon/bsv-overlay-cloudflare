# NS-2-ARCREG review fold (#640)

Review: `bsv-low/docs/audit/NS-2-ARCREG-review-2026-10-12.md` (read-only).
This is the overlay's register text for the captain to carry to LOW D1.

## H1: registration window

Registration shares NL-6d's sizing constants: 32 ancestors in a request,
256 in a consumer run. It is a cap, not a paused corroboration: the subject
is always posted, then only its answer decides admission. Ancestors are
selected breadth-first from the subject's EF input references, nearest first,
with duplicates excluded; scripts are skipped as borrowed bytes, not parsed
or hydrated. The cursor remains the corroborator's; registration uses the
nearest ancestors on each invocation. Unselected ancestors are not posted or
registered by this rung. Their proofs remain the poll backstop's and the
couriers', as before #640. `arc_registration_legs_skipped_total` counts them.

Arcade's `services/api_server/routes.go` exposes only `GET /tx/:txid` for
transaction status: there is no batched status-read endpoint to use.
`POST /txs` shares one callback header pair for the body, so registration
under each txid requires a separate `POST /tx`. Both ancestor operations
have a two-second deadline, including response-body reads. Faults, refusals
and timeouts are best effort and cannot prevent the subject's own POST.

Rung-3 calls before this fold: `2N + 1`, unbounded. After: at most
`2 min(N, 32) + 1 = 65` in requests, `513` in consumer runs, plus unchanged
subject polling. For 100 ancestors: 32 registered, subject registered, 68
skipped, 65 calls (formerly 201). For 80: 32 plus subject, 48 skipped,
65 calls (formerly 161). Known ancestors omit callback headers; terminal
skips are described below when folded. The pre-#640 batch was one POST.

Pins use an injectable transport seam plus source checks connecting the seam,
timeout adapters, body/token identity and subject gate to production.
Arcade's live behaviour on this windowed registration remains unverified.

## L1: rung-3 echo and D1 register text

**Since #640 (2026-10-12, overlay `lane/ns-2-arcreg`): a callback's token
is the txid of the ONE transaction its POST carries.** The ladder's
full-batch rung (attempt 3, reached on an orphan view or an inconclusive
pre-batch corroboration) posts at most 32 unproven ancestors per request,
256 per consumer run, NEAREST the subject first, each alone to Arcade `/tx`
under its own txid and registered only when Arcade's own status read does
not hold it; their answers are discarded. Then it posts the subject alone
under its own txid with the register-once rule unchanged. The `/txs` body
that registered every ancestor under the SUBJECT's token is gone. The rest
remain unregistered and counted; their proofs remain the backstop poll's
and the couriers'. An ancestor's MINED push now arrives as its own: D6's
`unknown_txid` ack for a hop, the stitch and `readmit_if_evicted` for a held
pot, never a 401. The subject's rung-3 POST is a one-transaction body, so
the echoed-status short-circuit applies there as on rungs 1 and 2 (under
admit-fast an echoed Pending answers `AcceptedPending`, with the pending
watch armed by the route). A witnessed acceptance keeps the two-witness bar
for unproven ancestry. This confirms review §5 with the owner's 32/256 cap
and nearest-first choice in place of its proposed pause or 128-leg tail.

The L1 wiring pin drives rung 1 orphan → rung 3 echoed Pending →
`AcceptedPending`, with zero corroborations. Source pins tie the real rung's
one-transaction body to the echo fast return and the route's pending watch;
the Worker Fetch runtime cannot run in native unit tests.

## L2: bounded refusal line (D1 register continuation)

`/arc-ingest`'s refusal line names the pushed txid (64 hex or `other`),
`txStatus` (at most 80 characters), User-Agent (at most 64 characters), and
bearer shape (`64-hex` or `other`) plus its byte length, never its value:
`POST /arc-ingest txid=".." txStatus=".." ua=".." bearer=64-hex bearerLen=64 -> 401 (<why>)`.
Fields are debug-quoted on one line; the formatter receives only bearer
shape metadata. Both no-token and bad-token arms use it; their counters are
unchanged. With two mismatching credentials, shape describes Authorization's
Bearer candidate first, then X-CallbackToken; absent/malformed schemes have
shape `other`, length 0. The auth classifier still compares both candidates.
The pin covers a near-1-MiB body, huge status/UA, malformed headers, both
shape words and absence of the credential's value.
