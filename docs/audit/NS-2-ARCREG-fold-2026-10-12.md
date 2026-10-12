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
