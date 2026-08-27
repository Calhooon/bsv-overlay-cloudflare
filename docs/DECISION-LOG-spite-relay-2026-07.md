
### 2026-08-27 — addendum 21: layer 10 — beta crossed its own 500-pot window

happyPathEyes, the first cell fully on the layer-9 stack, went red with a NEW
console line: "app-layer /leaderboard page is TRUNCATED — using the slow
gather", every minute through the assert window. The 08-25 campaign hit this
exact cliff at 200 pots and raised BOARD_CLAIM_LIMIT to 500 with the warning
that paging (#403 item 4) is the real fix; beta's history crossed 500 today.
A truncated window can hide the viewer's own pots, so the client's refusal to
fast-path it is PRINCIPLED — the fix is not to weaken the check but to shrink
the window: the #375 era cutoff, set on beta for the first time (the
rehearsal its wrangler note anticipated): 2026-08-27 00:00Z / h964063
(first block past midnight, 00:12:31Z). Deploy f0455f93; verified
`truncated:false`, 28 rows, today's evidence intact. Beta's cutoff bumps
forward the same way whenever its window re-fills; prod keeps the launch-eve
rule. Board paging remains the durable fix.
