# NS-2-PRESENCE third fold

NS2PD2-M1 chooses (b). Arcade POST `submitResponse` carries `txid`, `status`,
`txStatus`, no `timestamp`; the row timestamp belongs to GET /tx alone.
Under an OPEN eviction, only the network's fresh word on OUR re-present
readmits through POST; a stored echo never does. With no eviction the echo
is an idempotent fast ack. The helper's stamped comparison is defensive,
not a delivered rule. The pre-flight stored SEEN readmission (#636), MINED
proof readmission and witnessed-mode #637 remain as recorded in lens 2.

Register D1 amendment (for the captain's bsv-low register): under an OPEN
eviction a stored POST echo is refused with 502, never an admission;
Arcade's submit echo has no timestamp. A fresh RECEIVED accept on OUR
re-present takes SyncAccepted and readmit_if_evicted (#513). No eviction:
a stored echo of any age is the idempotent fast ack.

The read-only second delta lens's M1 terms are adopted here. The wire body
pin and route-source pin cover the eviction passed into the Reached arm,
its refusal, and the Pending fast accept's readmission wiring.

NS2PD2-L1 keeps `/owed` at 28 statements (recorded 26 + 2 margin). Owed's
external spender skips the scope/anchor query; other callers retain it.
The ceiling and D1 budget doc agree; fold 2's unmeasured 29 is withdrawn.

Pins: fold3-m1 neuters the Reached eviction argument; fold3-l1 disables the
owed bypass, each in a disposable copy. Both RED at exit 101, never a
compile failure; intact M1 and L1 pins GREEN. CI evidence follows in the PR.

Final validation: changed-hunk rustfmt and diff checks pass; both full worker
package test commands and all-targets clippy with `-D warnings` exit 0.
All 25 NS-2 pins pass (original 23 plus M1 and L1). Storage ownership: 694
references match. `make ci-d1-budget` exits 0, fixture owed 352 reads / 18
writes / 26 statements against 528 / 20 / 28. Route leg
`make -o ci-d1-budget ci-route` exits 0 (the prerequisite just proved, split
into its own foreground leg). Starts at 1-min load 18.71 and 23.53, each
wrapped at 570 seconds; bindgen CLI 0.2.126, LANE_BASE=19891. Both locks
unchanged. No deploy or spend. Isolated target removed after checks.
Unverified: live capability/stamps, concurrent D1 writes, #636/#637; PR gate
is the next step.
