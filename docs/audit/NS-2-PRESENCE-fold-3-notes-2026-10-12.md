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
