# L3: the door's 21 sites on this tip (bsv-low #619, plan row W1.5d, lane W15D-DOOR-TRACKER)

Stamp: 2026-10-23T16:32Z. Base: `d370d906` (origin/main). The table is LOW's words map
(`bsv-low/docs/DESIGN-LOW-WORDS-2026-10-12.md`, table 1a) re-read against this tip's sources: for each site, the
word it HAND-WRITES today (a decision or a constant written in this repository, not handed to bsv-tracker), the
tracker type that carries it, and the policy that stays the host's.

The lane's bar (the captain's brief): the admission's outcomes are unchanged and every divergence row of LOW's
register D1 stays true (the two-source refusal, the age floor on absence #513 / #740, the terminal judgement by the
indexers #519, the live-race word #602, the stored echo #612, the conflict pass, the ghost pot's end #831, the
retire pass's arms #765). So the design's "becomes" column is taken where it carries the SAME answer (the word
enters as the tracker's value, the decision stays host policy over it), and NOT where it changes an answer (an
eviction that rests only on `conflicted` / `rejected`, a mined height only from `Headers::check`): those are the
design's step C2 proper and are named as left below, each with its file and line.

What L8 (overlay PR 113, bsv-low #624) already moved: every Arcade transaction word the door hears (the gate's
echo and poll, the probe, the pending watch's look, the witness, the `/arc-ingest` push, the retire pass's live
look, the proof look) is `bsv_tracker::Verdict::from_arc_body` then `into_hint("arcade", t)`
(`door_words.rs:84 arcade_hint`), and the door's answer is host policy over that `Hint` (`door_words::gate`,
`callback`, `live_look`, `proof_class`, `refusal`, `network_holds`).

What was still hand-written at the base: the COURIERS' presence word (S11: Bitails' and WhatsOnChain's `/tx`
answer read as "held at a height", "held unmined", "absent", "could not say") and the ARC corroborator's words
(S2, S3: TAAL / GorillaPool's `txStatus` read by a local list and a local rank).

## The table

`bc` = `crates/overlay-cloudflare/src/broadcaster.rs`, `af` = `admit_fast.rs`, `pf` = `proof_fetcher.rs`,
`dw` = `door_words.rs`, all under `crates/overlay-cloudflare/src/`. Lines are the base's.

| site | file:line (base) | the word it hand-writes today | the tracker type that carries it | the policy that stays the host's | this lane |
|---|---|---|---|---|---|
| S1 | `bc:1221 classify_arcade_status`, `bc:1100 GateVerdict` | none (L8): Arcade's `txStatus` is the tracker's | `Verdict::from_arc_body` + `into_hint("arcade")` (`dw:84`) | the gate's bar (`dw::network_holds`, `dw::gate`): seen, in a block or mined admits; the orphan view routes to the ancestry rungs | done before (L8) |
| S2 (Arcade) | `bc:1569 classify_submit_response`, `bc:1680 ladder_step` | Arcade's synchronous HTTP 400 body: `status` 476 / 466 / 467 and `error` "validation failed" | none: `Verdict` reads a `txStatus` body and never an HTTP status ("HTTP status is never consulted", bsv-tracker 0.7.0 `hints.rs:86`, `Verdict::from_arc_body`); the 400 body has no `txStatus` | the 476 transport, the 466 provider view (corroborate), the 467 / validation-failed 422: host policy over the broadcaster's HTTP refusal | stays, by name (no tracker type carries it) |
| S2 (ARC) | `bc:392 arc_verdict`, `bc:549 corroborator_verdict`, `bc:1169 arcade_status_rank` | ARC's `txStatus` by a local list (`DOUBLE_SPEND_ATTEMPTED`, `REJECTED`, `INVALID`, `MALFORMED`), an `ORPHAN` substring of `extraInfo`, `MINED_IN_STALE_BLOCK` transient, and a local rank for the corroborator's SEEN bar | `Verdict` / `Hint` would carry it | the 422 on two broadcasters, the transport fallback, the already-known dress | LEFT, a finding: the tracker's reading differs on recorded answers (below) |
| S3 | `bc:665 corroborated_unseen`, `bc:727 corroborated_accept_claim`, `bc:766 corroborated_mined_claim`, `bc:799 fold_refuse_bar`, `bc:1798 gate_accept_claim_with` | inherits S2 (ARC) | two `Hint`s | the #267 two-witness bar | left with S2 (ARC) |
| S4 | `bc:2189 terminal_at_arcade_by_presence`, `bc:2878 terminal_presence_fresh`, `bc:2395 TerminalJudgement`, `bc:2253 must_reask_before_refusing` | the couriers' presence (S11) folded by `pf::classify_presence` | `Hint { source: "bitails" \| "woc", status: Mined { height } }` (a courier's mined CLAIM) or `MempoolPresent { source }`; absence and a fault are no input | #519 / #602: a courier's mined claim admits (D1's stated trust, unverified against chaintracks), mempool presence is the race word, aged absence the 422, young absence and a fault retryable | slice 2: the fresh read folds the couriers' hints |
| S5 | `af:69 callback_action` | none (L8) | `arcade_hint` | `dw::callback`: a holding word triggers the live read, a refusal the evidence check | done before (L8) |
| S6 | `af:2375 evidence_verdict`, `af:2491 evidence_check`, `af:2536 verdict_of_look`, `af:2332 REFUSAL_ABSENT_MIN_AGE_MS` | Arcade: none (L8, `ArcadeLook` is `dw::live_look` over the hint). The couriers: a bare `Option<bool>` (`pf::presence_bool`, the mined height dropped) folded by `pf::classify_presence` | the couriers' `Hint`s as S4 | #214 / #513 / #740: a refusal needs Arcade's live fatal word or its absence AND both indexers' clean absence; an absence-based refusal takes the 10 min floor; any courier hint is a witness against it | slice 1 |
| S7 | `af:2753 refusal_job`, `af:2833 pending_watch_job`, `af:1485 write_refused_verdict` | consumers of S6 (the watch's look is L8's `witness_look_of` over the hint) | as S6 | the eviction (the shadow move) on a corroborated refusal; the `refused` memo | slice 1 (through `evidence_check`) |
| S8 | `af:2725 witness_job`, `ops.rs:1144 latch_network_seen` | none at the door (the SEEN is L8's hint); the latch is a write-once flag | `Hint::Seen` | the latch itself (its shape, M in the map, is the app layer's served state) | stays (shape is C1's) |
| S9 | `af:1701 CourierConflictPresence`, `af:1792 converge_confirmed_conflict`, `af:2157 confirmed_conflict_pass` | the competitor's presence (S11) by `pf::classify_presence`; a courier height read as "confirmed" | the competitor's courier `Hint::Mined { height }` | #602: a content-addressed competitor with an indexer's positive mined height evicts (D1's stated trust) | slice 3 |
| S10 | `af:1539 readmit_if_evicted` | none: a pushed MINED proof is checked against chaintracks | `Evidence::Proof` (the design's shape; the check here is `pf::verify_bump_outcome`) | the readmission | stays |
| S11 | `pf:4009 classify_presence`, `pf:3993 NetworkPresence`, `pf:4037 presence_response`, `pf:4058 presence_bool`, `pf:4063 bool_presence`, `pf:187 bitails_presence`, `pf:209 woc_presence` | THE producer: a courier's HTTP answer as present-at-height, present-unmined, absent, could-not-say, and the two-source fold | `courier_hint(source, presence)` (`dw`, new): `Mined { height }` / `MempoolPresent { source }`, or no input (`CourierWord::Absent`, `CourierWord::Fault`) | the two-source bar (a negative needs both indexers' clean 404, #212 / #213 / #214), the height taken first from Bitails then WoC: `dw::courier_presence` (new) | slices 1 to 3 move every production fold onto it; `presence_response` stays the HTTP reader in front of the adapter |
| S12 | `pf:4507 retire_verdict`, `pf:4611 fold_arcade_look`, `pf:4480 verdict_kind_of_retire_reason`, `pf:5641 run_retire_pass` | Arcade: none (L8). The couriers: `Option<bool>` folded by `pf::classify_presence` | as S6 | #765: 1 h on a live fatal word plus both absent, 48 h on absence, the recorded word as absence; the `refused` / `retired` memo | slice 3 |
| S13 | `pf:4190 rebroadcast_absent_admitted`, `pf:4344 rebroadcast_eligible`, `pf:4362 run_rebroadcast_backstop` | the couriers' presence by `pf::classify_presence` | as S6 | #273: rebroadcast ancestry-first on both indexers' absence | slice 3 |
| S14 | `pf:1241 verify_bump_outcome` | none: the root checked against chaintracks | `Headers::check` (the design's `impl Headers for WorkerChainTracker`) | the ladder's rungs | stays (the binding is C2's chain slice) |
| S15 | `pf:3123 complete_spend_confirmations`, `af:3089 confirm_spend_now_with` | none: a chaintracks-verified bump | `Evidence::Proof` | the CAS | stays |
| S16 | `pf:2169 discover_spend_for_row` | a courier-named spender written as `spent = 1, spentConfirmed = 0` | a `DoubleSpend { competitors }`-shaped hint about the outpoint | the discovery pass | left (M: the wire must name the tier; a wire change) |
| S17 | `arcade_reorg.rs:380 consume_arcade_reorg_events` | Arcade's orphan row demotes to SEEN | `HintStatus::StaleBlock`, `Tracker::on_chain` | the re-anchor | left (M: the written word is the served state's) |
| S18 | `reorg_sweep.rs`, `tip_pass.rs` `REORG_SWEEP_DEPTH` | the depth-3 re-verify window | `recheck` | the no-feed host's bounded poll | left (M: deleted only once the reorg envelope's delivery is proven, #25) |
| S19 | `tip_pass.rs:31 parse_tip_changed` (and its hash and `reorgFrom` readers) | the app layer's kindless `{height, hash, reorgFrom}` push | `bsv_tracker::decode_envelope` reads the v1 / v2 envelope, a different wire | the tip pass | left (M: a wire change on both workers) |
| S20 | `pf:5986 apply_pushed_proof_to_pot_stores` | none: the pushed path verified against chaintracks | `Evidence::Proof` | the D6 unknown-txid ack | stays |
| S21 | `overlay-discovery/src/pot/storage.rs:495 mark_spent` | none: the bump checked at write, else the host's unconfirmed pointer | the host's own record | the index row | stays |

## S2 (ARC): why it is not moved here (a finding, not a fix)

`arc_verdict` and `corroborator_verdict` read ARC's 2xx `txStatus`. Through `Verdict::from_arc_body` +
`into_hint`, five recorded answers would move:

1. `UTXO_SPENT` is the tracker's `DoubleSpend`; `arc_verdict` reads it as Accepted (not in its error list).
2. A `competingTxs` list behind an accepting word is the tracker's `DoubleSpend` (a refusal); `arc_verdict` reads
   the accepting word.
3. An `ORPHAN` in `extraInfo` is a refusal in `arc_verdict` (and inconclusive in the corroborator); the tracker
   reads the status word only (0.4.1: "an ORPHAN there no longer makes OrphanMempool").
4. A word outside the vocabulary (or empty) is `UnknownStatus` (no input) in the tracker; `arc_verdict` reads it
   as Accepted (the primary gate also polls).
5. The echoed `txid` is checked by `from_arc_body` (`TxidMismatch`); `arc_verdict` does not know the subject.

Each is an admission outcome (1, 2 and 4 on the TAAL leg, 3 on both). The design's L3 does not say they move, so
they stay hand-written, named here for the C2 lens: `bc:392`, `bc:549`, `bc:1169`.

## What stays the host's, by name (every D1 row)

- The two-source refusal (#214, #513): `dw::courier_presence`'s `Absent` needs both indexers' clean absence; any
  courier hint is presence. A courier hint never refuses and never admits by itself at the evidence check.
- The age floor on absence (#513, #740): `af::refusal_rests_on_absence`, `REFUSAL_ABSENT_MIN_AGE_MS`, unchanged.
- The terminal judgement by the indexers (#519) and the live-race word (#602): `bc::terminal_at_arcade_by_presence`
  reads the fold; a courier's `Mined { height }` hint is a CLAIM the host admits on (D1's stated trust, the design's
  S4 gap H, not closed here), `MempoolPresent` is the race word.
- The stored echo (#612, #636): `bc::probe_answer`, untouched.
- The conflict pass (#602, #825): the competitor's courier `Mined { height }` hint evicts, as before (S9's H).
- The ghost pot's end (#831) and the retire pass's arms (#765): `pf::retire_verdict`'s arms, unchanged.
- The non-final word (#836) and the ranks below SEEN (#845): `dw::live_look`, `dw::bound_look`, untouched.

## What this lane landed (three slices, each RED before GREEN)

The adapter (`door_words.rs`): `CourierWord` (`Holds(Hint)`, `Absent`, `Fault`), `courier_hint(source, presence,
observed)` (a courier's mined claim `HintStatus::Mined { height }`, held unmined `HintStatus::MempoolPresent
{ source }`, a clean 404 and a fault no input), `courier_of_bool` (the pinned tables' bare yes or no), and the
host's two-source fold `courier_presence`, pinned equal to the hand-written `classify_presence` (kept verbatim in
the test) over all 36 pairs of answers. A pin steps a bsv-tracker `State` on the courier hints: a mempool presence
is `announced` over `built`, a mined claim is never `mined`.

1. The evidence check (S6, S7): `admit_fast::evidence_check`, `verdict_of_look`, `evidence_verdict_of`,
   `never_refused` read the couriers' hints; `evidence_verdict` stays the tables' bool entrance. Pin
   `l3_slice1_the_evidence_check_reads_the_couriers_hints`.
2. The ladder's terminal judgement (S4): `broadcaster::terminal_presence_fresh`. Pin
   `l3_slice2_the_ladders_terminal_read_folds_the_couriers_hints`.
3. The retire pass (S9, S12, S13): `retire_evidence` (`RetireEvidence`'s fields are `CourierWord`s),
   `retire_verdict_of`, `CourierConflictPresence`, `rebroadcast_absent_admitted`; `classify_presence` delegates to
   the host's fold. Pin `l3_slice3_the_retire_pass_reads_the_couriers_hints`, which also holds that no production
   fold of the couriers in `proof_fetcher.rs`, `admit_fast.rs` or `broadcaster.rs` calls `classify_presence`.

No recorded answer moved: `door_answer_table`, `arcade_vector_replay`, the #513 / #519 / #602 / #740 / #765 / #831 /
#836 / #845 pins pass unchanged. One log line's text moves (the retire backlog's `no verdict on the fresh look
({evidence:?})`: its courier fields print as `CourierWord`s, where they printed `Some(true)` / `Some(false)`).

## Left, with file and line (this tip)

- S2 (ARC) and S3: `broadcaster.rs:392 arc_verdict`, `:549 corroborator_verdict`, `:1169 arcade_status_rank` (the
  finding above: five recorded answers would move).
- S2 (Arcade's HTTP 400 body): `broadcaster.rs:1569 classify_submit_response`: no tracker type carries an HTTP code.
- S4, S6, S9, S12 as the design writes them (an eviction only on `conflicted` / `rejected`, a mined height only from
  `Headers::check`): each changes an admission or eviction outcome and a D1 row; C2's own lens.
- S16 to S19: wire or served-state changes (`proof_fetcher.rs:2169`, `arcade_reorg.rs:380`, `reorg_sweep.rs`,
  `tip_pass.rs:31`).
- The pin is bsv-tracker `=0.7.0`. 0.8.0 is bsv-rs 0.8.0 and nothing else ("no verdict, no word and no re-ask
  moves", its CHANGELOG); both workspaces are on bsv-rs `=0.7.2` with the middleware's floor `^0.7.0`, so the move is
  a repin lane of its own (bsv-rs, the middleware, both locks), not this one. `diff -r` of the two crates' `src/`
  is empty: every type and rule used here is 0.8.0's.
