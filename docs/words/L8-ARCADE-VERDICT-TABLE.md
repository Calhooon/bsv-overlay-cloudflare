# L8: Arcade's words, the door's reading against bsv-tracker 0.6.0

bsv-low #624 (L8), lane FIX-624-ARCADE-VERDICT, written 2026-10-19T22:52Z on
the overlay's main `ad510b9` (bsv-rs 0.7.0, bsv-tracker `=0.6.0` on both
Workers, the stack's W8e).

## What moves

Before L8 the door had its own parser of Arcade's transaction words
(`overlay_discovery::arcade_words::ArcadeVerdict`, nine classes, through
`arcade_verdict`), and fed that into the tracker's `Hint` by hand
(`door_words::arcade_hint`, #619 L3). After L8 the tracker does the parsing:
`door_words::arcade_hint` hands the status word to
`Verdict::from_arc_body` (the vocabulary, the trim, the letter case and the
metadata bounds) and `Verdict::into_hint` (the hint tier). `ArcadeVerdict` is
deleted. The door's POLICY over the hint (`gate`, `callback`, `refusal`,
`live_look`, `proof_class`) stays host code. This is #619's rule: admission
is host policy over hints.

The door reads the STATUS WORD only, as it always has. #213 says no gate
reads `extraInfo`. The adapter hands the tracker the word's own body
(`{"txStatus": <word>}`), so it never reads a body's `blockHeight`,
`merklePath`, `competingTxs` or `txid`. Their own readers handle those: the
proof look checks the path against chaintracks, the conflict pass of #602
handles competitors, and the submit classifier handles the txid echo. The
reason text (`extraInfo`) is attached to `Verdict::reason` after the word is
read, so its bounds can never unread a word.

The host keeps three readings that `HintStatus` does not carry
(`door_words.rs`):

- `arcade_speaks`: the tracker knows more words than Arcade defines (it reads
  ARC too). A word Arcade does not define is no input, with one exception:
  the orphan view (SEEN_IN_ORPHAN_MEMPOOL). The host's list of Arcade's words,
  `ARCADE_WORDS`, is the twelve of `[SRC] arcade@1ae1208
  models/transaction.go:89-126`. It is pinned equal to the vector's `words` by
  `arcade_words_are_the_vectors_twelve`. The list says which words this
  SOURCE speaks. It does not parse anything.
- `in_a_block`: STUMP_PROCESSING. The tracker reads it as `Accepted`; the
  gate's bar holds it.
- `mined_without_height`: MINED / IMMUTABLE with no height. The tracker reads
  it as `Unknown` and keeps the word on `verdict.status`. The host reads it as
  Arcade's mined claim, never as a chain word.

## The table

The columns:

- **door today**: `ArcadeVerdict` on base `ad510b9`, then the door's answers
  (gate / `/arc-ingest` callback / retire and evidence live look / proof
  look).
- **tracker 0.6.0**: `from_arc_body` then `into_hint` (`Err` means no hint).
- **after L8**: the door's answers on this branch.
- **rule**: the register's rule for any difference.

`=` in the last column means the answers are byte-identical to the base on
every surface of the answer table (`door_answer_table.rs`, 1,025 recorded
answers).

### Arcade's twelve words (any letter case, surrounding spaces)

| word | door today | tracker 0.6.0 | after L8 | rule |
|---|---|---|---|---|
| UNKNOWN | Parked: Pending / Ignore / Present / Other | `Unknown` | = | same answer |
| RECEIVED, SENT_TO_NETWORK, ACCEPTED_BY_NETWORK | Pending: Pending / Ignore / Present / Other | `Accepted` | = | same answer; D1: a pre-SEEN rank never admits and never refuses |
| SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES | Seen: Reached / LatchSeen / Present / Seen | `Seen` | = | same answer |
| DOUBLE_SPEND_ATTEMPTED | Conflict: Fatal / EvidenceCheck / Fatal / Other | `DoubleSpend { competitors: [] }` | = | same answer; the refusal is a HINT, corroborated by the two-source bar (D1, #214) |
| REJECTED | Rejected: Fatal / EvidenceCheck / Fatal / Other | `Rejected { reason }` | = | same answer; as above |
| PENDING_RETRY | Parked: Pending / Ignore / Present / Other | `Accepted` (K3 closed at 0.4.1) | = | same answer |
| **STUMP_PROCESSING** | InBlock: Reached / LatchSeen / Present / Other | `Accepted` | = (host `in_a_block`) | **DECISION ROW**, below |
| **MINED, IMMUTABLE (word only, no height)** | Mined: Reached / LatchSeen / Present / Mined | `Unknown`, word on `verdict.status` | = (host `mined_without_height`) | **DECISION ROW**, below |
| lower case (`seen_on_network`, `rejected`, `pending_retry`, `Double_Spend_Attempted`) | as the upper-case word | trimmed, upper-cased, as the upper-case word | = | same answer |
| surrounding spaces (` stump_processing `) | as the word | trimmed (0.4.1) | = | same answer |
| ` MINED\n` (a control character) | Mined: Reached / LatchSeen / Present / Mined | `Err(InvalidMetadata)` | no input: Pending / Ignore / Fault / Other | VOCABULARY ROW: the tracker is right, below |

### Words Arcade does not define

| word | door today | tracker 0.6.0 | after L8 | rule |
|---|---|---|---|---|
| SEEN_IN_ORPHAN_MEMPOOL (ARC's, carried for the ORPHAN rule) | Orphan: Orphan / EvidenceCheck / Present / Other | `OrphanMempool` | = | same answer; #267 |
| `orphaned`, `ORPHAN` | Orphan (the vector's "any word containing ORPHAN") | `Err(UnknownStatus)` | no input: Pending / Ignore / Fault / Other | VOCABULARY ROW: the tracker is right, below |
| STORED, QUEUED, SUCCESS, ANNOUNCED_TO_NETWORK, REQUESTED_BY_NETWORK | Invalid: no input | `Accepted` | = (host `arcade_speaks`) | **DECISION ROW**, below |
| INVALID, MALFORMED | Invalid: no input | `Rejected { reason: word }` | = (host `arcade_speaks`) | **DECISION ROW**, below |
| UTXO_SPENT | Invalid: no input | `DoubleSpend { competitors: [] }` | = (host `arcade_speaks`) | **DECISION ROW**, below |
| CONFIRMED (the word has no height) | Invalid: no input | `Unknown` (`Mined { height }` with one) | = (host `arcade_speaks`) | **DECISION ROW**, below |
| MINED_IN_STALE_BLOCK | Invalid: no input | `StaleBlock` | = (host `arcade_speaks`) | **DECISION ROW**, below |
| REORG_UNMINED (as a status word) | Invalid: no input | `Err(UnknownStatus)` | = | same answer |
| NOT_A_STATUS, empty, blank | Invalid: no input | `Err` | = | same answer; the vector's `invalid` rule |

### Body shapes (the nine bodies of bsv-stack-lean #169, and the vector's bodies)

The door reads the word, so a body's other fields never reach the hint. Each
row below gives the tracker's reading of the WHOLE body
(`door_words::tests::the_doors_policy_differs_from_the_bare_hint_exactly_here`).

| body | door today | tracker 0.6.0, whole body | after L8 | rule |
|---|---|---|---|---|
| MINED / IMMUTABLE with `blockHeight` (and `merklePath`) | Mined: Reached; the proof look checks the path | `Mined { height }` | = | same answer; the height is a claim, the path is checked against chaintracks (NS-3) |
| SEEN_MULTIPLE_NODES with a stale `ORPHAN` `extraInfo` (#213) | Seen: Reached | `Seen` (0.4.1: only the word moves the status) | = | same answer |
| SEEN with `competingTxs` | Seen: Reached | `DoubleSpend { competitors }` | = (the word only) | **DECISION ROW**, below |
| ` SEEN_ON_NETWORK ` | Seen | `Seen` (0.4.1 trims) | = | same answer |
| INVALID, UTXO_SPENT | Invalid | `Rejected`, `DoubleSpend` | = | the decision row "words Arcade does not define" |
| CONFIRMED with `blockHeight` | Invalid | `Mined { height }` | = | as above |
| REJECTED 466 with `competingTxs` (vector `status_rejected_466_conflict`, `push_rejected_466_conflict`) | Rejected: Fatal / EvidenceCheck | `Rejected` (the competitor override applies only to accepting words) | = | same answer; #602 names the competitor |
| SEEN with `extraInfo` `reorg_unmined`; MINED with `reorg_reanchor` | Seen / Mined; the marker is read by `arcade_reorg_marker` | `Seen` / `Mined` (0.4.1: `extraInfo` is text) | = | same answer; the marker is a re-ask (vector `reorg`) |
| the vector's submit / status / push bodies, all 75 engine checks | as recorded | | = | `arcade_vector_replay::every_engine_column_of_the_arcade_vector_holds`, green |

## Decision rows (the door's behaviour kept behind the tracker's reading)

1. **STUMP_PROCESSING.** Kept: the door. The tracker reads `Accepted`; the
   door reads the gate's bar (Reached; LatchSeen on a push, which still asks
   Arcade LIVE before it latches). Rule: D1, which admits on SEEN or better.
   Arcade's STUMP_PROCESSING means "in a block, the BUMP not built"
   (`models/transaction.go:108-109`). The vector's `engine_gate` /
   `engine_callback` columns record `reached` / `latch_seen`
   (`submit_202_stump_processing`, `push_stump_processing`). The keeper's #169
   reading: the door's readings that differ are its policy over the hint.
2. **MINED / IMMUTABLE with no height.** Kept: the door. The tracker reads
   `Unknown`; the door reads Reached / LatchSeen / proof look `Mined`. Rule:
   D1 (a mined claim is SEEN or better) and NS-3 (`mined` needs a merkle path
   checked against the CHAINTRACKS header). The proof look's `Mined` is a
   candidate its caller checks; the word writes no chain word (pinned in
   `the_hint_carries_the_word_and_the_reason_never_a_chain_word`). The vector
   records `reached` / `latch_seen` (`submit_202_mined`,
   `push_mined_without_path`). The tracker keeping `Unknown` is the stack's
   deliberate rule (#169), so the reading is host policy.
3. **SEEN with `competingTxs`.** Kept: the door. The tracker's whole-body
   reading is `DoubleSpend`; the door reads the status word only, so Reached.
   Rule: #213 (no gate reads anything but the status word) and #602 (a
   competitor evicts only as a content-addressed transaction with an
   indexer's positive mined height, found by the conflict pass). Also D1: a
   refusal needs the two-source bar, never a broadcaster's word alone. LOW
   has never met such a body from Arcade (Arcade names competitors on
   REJECTED 466). If it ever does, the conflict pass is the reader that acts.
4. **Words Arcade does not define** (ARC's STORED, QUEUED, SUCCESS,
   ANNOUNCED_TO_NETWORK, REQUESTED_BY_NETWORK, INVALID, MALFORMED, UTXO_SPENT,
   CONFIRMED, MINED_IN_STALE_BLOCK). Kept: the door. They are no input:
   gate Pending, callback Ignore, live look Fault. The tracker would answer
   `Accepted` / `Rejected` / `DoubleSpend` / `Unknown` / `StaleBlock`. With
   the tracker's reading, a stranger's `/arc-ingest` push of UTXO_SPENT would
   run the evidence check, and a live look of STORED would be Present (which
   holds off a retire or an eviction). Rule: the vector's `invalid` rule
   (P0-2d, #52: "a word Arcade does not define ... never a success and never a
   verdict on the transaction") and #740 / #765 (a word that is not Arcade's
   own is never read as Arcade's own; an undefined live word is fault-shaped).
   Implemented as `arcade_speaks` over the tracker's `verdict.status`.

## Vocabulary rows (the tracker is right and the register agrees)

These answers moved: 25 of the 1,025 recorded lines, pinned by name in
`door_answer_table.rs` `L8_MOVED`. None of the three inputs is a word Arcade
sends. They are not bodies LOW meets. No retire answer moved (`retire_absent_2h` /
`retire_absent_49h` read `None` for all three before and after), and no
latch moved: the callback of these words was never a latch.

- **` MINED\n`** (a status word with a control character). The tracker
  refuses it (`InvalidMetadata`), so it is no input. Before L8 the door read
  it as Mined. It moved on every surface: gate Reached to Pending, callback
  LatchSeen to Ignore, live look Present to Fault (and the fold, evidence and
  proof-look lines that follow from it). Arcade's JSON carries its Go enum
  strings; no control character comes from Arcade.
- **`orphaned`, `ORPHAN`.** Outside every broadcaster's vocabulary. The
  tracker refuses them (`UnknownStatus`), so they are no input. Before L8 the
  door read any word containing ORPHAN as the orphan view (the vector's
  `failure` clause). They moved: gate Orphan to Pending, callback
  EvidenceCheck to Ignore, live look Present to Fault. ARC's orphan word,
  SEEN_IN_ORPHAN_MEMPOOL, is unchanged.

The register agrees on two points. First, D1: no answer never admits and
never refuses (the ladder ends at the second broadcaster). Second, a live
look that is no answer is fault-shaped. #740 names "a FAULT or an undefined
live word" together, and #513 gives the rule for that shape: an
absence-based refusal needs both indexers definitively absent and an
admission at least 10 min old. A young or never-admitted subject stays
Uncertain. A refusal is never Arcade's own word, because the look holds no
word at all. One consequence follows: an AGED subject whose live word is one
of these three, with both indexers absent, now takes the absence-based
refusal where the old reading kept it (`evidence_absent_young_false`:
Present to `Refused("arcade faulted, both indexers absent")`). That is the
register's rule for a look Arcade did not answer. The stack's tracker charter
makes the vocabulary the stack's rule.

The vector file (`crates/overlay-discovery/vectors/arcade_status_verdicts.json`)
is the toolbox's bytes and is not edited here (its sha256 pin). Its `failure`
clause "any word containing ORPHAN" is wider than the tracker's vocabulary.
Every captured body in it reads the same after L8: all 75 engine checks pass,
and so does the twelve-word class pin. The clause is a follow-up for the
toolbox's copy.

## Outside this item

The app layer's `txany::parse_arcade_word` (SeenFresh / Mined / Unknown, the
`/tx-any` presence hint with its freshness window) is the app layer's own
reading. It is not `ArcadeVerdict` and it does not move here.
