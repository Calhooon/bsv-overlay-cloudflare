# Archive: the E657 atomic-admission lanes of 2026-10-13 and 2026-10-14

Written 2026-10-31 by LOW's captain on the owner's word of 2026-10-31 16:2xZ (the stack's decisions page, item 22.7 (3): "agreed"; bsv-low record 1685). The ten worktrees `~/bsv/wt-overlay-atomic-*` are removed and their local branches deleted; every branch is on the remote and every worktree's tip carries an annotated tag `archive/atomic-admission-<name>`. Nothing of the design is lost; the trusted word (an unlanded branch is a claim about what it would do) became a record.

## The tags (the remote `origin`, this repository)

| tag | tip | what the worktree held |
|---|---|---|
| `archive/atomic-admission-spike` | ddf0c715 | 2026-10-13, 4 commits on 970aad12: the spike for bsv-low #657 (`docs/audit/2026-10-13-atomic-spike.md`): the local D1 batch rollback and the failing guard forms proven, the local D1 budget non-enforcement measured and the hosted blocker recorded, two ignored RED atomic-admission witnesses retained, the guard pins and witness fixtures finalized |
| `archive/atomic-admission-pr1` | b5dcede8 | the spike plus A's first engine PR (`docs/audit/2026-10-14-atomic-pr1.md`): the ordinary `Engine::submit` refactored into the atomic unit; 40 files that never reached a pull request (the engine's `engine/` and `storage/` modules, every topic manager's signature, `admission_submit.rs` and `admission_unit.rs`), committed whole at the archive |
| `archive/atomic-admission-a1` | 9d5a75a1 | A1 (`docs/audit/2026-10-14-atomic-a1.md`, 40 commits on de5cda76): the bounded walk split from an evidence-only atomic admission; a memory-log rollback, a one-poll guard and a committed outbox; the revision 4 witnesses RED on the keeper's main; then B's preflight commits over it |
| `archive/atomic-admission-a1-lens` | 89bcf8c0 | the lens on A1 at e5f5217 (`docs/audit/2026-10-14-atomic-a1-lens.md`): ACCEPT WITH CORRECTIONS |
| `archive/atomic-admission-a1-relens` | 5ff3dfdc | the re-lens after A1's eleven corrections C1 to C11 (`docs/audit/2026-10-14-atomic-a1-relens.md`): ACCEPT WITH CORRECTIONS |
| `archive/atomic-admission-b` | 4e9ba132 | B, the engine half on engine 0.1.1 (`docs/audit/2026-10-14-atomic-b.md`, 6 commits on 5fb28955): pure lookup-admission and spent-check hooks; every lookup payload preflighted before the topic apply; typed lookup refusals naming the service and the site; a refused topic isolated from its sibling's coins (`refused_topic_skips_apply_while_sibling_lands`) |
| `archive/atomic-admission-b-lens` | 6e69391e | the lens on B (`docs/audit/2026-10-14-atomic-b-lens.md`): ACCEPT WITH CORRECTIONS |
| `archive/atomic-admission-b-045` | d72b8036 | B rebased on bsv-rs 0.4.5 (`docs/audit/2026-10-14-atomic-b-045.md`, 16 commits on dc5b1710), its four uncommitted files (the engine, the GASP topic-manager tests, the audit page, the changelog) committed at the archive |
| `archive/atomic-admission-b-fix` | e2bcded6 | the fix worktree on the same branch point as b-045 with its own three uncommitted files (the engine, the GASP topic-manager tests, the audit page) committed on a new branch `atomic-admission-b-fix` at the archive |
| `archive/atomic-admission-b-fix-2` | bd6cdb47 | the second fix worktree, clean, at b-045's branch point before the archive commit |

## What E657 tried

bsv-low #657 (filed 2026-10-13): a lookup-service notification error after the apply left the admission applied with the old tip deleted and no undo, so the same bytes then failed on the replay. The atomic-admission program answered with an ATOMIC UNIT: the submit as one durable write, a memory log that rolls back on any fault, a committed outbox for the notifications, the lookup services' validation run as a PREFLIGHT before any topic applies (B), and typed refusals an operator can read. Two lenses accepted each half with corrections; the spike found the local D1 batch rollback usable and the hosted D1 budget unenforced locally (the blocker for a hosted proof).

## What Wave C and Wave E took instead

- The fault's cure (Wave C, 2026-10-24, overlay PR 155, bsv-low #657): a lookup-service notification fault is a fault BEFORE the transaction landed; the inserts are undone, the stale coin kept, the replay re-applies whole (bsv-low's register row D17, the five rules of e1c and e1d: inserts before deletes, the undo only when no delete was started and every stale coin reads back held, one applied row per transaction, the bounded predecessor question, no retraction of a notification on an undo).
- The write's durability by the queue, not by an atomic unit: S2's queue-durable admission (the replay carrying the BEEF whole, inline or by its R2 key), D17's one head and never a dropped submit (#559, #575), FIX-791-C's named inputs (#791), the walk under the engine's own budget (#592).
- The owed write path by the actor (D2-ACTOR, Wave D and E): the judge as a Durable Object per identity on queue frames, the triggers marking the writers' pairs; nothing of the admission's atomic unit was needed for it.

## Still wanted, filed as queue rows (bsv-low, Wave G)

- bsv-low #1022: the lookup-service preflight before any topic apply, with typed refusals naming the service and the site (B's shape).
- bsv-low #1023: the admission write's committed outbox and memory-log rollback (A1's shape), read against S2 and D17 before any lane.

Everything else in these branches is superseded by the rows above and stays readable at its tag.
