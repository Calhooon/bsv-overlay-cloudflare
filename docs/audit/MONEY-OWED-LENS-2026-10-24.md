# MONEY-OWED-LENS: the one lens on overlay PR 168 (bsv-low #861, #770, #649)

Lane MONEY-OWED-LENS, started 2026-10-24 17:55 UTC (`date -u`), on `lane/MONEY-OWED-LENS` cut from PR 168's head
`f6c0b017` (one commit over `origin/main` `db3a0931`). Read whole: `git diff origin/main...HEAD` (CLAUDE.md +6,
`crates/low-app-layer/src/owed.rs` +361 -15, `crates/low-app-layer/src/routes.rs` +2), the `/owed` read handler
(`routes.rs:7002-7350`) and every helper it calls on a shed path, the client's parse and retry
(`bsv-low/app/src/lib/owed.ts`, `components/OwedList.tsx`), `admit_fast.rs`'s settle and its #649 pins.

## Verdict: SOUND

Nothing above LOW. #861 is the only behaviour change, it can only remove a press from a served body, it touches
nothing outside a shed read that serves a stored snapshot, and fresh and unshed bodies are byte-identical to main
(LENS-3). #770 is provably a refactor: `settle_spend_confirmed` is main's two boolean expressions, algebraically
and over an exhaustive grid (LENS-5). #649 is on main as `d7a60fc9`. Two LOWs and four NOTEs below, none a money
loss.

## The money questions

**1. How long can a shed HIDE an honest Collect, and does the row say so.** A read is shed on three arms, all in
`routes.rs` `owed` (the ceiling arm `owed_read_shed`, the claim admitted-refused arm, and the no-`wait` harness arm;
production always has `wait`, `lib.rs:439`). Each sets `refresh_retry` from `RecomputeRate::retry_after_ms`
(`owed.rs:5729`): the rest of the identity's #648 window, or the #800 hand-off's deadline (60 to 93 s past a
dropped walk), whichever is later; every shed arm also arms the window's reserved deferred walk. So a press is
withheld on THIS isolate for at most the window's remainder plus the alarm walk's 60 s deadline, about 120 s,
after which the alarm's landed walk writes fresh rows and pushes `owed-changed`; the next read is unstale and young
and is served unshed (`should_recompute` false), press included. A heavy identity whose alarm walk is dropped too
gets an admitted request-path walk after the hand-off, and that read serves the stored rows UNSHED (the stated
residual, below), so a press is never withheld across two cycles. The row says so: `claimable: false`,
`claimReason` `OWED_SHED_PRESS_REASON` (rendered by `OwedList.tsx:224-226`), `retryAfterMs` per row and at the
top; the client re-reads after `retryAfterMs` at most twice (`owed.ts:195-202`), then waits for the push. Money:
a delay, never a loss (the seat's home-page path, `UnfinishedBusiness`, does not read `/owed`'s `claimable`).

**2. Can a shed SHOW a press a fresh read withholds.** Not after the change: every served row whose `claimable`
is `true` is demoted on every shed arm (all three set `refresh_retry` and leave `served_snapshot_is_fresh` false;
`owed_read_answer` returns `fresh` only for the read's own landed walk, which a shed read never starts). The
"cached snapshot served after a transport fault" of #648: in production the compute runs in `wait_until` and a
fault there serves the stored rows exactly as the admitted-not-landed read does (unshed, stated in CLAUDE.md as
the residual); the `markerless_faulted_rows` arm (`routes.rs` `Err(_)`) is the no-`wait` harness arm only. The
reserved first read (`board_view.rs:211`) is a fresh `owed_compute_on_read`, never a cached snapshot. What still
SHOWS a stale stored press, unchanged and stated by the PR: a stale read whose claim another walk holds (coalesced)
and an admitted read whose walk does not land whole inside 2 s (NOTE-1).

**3. Is `claimableBeforeShed` display only.** Yes. Its only reader in either repository is `carry_shed_rows`
(`owed.rs:1913-1918`, the #837 carry of a STORED row, which never sees this body-only demotion). The client never
reads it (grep of `bsv-low/app/src`, `crates`, `scripts`, `tools`: no hit). No press, filing or monitor reads it.

**4. Fresh and unshed bodies byte-identical to main.** `demote_shed_presses` returns before touching a row when
`refresh_retry` is `None` or `fresh` is true (`owed.rs:1981-1987`); LENS-3 holds `owed_body`'s string equal with
and without the call for `(None, false)`, `(None, true)`, `(Some, true)` over one row of every press family, and
shows a shed body differs from main's only in the demoted rows' `claimable`, `claimReason`, `stale`, `refresh`,
`retryAfterMs` and the added `claimableBeforeShed`. `fix861_a` holds the same for one door strand.

## #770: the refactor

Main's settle payout `claimable` (f6c0b017^): `h || ipsm(i, s, l, l || h)` where `h = at_height.is_some()`,
`l = spent_confirmed == Some(true)`, `ipsm(i, s, p, g) = p || spender_mined(i, s, g)`; so `h || l ||
spender_mined(i, s, l || h)`. The new `settle_spend_confirmed` (`owed.rs:2893`): `ipsm(i, s, h || l, h || l)` =
`h || l || spender_mined(i, s, h || l)`. Identical. Main's `awaiting_block` evidence half: `!h && !(l ||
spender_mined(i, s, l || h))` = `!(h || l || spender_mined(..))` = `!settle_spend_confirmed`. Identical. LENS-5
checks both over 96 evidence combinations (latch unread / false / true, height none / some, settle txid none /
some, no word map, a map with no row, checked `mined`, unchecked `mined`, `cannotCheck`, `stale`, `seen`,
`rejected`): equal everywhere. "No behaviour change" is honest.

`fix770_a` is a CROSS-ARM parity pin (settle against the index-named hop arm), green on main by construction, and
the PR says so. It cannot be RED on main; its REDs are against mutants (the pre-#736 word-first rule, the latch
dropped from `awaiting_block`), which is what a parity pin can offer. Its grid covers latch with and without
height, checked and unchecked `mined`, `stale`, `cannotCheck`, `seen`, `rejected`, no map and a map with no row
(the word store's fault answers no map). Not in it, by construction: a courier's `spentConfirmed` (the settle has
no chain rung, so there is nothing to compare; the hop arm's courier-yields-to-the-word rule is #736's own pins'),
and the latch AND a height together (trivially confirmed in both arms). LENS-5 is the pin that would go RED on a
change to the function itself; `fix770_a` would go RED only if the two arms diverged.

## #649

`d7a60fc9` ("#649: count and log an eviction before its flush, on both settle paths") is an ancestor of
`origin/main` (`git merge-base --is-ancestor`). `e649_a_c` (`admit_fast.rs:5763`) polls the SHIPPED
`settle_corroborated_refusal` (`admit_fast.rs:2756`) over a port fake whose flush never answers and asserts memo,
count and log land before the flush, for both counters, and nothing over a yielded pass; the jobs' wiring (both
call the settle after `evict_txid_everywhere`, the refused arm flushes, counts and memos only through it) is a
SOURCE pin. So the pin drives the real settle function and its order, not the eviction itself (NOTE-4). The RED
the PR cites (the flush moved first) is a mutant of that function; not re-run here.

## Findings

**LOW-1. The scope pin models a rejoin as "no press"; the derivation's rejoin IS a press and is withheld.**
`owed.rs:15622` (`fix861_b`): `row("d:0", OwedFamily::InProgress, json!({"claim": "rejoin"}))`, asserted "no
press, or already shed: as stored". The derivation writes `claimable: true` on every rejoin (`owed.rs:3708-3709`,
`4101-4102`), so on a shed read a live seat's Rejoin is demoted (LENS-1: payout, refund, rejoin and door strand
all withheld). CLAUDE.md's sentence ("every served row whose `claimable` is `true`") is accurate; the pin's
fixture is not the shipped shape. Money: none (the seat's hand is reached from the table and home, and the rejoin
returns with the next walk, at most about 120 s); the risk is a reader of the pin believing in-progress rows are
spared. Fix shape: give `d:0` `"claimable": true` and assert it demoted, or state in the pin and CLAUDE.md that a
rejoin is withheld too.

**LOW-2. The demotion reads a literal `true`; the client reads an ABSENT `claimable` as a press.**
`owed.rs:1990` (`r.facts["claimable"] != Value::Bool(true)` skips) against `bsv-low/app/src/lib/owed.ts:107`
(`claimable: facts.claimable !== false`). A stored row with a claim and no `claimable` would be served as a press
on a shed read. LENS-2 is RED on the branch ("the client still reads a press: Object {"claim":
String("internalize")}"). Not reachable today: every non-null claim the derivation writes sets `claimable`
(`owed.rs:3554/3562`, `3649-3650`, `3675-3676`, `3708-3709`, `3861/3863`, `4101-4102`, `4217-4218`, `4307-4308`).
Money: none today. Fix shape: demote on `claim` non-null and `claimable != false` (the client's own parse), so a
future row shape cannot slip past.

## NOTEs

- **NOTE-1 (stated by the PR, reach named).** #861 protects SHED reads only. On a heavy identity (walks of 12 to 26
  s) the reads alternate between shed (demoted) and admitted-not-landed (stored rows served unshed, presses
  included), so the issue's stale door strand can still be offered on an admitted read, for at most that walk's
  life (it lands and pushes). #861 shortens the stale press's window from the shed cycle to one walk; it does not
  close it.
- **NOTE-2.** The demotion hides the badge's count during a shed (`useOwedBadge.ts:31` counts `claimable` rows):
  display only.
- **NOTE-3.** A walk that lands cut (`budget_cut`) inside the read's 2 s is served as the stored snapshot, unshed;
  unreachable in practice (the outward budget is 12 s), the same class as NOTE-1.
- **NOTE-4.** `e649_a_c` drives the shipped settle over a fake port, the wiring by source shape; no pin executes
  `pending_watch_job` or `refusal_job` end to end with a hanging flush.

## The lens's cases (worktree only, not pushed to the PR's branch)

Appended to `owed.rs`'s tests module; the patch is kept at `/Volumes/Lanes/out/MONEY-OWED-LENS/lens-tests.patch`
(220 lines) and its five tests are summarised here:

| case | what it holds | result on the branch |
|---|---|---|
| `money_owed_lens_1_every_derived_press_family_is_withheld_under_a_shed` | payout, refund-due, in-progress rejoin and door strand, each as the derivation writes it (`claimable: true`), all demoted with `claimableBeforeShed`, `retryAfterMs`, `claimReason` | ok (LOW-1's evidence) |
| `money_owed_lens_2_a_claim_with_no_claimable_word_is_withheld_as_the_client_reads_it` | a claim with no `claimable` is no client press after a shed demotion | FAILED: `the client still reads a press: Object {"claim": String("internalize")}` (LOW-2) |
| `money_owed_lens_3_fresh_and_unshed_bodies_are_byte_identical_to_main` | `owed_body` string equal with and without the call on every non-shed input; a shed body differs only in the demoted facts | ok |
| `money_owed_lens_4_a_degraded_sweep_and_a_withheld_row_keep_their_own_words` | #669's degraded sweep and a `withheld` row are untouched by the demotion (0 demoted) | ok |
| `money_owed_lens_5_the_settle_rule_is_mains_two_expressions_over_every_evidence` | `settle_spend_confirmed` equals main's `claimable` and `awaiting_block` expressions over 96 combinations | ok |

Run: `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib money_owed_lens`: 4 passed, 1 failed
(LENS-2), as above. The tree was then restored (`git checkout crates/low-app-layer/src/owed.rs`) and the gates
below were run on the PR's own tree.

## Commands and exits

All under `CARGO_TARGET_DIR=/Volumes/Lanes/targets/MONEY-OWED-LENS`, the workspace toolchain.

- `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib fix861`: 2 passed, exit 0.
- `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib money_owed_lens` (the lens's cases, then
  reverted): 4 passed, 1 failed (LENS-2), exit 101.
- `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib` (the PR's tree): 703 passed, 0 failed, 2
  ignored, 627.41 s, exit 0.
- `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --tests`: 95 test binaries, 1,040 passed, 0
  failed, 2 ignored, exit 0.
- `cargo clippy --manifest-path workers/Cargo.toml -p low-app-layer --all-targets -- -D warnings`: no warning,
  exit 0.
- `rustfmt --edition 2021 --check` on `owed.rs` and `routes.rs`: exit 0 each, no diff.
- `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib e649`: 26 passed (972 filtered),
  exit 0.
- `git merge-base --is-ancestor d7a60fc9 origin/main`: exit 0.

The two long runs outlasted the harness's 10 minute foreground cap and the harness moved them to the background
on its own; each was awaited to its exit before the next command (no other cargo command ran beside them).

## Not verified

- The route tier (`make ci-route`, `ci-d1-budget`) and `make ci`: not run (the captain's).
- The RED claims of the PR's own pins (`fix861_a` on main, `fix770_a` against its mutants, `e649_a_c` with the
  flush first): read, not re-run.
- The shed's withhold bound on beta (the ~120 s is the code's arithmetic per isolate, not a measurement); a
  second isolate has its own window.
- The overlay crate's whole lib (997): only the `e649` pins were run.
