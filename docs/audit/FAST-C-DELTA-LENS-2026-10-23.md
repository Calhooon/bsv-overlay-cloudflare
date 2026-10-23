# FAST-C-DELTA-LENS: the delta lens on overlay PR 151 after its fold and rebase (bsv-low #848, FAST-C)

Stamp: 2026-10-23T20:44:34Z (`date -u`, the lane's start). Lens branch `lane/FAST-C-DELTA-LENS`, worktree at the PR
head `c106fd32` (`lane/FAST-C`), main at `f809940a` (merge base `65027163`, PR 152). The second and last lens on this
seam. The first lens (`FAST-C-LENS-2026-10-23.md`, on `754f8997`) was NOT SOUND: one MEDIUM and five LOWs. This lens
reads the FOLD (`adacf538`, its RED pins `9ddc9669`) and the REBASE (`fbba4018` over `3ca0c20c`; the count pin
`c106fd32`; the carried audit `c01e623a`) against those six findings, and the new paths the fold opens.

## Verdict: LAND (nothing above LOW; the fold answers the first lens)

Each of the six findings is answered as stated, and each fold rule has a pin that is RED with that rule alone
reverted (four reverted locally in this lane, below). The rebase moved nothing but the migration numbers and their
pins. The fold opens one new path, the idempotent re-filing, and it gives a stranger no lever: a replay of an honest
filer's batch refreshes no cap window (the rows' `createdAt` is never rewritten), recomputes nothing when it writes
nothing, and the one case in which it still recomputes (a refund promoted from rank 0 to rank 1) is a once-only
truth about the honest seat's own refund that the direct `potrefund` door already allows. Two NOTEs on the fold are
stated for the record; no code change is asked.

## The six findings, answered

| the first lens's finding | the fold | answered? | the pin, and its RED |
|---|---|---|---|
| MEDIUM-1: a re-filing of the same batch, or a handfile after a direct v1 filing, is refused 409 by the potparty cap's pending arithmetic | `handfile.rs:548-562` `POTPARTY_HANDFILE_ROWS_SQL` counts the pair ONCE with both active content keys excluded (`txid NOT IN (?4, ?5)`); `handfile_cap_queries` (`handfile.rs:635-728`) reserves the pair's active contents together (`other_rows + contents > cap`); the day's count projects the state AFTER the write (`COUNT(*) + missing contents`, `> 200`), so a landed row counts once and an old one not at all | yes: a re-file of a landed batch reads 0 other rows and 0 missing contents; a direct v1 then the handfile reads 0 and 1; the day's 200 is exactly the direct door's `>= 200` for one content | `lens_b`, `fold_m1_direct_versions_and_daily_boundary_count_each_content_once`, `fold_m1_chain_noops_*`, `lens_d`. Mutant A (the v1 key no longer excluded): `lens_b` `left: Some(("potparty", TooManyFiled))`, `fold_m1_direct_versions` `left: Some(TooManyFiled) right: None`, `lens_d` the same |
| LOW-1: the doc's refusal order was not the code's | `docs/HANDFILE-WIRE.md` §4 rewritten: the raw body cap, the shape and size checks, the DER gate, the parsers, consistency, poster, then the two ECDSA checks; the code (`verify_handfile`, `handfile.rs:350-427`) is unchanged | yes, read against the code step by step | `fold_l1_der_then_parsers_then_consistency_then_poster_then_signatures` (a payload of one byte with a non-DER identity signature is `SignatureInvalid`; a stranger with a bad payload is `HandfilePayloadNotAMarker`, then `HandfileInconsistent`, then `PosterMismatch`; canonical signatures over another digest reach ECDSA only after those) |
| LOW-2: a no-op re-file still marked stale and recomputed | `handfile.rs:950-973`: `wrote` is `any` statement of the batch with a non-zero `changes` (unreadable = written, the direct door's rule); a no-op answers `filed: false, alreadyFiled: true` with the same key and rows and returns BEFORE `note_filing_and_recompute`; the refund latch (`POTREFUND_LATCH_SQL`, guarded `COALESCE(refundValid, 0) = 0`) is the one statement a retry can still change | yes | `fold_l2_noop_refile_and_refund_promotion_have_different_write_outcomes` (the shipped statements under real SQLite: the second explode is 0 changes by `total_changes`, which counts triggers too, stricter than D1's per-statement `changes`; a promotion is a change; a second promotion is 0). The route's `if !wrote` branch itself has no route-level pin (NOTE 4) |
| LOW-3: the body cap met a refund raw the direct door admits | `record_post.rs:143` `RECORD_POST_MAX_BODY_BYTES = RECORD_POST_MAX_SCRIPT_BYTES * 2 + 2_048` (203,072; it was `+ 64`), shared with the direct door, whose own hex cap (`record_post.rs:1933`) is unchanged | yes: a 100,000-byte refund raw in a handfile body is 201,657 bytes, under the cap; the direct door gains 1,984 bytes of JSON room and nothing else | `lens_f_the_body_cap_admits_a_maximal_refund_the_direct_door_admits`. Mutant C (`+ 64` back): `201657 > 201088` |
| LOW-4: the hop row's cap was per (poster, game, hop outpoint) | `HOPPARTY_FILED_ROWS_SQL` (`handfile.rs:542-543`) drops `gameId`: 1 filed hop row per (poster, hop outpoint) across games | yes; and it changes no honest answer (NOTE 1) | `lens_d` (another game's row is counted, the batch refused `hopparty`), `fastc_g`. Mutant LOW-4 (the other game's row not counted): both RED, `left: 0 right: 1` |
| LOW-5: a cap refusal did not name the family | `record_post.rs:1868-1889` `refuse_with_family` / `record_refusal_body`: the existing `error` text plus `family`; the direct door's bodies unchanged (`family` absent) | yes | `fold_l5_cap_bodies_name_each_family_and_keep_the_direct_error_shape`. Mutant D (the field dropped): `left: Null right: "hopparty"` |

The RED pins commit (`9ddc9669`) carried the six lens cases unchanged onto the implementation; `lens_b` alone failed
there with the first lens's words (`Some(TooManyFiled)` against `None`), the fold's own RED-first record.

## The new paths the fold opens

**A stranger replaying an honest filer's batch.** The door's identity is the session's, or for an anonymous caller
the `?identity=` claim under the lenient mode (`auth::resolve_view_identity`, `routes::view_identity`); the poster
must equal the identity every payload names (403), and the two signatures must verify under that identity and its
settle key, so a stranger can post only bytes the honest seat signed, which the lookup services serve to anyone as
the row's `batch` (D11's design: a filing is a public signed record, idempotent by content). What such a replay buys:

- No cap window refresh. Every family write is `INSERT OR IGNORE` on the content key; `createdAt` is never
  rewritten; the day's count reads the rows' own stamps (`fold_m1`: a landed row older than the day adds nothing).
- No recompute on a no-op (LOW-2 above): the honest identity's #499 ceiling is not spent, nothing is marked stale,
  no `owed-changed` is pushed. The B3 lens fold's rule (`record_post.rs:2155-2164`) now holds at both doors.
- One recompute when the replay PROMOTES the refund: the pot was unindexed at the first filing (rank 0), the index
  holds it now, and `bind_refund_backup` binds the honest refund at rank 1, so the latch is a change and
  `note_filing_and_recompute` walks the POSTER, the honest seat (#648: a filing's recompute walks the filer only;
  here the filer IS the honest identity, since the poster must be the payloads' identity). That is the truth about
  the seat's own pre-signed refund arriving a little earlier than the seat's own retry would bring it, once per batch
  (the second replay is a no-op), and the direct `potrefund` door has the same promotion path for a replayed public
  `scriptHex` (`record_post.rs:2054-2086`). Not a lever: bounded to one walk per batch per promotion, of the identity
  whose evidence it is. Stated.
- No oracle beyond public data. A stranger holds a batch only from a served row, where it is already public; the
  answer's `alreadyFiled` tells them what the lookup service told them. A 200 for a replayed batch against a 4xx for
  a fresh one needs a fresh batch, which needs both keys.

**The chain no-op slots and the pair count.** An inactive slot (its family's verified chain marker already indexed
for this identity, game and pot) binds an empty key, which `NOT IN (?4, '')` and `'' <> ''` read as "no content" (the
SQLite check below); its landed filed row, if any, then counts as another content, which is the cap's meaning (a
third potparty content for one pot refuses). Pinned by `fold_m1_chain_noops_reserve_only_active_slots_*`.

## NOTEs, stated (no change asked)

1. **LOW-4's fold is unreachable for an honest filer.** The settle key is `[2,'low settle']` with `keyID = gameId`
   and `counterparty = opponent` (bsv-low `app/src/lib/potParty.ts:404-420`, `hopParty.ts:77`), so a hop output pays
   hash160 of ONE game's settle key, and a handfile naming another game over that hop fails `hop_facts_agree`
   (`handfile.rs:518-522`: the output at `hopVout` must be the P2PKH of the batch's settle key) at step 5, the 422
   `hop_marker_refuted`, before the caps at step 7. The across-games cap therefore bites no honest rejoin: the only
   second row for one hop outpoint by one poster whose facts agree is one that names the same game and settle key with
   another sats figure, which the facts refuse too. It costs nothing and the per-game count it replaced bounded
   nothing either (the first lens's "N games name N rows" needed N settle keys the hop does not pay).
2. **The fold binds integers as TEXT and relies on column affinity.** `handfile_cap_queries` binds every parameter as a
   string (`day_floor.to_string()`, `hop_vout.to_string()`, `hop_row_vout.to_string()`), where the pre-fold route bound
   `js_num`. The compared columns are declared `INTEGER` (`createdAt INTEGER`, `hopVout INTEGER NOT NULL`, `outputIndex
   INTEGER NOT NULL` in `d1/mod.rs:1067-1096, 1333-1348`), so SQLite applies numeric affinity to the TEXT operand before
   the comparison (checked with sqlite3 3.51.0: `createdAt >= '1001'` is 0 and `>= '999'` is 1 on a row at 1000;
   `outputIndex = '2147483649'` is 1; the day projection's `('' <> '' AND NOT EXISTS ..)` is 0 and the missing content's
   is 1). The #565 lesson (`record_post.rs:3780-3782`: a TEXT `'1'` never equalled an INTEGER expression) is about a
   comparison against an EXPRESSION, which has no affinity; none of the fold's comparisons is one. D1 is SQLite and the
   lens cases run the shipped statements with the same string binds under rusqlite. A future statement that compares a
   TEXT bind with an expression would not be caught by this; the prepare pin (`sql_prepares_sqlite.rs`) checks
   preparation, not affinity.
3. **The no-op answer's `refundValid` is this request's bind, not the row's.** `handfile_answer(&verified,
   refund_valid, false)` carries the rank this request computed. If the stored row is at rank 1 and this request binds
   0 (the pot left `pot_records` since, an eviction), the no-op answer says 0 while the row holds 1. Display only: the
   direct door's `already_filed_body` carries no `refundValid` at all, and `/refund-backups` serves the row's.
4. **The route's `wrote` gate has no route-level pin.** `fold_l2` pins the statements' change counts and the answer
   helper; the `if !wrote` branch (`handfile.rs:958`) is read, not run (the route needs a worker env). The direct
   door's equivalent (`record_post.rs:2164`) is in the same position. A mutant `let wrote = true;` is not caught by a
   test; its effect is the pre-fold recompute on a no-op, bounded by #499.
5. `HOPPARTY_FILED_TODAY_SQL` (`handfile.rs:545-546`) is no longer the route's statement (the day's count is
   `HOPPARTY_HANDFILE_DAY_SQL`); it stays in the prepare pin and `lens_d`. Dead on the production path; cosmetic.
6. The fold's commit (`adacf538`) was written by a Codex lane (its trailer); the first lens's cases are kept
   verbatim beside the fold's.

## The rebase

- `fbba4018` against `754f8997`: `git diff <sha>^..<sha>` of the two implementation commits, `d1/mod.rs` excluded,
  differs only in blob index lines; `d1/mod.rs`'s patch differs only in one hunk offset (`@@ -3019` to `@@ -3022`,
  PR 152's three test lines). Nothing else moved.
- `d1/mod.rs:2291-2295`: 346 `crate::conflict_repair::REPAIR_CREATE` (PR 152), 347 `HANDFILE_BATCHES_CREATE`, 348
  `HANDFILE_ROWS_CREATE`; `OVERLAY_MIGRATION_COUNT` 348 (`d1/mod.rs:620`), its doc line "346 to 348".
- The fingerprint, printed through a throwaway `eprintln!` graft in `migration_fingerprint_moves_on_content_*`
  (restored, not committed): `FINGERPRINT=3923701813 COUNT=348`, the last statement `CREATE TABLE IF NOT EXISTS
  handfile_rows (...)`. The pin asserts determinism and non-zero, not a frozen value; the brief's figure holds.
- `tests/e602_convergence.rs:779-782`: `OVERLAY_MIGRATIONS.len() == 348` and `OVERLAY_MIGRATIONS[345] == REPAIR_CREATE`
  (the count pin `c106fd32`).
- The two CREATEs are byte-identical between `d1/mod.rs:623,625` and `handfile.rs:527,529` (diffed); the app layer's
  `CREATE_TABLE_CATCHUPS` (`schema.rs:294-298`) carries both, the pin asserts 16 and matches each to its owning
  migration (`every_create_table_catchup_is_byte_identical_to_its_overlay_migration`, in the lib suite).
- `make ownership`: 1302 references match (the fold's four new statements granted).

## Commands and exits (`CARGO_TARGET_DIR=~/bsv/targets/wt-overlay-FAST-C-DELTA-LENS`, rustc 1.95.0, the tree at `c106fd32`)

| command | exit | result |
|---|---|---|
| `make ownership` | 0 | self-test 39 ok; 1302 SQL references in 3 crates match |
| `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib handfile` | 0 | 19 passed (8 `fastc_*`, 6 `lens_*`, 5 `fold_*`) |
| `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib` | 0 | 696 tests listed by `--list`; the run passed (its summary line was lost to the lane's own filter; the harness parked the run past 600 s and it exited 0) |
| mutant A: `POTPARTY_HANDFILE_ROWS_SQL` `txid <> ?5` (the v1 key counted) | 101 | `lens_b`, `fold_m1_direct_versions_*`, `lens_d` FAILED as above; restored |
| mutant C: `RECORD_POST_MAX_BODY_BYTES` `+ 64` | 101 | `lens_f` FAILED `201657 > 201088`; restored |
| mutant D: `record_refusal_body` drops `family` | 101 | `fold_l5` FAILED `left: Null right: "hopparty"`; restored |
| mutant LOW-4: `HOPPARTY_FILED_ROWS_SQL ... AND gameId <> gameId` (another game's row not counted) | 101 | `lens_d`, `fastc_g` FAILED `left: 0 right: 1`; restored |
| `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib -- --nocapture migration d1::tests::every_create` (the fingerprint graft, restored) | 0 | 20 passed; `FINGERPRINT=3923701813 COUNT=348` |
| `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib -- handfile catchup catch_up relatch fastc` | 0 | 14 passed (`fastc_a_door_filed_row_keeps_its_verdict_through_the_relatch_sweep` among them) |
| `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib` | 0 | 971 passed, 0 failed, 1 ignored (130.9 s) |
| `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --test e602_convergence` | 0 | 10 passed, `e607_append_only_migration_and_the_page_uses_the_rowid_range` among them |
| `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --test <19 names>` twice (the 38 binaries in two halves; a first attempt passed the names as one word, zsh's `$VAR`, and ran them as filters: 0 tests, discarded) | 0, 0 | 19 binaries 177 passed; 19 binaries 160 passed; 0 failed (the lib's 696 are above) |
| `cargo clippy --manifest-path workers/Cargo.toml --workspace --all-targets -- -D warnings` | 0 | clean |
| `cargo clippy --workspace --all-targets --features bsv-overlay-engine/memory-storage -- -D warnings` (`CARGO_TARGET_DIR=...-root`) | 0 | clean |
| `cargo test --workspace --features bsv-overlay-engine/memory-storage` (root, `CARGO_TARGET_DIR=...-root`) | 0 | 101 binaries, 1731 passed, 0 failed, 66 ignored (one run, no retry needed) |
| sqlite3 3.51.0 affinity check (NOTE 2) | 0 | the seven expected values |

Not run (the captain's, at the land): `make ci-route`, `make ci-d1-budget`, the wasm32 builds, `make ci-deploy`.
Nothing deployed. `git status` clean after every mutant.

## Read

The first lens's audit and patch; `docs/HANDFILE-WIRE.md` (the fold's §2, §4, §5); the fold's diff in full
(`handfile.rs`, `handfile_lens.rs`, `record_post.rs`, `lib.rs`, `sql_prepares_sqlite.rs`); `handfile.rs` 350-490
(`verify_handfile`), 518-566 (the statements), 611-760 (`HandfileCapQuery`, `handfile_cap_queries`,
`handfile_answer`), 761-985 (`handfile_post`); `record_post.rs` 1383-1625 (the direct caps, the latch,
`insert_wrote_a_row`), 1891-1934 (the dispatch), 2040-2260 (the refund arm, the no-op rule, the recompute);
`auth::resolve_view_identity`; `d1/mod.rs` 600-640, 1067-1115, 1333-1360, 2280-2296; `schema.rs` 268-298, 525-560;
`tests/e602_convergence.rs` 770-790; `storage-ownership.json` (the handfile rows); PR 151 and its two comments; this
repository's `CLAUDE.md`; LOW's `CLAUDE.md` row D11 and the #648 paragraph; bsv-low `potParty.ts`, `hopParty.ts`
(the settle key's `keyID = gameId`).

End stamp: 2026-10-23T21:40:45Z (`date -u`).
