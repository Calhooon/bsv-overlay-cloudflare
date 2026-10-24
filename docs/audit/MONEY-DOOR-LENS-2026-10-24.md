# MONEY-DOOR-LENS: the one lens on overlay PR 169 (bsv-low #798, #663)

Lens stamped 2026-10-24 18:39 UTC (`date -u`). Branch `lane/MONEY-DOOR-LENS` cut from PR 169's head `55591bab`
(over `129125fb`, base `origin/main` `db3a0931`). Read whole: `git diff origin/main...HEAD` (5 files, +1038 / -274).

## Verdict: SOUND

Nothing above LOW. #798: the potrefund chain no-op now stands only on a chain row that passes the filed path's whole
bar (the marker signature under the identity through the one canonical-DER gate AND the raw bound rank 1 to the
indexed pot by both committed settle keys), the own row is latched `refundValid = 1` before the no-op answers, and a
junk row is never the no-op, never demoted into, never counted against the victim's cap. #663: the hop sweep's
gameId is compared with the hop row's own game after the row is proven the poster's (latched or replayed over the
row's game), in both arms the door has; the replay arm's "no RED on base" is right and the new refusal is strictly at
least as safe. The extraction (`129125fb`) is behaviour-unchanged for every honest filing and every refusal code,
with one stated exception at LOW-2.

## Findings

### LOW-1: the no-op's latch moves the owed list's evidence without a stale mark

- Where: `crates/low-app-layer/src/record_post.rs:1568` (`potrefund_chain_noop` latches the own chain row),
  `record_post.rs:2468` (the route answers `AlreadyIndexed` and returns), before `note_filing_and_recompute` at
  `record_post.rs:2498`; the handfile arm likewise (`handfile.rs:833` latches, `handfile.rs:963` `if !wrote` returns
  with no recompute when no insert changed a row).
- Input: the victim's valid backup is already on chain as its own marker (unlatched, `refundValid` NULL); the victim
  (or its outbox) files it.
- Consequence: the latch flips `refundValid` to 1, which the owed recompute reads as the pot's valid refund
  (`owed.rs:5884` `valid_refunds_sql`, any identity), but no party of the pot is marked stale and no walk is
  scheduled, so the refund-due row's `present-refund` press appears only at the read's age rule or the next event
  (display latency, self-healing; `/refund-backups` reads the table directly and is current). Not a regression: on
  the base that chain row was never latched at all.
- Fix shape: when `potrefund_chain_noop` latched a row (its batch's `changes > 0`), treat the answer as a write for
  the stale marks (`OWED_STALE_FOR_POT_SQL` for the pot) while still answering `alreadyIndexed`.

### LOW-2 (PLAUSIBLE, not reproduced): one reading the extraction changed

- Where: `routes.rs:3163` (`D1Sql::run_batch`: `changes` is `meta().changes.unwrap_or(0)`) into
  `record_post.rs:2365` (`insert_wrote_a_row(Some(done.changes as usize))`). Before `129125fb` the route passed
  `meta().ok().flatten().and_then(|m| m.changes)`, so an unreadable meta was `None`, read as WRITTEN
  (`insert_wrote_a_row(None) == true`, `record_post.rs:1757`, and the comment at the call still says so). Now it
  reads as NOT written. The handfile arm keeps the old reading (`handfile.rs:960`).
- Consequence: only if D1 ever answers a `collected` / `held` insert without meta: a first filing answers
  `alreadyFiled` and skips its stale mark (heals at the 15 minute age rule). D1 returns meta on every statement in
  practice, so not reproduced.
- Fix shape: carry `changes` as `Option<u64>` on `StmtOut`, or read `None` as written at this one site.

### NOTEs (no change asked)

- N1, the extraction's other differences, none observable on an honest filing: the statements now run as a
  one-statement `counted_batch` (still ledgered); a D1 fault surfaces as `worker::Error::RustError(<string>)` (the
  500's text, not its status, can differ); a `collected` chain row that does not deserialize is skipped where the
  route errored. Every refusal's code and body are produced by the unchanged `refuse` from the core's
  `DoorOutcome::Refused` (`record_post.rs:2452`ff), and the counters bumped are the same per arm.
- N2, the read bound: `POTREFUND_CHAIN_ROWS_SQL` (`record_post.rs:1449`) reads 16 rows with no `ORDER BY`. A
  stranger who buys 16 or more junk chain markers naming the victim can hide the victim's own valid chain row; the
  door then WRITES the victim's own rank-1 row (the safe direction; `lens_798_c` ran it: `Written`). The victim's
  cap of 1 per (game, pot) is then spent by the victim's own row, never by the junk.
- N3, #663's row choice (unchanged by the PR): `HOP_CONTEXT_SQL` (`hopsweep.rs:350`) takes the poster's row of the
  hop outpoint by `markerValid` DESC then oldest. If both the victim's row and an older stranger-shaped row under
  another game are unlatched or 0, the older is read and the honest sweep is 425 `HopMarkerUnverified` until the
  overlay's latch (`d1_discovery.rs:6620`, computed at admission) leads the victim's row; on the base it was the same
  425. `lens_663_b` pins all four states.
- N4, "every arm" of #663: the hop sweep has no handfile slot (`handfile.rs:591`, `HandfileSlot` holds hopparty,
  potparty v1 / v2 and potrefund only), so the arms are the direct door's latched and replay arms, both covered.
- N5, the unverified-row counter is per isolate (`record_post.rs:1583`), bumped once per junk row per filing read;
  telemetry only.

## The forger's questions, answered from the code

1. Can a stranger still pre-plant a row the door treats as present? A FILED row at the victim's content key needs the
   poster to be the identity (`PosterMismatch`, `record_post.rs:1180`) and the identity's signature through the
   canonical-DER gate (`record_post.rs:1191`); a replay of the victim's own public marker is the same content, so
   idempotent. A CHAIN row is admitted by bytes (`tm_potrefund`) and stored with `refundValid` NULL; the door now
   reads those rows (`record_post.rs:1449`, keyed by identity, game, pot txid AND vout) and counts one only when
   `potrefund_chain_row_is_own` (`record_post.rs:1504`) passes. The readers that serve "valid":
   `/refund-backups` orders on `COALESCE(refundValid, 0) DESC` (`refund_backups.rs:77`), `ls_potrefund byPot` the
   same (`d1_discovery.rs:5370`), the owed list reads `refundValid = 1` only (`owed.rs:5884`); the only writers of 1
   are the filed path's bind and the two latches, each behind the same bar. A junk row is served below every latched
   row and never as valid.
2. The victim's valid filing over a junk row: the junk row is KEPT beside (different key: its txid is the chain
   txid, the filing's `filed:`), stays NULL, and the filed row is written at rank 1 and served first
   (`money_door_798_a`; `lens_798_a` with the junk planted after the valid row).
3. The victim's own repeat stays a quiet no-op: an own chain row is `AlreadyIndexed` (and latched once; a second
   latch is a no-op by `COALESCE(refundValid, 0) = 0`); a filed row's re-file is the idempotent `INSERT OR IGNORE`
   (`money_door_798_b`, `lens_798_a` case 2).
4. A later junk row never demotes a valid row: nothing writes `refundValid` down (`POTREFUND_CHAIN_LATCH_SQL`,
   `record_post.rs:1458`, sets 1 only from 0 / NULL); `lens_798_a` both cases.
5. The caps: `POTREFUND_FILED_ROWS_SQL` and `POTREFUND_FILED_TODAY_SQL` count `txid LIKE 'filed:%'` only
   (`record_post.rs:1398`, `:1653`), so chain junk spends neither the victim's per-pot cap nor its day; the stranger
   cannot file under the victim at all. `lens_798_c`: 20 junk rows, the victim's filing written, the next DIFFERENT
   content refused only by the victim's own filed row.
6. #663 which row: `hopparty_records` for (hop txid, vout, poster) (`hopsweep.rs:350`), not `pot_records` nor
   `hop_pot_links`. Absent: `HopNotIndexed` 425 as before (`lens_663_b`). The comparison (`hopsweep.rs:236`) runs
   after the row is proven the poster's: latched, or its own signature replayed over the ROW's game
   (`hopsweep.rs:213`, `:222`, `:229`), so a stranger's row can never become the game the filing is held to.
7. "No RED on base" for the replay arm: right. The base replayed over the FILING's game (`origin/main`
   `hopsweep.rs:218`), so a wrong game over an unlatched row failed the signature and answered 425; it never
   accepted. Now a verified row with another game is 422 `SweepGameMismatch`, an unverifiable one 425: both refuse,
   nothing written, and the latched arm (which accepted any game on the base) now refuses too. Strictly at least as
   safe.

## The cases written by the lens (not pushed to PR 169's branch; patch below) and their result

Run against the branch head `55591bab` with the patch applied (`cargo test --manifest-path workers/Cargo.toml -p
low-app-layer --lib -- lens_798 lens_663`): 4 passed, 0 failed.

- `lens_798_a_valid_row_then_junk_never_demotes`: a filed rank-1 row then junk then the re-file (row stays 1, served
  first, one filed row); the own chain row latched, junk planted, the re-file still `AlreadyIndexed`, latch kept.
- `lens_798_b_the_forger_cannot_file_at_the_door`: a junk-signed marker naming the victim is `SignatureInvalid`; the
  victim's marker posted by a stranger is `PosterMismatch`; a stranger's own-identity junk raw on the indexed pot is
  refused at the bind and writes nothing.
- `lens_798_c_junk_flood_spends_no_cap_and_is_never_the_noop`: 20 junk rows plus the victim's own chain row as the
  21st: the door answered `Written { row_written: true }` (the own row past the read bound; the filing's rank-1 row
  leads), the junk spent no cap.
- `lens_663_b_the_hop_row_read_and_the_absent_hop`: no hop row is `HopNotIndexed`; with a stranger-shaped row under
  another game: own latched leads (written), own latched with junk 0 (written), own NULL with junk 0 (425, the older
  row is read, as on base), both NULL (425); the wrong game is refused in every state.

Not run: the PR's RED lines on `129125fb` (stated by the lane, not re-run here), the route tier, wasm32.

## Commands and exits

- `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib` (with the lens patch): exit 0, `707 passed;
  0 failed; 2 ignored` (525 s).
- `cargo test ... -p low-app-layer --tests --no-run`, then each of the 38 integration binaries run directly: all 38
  `test result: ok`, none failed.
- `cargo clippy --manifest-path workers/Cargo.toml -p low-app-layer --all-targets -- -D warnings` (with the patch):
  exit 0.
- `rustfmt --edition 2021 --check` on `handfile.rs`, `hopsweep.rs`, `applayer_counters.rs`: exit 0; on the branch's
  `record_post.rs` (stdin): exit 0.
- `CARGO_TARGET_DIR=/Volumes/Lanes/targets/MONEY-DOOR-LENS`; nothing deployed, no `make ci`, no route tier.

## The patch (`crates/low-app-layer/src/record_post.rs`, appended to `mod tests`)

```diff
diff --git a/crates/low-app-layer/src/record_post.rs b/crates/low-app-layer/src/record_post.rs
index 9b5e293e..d266114e 100644
--- a/crates/low-app-layer/src/record_post.rs
+++ b/crates/low-app-layer/src/record_post.rs
@@ -4698,4 +4698,233 @@ pub(crate) mod tests {
             );
         }
     }
+
+    // ── MONEY-DOOR-LENS: the lens's negative cases (not part of PR 169) ──
+
+    /// The lens: a VALID row first, then a stranger's junk chain row at the same (identity, game, pot): the valid
+    /// row is never demoted, the victim's re-file stays idempotent, and the valid row leads `/refund-backups`.
+    #[tokio::test(flavor = "current_thread")]
+    async fn lens_798_a_valid_row_then_junk_never_demotes() {
+        let victim = wallet(0x51);
+        let id = identity(&victim);
+        let id_lc = hex::encode(&id);
+        let gid = [0x5au8; 32];
+        let filing = potrefund_script(&victim, &gid, &presigned_refund());
+        // (1) a FILED rank-1 row, then junk planted after it, then the victim's re-file
+        let db = crate::pot_hook::tests::SqliteSql::migrated();
+        md_index_the_pot(&db);
+        md_seat(&db, &id_lc, &gid);
+        let (out, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
+        assert_eq!(out, DoorOutcome::Written { row_written: true });
+        md_chain_admit(&db, &md_junk_marker(&id, &gid), &"e1".repeat(32), 9_000);
+        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
+        assert_eq!(
+            out,
+            DoorOutcome::Written { row_written: true },
+            "re-file: the idempotent write"
+        );
+        assert_eq!(
+            md_refund_rows(&db),
+            vec![(v.key().to_string(), Some(1)), ("e1".repeat(32), None)],
+            "the filed row keeps refundValid = 1; one filed row"
+        );
+        assert_eq!(
+            md_served(&db, &id_lc).first(),
+            Some(&(v.key().to_string(), Some(1)))
+        );
+        // (2) the victim's own valid CHAIN row latched, then junk planted, then a re-file: still the no-op, latch kept
+        let db = crate::pot_hook::tests::SqliteSql::migrated();
+        md_index_the_pot(&db);
+        md_seat(&db, &id_lc, &gid);
+        md_chain_admit(&db, &filing, &"f1".repeat(32), 1_500);
+        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
+        assert_eq!(out, DoorOutcome::AlreadyIndexed);
+        md_chain_admit(&db, &md_junk_marker(&id, &gid), &"f2".repeat(32), 9_000);
+        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
+        assert_eq!(
+            out,
+            DoorOutcome::AlreadyIndexed,
+            "the own row is still the no-op over the junk"
+        );
+        assert_eq!(
+            md_refund_rows(&db),
+            vec![("f1".repeat(32), Some(1)), ("f2".repeat(32), None)]
+        );
+        assert_eq!(
+            md_served(&db, &id_lc).first(),
+            Some(&("f1".repeat(32), Some(1)))
+        );
+    }
+
+    /// The lens: the forger cannot reach the door as the victim (poster mismatch, junk signature), and a stranger's
+    /// OWN-identity filing of a junk raw naming the victim's indexed pot is refused at the bind.
+    #[tokio::test(flavor = "current_thread")]
+    async fn lens_798_b_the_forger_cannot_file_at_the_door() {
+        let victim = wallet(0x52);
+        let id = identity(&victim);
+        let id_lc = hex::encode(&id);
+        let gid = [0x5bu8; 32];
+        let stranger = wallet(0x53);
+        let s_lc = hex::encode(identity(&stranger));
+        assert_eq!(
+            verify_record_post(RecordKind::Potrefund, &md_junk_marker(&id, &gid), &id_lc).err(),
+            Some(RecordRefusal::SignatureInvalid),
+            "a junk-signed marker naming the victim is refused at the door"
+        );
+        assert_eq!(
+            verify_record_post(
+                RecordKind::Potrefund,
+                &potrefund_script(&victim, &gid, &presigned_refund()),
+                &s_lc
+            )
+            .err(),
+            Some(RecordRefusal::PosterMismatch)
+        );
+        let db = crate::pot_hook::tests::SqliteSql::migrated();
+        md_index_the_pot(&db);
+        let junk = potrefund_script(&stranger, &gid, &junk_refund_raw(&POT_TXID, 0));
+        let (out, _) = md_file(&db, RecordKind::Potrefund, &junk, &s_lc).await;
+        assert!(
+            matches!(out, DoorOutcome::Refused(_)),
+            "a stranger's junk raw on an indexed pot is refused: {out:?}"
+        );
+        assert!(md_refund_rows(&db).is_empty());
+    }
+
+    /// The lens: the read bound. Twenty junk chain rows naming the victim never spend the victim's cap and never
+    /// make the filing a no-op; past the 16 rows read, an own chain row may go unseen, and the filing then WRITES
+    /// its own rank-1 row (the safe direction), never a junk no-op.
+    #[tokio::test(flavor = "current_thread")]
+    async fn lens_798_c_junk_flood_spends_no_cap_and_is_never_the_noop() {
+        let victim = wallet(0x54);
+        let id = identity(&victim);
+        let id_lc = hex::encode(&id);
+        let gid = [0x5cu8; 32];
+        let filing = potrefund_script(&victim, &gid, &presigned_refund());
+        let db = crate::pot_hook::tests::SqliteSql::migrated();
+        md_index_the_pot(&db);
+        md_seat(&db, &id_lc, &gid);
+        for i in 0..20u8 {
+            md_chain_admit(
+                &db,
+                &md_junk_marker(&id, &gid),
+                &format!("{:02x}", i).repeat(32),
+                100 + i64::from(i),
+            );
+        }
+        // the victim's own valid chain row, the 21st by rowid
+        md_chain_admit(&db, &filing, &"fe".repeat(32), 50);
+        let (out, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
+        let latched: Vec<_> = md_refund_rows(&db)
+            .into_iter()
+            .filter(|(_, r)| *r == Some(1))
+            .collect();
+        match out {
+            DoorOutcome::AlreadyIndexed => assert_eq!(latched, vec![("fe".repeat(32), Some(1))]),
+            DoorOutcome::Written { row_written: true } => {
+                assert_eq!(
+                    latched,
+                    vec![(v.key().to_string(), Some(1))],
+                    "the filing's own rank-1 row"
+                )
+            }
+            other => panic!("never a refusal under a junk flood: {other:?}"),
+        }
+        eprintln!("lens_798_c: past 16 junk rows the door answered {out:?}");
+        assert_eq!(
+            md_served(&db, &id_lc).first().map(|r| r.1),
+            Some(Some(1)),
+            "a valid row leads"
+        );
+        // the junk rows spent no cap: a DIFFERENT valid content is refused only by the victim's own filed row, if any
+        let k = pot_keys();
+        let other = spend_signed_by(&k[0], &k[1], 0xffff_fffd, 900_000);
+        let (out2, _) = md_file(
+            &db,
+            RecordKind::Potrefund,
+            &potrefund_script(&victim, &gid, &other),
+            &id_lc,
+        )
+        .await;
+        if matches!(out, DoorOutcome::AlreadyIndexed) {
+            assert_eq!(
+                out2,
+                DoorOutcome::Written { row_written: true },
+                "no filed row yet: the cap is untouched"
+            );
+        } else {
+            assert_eq!(
+                out2,
+                DoorOutcome::Refused(RecordRefusal::TooManyFiled),
+                "only the victim's own filed row counts"
+            );
+        }
+    }
+
+    /// The lens: #663's hop row choice. A stranger-shaped hopparty row of the victim's hop under ANOTHER game: when
+    /// the victim's own row is latched it leads and the honest sweep files; when neither is latched the older row
+    /// is read and the filing is 425 (as on the base); with no hop row the filing is 425 (unchanged).
+    #[tokio::test(flavor = "current_thread")]
+    async fn lens_663_b_the_hop_row_read_and_the_absent_hop() {
+        use crate::hopsweep::tests as hs;
+        let w = hs::wallet(0x55);
+        let id = hs::identity(&w);
+        let id_lc = hex::encode(&id);
+        let seat = hs::skey(13);
+        let raw = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &hs::DEST);
+        let honest = md_sweep_script(&id, &hs::GID, &raw);
+        // no hop row
+        let db = crate::pot_hook::tests::SqliteSql::migrated();
+        let (out, _) = md_file(&db, RecordKind::Hopsweep, &honest, &id_lc).await;
+        assert_eq!(format!("{out:?}"), "Refused(HopNotIndexed)");
+        let plant = |db: &crate::pot_hook::tests::SqliteSql, valid: Option<i64>| {
+            let h = hs::hop_ctx(&w, &seat, true);
+            db.conn
+                .execute(
+                    crate::handfile::HOPPARTY_HANDFILE_FILE_SQL,
+                    rusqlite::params![
+                        h.identity,
+                        h.opponent_identity,
+                        hex::encode([0x98u8; 32]),
+                        h.hop_vout,
+                        h.hop_sats as i64,
+                        h.seat_settle_pubkey,
+                        "3044",
+                        "3044",
+                        h.hop_lock_hex,
+                        h.hop_sats_on_chain.map(|v| v as i64),
+                        2,
+                        hex::encode(hs::HOP),
+                        7,
+                        500,
+                        valid
+                    ],
+                )
+                .unwrap();
+        };
+        for (own, junk, want) in [
+            (Some(1i64), None, "Written { row_written: true }"),
+            (Some(1), Some(0), "Written { row_written: true }"),
+            (None, Some(0), "Refused(HopMarkerUnverified)"),
+            (None, None, "Refused(HopMarkerUnverified)"),
+        ] {
+            let db = crate::pot_hook::tests::SqliteSql::migrated();
+            md_index_the_hop(&db, &hs::hop_ctx(&w, &seat, true), own);
+            plant(&db, junk);
+            let (out, _) = md_file(&db, RecordKind::Hopsweep, &honest, &id_lc).await;
+            assert_eq!(format!("{out:?}"), want, "own {own:?} junk {junk:?}");
+            // the wrong game is never written in any row state
+            let (out, _) = md_file(
+                &db,
+                RecordKind::Hopsweep,
+                &md_sweep_script(&id, &[0x98u8; 32], &raw),
+                &id_lc,
+            )
+            .await;
+            assert!(
+                matches!(out, DoorOutcome::Refused(_)),
+                "own {own:?} junk {junk:?}: {out:?}"
+            );
+        }
+    }
 }
```
