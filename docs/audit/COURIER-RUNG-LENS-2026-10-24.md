# COURIER-RUNG-LENS: the one lens on overlay PR 170 (bsv-low #868), 2026-10-24

Lane COURIER-RUNG-LENS, branch `lane/COURIER-RUNG-LENS` cut from PR 170's head `f5d2d700` (equal to `origin/lane/COURIER-RUNG` and the PR's `headRefOid` at 2026-10-24 19:00Z). Base `db3a0931` (the merge base with `origin/main`). Begun 18:19Z, written 19:10Z (`date -u`). No live courier was asked; every body read is the probe's (`/Volumes/Lanes/out/bsv-low/COURIER-PROBE/requests.jsonl`).

## Verdict: NOT SOUND (two MEDIUM, three LOW)

The parsers are right and match the probe byte for byte; `spendScope` and the native `/tx` are read as documented; `confirmation_unknown` is "could not look" on every path; no `unconfirmed` or absent scope is ever a negative; the app layer proves a BananaBlocks mined claim only through WoC's `/proof/tsc` checked against chaintracks. Two things keep it from landing as is: the overlay half's pins are not compiled at all (M2), and the app layer now asserts a request-time `present: false` again, on a BananaBlocks 404 that is held to a weaker bar than WoC's, which a durable client consumer stamps forever (M1). Both fixes are small.

## Findings

### M1 (MEDIUM, server behaviour CONFIRMED, money consequence PLAUSIBLE): `/tx-any` answers `present: false` again, on a BananaBlocks 404 with no route anchor, and a client stamp makes it permanent

- Where: `crates/low-app-layer/src/routes.rs:10659-10680` (BananaBlocks' rung) and `:10722-10731` (`TxObservation::Absent if banana_not_found` gives `CorroboratedAbsent`); WoC's own 404 counts only behind its proven route, `:10627-10645` (the anchor `txany::KNOWN_MINED_TXID`, "the Bitails rule applied symmetrically"). BananaBlocks' 404 has no such check: `txany::bananablocks_tx_not_found` (`txany.rs`, the body match) is all.
- What changed: on `db3a0931` the external leg NEVER answered an absence (`routes.rs` base `:10644-10645`: "No second general mempool-capable source has been established. `let absence = AbsenceCorroboration::Unknown;`"), so since #614 no request-time `present: false` existed for an index-unknown txid. The PR revives it (`decide_tx_any`, `txany.rs:676-688`), `terminal: false`.
- Who reads it: in this repository nobody acts on it (owed reads only `proof_mined_word` and the fault shape, `owed.rs:5040-5048`; no memo is written). In bsv-low's client: `app/src/lib/homeCards.ts:834-842` `stakeFundingProvablyAbsent` (record older than 6 h, `present === false`, `terminal` NOT read) and `:919-923`, which calls `stampStakeFundingAbsent`: a WRITE-ONCE `funding_absent_at` on the `low_refund_` record, "nothing-owed forever (no card, no History row, no re-probing)" (`:845-846`). Its doc comment (`:826-832`) still describes a bar of WoC + Bitails + a Bitails anchor. That consumer was dormant on `db3a0931` and is live again with this PR. Others: `stake.ts:8345`, `:9721` (a re-present, money-safe), `orchestrator.ts:2851` (a table error, display).
- Input: a legacy stake's DUE refund whose funding is mined but which BananaBlocks' native index does not hold (the probe asked only fresh and recent subjects, blocks 972341 to 972477; BananaBlocks' coverage of OLD history is unmeasured), on an isolate where WoC also answers 404 behind a healthy route.
- Money consequence: the refund card and History row are hidden for good on that device, for money that is on chain.
- Reproduced at the leg (`lens868_h_bananablocks_not_found_needs_no_route_anchor`, green on the tip): a BananaBlocks that answers its not-found for every txid, `KNOWN_MINED_TXID` included, still completes `CorroboratedAbsent`; WoC's route is asked its anchor, BananaBlocks' never is. Not reproduced: the client's stamp (TypeScript, not run here) or any live BananaBlocks history gap.
- Fix shape: hold BananaBlocks' 404 to WoC's rule (one read of its native `/tx/{KNOWN_MINED_TXID}` an isolate, counted healthy only on `confirmed: true` with a positive `block_height`, before its not-found votes); and name the client consumer in CLAUDE.md, with bsv-low's `stakeFundingProvablyAbsent` made to require `terminal` (or its doc corrected) before the PR lands.

### M2 (MEDIUM, CONFIRMED): the overlay half's pins are never compiled

- Where: `crates/overlay-cloudflare/src/proof_fetcher/c868.rs` exists, but no `mod c868;` is declared anywhere (`grep -rn "mod c868" crates/overlay-cloudflare` is empty; `fix834` is declared at `proof_fetcher.rs:36-37`, `c868` is not).
- Measured: `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib c868 -- --list` prints `0 tests, 0 benchmarks` on `f5d2d700`.
- Consequence: the CLAUDE.md's pin line for the overlay (`-p bsv-overlay-cloudflare --lib c868 ... RED on db3a0931 at its first arm`) names a test that does not run; the overlay's money-adjacent change (Bitails' 404 no longer an absence; BananaBlocks the absence's second source at the rebroadcast backstop, the retire pass, the conflict pass's competitor and the evidence check) is held by three source-text pins only (`fix834m_b`, `e602_courier_functions_keep_the_response_body`, `door_words` tests). A revert of `bitails_presence_response` to `presence_response` would pass the suite.
- Declared bare, the file then fails the crate's `#![deny(clippy::unwrap_used)]` (`lib.rs:9`) six times (`c868.rs:14-20`): the PR's "clippy clean" held because the file was never built.
- Fix shape: `#[cfg(all(test, not(target_arch = "wasm32")))] #[allow(clippy::unwrap_used, reason = "test code")] mod c868;` beside `fix834` (this lane's patch; both pins then pass and clippy is clean, see below).

### L1 (LOW): in the overlay a BananaBlocks mined body is an unchecked height, taken before WoC's

- `door_words::courier_presence` (`door_words.rs:482-491`) returns `mined_claim(first slot).or_else(woc)`; the first slot is now `mempool_witness_presence_counted` (`proof_fetcher.rs:229-246`), BananaBlocks when Bitails holds no positive. So BananaBlocks' `confirmed: true, block_height` is the #214 terminal admission's mined word (`broadcaster.rs:2895-2912`) and #602's competitor confirmation (`admit_fast.rs:1755-1770`), with no proof checked, and ahead of WoC's height (`lens868_d`, green: BananaBlocks 972400 served over WoC's 972476).
- It is D1's stated courier trust ("the courier's `blockheight` is trusted as the chain's word, unverified against chaintracks"), extended from Bitails and WoC to a third courier, reversible (MINED push, conflict pass, retire pass). But the PR's CLAUDE.md says "A mined claim from BananaBlocks or Bitails is still proven only through WoC's `/proof/tsc`", true of the app layer's `/tx-any` only.
- Fix shape: state it in the #868 paragraph (and, if wanted, prefer WoC's height when both claim).

### L2 (LOW): BananaBlocks' `spendScope: confirmed` can now make a filed sweep's payout `claimable`

- Consumers of the ladder's `spent_confirmed` (`routes.rs:9722-9732` builds it; probes at `:4343`, `:5935`, `:9842`): `owed.rs:3226` (the FILED sweep's `confirmed` from `by_chain.and_then(|w| w.spent_confirmed) == Some(true)`, which `owed.rs:3819` serves as `claimable`), `owed.rs:3141-3146` (`confirmed_by_other`, a veto of the index's proof, fail direction "could not judge"), `owed.rs:3025` (`non_pot_spender`), `hops_view.rs:1308` (an index-unspent hop shown `Spent` by the chain) and the memo's long window (`hops_view.rs:913`).
- Each yields to the tracker's checked word (`spender_mined`, `owed.rs:2826-2830`) and the index's proof, and courier-bytes rows stay `claimable: false` (`owed.rs:3828`), so NS-3 holds where the tracker has a row. Where it has none, `owed.rs:3226` is the one consumer that turns a courier's bare word into `claimable`: WoC's since before, BananaBlocks' now as well under a WoC fault (on `db3a0931` #723 nulled it there). The sweep is the identity's own filed transaction; a false word presses an internalize of an unmined sweep.
- Fix shape: none required for the lane (the trust class is unchanged); state in CLAUDE.md that BananaBlocks' scope reaches `claimable` on the filed arm.

### L3 (LOW, observation): a deployment without `bananablocks` in `PROOF_SOURCES` reads no absence at all

`wrangler.toml` sets no `PROOF_SOURCES` (`ProofSources::from_env` then enables nothing); `wrangler.low.toml:78` and `:311` include `bananablocks`. With BananaBlocks off, Bitails' 404 is "could not look" and BananaBlocks is never asked, so the rebroadcast backstop, the retire pass's absence arm and the evidence check's absence never fire (`lens868_a`, green). Fail-safe direction; worth one sentence in CLAUDE.md beside the tier's note.

### Notes (no finding)

- N1. `parse_woc_spent_body` (`results.rs:3467`, unchanged) still reads a WoC spend body with NO `status` as `Some(false)`, where BananaBlocks' absent scope is `None`. Every money reader keys on `== Some(true)`, so it fails closed; the asymmetry is pre-existing.
- N2. `presence_response` (overlay, unchanged) reads any JSON object under a 2xx as presence without checking the txid; Bitails still goes through it (`bitails_presence_response`). Pre-existing; positive direction only.
- N3. A native body `confirmed: false` beside a positive `block_height` reads present-unmined (pinned in `lens868_c` as observed): a hint, never an absence or a mined fact.
- N4. The door's terminal read (`broadcaster.rs:2895`) now spends one more unpaced subrequest (BananaBlocks) whenever Bitails holds no positive.

## The money questions, answered from the code

1. A BananaBlocks `spendScope: confirmed` as a proof: never as `swept.confirmed` of a courier-bytes row (`owed.rs:3828`); as `claimable` on the filed arm where the tracker holds no row (L2, the one consumer). `spentConfirmedSource` stays `courier` for it (`results.rs:3424-3439`, index proof, tracker and `/tx-any` proof first).
2. `unconfirmed` or an absent scope as a negative: nowhere. `decide_spent_any` (`results.rs:3349-3360`) serves `spent: true` with the scope's `Some(false)` or `None`; a spender whose raw does not verify is `known: false`, `spentConfirmed: null` whatever the scope (`lens868_a`); a scope beside `spent: false` is `NotSpent` (the ladder's existing second-provider bar applies); any other spelling is `None` (`lens868_b`).
3. A BananaBlocks mined body as a mined FACT: in the app layer only through WoC's `/proof/tsc` at the claimed height, checked by chaintracks (`routes.rs:10486-10512`; `lens868_g`: proof checked serves `confirmed` and the height, refused or absent serves the hint with `claimedHeight`; BananaBlocks' own proof route is never asked). In the overlay, as an unchecked height (L1).
4. `confirmation_unknown`: "could not look" in both readers (`txany::parse_bananablocks_tx_claim`, `proof_fetcher::bananablocks_presence_response`), beside WoC's 404 never an absence (`lens868_e`, overlay `lens868_b`), beside a mined claim too (`lens868_c`).
5. The two-source bar. Overlay (#519): STRICTER. Before, WoC 404 + Bitails 404 was an absence; Bitails' 404 is proven not to be (it dropped `e3340463...` while both others held it). Now the pair is WoC's 404 + BananaBlocks' exact recorded not-found; Bitails' 404 beside WoC's is `Inconclusive`, any other BananaBlocks answer is no vote, a positive anywhere wins (`c868` (1)-(8), overlay `lens868_b`, `lens868_c`). App layer (`/tx-any`): LOOSER than its own base, which never asserted an absence on the external leg; the new pair is WoC behind its anchor + BananaBlocks with no anchor (M1). The overlay's WoC 404 has no anchor either (pre-existing, both sides symmetric there).

## The two amended pins

- `fix834m_b_the_retire_pass_and_its_callers_carry_the_pacer` (`proof_fetcher/fix834.rs:475`): the contract is "every look of the retire port passes the pass's counters (the pacer)". The look is now `.mempool_witness_presence_counted(txid,self.counters.as_ref())`, and that function hands `counters` to both inner rungs (`proof_fetcher.rs:238`, `:244`), each through `presence_counted` and `http_get_counted`. Correctly re-stated; no contract dropped. The source pin does not see inside the wrapper (the inner forwarding is read here, not pinned).
- `e602_courier_functions_keep_the_response_body` (`tests/e602_terminal_race.rs:128-149`): the #625 contract is "every presence read goes through the one counted reader, which reads the body it kept". It was spelled `presence_response(status,&body)` inside `presence_counted`; it is now `read(status,&body)` once, plus a check that each of the three callers hands its own reader. Correctly re-stated and tightened (the per-function reader is new). It does not name `mempool_witness_presence_counted`, which calls two of the three it names.

## The parsers against the probe

All 16 fixture bodies (`crates/low-app-layer/tests/fixtures/courier_probe_868.json`) are VERBATIM rows of `requests.jsonl` (url, status and body equal byte for byte; a script compared each). `bb_tx_mined` carries no `confirmations` and reads `(true, Some(972476))`; `bb_tx_unmined` reads present-unmined; the not-found body is the exact `{"error":"Transaction not found"}`; Bitails `confirmations: 0` is presence and its 404s are no word. The WoC-style mirror is never asked by either crate (`grep "bsv/main"` finds tests only); its recorded mined shape (no `confirmed`, no `confirmations`) is no word to the native reader (`lens868_c`). `confirmation_unknown` and an omitted `spendScope` were never seen live; the pins apply them over the recorded bodies as the docs describe.

## The cases this lane wrote (in this worktree; the patch is the commit beside this file)

App layer, `routes::e606_tests::courier_rung_lens` (8, all green on `f5d2d700`): `lens868_a` (a confirmed scope never survives an unverified spender), `_b` (scope words outside the two; a scope beside `spent: false`; a spend naming no spender; a 503 carrying a scope), `_c` (nine native bodies that are no word; the not-found reader's exact match), `_d` (no absence without WoC's healthy route), `_e` (six BananaBlocks answers beside WoC's 404, none an absence), `_f` (a BananaBlocks positive outranks WoC's 404), `_g` (a mined claim confirmed only through the checked proof), `_h` (M1 as behaviour).

Overlay, `proof_fetcher::lens868` (4, green): `lens868_a` (L3), `_b` (ten BananaBlocks answers beside WoC's 404, none an absence), `_c` (BananaBlocks' not-found never outvotes WoC's mempool or mined body), `_d` (L1 as behaviour). Plus the one-line `mod c868;` (M2): the PR's two overlay pins then run and pass.

None of these is RED on the tip: the cases are the negatives the PR's pins leave out, and they hold. `_h` and overlay `_d` pin the behaviour the findings name; a fix to M1 or L1 must flip them.

## Commands and exits

`export CARGO_TARGET_DIR=/Volumes/Lanes/targets/COURIER-RUNG-LENS`, cargo 1.95.0 (no `rust-toolchain.toml` in the tree).

| Command (all `--manifest-path workers/Cargo.toml`) | Exit | Read |
|---|---|---|
| `cargo test -p low-app-layer --lib` | 0 | 712 passed, 0 failed, 2 ignored (the tip's 704 and this lane's 8), 506 s |
| `cargo test -p low-app-layer --tests` (lib again, then the 39 integration binaries) | 0 | 1049 passed, 0 failed, 2 ignored over its 96 result lines |
| `cargo test -p bsv-overlay-cloudflare --lib c868 -- --list` (the tip, before the patch) | 0 | `0 tests, 0 benchmarks` (M2) |
| `cargo test -p bsv-overlay-cloudflare --lib` (with the patch) | 0 | 1003 passed, 0 failed, 1 ignored (the tip's 998, `c868`'s 2, this lane's 4) |
| `cargo test -p bsv-overlay-cloudflare --test e602_terminal_race` | 0 | 9 passed, `e602_courier_functions_keep_the_response_body` among them |
| `cargo test -p bsv-overlay-cloudflare --lib fix834m_b` | 0 | 1 passed |
| `cargo clippy -p low-app-layer -p bsv-overlay-cloudflare --all-targets -- -D warnings`, `c868` declared bare | 101 | six `clippy::unwrap_used` errors in `proof_fetcher/c868.rs:14-20` (the crate denies it, `lib.rs:9`) |
| the same, `c868` declared with `#[allow(clippy::unwrap_used, reason = "test code")]` (as `proof_fetcher.rs:6298` does) | 0 | clean |
| `rustfmt --edition 2021 --check` on every `.rs` the PR touches | 1 on 7 files | none of the reds falls on a line the PR changed (attributed hunk by hunk against `git diff -U0`): pre-existing, named, not fixed; this lane's own hunks formatted |

So the PR's "clippy clean" held only because `c868.rs` was never compiled: declared, it is six clippy errors (part of M2's fix).

## What the route tier must show (the captain's, on the box)

The #831 cell's ghost pots end with the stand-in's BananaBlocks not-found (the absence pair is WoC + BananaBlocks now); the #834 cell's ceilings re-measured with `bananablocks` > 0 (each absence look asks BananaBlocks once when Bitails holds no positive); and one cell where the stand-in answers BananaBlocks 404 with a non-recorded body must show the ghost pot NOT ended.

## Not verified

The client's `stampStakeFundingAbsent` path (TypeScript, read not run); BananaBlocks' coverage of old mined history; `confirmation_unknown` and an omitted `spendScope` live; wasm32; the route tier.

## Appendix: the patch (this lane's cases and the `mod c868` declaration, `git diff f5d2d700`)

```diff
diff --git a/crates/low-app-layer/src/routes.rs b/crates/low-app-layer/src/routes.rs
index ce5eaaee..2da8bedf 100644
--- a/crates/low-app-layer/src/routes.rs
+++ b/crates/low-app-layer/src/routes.rs
@@ -14464,6 +14464,376 @@ mod e606_tests {
         );
         assert_eq!(held(), (1, w));
     }
+
+    /// Lane COURIER-RUNG-LENS (the one lens on PR 170, bsv-low #868): the negative cases the PR's pins leave out.
+    mod courier_rung_lens {
+        use super::*;
+
+        /// The presence leg over a courier map with the route health and the proof check chosen by the case.
+        fn lens_leg(
+            txid: &str,
+            m: HashMap<String, (u16, Vec<u8>)>,
+            woc_route: Option<bool>,
+            proof_ok: bool,
+        ) -> (crate::txany::TxAnyAnswer, AbsenceCorroboration, Vec<String>) {
+            WOC_ROUTE_HEALTHY.with(|c| c.set(woc_route));
+            let asked = std::cell::RefCell::new(Vec::new());
+            let get = |url: String| {
+                asked.borrow_mut().push(url.clone());
+                std::future::ready(m.get(&url).cloned())
+            };
+            let now = crate::txany::rfc3339_utc_ms("2026-10-24T14:40:00Z").unwrap();
+            let (obs, absence, height) = run(tx_any_presence_leg_via(
+                txid,
+                now,
+                false,
+                None,
+                get,
+                move |_, _| async move { proof_ok },
+            ));
+            let mut a = decide_tx_any(None, None, Some(&obs), absence);
+            apply_external_height(&mut a, &obs, height);
+            (a, absence, asked.into_inner())
+        }
+
+        fn nf() -> (u16, Vec<u8>) {
+            let (s, b, _) = c868_recorded("bb_tx_not_found");
+            (s, b)
+        }
+
+        /// (a) A `spendScope: "confirmed"` whose spender's raw no rung serves (or serves bytes that do not spend the
+        /// outpoint) is never a confirmed word: the honest unknown, `spentConfirmed: null`.
+        #[test]
+        fn lens868_a_a_confirmed_scope_never_survives_an_unverified_spender() {
+            let t = tx(0x07);
+            let (sp, _) = competitor();
+            for raw in [None, Some(vec![0u8; 10])] {
+                let mut m = HashMap::new();
+                m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), (503, Vec::new()));
+                m.insert(
+                    format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
+                    (200, c868_spend_body("bb_spend_confirmed", false)),
+                );
+                if let Some(r) = raw.clone() {
+                    m.insert(
+                        format!("{WOC_BASE}/tx/{sp}/hex"),
+                        (200, hex::encode(r).into_bytes()),
+                    );
+                }
+                let (row, verified) = run(spent_any_resolve_via(&t, 0, courier(m)));
+                assert_eq!(
+                    (
+                        row.known,
+                        row.spent,
+                        row.spent_confirmed,
+                        verified.is_some()
+                    ),
+                    (false, None, None, false),
+                    "raw {raw:?}"
+                );
+                assert_eq!(
+                    row.reason,
+                    Some(crate::logic::SPENT_ANY_REASON_UNVERIFIED_SPENDER)
+                );
+            }
+        }
+
+        /// (b) Only the two documented words are a scope; any other spelling is unknown, and a scope beside
+        /// `spent: false` (or a spend naming no spender) is no mined word at all.
+        #[test]
+        fn lens868_b_scope_words_outside_the_two_are_unknown() {
+            use crate::results::{
+                bananablocks_spend_scope, parse_bananablocks_spent, SpentObservation,
+            };
+            let sp = "ab".repeat(32);
+            for scope in [
+                serde_json::json!("CONFIRMED"),
+                serde_json::json!("Confirmed"),
+                serde_json::json!("mined"),
+                serde_json::json!(""),
+                serde_json::json!(true),
+                serde_json::json!(1),
+                serde_json::Value::Null,
+            ] {
+                let body = serde_json::json!({"spent": true, "spentTxid": sp, "spendScope": scope});
+                assert_eq!(bananablocks_spend_scope(&body), None, "{scope}");
+                assert_eq!(
+                    parse_bananablocks_spent(200, Some(&body)),
+                    SpentObservation::Spent {
+                        txid: sp.clone(),
+                        confirmed: None
+                    },
+                    "{scope}"
+                );
+            }
+            assert_eq!(
+                parse_bananablocks_spent(
+                    200,
+                    Some(&serde_json::json!({"spent": false, "spendScope": "confirmed"}))
+                ),
+                SpentObservation::NotSpent
+            );
+            assert_eq!(
+                parse_bananablocks_spent(
+                    200,
+                    Some(&serde_json::json!({"spent": true, "spendScope": "confirmed"}))
+                ),
+                SpentObservation::Fault
+            );
+            // a 5xx or a 429 that happens to carry a scope is never read
+            assert_eq!(
+                parse_bananablocks_spent(
+                    503,
+                    Some(
+                        &serde_json::json!({"spent": true, "spentTxid": sp, "spendScope": "confirmed"})
+                    )
+                ),
+                SpentObservation::Fault
+            );
+        }
+
+        /// (c) BananaBlocks' native `/tx` bodies that are no word: a mined claim with no positive height, a
+        /// `confirmed` that is not a boolean, `confirmation_unknown` beside `confirmed: true`, a body with no txid,
+        /// the WoC-style mirror's MINED body (recorded shape: no `confirmed`, no `confirmations`).
+        #[test]
+        fn lens868_c_native_tx_bodies_that_are_no_word() {
+            use crate::txany::parse_bananablocks_tx_claim;
+            let t = "cd".repeat(32);
+            for (label, body) in [
+                (
+                    "mined, height 0",
+                    serde_json::json!({"txid": t, "confirmed": true, "block_height": 0}),
+                ),
+                (
+                    "mined, no height",
+                    serde_json::json!({"txid": t, "confirmed": true}),
+                ),
+                (
+                    "mined, height a string",
+                    serde_json::json!({"txid": t, "confirmed": true, "block_height": "972476"}),
+                ),
+                (
+                    "confirmed a string",
+                    serde_json::json!({"txid": t, "confirmed": "false", "block_height": 0}),
+                ),
+                (
+                    "confirmed null",
+                    serde_json::json!({"txid": t, "confirmed": null}),
+                ),
+                (
+                    "unknown beside a mined claim",
+                    serde_json::json!({"txid": t, "confirmed": true, "block_height": 972_476,
+                        "confirmation_unknown": true, "partial": true}),
+                ),
+                (
+                    "no txid",
+                    serde_json::json!({"confirmed": false, "block_height": 0}),
+                ),
+                (
+                    "the mirror's mined body",
+                    serde_json::json!({"txid": t, "hash": t, "version": 1, "size": 200, "locktime": 0,
+                        "vin": [], "vout": [], "blockhash": "00".repeat(32), "time": 1,
+                        "blocktime": 1, "blockheight": 972_476}),
+                ),
+                (
+                    "the not-found body under a 200",
+                    serde_json::json!({"error": "Transaction not found"}),
+                ),
+            ] {
+                assert_eq!(parse_bananablocks_tx_claim(&body, &t), None, "{label}");
+            }
+            // the not-found reader needs the 404 AND the recorded body, exactly
+            let nf_body = br#"{"error":"Transaction not found"}"#;
+            assert!(!crate::txany::bananablocks_tx_not_found(200, nf_body));
+            assert!(!crate::txany::bananablocks_tx_not_found(410, nf_body));
+            assert!(!crate::txany::bananablocks_tx_not_found(
+                404,
+                br#"{"error":"transaction not found"}"#
+            ));
+            assert!(!crate::txany::bananablocks_tx_not_found(
+                404,
+                b"Transaction not found"
+            ));
+            // observed, not a defect: `confirmed: false` beside a positive height reads unmined (a hint, never an
+            // absence and never a mined fact)
+            assert_eq!(
+                parse_bananablocks_tx_claim(
+                    &serde_json::json!({"txid": t, "confirmed": false, "block_height": 972_476}),
+                    &t
+                ),
+                Some((false, None))
+            );
+        }
+
+        /// (d) BananaBlocks' not-found never completes an absence when WoC's 404 is not behind a healthy route (the
+        /// anchor read faulted): the leg stays unknown.
+        #[test]
+        fn lens868_d_no_absence_without_wocs_healthy_route() {
+            let t = c868_subject("bb_tx_not_found");
+            let mut m = HashMap::new();
+            m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
+            m.insert(
+                format!("{WOC_BASE}/tx/hash/{}", crate::txany::KNOWN_MINED_TXID),
+                (503, Vec::new()),
+            );
+            m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), nf());
+            let (a, absence, _) = lens_leg(&t, m, None, false);
+            assert_eq!((a.present, absence), (None, AbsenceCorroboration::Unknown));
+        }
+
+        /// (e) Beside WoC's healthy 404, every BananaBlocks answer that is not its recorded 404 is no vote: its
+        /// `confirmation_unknown`, a mined claim with no height, its not-found body under 200 or 410, a 429, a
+        /// transport fault.
+        #[test]
+        fn lens868_e_no_bananablocks_word_beside_wocs_404_is_never_an_absence() {
+            let t = c868_subject("bb_tx_unmined");
+            let (_, unmined, _) = c868_recorded("bb_tx_unmined");
+            let mut unknown: serde_json::Value = serde_json::from_slice(&unmined).unwrap();
+            unknown["confirmation_unknown"] = serde_json::json!(true);
+            let (_, nf_body) = nf();
+            for (label, banana) in [
+                (
+                    "confirmation_unknown",
+                    Some((200u16, unknown.to_string().into_bytes())),
+                ),
+                (
+                    "mined, no height",
+                    Some((
+                        200,
+                        serde_json::json!({"txid": t, "confirmed": true})
+                            .to_string()
+                            .into_bytes(),
+                    )),
+                ),
+                ("not-found under 200", Some((200, nf_body.clone()))),
+                ("not-found under 410", Some((410, nf_body.clone()))),
+                ("429", Some((429, Vec::new()))),
+                ("transport", None),
+            ] {
+                let mut m = HashMap::new();
+                m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
+                if let Some(b) = banana {
+                    m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), b);
+                }
+                let (a, absence, _) = lens_leg(&t, m, Some(true), false);
+                assert_eq!(
+                    (a.present, a.hint_source, absence),
+                    (None, None, AbsenceCorroboration::Unknown),
+                    "{label}"
+                );
+            }
+        }
+
+        /// (f) A positive outranks WoC's healthy 404: BananaBlocks' unmined body beside it is the hint, no absence.
+        #[test]
+        fn lens868_f_bananablocks_presence_outranks_wocs_404() {
+            let t = c868_subject("bb_tx_unmined");
+            let (s, b, _) = c868_recorded("bb_tx_unmined");
+            let mut m = HashMap::new();
+            m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
+            m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), (s, b));
+            let (a, absence, asked) = lens_leg(&t, m, Some(true), false);
+            assert_eq!(
+                (a.present, a.hint_source, absence),
+                (None, Some("bananablocks"), AbsenceCorroboration::Unknown)
+            );
+            assert!(
+                !asked
+                    .iter()
+                    .any(|u| u.starts_with(BITAILS_BASE) && !u.contains("/download/")),
+                "a BananaBlocks positive asks Bitails' presence no further: {asked:?}"
+            );
+        }
+
+        /// (h) THE LENS'S M1, reproduced as behaviour: WoC's 404 counts only behind its proven route (the anchor
+        /// `KNOWN_MINED_TXID` served), BananaBlocks' recorded not-found counts with NO such check. A BananaBlocks that
+        /// answers its not-found for EVERY txid, the known-mined anchor included (an index that lost history, a
+        /// partial node behind its balancer), still completes the corroborated absence (`present: false`).
+        #[test]
+        fn lens868_h_bananablocks_not_found_needs_no_route_anchor() {
+            let t = c868_subject("bb_tx_not_found");
+            let mut m = HashMap::new();
+            m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
+            m.insert(
+                format!("{WOC_BASE}/tx/hash/{}", crate::txany::KNOWN_MINED_TXID),
+                (200, b"{}".to_vec()),
+            );
+            m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), nf());
+            m.insert(
+                format!("{BANANABLOCKS_BASE}/tx/{}", crate::txany::KNOWN_MINED_TXID),
+                nf(),
+            );
+            let (a, absence, asked) = lens_leg(&t, m, None, false);
+            assert_eq!(
+                (a.present, absence),
+                (Some(false), AbsenceCorroboration::CorroboratedAbsent)
+            );
+            assert!(
+                asked.iter().any(|u| u.ends_with(crate::txany::KNOWN_MINED_TXID) && u.starts_with(WOC_BASE)),
+                "WoC's route is proven: {asked:?}"
+            );
+            assert!(
+                !asked
+                    .iter()
+                    .any(|u| u.ends_with(crate::txany::KNOWN_MINED_TXID)
+                        && u.starts_with(BANANABLOCKS_BASE)),
+                "BananaBlocks' route is never proven: {asked:?}"
+            );
+        }
+
+        /// (g) A BananaBlocks MINED claim is served `confirmed` only through WoC's `/proof/tsc` checked against
+        /// chaintracks at the CLAIMED height; with the check refusing it is the hint with `claimedHeight`, and with
+        /// no proof route answering likewise. Never BananaBlocks' own proof route.
+        #[test]
+        fn lens868_g_a_bananablocks_mined_claim_needs_the_checked_proof() {
+            // the recorded mined body (no `confirmations`), re-keyed to the harness's spender so its raw verifies
+            let (t, raw) = competitor();
+            let (s, b, _) = c868_recorded("bb_tx_mined");
+            let mut v: serde_json::Value = serde_json::from_slice(&b).unwrap();
+            v["txid"] = serde_json::json!(t);
+            let b = v.to_string().into_bytes();
+            let tsc = serde_json::json!({"txOrId": t, "index": 0, "nodes": ["*"]})
+                .to_string()
+                .into_bytes();
+            for (label, proof, check, want_confirmed) in [
+                ("proof checked", Some(tsc.clone()), true, Some(true)),
+                ("proof refused", Some(tsc.clone()), false, None),
+                ("no proof", None, true, None),
+            ] {
+                let mut m = HashMap::new();
+                m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (429, Vec::new()));
+                m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), (s, b.clone()));
+                m.insert(
+                    format!("{WOC_BASE}/tx/{t}/hex"),
+                    (200, hex::encode(&raw).into_bytes()),
+                );
+                if let Some(p) = proof {
+                    m.insert(format!("{WOC_BASE}/tx/{t}/proof/tsc"), (200, p));
+                }
+                let (a, _, asked) = lens_leg(&t, m, Some(true), check);
+                assert_eq!(a.confirmed, want_confirmed, "{label}");
+                // the height is served only where the proof was checked at it
+                assert_eq!(
+                    a.height,
+                    want_confirmed.map(|_| 972_476),
+                    "{label}: a height only behind the checked proof"
+                );
+                if want_confirmed.is_none() {
+                    assert_eq!(
+                        (a.present, a.hint_source, a.claimed_height),
+                        (None, Some("bananablocks"), Some(972_476)),
+                        "{label}"
+                    );
+                }
+                assert!(
+                    !asked
+                        .iter()
+                        .any(|u| u.starts_with(BANANABLOCKS_BASE) && u.contains("proof")),
+                    "{label}: {asked:?}"
+                );
+            }
+        }
+    }
 }
 
 #[cfg(test)]
diff --git a/crates/overlay-cloudflare/src/proof_fetcher.rs b/crates/overlay-cloudflare/src/proof_fetcher.rs
index 64cc057b..0da95c28 100644
--- a/crates/overlay-cloudflare/src/proof_fetcher.rs
+++ b/crates/overlay-cloudflare/src/proof_fetcher.rs
@@ -35,6 +35,13 @@
 mod diagnostics;
 #[cfg(all(test, not(target_arch = "wasm32")))]
 mod fix834;
+// Lane COURIER-RUNG-LENS: PR 170 adds `proof_fetcher/c868.rs` but never declares it, so its pins are not compiled.
+// It also fails the crate's `clippy::unwrap_used` deny once compiled; the test allowance below is this file's own.
+#[cfg(all(test, not(target_arch = "wasm32")))]
+#[allow(clippy::unwrap_used, reason = "test code")]
+mod c868;
+#[cfg(all(test, not(target_arch = "wasm32")))]
+mod lens868;
 use crate::courier_counters::{Counters as CourierCounters, Outcome as CourierOutcome, Attempt as CourierAttempt};
 pub(crate) use diagnostics::{ProofProbe, PROOF_RUNGS};
 
diff --git a/crates/overlay-cloudflare/src/proof_fetcher/lens868.rs b/crates/overlay-cloudflare/src/proof_fetcher/lens868.rs
new file mode 100644
index 00000000..1ade377d
--- /dev/null
+++ b/crates/overlay-cloudflare/src/proof_fetcher/lens868.rs
@@ -0,0 +1,126 @@
+//! Lane COURIER-RUNG-LENS (the one lens on PR 170, bsv-low #868): the #519 presence ladder's negative cases the PR's
+//! `c868` pins leave out. Every BananaBlocks answer that is not its recorded not-found is "could not look", and an
+//! absence needs WoC's 404 beside it.
+use super::*;
+use crate::admit_fast::{ConflictPresence, CourierConflictPresence};
+
+const TXID: &str = "e3340463baaceb6943dec5d82f612a247d2d1227f1b64f6e5c1438e5ff4e8d58";
+const NOT_FOUND: &str = r#"{"error":"Transaction not found"}"#;
+
+fn answer(url: String, status: u16, body: &str) {
+    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().insert(url, (status, body.to_string())));
+}
+
+fn couriers(bitails: (u16, &str), woc: (u16, &str), banana: Option<(u16, &str)>) {
+    crate::courier_counters::clear_cooldowns();
+    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
+    answer(
+        format!("{DEFAULT_BITAILS_BASE}/tx/{TXID}"),
+        bitails.0,
+        bitails.1,
+    );
+    answer(format!("{DEFAULT_WOC_BASE}/tx/hash/{TXID}"), woc.0, woc.1);
+    if let Some((s, b)) = banana {
+        answer(format!("{DEFAULT_BANANABLOCKS_BASE}/tx/{TXID}"), s, b);
+    }
+}
+
+async fn presence(sources: &str) -> Option<NetworkPresence> {
+    CourierConflictPresence {
+        sources: ProofSources::parse(sources),
+        woc_api_key: None,
+        counters: None,
+    }
+    .presence(TXID)
+    .await
+}
+
+/// (a) A deployment whose `PROOF_SOURCES` omits `bananablocks` (`wrangler.toml` sets none) can no longer read ANY
+/// absence: Bitails' 404 is "could not look" and BananaBlocks is never asked, so WoC's 404 stands alone.
+#[tokio::test]
+async fn lens868_a_without_bananablocks_enabled_no_absence_is_read() {
+    couriers((404, ""), (404, ""), Some((404, NOT_FOUND)));
+    assert_eq!(
+        presence("arcade,woc,bitails").await,
+        Some(NetworkPresence::Inconclusive)
+    );
+    assert_eq!(presence("").await, Some(NetworkPresence::Inconclusive));
+    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
+}
+
+/// (b) Beside WoC's 404, no BananaBlocks answer but its recorded 404 is a vote.
+#[tokio::test]
+async fn lens868_b_no_bananablocks_word_beside_wocs_404_is_never_an_absence() {
+    let mined_no_height = serde_json::json!({"txid": TXID, "confirmed": true}).to_string();
+    let mined_zero =
+        serde_json::json!({"txid": TXID, "confirmed": true, "block_height": 0}).to_string();
+    let unknown = serde_json::json!({"txid": TXID, "confirmed": false, "block_height": 0,
+        "confirmation_unknown": true, "partial": true})
+    .to_string();
+    let string_confirmed = serde_json::json!({"txid": TXID, "confirmed": "false"}).to_string();
+    for (label, banana) in [
+        ("not-found under 200", Some((200u16, NOT_FOUND))),
+        ("not-found under 410", Some((410, NOT_FOUND))),
+        (
+            "404 lower case",
+            Some((404, r#"{"error":"transaction not found"}"#)),
+        ),
+        ("404 plain text", Some((404, "Transaction not found"))),
+        ("mined, no height", Some((200, mined_no_height.as_str()))),
+        ("mined, height 0", Some((200, mined_zero.as_str()))),
+        ("confirmation_unknown", Some((200, unknown.as_str()))),
+        ("confirmed a string", Some((200, string_confirmed.as_str()))),
+        ("429", Some((429, ""))),
+        ("unanswered", None),
+    ] {
+        couriers((404, ""), (404, ""), banana);
+        assert_eq!(
+            presence("arcade,bananablocks,woc,bitails").await,
+            Some(NetworkPresence::Inconclusive),
+            "{label}"
+        );
+    }
+    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
+}
+
+/// (c) BananaBlocks' clean not-found never outvotes a positive: WoC's mempool body, or WoC's mined height.
+#[tokio::test]
+async fn lens868_c_a_bananablocks_absence_never_outvotes_a_positive() {
+    let woc_mempool = serde_json::json!({"txid": TXID, "hash": TXID, "version": 1, "size": 2168,
+        "locktime": 0, "vin": [], "vout": []})
+    .to_string();
+    couriers((404, ""), (200, &woc_mempool), Some((404, NOT_FOUND)));
+    assert_eq!(
+        presence("arcade,bananablocks,woc,bitails").await,
+        Some(NetworkPresence::Present { height: None })
+    );
+    let woc_mined =
+        serde_json::json!({"txid": TXID, "blockheight": 972_476, "confirmations": 1}).to_string();
+    couriers((404, ""), (200, &woc_mined), Some((404, NOT_FOUND)));
+    assert_eq!(
+        presence("arcade,bananablocks,woc,bitails").await,
+        Some(NetworkPresence::Present {
+            height: Some(972_476)
+        })
+    );
+    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
+}
+
+/// (d) OBSERVED, the M2 of the lens: in the overlay's fold a BananaBlocks MINED body is a mined HEIGHT with no proof
+/// checked (`door_words::courier_presence`'s `mined_claim`, the #214 admission's and #602's confirmation input),
+/// and it is taken FIRST, ahead of WoC's own height, when Bitails holds no positive.
+#[tokio::test]
+async fn lens868_d_a_bananablocks_mined_body_is_an_unchecked_height_taken_first() {
+    let bb_mined =
+        serde_json::json!({"txid": TXID, "confirmed": true, "block_height": 972_400}).to_string();
+    let woc_mined =
+        serde_json::json!({"txid": TXID, "blockheight": 972_476, "confirmations": 1}).to_string();
+    couriers((404, ""), (200, &woc_mined), Some((200, &bb_mined)));
+    assert_eq!(
+        presence("arcade,bananablocks,woc,bitails").await,
+        Some(NetworkPresence::Present {
+            height: Some(972_400)
+        })
+    );
+    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
+}
```
