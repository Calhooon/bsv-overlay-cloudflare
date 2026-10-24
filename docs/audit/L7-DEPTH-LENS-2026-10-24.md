# L7-DEPTH-LENS: the lens on overlay PR 167 (bsv-low #862, the app layer serves `spendDepth`)

Stamp: 2026-10-24 17:20 UTC (`date -u`). Lens of `lane/L7-DEPTH` at `6546ba67` (8d41eea1, 39231058, 6546ba67 over
`origin/main` db3a0931), read whole (`git diff origin/main...HEAD`: 7 files, +488 / -6). The client half (bsv-low
`lane/L7-TOWER` at `8d6d8e0b`, `creditWord.ts`, `chainReads.ts`) was read to judge the contract only.

## Verdict

NOT SOUND, on one MEDIUM: the two halves disagree on a depth past 144 (MEDIUM-1). It cannot be reached at either
render's value today, because both set `"1"`. The rest of the claims hold: the field rides on exactly the decorated
objects of the three routes, the legacy fault body gains nothing, only the operator's var moves the value, the parse
is strict, the count fires once per isolate and the overlay seeds it. No migration is touched. Both renders set
`SPEND_DEPTH = "1"`, the owner's floor and today's behaviour, so there is no value above 1 to carry to the owner.

## Findings

### MEDIUM-1: a depth past 144 is served as set, and the client reads it as the floor 1

- Where: `crates/low-app-layer/src/tracker_host/runtime.rs:337` (`parse::<u32>()`: every value from 1 to 4,294,967,295
  is accepted, `from_var: true`, served unclamped), against bsv-low `app/src/lib/chainReads.ts:2601`
  (`SERVED_SPEND_DEPTH_MAX` 144: a larger value is "not served", so `creditWord.ts hostDepth` falls to
  `CREDIT_DEPTH_FLOOR` 1).
- Input: `SPEND_DEPTH = "145"` (or `"1000"`, or `"4294967295"`) in either render.
- What happens: every word carries `spendDepth: 145`, and `/health.spendDepth` reads `{depth: 145, source: "var"}`.
  The wallet credits at ONE block.
- Money consequence: a credit lands at the floor where the operator asked for 145 blocks. That is never a phantom
  credit (a checked `mined` is still required) and never a lost one. But the host's own health read misstates what
  the wallet waits for, and it does so in the fail-open direction.
- Reproduced: `lens_m1_a_depth_past_the_clients_bound_is_served_unclamped` pins the server half as it stands. The
  client half was read, not run.
- Fix shape: one bound on both sides. Recommended: the server reads a value past 144 as malformed (the floor served,
  `spend_depth_var_malformed_total` counted, `/health.spendDepth.source: "floor"`). That matches what the client
  already does, and an operator sees the refusal on `/health`. The other option is to clamp to 144 on the server,
  which the client then honours as 144. It is stricter, but it turns a typo into a day's hold on every credit.
  Either way, name the shared constant on both sides and pin it.

### LOW-1 (PLAUSIBLE): with a depth above 1, one faulted `/tx-any` read drops that credit check to the floor

- Where: `runtime.rs:191-193` (`body_loaded`: on a word-store fault the body is returned before
  `stamp_spend_depth`). The client reads the depth only from the `/tx-any` subject (`creditWord.ts:227-228`;
  absent means the floor).
- Input: depth N > 1. The `/tx-any` read meets a transient word-store fault and returns the legacy body without
  `spendDepth`. The separate `/utxo-status` or `/spent-any` read then succeeds and serves `lock: spent`.
- Consequence: that credit check runs at depth 1, not N, so the credit can land before N blocks. It is unreachable
  at today's `"1"`.
- Status: not reproduced end to end, because the client is the tower's.
- Fix shape: either the server stamps `spendDepth` on every body of the three routes, the legacy fault body
  included (the depth depends on the var alone, not the word store, though that breaks the byte-identity the PR
  pins), or the client keeps the largest depth it has seen in the session and reads a later absent depth as
  pending. The brief describes that session rule, but `8d6d8e0b` reads the depth per call. The client half should
  decide; the server's choice is coherent.

### NOTES (no change asked)

- N1, who moves the depth: only the env var. Every stamp site passes `spend_depth(env).depth` or
  `spend_depth(&ctx.env).depth`. No request field, header or stored row reaches it. Pinned by
  `lens_only_the_var_moves_the_depth`.
- N2, flips: inside an isolate the value is fixed (a var). A smaller depth after a larger one is possible only
  across a deploy that lowers the var, while isolates of both versions answer. That only delays the depth's own
  change; it never makes it stricter than the owner set.
- N3, the edges: absent, empty, blank, `0`, a sign, a fraction, `1e2`, hex, full-width digits, NUL, and anything
  past `u32` all serve the floor and are counted once per isolate. Leading zeros and surrounding whitespace are
  accepted (`"006"` is 6, `"\t2\n"` is 2), which is harmless.
- N4: `/health.spendDepth.var` echoes the raw var. It is the operator's render value, not a secret.
- N5, the fault cell (`tools/lane-499/tracker_fault_route_ci.mjs:26-27`): it asserts `spendDepth` before the fault
  on `/tx-any` and on `/utxo-status` only, and its `legacy()` strip proves the depth is absent after the fault on
  all four paths. The cell was not run (the route tier is the captain's).
- N6, batches: a batch's word-store read is one read, so the fault is all-or-nothing. There is no "one faulted id"
  inside a decorated batch. An id the batch could not answer sits in `unknown` / `reasons` and gains nothing; every
  `answers[id]` carries `tracker` and the depth. Pinned by
  `lens_a_batch_with_an_unanswered_id_stamps_only_its_answers`.

## The two renders

| render | `SPEND_DEPTH` | judgement |
|---|---|---|
| prod `[vars]` (`crates/low-app-layer/wrangler.toml:115`) | `"1"` | the floor, today's behaviour; no owner word needed |
| beta `[env.beta.vars]` (`wrangler.toml:292`) | `"1"` | the same; set in both, intended |

## The cases written (lens only, not pushed to `lane/L7-DEPTH`)

`crates/low-app-layer/tests/spend_depth_l7_lens.rs`, 7 tests, all green against `6546ba67`:

- `lens_the_vars_edge_values`
- `lens_m1_a_depth_past_the_clients_bound_is_served_unclamped` (pins MEDIUM-1 as it stands)
- `lens_a_legacy_fault_body_gains_nothing` (all four route shapes, depths 1, 6, 144)
- `lens_a_batch_with_an_unanswered_id_stamps_only_its_answers`
- `lens_an_outpoint_with_no_word_row_carries_lock_and_depth` (`locked`, `free`, `unknown`, each with the depth)
- `lens_the_stamp_touches_only_its_objects` (idempotent; nested and non-entry `tracker` / `lock` untouched)
- `lens_only_the_var_moves_the_depth`

The patch is at the end of this file.

## Commands and exits (logs under `/Volumes/Lanes/out/L7-DEPTH-LENS/`)

All commands ran with `CARGO_TARGET_DIR=/Volumes/Lanes/targets/L7-DEPTH-LENS` and `--manifest-path workers/Cargo.toml`.

| command | result |
|---|---|
| `cargo test -p low-app-layer --test spend_depth_l7 --test spend_depth_l7_lens` | exit 0; 8 + 7 passed (`lens-tests.log`) |
| `cargo test -p low-app-layer --lib` | exit 0; 700 passed, 2 ignored, 681 s (`lib.log`). The harness moved it to the background at its 600 s cap; it was awaited, not cut. |
| `cargo test -p low-app-layer --tests --no-run`, then each of the 40 binaries run from the crate directory | every one exit 0 and `test result: ok` (`int.log`, `int-<n>.log`); 39 binaries are the PR's, the 40th is the lens file. Binaries 1 to 20 also ran in the background past the cap and were awaited. |
| `cargo test -p bsv-overlay-cloudflare --lib applayer_counters` | exit 0; 1 passed |
| `cargo clippy -p low-app-layer -p bsv-overlay-cloudflare --all-targets -- -D warnings` | exit 0 (`clippy.log`) |
| `cargo fmt --all --check` | exit 1 on drift that existed before the PR: 28 files, none of them the PR's. The only `routes.rs` in the list is `crates/overlay-cloudflare/src/routes.rs`, which the PR does not touch. Named, not fixed (`fmt.log`). |

The PR touches no migration, so no migration pin applies.

## Not verified

- The route tier and the NS3D-M1 fault cell (the captain's).
- The client half was read, not run.
- wasm32 and the deployed renders.

## The patch

```diff
diff --git a/crates/low-app-layer/tests/spend_depth_l7_lens.rs b/crates/low-app-layer/tests/spend_depth_l7_lens.rs
new file mode 100644
index 00000000..1d088257
--- /dev/null
+++ b/crates/low-app-layer/tests/spend_depth_l7_lens.rs
@@ -0,0 +1,202 @@
+//! L7-DEPTH-LENS (bsv-low #862): the lens's negative cases on PR 167, beside the lane's eight pins.
+use low_app_layer::tracker_host::runtime;
+use low_app_layer::{logic, txany};
+use serde_json::{json, Value};
+use std::collections::BTreeMap;
+
+fn tx(n: u8) -> String {
+    format!("{n:02x}").repeat(32)
+}
+
+fn status(txid: &str, spent: Option<bool>, spender: Option<&str>) -> logic::OutpointStatus {
+    logic::OutpointStatus {
+        txid: txid.into(),
+        vout: 0,
+        known: spent.is_some(),
+        spent,
+        spending_txid: spender.map(str::to_string),
+        spent_confirmed: Some(false),
+        spender_seen: Some(true),
+        spender_final: Some(true),
+        reason: None,
+        funding_seen: Some(true),
+        funding_seen_by: None,
+        evicted: logic::ServedEviction::NotRead,
+    }
+}
+
+fn answer() -> txany::TxAnyAnswer {
+    txany::decide_tx_any(
+        None,
+        None,
+        Some(&txany::TxObservation::FirstPartyPresent {
+            confirmed: false,
+            raw_hex: Some("0100".into()),
+        }),
+        txany::AbsenceCorroboration::Unknown,
+    )
+}
+
+/// The var's edges: what the server serves for each value an operator could set.
+#[test]
+fn lens_the_vars_edge_values() {
+    for (raw, want) in [
+        ("1", (1, true)),
+        ("01", (1, true)),
+        ("006", (6, true)),
+        ("\t2\n", (2, true)),
+        ("144", (144, true)),
+        ("4294967295", (4_294_967_295, true)),
+        ("4294967296", (1, false)),
+        ("\u{ff12}", (1, false)),
+        ("1 2", (1, false)),
+        ("1e2", (1, false)),
+        ("\u{0}", (1, false)),
+    ] {
+        let d = runtime::spend_depth_of(Some(raw));
+        assert_eq!((d.depth, d.from_var), want, "SPEND_DEPTH={raw:?}");
+    }
+}
+
+/// FINDING (the lens's MEDIUM-1): the server serves a well-formed depth past the client's bound
+/// (`SERVED_SPEND_DEPTH_MAX` 144 in bsv-low `chainReads.ts`), which the client reads as NOT SERVED, the floor 1.
+/// Pinned as it stands: a var of 145 is served 145, `source: "var"`, while the client credits at 1.
+#[test]
+fn lens_m1_a_depth_past_the_clients_bound_is_served_unclamped() {
+    const CLIENT_MAX: u64 = 144;
+    for raw in ["145", "1000", "4294967295"] {
+        let d = runtime::spend_depth_of(Some(raw));
+        assert!(d.from_var && d.depth > CLIENT_MAX, "{raw}: {d:?}");
+        let h = runtime::spend_depth_health_json(Some(raw));
+        assert_eq!(h["source"], "var", "{h}");
+        let mut body: Value = serde_json::from_str(&txany::tx_any_body(&tx(1), &answer())).unwrap();
+        runtime::decorate(&mut body, Some(&tx(1)), &BTreeMap::new());
+        runtime::stamp_spend_depth(&mut body, d.depth);
+        assert_eq!(body["spendDepth"], json!(d.depth));
+    }
+}
+
+/// The legacy bodies a word-store fault serves (no `tracker`, no `lock`) gain nothing on every route shape,
+/// whatever the depth.
+#[test]
+fn lens_a_legacy_fault_body_gains_nothing() {
+    let single = txany::tx_any_body(&tx(1), &answer());
+    let batch =
+        txany::tx_any_batch_body(&[(tx(1), answer()), (tx(2), answer())], &[(tx(3), "fault")]);
+    let entries = [
+        status(&tx(5), Some(true), Some(&tx(1))),
+        status(&tx(5), Some(false), None),
+        status(&tx(6), None, None),
+    ];
+    for raw in [
+        single,
+        batch,
+        logic::utxo_status_body(&entries),
+        txany::spent_any_body(&entries, &[false; 3]),
+    ] {
+        for depth in [1, 6, 144] {
+            let mut v: Value = serde_json::from_str(&raw).unwrap();
+            runtime::stamp_spend_depth(&mut v, depth);
+            assert_eq!(
+                v.to_string(),
+                raw,
+                "a legacy body gained a key at depth {depth}"
+            );
+        }
+    }
+}
+
+/// A batch whose store read held no row for any id: every answer carries `tracker: unknown` and the depth; the
+/// ids the batch could not answer (`unknown`, `reasons`) gain nothing.
+#[test]
+fn lens_a_batch_with_an_unanswered_id_stamps_only_its_answers() {
+    let mut v: Value = serde_json::from_str(&txany::tx_any_batch_body(
+        &[(tx(1), answer())],
+        &[(tx(3), "fault")],
+    ))
+    .unwrap();
+    runtime::decorate(&mut v, None, &BTreeMap::new());
+    runtime::stamp_spend_depth(&mut v, 3);
+    assert_eq!(v["answers"][tx(1)]["tracker"]["word"], "unknown", "{v}");
+    assert_eq!(v["answers"][tx(1)]["spendDepth"], 3, "{v}");
+    assert!(
+        v.get("spendDepth").is_none(),
+        "the batch's top level has no tracker: {v}"
+    );
+    assert_eq!(v["unknown"], json!([tx(3)]), "{v}");
+    assert_eq!(v["reasons"], json!({ tx(3): "fault" }), "{v}");
+}
+
+/// `/utxo-status` entries for outpoints with no word row (the store answered, empty): each carries a `lock`
+/// (`locked` with an unknown spender word, `free`, `unknown`) and the depth beside it.
+#[test]
+fn lens_an_outpoint_with_no_word_row_carries_lock_and_depth() {
+    let entries = [
+        status(&tx(5), Some(true), Some(&tx(9))),
+        status(&tx(5), Some(false), None),
+        status(&tx(6), None, None),
+    ];
+    let mut v: Value = serde_json::from_str(&logic::utxo_status_body(&entries)).unwrap();
+    let none = BTreeMap::new();
+    runtime::decorate(&mut v, None, &none);
+    runtime::decorate_outpoints(&mut v, &none, "free", "host");
+    runtime::stamp_spend_depth(&mut v, 2);
+    let words: Vec<_> = (0..3).map(|i| v[i]["lock"]["word"].clone()).collect();
+    assert_eq!(
+        words,
+        vec![json!("locked"), json!("free"), json!("unknown")],
+        "{v}"
+    );
+    for i in 0..3 {
+        assert_eq!(v[i]["spendDepth"], 2, "entry {i}: {v}");
+        assert_eq!(v[i]["fundingWord"]["word"], "unknown", "entry {i}: {v}");
+    }
+}
+
+/// The stamp is idempotent and touches only the named objects: stamping twice is stamping once, a nested
+/// `tracker` or `lock` (not a subject, an answer or an entry) is not stamped, a top-level `lock` on an object is
+/// not an outpoint entry, and a scalar body is untouched.
+#[test]
+fn lens_the_stamp_touches_only_its_objects() {
+    let mut v =
+        json!({"tracker": {"word": "seen"}, "nested": {"tracker": {}, "lock": {}}, "lock": {}});
+    runtime::stamp_spend_depth(&mut v, 4);
+    let once = v.clone();
+    runtime::stamp_spend_depth(&mut v, 4);
+    assert_eq!(v, once);
+    assert_eq!(v["spendDepth"], 4);
+    assert!(v["nested"].get("spendDepth").is_none(), "{v}");
+    let mut a = json!([{"tracker": {}}, {"lock": {}}, 7, null]);
+    runtime::stamp_spend_depth(&mut a, 4);
+    assert_eq!(
+        a,
+        json!([{"tracker": {}}, {"lock": {}, "spendDepth": 4}, 7, null])
+    );
+    let mut s = json!("x");
+    runtime::stamp_spend_depth(&mut s, 4);
+    assert_eq!(s, json!("x"));
+}
+
+/// No request input reaches the depth: the only reader is the env var (`spend_depth(env)`), and every stamp site
+/// passes `spend_depth(..).depth`, never a value from the request or a stored row.
+#[test]
+fn lens_only_the_var_moves_the_depth() {
+    let rt = include_str!("../src/tracker_host/runtime.rs");
+    let routes = include_str!("../src/routes.rs");
+    for src in [rt, routes] {
+        for (i, _) in src.match_indices("stamp_spend_depth(") {
+            let call = &src[i..i + src[i..].find(");").unwrap()];
+            if call.contains("value: &mut Value") {
+                continue; // the definition
+            }
+            assert!(
+                call.contains("spend_depth(env).depth")
+                    || call.contains("spend_depth(&ctx.env).depth"),
+                "{call}"
+            );
+        }
+    }
+    let reader = &rt[rt.find("pub fn spend_depth(env").unwrap()..];
+    let reader = &reader[..reader.find("\n}\n").unwrap()];
+    assert!(reader.contains("env.var(SPEND_DEPTH_VAR)"), "{reader}");
+}
```
