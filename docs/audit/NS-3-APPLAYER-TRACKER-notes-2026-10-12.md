# NS-3-APPLAYER-TRACKER readiness, 2026-10-12
Status: BLOCKED at the compact persistence contract; the lane is not complete.
Host `box`; existing branch worktree `~/bsv/wt-ns-3-tracker` used because `wt-overlay-box` has unrelated edits; no deploy or spend.
Step 1 remains `a4304da` (dependency) + `9165e16` (first-launch notes); tracker exactly 0.2.2, one bsv-rs 0.5.0; no lock changes.
Step 2 `c59c0a2`: `src/tracker_host/mod.rs`, Headers via CHAINTRACKS snapshots, staged CourierProofs, BroadcasterHints and WorkerClock.
Headers: header_response/headers stage `/findHeaderHexForHeight` + `/getPresentHeight`; missing, behind, malformed and transport fault cannot check; changing tip fails closed.
ProofFetcher: execute_reask asks Arcade only for held/submitted subjects, then WoC, Bitails, BananaBlocks; tsc_proof reduces SDK paths, Headers.check alone accepts them.
BananaBlocks route/body probed read-only: https://bananablocks.com/api/docs; `/tx/{txid}/proof`, fixture `tests/fixtures/ns3/banana-tsc.json`, height 956597, root d204a3ac…47fcd57.
HintSource: Verdict::from_arc_body/into_hint keeps metadata, source and seconds; broadcaster words cannot write chain words. Clock: Date::now milliseconds / 1000.
Step 3 `4cc64ce`: `tracker_host/served.rs` fields/transaction/spenders render State; helpers only, production `/tx-any`, `/spent-any`, `/utxo-status` and owed are NOT wired.
Synthetic legacy-body pin before: confirmed=true,height=null,claimedHeight=10; after: those fields identical, word=seen,tier=hint,verifyPath=null; this disagreement is a lens finding.
Checked Mined renders the path, root, height and header hash; stale/suspect renders no verifyPath; the complete height tuple is nested under tracker to preserve the old height field.
Persistence spike `a2f3d43`: `tests/support/tracker_stored.rs` Record::replay/rechecked and `tests/tracker_stored.rs`; strictly test-only.
At v0.2.2, src/state.rs exposes new/step/getters, no State serde or restore; CheckedProof has no trusted deserializer (README and compile-fail docs).
A journal preserves the machine but restores it by replaying historical inputs/snapshots on reads; that violates the requested lookup-only rule. No such production read was shipped.
Decision requested: permit a persisted tracker-produced wire view with proof-only restore/recheck, or obtain a compact snapshot/restore boundary in the pinned crate; do not manufacture words.
`tx_words` CREATE/UPSERT is pinned under SQLite in the spike only; no production migration or submit/callback/courier/retire write hooks shipped.
Step 5 NOT IMPLEMENTED: derive_owed_rows remains legacy; the named D3 SEEN+spenderFinal arm stays; spent/pending/lost tracker inputs await persistence.
`a2f3d43` also imports NS-2's five-body production-builder fixture (results, refund-view, recovery-view, refund-backups, owed; fleet-shaped rows and three page offsets), byte-identical GREEN.
Clean `cargo test -p low-app-layer`: 658 passing; all-target clippy -D warnings and storage-ownership GREEN; two redundant format borrows removed, intentional unit transport error documented.
Separate neutered worktree/target: five adapter pins and one renderer pin RED; clean counterparts GREEN; six adapter pins, two renderer pins and three persistence pins pass.
Baseline `make ci-d1-budget` + `make -o ci-d1-budget ci-route`: GREEN; utxo 8/0/1, pots 8/0/1, results:view 191/0/6, owed 352/18/26 (reads/writes/statements); NOT tracker-integrated figures.
Final wasm32 build and worker-build --release GREEN, wasm-bindgen 0.2.126, Node 22; logs `/tmp/ns3-{native-tests,clippy,d1-budget,route,wasm,worker-build}.log`.
New Rust files and changed additions formatted; whole-crate cargo fmt --check reports existing debt outside these hunks; no mass formatting. Temporary targets/worktree removed at end.
Unverified: compact persistence decision, production route tier/new budget delta, write-event delivery, owed integration, from-scratch and delta lenses, live deployment; next finish steps 3–6 after the boundary is settled.
