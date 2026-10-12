# NS-3-APPLAYER-TRACKER readiness, 2026-10-12
Status: WALLED at the step-1 wasm prerequisite; steps 2 through 6 are outstanding.
Branch `lane/ns-3-applayer-tracker`, fetched base `ec83281`; no deploy or spend.
Step 1 commit: `a4304da`, app-layer dependency exactly `bsv-tracker = "=0.2.2"`.
Files: `crates/low-app-layer/Cargo.toml`, `workers/Cargo.lock`, `tests/tracker_dependency.rs` in that crate.
`cargo tree --manifest-path workers/Cargo.toml -p low-app-layer -i bsv-rs`: one SDK, 0.5.0, including tracker 0.2.2.
Lock delta: tracker and its dependency edge only; no existing version changed. CLI checked: wasm-bindgen 0.2.126.
Native `cargo test --manifest-path workers/Cargo.toml -p low-app-layer`: GREEN.
Named pin `ns3_dependency_uses_sdk_050_and_checked_evidence`: GREEN; real SDK path passed directly to the tracker.
Pin evidence: Arcade MINED at height 10 leaves Built; a proof checked by Headers writes Mined(10), root and retained header.
Changed-file rustfmt, `git diff --check`, and app-layer all-target clippy with `-D warnings`: GREEN.
wasm32: NOT RUN; every sampled one-minute load before this checkpoint exceeded 24 (24.58 to 28.46; last 24.77).
The load rule also defers `make ci-route` and `make ci-d1-budget`; no budget figures were measured.
Headers adapter: NOT IMPLEMENTED; must preserve binding transport faults, missing/behind headers and fresh snapshot semantics.
ProofFetcher adapter: NOT IMPLEMENTED; Arcade only for held/submitted subjects, then all three courier fallbacks.
HintSource adapter: NOT IMPLEMENTED; Verdict::from_arc_body emits hints, never checked evidence.
Clock adapter: NOT IMPLEMENTED; Worker Date::now() must supply seconds as required by Clock::now.
Served fields: before = deployed producers; after = identical producers, with no new word/tier/verifyPath fields yet.
Captured route-body before/after comparison and new adapter/route RED/GREEN pins: NOT RUN.
Five production-body compatibility pins: absent on fetched main; found on origin/lane/ns-2-d1bound, not imported.
tx_words migration and admission/callback/proof/retire write events: NOT IMPLEMENTED.
Persistence constraint: State and CheckedProof have no trusted deserializer; stored raw proof inputs need a Headers check.
Owed arm retained unchanged: D3 SEEN+final; existing `a_seen_and_final_settle_classifies_with_no_proof_through_the_real_sql` passes.
No integrated tracker-versus-legacy served fixture disagreements measured; MINED-hint evidence boundary is pinned above.
Next: prove wasm under load 24, then adapters, additive served words, write-time persistence, owed inputs and full gates/lenses.
