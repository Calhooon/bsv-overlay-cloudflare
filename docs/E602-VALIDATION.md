# E602 validation evidence

Base: `origin/main` = `71aa1e7d2d2cf7eeba4143f4cb6d9a4f6a2e8d62`, before production edits.
The original `e602_mempool_conflict_never_readmits` integration test used the base API's `classify_presence(Some(false), Some(true))` and failed with `mempool conflict readmitted: Ok(Accepted("subject"))` (exit 101, `/tmp/e602-red.log`). The committed pin names the same observation with the new height-bearing type.
The source pins can read the base using `E602_BASE_SOURCE_DIR`: extract `routes.rs` and `proof_fetcher.rs` from that revision into a directory and run `e602_terminal_race` with that environment variable.
The latch pin fails against the base's unconditional latch (`/tmp/e602-red-latch.log`); the courier-body pin fails because both adapters discarded the body (`/tmp/e602-red-courier.log`); the retire trigger pin fails because the call is absent (`/tmp/e602-red-convergence-wiring.log`). Each exits 101.
Those three are source-wiring REDs, not claims of native execution of Worker HTTP or D1 APIs. The mempool RED executes the actual base pure function.
`e602_convergence` executes the actual shadow storage trait over real SQLite with all shipped migrations: evict, restore, stamp readmission, then re-evict on a bound confirmed competitor. Mempool and wrong-input competitors keep the pot. The production candidate SQL selects a retired, readmitted pot and its stored BEEF is parsed into the actual subject bytes.
The SQLite convergence pin was not executed behaviorally against the base, which has no native convergence seam. Its base evidence is the missing retire trigger above.
Final worker test gate: 690 unit tests and 8 integration tests passed, one existing unit test ignored (`/tmp/e602-tests-final.log`). Engine code is untouched.
Native all-target and wasm32 lib Clippy, warnings denied: passed (`/tmp/e602-clippy-native-final.log`, `/tmp/e602-clippy-wasm-final.log`).
Root `cargo fmt --all -- --check`: passed. The additional workers-workspace check fails on extensive pre-existing formatting, including untouched `low-app-layer` files (`/tmp/e602-fmt-workers.log`); no repository-wide reformat was made.
Storage ownership: 688 references passed the repository's `scripts/check-storage-ownership.py`.
Route gate split at the successful prerequisite: `LANE_BASE=16020 make ci-route` passed all ci-d1-budget cells, then was terminated during main-tier startup and cleaned up (`/tmp/e602-route.log`). `LANE_BASE=16020 make -o ci-d1-budget ci-route` passed the remaining tier, exit 0 (`/tmp/e602-route-main.log`). Each foreground command is bounded to 570 seconds plus 15 seconds teardown.
The source pins normalize whitespace and assert nonzero, exact occurrences. The fixture and pure-function pins are in `crates/overlay-cloudflare/tests/e602_*.rs`.
The captain's from-scratch lens, LOW's `make ci`, beta deployment, and `spec-admit-fast-join-refused` remain outside this lane's validation. Nothing was pushed, deployed or spent.
