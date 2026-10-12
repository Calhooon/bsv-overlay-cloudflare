# NS-3-APPLAYER-TRACKER readiness, 2026-10-12
Status: CODE COMPLETE, ready for the independent from-scratch and delta lenses; no deploy or spend.
Host box; branch lane/ns-3-applayer-tracker in ~/bsv/wt-ns-3-tracker (the branch already occupied this worktree; wt-overlay-box has another lane).
Existing steps 1–2: a4304da/9165e16 (tracker =0.2.2, one SDK 0.5.0); c59c0a2 (four adapters); 4cc64ce/a2f3d43/9013ed5 (renderer, compatibility fixture and superseded persistence blocker).
Step 3 3cccc0a: routes.rs wires served::body into single/batch tx-any, spent-any, utxo-status and owed spender bodies.
Step 4 798e06d: tracker_host/stored.rs + runtime.rs, schema.rs, overlay d1/mod.rs, tracker_events.rs and the producer/flush hooks; compact persistence is the captain's K11 replacement boundary.
Step 5 220ed5c: owed.rs spender_mined, tracker_confirmed_by_other, spender_coin_word and derive_owed_rows consume tracker views; production OwedInputs always carries the views.
Step 6: the gate commit adds indexed one-bind fleet lookup, Worker route pins, bounded WRITE-clock reasks, cached raw reuse, atomic race guards and these notes.
Headers: HeaderSnapshot/header_response/headers over CHAINTRACKS /findHeaderHexForHeight + /getPresentHeight; 404, behind, malformed, transport and changing tip fail closed (cannotCheck, no usable path).
ProofFetcher: CourierProofs/execute_reask/tsc_proof; Arcade first for held/submitted subjects, then WoC, Bitails, BananaBlocks; only Headers.check and State.step(Evidence::Proof) promote a word.
HintSource: BroadcasterHints, Verdict::from_arc_body/into_hint, live Arcade GET on callback triggers, admission accepting hints and courier Seen with source/time; no broadcaster writes a chain verdict.
Clock: WorkerClock Date::now()/1000; retire Tick and oldest-due owed WRITE passes (two subjects, existing time budget); chain words are never age-polled.
Pins: tracker_host.rs six fake-port cells; tracker_served.rs three body cells; tracker_compact.rs eight lookup, recheck, SQLite race, indexed fleet, event and clock cells; the owed disagreement cell.
The Worker pins inside tools/lane-499/d1_budget_route_ci.mjs exercise single/batch tx-any, utxo-status, expired spent-any pointers, unavailable headers and zero courier calls on stored reads.
Served: transaction word/tier/verifyPath plus tracker.{word,tier,height,verifyPath}; spenderWord holds that tuple on the spender; owed adds coinWord (spent/pending/lost/suspended); all legacy fields/types survive.
Captured fixture before confirmed=true,height=null,claimedHeight=10; after those identical plus word=seen,tier=hint,tracker.height=null,verifyPath=null,hintSource=woc,hintAt=123.
Lens disagreements: that confirmed=true/Seen case; legacy spentConfirmed=true beside Seen (owed claim waits); legacy spentConfirmed=false beside checked Mined(10) (compact spender fixture). Each old flag remains visible.
The D3 SEEN + spenderFinal verdict-publication arm in the results producer remains; None spender_words is the pre-tracker test oracle, never the production judgement.
Migration 190 / byte-identical app catch-up CREATE tx_words; columns txid,word,tier,height,proof_root,proof_height,proof_hex,header_hash,hint_source,hint_at,updated_at,legacy_body. proof_hex enables re-seeding; legacy_body retains raw/old fields without another courier.
Writes: successful submit, pending acceptance, live SEEN latch, callback trigger/proof, verified courier proof and retire pass; APP_LAYER /internal/tx-word courier; absent-row fallback once; owed WRITE reasks. Delivery is logged and bounded; live delivery/isolate-loss recovery remains unverified.
Reads: one indexed json_each bind for the subjects; no journal; only Mined re-seeds one retained proof and executes the tracker's Recheck against one current header. Mismatch serves Stale; unavailable headers serve cannotCheck and no verifyPath.
Race pin: late concurrent hints cannot demote chain/Seen rows; checked proof beats a newer hint; updated_at remains monotonic and newer hint metadata survives.
Gate: 668 native tests, all-target clippy -D warnings, five fleet production-body compatibility pins at three offsets, ownership and changed-hunk formatting GREEN; whole-crate fmt still reports pre-existing debt. Neutered adapter/renderer/recheck/owed/race copies RED; five Worker word/path pins RED then GREEN.
Budget reads/writes/statements: utxo 8/0/1 (with spender pin maximum 8/0/2); pots 8/0/1; results:view 191/0/6; owed first 352/18/26, served 17/0/2; submit 0/13/8, all original ceilings unchanged. Full ci-d1-budget + ci-route GREEN; final wasm32 and worker-build --release GREEN, bindgen 0.2.126 / Node 22; logs /tmp/ns3-*.log.
Unverified: from-scratch and delta lenses, live write-hook delivery/recovery and live deployment. The lookup route tier is verified locally. Next: lens the money judgement, fold findings, then promotion by the owner; temporary target and neutered worktree removed.

### Fold M1: captain's D1/#602 ruling
Courier mempool presence is the live-race hint `announced`, never broadcaster `seen`.
bsv-tracker 0.2.2 has no `HintStatus::Announced`, so this host writes `Unknown` with the courier source and time, without promotion.
K-ask: add `HintStatus::Announced` as a non-promoting courier presence hint in bsv-tracker; keep broadcaster Accepted/Seen distinct.
Captain follow-up in bsv-low: words design §2d and S11 must say courier `announced` (0.2.2 fallback Unknown), broadcaster SEEN only.
