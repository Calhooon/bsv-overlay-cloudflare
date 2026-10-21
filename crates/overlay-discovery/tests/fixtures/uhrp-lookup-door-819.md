# A misspelt key in an `ls_uhrp` lookup answers the store: the reference refuses it at the door

**Class.** A public lookup is a door: a caller's query either names a filter the service knows or is refused. The reference's `ls_uhrp` checks every key of the query against an allow-list, requires a filter and bounds the page before it reads storage. The engine's `ls_uhrp` deserializes the query with serde's default, which drops a key it does not know, so a misspelt filter is an empty filter and the store answers; the string `"findAll"` is a public find-all. The six tests: an unfiltered answer scales with the store, not with the caller's use (T1), and a typo is a scan of the store (T3).

**Witness.**

| observation | source |
|---|---|
| the reference refuses a question or query that is not a plain object, and any key that is not a string, is prototype-sensitive or is outside the allow-list, with `Invalid lookup query: <message>` | `[SRC] ts-stack@9262cac66 packages/overlays/topics/src/shared/queryValidation.ts:6-10,16-44` |
| the reference's `ls_uhrp` allow-list is `outpoint`, `uhrpUrl`, `expiryTime`, `hostIdentityKey`, `limit`, `offset`; an outpoint answers that one output; otherwise one of `uhrpUrl`, `expiryTime`, `hostIdentityKey` is required; `limit` 1 to 200, default 50; `offset` 0 to 100,000; the filter is built field by field, never forwarded; there is no find-all | `[SRC] ts-stack@9262cac66 packages/overlays/topics/src/uhrp/UHRPLookupService.ts:81-140` |
| the reference's tests of that door: the lookup's boundaries, the pagination, the shared validation | `[SRC] ts-stack@9262cac66 packages/overlays/topics/src/__tests__/lookup-query-security.test.ts:44-133,200-225`; `src/__tests__/uhrp-pagination.test.ts:16-32`; `src/shared/__tests__/queryValidation.security.test.ts:24-61,71-80`. `src/__tests__/uhrp.test.ts` at the pin tests the topic manager only (`:132-250`); it holds no lookup case |
| the engine answers the string `"findAll"` with every unexpired record | `[SRC] Calhooon/bsv-overlay-cloudflare@c6b5bff69 crates/overlay-discovery/src/uhrp/lookup_service.rs:187-202` |
| the engine deserializes any other query into `UHRPQuery` with serde's default (no `deny_unknown_fields`), so an unknown key is dropped; it refuses `limit` 0 and nothing else of its own | `[SRC] lookup_service.rs:204-205,221-227`; `crates/overlay-discovery/src/uhrp/storage.rs:47-106` (the struct, whose comment at `:49-53` records the earlier form of this fault: the camel-case keys before the rename "silently deserialized as `None`, so every filtered query fell through to an unfiltered find_all") |
| the engine's keys are `findAll`, `uhrpUrl`, `hostIdentityKey`, `limit`, `skip`, `sortOrder`, `includeExpired`, `now_unix_seconds`; `outpoint`, `expiryTime` and `offset` are not among them and are dropped | `[SRC] storage.rs:54-106` |
| an empty filter answers every unexpired record: in memory with no default limit, on D1 up to the lookup window of 1,000 | `[SRC] storage.rs:309-364`; `crates/overlay-cloudflare/src/d1_discovery.rs:830-876`; `crates/overlay-cloudflare/src/proof_window.rs:11` (`LOOKUP_WINDOW_LIMIT = 1000`) |
| the beta overlay, 2026-10-21 12:25Z, the keeper's reads: `uhrpUrl` U answered 2 outputs; a misspelt key answered the find-all, 1,000 outputs | `[X]` `docs/p0/l2-storage-soak-harness.md:81`; `docs/RULINGS-2026-10-12.md` the 12:3xZ entry of 2026-10-21 |
| the beta read "5 outputs with `limit` 5" named in the lane's brief | `[X]`: the keeper's third probe of 12:25Z, recorded in `docs/p0/l2-storage-soak-harness.md` (the paid run's section) at the landing of this scenario; not re-read (the lane makes no live call) |

**Machine.** The overlay (the lookup's door).

**Initial chain.** None: the lookup is a read of the store, and no step reads the chain. A store holding N = 60 UHRP adverts, every one by host key K = `03` followed by `22` repeated 32 times (the reference test's `CERTIFIER_KEY`, `lookup-query-security.test.ts:14`), every expiry 0 (never expires, so the engine's expiry filter hides none), content length 1. Advert 1 is the outpoint `ab` repeated 32 times, output 1 (the test's `TXID`, `:16`), for the URL U = `XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m` (the Base58Check of thirty-two `0x01` bytes under the prefix `ce00`, the tests' `getURLForHash(Array(32).fill(1))`, `uhrp-pagination.test.ts:15`, `lookup-query-security.test.ts:105`). Adverts 2 to 60 are outpoint `<i as 64 hex digits>.0`, each for the URL of thirty-two bytes of value i.

**Steps.**

1. Look up `ls_uhrp` with the misspelt key: `{"uhrp_url": U}` (vector V1).
2. Look up `ls_uhrp` with the empty object `{}` (V2).
3. Look up `ls_uhrp` with the string `"findAll"` (V3).
4. Look up `ls_uhrp` with `{"uhrpUrl": U}` (V4).
5. Look up `ls_uhrp` with `{"uhrpUrl": U, "limit": 0}` (V5).
6. Look up `ls_uhrp` with an unknown key beside a valid `uhrpUrl`: `{"uhrpUrl": U, "limit": 200, "$where": "bad"}` (V6, the reference's own case).
7. Look up `ls_uhrp` with a query that is an array: `[]` (V7).

**Expected observations per machine.**

| machine | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|
| The overlay, the reference at `ts-stack@9262cac66` `[SRC]` | refused `Invalid lookup query: unexpected field uhrp_url`; storage not read | refused `Lookup must specify either outpoint, or at least one of (uhrpUrl, expiryTime, hostIdentityKey)`; storage not read | refused `Invalid lookup query: query must be an object` | answers 1 output | refused `Invalid lookup query: limit must be an integer from 1 to 200`; storage not read | refused `Invalid lookup query: unexpected field $where`; storage not read | refused `Invalid lookup query: query must be an object` |
| The overlay, the engine at `c6b5bff69` (the runner's answers `[X]`) | answers 60 outputs, the store | answers 60 outputs, the store | answers 60 outputs, the store | answers 1 output | refused `invalid query: limit must be positive` | answers 1 output, `$where` dropped | refused `invalid query: invalid length 0, expected struct UHRPQuery with 8 elements` |
| The overlay, the engine on beta, 2026-10-21 12:25Z (the keeper's reads) | answers 1,000 outputs, the lookup window `[X]` | not read `[D]` | not read `[D]` | answers 2 outputs `[X]` | not read `[D]` | not read `[D]` | not read `[D]` |

**The vectors.** The reference's validation cases at the pin, put to the store above. Each input is the `query` of a `{"service": ..., "query": ...}` question as JSON; the service is `ls_uhrp` unless the row names another. The verdict is the reference's: `refused` (the sentence, or the part of it the test matches) or `answers n` on the Initial chain's store (n read from the filter the code builds at `UHRPLookupService.ts:126-139`; the tests run on a mock that answers one row, so a count on this store is `[SRC]` by the code, not by a test's assertion). A source of `code` names the line and no test asserts the case. These are the rows a runner replays (`corpus/runners/uhrp-lookup/`).

| id | step | service | query | the reference's verdict | the reference's sentence | source at `ts-stack@9262cac66` |
|---|---|---|---|---|---|---|
| V1 | 1 | ls_uhrp | `{"uhrp_url":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m"}` | refused | `unexpected field uhrp_url` | code `queryValidation.ts:35-39`; the test of the same rule `lookup-query-security.test.ts:45-55` |
| V2 | 2 | ls_uhrp | `{}` | refused | `must specify either outpoint` | `lookup-query-security.test.ts:126`; code `UHRPLookupService.ts:120-124` |
| V3 | 3 | ls_uhrp | `"findAll"` | refused | `query must be an object` | `queryValidation.security.test.ts:42-46`; code `queryValidation.ts:26-29` |
| V4 | 4 | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m"}` | answers 1 | | `lookup-query-security.test.ts:103-116`; code `UHRPLookupService.ts:98-104,128-139` |
| V5 | 5 | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","limit":0}` | refused | `limit must be an integer from 1 to 200` | `uhrp-pagination.test.ts:23-27`; `lookup-query-security.test.ts:78-80` |
| V6 | 6 | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","limit":200,"$where":"bad"}` | refused | `unexpected field $where` | `uhrp-pagination.test.ts:28-32` |
| V7 | 7 | ls_uhrp | `[]` | refused | `query must be an object` | `queryValidation.security.test.ts:42-46` |
| V8 | | ls_uhrp | `null` | refused | `query must be an object` | `queryValidation.security.test.ts:42-46` |
| V9 | | ls_other | `{}` | refused | `Lookup service not supported` | `queryValidation.security.test.ts:37-39`; code `queryValidation.ts:24` |
| V10 | | ls_uhrp | `{"hostIdentityKey":"032222222222222222222222222222222222222222222222222222222222222222","$where":"while(true){}"}` | refused | `unexpected field $where` | `lookup-query-security.test.ts:45-55` |
| V11 | | ls_uhrp | `{"hostIdentityKey":"032222222222222222222222222222222222222222222222222222222222222222","expiryTime":123,"limit":25,"offset":4}` | answers 0 | | `lookup-query-security.test.ts:57-73` (the filter `{hostIdentityKey, expiryTime: 123}`, skip 4, limit 25; no advert has expiry 123) |
| V12 | | ls_uhrp | `{"hostIdentityKey":"032222222222222222222222222222222222222222222222222222222222222222","limit":0}` | refused | `limit must be an integer from 1 to 200` | `lookup-query-security.test.ts:78-80` |
| V13 | | ls_uhrp | `{"outpoint":"abababababababababababababababababababababababababababababababab.1.trailing"}` | refused | `canonical outpoint` | `lookup-query-security.test.ts:81-83` |
| V14 | | ls_uhrp | `{"outpoint":"ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB.1"}` | answers 1 | | `lookup-query-security.test.ts:88-95` |
| V15 | | ls_uhrp | `{"outpoint":"abababababababababababababababababababababababababababababababab.2"}` | answers 0 | | `lookup-query-security.test.ts:97-100` |
| V16 | | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","hostIdentityKey":"032222222222222222222222222222222222222222222222222222222222222222","expiryTime":123}` | answers 0 | | `lookup-query-security.test.ts:103-116` |
| V17 | | ls_uhrp | `{"uhrpUrl":"not-uhrp"}` | refused | `uhrpUrl is invalid` | `lookup-query-security.test.ts:119` |
| V18 | | ls_uhrp | `{"hostIdentityKey":"02deadbeef"}` | refused | `compressed public key` | `lookup-query-security.test.ts:120` |
| V19 | | ls_uhrp | `{"hostIdentityKey":"021111111111111111111111111111111111111111111111111111111111111111"}` | refused | `valid public key` | `lookup-query-security.test.ts:121-125` |
| V20 | | ls_uhrp | `{"hostIdentityKey":"032222222222222222222222222222222222222222222222222222222222222222","__proto__":"x"}` | refused | `unexpected field __proto__` | `lookup-query-security.test.ts:218-223` |
| V21 | | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","limit":200,"offset":0}` | answers 1 | | `uhrp-pagination.test.ts:16-22` |
| V22 | | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","limit":201}` | refused | `limit must be an integer from 1 to 200` | `uhrp-pagination.test.ts:23-27` (the test matches any throw; the sentence is the code's, `queryValidation.ts:71-73`) |
| V23 | | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","limit":-1}` | refused | `limit must be an integer from 1 to 200` | `uhrp-pagination.test.ts:23-27` |
| V24 | | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","limit":1.5}` | refused | `limit must be an integer from 1 to 200` | `uhrp-pagination.test.ts:23-27` |
| V25 | | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","limit":"200"}` | refused | `limit must be an integer from 1 to 200` | `uhrp-pagination.test.ts:23-27` |
| V26 | | ls_uhrp | `{"hostIdentityKey":"032222222222222222222222222222222222222222222222222222222222222222"}` | answers 50 | | code `UHRPLookupService.ts:126` (the default limit 50) |
| V27 | | ls_uhrp | `{"hostIdentityKey":"032222222222222222222222222222222222222222222222222222222222222222","offset":100001}` | refused | `offset must be an integer from 0 to 100000` | code `UHRPLookupService.ts:127` |
| V28 | | ls_uhrp | `{"findAll":true}` | refused | `unexpected field findAll` | code `UHRPLookupService.ts:82-89`, `queryValidation.ts:35-39` |
| V29 | | ls_uhrp | `{"uhrpUrl":"XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m","includeExpired":true}` | refused | `unexpected field includeExpired` | code `UHRPLookupService.ts:82-89`, `queryValidation.ts:35-39` |
| V30 | | ls_uhrp | `{"constructor":"pollute"}` | refused | `unexpected field constructor` | `queryValidation.security.test.ts:48-60`; code `queryValidation.ts:6,37` |

Not in the vectors: the reference's cases a JSON body cannot carry (a symbol key, an inherited or accessor property, `queryValidation.security.test.ts:48-60`, `lookup-query-security.test.ts:200-216`), and the admission cases of `lookup-query-security.test.ts:135-156` (not a lookup).

**Replay.** `corpus/runners/uhrp-lookup/run.sh` builds the engine's `UHRPLookupService` over its in-memory `MemoryUHRPStorage` from a detached worktree of `Calhooon/bsv-overlay-cloudflare` at `c6b5bff69` (the engine's own `Cargo.lock`), seeds the Initial chain's store, reads the vectors from this file and prints one line a vector: the verdict word (`PASS` where the engine's verdict and count equal the reference's, `FAIL` otherwise), the vector, the step, the machine, the reference's expectation, the engine's answer. The answers of record: `corpus/runners/results/scenarios-uhrp-lookup.txt` `[X]`, the run of 2026-10-21: 10 `PASS`, 20 `FAIL`. The ten that agree are V4, V21 (one output for U), and V5, V7, V8, V9, V12, V23, V24, V25, refused by the engine too, in its own sentences (serde's, or `limit must be positive`). The twenty that differ: a dropped key answers the store or ignores a filter (V1, V6, V10, V11, V13 to V16, V20, V27 to V30), the empty query and `"findAll"` answer the store (V2, V3), an invalid value is a filter that matches nothing rather than a refusal (V17 to V19), and the engine has no upper bound and no default for `limit` (V22, V26). The memory store has no default limit where D1 has the window of 1,000 (`d1_discovery.rs:869-873`), so a find-all answers 60 here and 1,000 on beta; the door is the same.

**The fix.** The owner's ruling of 2026-10-21 13:1xZ (`docs/RULINGS-2026-10-12.md`, option (a) of the 12:3xZ entry): the reference's door in the engine, LOW's lane bsv-low #819 (`deny_unknown_fields` on the query, the allow-list, a filter required with the reference's sentence, the reference's limits, find-all out of the public lookup, an operator's listing as an admin read behind the door). The planned fix: LOW's queued brief, bsv-low main `24479e54` `docs/lanes/W2/UHRP-819-DOOR.md` `[D]` (cited from the keeper's word; not read by this lane). The fix's sha: none yet; cited here when LOW's PR lands, and the runner re-run at it.

**Matrix cells.** Nearest `overlay.malformed_input`; a candidate event row (a lookup query outside the door) for the matrix lane.

**Six tests.** T1 (an unfiltered answer grows with the store, not with the caller's use); T3 (a typo is a scan of the store); T6 (the replay is the runner above).

Cited by the reading issue #183 (`[reading] The ls_uhrp door`, 2026-10-21 13:40Z); the rows are of record from that citation, the fix line `[D]` until LOW's PR sha replaces `24479e54`.
