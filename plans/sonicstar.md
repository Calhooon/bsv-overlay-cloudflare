# Sonicstar Plugin Implementation Plan

Status: draft for review.
Owner: John.
Source files received: `ruth/sonicstarTopic.ts`, `ruth/sonicstarLookup.ts`, `ruth/sonicstarProtocol.ts`.
Live reference endpoints: `https://sonicstar.net/api/overlay-parity/{admit,lookup,docs}`.

## 1. Goal

Add a `sonicstar` plugin to the `overlay-discovery` crate that mirrors Ruth's TypeScript implementation byte for byte, plus the matching D1 storage and Cloudflare Worker wiring. The plugin admits SonicStar Song Source Protocol (`sssp`) outputs into the `tm_sonicstar` topic and exposes the `ls_sonicstar` lookup service.

This is the first overlay-discovery plugin in this repo that uses bare `OP_RETURN` rather than `PushDrop`. Every other plugin (`ship`, `slap`, `uhrp`, `agent`, `dm_delegation`) decodes `PushDrop` fields. Sonicstar parses a single `OP_RETURN <push>` where the push is a UTF-8 JSON document.

## 2. On-wire format (recap)

```
locking_script := OP_RETURN <push: utf-8 JSON>
```

JSON envelope:

```json
{
  "protocol": "sssp",
  "securityLevel": 2,
  "songTitle": "...",
  "artistName": "...",
  "duration": 240,
  "songFileURL": "uhrp://...",
  "artFileURL": "uhrp://...",
  "previewURL": "https://...",
  "genre": "...",
  "album": "...",
  "releaseDate": "2025-04-25",
  "pricePerPlay": 1000,
  "royaltyRate": 75
}
```

Admission rules (`sonicstarTopic.ts:34-37`):

1. JSON parses successfully.
2. `protocol === "sssp"`.
3. `songTitle`, `artistName`, `songFileURL` are all non-empty strings.

`securityLevel` is informational, not enforced. Per Ruth's clarification, the TypeScript decoder is permissive: it tries the data after `OP_RETURN`, then re-parses that buffer as an inner script, and looks for the first push that round-trips as a JSON object whose `protocol` is `sssp`. We mirror this exactly.

## 3. Scope and file tree

### New files

```
crates/overlay-discovery/
├── src/sonicstar/
│   ├── mod.rs                 (~50 lines: doc + 3 pub mod)
│   ├── topic_manager.rs       (~250 lines: SonicstarTopicManager + tests)
│   ├── lookup_service.rs      (~450 lines: SonicstarLookupService + tests)
│   └── storage.rs             (~350 lines: trait + MemorySonicstarStorage + tests)
└── docs/
    └── sonicstar_topic.md     (~80 lines: protocol doc loaded by include_str!)

crates/overlay-cloudflare/
├── src/d1_discovery.rs        (modified: add D1SonicstarStorage block, ~250 lines added)
├── src/d1/mod.rs              (modified: add 1 CREATE TABLE + 2 CREATE INDEX migrations)
├── src/lib.rs                 (modified: ~10 lines added across 2 match arms + 1 storage instantiation)
└── wrangler.toml              (modified: add tm_sonicstar / ls_sonicstar to default CSV optional)

parity-harness/
└── corpus/sonicstar/          (new: ~12 scenario JSON files, see §7)

crates/overlay-cloudflare/tests/
└── sonicstar_live_parity.rs   (new: optional integration test against sonicstar.net)
```

### No changes needed in

- `crates/overlay-engine/` (the `TopicManager`, `LookupService`, `EngineBuilder` traits all work as is)
- `crates/overlay-cloudflare/src/routes.rs` (the `/lookup` handler dispatches by `service` field via `HashMap.get()`, plugin agnostic)
- `crates/overlay-cloudflare/src/janitor.rs` (sweeps SHIP/SLAP only by domain, sonicstar has no per-domain health concept)

## 4. Module by module

### 4.1 `crates/overlay-discovery/src/sonicstar/mod.rs`

Convention follows `dm_delegation/mod.rs:1-51`. Module doc covers: purpose, on-wire format, admission rules, lookup query types, design rationale (why no signature linkage, why match TS decoder permissiveness exactly).

```rust
//! `tm_sonicstar` topic manager and `ls_sonicstar` lookup service for the
//! SonicStar Song Source Protocol (sssp).
//! [...]

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
```

Re-export from `crates/overlay-discovery/src/lib.rs`:

```rust
pub mod sonicstar;
```

### 4.2 `topic_manager.rs`

Follows `dm_delegation/topic_manager.rs:37-100` shape. Differences:

- No `PushDrop::decode`. Use `bsv_rs::script::Script::from_binary()` and inspect `chunks()`. The bsv-rs parser captures everything after `OP_RETURN` as `chunks[0].data` in a single buffer (matches the TS reference's main path).
- No signature verification, no key derivation, no BRC-43/BRC-42 protocol name. Pure JSON validation.

Public surface:

```rust
pub struct SonicstarTopicManager;

impl SonicstarTopicManager {
    pub fn new() -> Self { Self }

    /// Extract the candidate JSON push bytes from an output, or None.
    /// Mirrors the TS decoder's three-path candidate scan:
    ///   1. chunks after OP_RETURN (chunks[1..])
    ///   2. chunks[0].data raw (the bsv-rs parser collapses tail here)
    ///   3. chunks[0].data re-parsed as inner script, each push tried in turn
    fn candidate_pushes(locking_script: &LockingScript) -> Vec<Vec<u8>>;

    /// Decode + validate. Returns Some(SonicstarMetadata) if admissible.
    pub fn decode_song_metadata(locking_script: &LockingScript) -> Option<SonicstarMetadata>;
}

#[async_trait(?Send)]
impl TopicManager for SonicstarTopicManager {
    async fn identify_admissible_outputs(...) -> Result<AdmittanceInstructions, TopicManagerError>;
    async fn get_documentation(&self) -> String;  // include_str!("../../docs/sonicstar_topic.md")
    async fn get_metadata(&self) -> ServiceMetadata;
}
```

A `SonicstarMetadata` struct with the 13 documented fields, derived `Serialize` and `Deserialize`, plus `#[serde(skip_serializing_if = "Option::is_none")]` on the optionals so `undefined` fields drop out of the persisted record exactly as Mongo does.

Field defaults match TS exactly (note the falsy `||` quirk):

- `description`, `artFileURL`, `previewURL`, `genre`, `album`, `releaseDate`: `Option<String>`, dropped when absent.
- `duration`: `u64`, default `0`. An explicit `0` in the input is overwritten by `0` (no change observable, but match the rule).
- `pricePerPlay`: `u64`, default `1000`. Explicit `0` becomes `1000`.
- `royaltyRate`: `u8`, default `75`. Explicit `0` becomes `75`.
- `artistIdentityKey`: always `""` (TS hard codes this with a TODO; we match).

Permissive JSON extraction: `text.find('{')` to `text.rfind('}')` inclusive, then `serde_json::from_str` on the slice.

Tests (sync `#[test]` per dm_delegation pattern):

- well-formed sssp output → admitted
- missing songTitle / artistName / songFileURL → not admitted
- protocol field wrong or missing → not admitted
- non OP_RETURN output → not admitted
- malformed JSON → not admitted
- `OP_FALSE OP_RETURN <push>` variant → admitted (per Ruth's "permissive decoder")
- explicit `pricePerPlay: 0` → admitted, persisted as `1000`
- explicit `duration: 0` → admitted, persisted as `0`
- unknown extra fields in JSON → admitted, extras silently dropped
- leading/trailing junk around the JSON in the push → admitted (find first `{` rfind last `}`)

### 4.3 `storage.rs`

Trait shape mirrors `dm_delegation/storage.rs:31-79`. Method per query, not a query struct (sonicstar's filter set is small and stable).

```rust
#[async_trait(?Send)]
pub trait SonicstarStorage {
    async fn has_duplicate_record(&self, txid: &str, output_index: u32)
        -> Result<bool, SonicstarStorageError>;

    async fn store_record(&self, record: &SonicstarRecord)
        -> Result<(), SonicstarStorageError>;

    async fn delete_record(&self, txid: &str, output_index: u32)
        -> Result<(), SonicstarStorageError>;

    async fn find_by_outpoint(&self, txid: &str, output_index: u32)
        -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    async fn find_by_txid(&self, txid: &str)
        -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    async fn find_by_artist_name(&self, name_substr: &str, limit: Option<u32>, skip: Option<u32>)
        -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    async fn find_by_genre(&self, genre: &str, limit: Option<u32>, skip: Option<u32>)
        -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    async fn find_by_search_text(&self, q: &str, limit: Option<u32>, skip: Option<u32>)
        -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    async fn find_all(&self, limit: Option<u32>, skip: Option<u32>)
        -> Result<Vec<UTXOReference>, SonicstarStorageError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SonicstarStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}
```

`SonicstarRecord` mirrors the 17-key TS shape:

```rust
pub struct SonicstarRecord {
    // 13 metadata fields (from SonicstarMetadata)
    pub song_title: String,
    pub artist_name: String,
    pub artist_identity_key: String,    // always ""
    pub description: Option<String>,
    pub duration: u64,
    pub song_file_url: String,
    pub art_file_url: Option<String>,
    pub preview_url: Option<String>,
    pub genre: Option<String>,
    pub album: Option<String>,
    pub release_date: Option<String>,
    pub price_per_play: u64,
    pub royalty_rate: u8,
    // 4 overlay-supplied fields
    pub txid: String,
    pub output_index: u32,
    pub satoshis: u64,
    pub admitted_at: i64,               // unix millis
}
```

`MemorySonicstarStorage`: `Mutex<Vec<SonicstarRecord>>`. Sort by `admitted_at` descending in every paginated read. Search semantics:

- `find_by_artist_name`: case insensitive substring on `artist_name`.
- `find_by_genre`: exact match (case sensitive, matches Mongo equality).
- `find_by_search_text`: case insensitive substring across `song_title`, `artist_name`, `album` (3 fields, NOT 4: the TS docstring is stale, the code does 3).

Tests (`#[tokio::test]`): store, dup detection, delete, each find variant, pagination, empty filter cases, sort order is admittedAt DESC.

### 4.4 `lookup_service.rs`

Follows `dm_delegation/lookup_service.rs:36-288` shape.

```rust
pub struct SonicstarLookupService {
    storage: Rc<dyn SonicstarStorage>,
}

impl SonicstarLookupService {
    pub fn new(storage: Rc<dyn SonicstarStorage>) -> Self;
}

#[async_trait(?Send)]
impl LookupService for SonicstarLookupService {
    fn admission_mode(&self) -> AdmissionMode { AdmissionMode::LockingScript }
    fn spend_notification_mode(&self) -> SpendNotificationMode { SpendNotificationMode::None }

    async fn output_admitted_by_topic(&self, payload: &OutputAdmittedByTopic)
        -> Result<(), LookupServiceError>;

    async fn output_spent(&self, payload: &OutputSpent)
        -> Result<(), LookupServiceError>;

    async fn output_evicted(&self, txid: &str, output_index: u32)
        -> Result<(), LookupServiceError>;

    async fn lookup(&self, question: &LookupQuestion)
        -> Result<Vec<UTXOReference>, LookupServiceError>;

    async fn get_documentation(&self) -> String;
    async fn get_metadata(&self) -> ServiceMetadata;
}
```

`output_admitted_by_topic`: pattern match `OutputAdmittedByTopic::LockingScript { txid, output_index, topic, locking_script, satoshis }`. Drop if `topic != "tm_sonicstar"`. Decode the JSON via `SonicstarTopicManager::decode_song_metadata`, build a `SonicstarRecord` with `admitted_at = now_millis()`, call `storage.store_record`.

`output_spent`: pattern match `OutputSpent::None { topic, txid, output_index }` only (matches TS `mode === "none"`). Delete the record.

`output_evicted`: just delete by `(txid, output_index)`.

`lookup`: parse `question.query` per the TS contract:

- `"findAll"` (string) or `{}` or `{ "findAll": true }` → enumerate all
- `{ "txid": "..." }` exact match
- `{ "artistName": "..." }` case insensitive substring
- `{ "genre": "..." }` exact
- `{ "searchText": "..." }` case insensitive substring across song_title / artist_name / album
- `limit` clamped to `[1, 200]`, default `50`
- `skip` clamped to `[0, ..]`, default `0`
- multiple filter keys combine via AND (matches Mongo filter object semantics)
- empty query `{}` defaults to findAll
- unknown service name returns `LookupServiceError::InvalidQuery`

Returns `Vec<UTXOReference>` (the engine `lookup()` contract). The richer record retrieval that Ruth's `lookupRecords()` provides is not part of the engine contract; the existing `/lookup` route returns outpoints, which is correct. Sonicstar's worker will hydrate to records via the engine's existing BEEF retrieval path (or, if richer record metadata is needed in responses, that is a separate route addition discussed in §10).

Tests (`#[tokio::test]`):

- admission mode is LockingScript, spend mode is None
- output_admitted_by_topic stores with admittedAt
- output_admitted_by_topic ignores wrong topic
- output_spent::None deletes; other variants ignored
- output_evicted deletes
- lookup with each filter type
- lookup with combined filters
- lookup pagination (default 50, max 200, min 1)
- lookup `"findAll"` string, `{ findAll: true }` object, `{}` empty all enumerate
- lookup unknown service → error
- sort order admittedAt DESC

### 4.5 `crates/overlay-discovery/docs/sonicstar_topic.md`

Loaded by `include_str!` from `topic_manager.rs::get_documentation`. Mirrors the structure of `dm_delegation_topic.md`. Sections: overview, on-wire format, admission rules, JSON schema, query types, references.

### 4.6 `crates/overlay-cloudflare/src/d1_discovery.rs`

Append a new block after the `D1DmDelegationStorage` impl. Pattern from `D1DmDelegationStorage` (lines 616-end).

```rust
pub struct D1SonicstarStorage {
    db: Rc<D1Database>,
}

impl D1SonicstarStorage {
    pub fn new(db: Rc<D1Database>) -> Self { Self { db } }
}

fn sonicstar_err(e: String) -> SonicstarStorageError {
    SonicstarStorageError::Database(e)
}

#[async_trait(?Send)]
impl SonicstarStorage for D1SonicstarStorage {
    // CRUD via prepared statements against `sonicstar_records`
    // searchText uses LOWER(col) LIKE ? with %escaped%
    // artist substring same
    // genre exact
    // ORDER BY admitted_at DESC LIMIT ? OFFSET ?
}
```

Each method binds parameters and uses `db.prepare(sql).bind(&args).first/all`. Error mapping: any worker error becomes `sonicstar_err`.

### 4.7 `crates/overlay-cloudflare/src/d1/mod.rs`

Add to `OVERLAY_MIGRATIONS` array (currently 23 entries at line 226). Increment `OVERLAY_MIGRATION_COUNT` to 24. Append the test assertion at line 435 to include `sonicstar_records`.

```sql
CREATE TABLE IF NOT EXISTS sonicstar_records (
    txid              TEXT    NOT NULL,
    output_index      INTEGER NOT NULL,
    satoshis          INTEGER NOT NULL,
    admitted_at       INTEGER NOT NULL,           -- unix millis, descending sort key
    song_title        TEXT    NOT NULL,
    artist_name       TEXT    NOT NULL,
    artist_identity_key TEXT  NOT NULL DEFAULT '',
    description       TEXT,
    duration          INTEGER NOT NULL DEFAULT 0,
    song_file_url     TEXT    NOT NULL,
    art_file_url      TEXT,
    preview_url       TEXT,
    genre             TEXT,
    album             TEXT,
    release_date      TEXT,
    price_per_play    INTEGER NOT NULL DEFAULT 1000,
    royalty_rate      INTEGER NOT NULL DEFAULT 75,
    PRIMARY KEY (txid, output_index)
);

CREATE INDEX IF NOT EXISTS idx_sonicstar_artist_name
    ON sonicstar_records (artist_name);

CREATE INDEX IF NOT EXISTS idx_sonicstar_genre
    ON sonicstar_records (genre);

CREATE INDEX IF NOT EXISTS idx_sonicstar_admitted_at
    ON sonicstar_records (admitted_at DESC);
```

Note: I added a fourth index on `admitted_at` because every paginated lookup sorts by it. SQLite can use it for ORDER BY without a filesort.

### 4.8 `crates/overlay-cloudflare/src/lib.rs`

Three small additions in `build_engine_with_storage`:

1. After line 118 (the discovery storage instantiations), add:

   ```rust
   let sonicstar_storage: Rc<dyn SonicstarStorage> =
       Rc::new(D1SonicstarStorage::new(db.clone()));
   ```

2. In the topic manager match (lines 298-324), add an arm:

   ```rust
   "tm_sonicstar" => {
       managers.insert("tm_sonicstar".into(), Box::new(SonicstarTopicManager::new()));
   }
   ```

3. In the lookup service match (lines 327-365), add an arm:

   ```rust
   "ls_sonicstar" => {
       lookup_services.insert(
           "ls_sonicstar".into(),
           Box::new(SonicstarLookupService::new(sonicstar_storage.clone())),
       );
   }
   ```

That is the full wiring.

### 4.9 `crates/overlay-cloudflare/wrangler.toml`

Decision point: do we add `tm_sonicstar` / `ls_sonicstar` to the default `TOPIC_MANAGERS` and `LOOKUP_SERVICES` env vars in `wrangler.toml`, or leave them off so each deployment opts in?

Recommendation: leave the defaults alone (`tm_ship,tm_slap` only). Sonicstar is a SonicStar specific extension, not a baseline. Their deployment sets:

```toml
[vars]
TOPIC_MANAGERS = "tm_ship,tm_slap,tm_uhrp,tm_sonicstar"
LOOKUP_SERVICES = "ls_ship,ls_slap,ls_uhrp,ls_sonicstar"
```

Document this in `crates/overlay-cloudflare/README.md` and `CLAUDE.md`.

## 5. Parity strategy

The reference docker container in `parity-harness/` runs vanilla `@bsv/overlay-express@2.2.0` with no custom plugins (audit confirmed at `reference/server.mjs`). It does NOT include sonicstar. So the standard parity harness cannot diff our worker's sonicstar behavior against a TS reference.

Three layers cover this:

### Layer A: unit tests (in repo)

`#[test]` and `#[tokio::test]` blocks in each sonicstar source file. Cover the admission rules and lookup query parsing comprehensively. These pass without any external dependency.

### Layer B: standard parity harness scenarios (informational only)

Add `parity-harness/corpus/sonicstar/*.json` scenarios that submit and look up sonicstar txs. These will diverge from the vanilla reference (which returns 404 / empty). Use the `note` field to mark them as expected divergences:

```json
{
  "name": "sonicstar_lookup_findAll",
  "method": "POST",
  "path": "/lookup",
  "headers": { "Content-Type": "application/json" },
  "body": { "service": "ls_sonicstar", "query": "findAll" },
  "note": "RO-002: tm_sonicstar / ls_sonicstar are extensions, not in vanilla 2.2.0 reference"
}
```

These confirm the worker handles sonicstar requests without crashing and produces stable responses. They do NOT establish parity with TS sonicstar code.

### Layer C: live parity against sonicstar.net (the real test)

Ruth exposed `https://sonicstar.net/api/overlay-parity/{admit,lookup,docs}` which run the same `SonicStarTopicManager` and `SonicStarLookupService` classes directly. This is the actual reference for byte parity.

Build `crates/overlay-cloudflare/tests/sonicstar_live_parity.rs` (gated behind `--ignored`, opt in via `SONICSTAR_REFERENCE_URL`):

- For each of the 12 mainnet txids Ruth provided, fetch BEEF from WhatsOnChain (`/v1/bsv/main/tx/{txid}/beef`).
- POST each BEEF to her `/api/overlay-parity/admit` and to our local worker at `OVERLAY_URL`.
- Diff `outpoints[]` after sorting by `(txid, output_index)`. Ignore `indexBuiltAt`, `indexSize`.
- Run a matrix of synthetic lookup queries (`findAll`, by txid, by artistName, by genre, searchText) against both endpoints. Diff `outpoints[]` and (optionally) `records[]` in the union response shape.
- For records diff, the TS endpoint returns the rich shape with all 17 keys; our worker would need a `/sonicstar/records` style endpoint or this diff is skipped.

This test runs against deployed environments, not in CI by default. Run before each release.

### Layer D: synthetic negatives

We can construct our own rejection cases (malformed JSON, wrong protocol, missing required field, empty strings) and POST them to her `/admit` endpoint. Confirm she rejects, confirm we reject, confirm both return the same shape. Lives in the same `sonicstar_live_parity.rs` test.

## 6. D1 schema rationale

- `(txid, output_index)` as composite primary key (matches Mongo `(txid, outputIndex)` unique).
- `admitted_at INTEGER` as unix millis. SQLite stores as integer, sortable, no timezone confusion. We compare to the TS `Date` shape on wire by formatting in route responses if we expose them, but for the engine `lookup()` response shape (outpoints only), this never appears.
- Optional fields use SQLite `NULL`. The Rust `Option<String>` round trips cleanly via the `worker` crate's D1 query helpers.
- Indexes: artist_name (substring queries hit this for prefix, fall back to scan for general substring at this scale), genre (exact equality), admitted_at DESC (sort key). At about 70 records today, none of this matters for performance. The indexes future proof to roughly 100k records on D1 before we need FTS5.

For `searchText` we use:

```sql
WHERE LOWER(song_title) LIKE ? OR LOWER(artist_name) LIKE ? OR LOWER(album) LIKE ?
ORDER BY admitted_at DESC LIMIT ? OFFSET ?
```

with the parameter as `%escaped_lower%`. Escaping handles `%` and `_` in the input. At 100k+ rows we would migrate to FTS5; that is out of scope for this plan.

## 7. Test matrix

### In repo (CI)

| Layer | File | Count | Runtime |
|---|---|---|---|
| Topic manager unit | `topic_manager.rs` | ~12 tests | sync |
| Storage unit | `storage.rs` | ~10 tests | tokio |
| Lookup service unit | `lookup_service.rs` | ~15 tests | tokio |
| D1 storage integration | could mock or rely on live tests | TBD |
| Parity harness scenarios | `parity-harness/corpus/sonicstar/*.json` | ~12 scenarios | informational |

### Out of repo (manual / pre-release)

| Layer | File | Trigger |
|---|---|---|
| Live parity vs sonicstar.net | `sonicstar_live_parity.rs` | `--ignored` + env vars |
| End to end submit/lookup against deployed worker | manual `curl` or extend `tools/e2e_*.sh` | manual |

## 8. Risks and open questions

### Risks

- **R1: Permissive decoder edge cases.** The TS decoder has three candidate paths (after OP_RETURN, raw chunks[0].data, re-parsed inner script). We mirror all three, but there may be on-chain txs where one path admits and another rejects. Mitigation: layer C live parity diff catches this.
- **R2: D1 LIKE substring at scale.** Fine at 70 records, slow at 100k+. Mitigation: documented in plan, not addressed now.
- **R3: `securityLevel` change of policy.** Currently informational. If they later make it enforced, we have to add a check. Mitigation: comment in topic manager noting this.
- **R4: `artistIdentityKey` always empty.** Their TODO. If they wire it up, we will need to mirror the lift from transaction context. Mitigation: comment noting parity dependency.
- **R5: Engine hydration shape.** Our `/lookup` returns engine outpoints + BEEF (per the standard route). Their `/api/overlay-parity/lookup` returns a union of outpoints and records. If they expect a record-shaped response on `/lookup`, that is a different contract. Resolved: the TS engine's `lookup()` returns outpoints; rich records are a separate `lookupRecords()` method exposed via their `/api/discover/overlay` route. Our parity is on outpoints. Records are only used in their parity helper endpoint, where Ruth said diff `outpoints[]` for byte-stable comparison.

### Open questions

- **Q1: Should sonicstar gate behind `ENABLE_SONICSTAR` env var?** The audit showed `ENABLE_EXTENSIONS` is documentation only; CSV env vars do real gating. Recommendation: rely on the CSV pattern, no separate gate. If you disagree, say so.
- **Q2: Do we add a `/sonicstar/records` (or similar) route that returns the rich record shape, matching their `lookupRecords()`?** This is convenient for SonicStar's frontend but is a new HTTP route, not just a plugin. Suggest deferring until they ask, since the engine `/lookup` outpoints are what overlay-express clients actually call.
- **Q3: BEEF round trip storage.** Each admitted output gets stored as a BEEF blob in `D1Storage` (the generic engine storage), separately from `sonicstar_records`. Per the existing pattern this is automatic via `engine.submit()`. Worth confirming it stores correctly for outputs whose script is just OP_RETURN (no spendable value beyond the 1 sat marker).
- **Q4: Genre case sensitivity.** TS Mongo `filter.genre = string` is case sensitive equality. Our SQLite `WHERE genre = ?` is also case sensitive by default. Match. Worth flagging in our docs.
- **Q5: Cron / janitor.** Sonicstar records are admitted but never expire. Should the janitor sweep stale ones? TS does not. Skip.

### Things I will check before writing code

1. `MemorySonicstarStorage` design: how to store sorted by `admitted_at` efficiently. A `BTreeMap<i64, Vec<Record>>` keyed by negative timestamp gives O(log n) insertion and O(n) sorted enumeration; or just a `Vec` and re-sort on read. At test scale either is fine; pick `Vec` for simplicity.
2. `OutputAdmittedByTopic::LockingScript` exact field names (the audit showed the variant signature, but I should re-read it once before writing).
3. `worker` crate D1 binding semantics for `Option<String>` parameters (what happens with `None`? `JsValue::null()`?). Quick check against `D1AgentStorage` usage.

## 9. Implementation sequence

Suggested order, each step compiles and tests before the next:

1. Scaffolding: create `sonicstar/` directory, `mod.rs`, empty submodules, register in `lib.rs` of overlay-discovery. `cargo check` passes.
2. Storage trait and `MemorySonicstarStorage`. Tests pass.
3. Topic manager. Tests pass.
4. Lookup service. Tests pass.
5. Documentation file `sonicstar_topic.md`.
6. D1 schema migrations.
7. `D1SonicstarStorage` impl.
8. Worker wiring in `lib.rs`.
9. Parity harness corpus scenarios.
10. Live parity test (`sonicstar_live_parity.rs`, `--ignored`).
11. README and CLAUDE.md doc updates.

Estimated work: 1 to 2 focused days. Most of it is mechanical from templates; the only novel piece is the OP_RETURN parser and the candidate scan, which the bsv-rs audit confirmed is straightforward.

## 10. Out of scope

- Adding a `/sonicstar/records` rich response route (open question Q2).
- Migrating to D1 FTS5 for searchText (deferred per §6).
- Wiring `artistIdentityKey` from transaction context (their TODO).
- Generalizing the OP_RETURN parser into a shared `overlay-discovery::op_return` helper (premature; sonicstar is the only consumer).

## 11. Sign off checklist for John

- [ ] Module layout under `sonicstar/` matches your preference
- [ ] D1 schema column names and types are acceptable
- [ ] CSV opt in pattern (no `ENABLE_SONICSTAR` flag) is OK
- [ ] Live parity test as `--ignored` (manual run) is sufficient, or do you want it in CI
- [ ] Layer B parity scenarios with `note` markers are acceptable, or skip them entirely
- [ ] Open questions Q1 to Q5 either resolved or accepted as deferred
- [ ] Implementation sequence in §9 is the right order
