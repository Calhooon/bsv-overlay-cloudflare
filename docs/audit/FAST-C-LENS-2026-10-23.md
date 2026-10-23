# FAST-C-LENS: the lens on overlay PR 151 (bsv-low #848, #461 design C: the handfile family)

Stamp: 2026-10-23T18:08:19Z (`date -u`). Lens branch `lane/FAST-C-LENS-F`, worktree at the PR head `754f8997`
(`lane/FAST-C`: `49c9b036` the pins RED first, `754f8997` the implementation), main at `d370d906`. Money-adjacent:
the filings are the owed list's evidence. This Fable round replaces an Opus start the captain stopped.

## Verdict: NOT SOUND (one MEDIUM, five LOW)

The forger's doors are shut: a batch binds ONE identity to ONE settle key and only the holder of both keys files
it, a marker's bytes are one row under one content key whatever carries them, a refusable payload refuses the whole
batch before any write, the exploded rows are counted against every direct cap, `sigValid = 1` and `markerValid = 1`
are bound only after both signatures verified, every reader of the K binding trusts the latched verdict and none
re-verifies a row's own columns, the served `batch` is read from the stored tables, an old row's bytes are unchanged,
the two migrations append. One contract the wire doc makes does not hold: a re-filing of the same batch is REFUSED
(409 `too_many_filed`) by the route's own potparty cap arithmetic, and so is a handfile whose v1 marker the seat
filed directly first. Nothing is forgeable and no landed row is lost; the fix is one statement's shape.

## Findings

### MEDIUM-1: a re-filing of the same handfile, or a handfile after a direct v1 filing, is refused by the potparty cap

`crates/low-app-layer/src/handfile.rs:736-773` (the loop over the v1, v2 and refund slots; `potparty_pending`).

The potparty family's cap is 2 rows per (poster, game, pot) of OTHER content than the one being filed
(`POTPARTY_FILED_ROWS_SQL`: `txid <> ?4`). The handfile files two potparty contents (v1 and v2) and carries a
`potparty_pending` of 1 into the v2 slot's count so the batch's own v1 is "each other's company". On the FIRST filing
that reads 0 then 0 + 1 = 1 and passes. On a RE-FILING both rows are on file: the v1 slot counts the v2 row (1, the
same key excluded), the v2 slot counts the v1 row (1) plus the pending 1 = 2, and `cap_refusal_at(2, 2, ..)` answers
`TooManyFiled` (409) before any write. The same arithmetic refuses a handfile whose v1 marker the seat already filed
directly (the v1 slot excludes its own key and reads 0; the v2 slot reads the v1 row 1 + 1 = 2). The module doc's
comment at line 734 ("a re-filing 1 then 1") is not what the code computes.

The forger's input: none (an honest client's retry). The evidence consequence: on a retry after a landed write
(a timeout, a second device, an outbox entry not yet cleared) nothing is lost but the wire doc's §5 and §7 contract
("a re-filing is a no-op, the same answer"; "its retry is idempotent by content") does not hold and the outbox meets
a terminal 409; in the mixed sequence (a direct v1 then the handfile, a transitional client) the batch's hopparty
row, v2 row and refund row are NOT filed, so the hand's K binding (#610) and its refund backup are missing until the
client files them directly. The direct door never meets this: it files one content at a time.

Fix shape: count the potparty cap once for the pair, with BOTH keys excluded (`txid NOT IN (?4, ?5)`), and refuse
when that count plus the batch's two contents exceeds the cap (so any other content refuses, a landed v1 or v2 of
this batch never does); or add `potparty_pending` only when the slot's own row is not yet on file. The day count has
the same over-count (a landed row counted once as a row and once as pending), harmless under 200. Pin: `lens_b`
below, RED on the branch: `left: Some(TooManyFiled) right: None`.

### LOW-1: the wire doc's refusal order is not the code's

`docs/HANDFILE-WIRE.md` §4 ("in order; the first refusal answers") lists `signature_invalid` and
`settle_signature_invalid` before `handfile_payload_not_a_marker`, `handfile_inconsistent` and `poster_mismatch`.
`verify_handfile` (`handfile.rs:369-427`) runs the canonical-DER gate first, then the four parsers, the consistency
check and the poster check, and only then the two ECDSA verifications. So a stranger posting a seat's honest batch is
403 `poster_mismatch` (the doc says 422 `signature_invalid`), and a bad payload is 400 before the signatures are
looked at. The module doc's step 1 ("verifies the two signatures ONCE ... before anything is parsed") is the DER gate
only. Harmless to money; the client lane reads the doc.

### LOW-2: a re-filing that writes nothing still marks stale and recomputes

`handfile.rs:858-878`. The direct door answers a no-op re-file `filed: false, alreadyFiled: true` without the stale
marks and the recompute (`record_post.rs:2169-2172`); the handfile door (once MEDIUM-1 is fixed, when a re-file can
reach the write) answers `filed: true` by design (the doc's "the same answer") and runs `note_filing_and_recompute`
whether the batch row was written or not. A retry costs a stale mark of the pot's parties and the filer's coalesced
walk, bounded by #499's ceiling. Stated, not a defect; a `batch_written` branch is in place to gate it.

### LOW-3: the body cap meets a refund raw the direct door admits

`record_post.rs:1900` (the raw body's `RECORD_POST_MAX_BODY_BYTES`, 201,088, read before the handfile dispatch).
A handfile body is the refund payload's hex plus the three other payloads, two signatures and the JSON frame, so a
refund raw of 99,716 bytes and up answers 413 `body_too_large` at the handfile door where the direct `potrefund` door
admits up to 100,000 (`lens_f`, measured: 201,657 bytes at 100,000). The honest refund in the fixtures is 3,520 bytes
(a body of 8,697). The doc's §2 sentence ("the refund raw's 100 KB as hex plus the frame") overstates the room by
about 300 bytes. Stated limit.

### LOW-4: the hop row's cap is per (poster, game, hop outpoint)

`handfile.rs:540-541` (`HOPPARTY_FILED_ROWS_SQL`). A poster may file one handfile hop row per GAME for one hop, so N
games name N rows for one hop, bounded by the day's 200 (`lens_d`, last block). The rows name nothing on their own:
`hop_pot_links` derives from the hop's spend and `pot_hop_seats` from the pot's committed keys, and each row needs the
settle key's signature over the batch. The direct families' caps are per game too. Stated.

### LOW-5: a cap refusal does not name the family

`handfile.rs:727-732, 766-769`. The 409 and 429 bodies are the shared `RecordRefusal` text ("too many filed",
"daily cap reached") with no family; the doc's §4 names the per-family rule but the outbox cannot tell which family
refused. Cosmetic; a `family` field on the refusal body would close it.

### NOTEs (no change asked)

- N1. `handfile_rows` is `INSERT OR IGNORE` on (family, rowTxid, rowVout): the FIRST batch to name a row owns its
  served `batch`. A second batch carrying the same v1 and v2 under another refund raw (refused by the refund cap in
  any case) would leave the v2 row pointed at the first batch, whose payload at `index` is still the row's bytes, so
  the client's inclusion check holds either way (`lens_b`). A direct filing then a handfile of the same marker keeps
  the direct row's own signatures and gains the batch pointer; inclusion holds (`lens_e`).
- N2. The settle signature is verified as `PublicKey::verify(sha256(preimage), der)`; bsv-rs 0.7.2's `verify` takes
  the digest and hashes nothing, the same call as `verify_seat_sig`, so a wallet signing `data: preimage` once
  matches (doc §3). No golden cross-repo vector for the handfile digest exists yet; the client lane should add one as
  `potParty.test.ts` did for v1.
- N3. The hopparty identity challenge carries no hop txid; the batch digest binds it and the hop row's content key
  carries it first, so two hops of one seat in one game are two rows.
- N4. The relatch skip (`relatch::door_filed_row`) now leaves EVERY `filed:` potparty row alone, directly filed rows
  included; those were verified at the door, so no verdict is lost.
- N5. `hopsweep::bind_hop_sweep` and `results::attribute_seats` / `fill_seats_from_hop_markers` trust the latched
  `markerValid` / `sigValid` and replay a signature only on a NULL latch, so a handfile hop can be swept and a
  handfile seat attributed; a relatch can never demote them (N4).
- N6. The hop row's `outputIndex` is `0x8000_0000 | sha256(content key)[..31 bits]`; a collision needs an output
  of the same hop transaction paying the colliding filer's settle key, which a stranger does not have.
- N7. The migration pins moved: `OVERLAY_MIGRATION_COUNT` 347, the app layer's catch-ups 16, the overlay test's table
  list; the fingerprint pin asserts determinism, not a frozen value. Both migrations are `CREATE TABLE IF NOT
  EXISTS`; no ALTER, no row touched.

## The forger's questions, answered

| question | answer | where |
|---|---|---|
| a batch signed by X exploding a row naming Y, or binding Y's K to X | refused: every payload's identity must equal the poster (403), the identity signature is verified under that identity (422), the settle signature under the v2 payload's key, the hop payload's key must equal it (422) | `verify_handfile` 388-427; `lens_a`, `fastc_b`, `fastc_c` |
| the same bytes filed twice under two content keys | a row's key is its FAMILY content key, the same a direct filing gets; two batches of one marker are one row | `lens_b`, `lens_e`, `fastc_a` |
| one refusable payload writes nothing | verification, the refund bind, the hop facts, the chain no-op and every cap run before the one D1 batch | `handfile_post` 644-858; `lens_c`, `fastc_d` |
| the answer names the family | the payload refusal's text names the order; a cap refusal does not (LOW-5) | |
| a cap exceeded by batching | the exploded rows are the direct rows: two direct contents refuse the batch, one leaves no room for two, the day's 200 counts the refund row | `lens_d`, `fastc_g` |
| `sigValid = 1` only after both signatures | the literal 1 is bound only after `verify_handfile` answered `Ok` (both verified) and the hop facts agreed | `handfile_post` 812-826, 802 |
| the owed judgement reads an exploded row as a direct one | `markerless_seats_sql`, `MARKERLESS_CANDIDATES_SQL` read the columns; `attribute_seats` trusts the latch; the batch is read from `handfile_rows` joined to `handfile_batches`, never the request | `owed.rs:1495, 1503`; `results.rs:546-556`; `handfile.rs:549-554`, `d1_discovery.rs` `handfile_batches_sql` |
| old rows byte-identical | `attach_batch` inserts only when a batch was found; pinned in `hops_view`, `refund_backups`, the discovery `handfile` tests and `fastc_h` | |
| the migrations append; the pins moved | two `CREATE TABLE IF NOT EXISTS`; 347, 16 | N7 |

## The cases written (the patch `FAST-C-LENS-2026-10-23.patch` beside this file; not pushed to `lane/FAST-C`)

`crates/low-app-layer/src/handfile_lens.rs` (a `#[cfg(test)]` module registered in `lib.rs`; `record_post::tests::pot_ctx`
made `pub(crate)`), run with `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib handfile`:

| case | result on `754f8997` |
|---|---|
| `lens_a_a_batch_cannot_bind_another_identitys_hop_or_key`: payloads naming Y signed by X (posted as X: `PosterMismatch`; as Y: `SignatureInvalid`), X naming Y's settle key (`SettleSignatureInvalid`), the hop and v2 payloads naming two keys, the hop or refund payload naming Y (`HandfileInconsistent`), Y's batch re-signed by X (`SignatureInvalid`) | ok |
| `lens_b_one_marker_under_two_batches_is_one_row_and_a_refile_is_not_refused`: a second batch of the same v1 and v2 under another refund is refused by the refund cap; written anyway (statements alone) it leaves one row per marker pointed at the first batch, inclusion holding; then the route's cap arithmetic on a re-filing of the landed batch | FAILED: `left: Some(TooManyFiled) right: None` (MEDIUM-1) |
| `lens_c_one_bad_payload_refuses_the_whole_batch_before_any_write`: a refund that spends the pot but is not its pre-signed spend (`RefundNotThePresignedSpend` at the bind over the indexed pot; rank 0 unindexed; the honest refund rank 1), a BEEF without the hop (no facts, the 425), bytes that are no BEEF, a hop paying another key | ok |
| `lens_d_a_poster_cannot_exceed_a_family_cap_by_batching`: two direct potparty contents refuse the batch; one leaves no room; 200 refund rows today refuse it (`DailyCapReached`); the hop row cap is per game (LOW-4) | ok |
| `lens_e_a_direct_row_has_no_batch_and_a_shared_row_serves_a_consistent_one`: a direct v2 filing has no batch row; the handfile after it shares the row (the direct signatures kept, `sigValid` 1), the served batch's payload at `index` is the v2 challenge rebuilt from the row | ok |
| `lens_f_the_body_cap_meets_a_near_maximal_refund_the_direct_door_admits`: the smallest refused refund raw is 99,716 bytes; the honest body 8,697 (LOW-3) | ok |

The branch's own eight `fastc_*` pins pass beside them (11 of 14 ok on the first run; the two other reds were the
lens's fixtures, fixed: X's opponent was Y, and the honest body is 8.7 KB).

## Commands and exits (the branch tree, the lens patch removed; `CARGO_TARGET_DIR=~/bsv/targets/wt-overlay-FAST-C-LENS-F`)

| command | exit | result |
|---|---|---|
| `make ownership` | 0 | self-test 39 ok; 1278 SQL references in 3 crates match `storage-ownership.json` |
| `git apply --check docs/audit/FAST-C-LENS-2026-10-23.patch` | 0 | the lens patch applies to `754f8997` |
| `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --lib handfile` (the patch applied) | 101 | 14 ran: the branch's 8 `fastc_*` ok, the lens's 5 ok, `lens_b` FAILED (`left: Some(TooManyFiled) right: None`, MEDIUM-1) |
| `cargo test --manifest-path workers/Cargo.toml -p low-app-layer --no-fail-fast` | 0 | 96 binaries; the lib 679 passed, 0 failed, 2 ignored (the lane's claim: 678) |
| `cargo test --manifest-path workers/Cargo.toml --workspace --no-fail-fast` | 0 | 107 binaries, 2007 passed, 0 failed: `bsv-overlay-cloudflare` lib 966 (1 ignored), `low-app-layer` lib 679, `low-proof-replay` 5 (the lane's "workers 991" is not a figure this run produced; the overlay lib is 966) |
| `cargo test --workspace --features bsv-overlay-engine/memory-storage --no-fail-fast` (root) | 0 | 101 binaries, 1731 passed, 0 failed (the lane's claim: 1731) |
| `cargo clippy --manifest-path workers/Cargo.toml --workspace --all-targets -- -D warnings` | 0 | clean |
| `cargo clippy --workspace --all-targets --features bsv-overlay-engine/memory-storage -- -D warnings` | 0 | clean |
| the migration pins (`d1::tests` `migration_count_matches`, the catch-up table list; `schema::tests` 16 catch-ups; `sql_prepares_sqlite`) | 0 | inside the suites above |

Not run (the captain's, at the land): the route and budget tiers (`make ci-route`, `make ci-d1-budget`), the wasm32
builds, `make ci-deploy`. The toolchain was 1.95.0 (LOW's `rust-toolchain.toml`; this repository carries none). Two
gate runs overran the 600 s bound and the harness parked them to finish (the app-layer and the workers suites); a
third was discarded because its own `head` cut the pipe. Every figure above is from a run that exited on its own.

## Read

The overlay's `CLAUDE.md`; LOW's register rows D11 and D1 (#610); `DESIGN-461-WALLET-CALLS-PER-HAND-2026-10-23.md`
§3.3; the PR's `docs/HANDFILE-WIRE.md`; the whole diff `origin/main...HEAD` (26 files); `record_post.rs` (the direct
door's verifier, caps, chain no-op, re-file path), `potparty/validity.rs`, `hopparty/validity.rs` and parser,
`results.rs` (`attribute_seats`, `fill_seats_from_hop_markers`), `hopsweep.rs` (`bind_hop_sweep`), `owed.rs` (the
K-binding statements), `derived_hops.rs` (the link and seat triggers), `relatch.rs`, the three lookup services,
`pot/lookup_service.rs` (the hop's BEEF in `pot_beefs`), bsv-rs 0.7.2 `PublicKey::verify`.
