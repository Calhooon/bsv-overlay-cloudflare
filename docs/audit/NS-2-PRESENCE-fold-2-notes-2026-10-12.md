# NS-2-PRESENCE second fold notes

The delta lens is read-only in bsv-low. This fold restores #606's verified
spender content without promoting courier presence to finality. Served owed
facts carry `spenderPresence: {word, tier, source, at}` for a pending spender.
The chain spend word still supplies confirmation; classification grants no
claim, credit, release or void. No deploy or spend.

N1: The first fold's M2, vocabulary, scope, absence retirement and proof binding
remain pinned. The pre-flight probe readmission remains filed as #636 and the
witnessed corroborator echo as #637. This fold fixes the POST echo path.

N2: The D1 budget is a real gate, not a formality. The recorded first-read
baseline is 26 statements, the former ceiling 28. The first fold adds one
scope statement per externally resolved spender (two courier resolutions per
recompute), plus at most one held-anchor statement per isolate's 30-second
probe window. Both positive and negative probes now share that window; a
recompute has a shorter time budget. Worst added scope/probe cost is 2 + 1,
so this fold explicitly raises the statement ceiling to 29 (26 + 3).
H1's content reads and evidence annotation add zero D1 statements. M1 removes
up to four admission-age statements per evicted gated submit. L1 and L2 add
zero D1 statements; L3 bounds the existing anchor statement rather than adding
one. Reads/writes ceilings stay in force. The seeded route measurement does
not by itself cover every externally resolved spender shape.

N3: Confirmed foreign subjects still ask WoC hash, hex and TSC, then cached
canonical headers. Own subjects ask Arcade first, with a held anchor when due.
Foreign subjects never ask Arcade. The existing TTL and owed caps bound asks;
per-minute and live latency measurements remain unverified.

N4: The off-format app routes presence-leg fold hunk is formatted. Changed
hunks are formatted without sweeping unrelated baseline formatting drift.

N5: The Reached echo counter includes a first door submit already seen through
the client's belt. Its name is not proof of a repeated door submit.

N6: The #614 belt-only subject without a local record is foreign. A WoC 404
there is one capable source and yields unknown, not first-party presence.

N7: Both RFC3339 parsers accept Arcade's UTC fractional stamp. Arcade rawTx
could avoid the first-party courier hex GET; that optimization is not folded.

N8: CorroboratedAbsent has no request-time producer. Legacy decision arms stay;
both old request-time absence generations retire on read, including the one
that was never written in production.

N9: The owed home walk now reads hash-verified hint bytes from the cache rather
than answering held-without-bytes. Presence and finality remain separate.

The regression tool adds seven disposable-copy mutants: hidden hint bytes,
a claim from unconfirmed spender content, bypassed eviction freshness, the
global no-eviction age bound, discarded failed-proof content, lost claimed
height and unbounded anchor probes. Each must fail a behavior assertion with
exit 101, never compilation. The old scope mutant no longer treats readable
hint bytes as a defect; delta-h1 pins the corrected rule instead.

The captain owns the second delta lens and the bsv-low register update.

## Verification on the Studio

- H1, M1, L1, L2 and L3 pins each assertion-RED on the unfixed tree and GREEN
  after the corresponding fold. H1 reproduced #606 on the first-fold head.
- All seven second-fold disposable-copy mutants are behavior RED, exit 101;
  no compile failures. The complete final suite supplies their GREEN side.
- `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare
  -p low-app-layer --no-fail-fast`: 1,373 passed, zero failed, one ignored;
  includes all 23 NS-2 pins and 90 broadcaster tests (the original 88 plus two).
  Whole libs: overlay 698 passed / one ignored, app layer 463 passed.
- `cargo clippy --manifest-path workers/Cargo.toml --workspace --all-targets
  -- -D warnings`: passed. All five changed Rust files' changed hunks are
  rustfmt-clean. Storage ownership: 694 references match. Diff whitespace clean.
- `make ci-d1-budget`: started at 1-minute load 17.17, blocked at wasm-bindgen.
  The app-layer release wasm compiled; the locked Rust schema is 0.2.126 and
  the prescribed `$HOME/.cargo/bin/wasm-bindgen` has schema 0.2.129.
- `make -o ci-d1-budget ci-route`: default-port attempt refused another user's
  server on 8791; retried at `LANE_BASE=18891`, load 14.05, with the other server
  untouched. Overlay release wasm compiled, then the same CLI schema mismatch
  blocked the fixture. No route cells or seeded D1 statement count verified.
- No shared CLI replacement or dependency-lock change. Align the shared CLI
  with locked 0.2.126, then rerun both real CI gates before landing. No live owed
  request, deploy or spend; the captain's second delta lens remains unverified.
