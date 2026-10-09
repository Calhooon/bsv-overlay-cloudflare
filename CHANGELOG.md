# Changelog

## Unreleased: a BEEF of any size (NL-6, 2026-10-09)

The posture, from the charter "a BEEF of any size" (bsv-stack-lean
`docs/charters/beef-of-any-size.md`; the Lean definition is
`lean/BeefOfAnySize.lean` there): **a valid BEEF is never refused for its size
or its counts. A refusal is for invalid bytes only, and names them: the offset
of the byte and the kind.** This reverses the verdict by size that P0-5f put
on the engine's doors; what P0-5f fixed underneath (the linear BUMP walk, the
linear parse) stays, in bsv-rs.

- **bsv-rs 0.4.0** in both workspaces. The workers' lock also holds bsv-rs
  0.3.35, because bsv-middleware-cloudflare 0.4.1 and bsv-middleware-core
  0.1.0 are built on it; the `bsv-rs-03` dependency of `overlay-cloudflare`
  bridges the one value that crosses (the wallet handed to
  `WorkerStorageClient` in `wallet/client.rs`, built from the admin key's 32
  bytes). It leaves when the middleware moves to 0.4.
- **`overlay_engine::beef_limits` is the streaming door.** It keeps its name
  and its constants as names; no function in it refuses a BEEF by size or
  count. `fold_beef` reads any `std::io::Read` through bsv-rs 0.4.0's
  `BeefStream`, one element in hand; `read_beef`, `has_proof`, `own_proof`
  and `own_bump` fold over held bytes; `parse_beef` and
  `transaction_from_beef` run the same door and then build the in-memory
  `Beef` for a caller that merges, links, sorts or re-serializes one. A
  refusal reads `invalid BEEF at byte <offset>: <kind and its data>`.
  `is_limit_breach` answers `false` and stays exported for its callers.
- **Refusals that are gone:** `/submit`'s 413 over 10,000,000 bytes; the 512
  transaction and 512 BUMP counts at every door; the byte budget of every
  `*_BEEF_LIMITS` (the 10 MB doors, the census's 2 MiB and the queue's 90 KB
  as the module applied them); the base64 length bound of the queue's and
  the dead letter's reader; the byte bound on a stored funding body; the
  4,095-byte bound on a proof read back from a store or handed by a GASP
  peer (`proof_from_hex`).
- **What is stricter, by the same rule:** the door is bsv-rs 0.4.0's
  streaming decoder, which checks the whole frame and each BUMP's own
  agreement (a byte after the frame, a tree height of 0 or over 64, nodes
  that disagree, a missing sibling), each refused as invalid bytes with an
  offset. The in-memory parser read a body with a byte after its frame (the
  witness's `TrailingBytes` shape); such a body is refused now, at every
  door, stored rows included. No stored corpus was measured against the
  decoder by this change.
- **What stays, and why:** `COURIER_PROOF_MAX_BYTES` and
  `PUSH_PROOF_MAX_BYTES` (an 8 KiB JSON hex field of a courier's wire, not a
  BEEF); the ARC callback's 1 MiB JSON body (not a BEEF). bsv-low #585 owns
  and re-wires four doors after this change: the script door's byte budget,
  the census's `MAX_CENSUS_EVAL_BYTES`, the queue producer's
  `QUEUE_BEEF_SIZE_LIMIT` and the deferred-graph record.
- **What does not stream yet, said plainly:** the Worker reads a request
  body whole and D1 holds a BEEF in a row; the doors stream over the held
  value. The Worker has no R2 binding. The sites that take one are named in
  the code (`routes.rs` `/submit`, `d1_storage.rs` `insert_output` and
  `update_transaction_beef`, `queue.rs` `decode_beef_b64`); the binding and
  the buckets `low-overlay-beefs-beta` and `low-overlay-beefs` arrive with
  bsv-low #585 door 3.
- **Witnesses:** `crates/overlay-engine/tests/beef_doors.rs` and its
  siblings in `overlay-discovery`, `overlay-cloudflare` and `low-app-layer`
  (P0-5f's, inverted); `tools/lane-nl6/submit_any_size_ci.mjs` in
  `make ci-route`.

Nothing is deployed by this change.
