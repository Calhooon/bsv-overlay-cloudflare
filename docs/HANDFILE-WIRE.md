# The `handfile` family: the four funding-moment filings under one signature per key

bsv-low #848 (FAST-C), #461 design C (section 3.3 of `DESIGN-461-WALLET-CALLS-PER-HAND-2026-10-23.md` in bsv-low),
the owner's ruling of 2026-10-23 (record 1117). This page is the wire contract the client lane (recordPost.ts, the
outbox, the owed list's reader) consumes. The app layer half is `crates/low-app-layer/src/handfile.rs`; the shared
served type is `crates/overlay-discovery/src/handfile.rs`.

## 1. What it replaces

At funding a seat files four markers, six wallet signatures: `hopparty` (settle-key signature and identity
signature), `potparty` v1 (identity signature), `potparty` v2 (settle-key signature and identity signature) and
`potrefund` (identity signature). A `handfile` carries the same four markers' canonical bytes UNSIGNED plus ONE
identity signature and ONE settle-key signature over one digest: two signatures for four rows. The four direct
families stay as they are; a client may keep filing them directly.

## 2. The request

`POST /record?kind=handfile`, the records door (divergence D11), the same identity resolution as the other kinds
(`?identity=` for an anonymous caller, the session's identity for a verified one; the poster must be the identity
every payload names).

```json
{
  "hopTxid": "<64 hex: the funding hop's txid>",
  "payloads": ["<hex>", "<hex>", "<hex>", "<hex>"],
  "identitySigHex": "<hex DER>",
  "settleSigHex": "<hex DER>"
}
```

`payloads`, in this order and no other:

| index | family | the payload (the family's IDENTITY CHALLENGE, byte for byte what its identity signature signs today) | length |
|---|---|---|---|
| 0 | hopparty | `"LOW/hopparty/v1" ‖ identity(33) ‖ opponent(33) ‖ gameId(32) ‖ hopVout(4 LE) ‖ hopSats(8 LE) ‖ seatSettlePubkey(33)` (the client's `hopPartyChallenge`) | 158 |
| 1 | potparty v1 | `"LOW/potparty/v1" ‖ identity(33) ‖ opponent(33) ‖ gameId(32) ‖ potTxid(32) ‖ potVout(4 LE) ‖ recoveryHeight(4 LE)` (`potPartyChallenge`) | 153 |
| 2 | potparty v2 | the v1 layout under `"LOW/potparty/v2"` plus `seatSettlePubkey(33)` (`potPartyV2Challenge`) | 186 |
| 3 | potrefund | `"LOW/potrefund/v1" ‖ identity(33) ‖ gameId(32) ‖ potTxid(32) ‖ potVout(4 LE) ‖ refundRaw` (`potRefundChallenge`; the raw is the pre-signed refund transaction, 1 to 100,000 bytes) | 118 and up |

The txids are the 32 bytes of the display hex, as the markers carry them. The body size cap is the shared door's
`RECORD_POST_MAX_BODY_BYTES`: `RECORD_POST_MAX_SCRIPT_BYTES * 2 + 2_048`, or 203,072 bytes. It accommodates the
refund raw's full 100,000 bytes as hex, the other three payloads, both signatures, the hop txid and the JSON frame.
The refund raw's own 100,000-byte limit is unchanged.

## 3. The digest and the two signatures

```
preimage = "low/handfile/v1" ‖ hopTxid(32) ‖ len(p0)(4 LE) ‖ p0 ‖ len(p1)(4 LE) ‖ p1 ‖ len(p2)(4 LE) ‖ p2 ‖ len(p3)(4 LE) ‖ p3
digest   = sha256(preimage)
```

- `identitySigHex`: the wallet's `createSignature({ data: preimage, protocolID: [1, 'low potparty'], keyID:
  gameId (lowercase hex), counterparty: 'anyone' })`, exactly the potparty families' recipe with the batch preimage
  as the data. Verified server-side as the potparty identity signature is (BRC-42/43 'anyone' verification under
  the poster's identity key).
- `settleSigHex`: the seat's settle key (`[2, 'low settle']`, the key the v2 payload and the hopparty payload name
  as `seatSettlePubkey`) over `digest`, plain ECDSA: `createSignature({ data: preimage, protocolID: [2, 'low settle'],
  keyID: ..., counterparty: 'self' })` or the equivalent, as the v2 `seatSig` and the hopparty seat signature are
  made today (the wallet hashes `data` once).

Both signatures go through the one canonical-DER gate (strict DER, low-S, byte-exact re-encoding): a padded or
high-S twin is refused. Signatures are excluded from the batch key, so a re-signed batch is the same batch.

## 4. What the door checks, and the refusal codes

In order; the first refusal answers and nothing is written. The raw body cap precedes JSON parsing. Within the
decoded body, shape and payload-size checks precede the canonical-DER gate; the four parsers, consistency check
and poster check precede either ECDSA verification. The code column names the refusal's counter class.

| status | code (`applayer_record_refused_<code>_total`) | when |
|---|---|---|
| 413 | `body_too_large` | the raw body exceeds `RECORD_POST_MAX_BODY_BYTES` |
| 400 | `bad_body` | the body is not the JSON above |
| 400 / 413 | `handfile_shape`, `script_too_large` | check `hopTxid` and the four-slot shape, then each payload's size and hex in order, then both signatures' hex |
| 422 | `signature_invalid` | the identity signature is not canonical DER |
| 422 | `settle_signature_invalid` | the settle-key signature is not canonical DER |
| 400 | `handfile_payload_not_a_marker` | a payload is not its family's canonical bytes: the family's own parser refuses it (a self-paired hopparty or potparty, a wrong length or tag, a v1 payload in the v2 slot, an empty refund raw) or the family's challenge builder does not give the payload back |
| 422 | `handfile_inconsistent` | the four payloads do not name one seat's one hand: identity, opponent, game, pot outpoint, recovery height and settle key must agree across them |
| 403 | `poster_mismatch` | the poster is not the identity the payloads name |
| 422 | `signature_invalid` | the identity signature does not verify under the poster's identity over the digest (the hop txid and the four payloads are what it binds) |
| 422 | `settle_signature_invalid` | the settle-key signature does not verify under the v2 payload's settle key |
| 422 | `refund_does_not_spend_the_pot` | the refund raw's first input is not the pot outpoint it names |
| 422 | `refund_not_the_presigned_spend` | the index holds the pot and the raw is not its pre-signed 2-of-2 spend in the template's unlock shape (`settle_signers_for_spend == Coop`), exactly the direct `potrefund` rule; an unindexed pot files at `refundValid: 0`, as today |
| 425 | `hop_not_indexed` | the index does not hold the hop's bytes yet (`pot_beefs` under the hop txid): file again once the hop is admitted, the outbox's rule for `hopsweep` |
| 422 | `hop_marker_refuted` | the hop's output at `hopVout` is not the P2PKH of the batch's settle key paying `hopSats` |
| 409 / 429 | `too_many_filed`, `daily_cap_reached` | for each active family in order (hopparty, potparty, potrefund), check its content cap, then its daily cap |

The content caps are potparty 2 (v1 and v2) and potrefund 1 per (poster, game, pot), and hopparty 1 per
(poster, hop outpoint), across games. The potparty pair is counted together with both active keys excluded;
existing copies of either content do not consume a second place. The daily cap is 200 filed rows per poster and
family: only missing rows add to the day's count. Chain-covered slots reserve no new place.

A cap refusal retains its `error` text and adds `family`: `hopparty`, `potparty` or `potrefund`. For example:

```json
{ "error": "this identity already holds its filed rows for this family, game and pot", "family": "potparty" }
```

## 5. What is written

The batch is exploded into the four families' own tables exactly as a filed row, keyed by the SAME content key a
direct filing of that marker gets (`filed:<sha256 of the family tag and the signed fields>[..56]`), so a direct
filing and a handfile of one marker are one row and a re-filing is a no-op:

- `potparty_records`: the v1 row and the v2 row, `sigHex` the batch's identity signature, the v2 row's
  `seatSigHex` the batch's settle signature, `sigValid = 1` (both batch signatures verified at the write).
- `potrefund_records`: the refund row, `sigHex` the batch's identity signature, `refundValid` as the direct door
  binds it.
- `hopparty_records`: the hop row under the HOP's txid (the derived links and seats join it as a chain marker) at
  `outputIndex = 0x8000_0000 | (the row's content hash & 0x7fff_ffff)`, an index no transaction has; `seatSigHex`
  and `identitySigHex` the batch's two signatures; `hopLockHex`, `hopSatsOnChain`, `containerOutputs` the facts of
  the index's own copy of the hop; `markerValid = 1`.
- `handfile_batches`: one row per batch, keyed `handfile:<digest hex>`: the identity, the game, the hop txid, the
  four payloads (a JSON array of hex), the two signatures, the time.
- `handfile_rows`: (family, rowTxid, rowVout) to (batchKey, index), one per exploded row.

A family whose verified chain marker the index already holds for this identity, game and pot writes nothing for
that family (the direct door's `alreadyIndexed` rule), the rest is written. The write is one D1 batch.

The answer on a filing that changes the stored rows:

```json
{
  "filed": true,
  "kind": "handfile",
  "key": "handfile:<digest hex>",
  "rows": {
    "hopparty": { "txid": "<hop txid>", "outputIndex": 2147483648 },
    "potpartyV1": "filed:<56 hex>",
    "potpartyV2": "filed:<56 hex>",
    "potrefund": "filed:<56 hex>"
  },
  "refundValid": 0
}
```

A re-filing that changes nothing returns the same `kind`, `key`, `rows` and `refundValid` fields with
`filed: false` and `alreadyFiled: true`, as the direct door does. It marks nothing stale and recomputes nothing.
A re-filing that promotes an existing refund from rank 0 to rank 1 is a change: it returns `filed: true` and
recomputes the affected view. An unreadable write count is treated as a change, matching the direct door.

The write-time verdicts are what every reader of the K binding reads: the owed list's evidence rule
(`sigValid = 1 AND seatSettlePubkey IS NOT NULL` on a potparty row) reads an exploded v2 row exactly as a directly
filed one. The overlay's relatch passes, which recompute a row's verdict from its own signature columns, leave a
door-filed row alone (a `filed:` key, a handfile vout): its columns hold the batch's signatures.

## 6. The served `batch`

The one additive wire delta. A served row that was exploded from a batch MAY carry:

```json
"batch": {
  "key": "handfile:<digest hex>",
  "hopTxid": "<64 hex>",
  "payloads": ["<hex>", "<hex>", "<hex>", "<hex>"],
  "sigs": { "identity": "<hex DER>", "settle": "<hex DER>" },
  "index": 2
}
```

where `index` is the served row's place among `payloads`. The client verifies inclusion (the payload at `index`
is the row's own canonical bytes, rebuilt from the row's fields) and the two signatures over the recomputed
digest (section 3). A row filed directly or admitted from the chain carries no `batch`: its bytes are unchanged.

Where it is served: the overlay's lookup services `ls_potparty` (`partyFor`, `byPot`), `ls_potrefund` and
`ls_hopparty` (every entry), and the app layer's `/hops-view` entries and `/refund-backups` rows (at most 16 batch
reads a view call; a row past that is served without its batch, never withheld). A reader that cannot read the
batch serves the row without it.

## 7. What the client lane does with this

- `recordPost.ts`: at funding, compute the four challenges as today, build the preimage, ask the wallet for the
  two signatures (one `[1,'low potparty']` identity signature with the gameId keyID, one settle-key signature),
  POST the body of section 2; on `hop_not_indexed` (425) the outbox re-files, as for `hopsweep`.
- The outbox: a `handfile` entry is one item; its retry is idempotent by content.
- The owed list's reader and every row verifier: a row with `batch` is verified through the batch; a row without
  one as today.
