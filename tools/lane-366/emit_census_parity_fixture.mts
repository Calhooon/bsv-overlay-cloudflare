/**
 * #366 cross-language parity fixture EMITTER (run manually, committed output).
 *
 * Rule 16: the Rust census (`submit_census::census_verdict`) and the client's
 * TS predicate (`bsv-low app/src/lib/overlay.ts beefGatedReadiness`) sit on a
 * cross-language boundary. Rather than re-deriving the client's convention in
 * Rust tests (which would encode ONE side's understanding on BOTH sides of
 * every comparison — the bsv-low #343 lesson), this script drives the REAL
 * producers:
 *
 *  - bodies are serialized by the REAL client-side library (`@bsv/sdk`, the
 *    same Beef/Transaction/MerklePath code every client `/submit` body rides
 *    through), and
 *  - the recorded verdicts come from the REAL `beefGatedReadiness`, imported
 *    from the bsv-low checkout (read-only).
 *
 * The output is committed at
 * `crates/overlay-cloudflare/tests/fixtures/census/client_parity.fixture.json`
 * and read by `submit_census.rs::census_parity_fixture_agrees_with_the_client_predicate`,
 * which asserts the documented verdict MAPPING (not identity — the two
 * predicates answer different questions; see submit_census.rs module doc).
 *
 * Regenerate (from the bsv-low app dir, whose node_modules carry vite-node +
 * the real @bsv/sdk):
 *
 *   cd /Users/johncalhoun/bsv/bsv-low/app && \
 *     npx vite-node <this-repo>/tools/lane-366/emit_census_parity_fixture.mts
 *
 * …then commit the diff HERE. Never hand-edit the fixture.
 */
import { writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Beef, LockingScript, MerklePath, Transaction, UnlockingScript } from '@bsv/sdk'
// THE REAL CLIENT PREDICATE — imported from the bsv-low checkout, read-only.
import { beefGatedReadiness } from '/Users/johncalhoun/bsv/bsv-low/app/src/lib/overlay'

const OUT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../crates/overlay-cloudflare/tests/fixtures/census/client_parity.fixture.json',
)

/** The client suite's `walletAtomicBeef` construction (overlay.gatedReadiness
 *  .test.ts): a funded parent + the child that spends it — the shape a real
 *  wallet `createAction` AtomicBEEF has. */
function fundedParentAndChild(): { parent: Transaction; subject: Transaction } {
  const parent = new Transaction()
  parent.addOutput({ satoshis: 5_000, lockingScript: LockingScript.fromHex('51') })
  const subject = new Transaction()
  subject.addInput({
    sourceTXID: parent.id('hex'),
    sourceOutputIndex: 0,
    unlockingScript: UnlockingScript.fromHex('00'),
    sequence: 0xffffffff,
  })
  subject.addOutput({ satoshis: 0, lockingScript: LockingScript.fromHex('006a04deadbeef') })
  subject.addOutput({ satoshis: 4_900, lockingScript: LockingScript.fromHex('52') })
  return { parent, subject }
}

interface Case {
  label: string
  beefHex: string
  subjectTxid: string
  /** Does the SUBJECT carry a merkle-proof claim (the mined-claim shape)? */
  subjectProven: boolean
  client: { ready: boolean; reason: string | null; subjectAssumed: boolean }
}

const cases: Case[] = []

function record(label: string, beefBytes: number[] | Uint8Array, subjectTxid: string, subjectProven: boolean) {
  const v = beefGatedReadiness(beefBytes, subjectTxid)
  cases.push({
    label,
    beefHex: Buffer.from(beefBytes as Uint8Array).toString('hex'),
    subjectTxid,
    subjectProven,
    client: { ready: v.ready, reason: v.reason, subjectAssumed: v.subjectAssumed },
  })
}

// 1. ready-unmined: ancestry-carrying (the honest wallet AtomicBEEF shape).
{
  const { parent, subject } = fundedParentAndChild()
  const beef = new Beef()
  beef.mergeTransaction(parent)
  beef.mergeTransaction(subject)
  record('ready-unmined: funded parent + spending subject', beef.toBinary(), subject.id('hex'), false)
}

// 2. ready-proven: subject carries a bump COMMITTING TO ITSELF (mined-claim).
{
  const { subject } = fundedParentAndChild()
  const txid = subject.id('hex')
  subject.merklePath = new MerklePath(800_000, [[{ offset: 0, hash: txid, txid: true }]])
  const beef = new Beef()
  beef.mergeTransaction(subject)
  record('ready-proven: subject with self-committing bump (mined claim)', beef.toBinary(), txid, true)
}

// 3. not-ready: no ancestry, no proof — THE bsv-low #351 ten-surface shape.
{
  const { subject } = fundedParentAndChild()
  const beef = new Beef()
  beef.mergeRawTx(Array.from(Buffer.from(subject.toHex(), 'hex')))
  record('not-ready: subject alone, sources absent (the #351 shape)', beef.toBinary(), subject.id('hex'), false)
}

// 4. not-ready: parent present only as a TXID-ONLY stub (gate A HIGH-3 —
//    stubs survive into stored hop BEEFs by design via beefPrune).
{
  const { parent, subject } = fundedParentAndChild()
  const beef = new Beef()
  beef.mergeTxidOnly(parent.id('hex'))
  beef.mergeRawTx(Array.from(Buffer.from(subject.toHex(), 'hex')))
  record('not-ready: parent is a txid-only stub', beef.toBinary(), subject.id('hex'), false)
}

// 5. not-ready: PARTIAL ancestry — one source present, one absent (the
//    residual shape the issue names).
{
  const present = new Transaction()
  present.addOutput({ satoshis: 3_000, lockingScript: LockingScript.fromHex('51') })
  const absent = new Transaction()
  absent.addOutput({ satoshis: 2_000, lockingScript: LockingScript.fromHex('51') })
  const subject = new Transaction()
  for (const src of [present, absent]) {
    subject.addInput({
      sourceTXID: src.id('hex'),
      sourceOutputIndex: 0,
      unlockingScript: UnlockingScript.fromHex('00'),
      sequence: 0xffffffff,
    })
  }
  subject.addOutput({ satoshis: 4_500, lockingScript: LockingScript.fromHex('52') })
  const beef = new Beef()
  beef.mergeTransaction(present)
  beef.mergeRawTx(Array.from(Buffer.from(subject.toHex(), 'hex')))
  record('not-ready: partial ancestry (one source absent)', beef.toBinary(), subject.id('hex'), false)
}

// 6. not-ready: unparseable bytes.
record('not-ready: unparseable garbage', Uint8Array.from([0xde, 0xad, 0xbe, 0xef]), '0'.repeat(64), false)

// Producer honesty gate: the fixture must cover all three mapping rows, or
// regenerating it would silently shrink what the Rust cell can prove.
const rows = {
  readyUnmined: cases.some((c) => c.client.ready && !c.subjectProven),
  readyProven: cases.some((c) => c.client.ready && c.subjectProven),
  notReady: cases.some((c) => !c.client.ready),
}
if (!rows.readyUnmined || !rows.readyProven || !rows.notReady) {
  console.error('FIXTURE NOT EMITTED — missing mapping rows:', rows)
  for (const c of cases) console.error(` ${c.label}: ready=${c.client.ready} reason=${c.client.reason}`)
  process.exit(1)
}

writeFileSync(
  OUT,
  JSON.stringify(
    {
      generatedBy: 'tools/lane-366/emit_census_parity_fixture.mts (real @bsv/sdk bodies, real beefGatedReadiness verdicts)',
      cases,
    },
    null,
    2,
  ) + '\n',
)
console.log(`wrote ${cases.length} cases to ${OUT}`)
for (const c of cases) console.log(` ${c.label}: ready=${c.client.ready} reason=${c.client.reason}`)
