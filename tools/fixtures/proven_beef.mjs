import { createHash } from 'node:crypto'

const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
const varint = (n) => {
  if (n < 0xfd) return Buffer.from([n])
  if (n <= 0xffff) { const b = Buffer.alloc(3); b[0] = 0xfd; b.writeUInt16LE(n, 1); return b }
  return Buffer.concat([Buffer.from([0xfe]), u32(n)])
}
export const txidLE = (raw) => createHash('sha256').update(createHash('sha256').update(raw).digest()).digest()
const isCoinbase = (raw) => raw[4] === 1 && raw.subarray(5, 37).equals(Buffer.alloc(32)) && raw.readUInt32LE(37) === 0xffffffff

/** A null outpoint with the serialized height of the synthetic block. */
export const blockCoinbaseInput = (height = 800_000) => Buffer.concat([
  Buffer.alloc(32), u32(0xffffffff), Buffer.from([4, 3]), u32(height).subarray(0, 3), u32(0xffffffff),
])

/** A synthetic coinbase paying an OP_TRUE output, for a funding parent. */
export const fundingCoinbase = (sats, height = 799_900) => Buffer.concat([
  u32(1), Buffer.from([1]), blockCoinbaseInput(height),
  Buffer.from([1]), u64(sats), Buffer.from([1, 0x51]), u32(0),
])

/** Prove the leading raw transaction; later raws remain unproven spends.
 * With no parent the anchor must be the coinbase of a one-transaction block.
 * With a parent it spends that older funding coinbase and is leaf 1 beside
 * a different coinbase in the current two-transaction block.
 * These are synthetic proofs for local harnesses, never evidence of real mining.
 */
export function provedBeefV1(raws, parentRaw) {
  const anchor = raws[0]
  let leaves
  if (parentRaw) {
    if (!isCoinbase(parentRaw) || !anchor.subarray(5, 37).equals(txidLE(parentRaw)) || anchor.readUInt32LE(37) !== 0) {
      throw new Error('the two-leaf anchor must spend its older funding parent output 0')
    }
    const siblingRaw = fundingCoinbase(0, 800_000)
    leaves = Buffer.concat([varint(2), varint(0), Buffer.from([0]), txidLE(siblingRaw), varint(1), Buffer.from([2]), txidLE(anchor)])
  } else {
    if (!isCoinbase(anchor)) throw new Error('a lone-leaf anchor must be coinbase-shaped')
    leaves = Buffer.concat([varint(1), varint(0), Buffer.from([2]), txidLE(anchor)])
  }
  const bump = Buffer.concat([varint(800_000), Buffer.from([1]), leaves])
  return Buffer.concat([
    Buffer.from([1, 0, 0xbe, 0xef]), varint(1), bump, varint(raws.length),
    ...raws.flatMap((raw, i) => [raw, i === 0 ? Buffer.from([1, 0]) : Buffer.from([0])]),
  ])
}
