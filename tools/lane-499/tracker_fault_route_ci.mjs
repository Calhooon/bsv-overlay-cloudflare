import { basename } from 'node:path'
// NS3D-M1: real Worker + local D1 fault, no production data or network spend.
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
const [base, state] = process.argv.slice(2)
assert(base?.startsWith('http://127.0.0.1:') && state && basename(state).startsWith('lane499-d1-state.'))
const d1 = (sql) => execFileSync('npx', ['wrangler', 'd1', 'execute', 'low-overlay-db', '--local', '--persist-to', state, '--command', sql], {
  cwd: new URL('../../crates/low-app-layer', import.meta.url), timeout: 60000, stdio: 'pipe',
})
const parent = 'f1'.repeat(32), seen = 'f2'.repeat(32)
const paths = [`/tx-any/${seen}`, `/tx-any?txids=${seen}`, `/utxo-status?outpoints=${parent}.0`, `/spent-any?outpoints=${parent}.0`]
const get = async (path) => {
  const response = await fetch(base + path, { signal: AbortSignal.timeout(15000) })
  assert.equal(response.status, 200, `${path}: word lookup must not fail an available legacy answer`)
  return response.json()
}
const legacy = (v) => {
  if (Array.isArray(v)) return v.map(legacy)
  if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).filter(([k]) => !['tracker','spenderWord','coinWord','fundingWord','lock','spendDepth'].includes(k)).map(([k,v]) => [k,legacy(v)]))
  return v
}
d1(`UPDATE hop_chain_probes SET probedAtMs=${Date.now()}, spendingTxid='${seen}', spent=1, spentConfirmed=1 WHERE outpoint='${parent}.0'`)
const before = []
for (const path of paths) before.push(await get(path))
assert.equal(before[0].tracker.word, 'seen', 'fixture has a stored word before the fault')
// bsv-low #862 (L7-DEPTH): the host's spend depth rides beside the word (the render's SPEND_DEPTH, 1), and a fault's legacy body carries none
assert.equal(before[0].spendDepth, 1, 'the subject serves spendDepth beside tracker')
assert.equal(before[2][0].spendDepth, 1, 'a /utxo-status entry serves spendDepth beside lock')
let renamed = false
try {
  d1('ALTER TABLE tx_words RENAME TO tx_words_fault_pin')
  renamed = true
  for (let n = 0; n < paths.length; n++) {
    const after = await get(paths[n])
    assert.deepEqual(after, legacy(before[n]), `${paths[n]}: every legacy key/value unchanged and word fields absent`)
    console.log(`PASS NS3D-M1 real D1 fault -> 200, legacy unchanged, tracker absent: ${paths[n]}`)
  }
} finally {
  if (renamed) d1('ALTER TABLE tx_words_fault_pin RENAME TO tx_words')
}
