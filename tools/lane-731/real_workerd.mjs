#!/usr/bin/env node
// bsv-low #731: print the workerd binary the `wrangler` on PATH would run (the one `npx wrangler` resolves in the
// tier's crates, which hold no node_modules of their own), so `workerd-sealed.sh` can exec it under the seal.
import { createRequire } from 'node:module'
import { realpathSync } from 'node:fs'
import { execFileSync } from 'node:child_process'

const bin = execFileSync('sh', ['-c', 'command -v wrangler'], { encoding: 'utf8' }).trim()
const require = createRequire(realpathSync(bin))
const workerd = require(require.resolve('workerd'))
const path = workerd.default ?? workerd
if (typeof path !== 'string' || !path) throw new Error(`no workerd binary resolved from ${bin}`)
console.log(realpathSync(path))
