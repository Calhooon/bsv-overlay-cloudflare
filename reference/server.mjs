// Parity reference: mainline @bsv/overlay-express@2.2.0
// Boots a minimal headless instance for the differential parity harness.
//
// Env contract (see docker-compose.yml):
//   SERVER_PRIVATE_KEY  hex-encoded 32-byte secp256k1 private key
//   HOSTING_URL         public URL (http://localhost:8080 for the harness)
//   KNEX_URL            postgres connection string
//   MONGO_URL           mongodb connection string
//   PORT                HTTP port (default 8080)

import OverlayExpress from '@bsv/overlay-express'
import { WhatsOnChain, FetchHttpClient } from '@bsv/sdk'

const PRIVATE_KEY = process.env.SERVER_PRIVATE_KEY
const HOSTING_URL = process.env.HOSTING_URL || 'http://localhost:8080'
const KNEX_URL = process.env.KNEX_URL
const MONGO_URL = process.env.MONGO_URL
const PORT = parseInt(process.env.PORT || '8080', 10)
const NODE_NAME = process.env.NODE_NAME || 'parityref'
// Admin Bearer token for authed admin routes. Mainline accepts this as a
// constructor arg (defaults to a fresh UUID if omitted). We pass our harness
// token so /admin/* happy-path corpus entries can authenticate the same way
// on both sides.
const ADMIN_TOKEN = process.env.ADMIN_TOKEN

if (!PRIVATE_KEY) throw new Error('SERVER_PRIVATE_KEY is required')
if (!KNEX_URL) throw new Error('KNEX_URL is required')
if (!MONGO_URL) throw new Error('MONGO_URL is required')

console.log(`[parityref] starting overlay-express 2.2.0 on :${PORT}`)
console.log(`[parityref] hosting=${HOSTING_URL} mongo=${MONGO_URL.replace(/\/\/.*@/, '//…@')} knex=${KNEX_URL.replace(/\/\/.*@/, '//…@')}`)

const server = new OverlayExpress(NODE_NAME, PRIVATE_KEY, HOSTING_URL, ADMIN_TOKEN)

server.configurePort(PORT)
await server.configureKnex(KNEX_URL)
await server.configureMongo(MONGO_URL)
// GASP sync ON so /requestSyncResponse + /requestForeignGASPNode routes
// register (mainline only mounts them when GASP is enabled). Empty SHIP
// store at boot means no peers are discovered — the server still idles
// happily and the routes respond deterministically to harness probes.
server.configureEnableGASPSync(true)
// Explicit chain tracker with an explicit fetch client. The default
// `configureChainTracker()` instantiates WhatsOnChain + @bsv/sdk's
// DefaultHttpClient, which throws "No method available..." on Node 20
// ESM because it uses `typeof require !== 'undefined'` to detect Node,
// which is false in ESM modules. Bug in @bsv/sdk.
const fetchClient = new FetchHttpClient(globalThis.fetch)
server.configureChainTracker(new WhatsOnChain('main', { httpClient: fetchClient }))
await server.configureEngine()
await server.start()

console.log(`[parityref] ready on :${PORT}`)
