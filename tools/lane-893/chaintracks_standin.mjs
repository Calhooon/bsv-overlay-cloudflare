// The belt tier's CHAINTRACKS stand-in (a second worker in the same `wrangler dev`, bound as the service): the tip the
// fleet seed was judged at (`examples/owed_read_seed.rs`, 900000), so the judge judges the belt's finds. Since main's
// TIP-877 (PR 172) the app layer reads a VERIFIED header tip (`findChainTipHeaderHex`, then a height's header), so
// both routes answer one fixed header beside the legacy `getPresentHeight`.
const HEIGHT = 900000
const header = height => ({
  status: 'success',
  value: { height, hash: 'ab'.repeat(32), merkleRoot: 'cd'.repeat(32), time: Math.floor(Date.now() / 1000) },
})
export default {
  async fetch(request) {
    const { pathname, searchParams } = new URL(request.url)
    if (pathname === '/getPresentHeight') return Response.json({ status: 'success', value: HEIGHT })
    if (pathname === '/findChainTipHeaderHex') return Response.json(header(HEIGHT))
    if (pathname === '/findHeaderHexForHeight') {
      const height = Number(searchParams.get('height'))
      return height === HEIGHT ? Response.json(header(height)) : new Response('{}', { status: 404 })
    }
    return new Response('{}', { status: 404 })
  },
}
