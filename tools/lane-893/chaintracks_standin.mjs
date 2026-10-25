// The belt tier's CHAINTRACKS stand-in (a second worker in the same `wrangler dev`, bound as the service): the tip the
// fleet seed was judged at (`examples/owed_read_seed.rs`, 900000), so the judge judges the belt's finds.
export default {
  async fetch(request) {
    const { pathname } = new URL(request.url)
    if (pathname === '/getPresentHeight') return Response.json({ status: 'success', value: 900000 })
    return new Response('{}', { status: 404 })
  },
}
