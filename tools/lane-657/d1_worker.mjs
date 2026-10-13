// Isolated fixture tables only. Every probe runs through the D1 Worker binding.
export default {
  async fetch(request, { DB }) {
    const { statements } = await request.json()
    try {
      const results = await DB.batch(statements.map(sql => DB.prepare(sql)))
      return Response.json({ results })
    } catch (error) {
      return Response.json({ error: { name: error.name, message: error.message,
        cause: error.cause ? { name: error.cause.name, message: error.cause.message } : null } })
    }
  }
}
