// Isolated fixture tables only. Every probe runs through the D1 Worker binding.
export default {
  async fetch(request, { DB }) {
    const { statements, budget } = await request.json()
    if (budget) {
      let completedBatches = 0
      let completedStatements = 0
      try {
        for (let i = 0; i < budget.batches; i++) {
          const results = await DB.batch(Array.from({ length: budget.width }, () => DB.prepare('SELECT 1 AS ok')))
          if (results.length !== budget.width || results.some(r => !r.success || r.results[0].ok !== 1)) throw new Error('unexpected SELECT result')
          completedBatches++
          completedStatements += results.length
        }
        return Response.json({ completedBatches, completedStatements, error: null })
      } catch (error) {
        return Response.json({ completedBatches, completedStatements,
          error: { name: error.name, message: error.message, cause: error.cause?.message ?? null } })
      }
    }
    try {
      const results = await DB.batch(statements.map(sql => DB.prepare(sql)))
      return Response.json({ results })
    } catch (error) {
      return Response.json({ error: { name: error.name, message: error.message,
        cause: error.cause ? { name: error.cause.name, message: error.cause.message } : null } })
    }
  }
}
