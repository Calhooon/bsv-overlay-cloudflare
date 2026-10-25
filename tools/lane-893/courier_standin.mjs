// The belt tier's courier stand-in: answers by path (the worker's COURIER_SINK keeps the courier's path), counts
// every call, and switches world on `POST /__mode` (`find`: WoC says unspent, BananaBlocks corroborates; `429`: every
// courier rate-limits). `GET /__calls` answers the calls since the last `POST /__reset`.
import http from 'node:http'
const port = Number(process.argv[2])
let mode = 'find'
let calls = []
http.createServer((req, res) => {
  if (req.url === '/__calls') return res.end(JSON.stringify(calls))
  if (req.url === '/__reset') { calls = []; return res.end('reset') }
  if (req.url.startsWith('/__mode/')) { mode = req.url.slice('/__mode/'.length); return res.end(mode) }
  calls.push(req.url)
  if (mode === '429') { res.statusCode = 429; return res.end('{}') }
  if (/\/tx\/[0-9a-f]{64}\/\d+\/spent$/.test(req.url)) { res.statusCode = 404; return res.end('{}') }
  if (/\/txo\/[0-9a-f]{64}\/\d+\/spend$/.test(req.url)) { res.setHeader('content-type', 'application/json'); return res.end('{"spent":false}') }
  res.statusCode = 404
  res.end('{}')
}).listen(port, '127.0.0.1', () => console.log(`STANDIN ${port}`))
