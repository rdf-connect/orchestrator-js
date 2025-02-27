import { start } from '../lib/client.js'

const host = process.argv[2]
const port = process.argv[3]

start(host, port)
