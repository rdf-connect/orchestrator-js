import { start } from '../lib/orchestrator.js'
import path from 'path'

const location = path.resolve(process.argv[2])
start(location)
