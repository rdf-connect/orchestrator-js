#!/usr/bin/env node

import { start } from '../lib/index.js'
import path from 'path'

const location = path.resolve(process.argv[2])
start(location).catch(() => {
    console.log('An error happened')
    process.exit(1)
})
