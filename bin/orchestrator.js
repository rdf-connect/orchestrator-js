#!/usr/bin/env node

import { start } from '../lib/index.js'
import path from 'path'

const location = path.resolve(process.argv[2])
start(location).catch((ex) => {
    if (ex instanceof Error) {
        console.error(ex.stack)
    } else {
        console.error('An error happened')
        console.error(ex)
    }
    process.exit(1)
})
