#!/usr/bin/env node

import { start } from '../lib/orchestrator.js'
import path from 'path'

const location = path.resolve(process.argv[2])
start(location).catch((ex) => {
  if (Array.isArray(ex)) {
    console.log('Errors happened')
    for (const e of ex) {
      console.error(e)
    }
  } else {
    console.log('Error happened')
    console.error(ex)
  }
  throw ex
})
