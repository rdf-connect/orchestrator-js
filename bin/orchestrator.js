#!/usr/bin/env node

import { start } from '../lib/index.js'
import path from 'path'

/**
 * @param {string[]} args
 */
function parseArgs(args) {
    let port = 50051
    const portFlagIdx = args.findIndex((v) => v === '-p' || v === '--port')
    if (portFlagIdx !== -1) {
        const portArgument = args.splice(portFlagIdx, 2)
        port = parseInt(portArgument[1])
    }
    if (isNaN(port) || port <= 0 || port >= 65536) {
        throw new Error('Invalid port number')
    }
    if (args.length !== 3) {
        const thisArgumentCount = portFlagIdx === -1 ? 3 : 5
        const receivedArgsCount =
            portFlagIdx === -1 ? args.length : args.length + 2
        throw new Error(
            `Invalid arguments, expected ${thisArgumentCount} arguments, got ${receivedArgsCount}`,
        )
    }
    const location = path.resolve(args[2])
    return { port, location }
}

try {
    const { port, location } = parseArgs(process.argv.slice())

    start(location, port).catch((ex) => {
        if (ex instanceof Error) {
            console.error(ex.stack)
        } else {
            console.error('An error happened')
            console.error(ex)
        }
        process.exit(1)
    })
} catch (ex) {
    const msg = ex instanceof Error ? ex.message : ex
    console.error('Incorrect cli usage:', msg)
    console.error(
        `Please use ${process.argv[0]} ${process.argv[1]} [-p PORT] PIPELINE.TTL`,
    )
}
