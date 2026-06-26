#!/usr/bin/env node

import { start } from '../lib/index.js'
import net from 'net'
import path from 'path'

const DEFAULT_PORT = 50051

/**
 * Checks whether the given TCP port is available to bind on 0.0.0.0.
 *
 * @param {number} port
 * @returns {Promise<boolean>}
 */
function isPortAvailable(port) {
    return new Promise((resolve) => {
        const tester = net
            .createServer()
            .once('error', (err) => {
                if (err.code === 'EADDRINUSE') {
                    resolve(false)
                } else {
                    // Treat any other error as the port not being usable.
                    resolve(false)
                }
            })
            .once('listening', () => {
                tester.close(() => resolve(true))
            })
            .listen(port, '0.0.0.0')
    })
}

/**
 * Resolves the port to bind on.
 *
 * If the port was not explicitly specified, starts from the default port and
 * increments until a free port is found. If the port was explicitly specified
 * and is already in use, an error is thrown.
 *
 * @param {number} port
 * @param {boolean} explicit - Whether the port was provided via CLI arguments
 * @returns {Promise<number>}
 */
async function resolvePort(port, explicit) {
    if (explicit) {
        if (!(await isPortAvailable(port))) {
            throw new Error(`Port ${port} is already in use`)
        }
        return port
    }

    let candidate = port
    while (candidate < 65536) {
        if (await isPortAvailable(candidate)) {
            return candidate
        }
        candidate++
    }
    throw new Error('No available port found')
}

/**
 * @param {string[]} args
 */
function parseArgs(args) {
    let port = DEFAULT_PORT
    let portExplicit = false
    const portFlagIdx = args.findIndex((v) => v === '-p' || v === '--port')
    if (portFlagIdx !== -1) {
        const portArgument = args.splice(portFlagIdx, 2)
        port = parseInt(portArgument[1])
        portExplicit = true
    }
    if (isNaN(port) || port <= 0 || port >= 65536) {
        throw new Error('Invalid port number')
    }

    let provenanceLocation
    const provenanceFlagIdx = args.findIndex((v) => v === '--provenance')
    if (provenanceFlagIdx !== -1) {
        const provenanceArgument = args.splice(provenanceFlagIdx, 2)
        provenanceLocation = provenanceArgument[1]
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
    return { port, portExplicit, location, provenanceLocation }
}

let parsed
try {
    parsed = parseArgs(process.argv.slice())
} catch (ex) {
    const msg = ex instanceof Error ? ex.message : ex
    console.error('Incorrect cli usage:', msg)
    console.error(
        `Please use ${process.argv[0]} ${process.argv[1]} [-p PORT] PIPELINE.TTL`,
    )
    process.exit(1)
}

const { port, portExplicit, location, provenanceLocation } = parsed

try {
    const resolvedPort = await resolvePort(port, portExplicit)
    await start(location, resolvedPort, provenanceLocation)
} catch (ex) {
    if (ex instanceof Error) {
        console.error(ex.stack)
    } else {
        console.error('An error happened')
        console.error(ex)
    }
    process.exit(1)
}
