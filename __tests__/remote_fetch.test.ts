import { afterEach, beforeEach, describe, expect, test } from 'vitest'
import { createServer, Server } from 'node:http'
import { AddressInfo } from 'node:net'
import { readQuads, remoteFetchTimeoutMs } from '../lib/util.js'

const TURTLE = '<http://example.com/s> <http://example.com/p> "o".\n'

/**
 * A server that leaves the first `hangs` requests unanswered, like JitPack
 * looking up a snapshot it has not cached yet, and answers the others.
 */
async function serve(hangs: number) {
    let requests = 0
    const server = createServer((_, res) => {
        requests += 1
        if (requests <= hangs) return
        res.writeHead(200, { 'Content-Type': 'text/turtle' })
        res.end(TURTLE)
    })
    await new Promise<void>((resolve) => server.listen(0, resolve))
    const { port } = server.address() as AddressInfo
    return {
        server,
        url: `http://127.0.0.1:${port}/index.ttl`,
        requests: () => requests,
    }
}

describe('remote imports', () => {
    let server: Server | undefined
    const previous = process.env.RDFC_FETCH_TIMEOUT_MS

    beforeEach(() => {
        process.env.RDFC_FETCH_TIMEOUT_MS = '200'
    })

    afterEach(async () => {
        if (previous === undefined) delete process.env.RDFC_FETCH_TIMEOUT_MS
        else process.env.RDFC_FETCH_TIMEOUT_MS = previous
        server?.closeAllConnections()
        await new Promise((resolve) => server?.close(resolve))
        server = undefined
    })

    test('are tried again after a timeout', async () => {
        const s = await serve(1)
        server = s.server

        const quads = await readQuads([s.url])

        expect(s.requests()).toBe(2)
        expect(quads.some((q) => q.object.value === 'o')).toBe(true)
    })

    test('fail after the second timeout, naming the setting to wait longer', async () => {
        const s = await serve(Infinity)
        server = s.server

        await expect(readQuads([s.url])).rejects.toThrow(
            /Timed out after 200ms fetching .*index\.ttl, 2 times; set RDFC_FETCH_TIMEOUT_MS/,
        )
        expect(s.requests()).toBe(2)
    })
})

describe('remoteFetchTimeoutMs', () => {
    test('defaults to a minute', () => {
        expect(remoteFetchTimeoutMs({})).toBe(60_000)
        expect(remoteFetchTimeoutMs({ RDFC_FETCH_TIMEOUT_MS: '' })).toBe(60_000)
    })

    test('takes RDFC_FETCH_TIMEOUT_MS', () => {
        expect(remoteFetchTimeoutMs({ RDFC_FETCH_TIMEOUT_MS: '120000' })).toBe(
            120_000,
        )
    })

    test('ignores a value that is not a positive number', () => {
        expect(remoteFetchTimeoutMs({ RDFC_FETCH_TIMEOUT_MS: 'soon' })).toBe(
            60_000,
        )
        expect(remoteFetchTimeoutMs({ RDFC_FETCH_TIMEOUT_MS: '0' })).toBe(
            60_000,
        )
    })
})
