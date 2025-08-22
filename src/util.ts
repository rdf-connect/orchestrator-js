import { JsonLdParser } from 'jsonld-streaming-parser'
import { Context, Document, modelQuads } from '.'
import { Quad } from '@rdfjs/types'
import { NamedNode, Parser, PrefixCallback } from 'n3'
import { createNamespace, createTermNamespace } from '@treecg/types'
import { readFile } from 'fs/promises'
import { getLoggerFor, prefixFound } from './logUtil'

import JSZip from 'jszip'

const OWL = createTermNamespace('http://www.w3.org/2002/07/owl#', 'imports')
const logger = getLoggerFor(['util.ts'])

export const RDFC = createNamespace(
    'https://w3id.org/rdf-connect#',
    (x) => x,
    'Reader',
)

export function jsonld_to_string(document: Document): string
export function jsonld_to_string(document: Document, context: Context): string
export function jsonld_to_string(document: Document, context?: Context): string
export function jsonld_to_string(
    document: Document,
    context?: Context,
): string {
    document = JSON.parse(JSON.stringify(document))
    if (context === undefined) {
        context = <Context>(document['@context'] || {})
        delete document['@context']
    }

    const json = `{"@context": ${JSON.stringify(context)},${JSON.stringify(
        document,
    ).slice(1)}`
    return json
}

export async function jsonld_to_quads(document: Document): Promise<Quad[]>
export async function jsonld_to_quads(
    document: Document,
    context: Context,
): Promise<Quad[]>
export async function jsonld_to_quads(document: Document, context?: Context) {
    const quads: Quad[] = []
    const myParser = new JsonLdParser()
    const endPromise = new Promise((res, rej) => {
        myParser.on('end', res)
        myParser.on('error', rej)
    })

    myParser.on('data', (quad) => quads.push(quad))
    myParser.write(jsonld_to_string(document, context))

    myParser.end()
    await endPromise
    return quads
}

async function parseText(
    content: string,
    baseIRI: string,
    prefixFound: PrefixCallback,
) {
    return new Parser({ baseIRI }).parse(content, undefined, prefixFound)
}

async function parseArchive(
    bytes: Uint8Array | ArrayBuffer,
    baseIRI: string,
    prefixFound: PrefixCallback,
) {
    const zip = await JSZip.loadAsync(bytes)
    const quads: Quad[] = []

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    for (const [_, file] of Object.entries(zip.files)) {
        if (file.dir) continue

        // Only parse files with allowed extensions
        if (!file.name.match(/\.(ttl|trig|jsonld)$/i)) continue

        try {
            logger.debug('Found config file in ' + file.name)
            const content = await file.async('string')
            const nq = await parseText(content, baseIRI, prefixFound)
            quads.push(...nq)
        } catch (err) {
            logger.debug(`Skipping ${file.name}: ${err}`)
        }
    }

    return quads
}

async function fetchResource(url: URL): Promise<Uint8Array | ArrayBuffer> {
    if (url.protocol === 'file:') {
        return readFile(url.pathname)
    } else if (url.protocol === 'http:' || url.protocol === 'https:') {
        const resp = await fetch(url)
        logger.debug(
            `${url.toString()} status ${resp.status} ${resp.statusText}`,
        )
        const blob = await resp.blob()
        return await blob.arrayBuffer()
    } else {
        throw new Error('No supported protocol ' + url.protocol)
    }
}

export async function readQuads(
    todo: string[],
    done = new Set<string>(),
    quads: Quad[] = [],
): Promise<Quad[]> {
    let current = todo.pop()

    if (quads.length === 0) {
        quads.push(...modelQuads)
    }

    while (current !== undefined) {
        if (!done.has(current)) {
            done.add(current)
            logger.debug('Expanding ' + current)

            const url = new URL(current)
            const newQuads: Quad[] = []

            const bytes = await fetchResource(url)

            if (
                url.pathname.endsWith('.jar') ||
                url.pathname.endsWith('.zip')
            ) {
                newQuads.push(
                    ...(await parseArchive(bytes, current, prefixFound)),
                )
            } else {
                const text = new TextDecoder('utf-8').decode(
                    bytes as ArrayBuffer,
                )
                newQuads.push(...(await parseText(text, current, prefixFound)))
            }

            todo.push(
                ...newQuads
                    .filter(
                        (q) =>
                            q.subject.equals(new NamedNode(current!)) &&
                            q.predicate.equals(OWL.imports),
                    )
                    .map((x) => x.object.value),
            )

            quads.push(...newQuads)
        }

        current = todo.pop()
    }

    return quads
}

export async function expandQuads(uri: string, quads: Quad[]): Promise<Quad[]> {
    quads.push(...modelQuads)
    const todo = quads
        .filter(
            (q) =>
                q.subject.equals(new NamedNode(uri)) &&
                q.predicate.equals(OWL.imports),
        )
        .map((x) => x.object.value)

    return await readQuads(todo, new Set(uri), quads)
}

export function createAsyncIterable<T>() {
    const queue: T[] = []
    let resolveNext: ((value: T) => void) | null = null

    return {
        push(item: T) {
            if (resolveNext) {
                resolveNext(item)
                resolveNext = null
            } else {
                queue.push(item)
            }
        },

        async *[Symbol.asyncIterator]() {
            while (true) {
                if (queue.length > 0) {
                    yield queue.shift()!
                } else {
                    yield await new Promise<T>(
                        (resolve) => (resolveNext = resolve),
                    )
                }
            }
        },
    }
}
