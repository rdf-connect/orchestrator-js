import { JsonLdParser } from 'jsonld-streaming-parser'
import { Context, Document, modelQuads } from '.'
import { Quad } from '@rdfjs/types'
import { NamedNode, Parser } from 'n3'
import { createNamespace, createTermNamespace } from '@treecg/types'
import { readFile } from 'fs/promises'
import { getLoggerFor, prefixFound } from './logUtil'

const OWL = createTermNamespace('http://www.w3.org/2002/07/owl#', 'imports')
const logger = getLoggerFor(["util.ts"])

export const RDFC = createNamespace(
    'https://w3id.org/rdf-connect/ontology#',
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
            logger.debug("Expanding " + current);

            const url = new URL(current)
            if (url.protocol !== 'file:') {
                throw 'No supported protocol ' + url.protocol
            }

            // Read the quads
            const txt = await readFile(url.pathname, { encoding: 'utf8' })
            const newQuads = new Parser({ baseIRI: current }).parse(
                txt,
                undefined,
                prefixFound,
            )

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
