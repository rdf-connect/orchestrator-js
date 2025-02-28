import { JsonLdParser } from 'jsonld-streaming-parser'
import { Context, Document } from '.'
import { Quad } from '@rdfjs/types'

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
