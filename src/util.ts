import { JsonLdParser } from 'jsonld-streaming-parser'
import { Context, Document } from '.'
import { Quad } from '@rdfjs/types'

export async function jsonld_to_quads(document: Document): Promise<Quad[]>
export async function jsonld_to_quads(
  context: Context,
  document: Document,
): Promise<Quad[]>
export async function jsonld_to_quads(
  context: Context | Document,
  document?: Document,
) {
  const quads: Quad[] = []
  const myParser = new JsonLdParser()
  const endPromise = new Promise((res, rej) => {
    myParser.on('end', res)
    myParser.on('error', rej)
  })

  myParser.on('data', (quad) => quads.push(quad))
  if (document) {
    const json = `{"@context": ${JSON.stringify(context)},${JSON.stringify(
      document,
    ).slice(1)}`
    myParser.write(json)
  } else {
    myParser.write(JSON.stringify(context))
  }

  myParser.end()
  await endPromise
  return quads
}
