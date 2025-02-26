import { readFile } from 'fs/promises'
import { NamedNode, Parser, Writer } from 'n3'
import { modelShapes, PipelineShape } from './model'
import path from 'path'
import { Context, parse_processors } from '.'
import { jsonld_to_quads } from './util'

export async function main() {
  const location = path.resolve(process.argv[2])
  console.log('Loading', location)

  const file = await readFile(location, { encoding: 'utf8' })
  const quads = new Parser({ baseIRI: location }).parse(file)

  // const discoveredShapes = extractShapes(quads)
  const discoveredShapes = parse_processors(quads)
  const pipeline = PipelineShape.execute({
    id: new NamedNode(location),
    quads,
  })
  console.log('Got pipeline')
  console.log(JSON.stringify(pipeline, undefined, 2))
  console.log(Object.keys(modelShapes.lenses))
  console.log(Object.keys(discoveredShapes))

  for (const proc of pipeline.processors) {
    console.log('--- proc', proc.id.value)

    const shape = discoveredShapes[proc.type.id.value]
    console.log('Found shape', !!shape)

    const context: Context = {
      '@version': 1.1,
    }
    shape.addToContext(context)
    const jsonld_document = shape.addToDocument(
      proc.id,
      quads,
      discoveredShapes,
      {},
    )
    jsonld_document['@context'] = context

    console.log('Arguments\n', JSON.stringify(jsonld_document, undefined, 2))
    console.log(
      'Quads -----\n',
      new Writer().quadsToString(await jsonld_to_quads(jsonld_document)),
      '------',
    )

    const runner = pipeline.runners.find((x) =>
      x.handles.some((handle) => handle === proc.type.runner_type),
    )
    console.log('Found runner', runner)
    if (runner) {
      const processorShape = discoveredShapes[runner.processor_definition]
      console.log('Found shape', !!processorShape)

      const context: Context = {
        '@version': 1.1,
      }
      processorShape.addToContext(context)

      const jsonld_document = processorShape.addToDocument(
        proc.type.id,
        quads,
        discoveredShapes,
        {},
      )
      jsonld_document['@context'] = context
      console.log(
        'Runner arguments\n',
        JSON.stringify(jsonld_document, undefined, 2),
      )
      console.log(
        'Quads -----\n',
        new Writer().quadsToString(await jsonld_to_quads(jsonld_document)),
        '------',
      )
      // const processorConfig =
    }
  }
}

