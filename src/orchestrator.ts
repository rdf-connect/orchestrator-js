import { readFile } from 'fs/promises'
import { NamedNode, Parser } from 'n3'
import { Pipeline, PipelineShape, Processor, Runner } from './model'
import { Context, Definitions, parse_processors } from '.'
import { jsonld_to_string } from './util'
import { Quad } from '@rdfjs/types'
import { Instance, startInstance } from './grpc'
import { Message } from './generated/service'

export class ProcessorInstance {
  proc: Processor
  runner: Runner
  arguments: string
  constructor(
    proc: Processor,
    pipeline: Pipeline,
    quads: Quad[],
    discoveredShapes: Definitions,
  ) {
    this.proc = proc
    console.log('--- proc', proc.id.value)

    const shape = discoveredShapes[proc.type.id.value]
    if (!shape) {
      console.error(
        `Failed to find a shape defintion for ${proc.id.value} (expects shape for ${proc.type.id.value})`,
      )
      throw 'No shape definition found'
    }

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

    this.arguments = jsonld_to_string(jsonld_document)
    this.runner = this.findRunner(proc, pipeline, quads, discoveredShapes)
  }

  private findRunner(
    proc: Processor,
    pipeline: Pipeline,
    quads: Quad[],
    discoveredShapes: Definitions,
  ): Runner {
    const runners = pipeline.runners.filter((x) =>
      x.handles.some((handle) => handle === proc.type.runner_type),
    )
    if (runners.length !== 1) {
      if (runners.length === 0) {
        console.error(
          `No viable runners found for processor ${proc.id.value} (expects runner for ${proc.type.runner_type})`,
        )
        throw 'No runner found'
      }
      console.error(
        `Too many viable runners found for processor ${proc.id.value} (expects runner for ${proc.type.runner_type}) (found ${runners.map(
          (x) => x.id.value,
        )})`,
      )
      throw 'Too many runners found'
    }

    const runner = runners[0]
    const processorShape = discoveredShapes[runner.processor_definition]
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
    return runner
  }
}

export async function start(location: string) {
  const iri = 'file://' + location
  console.log('Loading', location)

  const file = await readFile(location, { encoding: 'utf8' })
  const quads = new Parser({ baseIRI: iri }).parse(file)

  const discoveredShapes = parse_processors(quads)
  const pipeline = PipelineShape.execute({
    id: new NamedNode(iri),
    quads,
  })

  const errors = []
  const processors = []
  for (const proc of pipeline.processors) {
    try {
      processors.push(
        new ProcessorInstance(proc, pipeline, quads, discoveredShapes),
      )
    } catch (ex) {
      errors.push(ex)
    }
  }

  if (errors.length > 0) {
    throw errors
  }

  const runners: { [id: string]: Runner } = {}
  for (const proc of processors) {
    if (!runners[proc.runner.id.value]) {
      runners[proc.runner.id.value] = proc.runner
    }
  }

  const instances: { [id: string]: Instance } = {}
  const cb = async (msg: Message) => {
    console.log('Cb got message', msg)
    await Promise.all(Object.values(instances).map((inst) => inst.send(msg)))
  }
  await Promise.all(
    Object.values(runners).map(async (runner) => {
      instances[runner.id.value] = await startInstance(runner, cb)
    }),
  )

  console.log('All runners are instanciated')

  await Promise.all(
    processors.map((processor) =>
      instances[processor.runner.id.value].init(processor),
    ),
  )
  console.log('All processors are instanciated')

  //  We found the runners, lets start them

  await Promise.all(Object.values(instances).map((x) => x.start()))
  console.log('All processors are started')
}
