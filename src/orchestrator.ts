import * as grpc from '@grpc/grpc-js'
import { readFile } from 'fs/promises'
import { NamedNode, Parser } from 'n3'
import { cbs, Pipeline, PipelineShape, Processor } from './model'
import { Definitions, Document, parse_processors } from '.'
import { jsonld_to_string } from './util'
import { Quad } from '@rdfjs/types'
import { Close, Message, RunnerService } from './generated/service'
import { Server } from './server'
import { Runner } from './runner'

export type Callbacks = {
  msg: (msg: Message) => Promise<void>
  close: (close: Close) => Promise<void>
}

function findRunner(
  proc: Processor,
  pipeline: Pipeline,
  quads: Quad[],
  discoveredShapes: Definitions,
): { runner: Runner; document: Document } {
  const runners = pipeline.runners.filter((x) =>
    x.handles.some((handle) => handle === proc.type.runner_type),
  )
  if (runners.length !== 1) {
    if (runners.length === 0) {
      throw `No viable runners found for processor ${proc.id.value} (expects runner for ${proc.type.runner_type})`
    }
    throw `Too many viable runners found for processor ${proc.id.value} (expects runner for ${proc.type.runner_type}) (found ${runners.map(
      (x) => x.id.value,
    )})`
  }

  const runner = runners[0]
  const processorShape = discoveredShapes[runner.processor_definition]

  const document = processorShape.addToDocument(
    proc.type.id,
    quads,
    discoveredShapes,
  )
  return { runner, document }
}

export class ProcessorInstance {
  proc: Processor
  runner: Runner
  document: Document
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

    const jsonld_document = shape.addToDocument(
      proc.id,
      quads,
      discoveredShapes,
    )

    this.arguments = jsonld_to_string(jsonld_document)
    const { runner, document } = findRunner(
      proc,
      pipeline,
      quads,
      discoveredShapes,
    )
    this.runner = runner
    this.document = document
  }
}

export async function start(location: string) {
  const orchestrator = new Server()

  const port = 50051
  const grpcServer = new grpc.Server()
  grpcServer.addService(RunnerService, orchestrator.server)
  await new Promise((res) =>
    grpcServer.bindAsync(
      '0.0.0.0:' + port,
      grpc.ServerCredentials.createInsecure(),
      res,
    ),
  )
  const addr = 'localhost:' + port
  console.log('Grpc server is bound!', addr)

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
  const runners: { [id: string]: Runner } = {}

  cbs.close = async (close: Close) => {
    await Promise.all(Object.values(runners).map((inst) => inst.close(close)))
  }
  cbs.msg = async (msg: Message) => {
    await Promise.all(Object.values(runners).map((inst) => inst.msg(msg)))
  }

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

  for (const proc of processors) {
    if (!runners[proc.runner.id.value]) {
      runners[proc.runner.id.value] = proc.runner
    }
  }

  await Promise.all(
    Object.values(runners).map(async (r) => {
      const prom = orchestrator.expectRunner(r)
      await r.start(addr)
      await prom
    }),
  )

  console.log('All runners are instanciated')

  await Promise.all(
    processors.map(async (processor) => {
      processor.runner.addProcessor(processor)
    }),
  )

  console.log('All processors are instanciated')
  await Promise.all(Object.values(runners).map((x) => x.startProcessors()))
}
