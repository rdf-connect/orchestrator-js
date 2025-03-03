import * as grpc from '@grpc/grpc-js'
import { readFile } from 'fs/promises'
import { NamedNode, Parser } from 'n3'
import { emptyPipeline, Pipeline, PipelineShape, Processor } from './model'
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

export class ProcessorInstance {
  proc: Processor
  runner: Runner
  document: Document
  arguments: string
  constructor(
    proc: Processor,
    quads: Quad[],
    discoveredShapes: Definitions,
    runner: Runner,
    document: Document,
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
    this.runner = runner
    this.document = document
  }
}

function pipelineIsString(pipeline: Pipeline | string): pipeline is string {
  return typeof pipeline === 'string' || pipeline instanceof String
}

export class Orchestrator implements Callbacks {
  server: Server

  pipeline: Pipeline = emptyPipeline
  quads: Quad[] = []
  definitions: Definitions = {}

  constructor(server: Server) {
    this.server = server
  }

  setPipeline(quads: Quad[], uri: string): void
  setPipeline(quads: Quad[], pipeline: Pipeline, definitions: Definitions): void
  setPipeline(
    quads: Quad[],
    pipeline: Pipeline | string,
    definitions?: Definitions,
  ) {
    this.quads = quads
    if (definitions === undefined) {
      this.definitions = parse_processors(quads)
    } else {
      this.definitions = definitions
    }
    if (pipelineIsString(pipeline)) {
      this.pipeline = PipelineShape.execute({
        id: new NamedNode(pipeline),
        quads,
      })
    } else {
      this.pipeline = pipeline
    }
  }

  findRunner(proc: Processor) {
    const runners = this.pipeline.runners.filter((x) =>
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
    const processorShape = this.definitions[runner.processor_definition]
    console.log(
      'Found processor shape',
      runner.processor_definition,
      !!processorShape,
    )

    const document = processorShape.addToDocument(
      proc.type.id,
      this.quads,
      this.definitions,
    )
    return { runner, document }
  }

  async close(close: Close) {
    await Promise.all(this.pipeline.runners.map((inst) => inst.close(close)))
  }

  async msg(msg: Message) {
    await Promise.all(this.pipeline.runners.map((inst) => inst.msg(msg)))
  }

  async startRunners(addr: string) {
    await Promise.all(
      Object.values(this.pipeline.runners).map(async (r) => {
        const prom = this.server.expectRunner(r)
        await r.start(addr)
        await prom
      }),
    )
  }

  async startProcessors() {
    const errors = []
    const instances = []
    for (const proc of this.pipeline.processors) {
      try {
        const { runner, document } = this.findRunner(proc)
        instances.push(
          new ProcessorInstance(
            proc,
            this.quads,
            this.definitions,
            runner,
            document,
          ),
        )
      } catch (ex) {
        errors.push(ex)
      }
    }

    if (errors.length > 0) {
      throw errors
    }

    instances.forEach(async (processor) => {
      processor.runner.addProcessor(processor)
    })

    await Promise.all(
      Object.values(this.pipeline.runners).map((x) => x.startProcessors()),
    )
  }
}

export async function start(location: string) {
  const port = 50051
  const grpcServer = new grpc.Server()
  grpcServer.addService(RunnerService, orchestrator.server.server)
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

  orchestrator.setPipeline(quads, iri)
  await orchestrator.startRunners(addr)
  await orchestrator.startProcessors()
}

export const orchestrator = new Orchestrator(new Server())
