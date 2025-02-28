import * as grpc from '@grpc/grpc-js'
import { readFile } from 'fs/promises'
import { NamedNode, Parser } from 'n3'
import { Pipeline, PipelineShape, Processor, Runner } from './model'
import { Definitions, Document, parse_processors } from '.'
import { jsonld_to_string } from './util'
import { Quad } from '@rdfjs/types'
import {
  Close,
  Message,
  ProcessorInit,
  RunnerService,
} from './generated/service'
import { FromRunner, Server, stubToRunner, ToRunner } from './server'
import { CommandRunner, RunnerTrait } from './grpc'

export type Callbacks = {
  msg: (msg: Message) => Promise<void>
  close: (close: Close) => Promise<void>
}

export class RunnerInstance implements FromRunner {
  processorsPromises: { [id: string]: () => void } = {}
  toRunner: ToRunner = stubToRunner()

  runner: Runner
  cbs: Callbacks
  runnerCmd: RunnerTrait

  constructor(runner: Runner, cbs: Callbacks) {
    this.runner = runner
    this.cbs = cbs
    this.runnerCmd = new CommandRunner(runner.command)
  }

  async start(addr: string) {
    await this.runnerCmd.start(addr, this.runner.id.value)
  }
  async setWriter(orchestrator: ToRunner): Promise<void> {
    console.log('Writer set')
    this.toRunner = orchestrator
  }
  async init(msg: ProcessorInit): Promise<void> {
    this.processorsPromises[msg.uri]()
  }
  msg(msg: Message): Promise<void> {
    return this.cbs.msg(msg)
  }
  close(close: Close): Promise<void> {
    return this.cbs.close(close)
  }
  addProcessor(processor: ProcessorInstance): Promise<void> {
    this.toRunner.processor({
      uri: processor.proc.id.value,
      config: '{}',
      arguments: processor.arguments,
    })
    return new Promise((res) => {
      this.processorsPromises[processor.proc.id.value] = res
    })
  }
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
    const { runner, document } = this.findRunner(
      proc,
      pipeline,
      quads,
      discoveredShapes,
    )
    this.runner = runner
    this.document = document
  }

  private findRunner(
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

    const document = processorShape.addToDocument(
      proc.type.id,
      quads,
      discoveredShapes,
    )
    return { runner, document }
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

  const instances: { [id: string]: RunnerInstance } = {}
  const callbacks: Callbacks = {
    msg: async (msg: Message) => {
      console.log('Cb got message', msg)
      await Promise.all(
        Object.values(instances).map((inst) => inst.toRunner.message(msg)),
      )
    },
    close: async (close: Close) => {
      await Promise.all(
        Object.values(instances).map((inst) => inst.toRunner.close(close)),
      )
    },
  }

  await Promise.all(
    Object.values(runners).map(async (r) => {
      const runner = new RunnerInstance(r, callbacks)
      instances[r.id.value] = runner
      await runner.start(addr)
      console.log('Runner started', r.id.value)
      await orchestrator.addRunner(r.id.value, runner)
      console.log('Runner registered', r.id.value)
    }),
  )

  console.log('All runners are instanciated')

  await Promise.all(
    processors.map(async (processor) => {
      console.log('Starting proc', processor)
      await instances[processor.runner.id.value].addProcessor(processor)
    }),
  )

  console.log('All processors are instanciated')
  await Promise.all(Object.values(instances).map((x) => x.toRunner.start()))
}
