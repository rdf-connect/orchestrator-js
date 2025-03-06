import * as grpc from '@grpc/grpc-js'
import { NamedNode } from 'n3'
import {
  emptyPipeline,
  modelShapes,
  Pipeline,
  PipelineShape,
  Processor,
} from './model'
import { getLoggerFor, reevaluteLevels, setPipelineFile } from './logUtil'
import { Definitions, parse_processors } from '.'
import { readQuads } from './util'
import { Quad } from '@rdfjs/types'
import { Close, Message, RunnerService, StreamMessage } from '@rdfc/proto'
import { Server } from './server'
import { empty } from 'rdf-lens'

export type Callbacks = {
  msg: (msg: Message) => Promise<void>
  close: (close: Close) => Promise<void>
}

function pipelineIsString(pipeline: Pipeline | string): pipeline is string {
  return typeof pipeline === 'string' || pipeline instanceof String
}

export class Orchestrator implements Callbacks {
  protected readonly logger = getLoggerFor([this])

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
    this.logger.info(
      'Found definitions ' + JSON.stringify(Object.keys(this.definitions)),
    )
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
      const error =
        runners.length === 0
          ? `No viable runners found for processor ${proc.id.value} (expects runner for ${proc.type.runner_type})`
          : `Too many viable runners found for processor ${proc.id.value} (expects runner for ${proc.type.runner_type}) (found ${runners.map(
              (x) => x.id.value,
            )})`

      this.logger.error(error)
      throw error
    }

    return runners[0]
  }

  async close(close: Close) {
    this.logger.debug('Got close message for channel ' + close.channel)
    await Promise.all(this.pipeline.runners.map((inst) => inst.close(close)))
  }

  async msg(msg: Message) {
    this.logger.debug('Got data message for channel ' + msg.channel)
    await Promise.all(this.pipeline.runners.map((runner) => runner.msg(msg)))
  }

  async streamMessage(msg: StreamMessage) {
    this.logger.debug('Got data stream message for channel ' + msg.channel)
    await Promise.all(
      this.pipeline.runners.map((runner) => runner.streamMessage(msg)),
    )
  }

  async startRunners(addr: string) {
    this.logger.debug('Starting ' + this.pipeline.runners.length + ' runners')
    await Promise.all(
      Object.values(this.pipeline.runners).map(async (r) => {
        const prom = this.server.expectRunner(r)
        await r.start(addr)
        await prom
      }),
    )
  }

  async startProcessors() {
    this.logger.debug(
      'Starting ' + this.pipeline.processors.length + ' processors',
    )
    const errors = []
    const promises = []

    for (const proc of this.pipeline.processors) {
      try {
        const runner = this.findRunner(proc)
        promises.push(runner.addProcessor(proc, this.quads, this.definitions))
      } catch (ex) {
        errors.push(ex)
      }
    }

    if (errors.length > 0) {
      for (const e of errors) {
        this.logger.error(e)
      }
      throw errors
    }

    const results = await Promise.allSettled(promises)
    const promiseErrors = results.filter((x) => x.status === 'rejected')
    if (promiseErrors.length > 0) {
      for (const e of promiseErrors) {
        this.logger.error(e.reason)
      }
      throw promiseErrors
    }

    await Promise.all(this.pipeline.runners.map((x) => x.startProcessors()))
  }
}

export async function start(location: string) {
  const logger = getLoggerFor(['start'])
  const port = 50051
  const grpcServer = new grpc.Server()
  const orchestrator = new Orchestrator(new Server())
  setupOrchestratorLens(orchestrator)

  grpcServer.addService(RunnerService, orchestrator.server.server)
  await new Promise((res) =>
    grpcServer.bindAsync(
      '0.0.0.0:' + port,
      grpc.ServerCredentials.createInsecure(),
      res,
    ),
  )

  const addr = 'localhost:' + port
  logger.info('Grpc server is bound! ' + addr)
  const iri = 'file://' + location
  setPipelineFile(iri)
  const quads = await readQuads([iri])
  reevaluteLevels()
  orchestrator.setPipeline(quads, iri)
  await orchestrator.startRunners(addr)
  await orchestrator.startProcessors()
}

// Maps rdfc:Orchestrator to this orchestrator
function setupOrchestratorLens(orchestrator: Orchestrator) {
  modelShapes.lenses['https://w3id.org/rdf-connect/ontology#Orchestrator'] =
    empty().map(() => orchestrator)
}
