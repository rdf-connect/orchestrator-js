import * as grpc from '@grpc/grpc-js'
import { NamedNode, Writer } from 'n3'
import { emptyPipeline, modelShapes, Pipeline, PipelineShape } from './model'
import { getLoggerFor, reevaluteLevels, setPipelineFile } from './logUtil'
import { Definitions, parse_processors } from '.'
import { readQuads } from './util'
import { Quad } from '@rdfjs/types'
import { Close, Message, RunnerService, StreamMessage } from '@rdfc/proto'
import { Server } from './server'
import { Cont, empty, LensError } from 'rdf-lens'

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
    setPipeline(
        quads: Quad[],
        pipeline: Pipeline,
        definitions: Definitions,
    ): void
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
            'Found definitions ' +
                JSON.stringify(Object.keys(this.definitions)),
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

    async close(close: Close) {
        this.logger.debug('Got close message for channel ' + close.channel)
        await Promise.all(
            this.pipeline.parts.map((part) => part.runner.close(close)),
        )
    }

    async msg(msg: Message) {
        this.logger.debug('Got data message for channel ' + msg.channel)
        await Promise.all(
            this.pipeline.parts.map((part) => part.runner.msg(msg)),
        )
    }

    async streamMessage(msg: StreamMessage) {
        this.logger.debug('Got data stream message for channel ' + msg.channel)
        await Promise.all(
            this.pipeline.parts.map((part) => part.runner.streamMessage(msg)),
        )
    }

    async startRunners(addr: string, pipeline: string) {
        this.logger.debug('Starting ' + this.pipeline.parts.length + ' runners')
        await Promise.all(
            Object.values(this.pipeline.parts).map(async (part) => {
                const r = part.runner
                const prom = this.server.expectRunner(r)
                await r.start(addr)
                await prom
                await r.sendPipeline(pipeline)
            }),
        )
    }

    async waitClose() {
        await Promise.all(this.pipeline.parts.map((x) => x.runner.endPromise))
    }

    async startProcessors() {
        this.logger.debug(
            'Starting ' +
                this.pipeline.parts.map((x) => x.processors.length) +
                ' processors',
        )
        const errors = []

        for (const part of this.pipeline.parts) {
            const runner = part.runner
            for (const procId of part.processors) {
                try {
                    this.logger.debug(
                        `Adding processor ${procId.id.value} (${procId.type.value}) to runner ${runner.id.value}`,
                    )
                    runner.addProcessor(procId, this.quads, this.definitions)
                } catch (ex) {
                    errors.push(ex)
                }
            }
        }

        if (errors.length > 0) {
            for (const e of errors) {
                this.logger.error(e)
                console.error(e)
            }

            throw errors
        }

        // const results = await Promise.allSettled(promises)
        // const promiseErrors = results.filter((x) => x.status === 'rejected')
        // if (promiseErrors.length > 0) {
        //     for (const e of promiseErrors) {
        //         console.log(e);
        //         this.logger.error(e.reason)
        //     }
        //     throw promiseErrors
        // }

        await Promise.all(
            this.pipeline.parts.map((x) => x.runner.startProcessors()),
        )
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
    logger.debug('Setting pipeline')
    try {
        orchestrator.setPipeline(quads, iri)
    } catch (ex: unknown) {
        if (ex instanceof LensError) {
            console.error(ex.message)
            for (const lin of ex.lineage) {
                console.error(lin.name, lin.opts)
            }

            process.exit(1)
        }
    }

    for (const p of orchestrator.pipeline.parts) {
        console.log(p.runner.id.value)
        for (const pr of p.processors) {
            console.log(pr)
        }
    }

    await orchestrator.startRunners(addr, new Writer().quadsToString(quads))
    await orchestrator.startProcessors()

    await orchestrator.waitClose()
    grpcServer.tryShutdown((e) => {
        if (e !== undefined) {
            logger.error(e)
            process.exit(1)
        } else {
            process.exit(0)
        }
    })
}

// Maps rdfc:Orchestrator to this orchestrator
function setupOrchestratorLens(orchestrator: Orchestrator) {
    modelShapes.lenses['https://w3id.org/rdf-connect/ontology#Orchestrator'] =
        empty<Cont>().map(() => orchestrator)
}
