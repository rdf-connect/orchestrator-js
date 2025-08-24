/**
 * @module orchestrator
 * @description Core orchestrator implementation that manages the execution of RDF processing pipelines.
 * Handles pipeline configuration, runner management, and message routing.
 */

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
import { pathToFileURL } from 'url'

/**
 * Defines the callback interface for handling messages and connection closures.
 * @interface Callbacks
 * @property {Function} msg - Callback for processing incoming messages
 * @property {Function} close - Callback for handling connection closures
 */
export type Callbacks = {
    /**
     * Handles incoming messages from runners.
     * @param {Message} msg - The received message
     * @returns {Promise<void>}
     */
    msg: (msg: Message) => Promise<void>

    /**
     * Handles connection closures.
     * @param {Close} close - Close event details
     * @returns {Promise<void>}
     */
    close: (close: Close) => Promise<void>
}

/**
 * Type guard to check if a pipeline is defined as a string.
 * @param {Pipeline | string} pipeline - The pipeline to check
 * @returns {boolean} True if the pipeline is a string
 */
function pipelineIsString(pipeline: Pipeline | string): pipeline is string {
    return typeof pipeline === 'string' || pipeline instanceof String
}

/**
 * Main orchestrator class that manages the execution of RDF processing pipelines.
 * Implements the Callbacks interface for handling messages and connection events.
 */
export class Orchestrator implements Callbacks {
    /** Logger instance for this orchestrator */
    protected readonly logger = getLoggerFor([this])

    /** The gRPC server instance */
    server: Server

    /** Current pipeline configuration */
    pipeline: Pipeline = emptyPipeline

    /** RDF quads representing the current pipeline */
    quads: Quad[] = []

    /** Processor definitions parsed from the pipeline */
    definitions: Definitions = {}

    /**
     * Creates a new Orchestrator instance.
     * @param {Server} server - The gRPC server instance to use
     */
    constructor(server: Server) {
        this.server = server
    }

    /**
     * Sets the pipeline configuration from a URI.
     * @param {Quad[]} quads - RDF quads representing the pipeline
     * @param {string} uri - URI of the pipeline configuration
     * @returns {void}
     */
    setPipeline(quads: Quad[], uri: string): void

    /**
     * Sets the pipeline configuration with provided pipeline and definitions.
     * @param {Quad[]} quads - RDF quads representing the pipeline
     * @param {Pipeline} pipeline - The pipeline configuration
     * @param {Definitions} definitions - Processor definitions
     * @returns {void}
     */
    setPipeline(
        quads: Quad[],
        pipeline: Pipeline,
        definitions: Definitions,
    ): void

    /**
     * Implementation of setPipeline that handles both overloads.
     * @private
     */
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
        this.logger.debug(
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

    /**
     * Handles connection closure for a specific channel.
     * Propagates the close event to all runners in the pipeline.
     *
     * @param {Close} close - Close event details including the channel identifier
     * @returns {Promise<void>}
     */
    async close(close: Close) {
        this.logger.debug('Got close message for channel ' + close.channel)
        await Promise.all(
            this.pipeline.parts.map((part) => part.instantiator.close(close)),
        )
    }

    /**
     * Processes an incoming message by forwarding it to all runners in the pipeline.
     *
     * @param {Message} msg - The message to process
     * @returns {Promise<void>}
     */
    async msg(msg: Message) {
        this.logger.debug('Got data message for channel ' + msg.channel)
        await Promise.all(
            this.pipeline.parts.map((part) => part.instantiator.msg(msg)),
        )
    }

    /**
     * Handles streaming messages by forwarding them to all runners in the pipeline.
     *
     * @param {StreamMessage} msg - The stream message to process
     * @returns {Promise<void>}
     */
    async streamMessage(msg: StreamMessage) {
        this.logger.debug('Got data stream message for channel ' + msg.channel)
        await Promise.all(
            this.pipeline.parts.map((part) =>
                part.instantiator.streamMessage(msg),
            ),
        )
    }

    /**
     * Initializes and starts all runners in the pipeline.
     *
     * @param {string} addr - The address to start the runners on
     * @param {string} pipeline - The pipeline configuration to send to runners
     * @returns {Promise<void>}
     *
     * Process Flow:
     * For each part in the pipeline:
     *    a. Registers the runner with the server
     *    b. Starts the runner with the provided address
     *    c. Sends the pipeline configuration to the runner
     */
    async startInstantiators(addr: string, pipeline: string) {
        this.logger.debug('Starting ' + this.pipeline.parts.length + ' runners')
        await Promise.all(
            Object.values(this.pipeline.parts).map(async (part) => {
                const r = part.instantiator
                const prom = this.server.expectRunner(r)
                await r.start(addr)
                await prom
                await r.sendPipeline(pipeline)
            }),
        )
    }

    /**
     * Waits for all runners in the pipeline to complete their execution.
     *
     * @returns {Promise<void>}
     * @throws {Error} If any runner ends with an error
     */
    async waitClose() {
        await Promise.all(
            this.pipeline.parts.map((x) => x.instantiator.endPromise),
        )
    }

    /**
     * Initializes and starts all processors in the pipeline.
     *
     * @returns {Promise<void>}
     * @throws {Array<Error>} If any processor fails to start
     *
     * Process Flow:
     * 1. For each part in the pipeline:
     *    a. For each processor in the part:
     *       i. Attempts to add the processor to the runner
     *       ii. Collects any errors that occur
     * 2. If any errors occurred:
     *    a. Logs each error
     *    b. Throws an array of all errors
     */
    async startProcessors() {
        this.logger.debug(
            'Starting ' +
                this.pipeline.parts.map((x) => x.processors.length) +
                ' processors',
        )
        const errors = []

        for (const part of this.pipeline.parts) {
            const runner = part.instantiator
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

        await Promise.all(
            this.pipeline.parts.map((x) => x.instantiator.startProcessors()),
        )
    }
}

/**
 * Initializes and starts the orchestrator with the specified pipeline configuration.
 * This is the main entry point for the orchestrator service.
 *
 * @param {string} location - Filesystem path to the pipeline configuration file
 * @returns {Promise<void>}
 *
 * @throws {LensError} If there's an error processing the pipeline configuration
 * @throws {Error} For other runtime errors during startup
 *
 * Process Flow:
 * 1. Initializes gRPC server and orchestrator instance
 * 2. Binds the gRPC server to the specified port (default: 50051)
 * 3. Loads and parses the pipeline configuration
 * 4. Sets up the pipeline with the loaded configuration
 * 5. Starts all runners and processors
 * 6. Waits for the pipeline to complete
 * 7. Handles graceful shutdown
 */
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
    const iri = pathToFileURL(location);
    setPipelineFile(iri)
    const quads = await readQuads([iri.toString()])

    reevaluteLevels()
    logger.debug('Setting pipeline')
    try {
        orchestrator.setPipeline(quads, iri.toString())
    } catch (ex: unknown) {
        if (ex instanceof LensError) {
            console.error(ex.message)
            for (const lin of ex.lineage) {
                console.error(lin.name, lin.opts)
            }

            process.exit(1)
        }
    }

    await orchestrator.startInstantiators(
        addr,
        new Writer().quadsToString(quads),
    )
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
    modelShapes.lenses['https://w3id.org/rdf-connect#Orchestrator'] =
        empty<Cont>().map(() => orchestrator)
}
