/**
 * @module orchestrator
 * @description Core orchestrator implementation that manages the execution of RDF processing pipelines.
 * Handles pipeline configuration, runner management, and message routing.
 */

import * as grpc from '@grpc/grpc-js'
import { NamedNode, Writer } from 'n3'
import {
    emptyPipeline,
    modelShapes,
    Pipeline,
    PipelineShape,
    SmallProc,
} from './model'
import {
    collapseLast,
    getLoggerFor,
    reevaluteLevels,
    setPipelineFile,
} from './logUtil'
import {
    Channels,
    Definitions,
    Instantiator,
    parse_processors,
    Sender,
    Server,
} from '.'
import { jsonld_to_string, RDFC, readQuads, walkJson } from './util'
import { Quad } from '@rdfjs/types'
import { Close, Message, RunnerService } from '@rdfc/proto'
import { Cont, empty, envReplace, LensError } from 'rdf-lens'
import { pathToFileURL } from 'url'
import {
    DataChunk,
    MessageProcessed,
    StreamChunk,
    StreamIdentify,
} from '@rdfc/proto/lib/generated/common'

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
     * @param {() => void} onEnd - Callback to be called when all receiving instantiator indicate the message has been handled
     * @returns {Promise<void>}
     */
    msg: (msg: Message, onEnd: () => void) => Promise<void>

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

    /** Current pipeline configuration */
    pipeline: Pipeline = emptyPipeline

    /** RDF quads representing the current pipeline */
    quads: Quad[] = []

    /** Processor definitions parsed from the pipeline */
    definitions: Definitions = {}

    /** Message count, used for translation between instantiator tick and orchestrator tick */
    protected messageCount = 0

    /**
     * Maps the messageId to resolving promise callback functions.
     * Invoking the callback indicates the message has been handled
     */
    protected runningMessages: { [id: number]: () => void } = {}

    /**
     * Maps the messageId to connecting streams promise callbacks.
     * Invoking the callback indicates the receiving stream handler is attached
     */
    protected connectingStreams: {
        [id: number]: { promise: () => void; write: Sender<DataChunk> }
    } = {}

    /** Maps runner URIs to their instantiator instances and promise resolution callbacks */
    readonly instantiators: {
        [uri: string]: { part: Instantiator; promise: () => void }
    } = {}

    /** Maps channel URIs to their target instantiator instances for message routing */
    readonly channelToInstantiator: {
        [uri: string]: Instantiator
    } = {}

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
            try {
                this.pipeline = PipelineShape.execute({
                    id: new NamedNode(pipeline),
                    quads,
                })
            } catch (ex: unknown) {
                if (ex instanceof LensError) {
                    const id = ex.lineage
                        .filter((x) => x.name === 'id' || x.name === 'pred')
                        .map((x) => <string>x.opts)
                        .join(' -> ')
                    const linReversed = ex.lineage.slice().reverse()
                    this.logger.error('Error happend when parsing at ' + id)
                    const lastPred = <string>(
                        linReversed.find((x) => x.name === 'pred')?.opts
                    )
                    const lastId = <string>(
                        linReversed.find((x) => x.name === 'id')?.opts
                    )
                    const foundSome = linReversed[0].name !== 'pred'
                    const isType =
                        lastPred ==
                        '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'

                    if (!foundSome) {
                        if (isType) {
                            this.logger.error(
                                'Cannot find a type for ' +
                                    collapseLast(lastId) +
                                    ', maybe it does not exist. Try importing the object or check for typos.',
                            )
                        } else {
                            this.logger.error(
                                'No matching triples found for predicate ' +
                                    collapseLast(lastPred) +
                                    ' on subject ' +
                                    collapseLast(lastId),
                            )
                            // this.logger.error(`Missing triple ${lastId} ${lastPred} ?? .`);
                        }
                    } else {
                        const expectedType = linReversed.find(
                            (x) => x.name === 'extracting class',
                        )
                        if (
                            expectedType &&
                            expectedType.opts ===
                                'https://w3id.org/rdf-lens/ontology#TypedExtract'
                        ) {
                            this.logger.error(
                                'Expected a type triple for ' +
                                    collapseLast(lastId) +
                                    ' but found none, maybe you refered to a not existing object. Try importing the object or check for typos.',
                            )
                        }
                    }
                }
                throw ex
            }
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
    async msg(msg: Message, onEnd: () => void): Promise<void> {
        this.logger.debug('Got data message for channel ' + msg.channel)

        const tick = this.messageCount++
        const translatedMessage: Message = {
            channel: msg.channel,
            data: msg.data,
            tick,
        }

        const targetInstnatiator =
            this.channelToInstantiator[translatedMessage.channel]
        if (targetInstnatiator) {
            this.runningMessages[tick] = onEnd
            targetInstnatiator.msg(translatedMessage)
        } else {
            this.logger.error(
                `Receiving msg for channel ${translatedMessage.channel} without a connected reader`,
            )
            onEnd()
        }
    }

    /**
     * Handles message processing completion notifications.
     * Called when a message has been processed by the target instantiators.
     *
     * @param {MessageProcessed} msg - The message processing notification
     * @returns {void}
     */
    processed(msg: MessageProcessed) {
        const cb = this.runningMessages[msg.tick]
        if (cb) {
            cb()
            this.logger.info(
                'Succesfully processed message with tick ' + msg.tick,
            )
            delete this.runningMessages[msg.tick]
        } else {
            this.logger.error(
                'Expected to find state for tick ' +
                    msg.tick +
                    ' has it already been handled?',
            )
        }
    }

    async startStreamMessage(streamIdentify: StreamIdentify): Promise<number> {
        const tick = this.messageCount++

        const sourceRunner = this.instantiators[streamIdentify.runner]
        if (!sourceRunner) {
            throw (
                'Failed to find correct source runner with uri ' +
                streamIdentify.runner
            )
        }

        const targetInstantiator =
            this.channelToInstantiator[streamIdentify.channel]
        if (targetInstantiator) {
            this.runningMessages[tick] = sourceRunner.part.onMessageProcessedCb(
                streamIdentify.tick,
                streamIdentify.channel,
            )
            const prom = new Promise(
                (res) =>
                    (this.connectingStreams[tick] = {
                        promise: () => res(null),
                        write: { write: async () => {}, close: async () => {} },
                    }),
            )

            targetInstantiator.streamMessage({
                channel: streamIdentify.channel,
                id: tick,
                tick: tick,
            })
            await prom
        } else {
            this.logger.error(
                `Receiving stream message for channel ${streamIdentify.channel} without a connected reader`,
            )
            const onEnd = sourceRunner.part.onMessageProcessedCb(
                streamIdentify.tick,
                streamIdentify.channel,
            )

            this.connectingStreams[tick] = {
                promise: () => {},
                write: {
                    write: async () => {},
                    close: async () => {
                        onEnd()
                    },
                },
            }
        }

        return tick
    }

    /**
     * Forwards streaming data chunks from source to target through the connecting stream.
     *
     * @param {number} tick - The message tick ID for the stream
     * @param {AsyncIterable<StreamChunk>} stream - The stream of data chunks to forward
     * @returns {Promise<void>}
     */
    async forwardStream(tick: number, stream: AsyncIterable<StreamChunk>) {
        const obj = this.connectingStreams[tick]

        for await (const chunk of stream) {
            await obj.write.write(chunk.data!)
        }

        obj.write.close()
    }

    /**
     * Establishes a connection for receiving streaming data.
     * Links the stream writer to the connecting stream identified by the message tick.
     *
     * @param {number} id - The message tick ID for the stream connection
     * @param {Sender<DataChunk>} write - The sender for writing data chunks to the stream
     * @returns {Promise<void>}
     */
    async connectingReceivingStream(id: number, write: Sender<DataChunk>) {
        this.logger.info('connecting for stream message ' + id)
        const obj = this.connectingStreams[id]
        if (obj === undefined) {
            this.logger.error('Expected a set up stream message with id ' + id)
            return
        }

        obj.write = write
        obj.promise()
    }

    /**
     * Establishes communication channels for a connected runner.
     * Sets up the runner's channel configuration and completes the connection promise.
     *
     * @param {string} uri - The URI of the runner to connect
     * @param {Channels} channels - The communication channels for the runner
     * @returns {Promise<void>}
     */
    async connectingRunner(uri: string, channels: Channels) {
        const item = this.instantiators[uri]
        if (item === undefined) {
            this.logger.error('Unexpected runner with id ' + uri)
            return
        }

        item.part.setChannel(channels)
        item.promise()
    }

    /**
     * Creates a promise that resolves when the specified instantiator connects.
     * Used to wait for runner initialization before proceeding with pipeline setup.
     *
     * @param {Instantiator} instantiator - The instantiator instance to wait for
     * @returns {Promise<null>} A promise that resolves when the instantiator connects
     */
    expectRunner(instantiator: Instantiator): Promise<null> {
        return new Promise((res) => {
            this.instantiators[instantiator.id.value] = {
                part: instantiator,
                promise: () => res(null),
            }
        })
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
        const resolved = await Promise.allSettled(
            Object.values(this.pipeline.parts).map(async (part) => {
                const r = part.instantiator
                const prom = this.expectRunner(r)
                await r.start(addr)
                await prom
                await r.sendPipeline(pipeline)
            }),
        )

        const errors = resolved
            .filter((x) => x.status == 'rejected')
            .map((x) => x.reason)
        if (errors.length > 0) {
            for (const e of errors) {
                this.logger.error(e)
            }
            process.exit(1)
        }
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
     * Generates configuration arguments for a processor based on its RDF definition.
     * Creates a JSON-LD document with the processor configuration and tracks channel mappings.
     *
     * @param {SmallProc} proc - The processor configuration to generate arguments for
     * @param {Instantiator} instantiator - The instantiator that will run the processor
     * @param {Object} state - Object tracking reader/writer channel mappings
     * @param {Set<string>} state.readers - Set of reader channel URIs
     * @param {Set<string>} state.writers - Set of writer channel URIs
     * @returns {string} JSON-LD string containing the processor configuration
     * @throws {Error} If the processor shape definition is not found
     * @throws {Error} If multiple readers or writers are assigned to the same channel
     */
    getArguments(
        proc: SmallProc,
        instantiator: Instantiator,
        state: {
            readers: Set<string>
            writers: Set<string>
        },
    ): string {
        const shape = this.definitions[proc.type.value]
        if (!shape) {
            throw `Failed to find a shape definition for ${collapseLast(proc.id.value)} (expects shape for ${collapseLast(proc.type.value)}). Try importing the processor or check for typos.`
        }

        const jsonld_document = shape.addToDocument(
            proc.id,
            this.quads,
            this.definitions,
        )

        walkJson(jsonld_document, (obj) => {
            if (obj['@type'] && obj['@id'] && obj['@type'] === RDFC.Reader) {
                const ids = Array.isArray(obj['@id'])
                    ? obj['@id']
                    : [obj['@id']]
                for (const id of ids) {
                    if (typeof id === 'string') {
                        this.channelToInstantiator[id] = instantiator
                        // count it
                        if (state.readers.has(id)) {
                            throw new Error(
                                `Only expected a single reader for channel ${collapseLast(id)}, but found multiple`,
                            )
                        } else {
                            state.readers.add(id)
                        }
                    }
                }
            }

            if (obj['@type'] && obj['@id'] && obj['@type'] === RDFC.Writer) {
                const ids = Array.isArray(obj['@id'])
                    ? obj['@id']
                    : [obj['@id']]
                for (const id of ids) {
                    if (typeof id === 'string') {
                        // count it
                        if (state.writers.has(id)) {
                            throw new Error(
                                `Only expected a single writer for channel ${collapseLast(id)}, but found multiple`,
                            )
                        } else {
                            state.writers.add(id)
                        }
                    }
                }
            }
        })

        const args = jsonld_to_string(jsonld_document)
        return args
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

        const startPromises = []

        const state = {
            readers: new Set<string>(),
            writers: new Set<string>(),
        }

        for (const part of this.pipeline.parts) {
            const runner = part.instantiator
            for (const procId of part.processors) {
                this.logger.debug(
                    `Adding processor ${procId.id.value} (${procId.type.value}) to runner ${runner.id.value}`,
                )

                const args = this.getArguments(procId, runner, state)
                startPromises.push(
                    runner.addProcessor(
                        procId,
                        this.quads,
                        this.definitions,
                        args,
                    ),
                )
            }
        }

        // See if all channels are connected
        if (state.readers != state.writers) {
            for (const r of state.writers) {
                if (!state.readers.delete(r)) {
                    this.logger.error(
                        `Writer ${collapseLast(r)} has no linked Reader.`,
                    )
                }
            }

            for (const r of state.readers) {
                this.logger.error(
                    `Reader ${collapseLast(r)} has no linked Writer.`,
                )
            }
        }

        const errors = (await Promise.allSettled(startPromises))
            .filter((x) => x.status === 'rejected')
            .map((x) => x.reason)

        if (errors.length > 0) {
            for (const e of errors) {
                this.logger.error(e)
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
    const orchestrator = new Orchestrator()
    const server = new Server(orchestrator)
    setupOrchestratorLens(orchestrator)

    grpcServer.addService(RunnerService, server.server)
    await new Promise((res) =>
        grpcServer.bindAsync(
            '0.0.0.0:' + port,
            grpc.ServerCredentials.createInsecure(),
            res,
        ),
    )

    const addr = 'localhost:' + port
    logger.info('Grpc server is bound! ' + addr)
    const iri = pathToFileURL(location)
    setPipelineFile(iri)
    let quads = await readQuads([iri.toString()])
    quads = envReplace().execute(quads)

    reevaluteLevels()
    logger.debug('Setting pipeline')
    orchestrator.setPipeline(quads, iri.toString())

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

/**
 * Sets up the RDF lens mapping for the Orchestrator class.
 * Maps the rdfc:Orchestrator RDF type to this orchestrator instance for RDF processing.
 *
 * @param {Orchestrator} orchestrator - The orchestrator instance to map
 * @returns {void}
 */
function setupOrchestratorLens(orchestrator: Orchestrator) {
    modelShapes.lenses['https://w3id.org/rdf-connect#Orchestrator'] =
        empty<Cont>().map(() => orchestrator)
}
