/**
 * @module orchestrator
 * @description Core orchestrator implementation that manages the execution of RDF processing pipelines.
 * Handles pipeline configuration, runner management, and message routing.
 */

import * as grpc from '@grpc/grpc-js'
import { NamedNode } from 'n3'
import { emptyPipeline, Pipeline, PipelineShape, SmallProc } from './model'
import { collapseLast, getLoggerFor } from './logUtil'
import { Channels, Definitions, Instantiator, parse_processors } from '.'
import { jsonld_to_string, RDFC, walkJson } from './util'
import { Quad } from '@rdfjs/types'

import {
    Close,
    DataChunk,
    GlobalAck,
    ReceivingMessage,
    ReceivingStreamControl,
    SendingMessage,
    StreamChunk,
    StreamIdentify,
} from '@rdfc/proto'
import { envReplace, LensError } from 'rdf-lens'
import { Logger } from 'winston'
import { promisify } from 'util'

const decoder = new TextDecoder()
/**
 * Defines the callback interface for handling messages and connection closures.
 * @interface Callbacks
 * @property {Function} msg - Callback for processing incoming messages
 * @property {Function} close - Callback for handling connection closures
 */
export type Callbacks = {
    /**
     * Handles incoming messages from runners.
     * @param {SendingMessage} msg - The received message
     * @param {() => void} onEnd - Callback to be called when the receiving runner indicates that the message has been handled
     * @returns {Promise<void>}
     */
    msg: (msg: SendingMessage, onEnd: () => Promise<void>) => Promise<void>

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

type ReceivingStream = grpc.ServerDuplexStream<
    ReceivingStreamControl,
    DataChunk
>
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

    /** Global message count, runners send message with their localSequenceNumber, which is translated to this globalSequenceNumber */
    protected globalSequenceNumber = 0

    /**
     * Maps the messageId to resolving promise callback functions.
     * Invoking the callback indicates the message has been handled
     */
    protected runningMessages: { [id: number]: () => Promise<void> } = {}

    /**
     * Maps the messageId to connecting streams promise callbacks.
     * Invoking the callback indicates the receiving stream handler is attached
     */
    protected waitForConnectingReceivingStream: {
        [id: number]: (obj: ReceivingStream) => void
    } = {}

    /**
     * Maps the runner id to promise callbacks.
     * Invoking the callback indicates the runner is attached
     */
    onConnectingRunners: {
        [uri: string]: (obj: Channels) => void
    } = {}

    /** Maps runner URIs to their instantiator instances and promise resolution callbacks */
    readonly instantiators: {
        [uri: string]: Instantiator
    } = {}

    /** Maps channel URIs to their target instantiator instances for message routing */
    readonly channelToInstantiator: {
        [channel: string]: Instantiator
    } = {}

    /** Collection of all open channel connections from runners */
    readonly openChannels: Promise<unknown>[] = []

    protected readonly logLevels: { [uri: string]: (msg: string) => void } = {}

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
     */
    setPipeline(
        quads: Quad[],
        pipeline: Pipeline | string,
        definitions?: Definitions,
    ) {
        this.quads = envReplace().execute(quads)
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
                    this.logger.error('Error happened when parsing at ' + id)
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
                                ' but found none, maybe you referred to a not existing object. Try importing the object or check for typos.',
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
     * Processes an incoming message by forwarding it to the receiving runner.
     *
     * @param {SendingMessage} msg - The message to process
     * @param {() => Promise<void>} onEnd - callback called when the message has been processed by the runner
     * @returns {Promise<void>}
     */
    async msg(msg: SendingMessage, onEnd: () => Promise<void>): Promise<void> {
        this.logger.debug('Got data message for channel ' + msg.channel)

        const globalSequenceNumber = this.globalSequenceNumber++
        const translatedMessage: ReceivingMessage = {
            globalSequenceNumber,
            channel: msg.channel,
            data: msg.data,
        }

        const logFn = this.logLevels[msg.channel]
        if (logFn !== undefined) {
            logFn(decoder.decode(msg.data))
        }

        const targetInstantiator =
            this.channelToInstantiator[translatedMessage.channel]
        if (targetInstantiator) {
            this.runningMessages[globalSequenceNumber] = onEnd
            await targetInstantiator.msg(translatedMessage)
        } else {
            this.logger.error(
                `Receiving msg for channel ${translatedMessage.channel} without a connected reader`,
            )
            await onEnd()
        }
    }

    /**
     * Handles message processing completion notifications.
     * Called when a message has been processed by the target instantiators.
     *
     * @param {GlobalAck} msg - The message processing notification
     * @returns {void}
     */
    async processed(msg: GlobalAck) {
        const cb = this.runningMessages[msg.globalSequenceNumber]
        if (cb) {
            delete this.runningMessages[msg.globalSequenceNumber]
            this.logger.debug(
                `Successfully processed message with sequence number ${msg.globalSequenceNumber}`,
            )
            await cb()
        } else {
            this.logger.error(
                `Expected to find state with sequence number '${msg.globalSequenceNumber}', but did not. Has it already been handled?`,
            )
        }
    }

    async startStreamMessage(
        streamIdentify: StreamIdentify,
        sendingStream: AsyncIterable<StreamChunk> &
            grpc.ServerDuplexStream<StreamChunk, ReceivingStreamControl>,
    ): Promise<number> {
        const globalSequenceNumber = this.globalSequenceNumber++
        const writeToSender = promisify(sendingStream.write.bind(sendingStream))

        const sourceRunner = this.instantiators[streamIdentify.runner]
        if (!sourceRunner) {
            throw (
                'Failed to find correct source runner with uri ' +
                streamIdentify.runner
            )
        }

        const targetInstantiator =
            this.channelToInstantiator[streamIdentify.channel]

        // If the instantiator exists, it will send a processed message
        // When this happens, the onMessageProcessedCb will be called
        // If the instantiator does not exist, it cannot send a processed message
        // So the onMessageProcessedCb should be called when the stream is closed on the writer's side
        if (targetInstantiator) {
            this.runningMessages[globalSequenceNumber] =
                sourceRunner.onMessageProcessedCb(
                    streamIdentify.localSequenceNumber,
                    streamIdentify.channel,
                )

            const readerConnected = new Promise<ReceivingStream>((res) => {
                this.waitForConnectingReceivingStream[globalSequenceNumber] =
                    res
            })

            await targetInstantiator.streamMessage({
                channel: streamIdentify.channel,
                globalSequenceNumber,
            })

            const receivingStream = await readerConnected
            const writeToReceiver = promisify(
                receivingStream.write.bind(receivingStream),
            )
            const log = this.logLevels[streamIdentify.channel]

            sendingStream.on('data', async (chunk: StreamChunk) => {
                if (chunk.data !== undefined) {
                    if (log) {
                        log(decoder.decode(chunk.data!.data))
                    }
                    await writeToReceiver(chunk.data)
                }
            })

            sendingStream.on('end', () => {
                try {
                    receivingStream.end()
                } catch (ex) {
                    if (ex instanceof Error) {
                        this.logger.error(
                            'Error happened: ' + ex.name + ' ' + ex.message,
                        )
                        this.logger.error(ex.stack)
                    } else {
                        this.logger.error('Error happened: ' + ex)
                    }
                }
            })

            receivingStream.on('data', async (d: ReceivingStreamControl) => {
                try {
                    await writeToSender(d)
                } catch (ex) {
                    if (ex instanceof Error) {
                        this.logger.error(
                            'Error happened: ' + ex.name + ' ' + ex.message,
                        )
                        this.logger.error(ex.stack)
                    } else {
                        this.logger.error('Error happened: ' + ex)
                    }
                }
            })

            await writeToSender({ streamSequenceNumber: globalSequenceNumber })
        } else {
            this.logger.error(
                `Receiving stream message for channel ${streamIdentify.channel} without a connected reader`,
            )
            const onEnd = sourceRunner.onMessageProcessedCb(
                streamIdentify.localSequenceNumber,
                streamIdentify.channel,
            )

            let chunkCount = 0
            await writeToSender({ streamSequenceNumber: chunkCount })
            sendingStream.on('data', () => {
                return writeToSender({ streamSequenceNumber: ++chunkCount })
            })
            sendingStream.on('end', onEnd)
        }

        return globalSequenceNumber
    }

    /**
     * Establishes a connection for receiving streaming data.
     * Links the stream writer to the connecting stream identified by the message globalSequenceNumber.
     */
    onReceivingStreamConnected(
        globalSequenceNumber: number,
        stream: ReceivingStream,
    ) {
        this.logger.info(
            'connecting for stream message ' + globalSequenceNumber,
        )
        const connectingStreamResolve =
            this.waitForConnectingReceivingStream[globalSequenceNumber]
        if (connectingStreamResolve === undefined) {
            this.logger.error(
                'Expected a set up stream message with id ' +
                globalSequenceNumber,
            )
            return
        }

        delete this.waitForConnectingReceivingStream[globalSequenceNumber]

        connectingStreamResolve(stream)
    }

    /**
     * Establishes communication channels for a connected runner.
     * Sets up the runner's channel configuration and completes the connection promise.
     */
    connectRunner(uri: string, channels: Channels) {
        const onConnectingRunner = this.onConnectingRunners[uri]
        if (onConnectingRunner === undefined) {
            this.logger.error(
                `Unexpected runner with id  ${uri} (only runners with ids ${Object.keys(this.instantiators)} were expected)`,
            )
            return
        }

        onConnectingRunner(channels)
        delete this.onConnectingRunners[uri]
    }

    /**
     * Creates a promise that resolves when the specified runner connects.
     * Used to wait for runner initialization before proceeding with pipeline setup.
     */
    expectRunner(instantiator: Instantiator): Promise<void> {
        return new Promise((res) => {
            this.onConnectingRunners[instantiator.id.value] = (channels) => {
                this.openChannels.push(
                    this.instantiators[instantiator.id.value].setChannel(
                        channels,
                    ),
                )
                res()
            }
        })
    }

    /**
     * Initializes and starts all runners in the pipeline.
     * Process Flow:
     * For each part in the pipeline:
     *    a. Registers the runner with the server
     *    b. Starts the runner with the provided address
     *    c. Sends the pipeline configuration to the runner
     */
    async startInstantiators(addr: string, pipeline: string) {
        const resolved = await Promise.allSettled(
            Object.values(this.pipeline.parts).map(async (part) => {
                const instantiator = part.instantiator
                this.instantiators[instantiator.id.value] = instantiator

                const runnerHasConnected = this.expectRunner(instantiator)
                await instantiator.start(addr)

                await runnerHasConnected
                await instantiator.sendPipeline(pipeline)
            }),
        )

        const errors = resolved
            .filter((x) => x.status == 'rejected')
            .map((x) => x.reason)
        if (errors.length > 0) {
            for (const e of errors) {
                this.logger.error(e)
                if (e instanceof Error) {
                    this.logger.error(e.stack)
                }
            }
            process.exit(1)
        }
    }

    /**
     * Waits for all runners in the pipeline to complete their execution.
     */
    async waitClose() {
        await Promise.all(this.openChannels)
    }

    /**
     * Generates configuration arguments for a processor based on its RDF definition.
     * Creates a JSON-LD document with the processor configuration and tracks channel mappings.
     * It also keeps track of the channels, linking the Reader parts to the runner that should receive the messages.
     */
    getArguments(
        proc: SmallProc,
        instantiator: Instantiator,
        state: ChannelChecker,
    ): string {
        const shape = this.definitions[proc.type.value]
        if (!shape) {
            throw `Failed to find a shape definition for ${collapseLast(proc.id.value)} (expects shape for ${collapseLast(proc.type.value)}). Try importing the processor or check for typos.`
        }

        const jsonldDocument = shape.addToDocument(
            proc.id,
            this.quads,
            this.definitions,
        )

        // Returns the potentially found identifiers if the object is the expected type
        const findOfType = (ty: string, obj: { [key: string]: unknown }) => {
            if (obj['@type'] && obj['@id'] && obj['@type'] === ty) {
                const ids = Array.isArray(obj['@id'])
                    ? obj['@id']
                    : [obj['@id']]
                return ids.filter((x) => typeof x === 'string')
            } else {
                return []
            }
        }

        walkJson(jsonldDocument, (obj) => {
            const logLevel =
                'logLevel' in obj && typeof obj['logLevel'] === 'string'
                    ? obj['logLevel'].toLowerCase()
                    : undefined

            for (const id of findOfType(RDFC.Reader, obj)) {
                this.channelToInstantiator[id] = instantiator
                state.addReader(id)
            }

            for (const id of findOfType(RDFC.Writer, obj)) {
                if (logLevel) {
                    const logger = getLoggerFor([id, 'channel'])
                    this.logLevels[id] = (st) => logger.log(logLevel, st)
                }
                state.addWriter(id)
            }
        })

        return jsonld_to_string(jsonldDocument)
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

        const state = new ChannelChecker(this.logger)

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

class ChannelChecker {
    private readers: Set<string> = new Set()
    private writers: Set<string> = new Set()
    private logger: Logger

    constructor(logger: Logger) {
        this.logger = logger
    }

    addReader(uri: string) {
        if (this.readers.has(uri)) {
            throw new Error(
                `Only expected a single writer for channel ${collapseLast(uri)}, but found multiple`,
            )
        } else {
            this.readers.add(uri)
        }
    }

    addWriter(uri: string) {
        if (this.writers.has(uri)) {
            throw new Error(
                `Only expected a single writer for channel ${collapseLast(uri)}, but found multiple`,
            )
        } else {
            this.writers.add(uri)
        }
    }

    check() {
        // See if all channels are connected
        if (this.readers != this.writers) {
            for (const writer of this.writers) {
                // If this reader didn't exist, log an error
                if (!this.readers.delete(writer)) {
                    this.logger.error(
                        `Writer ${collapseLast(writer)} has no linked Reader.`,
                    )
                }
            }

            // If leftover readers exist, log an error
            for (const leftoverReader of this.readers) {
                this.logger.error(
                    `Reader ${collapseLast(leftoverReader)} has no linked Writer.`,
                )
            }
        }
    }
}
