/**
 * @module orchestrator
 * @description Core orchestrator implementation that manages the execution of RDF processing pipelines.
 * Handles pipeline configuration, runner management, and message routing.
 */

import * as grpc from '@grpc/grpc-js'
import { NamedNode } from 'n3'
import { emptyPipeline, Pipeline, PipelineShape, SmallProc } from './model.js'
import { collapseLast, getLoggerFor, prettyTurtle } from './logUtil.js'
import {
    Channels,
    Definitions,
    Instantiator,
    parse_processors,
    PROV,
} from './index.js'
import { jsonld_to_string, RDFC, walkJson } from './util.js'
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
import { writeFile } from 'fs/promises'
import { dateTimeLiteral } from './provenance.js'
import { DataFactory } from 'rdf-data-factory'
import { MessageRouter, RunnerRegistry } from './orchestrator_state.js'

const df = new DataFactory()

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
    /**Add a comment on  line L55Add diff commentMarkdown input:  edit mode selected.WritePreviewHeadingBoldItalicQuoteCodeLinkUnordered listNumbered listTask listMentionReferenceMore Formatting tools items 0Saved repliesAdd FilesPaste, drop, or click to add filesCancelCommentStart a review
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

export type ReceivingStream = grpc.ServerDuplexStream<
    ReceivingStreamControl,
    DataChunk
>

// ─── Orchestrator ───────────────────────────────────────────────────────────

/**
 * Main orchestrator class that manages the execution of RDF processing pipelines.
 * Implements the Callbacks interface for handling messages and connection events.
 */
export class Orchestrator implements Callbacks {
    protected readonly logger = getLoggerFor([this])

    /** Current pipeline configuration */
    pipeline: Pipeline = emptyPipeline
    /** RDF quads representing the current pipeline */
    quads: Quad[] = []
    /** Processor definitions parsed from the pipeline */
    definitions: Definitions = {}

    private readonly router = new MessageRouter()
    private readonly runners = new RunnerRegistry()

    /** Moment the start signal was sent to the processors (prov:startedAtTime). */
    protected processorsStartedAt?: Date

    /** Moment the processors finished, i.e. when all channels closed (prov:endedAtTime). */
    protected processorsEndedAt?: Date

    /** Maps channel URIs to the moment they were closed (prov:generatedAtTime). */
    protected readonly channelClosedAt: Map<string, Date> = new Map()

    // ── Pipeline Setup ──────────────────────────────────────────────────

    /**
     * Sets the pipeline configuration from a URI.
     * @param {Quad[]} quads - RDF quads representing the pipeline
     * @param {string} uri - URI of the pipeline configuration
     * @returns {void}
     */
    setPipeline(quads: Quad[], uri: string): PromiseLike<void>
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
    ): PromiseLike<void>
    async setPipeline(
        quads: Quad[],
        pipeline: Pipeline | string,
        definitions?: Definitions,
    ) {
        this.quads = envReplace().execute(quads)
        this.definitions = definitions ?? parse_processors(quads)

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
                    this.logLensError(ex)
                    await this.dumpExpandedPipeline(quads)
                }
                throw ex
            }
        } else {
            this.pipeline = pipeline
        }
    }

    // ── Runner Lifecycle ────────────────────────────────────────────────
    /**
     * Establishes communication channels for a connected runner.
     * Sets up the runner's channel configuration and completes the connection promise.
     */
    connectRunner(uri: string, channels: Channels) {
        if (!this.runners.connect(uri, channels)) {
            this.logger.error(
                `Unexpected runner with id ${uri} (only runners with ids ${this.runners.registeredIds} were expected)`,
            )
        }
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
            this.pipeline.parts.map(async (part) => {
                const instantiator = part.instantiator
                this.runners.register(instantiator)

                const connected = this.runners.awaitConnection(instantiator)
                await instantiator.start(addr)
                await connected
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
        await this.runners.waitAllClosed()
        this.processorsEndedAt = new Date()
    }

    // ── Message Handling ────────────────────────────────────────────────

    /**
     * Processes an incoming message by forwarding it to the receiving runner.
     *
     * @param {SendingMessage} msg - The message to process
     * @param {() => Promise<void>} onEnd - callback called when the message has been processed by the runner
     * @returns {Promise<void>}
     */
    async msg(msg: SendingMessage, onEnd: () => Promise<void>): Promise<void> {
        this.logger.debug('Got data message for channel ' + msg.channel)

        const seq = this.router.nextSequence()
        const translated: ReceivingMessage = {
            globalSequenceNumber: seq,
            channel: msg.channel,
            data: msg.data,
        }

        this.router.logIfTracked(msg.channel, msg.data)

        const target = this.router.getTarget(translated.channel)
        if (target) {
            this.router.trackAck(seq, onEnd)
            await target.msg(translated)
        } else {
            this.logger.error(
                `Receiving msg for channel ${translated.channel} without a connected reader`,
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
        const cb = this.router.resolveAck(msg.globalSequenceNumber)
        if (cb) {
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

    /**
     * Handles connection closure for a specific channel.
     * Propagates the close event to all runners in the pipeline.
     *
     * @param {Close} close - Close event details including the channel identifier
     * @returns {Promise<void>}
     */
    async close(close: Close) {
        this.logger.debug('Got close message for channel ' + close.channel)
        if (!this.channelClosedAt.has(close.channel)) {
            this.channelClosedAt.set(close.channel, new Date())
        }
        this.router.markClosed(close.channel)

        await Promise.all(
            this.pipeline.parts.map((part) => part.instantiator.close(close)),
        )

        const open = this.router.openChannelIds()
        if (open.length > 0) {
            this.logger.debug(
                `Channels still open (${open.length}/${this.router.totalChannelCount}): ${open.join(', ')}`,
            )
        } else if (this.router.totalChannelCount > 0) {
            this.logger.info('All channels closed')
        }
    }

    // ── Stream Messages ─────────────────────────────────────────────────

    async startStreamMessage(
        streamIdentify: StreamIdentify,
        sendingStream: AsyncIterable<StreamChunk> &
            grpc.ServerDuplexStream<StreamChunk, ReceivingStreamControl>,
    ): Promise<number> {
        const seq = this.router.nextSequence()
        const writeToSender = promisify(sendingStream.write.bind(sendingStream))

        const sourceRunner = this.runners.get(streamIdentify.runner)
        if (!sourceRunner) {
            throw (
                'Failed to find correct source runner with uri ' +
                streamIdentify.runner
            )
        }

        const target = this.router.getTarget(streamIdentify.channel)

        if (target) {
            this.router.trackAck(
                seq,
                sourceRunner.onMessageProcessedCb(
                    streamIdentify.localSequenceNumber,
                    streamIdentify.channel,
                ),
            )

            const readerConnected = this.router.awaitStream(seq)

            await target.streamMessage({
                channel: streamIdentify.channel,
                globalSequenceNumber: seq,
            })

            const receivingStream = await readerConnected
            const writeToReceiver = promisify(
                receivingStream.write.bind(receivingStream),
            )

            sendingStream.on('data', async (chunk: StreamChunk) => {
                if (chunk.data !== undefined) {
                    this.router.logIfTracked(
                        streamIdentify.channel,
                        chunk.data!.data,
                    )
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

            await writeToSender({ streamSequenceNumber: seq })
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

        return seq
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
        if (!this.router.connectStream(globalSequenceNumber, stream)) {
            this.logger.error(
                'Expected a set up stream message with id ' +
                    globalSequenceNumber,
            )
        }
    }

    // ── Processors ──────────────────────────────────────────────────────

    /**
     * Builds the runtime timing provenance quads collected during execution.
     *
     * For every processor a `prov:startedAtTime` (when the start signal was
     * sent) and a `prov:endedAtTime` (when all channels closed) triple is
     * emitted. For every closed channel a `prov:generatedAtTime` triple is
     * emitted with the moment the channel was closed.
     *
     * @returns {Quad[]} The collected timing quads (empty if nothing ran yet).
     */
    getProvenanceTimingQuads(): Quad[] {
        const quads: Quad[] = []

        for (const part of this.pipeline.parts) {
            for (const proc of part.processors) {
                if (proc.id.termType !== 'NamedNode') {
                    continue
                }
                if (this.processorsStartedAt) {
                    quads.push(
                        df.quad(
                            proc.id,
                            PROV.terms.startedAtTime,
                            dateTimeLiteral(this.processorsStartedAt),
                        ),
                    )
                }
                if (this.processorsEndedAt) {
                    quads.push(
                        df.quad(
                            proc.id,
                            PROV.terms.endedAtTime,
                            dateTimeLiteral(this.processorsEndedAt),
                        ),
                    )
                }
            }
        }

        for (const [channel, closedAt] of this.channelClosedAt) {
            quads.push(
                df.quad(
                    df.namedNode(channel),
                    PROV.terms.generatedAtTime,
                    dateTimeLiteral(closedAt),
                ),
            )
        }

        return quads
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
        const checker = new ChannelChecker(this.logger)

        for (const part of this.pipeline.parts) {
            const runner = part.instantiator
            for (const proc of part.processors) {
                this.logger.debug(
                    `Adding processor ${proc.id.value} (${proc.type.value}) to runner ${runner.id.value}`,
                )

                const args = this.buildProcessorArgs(proc, runner, checker)
                startPromises.push(
                    runner.addProcessor(
                        proc,
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
        this.processorsStartedAt = new Date()
    }

    // ── Private Helpers ─────────────────────────────────────────────────
    /**
     * Generates configuration arguments for a processor based on its RDF definition.
     * Creates a JSON-LD document with the processor configuration and tracks channel mappings.
     * It also keeps track of the channels, linking the Reader parts to the runner that should receive the messages.
     */
    private buildProcessorArgs(
        proc: SmallProc,
        instantiator: Instantiator,
        checker: ChannelChecker,
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
                this.router.registerChannel(id, instantiator)
                checker.addReader(id)
            }

            for (const id of findOfType(RDFC.Writer, obj)) {
                if (logLevel) {
                    const logger = getLoggerFor([id, 'channel'])
                    this.router.registerLogFn(id, (st) =>
                        logger.log(logLevel, st),
                    )
                }
                checker.addWriter(id)
            }
        })

        return jsonld_to_string(jsonldDocument)
    }

    private logLensError(ex: LensError) {
        const id = ex.lineage
            .filter((x) => x.name === 'id' || x.name === 'pred')
            .map((x) => <string>x.opts)
            .join(' -> ')
        const linReversed = ex.lineage.slice().reverse()
        this.logger.error('Error happened when parsing at ' + id)

        const lastPred = <string>(
            linReversed.find((x) => x.name === 'pred')?.opts
        )
        const lastId = <string>linReversed.find((x) => x.name === 'id')?.opts
        const foundSome = linReversed[0].name !== 'pred'
        const isType =
            lastPred == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'

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

    private async dumpExpandedPipeline(quads: Quad[]) {
        try {
            const tts = await prettyTurtle(quads)
            await writeFile('/tmp/expanded.ttl', tts, { encoding: 'utf-8' })
            this.logger.error('Expanded pipeline written to /tmp/expanded.ttl')
        } catch (ex) {
            this.logger.error('Writing /tmp/expanded.ttl failed')
            if (ex instanceof Error) {
                this.logger.error(ex.name, ex.message, ex.cause)
            } else {
                this.logger.error(JSON.stringify(ex))
            }
        }
    }
}

// ─── Channel Checker ────────────────────────────────────────────────────────

class ChannelChecker {
    private readers = new Set<string>()
    private writers = new Set<string>()
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
