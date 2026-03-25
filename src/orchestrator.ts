/**
 * @module orchestrator
 * @description Core orchestrator implementation that manages the execution of RDF processing pipelines.
 * Handles pipeline configuration, runner management, and message routing.
 */

import * as grpc from '@grpc/grpc-js'
import { NamedNode } from 'n3'
import { emptyPipeline, Pipeline, PipelineShape, SmallProc } from './model'
import { collapseLast, getLoggerFor, prettyTurtle } from './logUtil'
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
import { writeFile } from 'fs/promises'

const decoder = new TextDecoder()

/**
 * Defines the callback interface for handling messages and connection closures.
 */
export type Callbacks = {
    msg: (msg: SendingMessage, onEnd: () => Promise<void>) => Promise<void>
    close: (close: Close) => Promise<void>
}

function pipelineIsString(pipeline: Pipeline | string): pipeline is string {
    return typeof pipeline === 'string' || pipeline instanceof String
}

type ReceivingStream = grpc.ServerDuplexStream<
    ReceivingStreamControl,
    DataChunk
>

// ─── Channel Routing ────────────────────────────────────────────────────────

/**
 * Manages channel-to-instantiator routing, message sequence numbers,
 * acknowledgement tracking, and stream connection handshakes.
 */
class MessageRouter {
    private readonly channels = new Map<string, Instantiator>()
    private readonly closed = new Set<string>()
    private readonly logFns = new Map<string, (msg: string) => void>()
    private readonly pendingAcks = new Map<number, () => Promise<void>>()
    private readonly pendingStreams = new Map<
        number,
        (stream: ReceivingStream) => void
    >()
    private sequenceNumber = 0

    nextSequence(): number {
        return this.sequenceNumber++
    }

    registerChannel(channelId: string, target: Instantiator) {
        this.channels.set(channelId, target)
    }

    registerLogFn(channelId: string, fn: (msg: string) => void) {
        this.logFns.set(channelId, fn)
    }

    getTarget(channelId: string): Instantiator | undefined {
        return this.channels.get(channelId)
    }

    logIfTracked(channelId: string, data: Uint8Array) {
        this.logFns.get(channelId)?.(decoder.decode(data))
    }

    trackAck(seqNum: number, onEnd: () => Promise<void>) {
        this.pendingAcks.set(seqNum, onEnd)
    }

    resolveAck(seqNum: number): (() => Promise<void>) | undefined {
        const cb = this.pendingAcks.get(seqNum)
        if (cb) this.pendingAcks.delete(seqNum)
        return cb
    }

    markClosed(channelId: string) {
        this.closed.add(channelId)
    }

    openChannelIds(): string[] {
        return [...this.channels.keys()].filter((ch) => !this.closed.has(ch))
    }

    get totalChannelCount(): number {
        return this.channels.size
    }

    awaitStream(seqNum: number): Promise<ReceivingStream> {
        return new Promise((resolve) => {
            this.pendingStreams.set(seqNum, resolve)
        })
    }

    connectStream(seqNum: number, stream: ReceivingStream): boolean {
        const resolve = this.pendingStreams.get(seqNum)
        if (!resolve) return false
        this.pendingStreams.delete(seqNum)
        resolve(stream)
        return true
    }
}

// ─── Runner Registry ────────────────────────────────────────────────────────

/**
 * Tracks runner registration, pending connection handshakes,
 * and open channel promises.
 */
class RunnerRegistry {
    private readonly runners = new Map<string, Instantiator>()
    private readonly pending = new Map<string, (channels: Channels) => void>()
    private readonly channelPromises: Promise<unknown>[] = []

    register(instantiator: Instantiator) {
        this.runners.set(instantiator.id.value, instantiator)
    }

    get(uri: string): Instantiator | undefined {
        return this.runners.get(uri)
    }

    get registeredIds(): string[] {
        return [...this.runners.keys()]
    }

    /** Sets up a promise that resolves when the runner connects via gRPC. */
    awaitConnection(instantiator: Instantiator): Promise<void> {
        return new Promise((resolve) => {
            this.pending.set(instantiator.id.value, (channels) => {
                this.channelPromises.push(instantiator.setChannel(channels))
                resolve()
            })
        })
    }

    /** Completes a pending runner connection. Returns false if unexpected. */
    connect(uri: string, channels: Channels): boolean {
        const cb = this.pending.get(uri)
        if (!cb) return false
        cb(channels)
        this.pending.delete(uri)
        return true
    }

    async waitAllClosed(): Promise<void> {
        await Promise.all(this.channelPromises)
    }
}

// ─── Orchestrator ───────────────────────────────────────────────────────────

/**
 * Main orchestrator class that manages the execution of RDF processing pipelines.
 * Implements the Callbacks interface for handling messages and connection events.
 */
export class Orchestrator implements Callbacks {
    protected readonly logger = getLoggerFor([this])

    pipeline: Pipeline = emptyPipeline
    quads: Quad[] = []
    definitions: Definitions = {}

    private readonly router = new MessageRouter()
    private readonly runners = new RunnerRegistry()

    // ── Pipeline Setup ──────────────────────────────────────────────────

    setPipeline(quads: Quad[], uri: string): PromiseLike<void>
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

    connectRunner(uri: string, channels: Channels) {
        if (!this.runners.connect(uri, channels)) {
            this.logger.error(
                `Unexpected runner with id ${uri} (only runners with ids ${this.runners.registeredIds} were expected)`,
            )
        }
    }

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

    async waitClose() {
        await this.runners.waitAllClosed()
    }

    // ── Message Handling ────────────────────────────────────────────────

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

    async close(close: Close) {
        this.logger.debug('Got close message for channel ' + close.channel)
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
    }

    // ── Private Helpers ─────────────────────────────────────────────────

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
