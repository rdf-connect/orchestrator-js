/**
 * @module server
 * @description Implements the gRPC server for handling communication between runners and the orchestrator.
 * Manages connections, message routing, and stream handling.
 */

import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import {
    DataChunk,
    Id,
    LogMessage,
    OrchestratorMessage,
    RunnerMessage,
    RunnerServer,
} from '@rdfc/proto'
import { Instantiator } from './instantiator'
import { getLoggerFor } from './logUtil'

/**
 * Represents a receiver for streamed data chunks.
 * @typedef {Object} Receiver
 * @property {Function} data - Callback for handling incoming data chunks
 * @property {Function} close - Callback for handling stream closure
 */
type Receiver = {
    data: (chunk: DataChunk) => Promise<unknown>
    close: () => Promise<unknown>
}

/**
 * Represents an open stream with its receivers and buffered data.
 * @typedef {Object} OpenStream
 * @property {DataChunk[]} done - Buffered chunks of data
 * @property {Receiver[]} receivers - Registered receivers for this stream
 */
type OpenStream = {
    done: DataChunk[]
    receivers: Receiver[]
}

/**
 * gRPC Server implementation for handling runner connections and message routing.
 * Manages the lifecycle of runners and their communication channels.
 */
export class Server {
    /** Logger instance for the server */
    protected logger = getLoggerFor([this])

    /** Counter for generating unique stream message IDs */
    protected streamMsgId = 0

    /** Map of active message streams */
    protected msgStreams: { [id: number]: OpenStream } = {}

    /** gRPC server instance */
    server: RunnerServer

    /**
     * Registry of all connected instantiators and their associated promises.
     *
     * @type {Object.<string, {part: Instantiator, promise: () => void}>}
     * @property {Object} [instantiatorId] - Each key is a unique instantiator identifier (URI)
     * @property {Instantiator} instantiatorId.part - The Instantiator instance handling the connection
     * @property {() => void} instantiatorId.promise - Resolver function for the connection promise
     */
    readonly instantiators: {
        [instantiatorId: string]: { part: Instantiator; promise: () => void }
    } = {}

    /**
     * Creates a new Server instance and initializes the gRPC server handlers.
     * Sets up the following gRPC service methods:
     * - connect: Handles new runner connections
     * - sendStreamMessage: Manages outgoing data streams
     * - receiveStreamMessage: Handles incoming data streams
     * - logStream: Processes log messages from runners
     */
    constructor() {
        this.server = {
            /**
             * Handles new runner connections.
             *
             * @param {grpc.ServerDuplexStream<OrchestratorMessage, RunnerMessage>} stream - Bidirectional stream for communication
             * @throws {Error} If the first message is not an identify message
             *
             * Process Flow:
             * 1. Waits for the first message which must be an 'identify' message
             * 2. Sets up the communication channel with the runner
             * 3. Resolves the runner's connection promise
             */
            connect: async (
                stream: grpc.ServerDuplexStream<
                    OrchestratorMessage,
                    RunnerMessage
                >,
            ) => {
                const msg = <OrchestratorMessage>(
                    await new Promise((res) => stream.once('data', res))
                )
                if (!msg.identify) {
                    this.logger.error(
                        'Expected the first msg to be an identify message',
                    )
                    throw new Error(
                        'Expected the first msg to be an identify message',
                    )
                }
                this.logger.debug('Got identify message')

                let closed = false
                stream.on('end', () => (closed = true))
                stream.on('close', () => (closed = true))
                stream.on('error', (err) => {
                    this.logger.debug(
                        'Unexpected stream error: ' +
                            err.name +
                            ' ' +
                            err.message,
                    )
                    closed = true
                })

                const write = promisify(stream.write.bind(stream))
                const sendMessage: (
                    msg: RunnerMessage,
                ) => Promise<void> = async (msg) => {
                    if (
                        !closed &&
                        !stream.cancelled &&
                        !stream.writableFinished &&
                        !stream.destroyed
                    ) {
                        await write(msg)
                    } else {
                        this.logger.debug('Cannot send message, stream closed')
                    }
                }
                const runner = this.instantiators[msg.identify.uri]

                runner.part.setChannel({
                    sendMessage,
                    receiveMessage: stream,
                })

                runner.promise()
            },
            /**
             * Handles incoming data streams from runners.
             *
             * @param {grpc.ServerDuplexStream<DataChunk, Id>} stream - Bidirectional stream for data transfer
             *
             * Process Flow:
             * 1. Creates a new stream with a unique ID
             * 2. Sets up storage for stream data and receivers
             * 3. Forwards received chunks to all registered receivers
             * 4. Cleans up resources when the stream ends
             */
            sendStreamMessage: async (
                stream: grpc.ServerDuplexStream<DataChunk, Id>,
            ) => {
                const id = this.streamMsgId
                this.streamMsgId += 1
                this.logger.debug('Openin stream with id ' + id)

                const obj: OpenStream = {
                    done: [],
                    receivers: [],
                }
                this.msgStreams[id] = obj

                // Sending only message on the stream
                await new Promise((res) => stream.write({ id: id }, res))

                try {
                    for await (const chunk of stream) {
                        const c: DataChunk = chunk
                        this.logger.debug('got chunk for stream ' + id)
                        obj.done.push(c)
                        for (const listener of obj.receivers) {
                            await listener.data(c)
                        }
                    }
                } catch (ex) {
                    if (ex instanceof Error) {
                        this.logger.debug(
                            'Stream message stream closed:' +
                                ex.name +
                                ' ' +
                                ex.message,
                        )
                    }
                }

                let c = obj.receivers.pop()
                while (c !== undefined) {
                    await c.close()
                    c = obj.receivers.pop()
                }

                this.logger.debug(
                    'data stream is finished, closing in 500ms ' + id,
                )
                setTimeout(() => {
                    this.logger.debug('closing data stream ' + id)
                    delete this.msgStreams[id]
                    for (const receiver of obj.receivers) {
                        receiver.close()
                    }
                }, 500)
            },
            /**
             * Handles outgoing data streams to runners.
             *
             * @param {grpc.ServerDuplexStream<Id, DataChunk>} call - Bidirectional stream call
             *
             * Process Flow:
             * 1. Looks up the stream by ID
             * 2. Sends any buffered data to the new receiver
             * 3. Registers the receiver for future data chunks
             * 4. Handles stream closure
             */
            receiveStreamMessage: async (call) => {
                const id = call.request.id
                const obj = this.msgStreams[id]
                const write = promisify(call.write.bind(call))
                if (obj === undefined) {
                    this.logger.error('No open streams found for', id)
                    call.end()
                    return
                } else {
                    this.logger.debug('Found open stream for id ' + id)
                    for (let i = 0; i < obj.done.length; i++) {
                        await write(obj.done[i])
                    }
                    obj.receivers.push({
                        close: async () => {
                            call.end()
                        },
                        data: write,
                    })
                }
            },
            /**
             * Processes log messages from runners.
             *
             * @param {grpc.ServerReadableStream<LogMessage>} call - Stream of log messages
             *
             * Process Flow:
             * 1. Iterates through incoming log messages
             * 2. Routes each message to the appropriate logger
             */
            logStream: async (call) => {
                try {
                    for await (const chunk of call) {
                        const msg: LogMessage = chunk
                        const logger = getLoggerFor(msg.entities, msg.aliases)
                        logger.log(msg.level, msg.msg)
                    }
                } catch (ex) {
                    if (ex instanceof Error) {
                        this.logger.debug(
                            'Log stream closed: ' + ex.name + ' ' + ex.message,
                        )
                    }
                }
            },
        }
    }

    /// Tell the server to expect a runner to connect, returning a promise that resolves when this happens
    expectRunner(runner: Instantiator): Promise<void> {
        return new Promise((res) => {
            this.instantiators[runner.id.value] = {
                part: runner,
                promise: () => res(),
            }
        })
    }
}
