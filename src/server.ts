/**
 * @module server
 * @description Implements the gRPC server for handling communication between runners and the orchestrator.
 * Manages connections, message routing, and stream handling.
 */

import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { FromRunner, LogMessage, RunnerServer, ToRunner } from '@rdfc/proto'
import { getLoggerFor } from './logUtil'
import {
    DataChunk,
    ReceivingStreamControl,
    SendingStreamControl,
    StreamChunk,
} from '@rdfc/proto/lib/generated/common'
import { Orchestrator } from './orchestrator'

/**
 * gRPC Server implementation for handling runner connections and message routing.
 * Manages the lifecycle of runners and their communication channels.
 */
export class Server {
    /** Logger instance for the server */
    protected logger = getLoggerFor([this])

    /** gRPC server instance */
    readonly server: RunnerServer

    protected readonly orchestrator: Orchestrator

    /**
     * Creates a new Server instance and initializes the gRPC server handlers.
     * Sets up the following gRPC service methods:
     * - connect: Handles new runner connections
     * - sendStreamMessage: Manages outgoing data streams
     * - receiveStreamMessage: Handles incoming data streams
     * - logStream: Processes log messages from runners
     */
    constructor(orchestrator: Orchestrator) {
        this.orchestrator = orchestrator
        this.server = {
            /**
             * Handles new runner connections.
             *
             * @param {grpc.ServerDuplexStream<FromRunner, ToRunner>} stream - Bidirectional stream for communication
             * @throws {Error} If the first message is not an identify message
             *
             * Process Flow:
             * 1. Waits for the first message which must be an 'identify' message
             * 2. Sets up the communication channel with the runner
             * 3. Resolves the runner's connection promise
             */
            connect: async (
                stream: grpc.ServerDuplexStream<FromRunner, ToRunner>,
            ) => {
                const msg = <FromRunner>(
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
                const sendMessage: (msg: ToRunner) => Promise<void> = async (
                    msg,
                ) => {
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

                const channels = {
                    sendMessage: {
                        write: sendMessage,
                        close: async () => {
                            stream.end()
                        },
                    },
                    receiveMessage: stream,
                }
                this.orchestrator.connectRunner(msg.identify.uri, channels)
            },
            /**
             * Handles incoming data streams from runners.
             *
             * @param {grpc.ServerDuplexStream<StreamChunk, ReceivingStreamControl>} stream - Bidirectional stream for data transfer
             *
             * Process Flow:
             * 1. Creates a new stream with a unique ID
             * 2. Notifies the receiving runner that a stream message is pending
             * 3. Awaits for each instantiator that has a reader for that channel to connect
             * 4. Notify writing runner to start sending data
             * 5. Forwards received chunks to all registered receivers
             * 6. For each chunk forward the processed control message back to the sending runner
             * 7. Cleans up resources when the stream ends
             */
            sendStreamMessage: async (
                stream: grpc.ServerDuplexStream<
                    StreamChunk,
                    ReceivingStreamControl
                >,
            ) => {
                // Get the actual identifier of the channel
                const identify = <StreamChunk>(
                    await new Promise((res) => stream.once('data', res))
                )

                if (!identify.id) {
                    throw new Error(
                        'The first message of a sending stream message must be a stream identifier',
                    )
                }

                await this.orchestrator.startStreamMessage(identify.id, stream)
            },
            /**
             * Handles outgoing data streams to runners.
             *
             * Notifies the orchestrator that a receiving message stream call is connected.
             * When stream message is finished, close the stream.
             */
            receiveStreamMessage: async (
                call: grpc.ServerDuplexStream<SendingStreamControl, DataChunk>,
            ) => {
                const ctrl = <SendingStreamControl>(
                    await new Promise((res) => call.once('data', res))
                )
                if (ctrl.globalSequenceNumber === undefined) {
                    throw 'Expected identifying control message as first message of stream message, identifying which stream it wants to receive'
                }
                const id = ctrl.globalSequenceNumber!
                this.logger.debug('Receive stream message ' + id)

                this.orchestrator.onReceivingStreamConnected(id, call)
            },
            /**
             * Processes log messages from runners.
             *
             * Process Flow:
             * 1. Iterates through incoming log messages
             * 2. Routes each message to the appropriate logger
             */
            logStream: async (
                call: grpc.ServerReadableStream<LogMessage, null>,
            ) => {
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
}
