/**
 * @module instantiator
 * @description Implements the instantiator functionality for managing runner instances.
 * Defines the base Instantiator class and specific implementations like CommandInstantiator and TestInstantiator.
 */

import {
    Close,
    Empty,
    FromRunner,
    ReceivingMessage,
    ReceivingStreamMessage,
    ToRunner,
} from '@rdfc/proto'
import { Orchestrator } from './orchestrator'
import { spawn } from 'child_process'
import { Quad, Term } from '@rdfjs/types'
import { Definitions } from './jsonld'
import { SmallProc } from './model'
import { jsonld_to_string } from './util'
import { getLoggerFor } from './logUtil'
import { Logger } from 'winston'

export type Sender<T> = {
    write: (msg: T) => Promise<unknown>
    close: () => void | Promise<void>
}

/**
 * Defines the communication channels between runner and orchestrator.
 */
export type Channels = {
    sendMessage: Sender<ToRunner>
    receiveMessage: AsyncIterable<FromRunner>
}

/**
 * Configuration for initializing an Instantiator.
 * @typedef {Object} InstantiatorConfig
 * @property {Term} id - Unique identifier for the instantiator
 * @property {Term} handles - The type of processors this runner can handle
 * @property {Orchestrator} orchestrator - Reference to the parent orchestrator
 */
export type InstantiatorConfig = {
    id: Term
    handles: Term
    orchestrator: Orchestrator
}

/**
 * Abstract base class for all instantiator implementations.
 * Handles communication with the orchestrator and manage the runner instance and their processors.
 */
export abstract class Instantiator {
    /** Unique identifier for this instantiator */
    readonly id: Term
    /** The type of processors this instantiator can handle */
    readonly handles: Term
    /** Logger instance for the runner */
    protected logger: Logger
    /** Map of processor IDs to their startup functions */
    protected processorsStartupFns: { [id: string]: () => void } = {}
    /** Reference to the parent orchestrator */
    protected orchestrator: Orchestrator
    /** Function to send messages to the runner */
    protected sendMessage: Sender<ToRunner> = {
        write: async () => {},
        close: async () => {},
    }

    /**
     * Creates a new Instantiator instance.
     * @param {InstantiatorConfig} config - Configuration for the instantiator
     */
    constructor(config: InstantiatorConfig) {
        Object.assign(this, config)
        this.logger = getLoggerFor([this.id.value, this])
    }

    /**
     * Abstract method that must be implemented by subclasses to actually start the runner.
     * @param {string} addr - The address that points back to the orchestrator
     * @returns {Promise<void>}
     */
    abstract start(addr: string): Promise<void>

    /**
     * Sets up bidirectional communication channels between the runner and orchestrator.
     * Routes incoming messages to the appropriate handlers and manages the message loop.
     *
     * @param {Channels} channels - Communication channels for message passing
     * @returns {Promise<void>}
     */
    async setChannel(channels: Channels) {
        this.sendMessage = channels.sendMessage

        try {
            for await (const msg of channels.receiveMessage) {
                await this.handleMessage(msg)
            }
        } catch (ex) {
            if (ex instanceof Error) {
                this.logger.info(
                    'Received an error when async reading messages ' +
                        ex.name +
                        ' ' +
                        ex.message,
                )
            }
        }

        this.logger.info('Runner ended')
    }

    /**
     * Sends a pipeline configuration to the runner.
     * @param {string} pipeline - The pipeline configuration to send
     * @returns {Promise<void>}
     */
    async sendPipeline(pipeline: string) {
        await this.sendMessage.write({ pipeline })
    }

    /**
     * Signals the runner to start all registered processors.
     * @returns {Promise<void>}
     */
    async startProcessors() {
        await this.sendMessage.write({ start: Empty })
    }

    /**
     * Forwards a message to the runner if it handles the specified channel.
     * @param {ReceivingMessage} msg - The message to forward
     * @returns {boolean}
     */
    async msg(msg: ReceivingMessage): Promise<void> {
        await this.sendMessage.write({ msg })
    }

    /**
     * Forwards a stream message to the runner if it handles the specified channel.
     * Returns true if this instantiator has a reader for this channel
     * @param {ReceivingStreamMessage} streamMsg - The stream message to forward
     * @returns {Promise<boolean>}
     */
    async streamMessage(streamMsg: ReceivingStreamMessage): Promise<void> {
        await this.sendMessage.write({ streamMsg })
    }

    /**
     * Sends a close message to the runner.
     * @param {Close} close - Close message details
     * @returns {Promise<void>}
     */
    async close(close: Close): Promise<void> {
        await this.sendMessage.write({ close })
    }

    /**
     * Handles incoming messages from the runner and routes them to the appropriate handler.
     * @param {FromRunner} msg - The message to handle
     * @returns {Promise<void>}
     */
    async handleMessage(msg: FromRunner): Promise<void> {
        if (msg.msg) {
            this.logger.debug('Runner handle data msg to ', msg.msg.channel)
            await this.orchestrator.msg(
                msg.msg,
                this.onMessageProcessedCb(
                    msg.msg.localSequenceNumber,
                    msg.msg.channel,
                ),
            )
        }

        if (msg.close) {
            this.logger.debug('Runner handle close msg to ' + msg.close.channel)
            await this.orchestrator.close(msg.close)
        }

        if (msg.initialized) {
            const init = msg.initialized
            this.logger.debug('Runner handle init msg for ' + init.uri)
            if (init.error) {
                this.logger.error('Init message error ' + init.error)
            }
            this.processorsStartupFns[init.uri]()
        }

        if (msg.identify) {
            this.logger.error("Didn't expect identify message")
        }

        if (msg.processed) {
            await this.orchestrator.processed(msg.processed)
        }
    }

    // Returns a callback that should be called when the message has been handled
    // This forwards the processed message to the runner
    onMessageProcessedCb(
        localSequenceNumber: number,
        channel: string,
    ): () => Promise<void> {
        const processedMsg: ToRunner = {
            processed: {
                localSequenceNumber,
                channel,
            },
        }
        return async () => {
            await this.sendMessage.write(processedMsg)
        }
    }

    /**
     * Instructs the runner to start a new processor with the given configuration.
     * The method resolves when the processor is fully initialized.
     *
     * @param {SmallProc} proc - The processor to start
     * @param {Quad[]} quads - RDF quads containing processor configuration
     * @param {Definitions} discoveredShapes - Available shape definitions
     * @param {string} args - serialized JSON-LD object representing the arguments of the processor
     * @returns {Promise<void>}
     * @throws {Error} If no shape definition is found for the processor
     */
    async addProcessor(
        proc: SmallProc,
        quads: Quad[],
        discoveredShapes: Definitions,
        args: string,
    ): Promise<void> {
        const processorShape = discoveredShapes[this.handles.value]
        if (processorShape === undefined) {
            const error = new Error(
                'Failed to find processor shape property ' + this.handles.value,
            )
            this.logger.error(error.message)
            throw error
        }

        const document = processorShape.addToDocument(
            proc.type,
            quads,
            discoveredShapes,
        )

        const processorIsInit = new Promise(
            (res) =>
                (this.processorsStartupFns[proc.id.value] = () =>
                    res(undefined)),
        )
        const jsonldDoc = jsonld_to_string(document)

        await this.sendMessage.write({
            proc: {
                uri: proc.id.value,
                config: jsonldDoc,
                arguments: args,
            },
        })

        await processorIsInit
    }
}

/**
 * An Instantiator implementation that starts a runner from an external command.
 * Manages the lifecycle of external runner processes.
 */
export class CommandInstantiator extends Instantiator {
    /** The command that starts this runner */
    private command: string

    /**
     * Creates a new CommandInstantiator instance.
     * @param {InstantiatorConfig & { command: string }} config - Instantiator configuration including the command to execute
     */
    constructor(config: InstantiatorConfig & { command: string }) {
        super(config)
        this.command = config.command
        this.logger.debug('Built a command runner!')
    }

    /**
     * Starts the command runner by executing the configured command.
     * Sets up stdout/stderr handlers and manages the child process.
     *
     * @param {string} addr - The address to connect to
     * @returns {Promise<void>}
     */
    async start(addr: string) {
        const uri = this.id.value
        // const args = parse(this.command) as string[]
        // args.push(addr, uri)

        let args = this.command.slice()
        args += ' ' + addr + ' ' + uri

        this.logger.info('debug msg should follow')
        this.logger.debug(
            'starting with ' + JSON.stringify(['bash', ['-l', '-c', args]]),
        )
        const child = spawn('bash', ['-l', '-c', args])

        child.stdout.on('data', (data) => {
            this.logger.debug(
                'From command ' + (<string>data.toString()).trim(),
            )
        })

        child.stderr.on('data', (data) => {
            this.logger.error((<string>data.toString()).trim())
        })

        child.on('close', (code) => {
            this.logger.info(`exited with code ${code}`)
        })
    }
}

/**
 * A test implementation of the Instantiator interface for testing purposes.
 * Simulates runner instantiation without executing actual processes.
 */
export class TestInstantiator extends Instantiator {
    /** List of processor URIs that have been started */
    private startedProcessors: string[] = []

    /**
     * Creates a new TestInstantiator instance.
     * @param {InstantiatorConfig} config - Instantiator configuration
     */
    constructor(config: InstantiatorConfig) {
        super(config)
        this.logger.info('Built test instantiator')
    }

    /**
     * Simulates starting the test runner.
     * @param {string} addr - The address to connect to
     * @returns {Promise<void>}
     */
    async start(addr: string): Promise<void> {
        this.logger.info("Test runner 'starting'", addr)
        this.logger.info('debug msg should follow')
        this.logger.debug('connecting with ' + addr)
    }

    /**
     * Simulates starting all registered processors.
     * Used for testing processor initialization.
     * @returns {Promise<void>}
     */
    async mockStartProcessor(): Promise<void> {
        this.logger.info(
            'Mock start processors ' + JSON.stringify(this.startedProcessors),
        )
        for (const uri of this.startedProcessors) {
            this.logger.info('Start processors ' + uri)
            await this.handleMessage({ initialized: { uri } })
        }
    }

    /**
     * Adds a processor to this test runner and tracks it in the started processors list.
     * Overrides the parent class method to add test-specific behavior.
     *
     * @param {SmallProc} proc - The processor to add
     * @param {Quad[]} quads - RDF quads containing processor configuration
     * @param {Definitions} discoveredShapes - Available shape definitions
     * @param {string} args - serialized JSON-LD object representing the arguments of the processor
     * @returns {Promise<void>} Resolves when the processor is added
     *
     * Process Flow:
     * 1. Tracks the processor ID in startedProcessors for test verification
     * 2. Delegates to parent class implementation for actual processor setup
     * 3. Awaits the completion of processor initialization
     */
    async addProcessor(
        proc: SmallProc,
        quads: Quad[],
        discoveredShapes: Definitions,
        args: string,
    ): Promise<void> {
        this.startedProcessors.push(proc.id.value)
        await super.addProcessor(proc, quads, discoveredShapes, args)
    }
}
