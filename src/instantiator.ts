/**
 * @module instantiator
 * @description Implements the instantiator functionality for managing runner instances.
 * Defines the base Instantiator class and specific implementations like CommandInstantiator and TestInstantiator.
 */

import {
    Close,
    Empty,
    Message,
    OrchestratorMessage,
    RunnerMessage,
    StreamMessage,
} from '@rdfc/proto'
import { Orchestrator } from './orchestrator'
import { spawn } from 'child_process'
import { Quad, Term } from '@rdfjs/types'
import { ObjectReadable } from '@grpc/grpc-js/build/src/object-stream'
import { Definitions } from './jsonld'
import { SmallProc } from './model'
import { jsonld_to_string, RDFC } from './util'
import { getLoggerFor } from './logUtil'
import { Logger } from 'winston'

/**
 * Recursively walks through a JSON object and applies a callback to each object.
 * @param {unknown} obj - The object to walk through
 * @param {(value: { [id: string]: unknown }) => void} cb - Callback function to apply to each object
 */
function walkJson(
    obj: unknown,
    cb: (value: { [id: string]: unknown }) => void,
) {
    if (obj && typeof obj === 'object') {
        cb(<{ [id: string]: unknown }>obj) // Call function on the current object
    }

    if ((obj && typeof obj === 'object') || Array.isArray(obj)) {
        for (const v of Object.values(obj)) {
            walkJson(v, cb)
        }
    }
}

/**
 * Defines the communication channels between runner and orchestrator.
 * @typedef {Object} Channels
 * @property {Function} sendMessage - Function to send messages to the runner
 * @property {ObjectReadable<OrchestratorMessage>} receiveMessage - Stream for receiving messages from the orchestrator
 */
export type Channels = {
    sendMessage: (msg: RunnerMessage) => Promise<void>
    receiveMessage: ObjectReadable<OrchestratorMessage>
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
    /** Set of channel IRIs this runner is currently handling */
    readonly handlesChannels: Set<string> = new Set()
    /** Promise that resolves when the runner ends */
    readonly endPromise: Promise<unknown>
    /** Logger instance for the runner */
    protected logger: Logger
    /** Map of processor IDs to their startup functions */
    protected processorsStartupFns: { [id: string]: () => void } = {}
    /** Reference to the parent orchestrator */
    protected orchestrator: Orchestrator
    /** Callback for when the runner ends */
    private endCb!: (v: unknown) => unknown

    /**
     * Creates a new Instantiator instance.
     * @param {InstantiatorConfig} config - Configuration for the instantiator
     */
    constructor(config: InstantiatorConfig) {
        Object.assign(this, config)
        this.logger = getLoggerFor([this.id.value, this])
        this.endPromise = new Promise((res) => (this.endCb = res))
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

        for await (const msg of channels.receiveMessage) {
            await this.handleMessage(msg)
        }

        this.logger.info('Runner ended')
        this.endCb(undefined)
    }

    /**
     * Sends a pipeline configuration to the runner.
     * @param {string} pipeline - The pipeline configuration to send
     * @returns {Promise<void>}
     */
    async sendPipeline(pipeline: string) {
        await this.sendMessage({ pipeline })
    }

    /**
     * Signals the runner to start all registered processors.
     * @returns {Promise<void>}
     */
    async startProcessors() {
        await this.sendMessage({ start: Empty })
    }

    /**
     * Forwards a message to the runner if it handles the specified channel.
     * @param {Message} msg - The message to forward
     * @returns {Promise<void>}
     */
    async msg(msg: Message) {
        if (this.handlesChannels.has(msg.channel)) {
            await this.sendMessage({ msg })
        }
    }

    /**
     * Forwards a stream message to the runner if it handles the specified channel.
     * @param {StreamMessage} streamMsg - The stream message to forward
     * @returns {Promise<void>}
     */
    async streamMessage(streamMsg: StreamMessage) {
        if (this.handlesChannels.has(streamMsg.channel)) {
            await this.sendMessage({ streamMsg })
        }
    }

    /**
     * Sends a close message to the runner.
     * @param {Close} close - Close message details
     * @returns {Promise<void>}
     */
    async close(close: Close) {
        await this.sendMessage({ close })
    }

    /**
     * Handles incoming messages from the runner and routes them to the appropriate handler.
     * @param {OrchestratorMessage} msg - The message to handle
     * @returns {Promise<void>}
     */
    async handleMessage(msg: OrchestratorMessage): Promise<void> {
        if (msg.msg) {
            this.logger.debug('Runner handle data msg to ', msg.msg.channel)
            await this.orchestrator.msg(msg.msg)
        }
        if (msg.streamMsg) {
            this.logger.debug(
                'Runner handle stream data msg to ',
                msg.streamMsg.channel,
            )
            await this.orchestrator.streamMessage(msg.streamMsg)
        }
        if (msg.close) {
            this.logger.debug('Runner handle close msg to ' + msg.close.channel)
            await this.orchestrator.close(msg.close)
        }
        if (msg.init) {
            this.logger.debug('Runner handle init msg for ' + msg.init.uri)
            if (msg.init.error) {
                this.logger.error('Init message error ' + msg.init.error)
            }
            this.processorsStartupFns[msg.init.uri]()
        }
        if (msg.identify) {
            this.logger.error("Didn't expect identify message")
        }
    }

    /**
     * Instructs the runner to start a new processor with the given configuration.
     * The method resolves when the processor is fully initialized.
     *
     * @param {SmallProc} proc - The processor to start
     * @param {Quad[]} quads - RDF quads containing processor configuration
     * @param {Definitions} discoveredShapes - Available shape definitions
     * @returns {Promise<void>}
     * @throws {Error} If no shape definition is found for the processor
     */
    async addProcessor(
        proc: SmallProc,
        quads: Quad[],
        discoveredShapes: Definitions,
    ): Promise<void> {
        const shape = discoveredShapes[proc.type.value]
        if (!shape) {
            this.logger.error(
                `Failed to find a shape definition for ${proc.id.value} (expects shape for ${proc.type.value})`,
            )
            throw 'No shape definition found'
        }

        const jsonld_document = shape.addToDocument(
            proc.id,
            quads,
            discoveredShapes,
        )

        walkJson(jsonld_document, (obj) => {
            if (obj['@type'] && obj['@id'] && obj['@type'] === RDFC.Reader) {
                const ids = Array.isArray(obj['@id'])
                    ? obj['@id']
                    : [obj['@id']]
                for (const id of ids) {
                    if (typeof id === 'string') {
                        this.handlesChannels.add(id)
                    }
                }
            }
        })

        const args = jsonld_to_string(jsonld_document)

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

        await this.sendMessage({
            proc: {
                uri: proc.id.value,
                config: jsonldDoc,
                arguments: args,
            },
        })

        await processorIsInit
    }

    /** Function to send messages to the runner */
    protected sendMessage: (msg: RunnerMessage) => Promise<void> =
        async () => { }
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
            this.logger.info('From command ' + (<string>data.toString()).trim())
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
            await this.handleMessage({ init: { uri } })
        }
    }

    /**
     * Adds a processor to this test runner and tracks it in the started processors list.
     * Overrides the parent class method to add test-specific behavior.
     *
     * @param {SmallProc} proc - The processor to add
     * @param {Quad[]} quads - RDF quads containing processor configuration
     * @param {Definitions} discoveredShapes - Available shape definitions
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
    ): Promise<void> {
        this.startedProcessors.push(proc.id.value)
        const res = super.addProcessor(proc, quads, discoveredShapes)
        await res
    }
}


