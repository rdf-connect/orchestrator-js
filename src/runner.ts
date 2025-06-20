import { parse } from 'shell-quote'
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

export type Channels = {
    sendMessage: (msg: RunnerMessage) => Promise<void>
    receiveMessage: ObjectReadable<OrchestratorMessage>
}

export type RunnerConfig = {
    id: Term
    handles: Term
    orchestrator: Orchestrator
}

export abstract class Runner {
    protected logger: Logger

    protected sendMessage: (msg: RunnerMessage) => Promise<void> =
        async () => {}
    protected processors: { [id: string]: () => void } = {}
    protected orchestrator: Orchestrator

    readonly id: Term
    readonly handles: Term

    readonly handlesChannels: Set<string> = new Set()

    private endCb!: (v: unknown) => unknown
    readonly endPromise: Promise<unknown>

    constructor(config: RunnerConfig) {
        Object.assign(this, config)
        this.logger = getLoggerFor([this.id.value, this])
        this.endPromise = new Promise((res) => (this.endCb = res))
    }

    abstract start(addr: string): Promise<void>

    // Sets up the communication channels to and from the runner
    // Routing incoming messages to the orchestrator
    // And sending messages to the runner
    // Including starting new processors
    async setChannel(channels: Channels) {
        this.sendMessage = channels.sendMessage

        for await (const msg of channels.receiveMessage) {
            await this.handleMessage(msg)
        }

        this.logger.info('Runner ended')
        this.endCb(undefined)
    }

    async sendPipeline(pipeline: string) {
        await this.sendMessage({ pipeline })
    }

    async startProcessors() {
        await this.sendMessage({ start: Empty })
    }

    async msg(msg: Message) {
        if (this.handlesChannels.has(msg.channel)) {
            await this.sendMessage({ msg })
        }
    }

    async streamMessage(streamMsg: StreamMessage) {
        if (this.handlesChannels.has(streamMsg.channel)) {
            await this.sendMessage({ streamMsg })
        }
    }

    async close(close: Close) {
        await this.sendMessage({ close })
    }

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
            this.processors[msg.init.uri]()
        }
        if (msg.identify) {
            this.logger.error("Didn't expect identify message")
        }
    }

    // Tells the runner to start a processor with configuration
    // Returning a promise that resolves when the processor is initialized
    async addProcessor(
        proc: SmallProc,
        quads: Quad[],
        discoveredShapes: Definitions,
    ): Promise<void> {
        const shape = discoveredShapes[proc.type.value]
        if (!shape) {
            this.logger.error(
                `Failed to find a shape defintion for ${proc.id.value} (expects shape for ${proc.type.value})`,
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
            (res) => (this.processors[proc.id.value] = () => res(undefined)),
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
}

export class CommandRunner extends Runner {
    private command: string
    constructor(config: RunnerConfig & { command: string }) {
        super(config)
        this.command = config.command
        this.logger.debug('Built a command runner!')
    }

    async start(addr: string) {
        const uri = this.id.value
        const [cmd, ...args] = parse(this.command) as string[]
        args.push(addr, uri)

        this.logger.info('debug msg should follow')
        this.logger.debug('starting with ' + JSON.stringify([cmd, ...args]))
        const child = spawn(cmd, args)

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

export class TestRunner extends Runner {
    private startedProcessors: string[] = []
    constructor(config: RunnerConfig) {
        super(config)
        this.logger.info('Built testrunner')
    }

    async start(addr: string): Promise<void> {
        this.logger.info("Test runner 'starting'", addr)
        this.logger.info('debug msg should follow')
        this.logger.debug('connecting with ' + addr)
    }

    async mockStartProcessor(): Promise<void> {
        this.logger.info(
            'Mock start processors ' + JSON.stringify(this.startedProcessors),
        )
        for (const uri of this.startedProcessors) {
            this.logger.info('Start processors ' + uri)
            await this.handleMessage({ init: { uri } })
        }
    }

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
