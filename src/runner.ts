import { parse } from 'shell-quote'
import {
  Close,
  Message,
  OrchestratorMessage,
  RunnerMessage,
} from './generated/service'
import { Orchestrator } from './orchestrator'
import { spawn } from 'child_process'
import { Empty } from './generated/google/protobuf/empty'
import { Quad, Term } from '@rdfjs/types'
import { URI } from './model'
import { ObjectReadable } from '@grpc/grpc-js/build/src/object-stream'
import { Definitions } from './jsonld'
import { Processor } from './model'
import { jsonld_to_string, RDFC } from './util'

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
  handles: URI[]
  processor_definition: URI
  orchestrator: Orchestrator
}

export abstract class Runner {
  protected sendMessage: (msg: RunnerMessage) => Promise<void> = async () => {}
  protected processors: { [id: string]: () => void } = {}
  protected orchestrator: Orchestrator

  readonly id: Term
  readonly handles: URI[]
  readonly processor_definition: URI

  readonly handlesChannels: Set<string> = new Set()
  constructor(config: RunnerConfig) {
    Object.assign(this, config)
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
  }

  async startProcessors() {
    await this.sendMessage({ start: Empty })
  }

  async msg(msg: Message) {
    if (this.handlesChannels.has(msg.channel)) {
      await this.sendMessage({ msg })
    }
  }

  async close(close: Close) {
    await this.sendMessage({ close })
  }

  async handleMessage(msg: OrchestratorMessage): Promise<void> {
    console.log('Runner handle msg', msg)
    if (msg.msg) {
      await this.orchestrator.msg(msg.msg)
    }
    if (msg.close) {
      await this.orchestrator.close(msg.close)
    }
    if (msg.init) {
      this.processors[msg.init.uri]()
    }
    if (msg.identify) {
      console.error("Didn't expect identify message")
    }
  }

  // Tells the runner to start a processor with configuration
  // Returning a promise that resolves when the processor is initialized
  async addProcessor(
    proc: Processor,
    quads: Quad[],
    discoveredShapes: Definitions,
  ): Promise<void> {
    const shape = discoveredShapes[proc.type.id.value]
    if (!shape) {
      console.error(
        `Failed to find a shape defintion for ${proc.id.value} (expects shape for ${proc.type.id.value})`,
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
        const ids = Array.isArray(obj['@id']) ? obj['@id'] : [obj['@id']]
        for (const id of ids) {
          if (typeof id === 'string') {
            console.log('Found writer!', id)
            this.handlesChannels.add(id)
          }
        }
      }
    })

    const args = jsonld_to_string(jsonld_document)

    const processorShape = discoveredShapes[this.processor_definition]
    console.log(
      'Found processor shape for',
      this.processor_definition,
      !!processorShape,
    )

    const document = processorShape.addToDocument(
      proc.type.id,
      quads,
      discoveredShapes,
    )

    const processorIsInit = new Promise(
      (res) => (this.processors[proc.id.value] = () => res(undefined)),
    )

    await this.sendMessage({
      proc: {
        uri: proc.id.value,
        config: jsonld_to_string(document),
        arguments: args,
      },
    })

    await processorIsInit
  }
}

export class CommandRunner extends Runner {
  private command: string
  constructor(command: string, config: RunnerConfig) {
    super(config)
    console.log('Built a command runner!')
    this.command = command
  }
  async start(addr: string) {
    const uri = this.id.value
    const [cmd, ...args] = parse(this.command) as string[]
    args.push(addr, uri)
    console.log('starting with ', [cmd, ...args])
    const child = spawn(cmd, args)

    child.stdout.on('data', (data) => {
      console.log(`${uri}: ${data}`)
    })

    child.stderr.on('data', (data) => {
      console.error(`${uri}: ${data}`)
    })

    child.on('close', (code) => {
      console.log(`${uri}: exited with code ${code}`)
    })
  }
}

export class TestRunner extends Runner {
  private startedProcessors: string[] = []
  constructor(config: RunnerConfig) {
    super(config)
    console.log('Built testrunner')
  }

  async start(addr: string): Promise<void> {
    console.log("Test runner 'starting'", addr)
  }

  async mockStartProcessor(): Promise<void> {
    console.log('Mock start processors')
    for (const uri of this.startedProcessors) {
      await this.handleMessage({ init: { uri } })
    }
  }

  async addProcessor(
    proc: Processor,
    quads: Quad[],
    discoveredShapes: Definitions,
  ): Promise<void> {
    this.startedProcessors.push(proc.id.value)
    const res = super.addProcessor(proc, quads, discoveredShapes)
    await res
  }
}
