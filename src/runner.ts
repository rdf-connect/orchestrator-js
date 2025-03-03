import { parse } from 'shell-quote'
import {
  Close,
  Message,
  OrchestratorMessage,
  RunnerMessage,
} from './generated/service'
import { orchestrator, ProcessorInstance } from './orchestrator'
import { spawn } from 'child_process'
import { Empty } from './generated/google/protobuf/empty'
import { Term } from '@rdfjs/types'
import { URI } from './model'
import { ObjectReadable } from '@grpc/grpc-js/build/src/object-stream'

export type Channels = {
  sendMessage: (msg: RunnerMessage) => Promise<void>
  receiveMessage: ObjectReadable<OrchestratorMessage>
}

export type RunnerConfig = {
  id: Term
  handles: URI[]
  processor_definition: URI
}

export abstract class Runner {
  protected sendMessage: (msg: RunnerMessage) => Promise<void> = async () => {}

  protected processors: { [id: string]: () => void } = {}

  readonly id: Term
  readonly handles: URI[]
  readonly processor_definition: URI
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
    await this.sendMessage({ msg })
  }

  async close(close: Close) {
    await this.sendMessage({ close })
  }

  async handleMessage(msg: OrchestratorMessage): Promise<void> {
    if (msg.msg) {
      await orchestrator.msg(msg.msg)
    }
    if (msg.close) {
      await orchestrator.close(msg.close)
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
  addProcessor(processor: ProcessorInstance): Promise<void> {
    this.sendMessage({
      proc: {
        uri: processor.proc.id.value,
        config: '{}',
        arguments: processor.arguments,
      },
    })
    return new Promise((res) => {
      this.processors[processor.proc.id.value] = res
    })
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
