import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import {
  Close,
  Message,
  OrchestratorMessage,
  Processor,
  ProcessorInit,
  RunnerMessage,
  RunnerServer,
} from './generated/service'
import { Empty } from './generated/google/protobuf/empty'

export interface ToRunner {
  processor(proc: Processor): Promise<unknown>
  start(): Promise<unknown>
  message(msg: Message): Promise<unknown>
  close(msg: Close): Promise<unknown>
}

export function stubToRunner(): ToRunner {
  return {
    processor: async () => {},
    start: async () => {},
    message: async () => {},
    close: async () => {},
  }
}

export interface FromRunner {
  setWriter(orchestrator: ToRunner): Promise<void>
  init(msg: ProcessorInit): Promise<void>
  msg(msg: Message): Promise<void>
  close(msg: Close): Promise<void>
}

export class Server {
  server: RunnerServer
  readonly runners: {
    [label: string]: { part: FromRunner; promise: () => void }
  } = {}

  constructor() {
    this.server = {
      connect: async (
        stream: grpc.ServerDuplexStream<OrchestratorMessage, RunnerMessage>,
      ) => {
        const msg = <OrchestratorMessage>(
          await new Promise((res) => stream.once('data', res))
        )
        if (!msg.identify) {
          throw 'Expected the first msg to be an identify message'
        } else {
          console.log('Got identify message')
        }

        const write = promisify(stream.write.bind(stream))
        const orchestrator_part: ToRunner = {
          processor: (proc: Processor) => write({ proc }),
          start: () => write({ start: Empty }),
          message: (msg: Message) => write({ msg }),
          close: (close: Close) => write({ close }),
        }

        const runner = this.runners[msg.identify.uri]

        ;(async () => {
          for await (const chunk of stream) {
            const msg: OrchestratorMessage = chunk
            if (msg.msg) {
              console.log('Data message', msg.msg)
              runner.part.msg(msg.msg)
            }
            if (msg.init) {
              console.log('Init message', msg.init)
              runner.part.init(msg.init)
            }
            if (msg.close) {
              console.log('Close message', msg.close)
              runner.part.close(msg.close)
            }
            if (msg.identify) {
              console.error("Didn't expect identify message")
            }
          }
        })()
        runner.part.setWriter(orchestrator_part)
        runner.promise()
      },
    }
  }

  addRunner(uri: string, part: FromRunner): Promise<void> {
    return new Promise((res) => {
      this.runners[uri] = {
        part,
        promise: () => res(),
      }
    })
  }
}
