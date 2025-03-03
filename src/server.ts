import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import {
  OrchestratorMessage,
  RunnerMessage,
  RunnerServer,
} from './generated/service'
import { Runner } from './runner'

export class Server {
  server: RunnerServer
  readonly runners: {
    [label: string]: { part: Runner; promise: () => void }
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
        const runner = this.runners[msg.identify.uri]

        runner.part.setChannel({
          sendMessage: <(msg: RunnerMessage) => Promise<void>>write,
          receiveMessage: stream,
        })

        runner.promise()
      },
    }
  }

  /// Tell the server to expect a runner to connect, returning a promise that resolves when this happens
  expectRunner(runner: Runner): Promise<void> {
    return new Promise((res) => {
      this.runners[runner.id.value] = {
        part: runner,
        promise: () => res(),
      }
    })
  }
}
