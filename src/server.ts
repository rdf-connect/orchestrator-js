import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import {
  DataChunk,
  Id,
  LogMessage,
  OrchestratorMessage,
  RunnerMessage,
  RunnerServer,
} from './generated/service'
import { Runner } from './runner'
import { getLoggerFor } from './logUtil'

type Receiver = {
  data: (chunk: DataChunk) => Promise<unknown>
  close: () => Promise<unknown>
}

type OpenStream = {
  done: DataChunk[]
  receivers: Receiver[]
}

export class Server {
  protected logger = getLoggerFor([this])
  protected streaMsgId = 0
  protected msgStreams: { [id: number]: OpenStream } = {}
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
          this.logger.error('Expected the first msg to be an identify message')
          throw new Error('Expected the first msg to be an identify message')
        }
        this.logger.debug('Got identify message')

        const write = promisify(stream.write.bind(stream))
        const runner = this.runners[msg.identify.uri]

        runner.part.setChannel({
          sendMessage: <(msg: RunnerMessage) => Promise<void>>write,
          receiveMessage: stream,
        })

        runner.promise()
      },
      sendStreamMessage: async (
        stream: grpc.ServerDuplexStream<DataChunk, Id>,
      ) => {
        const id = this.streaMsgId
        this.streaMsgId += 1
        this.logger.debug('Openin stream with id ' + id)

        const obj: OpenStream = {
          done: [],
          receivers: [],
        }
        this.msgStreams[id] = obj

        // Sending only message on the stream
        await new Promise((res) => stream.write({ id: id }, res))

        for await (const chunk of stream) {
          const c: DataChunk = chunk
          this.logger.debug('got chunk for stream ' + id)
          obj.done.push(c)
          for (const listener of obj.receivers) {
            await listener.data(c)
          }
        }

        let c = obj.receivers.pop()
        while (c !== undefined) {
          await c.close()
          c = obj.receivers.pop()
        }

        this.logger.debug('data stream is finished, closing in 500ms ' + id)
        setTimeout(() => {
          this.logger.debug('closing data stream ' + id)
          delete this.msgStreams[id]
          for (const receiver of obj.receivers) {
            receiver.close()
          }
        }, 500)
      },
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
      logStream: async (call) => {
        for await (const chunk of call) {
          const msg: LogMessage = chunk
          const logger = getLoggerFor(msg.entities, msg.aliases)
          logger.log(msg.level, msg.msg)
        }
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
