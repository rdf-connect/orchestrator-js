import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { Message, RunnerServer, RunnerService } from './generated/service'
import { Empty } from './generated/google/protobuf/empty'

const server: RunnerServer = {
  init: (processor, callback) => {
    console.log('Starting processor with ', processor)
    callback(null, Empty)
  },

  start: async (stream) => {
    const writable = promisify(stream.write.bind(stream))
    writable({ uri: '', data: 5 + '' })
    for await (const chunk of stream) {
      const msg: Message = chunk
      console.log('Got message', msg)
      if (msg.data) {
        const int = parseInt(msg.data)
        if (int !== 0) {
          await writable({ uri: '', data: int - 1 + '' })
        } else {
          await writable({ uri: '', close: 1 })
          break
        }
      } else {
        stream.end()
        break
      }
    }

    console.log('Closed')
    grpcServer.tryShutdown((e) => console.error('shutdown', e))
    // process.exit(0)
  },
}

const grpcServer = new grpc.Server()
grpcServer.addService(RunnerService, server)

export function start(host: string, port: string) {
  grpcServer.bindAsync(
    host + ':' + port,
    grpc.ServerCredentials.createInsecure(),
    () => {
      console.log('Server running on port ' + port)
      grpcServer.start()
    },
  )
}
