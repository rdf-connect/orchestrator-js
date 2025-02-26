import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { MyServiceServer, MyServiceService } from './generated/service'
import { Request, Response } from './generated/service'

const server: MyServiceServer = {
  sayHello: (call, callback) => {
    const request: Request = call.request
    const response: Response = { message: `Hello, ${request.name}!` }
    callback(null, response)
  },

  chatStream: async (call) => {
    const writable = promisify(call.write.bind(call))
    for await (const msg of call) {
      console.log(`Received: ${msg.user}: ${msg.message}`)
      await writable({ user: 'Server', message: `Echo: ${msg.message}` })
    }
    call.end()
    console.log('Closed')
  },
}

const grpcServer = new grpc.Server()
grpcServer.addService(MyServiceService, server)

grpcServer.bindAsync(
  '0.0.0.0:50051',
  grpc.ServerCredentials.createInsecure(),
  () => {
    console.log('Server running on port 50051')
    grpcServer.start()
  },
)
