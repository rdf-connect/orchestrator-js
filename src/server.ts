import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { MyServiceClient, Request, Response } from './generated/service'

const client = new MyServiceClient(
  'localhost:50051',
  grpc.credentials.createInsecure(),
)

const sayHelloAsync = promisify<Request, Response>(client.sayHello.bind(client))

async function startChat() {
  const chatStream = client.chatStream()

  const sendPromise = promisify(chatStream.write.bind(chatStream)) // Send messages
  ;(async function () {
    for (const msg of ['Hello', 'How are you?', 'Goodbye']) {
      console.log('Sending:', msg)
      await sendPromise({ user: 'Client', message: msg })
      await new Promise((resolve) => setTimeout(resolve, 1000)) // Simulate delay
    }
    console.log('Ended')
  })()

  // Receive messages
  for await (const response of chatStream) {
    console.log('Received:', response.user, response.message)
    console.log(typeof response.message)
    if (response.message.includes('Goodbye')) {
      chatStream.cancel()
      return
    }
  }
}

// Using async/await
export async function callSayHello() {
  try {
    const request: Request = { name: 'Alice' }
    const response = await sayHelloAsync(request)
    console.log(response.message)
  } catch (err) {
    console.error('Error:', err)
  }
}

startChat()
