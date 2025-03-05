import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { RunnerClient, RunnerMessage } from './generated/service'

export async function start(addr: string, uri: string) {
  const client = new RunnerClient(addr, grpc.credentials.createInsecure())

  const stream = client.connect()
  console.log('Connected with server ' + addr)
  const writable = promisify(stream.write.bind(stream))
  await writable({ identify: { uri } })

  for await (const chunk of stream) {
    const msg: RunnerMessage = chunk
    if (msg.proc) {
      const processor = msg.proc
      console.log('Starting processor ' + processor.uri)
      const args = JSON.parse(processor.arguments)
      console.log(JSON.stringify(args))

      await writable({ init: { uri: processor.uri } })
    }

    if (msg.start) {
      console.log('Start message')
      await writable({ msg: { data: 5 + '', channel: uri } })
    }
    if (msg.close) {
      console.log('Close message')
      break
    }
    if (msg.msg) {
      console.log('Data message to ' + msg.msg.channel)
      const int = parseInt(msg.msg.data)
      if (int !== 0) {
        await writable({
          msg: { data: int - 1 + '', channel: msg.msg.channel },
        })
      } else {
        await writable({ close: { channel: msg.msg.channel } })
      }
    }
  }

  stream.cancel()
  stream.end()
}
