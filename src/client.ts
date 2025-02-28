import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { RunnerClient, RunnerMessage } from './generated/service'
import { jsonld_to_quads } from './util'
import { Writer } from 'n3'

export async function start(addr: string, uri: string) {
  const client = new RunnerClient(addr, grpc.credentials.createInsecure())

  const stream = client.connect()
  console.log('Connected with server', addr)
  const writable = promisify(stream.write.bind(stream))
  await writable({ identify: { uri } })

  for await (const chunk of stream) {
    const msg: RunnerMessage = chunk
    if (msg.proc) {
      const processor = msg.proc
      console.log('Starting processor with ', processor)
      const args = JSON.parse(processor.arguments)
      console.log(JSON.stringify(args, undefined, 2))

      const quads = await jsonld_to_quads(args)
      console.log(new Writer().quadsToString(quads))

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
      console.log('Data message')
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
