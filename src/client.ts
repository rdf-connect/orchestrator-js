import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import {
  DataChunk,
  Id,
  OrchestratorMessage,
  RunnerClient,
  RunnerMessage,
} from './generated/service'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
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

    if (msg.streamMsg) {
      console.log('Start receive stream message')
      startStreamMessage(msg.streamMsg.id!, client)
    }

    if (msg.start) {
      const uri = 'http://example.org/input'
      // console.log('Start message, starting send to', uri)
      // const st = 5 + ''
      // await writable({ msg: { data: encoder.encode(st), channel: uri } })

      sendStream(2, uri, client, writable)
    }
    if (msg.close) {
      console.log('Close message')
      break
    }

    if (msg.msg) {
      const int = parseInt(decoder.decode(msg.msg.data))
      console.log('Data message to ' + msg.msg.channel, int)
      if (int !== 0) {
        await writable({
          msg: { data: encoder.encode(int - 1 + ''), channel: msg.msg.channel },
        })
      } else {
        await writable({ close: { channel: msg.msg.channel } })
      }
    }
  }

  stream.cancel()
  stream.end()
}

async function startStreamMessage(id: Id, client: RunnerClient) {
  console.log('startStreamMessage', id.id)
  const st = client.receiveStreamMessage(id)
  for await (const c of st) {
    const chunk: DataChunk = c
    console.log('Got chunk', id.id, decoder.decode(chunk.data))
  }
}

async function sendStream(
  chunks: number,
  uri: string,
  client: RunnerClient,
  writable: (msg: OrchestratorMessage) => Promise<unknown>,
  timeBetween = 100,
) {
  console.log('starting stream', chunks, 'chunks to', uri)
  const stream = client.sendStreamMessage()
  const id: Id = await new Promise((res) => stream.once('data', res))

  console.log('Send streamMsg event')
  await writable({ streamMsg: { id, channel: uri } })

  const write = promisify(stream.write.bind(stream))

  for (let i = 0; i < chunks; i++) {
    console.log('Sending chunk', i)
    await write({ data: encoder.encode('Chunk ' + i) })
    await new Promise((res) => setTimeout(res, timeBetween))
  }

  console.log('StreamMsg is done')
  stream.end()

  if (chunks > 0) {
    await sendStream(chunks - 1, uri, client, writable)
  }
}
