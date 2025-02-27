import * as grpc from '@grpc/grpc-js'
import { Message, RunnerClient } from './generated/service'
import { Runner } from './model'
import { ProcessorInstance } from './orchestrator'
import { ChildProcess, spawn } from 'child_process'
import { parse } from 'shell-quote'

export type SendMsg = (msg: Message) => Promise<void>
let port = 50052
function getNextPort() {
  port += 1
  return port
}

export async function startInstance(
  runner: Runner,
  cb: SendMsg,
  waitTime = 200,
) {
  const port = getNextPort() + ''
  const [cmd, ...args] = parse(runner.command) as string[]
  args.push('0.0.0.0', port)
  console.log('starting with ', [cmd, ...args])
  const child = spawn(cmd, args)

  child.stdout.on('data', (data) => {
    console.log(`${runner.id.value}: ${data}`)
  })

  child.stderr.on('data', (data) => {
    console.error(`${runner.id.value}: ${data}`)
  })

  child.on('close', (code) => {
    console.log(`${runner.id.value}: exited with code ${code}`)
  })

  await new Promise((res) => setTimeout(res, waitTime))

  const runnerClient = new RunnerClient(
    'localhost:' + port,
    grpc.credentials.createInsecure(),
  )

  return new Instance(runnerClient, child, cb)
}

export class Instance {
  private _child: ChildProcess
  private client: RunnerClient
  private cb: SendMsg

  private channels: SendMsg[] = []

  constructor(client: RunnerClient, child: ChildProcess, cb: SendMsg) {
    this.client = client
    this._child = child
    this.cb = cb
    // maybe we want to see if the child dies, we don't send any msg anymore
  }

  async send(message: Message) {
    for (const channel of this.channels) {
      await channel(message)
    }
  }

  async init(processor: ProcessorInstance) {
    await new Promise((res, rej) =>
      this.client.init(
        {
          uri: processor.proc.id.value,
          arguments: processor.arguments,
          config: '',
        },
        (error) => {
          if (error) rej(error)
          res(null)
        },
      ),
    )
  }

  async start() {
    const stream = this.client.start()
    ;(async () => {
      for await (const msg of stream) {
        this.cb(msg)
        if (msg.data === undefined) {
          break
        }
      }
      console.log('Closing server side')
      stream.cancel()
    })()

    this.channels.push(
      (msg) =>
        new Promise((res, rej) => {
          stream.write(msg, (error?: unknown) => {
            if (error) rej(error)
            else res()
          })
        }),
    )
  }
}
