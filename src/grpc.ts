import { spawn } from 'child_process'
import { parse } from 'shell-quote'

export interface RunnerTrait {
  start(addr: string, uri: string): Promise<void>
}

export class CommandRunner implements RunnerTrait {
  private command: string

  constructor(command: string) {
    this.command = command
  }

  async start(addr: string, uri: string) {
    const [cmd, ...args] = parse(this.command) as string[]
    args.push(addr, uri)
    console.log('starting with ', [cmd, ...args])
    const child = spawn(cmd, args)

    child.stdout.on('data', (data) => {
      console.log(`${uri}: ${data}`)
    })

    child.stderr.on('data', (data) => {
      console.error(`${uri}: ${data}`)
    })

    child.on('close', (code) => {
      console.log(`${uri}: exited with code ${code}`)
    })
  }
}
