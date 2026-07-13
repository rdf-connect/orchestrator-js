import { Instantiator } from './base.js'
import { InstantiatorConfig } from './index.js'
import { spawn } from 'child_process'

/**
 * An Instantiator implementation that starts a runner from an external command.
 * Manages the lifecycle of external runner processes.
 */
export class CommandInstantiator extends Instantiator {
    /** The command that starts this runner */
    private command: string

    /**
     * Creates a new CommandInstantiator instance.
     * @param {InstantiatorConfig & { command: string }} config - Instantiator configuration including the command to execute
     */
    constructor(config: InstantiatorConfig & { command: string }) {
        super(config)
        this.command = config.command
        this.logger.debug('Built a command runner!')
    }

    /**
     * Starts the command runner by executing the configured command.
     * Sets up stdout/stderr handlers and manages the child process.
     *
     * @param {string} addr - The address to connect to
     * @returns {Promise<void>}
     */
    async start(addr: string) {
        const uri = this.id.value
        // const args = parse(this.command) as string[]
        // args.push(addr, uri)

        let args = this.command.slice()
        args += ' ' + addr + ' ' + uri

        this.logger.info('debug msg should follow')
        this.logger.debug(
            'starting with ' + JSON.stringify(['bash', ['-l', '-c', args]]),
        )
        const child = spawn('bash', ['-l', '-c', args])

        child.stdout.on('data', (data) => {
            this.logger.debug(
                'From command ' + (<string>data.toString()).trim(),
            )
        })

        child.stderr.on('data', (data) => {
            this.logger.error((<string>data.toString()).trim())
        })

        child.on('close', (code) => {
            this.logger.info(`exited with code ${code}`)
        })
    }
}
