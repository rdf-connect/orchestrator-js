import { ConnectionInjector } from '@grpc/grpc-js'
import { createConnection, type Socket } from 'net'

import { Instantiator } from './base.js'
import { InstantiatorConfig } from './index.js'

/**
 * An Instantiator implementation that connects to a remote runner server via plain TCP.
 *
 * The orchestrator opens a TCP connection to the runner's gRPC endpoint, sends the runner URI,
 * and the runner reverse-upgrades this TCP socket as the gRPC transport back to the orchestrator.
 * The orchestrator injects its end of the socket into the gRPC server so it is handled as an
 * incoming runner connection.
 */
export class TcpInstantiator extends Instantiator {
    readonly grpc: string
    readonly injector: ConnectionInjector

    constructor(
        config: InstantiatorConfig & {
            grpc: string
            injector: ConnectionInjector
        },
    ) {
        super(config)
        this.grpc = config.grpc
        this.logger.debug('Built an TCP runner!')
    }

    async start(): Promise<void> {
        const [host, portString]: string[] = this.grpc.split(':')
        const port = Number(portString)

        this.logger.info(
            `Opening TCP connection to ${host}:${port} for runner ${this.id.value}`,
        )

        const socket = await new Promise<Socket>((resolve, reject) => {
            const s = createConnection({ port, host })
            s.once('connect', () => resolve(s))
            s.once('error', reject)
        })

        // Send the runner URI so the remote side knows which pipeline this is for
        socket.write(this.id.value + '\n')

        // Hand the socket to the gRPC server — it will treat it as an incoming connection
        this.injector.injectConnection(socket)
    }
}
