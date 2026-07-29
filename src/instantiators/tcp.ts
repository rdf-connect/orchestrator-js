import { ConnectionInjector } from '@grpc/grpc-js'
import { createConnection, type Socket } from 'net'

import { Instantiator } from './base.js'
import { InstantiatorConfig } from './index.js'

const CONNECT_TIMEOUT_MS = 10_000

/**
 * Splits a `host:port` address into its parts. Supports bracketed IPv6
 * literals (e.g. `[::1]:4000`) by splitting on the last colon rather than
 * the first.
 */
function parseHostPort(address: string): { host: string; port: number } {
    const match = /^(?:\[(?<v6>[^\]]+)\]|(?<v4>[^:]+)):(?<port>\d+)$/.exec(
        address,
    )
    const host = match?.groups?.v6 ?? match?.groups?.v4
    const port = Number(match?.groups?.port)

    if (!host || !Number.isInteger(port) || port <= 0 || port >= 65536) {
        throw new Error(
            `Invalid rdfc:grpc address '${address}', expected 'host:port' (use '[::1]:port' for IPv6 literals)`,
        )
    }

    return { host, port }
}

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
        this.injector = config.injector
        this.logger.debug('Built an TCP runner!')
    }

    async start(): Promise<void> {
        const { host, port } = parseHostPort(this.grpc)

        this.logger.info(
            `Opening TCP connection to ${host}:${port} for runner ${this.id.value}`,
        )

        const socket = await new Promise<Socket>((resolve, reject) => {
            const s = createConnection({ port, host })

            const cleanup = () => {
                s.removeListener('timeout', onTimeout)
                s.removeListener('connect', onConnect)
                s.removeListener('error', onError)
            }
            const onTimeout = () => {
                cleanup()
                s.destroy()
                reject(
                    new Error(
                        `Timed out connecting to ${host}:${port} after ${CONNECT_TIMEOUT_MS}ms`,
                    ),
                )
            }
            const onConnect = () => {
                cleanup()
                s.setTimeout(0)
                resolve(s)
            }
            const onError = (err: Error) => {
                cleanup()
                s.destroy()
                reject(err)
            }

            s.setTimeout(CONNECT_TIMEOUT_MS, onTimeout)
            s.once('connect', onConnect)
            s.once('error', onError)
        })

        // The setup-time listeners above are torn down once the connection
        // settles; attach a persistent one so errors after handoff to gRPC
        // are logged instead of silently vanishing (and so the socket is
        // never left without an 'error' listener, which Node treats as fatal).
        socket.on('error', (err) => {
            this.logger.debug(
                `TCP socket for runner ${this.id.value} errored after handoff: ${err.name} ${err.message}`,
            )
        })

        // Send the runner URI so the remote side knows which pipeline this is for
        socket.write(this.id.value + '\n')

        // Hand the socket to the gRPC server — it will treat it as an incoming connection
        this.injector.injectConnection(socket)
    }
}
