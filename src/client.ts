import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import {
    DataChunk,
    Id,
    LogMessage,
    OrchestratorMessage,
    RunnerClient,
    RunnerMessage,
} from '@rdfc/proto'
import winston from 'winston'
import Transport from 'winston-transport'

class RpcTransport extends Transport {
    private readonly stream: grpc.ClientWritableStream<LogMessage>
    private readonly entities: string[]
    private readonly aliases: string[]
    constructor(opts: {
        stream: grpc.ClientWritableStream<LogMessage>
        entities: string[]
        aliases?: string[]
    }) {
        super({ level: 'debug' })

        this.stream = opts.stream
        this.entities = opts.entities
        this.aliases = opts.aliases || []
    }

    log(info: winston.LogEntry, callback: () => void) {
        this.stream.write(
            {
                msg: info.message,
                level: info.level,
                entities: this.entities,
                aliases: this.aliases,
            },
            callback,
        )
    }
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()
export async function start(addr: string, uri: string) {
    const client = new RunnerClient(addr, grpc.credentials.createInsecure())

    const logger = winston.createLogger({
        transports: [
            new RpcTransport({
                entities: [uri, 'cli'],
                stream: client.logStream(() => {}),
            }),
        ],
    })

    const stream = client.connect()
    logger.info('Connected with server ' + addr)
    const writable = promisify(stream.write.bind(stream))
    await writable({ identify: { uri } })

    for await (const chunk of stream) {
        const msg: RunnerMessage = chunk
        if (msg.proc) {
            const procLogger = winston.createLogger({
                transports: [
                    new RpcTransport({
                        entities: [msg.proc.uri, uri],
                        stream: client.logStream(() => {}),
                    }),
                ],
            })

            const processor = msg.proc
            logger.info('Starting processor ' + processor.uri)
            const args = JSON.parse(processor.arguments)
            procLogger.info('Starting with ' + JSON.stringify(args))

            await writable({ init: { uri: processor.uri } })
        }

        if (msg.streamMsg) {
            logger.debug('Start receive stream message')
            startStreamMessage(msg.streamMsg.id!, client, logger)
        }

        if (msg.start) {
            const uri = 'http://example.org/input'
            const st = 5 + ''
            await writable({ msg: { data: encoder.encode(st), channel: uri } })

            sendStream(2, uri, client, writable, logger)
        }
        if (msg.close) {
            logger.info('Close message')
            break
        }

        if (msg.msg) {
            const int = parseInt(decoder.decode(msg.msg.data))
            logger.debug('Data message to ' + msg.msg.channel, int)
            if (int !== 0) {
                await writable({
                    msg: {
                        data: encoder.encode(int - 1 + ''),
                        channel: msg.msg.channel,
                    },
                })
            }
        }
    }

    stream.cancel()
    stream.end()
}

async function startStreamMessage(
    id: Id,
    client: RunnerClient,
    logger: winston.Logger,
) {
    logger.debug('startStreamMessage', id.id)
    const st = client.receiveStreamMessage(id)
    for await (const c of st) {
        const chunk: DataChunk = c
        logger.debug('Got chunk', id.id, decoder.decode(chunk.data))
    }
}

async function sendStream(
    chunks: number,
    uri: string,
    client: RunnerClient,
    writable: (msg: OrchestratorMessage) => Promise<unknown>,
    logger: winston.Logger,
    timeBetween = 500,
) {
    logger.debug('starting stream ' + chunks + ' chunks to ' + uri)
    const stream = client.sendStreamMessage()
    const id: Id = await new Promise((res) => stream.once('data', res))

    await writable({ streamMsg: { id, channel: uri } })

    const write = promisify(stream.write.bind(stream))

    for (let i = 0; i < chunks; i++) {
        logger.debug('Sending chunk', i)
        await write({ data: encoder.encode('Chunk ' + i) })
        await new Promise((res) => setTimeout(res, timeBetween))
    }

    logger.debug('StreamMsg is done')
    stream.end()

    if (chunks > 0) {
        await sendStream(chunks - 1, uri, client, writable, logger)
    }
}
