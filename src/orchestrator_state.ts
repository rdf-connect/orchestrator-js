// ─── Channel Routing ────────────────────────────────────────────────────────

import { Instantiator } from './instantiators/base.js'
import { Channels } from './instantiators/index.js'
import { ReceivingStream } from './orchestrator.js'

const decoder = new TextDecoder()
/**
 * Manages channel-to-instantiator routing, message sequence numbers,
 * acknowledgement tracking, and stream connection handshakes.
 */
export class MessageRouter {
    private readonly channels = new Map<string, Instantiator>()
    private readonly closed = new Set<string>()
    private readonly logFns = new Map<string, (msg: string) => void>()
    /**
     * Maps the messageId to resolving promise callback functions.
     * Invoking the callback indicates the message has been handled
     */
    private readonly pendingAcks = new Map<number, () => Promise<void>>()
    /**
     * Maps the messageId to connecting streams promise callbacks.
     * Invoking the callback indicates the receiving stream handler is attached
     */
    private readonly pendingStreams = new Map<
        number,
        (stream: ReceivingStream) => void
    >()

    /** Global message count, runners send message with their localSequenceNumber, which is translated to this globalSequenceNumber */
    private sequenceNumber = 0

    nextSequence(): number {
        return this.sequenceNumber++
    }

    registerChannel(channelId: string, target: Instantiator) {
        this.channels.set(channelId, target)
    }

    registerLogFn(channelId: string, fn: (msg: string) => void) {
        this.logFns.set(channelId, fn)
    }

    getTarget(channelId: string): Instantiator | undefined {
        return this.channels.get(channelId)
    }

    logIfTracked(channelId: string, data: Uint8Array) {
        this.logFns.get(channelId)?.(decoder.decode(data))
    }

    trackAck(seqNum: number, onEnd: () => Promise<void>) {
        this.pendingAcks.set(seqNum, onEnd)
    }

    resolveAck(seqNum: number): (() => Promise<void>) | undefined {
        const cb = this.pendingAcks.get(seqNum)
        if (cb) this.pendingAcks.delete(seqNum)
        return cb
    }

    markClosed(channelId: string) {
        this.closed.add(channelId)
    }

    openChannelIds(): string[] {
        return [...this.channels.keys()].filter((ch) => !this.closed.has(ch))
    }

    get totalChannelCount(): number {
        return this.channels.size
    }

    awaitStream(seqNum: number): Promise<ReceivingStream> {
        return new Promise((resolve) => {
            this.pendingStreams.set(seqNum, resolve)
        })
    }

    connectStream(seqNum: number, stream: ReceivingStream): boolean {
        const resolve = this.pendingStreams.get(seqNum)
        if (!resolve) return false
        this.pendingStreams.delete(seqNum)
        resolve(stream)
        return true
    }
}

// ─── Runner Registry ────────────────────────────────────────────────────────

/**
 * Tracks runner registration, pending connection handshakes,
 * and open channel promises.
 */
export class RunnerRegistry {
    /** Maps runner URIs to their instantiator instances and promise resolution callbacks */
    private readonly runners = new Map<string, Instantiator>()
    /**
     * Maps the runner id to promise callbacks.
     * Invoking the callback indicates the runner is attached
     */
    private readonly pending = new Map<string, (channels: Channels) => void>()
    /** Collection of all open channel connections from runners */
    private readonly channelPromises: Promise<unknown>[] = []

    register(instantiator: Instantiator) {
        this.runners.set(instantiator.id.value, instantiator)
    }

    get(uri: string): Instantiator | undefined {
        return this.runners.get(uri)
    }

    get registeredIds(): string[] {
        return [...this.runners.keys()]
    }

    /**
     * Creates a promise that resolves when the specified runner connects.
     * Used to wait for runner initialization before proceeding with pipeline setup.
     */
    awaitConnection(instantiator: Instantiator): Promise<void> {
        return new Promise((resolve) => {
            this.pending.set(instantiator.id.value, (channels) => {
                this.channelPromises.push(instantiator.setChannel(channels))
                resolve()
            })
        })
    }

    /** Completes a pending runner connection. Returns false if unexpected. */
    connect(uri: string, channels: Channels): boolean {
        const cb = this.pending.get(uri)
        if (!cb) return false
        cb(channels)
        this.pending.delete(uri)
        return true
    }

    async waitAllClosed(): Promise<void> {
        await Promise.all(this.channelPromises)
    }
}
