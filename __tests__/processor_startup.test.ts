import { describe, expect, test, vi } from 'vitest'
import { FromRunner, ToRunner } from '@rdfc/proto'
import { OrchestratorTestFixture } from './test-utils.js'

/**
 * A runner connection whose messages, and whose end, the test decides.
 */
function fakeRunner() {
    const queue: (FromRunner | null)[] = []
    let wake: (() => void) | undefined
    const received: ToRunner[] = []

    async function* stream(): AsyncGenerator<FromRunner> {
        while (true) {
            while (queue.length === 0) {
                await new Promise<void>((res) => (wake = res))
            }
            const next = queue.shift()!
            if (next === null) return
            yield next
        }
    }

    const put = (item: FromRunner | null) => {
        queue.push(item)
        wake?.()
        wake = undefined
    }

    return {
        received,
        channels: {
            sendMessage: {
                write: async (msg: ToRunner) => {
                    received.push(msg)
                },
            },
            receiveMessage: stream(),
        },
        send: (msg: FromRunner) => put(msg),
        end: () => put(null),
    }
}

/**
 * Sets up the test pipeline, two runners with a processor each, and connects
 * a fake runner for each of them.
 */
async function connectedPipeline() {
    const fixture = new OrchestratorTestFixture()
    await fixture.setupPipeline('name')

    const parts = fixture.orchestrator.pipeline.parts
    const runners = parts.map(() => fakeRunner())
    const connected = fixture.orchestrator.startInstantiators('', '')
    parts.forEach((part, i) =>
        fixture.orchestrator.connectRunner(
            part.instantiator.id.value,
            runners[i].channels,
        ),
    )
    await connected

    const processors = parts.map((part) => part.processors[0].id.value)
    return { fixture, runners, processors }
}

/** Waits until the runner was asked to start a processor */
async function processorSent(runner: ReturnType<typeof fakeRunner>) {
    await vi.waitFor(() => {
        expect(runner.received.some((msg) => msg.proc)).toBe(true)
    })
}

/** The errors startProcessors failed with */
async function startFailures(started: Promise<void>): Promise<string[]> {
    try {
        await started
    } catch (ex) {
        expect(ex).toBeInstanceOf(AggregateError)
        return (<AggregateError>ex).errors.map((x: Error) => x.message)
    }
    throw new Error('expected startProcessors to fail')
}

describe('Processor startup failures', () => {
    test('a processor the runner fails to start fails the pipeline', async () => {
        const { fixture, runners, processors } = await connectedPipeline()

        const started = fixture.orchestrator.startProcessors()
        await processorSent(runners[0])
        await processorSent(runners[1])
        runners[0].send({
            initialized: {
                uri: processors[0],
                error: { cause: "Cannot find module './missing.js'" },
            },
        })
        runners[1].send({ initialized: { uri: processors[1] } })

        expect(await startFailures(started)).toEqual([
            `Processor ${processors[0]} failed to start: Cannot find module './missing.js'`,
        ])
    })

    test('a runner that ends while starting a processor fails the pipeline', async () => {
        const { fixture, runners, processors } = await connectedPipeline()

        const started = fixture.orchestrator.startProcessors()
        await processorSent(runners[0])
        await processorSent(runners[1])
        runners[0].end()
        runners[1].send({ initialized: { uri: processors[1] } })

        expect(await startFailures(started)).toEqual([
            `Processor ${processors[0]} did not start: runner http://example.org/runner1 ended before initializing it`,
        ])
    })

    test('a runner that ended before a processor is added fails the pipeline', async () => {
        const { fixture, runners, processors } = await connectedPipeline()

        runners[0].end()
        // Let the orchestrator read the end of the runner's stream
        await new Promise((res) => setTimeout(res, 10))

        const started = fixture.orchestrator.startProcessors()
        await processorSent(runners[1])
        runners[1].send({ initialized: { uri: processors[1] } })

        expect(await startFailures(started)).toEqual([
            `Processor ${processors[0]} did not start: runner http://example.org/runner1 ended before it was added`,
        ])
    })

    test('processors that start do not fail the pipeline', async () => {
        const { fixture, runners, processors } = await connectedPipeline()

        const started = fixture.orchestrator.startProcessors()
        for (const [i, runner] of runners.entries()) {
            await processorSent(runner)
            runner.send({ initialized: { uri: processors[i] } })
        }

        await expect(started).resolves.toBeUndefined()
    })
})
