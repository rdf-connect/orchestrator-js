import { describe, expect, test, beforeEach, afterEach } from 'vitest'
import { OrchestratorTestFixture } from './test-utils'
import { TestInstantiator } from '../lib'

describe('Orchestrator Pipeline Management', () => {
    let fixture: OrchestratorTestFixture

    beforeEach(async () => {
        fixture = new OrchestratorTestFixture()
        await fixture.setupPipeline()
    })

    afterEach(async () => {
        // Clean up any resources if needed
        fixture.runnerConnections.clear()
    })

    describe('Pipeline Configuration', () => {
        beforeEach(async () => {
            await fixture.startPipeline()
        })
        test('should parse pipeline with correct number of runners and processors', () => {
            // Verify pipeline structure
            expect(fixture.orchestrator.pipeline.parts).toHaveLength(2)

            // Count total processors across all runners
            const totalProcessors = fixture.orchestrator.pipeline.parts.reduce(
                (total, part) => total + part.processors.length,
                0,
            )

            expect(totalProcessors).toBe(2)
        })
    })

    describe('Runner Communication', () => {
        beforeEach(async () => {
            await fixture.startPipeline()
        })

        test('should start runners and establish connections', async () => {
            const runnerIds = fixture.getRunnerIds()

            // Verify both runners are connected
            expect(runnerIds).toHaveLength(2)
            expect(runnerIds).toContain('http://example.org/runner1')
            expect(runnerIds).toContain('http://example.org/runner2')

            // Verify each runner has a test instantiator
            fixture.orchestrator.pipeline.parts.forEach((part) => {
                expect(part.instantiator).toBeInstanceOf(TestInstantiator)
            })
        })

        test('should route messages between processors correctly', async () => {
            const runner1 = fixture.getRunner('http://example.org/runner1')
            const runner2 = fixture.getRunner('http://example.org/runner2')

            // Start with clean message counts
            const initialRunner1MessageCount = runner1.messages.length
            const initialRunner2MessageCount = runner2.messages.length

            // Send a message to channel c1 (input to processor p1)
            const prom = fixture.sendMessageThroughOrchestrator(
                'http://example.org/c1',
                'Hello world',
                'http://example.org/runner2',
            )

            // Simulate message processing completion
            fixture.simulateMessageProcessed(0, 'http://example.org/c1')

            await prom

            await new Promise((res) => setTimeout(res, 20))

            // Runner1 should receive the original message plus orchestrator messages
            expect(runner1.messages.length).toBe(initialRunner1MessageCount + 1)

            // Runner2 should receive forwarded message plus orchestrator messages
            expect(runner2.messages.length).toBe(initialRunner2MessageCount + 1)
        })
    })

    describe('Message Flow Verification', () => {
        beforeEach(async () => {
            await fixture.startPipeline()
        })

        test('should correctly route messages through the pipeline', async () => {
            const testMessage = 'Test message through pipeline'
            const testChannel = 'http://example.org/c1'

            // Send message through orchestrator
            await fixture.sendMessageThroughOrchestrator(
                testChannel,
                testMessage,
                'http://example.org/runner2',
            )

            // Complete the message processing
            fixture.simulateMessageProcessed(0, testChannel)

            // Verify the message was routed correctly
            const runner1 = fixture.getRunner('http://example.org/runner1')
            const runner2 = fixture.getRunner('http://example.org/runner2')

            // Check that both runners received the expected number of messages
            expect(runner1.messages.length).toBeGreaterThan(0)
            expect(runner2.messages.length).toBeGreaterThan(0)
        })
    })
})
