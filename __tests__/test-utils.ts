/**
 * Test utilities for orchestrator testing
 */
import { OrchestratorMessage, RunnerMessage } from '@rdfc/proto'
import { createAsyncIterable } from '../lib/util'
import { Server } from '../lib/server'
import { expandQuads, Orchestrator, TestInstantiator } from '../lib'
import { Cont, empty } from 'rdf-lens'
import { modelShapes } from '../lib/model'
import path from 'path'
import { Parser } from 'n3'

/**
 * Test server that provides controlled access to runner communication
 */
export class TestOrchestratorServer extends Server {
    private runnerConnections: Map<string, RunnerTestConnection> = new Map()

    /**
     * Get all connected runners with their message history and send functions
     */
    getRunnerConnections(): Map<string, RunnerTestConnection> {
        return this.runnerConnections
    }

    /**
     * Connect a test runner with controlled messaging
     */
    connectTestRunner(runnerId: string): RunnerTestConnection {
        const messages: RunnerMessage[] = []
        const messageStream = createAsyncIterable<OrchestratorMessage>()

        const connection: RunnerTestConnection = {
            runnerId,
            messages,
            sendMessage: async (msg: RunnerMessage) => {
                messages.push(msg)
            },
            receiveMessages: messageStream,
            close: async () => {
                messageStream.push({}) // Signal end of stream
            },
        }

        this.runnerConnections.set(runnerId, connection)

        // Set up the orchestrator lens for RDF processing
        modelShapes.lenses['https://w3id.org/rdf-connect#Orchestrator'] =
            empty<Cont>().map(() => this.orchestrator)

        // Set up the channel for the runner
        const runner = this.orchestrator.instantiators[runnerId]
        if (runner) {
            runner.part.setChannel({
                sendMessage: {
                    write: connection.sendMessage,
                    close: connection.close,
                },
                receiveMessage: connection.receiveMessages,
            })
            runner.promise()
        }

        return connection
    }
}

/**
 * Represents a connection to a test runner with message history
 */
export interface RunnerTestConnection {
    runnerId: string
    messages: RunnerMessage[]
    sendMessage: (msg: RunnerMessage) => Promise<void>
    receiveMessages: AsyncIterable<OrchestratorMessage>
    close: () => Promise<void>
}

/**
 * Test fixture for creating a complete orchestrator test environment
 */
export class OrchestratorTestFixture {
    public orchestrator: Orchestrator
    public server: TestOrchestratorServer
    public runnerConnections: Map<string, RunnerTestConnection> = new Map()

    constructor() {
        this.orchestrator = new Orchestrator()
        this.server = new TestOrchestratorServer(this.orchestrator)
    }

    /**
     * Set up the test pipeline configuration
     */
    async setupPipeline(): Promise<void> {
        // Set up the orchestrator lens for RDF processing
        modelShapes.lenses['https://w3id.org/rdf-connect#Orchestrator'] =
            empty<Cont>().map(() => this.orchestrator)

        // Define the test pipeline configuration
        const pipelineConfig = `
    @prefix ex: <http://example.org/>.
    @prefix owl: <http://www.w3.org/2002/07/owl#>.
    @prefix rdfc: <https://w3id.org/rdf-connect#>.

    <> owl:imports <__tests__/config.ttl>.
    <> a rdfc:Pipeline;
        rdfc:consistsOf [
            rdfc:instantiates ex:runner1;
            rdfc:processor <p1>;
        ], [
            rdfc:instantiates ex:runner2;
            rdfc:processor <p2>;
        ].

    <p1> a ex:Proc1;
      rdfc:input ex:c1;
      rdfc:output ex:c2 .

    <p2> a ex:Proc2;
      rdfc:input ex:c2;
      rdfc:output ex:c1.
    `

        const location = path.resolve('./pipeline.ttl')

        const iri = 'file://' + location
        const quads = await expandQuads(
            iri,
            new Parser({ baseIRI: iri }).parse(pipelineConfig),
        )

        this.orchestrator.setPipeline(quads, iri)
    }

    /**
     * Start the pipeline and connect all runners
     */
    async startPipeline(): Promise<void> {
        // Start the instantiators (this creates the runner connections)
        const prom = this.orchestrator.startInstantiators('', '')

        for (const part of this.orchestrator.pipeline.parts) {
            this.server.connectTestRunner(part.instantiator.id.value)
        }
        //
        await prom
        // Connect test runners for controlled message handling
        this.runnerConnections = this.server.getRunnerConnections()

        // Start processors using the test instantiators
        const processorStartPromise = this.orchestrator.startProcessors()

        // Trigger mock processor startup for test instantiators
        this.orchestrator.pipeline.parts.forEach((part) => {
            if (part.instantiator instanceof TestInstantiator) {
                part.instantiator.mockStartProcessor()
            }
        })

        await processorStartPromise
    }

    /**
     * Get a specific runner connection by ID
     */
    getRunner(runnerId: string): RunnerTestConnection {
        const connection = this.runnerConnections.get(runnerId)
        if (!connection) {
            throw new Error(`Runner ${runnerId} not found`)
        }
        return connection
    }

    /**
     * Get all runner IDs
     */
    getRunnerIds(): string[] {
        return Array.from(this.runnerConnections.keys())
    }

    /**
     * Send a message through the orchestrator and wait for processing
     */
    async sendMessageThroughOrchestrator(
        channel: string,
        data: string,
        from: string,
        tick: number = 0,
    ): Promise<void> {
        const encoder = new TextEncoder()
        const instantiator = this.orchestrator.pipeline.parts.find(
            (x) => x.instantiator.id.value === from,
        )!.instantiator

        const onEnd = instantiator.onMessageProcessedCb(tick, channel)
        await this.orchestrator.msg(
            {
                data: encoder.encode(data),
                channel,
                tick,
            },
            onEnd,
        )
    }

    /**
     * Simulate message processing completion
     */
    simulateMessageProcessed(tick: number, channel: string): void {
        this.orchestrator.processed({ tick, uri: channel })
    }
}
