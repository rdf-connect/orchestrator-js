import { describe, expect, test } from 'vitest'
import { Server } from '../lib/server'
import { Channels } from '../lib/runner'
import { createAsyncIterable, expandQuads } from '../lib/util'
import { OrchestratorMessage, RunnerMessage } from '@rdfc/proto'
import {
    Instantiator,
    modelShapes,
    Orchestrator,
    TestInstantiator,
} from '../lib'
import path from 'path'
import { Parser } from 'n3'
import { Cont, empty } from 'rdf-lens'

const encoder = new TextEncoder()
class TestServer extends Server {
    expectInstantiator(runner: Instantiator): Promise<void> {
        this.logger.info('Expecting runner')
        return super.expectRunner(runner);
    }
    connectRunners(): {
        [runnerId: string]: {
            msgs: RunnerMessage[]
            send: (msg: OrchestratorMessage) => void
        }
    } {
        const out = {}
        for (const runner of Object.values(this.instantiators)) {
            const msgs: RunnerMessage[] = []
            const rs = createAsyncIterable<OrchestratorMessage>()
            const send = (msg: OrchestratorMessage) => {
                this.logger.info('Got msg ', msg)
                rs.push(msg)
            }
            out[runner.part.id.value] = { msgs, send }

            runner.part.setChannel({
                sendMessage: async (msg) => {
                    msgs.push(msg)
                },
                receiveMessage: <Channels['receiveMessage']>rs,
            })
            runner.promise()
        }
        return out
    }
}

describe('Setup orchestrator', async () => {
    const server = new TestServer()
    const orchestrator = new Orchestrator(server)
    modelShapes.lenses['https://w3id.org/rdf-connect#Orchestrator'] =
        empty<Cont>().map(() => orchestrator)
    const location = path.resolve('./pipeline.ttl')

    const content = `
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

    const iri = 'file://' + location
    const quads = await expandQuads(
        iri,
        new Parser({ baseIRI: iri }).parse(content),
    )

    orchestrator.setPipeline(quads, iri)
    test('pipeline parsed', () => {
        expect(orchestrator.pipeline.parts.length, 'found 2 runners').toBe(2)

        const count = orchestrator.pipeline.parts
            .flatMap((p) => p.processors.length)
            .reduce((x, y) => x + y, 0)
        expect(count, 'found 2 processors').toBe(2)
    })

    test('pipeline starts', async () => {
        const prom = orchestrator.startInstantiators('', '')
        await new Promise((res) => setTimeout(res, 200))
        const runnerDict = server.connectRunners()

        expect([...Object.keys(runnerDict)]).toEqual([
            'http://example.org/runner1',
            'http://example.org/runner2',
        ])
        await prom

        const startingPromise = orchestrator.startProcessors()
        orchestrator.pipeline.parts.forEach((r) => {
            console.log("Got runner ", r, r.instantiator instanceof TestInstantiator)
            if (r.instantiator instanceof TestInstantiator) {
                r.instantiator.mockStartProcessor()
            }
        })

        // This promise resolves after the procesors are started
        await startingPromise

        // Try send message directly via orchestrator to <p1> which is part of runner1
        await orchestrator.msg({
            data: encoder.encode('Hello world'),
            channel: 'http://example.org/c1',
        })

        await new Promise((res) => setTimeout(res, 20))
        expect(
            runnerDict['http://example.org/runner1'].msgs.length,
            'this runner received a message',
        ).toBe(4)
        expect(
            runnerDict['http://example.org/runner2'].msgs.length,
            "this runner didnt' received a message",
        ).toBe(3)

        // Try send message directly from <p1> runner1 to <p2> which is part of runner2
        runnerDict['http://example.org/runner1'].send({
            msg: {
                data: encoder.encode('Hello world'),
                channel: 'http://example.org/c2',
            },
        })

        await new Promise((res) => setTimeout(res, 20))
        expect(
            runnerDict['http://example.org/runner1'].msgs.length,
            'this runner received a message',
        ).toBe(4)
        expect(
            runnerDict['http://example.org/runner2'].msgs.length,
            'this runner received a message',
        ).toBe(4)
    })
})
