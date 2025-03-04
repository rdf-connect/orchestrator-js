import { describe, expect, test } from 'vitest'
import { Server } from '../lib/server'
import { Channels, Runner, TestRunner } from '../lib/runner'
import { OrchestratorMessage, RunnerMessage } from '../lib/generated/service'
import {
  createAsyncIterable,
  expandQuads,
  modelShapes,
  Orchestrator,
} from '../lib'
import path from 'path'
import { Parser } from 'n3'
import { empty } from 'rdf-lens'

class TestServer extends Server {
  expectRunner(runner: Runner): Promise<void> {
    console.log('Expecting runner')
    return super.expectRunner(runner)
  }
  connectRunners(): {
    [runnerId: string]: {
      msgs: RunnerMessage[]
      send: (msg: OrchestratorMessage) => void
    }
  } {
    const out = {}
    for (const runner of Object.values(this.runners)) {
      const msgs: RunnerMessage[] = []
      const rs = createAsyncIterable<OrchestratorMessage>()
      const send = (msg: OrchestratorMessage) => {
        console.log('Got msg ', msg)
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

describe.only('Setup orchestrator', async () => {
  const server = new TestServer()
  const orchestrator = new Orchestrator(server)
  modelShapes.lenses['https://w3id.org/rdf-connect/ontology#Orchestrator'] =
    empty().map(() => orchestrator)
  const location = path.resolve('./pipeline.ttl')

  const content = `
@prefix ex: <http://example.org/>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfc: <https://w3id.org/rdf-connect/ontology#>.

<> owl:imports <__tests__/config.ttl>.
<> a rdfc:Pipeline;
  rdfc:processor <p1>, <p2>;
  rdfc:runner ex:runner1, ex:runner2.

<p1> a ex:Proc1;
  rdfc:input "proc1 input";
  rdfc:output "proc1 output".

<p2> a ex:Proc2;
  rdfc:input "proc2 input";
  rdfc:output "proc2 output".
`

  const iri = 'file://' + location
  const quads = await expandQuads(
    iri,
    new Parser({ baseIRI: iri }).parse(content),
  )

  orchestrator.setPipeline(quads, iri)
  test('pipeline parsed', () => {
    expect(orchestrator.pipeline.runners.length, 'found 2 runners').toBe(2)
    expect(orchestrator.pipeline.processors.length, 'found 2 processors').toBe(
      2,
    )
  })

  test('pipeline starts', async () => {
    const prom = orchestrator.startRunners('')
    await new Promise((res) => setTimeout(res, 200))
    const runnerDict = server.connectRunners()

    console.log(Object.keys(runnerDict))
    expect([...Object.keys(runnerDict)]).toEqual([
      'http://example.org/runner1',
      'http://example.org/runner2',
    ])
    await prom

    const startingPromise = orchestrator.startProcessors()

    orchestrator.pipeline.runners.forEach((r) => {
      if (r instanceof TestRunner) {
        r.mockStartProcessor()
      }
    })

    // This promise resolves after the procesors are started
    await startingPromise

    console.log(Object.values(runnerDict).map((x) => x.msgs))

    await orchestrator.pipeline.runners[0].msg({
      data: 'Hello world',
      channel: 'SomeIri',
    })

    console.log(Object.values(runnerDict).map((x) => x.msgs))
  })
})
