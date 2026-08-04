import { describe, expect, test } from 'vitest'
import { Parser } from 'n3'
import { Quad } from '@rdfjs/types'
import { inferProvenance } from '../lib/provenance.js'

const PROV = 'http://www.w3.org/ns/prov#'
const RDFC = 'https://w3id.org/rdf-connect#'
const EX = 'http://example.org/#'
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
const RDFS_SUBCLASSOF = 'http://www.w3.org/2000/01/rdf-schema#subClassOf'

/**
 * The fragment of the RDF-Connect ontology the provenance rules rely on.
 * Inlined rather than fetched from the namespace IRI so the test stays offline
 * and its expectations remain self-contained.
 */
const ONTOLOGY = `
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix prov: <http://www.w3.org/ns/prov#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.

rdfc:Processor rdfs:subClassOf prov:Activity.
rdfc:Runner rdfs:subClassOf prov:SoftwareAgent.
rdfc:CommandRunner rdfs:subClassOf rdfc:Runner.
rdfc:Pipeline rdfs:subClassOf prov:Plan.
rdfc:ExecutionContext rdfs:subClassOf prov:Association, prov:ActivityInfluence.
rdfc:Reader rdfs:subClassOf prov:Entity.
rdfc:Writer rdfs:subClassOf prov:Entity.

rdfc:implementationOf rdfs:subPropertyOf rdfs:subClassOf.
rdfc:jsImplementationOf rdfs:subPropertyOf rdfc:implementationOf.
rdfc:consistsOf owl:inverseOf prov:hadPlan.
rdfc:instantiates rdfs:subPropertyOf prov:agent.
rdfc:processor rdfs:subPropertyOf prov:activity.
`

/**
 * A minimal two-processor pipeline: <fetch> writes to <channel>, <log> reads
 * from it. Both processor contracts declare their channel parameters with
 * sh:class, which is what the dataflow rules key on.
 */
const PIPELINE = `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix ex: <http://example.org/#>.

rdfc:NodeRunner a rdfc:CommandRunner;
  rdfc:handlesSubjectsOf rdfc:jsImplementationOf;
  rdfc:command "npx js-runner".

ex:Fetch rdfc:jsImplementationOf rdfc:Processor.
[] a sh:NodeShape;
  sh:targetClass ex:Fetch;
  sh:property [
    sh:path rdfc:writer;
    sh:codeIdentifier "writer";
    sh:class rdfc:Writer;
    sh:minCount 1;
    sh:maxCount 1;
  ].

ex:Log rdfc:jsImplementationOf rdfc:Processor.
[] a sh:NodeShape;
  sh:targetClass ex:Log;
  sh:property [
    sh:path rdfc:reader;
    sh:codeIdentifier "reader";
    sh:class rdfc:Reader;
    sh:minCount 1;
    sh:maxCount 1;
  ].

ex:pipeline a rdfc:Pipeline;
  rdfc:consistsOf ex:ctx.

ex:ctx a rdfc:ExecutionContext;
  rdfc:instantiates rdfc:NodeRunner;
  rdfc:processor ex:fetch, ex:log.

ex:channel a rdfc:Reader, rdfc:Writer.

ex:fetch a ex:Fetch;
  rdfc:writer ex:channel.

ex:log a ex:Log;
  rdfc:reader ex:channel.
`

function infer(): Quad[] {
    return inferProvenance(
        new Parser().parse(PIPELINE),
        new Parser().parse(ONTOLOGY),
    )
}

function has(quads: Quad[], s: string, p: string, o: string): boolean {
    return quads.some(
        (q) =>
            q.subject.value === s &&
            q.predicate.value === p &&
            q.object.value === o,
    )
}

describe('inferProvenance', () => {
    const quads = infer()

    describe('RDFS subclass entailment', () => {
        test('types a processor instance as a prov:Activity', () => {
            expect(has(quads, EX + 'fetch', RDF_TYPE, PROV + 'Activity')).toBe(
                true,
            )
            expect(has(quads, EX + 'log', RDF_TYPE, PROV + 'Activity')).toBe(
                true,
            )
        })

        test('types a channel as a prov:Entity and the pipeline as a prov:Plan', () => {
            expect(has(quads, EX + 'channel', RDF_TYPE, PROV + 'Entity')).toBe(
                true,
            )
            expect(has(quads, EX + 'pipeline', RDF_TYPE, PROV + 'Plan')).toBe(
                true,
            )
        })

        test('types a runner as a prov:SoftwareAgent through the subclass chain', () => {
            expect(
                has(
                    quads,
                    RDFC + 'NodeRunner',
                    RDF_TYPE,
                    PROV + 'SoftwareAgent',
                ),
            ).toBe(true)
        })

        test('types an execution context as both influence classes', () => {
            expect(
                has(quads, EX + 'ctx', RDF_TYPE, PROV + 'Association'),
            ).toBe(true)
            expect(
                has(quads, EX + 'ctx', RDF_TYPE, PROV + 'ActivityInfluence'),
            ).toBe(true)
        })
    })

    describe('RDFS subproperty entailment', () => {
        test('cites the runner of an execution context with prov:agent', () => {
            expect(
                has(quads, EX + 'ctx', PROV + 'agent', RDFC + 'NodeRunner'),
            ).toBe(true)
        })

        test('cites the processors of an execution context with prov:activity', () => {
            expect(
                has(quads, EX + 'ctx', PROV + 'activity', EX + 'fetch'),
            ).toBe(true)
            expect(has(quads, EX + 'ctx', PROV + 'activity', EX + 'log')).toBe(
                true,
            )
        })
    })

    describe('OWL inverse entailment', () => {
        // Regression: this rule mentions owl:, a prefix the reasoner does not
        // resolve by default, so it silently stopped firing when the rule file
        // carried no @prefix declarations.
        test('links an execution context to its plan with prov:hadPlan', () => {
            expect(
                has(quads, EX + 'ctx', PROV + 'hadPlan', EX + 'pipeline'),
            ).toBe(true)
        })
    })

    describe('shape-driven dataflow', () => {
        // Regression: these two rules mention rdfc:, sh: and prov:, none of
        // which the reasoner resolves by default. Without them the recorded
        // provenance carried no dataflow at all.
        test('derives prov:generated from a sh:class rdfc:Writer parameter', () => {
            expect(
                has(
                    quads,
                    EX + 'fetch',
                    PROV + 'generated',
                    EX + 'channel',
                ),
            ).toBe(true)
        })

        test('derives prov:used from a sh:class rdfc:Reader parameter', () => {
            expect(
                has(quads, EX + 'log', PROV + 'used', EX + 'channel'),
            ).toBe(true)
        })

        test('does not invert the direction of the dataflow', () => {
            expect(
                has(quads, EX + 'fetch', PROV + 'used', EX + 'channel'),
            ).toBe(false)
            expect(
                has(quads, EX + 'log', PROV + 'generated', EX + 'channel'),
            ).toBe(false)
        })
    })

    describe('result composition', () => {
        test('retains the original pipeline description', () => {
            const pipeline = new Parser().parse(PIPELINE)
            for (const q of pipeline.filter(
                (x) =>
                    x.subject.termType === 'NamedNode' &&
                    x.object.termType === 'NamedNode',
            )) {
                expect(
                    has(
                        quads,
                        q.subject.value,
                        q.predicate.value,
                        q.object.value,
                    ),
                    `missing pipeline quad ${q.subject.value} ${q.predicate.value} ${q.object.value}`,
                ).toBe(true)
            }
        })

        test('does not merge the ontology into the result', () => {
            expect(
                has(
                    quads,
                    RDFC + 'Processor',
                    RDFS_SUBCLASSOF,
                    PROV + 'Activity',
                ),
            ).toBe(false)
        })
    })
})
