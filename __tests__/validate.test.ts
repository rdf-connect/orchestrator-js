import { describe, expect, test } from 'vitest'
import { Parser } from 'n3'
import { assertValid, validatePipeline } from '../lib/validate.js'
import { modelQuads } from '../lib/model.js'

const CONTRACT = `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix ex: <http://example.org/#>.

[] a sh:NodeShape;
  sh:targetClass ex:Fetch;
  sh:property [
    sh:path rdfc:url;
    sh:codeIdentifier "url";
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:path rdfc:writer;
    sh:codeIdentifier "writer";
    sh:class rdfc:Writer;
    sh:minCount 1;
    sh:maxCount 1;
  ].

ex:channel a rdfc:Reader, rdfc:Writer.
`

function pipeline(body: string) {
    // modelQuads accompany every pipeline the orchestrator reads; including them
    // here proves they are excluded from the shapes graph rather than validated.
    return [...modelQuads, ...new Parser().parse(CONTRACT + body)]
}

describe('validatePipeline', () => {
    test('reports nothing for a conforming pipeline', async () => {
        const quads = pipeline(`
            ex:fetch a ex:Fetch;
              rdfc:url "https://example.org/data.json";
              rdfc:writer ex:channel.
        `)
        expect(await validatePipeline(quads)).toEqual([])
    })

    test('reports a missing required parameter', async () => {
        const quads = pipeline(`ex:fetch a ex:Fetch; rdfc:writer ex:channel.`)
        const results = await validatePipeline(quads)
        expect(results.length).toBeGreaterThan(0)
        expect(results.some((r) => r.path?.endsWith('url'))).toBe(true)
    })

    test('reports a parameter of the wrong datatype', async () => {
        const quads = pipeline(`
            ex:fetch a ex:Fetch;
              rdfc:url 42;
              rdfc:writer ex:channel.
        `)
        const results = await validatePipeline(quads)
        expect(results.some((r) => r.path?.endsWith('url'))).toBe(true)
    })

    test('accepts an IRI where the code expects a string', async () => {
        // A file location is written as an IRI but handed to code as a string.
        // sh:nodeKind constrains the data, sh:codeType only tells extraction
        // what to produce, so a conforming engine has nothing to report.
        const quads = [
            ...modelQuads,
            ...new Parser().parse(`
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix ex: <http://example.org/#>.
[] a sh:NodeShape;
  sh:targetClass ex:Located;
  sh:property [
    sh:path rdfc:file;
    sh:nodeKind sh:IRIOrLiteral;
    sh:codeType xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ].
ex:iri a ex:Located; rdfc:file <./somewhere.js>.
ex:literal a ex:Located; rdfc:file "./somewhere.js".
            `),
        ]
        expect(await validatePipeline(quads)).toEqual([])
    })

    test('honours sh:severity, so a warning is not a violation', async () => {
        const quads = [
            ...modelQuads,
            ...new Parser().parse(`
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix ex: <http://example.org/#>.
[] a sh:NodeShape;
  sh:targetClass ex:Loose;
  sh:property [
    sh:path rdfc:file;
    sh:datatype xsd:string;
    sh:severity sh:Warning;
  ].
ex:proc a ex:Loose; rdfc:file <./somewhere.js>.
            `),
        ]
        const results = await validatePipeline(quads)
        expect(results.length).toBe(1)
        expect(results[0].severity).toBe('Warning')
        // Warnings never abort startup, even under enforcement.
        await expect(assertValid(quads)).resolves.toBeUndefined()
    })

    test('does not validate the orchestrator its own extraction shapes', async () => {
        // model.ttl uses sh:path ( ) and requires values the orchestrator
        // supplies during extraction. Passing it alone must stay quiet.
        expect(await validatePipeline([...modelQuads])).toEqual([])
    })
})

describe('processors without a contract', () => {
    test('are reported together with the other results', async () => {
        const quads = pipeline(`
            <> rdfc:consistsOf [ rdfc:processor ex:fetch, ex:unknown ].
            ex:fetch a ex:Fetch; rdfc:writer ex:channel.
            ex:unknown a ex:Unknown.
        `)
        const results = await validatePipeline(quads)
        expect(
            results.find((r) => r.focusNode === 'http://example.org/#unknown'),
        ).toMatchObject({
            message: expect.stringMatching(/No contract was imported for/),
            severity: 'Violation',
        })
        // The missing url of ex:fetch is still reported next to it
        expect(results.some((r) => r.path?.endsWith('url'))).toBe(true)
    })

    test('are not reported when a contract targets their type', async () => {
        const quads = pipeline(`
            <> rdfc:consistsOf [ rdfc:processor ex:fetch ].
            ex:fetch a ex:Fetch;
              rdfc:url "https://example.org/data.json";
              rdfc:writer ex:channel.
        `)
        expect(await validatePipeline(quads)).toEqual([])
    })
})

describe('assertValid', () => {
    const broken = () =>
        pipeline(`ex:fetch a ex:Fetch; rdfc:writer ex:channel.`)

    test('throws by default', async () => {
        await expect(assertValid(broken())).rejects.toThrow(/does not conform/)
    })
})
