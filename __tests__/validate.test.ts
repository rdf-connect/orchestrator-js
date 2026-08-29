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

    test('honours sh:severity, so an advisory constraint is not a violation', async () => {
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
        // Advisory results never abort startup, even under enforcement.
        await expect(assertValid(quads, true)).resolves.toBeUndefined()
    })

    test('does not validate the orchestrator its own extraction shapes', async () => {
        // model.ttl uses sh:path ( ) and datatypes such as xsd:iri that no
        // conforming SHACL processor accepts. Passing it alone must stay quiet.
        expect(await validatePipeline([...modelQuads])).toEqual([])
    })
})

describe('assertValid', () => {
    const broken = () =>
        pipeline(`ex:fetch a ex:Fetch; rdfc:writer ex:channel.`)

    test('throws by default', async () => {
        await expect(assertValid(broken())).rejects.toThrow(/does not conform/)
    })

    test('reports without throwing when enforcement is disabled', async () => {
        await expect(assertValid(broken(), false)).resolves.toBeUndefined()
    })
})
