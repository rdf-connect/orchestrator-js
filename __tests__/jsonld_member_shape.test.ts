import { describe, expect, test } from 'vitest'
import { NamedNode, Parser } from 'n3'
import { parse_processors } from '../lib/index.js'

const prefixes = `
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix ex: <http://example.org/>.
`

// Derives the arguments of ex:p from the given shapes and data, the way the
// orchestrator sends them to a runner
function derive(shapes: string, data: string) {
    const quads = new Parser().parse(prefixes + shapes + data)
    const definitions = parse_processors(quads)
    const document = definitions['http://example.org/Proc'].addToDocument(
        new NamedNode('http://example.org/p'),
        quads,
        definitions,
    )
    return JSON.parse(JSON.stringify(document))
}

describe('sh:memberShape', () => {
    const shapes = (constraint = 'sh:maxCount 1;') => `
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [
    sh:path ex:strategy; sh:name "strategies";
    sh:memberShape ex:StrategyShape; ${constraint}
  ].

ex:StrategyShape a sh:NodeShape;
  sh:property [
    sh:path ex:size; sh:name "size"; sh:datatype xsd:integer; sh:maxCount 1;
  ].
`

    test('gives the members of the list, in order', () => {
        const doc = derive(
            shapes(),
            'ex:p a ex:Proc; ex:strategy ( [ ex:size 3 ] [ ex:size 1 ] ).',
        )
        expect(doc.strategies.map((x: { size: number }) => x.size)).toEqual([
            3, 1,
        ])
        expect(doc['@context'].strategies['@container']).toBe('@list')
    })

    test('gives an empty array for the empty list', () => {
        expect(
            derive(shapes(), 'ex:p a ex:Proc; ex:strategy ( ).').strategies,
        ).toEqual([])
    })

    test('falls back to a default list', () => {
        const doc = derive(
            shapes('sh:maxCount 1; sh:defaultValue ( [ ex:size 5 ] );'),
            'ex:p a ex:Proc.',
        )
        expect(doc.strategies.map((x: { size: number }) => x.size)).toEqual([5])
    })

    test('rejects a value that is no list', () => {
        expect(() =>
            derive(shapes(), 'ex:p a ex:Proc; ex:strategy [ ex:size 3 ].'),
        ).toThrow(/Expected an RDF list/)
    })

    test('enforces sh:minCount on the lists', () => {
        expect(() =>
            derive(shapes('sh:minCount 1; sh:maxCount 1;'), 'ex:p a ex:Proc.'),
        ).toThrow(/Expected 1 or more objects/)
    })
})

describe('sh:memberShape without properties constrains the members', () => {
    const shapes = (memberShape: string) => `
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [
    sh:path ex:value; sh:name "values";
    sh:memberShape ${memberShape}; sh:maxCount 1;
  ].
`

    test('sh:datatype converts every member, in order', () => {
        const doc = derive(
            shapes('[ sh:datatype xsd:integer ]'),
            'ex:p a ex:Proc; ex:value ( 3 1 2 ).',
        )
        expect(doc.values).toEqual([3, 1, 2])
        expect(doc['@context'].values).toEqual({
            '@id': 'http://example.org/value',
            '@container': '@list',
            '@type': 'http://www.w3.org/2001/XMLSchema#integer',
        })
    })

    test('sh:codeType converts every member', () => {
        expect(
            derive(
                shapes(
                    '[ sh:nodeKind sh:IRIOrLiteral; sh:codeType xsd:string ]',
                ),
                'ex:p a ex:Proc; ex:value ( ex:a 5 ).',
            ).values,
        ).toEqual(['http://example.org/a', '5'])
    })

    test('sh:nodeKind sh:IRI gives the IRIs, typed as @id', () => {
        const doc = derive(
            shapes('[ sh:nodeKind sh:IRI ]'),
            'ex:p a ex:Proc; ex:value ( ex:a ex:b ).',
        )
        expect(doc.values).toEqual([
            'http://example.org/a',
            'http://example.org/b',
        ])
        expect(doc['@context'].values['@type']).toBe('@id')
    })

    test('sh:node builds every member with that node shape', () => {
        const doc = derive(
            shapes('[ sh:node ex:Size ]') +
                `ex:Size a sh:NodeShape; sh:property [
                   sh:path ex:size; sh:name "size";
                   sh:datatype xsd:integer; sh:maxCount 1 ].`,
            'ex:p a ex:Proc; ex:value ( [ ex:size 3 ] ).',
        )
        expect(doc.values.map((x: { size: number }) => x.size)).toEqual([3])
        expect(doc['@context'].values['@type']).toBeUndefined()
    })
})

describe('Built-in definitions through sh:codeType', () => {
    const shapes = (codeType: string) => `
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [
    sh:path ex:value; sh:name "value"; sh:codeType ${codeType}; sh:maxCount 1;
  ].
`

    test('rdfl:CBD gives the description of the value, not a string', () => {
        const doc = derive(
            shapes('rdfl:CBD'),
            'ex:p a ex:Proc; ex:value ex:v. ex:v ex:name "a".',
        )
        expect(doc.value['@type']).toBe(
            'https://w3id.org/rdf-lens/ontology#CBD',
        )
        expect(doc.value['http://example.org/name']).toEqual([
            {
                '@value': 'a',
                '@type': 'http://www.w3.org/2001/XMLSchema#string',
            },
        ])
        expect(doc['@context'].value['@type']).toBeUndefined()
    })

    test('rdfl:PathLens keeps a sequence path whole', () => {
        const doc = derive(
            shapes('rdfl:PathLens'),
            'ex:p a ex:Proc; ex:value ( ex:a ex:b ).',
        )
        expect(doc.value['@type']).toBe(
            'https://w3id.org/rdf-lens/ontology#PathLens',
        )
        expect(
            doc.value['http://www.w3.org/1999/02/22-rdf-syntax-ns#first'],
        ).toEqual([{ '@id': 'http://example.org/a' }])
    })

    test('a datatype as code type still converts the value', () => {
        expect(
            derive(shapes('xsd:integer'), 'ex:p a ex:Proc; ex:value "4".')
                .value,
        ).toBe(4)
    })
})
