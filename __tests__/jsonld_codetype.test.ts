import { describe, expect, test } from 'vitest'
import { NamedNode, Parser } from 'n3'
import { parse_processors } from '../lib/index.js'

const prefixes = `
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix ex: <http://example.org/>.
`

const XSD = 'http://www.w3.org/2001/XMLSchema#'

// Derives the arguments of ex:p, whose ex:v is extracted with the given
// constraints, the way the orchestrator sends them to a runner
function derive(constraints: string, value: string) {
    const quads = new Parser({ baseIRI: 'file:///pipeline/' }).parse(`
${prefixes}
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [ sh:path ex:v; sh:name "v"; sh:maxCount 1; ${constraints} ].

[ ] a sh:NodeShape;
  sh:targetClass ex:Nested;
  sh:property [ sh:path ex:x; sh:name "x"; sh:maxCount 1; sh:datatype xsd:integer ].

ex:p a ex:Proc; ex:v ${value}.
ex:n1 ex:x "1".
`)
    const definitions = parse_processors(quads)
    const document = definitions['http://example.org/Proc'].addToDocument(
        new NamedNode('http://example.org/p'),
        quads,
        definitions,
    )
    const context = <{ [key: string]: unknown }>document['@context']
    return { value: document['v'], context: context['v'] }
}

describe('Code types', () => {
    test('sh:codeType alone converts the value', () => {
        expect(derive('sh:codeType xsd:integer', '"4"')).toEqual({
            value: 4,
            context: {
                '@id': 'http://example.org/v',
                '@type': XSD + 'integer',
            },
        })
    })

    test('sh:codeType reads an IRI as a string', () => {
        expect(
            derive(
                'sh:nodeKind sh:IRIOrLiteral; sh:codeType xsd:string',
                '<./data.ttl>',
            ),
        ).toEqual({
            value: 'file:///pipeline/data.ttl',
            context: { '@id': 'http://example.org/v', '@type': XSD + 'string' },
        })
    })

    test('sh:codeType goes before sh:class', () => {
        expect(
            derive('sh:class ex:Nested; sh:codeType xsd:anyURI', 'ex:n1'),
        ).toEqual({
            value: 'http://example.org/n1',
            context: { '@id': 'http://example.org/v', '@type': '@id' },
        })
    })

    test('sh:nodeKind sh:IRI alone gives an IRI', () => {
        expect(derive('sh:nodeKind sh:IRI', '<./data.ttl>')).toEqual({
            value: 'file:///pipeline/data.ttl',
            context: { '@id': 'http://example.org/v', '@type': '@id' },
        })
    })
})
