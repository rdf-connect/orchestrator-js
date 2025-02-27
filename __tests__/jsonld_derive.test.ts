import { describe, expect, test } from 'vitest'
import { Quad } from '@rdfjs/types'
import { Context, parse_processors } from '../lib/index'
import { jsonld_to_quads } from '../lib/util'
import { NamedNode, Parser, Writer } from 'n3'

function parse_quads(inp: string): Quad[] {
  return new Parser().parse(inp)
}

describe('Extract processor correctly', () => {
  const config = `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix ex: <http://example.org/ns#>.

ex:MyShape a sh:NodeShape;
  sh:targetClass sh:MyTargetClass;
  sh:property [
    sh:name "type";
    sh:path ex:type;
    sh:datatype xsd:iri;
    sh:maxCount 1;
    sh:minCount 1;
  ], [
    sh:name "config";
    sh:path ( );
    sh:class rdfl:CBD;
    sh:maxCount 1;
    sh:minCount 1;
  ].
`
  const config_quads = parse_quads(config)
  const processors = parse_processors(config_quads)
  const processor = processors['http://www.w3.org/ns/shacl#MyTargetClass']

  test('can build context', () => {
    const context: Context = {
      '@version': 1.1,
    }
    processor.addToContext(context)
    expect(context).toEqual({
      '@version': 1.1,
      'http://www.w3.org/ns/shacl#MyTargetClass': {
        '@id': 'http://www.w3.org/ns/shacl#MyTargetClass',
        '@context': {
          '@version': 1.1,
          config: '@nest',
          type: { '@id': 'http://example.org/ns#type', '@type': '@id' },
        },
      },
    })
  })

  const data = `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix ex: <http://example.org/ns#>.
ex:a a sh:MyTargetClass;
  ex:type ex:SomeType;
  ex:b [ ex:c ex:d ].
`

  const data_quads = parse_quads(data)
  const document = processor.addToDocument(
    new NamedNode('http://example.org/ns#a'),
    data_quads,
    processors,
    {},
  )
  test('can transform data', () => {
    expect(document).toEqual({
      '@id': 'http://example.org/ns#a',
      '@type': 'http://www.w3.org/ns/shacl#MyTargetClass',
      type: 'http://example.org/ns#SomeType',
      config: [
        {
          '@id': {
            '@value': 'http://example.org/ns#a',
            '@type': 'http://www.w3.org/2001/XMLSchema#iri',
          },
        },
      ],
      'http://example.org/ns#type': [
        { '@id': 'http://example.org/ns#SomeType' },
      ],
      'http://www.w3.org/1999/02/22-rdf-syntax-ns#type': [
        { '@id': 'http://www.w3.org/ns/shacl#MyTargetClass' },
      ],
      'http://example.org/ns#b': [
        {
          '@id': '_:n3-29',
          'http://example.org/ns#c': [{ '@id': 'http://example.org/ns#d' }],
        },
      ],
    })
  })

  test('Handles blanknodes correctly', async () => {
    const data = `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix ex: <http://example.org/ns#>.
ex:a a sh:MyTargetClass;
  ex:type ex:SomeType;
  ex:b _:b0;
  ex:x _:b0.

_:b0 ex:b [ ex:b ex:c ].
`

    const data_quads = parse_quads(data)
    const document = processor.addToDocument(
      new NamedNode('http://example.org/ns#a'),
      data_quads,
      processors,
      {},
    )

    expect(document).toEqual({
      '@id': 'http://example.org/ns#a',
      '@type': 'http://www.w3.org/ns/shacl#MyTargetClass',
      type: 'http://example.org/ns#SomeType',
      config: [
        {
          '@id': {
            '@value': 'http://example.org/ns#a',
            '@type': 'http://www.w3.org/2001/XMLSchema#iri',
          },
        },
      ],
      'http://example.org/ns#type': [
        { '@id': 'http://example.org/ns#SomeType' },
      ],
      'http://www.w3.org/1999/02/22-rdf-syntax-ns#type': [
        { '@id': 'http://www.w3.org/ns/shacl#MyTargetClass' },
      ],
      'http://example.org/ns#b': [
        {
          '@id': '_:b4_b0',
          'http://example.org/ns#b': [
            {
              '@id': '_:n3-30',
            },
          ],
        },
      ],
      'http://example.org/ns#x': [{ '@id': '_:b4_b0' }],
    })

    const context: Context = {
      '@version': 1.1,
    }
    processor.addToContext(context)
    const quads = await jsonld_to_quads(document, context)
    const quads_str = new Writer().quadsToString(quads)
    expect(quads_str).toEqual(
      `<http://example.org/ns#a> <http://example.org/ns#type> <http://example.org/ns#SomeType> .
<http://example.org/ns#a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#MyTargetClass> .
<http://example.org/ns#a> <http://example.org/ns#type> <http://example.org/ns#SomeType> .
_:b4_b0 <http://example.org/ns#b> _:n3-30 .
<http://example.org/ns#a> <http://example.org/ns#b> _:b4_b0 .
<http://example.org/ns#a> <http://example.org/ns#x> _:b4_b0 .
<http://example.org/ns#a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#MyTargetClass> .
`,
    )
  })

  test('JSON-LD contains correct triples', async () => {
    const context: Context = {
      '@version': 1.1,
    }

    processor.addToContext(context)
    const quads = await jsonld_to_quads(document, context)
    const quads_str = new Writer().quadsToString(quads)
    expect(quads_str).toEqual(
      `<http://example.org/ns#a> <http://example.org/ns#type> <http://example.org/ns#SomeType> .
<http://example.org/ns#a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#MyTargetClass> .
<http://example.org/ns#a> <http://example.org/ns#type> <http://example.org/ns#SomeType> .
_:n3-29 <http://example.org/ns#c> <http://example.org/ns#d> .
<http://example.org/ns#a> <http://example.org/ns#b> _:n3-29 .
<http://example.org/ns#a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#MyTargetClass> .
`,
    )
  })
})
