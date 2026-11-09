import { describe, expect, test } from 'vitest'
import { NamedNode, Parser } from 'n3'
import { parse_processors } from '../lib/index.js'

const prefixes = `
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix ex: <http://example.org/>.
`

const XSD = 'http://www.w3.org/2001/XMLSchema#'

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

describe('sh:defaultValue', () => {
    const shapes = `
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [
    sh:path ex:level; sh:name "level";
    sh:datatype xsd:string; sh:maxCount 1; sh:defaultValue "info";
  ], [
    sh:path ex:count; sh:name "count";
    sh:datatype xsd:integer; sh:minCount 1; sh:maxCount 1; sh:defaultValue 5;
  ], [
    sh:path ex:tag; sh:name "tag";
    sh:datatype xsd:string; sh:defaultValue ( "a" "b" );
  ], [
    sh:path ex:target; sh:name "target";
    sh:nodeKind sh:IRI; sh:maxCount 1; sh:defaultValue ex:somewhere;
  ], [
    sh:path ex:raw; sh:name "raw";
    sh:datatype xsd:boolean; sh:maxCount 1; sh:defaultValue false;
  ], [
    sh:path ex:ratio; sh:name "ratio";
    sh:datatype xsd:double; sh:maxCount 1; sh:defaultValue 0.5;
  ].
`

    test('a missing value gets the default', () => {
        const document = derive(shapes, 'ex:p a ex:Proc.')
        expect(document['level']).toBe('info')
        expect(document['count']).toBe(5)
        expect(document['tag']).toEqual(['a', 'b'])
        expect(document['target']).toBe('http://example.org/somewhere')
        // A runner reads a JSON boolean, the string "false" would be true
        expect(document['raw']).toBe(false)
        expect(document['ratio']).toBe(0.5)
        expect(document['@context']['level']).toEqual({
            '@id': 'http://example.org/level',
            '@type': XSD + 'string',
        })
    })

    test('a value in the data goes before the default', () => {
        const document = derive(
            shapes,
            `ex:p a ex:Proc; ex:level "debug"; ex:count 7; ex:tag "c";
                ex:target ex:elsewhere; ex:raw true; ex:ratio 1.5e0.`,
        )
        expect(document['level']).toBe('debug')
        expect(document['count']).toBe(7)
        expect(document['tag']).toEqual(['c'])
        expect(document['target']).toBe('http://example.org/elsewhere')
        expect(document['raw']).toBe(true)
        expect(document['ratio']).toBe(1.5)
    })

    test('sh:minCount counts the default', () => {
        expect(() => derive(shapes, 'ex:p a ex:Proc.')).not.toThrow()
    })

    test('sh:minCount still fails without a value or default', () => {
        expect(() =>
            derive(
                `
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [
    sh:path ex:count; sh:name "count";
    sh:datatype xsd:integer; sh:minCount 1; sh:maxCount 1;
  ].
`,
                'ex:p a ex:Proc.',
            ),
        ).toThrow(/Expected 1 or more objects/)
    })
})

describe('sh:node', () => {
    const optionsShape = `
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [
    sh:path ex:options; sh:name "options"; sh:maxCount 1;
    sh:node ex:OptionsShape;
  ].

ex:OptionsShape a sh:NodeShape;
  sh:property [
    sh:path ex:method; sh:name "method";
    sh:datatype xsd:string; sh:maxCount 1; sh:defaultValue "GET";
  ], [
    sh:path ex:retries; sh:name "retries";
    sh:datatype xsd:integer; sh:maxCount 1;
  ].
`

    test('reads a nested object with a node shape without target class', () => {
        const document = derive(
            optionsShape,
            'ex:p a ex:Proc; ex:options [ ex:method "POST"; ex:retries 3 ].',
        )
        expect(document['options']).toMatchObject({
            method: 'POST',
            retries: 3,
        })
        expect(document['options']['@type']).toBeUndefined()
        expect(document['options']['@context']['method']).toEqual({
            '@id': 'http://example.org/method',
            '@type': XSD + 'string',
        })
    })

    test('applies the defaults of the node shape', () => {
        const document = derive(
            optionsShape,
            'ex:p a ex:Proc; ex:options [ ex:retries 3 ].',
        )
        expect(document['options']).toMatchObject({
            method: 'GET',
            retries: 3,
        })
    })

    test('reads an inline node shape', () => {
        const document = derive(
            `
[ ] a sh:NodeShape;
  sh:targetClass ex:Proc;
  sh:property [
    sh:path ex:options; sh:name "options"; sh:maxCount 1;
    sh:node [
      sh:property [
        sh:path ex:method; sh:name "method";
        sh:datatype xsd:string; sh:maxCount 1;
      ];
    ];
  ].
`,
            'ex:p a ex:Proc; ex:options [ ex:method "PUT" ].',
        )
        expect(document['options']).toMatchObject({ method: 'PUT' })
    })

    test('sh:defaultValue [ ] gives the defaults of the node shape', () => {
        const document = derive(
            optionsShape.replace(
                'sh:node ex:OptionsShape;',
                'sh:node ex:OptionsShape; sh:minCount 1; sh:defaultValue [ ];',
            ),
            'ex:p a ex:Proc.',
        )
        expect(document['options']).toMatchObject({ method: 'GET' })
    })

    test('a node shape that is not defined fails', () => {
        expect(() =>
            derive(
                optionsShape.replace(
                    'sh:node ex:OptionsShape;',
                    'sh:node ex:Typo;',
                ),
                'ex:p a ex:Proc; ex:options [ ex:method "POST" ].',
            ),
        ).toThrow(/ex:Typo|http:\/\/example.org\/Typo/)
    })
})
