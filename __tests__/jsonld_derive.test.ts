import { describe, expect, test } from 'vitest'
import { Quad, Term } from '@rdfjs/types'
import { Document, parse_processors } from '../lib/index'
import { jsonld_to_quads } from '../lib/util'
import { NamedNode, Parser, Writer } from 'n3'

function parse_quads(inp: string): Quad[] {
    return new Parser().parse(inp)
}

function subjectSet(a: Quad[]): Set<string> {
    return new Set(
        a
            .map((a) => a.subject)
            .filter((a) => a.termType == 'NamedNode')
            .map((a) => a.value),
    )
}

function per_predicate(quads: Quad[]): {
    [pred: string]: { bn: Term[]; others: Term[] }
} {
    const out: {
        [pred: string]: { bn: Term[]; others: Term[] }
    } = {}
    quads.forEach((t) => {
        if (!out[t.predicate.value]) {
            out[t.predicate.value] = { bn: [], others: [] }
        }
        const obj = out[t.predicate.value]

        if (t.object.termType == 'BlankNode') {
            obj.bn.push(t.object)
        } else {
            obj.others.push(t.object)
        }
    })
    return out
}

function check_quads_are_equal(a: Quad[], b: Quad[]) {
    const s1: Set<string> = subjectSet(a)
    const s2: Set<string> = subjectSet(b)

    expect(s1, 'Quads have same namednode subjects').toEqual(s2)

    function equal_for(
        sa: Term,
        sb: Term,
        da: Term[] = [],
        db: Term[] = [],
    ): string | undefined {
        const done_a = da.some((x) => x.equals(sa))
        const done_b = db.some((x) => x.equals(sb))
        if (done_a !== done_b) {
            return `subject loop detected between ${sa} ${done_a} and ${sb} ${done_b}`
        }
        da.push(sa)
        db.push(sb)

        const a_triples = a.filter((x) => x.subject.equals(sa))
        const b_triples = b.filter((x) => x.subject.equals(sb))

        if (a_triples.length !== b_triples.length) {
            return `Found inequal amount of triples for ${sa.value} and ${sb.value} `
        }

        const per_predicate_a = per_predicate(a_triples)
        const per_predicate_b = per_predicate(b_triples)

        for (const pred of Object.keys(per_predicate_a)) {
            const tas = per_predicate_a[pred]
            const tbs = per_predicate_b[pred] || []
            if (tas.bn.length !== tbs.bn.length) {
                return `Expected equal amount of blanknode objects for ${sa.value}/${sb.value} ${pred}`
            }

            if (tas.others.length !== tbs.others.length) {
                return `Expected equal amount of other objects for ${sa.value}/${sb.value} ${pred}`
            }

            // check others
            for (const other of tas.others) {
                if (!tbs.others.some((b) => other.equals(b))) {
                    const expected = {
                        v: other.value,
                        tt: other.termType,
                    }
                    const found = tbs.others.map((x) => ({
                        v: x.value,
                        tt: x.termType,
                    }))
                    return `Didn't find correct objects ${JSON.stringify(expected)} in ${JSON.stringify(
                        found,
                    )}`
                }
            }

            // Check blank nodes
            for (const sa of tas.bn) {
                const tries = tbs.bn.map((sb) =>
                    equal_for(sa, sb, da.slice(), db.slice()),
                )

                const found_correct =
                    tries.findIndex((a) => a === undefined) != -1
                if (!found_correct) {
                    return tries.join(' or ')
                }
            }
        }
    }

    for (const subj of s1) {
        const err = equal_for(new NamedNode(subj), new NamedNode(subj))
        if (err !== undefined) {
            console.error(err)
        }
        expect(err, 'Subject equality for ' + subj).toBeUndefined()
    }
}

describe('Extract processor correctly', () => {
    const config = `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix ex: <http://example.org/ns#>.

[] a sh:NodeShape;
  sh:targetClass ex:InvalidProperty;
  sh:property [
    sh:name "cbd";
    sh:path ex:nested;
    sh:maxCount 1;
    sh:minCount 1;
  ].

[] a sh:NodeShape;
  sh:targetClass ex:Invalid;
  sh:property [
    sh:name "cbd";
    sh:path ex:nested;
    sh:class ex:NonExistend;
    sh:maxCount 1;
    sh:minCount 1;
  ].

[] a sh:NodeShape;
  sh:targetClass ex:CBD;
  sh:property [
    sh:name "cbd";
    sh:path ex:nested;
    sh:class rdfl:Path;
    sh:maxCount 1;
    sh:minCount 1;
  ].

[] a sh:NodeShape;
  sh:targetClass ex:Collection;
  sh:property [
    sh:name "numbers";
    sh:path ex:number;
    sh:datatype xsd:integer;
  ].

[] a sh:NodeShape;
  sh:targetClass ex:Nested;
  sh:property [
    sh:name "foobar";
    sh:path ex:number;
    sh:datatype xsd:integer;
    sh:maxCount 1;
    sh:minCount 1;
  ],[
    sh:name "inner";
    sh:path ( );
    sh:class ex:SimpleShape;
    sh:maxCount 1;
    sh:minCount 1;
  ].

[] a sh:NodeShape;
  sh:targetClass ex:SimpleShape;
  sh:property [
    sh:name "number";
    sh:path ex:number;
    sh:datatype xsd:integer;
  ],[
    sh:name "string";
    sh:path ex:string;
    sh:datatype xsd:string;
    sh:maxCount 1;
  ],[
    sh:name "iri";
    sh:path ex:iri;
    sh:datatype xsd:iri;
    sh:maxCount 1;
  ], [
    sh:name "nested";
    sh:path ex:nested;
    sh:class ex:SimpleShape;
    sh:maxCount 1;
  ].
`

    const EX = 'http://example.org/ns#'
    const data = `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix ex: <http://example.org/ns#>.
ex:simple a ex:SimpleShape;
  ex:number 42;
  ex:string "42";
  ex:iri ex:fourtyTwo;
  ex:nested [
    ex:number 43;
    ex:string "43";
    ex:iri ex:fourtyThree;
  ].

ex:collection a ex:Collection;
    ex:number ( 1 2 3 ).
`

    const config_quads = parse_quads(config)
    const processors = parse_processors(config_quads)
    const data_quads = parse_quads(data)

    function getDocument(iri: string, shape: string): Document {
        return processors[shape].addToDocument(
            new NamedNode(iri),
            data_quads,
            processors,
        )
    }

    async function getTurtle(doc: Document) {
        const quads = await jsonld_to_quads(doc)
        return new Writer().quadsToString(quads)
    }

    const simpleShapeContext = {
        number: {
            '@id': 'http://example.org/ns#number',
            '@type': 'http://www.w3.org/2001/XMLSchema#integer',
        },
        string: {
            '@id': 'http://example.org/ns#string',
            '@type': 'http://www.w3.org/2001/XMLSchema#string',
        },
        iri: {
            '@id': 'http://example.org/ns#iri',
            '@type': '@id',
        },
        nested: {
            '@id': 'http://example.org/ns#nested',
        },
    }

    const simpleShape = {
        '@type': 'http://example.org/ns#SimpleShape',
        '@context': simpleShapeContext,
        number: [42],
        string: '42',
        iri: 'http://example.org/ns#fourtyTwo',
        nested: {
            '@id': '_:n3-48',
            '@type': 'http://example.org/ns#SimpleShape',
            '@context': simpleShapeContext,
            string: '43',
            number: [43],
            nested: undefined,
            iri: 'http://example.org/ns#fourtyThree',
        },
    }

    test('can parse simple shapes', async () => {
        const document = getDocument(EX + 'simple', EX + 'SimpleShape')
        expect(document).toEqual(
            Object.assign(
                { '@id': 'http://example.org/ns#simple' },
                simpleShape,
            ),
        )
        const quads_str = await getTurtle(document)
        const eql_to = `<http://example.org/ns#simple> <http://example.org/ns#number> 42 .
<http://example.org/ns#simple> <http://example.org/ns#string> "42" .
<http://example.org/ns#simple> <http://example.org/ns#iri> <http://example.org/ns#fourtyTwo> .
_:n3-43 <http://example.org/ns#number> 43 .
_:n3-43 <http://example.org/ns#string> "43" .
_:n3-43 <http://example.org/ns#iri> <http://example.org/ns#fourtyThree> .
_:n3-43 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns#SimpleShape> .
<http://example.org/ns#simple> <http://example.org/ns#nested> _:n3-43 .
<http://example.org/ns#simple> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns#SimpleShape> .
`
        check_quads_are_equal(
            new Parser().parse(quads_str),
            new Parser().parse(eql_to),
        )
    })

    test('can parse nested shapes', async () => {
        const document = getDocument(EX + 'simple', EX + 'Nested')
        expect(document).toEqual({
            '@id': 'http://example.org/ns#simple',
            '@type': 'http://example.org/ns#Nested',
            '@context': {
                foobar: {
                    '@id': 'http://example.org/ns#number',
                    '@type': 'http://www.w3.org/2001/XMLSchema#integer',
                },
                inner: '@nest',
                '@version': 1.1,
            },
            foobar: 42,
            inner: simpleShape,
        })
        const quads_str = await getTurtle(document)
        const eql_to = `<http://example.org/ns#simple> <http://example.org/ns#number> 42 .
<http://example.org/ns#simple> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns#SimpleShape> .
<http://example.org/ns#simple> <http://example.org/ns#number> 42 .
<http://example.org/ns#simple> <http://example.org/ns#string> "42" .
<http://example.org/ns#simple> <http://example.org/ns#iri> <http://example.org/ns#fourtyTwo> .
_:n3-44 <http://example.org/ns#number> 43 .
_:n3-44 <http://example.org/ns#string> "43" .
_:n3-44 <http://example.org/ns#iri> <http://example.org/ns#fourtyThree> .
_:n3-44 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns#SimpleShape> .
<http://example.org/ns#simple> <http://example.org/ns#nested> _:n3-44 .
<http://example.org/ns#simple> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns#Nested> .
`
        check_quads_are_equal(
            new Parser().parse(quads_str),
            new Parser().parse(eql_to),
        )
    })

    test('can parse cbd shapes', async () => {
        const document = getDocument(EX + 'simple', EX + 'CBD')
        expect(document).toEqual({
            '@id': 'http://example.org/ns#simple',
            '@type': 'http://example.org/ns#CBD',
            '@context': {
                cbd: {
                    '@id': 'http://example.org/ns#nested',
                },
            },
            cbd: {
                '@id': '_:n3-48',
                '@type': 'https://w3id.org/rdf-lens/ontology#Path',
                'http://example.org/ns#number': [
                    {
                        '@type': 'http://www.w3.org/2001/XMLSchema#integer',
                        '@value': '43',
                    },
                ],
                'http://example.org/ns#string': [
                    {
                        '@type': 'http://www.w3.org/2001/XMLSchema#string',
                        '@value': '43',
                    },
                ],
                'http://example.org/ns#iri': [
                    {
                        '@id': 'http://example.org/ns#fourtyThree',
                    },
                ],
            },
        })
        const quads_str = await getTurtle(document)
        const eql_to = `_:n3-44 <http://example.org/ns#number> 43 .
_:n3-44 <http://example.org/ns#string> "43" .
_:n3-44 <http://example.org/ns#iri> <http://example.org/ns#fourtyThree> .
_:n3-44 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/rdf-lens/ontology#Path> .
<http://example.org/ns#simple> <http://example.org/ns#nested> _:n3-44 .
<http://example.org/ns#simple> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns#CBD> .
`
        check_quads_are_equal(
            new Parser().parse(quads_str),
            new Parser().parse(eql_to),
        )
    })

    test('Can parse collection', async () => {
        const document = getDocument(EX + 'collection', EX + 'Collection')
        expect(document).toEqual({
            '@id': 'http://example.org/ns#collection',
            '@type': 'http://example.org/ns#Collection',
            '@context': {
                numbers: {
                    '@id': 'http://example.org/ns#number',
                    '@type': 'http://www.w3.org/2001/XMLSchema#integer',
                },
            },
            numbers: [1, 2, 3],
        })
    })

    test('cannot parse invalid shape', async () => {
        expect(() => getDocument(EX + 'simple', EX + 'Invalid')).toThrow()
    })

    test('cannot parse invalid property', async () => {
        expect(() =>
            getDocument(EX + 'simple', EX + 'InvalidProperty'),
        ).toThrow()
    })
})
