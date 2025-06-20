import { NamedNode, Quad, Term } from '@rdfjs/types'
import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { BasicLens, Cont, extractShapes, match, subject } from 'rdf-lens'
import { Parser } from 'n3'
import {
    createUriAndTermNamespace,
    Namespace,
    RDF,
    SHACL,
    XSD,
} from '@treecg/types'

export const RDFL = createUriAndTermNamespace(
    'https://w3id.org/rdf-lens/ontology#',
    'CBD',
    'Path',
    'PathLens',
    'Context',
    'TypedExtract',
    'EnvVariable',
    'envKey',
    'envDefault',
    'datatype',
) as Namespace<string[], string, string> & {
    terms: Namespace<string[], NamedNode, string>
}

const processor = $INLINE_FILE('./jsonld.ttl')
const shapes = extractShapes(new Parser().parse(processor))
const processorShape = <BasicLens<Cont<Term>, PlainDefinition>>(
    shapes.lenses['Processor'].map(
        (dto) => new PlainDefinition(<ProcessorDTO>dto),
    )
)

export type PropertyDTO = {
    name: string
    path: {
        id: Term
        quads: Quad[]
    }
    clazz?: Term
    datatype?: Term
    minCount?: number
    maxCount?: number
}
export type ProcessorDTO = {
    target: Term[]
    targetSubjects: Term[]
    properties: PropertyDTO[]
}

export type ContextHeader = {
    '@type'?: string
    '@id'?: string
    '@context'?: Context
    '@version'?: 1.1
}

export type Context = {
    [key: string]: (Context & ContextHeader) | string | number
} & ContextHeader

export type Document = {
    [key: string]: DocumentValue | DocumentValue[]
}
export type DocumentValue = Document | string | number

export abstract class Definition {
    abstract addToContext(context: Context): void
    abstract addToDocument(
        id: Term,
        quads: Quad[],
        others: { [id: string]: Definition },
        isNest?: boolean,
    ): Document

    // Gets the already started document for an identifier if it already exists
    // In JSON-LD, identifiers are not allowed to be repeated with content
    // If no document exists, it is created and inserted into  the cache
    protected getFromCache(id: Term, isNest = false): Document {
        const actualId = id.termType == 'BlankNode' ? '_:' + id.value : id.value
        if (id.termType === 'Literal') {
            return {
                '@value': id.value,
                '@type': id.datatype.value,
            }
        }
        const out: Document = isNest
            ? {}
            : {
                  '@id': actualId,
              }
        return out // }
    }
}

export class CBDDefinition extends Definition {
    private readonly type: string
    constructor(type: string) {
        super()
        this.type = type
    }
    addToContext(): void {}
    addToDocument(
        id: Term,
        quads: Quad[],
        others: { [id: string]: Definition },
        isNest = false,
        deep = false,
    ): Document {
        const out = this.getFromCache(id, isNest)

        for (const t of quads.filter((x) => x.subject.equals(id))) {
            if (!out[t.predicate.value]) {
                out[t.predicate.value] = []
            }

            ;(<Document[]>out[t.predicate.value]).push(
                this.addToDocument(t.object, quads, others, isNest, true),
            )
        }

        if (!deep) {
            out['@type'] = this.type
            // if(!out["@type"]) {
            //   out["@type"] = this.type;
            // }
            // else {
            //   if(Array.isArray(out["@type"])) {
            //     out["@type"].push(this.type);
            //   } else {
            //     out["@type"] = [out["@type"], this.type];
            //   }
            // }
        }

        return out
    }
}

function isNestedProperty(property: PropertyDTO): boolean {
    return property.path.id.equals(RDF.terms.nil)
}

export class PlainDefinition extends Definition implements ProcessorDTO {
    target: Term[]
    targetSubjects: Term[]
    properties: PropertyDTO[]

    constructor(dto: ProcessorDTO) {
        super()
        Object.assign(this, dto)
    }

    addToContext(context: Document) {
        const innerCtx: {
            [id: string]: string | number | { '@id': string; '@type'?: string }
        } = {}
        let needs11 = false

        for (const property of this.properties) {
            if (isNestedProperty(property)) {
                needs11 = true
                innerCtx[property.name] = '@nest'
            } else {
                const obj: { '@id': string; '@type'?: string } = {
                    '@id': property.path.id.value,
                }
                if (property.datatype) {
                    obj['@type'] =
                        property.datatype.value ===
                        'http://www.w3.org/2001/XMLSchema#iri'
                            ? '@id'
                            : property.datatype.value
                }
                innerCtx[property.name] = obj
            }
        }

        if (needs11) {
            innerCtx['@version'] = 1.1
        }

        context['@context'] = innerCtx
    }

    private handleClazzProperty(
        quads: Quad[],
        others: { [id: string]: Definition },
        values: Term[],
        property: PropertyDTO,
        editing: Document,
        id: Term,
    ) {
        const vs = []
        editing[property.name] = []
        for (const v of values) {
            try {
                vs.push(
                    others[property.clazz!.value].addToDocument(
                        v,
                        quads,
                        others,
                        isNestedProperty(property),
                    ),
                )
            } catch (ex) {
                console.error('Failed at property', {
                    clazz: property.clazz!.value,
                    found: Object.keys(others),
                })
                throw ex
            }
        }

        editing[property.name] = handleAccordingToProperty(vs, property, id)
    }

    addToDocument(
        id: Term,
        quads: Quad[],
        others: { [id: string]: Definition },
        isNest: boolean = false,
    ): Document {
        const out = this.getFromCache(id, isNest)
        if (this.target.length == 1) {
            out['@type'] = this.target[0].value
        } else {
            out['@type'] = this.target.map((x) => x.value)
        }
        this.addToContext(out)

        for (const property of this.properties) {
            const values = isNestedProperty(property)
                ? [id]
                : quads
                      .filter(
                          (x) =>
                              x.subject.equals(id) &&
                              x.predicate.equals(property.path.id),
                      )
                      .map((x) => x.object)

            if (property.clazz) {
                this.handleClazzProperty(
                    quads,
                    others,
                    values,
                    property,
                    out,
                    id,
                )
                continue
            }

            if (property.datatype) {
                const items = values.map((v) =>
                    handleAccordingToDatatype(v.value, property.datatype!),
                )

                out[property.name] = handleAccordingToProperty(
                    items,
                    property,
                    id,
                )
                continue
            }

            throw 'No class nor datatype'
        }

        return out
    }
}

function handleAccordingToDatatype(inp: string, datatype: Term) {
    if (datatype.equals(XSD.terms.integer)) return parseInt(inp)
    return inp
}
function handleAccordingToProperty<T>(
    ts: T[],
    property: PropertyDTO,
    id: Term,
): T | T[] {
    if (property.maxCount === undefined || property.maxCount > 1) {
        return ts
    } else {
        if (ts.length > 1) {
            console.error(
                'Expected at most one item ',
                property.name,
                property.path.id.value,
                'for',
                id.value,
            )
        }

        return ts[0]
    }
}

export type Definitions = { [id: string]: Definition }
export function parse_processors(quads: Quad[]): Definitions {
    const dtos = match(undefined, RDF.terms.type, SHACL.terms.NodeShape)
        .thenAll(subject)
        .thenSome(processorShape)
        .map((xs) => {
            const out: { [id: string]: Definition } = {}
            for (const processor of xs) {
                for (const v of processor.target) {
                    out[v.value] = processor
                }

                for (const v of processor.targetSubjects) {
                    out[v.value] = processor
                }
            }
            return out
        })
        .execute(quads)

    dtos[RDFL.CBD] = new CBDDefinition(RDFL.CBD)
    dtos[RDFL.Path] = new CBDDefinition(RDFL.Path)
    dtos[RDFL.TypedExtract] = new CBDDefinition(RDFL.TypedExtract)

    return dtos
}
