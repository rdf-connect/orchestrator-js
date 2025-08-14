/**
 * @module jsonld
 * @description Provides utilities for working with JSON-LD and RDF data.
 * Handles conversion between RDF and JSON-LD formats, and provides type definitions
 * for working with RDF data in a more structured way.
 */

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

/**
 * RDFL namespace with common RDF Lens terms.
 * Provides type-safe access to RDF Lens vocabulary terms.
 * @type {Namespace<string[], string, string> & { terms: Namespace<string[], NamedNode, string> }}
 */
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

// Load and parse the JSON-LD processor definition
const processor = $INLINE_FILE('./jsonld.ttl')

// Extract shapes from the RDF definition
const shapes = extractShapes(new Parser().parse(processor))

/**
 * Lens for processing Processor definitions from RDF data.
 * @type {BasicLens<Cont<Term>, PlainDefinition>}
 */
const processorShape = <BasicLens<Cont<Term>, PlainDefinition>>(
    shapes.lenses['Processor'].map(
        (dto) => new PlainDefinition(<ProcessorDTO>dto),
    )
)

/**
 * Data Transfer Object for property definitions.
 * @typedef {Object} PropertyDTO
 * @property {string} name - Name of the property
 * @property {Object} path - Path definition for the property
 * @property {Term} path.id - Term identifying the path
 * @property {Quad[]} path.quads - RDF quads defining the path
 * @property {Term} [clazz] - Optional class constraint for the property
 * @property {Term} [datatype] - Optional datatype for the property values
 * @property {number} [minCount] - Optional minimum count constraint
 * @property {number} [maxCount] - Optional maximum count constraint
 */
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

/**
 * Data Transfer Object for processor definitions.
 * @typedef {Object} ProcessorDTO
 * @property {Term[]} target - Target types for the processor
 * @property {Term[]} targetSubjects - Subject terms that match the target types
 * @property {PropertyDTO[]} properties - Properties defined for the processor
 */
export type ProcessorDTO = {
    target: Term[]
    targetSubjects: Term[]
    properties: PropertyDTO[]
}

/**
 * Represents the header fields in a JSON-LD context.
 * @typedef {Object} ContextHeader
 * @property {string} [@type] - The type of the context
 * @property {string} [@id] - The identifier for the context
 * @property {Context} [@context] - Nested context definition
 * @property {1.1} [@version] - JSON-LD version (1.1)
 */
export type ContextHeader = {
    '@type'?: string
    '@id'?: string
    '@context'?: Context
    '@version'?: 1.1
}

/**
 * Represents a JSON-LD context object that maps terms to IRIs or other contexts.
 * @typedef {Object} Context
 * @property {string} [key: string] - Term to IRI or nested context mapping
 * @extends ContextHeader
 */
export type Context = {
    [key: string]: (Context & ContextHeader) | string | number
} & ContextHeader

/**
 * Represents a JSON-LD document with flexible value types.
 * @typedef {Object} Document
 * @property {DocumentValue | DocumentValue[]} [key: string] - Document properties
 */
export type Document = {
    [key: string]: DocumentValue | DocumentValue[]
}

/**
 * Represents a value in a JSON-LD document.
 * Can be a nested document, string, or number.
 * @typedef {Document | string | number} DocumentValue
 */
export type DocumentValue = Document | string | number

/**
 * Abstract base class for JSON-LD definitions.
 * Provides common functionality for converting RDF quads to JSON-LD documents.
 */
export abstract class Definition {
    /**
     * Adds this definition's context to the provided context object.
     * @abstract
     * @param {Context} context - The context object to extend
     */
    abstract addToContext(context: Context): void

    /**
     * Converts RDF quads to a JSON-LD document.
     * @abstract
     * @param {Term} id - The subject term to process
     * @param {Quad[]} quads - Array of RDF quads to process
     * @param {{ [id: string]: Definition }} others - Other definitions for reference
     * @param {boolean} [isNest] - Whether this is a nested document
     * @returns {Document} The resulting JSON-LD document
     */
    abstract addToDocument(
        id: Term,
        quads: Quad[],
        others: { [id: string]: Definition },
        isNest?: boolean,
    ): Document

    /**
     * Creates a new JSON-LD document structure for the given RDF term.
     * Handles different term types (Literals, URIs, Blank Nodes) appropriately.
     *
     * @protected
     * @param {Term} id - The RDF term to create a document for
     * @param {boolean} [isNest=false] - If true, creates a nested document without @id
     * @returns {Document} A new document with appropriate JSON-LD structure
     */
    protected createDocumentBase(id: Term, isNest = false): Document {
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

/**
 * Implements Concise Bounded Description (CBD) for RDF to JSON-LD conversion.
 * Follows the CBD algorithm to extract a subgraph about a resource.
 */
export class CBDDefinition extends Definition {
    /** The RDF type this definition represents */
    private readonly type: string

    /**
     * Creates a new CBDDefinition instance.
     * @param {string} type - The RDF type this definition represents
     */
    constructor(type: string) {
        super()
        this.type = type
    }
    /**
     * No-op implementation for adding to context (handled by PlainDefinition).
     * @override
     */
    addToContext(): void {}

    /**
     * Converts RDF quads to a JSON-LD document using CBD algorithm.
     * @override
     * @param {Term} id - The subject term to process
     * @param {Quad[]} quads - Array of RDF quads to process
     * @param {{ [id: string]: Definition }} others - Other definitions for reference
     * @param {boolean} [isNest=false] - Whether this is a nested document
     * @param {boolean} [deep=false] - Internal flag for recursion
     * @returns {Document} The resulting JSON-LD document
     */
    addToDocument(
        id: Term,
        quads: Quad[],
        others: { [id: string]: Definition },
        isNest = false,
        deep = false,
    ): Document {
        const out = this.createDocumentBase(id, isNest)

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
        const out = this.createDocumentBase(id, isNest)
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

/**
 * Represents a collection of definitions.
 */
export type Definitions = { [id: string]: Definition }

/**
 * Parses RDF quads into processor definitions.
 * Extracts and processes processor definitions from RDF data.
 *
 * The function automatically includes the following built-in definitions:
 * - CBD (Concise Bounded Description): Extracts a subgraph about a resource
 * - Path: Handles RDF path expressions
 * - TypedExtract: Extracts typed literals from RDF data
 *
 * @param {Quad[]} quads - RDF quads to parse
 * @returns {Definitions} Processed definitions including both parsed and built-in definitions
 * @throws {Error} If no processor definitions are found in the input quads
 */
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
    dtos[RDFL.Context] = new CBDDefinition(RDFL.CBD)

    return dtos
}
