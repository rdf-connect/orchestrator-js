/**
 * @module jsonld
 * @description Provides utilities for working with JSON-LD and RDF data.
 * Handles conversion between RDF and JSON-LD formats, and provides type definitions
 * for working with RDF data in a more structured way.
 */

import { NamedNode, Quad, Quad_Object, Term } from '@rdfjs/types'
import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { BasicLens, Cont, extractShapes } from 'rdf-lens'
import { Parser } from 'n3'
import {
    createUriAndTermNamespace,
    Namespace,
    RDF,
    SHACL,
    XSD,
} from '@treecg/types'
import { collapseLast } from './logUtil.js'

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

const SHACL_NODE = SHACL.terms.custom('node')

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
 * @property {Term} [node] - Optional node shape the property values must conform to (sh:node)
 * @property {Term} [datatype] - Optional datatype for the property values
 * @property {Term} [nodeKind] - Optional node kind the property values must have
 * @property {Term} [codeType] - Optional type the processor receives (sh:codeType)
 * @property {number} [minCount] - Optional minimum count constraint
 * @property {number} [maxCount] - Optional maximum count constraint
 * @property {Term} [defaultValue] - Optional value used when the data has none (sh:defaultValue)
 */
export type PropertyDTO = {
    name: string
    path: {
        id: Term
        quads: Quad[]
    }
    clazz?: Term
    node?: Term
    datatype?: Term
    nodeKind?: Term
    codeType?: Term
    minCount?: number
    maxCount?: number
    defaultValue?: Term
}

/**
 * Data Transfer Object for processor definitions.
 * @typedef {Object} ProcessorDTO
 * @property {Term} id - The node shape itself
 * @property {Term[]} target - Target types for the processor
 * @property {Term[]} targetSubjects - Subject terms that match the target types
 * @property {PropertyDTO[]} properties - Properties defined for the processor
 */
export type ProcessorDTO = {
    id: Term
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
 * Can be a nested document, string, number, or boolean.
 * @typedef {Document | string | number | boolean} DocumentValue
 */
export type DocumentValue = Document | string | number | boolean

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

/**
 * The key under which a node shape referred to with sh:node is found in the
 * definitions: its IRI, or its blank node label prefixed with `_:`.
 */
export function nodeShapeKey(shape: Term): string {
    return shape.termType === 'BlankNode' ? '_:' + shape.value : shape.value
}

export class PlainDefinition extends Definition implements ProcessorDTO {
    id: Term
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
                const type = property.codeType ?? property.datatype
                if (type) {
                    obj['@type'] =
                        type.value === 'http://www.w3.org/2001/XMLSchema#iri' ||
                        type.value === 'http://www.w3.org/2001/XMLSchema#anyURI'
                            ? '@id'
                            : type.value
                } else if (
                    property.nodeKind?.value ===
                    'http://www.w3.org/ns/shacl#IRI'
                ) {
                    obj['@type'] = '@id'
                }
                innerCtx[property.name] = obj
            }
        }

        if (needs11) {
            innerCtx['@version'] = 1.1
        }

        context['@context'] = innerCtx
    }

    private handleNestedProperty(
        quads: Quad[],
        others: { [id: string]: Definition },
        values: Term[],
        property: PropertyDTO,
        editing: Document,
        id: Term,
    ) {
        // sh:node goes before sh:class, like in rdf-lens
        const key = property.node
            ? nodeShapeKey(property.node)
            : property.clazz!.value
        const definition = others[key]
        if (values.length > 0 && !definition) {
            console.error('Failed at property', {
                [property.node ? 'node' : 'clazz']: key,
                found: Object.keys(others),
            })
            throw property.node
                ? `No node shape ${key} found for property ${property.name} (sh:node)`
                : `No shape found for class ${key} of property ${property.name} (sh:class)`
        }

        const vs = []
        editing[property.name] = []
        for (const v of values) {
            vs.push(
                definition.addToDocument(
                    v,
                    quads,
                    others,
                    isNestedProperty(property),
                ),
            )
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
        } else if (this.target.length > 1) {
            out['@type'] = this.target.map((x) => x.value)
        }
        this.addToContext(out)

        for (const property of this.properties) {
            // TODO: handle collections here as well
            let values = isNestedProperty(property)
                ? [id]
                : findMatchingObjects(quads, id, property)

            // Like rdf-lens, the default is used when the data has no value,
            // and counts for sh:minCount
            if (values.length === 0 && property.defaultValue) {
                values = findDefaultValues(quads, property.defaultValue)
            }

            // Like rdf-lens, sh:codeType goes before sh:node and sh:class
            // (SHACL 1.2)
            if ((property.node || property.clazz) && !property.codeType) {
                this.handleNestedProperty(
                    quads,
                    others,
                    values,
                    property,
                    out,
                    id,
                )
                continue
            }

            if (property.datatype || property.codeType || property.nodeKind) {
                const items = values.map((v) =>
                    handleAccordingToDatatype(
                        v.value,
                        property.codeType ?? property.datatype,
                    ),
                )

                out[property.name] = handleAccordingToProperty(
                    items,
                    property,
                    id,
                )
                continue
            }

            throw `No sh:node, sh:class, sh:datatype, sh:nodeKind nor sh:codeType for property ${property.name}`
        }

        return out
    }
}

function findMatchingObjectsFromCollection(
    quads: Quad[],
    id: Term,
): Quad_Object[] {
    const out: Quad_Object[] = []
    while (!id.equals(RDF.terms.nil)) {
        const value = quads.find(
            (q) => q.subject.equals(id) && q.predicate.equals(RDF.terms.first),
        )?.object
        if (value === undefined) {
            throw 'No RDF:first on quad ' + id.value
        }

        const rest = quads.find(
            (q) => q.subject.equals(id) && q.predicate.equals(RDF.terms.rest),
        )?.object
        if (rest === undefined) {
            throw 'No RDF:rest on quad ' + id.value
        }

        out.push(value)
        id = rest
    }
    return out
}

function findMatchingObjects(
    quads: Quad[],
    id: Term,
    property: PropertyDTO,
): Quad_Object[] {
    return quads
        .filter(
            (x) => x.subject.equals(id) && x.predicate.equals(property.path.id),
        )
        .map((x) => x.object)
        .flatMap((object) => {
            try {
                // Check if it is a collection
                return findMatchingObjectsFromCollection(quads, object)
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
            } catch (ex: unknown) {
                // Else it is not
                return [object]
            }
        })
}

/**
 * The values of a sh:defaultValue: the items of an RDF list, or the term
 * itself, like rdf-lens.
 */
function findDefaultValues(quads: Quad[], defaultValue: Term): Term[] {
    try {
        return findMatchingObjectsFromCollection(quads, defaultValue)
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
    } catch (ex: unknown) {
        return [defaultValue]
    }
}

const NUMBER_TYPES = ['float', 'double', 'decimal'].map((x) =>
    XSD.terms.custom(x),
)

/**
 * Converts a value to the JSON value a runner expects for its datatype, like
 * rdf-lens: numbers and booleans become JSON numbers and booleans.
 */
function handleAccordingToDatatype(inp: string, datatype?: Term) {
    if (!datatype) return inp
    if (datatype.equals(XSD.terms.integer)) return parseInt(inp)
    if (NUMBER_TYPES.some((x) => x.equals(datatype))) return +inp
    if (datatype.equals(XSD.terms.custom('boolean'))) return inp === 'true'
    return inp
}
function handleAccordingToProperty<T>(
    ts: T[],
    property: PropertyDTO,
    id: Term,
): T | T[] {
    if (property.minCount !== undefined && property.minCount > ts.length) {
        throw `Expected ${property.minCount} or more objects for property ${collapseLast(property.path.id.value)} (${property.name}) on ${collapseLast(id.value)} (found ${ts.length}) ${JSON.stringify(property)}`
    }
    if (property.maxCount !== undefined && property.maxCount < ts.length) {
        throw `Expected ${property.maxCount} or less objects for property ${collapseLast(property.path.id.value)} (${property.name}) on ${collapseLast(id.value)} (found ${ts.length})`
    }

    if (property.maxCount === undefined || property.maxCount > 1) {
        return ts
    } else {
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
 * Node shapes are found by their target classes and target subjects. Node
 * shapes referred to with sh:node, which need no sh:targetClass nor
 * rdf:type and may be inline blank nodes, are also found by their own key
 * (see {@link nodeShapeKey}).
 *
 * @param {Quad[]} quads - RDF quads to parse
 * @returns {Definitions} Processed definitions including both parsed and built-in definitions
 * @throws {Error} If no processor definitions are found in the input quads
 */
export function parse_processors(quads: Quad[]): Definitions {
    const typed = quads
        .filter(
            (q) =>
                q.predicate.equals(RDF.terms.type) &&
                q.object.equals(SHACL.terms.NodeShape),
        )
        .map((q) => q.subject)
    // A node shape without any triples, a typo for example, is not defined
    const referred = quads
        .filter((q) => q.predicate.equals(SHACL_NODE))
        .map((q) => q.object)
        .filter((shape) => quads.some((q) => q.subject.equals(shape)))

    const shapes: Term[] = []
    for (const shape of [...typed, ...referred]) {
        if (!shapes.some((x) => x.equals(shape))) shapes.push(shape)
    }

    const dtos: Definitions = {}
    for (const shape of shapes) {
        let processor: PlainDefinition
        try {
            processor = processorShape.execute({ id: shape, quads })
        } catch {
            // Like before, a shape that cannot be read is skipped
            continue
        }

        for (const v of processor.target) {
            dtos[v.value] = processor
        }

        for (const v of processor.targetSubjects) {
            dtos[v.value] = processor
        }

        if (referred.some((x) => x.equals(shape))) {
            dtos[nodeShapeKey(shape)] = processor
        }
    }

    dtos[RDFL.CBD] = new CBDDefinition(RDFL.CBD)
    dtos[RDFL.Path] = new CBDDefinition(RDFL.Path)
    dtos[RDFL.TypedExtract] = new CBDDefinition(RDFL.TypedExtract)
    dtos[RDFL.Context] = new CBDDefinition(RDFL.CBD)

    return dtos
}
