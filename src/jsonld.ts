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
const SHACL_MEMBER_SHAPE = SHACL.terms.custom('memberShape')

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
 * @property {Term} [memberShape] - Optional node shape the members of each list value must conform to (sh:memberShape)
 * @property {Term} [datatype] - Optional datatype for the property values
 * @property {Term} [nodeKind] - Optional node kind the property values must have
 * @property {Term} [codeType] - Optional type the processor receives (sh:codeType)
 * @property {number} [minCount] - Optional minimum count constraint
 * @property {number} [maxCount] - Optional maximum count constraint
 * @property {Term} [defaultValue] - Optional value used when the data has none (sh:defaultValue)
 * @property {MemberDTO} [member] - The constraints of a member shape without properties of its own
 */
export type PropertyDTO = {
    name: string
    path: {
        id: Term
        quads: Quad[]
    }
    clazz?: Term
    node?: Term
    memberShape?: Term
    datatype?: Term
    nodeKind?: Term
    codeType?: Term
    minCount?: number
    maxCount?: number
    defaultValue?: Term
    member?: MemberDTO
}

/**
 * The constraints of a member shape without properties of its own, such as
 * [ sh:datatype xsd:integer ]: they constrain the members themselves, which
 * are read as the values of a property with those constraints would be.
 */
export type MemberDTO = Pick<
    PropertyDTO,
    'clazz' | 'node' | 'datatype' | 'nodeKind' | 'codeType'
>

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

/**
 * The rdf-lens classes the orchestrator provides definitions for itself. They
 * say what a processor receives rather than what the data is, so contracts
 * give them as sh:codeType, which validation ignores, as well as sh:class.
 */
const BUILT_IN_DEFINITIONS = [
    RDFL.CBD,
    RDFL.Path,
    RDFL.PathLens,
    RDFL.Context,
    RDFL.TypedExtract,
]

function builtInCodeType(property: PropertyDTO): string | undefined {
    const codeType = property.codeType?.value
    return codeType && BUILT_IN_DEFINITIONS.includes(codeType)
        ? codeType
        : undefined
}

/**
 * The definition key of the nested object a property holds: the built-in
 * definition its sh:codeType names, its sh:node, or its sh:class, in that
 * order, like rdf-lens. Undefined when its values are not nested objects.
 */
function nestedKey(property: PropertyDTO): string | undefined {
    const builtIn = builtInCodeType(property)
    if (builtIn) return builtIn
    if (property.codeType) return undefined
    if (property.node) return nodeShapeKey(property.node)
    return property.clazz?.value
}

/**
 * The JSON-LD type of the values of a property in the context: '@id' for
 * IRIs, the code type or datatype for literals, none for nested objects.
 */
function contextType(property: MemberDTO): string | undefined {
    if (builtInCodeType(<PropertyDTO>property)) return undefined
    const type = property.codeType ?? property.datatype
    if (type) {
        return type.value === 'http://www.w3.org/2001/XMLSchema#iri' ||
            type.value === 'http://www.w3.org/2001/XMLSchema#anyURI'
            ? '@id'
            : type.value
    }
    if (property.nodeKind?.value === 'http://www.w3.org/ns/shacl#IRI') {
        return '@id'
    }
    return undefined
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
            [id: string]:
                | string
                | number
                | { '@id': string; '@type'?: string; '@container'?: string }
        } = {}
        let needs11 = false

        for (const property of this.properties) {
            if (isNestedProperty(property)) {
                needs11 = true
                innerCtx[property.name] = '@nest'
            } else {
                const obj: {
                    '@id': string
                    '@type'?: string
                    '@container'?: string
                } = {
                    '@id': property.path.id.value,
                }
                // Each value of a sh:memberShape is an RDF list, whose
                // members have the type of the member shape, if any
                const isList = property.memberShape && !property.codeType
                if (isList && property.maxCount === 1) {
                    obj['@container'] = '@list'
                }
                const type = isList
                    ? property.member && contextType(property.member)
                    : contextType(property)
                if (type) obj['@type'] = type
                innerCtx[property.name] = obj
            }
        }

        if (needs11) {
            innerCtx['@version'] = 1.1
        }

        context['@context'] = innerCtx
    }

    /**
     * What the processor receives for the values of a property: nested
     * objects, or values converted with the code type or datatype.
     */
    private convertValues(
        quads: Quad[],
        others: { [id: string]: Definition },
        values: Term[],
        property: PropertyDTO,
    ): DocumentValue[] {
        // Like rdf-lens, sh:codeType goes before sh:node and sh:class
        // (SHACL 1.2), unless it names a built-in definition
        if (nestedKey(property)) {
            return this.nestedValues(quads, others, values, property)
        }

        if (property.datatype || property.codeType || property.nodeKind) {
            return values.map((v) =>
                handleAccordingToDatatype(
                    v.value,
                    property.codeType ?? property.datatype,
                ),
            )
        }

        throw `No sh:memberShape, sh:node, sh:class, sh:datatype, sh:nodeKind nor sh:codeType for property ${property.name}`
    }

    private nestedValues(
        quads: Quad[],
        others: { [id: string]: Definition },
        values: Term[],
        property: PropertyDTO,
    ): Document[] {
        const key = nestedKey(property)!
        const definition = others[key]
        if (values.length > 0 && !definition) {
            const by = builtInCodeType(property)
                ? 'codeType'
                : property.node
                  ? 'node'
                  : 'clazz'
            console.error('Failed at property', {
                [by]: key,
                found: Object.keys(others),
            })
            throw by === 'codeType'
                ? `No definition found for code type ${key} of property ${property.name} (sh:codeType)`
                : by === 'node'
                  ? `No node shape ${key} found for property ${property.name} (sh:node)`
                  : `No shape found for class ${key} of property ${property.name} (sh:class)`
        }

        return values.map((v) =>
            definition.addToDocument(
                v,
                quads,
                others,
                isNestedProperty(property),
            ),
        )
    }

    /**
     * A property with sh:memberShape: each value is an RDF list, so the value
     * becomes an array of the members, in order, like rdf-lens. Every member
     * is built with that node shape, or, when the member shape has no
     * properties of its own, read with its constraints.
     */
    private handleMemberShapeProperty(
        quads: Quad[],
        others: { [id: string]: Definition },
        property: PropertyDTO,
        editing: Document,
        id: Term,
    ) {
        const key = nodeShapeKey(property.memberShape!)
        const definition = others[key]

        let lists: Term[] = quads
            .filter(
                (q) =>
                    q.subject.equals(id) &&
                    q.predicate.equals(property.path.id),
            )
            .map((q) => q.object)
        if (lists.length === 0 && property.defaultValue) {
            lists = [property.defaultValue]
        }
        if (lists.length > 0 && !definition && !property.member) {
            throw `No node shape ${key} found for property ${property.name} (sh:memberShape)`
        }

        const values = lists.map((list) => {
            let members: Term[]
            try {
                members = findMatchingObjectsFromCollection(quads, list)
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
            } catch (ex: unknown) {
                throw `Expected an RDF list for property ${property.name} on ${collapseLast(id.value)} (sh:memberShape), found ${collapseLast(list.value)}`
            }
            if (property.member) {
                return this.convertValues(quads, others, members, {
                    name: property.name,
                    path: property.path,
                    ...property.member,
                })
            }
            return members.map((member) =>
                definition.addToDocument(member, quads, others),
            )
        })

        editing[property.name] = <DocumentValue | DocumentValue[]>(
            (<unknown>handleAccordingToProperty(values, property, id))
        )
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
            // SHACL 1.2: sh:codeType goes before the nested objects
            if (property.memberShape && !property.codeType) {
                this.handleMemberShapeProperty(quads, others, property, out, id)
                continue
            }

            // TODO: handle collections here as well
            // A path is passed whole: a sequence path is itself an RDF list
            const builtIn = builtInCodeType(property)
            const isPath = builtIn === RDFL.Path || builtIn === RDFL.PathLens
            let values = isNestedProperty(property)
                ? [id]
                : findMatchingObjects(quads, id, property, !isPath)

            // Like rdf-lens, the default is used when the data has no value,
            // and counts for sh:minCount
            if (values.length === 0 && property.defaultValue) {
                values = findDefaultValues(quads, property.defaultValue)
            }

            out[property.name] = handleAccordingToProperty(
                this.convertValues(quads, others, values, property),
                property,
                id,
            )
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
    unrollLists = true,
): Quad_Object[] {
    const objects = quads
        .filter(
            (x) => x.subject.equals(id) && x.predicate.equals(property.path.id),
        )
        .map((x) => x.object)
    if (!unrollLists) return objects
    return objects.flatMap((object) => {
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
 * - Path and PathLens: Handles RDF path expressions
 * - TypedExtract: Extracts typed literals from RDF data
 *
 * Node shapes are found by their target classes and target subjects. Node
 * shapes referred to with sh:node or sh:memberShape, which need no sh:targetClass nor
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
        .filter(
            (q) =>
                q.predicate.equals(SHACL_NODE) ||
                q.predicate.equals(SHACL_MEMBER_SHAPE),
        )
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

    for (const definition of new Set(Object.values(dtos))) {
        for (const property of (<PlainDefinition>definition).properties) {
            if (property.memberShape) {
                property.member = memberConstraints(quads, property.memberShape)
            }
        }
    }

    dtos[RDFL.CBD] = new CBDDefinition(RDFL.CBD)
    dtos[RDFL.Path] = new CBDDefinition(RDFL.Path)
    dtos[RDFL.PathLens] = new CBDDefinition(RDFL.PathLens)
    dtos[RDFL.TypedExtract] = new CBDDefinition(RDFL.TypedExtract)
    dtos[RDFL.Context] = new CBDDefinition(RDFL.CBD)

    return dtos
}

/**
 * The constraints of a member shape without properties of its own, which
 * constrain the members themselves, like rdf-lens. Undefined for a member
 * shape with properties, whose members are nested objects.
 */
function memberConstraints(quads: Quad[], shape: Term): MemberDTO | undefined {
    const object = (predicate: Term) =>
        quads.find(
            (q) => q.subject.equals(shape) && q.predicate.equals(predicate),
        )?.object
    if (object(SHACL.terms.property)) return undefined
    const member: MemberDTO = {
        clazz: object(SHACL.terms.custom('class')),
        node: object(SHACL_NODE),
        datatype: object(SHACL.terms.datatype),
        nodeKind: object(SHACL.terms.nodeKind),
        codeType: object(SHACL.terms.custom('codeType')),
    }
    return Object.values(member).some((x) => x) ? member : undefined
}
