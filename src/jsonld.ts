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
  target: Term
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
  [key: string]: Document | string | string[] | number | Document[]
}

type Cache = { [id: string]: Document }

export abstract class Definition {
  abstract addToContext(context: Context): void
  abstract addToDocument(
    id: Term,
    quads: Quad[],
    others: { [id: string]: Definition },
    cache: Cache,
  ): Document

  // Gets the already started document for an identifier if it already exists
  // In JSON-LD, identifiers are not allowed to be repeated with content
  // If no document exists, it is created and inserted into  the cache
  protected getFromCache(
    id: Term,
    cache: Cache,
  ): { out: Document; editing: Document } {
    const actualId = id.termType == 'BlankNode' ? '_:' + id.value : id.value
    if (cache[actualId] !== undefined) {
      if (id.termType == 'NamedNode') {
        return {
          out: {
            '@id': {
              '@value': id.value,
              '@type': XSD.custom('iri'),
            },
          },
          editing: cache[actualId],
        }
      } else {
        return {
          out: {
            '@id': actualId,
          },
          editing: cache[actualId],
        }
      }
    } else {
      const out = {
        '@id': actualId,
      }
      cache[actualId] = out
      return {
        editing: out,
        out,
      }
    }
  }
}

export class CBDDefinition extends Definition {
  addToContext(): void {}
  addToDocument(
    id: Term,
    quads: Quad[],
    others: { [id: string]: Definition },
    cache: { [id: string]: Document },
  ): Document {
    const { out, editing } = this.getFromCache(id, cache)

    for (const t of quads.filter((x) => x.subject.equals(id))) {
      if (!out[t.predicate.value]) {
        editing[t.predicate.value] = []
      }

      ;(<Document[]>editing[t.predicate.value]).push(
        this.addToDocument(t.object, quads, others, cache),
      )
    }

    return out
  }
}

export class PlainDefinition extends Definition implements ProcessorDTO {
  target: Term
  properties: PropertyDTO[]

  constructor(dto: ProcessorDTO) {
    super()
    Object.assign(this, dto)
  }

  addToContext(context: Context) {
    const innerCtx: {
      [id: string]: string | number | { '@id': string; '@type'?: string }
    } = {
      '@version': 1.1,
    }
    for (const property of this.properties) {
      if (property.path.id.equals(RDF.terms.nil)) {
        innerCtx[property.name] = '@nest'
      } else {
        const obj: { '@id': string; '@type'?: string } = {
          '@id': property.path.id.value,
        }
        if (property.datatype) {
          obj['@type'] =
            property.datatype.value === 'http://www.w3.org/2001/XMLSchema#iri'
              ? '@id'
              : property.datatype.value
        }
        innerCtx[property.name] = obj
      }
    }

    context[this.target.value] = {
      '@id': this.target.value,
      '@context': innerCtx,
    }
  }

  private handleClazzProperty(
    quads: Quad[],
    others: { [id: string]: Definition },
    cache: { [id: string]: Document },
    values: Term[],
    property: PropertyDTO,
    editing: Document,
  ) {
    const vs = []
    editing[property.name] = []
    for (const v of values) {
      try {
        vs.push(
          others[property.clazz!.value].addToDocument(v, quads, others, cache),
        )
      } catch (ex) {
        console.error('Failed at property', {
          clazz: property.clazz!.value,
        })
        throw ex
      }
    }

    editing[property.name] = vs
  }

  addToDocument(
    id: Term,
    quads: Quad[],
    others: { [id: string]: Definition },
    cache: { [id: string]: Document },
  ): Document {
    const { out, editing } = this.getFromCache(id, cache)
    editing['@type'] = this.target.value

    for (const property of this.properties) {
      const values = property.path.id.equals(RDF.terms.nil)
        ? [id]
        : quads
            .filter(
              (x) =>
                x.subject.equals(id) && x.predicate.equals(property.path.id),
            )
            .map((x) => x.object)

      if (property.clazz) {
        this.handleClazzProperty(
          quads,
          others,
          cache,
          values,
          property,
          editing,
        )
        continue
      }

      if (property.datatype) {
        const items = values.map((v) => v.value)
        // const items =
        //   property.datatype.value === 'http://www.w3.org/2001/XMLSchema#iri'
        //     ? values.map((v) => ({ '@id': v.value }))
        //     : values.map((v) => v.value)
        if (property.maxCount === undefined || property.maxCount > 1) {
          editing[property.name] = items
        } else {
          if (items.length > 1) {
            console.error('Expected at most one item')
          }

          editing[property.name] = items[0]
        }
        continue
      }

      throw 'No class nor datatype'
    }

    return out
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
        out[processor.target.value] = processor
      }
      return out
    })
    .execute(quads)

  dtos[RDFL.CBD] = new CBDDefinition()

  return dtos
}
