import { reasonStream } from 'eyeling'
import * as RDF from '@rdfjs/types'
import { Quad } from '@rdfjs/types'
import { writeFile } from 'node:fs/promises'
import { rdfSerializer } from 'rdf-serialize'
import { streamifyArray } from 'streamify-array'
import stringifyStream from 'stream-to-string'
import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { DataFactory } from 'rdf-data-factory'
import { RdfStore } from 'rdf-stores'
import { getLoggerFor } from './logUtil.js'

const df: RDF.DataFactory = new DataFactory()
const provenanceRules = $INLINE_FILE('./provenanceRules.n3')
const logger = getLoggerFor(['provenance'])

const XSD_DATETIME = 'http://www.w3.org/2001/XMLSchema#dateTime'

/**
 * Creates an `xsd:dateTime` literal for the given date.
 *
 * @param {Date} date - The moment to encode.
 * @returns {RDF.Literal} A literal with the ISO-8601 value and `xsd:dateTime` datatype.
 */
export function dateTimeLiteral(date: Date): RDF.Literal {
    return df.literal(date.toISOString(), df.namedNode(XSD_DATETIME))
}

/**
 * Computes the inferred PROV-O metadata for a pipeline.
 *
 * Reasoning is performed over the combination of the pipeline quads, the
 * RDF-Connect ontology quads and the provenance N3 rules. The newly derived
 * triples are merged with the original pipeline quads into a single deduplicated store.
 *
 * The ontology quads and the rules are only used as reasoning input and are
 * never part of the returned result.
 *
 * @param {Quad[]} pipelineQuads - The quads retrieved from the pipeline file.
 * @param {Quad[]} ontologyQuads - The RDF-Connect ontology quads (reasoning input only).
 * @returns {Quad[]} The deduplicated provenance quads (pipeline + relevant derived).
 */
export function inferProvenance(
    pipelineQuads: Quad[],
    ontologyQuads: Quad[],
): Quad[] {
    const store = RdfStore.createDefault()
    for (const quad of pipelineQuads) {
        store.addQuad(quad)
    }

    reasonStream(
        {
            quads: [...pipelineQuads, ...ontologyQuads],
            n3: provenanceRules,
        },
        {
            rdfjs: true,
            skipUnsupportedRdfJs: true,
            onDerived: ({ quad }) => {
                if (!quad) {
                    return
                }

                // The store deduplicates against existing pipeline quads.
                store.addQuad(quad)
            },
        },
    )

    const provenance = store.getQuads()
    logger.debug(
        `Inferred provenance: ${provenance.length} quads (from ${pipelineQuads.length} pipeline quads)`,
    )
    return provenance
}

/**
 * Serializes the provenance quads and writes them to the given location.
 * The serialization format is derived from the file extension.
 *
 * @param {Quad[]} provenance - The provenance quads to write.
 * @param {string} location - Filesystem path to write the provenance to.
 * @param {Record<string, string>} [prefixes] - Prefixes to use during serialization.
 * @returns {Promise<void>}
 */
export async function writeProvenance(
    provenance: Quad[],
    location: string,
    prefixes?: Record<string, string>,
): Promise<void> {
    const provenanceString = await stringifyStream(
        rdfSerializer.serialize(streamifyArray(provenance.slice()), {
            path: location,
            prefixes,
        }),
    )
    await writeFile(location, provenanceString, { encoding: 'utf8' })
}
