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
// Helper terms of the provenance rules, which are not part of the provenance.
const INTERNAL = 'https://w3id.org/rdf-connect/provenance-rules#'

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
            // Sometimes we have `sh:path ( )` in our input. We need to skip such input as we cannot derive an OWL definition for that.
            skipUnsupportedRdfJs: true,
            onDerived: ({ quad }) => {
                if (!quad || quad.predicate.value.startsWith(INTERNAL)) {
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
 * Serializes quads, in the format derived from the extension of `path`.
 * The quads are copied first: streamifyArray empties the array it is given,
 * and the same provenance quads are serialized more than once.
 *
 * @param {Quad[]} quads - The quads to serialize; left unchanged.
 * @param {string} path - A file name whose extension selects the format.
 * @param {Record<string, string>} [prefixes] - Prefixes to use during serialization.
 * @returns {Promise<string>} The serialized quads.
 */
export async function serializeQuads(
    quads: Quad[],
    path: string,
    prefixes?: Record<string, string>,
): Promise<string> {
    return stringifyStream(
        rdfSerializer.serialize(streamifyArray(quads.slice()), {
            path,
            prefixes,
        }),
    )
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
    const provenanceString = await serializeQuads(
        provenance,
        location,
        prefixes,
    )
    await writeFile(location, provenanceString, { encoding: 'utf8' })
}
