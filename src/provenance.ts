import { RdfJsTerm, reasonStream } from 'eyeling'
import * as RDF from '@rdfjs/types'
import { Quad, Quad_Predicate, Quad_Subject } from '@rdfjs/types'
import { writeFile } from 'node:fs/promises'
import { rdfSerializer } from 'rdf-serialize'
import { streamifyArray } from 'streamify-array'
import stringifyStream from 'stream-to-string'
import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { DataFactory } from 'rdf-data-factory'
import { RdfStore } from 'rdf-stores'
import { getLoggerFor } from './logUtil'

const df: RDF.DataFactory = new DataFactory()
const provenanceRules = $INLINE_FILE('./provenanceRules.n3')
const logger = getLoggerFor(['provenance'])

/**
 * Computes the inferred PROV-O metadata for a pipeline.
 *
 * Reasoning is performed over the combination of the pipeline quads, the
 * RDF-Connect ontology quads and the provenance N3 rules. The newly derived
 * triples are filtered so that ontology-only derivations are dropped while all
 * pipeline-related derivations are kept, and are then merged with the original
 * pipeline quads into a single deduplicated store.
 *
 * The ontology quads and the rules are only used as reasoning input and are
 * never part of the returned result. The derived triples are used solely for
 * the provenance artifact and are not fed back into pipeline execution.
 *
 * @param {Quad[]} pipelineQuads - The quads retrieved from the pipeline file.
 * @param {Quad[]} ontologyQuads - The RDF-Connect ontology quads (reasoning input only).
 * @returns {Quad[]} The deduplicated provenance quads (pipeline + relevant derived).
 */
export function inferProvenance(
    pipelineQuads: Quad[],
    ontologyQuads: Quad[],
): Quad[] {
    // All terms (subjects and objects) that occur in the pipeline. Used to
    // distinguish pipeline-related derivations from ontology-only ones.
    const pipelineTerms = new Set<string>()
    for (const quad of pipelineQuads) {
        pipelineTerms.add(quad.subject.value)
        pipelineTerms.add(quad.object.value)
    }

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
            onDerived: ({ quad }) => {
                if (!quad) {
                    return
                }
                const derived = df.quad(
                    toTerm(quad.subject) as Quad_Subject,
                    toTerm(quad.predicate) as Quad_Predicate,
                    toTerm(quad.object),
                )

                // Keep only derivations anchored to a pipeline entity, dropping
                // triples derived purely from within the ontology.
                if (
                    !pipelineTerms.has(derived.subject.value) &&
                    !pipelineTerms.has(derived.object.value)
                ) {
                    return
                }

                // The store deduplicates against existing pipeline quads.
                store.addQuad(derived)
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

function toTerm(eyeTerm: RdfJsTerm) {
    if (eyeTerm.termType === 'NamedNode') {
        return df.namedNode(eyeTerm.value)
    } else if (eyeTerm.termType === 'BlankNode') {
        return df.blankNode(eyeTerm.value)
    } else if (eyeTerm.termType === 'Literal') {
        return df.literal(eyeTerm.value)
    } else {
        throw new Error('Unknown term type: ' + eyeTerm.termType)
    }
}
