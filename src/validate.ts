/**
 * @module validate
 * @description Validates a pipeline description against the SHACL contracts it
 * imports, before any runner is started.
 */

import { Quad, Term } from '@rdfjs/types'
import type { ShaclResult } from 'shacl-engine/Validator.js'
import { DataFactory } from 'rdf-data-factory'
import { Store } from 'n3'
import { getLoggerFor, collapseLast } from './logUtil.js'
import { modelQuads } from './model.js'

const df = new DataFactory()
const logger = getLoggerFor(['validate'])

/**
 * A single constraint violation, reduced to what a pipeline author needs.
 */
export type Violation = {
    focusNode?: string
    path?: string
    message: string
    severity: string
}

function store(quads: Quad[]): Store {
    return new Store(quads)
}

/**
 * The shapes a pipeline is validated against are the contracts it imports, and
 * only those.
 *
 * The orchestrator's own `model.ttl` is deliberately excluded. Its shapes are
 * extraction directives rather than constraints. They use `sh:path ( )` to mean
 * "the focus node itself", which is not a well-formed SHACL path, since a
 * sequence path needs at least two members. They also require values that the
 * orchestrator supplies during extraction rather than the pipeline, such as a
 * runner's `rdfc:Orchestrator`, so a SHACL processor would report every
 * runner in every pipeline as a violation.
 */
function contractQuads(quads: Quad[]): Quad[] {
    const internal = new Set(
        modelQuads.map(
            (q) => `${q.subject.value} ${q.predicate.value} ${q.object.value}`,
        ),
    )
    return quads.filter(
        (q) =>
            !internal.has(
                `${q.subject.value} ${q.predicate.value} ${q.object.value}`,
            ),
    )
}

const SHACL = 'http://www.w3.org/ns/shacl#'
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
const RDFC_PROCESSOR = 'https://w3id.org/rdf-connect#processor'

/**
 * Processors whose type no imported contract targets.
 *
 * A SHACL engine checks a node only against the shapes that target it, so a
 * processor without a contract conforms vacuously. Extraction looks up a
 * processor's contract by its `sh:targetClass`, so the same lookup is done here
 * and every processor it would fail for is reported with the other results.
 */
function uncontracted(quads: Quad[], shapes: Quad[]): Violation[] {
    const targeted = new Set(
        shapes
            .filter((q) => q.predicate.value === SHACL + 'targetClass')
            .map((q) => q.object.value),
    )
    const processors: Term[] = []
    for (const q of quads) {
        if (
            q.predicate.value === RDFC_PROCESSOR &&
            !processors.some((p) => p.equals(q.object))
        ) {
            processors.push(q.object)
        }
    }

    const results: Violation[] = []
    for (const processor of processors) {
        const types = quads
            .filter(
                (q) =>
                    q.subject.equals(processor) &&
                    q.predicate.value === RDF_TYPE,
            )
            .map((q) => q.object.value)
        if (types.some((t) => targeted.has(t))) continue
        results.push({
            focusNode: processor.value,
            message:
                types.length === 0
                    ? 'Processor has no type, so no contract applies to it'
                    : `No contract was imported for ${types.map(collapseLast).join(', ')}. Try importing the processor or check for typos.`,
            severity: 'Violation',
        })
    }
    return results
}

/**
 * Validates `quads` against the SHACL contracts contained in it, and reports
 * every processor whose type no imported contract describes.
 *
 * Call this after environment variables have been substituted and before the
 * pipeline is extracted, so that what is checked is what will be executed.
 * The SHACL engine is loaded on the first call, so a pipeline started without
 * validation does not load it.
 *
 * @param {Quad[]} quads - The fully resolved pipeline description.
 * @returns {Promise<Violation[]>} Every violation found, empty when the pipeline conforms.
 */
export async function validatePipeline(quads: Quad[]): Promise<Violation[]> {
    const { default: Validator } = await import('shacl-engine/Validator.js')
    const shapes = contractQuads(quads)
    const validator = new Validator(store(shapes), { factory: df })
    const report = await validator.validate({ dataset: store(quads) })

    const results = report.results.map((r: ShaclResult) => {
        const severity = r.severity?.value ?? SHACL + 'Violation'

        return {
            focusNode: r.focusNode?.value,
            path: r.path?.[0]?.predicates?.[0]?.value ?? undefined,
            message:
                r.message.map((m) => m.value).join(' ') ||
                'constraint violated',
            severity: severity.split(/[#/]/).pop() ?? 'Violation',
        }
    })
    return [...uncontracted(quads, shapes), ...results]
}

/**
 * Validates the pipeline and reports every result.
 *
 * Violations abort startup. Results of shapes that declare a lower
 * `sh:severity`, such as `sh:Warning` or `sh:Info`, are reported and never fatal.
 *
 * @param {Quad[]} quads - The fully resolved pipeline description.
 * @throws {Error} When at least one violation was found.
 */
export async function assertValid(quads: Quad[]): Promise<void> {
    const results = await validatePipeline(quads)
    if (results.length === 0) {
        logger.debug('Pipeline conforms to the imported contracts')
        return
    }

    for (const v of results) {
        const where = [
            v.focusNode && collapseLast(v.focusNode),
            v.path && collapseLast(v.path),
        ]
            .filter(Boolean)
            .join(' ')
        const line = `${v.message}${where ? ` (${where})` : ''}`
        if (v.severity === 'Violation') {
            logger.error(line)
        } else {
            logger.warn(`${v.severity}: ${line}`)
        }
    }

    const violations = results.filter((v) => v.severity === 'Violation')
    if (violations.length > 0) {
        throw new Error(
            `Pipeline does not conform to the imported contracts: ${violations.length} violation(s)`,
        )
    }
}
