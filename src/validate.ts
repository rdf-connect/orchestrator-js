/**
 * @module validate
 * @description Validates a pipeline description against the SHACL contracts it
 * imports, before any runner is started.
 */

import { Quad } from '@rdfjs/types'
import Validator, { type ShaclResult } from 'shacl-engine/Validator.js'
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

/**
 * Validates `quads` against the SHACL contracts contained in it.
 *
 * Call this after environment variables have been substituted and before the
 * pipeline is extracted, so that what is checked is what will be executed.
 *
 * @param {Quad[]} quads - The fully resolved pipeline description.
 * @returns {Promise<Violation[]>} Every violation found, empty when the pipeline conforms.
 */
export async function validatePipeline(quads: Quad[]): Promise<Violation[]> {
    const shapes = contractQuads(quads)
    const validator = new Validator(store(shapes), { factory: df })
    const report = await validator.validate({ dataset: store(quads) })

    return report.results.map((r: ShaclResult) => {
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
}

/**
 * Validates the pipeline and reports every result.
 *
 * Violations abort startup. Results of shapes that declare a lower
 * `sh:severity`, such as `sh:Warning` or `sh:Info`, are reported and never fatal.
 *
 * @param {Quad[]} quads - The fully resolved pipeline description.
 * @param {boolean} enforce - Whether violations abort the pipeline; on by default.
 * @throws {Error} When at least one violation was found and `enforce` holds.
 */
export async function assertValid(
    quads: Quad[],
    enforce: boolean = true,
): Promise<void> {
    const results = await validatePipeline(quads)
    if (results.length === 0) {
        logger.debug('Pipeline conforms to the imported contracts')
        return
    }

    const violations = results.filter((v) => v.severity === 'Violation')

    for (const v of results) {
        const where = [
            v.focusNode && collapseLast(v.focusNode),
            v.path && collapseLast(v.path),
        ]
            .filter(Boolean)
            .join(' ')
        const line = `${v.message}${where ? ` (${where})` : ''}`
        if (v.severity === 'Violation' && enforce) {
            logger.error(line)
        } else {
            logger.warn(`${v.severity}: ${line}`)
        }
    }

    if (enforce && violations.length > 0) {
        throw new Error(
            `Pipeline does not conform to the imported contracts: ${violations.length} violation(s)`,
        )
    }

    if (violations.length > 0) {
        logger.warn(
            `${violations.length} violation(s) found; validation is not being enforced`,
        )
    }
}
