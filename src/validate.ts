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
 * extraction directives rather than constraints: they use `sh:path ( )` to mean
 * "the focus node itself" and datatypes such as `xsd:iri` and `xsd:any` that do
 * not exist, so a conforming SHACL processor would report violations for every
 * pipeline ever written. Those shapes are interpreted by the extraction step
 * (Section: SHACL as configuration schema); they are not claims about the data.
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
const REIFIES = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies'

/**
 * The parameter a core constraint component is declared with, derived from the
 * component's own name: sh:DatatypeConstraintComponent is parameterised by
 * sh:datatype, sh:MinCountConstraintComponent by sh:minCount, and so on.
 */
function parameterOf(constraintComponent: string): string | undefined {
    if (!constraintComponent.startsWith(SHACL)) return undefined
    const local = constraintComponent.slice(SHACL.length)
    if (!local.endsWith('ConstraintComponent')) return undefined
    const name = local.slice(0, -'ConstraintComponent'.length)
    return name ? SHACL + name[0].toLowerCase() + name.slice(1) : undefined
}

/**
 * Resolves the severity of a single constraint rather than of the shape that
 * carries it.
 *
 * SHACL 1.2 Core allows sh:severity on a reifier for the triple that declares a
 * constraint, so that one parameter of a property shape can be advisory while
 * the rest stay fatal:
 *
 *     sh:datatype xsd:string {| sh:severity sh:Warning |}
 *
 * shacl-engine reads sh:severity from the shape only, so the annotation is
 * resolved here instead. It can be dropped once the engine implements it.
 */
function reifiedSeverity(
    quads: Quad[],
    shape: string | undefined,
    constraintComponent: string | undefined,
): string | undefined {
    if (!shape || !constraintComponent) return undefined
    const parameter = parameterOf(constraintComponent)
    if (!parameter) return undefined

    const reifiers = quads
        .filter(
            (q) =>
                q.predicate.value === REIFIES &&
                q.object.termType === 'Quad' &&
                q.object.subject.value === shape &&
                q.object.predicate.value === parameter,
        )
        .map((q) => q.subject.value)

    for (const reifier of reifiers) {
        const severity = quads.find(
            (q) =>
                q.subject.value === reifier &&
                q.predicate.value === SHACL + 'severity',
        )
        if (severity) return severity.object.value
    }
    return undefined
}

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
        const severity =
            reifiedSeverity(
                quads,
                r.shape?.ptr?.term?.value,
                r.constraintComponent?.value,
            ) ??
            r.severity?.value ??
            SHACL + 'Violation'

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
 * Violations abort startup. A constraint that is advisory rather than a claim
 * about the data, such as one whose `sh:datatype` tells the extraction step how
 * to hand a value over rather than what the term is, should carry
 * `sh:severity sh:Warning`; those results are reported and never fatal.
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
