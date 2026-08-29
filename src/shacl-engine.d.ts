/**
 * Minimal declarations for shacl-engine, which ships none.
 * Only the surface used by src/validate.ts is described here.
 */
declare module 'shacl-engine/Validator.js' {
    import type { DatasetCore, DataFactory, Term } from '@rdfjs/types'

    export type ShaclResult = {
        focusNode?: Term
        path?: { predicates?: Term[] }[]
        message: { value: string }[]
        severity?: Term
        shape?: { ptr?: { term?: Term } }
        constraintComponent?: Term
    }

    export type ShaclReport = {
        conforms: boolean
        results: ShaclResult[]
    }

    export default class Validator {
        constructor(shapes: DatasetCore, options: { factory: DataFactory })
        validate(data: { dataset: DatasetCore }): Promise<ShaclReport>
    }
}
