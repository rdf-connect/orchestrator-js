import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { Term } from '@rdfjs/types'
import { Parser } from 'n3'
import { BasicLens, Cont, extractShapes } from 'rdf-lens'

export type Pipeline = {
  id: Term
  dependency: string[]
  processors: Processor[]
  runners: Runner[]
}

export type URI = string

// export type Runner = {
//   id: URI
//   type: RunnerType
// }

export type Runner = {
  id: Term
  handles: URI[]
  processor_definition: URI
  command: string
}

export type Processor = {
  id: Term
  type: ProcessorType
}

export type ProcessorType = {
  id: Term
  runner_type: URI
}

const processor = $INLINE_FILE('./model.ttl')
export const modelShapes = extractShapes(new Parser().parse(processor))
export const PipelineShape = <BasicLens<Cont<Term>, Pipeline>>(
  modelShapes.lenses['Pipeline']
)
