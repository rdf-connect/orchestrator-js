import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { Term } from '@rdfjs/types'
import { Parser } from 'n3'
import { BasicLens, Cont, extractShapes } from 'rdf-lens'
import { CommandRunner, Runner, RunnerConfig } from './runner'
import { Callbacks } from './orchestrator'

export type Pipeline = {
  id: Term
  dependency: string[]
  processors: Processor[]
  runners: Runner[]
}

export type URI = string

export type Processor = {
  id: Term
  type: ProcessorType
}

export type ProcessorType = {
  id: Term
  runner_type: URI
}

export const cbs: Callbacks = {
  msg: async () => {},
  close: async () => {},
}

const processor = $INLINE_FILE('./model.ttl')
export const modelShapes = extractShapes(new Parser().parse(processor), {
  'https://w3id.org/rdf-connect/ontology#CommandRunner': (inp: {
    config: RunnerConfig
    command: string
  }) => new CommandRunner(inp.command, inp.config),
})
export const PipelineShape = <BasicLens<Cont<Term>, Pipeline>>(
  modelShapes.lenses['Pipeline']
)
