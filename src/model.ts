import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { Term } from '@rdfjs/types'
import { NamedNode, Parser } from 'n3'
import { BasicLens, Cont, extractShapes } from 'rdf-lens'
import { CommandRunner, Runner, RunnerConfig } from './runner'

export type Pipeline = {
  id: Term
  dependency: string[]
  processors: Processor[]
  runners: Runner[]
}

export const emptyPipeline: Pipeline = {
  id: new NamedNode(''),
  dependency: [],
  processors: [],
  runners: [],
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
