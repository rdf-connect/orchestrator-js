import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { Term } from '@rdfjs/types'
import { NamedNode, Parser } from 'n3'
import { BasicLens, Cont, extractShapes } from 'rdf-lens'
import { CommandRunner, Runner, RunnerConfig, TestRunner } from './runner'

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
export const modelQuads = new Parser().parse(processor)
export const modelShapes = extractShapes(modelQuads, {
  'https://w3id.org/rdf-connect/ontology#CommandRunner': (inp: {
    config: RunnerConfig
    command: string
  }) => new CommandRunner(inp.command, inp.config),
  'https://w3id.org/rdf-connect/ontology#TestRunner': (inp: {
    config: RunnerConfig
  }) => new TestRunner(inp.config),
})

export const PipelineShape = <BasicLens<Cont<Term>, Pipeline>>(
  modelShapes.lenses['Pipeline']
)
