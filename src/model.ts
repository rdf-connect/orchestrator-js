import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { Term } from '@rdfjs/types'
import { NamedNode, Parser } from 'n3'
import { BasicLens, Cont, extractShapes } from 'rdf-lens'
import { CommandRunner, Runner, RunnerConfig, TestRunner } from './runner'

export type Pipeline = {
    id: Term
    parts: Part[]
}

export type Part = {
    runner: Runner
    processors: SmallProc[]
}
export type SmallProc = {
    type: Term
    id: Term
}

export const emptyPipeline: Pipeline = {
    id: new NamedNode(''),
    parts: [],
}

export type URI = string

const processor = $INLINE_FILE('./model.ttl')
export const modelQuads = new Parser().parse(processor)
export const modelShapes = extractShapes(modelQuads, {
    'https://w3id.org/rdf-connect/ontology#Runner': (
        inp: RunnerConfig & { command: string },
    ) => new CommandRunner(inp),
    'https://w3id.org/rdf-connect/ontology#TestRunner': (inp: RunnerConfig) =>
        new TestRunner(inp),
})

export const PipelineShape = <BasicLens<Cont<Term>, Pipeline>>(
    modelShapes.lenses['Pipeline']
)
