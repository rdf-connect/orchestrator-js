/**
 * @module model
 * @description Core data models and type definitions for the orchestrator.
 * Defines the pipeline structure, RDF shapes, and related types.
 */

import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { Term } from '@rdfjs/types'
import { NamedNode, Parser } from 'n3'
import { BasicLens, Cont, extractShapes } from 'rdf-lens'
import {
    CommandInstantiator,
    Instantiator,
    InstantiatorConfig,
    TestInstantiator,
} from './instantiator'

/**
 * Represents a complete processing pipeline configuration.
 * @typedef {Object} Pipeline
 * @property {Term} id - Unique identifier for the pipeline
 * @property {Part[]} parts - Array of pipeline parts (instantiators with their processors)
 */
export type Pipeline = {
    id: Term
    parts: Part[]
}

/**
 * Represents a part of the pipeline containing a runner and its processors.
 * @typedef {Object} Part
 * @property {Instantiator} instantiator - The instantiator responsible for starting the runner and executing processors
 * @property {SmallProc[]} processors - Array of processors to be executed by the runner
 */
export type Part = {
    instantiator: Instantiator
    processors: SmallProc[]
}

/**
 * Represents a lightweight processor definition.
 * @typedef {Object} SmallProc
 * @property {Term} type - The type/class of the processor
 * @property {Term} id - Unique identifier for the processor
 */
export type SmallProc = {
    type: Term
    id: Term
}

/**
 * An empty pipeline template with default values.
 * @type {Pipeline}
 */
export const emptyPipeline: Pipeline = {
    id: new NamedNode(''),
    parts: [],
}

/**
 * Represents a Uniform Resource Identifier (URI).
 * @typedef {string} URI
 */
export type URI = string

// Load and parse the RDF model definition
const processor = $INLINE_FILE('./model.ttl')

/**
 * Parsed RDF quads from the model definition.
 * @type {import('n3').Quad[]}
 */
export const modelQuads = new Parser().parse(processor)

/**
 * Extracted shapes from the RDF model with custom constructors for different instnatiator types.
 * @type {Object}
 * @property {Function} 'https://w3id.org/rdf-connect#Runner' - Constructor for CommandRunner
 * @property {Function} 'https://w3id.org/rdf-connect#TestRunner' - Constructor for TestRunner
 */
export const modelShapes = extractShapes(modelQuads, {
    'https://w3id.org/rdf-connect#Runner': (
        inp: InstantiatorConfig & { command: string },
    ) => new CommandInstantiator(inp),
    'https://w3id.org/rdf-connect#TestRunner': (inp: InstantiatorConfig) =>
        new TestInstantiator(inp),
})

/**
 * Lens for validating and transforming RDF data into Pipeline objects.
 * Uses the Pipeline shape defined in the RDF model.
 * @type {BasicLens<Cont<Term>, Pipeline>}
 */
export const PipelineShape = <BasicLens<Cont<Term>, Pipeline>>(
    modelShapes.lenses['Pipeline']
)
