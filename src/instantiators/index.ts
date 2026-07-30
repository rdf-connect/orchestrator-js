import { FromRunner, ToRunner } from '@rdfc/proto'
import { Orchestrator } from '../orchestrator.js'
import { Term } from '@rdfjs/types'

export type Sender<T> = {
    write: (msg: T) => Promise<unknown>
}

/**
 * Defines the communication channels between runner and orchestrator.
 */
export type Channels = {
    sendMessage: Sender<ToRunner>
    receiveMessage: AsyncIterable<FromRunner>
}

/**
 * Configuration for initializing an Instantiator.
 * @typedef {Object} InstantiatorConfig
 * @property {Term} id - Unique identifier for the instantiator
 * @property {Term} handles - The type of processors this runner can handle
 * @property {Orchestrator} orchestrator - Reference to the parent orchestrator
 */
export type InstantiatorConfig = {
    id: Term
    handles: Term
    orchestrator: Orchestrator
}

export { Instantiator } from './base.js'
export { TcpInstantiator } from './tcp.js'
export { CommandInstantiator } from './command.js'
export { TestInstantiator } from './test.js'
