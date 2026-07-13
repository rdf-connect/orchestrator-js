import { Quad } from 'n3'
import { Definitions } from '../jsonld.js'
import { SmallProc } from '../model.js'
import { InstantiatorConfig } from './index.js'
import { Instantiator } from './base.js'

/**
 * A test implementation of the Instantiator interface for testing purposes.
 * Simulates runner instantiation without executing actual processes.
 */
export class TestInstantiator extends Instantiator {
    /** List of processor URIs that have been started */
    private startedProcessors: string[] = []

    /**
     * Creates a new TestInstantiator instance.
     * @param {InstantiatorConfig} config - Instantiator configuration
     */
    constructor(config: InstantiatorConfig) {
        super(config)
        this.logger.info('Built test instantiator')
    }

    /**
     * Simulates starting the test runner.
     * @param {string} addr - The address to connect to
     * @returns {Promise<void>}
     */
    async start(addr: string): Promise<void> {
        this.logger.info("Test runner 'starting'", addr)
        this.logger.info('debug msg should follow')
        this.logger.debug('connecting with ' + addr)
    }

    /**
     * Simulates starting all registered processors.
     * Used for testing processor initialization.
     * @returns {Promise<void>}
     */
    async mockStartProcessor(): Promise<void> {
        this.logger.info(
            'Mock start processors ' + JSON.stringify(this.startedProcessors),
        )
        for (const uri of this.startedProcessors) {
            this.logger.info('Start processors ' + uri)
            await this.handleMessage({ initialized: { uri } })
        }
    }

    /**
     * Adds a processor to this test runner and tracks it in the started processors list.
     * Overrides the parent class method to add test-specific behavior.
     *
     * @param {SmallProc} proc - The processor to add
     * @param {Quad[]} quads - RDF quads containing processor configuration
     * @param {Definitions} discoveredShapes - Available shape definitions
     * @param {string} args - serialized JSON-LD object representing the arguments of the processor
     * @returns {Promise<void>} Resolves when the processor is added
     *
     * Process Flow:
     * 1. Tracks the processor ID in startedProcessors for test verification
     * 2. Delegates to parent class implementation for actual processor setup
     * 3. Awaits the completion of processor initialization
     */
    async addProcessor(
        proc: SmallProc,
        quads: Quad[],
        discoveredShapes: Definitions,
        args: string,
    ): Promise<void> {
        this.startedProcessors.push(proc.id.value)
        await super.addProcessor(proc, quads, discoveredShapes, args)
    }
}
