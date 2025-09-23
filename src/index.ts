import { grpc, RunnerService } from '@rdfc/proto'
import { getLoggerFor, reevaluteLevels, setPipelineFile } from './logUtil'
import { Orchestrator } from './orchestrator'
import { Server } from './server'
import { pathToFileURL } from 'url'
import { readQuads } from './util'
import { Writer } from 'n3'
import { modelShapes } from './model'
import { Cont, empty } from 'rdf-lens'

export * from './jsonld'
export * from './logUtil'
export * from './model'
export * from './orchestrator'
export * from './instantiator'
export * from './server'
export * from './util'

/**
 * Initializes and starts the orchestrator with the specified pipeline configuration.
 * This is the main entry point for the orchestrator service.
 *
 * @param {string} location - Filesystem path to the pipeline configuration file
 * @returns {Promise<void>}
 *
 * @throws {LensError} If there's an error processing the pipeline configuration
 * @throws {Error} For other runtime errors during startup
 *
 * Process Flow:
 * 1. Initializes gRPC server and orchestrator instance
 * 2. Binds the gRPC server to the specified port (default: 50051)
 * 3. Loads and parses the pipeline configuration
 * 4. Sets up the pipeline with the loaded configuration
 * 5. Starts all runners and processors
 * 6. Waits for the pipeline to complete
 * 7. Handles graceful shutdown
 */
export async function start(location: string, port = 50051) {
    const logger = getLoggerFor(['start'])
    const grpcServer = new grpc.Server()
    const orchestrator = new Orchestrator()
    const server = new Server(orchestrator)
    setupOrchestratorLens(orchestrator)

    grpcServer.addService(RunnerService, server.server)
    await new Promise((res) =>
        grpcServer.bindAsync(
            '0.0.0.0:' + port,
            grpc.ServerCredentials.createInsecure(),
            res,
        ),
    )

    const addr = 'localhost:' + port
    logger.info('Grpc server is bound! ' + addr)
    const iri = pathToFileURL(location)
    setPipelineFile(iri)
    const quads = await readQuads([iri.toString()])

    reevaluteLevels()
    logger.debug('Setting pipeline')
    orchestrator.setPipeline(quads, iri.toString())

    await orchestrator.startInstantiators(
        addr,
        new Writer().quadsToString(quads),
    )

    await orchestrator.startProcessors()

    await orchestrator.waitClose()

    grpcServer.tryShutdown((e) => {
        if (e !== undefined) {
            logger.error(e)
            process.exit(1)
        } else {
            process.exit(0)
        }
    })
}

/**
 * Sets up the RDF lens mapping for the Orchestrator class.
 * Maps the rdfc:Orchestrator RDF type to this orchestrator instance for RDF processing.
 */
function setupOrchestratorLens(orchestrator: Orchestrator) {
    modelShapes.lenses['https://w3id.org/rdf-connect#Orchestrator'] =
        empty<Cont>().map(() => orchestrator)
}
