import { grpc, RunnerService } from '@rdfc/proto'
import {
    getLoggerFor,
    getPrefixes,
    reevaluteLevels,
    setPipelineFile,
} from './logUtil.js'
import { Orchestrator } from './orchestrator.js'
import { Server } from './server.js'
import { pathToFileURL } from 'url'
import { RDFC, readQuads } from './util.js'
import { modelShapes } from './model.js'
import { Cont, empty } from 'rdf-lens'
import { inferProvenance, writeProvenance } from './provenance.js'
import stringifyStream from 'stream-to-string'
import { rdfSerializer } from 'rdf-serialize'
import { streamifyArray } from 'streamify-array'
import { ConnectionInjector } from '@grpc/grpc-js'

export * from './jsonld.js'
export * from './logUtil.js'
export * from './model.js'
export * from './orchestrator.js'
export * from './instantiators/index.js'
export * from './server.js'
export * from './util.js'

/**
 * Initializes and starts the orchestrator with the specified pipeline configuration.
 * This is the main entry point for the orchestrator service.
 *
 * @param {string} location - Filesystem path to the pipeline configuration file
 * @param {number} port - Port number on which to initialize the gRPC server (default: 50051)
 * @param {string} provenanceLocation - Filesystem path to store the provenance metadata to
 * @returns {Promise<void>}
 *
 * @throws {LensError} If there's an error processing the pipeline configuration
 * @throws {Error} For other runtime errors during startup
 *
 * Process Flow:
 * 1. Loads and parses the pipeline configuration
 * 2. Initializes gRPC server and orchestrator instance
 * 3. Binds the gRPC server to the main port (default: 50051)
 * 4. Starts all runners and processors
 * 5. Waits for the pipeline to complete
 * 6. Handles graceful shutdown
 */
export async function start(
    location: string,
    port = 50051,
    provenanceLocation?: string,
): Promise<void> {
    const logger = getLoggerFor(['start'])
    const grpcServer = new grpc.Server()
    const orchestrator = new Orchestrator()
    const server = new Server(orchestrator)

    const injector = grpcServer.createConnectionInjector(
        grpc.ServerCredentials.createInsecure(),
    )

    setupOrchestratorLens(orchestrator, injector)

    grpcServer.addService(RunnerService, server.server)

    const iri = pathToFileURL(location)
    setPipelineFile(iri)
    const quads = await readQuads([iri.toString()])

    // Provenance metadata is always computed so it can later be exposed in the
    // pipeline itself. The RDF-Connect ontology is fetched fresh as it lives in
    // a separate repository, and is only used as reasoning input.
    const ontologyQuads = await readQuads([RDFC.namespace])
    const provenanceQuads = inferProvenance(quads, ontologyQuads)

    reevaluteLevels()
    logger.debug('Setting pipeline')
    await orchestrator.setPipeline(quads, iri.toString())

    // Bind the main gRPC port
    const connectionString = '0.0.0.0:' + port
    await new Promise((res) =>
        grpcServer.bindAsync(
            connectionString,
            grpc.ServerCredentials.createInsecure(),
            res,
        ),
    )

    const host = process.env.RDFC_ADVERTISE_HOST ?? 'localhost'
    const addr = host + ':' + port
    logger.info('Grpc server is bound! ' + addr)

    await orchestrator.startInstantiators(
        addr,
        await stringifyStream(
            rdfSerializer.serialize(streamifyArray(provenanceQuads), {
                path: location,
                prefixes: getPrefixes(),
            }),
        ),
    )

    await orchestrator.startProcessors()

    // Already save provenance after starting the pipeline.
    if (provenanceLocation) {
        await writeProvenance(
            provenanceQuads,
            provenanceLocation,
            getPrefixes(),
        )
    }

    await orchestrator.waitClose()

    // Save the full provenance with timing provenance on completion.
    if (provenanceLocation) {
        const timingQuads = orchestrator.getProvenanceTimingQuads()
        await writeProvenance(
            [...provenanceQuads, ...timingQuads],
            provenanceLocation,
            getPrefixes(),
        )
    }

    // Let the connections know they should start shutting down
    injector.drain(500)
    grpcServer.drain(connectionString, 500)

    // Actually shut down the server
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
function setupOrchestratorLens(
    orchestrator: Orchestrator,
    injector: ConnectionInjector,
) {
    modelShapes.lenses['https://w3id.org/rdf-connect#Orchestrator'] =
        empty<Cont>().map(() => orchestrator)
    modelShapes.lenses['https://w3id.org/rdf-connect#Injector'] =
        empty<Cont>().map(() => injector)
}
