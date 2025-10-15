# Orchestrator JS

A JavaScript/TypeScript implementation of an RDF-based orchestrator for managing and executing data processing pipelines.

## Table of Contents

- [Features](#features)
- [Usage](#usage)
    - [CLI](#cli)
    - [Programmatic API](#programmatic-api)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Development](#development)
    - [Prerequisites](#prerequisites)
    - [Building](#building)
    - [Testing](#testing)
    - [Linting & Formatting](#linting--formatting)
- [Contributing](#contributing)
- [License](#license)

## Features

- 🚀 **Pipeline Management**: Define and manage data processing pipelines using RDF
- ⚡ **TypeScript Support**: Built with TypeScript for better developer experience
- 🔄 **Modular Architecture**: Easily extensible with custom processors and runners
- 🧪 **Test Coverage**: Comprehensive test suite with Vitest
- 🛠️ **Developer Tools**: ESLint and Prettier for code quality

## Usage

### CLI

The orchestrator can be run using the provided CLI:

```bash
# Install the orchestrator
npm install @rdfc/orchestrator-js
# Run with a pipeline configuration
npx rdfc path/to/your/pipeline.ttl
```

The CLI tool loads the RDF pipeline configuration, starts the gRPC server, spawns the configured runners, initializes processors, and manages the entire pipeline lifecycle.

## Configuration

Pipeline configurations are defined using RDF/Turtle format. Here's an example configuration:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.


### Import runners and processors
<> owl:imports <./.venv/lib/python3.13/site-packages/rdfc_runner/index.ttl>.
<> owl:imports <./.venv/lib/python3.13/site-packages/rdfc_log_processor/processor.ttl>.


### Define the channels
<channel> a rdfc:Writer, rdfc:Reader;
    rdfc:logLevel "DEBUG". # optional, log messages to debug


### Define the pipeline
<> a rdfc:Pipeline;
   rdfc:consistsOf [
       rdfc:instantiates rdfc:PyRunner;
       rdfc:processor <log>, <send>;
   ].


### Define the processors
<send> a rdfc:SendProcessorPy;
       rdfc:writer <channel>;
       rdfc:msg "Hello, World!", "Good afternoon, World!",
                "Good evening, World!", "Good night, World!".

<log> a rdfc:LogProcessorPy;
      rdfc:reader <channel>;
      rdfc:level "info";
      rdfc:label "test".


```

## Development

### Prerequisites

- Node.js 16+
- npm 7+ or yarn
- TypeScript 4.7+

### Building

```bash
# Install dependencies
npm install

# Build the project
npm run build

# Watch for changes
npm run build -- --watch
```

### Testing

```bash
# Run tests
npm test

# Run tests with coverage
npm test -- --coverage

# Run specific test file
npm test path/to/test/file.test.ts
```

### Linting & Formatting

```bash
# Run linter
npm run lint

# Fix linting issues
npm run lint -- --fix

# Format code
npm run format
```

## Project Structure

```
orchestrator-js/
├── bin/                  # Executable scripts
│   └── orchestrator.js   # Main CLI entry point and pipeline executor
├── lib/                  # Compiled JavaScript output
├── src/                  # TypeScript source files
│   ├── index.ts          # Main export file
│   ├── instantiator.ts   # Runner instantiation logic
│   ├── jsonld.ts         # JSON-LD utilities and RDF processing
│   ├── jsonld.ttl        # JSON-LD processor definitions
│   ├── logUtil.ts        # Logging utilities
│   ├── model.ts          # Data models and types
│   ├── model.ttl         # RDF model definitions
│   ├── orchestrator.ts   # Core orchestrator logic
│   ├── server.ts         # gRPC server implementation
│   ├── util.ts           # Utility functions
│   ├── pipeline.ttl      # Pipeline configuration schema
│   └── minimal.ttl       # Minimal example configuration
├── __tests__/            # Test files
│   ├── orchestrator.test.ts
│   ├── jsonld_derive.test.ts
│   ├── config.ttl
│   └── ...
├── .github/              # GitHub workflows and templates
├── .husky/               # Git hooks
├── package.json          # Project configuration and dependencies
├── tsconfig.json         # TypeScript configuration
├── jest.config.js        # Jest test configuration
├── eslint.config.mjs     # ESLint configuration
├── .prettierrc           # Prettier configuration
├── .editorconfig         # Editor configuration
└── README.md             # This file
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Commit Message Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or modifying tests
- `chore`: Build process or auxiliary tool changes

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Architecture

The system follows a modular architecture with the following main components:

- **Orchestrator**: Manages the overall pipeline execution
- **Runners**: Handle the execution of processing tasks
- **Processors**: Individual processing units that transform or analyze data
- **Server/Client**: Communication layer between components

### Sequence Diagrams

#### Initialization Sequence

<details>
  <summary>Initialization sequence diagram</summary>

```mermaid
sequenceDiagram
    autonumber
    participant O as Orchestrator
    participant R as Runner
    participant P as Processor

    Note over O: Initialize gRPC server on port 50051 (by default)
    O->>O: Create server with orchestrator

    Note over O: Load and parse RDF pipeline configuration

    O->>O: startInstantiators(addr, pipeline)
    loop For each instantiator in pipeline
        O->>O: expectRunner(instantiator)
        Note over O: Create promise to wait for runner connection
        O->>R: Spawn runner process with address
        R->>O: gRPC Connect with identify message
        O->>O: Resolve runner connection promise
        O->>R: Send pipeline configuration
    end

    Note over O: Initialize all processors
    loop For each processor in each runner
        O->>O: expectProcessor(instantiator)
        Note over O: Generate JSON-LD configuration for processor
        O->>R: addProcessor with configuration
        R->>P: Initialize processor
        P->>R: Processor ready
        R->>O: Initialized message with processor URI
        O->>O: Resolve processor startup promise
    end

    Note over O: Start all runners
    loop For each runner
        O->>R: Start message
        loop For each processor in runner
            R->>P: Start processor execution
        end
    end
```

</details>

<details>
    <summary>Message sequence diagram</summary>

```mermaid
sequenceDiagram
    autonumber
    participant P1 as Processor 1<br/>Type: Writer
    participant R1 as Runner 1<br/>Channel A Writer
    participant O as Orchestrator<br/>Message Router
    participant R2 as Runner 2<br/>Channel A Reader
    participant P2 as Processor 2<br/>Type: Reader

    Note over P1: Processor generates message for Channel A
    P1->>R1: Message with data for Channel A
    Note over R1: Runner forwards message to orchestrator
    R1->>O: gRPC SendingMessage(localSequenceNumber, channel, msg)

    Note over O: Orchestrator routes message to target instantiator
    O->>O: Look up channelToInstantiator[channel]
    O->>O: Create translated localSequenceNumber to globalSequenceNumber
    O->>R2: gRPC ReceivingMessage(globalSequenceNumber, channel, msg)

    Note over R2: Runner forwards message to target processor
    R2->>P2: Process message from Channel A

    Note over P2: Processor finishes processing
    P2->>R2: Processing complete
    R2->>O: gRPC GlobalAck(globalSequenceNumber, channel)
    O->>O: Clean up message state
    O->>R1: gRPC LocalAck(localSequenceNumber, channel)
```

</details>

<details>
    <summary>Streaming message sequence diagram</summary>

```mermaid
sequenceDiagram
    autonumber
    participant P1 as Processor 1<br/>Stream Writer
    participant R1 as Runner 1<br/>Source Runner
    participant O as Orchestrator<br/>Stream Manager
    participant R2 as Runner 2<br/>Target Runner
    participant P2 as Processor 2<br/>Stream Reader

    Note over P1: Processor initiates streaming message
    P1->>R1: Request to start streaming to Channel B
    R1->>O: gRPC startStreamMessage(identify: (localSequenceNumber, channel, runner))
    O->>R1: Return chunk acknowledged

    Note over O: Set up stream routing between runners
    O->>O: Register connectingStreams[globalSequenceNumber]
    O->>R2: gRPC ReceivingStreamMessage(globalSequenceNumber, global)
    R2->>O: gRPC connectingReceivingStream(globalSequenceNumber)
    O->>O: Link stream writer to connecting stream
    O->>R1: Sends stream control - ReceivingStreamControl{streamSequenceNumber}


    Note over P1: Begin streaming data
    loop For Each Chunk
        P1->>R1: Stream data chunks
        R1->>O: sendStreamMessage() - StreamChunk{data: DataChunk(bytes)}
        O->>R2: receiveStreamMessage() - DataChunk(bytes)
        R2->>P2: Forward data chunks to processor
        P2->>R2: Chunk handled
        R2->>O: chunk handled - SendingStreamControl{streamSequenceNumber}
        O->>R1: chunk handled - ReceivingStreamControl{streamSequenceNumber}
    end

    P1->>R1: End of stream
    R1->>O: Stream closed
    O->>R2: Stream closed
    R2->>O: FromRunnner{processed: GlobalAck(globalSequenceNumber, channel)}
    O->>R1: ToRunner{processed: LocalAck(localSequenceNumber, channel)}
```

</details>
