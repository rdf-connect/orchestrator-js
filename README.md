# Orchestrator Javascipt edition

### Install

```bash
npm install
npm test
```

### Run

```bash
node bin/runner.js ./pipeline.ttl
```

### Diagrams

<details>
  <summary>Initialization sequence diagram</summary>

```mermaid
sequenceDiagram
    autonumber
    participant O as Orchestrator
    participant R as Runner
    participant P as Processor
    Note over O: Discovered Runners<br>and processors
    loop For every runner
        Note over R: Runner is started with cli
        O-->>R: Startup with address and uri
        R-->>O: RPC.Identify: with uri
    end
    loop For every processor
        O-->>R: RPC.Processor: Start processor
        Note over P: Load module and class
        R-->>P: Load processor
        R-->>P: Start processor with args
        R-->>O: RPC.ProcessorInit: processor started
    end
    loop For every runner
        O-->>R: RPC.Start: Start
        loop For every Processor
            R-->>P: Start
        end
    end
```

</details>

<details>
    <summary>Message sequence diagram</summary>

```mermaid
sequenceDiagram
    participant P1 as Processor 1
    participant R1 as Runner 1
    participant O as Orchestrator
    participant R2 as Runner 2
    participant P2 as Processor 2
    P1-->>R1: Msg to Channel A
    R1-->>O: Msg to Channel A
    Note over O: Channel A is connected<br>to processor of Runner 2
    O -->> R2: Msg to Channel A
    R2-->>P2: Msg to Channel A
```

</details>

Streaming messages vs blob messages.
Reader can have methods: asStreamReader or asBlobReader.
Same with writers

Can Kafka handle streaming body messages?

We need some metrics.
