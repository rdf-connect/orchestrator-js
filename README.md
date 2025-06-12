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

<details>
    <summary>Streaming message sequence diagram</summary>

```mermaid
sequenceDiagram
    autonumber
    participant P1 as Processor 1
    participant R1 as Runner 1
    participant O as Orchestrator
    participant R2 as Runner 2
    participant P2 as Processor 2
    P1 -->> R1: Send streaming<br>message
    critical Start stream message
        R1 ->> O: rpc.sendStreamMessage<br>(bidirectional stream)
        O -->> R1: sends generated ID<br>of stream message
        R1 -->> O: announce StreamMessage<br>with ID over normal stream

        O -->> R2: announce StreamMessage<br>with ID over normal stream
        R2 ->> O: rpc.receiveMessage with Id<br>starts receiving stream
        R2 -->> P2: incoming stream message
    end

    loop Streams data
        P1 -->> R1: Data chunks
        R1 -->> O: Data chunks over stream
        O -->> R2: Data chunks over stream
        R2 -->>P2: Data chnuks
    end
```

</details>
