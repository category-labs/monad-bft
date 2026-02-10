# TxPool Executor Architecture

## Component Overview

```mermaid
graph TB
    subgraph External["External Sources"]
        IPC["Unix IPC Socket<br/>(local txs)"]
        BFT["BFT Consensus Framework"]
        Peers["Peer Nodes<br/>(forwarded txs)"]
    end

    subgraph Executor["EthTxPoolExecutor&lt;ST, SCT, SBT, CCT, CRT, TIS&gt;"]
        direction TB

        subgraph Ingress["Ingress"]
            IpcServer["EthTxPoolIpcServer<br/><i>impl EthTxPoolTxInputStream</i><br/>batch: 128 tx / 8ms"]
            ResetGate["EthTxPoolResetTrigger<br/>blocks IPC until reset"]
            FwdMgrIn["ForwardingManager::ingress<br/>FairQueue&lt;ScoreReader&gt;<br/>128 tx/tick, 8ms interval"]
        end

        Recovery["Parallel Signature Recovery<br/>(rayon)"]

        subgraph Pool["EthTxPool&lt;ST, SCT, SBT, CCT, CRT&gt;"]
            TrackedMap["TrackedTxMap<br/>IndexMap&lt;Address, TrackedTxList&gt;"]
            PriorityMap["PriorityMap<br/>(sequencing)"]
            Limits["TrackedTxLimits"]
        end

        subgraph Egress["Egress"]
            FwdMgrOut["ForwardingManager::egress<br/>VecDeque&lt;Bytes&gt;<br/>max 448KB/batch"]
        end

        Preload["PreloadManager<br/>predict leader seqnums<br/>128 addr/chunk, 1ms interval"]
        BlockPolicy["EthBlockPolicy<br/>base fee, validation"]
        Score["ScoreProvider<br/>peer reputation"]
    end

    subgraph Swappable["Swappable Traits"]
        TIS_trait["EthTxPoolTxInputStream<br/><i>tx source</i>"]
        SBT_trait["StateBackend&lt;ST,SCT&gt;<br/><i>state storage</i>"]
        CCT_trait["ChainConfig&lt;CRT&gt;<br/><i>chain params</i>"]
        CRT_trait["ChainRevision<br/><i>chain versioning</i>"]
        Clock_trait["Clock<br/><i>time source</i>"]
    end

    %% Ingress flows
    IPC -->|"UnixListener"| IpcServer
    Peers -->|"ForwardedTxs{sender,txs}"| FwdMgrIn
    IpcServer -->|"Vec&lt;EthTxPoolIpcTx&gt;"| Recovery
    FwdMgrIn -->|"score-weighted drain"| Recovery
    ResetGate -.->|"gates"| IpcServer

    %% Recovery to pool
    Recovery -->|"Recovered&lt;TxEnvelope&gt;"| Pool

    %% Pool internals
    Pool -->|"on_insert callback<br/>(owned forwardable)"| FwdMgrOut
    Pool -->|"sender addresses"| Preload

    %% Commands from BFT
    BFT -->|"CreateProposal"| Pool
    BFT -->|"BlockCommit"| Pool
    BFT -->|"EnterRound"| Preload
    BFT -->|"Reset"| ResetGate
    BFT -->|"InsertForwardedTxs"| FwdMgrIn

    %% Outputs back to BFT
    Pool -->|"MempoolEvent::Proposal"| BFT
    FwdMgrOut -->|"MempoolEvent::ForwardTxs"| BFT
    IpcServer -.->|"broadcast tx events"| IPC

    %% Trait usage
    SBT_trait -.->|"balance/nonce queries"| Pool
    SBT_trait -.->|"preload lookups"| Preload
    CCT_trait -.->|"chain_id, revision"| BlockPolicy
    TIS_trait -.->|"implemented by"| IpcServer
    Clock_trait -.->|"used by"| Score

    %% Score integration
    Score -->|"ScoreReader"| FwdMgrIn
    Pool -->|"record_contribution"| Score

    %% Preload to policy
    Preload -->|"compute_account_base_balances"| BlockPolicy

    %% Styling
    classDef trait fill:#f9f,stroke:#333,stroke-width:1px
    classDef external fill:#bbf,stroke:#333,stroke-width:2px
    class TIS_trait,SBT_trait,CCT_trait,CRT_trait,Clock_trait trait
    class IPC,BFT,Peers external
```

## Transaction Lifecycle

```mermaid
sequenceDiagram
    participant IPC as IPC Socket
    participant Srv as IpcServer
    participant Rec as Rayon Recovery
    participant Pool as EthTxPool
    participant Fwd as ForwardingMgr
    participant Pre as PreloadMgr
    participant BFT as BFT Consensus

    IPC->>Srv: raw tx bytes
    Srv->>Rec: batch (128 tx / 8ms)
    Rec->>Pool: Recovered<TxEnvelope>
    Pool->>Pool: validate (nonce, balance, fees)
    Pool->>Fwd: on_insert → egress queue (owned txs)
    Pool->>Pre: sender address → preload set
    Fwd->>BFT: MempoolEvent::ForwardTxs

    Note over BFT: Leader round
    BFT->>Pool: CreateProposal
    Pool->>Pool: sequence txs (gas/byte limit)
    Pool->>BFT: MempoolEvent::Proposal

    Note over BFT: Block committed
    BFT->>Pool: BlockCommit
    Pool->>Pool: drop committed, update nonces
    Pool->>IPC: broadcast Commit events
```

## Swappable Abstractions

| Trait | Abstracts | Current Impl | Test Impl |
|-------|-----------|-------------|-----------|
| `EthTxPoolTxInputStream` | Transaction source | `EthTxPoolIpcServer` (Unix socket) | Custom streams |
| `StateBackend<ST,SCT>` | State storage & queries | Persistent backend | `InMemoryState` |
| `ChainConfig<CRT>` | Chain parameters | Production config | `MockChainConfig` |
| `ChainRevision` | Chain version rules | Typed revisions | `MockChainRevision` |
| `Clock` | Time source | `StdClock` | `MockClock` |

**Not swappable (concrete):** `EthTxPool`, `TrackedTxMap`, `ForwardingManager`, `PreloadManager`, `EthBlockPolicy`, `ScoreProvider`, `FairQueue`.

## Channel Architecture

```
EthTxPoolExecutorClient (spawns executor on dedicated tokio task)

  command_tx  ──(1024)──►  EthTxPoolExecutor::run()
  forwarded_tx ─(1024)──►    tokio::select! { biased }
                                │ 1. commands (highest)
  event_rx    ◄──(1024)──      │ 2. IPC + forwarding ingress + preload
                                │ 3. forwarded txs (lowest)
```

## Key Constants

| Parameter | Value |
|-----------|-------|
| IPC batch size | 128 txs |
| IPC batch timeout | 8ms |
| Ingress drain per tick | 128 txs |
| Ingress tick interval | 8ms |
| Preload chunk size | 128 addresses |
| Preload interval | 1ms |
| Egress max batch | 448 KB |
| Per-peer ingress limit | 10,000 txs |
| Global ingress limit | 100,000 txs |
| New-peer bandwidth share | 10% |
| Egress min commit diff | 5 seqnums |
| Egress max retries | 3 |
