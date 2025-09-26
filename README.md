# Monad BFT

## Overview

This repository contains implementation for the Monad consensus client and JsonRpc server. Monad consensus collects transactions and produces blocks which are written to a ledger filestream. These blocks are consumed by Monad execution, which then updates the state of the blockchain. The [triedb](monad-triedb/README.md) is a database which stores block information and the blockchain state.

## Getting Started

```sh
git submodule update --init --recursive
```

### Using Docker

The most straightforward way to start a consensus client + an execution client + a JsonRpc server. Run the following:
1. `cd docker/single-node`
2. `nets/run.sh`

This will start a single node with chain ID of 20143 and RPC at localhost:8080. The known [Foundry/Anvil accounts](https://getfoundry.sh/anvil/overview/) have each been loaded with [large initial balances](https://github.com/category-labs/monad/blob/ce4101b11701bf4ef3a9cd996a6144883735187f/category/execution/monad/chain/monad_devnet_alloc.hpp#L22).

### Using Cargo

To run a Monad consensus client, follow instructions [here](monad-node/README.md).
 
To run a JsonRpc server, follow instructions [here](monad-rpc/README.md).

## Architecture

```mermaid
sequenceDiagram
autonumber
    participant D as Driver
    box Purple Executor
    participant S as impl Stream
    participant E as impl Executor
    end
    participant State
    participant PersistenceLogger
    loop
    D ->>+ S: CALL next()
    Note over S: blocks until event ready
    S -->>- D: RETURN Event
    D ->> PersistenceLogger: CALL push(Event)
    D ->>+ State: CALL update(Event)
    Note over State: mutate state
    State -->>- D: RETURN Vec<Command>
    D ->> E: CALL exec(Vec<Command>)
    Note over E: apply side effects
    end
```

[tests-badge]: https://github.com/monad-crypto/monad-bft/actions/workflows/randomized.yml/badge.svg?branch=master
