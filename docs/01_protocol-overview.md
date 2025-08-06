# Protocol Overview

## What is Monad-BFT?
Monad-BFT is a Byzantine Fault Tolerant (BFT) consensus protocol designed to provide fast finality and strong safety guarantees, with a novel focus on *tail-fork resistance* to mitigate malicious reorgs and unfair MEV extraction.

## Core Innovations

1. **Speculative Finality**: Validators speculatively commit a block after one round of voting, achieving low-latency confirmation.  
2. **Tail-Fork Resistance**: Unlike classic BFT protocols, Monad prevents malicious leaders from creating disruptive forks at the tail of the chain by embedding equivocation-proof-based safety controls.  
3. **Quadratic Timeout Certificates**: A fallback mechanism that uses dynamically increasing timeouts to guarantee liveness without sacrificing safety.

## Why Monad-BFT Matters

- **Low Latency**: Speculative commits reduce confirmation times closer to one network round-trip.  
- **MEV Mitigation**: Tail-fork resistance defends against validators racing to extract value at the expense of network consistency.  
- **Robustness**: Timeout certificates ensure progress even under high latency or partial network partitions.

## Relationship to Other Protocols

| Feature                      | HotStuff         | Tendermint      | Monad-BFT               |
|------------------------------|------------------|-----------------|-------------------------|
| Rounds to Finality           | 3                | 2               | 1 speculative + 1 TC    |
| Equivocation Safety          | Yes              | Yes             | Yes (enhanced)          |
| Tail-Fork Resistance         | No               | No              | Yes                     |
| Timeout Mechanism            | Linear           | Linear          | Quadratic               |


*Next up: a detailed step-by-step consensus flow with sequence diagrams.*  
