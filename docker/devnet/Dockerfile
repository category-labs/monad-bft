FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-node && \
    mv target/release/monad-node node

# Runner
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

RUN mkdir -p /monad/config
COPY --from=builder /usr/src/monad-bft/node /usr/local/bin/monad-node
COPY --from=builder /usr/src/monad-bft/docker/devnet/monad/config /monad/config

# ENV RUST_LOG=info
CMD monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --node-config /monad/config/node.toml \
    --genesis-config /monad/config/genesis.toml \
    --wal-path /monad/wal \
    --mempool-ipc-path /monad/mempool.sock \
    --execution-ledger-path /monad/ledger \
    --blockdb-path /monad/blockdb
