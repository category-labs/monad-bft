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

COPY --from=builder /usr/src/monad-bft/node /usr/local/bin/monad-node

ENV RUST_LOG=info
CMD monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --config /monad/config/config.toml \
    --mempool-ipc-path /monad/mempool.sock \
    --execution-ledger-path /monad/ledger
