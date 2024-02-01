FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --example ethtx && \
    mv target/release/examples/ethtx ethtx

# Runner
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/ethtx /usr/local/bin/monad-ethtx-tool

ENV RUST_LOG=info
CMD monad-ethtx-tool \
    --ipc-path /monad/mempool.sock
