FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc monad-rpc

# Runner
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/monad-rpc /usr/local/bin/monad-rpc

ENV RUST_LOG=info
CMD monad-rpc \
    --ipc-path /monad/mempool.sock

