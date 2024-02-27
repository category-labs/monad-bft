FROM rust:1-buster AS builder

# builder stage
WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc monad-rpc-bin

# runner stage
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/monad-rpc-bin /usr/local/bin/monad-rpc

ENV RUST_LOG=info
