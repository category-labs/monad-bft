# builder stage
FROM rust:1-buster AS builder
WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# devnet image
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-node && \
    mv target/release/monad-node monad-node-bin

# runner stage
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/monad-node-bin /usr/local/bin/monad-node

RUN apt update && apt install -y net-tools iputils-ping iproute2

ENV RUST_LOG=info
