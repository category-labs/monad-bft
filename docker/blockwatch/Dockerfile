FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN cargo build --release --example blockwatch && \
    mv target/release/examples/blockwatch blockwatch

# Runner
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/blockwatch /usr/local/bin/blockwatch

ENV RUST_LOG=info
CMD blockwatch
