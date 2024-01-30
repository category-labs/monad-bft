FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y python3 clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-testground && \
    mv target/release/monad-testground testground
RUN python3 docker/testground/topology-gen.py topo.json
RUN python3 monad-scripts/tc/tc-gen.py topo.json tc.sh addresses

# Runner
FROM debian:buster-slim
SHELL ["/bin/bash", "-c"]
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y iproute2
RUN apt clean
COPY --from=builder /usr/src/monad-bft/testground /usr/local/bin/monad-testground
COPY --from=builder /usr/src/monad-bft/tc.sh .
COPY --from=builder /usr/src/monad-bft/addresses .

ENV RUST_LOG=monad_testground=DEBUG
CMD source tc.sh && MONAD_MEMPOOL_RNDUDS=true monad-testground -o http://jaeger:4317 --addresses $(<addresses)
