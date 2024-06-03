FROM ubuntu:24.04 AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang curl make

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"

# Builder
COPY . .
RUN --mount=type=cache,target=${CARGO_ROOT}/registry    \
    --mount=type=cache,target=${CARGO_ROOT}/git          \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-node && \
    mv target/release/monad-node node

# Runner
FROM ubuntu:24.04
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/node /usr/local/bin/monad-node

ENV RUST_LOG=debug
CMD monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --node-config /monad/config/node.toml \
    --genesis-config /monad/config/genesis.toml \
    --wal-path /monad/wal \
    --mempool-ipc-path /monad/mempool.sock \
    --execution-ledger-path /monad/ledger \
    --blockdb-path /monad/blockdb
