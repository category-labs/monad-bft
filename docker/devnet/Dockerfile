FROM ubuntu:24.04 AS base

WORKDIR /usr/src/monad-bft
RUN apt-get update && apt-get upgrade -y
RUN apt-get update && apt-get install -y clang curl make ca-certificates pkg-config gnupg software-properties-common wget cgroup-tools

RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN add-apt-repository -y ppa:mhier/libboost-latest
RUN apt-get update && apt-get install -y libstdc++-13-dev gcc-13 g++-13

RUN apt-get update && apt-get install -y \
  libboost-atomic1.83.0 \
  libboost-container1.83.0 \
  libboost-fiber1.83.0 \
  libboost-filesystem1.83.0 \
  libboost-graph1.83.0 \
  libboost-json1.83.0 \
  libboost-regex1.83.0 \
  libboost-stacktrace1.83.0

RUN apt-get update && apt-get install -y \
  libabsl-dev \
  libarchive-dev \
  libbenchmark-dev \
  libbrotli-dev \
  libcap-dev \
  libcgroup-dev \
  libcli11-dev \
  libgmock-dev \
  libgmp-dev \
  libgtest-dev \
  libmimalloc-dev \
  libtbb-dev \
  liburing-dev \
  libzstd-dev

FROM base AS builder

######## BEGIN CMAKE
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null \
  | gpg --dearmor - \
  | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
RUN echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' \
  | tee /etc/apt/sources.list.d/kitware.list >/dev/null
RUN apt-get update && apt-get -y install kitware-archive-keyring
RUN rm /usr/share/keyrings/kitware-archive-keyring.gpg
RUN apt-get update && apt-get -y install cmake
######## END CMAKE

RUN apt-get update && apt-get install -y clang libssl-dev

RUN apt-get update && apt-get install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev


###

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"

ARG TRIEDB_TARGET=triedb_driver

# Builder
COPY . .
RUN --mount=type=cache,target=${CARGO_ROOT}/registry    \
    --mount=type=cache,target=${CARGO_ROOT}/git          \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" \
    CC=gcc-13 CXX=g++-13 cargo build --release --bin monad-node --bin monad-keystore --bin debug-node --example wal2json --example wal-tool && \
    mv target/release/monad-node node && \
    mv target/release/monad-keystore keystore && \
    mv target/release/debug-node debug-node && \
    mv target/release/examples/wal2json wal2json && \
    mv target/release/examples/wal-tool wal-tool && \
    cp `ls -Lt $(find target/release | grep -e "libtriedb_driver.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libmonad_statesync.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libevmone.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libquill.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libkeccak.so") | awk -F/ '!seen[$NF]++'` .

RUN apt install -y binutils && strip \
    node \
    keystore \
    debug-node \
    wal2json \
    wal-tool \
    *.so \
    *.so.*

# Runner
FROM base AS runner
WORKDIR /usr/src/monad-bft

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
COPY --from=builder /usr/src/monad-bft/node /usr/local/bin/monad-node
COPY --from=builder /usr/src/monad-bft/keystore /usr/local/bin/keystore
COPY --from=builder /usr/src/monad-bft/debug-node /usr/local/bin/debug-node
COPY --from=builder /usr/src/monad-bft/wal2json /usr/local/bin/wal2json
COPY --from=builder /usr/src/monad-bft/wal-tool /usr/local/bin/wal-tool
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
COPY --from=builder /usr/src/monad-bft/*.so.* /usr/local/lib

ENV RUST_LOG=info
CMD monad-node \
    --secp-identity /monad/config/id-secp \
    --bls-identity /monad/config/id-bls \
    --node-config /monad/config/node.toml \
    --forkpoint-config /monad/config/forkpoint.genesis.toml \
    --genesis-path /monad/config/genesis.json \
    --statesync-ipc-path /monad/statesync.sock \
    --wal-path /monad/wal \
    --mempool-ipc-path /monad/mempool.sock \
    --control-panel-ipc-path /monad/controlpanel.sock \
    --execution-ledger-path /monad/ledger \
    --blockdb-path /monad/blockdb \
    --triedb-path /monad/triedb
