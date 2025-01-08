FROM ubuntu:24.04 AS base

WORKDIR /usr/src/monad-bft
RUN apt update && apt install -y \
  binutils \
  ca-certificates \
  curl \
  iproute2 \
  pkg-config \
  gnupg \
  software-properties-common \
  wget \
  cgroup-tools \
  libstdc++-13-dev \
  gcc-13 \
  g++-13

RUN apt update && apt install -y \
  libboost-atomic1.83.0 \
  libboost-container1.83.0 \
  libboost-fiber1.83.0 \
  libboost-filesystem1.83.0 \
  libboost-graph1.83.0 \
  libboost-json1.83.0 \
  libboost-regex1.83.0 \
  libboost-stacktrace1.83.0

RUN apt update && apt install -y \
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

RUN apt update && apt install -y \
  cmake \
  clang \
  libssl-dev

RUN apt update && apt install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"

ARG ETH_CALL_TARGET=monad_rpc_test
ARG TRIEDB_TARGET=triedb_driver

# Builder
COPY . .
RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" \
    CC=gcc-13 CXX=g++-13 cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc rpc && \
    cp `ls -Lt $(find target/release | grep -e "libevmone.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libquill.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libkeccak.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libsecp256k1.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libsilkpre.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "lib${TRIEDB_TARGET}.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "lib${ETH_CALL_TARGET}.so") | awk -F/ '!seen[$NF]++'` .

RUN strip \
    rpc \
    *.so \
    *.so.*

# Runner
FROM base AS runner
WORKDIR /usr/src/monad-bft

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

COPY --from=builder /usr/src/monad-bft/rpc /usr/local/bin/monad-rpc
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
COPY --from=builder /usr/src/monad-bft/*.so.* /usr/local/lib

ENV RUST_LOG=info
CMD monad-rpc \
    --ipc-path /monad/mempool.sock \
    --execution-ledger-path /monad/ledger \
    --triedb-path /monad/triedb
