FROM ubuntu:24.04 AS base

WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y \
  binutils \
  iproute2 \
  clang \
  curl \
  make \
  ca-certificates \
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
ARG TRIEDB_TARGET=triedb_driver

# Builder
COPY . .
RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" \
    CC=gcc-13 CXX=g++-13 cargo build --release \
    --bin monad-archiver \
    --bin monad-archive-checker \
    --bin monad-indexer \
    --bin monad-index-checker &&\
    mv target/release/monad-archiver archiver && \
    mv target/release/monad-archive-checker archive-checker && \
    mv target/release/monad-indexer indexer && \
    mv target/release/monad-index-checker index-checker && \
    cp `ls -Lt $(find target/release | grep -e "libtriedb_driver.so") | awk -F/ '!seen[$NF]++'` . 

RUN strip \
    archiver \
    archive-checker \
    indexer \
    index-checker \
    *.so 

# Runner
FROM base AS runner
WORKDIR /usr/src/monad-bft

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
COPY --from=builder /usr/src/monad-bft/archiver /usr/local/bin/monad-archiver
COPY --from=builder /usr/src/monad-bft/archive-checker /usr/local/bin/monad-archive-checker
COPY --from=builder /usr/src/monad-bft/indexer /usr/local/bin/monad-indexer
COPY --from=builder /usr/src/monad-bft/index-checker /usr/local/bin/monad-index-checker
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib

ENV RUST_LOG=info
