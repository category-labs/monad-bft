# syntax=docker/dockerfile:1
#
# Consolidated build for all Rust service images (node / rpc / archive / txgen).
#
# The four per-service Dockerfiles (docker/{devnet,rpc,archive,txgen}/Dockerfile)
# each ran their own full `cargo build`, recompiling the whole workspace AND the
# C++ monad_execution lib from scratch — 4x. Here a single `builder` stage
# compiles everything once; the thin per-service runner stages just COPY their
# binaries from it.
#
# Build the heavy stage once, then the cheap runners (all on ONE buildx builder
# so the `builder` layer is shared):
#   docker build -f docker/services.Dockerfile --target builder -t monad-builder .
#   docker build -f docker/services.Dockerfile --target node     -t monad-node    .
#   docker build -f docker/services.Dockerfile --target rpc       -t monad-rpc     .
#   ... (runner builds reuse the cached `builder` stage)

# ───────────────────────── shared base (builder + every runner) ─────────────────────────
FROM ubuntu:26.04 AS base

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
  git \
  python-is-python3 \
  cgroup-tools \
  libstdc++-15-dev \
  gcc-15 \
  g++-15

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
  libcrypto++-dev \
  libgmock-dev \
  libgmp-dev \
  libgtest-dev \
  libhugetlbfs-dev \
  libmagicenum-dev \
  libmimalloc-dev \
  libtbb-dev \
  liburing-dev \
  libzstd-dev

# ───────────────────────── one builder: whole workspace + C++ lib, once ─────────────────────────
FROM base AS builder

RUN apt update && apt install -y \
  cmake \
  libssl-dev

RUN apt update && apt install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev

ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"
RUN rustup toolchain install 1.91.1-x86_64-unknown-linux-gnu

# Drives monad-cxx/monad-triedb build.rs to build the C++ monad_execution lib.
ENV TRIEDB_TARGET=triedb_driver

ARG GIT_TAG_VERSION
RUN test -n "$GIT_TAG_VERSION"
ENV MONAD_VERSION=$GIT_TAG_VERSION

ARG GIT_COMMIT=""
ARG GIT_BRANCH=""
ARG GIT_TAG=""
ARG GIT_MODIFIED="false"
ENV GIT_COMMIT=$GIT_COMMIT
ENV GIT_BRANCH=$GIT_BRANCH
ENV GIT_TAG=$GIT_TAG
ENV GIT_MODIFIED=$GIT_MODIFIED

COPY . .

# ONE cargo build → C++ monad_execution lib + all Rust crates compiled once,
# producing every service's binaries/examples.
# CARGO_BUILD_JOBS=32: host has 128 cores but higher exhausts memory during the
# C++ build of monad-cxx (linked via cmake-rs).
RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell -fno-omit-frame-pointer" CXXFLAGS="-march=haswell -fno-omit-frame-pointer" \
    RUSTFLAGS="-C target-cpu=haswell -C force-frame-pointers=yes" CC=gcc-15 CXX=g++-15 CARGO_BUILD_JOBS=32 CMAKE_BUILD_PARALLEL_LEVEL=32 \
    cargo build --release \
      --bin monad-node --bin monad-keystore --bin monad-debug-node \
      --bin monad-rpc \
      --bin monad-archiver --bin monad-archive-checker --bin monad-indexer \
      --example ledger-tail --example wal2json --example triedb-bench \
      --example sign-name-record --example txgen

# Stage binaries + shared libs into a stable layout for the runner COPYs.
RUN set -eux; \
    mkdir -p /artifacts/bin /artifacts/lib; \
    cp target/release/monad-node           /artifacts/bin/; \
    cp target/release/monad-keystore       /artifacts/bin/keystore; \
    cp target/release/monad-debug-node     /artifacts/bin/; \
    cp target/release/monad-rpc            /artifacts/bin/; \
    cp target/release/monad-archiver       /artifacts/bin/; \
    cp target/release/monad-archive-checker /artifacts/bin/; \
    cp target/release/monad-indexer        /artifacts/bin/; \
    cp target/release/examples/ledger-tail      /artifacts/bin/; \
    cp target/release/examples/wal2json         /artifacts/bin/; \
    cp target/release/examples/triedb-bench     /artifacts/bin/; \
    cp target/release/examples/sign-name-record /artifacts/bin/; \
    cp target/release/examples/txgen            /artifacts/bin/; \
    SO="$(find target/release/build -name libtriedb_driver.so -printf '%T@ %p\n' | sort -rn | head -n1 | cut -d' ' -f2-)"; \
    test -n "$SO"; \
    cp "$SO" /artifacts/lib/; \
    ldd "$SO" | docker/filter-dependent-shared-objects.py | xargs -I{} cp {} /artifacts/lib/

# ───────────────────────── thin per-service runners ─────────────────────────
FROM base AS node
ENV LD_LIBRARY_PATH="/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1
COPY --from=builder /artifacts/lib/ /usr/local/lib/
COPY --from=builder \
  /artifacts/bin/monad-node \
  /artifacts/bin/keystore \
  /artifacts/bin/monad-debug-node \
  /artifacts/bin/ledger-tail \
  /artifacts/bin/wal2json \
  /artifacts/bin/triedb-bench \
  /artifacts/bin/sign-name-record \
  /usr/local/bin/

FROM base AS rpc
ENV LD_LIBRARY_PATH="/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1
COPY --from=builder /artifacts/lib/ /usr/local/lib/
COPY --from=builder /artifacts/bin/monad-rpc /usr/local/bin/

FROM base AS archive
ENV LD_LIBRARY_PATH="/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1
COPY --from=builder /artifacts/lib/ /usr/local/lib/
COPY --from=builder \
  /artifacts/bin/monad-archiver \
  /artifacts/bin/monad-archive-checker \
  /artifacts/bin/monad-indexer \
  /usr/local/bin/

FROM base AS txgen
ENV LD_LIBRARY_PATH="/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
ENV RUST_LOG=info
COPY --from=builder /artifacts/lib/ /usr/local/lib/
COPY --from=builder /artifacts/bin/txgen /usr/local/bin/
# Parity with the old docker/txgen/Dockerfile (`CMD txgen`). flexnet always
# launches with an explicit command, so this only affects standalone runs.
CMD ["txgen"]
