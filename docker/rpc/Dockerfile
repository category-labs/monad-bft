FROM ubuntu:25.04 AS base

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
COPY ./rust-toolchain.toml .
RUN cargo

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

# Builder
COPY . .
RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell -fno-omit-frame-pointer" CXXFLAGS="-march=haswell -fno-omit-frame-pointer" \
    RUSTFLAGS="-C force-frame-pointers=yes" CC=gcc-15 CXX=g++-15 cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc rpc && \
    cp `ls -Lt $(find target/release | grep -e "libtriedb_driver.so")` . && \
    ldd $(ls -Lt $(find target/release | grep -e "libtriedb_driver.so")) | docker/filter-dependent-shared-objects.py | xargs -I {} cp {} .

# Runner
FROM base AS runner

WORKDIR /usr/src/monad-bft
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

COPY --from=builder /usr/src/monad-bft/rpc /usr/local/bin/monad-rpc
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
COPY --from=builder /usr/src/monad-bft/*.so.* /usr/local/lib
