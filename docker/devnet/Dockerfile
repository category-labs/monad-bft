FROM ubuntu:25.04 AS base

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
RUN rustup toolchain install 1.91.1-x86_64-unknown-linux-gnu

ENV TRIEDB_TARGET=triedb_driver
ENV ASMFLAGS="-march=haswell"
ENV CFLAGS="-march=haswell -fno-omit-frame-pointer"
ENV CXXFLAGS="-march=haswell -fno-omit-frame-pointer"
ENV RUSTFLAGS="-C force-frame-pointers=yes"
ENV CC=gcc-15
ENV CXX=g++-15

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

RUN --mount=type=cache,target=/usr/src/monad-bft/target \
  --mount=type=cache,target=/root/.cargo/registry \
  --mount=type=cache,target=/root/.cargo/git \
  cargo build --release --bin monad-node --bin monad-keystore --bin monad-debug-node --example ledger-tail --example wal2json --example triedb-bench --example sign-name-record && \
  cp target/release/monad-node monad-node && \
  cp target/release/monad-keystore keystore && \
  cp target/release/monad-debug-node monad-debug-node && \
  cp target/release/examples/ledger-tail ledger-tail && \
  cp target/release/examples/wal2json wal2json && \
  cp target/release/examples/triedb-bench triedb-bench && \
  cp target/release/examples/sign-name-record sign-name-record && \
  cp `ls -Lt $(find target/release | grep -e "libtriedb_driver.so")` . && \
  ldd $(ls -Lt $(find target/release | grep -e "libtriedb_driver.so")) | docker/filter-dependent-shared-objects.py | xargs -I {} cp {} .

# Runner
FROM base AS runner

WORKDIR /usr/src/monad-bft
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

COPY --from=builder /usr/src/monad-bft/monad-node/monad-node /usr/local/bin/monad-node
COPY --from=builder /usr/src/monad-bft/keystore /usr/local/bin/keystore
COPY --from=builder /usr/src/monad-bft/monad-debug-node/monad-debug-node /usr/local/bin/monad-debug-node
COPY --from=builder /usr/src/monad-bft/ledger-tail /usr/local/bin/ledger-tail
COPY --from=builder /usr/src/monad-bft/wal2json /usr/local/bin/wal2json
COPY --from=builder /usr/src/monad-bft/triedb-bench /usr/local/bin/triedb-bench
COPY --from=builder /usr/src/monad-bft/sign-name-record /usr/local/bin/sign-name-record
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
COPY --from=builder /usr/src/monad-bft/*.so.* /usr/local/lib
