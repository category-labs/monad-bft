FROM ubuntu:25.04 AS base

WORKDIR /usr/src/monad-bft

COPY monad-execution/scripts/ubuntu-build /opt

RUN apt update && \
    apt install -y \
      binutils \
      cgroup-tools \
      iproute2 \
      libmimalloc-dev \
      libquill-dev \
      rustup && \
    /opt/install-boost.sh && \
    /opt/install-deps.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm -rf /opt

RUN rustup toolchain install 1.91.1 --profile minimal

FROM base AS builder

ARG TRIEDB_TARGET=triedb_driver

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
    RUSTFLAGS="-C target-cpu=haswell -C force-frame-pointers=yes" CC=gcc-15 CXX=g++-15 cargo build --release --bin monad-node --bin monad-keystore --bin monad-debug-node --example ledger-tail --example wal2json --example triedb-bench --example sign-name-record && \
    mv target/release/monad-node monad-node && \
    mv target/release/monad-keystore keystore && \
    mv target/release/monad-debug-node monad-debug-node && \
    mv target/release/examples/ledger-tail ledger-tail && \
    mv target/release/examples/wal2json wal2json && \
    mv target/release/examples/triedb-bench triedb-bench && \
    mv target/release/examples/sign-name-record sign-name-record && \
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
