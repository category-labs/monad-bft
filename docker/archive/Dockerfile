FROM ubuntu:25.04 AS base

WORKDIR /usr/src/monad-bft

COPY monad-execution/scripts/ubuntu-build /opt

RUN apt update && \
    apt upgrade -y && \
    apt install -y \
      binutils \
      cgroup-tools \
      iproute2 \
      libmimalloc-dev \
      libquill-dev \
      rustup && \
    /opt/install-boost.sh && \
    /opt/install-tools.sh && \
    /opt/install-deps.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm -rf /opt

RUN rustup toolchain install 1.91.1 --profile minimal

FROM base AS builder

ARG TRIEDB_TARGET=triedb_driver

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
RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" \
    CC=gcc-15 CXX=g++-15 cargo build --release \
    --bin monad-archiver \
    --bin monad-archive-checker \
    --bin monad-indexer && \
    mv target/release/monad-archiver archiver && \
    mv target/release/monad-archive-checker archive-checker && \
    mv target/release/monad-indexer indexer && \
    cp `ls -Lt $(find target/release | grep -e "libtriedb_driver.so")` . && \
    ldd $(ls -Lt $(find target/release | grep -e "libtriedb_driver.so")) | docker/filter-dependent-shared-objects.py | xargs -I {} cp {} .

# Runner
FROM base AS runner

WORKDIR /usr/src/monad-bft
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

COPY --from=builder /usr/src/monad-bft/archiver /usr/local/bin/monad-archiver
COPY --from=builder /usr/src/monad-bft/archive-checker /usr/local/bin/monad-archive-checker
COPY --from=builder /usr/src/monad-bft/indexer /usr/local/bin/monad-indexer
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
