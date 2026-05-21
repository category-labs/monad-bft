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
    RUSTFLAGS="-C target-cpu=haswell -C force-frame-pointers=yes" CC=gcc-15 CXX=g++-15 cargo build --release --bin monad-rpc && \
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
