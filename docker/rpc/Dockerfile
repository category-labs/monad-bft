# syntax=docker/dockerfile:1-labs

FROM ubuntu:22.04 as base

RUN apt update
RUN apt upgrade -y
RUN apt install -y ca-certificates curl gnupg software-properties-common wget

RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN add-apt-repository -y ppa:mhier/libboost-latest
RUN apt update

RUN apt install -y libstdc++-13-dev

RUN apt-get install -y \
  libboost-atomic1.83.0 \
  libboost-container1.83.0 \
  libboost-fiber1.83.0 \
  libboost-filesystem1.83.0 \
  libboost-graph1.83.0 \
  libboost-json1.83.0 \
  libboost-regex1.83.0 \
  libboost-stacktrace1.83.0

RUN apt install -y \
  libabsl-dev \
  libarchive-dev \
  libbenchmark-dev \
  libbrotli-dev \
  libcap-dev \
  libcli11-dev \
  libgmock-dev \
  libgmp-dev \
  libgtest-dev \
  libmimalloc-dev \
  libtbb-dev \
  liburing-dev \
  libzstd-dev

FROM base as build

RUN apt install -y gcc-13 g++-13

RUN apt install -y ninja-build pkg-config
RUN apt install -y python3-pytest

######## BEGIN CMAKE
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null \
  | gpg --dearmor - \
  | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
RUN echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' \
  | tee /etc/apt/sources.list.d/kitware.list >/dev/null
RUN apt-get update
RUN rm /usr/share/keyrings/kitware-archive-keyring.gpg
RUN apt-get -y install kitware-archive-keyring
RUN apt-get -y install cmake
######## END CMAKE

RUN apt-get install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev

######## Rust-specific builder stuff below here

RUN apt install -y clang libssl-dev

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"

WORKDIR /usr/src/monad-bft
COPY . .
RUN --mount=type=cache,target=${CARGO_ROOT}/registry    \
    --mount=type=cache,target=${CARGO_ROOT}/git         \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    TRIEDB_TARGET=triedb_driver ETH_CALL_TARGET=monad_shared \
    CXX=/usr/bin/g++-13 CC=/usr/bin/gcc-13 \
    ASMFLAGS=-march=haswell CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" \
    cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc rpc && \
    cp `ls -lt $(find target/release | grep libmonad_shared.so) | head -1 | awk '{print $9}'` . && \
    cp `ls -lt $(find target/release | grep libtriedb_driver.so) | head -1 | awk '{print $9}'` .

# Runner
FROM base as runner
WORKDIR /usr/src/monad-bft

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

COPY --from=build /usr/src/monad-bft/rpc /usr/local/bin/monad-rpc
COPY --from=build /usr/src/monad-bft/*.so /usr/local/lib

ENV RUST_LOG=info
CMD monad-rpc \
    --ipc-path /monad/mempool.sock \
    --blockdb-path /monad/blockdb \
    --execution-ledger-path /monad/ledger
