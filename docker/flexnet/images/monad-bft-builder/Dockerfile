FROM ubuntu:24.04 AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt-get update && apt-get upgrade -y
RUN apt-get update && apt-get install -y ca-certificates curl pkg-config gnupg software-properties-common wget

RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN add-apt-repository -y ppa:mhier/libboost-latest
RUN apt-get update && apt-get install -y libstdc++-13-dev
RUN apt-get update && apt-get install -y gcc-13 g++-13

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

######## BEGIN CMAKE
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null \
  | gpg --dearmor - \
  | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
RUN echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' \
  | tee /etc/apt/sources.list.d/kitware.list >/dev/null
RUN apt-get update
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

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"

RUN mkdir -p /output/bin
RUN mkdir -p /output/lib

# WORKDIR /usr/src/monad-bft
COPY . .

# default to mock versions
ARG ETH_CALL_TARGET=mock_eth_call
ARG TRIEDB_TARGET=triedb_driver

RUN --mount=type=cache,target=${CARGO_ROOT}/registry     \
    --mount=type=cache,target=${CARGO_ROOT}/git          \
    --mount=type=cache,target=/usr/src/monad-bft/target     \
    ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" \
    CC=gcc-13 CXX=g++-13 cargo build --release --bin monad-node --bin monad-rpc --example ethtx
    
RUN --mount=type=cache,target=/usr/src/monad-bft/target     \
    find target/release/ -maxdepth 1 -perm /a+x -type f -exec cp {} /output/bin \; && \
    find target/release/examples -maxdepth 1 -perm /a+x -type f -exec cp {} /output/bin \; && \
    cp `ls -lt $(find target/release | grep lib${ETH_CALL_TARGET}.so) | head -1 | awk '{print $9}'` /output/lib && \
    cp `ls -lt $(find target/release | grep lib${TRIEDB_TARGET}.so) | head -1 | awk '{print $9}'` /output/lib

FROM ubuntu:24.04 AS exporter
WORKDIR /output

COPY --from=builder /output /output
