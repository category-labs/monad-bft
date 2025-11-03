# Builder
FROM ubuntu:25.04 AS base

RUN apt update && apt upgrade -y
RUN apt update && apt install -y binutils ca-certificates curl iproute2 pkg-config gnupg software-properties-common wget cgroup-tools

RUN apt install -y libstdc++-15-dev
RUN apt install -y gcc-15 g++-15

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

WORKDIR /usr/src/monad-bft
RUN apt update && apt upgrade -y
RUN apt update && apt install -y binutils ca-certificates curl iproute2 pkg-config gnupg software-properties-common wget cgroup-tools
RUN apt install -y clang 

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

RUN apt install -y clang libssl-dev

RUN apt-get install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"
RUN rustup toolchain install 1.85.0-x86_64-unknown-linux-gnu

WORKDIR /usr/src/monad-bft

# Builder
COPY . .
RUN cargo build --release --example rpc-request-generator && \
    mv target/release/examples/rpc-request-generator rpc-request-generator

# Runner
FROM ubuntu:25.04 as runner
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/rpc-request-generator /usr/local/bin/rpc-request-generator

ENV RUST_LOG=info
CMD rpc-request-generator