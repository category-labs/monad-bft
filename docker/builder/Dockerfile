FROM ubuntu:25.04 AS base

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
  g++-15 \
  libcrypto++-dev \
  libhugetlbfs0 \
  libhugetlbfs-bin \
  libhugetlbfs-dev \
  libmagicenum-dev

RUN apt update && apt install -y \
  libboost1.83-dev \
  libboost-atomic1.83.0 \
  libboost-container1.83.0 \
  libboost-fiber1.83.0 \
  libboost-fiber1.83-dev \
  libboost-filesystem1.83.0 \
  libboost-graph1.83.0 \
  libboost-graph1.83-dev \
  libboost-json1.83.0 \
  libboost-json1.83-dev \
  libboost-regex1.83.0 \
  libboost-stacktrace1.83.0 \
  libboost-stacktrace1.83-dev

RUN apt update && apt install -y \
  libcgroup-dev \
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

RUN apt update && apt install -y \
  cmake \
  clang-19 \
  libssl-dev \
  gdb \
  python-is-python3 \
  valgrind \
  ninja-build \
  python3-pytest

WORKDIR /app

# Install Rust system-wide
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
COPY ./rust-toolchain.toml .
RUN cargo

RUN chmod -R a+rwX $RUSTUP_HOME $CARGO_HOME
