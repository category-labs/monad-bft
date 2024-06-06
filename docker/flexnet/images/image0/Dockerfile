# Default monad node

# runner stage
FROM ubuntu:24.04

RUN apt update
RUN apt upgrade -y
RUN apt-get update
RUN apt install -y ca-certificates curl pkg-config gnupg software-properties-common wget

RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN add-apt-repository -y ppa:mhier/libboost-latest
RUN apt update
RUN apt install -y libstdc++-13-dev
RUN apt install -y gcc-13 g++-13

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
  libgmock-dev \
  libgmp-dev \
  libgtest-dev \
  libmimalloc-dev \
  libtbb-dev \
  liburing-dev \
  libzstd-dev

WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y net-tools iputils-ping iproute2

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
COPY --from=monad-bft-builder /output/lib/*.so /usr/local/lib
COPY --from=monad-bft-builder /output/bin/monad-node /usr/local/bin/monad-node

ENV RUST_LOG=debug
