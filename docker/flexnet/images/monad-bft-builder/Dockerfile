FROM ubuntu:24.04 AS builder

RUN apt update
RUN apt-get update
RUN apt install -y ca-certificates \
    curl \
    gnupg \
    software-properties-common \
    wget \
    pkg-config \
    libssl-dev \
    clang

RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt install -y libstdc++-13-dev
RUN apt install -y gcc-13 g++-13

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

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"

RUN mkdir -p /output/bin
RUN mkdir -p /output/lib

WORKDIR /usr/src/monad-bft
COPY . .
RUN --mount=type=cache,target=${CARGO_ROOT}/registry     \
    --mount=type=cache,target=${CARGO_ROOT}/git          \
    --mount=type=cache,target=/usr/src/monad-bft/target     \
    CC=gcc-13 CXX=g++-13 cargo build --release --bin monad-node --bin monad-rpc --example ethtx
    
RUN --mount=type=cache,target=/usr/src/monad-bft/target     \
    find target/release/ -maxdepth 1 -perm /a+x -type f -exec cp {} /output/bin \; && \
    find target/release/examples -maxdepth 1 -perm /a+x -type f -exec cp {} /output/bin \; && \
    cp `ls -lt $(find target/release | grep libmock_eth_call) | head -1 | awk '{print $9}'` /output/lib && \
    cp `ls -lt $(find target/release | grep libtriedb) | head -1 | awk '{print $9}'` /output/lib


FROM ubuntu:22.04 AS exporter
WORKDIR /output

COPY --from=builder /output /output
