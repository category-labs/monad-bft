# Builder
FROM ubuntu:22.04 AS builder

RUN apt update
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
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /usr/src/monad-bft
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    CC=gcc-13 CXX=g++-13 cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc rpc && \
    cp `find target/release | grep -E "(libmock_eth_call|libtriedb)"` .

# Runner
FROM ubuntu:22.04
WORKDIR /usr/src/monad-bft

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

COPY --from=builder /usr/src/monad-bft/rpc /usr/local/bin/monad-rpc
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib

ENV RUST_LOG=info
CMD monad-rpc \
    --ipc-path /monad/mempool.sock \
    --blockdb-path /monad/blockdb
