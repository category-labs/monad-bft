FROM rust:1-buster AS builder
WORKDIR /usr/src/cmake
RUN apt update
RUN apt install -y clang

RUN wget https://cmake.org/files/v3.20/cmake-3.20.0-linux-x86_64.sh 
RUN bash cmake-3.20.0-linux-x86_64.sh --skip-license
ENV PATH="/usr/src/cmake/bin:$PATH" 

RUN mkdir -p /output/bin
RUN mkdir -p /output/lib

WORKDIR /usr/src/monad-bft
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry     \
    --mount=type=cache,target=/usr/local/cargo/git          \
    --mount=type=cache,target=/usr/src/monad-bft/target     \
    cargo build --release --bin monad-node --bin monad-rpc --example ethtx
    
RUN --mount=type=cache,target=/usr/src/monad-bft/target     \
    find target/release/ -maxdepth 1 -perm /a+x -type f -exec cp {} /output/bin \; && \
    find target/release/examples -maxdepth 1 -perm /a+x -type f -exec cp {} /output/bin \; && \
    cp `find target/release | grep libtriedb` /output/lib


FROM debian:buster-slim AS exporter
WORKDIR /output

COPY --from=builder /output /output
