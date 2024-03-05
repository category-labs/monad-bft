FROM rust:1-buster AS builder
WORKDIR /usr/src/cmake
RUN apt update
RUN apt install -y clang

RUN wget https://cmake.org/files/v3.20/cmake-3.20.0-linux-x86_64.sh 
RUN bash cmake-3.20.0-linux-x86_64.sh --skip-license
ENV PATH="/usr/src/cmake/bin:$PATH" 

RUN mkdir /output

WORKDIR /usr/src/monad-bft
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry     \
    --mount=type=cache,target=/usr/local/cargo/git          \
    --mount=type=cache,target=/usr/src/monad-bft/target     \
    cargo build --release --bin monad-node --bin monad-rpc --example ethtx && \
    cp -r target/release /output
    
RUN mkdir /output/release/lib && cp `find /output/release | grep libtriedb` /output/release/lib
