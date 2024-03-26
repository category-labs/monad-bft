# Builder
FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang
RUN wget https://cmake.org/files/v3.20/cmake-3.20.0-linux-x86_64.sh 
RUN bash cmake-3.20.0-linux-x86_64.sh --skip-license
ENV PATH="/usr/src/cmake/bin:$PATH" 

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc rpc && \
    cp `find target/release | grep libtriedb` .

# Runner
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

COPY --from=builder /usr/src/monad-bft/rpc /usr/local/bin/monad-rpc
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib

ENV RUST_LOG=info
CMD monad-rpc \
    --ipc-path /monad/mempool.sock \
    --blockdb-path /monad/blockdb
