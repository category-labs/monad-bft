FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y python3 clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --bin monad-testground && \
    mv target/release/monad-testground testground
RUN python3 tc-gen.py > tc.sh

# Runner
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y iproute2
RUN apt clean
COPY --from=builder /usr/src/monad-bft/testground /usr/local/bin/monad-testground
COPY --from=builder /usr/src/monad-bft/tc.sh .

ENV RUST_LOG=monad_testground=DEBUG
CMD ["bash", "tc.sh"]
