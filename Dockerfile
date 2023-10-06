FROM rust:1-buster AS chef 
RUN cargo install cargo-chef 
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y python3 clang

# Build dependencies (docker layer cache)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release --bin monad-testground
RUN python3 tc-gen.py > tc.sh


FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y iproute2
RUN apt clean
COPY --from=builder /usr/src/monad-bft/target/release/monad-testground /usr/local/bin/monad-testground
COPY --from=builder /usr/src/monad-bft/tc.sh .

ENV RUST_LOG=monad_testground=DEBUG
CMD ["bash", "tc.sh"]
