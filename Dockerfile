FROM rust:1-buster AS chef 
RUN cargo install cargo-chef 
WORKDIR app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y python3

COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo install --path monad-node
RUN python3 tc-gen.py > tc.sh


FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y iproute2
COPY --from=builder /usr/local/cargo/bin/monad-node /usr/local/bin/monad-node
COPY --from=builder /usr/src/monad-bft/tc.sh .

ENV RUST_LOG=monad_consensus=DEBUG
CMD ["bash", "tc.sh"]
