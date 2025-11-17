FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y python3 clang

ARG GIT_COMMIT=""
ARG GIT_BRANCH=""
ARG GIT_TAG=""
ARG GIT_MODIFIED="false"

ENV GIT_COMMIT=$GIT_COMMIT
ENV GIT_BRANCH=$GIT_BRANCH
ENV GIT_TAG=$GIT_TAG
ENV GIT_MODIFIED=$GIT_MODIFIED

# Builder
COPY . .
RUN cargo build --release --example service && \
    mv target/release/examples/service service
RUN python3 docker/gossip-testground/topology-gen.py topo.json
RUN python3 monad-scripts/tc/tc-gen.py topo.json tc.sh addresses

# Runner
FROM debian:buster-slim
SHELL ["/bin/bash", "-c"]
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y iproute2
RUN apt clean
COPY --from=builder /usr/src/monad-bft/service /usr/local/bin/service
COPY --from=builder /usr/src/monad-bft/tc.sh .
COPY --from=builder /usr/src/monad-bft/addresses .

ENV RUST_LOG=service=INFO
CMD source tc.sh && service --addresses $(<addresses)
