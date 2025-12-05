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
RUN cargo build --release --bin monad-testground && \
    mv target/release/monad-testground testground
RUN python3 docker/testground/topology-gen.py topo.json
RUN python3 monad-scripts/tc/tc-gen.py topo.json tc.sh addresses

# Runner
FROM debian:buster-slim
SHELL ["/bin/bash", "-c"]
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y iproute2
RUN apt clean
COPY --from=builder /usr/src/monad-bft/testground /usr/local/bin/monad-testground
COPY --from=builder /usr/src/monad-bft/tc.sh .
COPY --from=builder /usr/src/monad-bft/addresses .

ENV RUST_LOG=monad_testground=DEBUG
CMD source tc.sh && MONAD_MEMPOOL_RNDUDS=true monad-testground -o http://jaeger:4317 --addresses $(<addresses)
