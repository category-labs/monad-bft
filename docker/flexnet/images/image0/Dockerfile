# Default monad node

# runner stage
FROM ubuntu:24.04
WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y net-tools iputils-ping iproute2

COPY --from=monad-bft-builder /output/bin/monad-node /usr/local/bin/monad-node

ENV RUST_LOG=debug
