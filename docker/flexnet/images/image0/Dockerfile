# Default monad node

# runner stage
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y net-tools iputils-ping iproute2

COPY --from=monad-bft-builder /output/bin/monad-node /usr/local/bin/monad-node

ENV RUST_LOG=info
