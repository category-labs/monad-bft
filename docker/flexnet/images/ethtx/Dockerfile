# ethtx: a tool to send random eth transactions over UNIX sockets

FROM ubuntu:22.04
WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y openssl

COPY --from=monad-bft-builder /output/bin/ethtx /usr/local/bin/ethtx

ENV RUST_LOG=info
