# ethtx: a tool to send random eth transactions over UNIX sockets

FROM ubuntu:24.04
WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y openssl

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

COPY --from=monad-bft-builder /output/bin/debug-node /usr/local/bin/debug-node
COPY --from=monad-bft-builder /output/lib/*.so /usr/local/lib

ENV RUST_LOG=info
