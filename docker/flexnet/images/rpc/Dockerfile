# RPC server
FROM debian:buster-slim
WORKDIR /usr/src/monad-bft

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

COPY --from=monad-bft-builder /output/bin/monad-rpc /usr/local/bin/monad-rpc
COPY --from=monad-bft-builder /output/lib/*.so /usr/local/lib

ENV RUST_LOG=info