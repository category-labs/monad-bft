FROM rust:1-buster AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN cargo build --release --bin monad-testground && \
    mv target/release/monad-testground testground
RUN cp monad-scripts/testground/topology-gen.py .
RUN cp monad-scripts/testground/tc-gen.py .
RUN cp monad-scripts/testground/run_testground.sh .

# Runner
FROM debian:buster-slim
SHELL ["/bin/bash", "-c"]
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y ethtool iproute2 net-tools python3 tcpdump
RUN apt clean
COPY --from=builder /usr/src/monad-bft/testground /usr/local/bin/monad-testground
COPY --from=builder /usr/src/monad-bft/topology-gen.py .
COPY --from=builder /usr/src/monad-bft/tc-gen.py .
COPY --from=builder /usr/src/monad-bft/run_testground.sh .
RUN chmod u+x run_testground.sh

CMD ./run_testground.sh -l
