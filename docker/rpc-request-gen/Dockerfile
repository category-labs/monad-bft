FROM ubuntu:25.04 AS base

COPY monad-execution/scripts/ubuntu-build /opt

RUN apt update && \
    apt upgrade -y && \
    apt install -y \
      binutils \
      cgroup-tools \
      iproute2 \
      libmimalloc-dev \
      rustup && \
    /opt/install-boost.sh && \
    /opt/install-tools.sh && \
    /opt/install-deps.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm -rf /opt

RUN rustup toolchain install 1.91.1 --profile minimal

FROM base AS builder

WORKDIR /usr/src/monad-bft

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
RUN cargo build --release --example rpc-request-generator && \
    mv target/release/examples/rpc-request-generator rpc-request-generator

# Runner
FROM ubuntu:25.04 as runner
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/rpc-request-generator /usr/local/bin/rpc-request-generator

ENV RUST_LOG=info
CMD rpc-request-generator