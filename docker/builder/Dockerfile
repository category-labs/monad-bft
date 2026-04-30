FROM ubuntu:25.04 AS base

COPY monad-execution/scripts/ubuntu-build /opt

RUN apt update && \
    apt install -y \
      binutils \
      cgroup-tools \
      libhugetlbfs0 \
      libmimalloc-dev \
      iproute2 \
      rustup && \
    /opt/install-boost.sh && \
    /opt/install-tools.sh && \
    /opt/install-deps.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm -rf /opt

RUN rustup toolchain install 1.91.1 --profile minimal
