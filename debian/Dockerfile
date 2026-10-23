# syntax=docker/dockerfile:1

FROM ubuntu:26.04 AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      binutils build-essential ca-certificates clang-19 cmake g++-15 gcc-15 git \
      libabsl-dev libarchive-dev libbenchmark-dev libboost-all-dev libbrotli-dev libcap-dev \
      libcgroup-dev libclang-19-dev libcli11-dev libcrypto++-dev libgmp-dev \
      libgmock-dev libgtest-dev libhugetlbfs-dev libmagicenum-dev libmimalloc-dev \
      libsecp256k1-dev libssl-dev \
      debhelper-compat dpkg-dev libstdc++-15-dev libtbb-dev liburing-dev libzstd-dev \
      ninja-build patchelf pax-utils pkg-config rustup \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

ARG PACKAGE_VERSION
ARG GIT_COMMIT
ARG GIT_BRANCH
ARG GIT_TAG
ARG GIT_MODIFIED
ARG GIT_COMMIT_HASH
ARG STRIP_SYMBOLS

RUN --mount=type=cache,id=monad-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=monad-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=monad-target,target=/src/target \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    PACKAGE_VERSION="$PACKAGE_VERSION" \
    GIT_COMMIT="$GIT_COMMIT" GIT_BRANCH="$GIT_BRANCH" GIT_TAG="$GIT_TAG" \
    GIT_MODIFIED="$GIT_MODIFIED" GIT_COMMIT_HASH="$GIT_COMMIT_HASH" \
    DEB_OUTPUT_DIR=/out STRIP_SYMBOLS="$STRIP_SYMBOLS" \
    ./debian/scripts/package-deb

FROM scratch
COPY --from=build /out/ /
