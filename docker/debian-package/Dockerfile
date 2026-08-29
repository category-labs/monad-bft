# syntax=docker/dockerfile:1

# Toolchain comes from docker/builder/Dockerfile, the image Jenkins builds with.
# `make deb-container` builds it locally; override to reuse a prebuilt copy.
ARG BUILDER_IMAGE=monad-bft-builder:local
FROM ${BUILDER_IMAGE} AS package

ARG PACKAGE_VERSION

ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    LIBCLANG_PATH=/usr/lib/llvm-19/lib

# Only what docker/builder does not already carry: libclang for bindgen,
# build-essential for the packaging tools (including dpkg-shlibdeps), and
# patchelf to remove build-tree runpaths before dependency analysis.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libclang-19-dev patchelf \
    && rm -rf /var/lib/apt/lists/*

RUN test -n "$PACKAGE_VERSION"

# monad-version resolves these from git, then falls back to the environment.
# .dockerignore strips **/.git, so without them the binaries report no
# provenance. Same contract as docker/rpc/Dockerfile.
ARG GIT_COMMIT=""
ARG GIT_BRANCH=""
ARG GIT_TAG=""
ARG GIT_MODIFIED="false"
# Stamps monad / monad-cli / monad-mpt via monad-execution's cmake.
ARG GIT_COMMIT_HASH=""

ENV GIT_COMMIT=$GIT_COMMIT \
    GIT_BRANCH=$GIT_BRANCH \
    GIT_TAG=$GIT_TAG \
    GIT_MODIFIED=$GIT_MODIFIED \
    GIT_COMMIT_HASH=$GIT_COMMIT_HASH

WORKDIR /src
COPY . .

# A real release checkout must include the pinned execution repository.  This is
# deliberately verified here instead of cloning an unpinned dependency at build time.
RUN test -f monad-execution/CMakeLists.txt

RUN --mount=type=cache,id=monad-bft-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=monad-bft-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=monad-bft-target,target=/src/target \
    --mount=type=cache,id=monad-bft-execution-build,target=/src/monad-execution/build,sharing=locked \
    PACKAGE_VERSION="${PACKAGE_VERSION}" DEB_OUTPUT_DIR=/out ./scripts/build-deb

FROM scratch
COPY --from=package /out /out
