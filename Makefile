SHELL := /bin/sh

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
PACKAGE_VERSION ?= $(shell ./debian/scripts/dev-version)
GIT_COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
GIT_BRANCH ?= $(shell git symbolic-ref --short -q HEAD 2>/dev/null || echo detached)
GIT_TAG ?= $(shell git describe --tags --exact-match HEAD 2>/dev/null || echo untagged)
GIT_MODIFIED ?= $(shell test -n "$$(git status --porcelain --ignore-submodules=all 2>/dev/null)" && echo true || echo false)
GIT_COMMIT_HASH ?= $(shell git -C monad-execution rev-parse HEAD 2>/dev/null || echo unknown)
DEB_OUTPUT_DIR ?= dist
STRIP_SYMBOLS ?= 1

export CONTAINER_ENGINE PACKAGE_VERSION GIT_COMMIT GIT_BRANCH GIT_TAG GIT_MODIFIED GIT_COMMIT_HASH DEB_OUTPUT_DIR STRIP_SYMBOLS

.PHONY: deb deb-host clean

deb:
	./debian/scripts/package-deb-containerized

deb-host:
	./debian/scripts/package-deb

clean:
	rm -rf '$(DEB_OUTPUT_DIR)'
