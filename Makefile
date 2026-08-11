SHELL := /bin/sh

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
ifeq ($(origin PACKAGE_VERSION),undefined)
PACKAGE_VERSION := $(shell ./scripts/package-version)
endif
DEB_OUTPUT_DIR ?= dist
BUILDER_IMAGE ?= monad-bft-builder:local

.PHONY: build builder deb deb-container clean

build:
	PACKAGE_VERSION=$(PACKAGE_VERSION) ./scripts/build-binaries

deb:
	PACKAGE_VERSION=$(PACKAGE_VERSION) DEB_OUTPUT_DIR=$(DEB_OUTPUT_DIR) ./scripts/build-deb

builder:
	@test -n "$(CONTAINER_ENGINE)" || { echo "Install Docker or Podman, or set CONTAINER_ENGINE."; exit 1; }
	$(CONTAINER_ENGINE) build -t $(BUILDER_IMAGE) docker/builder

deb-container: builder
	$(CONTAINER_ENGINE) build \
		--build-arg BUILDER_IMAGE=$(BUILDER_IMAGE) \
		--build-arg PACKAGE_VERSION=$(PACKAGE_VERSION) \
		-t monad-bft-package:$(PACKAGE_VERSION) -f containers/Containerfile .
	@mkdir -p $(DEB_OUTPUT_DIR); container_id=$$($(CONTAINER_ENGINE) create monad-bft-package:$(PACKAGE_VERSION) true); trap '$(CONTAINER_ENGINE) rm -f $$container_id >/dev/null' EXIT; $(CONTAINER_ENGINE) cp $$container_id:/out/. $(DEB_OUTPUT_DIR)

clean:
	rm -rf $(DEB_OUTPUT_DIR)
