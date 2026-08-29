SHELL := /bin/sh

ifeq ($(origin PACKAGE_VERSION),undefined)
PACKAGE_VERSION := $(shell ./scripts/package-version)
endif

.PHONY: build

build:
	PACKAGE_VERSION=$(PACKAGE_VERSION) ./scripts/build-binaries
