#!/bin/bash

set -ex
wasm-pack build
cargo run --bin schema-gen
(cd frontend && pnpm install && pnpm codegen && pnpm build)
