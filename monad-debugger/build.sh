#!/bin/bash

set -ex
wasm-pack build
cargo run --bin schema_gen
(cd frontend && pnpm install && pnpm codegen && pnpm build)
