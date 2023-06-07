#!/bin/bash
cargo bench 2>&1 | tee bench_output.txt
