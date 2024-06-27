# metricswatch

A program to collect execution client metrics by polling triedb.

### Getting started

```sh
CXX=/usr/bin/g++-13 CC=/usr/bin/gcc-13 ASMFLAGS=-march=haswell CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" TRIEDB_TARGET=triedb_driver cargo run --example metricswatch -- --triedb-path <path> --record-metrics-interval-seconds <metrics_collection_interval> --otel-endpoint <endpoint>
```