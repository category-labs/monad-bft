# Raptorcast native histogram prototype

Raptorcast now records decoded-message latency in the cumulative histogram
`monad_raptorcast_broadcast_latency_seconds{mode="primary|secondary"}`.
The existing sender timestamp and decode completion point are preserved, including
skipping future timestamps and unspecified broadcast modes.

The reusable API lives in `monad-executor`. Named fields cache the histogram
handles at initialization; recording needs no string lookup:

```rust
monad_executor::histogram_labels! {
    struct BroadcastLatency {
        primary => "primary",
        secondary => "secondary",
    }
}

let latency = BroadcastLatency::new(&mut metrics, BROADCAST_LATENCY_SECONDS, "mode");
latency.primary.observe_duration(Duration::from_millis(12));
latency.secondary.observe_duration(Duration::from_millis(25));
```

`ExecutorMetrics::native_histogram` provides an unlabeled handle, and `observe`
accepts a numeric observation. `observe_duration` converts durations to seconds.
Initialize families before constructing the export registries, as with counters.

## Run the demonstration

The build requires the repository's pinned `monad-execution` submodule revision.
From the repository root, with that submodule initialized:

```sh
cargo run -p monad-raptorcast --example native_histograms
```

This starts a local protobuf endpoint at `127.0.0.1:19100/metrics`. Each scrape
records six synthetic primary latencies (1, 2, 5, 10, 20, 50 ms) through
`UdpStateMetrics::record_broadcast_latency`, plus six secondary latencies twice
as long. This exercises the real raptorcast metrics path; it does not send network
broadcasts. An optional first argument changes the listening address.

Use this configuration with Prometheus 3.14.0 (the version used to verify the demo):

```yaml
global:
  scrape_interval: 1s
  scrape_timeout: 1s
  scrape_native_histograms: true
  scrape_protocols: [PrometheusProto]
scrape_configs:
  - job_name: raptorcast-native
    static_configs:
      - targets: ['127.0.0.1:19100']
```

Save it as `/tmp/raptorcast-prometheus.yml`, then run on Linux:

```sh
docker run --rm --network host \
  --mount type=bind,source=/tmp/raptorcast-prometheus.yml,target=/etc/prometheus/prometheus.yml,readonly \
  prom/prometheus:v3.14.0 \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --web.listen-address=127.0.0.1:19101
```

Open http://127.0.0.1:19101 and allow a few scrapes before querying:

```promql
histogram_quantile(0.99,
  sum by (mode) (rate(monad_raptorcast_broadcast_latency_seconds[30s]))
)
```

Expect approximately 0.052 seconds for primary and 0.105 seconds for secondary.
Native bucket interpolation makes these estimates slightly different from the
largest input samples. Each scrape adds six observations per mode:

```promql
histogram_count(monad_raptorcast_broadcast_latency_seconds)
```

## Integration and migration

The node exports the new family on its existing `/metrics` endpoint, with the same
global labels as scalar metrics. Protobuf scrapes include native buckets; text
scrapes include classic buckets, sum, and count. Native ingestion must be enabled
in Prometheus. Configuration reference:
https://prometheus.io/docs/prometheus/latest/configuration/configuration/

The prototype uses `prometheus-client` for histograms alongside the existing
`prometheus` scalar registry. Both produce compatible length-delimited protobuf
MetricFamily messages, which the endpoint combines. This avoids replacing all
existing collectors. The protobuf build uses the Rust `protox` compiler.

The four previous primary/secondary p99 and count gauges are removed. Dashboards
must use the new histogram queries. Observations no longer reset after 30 seconds;
choose the query window in PromQL. The old p99 gauges used milliseconds; the new
family uses seconds. Cross-node aggregation can sum histograms before computing
quantiles, as shown above.

Defaults request a native bucket factor of 1.1 and a best-effort bucket limit of
256. Classic fallback buckets cover 100 microseconds through 10 seconds, with an
overflow bucket. Long observations remain represented by native buckets. These
are latency-oriented defaults; a configurable bucket policy can be added for
other distributions. The client's histogram uses a lock and maintains both
representations; recording cost still needs benchmarking before rollout.
