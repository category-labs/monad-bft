// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;

use chrono::{DateTime, Local, TimeZone, Utc};
use eyre::{eyre, Context, Result};
use futures::future::join_all;
use reqwest::Client;
use serde::{de::Error as _, Deserialize, Serialize};
use serde_json::Value;
use tracing::error;

pub async fn join_stats(
    prom_url: Option<String>,
    start_dt: DateTime<Utc>,
    end_dt: DateTime<Utc>,
) -> Result<(HashMap<String, CounterStatsReport>, String)> {
    // Prometheus base URL
    let prom_url = match prom_url {
        Some(url) => url,
        None => std::env::var("PROM_URL").wrap_err("prometheus url not set")?,
    };

    // Start and end - default to last 15 minutes, but allow env overrides START and END (RFC3339)
    if end_dt <= start_dt {
        return Err(eyre!("END must be after START"));
    }

    let step = "15s".to_string();
    let tags = r#"job=~"stressnet_.+",job!~"stressnet-[0-9]+.*",source=~".*(external|internal).*""#;

    let specs = [
        CounterSpec {
            metric: "monad_bft_txpool_create_proposal_elapsed_ns",
            label: "BFT Proposal Creation Latency",
            unit: "ms",
            scale: 1.0 / 1_000_000.0,
            query: format!(
                "quantile(0.5, avg(monad_bft_txpool_create_proposal_elapsed_ns{{{tags}}}[1m]))"
            ),
        },
        CounterSpec {
            metric: "monad_state_consensus_events_local_timeout",
            label: "Consensus Local Timeout Events",
            unit: "count",
            scale: 1.0,
            query: format!("max(rate(monad_state_consensus_events_local_timeout{{{tags}}}[1m]))"),
        },
        CounterSpec {
            metric: "monad_state_consensus_events_proposal_with_tc",
            label: "Consensus Proposals with TC",
            unit: "count",
            scale: 1.0,
            query: format!(
                "max(rate(monad_state_consensus_events_proposal_with_tc{{{tags}}}[1m]))"
            ),
        },
        CounterSpec {
            metric: "monad_state_consensus_events_creating_proposal",
            label: "Consensus Creating Proposal Events",
            unit: "count",
            scale: 1.0,
            query: format!(
                "max(rate(monad_state_consensus_events_creating_proposal{{{tags}}}[1m]))"
            ),
        },
        CounterSpec {
            metric: "monad_execution_ledger_num_tx_commits",
            label: "Execution Ledger TX Commits",
            unit: "ops/sec",
            scale: 1.0,
            query: format!(
                "quantile(0.5, rate(monad_execution_ledger_num_tx_commits{{{tags}}}[1m]))"
            ),
        },
    ];

    println!(
        "Interval: {} -> {}  step={}",
        start_dt.to_rfc3339(),
        end_dt.to_rfc3339(),
        step
    );

    let stats = join_all(specs.iter().map(|spec| {
        get_stats_for_counter(
            prom_url.clone(),
            spec.metric.to_string(),
            start_dt,
            end_dt,
            step.clone(),
            spec.query.clone(),
        )
    }))
    .await;

    let mut formatted = String::with_capacity(32_768);
    let mut reports = HashMap::new();
    for (spec, report) in specs.iter().zip(stats) {
        if let Err(e) = report {
            error!("Error getting stats for {}: {}", spec.metric, e);
            continue;
        }
        let mut report = report.unwrap();
        if (spec.scale - 1.0).abs() > f64::EPSILON {
            scale_report(&mut report, spec.scale);
        }

        let report_str = format_report(&report, spec);
        formatted.push_str(&report_str);
        reports.insert(spec.metric.to_string(), report);
    }

    Ok((reports, formatted))
}

/// Parse a Prometheus instant vector JSON into (timestamp, value) in local time - left here from your original code.
pub fn parse_prometheus_scalar(v: &Value) -> Result<(DateTime<Local>, f64)> {
    let arr = v["data"]["result"][0]["value"]
        .as_array()
        .ok_or_else(|| eyre!("missing or malformed value array"))?;

    if arr.len() != 2 {
        return Err(eyre!("expected 2 elements in value array"));
    }

    // keep local helper functional by parsing generically
    let (ts_sec, ts_nanos) = parse_timestamp_value(&arr[0]).map_err(|e| eyre!("{e}"))?;
    let dt = Local
        .timestamp_opt(ts_sec, ts_nanos)
        .single()
        .ok_or_else(|| eyre!("invalid timestamp"))?;

    let val_str = arr[1]
        .as_str()
        .ok_or_else(|| eyre!("value is not a string"))?;
    let val_f64: f64 = val_str.parse()?;

    Ok((dt, val_f64))
}

struct CounterSpec {
    metric: &'static str,
    label: &'static str,
    unit: &'static str,
    scale: f64,
    query: String,
}

fn scale_counter_stats(stats: &mut CounterStats, scale: f64) {
    stats.mean *= scale;
    stats.p25 *= scale;
    stats.p50 *= scale;
    stats.p90 *= scale;
    stats.p99 *= scale;
    stats.variance *= scale * scale;
}

fn scale_report(report: &mut CounterStatsReport, scale: f64) {
    scale_counter_stats(&mut report.overall, scale);
    for quarter in report.quarters.iter_mut().flatten() {
        scale_counter_stats(quarter, scale);
    }
}

fn format_report(report: &CounterStatsReport, spec: &CounterSpec) -> String {
    let mut s = String::with_capacity(1024);
    s.push_str(&format!(
        "\n=== {} ({}) [{}] ===\n",
        spec.label, spec.metric, spec.unit
    ));
    s.push_str(&format!("Samples: {}\n", report.overall.samples));
    s.push_str(&format!(
        "Overall: mean={:.6}, var={:.6}, p25={:.6}, p50={:.6}, p90={:.6}, p99={:.6}\n",
        report.overall.mean,
        report.overall.variance,
        report.overall.p25,
        report.overall.p50,
        report.overall.p90,
        report.overall.p99
    ));

    let labels = ["Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4"];
    s.push_str("Quarters:\n");
    for (label, stats) in labels.iter().zip(report.quarters.iter()) {
        match stats {
            Some(ss) => s.push_str(&format!(
                "  {:>9}: mean={:.3}, var={:.3}, p50={:.3}, p99={:.3}\n",
                label, ss.mean, ss.variance, ss.p50, ss.p99
            )),
            None => s.push_str(&format!("  {:>9}: (insufficient data)\n", label)),
        }
    }
    s
}

use super::stats::{
    mean_by_timestamp, parse_timestamp_value, timestamp_parts_to_ns, weighted_stats,
    windowed_weighted_stats,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CounterStats {
    pub mean: f64,
    pub variance: f64,
    pub p25: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub samples: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterStatsReport {
    pub counter: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub step: String,
    pub overall: CounterStats,
    pub quarters: [Option<CounterStats>; 4],
}

#[derive(Debug, Deserialize)]
struct PromQueryRangeResponse {
    status: String,
    data: PromRangeData,
}

#[derive(Debug, Deserialize)]
struct PromRangeData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PromSeries>,
}

#[derive(Debug, Deserialize)]
struct PromSeries {
    #[allow(dead_code)]
    metric: std::collections::HashMap<String, String>,
    // Each entry is [timestamp_seconds, "value_as_string"]
    values: Vec<PromSample>,
}

#[derive(Debug)]
struct PromSample {
    timestamp_ns: u64,
    value: String,
}

impl<'de> Deserialize<'de> for PromSample {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (timestamp_raw, value) = <(Value, String)>::deserialize(deserializer)?;
        let (seconds, nanos) = parse_timestamp_value(&timestamp_raw).map_err(D::Error::custom)?;
        let timestamp_ns = timestamp_parts_to_ns(seconds, nanos).map_err(D::Error::custom)?;
        Ok(Self {
            timestamp_ns,
            value,
        })
    }
}

async fn fetch_counter_series(
    client: &Client,
    prom_url: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step: &str,
    promql: String,
) -> Result<Vec<(u64, f64)>> {
    if end <= start {
        return Err(eyre!("END must be after START"));
    }

    let resp: PromQueryRangeResponse = client
        .get(format!("{}/api/v1/query_range", prom_url))
        .query(&[
            ("query", promql.as_str()),
            ("start", &start.to_rfc3339()),
            ("end", &end.to_rfc3339()),
            ("step", step),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    if resp.status != "success" {
        return Err(eyre!(
            "Prometheus returned non success status: {}",
            resp.status
        ));
    }
    if resp.data.result_type != "matrix" {
        return Err(eyre!(
            "expected matrix resultType, got {}",
            resp.data.result_type
        ));
    }
    if resp.data.result.is_empty() {
        return Err(eyre!("empty result from Prometheus"));
    }

    let series_list: Vec<Vec<(u64, f64)>> = resp
        .data
        .result
        .iter()
        .map(|s| {
            s.values
                .iter()
                .filter_map(|sample| match sample.value.parse::<f64>() {
                    Ok(val) if val.is_finite() => Some((sample.timestamp_ns, val)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let aligned = mean_by_timestamp(&series_list)?;
    Ok(aligned)
}

async fn collect_counter_stats(
    prom_url: String,
    counter: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step: String,
    promql: String,
) -> Result<CounterStatsReport> {
    let client = Client::builder()
        .no_proxy()
        .build()
        .map_err(|e| eyre!("failed to build HTTP client: {e}"))?;
    let series = fetch_counter_series(&client, &prom_url, start, end, &step, promql).await?;

    let overall =
        weighted_stats(&series).ok_or_else(|| eyre!("not enough samples to compute statistics"))?;

    let start_ns = timestamp_parts_to_ns(start.timestamp(), start.timestamp_subsec_nanos())
        .map_err(|e| eyre!("{e}"))?;
    let end_ns = timestamp_parts_to_ns(end.timestamp(), end.timestamp_subsec_nanos())
        .map_err(|e| eyre!("{e}"))?;
    let dur_ns = end_ns.saturating_sub(start_ns);
    let q1 = start_ns.saturating_add(dur_ns / 4);
    let q2 = start_ns.saturating_add(dur_ns / 2);
    let q3 = start_ns.saturating_add((dur_ns / 4) * 3);
    let windows = vec![(start_ns, q1), (q1, q2), (q2, q3), (q3, end_ns)];
    let quarters_vec = windowed_weighted_stats(&series, &windows);
    let quarters: [Option<CounterStats>; 4] = [
        quarters_vec.first().cloned().unwrap_or(None),
        quarters_vec.get(1).cloned().unwrap_or(None),
        quarters_vec.get(2).cloned().unwrap_or(None),
        quarters_vec.get(3).cloned().unwrap_or(None),
    ];

    Ok(CounterStatsReport {
        counter,
        start,
        end,
        step,
        overall,
        quarters,
    })
}

pub async fn get_stats_for_counter(
    prom_url: String,
    counter: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step: String,
    promql: String,
) -> Result<CounterStatsReport> {
    let handle = tokio::spawn(collect_counter_stats(
        prom_url, counter, start, end, step, promql,
    ));
    handle.await.map_err(|e| eyre!("task join error: {e}"))?
}
