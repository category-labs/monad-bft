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

use std::collections::BTreeMap;

use eyre::Result;
use serde_json::Value;

use super::CounterStats;

pub const NANOS_PER_SECOND: u64 = 1_000_000_000;
pub const NANOS_PER_SECOND_F64: f64 = NANOS_PER_SECOND as f64;

pub fn mean_by_timestamp(series_list: &[Vec<(u64, f64)>]) -> Result<Vec<(u64, f64)>> {
    let mut buckets: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
    for series in series_list {
        for (t, v) in series {
            buckets.entry(*t).or_default().push(*v);
        }
    }
    if buckets.is_empty() {
        return Err(eyre::eyre!("no samples after parsing"));
    }
    let mut out = Vec::with_capacity(buckets.len());
    for (t, vs) in buckets {
        if vs.is_empty() {
            continue;
        }
        let sum: f64 = vs.iter().copied().sum();
        let mean = sum / (vs.len() as f64);
        out.push((t, mean));
    }
    Ok(out)
}

pub fn time_weighted_average(samples: &[(u64, f64)]) -> Option<f64> {
    if samples.len() == 1 {
        return Some(samples[0].1);
    }
    let mut area = 0.0;
    let mut duration = 0.0;
    for w in samples.windows(2) {
        let (t0, v0) = w[0];
        let (t1, v1) = w[1];
        let dt_ns = match t1.checked_sub(t0) {
            Some(delta) if delta > 0 => delta,
            _ => continue,
        };
        let dt_secs = dt_ns as f64 / NANOS_PER_SECOND_F64;
        area += (v0 + v1) * 0.5 * dt_secs;
        duration += dt_secs;
    }
    if duration > 0.0 {
        Some(area / duration)
    } else {
        None
    }
}

pub fn weighted_stats(samples: &[(u64, f64)]) -> Option<CounterStats> {
    if samples.is_empty() {
        return None;
    }
    if samples.len() == 1 {
        let x = samples[0].1;
        return Some(CounterStats {
            mean: x,
            variance: 0.0,
            p25: x,
            p50: x,
            p90: x,
            p99: x,
            samples: 1,
        });
    }

    let mut segments: Vec<(f64, f64)> = Vec::with_capacity(samples.len().saturating_sub(1));
    for w in samples.windows(2) {
        let (t0, v0) = w[0];
        let (t1, _v1) = w[1];
        let Some(dt_ns) = t1.checked_sub(t0) else {
            continue;
        };
        if dt_ns == 0 {
            continue;
        }
        let dt = dt_ns as f64 / NANOS_PER_SECOND_F64;
        if dt.is_finite() && dt > 0.0 {
            segments.push((v0, dt));
        }
    }

    if segments.is_empty() {
        let x = samples[0].1;
        return Some(CounterStats {
            mean: x,
            variance: 0.0,
            p25: x,
            p50: x,
            p90: x,
            p99: x,
            samples: samples.len(),
        });
    }

    let total_w: f64 = segments.iter().map(|&(_, w)| w).sum();
    if !(total_w > 0.0) || !total_w.is_finite() {
        let x = samples[0].1;
        return Some(CounterStats {
            mean: x,
            variance: 0.0,
            p25: x,
            p50: x,
            p90: x,
            p99: x,
            samples: samples.len(),
        });
    }

    let sum_wx: f64 = segments.iter().map(|&(x, w)| x * w).sum();
    let sum_wx2: f64 = segments.iter().map(|&(x, w)| x * x * w).sum();
    let mean = sum_wx / total_w;
    let ex2 = sum_wx2 / total_w;
    let variance = (ex2 - mean * mean).max(0.0);

    let mut sorted = segments.clone();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let p = |q: f64| -> f64 {
        let target = q * total_w;
        let mut acc = 0.0;
        for (x, w) in &sorted {
            acc += *w;
            if acc >= target {
                return *x;
            }
        }
        sorted.last().map(|(x, _)| *x).unwrap_or(mean)
    };

    Some(CounterStats {
        mean,
        variance,
        p25: p(0.25),
        p50: p(0.50),
        p90: p(0.90),
        p99: p(0.99),
        samples: samples.len(),
    })
}

pub fn windowed_weighted_stats(
    samples: &[(u64, f64)],
    windows: &[(u64, u64)],
) -> Vec<Option<CounterStats>> {
    if samples.is_empty() {
        return vec![None; windows.len()];
    }

    // Precompute segments [t_i, t_{i+1}) with value v_i
    let mut segments: Vec<(u64, u64, f64)> = Vec::with_capacity(samples.len().saturating_sub(1));
    for w in samples.windows(2) {
        let (t0, v0) = w[0];
        let (t1, _v1) = w[1];
        let Some(dt_ns) = t1.checked_sub(t0) else {
            continue;
        };
        if dt_ns == 0 {
            continue;
        }
        segments.push((t0, t1, v0));
    }

    windows
        .iter()
        .map(|(ws, we)| {
            if we <= ws {
                return None;
            }
            let mut total_w = 0.0;
            let mut sum_wx = 0.0;
            let mut sum_wx2 = 0.0;
            let mut dist: Vec<(f64, f64)> = Vec::new();

            for &(t0, t1, x) in &segments {
                let start = t0.max(*ws);
                let end = t1.min(*we);
                if end <= start {
                    continue;
                }
                let w_ns = end - start;
                let w = w_ns as f64 / NANOS_PER_SECOND_F64;
                if w <= 0.0 || !w.is_finite() {
                    continue;
                }
                total_w += w;
                sum_wx += x * w;
                sum_wx2 += x * x * w;
                dist.push((x, w));
            }

            if !(total_w > 0.0) {
                return None;
            }
            let mean = sum_wx / total_w;
            let ex2 = sum_wx2 / total_w;
            let variance = (ex2 - mean * mean).max(0.0);

            dist.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let p = |q: f64| -> f64 {
                let target = q * total_w;
                let mut acc = 0.0;
                for (x, w) in &dist {
                    acc += *w;
                    if acc >= target {
                        return *x;
                    }
                }
                dist.last().map(|(x, _)| *x).unwrap_or(mean)
            };

            Some(CounterStats {
                mean,
                variance,
                p25: p(0.25),
                p50: p(0.50),
                p90: p(0.90),
                p99: p(0.99),
                samples: samples.len(),
            })
        })
        .collect()
}

pub fn parse_timestamp_value(value: &Value) -> Result<(i64, u32), String> {
    match value {
        Value::String(s) => parse_timestamp_str(s),
        Value::Number(n) => parse_timestamp_str(&n.to_string()),
        other => Err(format!("unsupported timestamp representation: {other}")),
    }
}

pub fn parse_timestamp_str(raw: &str) -> Result<(i64, u32), String> {
    if raw.is_empty() {
        return Err("timestamp is empty".to_string());
    }
    let mut parts = raw.splitn(2, '.');
    let seconds_part = parts.next().unwrap();
    let fraction_part = parts.next().unwrap_or("");

    let seconds: i64 = seconds_part
        .parse()
        .map_err(|e| format!("invalid seconds component `{seconds_part}`: {e}"))?;
    if seconds < 0 {
        return Err("timestamp must be non-negative".to_string());
    }

    if !fraction_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!(
            "fractional component `{fraction_part}` contains non-digit characters"
        ));
    }

    let nanos = if fraction_part.is_empty() {
        0
    } else {
        let mut nanos_str = fraction_part.to_string();
        if nanos_str.len() > 9 {
            nanos_str.truncate(9);
        } else {
            while nanos_str.len() < 9 {
                nanos_str.push('0');
            }
        }
        nanos_str
            .parse::<u32>()
            .map_err(|e| format!("invalid fractional component `{fraction_part}`: {e}"))?
    };

    Ok((seconds, nanos))
}

pub fn timestamp_parts_to_ns(seconds: i64, nanos: u32) -> Result<u64, String> {
    if seconds < 0 {
        return Err("timestamp must be non-negative".to_string());
    }
    let seconds = seconds as u64;
    seconds
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|base| base.checked_add(u64::from(nanos)))
        .ok_or_else(|| "timestamp overflow".to_string())
}
