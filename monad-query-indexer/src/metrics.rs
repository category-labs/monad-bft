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

//! Chain-data ingest metric names, kept from the pre-split archiver so future
//! otel wiring lines up with existing dashboards; nothing records them yet.

/// Chain-data ingest metric name/description pairs, matching the otel
/// instrument names `monad-archive`'s `MetricNames` used for these on the
/// branch this crate was split from.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
#[allow(non_camel_case_types)]
pub enum ChainDataMetricNames {
    CHAIN_DATA_INGEST_STAGE_A_DURATION_MS,
    CHAIN_DATA_INGEST_COMMIT_A_META_DURATION_MS,
    CHAIN_DATA_INGEST_COMMIT_A_BLOB_DURATION_MS,
    CHAIN_DATA_INGEST_READS_DURATION_MS,
    CHAIN_DATA_INGEST_STAGE_B_DURATION_MS,
    CHAIN_DATA_INGEST_COMMIT_B_DURATION_MS,
    CHAIN_DATA_INGEST_CAS_DURATION_MS,
    CHAIN_DATA_INGEST_BATCH_TOTAL_DURATION_MS,
    CHAIN_DATA_INGEST_BATCH_SIZE,
    /// Per-table cache hit ratio (0..=1), attribute-tagged by table name.
    CHAIN_DATA_CACHE_HIT_RATIO,
}

impl ChainDataMetricNames {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CHAIN_DATA_INGEST_STAGE_A_DURATION_MS => "chain_data_ingest_stage_a_duration_ms",
            Self::CHAIN_DATA_INGEST_COMMIT_A_META_DURATION_MS => {
                "chain_data_ingest_commit_a_meta_duration_ms"
            }
            Self::CHAIN_DATA_INGEST_COMMIT_A_BLOB_DURATION_MS => {
                "chain_data_ingest_commit_a_blob_duration_ms"
            }
            Self::CHAIN_DATA_INGEST_READS_DURATION_MS => "chain_data_ingest_reads_duration_ms",
            Self::CHAIN_DATA_INGEST_STAGE_B_DURATION_MS => "chain_data_ingest_stage_b_duration_ms",
            Self::CHAIN_DATA_INGEST_COMMIT_B_DURATION_MS => {
                "chain_data_ingest_commit_b_duration_ms"
            }
            Self::CHAIN_DATA_INGEST_CAS_DURATION_MS => "chain_data_ingest_cas_duration_ms",
            Self::CHAIN_DATA_INGEST_BATCH_TOTAL_DURATION_MS => {
                "chain_data_ingest_batch_total_duration_ms"
            }
            Self::CHAIN_DATA_INGEST_BATCH_SIZE => "chain_data_ingest_batch_size",
            Self::CHAIN_DATA_CACHE_HIT_RATIO => "chain_data_cache_hit_ratio",
        }
    }

    /// HELP description for Prometheus/OTEL, matching the pre-split source.
    pub fn description(&self) -> &'static str {
        match self {
            Self::CHAIN_DATA_INGEST_STAGE_A_DURATION_MS => {
                "chain-data ingest Phase A staging duration in ms"
            }
            Self::CHAIN_DATA_INGEST_COMMIT_A_META_DURATION_MS => {
                "chain-data ingest Phase A meta commit duration in ms"
            }
            Self::CHAIN_DATA_INGEST_COMMIT_A_BLOB_DURATION_MS => {
                "chain-data ingest Phase A blob commit duration in ms"
            }
            Self::CHAIN_DATA_INGEST_READS_DURATION_MS => {
                "chain-data ingest compaction-plan reads duration in ms"
            }
            Self::CHAIN_DATA_INGEST_STAGE_B_DURATION_MS => {
                "chain-data ingest Phase B staging duration in ms"
            }
            Self::CHAIN_DATA_INGEST_COMMIT_B_DURATION_MS => {
                "chain-data ingest Phase B commit (with CAS) duration in ms"
            }
            Self::CHAIN_DATA_INGEST_CAS_DURATION_MS => {
                "chain-data ingest standalone CAS-advance duration in ms"
            }
            Self::CHAIN_DATA_INGEST_BATCH_TOTAL_DURATION_MS => {
                "chain-data ingest total batch duration in ms"
            }
            Self::CHAIN_DATA_INGEST_BATCH_SIZE => "chain-data ingest blocks per batch",
            Self::CHAIN_DATA_CACHE_HIT_RATIO => {
                "chain-data per-table cache hit ratio (0..=1) over the last progress window"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_snake_case_of_variant() {
        assert_eq!(
            ChainDataMetricNames::CHAIN_DATA_CACHE_HIT_RATIO.as_str(),
            "chain_data_cache_hit_ratio"
        );
    }
}
