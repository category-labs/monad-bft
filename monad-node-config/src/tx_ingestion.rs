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

use serde::Deserialize;

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxIngestionConfig {
    #[serde(default)]
    pub peer_score: TxIngestionPeerScoreConfig,

    #[serde(default)]
    pub fair_queue: TxIngestionFairQueueConfig,

    #[serde(default)]
    pub leanudp: TxIngestionLeanUdpConfig,
}

impl Default for TxIngestionConfig {
    fn default() -> Self {
        Self {
            peer_score: TxIngestionPeerScoreConfig::default(),
            fair_queue: TxIngestionFairQueueConfig::default(),
            leanudp: TxIngestionLeanUdpConfig::default(),
        }
    }
}

fn default_promoted_capacity() -> usize {
    90_000
}
fn default_newcomer_capacity() -> usize {
    10_000
}
fn default_max_time_weight() -> f64 {
    1.0
}
fn default_time_weight_unit_seconds() -> u64 {
    10 * 60
}
fn default_ema_half_life_seconds() -> u64 {
    12 * 3600
}
fn default_block_time_millis() -> u64 {
    400
}
fn default_promotion_threshold() -> f64 {
    1_000_000.0
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxIngestionPeerScoreConfig {
    #[serde(default = "default_promoted_capacity")]
    pub promoted_capacity: usize,

    #[serde(default = "default_newcomer_capacity")]
    pub newcomer_capacity: usize,

    #[serde(default = "default_max_time_weight")]
    pub max_time_weight: f64,

    #[serde(default = "default_time_weight_unit_seconds")]
    pub time_weight_unit_seconds: u64,

    #[serde(default = "default_ema_half_life_seconds")]
    pub ema_half_life_seconds: u64,

    #[serde(default = "default_block_time_millis")]
    pub block_time_millis: u64,

    #[serde(default = "default_promotion_threshold")]
    pub promotion_threshold: f64,
}

impl Default for TxIngestionPeerScoreConfig {
    fn default() -> Self {
        Self {
            promoted_capacity: default_promoted_capacity(),
            newcomer_capacity: default_newcomer_capacity(),
            max_time_weight: default_max_time_weight(),
            time_weight_unit_seconds: default_time_weight_unit_seconds(),
            ema_half_life_seconds: default_ema_half_life_seconds(),
            block_time_millis: default_block_time_millis(),
            promotion_threshold: default_promotion_threshold(),
        }
    }
}

fn default_fair_queue_per_id_limit() -> usize {
    20_000
}
fn default_fair_queue_max_size() -> usize {
    100_000
}
fn default_fair_queue_regular_per_id_limit() -> usize {
    2_000
}
fn default_fair_queue_regular_max_size() -> usize {
    100_000
}
fn default_fair_queue_regular_bandwidth_pct() -> u8 {
    10
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxIngestionFairQueueConfig {
    #[serde(default = "default_fair_queue_per_id_limit")]
    pub per_id_limit: usize,

    #[serde(default = "default_fair_queue_max_size")]
    pub max_size: usize,

    #[serde(default = "default_fair_queue_regular_per_id_limit")]
    pub regular_per_id_limit: usize,

    #[serde(default = "default_fair_queue_regular_max_size")]
    pub regular_max_size: usize,

    #[serde(default = "default_fair_queue_regular_bandwidth_pct")]
    pub regular_bandwidth_pct: u8,
}

impl Default for TxIngestionFairQueueConfig {
    fn default() -> Self {
        Self {
            per_id_limit: default_fair_queue_per_id_limit(),
            max_size: default_fair_queue_max_size(),
            regular_per_id_limit: default_fair_queue_regular_per_id_limit(),
            regular_max_size: default_fair_queue_regular_max_size(),
            regular_bandwidth_pct: default_fair_queue_regular_bandwidth_pct(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxIngestionLeanUdpConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    // These are optional overrides; the node applies defaults from monad-leanudp::Config::default().
    pub max_message_size_bytes: Option<usize>,
    pub max_priority_messages: Option<usize>,
    pub max_regular_messages: Option<usize>,
    pub max_messages_per_identity: Option<usize>,
    pub max_bytes_per_identity: Option<usize>,
    pub message_timeout_ms: Option<u64>,
    pub max_fragment_payload_bytes: Option<usize>,
}

impl Default for TxIngestionLeanUdpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_message_size_bytes: None,
            max_priority_messages: None,
            max_regular_messages: None,
            max_messages_per_identity: None,
            max_bytes_per_identity: None,
            message_timeout_ms: None,
            max_fragment_payload_bytes: None,
        }
    }
}
