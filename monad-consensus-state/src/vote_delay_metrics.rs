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

use std::collections::VecDeque;

use monad_consensus_types::metrics::Metrics;
use monad_types::Round;

const VOTE_DELAY_WINDOW_MS: u64 = 5 * 60 * 1000;

pub(crate) fn ns_to_ms(timestamp_ns: u128) -> u64 {
    u64::try_from(timestamp_ns / 1_000_000).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct VoteDelayTimerStart {
    pub(crate) round: Round,
    pub(crate) started_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct TimedVoteDelaySample {
    recorded_at_ms: u64,
    ready_after_timer_start_ms: u64,
}

#[derive(Debug, Default)]
pub(crate) struct VoteDelayMetricsWindow {
    ready_after_timer_start_samples: VecDeque<TimedVoteDelaySample>,
}

impl VoteDelayMetricsWindow {
    pub(crate) fn record_ready_after_timer_start(
        &mut self,
        now_ms: u64,
        ready_after_timer_start_ms: u64,
        metrics: &mut Metrics,
    ) {
        self.prune(now_ms);
        self.ready_after_timer_start_samples
            .push_back(TimedVoteDelaySample {
                recorded_at_ms: now_ms,
                ready_after_timer_start_ms,
            });
        self.update_metrics(metrics);
    }

    pub(crate) fn refresh(&mut self, now_ms: u64, metrics: &mut Metrics) {
        if self.prune(now_ms) {
            self.update_metrics(metrics);
        }
    }

    fn prune(&mut self, now_ms: u64) -> bool {
        let cutoff_ms = now_ms.saturating_sub(VOTE_DELAY_WINDOW_MS);
        let mut pruned = false;

        while self
            .ready_after_timer_start_samples
            .front()
            .is_some_and(|sample| sample.recorded_at_ms < cutoff_ms)
        {
            self.ready_after_timer_start_samples.pop_front();
            pruned = true;
        }

        pruned
    }

    fn update_metrics(&self, metrics: &mut Metrics) {
        metrics.vote_delay.ready_after_timer_start_p50_ms = 0;
        metrics.vote_delay.ready_after_timer_start_p90_ms = 0;
        metrics.vote_delay.ready_after_timer_start_p99_ms = 0;

        if self.ready_after_timer_start_samples.is_empty() {
            return;
        }

        let mut values = self
            .ready_after_timer_start_samples
            .iter()
            .map(|sample| sample.ready_after_timer_start_ms)
            .collect::<Vec<_>>();
        values.sort_unstable();

        metrics.vote_delay.ready_after_timer_start_p50_ms = percentile(&values, 50);
        metrics.vote_delay.ready_after_timer_start_p90_ms = percentile(&values, 90);
        metrics.vote_delay.ready_after_timer_start_p99_ms = percentile(&values, 99);
    }
}

fn percentile(sorted_values: &[u64], percentile: usize) -> u64 {
    debug_assert!(!sorted_values.is_empty());

    let rank = sorted_values
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .max(1);
    sorted_values[rank - 1]
}

#[cfg(test)]
mod tests {
    use monad_consensus_types::metrics::Metrics;

    use crate::vote_delay_metrics::{VoteDelayMetricsWindow, VOTE_DELAY_WINDOW_MS};

    #[test]
    fn vote_delay_window_tracks_ready_after_timer_start_percentiles() {
        let mut metrics = Metrics::default();
        let mut window = VoteDelayMetricsWindow::default();

        window.record_ready_after_timer_start(10, 10, &mut metrics);
        window.record_ready_after_timer_start(20, 20, &mut metrics);
        window.record_ready_after_timer_start(30, 200, &mut metrics);

        assert_eq!(metrics.vote_delay.ready_after_timer_start_p50_ms, 20);
        assert_eq!(metrics.vote_delay.ready_after_timer_start_p90_ms, 200);
        assert_eq!(metrics.vote_delay.ready_after_timer_start_p99_ms, 200);
    }

    #[test]
    fn vote_delay_window_expires_old_samples() {
        let mut metrics = Metrics::default();
        let mut window = VoteDelayMetricsWindow::default();

        window.record_ready_after_timer_start(0, 100, &mut metrics);
        window.record_ready_after_timer_start(VOTE_DELAY_WINDOW_MS + 1, 50, &mut metrics);

        assert_eq!(metrics.vote_delay.ready_after_timer_start_p50_ms, 50);
        assert_eq!(metrics.vote_delay.ready_after_timer_start_p90_ms, 50);
        assert_eq!(metrics.vote_delay.ready_after_timer_start_p99_ms, 50);
    }
}
