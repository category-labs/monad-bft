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

#[derive(Debug, Clone, Copy)]
pub struct DynamicCapConfig {
    pub enter_pressure: f64,
    pub exit_pressure: f64,
    pub bootstrap_limit: usize,
    pub ema_alpha: f64,
}

impl DynamicCapConfig {
    pub const DEFAULT_ENTER_PRESSURE: f64 = 0.80;
    pub const DEFAULT_EXIT_PRESSURE: f64 = 0.60;
    pub const DEFAULT_BOOTSTRAP_LIMIT: usize = 64;
    pub const DEFAULT_EMA_ALPHA: f64 = 0.10;

    pub fn decay_base(&self) -> f64 {
        1.0 - self.ema_alpha
    }

    pub fn validate(&self) {
        // panic is acceptable for cfg validation: these invariants are expected
        // to be checked once at startup, and invalid config should fail fast.
        assert!(
            self.enter_pressure.is_finite() && self.enter_pressure >= 0.0,
            "enter_pressure must be finite and >= 0"
        );
        assert!(
            self.exit_pressure.is_finite() && self.exit_pressure >= 0.0,
            "exit_pressure must be finite and >= 0"
        );
        assert!(
            self.exit_pressure <= self.enter_pressure,
            "exit_pressure must be <= enter_pressure"
        );
        assert!(
            self.ema_alpha.is_finite() && self.ema_alpha > 0.0 && self.ema_alpha <= 1.0,
            "ema_alpha must be finite and in (0, 1]"
        );
    }
}

impl Default for DynamicCapConfig {
    fn default() -> Self {
        Self {
            enter_pressure: Self::DEFAULT_ENTER_PRESSURE,
            exit_pressure: Self::DEFAULT_EXIT_PRESSURE,
            bootstrap_limit: Self::DEFAULT_BOOTSTRAP_LIMIT,
            ema_alpha: Self::DEFAULT_EMA_ALPHA,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DynamicCapIdentity {
    share_ema: f64,
    service_seq: u64,
}

impl DynamicCapIdentity {
    pub fn new(current_service_seq: u64) -> Self {
        Self {
            share_ema: 0.0,
            service_seq: current_service_seq,
        }
    }

    pub fn decayed_share(&mut self, current_service_seq: u64, cfg: &DynamicCapConfig) -> f64 {
        let delta = current_service_seq.saturating_sub(self.service_seq);
        if delta > 0 {
            self.share_ema *= cfg.decay_base().powf(delta as f64);
            self.service_seq = current_service_seq;
        }
        self.share_ema = if self.share_ema.is_finite() {
            self.share_ema.clamp(0.0, 1.0)
        } else {
            0.0
        };
        self.share_ema
    }

    pub fn observe_service(&mut self, current_service_seq: u64, cfg: &DynamicCapConfig) {
        let _ = self.decayed_share(current_service_seq, cfg);
        self.share_ema += cfg.ema_alpha;
        self.share_ema = self.share_ema.clamp(0.0, 1.0);
        self.service_seq = current_service_seq;
    }
}

pub fn update_pressure_mode(
    currently_enforced: bool,
    total_items: usize,
    max_size: usize,
    incoming_items: usize,
    cfg: &DynamicCapConfig,
) -> bool {
    let occupancy = if max_size == 0 {
        1.0
    } else {
        total_items.saturating_add(incoming_items) as f64 / max_size as f64
    };
    if currently_enforced {
        occupancy > cfg.exit_pressure
    } else {
        occupancy >= cfg.enter_pressure
    }
}

pub fn effective_limit(
    pool_limit: usize,
    pool_max_size: usize,
    pressure_enforced: bool,
    share: f64,
    cfg: &DynamicCapConfig,
) -> usize {
    if !pressure_enforced {
        return pool_limit;
    }
    if pool_limit == 0 {
        return 0;
    }
    let bonus = (share * pool_max_size as f64).ceil() as usize;
    let dynamic_limit = cfg.bootstrap_limit.saturating_add(bonus);
    dynamic_limit.clamp(1, pool_limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_hysteresis() {
        let cfg = DynamicCapConfig::default();
        let mut enforced = false;
        enforced = update_pressure_mode(enforced, 79, 100, 0, &cfg);
        assert!(!enforced);
        enforced = update_pressure_mode(enforced, 80, 100, 0, &cfg);
        assert!(enforced);
        enforced = update_pressure_mode(enforced, 60, 100, 0, &cfg);
        assert!(!enforced);
    }

    #[test]
    fn service_decay_and_observe() {
        let cfg = DynamicCapConfig::default();
        let mut state = DynamicCapIdentity::new(0);

        state.observe_service(1, &cfg);
        let first = state.decayed_share(1, &cfg);
        assert!(first > 0.0);

        let decayed = state.decayed_share(100, &cfg);
        assert!(decayed < first);
    }

    #[test]
    fn effective_limit_uses_bootstrap_when_pressure_enabled() {
        let cfg = DynamicCapConfig::default();
        let limit = effective_limit(200, 320, true, 0.0, &cfg);
        assert_eq!(limit, cfg.bootstrap_limit);
    }

    #[test]
    fn decayed_share_resets_nan_to_zero() {
        let cfg = DynamicCapConfig::default();
        let mut state = DynamicCapIdentity {
            share_ema: f64::NAN,
            service_seq: 0,
        };

        assert_eq!(state.decayed_share(1, &cfg), 0.0);
    }
}
