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

//! The model behind the adaptive proposal lead (`docs/0005` §5), without
//! the planner around it: per-validator estimates of the lead each
//! validator needs, the risk rule that picks the lead, the `target_far`
//! send order and the model-based withhold.
//!
//! Follows the reference implementation, `ProbitModel` in
//! `monad-mcp-chorus/research/proposal-lead/leadsim.py`
//! (`use_passive=True`, `order_policy="target_far"`); the unit tests pin
//! values computed with it. Times are in milliseconds, stake is normalized
//! to sum to 1, validators are addressed by index.
//!
//! The math goes through the `libm` crate, not the platform's libm, so a
//! seeded simulation picks the same leads on every platform.

/// Model parameters (`docs/0005` §5.9). Node-local policy, not protocol.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptiveConfig {
    /// `θ`: bound on the negative stake on the own lane; below 1/3.
    pub theta: f64,
    /// `ε`: accepted risk per own slot of exceeding `θ`.
    pub epsilon: f64,
    /// `ε_w`: the risk of a split the withhold rule accepts.
    pub withhold_epsilon: f64,
    /// `ρ`: negative stake held back for unpredictable no-voters.
    pub reserve: f64,
    /// `α`: passive learning rate, after the warm-up average.
    pub alpha: f64,
    /// Samples per validator averaged plainly before `α` takes over.
    pub average_samples: u32,
    /// `η`: vote-calibration step, ms.
    pub eta: f64,
    /// `σ_min`: spread floor, ms.
    pub sigma_min: f64,
    /// Extra stake in the send order's core set.
    pub core_margin: f64,
    /// `L_min`, `L_max`: clamps on the lead, ms.
    pub min_lead: f64,
    pub max_lead: f64,
    /// First-hop bytes on the wire per payload byte (`κ`).
    pub wire_ratio: f64,
    /// Uplink rate `B`, bytes per ms.
    pub uplink_bytes_per_ms: f64,
    /// Encode time `e(P)`, ms per MB of payload.
    pub encode_ms_per_mb: f64,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            theta: 0.25,
            epsilon: 1e-3,
            withhold_epsilon: 1e-3,
            reserve: 0.0,
            alpha: 0.02,
            average_samples: 50,
            eta: 4.0,
            // leadsim's effective floor with passive data (`sv` = 5)
            sigma_min: 5.0,
            core_margin: 0.03,
            min_lead: 20.0,
            max_lead: 150.0,
            // measured S11 first-hop ratio 4.3–4.9 (docs/0005 §9.7)
            wire_ratio: 4.5,
            // 1 Gb/s
            uplink_bytes_per_ms: 125_000.0,
            encode_ms_per_mb: 2.0,
        }
    }
}

/// A lead chosen by the risk rule.
#[derive(Debug, Clone, PartialEq)]
pub struct LeadPlan {
    /// `L*`, ms.
    pub lead: f64,
    /// `M(L*)`: the predicted negative stake.
    pub mean: f64,
    /// `M(L*) + z √V(L*)`: the bound the rule holds below `θ − ρ`.
    pub bound: f64,
    /// False when no lead up to `L_max` meets the target: `L_max` is used.
    pub feasible: bool,
}

/// The per-validator state (`docs/0005` §5.1): what the network looks like
/// from this proposer's position.
#[derive(Debug, Clone)]
pub struct LeadModel {
    config: AdaptiveConfig,
    /// `z_{1−ε}` and `z_{1−ε_w}`
    z: f64,
    z_withhold: f64,
    /// stake fractions; the proposer's own is 0
    stake: Vec<f64>,
    /// `1 − w_p`
    others: f64,
    me: Option<usize>,
    /// `μ_v`, `δ_v`, `b_v`, and the passive sample count
    mu: Vec<f64>,
    dev: Vec<f64>,
    bias: Vec<f64>,
    seen: Vec<u32>,
}

impl LeadModel {
    /// `stakes` per validator index; `me` is the proposer's own index.
    pub fn new(config: AdaptiveConfig, stakes: &[u64], me: Option<usize>) -> Self {
        let total: f64 = stakes.iter().map(|&s| s as f64).sum();
        let mut stake: Vec<f64> = stakes.iter().map(|&s| s as f64 / total).collect();
        if let Some(me) = me {
            stake[me] = 0.0;
        }
        let others = stake.iter().sum();
        let n = stakes.len();
        Self {
            z: -ppf(config.epsilon),
            z_withhold: -ppf(config.withhold_epsilon),
            config,
            stake,
            others,
            me,
            mu: vec![0.0; n],
            dev: vec![5.0; n],
            bias: vec![0.0; n],
            seen: vec![0; n],
        }
    }

    pub fn config(&self) -> &AdaptiveConfig {
        &self.config
    }

    pub fn len(&self) -> usize {
        self.stake.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stake.is_empty()
    }

    /// The stake fraction of `v`; 0 for the proposer itself.
    pub fn stake(&self, v: usize) -> f64 {
        self.stake[v]
    }

    /// `r̂_v = μ_v + b_v`, if `v` has a sample yet.
    pub fn r_hat(&self, v: usize) -> Option<f64> {
        (self.seen[v] > 0).then(|| self.mu[v] + self.bias[v])
    }

    /// Passive learning (§5.3): `m` is the arrival of `v`'s batch vote,
    /// relative to the proposer's own deadline. A plain average over the
    /// first samples, then an EWMA.
    pub fn observe_arrival(&mut self, v: usize, m: f64) {
        let seen = self.seen[v];
        let a = if seen > self.config.average_samples {
            self.config.alpha
        } else {
            1.0 / f64::from(seen + 1)
        };
        if seen == 0 {
            self.mu[v] = m;
        }
        let d = m - self.mu[v];
        self.mu[v] += a * d;
        self.dev[v] += a * (d.abs() - self.dev[v]);
        self.seen[v] = seen.saturating_add(1);
    }

    /// Vote calibration (§5.3): one probit SGD step on `v`'s verdict at
    /// `lead`, with `extra` the encode-plus-upload term `v`'s share had.
    pub fn observe_vote(&mut self, v: usize, positive: bool, lead: f64, extra: f64) {
        if Some(v) == self.me || self.seen[v] == 0 {
            return;
        }
        let r = self.mu[v] + self.bias[v] + extra;
        let z = (lead - r) / self.sigma(v);
        let g = if positive {
            -inv_mills(-z)
        } else {
            inv_mills(z)
        };
        self.bias[v] += self.config.eta * g.clamp(-3.0, 3.0);
    }

    fn sigma(&self, v: usize) -> f64 {
        self.config.sigma_min.max(1.25 * self.dev[v])
    }

    /// `e(P)`, ms.
    pub fn encode_ms(&self, payload: usize) -> f64 {
        self.config.encode_ms_per_mb * payload as f64 / 1e6
    }

    /// The `target_far` send order (§5.5) over the other validators: the
    /// nearest prefix whose stake reaches `1 − (θ − ρ) + margin` of theirs,
    /// farthest first, then the rest nearest first. Validators without a
    /// sample go last.
    pub fn send_order(&self) -> Vec<usize> {
        let mut known: Vec<(f64, usize)> = (0..self.len())
            .filter(|&v| Some(v) != self.me)
            .filter_map(|v| self.r_hat(v).map(|r| (r, v)))
            .collect();
        known.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let cover = (1.0 - (self.config.theta - self.config.reserve) + self.config.core_margin)
            .min(1.0)
            * self.others;
        let mut cum = 0.0;
        let mut core = known.len();
        for (i, (_, v)) in known.iter().enumerate() {
            cum += self.stake[*v];
            if cum >= cover {
                core = i + 1;
                break;
            }
        }
        let mut order: Vec<usize> = known[..core].iter().rev().map(|(_, v)| *v).collect();
        order.extend(known[core..].iter().map(|(_, v)| *v));
        order.extend((0..self.len()).filter(|&v| Some(v) != self.me && self.seen[v] == 0));
        order
    }

    /// `u_v(P, π)` per validator index (§3): when the last byte of `v`'s
    /// stake-proportional share leaves the uplink, with shares sent back to
    /// back in `order`.
    pub fn upload_ms(&self, order: &[usize], payload: usize) -> Vec<f64> {
        let total = self.config.wire_ratio * payload as f64 / self.config.uplink_bytes_per_ms;
        let mut upload = vec![0.0; self.len()];
        let mut cum = 0.0;
        for &v in order {
            cum += self.stake[v];
            upload[v] = total * cum / self.others;
        }
        upload
    }

    /// `q̂_v(L)`: the predicted probability that `v` votes no at `lead`. A
    /// validator without history counts as a no.
    fn q(&self, v: usize, lead: f64, extra: f64) -> f64 {
        if self.seen[v] == 0 {
            return 1.0;
        }
        let r = self.mu[v] + self.bias[v] + extra;
        normal_sf((lead - r) / self.sigma(v))
    }

    /// `M(L)` and `V(L)` (§5.4) with `extra[v]` the encode-plus-upload term.
    pub fn risk(&self, lead: f64, extra: &[f64]) -> (f64, f64) {
        let mut mean = 0.0;
        let mut var = 0.0;
        for (v, &w) in self.stake.iter().enumerate() {
            if w == 0.0 {
                continue;
            }
            let q = self.q(v, lead, extra[v]);
            mean += w * q;
            var += w * w * q * (1.0 - q);
        }
        (mean, var)
    }

    /// The encode-plus-upload term per validator for `payload` sent in
    /// `order`.
    pub fn extra_ms(&self, order: &[usize], payload: usize) -> Vec<f64> {
        let encode = self.encode_ms(payload);
        self.upload_ms(order, payload)
            .into_iter()
            .map(|u| u + encode)
            .collect()
    }

    /// The decision rule (§5.4): the smallest lead whose
    /// `M + z√V ≤ θ − ρ`, by bisection over [0, 600] ms, then clamped to
    /// [`L_min`, `L_max`]. Infeasible at `L_max`: `L_max`.
    pub fn plan(&self, extra: &[f64]) -> LeadPlan {
        let target = self.config.theta - self.config.reserve;
        let bound = |lead: f64| {
            let (mean, var) = self.risk(lead, extra);
            (mean, mean + self.z * libm::sqrt(var))
        };
        let (mut lo, mut hi) = (0.0, 600.0);
        for _ in 0..40 {
            let mid = 0.5 * (lo + hi);
            if bound(mid).1 <= target {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let lead = hi.clamp(self.config.min_lead, self.config.max_lead);
        let (mean, at_lead) = bound(lead);
        LeadPlan {
            lead,
            mean,
            bound: at_lead,
            feasible: at_lead <= target,
        }
    }

    /// The withhold rule (§5.7): sending with only `lead` left risks a
    /// split, `M + z_w √V > 1/3`.
    pub fn withhold(&self, lead: f64, extra: &[f64]) -> bool {
        let (mean, var) = self.risk(lead, extra);
        mean + self.z_withhold * libm::sqrt(var) > 1.0 / 3.0
    }
}

const SQRT_2: f64 = std::f64::consts::SQRT_2;
// 1 / sqrt(2π)
const FRAC_1_SQRT_2PI: f64 = 0.398_942_280_401_432_7;

/// `1 − Φ(x)`.
fn normal_sf(x: f64) -> f64 {
    0.5 * libm::erfc(x / SQRT_2)
}

/// The inverse Mills ratio `λ(x) = φ(x) / (1 − Φ(x))`, stable for large
/// `x` (→ x), as leadsim's `_inv_mills`.
fn inv_mills(x: f64) -> f64 {
    if x > 8.0 {
        return x + 1.0 / x - 2.0 / (x * x * x);
    }
    FRAC_1_SQRT_2PI * libm::exp(-0.5 * x * x) / normal_sf(x)
}

/// `Φ⁻¹(p)`, by bisection on `erf`, as leadsim's `_ppf`.
fn ppf(p: f64) -> f64 {
    let (mut lo, mut hi) = (-10.0, 10.0);
    for _ in 0..80 {
        let mid = 0.5 * (lo + hi);
        if 0.5 * (1.0 + libm::erf(mid / SQRT_2)) < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    0.5 * (lo + hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    // expected values from leadsim.py's ProbitModel on the same inputs
    // (use_passive=True, target_no=0.25, eps=1e-3, cover_margin=0.03,
    // expansion 4.5, 125 kB/ms, 2 ms/MB), proposer 0
    const STAKES: [u64; 8] = [3, 1, 2, 5, 1, 4, 2, 2];
    const BASE: [f64; 8] = [0.0, 10.0, 40.0, 75.0, 20.0, 120.0, 5.0, 90.0];
    const WOBBLE: [f64; 8] = [0.0, 1.5, -2.0, 3.0, 0.5, -4.0, 1.0, 2.5];
    const PAYLOAD: usize = 400_000;

    fn close(actual: f64, expected: f64, tolerance: f64) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "{actual} != {expected} (± {tolerance})"
        );
    }

    fn warm_model() -> LeadModel {
        let mut model = LeadModel::new(AdaptiveConfig::default(), &STAKES, Some(0));
        for k in 0..60 {
            let wobble = libm::sin(0.7 * f64::from(k) + 0.3);
            for v in 1..STAKES.len() {
                model.observe_arrival(v, BASE[v] + WOBBLE[v] * wobble);
            }
        }
        model
    }

    #[test]
    fn passive_estimates_match_leadsim() {
        let model = warm_model();
        let mu = [
            10.0546978542649,
            39.927069527646815,
            75.10939570852983,
            20.01823261808831,
            119.85413905529363,
            5.0364652361766025,
            90.09116309044151,
        ];
        let dev = [
            0.9635357813201024,
            1.2847143750934678,
            1.9270715626402053,
            0.32117859377336766,
            2.5694287501869355,
            0.6423571875467347,
            1.6058929688668375,
        ];
        for v in 1..STAKES.len() {
            close(model.mu[v], mu[v - 1], 1e-9);
            close(model.dev[v], dev[v - 1], 1e-9);
        }
    }

    #[test]
    fn send_order_and_upload_match_leadsim() {
        let model = warm_model();
        let order = model.send_order();
        assert_eq!(order, [5, 7, 3, 2, 4, 1, 6]);
        let upload = model.upload_ms(&order, PAYLOAD);
        let expected = [
            12.705882352941178,
            11.011764705882355,
            9.31764705882353,
            11.858823529411765,
            3.3882352941176466,
            14.399999999999999,
            5.082352941176471,
        ];
        for v in 1..STAKES.len() {
            close(upload[v], expected[v - 1], 1e-9);
        }
    }

    // leadsim's lead uses an erf approximation (|err| < 1.5e-7), hence the
    // tolerance on leads
    #[test]
    fn lead_and_vote_calibration_match_leadsim() {
        let mut model = warm_model();
        let extra = model.extra_ms(&model.send_order(), PAYLOAD);
        let plan = model.plan(&extra);
        close(plan.lead, 129.2428193262822, 1e-3);
        assert!(plan.feasible);
        assert!(plan.bound <= 0.25 + 1e-12);

        let positive = [true, true, true, false, true, false, true, true];
        for v in 1..STAKES.len() {
            model.observe_vote(v, positive[v], plan.lead, extra[v]);
        }
        let bias = [
            0.0,
            0.0,
            12.0,
            0.0,
            6.229343051492409,
            0.0,
            -3.881710564507208e-10,
        ];
        for v in 1..STAKES.len() {
            close(model.bias[v], bias[v - 1], 1e-6);
        }

        let extra = model.extra_ms(&model.send_order(), PAYLOAD);
        close(model.plan(&extra).lead, 110.6089096821961, 1e-3);
        let extra = model.extra_ms(&model.send_order(), 10_000);
        close(model.plan(&extra).lead, 101.64675267278653, 1e-3);
    }

    #[test]
    fn normal_helpers_match_leadsim() {
        close(ppf(1e-3), -3.0902323061678256, 1e-12);
        let mills = [
            (-3.0, 0.004437839042125664),
            (0.0, 0.7978845608028654),
            (2.5, 2.8227447976639066),
            (7.9, 8.022817246208735),
            (8.5, 8.614390392835334),
            (30.0, 30.03325925925926),
        ];
        for (x, expected) in mills {
            close(inv_mills(x), expected, 1e-9 * f64::max(expected, 1.0));
        }
    }

    #[test]
    fn a_validator_without_history_counts_as_a_no() {
        let mut model = LeadModel::new(AdaptiveConfig::default(), &[1, 1, 1, 2], Some(0));
        model.observe_arrival(1, 10.0);
        model.observe_arrival(2, 10.0);
        let extra = [0.0; 4];
        let (mean, _) = model.risk(600.0, &extra);
        close(mean, 0.4, 1e-12);
        // validator 3 alone holds more than θ = 1/4
        assert!(!model.plan(&extra).feasible);
        assert_eq!(model.send_order().last(), Some(&3));
    }

    #[test]
    fn an_unreachable_target_falls_back_to_the_max_lead() {
        let mut model = LeadModel::new(AdaptiveConfig::default(), &[1, 1, 1, 1], Some(0));
        for v in 1..4 {
            model.observe_arrival(v, 400.0);
        }
        let plan = model.plan(&[0.0; 4]);
        assert!(!plan.feasible);
        close(plan.lead, AdaptiveConfig::default().max_lead, 0.0);
    }

    #[test]
    fn the_withhold_rule_tracks_the_split_risk() {
        let model = warm_model();
        let extra = model.extra_ms(&model.send_order(), PAYLOAD);
        let plan = model.plan(&extra);
        assert!(!model.withhold(plan.lead, &extra));
        assert!(model.withhold(5.0, &extra));
    }
}
