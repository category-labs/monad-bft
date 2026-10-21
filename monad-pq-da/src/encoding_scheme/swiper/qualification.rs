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

//! Weight qualification with an integer target (Swiper, Tonkikh and
//! Freitas): integer tickets t over weights w such that every set S
//! with w(S) > W/3 holds t(S) >= target. Tickets have the form
//! floor(s * w_i / W + 2/3) for the least scale s that qualifies.

use monad_mcp_chorus::spec::Stake as _;

use super::super::super::types::Stake;

// the tickets of the least qualifying scale. the caller must ensure
// target > 0 and a non-zero total weight.
pub(crate) fn tickets(weights: &[Stake], target: usize) -> Vec<usize> {
    assert!(target > 0);
    let total = weights.iter().copied().sum::<Stake>();

    // qualification is monotone in the scale: no ticket at 0, and
    // at 3 * target + n every qualifying set holds more than target
    let mut unqualified = 0;
    let mut qualified = max_scale(target, weights.len());
    while qualified - unqualified > 1 {
        let scale = unqualified + (qualified - unqualified) / 2;
        if qualifies(weights, &total, &tickets_at(weights, &total, scale), target) {
            qualified = scale;
        } else {
            unqualified = scale;
        }
    }
    tickets_at(weights, &total, qualified)
}

// an upper bound on the total tickets over n weights: at most
// max_scale + 2n/3
pub(crate) const fn max_total_tickets(target: usize, n: usize) -> usize {
    max_scale(target, n) + 2 * n / 3
}

const fn max_scale(target: usize, n: usize) -> usize {
    3 * target + n
}

// floor(scale * w / W + 2/3) as ceil(floor(3 * scale * w / W) / 3)
fn tickets_at(weights: &[Stake], total: &Stake, scale: usize) -> Vec<usize> {
    let mut tickets = Vec::with_capacity(weights.len());
    for weight in weights {
        let (thirds, _) = weight.obligation(total, 3 * scale);
        tickets.push(thirds.div_ceil(3));
    }
    tickets
}

// whether the heaviest set holding fewer than target tickets stays
// within W/3. knapsack by tickets: heaviest[t] is the heaviest set of
// ticketed nodes holding exactly t tickets.
fn qualifies(weights: &[Stake], total: &Stake, tickets: &[usize], target: usize) -> bool {
    // nodes without tickets join any set for free
    let mut free = Stake::ZERO;
    let mut heaviest: Vec<Option<Stake>> = vec![None; target];
    heaviest[0] = Some(Stake::ZERO);

    for (weight, held) in weights.iter().zip(tickets) {
        if *held == 0 {
            free = free + *weight;
            continue;
        }
        if *held >= target {
            continue;
        }
        for sum in (*held..target).rev() {
            let Some(base) = heaviest[sum - held] else {
                continue;
            };
            let candidate = base + *weight;
            if heaviest[sum].is_none_or(|current| candidate > current) {
                heaviest[sum] = Some(candidate);
            }
        }
    }

    let heaviest = heaviest.iter().flatten().max().copied();
    let heaviest = heaviest.expect("the empty set holds no tickets") + free;
    heaviest <= total.honest_threshold()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stakes(weights: &[u64]) -> Vec<Stake> {
        weights.iter().map(|w| Stake::from(*w)).collect()
    }

    // every set of weight > W/3 holds at least target tickets
    fn check(weights: &[u64], tickets: &[usize], target: usize) {
        let total: u64 = weights.iter().sum();
        for mask in 0u32..1 << weights.len() {
            let mut weight = 0;
            let mut held = 0;
            for i in 0..weights.len() {
                if mask & (1 << i) != 0 {
                    weight += weights[i];
                    held += tickets[i];
                }
            }
            if 3 * weight > total {
                assert!(held >= target, "{weights:?} {tickets:?} {mask:b}");
            }
        }
    }

    #[test]
    fn equal_weights_get_one_ticket_each_for_a_pair_target() {
        // any two of four qualify and need two tickets between them
        let weights = stakes(&[1, 1, 1, 1]);
        assert_eq!(tickets(&weights, 2), [1, 1, 1, 1]);
        check(&[1, 1, 1, 1], &[1, 1, 1, 1], 2);
    }

    #[test]
    fn light_nodes_still_get_tickets_when_they_qualify_together() {
        // the three light nodes together outweigh W/3, so scale 1
        // (tickets 1, 0, 0, 0) does not qualify
        let weights = stakes(&[3, 1, 1, 1]);
        assert_eq!(tickets(&weights, 1), [1, 1, 1, 1]);
    }

    #[test]
    fn the_least_scale_qualifies_and_respects_the_bound() {
        let cases: [&[u64]; 5] = [
            &[1],
            &[5, 3],
            &[10, 1, 1, 1, 1, 1],
            &[7, 7, 7, 1, 1, 1, 1, 1],
            &[100, 50, 25, 12, 6, 3, 1, 1, 1, 1],
        ];
        for weights in cases {
            for target in [1, 2, 3, 7, 20] {
                let tickets = tickets(&stakes(weights), target);
                check(weights, &tickets, target);
                let total: usize = tickets.iter().sum();
                assert!(total >= target);
                assert!(total <= max_total_tickets(target, weights.len()));
            }
        }
    }
}
