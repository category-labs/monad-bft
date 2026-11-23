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

use std::convert::Infallible;

use bytes::Bytes;
use monad_mcp_common::{ProposalScope, SlotLifecycle, Timestamp};
use rand_chacha::ChaCha8Rng;

use super::ProposalSource;

// proposes random bytes, for load tests without clients and for scenarios
// that do not care what a proposal carries
pub struct RandomSource {
    proposal_size_limit: usize,
    fixed: bool,
    rng: ChaCha8Rng,
}

impl RandomSource {
    // every proposal is exactly the size limit
    pub fn fixed_size(proposal_size_limit: usize, rng: ChaCha8Rng) -> Self {
        Self {
            proposal_size_limit,
            fixed: true,
            rng,
        }
    }

    pub fn log_uniform(proposal_size_limit: usize, rng: ChaCha8Rng) -> Self {
        Self {
            proposal_size_limit,
            fixed: false,
            rng,
        }
    }
}

impl ProposalSource for RandomSource {
    type LocalIn = Infallible;

    fn handle_lifecycle(&mut self, _event: SlotLifecycle) {}

    fn handle_input(&mut self, input: Infallible, _now: Timestamp) {
        match input {}
    }

    fn seal(&mut self, _scope: ProposalScope, _now: Timestamp) -> Bytes {
        if self.fixed {
            return random_bytes(&mut self.rng, self.proposal_size_limit.max(1));
        }
        random_payload(&mut self.rng, self.proposal_size_limit)
    }
}

// never empty, log-uniform in length: every octave [2^k, 2^(k+1)) from 1 byte
// up to the proposal size limit is equally likely, uniform within it. Integer
// only: libm's log2/powf differ in the last bit across platforms, which would
// make a seeded simulation diverge between, e.g., arm and x86.
fn random_payload(rng: &mut impl rand::Rng, proposal_size_limit: usize) -> Bytes {
    let limit = proposal_size_limit.max(1) as u64;
    let max_bits = u64::BITS - 1 - limit.leading_zeros();
    let low = 1u64 << rng.gen_range(0..=max_bits);
    let len = rng.gen_range(low..=(2 * low - 1).min(limit));
    random_bytes(rng, len as usize)
}

fn random_bytes(rng: &mut impl rand::Rng, len: usize) -> Bytes {
    let mut message = vec![0u8; len];
    rng.fill_bytes(&mut message);
    Bytes::from(message)
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;

    use super::*;

    // pinned: integer sampling and ChaCha8 are the same on every platform, so a
    // seeded run proposes the same lengths on arm and x86
    #[test]
    fn random_payload_lengths_are_fixed_by_the_seed() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let lengths: Vec<usize> = (0..8)
            .map(|_| random_payload(&mut rng, 1 << 20).len())
            .collect();
        assert_eq!(lengths, PINNED_LENGTHS);
    }

    const PINNED_LENGTHS: [usize; 8] = [9, 52470, 32166, 12, 6184, 391, 4996, 43471];

    #[test]
    fn a_random_payload_is_non_empty_and_within_the_proposal_size_limit() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        for proposal_size_limit in [0, 1, 7, 1024, 1 << 20] {
            for _ in 0..64 {
                let message = random_payload(&mut rng, proposal_size_limit);
                assert!(!message.is_empty());
                assert!(message.len() <= proposal_size_limit.max(1));
            }
        }
    }
}
