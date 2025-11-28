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

use super::SessionError;

const REPLAY_WINDOW_SIZE: usize = 32;
const REPLAY_WINDOW_BITS: usize = REPLAY_WINDOW_SIZE * 64;

pub struct ReplayFilter {
    next: u64,
    bitmap: [u64; REPLAY_WINDOW_SIZE],
}

impl Default for ReplayFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayFilter {
    pub fn new() -> Self {
        Self {
            next: 0,
            bitmap: [0; REPLAY_WINDOW_SIZE],
        }
    }

    pub fn check(&self, counter: u64) -> Result<(), SessionError> {
        if counter >= self.next {
            return Ok(());
        }

        if counter.saturating_add(REPLAY_WINDOW_BITS as u64) <= self.next {
            return Err(SessionError::NonceReplay { counter });
        }

        if self.is_set(counter) {
            return Err(SessionError::NonceReplay { counter });
        }

        Ok(())
    }

    pub fn update(&mut self, counter: u64) {
        if counter >= self.next {
            let gap = counter.saturating_sub(self.next);
            if gap >= REPLAY_WINDOW_BITS as u64 {
                self.bitmap.iter_mut().for_each(|word| *word = 0);
            } else {
                (self.next..counter).for_each(|i| self.clear(i));
            }
            self.next = counter.saturating_add(1);
        }

        self.set(counter);
    }

    fn is_set(&self, counter: u64) -> bool {
        let bit_idx = counter % REPLAY_WINDOW_BITS as u64;
        let word = (bit_idx / 64) as usize;
        let bit = (bit_idx % 64) as usize;
        ((self.bitmap[word] >> bit) & 1) == 1
    }

    fn set(&mut self, counter: u64) {
        let bit_idx = counter % REPLAY_WINDOW_BITS as u64;
        let word = (bit_idx / 64) as usize;
        let bit = (bit_idx % 64) as usize;
        self.bitmap[word] |= 1u64 << bit;
    }

    fn clear(&mut self, counter: u64) {
        let bit_idx = counter % REPLAY_WINDOW_BITS as u64;
        let word = (bit_idx / 64) as usize;
        let bit = (bit_idx % 64) as usize;
        self.bitmap[word] &= !(1u64 << bit);
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(100)]
    #[case(1000)]
    fn test_sequential_counters(#[case] start: u64) {
        let mut filter = ReplayFilter::new();

        for i in 0..100 {
            let counter = start.saturating_add(i);
            assert!(filter.check(counter).is_ok());
            filter.update(counter);
            assert_eq!(filter.next, counter + 1);
        }
    }

    #[rstest]
    #[case(0)]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_duplicate_detection(#[case] counter: u64) {
        let mut filter = ReplayFilter::new();

        assert!(filter.check(counter).is_ok());
        filter.update(counter);
        assert!(filter.check(counter).is_err());
        assert!(matches!(
            filter.check(counter),
            Err(SessionError::NonceReplay { .. })
        ));
    }

    #[rstest]
    #[case(vec![0, 2, 4, 6, 8])]
    #[case(vec![10, 5, 15, 7, 12])]
    #[case(vec![100, 50, 150, 75, 125])]
    fn test_out_of_order_within_window(#[case] counters: Vec<u64>) {
        let mut filter = ReplayFilter::new();
        let mut seen = std::collections::HashSet::new();

        for counter in counters {
            let is_new = seen.insert(counter);
            let result = filter.check(counter);
            assert_eq!(result.is_ok(), is_new);
            if is_new {
                filter.update(counter);
            }
        }
    }

    #[rstest]
    fn test_window_boundaries() {
        let mut filter = ReplayFilter::new();

        assert!(filter.check(0).is_ok());
        filter.update(0);
        assert!(filter.check(REPLAY_WINDOW_BITS as u64 - 1).is_ok());
        filter.update(REPLAY_WINDOW_BITS as u64 - 1);

        assert!(filter.check(0).is_err());

        assert!(filter.check(REPLAY_WINDOW_BITS as u64).is_ok());
        filter.update(REPLAY_WINDOW_BITS as u64);

        assert!(filter.check(0).is_err());
    }

    #[rstest]
    fn test_far_future_counter() {
        let mut filter = ReplayFilter::new();

        assert!(filter.check(0).is_ok());
        filter.update(0);

        let far_future = REPLAY_WINDOW_BITS as u64 * 2;
        assert!(filter.check(far_future).is_ok());
        filter.update(far_future);

        assert!(filter.check(0).is_err());
        assert!(filter.check(1).is_err());
        assert!(filter
            .check(far_future - REPLAY_WINDOW_BITS as u64)
            .is_err());

        assert!(filter
            .check(far_future - REPLAY_WINDOW_BITS as u64 + 1)
            .is_err());
    }

    #[rstest]
    #[case(0, 1)]
    #[case(0, 10)]
    #[case(0, REPLAY_WINDOW_BITS as u64 - 1)]
    #[case(100, 100 + REPLAY_WINDOW_BITS as u64 - 1)]
    fn test_gap_handling(#[case] start: u64, #[case] end: u64) {
        let mut filter = ReplayFilter::new();

        assert!(filter.check(start).is_ok());
        filter.update(start);
        assert!(filter.check(end).is_ok());
        filter.update(end);

        (start..=end).for_each(|i| {
            let should_accept = i != start && i != end && end - i < REPLAY_WINDOW_BITS as u64;
            let result = filter.check(i);
            assert_eq!(result.is_ok(), should_accept);
            if should_accept {
                filter.update(i);
            }
        });
    }

    #[rstest]
    fn test_bitmap_clearing_on_large_jump() {
        let mut filter = ReplayFilter::new();

        for i in 0..10 {
            assert!(filter.check(i).is_ok());
            filter.update(i);
        }

        let large_jump = REPLAY_WINDOW_BITS as u64 * 2;
        assert!(filter.check(large_jump).is_ok());
        filter.update(large_jump);

        let expected = if (large_jump % REPLAY_WINDOW_BITS as u64) < 64 {
            1u64 << (large_jump % REPLAY_WINDOW_BITS as u64)
        } else {
            0
        };
        if let Some(word) = filter.bitmap.iter().find(|&&word| word != 0) {
            assert_eq!(*word, expected);
        }
    }

    #[rstest]
    #[case(vec![5, 3, 7, 2, 8, 1, 9, 0, 6, 4])]
    #[case(vec![100, 95, 105, 90, 110, 85, 115, 80, 120, 75])]
    fn test_complex_out_of_order_sequence(#[case] sequence: Vec<u64>) {
        let mut filter = ReplayFilter::new();
        let mut processed = vec![];

        for counter in &sequence {
            let result = filter.check(*counter);
            assert!(result.is_ok(), "Counter {} should be accepted", counter);
            filter.update(*counter);
            processed.push(*counter);

            for &prev in &processed {
                assert!(
                    filter.check(prev).is_err(),
                    "Previously seen counter {} should be rejected",
                    prev
                );
            }
        }
    }

    #[rstest]
    fn test_window_wraparound() {
        let mut filter = ReplayFilter::new();

        for i in 0..REPLAY_WINDOW_BITS * 3 {
            let counter = i as u64;
            assert!(filter.check(counter).is_ok());
            filter.update(counter);

            let should_check_old = i > 0
                && counter >= REPLAY_WINDOW_BITS as u64
                && (i - 1) as u64 + REPLAY_WINDOW_BITS as u64 <= counter;

            if should_check_old {
                let old_counter = (i - 1) as u64;
                assert!(filter.check(old_counter).is_err());
            }
        }
    }

    #[rstest]
    #[case(0, REPLAY_WINDOW_BITS as u64)]
    #[case(1000, 1000 + REPLAY_WINDOW_BITS as u64)]
    fn test_edge_of_window(#[case] start: u64, #[case] boundary: u64) {
        let mut filter = ReplayFilter::new();

        assert!(filter.check(boundary).is_ok());
        filter.update(boundary);

        let should_reject_start = start + REPLAY_WINDOW_BITS as u64 <= boundary;
        if should_reject_start {
            assert!(filter.check(start).is_err());
        }

        let should_check_within = boundary > 0 && boundary >= REPLAY_WINDOW_BITS as u64;
        if should_check_within {
            let within_window = boundary - REPLAY_WINDOW_BITS as u64 + 1;
            assert!(filter.check(within_window).is_err());
        }
    }

    proptest! {
        #[test]
        fn prop_no_duplicate_acceptance(counters in prop::collection::vec(0u64..10000, 1..100)) {
            let mut filter = ReplayFilter::new();
            let mut seen = std::collections::HashSet::new();

            for counter in counters {
                let result = filter.check(counter);
                let is_duplicate = seen.contains(&counter);

                if is_duplicate {
                    prop_assert!(result.is_err(), "Duplicate counter {} should be rejected", counter);
                    continue;
                }

                if result.is_ok() {
                    filter.update(counter);
                    seen.insert(counter);
                }

                for &prev in &seen {
                    prop_assert!(filter.check(prev).is_err(), "Previously seen counter {} should be rejected", prev);
                }
            }
        }

        #[test]
        fn prop_sequential_always_accepted(start in any::<u64>(), len in 1usize..1000) {
            let mut filter = ReplayFilter::new();

            for i in 0..len {
                let counter = start.saturating_add(i as u64);
                prop_assert!(filter.check(counter).is_ok());
                filter.update(counter);
            }
        }

        #[test]
        fn prop_old_counters_rejected(
            current in 1000u64..u64::MAX - REPLAY_WINDOW_BITS as u64,
            old_offset in (REPLAY_WINDOW_BITS as u64 + 1)..10000u64
        ) {
            let mut filter = ReplayFilter::new();

            prop_assert!(filter.check(current).is_ok());
            filter.update(current);

            let old_counter = current.saturating_sub(old_offset);
            prop_assert!(filter.check(old_counter).is_err());
        }

        #[test]
        fn prop_window_consistency(counters in prop::collection::vec(0u64..1000, 10..100)) {
            let mut filter = ReplayFilter::new();
            let mut accepted = vec![];

            for counter in counters {
                if filter.check(counter).is_ok() {
                    filter.update(counter);
                    accepted.push(counter);

                    let max_accepted = *accepted.iter().max().unwrap();
                    for &prev in &accepted {
                        if prev + REPLAY_WINDOW_BITS as u64 > max_accepted {
                            prop_assert!(filter.check(prev).is_err());
                        }
                    }
                }
            }
        }

        #[test]
        fn prop_bitmap_integrity(operations in prop::collection::vec((0u64..REPLAY_WINDOW_BITS as u64 * 2, any::<bool>()), 1..200)) {
            let mut filter = ReplayFilter::new();
            let mut expected_set = std::collections::HashSet::new();

            for (counter, should_accept) in operations {
                let is_new = !expected_set.contains(&counter);
                if !should_accept || !is_new {
                    continue;
                }

                if counter >= REPLAY_WINDOW_BITS as u64 {
                    expected_set.clear();
                }
                let result = filter.check(counter);
                if result.is_ok() {
                    filter.update(counter);
                    expected_set.insert(counter);
                }
            }

            for counter in expected_set.iter() {
                if *counter + REPLAY_WINDOW_BITS as u64 > filter.next {
                    prop_assert!(filter.is_set(*counter));
                }
            }
        }

        #[test]
        fn prop_monotonic_next_counter(mut counters in prop::collection::vec(0u64..10000, 1..100)) {
            counters.sort_unstable();
            counters.dedup();

            let mut filter = ReplayFilter::new();
            let mut prev_next = 0u64;

            for counter in counters {
                if filter.check(counter).is_ok() {
                    filter.update(counter);
                    prop_assert!(filter.next > prev_next);
                    prop_assert!(filter.next > counter);
                    prev_next = filter.next;
                }
            }
        }

        #[test]
        fn prop_random_sequence_consistency(sequence in prop::collection::vec(0u64..1000, 1..500)) {
            let mut filter1 = ReplayFilter::new();
            let mut filter2 = ReplayFilter::new();

            let mut accepted1 = vec![];
            for &counter in &sequence {
                if filter1.check(counter).is_ok() {
                    filter1.update(counter);
                    accepted1.push(counter);
                }
            }

            let mut accepted2 = vec![];
            for &counter in &sequence {
                if filter2.check(counter).is_ok() {
                    filter2.update(counter);
                    accepted2.push(counter);
                }
            }

            prop_assert_eq!(accepted1, accepted2);
        }

        #[test]
        fn prop_large_gap_clears_bitmap(
            start in 0u64..1000,
            gap_multiplier in 2u64..10
        ) {
            let mut filter = ReplayFilter::new();

            for i in 0..10 {
                prop_assert!(filter.check(start + i).is_ok());
                filter.update(start + i);
            }

            let large_jump = start + REPLAY_WINDOW_BITS as u64 * gap_multiplier;
            prop_assert!(filter.check(large_jump).is_ok());
            filter.update(large_jump);

            for i in 0..10 {
                let old_counter = start + i;
                prop_assert!(filter.check(old_counter).is_err());
            }
        }
    }
}
