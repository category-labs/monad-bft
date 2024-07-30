// Find the smallest integer in [from, to) satisfying `condition`.
pub fn smallest_integer_satisfying(
    from: usize,
    to: usize,
    condition: impl Fn(usize) -> bool,
) -> Option<usize> {
    let mut lower = from;
    let mut upper = to;

    while lower < upper {
        let pivot = (lower + upper) / 2;

        if condition(pivot) {
            // If `pivot` satisfies the condition, the smallest integer satisfying the
            // condition must be in the interval [lower, pivot].
            upper = pivot;
        } else {
            // If `pivot` does not satisfy the condition, the smallest integer satifying
            // the condition, if it exists, must be in the interval [pivot + 1, upper].
            lower = pivot + 1;
        }
    }

    if lower == upper && upper < to {
        Some(upper)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;

    #[test]
    fn test_binary_search() {
        let mut rng = rand::thread_rng();

        for _ in 0..100000 {
            let interval_end = rng.gen_range(0..100);

            let threshold = rng.gen_range(0..=interval_end);

            let recovered_threshold =
                super::smallest_integer_satisfying(0, interval_end, |pivot| pivot >= threshold);

            assert_eq!(
                recovered_threshold,
                if threshold < interval_end {
                    Some(threshold)
                } else {
                    None
                }
            );
        }
    }
}
