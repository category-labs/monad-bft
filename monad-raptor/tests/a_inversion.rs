use std::cmp::min;

use monad_raptor::r10::{CodeParameters, SOURCE_SYMBOLS_MAX, SOURCE_SYMBOLS_MIN};
use rayon::prelude::*;

// Verify that the A matrix is invertible for the given range of num_source_symbols values.
fn check_a_invertible(from: usize, to: usize) {
    (from..=to).into_par_iter().for_each(|num_source_symbols| {
        println!("Testing {}", num_source_symbols);

        let params = CodeParameters::new(num_source_symbols).unwrap();

        let a = params.a_systematic_intermediate_matrix();
        if !a.check_invertible() {
            panic!("Inverting A matrix for K = {} failed", num_source_symbols);
        }
    });
}

#[test]
fn check_a_invertible_small() {
    check_a_invertible(SOURCE_SYMBOLS_MIN, min(128, SOURCE_SYMBOLS_MAX));
}

#[test]
#[ignore]
fn check_a_invertible_all() {
    check_a_invertible(SOURCE_SYMBOLS_MIN, SOURCE_SYMBOLS_MAX);
}
