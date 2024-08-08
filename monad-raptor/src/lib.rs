// An implementation of a non-systematic variant of the Raptor code described in RFC 5053.

mod binary_search;

mod matrix;

mod ordered_set;

pub mod r10;
pub use r10::{
    nonsystematic::{
        decoder::{BufferId, Decoder, ManagedDecoder},
        encoder::Encoder,
    },
    parameters::{SOURCE_SYMBOLS_MAX, SOURCE_SYMBOLS_MIN},
};

pub mod xor_eq;
