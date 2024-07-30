// An implementation of a non-systematic variant of the Raptor code described in RFC 5053.

mod binary_search;

pub mod r10;
pub use r10::nonsystematic::encoder::Encoder;

pub mod xor_eq;
