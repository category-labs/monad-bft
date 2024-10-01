// Copies of various lookup tables and implementations of various parameter generation
// functions for the Raptor code described in RFC 5053.

pub mod a;
pub mod degree;
pub mod half;
pub mod ldpc;
pub mod lt;
pub mod matrix;
pub mod nonsystematic;
pub mod parameters;
pub mod rand;
pub mod systematic_index;

pub use degree::{deg, MAX_DEGREE};
pub use parameters::{CodeParameters, SOURCE_SYMBOLS_MAX, SOURCE_SYMBOLS_MIN};
pub use rand::rand;
