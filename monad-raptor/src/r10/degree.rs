pub const MAX_V: u32 = 1048576;
pub const MAX_DEGREE: usize = 40;

// Compute the Degree Generator function Deg() defined in RFC 5053 section 5.4.4.2.
pub fn deg(v: u32) -> u8 {
    match v {
        0..=10240 => 1,
        10241..=491581 => 2,
        491582..=712793 => 3,
        712794..=831694 => 4,
        831695..=948445 => 10,
        948446..=1032188 => 11,
        1032189..=1048575 => 40,
        _ => panic!("Can't find Deg({})", v),
    }
}
