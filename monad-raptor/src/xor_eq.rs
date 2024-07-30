pub const MAX_SOURCES: usize = 6;

fn xor_eq1<const LEN: usize>(dst: &mut [u8; LEN], a: &[u8; LEN]) {
    for i in 0..LEN {
        dst[i] ^= a[i];
    }
}

fn xor_eq2<const LEN: usize>(dst: &mut [u8; LEN], a: &[u8; LEN], b: &[u8; LEN]) {
    for i in 0..LEN {
        dst[i] ^= a[i] ^ b[i];
    }
}

fn xor_eq3<const LEN: usize>(dst: &mut [u8; LEN], a: &[u8; LEN], b: &[u8; LEN], c: &[u8; LEN]) {
    for i in 0..LEN {
        dst[i] ^= a[i] ^ b[i] ^ c[i];
    }
}

fn xor_eq4<const LEN: usize>(
    dst: &mut [u8; LEN],
    a: &[u8; LEN],
    b: &[u8; LEN],
    c: &[u8; LEN],
    d: &[u8; LEN],
) {
    for i in 0..LEN {
        dst[i] ^= a[i] ^ b[i] ^ c[i] ^ d[i];
    }
}

fn xor_eq5<const LEN: usize>(
    dst: &mut [u8; LEN],
    a: &[u8; LEN],
    b: &[u8; LEN],
    c: &[u8; LEN],
    d: &[u8; LEN],
    e: &[u8; LEN],
) {
    for i in 0..LEN {
        dst[i] ^= a[i] ^ b[i] ^ c[i] ^ d[i] ^ e[i];
    }
}

fn xor_eq6<const LEN: usize>(
    dst: &mut [u8; LEN],
    a: &[u8; LEN],
    b: &[u8; LEN],
    c: &[u8; LEN],
    d: &[u8; LEN],
    e: &[u8; LEN],
    f: &[u8; LEN],
) {
    for i in 0..LEN {
        dst[i] ^= a[i] ^ b[i] ^ c[i] ^ d[i] ^ e[i] ^ f[i];
    }
}

pub fn xor_eq<const LEN: usize>(dst: &mut [u8; LEN], src: &[&[u8; LEN]]) {
    match src.len() {
        1 => xor_eq1::<LEN>(dst, src[0]),
        2 => xor_eq2::<LEN>(dst, src[0], src[1]),
        3 => xor_eq3::<LEN>(dst, src[0], src[1], src[2]),
        4 => xor_eq4::<LEN>(dst, src[0], src[1], src[2], src[3]),
        5 => xor_eq5::<LEN>(dst, src[0], src[1], src[2], src[3], src[4]),
        6 => xor_eq6::<LEN>(dst, src[0], src[1], src[2], src[3], src[4], src[5]),
        _ => panic!(),
    }
}
