pub const MAX_SOURCES: usize = 6;

fn xor_eq1(dst: &mut [u8], a: &[u8]) {
    assert_eq!(dst.len(), a.len());

    for i in 0..dst.len() {
        dst[i] ^= a[i];
    }
}

fn xor_eq2(dst: &mut [u8], a: &[u8], b: &[u8]) {
    assert_eq!(dst.len(), a.len());
    assert_eq!(dst.len(), b.len());

    for i in 0..dst.len() {
        dst[i] ^= a[i] ^ b[i];
    }
}

fn xor_eq3(dst: &mut [u8], a: &[u8], b: &[u8], c: &[u8]) {
    assert_eq!(dst.len(), a.len());
    assert_eq!(dst.len(), b.len());
    assert_eq!(dst.len(), c.len());

    for i in 0..dst.len() {
        dst[i] ^= a[i] ^ b[i] ^ c[i];
    }
}

fn xor_eq4(dst: &mut [u8], a: &[u8], b: &[u8], c: &[u8], d: &[u8]) {
    assert_eq!(dst.len(), a.len());
    assert_eq!(dst.len(), b.len());
    assert_eq!(dst.len(), c.len());
    assert_eq!(dst.len(), d.len());

    for i in 0..dst.len() {
        dst[i] ^= a[i] ^ b[i] ^ c[i] ^ d[i];
    }
}

fn xor_eq5(dst: &mut [u8], a: &[u8], b: &[u8], c: &[u8], d: &[u8], e: &[u8]) {
    assert_eq!(dst.len(), a.len());
    assert_eq!(dst.len(), b.len());
    assert_eq!(dst.len(), c.len());
    assert_eq!(dst.len(), d.len());
    assert_eq!(dst.len(), e.len());

    for i in 0..dst.len() {
        dst[i] ^= a[i] ^ b[i] ^ c[i] ^ d[i] ^ e[i];
    }
}

fn xor_eq6(dst: &mut [u8], a: &[u8], b: &[u8], c: &[u8], d: &[u8], e: &[u8], f: &[u8]) {
    assert_eq!(dst.len(), a.len());
    assert_eq!(dst.len(), b.len());
    assert_eq!(dst.len(), c.len());
    assert_eq!(dst.len(), d.len());
    assert_eq!(dst.len(), e.len());
    assert_eq!(dst.len(), f.len());

    for i in 0..dst.len() {
        dst[i] ^= a[i] ^ b[i] ^ c[i] ^ d[i] ^ e[i] ^ f[i];
    }
}

pub fn xor_eq(dst: &mut [u8], src: &[&[u8]]) {
    match src.len() {
        1 => xor_eq1(dst, src[0]),
        2 => xor_eq2(dst, src[0], src[1]),
        3 => xor_eq3(dst, src[0], src[1], src[2]),
        4 => xor_eq4(dst, src[0], src[1], src[2], src[3]),
        5 => xor_eq5(dst, src[0], src[1], src[2], src[3], src[4]),
        6 => xor_eq6(dst, src[0], src[1], src[2], src[3], src[4], src[5]),
        _ => panic!(),
    }
}
