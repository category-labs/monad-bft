#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DecodeHexError {
    InvalidLen,
    ParseErr,
}

fn nibble_to_char(nibble: u8) -> char {
    let nibble = nibble & 0xF;
    if nibble < 0xA {
        (b'0' + nibble) as char
    } else {
        (b'a' + nibble - 0xA) as char
    }
}

pub fn encode(bytes: &[u8]) -> String {
    let mut chars = vec!['0', 'x'];

    for byte in bytes {
        chars.push(nibble_to_char(byte >> 4));
        chars.push(nibble_to_char(*byte));
    }

    chars.into_iter().collect()
}

pub fn decode(s: &str) -> Result<Vec<u8>, DecodeHexError> {
    if s.len() & 1 == 1 || s.is_empty() {
        return Err(DecodeHexError::InvalidLen);
    }

    if !s.is_ascii() {
        return Err(DecodeHexError::ParseErr);
    }

    let Some(noprefix) = s.strip_prefix("0x") else {
        return Err(DecodeHexError::ParseErr);
    };

    if noprefix.is_empty() {
        return Ok(vec![]);
    }

    decode_even_suffix(noprefix)
}

// Must be prefixed by 0x
// No leading zeros allowed
pub fn decode_quantity(s: &str) -> Result<u64, DecodeHexError> {
    if s.is_empty() {
        return Err(DecodeHexError::InvalidLen);
    }

    let Some(noprefix) = s.strip_prefix("0x") else {
        return Err(DecodeHexError::ParseErr);
    };

    if noprefix.is_empty() {
        return Err(DecodeHexError::ParseErr);
    }

    // leading 0 is invalid but '0x0' is special case
    if let Some(leading_zero) = noprefix.strip_prefix("0") {
        if leading_zero.is_empty() {
            return Ok(0);
        }
        return Err(DecodeHexError::ParseErr);
    }

    u64::from_str_radix(noprefix, 16).map_err(|_| DecodeHexError::ParseErr)
}

fn decode_even_suffix(s: &str) -> Result<Vec<u8>, DecodeHexError> {
    debug_assert!(s.len() & 1 == 0);
    debug_assert!(!s.is_empty());
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_e| DecodeHexError::ParseErr))
        .collect()
}

#[cfg(test)]
mod test {
    use crate::hex::{decode, decode_quantity, encode, DecodeHexError};

    #[test]
    fn test_hex_invalid_len() {
        assert_eq!(Err(DecodeHexError::InvalidLen), decode("0x123"));
        assert_eq!(Err(DecodeHexError::InvalidLen), decode(""));
    }

    #[test]
    fn test_hex_parse_err() {
        assert_eq!(Err(DecodeHexError::ParseErr), decode("1234"));
        assert_eq!(Err(DecodeHexError::ParseErr), decode("x012"));
        assert_eq!(Err(DecodeHexError::ParseErr), decode("0xbbb˝a"));
        assert_eq!(Err(DecodeHexError::ParseErr), decode("0xghijkl"));
    }

    #[test]
    fn test_hex_decode() {
        assert_eq!(Ok(vec![171_u8]), decode("0xab"));
        assert_eq!(Ok(vec![171_u8]), decode("0xAB"));
        assert!(decode(
            "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
        )
        .is_ok());
        assert_eq!(Ok(vec![]), decode("0x"));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(&encode(&[171_u8]), "0xab");

        let hex =
            "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675";
        let bytes = decode(hex).unwrap();
        assert_eq!(&encode(&bytes), hex);
    }

    #[test]
    fn test_hex_quantity_decode() {
        assert_eq!(Err(DecodeHexError::InvalidLen), decode_quantity(""));
        assert_eq!(Err(DecodeHexError::ParseErr), decode_quantity("x"));
        assert_eq!(Err(DecodeHexError::ParseErr), decode_quantity("0x"));
        assert_eq!(Ok(0), decode_quantity("0x0"));
        assert_eq!(Err(DecodeHexError::ParseErr), decode_quantity("0x0400"));
        assert_eq!(Ok(1024), decode_quantity("0x400"));
    }
}
