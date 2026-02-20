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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EthHexDecodeError {
    InvalidLen,
    ParseErr,
}

impl From<hex::FromHexError> for EthHexDecodeError {
    fn from(e: hex::FromHexError) -> Self {
        match e {
            hex::FromHexError::InvalidStringLength => EthHexDecodeError::InvalidLen,
            hex::FromHexError::OddLength => EthHexDecodeError::InvalidLen,
            hex::FromHexError::InvalidHexCharacter { .. } => EthHexDecodeError::ParseErr,
        }
    }
}

pub fn encode_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub fn decode_bytes(s: &str) -> Result<Vec<u8>, EthHexDecodeError> {
    if s.is_empty() {
        return Err(EthHexDecodeError::InvalidLen);
    }
    let noprefix = s.strip_prefix("0x").ok_or(EthHexDecodeError::ParseErr)?;
    hex::decode(noprefix).map_err(Into::into)
}

pub fn decode_quantity(s: &str) -> Result<u64, EthHexDecodeError> {
    if s.is_empty() {
        return Err(EthHexDecodeError::InvalidLen);
    }

    let noprefix = s.strip_prefix("0x").ok_or(EthHexDecodeError::ParseErr)?;

    {
        let mut noprefix_chars = noprefix.chars();

        let Some(noprefix_first_char) = noprefix_chars.next() else {
            return Err(EthHexDecodeError::ParseErr);
        };

        if noprefix_first_char == '0' {
            return match noprefix_chars.next() {
                None => Ok(0),
                Some(_) => Err(EthHexDecodeError::ParseErr),
            };
        } else if noprefix_first_char == '+' || noprefix_first_char == '-' {
            return Err(EthHexDecodeError::ParseErr);
        }
    }

    u64::from_str_radix(noprefix, 16).map_err(|_| EthHexDecodeError::ParseErr)
}

#[cfg(test)]
mod test {
    use super::{decode_bytes, decode_quantity, encode_bytes, EthHexDecodeError};

    #[test]
    fn test_hex_invalid_len() {
        assert_eq!(Err(EthHexDecodeError::InvalidLen), decode_bytes("0x123"));
        assert_eq!(Err(EthHexDecodeError::InvalidLen), decode_bytes(""));
    }

    #[test]
    fn test_hex_parse_err() {
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_bytes("1234"));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_bytes("x012"));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_bytes("0xbbb˝a"));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_bytes("0xghijkl"));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_bytes("0x12+3"));
    }

    #[test]
    fn test_hex_decode() {
        assert_eq!(Ok(vec![171_u8]), decode_bytes("0xab"));
        assert_eq!(Ok(vec![171_u8]), decode_bytes("0xAB"));
        assert!(decode_bytes(
            "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
        )
        .is_ok());
        assert_eq!(Ok(vec![]), decode_bytes("0x"));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(&encode_bytes(&[171_u8]), "0xab");

        let hex =
            "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675";
        let bytes = decode_bytes(hex).unwrap();
        assert_eq!(&encode_bytes(&bytes), hex);
    }

    #[test]
    fn test_hex_quantity_decode() {
        assert_eq!(Err(EthHexDecodeError::InvalidLen), decode_quantity(""));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_quantity("x"));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_quantity("0x"));
        assert_eq!(Ok(0), decode_quantity("0x0"));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_quantity("0x0400"));
        assert_eq!(Ok(1024), decode_quantity("0x400"));
        assert_eq!(Err(EthHexDecodeError::ParseErr), decode_quantity("0x12+3"));
        assert_eq!(
            Err(EthHexDecodeError::ParseErr),
            decode_quantity("0x+deadbeef")
        );
    }
}
