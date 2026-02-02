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

use std::time::{SystemTime, UNIX_EPOCH};

use zerocopy::{BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, U32, U64};

// TAI64 offset from Unix epoch: 2^62 (1970-01-01 00:00:00 TAI) + 37 (leap seconds from UTC to TAI)
const TAI64_UNIX_EPOCH: u64 = 37 + (1u64 << 62);

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
)]
#[repr(C, packed)]
pub struct Tai64N {
    seconds: U64<BigEndian>,
    nanoseconds: U32<BigEndian>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Tai64NError {
    #[error("nanoseconds {0} exceed 1 second")]
    NanosOverflow(u32),
}

impl From<Tai64N> for [u8; 12] {
    fn from(tai: Tai64N) -> Self {
        zerocopy::transmute!(tai)
    }
}

impl TryFrom<[u8; 12]> for Tai64N {
    type Error = Tai64NError;

    fn try_from(bytes: [u8; 12]) -> Result<Self, Self::Error> {
        let tai: Self = zerocopy::transmute!(bytes);
        let nanos = tai.nanoseconds.get();
        if nanos >= 1_000_000_000 {
            return Err(Tai64NError::NanosOverflow(nanos));
        }
        Ok(tai)
    }
}

impl From<SystemTime> for Tai64N {
    fn from(t: SystemTime) -> Self {
        let duration = t.duration_since(UNIX_EPOCH).unwrap_or_default();
        Self {
            seconds: U64::new(duration.as_secs().wrapping_add(TAI64_UNIX_EPOCH)),
            nanoseconds: U32::new(duration.subsec_nanos()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let now: Tai64N = SystemTime::now().into();
        let bytes: [u8; 12] = now.into();
        let decoded = Tai64N::try_from(bytes).unwrap();
        assert_eq!(now, decoded);
    }

    #[test]
    fn from_system_time() {
        let t: Tai64N = SystemTime::now().into();
        let bytes: [u8; 12] = t.into();
        assert_eq!(bytes.len(), 12);
        let seconds = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        assert!(seconds > TAI64_UNIX_EPOCH);
    }

    #[test]
    fn ordering() {
        let a = Tai64N {
            seconds: U64::new(100),
            nanoseconds: U32::new(0),
        };
        let b = Tai64N {
            seconds: U64::new(100),
            nanoseconds: U32::new(1),
        };
        let c = Tai64N {
            seconds: U64::new(101),
            nanoseconds: U32::new(0),
        };
        assert!(a < b);
        assert!(b < c);
        assert_eq!(a.max(b), b);
    }

    #[test]
    fn invalid_nanos() {
        let mut bytes = [0u8; 12];
        bytes[8..].copy_from_slice(&1_000_000_000u32.to_be_bytes());
        assert!(Tai64N::try_from(bytes).is_err());
    }
}
