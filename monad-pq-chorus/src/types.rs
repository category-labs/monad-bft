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

use std::cmp::Ordering;

use alloy_rlp::{
    Decodable, EMPTY_LIST_CODE, EMPTY_STRING_CODE, Encodable, Header, encode_list, list_length,
};
use bytes::BufMut;
pub use monad_mcp_chorus::stub::types::{
    NodeId, ProposalId, ProposalIndex, ProposalMap, Slot, TimestampDelta,
};

/// A vote entry and a metablock value: a root (positive) or `⊥`
/// (negative, excluding the proposer).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Entry {
    Root(ProposalId),
    Bot,
}

/// The value finalized for each proposer.
pub type Metablock = ProposalMap<Entry>;

/// Alg. 1 `entry_i[j]`: a path-vote entry.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PathEntry {
    Fast(Entry),
    Fallback(Entry),
}

/// A Coda value: an input value or `EMPTY` (the paper's ALLSAFE), which is
/// never the same as `⊥`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CodaValue<V> {
    Value(V),
    Empty,
}

pub type CodaMetablock<V> = ProposalMap<CodaValue<V>>;

impl Entry {
    pub fn root(&self) -> Option<ProposalId> {
        match self {
            Self::Root(pid) => Some(*pid),
            Self::Bot => None,
        }
    }
}

// roots by pid bytes, then ⊥: the tie-break order of the design (§9.2)
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Root(a), Self::Root(b)) => a.as_bytes().cmp(b.as_bytes()),
            (Self::Root(_), Self::Bot) => Ordering::Less,
            (Self::Bot, Self::Root(_)) => Ordering::Greater,
            (Self::Bot, Self::Bot) => Ordering::Equal,
        }
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PathEntry {
    pub fn value(&self) -> Entry {
        match self {
            Self::Fast(value) | Self::Fallback(value) => *value,
        }
    }

    pub fn is_fast(&self) -> bool {
        matches!(self, Self::Fast(_))
    }
}

impl<V: Ord> Ord for CodaValue<V> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Value(a), Self::Value(b)) => a.cmp(b),
            (Self::Value(_), Self::Empty) => Ordering::Less,
            (Self::Empty, Self::Value(_)) => Ordering::Greater,
            (Self::Empty, Self::Empty) => Ordering::Equal,
        }
    }
}

impl<V: Ord> PartialOrd for CodaValue<V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Entry> for CodaValue<Entry> {
    fn from(entry: Entry) -> Self {
        Self::Value(entry)
    }
}

/// Alg. 1 `ln:qv:fallbackdecide`: EMPTY excludes the proposer like `⊥`.
pub fn coda_output_to_metablock(decided: &CodaMetablock<Entry>) -> Metablock {
    decided.as_ref().map(|value| match value {
        CodaValue::Value(entry) => *entry,
        CodaValue::Empty => Entry::Bot,
    })
}

/// A metablock ordered entry by entry, for deterministic maps and
/// tie-breaks.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct OrdMap<T>(pub ProposalMap<T>);

impl<T: Ord> Ord for OrdMap<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let len = self.0.size().cmp(&other.0.size());
        (0..self.0.size().min(other.0.size()))
            .map(|j| self.0[j].cmp(&other.0[j]))
            .find(|order| order.is_ne())
            .unwrap_or(len)
    }
}

impl<T: Ord> PartialOrd for OrdMap<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Root(pid) is the 32-byte pid string, ⊥ the empty string.
impl Encodable for Entry {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Root(pid) => pid.encode(out),
            Self::Bot => out.put_u8(EMPTY_STRING_CODE),
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Root(pid) => pid.length(),
            Self::Bot => 1,
        }
    }
}

impl Decodable for Entry {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        if buf.first() == Some(&EMPTY_STRING_CODE) {
            *buf = &buf[1..];
            return Ok(Self::Bot);
        }
        Ok(Self::Root(ProposalId::decode(buf)?))
    }
}

const FAST_TAG: u8 = 1;
const FALLBACK_TAG: u8 = 2;

impl PathEntry {
    fn tag(&self) -> u8 {
        match self {
            Self::Fast(_) => FAST_TAG,
            Self::Fallback(_) => FALLBACK_TAG,
        }
    }
}

impl Encodable for PathEntry {
    fn encode(&self, out: &mut dyn BufMut) {
        let value = self.value();
        let fields: [&dyn Encodable; 2] = [&self.tag(), &value];
        encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let value = self.value();
        let fields: [&dyn Encodable; 2] = [&self.tag(), &value];
        list_length::<_, dyn Encodable>(&fields)
    }
}

impl Decodable for PathEntry {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let tag = u8::decode(&mut payload)?;
        let value = Entry::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        match tag {
            FAST_TAG => Ok(Self::Fast(value)),
            FALLBACK_TAG => Ok(Self::Fallback(value)),
            _ => Err(alloy_rlp::Error::Custom("unknown PathEntry tag")),
        }
    }
}

// Value(v) is the list [v], EMPTY the empty list.
impl<V: Encodable> Encodable for CodaValue<V> {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Value(value) => {
                let fields: [&dyn Encodable; 1] = [value];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Empty => out.put_u8(EMPTY_LIST_CODE),
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Value(value) => {
                let fields: [&dyn Encodable; 1] = [value];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::Empty => 1,
        }
    }
}

impl<V: Decodable> Decodable for CodaValue<V> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        if payload.is_empty() {
            return Ok(Self::Empty);
        }
        let value = V::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(Self::Value(value))
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use monad_mcp_chorus::stub::types::ProposalId;

    /// A distinct pid per byte, through the stub's RLP wrapper.
    pub fn pid(byte: u8) -> ProposalId {
        let mut bytes = [0u8; 33];
        bytes[0] = 0xa0;
        bytes[1..].fill(byte);
        alloy_rlp::decode_exact(&bytes[..]).expect("32-byte string")
    }
}

#[cfg(test)]
mod tests {
    use super::{test_util::pid, *};

    fn roundtrip<T: Encodable + Decodable + PartialEq + std::fmt::Debug>(value: T) {
        let bytes = alloy_rlp::encode(&value);
        assert_eq!(bytes.len(), value.length());
        assert_eq!(alloy_rlp::decode_exact::<T>(&bytes).unwrap(), value);
    }

    #[test]
    fn entry_rlp() {
        roundtrip(Entry::Bot);
        roundtrip(Entry::Root(pid(3)));
        assert_eq!(alloy_rlp::encode(Entry::Bot), [EMPTY_STRING_CODE]);
        assert_eq!(Entry::Root(pid(3)).length(), 33);
    }

    #[test]
    fn path_entry_rlp() {
        for value in [Entry::Bot, Entry::Root(pid(1))] {
            roundtrip(PathEntry::Fast(value));
            roundtrip(PathEntry::Fallback(value));
        }
        assert!(alloy_rlp::decode_exact::<PathEntry>(&[0xc2, 3, 0x80][..]).is_err());
    }

    #[test]
    fn coda_value_rlp() {
        roundtrip(CodaValue::<Entry>::Empty);
        roundtrip(CodaValue::Value(Entry::Bot));
        roundtrip(CodaValue::Value(Entry::Root(pid(9))));
        roundtrip(CodaValue::Value(7u8));
        roundtrip(ProposalMap::new(3, |j| {
            [
                CodaValue::Empty,
                CodaValue::Value(Entry::Bot),
                CodaValue::Value(Entry::Root(pid(1))),
            ][j]
        }));
    }

    #[test]
    fn tie_break_order() {
        assert!(Entry::Root(pid(1)) < Entry::Root(pid(2)));
        assert!(Entry::Root(pid(255)) < Entry::Bot);
        assert!(CodaValue::Value(Entry::Bot) < CodaValue::Empty);
        let a = OrdMap(ProposalMap::new(2, |j| [1u8, 2][j]));
        let b = OrdMap(ProposalMap::new(2, |j| [1u8, 3][j]));
        assert!(a < b);
    }

    #[test]
    fn empty_maps_to_bot() {
        let decided = ProposalMap::new(2, |j| {
            [CodaValue::Empty, CodaValue::Value(Entry::Root(pid(4)))][j]
        });
        let finalized = coda_output_to_metablock(&decided);
        assert_eq!(finalized[0], Entry::Bot);
        assert_eq!(finalized[1], Entry::Root(pid(4)));
    }
}
