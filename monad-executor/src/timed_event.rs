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

use std::{array::TryFromSliceError, fmt::Debug, time::Duration};

use bytes::{Bytes, BytesMut};
use monad_types::{Deserializable, Serializable};

#[derive(Clone, PartialEq, Eq)]
pub struct TimedEvent<M> {
    pub timestamp: Duration, // ticks Duration in milliseconds
    pub event: M,
}

impl<M: Serializable<Bytes>> Serializable<Bytes> for TimedEvent<M> {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.timestamp.as_secs_f64().to_be_bytes());
        buf.extend_from_slice(&self.event.serialize());
        buf.into()
    }
}

impl<M: Deserializable<[u8]>> Deserializable<[u8]> for TimedEvent<M> {
    type ReadError = TryFromSliceError;
    fn deserialize(buf: &[u8]) -> Result<Self, Self::ReadError> {
        let (timestamp, buf) = buf.split_at(std::mem::size_of::<f64>());
        let timestamp = f64::from_be_bytes(timestamp.try_into().unwrap());
        let event = M::deserialize(buf).unwrap();
        Ok(Self {
            timestamp: Duration::from_secs_f64(timestamp),
            event,
        })
    }
}

impl<M: Debug> Debug for TimedEvent<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimedEvent")
            .field("ticks", &self.timestamp)
            .field("event", &self.event)
            .finish()
    }
}
