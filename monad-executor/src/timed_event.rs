use monad_types::{Deserializable, Serializable};
use std::array::TryFromSliceError;
use std::fmt::Debug;
use std::time::Duration;

#[derive(Clone)]
pub struct TimedEvent<M> {
    pub timestamp: Duration, // ticks Duration in milliseconds
    pub event: M,
}

impl<M: Serializable> Serializable for TimedEvent<M> {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.timestamp.as_secs_f64().to_be_bytes());
        buf.extend_from_slice(&self.event.serialize());
        buf
    }
}

impl<M: Deserializable> Deserializable for TimedEvent<M> {
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
