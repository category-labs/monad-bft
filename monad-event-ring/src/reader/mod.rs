use std::marker::PhantomData;

pub(crate) use self::raw::RawEventReader;
use crate::{EventDescriptor, EventNextResult, EventRingType};

mod raw;

/// An iterator over events in an [`EventRing`](crate::EventRing).
pub struct EventReader<'ring, T>
where
    T: EventRingType,
{
    pub(crate) raw: RawEventReader<'ring>,
    _phantom: PhantomData<T>,
}

impl<'ring, T> EventReader<'ring, T>
where
    T: EventRingType,
{
    pub(crate) fn new(raw: RawEventReader<'ring>) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn new_snapshot(mut raw: RawEventReader<'ring>) -> Self {
        raw.inner.read_last_seqno = 0;

        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    /// Produces the next event in the ring.
    pub fn next<'reader>(&'reader mut self) -> EventNextResult<EventDescriptor<'ring, 'reader, T>> {
        self.raw.next().map(EventDescriptor::new)
    }
}
