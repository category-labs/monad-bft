use std::marker::PhantomData;

pub(crate) use self::raw::RawEventReader;
use crate::{ffi::monad_event_iterator_reset, EventDecoder, EventDescriptor, EventNextResult};

mod raw;

/// Used to consume events from an [`EventRing`](crate::EventRing).
pub struct EventReader<'ring, D>
where
    D: EventDecoder,
{
    pub(crate) raw: RawEventReader<'ring>,
    _phantom: PhantomData<D>,
}

impl<'ring, D> EventReader<'ring, D>
where
    D: EventDecoder,
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
    pub fn next_descriptor(&mut self) -> EventNextResult<EventDescriptor<'ring, D>> {
        self.raw.next_descriptor().map(EventDescriptor::new)
    }

    /// Resets the reader to the latest event in the ring.
    pub fn reset(&mut self) {
        monad_event_iterator_reset(&mut self.raw.inner);
    }
}
