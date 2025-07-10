use crate::{
    ffi::{
        self, monad_event_descriptor, monad_event_iterator_reset, monad_event_iterator_try_next,
        monad_event_next_result,
    },
    RawEventDescriptor, RawEventReader,
};

/// The result of attempting to retrieve the next event from an [`EventRing`](crate::EventRing).
pub enum EventNextResult<T> {
    /// The next event is available and produced through `T`.
    Ready(T),

    /// The next event is not available.
    NotReady,

    /// The next event was lost due to a gap.
    ///
    /// Receiving this variant is a strong indicator that downstream consumers must switch to a
    /// recovery phase to backfill the data lost from the missing events. You should **not** ignore
    /// this variant unless you are aware of its implications. See
    /// [`EventReader`](crate::EventReader) for more details.
    Gap,
}

impl<'ring> EventNextResult<RawEventDescriptor<'ring>> {
    pub(crate) fn new_from_raw(reader: &mut RawEventReader<'ring>) -> Self {
        let (c_next_result, c_event_descriptor): (monad_event_next_result, monad_event_descriptor) =
            monad_event_iterator_try_next(&mut reader.inner);

        match c_next_result {
            ffi::monad_event_next_result_MONAD_EVENT_SUCCESS => Self::Ready(
                RawEventDescriptor::new(reader.event_ring, c_event_descriptor),
            ),
            ffi::monad_event_next_result_MONAD_EVENT_NOT_READY => Self::NotReady,
            ffi::monad_event_next_result_MONAD_EVENT_GAP => {
                monad_event_iterator_reset(&mut reader.inner);
                Self::Gap
            }
            _ => panic!("EventNextResult encountered unknown value {c_next_result}"),
        }
    }

    pub(crate) fn map<T>(
        self,
        f: impl FnOnce(RawEventDescriptor<'ring>) -> T,
    ) -> EventNextResult<T> {
        match self {
            EventNextResult::Ready(descriptor) => EventNextResult::Ready(f(descriptor)),
            EventNextResult::NotReady => EventNextResult::NotReady,
            EventNextResult::Gap => EventNextResult::Gap,
        }
    }
}

/// The result of attempting to read the payload from an
/// [`EventDescriptor`](crate::EventDescriptor).
#[derive(Debug)]
pub enum EventDescriptorPayload<T> {
    /// The payload was successfully retrieved.
    Payload(T),

    /// The payload's bytes were overwritten while reading them and the result is thus invalid.
    Expired,
}

impl<T> EventDescriptorPayload<T> {
    /// Maps the event descriptor [`Payload`](EventDescriptorPayload::Payload) variant to another
    /// type using the provided lambda.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> EventDescriptorPayload<U> {
        match self {
            EventDescriptorPayload::Payload(payload) => EventDescriptorPayload::Payload(f(payload)),
            EventDescriptorPayload::Expired => EventDescriptorPayload::Expired,
        }
    }
}
