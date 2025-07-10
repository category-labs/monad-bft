use crate::{
    ffi::{monad_event_descriptor, monad_event_ring_payload_check, monad_event_ring_payload_peek},
    EventDescriptorPayload, RawEventRing,
};

#[derive(Debug)]
pub(crate) struct RawEventDescriptor<'ring> {
    inner: monad_event_descriptor,
    ring: &'ring RawEventRing,
}

impl<'ring> RawEventDescriptor<'ring> {
    pub(crate) fn new(
        ring: &'ring RawEventRing,
        c_event_descriptor: monad_event_descriptor,
    ) -> Self {
        Self {
            inner: c_event_descriptor,
            ring,
        }
    }

    pub(crate) fn try_filter_map<T>(
        &self,
        f: impl FnOnce(RawEventDescriptorInfo, &[u8]) -> T,
    ) -> EventDescriptorPayload<T> {
        let Some(bytes) = monad_event_ring_payload_peek(&self.ring.inner, &self.inner) else {
            return EventDescriptorPayload::Expired;
        };

        let value = f(
            RawEventDescriptorInfo {
                seqno: self.inner.seqno,
                event_type: self.inner.event_type,
                user: self.inner.user,
            },
            bytes,
        );

        if monad_event_ring_payload_check(&self.ring.inner, &self.inner) {
            EventDescriptorPayload::Payload(value)
        } else {
            EventDescriptorPayload::Expired
        }
    }
}

pub(crate) struct RawEventDescriptorInfo {
    pub seqno: u64,

    pub event_type: u16,

    pub user: [u64; 4],
}
