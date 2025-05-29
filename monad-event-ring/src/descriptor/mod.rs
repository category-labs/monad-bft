use std::marker::PhantomData;

pub(crate) use self::raw::RawEventDescriptor;
use self::raw::RawEventDescriptorInfo;
use crate::{EventDescriptorPayload, EventRingType};

mod raw;

/// The metadata for an event in an [`EventRing`](crate::EventRing).
#[derive(Debug)]
pub struct EventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    raw: RawEventDescriptor<'ring, 'reader>,
    _phantom: PhantomData<T>,
}

impl<'ring, 'reader, T> EventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    pub(crate) fn new(raw: RawEventDescriptor<'ring, 'reader>) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }
}

impl<'ring, 'reader, T> EventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    /// Attempts to read the payload associated with this event descriptor as the associated
    /// [`T::Event`](EventRingType::Event) type.
    pub fn try_read(&self) -> EventDescriptorPayload<T::Event> {
        self.raw.try_filter_map(|raw_info, bytes| {
            let info = EventDescriptorInfo::<T>::new(raw_info);

            let event_ref = T::raw_to_event_ref(info, bytes);

            T::event_ref_to_event(event_ref)
        })
    }

    /// Attempts to selectively read the payload associated with this event descriptor to a
    /// user-specified type.
    ///
    /// This function enables a zero-copy API by allowing downstream consumers to `filter_map` the
    /// [`T::EventRef`](EventRingType::EventRef) type which is a zero-copy view of the underlying
    /// bytes.
    ///
    /// <div class="warning">
    ///
    /// The `filter_map` function `f` **must** be a pure function and thus side-effect free. During
    /// `f`'s execution, it is possible for the underlying paylod bytes to be partially or
    /// completely overwritten which invalidates the zero-copy
    /// [`T::EventRef`](EventRingType::EventRef). In this case, the result of the `filter_map` must
    /// be discarded, which is expressed through the [`EventDescriptorPayload::Expired`] variant.
    /// This requirement is further hinted at through the type definition for `f` which is
    /// intentionally a function pointer instead of a closure to avoid accidentally setting state
    /// outside the `filter_map`. Downstream consumers should **not** attempt to circumvent this
    /// behavior.
    ///
    /// </div>
    pub fn try_filter_map<R: 'static>(
        &self,
        f: fn(event_ref: T::EventRef<'_>) -> Option<R>,
    ) -> EventDescriptorPayload<Option<R>> {
        self.raw.try_filter_map(move |raw_info, bytes| {
            let info = EventDescriptorInfo::<T>::new(raw_info);

            let event_ref = T::raw_to_event_ref(info, bytes);

            f(event_ref)
        })
    }

    /// Attempts to selectively read the payload byte slice associated with this event descriptor to
    /// a user-specified type.
    ///
    /// This function enables a zero-copy API by providing downstream consumers the underlying
    /// payload byte slice. This method should **not** be used unless you explicitly need to work
    /// at a byte-level view.
    ///
    /// <div class="warning">
    ///
    /// See [`try_filter_map`](EventDescriptor::try_filter_map) for important semantics about `f`.
    ///
    /// </div>
    pub fn try_filter_map_raw<R: 'static>(
        &self,
        f: fn(info: EventDescriptorInfo<T>, payload_bytes: &[u8]) -> Option<R>,
    ) -> EventDescriptorPayload<Option<R>> {
        self.raw.try_filter_map(move |raw_info, bytes| {
            let info = EventDescriptorInfo::<T>::new(raw_info);

            f(info, bytes)
        })
    }
}

/// Information associated with an event descriptor.
pub struct EventDescriptorInfo<T>
where
    T: EventRingType,
{
    /// Sequence number used to check liveness / detect gapping.
    pub seqno: u64,

    /// Enables distinguishing between variadic inner event types.
    ///
    /// See [`EventRingType`] for more details.
    pub event_type: u16,

    /// The flow information associated with this event descriptor,
    ///
    /// See [`EventRingType::FlowInfo`] for more details.
    pub flow_info: T::FlowInfo,
}

impl<T> EventDescriptorInfo<T>
where
    T: EventRingType,
{
    fn new(raw: RawEventDescriptorInfo) -> Self {
        Self {
            seqno: raw.seqno,
            event_type: raw.event_type,
            flow_info: T::transmute_flow_info(raw.user),
        }
    }
}
