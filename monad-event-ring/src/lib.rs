#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]

//! Idiomatic and safe APIs for interacting with the `monad` execution event ring library.`
//!
//! # Introduction
//!
//! The `monad` execution daemon serves a real-time event notification system that informs clients
//! about execution progress, e.g., "a new proposed block is starting execution" or "transaction 13
//! emitted these 3 logs.".
//!
//! Execution events are communicated using a shared memory broadcast system called the "event ring
//! API." A general overview of event rings is provided in the `monad`
//! [execution daemon repository](<https://github.com/category-labs/monad/tree/main/libs/event/doc/event.md>).
//! The document also describes the low-level C API used by client programs to listen to events
//! which is the basis for this library as a safe and ergonomic wrapper to the core event ring
//! functionality.
//!
//! Notably, the event ring library is a generic broadcast notification system that can be used to
//! emit different kinds of events, not just `monad` execution events. As such, this crate only
//! contains bindings to use the event ring API and **not** the underlying event types themselves.
//! For `monad` execution event types to interact with the `monad` execution daemon, see the
//! [`monad_exec_events`](../monad_exec_events/index.html) library.
//!
//! ## API Usage Overview
//!
//! To begin using the event ring API, you must first create an [`EventRing`] which is responsible
//! for loading the event ring's shared memory mappings into the current process's address space.
//! The lifetime of this object thus controls when the memory mappings are unloaded (on [`Drop`]).
//! This type implements Send + Sync and can thus be shared by wrapping it with an
//! [`Arc`](std::sync::Arc).
//!
//! Now that the event ring is available in the process's address space, we can now start to consume
//! events using the [`EventReader`] which can be created by calling
//! [`TypedEventRing::create_reader`] on the [`EventRing`]. Event readers are similar in spirit to
//! an iterator and provide the [`EventReader::next`] method which produces
//! [`EventNextResult::Ready`] when there is another event available for consumption and
//! [`EventNextResult::NotReady`] when there isn't. If another event is writen to the event ring
//! after the reader produces an [`EventNextResult::NotReady`], subsequently calling
//! [`EventReader::next`] will eventually produce the event written. Unlike iterators however, event
//! rings are backed by a fixed-size descriptor array and a fixed-size payload array, both of which
//! can be overwritten if the current process is unable to consume events at the same rate they are
//! being produced and falls behind. If the next descriptor in the iteration sequence has been
//! overwritten by a newer descriptor, the user is informed through the [`EventNextResult::Gap`]
//! variant. Similarly, if the payload pointed to by an [`EventDescriptor`] is overwritten while
//! attempting to read it through the various [`EventDescriptor::try_*`] methods, the user is
//! informed through the [`EventDescriptorPayload::Expired`] variant. Once an event descriptor or
//! payload is overwritten, it is **unrecoverable** from the event ring. Programs that depend on
//! consuming all events of some kind produced by an event ring **must** enter a recovery phase to
//! recover the (possibly) missing data.
//!
//! <div class="warning">
//!
//! Production applications should **never** rely on the [`EventReader`] keeping up and **must**
//! implement a recovery phase which should be triggered when the event ring gaps. This mechanism
//! should be thoroughly tested, likely using a [`SnapshotEventRing`] in tests, to ensure smooth
//! operation in the unlikely event the reader falls behind.
//!
//! **Failing to recover / backfill missing events after an [`EventNextResult::Gap`] and/or ignoring
//! the variant will most likely lead to state inconsistency.**
//!
//! </div>
//!
//! Unlike [`TypedEventRing`]s, [`EventReader`]s are single threaded as they use the lifetime
//! of a reference to a [`TypedEventRing`] to ensure that the address space where events are stored
//! is still mapped. That being said, you can create multiple readers from the same event ring which
//! iterate independent of each other. In other words, all event readers produce every event exactly
//! once.

pub use self::{descriptor::*, r#type::*, reader::*, result::*, ring::*};

pub mod ffi;

mod descriptor;
mod reader;
mod result;
mod ring;
mod r#type;
