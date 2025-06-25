#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]

//! Idiomatic and safe types for interacting with `monad` execution event rings.
//!
//! # Quickstart
//!
//! Add `monad-event-ring` and `monad-exec-events` as dependencies to your Cargo.toml.
//!
//! ```custom
//! cargo add --git https://github.com/monad-crypto/monad-bft monad-event-ring monad-exec-events
//! ```
//!
//! Next, create an event ring and start consuming events!
//!
//! ```no_run
//! # use monad_event_ring::{DecodedEventRing, EventDescriptor, EventDescriptorPayload, EventNextResult};
//! # use monad_exec_events::{ExecEventRing};
//! #
//! # let event_ring_path: &'static str = unimplemented!();
//! #
//! let event_ring = ExecEventRing::new_from_path(event_ring_path).unwrap();
//!
//! let mut event_reader = event_ring.create_reader();
//!
//! loop {
//!     let event_descriptor = match event_reader.next_descriptor() {
//!         EventNextResult::Gap => {
//!             // Handle EventRing gap!
//!             unimplemented!();
//!         }
//!         EventNextResult::NotReady => {
//!             // This will busy-wait until an event is ready.
//!             continue;
//!         }
//!         EventNextResult::Ready(event_descriptor) => event_descriptor,
//!     };
//!
//!     match event_descriptor.try_read() {
//!         EventDescriptorPayload::Expired => {
//!             // Handle EventRing payload expiry (similar to gap)!
//!             unimplemented!();
//!         }
//!         EventDescriptorPayload::Payload(exec_event) => {
//!             // Do something with the event!
//!         }
//!     }
//! }
//! ```
//!
//!
//! For more details about EventRing operation in general, see the
//! [`monad_event_ring`](../monad_event_ring/index.html) library documentation.

use monad_event_ring::{EventRing, SnapshotEventRing};

pub use self::events::*;

mod events;
pub mod ffi;

/// A type alias for an event ring that produces monad execution events.
pub type ExecEventRing = EventRing<ExecEventDecoder>;

/// A type alias for a snapshot event ring that produces monad execution events.
pub type ExecSnapshotEventRing = SnapshotEventRing<ExecEventDecoder>;
