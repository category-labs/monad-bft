#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]

//! Idiomatic and safe types for interacting with `monad` execution event rings.
//!
//! # Quickstart
//!
//! Add `monad-event-ring` and `monad-exec-events` as dependencies to your Cargo.toml.
//!
//! ```bash
//! cargo add --git https://github.com/monad-crypto/monad-bft monad-event-ring monad-exec-events
//! ```
//!
//! Next, create an event ring and start consuming events!
//!
//! ```rust
//! let event_ring = ExecEventRing::new_from_path(event_ring_path).unwrap();
//!
//! let mut event_reader = event_ring.create_reader();
//!
//! loop {
//!     let event_descriptor = match event_reader.next() {
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
//!         EventDescriptor::Payload(exec_event) => {
//!             // Do something with the event!
//!         }
//!     }
//! }
//! ```
//!
//!
//! For more details about EventRing operation in general, see the
//! [`monad_event_ring`](../monad_event_ring/index.html) library documentation.
//!
//! # Block-Oriented Updates
//!
//! The [`ExecEventRingType`] enables an `EventRing` to produce individual `monad` execution events.
//! While many applications may benefit from operating on individual events or by observing them
//! as quickly as possible, many would prefer to process entire blocks. This library provides the
//! [`ExecutedBlockBuilder`] utility to reconstruct blocks from an event stream produced by an
//! [`ExecEventRing`]. See [`ExecutedBlockBuilder`] for more details.
//!
//! This utility however produces all blocks that are executed, which could be in any
//! [`BlockCommitState`]. For applications interested in consuming blocks once they have reached a
//! certain commit state, usually [`BlockCommitState::Finalized`], applications can use the
//! [`CommitStateBlockBuilder`] which produces a block along with its current commit state every time
//! the block's commit state changes. See [`CommitStateBlockBuilder`] for more details.

use monad_event_ring::{EventRing, SnapshotEventRing};

pub use self::events::*;

mod events;
pub mod ffi;

/// A type alias for an event ring that produces monad execution events.
pub type ExecEventRing = EventRing<ExecEventRingType>;

/// A type alias for a snapshot event ring that produces monad execution events.
pub type ExecSnapshotEventRing = SnapshotEventRing<ExecEventRingType>;
