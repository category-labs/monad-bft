pub mod aof;
pub mod mock;
pub mod wal;

use std::error::Error;

/// PersistenceLogger is used to log all events input to MonadState and have them
/// available for replay in case of nodes crashing
///
/// A situation where this would be required is if N nodes are in the network and
/// all N nodes crash, the nodes need to remember where they were in the round and
/// what votes they had cast (so they don't double vote)
///
/// the persistence layer only accepts one type of message
/// we can refactor M to Verified/Unverified type if we write to WAL after verifying the message
pub trait PersistenceLogger: Sized {
    /// Config for the file implementation
    type Config;

    /// The Event type to be logged
    type Event;
    type Error: Error;

    /// Create new file for logging if none exists or load an existing to continue appending
    /// and return the Events it contained for replay
    fn new(config: Self::Config) -> Result<(Self, Vec<Self::Event>), Self::Error>;

    /// Add an event to the log
    fn push(&mut self, message: &Self::Event) -> Result<(), Self::Error>;
}
