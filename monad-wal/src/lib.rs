pub mod aof;
pub mod mock;
pub mod wal;

use std::{error::Error, fmt::Debug, io};

use auto_impl::auto_impl;

pub trait PersistenceLoggerBuilder {
    type PersistenceLogger: PersistenceLogger;

    /// Create new file for logging if none exists or load an existing to continue appending
    /// and return the Events it contained for replay
    fn build(
        self,
    ) -> Result<
        (
            Self::PersistenceLogger,
            Vec<<Self::PersistenceLogger as PersistenceLogger>::Event>,
        ),
        WALError,
    >;
}

/// PersistenceLogger is used to log all events input to MonadState and have them
/// available for replay in case of nodes crashing
///
/// A situation where this would be required is if N nodes are in the network and
/// all N nodes crash, the nodes need to remember where they were in the round and
/// what votes they had cast (so they don't double vote)
///
/// the persistence layer only accepts one type of message
/// we can refactor M to Verified/Unverified type if we write to WAL after verifying the message
#[auto_impl(Box)]
pub trait PersistenceLogger {
    /// The Event type to be logged
    type Event;

    /// Add an event to the log
    fn push(&mut self, message: &Self::Event) -> Result<(), WALError>;
}

#[derive(Debug)]
pub enum WALError {
    IOError(io::Error),
    DeserError(Box<dyn Error>),
}

impl From<io::Error> for WALError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl std::fmt::Display for WALError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(self, f)
    }
}

impl std::error::Error for WALError {}
