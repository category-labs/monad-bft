mod aof;
pub mod mock;
pub mod wal;

use std::error::Error;

// the persistence layer only accepts one type of message
// we can refactor M to Verified/Unverified type if we write to WAL after verifying the message
pub trait PersistenceLogger: Sized {
    type Config;
    type Event;
    type Error: Error;

    fn new(config: Self::Config) -> Result<(Self, Vec<Self::Event>), Self::Error>;
    fn push(&mut self, message: &Self::Event) -> Result<(), Self::Error>;
}
