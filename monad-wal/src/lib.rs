mod aof;
pub mod mock;
pub mod wal;

use std::path::PathBuf;
use std::{error::Error, fmt::Debug};

use monad_executor::{Deserializable, Serializable};

// the persistence layer only accepts one type of message
// we can refactor M to Verified/Unverified type if we write to WAL after verifying the message
pub trait PersistenceLogger: Sized {
    type Event: Serializable + Deserializable + Debug;
    type Error: Error;

    fn new(file_path: PathBuf) -> Result<(Self, Vec<Self::Event>), Self::Error>;
    fn push(&mut self, message: &Self::Event) -> Result<(), Self::Error>;
}
