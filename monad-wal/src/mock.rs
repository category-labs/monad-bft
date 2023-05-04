use std::fmt::Debug;
use std::marker::PhantomData;

use monad_executor::{Deserializable, Serializable};

use crate::PersistenceLogger;

pub struct MockWALogger<M: Serializable + Deserializable> {
    _marker: PhantomData<M>,
}

#[derive(Debug)]
pub struct MockWALoggerError {}

impl std::fmt::Display for MockWALoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(self, f)
    }
}

impl std::error::Error for MockWALoggerError {}

impl<M: Serializable + Deserializable + Debug> PersistenceLogger for MockWALogger<M> {
    type Event = M;
    type Error = MockWALoggerError;

    fn new(_file_path: std::path::PathBuf) -> Result<(Self, Vec<Self::Event>), Self::Error> {
        Ok((
            MockWALogger {
                _marker: PhantomData,
            },
            Vec::new(),
        ))
    }

    fn push(&mut self, _message: &Self::Event) -> Result<(), Self::Error> {
        Ok(())
    }
}
