use std::fmt::Debug;
use std::marker::PhantomData;

use crate::PersistenceLogger;

pub struct MockWALogger<M> {
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

#[derive(Clone)]
pub struct MockWALoggerConfig;

impl<M> PersistenceLogger for MockWALogger<M> {
    type Event = M;
    type Error = MockWALoggerError;
    type Config = MockWALoggerConfig;

    fn new(_config: Self::Config) -> Result<(Self, Vec<Self::Event>), Self::Error> {
        Ok((
            Self {
                _marker: PhantomData,
            },
            Vec::new(),
        ))
    }

    fn push(&mut self, _message: &Self::Event) -> Result<(), Self::Error> {
        Ok(())
    }
}
