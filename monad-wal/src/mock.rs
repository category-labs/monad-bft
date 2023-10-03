use std::{fmt::Debug, marker::PhantomData};

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

#[derive(Clone)]
pub struct MockMemLoggerConfig<M: Clone> {
    log: Vec<M>,
}

impl<M: Clone> std::default::Default for MockMemLoggerConfig<M> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl<M: Clone> MockMemLoggerConfig<M> {
    pub fn new(log: Vec<M>) -> Self {
        Self { log }
    }
}

pub struct MockMemLogger<M> {
    pub log: Vec<M>,
}

impl<M: Debug + Clone> PersistenceLogger for MockMemLogger<M> {
    type Event = M;
    type Error = MockWALoggerError;
    type Config = MockMemLoggerConfig<M>;

    fn new(config: Self::Config) -> Result<(Self, Vec<Self::Event>), Self::Error> {
        let log = config.log;
        Ok((Self { log: log.clone() }, log))
    }

    fn push(&mut self, message: &Self::Event) -> Result<(), Self::Error> {
        self.log.push(message.clone());
        Ok(())
    }
}
