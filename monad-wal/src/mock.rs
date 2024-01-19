use std::marker::PhantomData;

use crate::{PersistenceLogger, PersistenceLoggerBuilder, WALError};

pub struct MockWALogger<M> {
    _marker: PhantomData<M>,
}

#[derive(Clone)]
pub struct MockWALoggerConfig<M>(PhantomData<M>);

impl<M> Default for MockWALoggerConfig<M> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<M> PersistenceLoggerBuilder for MockWALoggerConfig<M> {
    type PersistenceLogger = MockWALogger<M>;

    fn build(
        self,
    ) -> Result<
        (
            Self::PersistenceLogger,
            Vec<<Self::PersistenceLogger as PersistenceLogger>::Event>,
        ),
        WALError,
    > {
        Ok((
            Self::PersistenceLogger {
                _marker: PhantomData,
            },
            Vec::new(),
        ))
    }
}

impl<M> PersistenceLogger for MockWALogger<M> {
    type Event = M;

    fn push(&mut self, _message: &Self::Event) -> Result<(), WALError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct MockMemLoggerConfig<M> {
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

impl<M: Clone> PersistenceLoggerBuilder for MockMemLoggerConfig<M> {
    type PersistenceLogger = MockMemLogger<M>;

    fn build(
        self,
    ) -> Result<
        (
            Self::PersistenceLogger,
            Vec<<Self::PersistenceLogger as PersistenceLogger>::Event>,
        ),
        WALError,
    > {
        let log = self.log;
        Ok((Self::PersistenceLogger { log: log.clone() }, log))
    }
}

pub struct MockMemLogger<M> {
    pub log: Vec<M>,
}

impl<M: Clone> PersistenceLogger for MockMemLogger<M> {
    type Event = M;

    fn push(&mut self, message: &Self::Event) -> Result<(), WALError> {
        self.log.push(message.clone());
        Ok(())
    }
}
