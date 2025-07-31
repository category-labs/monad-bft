// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

    fn build(self) -> Result<Self::PersistenceLogger, WALError> {
        Ok(Self::PersistenceLogger {
            _marker: PhantomData,
        })
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

    fn build(self) -> Result<Self::PersistenceLogger, WALError> {
        Ok(Self::PersistenceLogger { log: self.log })
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
