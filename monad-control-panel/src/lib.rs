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

pub mod ipc;

use tracing::Subscriber;
use tracing_subscriber::{reload::Handle, EnvFilter};

pub trait TracingReload: Send {
    fn reload(&self, filter: EnvFilter) -> Result<(), Box<dyn std::error::Error>>;
}

impl<S: Subscriber> TracingReload for Handle<EnvFilter, S> {
    fn reload(&self, filter: EnvFilter) -> Result<(), Box<dyn std::error::Error>> {
        self.reload(filter)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
}

#[cfg(test)]
mod tests {}
