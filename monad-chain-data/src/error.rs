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

use thiserror::Error;

pub type Result<T> = std::result::Result<T, MonadChainDataError>;

#[derive(Debug, Error)]
pub enum MonadChainDataError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("decode error: {0}")]
    Decode(&'static str),
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("missing data: {0}")]
    MissingData(&'static str),
}
