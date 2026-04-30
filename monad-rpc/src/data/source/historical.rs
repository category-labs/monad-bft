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

use async_trait::async_trait;
use auto_impl::auto_impl;
use dyn_clone::{clone_trait_object, DynClone};

use super::DataSourceStack;

/// Trait for read-only queries against historical chain data.
#[async_trait]
#[auto_impl(Box)]
pub trait HistoricalDataSource: DynClone + Send + Sync {}

clone_trait_object!(HistoricalDataSource);

pub type HistoricalDataSourceStack = DataSourceStack<Box<dyn HistoricalDataSource>>;
