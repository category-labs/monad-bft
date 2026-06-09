// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! Re-export of the crate's `testkit` population helpers so the integration
//! tests can keep calling `common::populate::*`. The canonical implementation
//! lives in `monad_chain_data::testkit` (shared with downstream crates).

#![allow(unused_imports)]

pub use monad_chain_data::testkit::*;
