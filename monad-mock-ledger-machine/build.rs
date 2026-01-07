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

use std::env;

fn main() {
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");

    let has_execution_lib = env::var("TRIEDB_TARGET").is_ok_and(|target| target == "triedb_driver");
    if has_execution_lib {
        println!("cargo:rustc-link-lib=dylib=monad_execution");
    }
}
