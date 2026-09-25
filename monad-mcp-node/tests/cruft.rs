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

//! Runs the shell test of the ledger pruner, deploy/cruft.sh.

use std::process::Command;

#[test]
fn the_pruner_removes_only_old_block_directories() {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/cruft_test.sh");
    let output = Command::new("bash").arg(script).output().unwrap();
    assert!(
        output.status.success(),
        "{script} failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
