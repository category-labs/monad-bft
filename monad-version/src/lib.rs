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

use std::{
    ffi::OsStr,
    io,
    path::Path,
    process::{Command, Output},
};

fn git_command<D, I, S>(work_tree: D, args: I) -> io::Result<Output>
where
    D: AsRef<Path>,
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let work_tree = work_tree.as_ref();
    Command::new("git")
        .arg("-C")
        .arg(work_tree)
        .args(args)
        .output()
}

fn git_output<D, I, S>(work_tree: D, args: I) -> Option<String>
where
    D: AsRef<Path>,
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = git_command(work_tree, args).ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn print_rerun_if_changed<D>(work_tree: D)
where
    D: AsRef<Path>,
{
    let work_tree = work_tree.as_ref();

    [".git/HEAD", ".git/index", ".git/refs"]
        .iter()
        .for_each(|path| {
            println!("cargo:rerun-if-changed={}", work_tree.join(path).display());
        });
}

pub fn commit<D>(work_tree: D) -> String
where
    D: AsRef<Path>,
{
    git_output(work_tree, ["rev-parse", "HEAD"])
        .or_else(|| std::env::var("GIT_COMMIT").ok())
        .unwrap_or_default()
}

pub fn branch<D>(work_tree: D) -> String
where
    D: AsRef<Path>,
{
    git_output(work_tree, ["branch", "--show-current"])
        .or_else(|| std::env::var("GIT_BRANCH").ok())
        .unwrap_or_default()
}

pub fn tag<D>(work_tree: D) -> String
where
    D: AsRef<Path>,
{
    git_output(work_tree, ["describe", "--tags", "--exact-match", "HEAD"])
        .or_else(|| std::env::var("GIT_TAG").ok())
        .unwrap_or_default()
}

pub fn modified<D>(work_tree: D) -> bool
where
    D: AsRef<Path>,
{
    git_output(work_tree, ["status", "--porcelain"])
        .map(|output| !output.is_empty())
        .or_else(|| {
            std::env::var("GIT_MODIFIED")
                .ok()
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(false)
}

pub fn auto<D>(work_tree: D)
where
    D: AsRef<Path>,
{
    let work_tree = work_tree.as_ref();

    print_rerun_if_changed(work_tree);

    let commit_hash = commit(work_tree);
    let branch_name = branch(work_tree);
    let tag_name = tag(work_tree);
    let is_modified = modified(work_tree);

    let version_json = format!(
        r#"{{"commit":"{}","tag":"{}","branch":"{}","modified":{}}}"#,
        commit_hash, tag_name, branch_name, is_modified
    );

    println!("cargo:rustc-env=MONAD_CLI_VERSION={}", version_json);
}

#[macro_export]
macro_rules! version {
    () => {
        env!("MONAD_CLI_VERSION")
    };
}
