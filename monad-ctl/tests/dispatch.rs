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

#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use tempfile::tempdir;

const BIN_DIR_ENV: &str = "MONAD_CTL_BIN_DIR";

fn monad_ctl() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_monad-ctl"))
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn copy_wrapper(destination: &Path) -> PathBuf {
    let copied = destination.join("monad-ctl");
    fs::copy(monad_ctl(), &copied).unwrap();
    let mut permissions = fs::metadata(&copied).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&copied, permissions).unwrap();
    copied
}

#[test]
fn top_level_help_lists_supported_tools() {
    let output = Command::new(monad_ctl()).arg("--help").output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("cli"));
    assert!(stdout.contains("keystore"));
    assert!(stdout.contains("ledger-tail"));
    assert!(stdout.contains("mpt"));
    assert!(stdout.contains("sign-name-record"));
}

#[test]
fn forwards_arguments_using_env_bin_dir() {
    let temp = tempdir().unwrap();
    let bin_dir = temp.path().join("bin");
    fs::create_dir(&bin_dir).unwrap();
    write_executable(
        &bin_dir.join("monad-cli"),
        r#"#!/bin/sh
printf 'monad-cli'
for arg in "$@"; do
  printf ' [%s]' "$arg"
done
printf '\n'
"#,
    );

    let output = Command::new(monad_ctl())
        .arg("cli")
        .arg("--flag")
        .arg("value with spaces")
        .env(BIN_DIR_ENV, &bin_dir)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "monad-cli [--flag] [value with spaces]\n"
    );
}

#[test]
fn preserves_child_exit_code() {
    let temp = tempdir().unwrap();
    let bin_dir = temp.path().join("bin");
    fs::create_dir(&bin_dir).unwrap();
    write_executable(&bin_dir.join("monad-keystore"), "#!/bin/sh\nexit 37\n");

    let status = Command::new(monad_ctl())
        .arg("keystore")
        .env(BIN_DIR_ENV, &bin_dir)
        .status()
        .unwrap();

    assert_eq!(status.code(), Some(37));
}

#[test]
fn forwards_child_help_to_underlying_tool() {
    let temp = tempdir().unwrap();
    let bin_dir = temp.path().join("bin");
    fs::create_dir(&bin_dir).unwrap();
    write_executable(
        &bin_dir.join("monad-mpt"),
        r#"#!/bin/sh
if [ "$1" = "--help" ]; then
  printf 'child help for monad-mpt\n'
  exit 0
fi
printf 'unexpected args\n'
exit 1
"#,
    );

    let output = Command::new(monad_ctl())
        .arg("mpt")
        .arg("--help")
        .env(BIN_DIR_ENV, &bin_dir)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "child help for monad-mpt\n"
    );
}

#[test]
fn falls_back_to_path_when_no_env_or_colocated_binary_exists() {
    let temp = tempdir().unwrap();
    let wrapper_dir = temp.path().join("wrapper");
    let path_dir = temp.path().join("path");
    fs::create_dir(&wrapper_dir).unwrap();
    fs::create_dir(&path_dir).unwrap();
    let copied_wrapper = copy_wrapper(&wrapper_dir);
    write_executable(
        &path_dir.join("monad-sign-name-record"),
        "#!/bin/sh\nprintf 'from path [%s]\\n' \"$1\"\n",
    );

    let output = Command::new(copied_wrapper)
        .arg("sign-name-record")
        .arg("payload")
        .env_remove(BIN_DIR_ENV)
        .env("PATH", &path_dir)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "from path [payload]\n"
    );
}

#[test]
fn colocated_binary_precedes_path_lookup() {
    let temp = tempdir().unwrap();
    let wrapper_dir = temp.path().join("wrapper");
    let path_dir = temp.path().join("path");
    fs::create_dir(&wrapper_dir).unwrap();
    fs::create_dir(&path_dir).unwrap();
    let copied_wrapper = copy_wrapper(&wrapper_dir);
    write_executable(
        &wrapper_dir.join("monad-ledger-tail"),
        "#!/bin/sh\nprintf 'from colocated\\n'\n",
    );
    write_executable(
        &path_dir.join("monad-ledger-tail"),
        "#!/bin/sh\nprintf 'from path\\n'\n",
    );

    let output = Command::new(copied_wrapper)
        .arg("ledger-tail")
        .env_remove(BIN_DIR_ENV)
        .env("PATH", &path_dir)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "from colocated\n"
    );
}

#[test]
fn reports_missing_binary() {
    let temp = tempdir().unwrap();
    let missing_dir = temp.path().join("missing");

    let output = Command::new(monad_ctl())
        .arg("ledger-tail")
        .env(BIN_DIR_ENV, &missing_dir)
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("failed to execute monad-ledger-tail"));
    assert!(stderr.contains("monad-ledger-tail"));
}
