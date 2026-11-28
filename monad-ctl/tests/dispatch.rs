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

fn write_fake_systemctl(bin_dir: &Path) {
    write_executable(
        &bin_dir.join("systemctl"),
        r#"#!/bin/sh
printf '%s %s\n' "$1" "$2" >> "$MONAD_SYSTEMCTL_LOG"
if [ "$MONAD_SYSTEMCTL_FAIL_UNIT" = "$2" ]; then
  exit 23
fi
exit 0
"#,
    );
}

fn read_log_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
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
    assert!(stdout.contains("stack"));
}

#[test]
fn stack_help_lists_supported_actions() {
    let output = Command::new(monad_ctl())
        .arg("stack")
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("start"));
    assert!(stdout.contains("stop"));
    assert!(stdout.contains("restart"));
}

#[test]
fn stack_action_help_lists_supported_targets() {
    let output = Command::new(monad_ctl())
        .arg("stack")
        .arg("start")
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("bft"));
    assert!(stdout.contains("execution"));
    assert!(stdout.contains("rpc"));
    assert!(stdout.contains("mpt"));
    assert!(stdout.contains("all"));
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

#[test]
fn stack_start_single_target_invokes_systemctl_from_path() {
    let temp = tempdir().unwrap();
    let bin_dir = temp.path().join("bin");
    let tool_bin_dir = temp.path().join("tool-bin");
    let log_path = temp.path().join("systemctl.log");
    fs::create_dir(&bin_dir).unwrap();
    fs::create_dir(&tool_bin_dir).unwrap();
    write_fake_systemctl(&bin_dir);

    let output = Command::new(monad_ctl())
        .arg("stack")
        .arg("start")
        .arg("bft")
        .env("PATH", &bin_dir)
        .env("MONAD_SYSTEMCTL_LOG", &log_path)
        .env(BIN_DIR_ENV, &tool_bin_dir)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(read_log_lines(&log_path), vec!["start monad-bft.service"]);
}

#[test]
fn stack_stop_all_invokes_systemctl_in_reverse_order() {
    let temp = tempdir().unwrap();
    let bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("systemctl.log");
    fs::create_dir(&bin_dir).unwrap();
    write_fake_systemctl(&bin_dir);

    let output = Command::new(monad_ctl())
        .arg("stack")
        .arg("stop")
        .arg("all")
        .env("PATH", &bin_dir)
        .env("MONAD_SYSTEMCTL_LOG", &log_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        read_log_lines(&log_path),
        vec![
            "stop monad-rpc.service",
            "stop monad-bft.service",
            "stop monad-execution.service",
            "stop monad-mpt.service",
        ]
    );
}

#[test]
fn stack_restart_all_stops_then_starts_in_stack_order() {
    let temp = tempdir().unwrap();
    let bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("systemctl.log");
    fs::create_dir(&bin_dir).unwrap();
    write_fake_systemctl(&bin_dir);

    let output = Command::new(monad_ctl())
        .arg("stack")
        .arg("restart")
        .arg("all")
        .env("PATH", &bin_dir)
        .env("MONAD_SYSTEMCTL_LOG", &log_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        read_log_lines(&log_path),
        vec![
            "stop monad-rpc.service",
            "stop monad-bft.service",
            "stop monad-execution.service",
            "stop monad-mpt.service",
            "start monad-mpt.service",
            "start monad-execution.service",
            "start monad-bft.service",
            "start monad-rpc.service",
        ]
    );
}

#[test]
fn stack_all_stops_after_first_systemctl_failure() {
    let temp = tempdir().unwrap();
    let bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("systemctl.log");
    fs::create_dir(&bin_dir).unwrap();
    write_fake_systemctl(&bin_dir);

    let status = Command::new(monad_ctl())
        .arg("stack")
        .arg("start")
        .arg("all")
        .env("PATH", &bin_dir)
        .env("MONAD_SYSTEMCTL_LOG", &log_path)
        .env("MONAD_SYSTEMCTL_FAIL_UNIT", "monad-execution.service")
        .status()
        .unwrap();

    assert_eq!(status.code(), Some(23));
    assert_eq!(
        read_log_lines(&log_path),
        vec!["start monad-mpt.service", "start monad-execution.service"]
    );
}
