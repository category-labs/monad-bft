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
    env,
    ffi::OsString,
    path::{Path, PathBuf},
};

use clap::Command;

pub const BIN_DIR_ENV: &str = "MONAD_CTL_BIN_DIR";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Tool {
    pub subcommand: &'static str,
    pub binary: &'static str,
    pub about: &'static str,
}

pub const TOOLS: &[Tool] = &[
    Tool {
        subcommand: "cli",
        binary: "monad-cli",
        about: "Run the Monad execution CLI",
    },
    Tool {
        subcommand: "keystore",
        binary: "monad-keystore",
        about: "Manage Monad keystores",
    },
    Tool {
        subcommand: "ledger-tail",
        binary: "monad-ledger-tail",
        about: "Tail a Monad ledger",
    },
    Tool {
        subcommand: "mpt",
        binary: "monad-mpt",
        about: "Manage MPT databases",
    },
    Tool {
        subcommand: "sign-name-record",
        binary: "monad-sign-name-record",
        about: "Sign a peer-discovery name record",
    },
];

pub fn find_tool(subcommand: &str) -> Option<&'static Tool> {
    TOOLS.iter().find(|tool| tool.subcommand == subcommand)
}

pub fn cli_command() -> Command {
    let mut command = Command::new("monad-ctl")
        .about("Wrapper for Monad command-line tools")
        .version(env!("CARGO_PKG_VERSION"))
        .disable_help_subcommand(true)
        .after_help("Run `monad-ctl <command> --help` to view help for an underlying tool.");

    for tool in TOOLS {
        command = command.subcommand(
            Command::new(tool.subcommand)
                .about(tool.about)
                .disable_help_flag(true)
                .disable_version_flag(true),
        );
    }

    command
}

pub fn runtime_binary_path(binary: &str) -> PathBuf {
    let env_bin_dir = env::var_os(BIN_DIR_ENV).filter(|value| !value.is_empty());
    let current_exe = env::current_exe().ok();

    resolve_binary_path(binary, env_bin_dir.as_deref(), current_exe.as_deref())
}

pub fn resolve_binary_path(
    binary: &str,
    env_bin_dir: Option<&std::ffi::OsStr>,
    current_exe: Option<&Path>,
) -> PathBuf {
    if let Some(bin_dir) = env_bin_dir {
        return PathBuf::from(bin_dir).join(binary);
    }

    if let Some(current_exe) = current_exe {
        if let Some(exe_dir) = current_exe.parent() {
            let sibling = exe_dir.join(binary);
            if sibling.exists() {
                return sibling;
            }
        }
    }

    PathBuf::from(binary)
}

pub fn known_subcommands() -> impl Iterator<Item = &'static str> {
    TOOLS.iter().map(|tool| tool.subcommand)
}

pub fn args_after_subcommand(args: &[OsString]) -> &[OsString] {
    args.get(1..).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, fs::File};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn finds_registered_tool() {
        assert_eq!(
            find_tool("ledger-tail"),
            Some(&Tool {
                subcommand: "ledger-tail",
                binary: "monad-ledger-tail",
                about: "Tail a Monad ledger",
            })
        );
        assert_eq!(find_tool("unknown"), None);
    }

    #[test]
    fn env_bin_dir_takes_precedence() {
        let temp = tempdir().unwrap();
        let env_dir = temp.path().join("env-bin");
        let exe_dir = temp.path().join("exe-bin");
        std::fs::create_dir_all(&env_dir).unwrap();
        std::fs::create_dir_all(&exe_dir).unwrap();
        File::create(exe_dir.join("monad-cli")).unwrap();

        let resolved = resolve_binary_path(
            "monad-cli",
            Some(env_dir.as_os_str()),
            Some(&exe_dir.join("monad-ctl")),
        );

        assert_eq!(resolved, env_dir.join("monad-cli"));
    }

    #[test]
    fn colocated_binary_precedes_path_lookup() {
        let temp = tempdir().unwrap();
        let exe_dir = temp.path().join("bin");
        std::fs::create_dir_all(&exe_dir).unwrap();
        File::create(exe_dir.join("monad-keystore")).unwrap();

        let resolved =
            resolve_binary_path("monad-keystore", None, Some(&exe_dir.join("monad-ctl")));

        assert_eq!(resolved, exe_dir.join("monad-keystore"));
    }

    #[test]
    fn falls_back_to_path_lookup_name() {
        let temp = tempdir().unwrap();
        let exe_dir = temp.path().join("bin");
        std::fs::create_dir_all(&exe_dir).unwrap();

        let resolved = resolve_binary_path("monad-mpt", None, Some(&exe_dir.join("monad-ctl")));

        assert_eq!(resolved, PathBuf::from("monad-mpt"));
    }

    #[test]
    fn empty_env_bin_dir_is_ignored_by_runtime_filter() {
        let resolved = resolve_binary_path("monad-cli", Some(OsStr::new("")), None);

        assert_eq!(resolved, PathBuf::from("monad-cli"));
    }

    #[test]
    fn subcommand_args_skip_only_the_subcommand() {
        let args = vec![
            OsString::from("cli"),
            OsString::from("--help"),
            OsString::from("--literal"),
        ];

        assert_eq!(
            args_after_subcommand(&args),
            &[OsString::from("--help"), OsString::from("--literal")]
        );
    }
}
