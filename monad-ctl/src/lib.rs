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

use clap::{Arg, Command};

pub const BIN_DIR_ENV: &str = "MONAD_CTL_BIN_DIR";
pub const STACK_COMMAND: &str = "stack";

const STACK_TARGET_NAMES: [&str; 5] = ["bft", "execution", "rpc", "mpt", "all"];
const STACK_START_UNITS: [&str; 4] = [
    "monad-mpt.service",
    "monad-execution.service",
    "monad-bft.service",
    "monad-rpc.service",
];
const STACK_STOP_UNITS: [&str; 4] = [
    "monad-rpc.service",
    "monad-bft.service",
    "monad-execution.service",
    "monad-mpt.service",
];

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StackAction {
    Start,
    Stop,
    Restart,
}

impl StackAction {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "start" => Some(Self::Start),
            "stop" => Some(Self::Stop),
            "restart" => Some(Self::Restart),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }

    pub fn about(self) -> &'static str {
        match self {
            Self::Start => "Start unit(s)",
            Self::Stop => "Stop unit(s)",
            Self::Restart => "Restart unit(s)",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StackTarget {
    Bft,
    Execution,
    Rpc,
    Mpt,
    All,
}

impl StackTarget {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bft" => Some(Self::Bft),
            "execution" => Some(Self::Execution),
            "rpc" => Some(Self::Rpc),
            "mpt" => Some(Self::Mpt),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    pub fn unit(self) -> Option<&'static str> {
        match self {
            Self::Bft => Some("monad-bft.service"),
            Self::Execution => Some("monad-execution.service"),
            Self::Rpc => Some("monad-rpc.service"),
            Self::Mpt => Some("monad-mpt.service"),
            Self::All => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SystemctlInvocation {
    pub action: &'static str,
    pub unit: &'static str,
}

pub fn find_tool(subcommand: &str) -> Option<&'static Tool> {
    TOOLS.iter().find(|tool| tool.subcommand == subcommand)
}

pub fn cli_command() -> Command {
    let mut command = Command::new("monad-ctl")
        .about("Wrapper for Monad command-line tools")
        .version(env!("CARGO_PKG_VERSION"))
        .disable_help_subcommand(true)
        .after_help("Run `monad-ctl <command> --help` to view help for an underlying tool.");

    command = command.subcommand(stack_command());

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

pub fn stack_command() -> Command {
    Command::new(STACK_COMMAND)
        .about("Control Monad systemd service units")
        .subcommand_required(true)
        .subcommand(stack_action_command(StackAction::Start))
        .subcommand(stack_action_command(StackAction::Stop))
        .subcommand(stack_action_command(StackAction::Restart))
}

pub fn stack_action_command(action: StackAction) -> Command {
    Command::new(action.name()).about(action.about()).arg(
        Arg::new("target")
            .required(true)
            .value_parser(STACK_TARGET_NAMES)
            .help("Service target"),
    )
}

pub fn stack_systemctl_invocations(
    action: StackAction,
    target: StackTarget,
) -> Vec<SystemctlInvocation> {
    match (action, target) {
        (StackAction::Start, StackTarget::All) => systemctl_invocations("start", STACK_START_UNITS),
        (StackAction::Stop, StackTarget::All) => systemctl_invocations("stop", STACK_STOP_UNITS),
        (StackAction::Restart, StackTarget::All) => STACK_STOP_UNITS
            .into_iter()
            .map(|unit| SystemctlInvocation {
                action: "stop",
                unit,
            })
            .chain(
                STACK_START_UNITS
                    .into_iter()
                    .map(|unit| SystemctlInvocation {
                        action: "start",
                        unit,
                    }),
            )
            .collect(),
        (action, target) => vec![SystemctlInvocation {
            action: action.name(),
            unit: target
                .unit()
                .expect("non-all stack targets must map to a systemd unit"),
        }],
    }
}

fn systemctl_invocations(
    action: &'static str,
    units: [&'static str; 4],
) -> Vec<SystemctlInvocation> {
    units
        .into_iter()
        .map(|unit| SystemctlInvocation { action, unit })
        .collect()
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
    std::iter::once(STACK_COMMAND).chain(TOOLS.iter().map(|tool| tool.subcommand))
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
    fn parses_stack_actions_and_targets() {
        assert_eq!(StackAction::parse("start"), Some(StackAction::Start));
        assert_eq!(StackAction::parse("stop"), Some(StackAction::Stop));
        assert_eq!(StackAction::parse("restart"), Some(StackAction::Restart));
        assert_eq!(StackAction::parse("reload"), None);

        assert_eq!(StackTarget::parse("bft"), Some(StackTarget::Bft));
        assert_eq!(
            StackTarget::parse("execution"),
            Some(StackTarget::Execution)
        );
        assert_eq!(StackTarget::parse("rpc"), Some(StackTarget::Rpc));
        assert_eq!(StackTarget::parse("mpt"), Some(StackTarget::Mpt));
        assert_eq!(StackTarget::parse("all"), Some(StackTarget::All));
        assert_eq!(StackTarget::parse("validator"), None);
    }

    #[test]
    fn maps_stack_targets_to_systemd_units() {
        assert_eq!(StackTarget::Bft.unit(), Some("monad-bft.service"));
        assert_eq!(
            StackTarget::Execution.unit(),
            Some("monad-execution.service")
        );
        assert_eq!(StackTarget::Rpc.unit(), Some("monad-rpc.service"));
        assert_eq!(StackTarget::Mpt.unit(), Some("monad-mpt.service"));
        assert_eq!(StackTarget::All.unit(), None);
    }

    #[test]
    fn expands_all_stack_target_in_deterministic_order() {
        assert_eq!(
            stack_systemctl_invocations(StackAction::Start, StackTarget::All),
            vec![
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-mpt.service",
                },
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-execution.service",
                },
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-bft.service",
                },
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-rpc.service",
                },
            ]
        );

        assert_eq!(
            stack_systemctl_invocations(StackAction::Stop, StackTarget::All),
            vec![
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-rpc.service",
                },
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-bft.service",
                },
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-execution.service",
                },
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-mpt.service",
                },
            ]
        );

        assert_eq!(
            stack_systemctl_invocations(StackAction::Restart, StackTarget::All),
            vec![
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-rpc.service",
                },
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-bft.service",
                },
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-execution.service",
                },
                SystemctlInvocation {
                    action: "stop",
                    unit: "monad-mpt.service",
                },
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-mpt.service",
                },
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-execution.service",
                },
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-bft.service",
                },
                SystemctlInvocation {
                    action: "start",
                    unit: "monad-rpc.service",
                },
            ]
        );
    }

    #[test]
    fn expands_single_stack_target_to_one_systemctl_invocation() {
        assert_eq!(
            stack_systemctl_invocations(StackAction::Restart, StackTarget::Bft),
            vec![SystemctlInvocation {
                action: "restart",
                unit: "monad-bft.service",
            }]
        );
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
