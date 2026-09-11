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

#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    env,
    ffi::{OsStr, OsString},
    path::Path,
    process::{Command, ExitCode, ExitStatus},
};

use monad_ctl::{
    args_after_subcommand, cli_command, find_tool, runtime_binary_path, stack_action_command,
    stack_command, stack_systemctl_invocations, StackAction, StackTarget, SystemctlInvocation,
    Tool, STACK_COMMAND,
};

fn main() -> ExitCode {
    let args = env::args_os().skip(1).collect::<Vec<_>>();

    if args.is_empty() || is_help(&args[0]) {
        return print_help();
    }

    if is_version(&args[0]) {
        println!("monad-ctl {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }

    let Some(subcommand) = args[0].to_str() else {
        print_unknown_subcommand(&args[0]);
        return ExitCode::FAILURE;
    };

    if subcommand == STACK_COMMAND {
        return exec_stack(args_after_subcommand(&args));
    }

    let Some(tool) = find_tool(subcommand) else {
        print_unknown_subcommand(&args[0]);
        return ExitCode::FAILURE;
    };

    exec_tool(tool, args_after_subcommand(&args))
}

fn is_help(arg: &OsStr) -> bool {
    arg == "-h" || arg == "--help" || arg == "help"
}

fn is_version(arg: &OsStr) -> bool {
    arg == "-V" || arg == "--version"
}

fn print_help() -> ExitCode {
    let mut command = cli_command();
    match command.print_help() {
        Ok(()) => {
            println!();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("failed to print help: {error}");
            ExitCode::FAILURE
        }
    }
}

fn print_stack_help() -> ExitCode {
    let mut command = stack_command();
    command = command.bin_name("monad-ctl stack");
    print_command_help(&mut command)
}

fn print_stack_action_help(action: StackAction) -> ExitCode {
    let mut command = stack_action_command(action);
    command = command.bin_name(format!("monad-ctl stack {}", action.name()));
    print_command_help(&mut command)
}

fn print_command_help(command: &mut clap::Command) -> ExitCode {
    match command.print_help() {
        Ok(()) => {
            println!();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("failed to print help: {error}");
            ExitCode::FAILURE
        }
    }
}

fn print_unknown_subcommand(subcommand: &OsStr) {
    eprintln!(
        "error: unrecognized subcommand '{}'",
        subcommand.to_string_lossy()
    );
    eprintln!();
    eprintln!("Usage: monad-ctl <COMMAND> [ARGS]...");
    eprintln!();
    eprintln!("For more information, try '--help'.");
}

fn print_unknown_stack_action(action: &OsStr) {
    eprintln!(
        "error: unrecognized stack command '{}'",
        action.to_string_lossy()
    );
    eprintln!();
    eprintln!("Usage: monad-ctl stack <start|stop|restart> <bft|execution|rpc|mpt|all>");
    eprintln!();
    eprintln!("For more information, try 'monad-ctl stack --help'.");
}

fn print_missing_stack_target(action: StackAction) {
    eprintln!("error: missing stack target for '{}'", action.name());
    eprintln!();
    eprintln!(
        "Usage: monad-ctl stack {} <bft|execution|rpc|mpt|all>",
        action.name()
    );
}

fn print_unknown_stack_target(target: &OsStr) {
    eprintln!(
        "error: unrecognized stack target '{}'",
        target.to_string_lossy()
    );
    eprintln!();
    eprintln!("Usage: monad-ctl stack <start|stop|restart> <bft|execution|rpc|mpt|all>");
}

fn print_unexpected_stack_argument(arg: &OsStr) {
    eprintln!("error: unexpected argument '{}'", arg.to_string_lossy());
    eprintln!();
    eprintln!("Usage: monad-ctl stack <start|stop|restart> <bft|execution|rpc|mpt|all>");
}

fn exec_tool(tool: &Tool, args: &[OsString]) -> ExitCode {
    let binary_path = runtime_binary_path(tool.binary);
    let mut command = Command::new(&binary_path);
    command.args(args);

    run_child(command, tool.binary, &binary_path)
}

fn exec_stack(args: &[OsString]) -> ExitCode {
    if args.is_empty() || is_help(&args[0]) {
        return print_stack_help();
    }

    let Some(action_name) = args[0].to_str() else {
        print_unknown_stack_action(&args[0]);
        return ExitCode::FAILURE;
    };

    let Some(action) = StackAction::parse(action_name) else {
        print_unknown_stack_action(&args[0]);
        return ExitCode::FAILURE;
    };

    if let Some(arg) = args.get(1) {
        if is_help(arg) {
            return print_stack_action_help(action);
        }
    }

    if args.len() < 2 {
        print_missing_stack_target(action);
        return ExitCode::FAILURE;
    }

    if args.len() > 2 {
        print_unexpected_stack_argument(&args[2]);
        return ExitCode::FAILURE;
    }

    let Some(target_name) = args[1].to_str() else {
        print_unknown_stack_target(&args[1]);
        return ExitCode::FAILURE;
    };

    let Some(target) = StackTarget::parse(target_name) else {
        print_unknown_stack_target(&args[1]);
        return ExitCode::FAILURE;
    };

    exec_stack_invocations(stack_systemctl_invocations(action, target))
}

fn exec_stack_invocations(invocations: Vec<SystemctlInvocation>) -> ExitCode {
    for invocation in invocations {
        match run_systemctl(invocation) {
            Ok(()) => {}
            Err(exit_code) => return exit_code,
        }
    }

    ExitCode::SUCCESS
}

fn run_systemctl(invocation: SystemctlInvocation) -> Result<(), ExitCode> {
    let status = Command::new("systemctl")
        .arg(invocation.action)
        .arg(invocation.unit)
        .status();

    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(exit_code_from_status(status)),
        Err(error) => {
            eprintln!(
                "failed to execute systemctl {} {}: {error}",
                invocation.action, invocation.unit
            );
            Err(ExitCode::FAILURE)
        }
    }
}

fn exit_code_from_status(status: ExitStatus) -> ExitCode {
    ExitCode::from(status.code().unwrap_or(1) as u8)
}

#[cfg(unix)]
fn run_child(mut command: Command, binary: &str, binary_path: &Path) -> ExitCode {
    let error = command.exec();
    eprintln!(
        "failed to execute {binary} at '{}': {error}",
        binary_path.display()
    );
    ExitCode::FAILURE
}

#[cfg(not(unix))]
fn run_child(mut command: Command, binary: &str, binary_path: &Path) -> ExitCode {
    match command.status() {
        Ok(status) => {
            if status.success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(status.code().unwrap_or(1) as u8)
            }
        }
        Err(error) => {
            eprintln!(
                "failed to execute {binary} at '{}': {error}",
                binary_path.display()
            );
            ExitCode::FAILURE
        }
    }
}
