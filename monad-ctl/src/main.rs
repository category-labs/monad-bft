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
    process::{Command, ExitCode},
};

use monad_ctl::{args_after_subcommand, cli_command, find_tool, runtime_binary_path, Tool};

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

fn exec_tool(tool: &Tool, args: &[OsString]) -> ExitCode {
    let binary_path = runtime_binary_path(tool.binary);
    let mut command = Command::new(&binary_path);
    command.args(args);

    run_child(command, tool.binary, &binary_path)
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
