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

use std::path::PathBuf;

use clap::{builder::TypedValueParser, CommandFactory, FromArgMatches, Parser};
use monad_chain_config::MonadChainConfig;
use monad_consensus_types::validator_data::ValidatorsConfig;
use monad_node_config::{ForkpointConfig, NodeConfig, SignatureCollectionType, SignatureType};
use serde::de::DeserializeOwned;
use strum::{EnumString, VariantNames};

#[derive(Clone, Debug, EnumString, VariantNames)]
#[strum(serialize_all = "kebab-case")]
enum ConfigType {
    Node,
    Forkpoint,
    Validators,
    Chain,
}

#[derive(Debug, Parser)]
#[command(name = "monad-node-config-linter", about, long_about = None)]
struct Cli {
    #[arg(long, value_parser = clap::builder::PossibleValuesParser::new(ConfigType::VARIANTS).map(|s| s.parse::<ConfigType>().unwrap()))]
    config_type: ConfigType,

    path: PathBuf,
}

fn main() {
    let mut cmd = Cli::command();

    let Cli { config_type, path } = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())
        .unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    let contents = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        cmd.error(
            clap::error::ErrorKind::Io,
            format!(
                "Failed to read file {}, reason: {err}",
                path.to_str().unwrap()
            ),
        )
        .exit()
    });

    match config_type {
        ConfigType::Node => {
            let _: NodeConfig<SignatureType> = parse(path, contents);
        }
        ConfigType::Forkpoint => {
            let _: ForkpointConfig = parse(path, contents);
        }
        ConfigType::Validators => {
            let _: ValidatorsConfig<SignatureCollectionType> =
                parse_with(path, contents, ValidatorsConfig::read_from_str);
        }
        ConfigType::Chain => {
            let _: MonadChainConfig = parse(path, contents);
        }
    }
}

fn parse<T>(path: PathBuf, contents: String) -> T
where
    T: DeserializeOwned,
{
    parse_with(path, contents, |s| toml::from_str(s))
}

fn parse_with<T>(
    path: PathBuf,
    contents: String,
    parser: for<'a> fn(&'a str) -> Result<T, toml::de::Error>,
) -> T {
    let err = match parser(&contents) {
        Ok(value) => return value,
        Err(err) => err,
    };

    use codespan_reporting::{
        diagnostic::{Diagnostic, Label},
        files::SimpleFiles,
        term::termcolor::{ColorChoice, StandardStream},
    };

    let mut files = SimpleFiles::new();

    let file_id = files.add(path.to_str().unwrap(), contents);

    let diagnostic = Diagnostic::error().with_message(err.message()).with_labels(
        err.span()
            .map(|span| vec![Label::primary(file_id, span)])
            .unwrap_or_default(),
    );

    let writer = StandardStream::stderr(ColorChoice::Always);
    let config = codespan_reporting::term::Config::default();

    codespan_reporting::term::emit(&mut writer.lock(), &config, &files, &diagnostic).unwrap();
    std::process::exit(1);
}
