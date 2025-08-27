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

use std::{marker::PhantomData, path::PathBuf};

use monad_consensus_types::{
    checkpoint::Checkpoint,
    validator_data::{ValidatorSetDataWithEpoch, ValidatorsConfigFile},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::ConfigFileCommand;
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

pub struct MockConfigFile<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub checkpoint: Option<Checkpoint<ST, SCT, EPT>>,
    pub val_set_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    metrics: ExecutorMetrics,
}

impl<ST, SCT, EPT> Default for MockConfigFile<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self {
            checkpoint: None,
            val_set_data: None,
            metrics: Default::default(),
        }
    }
}

impl<ST, SCT, EPT> Executor for MockConfigFile<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = ConfigFileCommand<ST, SCT, EPT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ConfigFileCommand::Checkpoint {
                    root_seq_num: _,
                    checkpoint,
                } => self.checkpoint = Some(checkpoint),
                ConfigFileCommand::ValidatorSetData { validator_set_data } => {
                    self.val_set_data = Some(validator_set_data)
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

pub struct ConfigFile<ST, SCT, EPT> {
    forkpoint_path: PathBuf,
    validators_path: PathBuf,
    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> ConfigFile<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(forkpoint_path: PathBuf, validators_path: PathBuf) -> Self {
        Self {
            forkpoint_path,
            validators_path,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Executor for ConfigFile<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = ConfigFileCommand<ST, SCT, EPT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ConfigFileCommand::Checkpoint {
                    root_seq_num,
                    checkpoint,
                } => {
                    let checkpoint_str = toml::to_string_pretty(&checkpoint)
                        .expect("failed to serialize checkpoint");
                    let temp_path = {
                        let mut file_name = self
                            .forkpoint_path
                            .file_name()
                            .expect("invalid checkpoint file name")
                            .to_owned();
                        file_name.push(".wip");

                        let mut temp_path = self.forkpoint_path.clone();
                        temp_path.set_file_name(file_name);
                        temp_path
                    };
                    std::fs::write(
                        format!(
                            "{}.{}.{}",
                            self.forkpoint_path.to_string_lossy(),
                            root_seq_num.0,
                            checkpoint.high_certificate.round().0,
                        ),
                        &checkpoint_str,
                    )
                    .expect("failed to write checkpoint backup");
                    std::fs::write(&temp_path, &checkpoint_str)
                        .expect("failed to write checkpoint");
                    std::fs::rename(&temp_path, &self.forkpoint_path)
                        .expect("failed to rename checkpoint");
                }
                ConfigFileCommand::ValidatorSetData { validator_set_data } => {
                    let epoch = validator_set_data.epoch;
                    let validators_config_file = ValidatorsConfigFile {
                        validator_sets: vec![validator_set_data],
                    };
                    let validators_str = toml::to_string_pretty(&validators_config_file)
                        .expect("failed to serialize validators");
                    std::fs::write(
                        format!("{}.{}", self.validators_path.to_string_lossy(), epoch.0),
                        &validators_str,
                    )
                    .expect("failed to write validators");
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
