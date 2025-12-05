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
    marker::PhantomData,
    path::{Path, PathBuf},
};

use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{
    checkpoint::Checkpoint,
    validator_data::{ValidatorSetDataWithEpoch, ValidatorsConfigFile},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::ConfigFileCommand;
use monad_types::{Epoch, ExecutionProtocol, Round, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::warn;

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

pub struct ConfigFile<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    forkpoint_path: PathBuf,

    validators_path: PathBuf,
    last_validator_set: Option<ValidatorSetDataWithEpoch<SCT>>,
    staking_activation: Epoch,

    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, EPT)>,
}

impl<ST, SCT, EPT> ConfigFile<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new<CCT, CRT>(
        forkpoint_path: PathBuf,
        validators_path: PathBuf,
        chain_config: CCT,
    ) -> Self
    where
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        Self {
            forkpoint_path,
            validators_path,
            last_validator_set: None,
            staking_activation: chain_config.get_staking_activation(),
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    fn write_checkpoint(&self, root_seq_num: SeqNum, checkpoint: Checkpoint<ST, SCT, EPT>) {
        // write forkpoint.rlp or panic
        Self::write_checkpoint_bytes_to_path(
            &self.forkpoint_path.with_extension("rlp"),
            root_seq_num,
            checkpoint.high_certificate.round(),
            &checkpoint.to_rlp_bytes(),
        );
        // try to serialize and write forkpoint.toml
        let forkpoint_toml_path = self.forkpoint_path.with_extension("toml");
        match checkpoint.try_to_toml_string() {
            Ok(checkpoint_toml) => Self::write_checkpoint_bytes_to_path(
                &forkpoint_toml_path,
                root_seq_num,
                checkpoint.high_certificate.round(),
                &checkpoint_toml,
            ),
            Err(err) => {
                warn!(?err, "failed to write forkpoint to toml string");
                let _ = std::fs::remove_file(&forkpoint_toml_path);
            }
        }
    }

    fn write_checkpoint_bytes_to_path(
        path: &Path,
        root_seq_num: SeqNum,
        high_certificate_round: Round,
        checkpoint_bytes: &impl AsRef<[u8]>,
    ) {
        let temp_path = {
            let mut file_name = path
                .file_name()
                .expect("invalid checkpoint file name")
                .to_owned();
            file_name.push(".wip");

            let mut temp_path = path.to_owned();
            temp_path.set_file_name(file_name);
            temp_path
        };
        std::fs::write(
            format!(
                "{}.{}.{}",
                path.to_string_lossy(),
                root_seq_num.0,
                high_certificate_round.0,
            ),
            checkpoint_bytes,
        )
        .expect("failed to write checkpoint backup");
        std::fs::write(&temp_path, checkpoint_bytes).expect("failed to write checkpoint");
        std::fs::rename(&temp_path, path).expect("failed to rename checkpoint");
    }

    fn write_validator_set(&mut self, new_validator_set: ValidatorSetDataWithEpoch<SCT>) {
        let epoch = new_validator_set.epoch;
        let validator_sets = if let Some(last_validator_set) =
            self.last_validator_set.replace(new_validator_set.clone())
        {
            assert_eq!(epoch, last_validator_set.epoch + Epoch(1));
            vec![last_validator_set, new_validator_set]
        } else {
            vec![new_validator_set]
        };

        let validators_config_file = ValidatorsConfigFile { validator_sets };
        let validators_str = toml::to_string_pretty(&validators_config_file)
            .expect("failed to serialize validators");
        std::fs::write(
            format!("{}.{}", self.validators_path.to_string_lossy(), epoch.0),
            &validators_str,
        )
        .expect("failed to write validators");

        // if staking has been activated, overwrite validators.toml
        if epoch >= self.staking_activation {
            let temp_path = {
                let mut file_name = self
                    .validators_path
                    .file_name()
                    .expect("invalid validators config file name")
                    .to_owned();
                file_name.push(".wip");

                let mut temp_path = self.validators_path.clone();
                temp_path.set_file_name(file_name);
                temp_path
            };

            std::fs::write(&temp_path, &validators_str).expect("failed to write validators config");
            std::fs::rename(&temp_path, &self.validators_path)
                .expect("failed to rename validators config");
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
                    self.write_checkpoint(root_seq_num, checkpoint);
                }
                ConfigFileCommand::ValidatorSetData { validator_set_data } => {
                    self.write_validator_set(validator_set_data);
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
