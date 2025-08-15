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

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::CheckpointCommand;
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

pub struct MockCheckpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub checkpoint: Option<CheckpointCommand<ST, SCT, EPT>>,
    metrics: ExecutorMetrics,
}

impl<ST, SCT, EPT> Default for MockCheckpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self {
            checkpoint: None,
            metrics: Default::default(),
        }
    }
}

impl<ST, SCT, EPT> Executor for MockCheckpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = CheckpointCommand<ST, SCT, EPT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            self.checkpoint = Some(command);
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

pub struct FileCheckpoint<ST, SCT, EPT> {
    out_path: PathBuf,
    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> FileCheckpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(out_path: PathBuf) -> Self {
        Self {
            out_path,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Executor for FileCheckpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = CheckpointCommand<ST, SCT, EPT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            let CheckpointCommand {
                root_seq_num,
                checkpoint,
            } = command;
            let checkpoint_str =
                toml::to_string_pretty(&checkpoint).expect("failed to serialize checkpoint");
            let temp_path = {
                let mut file_name = self
                    .out_path
                    .file_name()
                    .expect("invalid checkpoint file name")
                    .to_owned();
                file_name.push(".wip");

                let mut temp_path = self.out_path.clone();
                temp_path.set_file_name(file_name);
                temp_path
            };
            std::fs::write(
                format!(
                    "{}.{}.{}",
                    self.out_path.to_string_lossy(),
                    root_seq_num.0,
                    checkpoint.high_certificate.round().0,
                ),
                &checkpoint_str,
            )
            .expect("failed to write checkpoint backup");
            std::fs::write(&temp_path, &checkpoint_str).expect("failed to write checkpoint");
            std::fs::rename(&temp_path, &self.out_path).expect("failed to rename checkpoint");
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
