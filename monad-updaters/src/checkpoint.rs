use std::{marker::PhantomData, path::PathBuf};

use monad_consensus_types::{checkpoint::Checkpoint, signature_collection::SignatureCollection};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::CheckpointCommand;

pub struct MockCheckpoint<SCT: SignatureCollection> {
    pub checkpoint: Option<Checkpoint<SCT>>,
    metrics: ExecutorMetrics,
}

impl<SCT: SignatureCollection> Default for MockCheckpoint<SCT> {
    fn default() -> Self {
        Self {
            checkpoint: None,
            metrics: Default::default(),
        }
    }
}

impl<SCT: SignatureCollection> Executor for MockCheckpoint<SCT> {
    type Command = CheckpointCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                CheckpointCommand::Save(c) => {
                    self.checkpoint = Some(c);
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

pub struct FileCheckpoint<SCT> {
    out_path: PathBuf,
    metrics: ExecutorMetrics,
    phantom: PhantomData<SCT>,
}

impl<SCT> FileCheckpoint<SCT>
where
    SCT: SignatureCollection + Clone,
{
    pub fn new(out_path: PathBuf) -> Self {
        Self {
            out_path,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<SCT> Executor for FileCheckpoint<SCT>
where
    SCT: SignatureCollection + Clone,
{
    type Command = CheckpointCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                CheckpointCommand::Save(checkpoint) => {
                    let checkpoint_str = toml::to_string_pretty(&checkpoint)
                        .expect("failed to serialize checkpoint");
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
                            checkpoint.root.seq_num.0,
                            checkpoint.high_qc.get_round().0
                        ),
                        &checkpoint_str,
                    )
                    .expect("failed to write checkpoint backup");
                    std::fs::write(&temp_path, &checkpoint_str)
                        .expect("failed to write checkpoint");
                    std::fs::rename(&temp_path, &self.out_path)
                        .expect("failed to rename checkpoint");
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
