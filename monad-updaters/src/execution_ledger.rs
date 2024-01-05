use std::{fs::File, io::Write, marker::PhantomData, path::PathBuf};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_executor::Executor;
use monad_executor_glue::ExecutionLedgerCommand;

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to a file which is read by Execution client
pub struct MonadFileLedger<SCT> {
    file: File,

    phantom: PhantomData<SCT>,
}

impl<SCT> Default for MonadFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    fn default() -> Self {
        Self::new(
            tempfile::tempdir()
                .unwrap()
                .into_path()
                .join("monad_file_ledger"),
        )
    }
}

impl<SCT> MonadFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file: File::create(file_path).unwrap(),

            phantom: PhantomData,
        }
    }
}

impl<SCT> Executor for MonadFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    type Command = ExecutionLedgerCommand<SCT>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            ExecutionLedgerCommand::LedgerCommit(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ExecutionLedgerCommand::LedgerCommit(full_blocks) => {
                    for full_block in full_blocks {
                        self.file
                            .write_all(&monad_ledger::encode_full_block(full_block))
                            .unwrap();
                    }
                }
            }
        }
    }
}
