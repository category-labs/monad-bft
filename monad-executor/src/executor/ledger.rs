use crate::{Executor, LedgerCommand};

pub struct MockLedger<O> {
    blockchain: Vec<O>,
}

impl<O> Default for MockLedger<O> {
    fn default() -> Self {
        Self {
            blockchain: Vec::new(),
        }
    }
}

impl<O> MockLedger<O> {
    pub fn get_blocks(&self) -> &Vec<O> {
        &self.blockchain
    }
}

impl<O> Executor for MockLedger<O> {
    type Command = LedgerCommand<O>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(block) => {
                    self.blockchain.push(block);
                }
            }
        }
    }
}
