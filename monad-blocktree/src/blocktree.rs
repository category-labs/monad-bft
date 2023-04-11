use monad_consensus::types::block::Block;
use monad_consensus::types::signature::SignatureCollection;
use monad_types::BlockId;
use monad_types::Round;
use ptree::print_tree;
use std::collections::HashMap;
use std::fmt;
use std::result::Result as StdResult;

use ptree::builder::TreeBuilder;

type Result<T> = StdResult<T, BlockTreeError>;

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum BlockTreeError {
    ParentNotPresent(String),
    BlockNotExist(String),
}

impl fmt::Display for BlockTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParentNotPresent(s) => write!(f, "Parent not present: {}", s),
            Self::BlockNotExist(s) => write!(f, "Block not exist: {}", s),
        }
    }
}

impl std::error::Error for BlockTreeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub struct BlockTreeBlock<T> {
    block: Block<T>,
    children: Vec<BlockId>,
}

impl<T: SignatureCollection> BlockTreeBlock<T> {
    const CHILD_INDENT: &str = "    ";
    fn new(b: Block<T>) -> Self {
        BlockTreeBlock {
            block: b,
            children: Vec::new(),
        }
    }

    fn tree_fmt(
        &self,
        tree: &BlockTree<T>,
        f: &mut fmt::Formatter<'_>,
        indent: &String,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}({:?}){:?}\n",
            indent,
            self.block.round.0,
            self.block.get_id()
        )?;
        let mut child_indent: String = indent.clone();
        child_indent.push_str(Self::CHILD_INDENT);
        for bid in self.children.iter() {
            let block = tree.tree.get(bid).unwrap();
            block.tree_fmt(tree, f, &child_indent)?;
        }
        Ok(())
    }

    fn build_ptree<'a>(
        &self,
        tree: &BlockTree<T>,
        tree_print: &'a mut TreeBuilder,
    ) -> &'a mut TreeBuilder {
        let mut result =
            tree_print.begin_child(format!("({}){:?}", self.block.round.0, self.block.get_id()));
        for bid in self.children.iter() {
            let block = tree.tree.get(bid).unwrap();
            result = block.build_ptree(tree, result);
        }
        result.end_child()
    }
}

pub struct BlockTree<T> {
    root: BlockId,
    tree: HashMap<BlockId, BlockTreeBlock<T>>,
    high_round: Round,
}

impl<T: SignatureCollection> std::fmt::Debug for BlockTree<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let root = self.tree.get(&self.root).unwrap();
        root.tree_fmt(&self, f, &"".to_owned())?;
        Ok(())
    }
}

impl<T: SignatureCollection> BlockTree<T> {
    pub fn new(genesis_block: Block<T>) -> Self {
        let bid = genesis_block.get_id();
        let round = genesis_block.round;
        let mut tree = HashMap::new();
        tree.insert(bid, BlockTreeBlock::new(genesis_block));
        Self {
            root: bid,
            tree: tree,
            high_round: round,
        }
    }

    /// let n = new_root_block.round
    /// Pruning all blocks with lower round than `n` is accurate and complete
    /// 1)  Nodes that aren't descendents of `n` are added before `n` and have round number smaller than `n`
    ///     It only prunes blocks not part of the `new_root` subtree -> accurate
    /// 2)  When prune is called on round `n` block, only round `n+1` block is added (as `n`'s children)
    ///     All the blocks that remains shouldn't be pruned -> complete
    ///
    /// Returns the blocks in increasing rounds
    pub fn prune(&mut self, new_root: &BlockId) -> Result<Vec<Block<T>>> {
        // retain the new root block, remove all other committable block to avoid copying
        let mut commit: Vec<Block<T>> = Vec::new();
        if &self.root == new_root {
            return Ok(commit);
        }
        let new_root_block = &self
            .tree
            .get(new_root)
            .ok_or(BlockTreeError::BlockNotExist(format!("{:?}", new_root)))?
            .block;
        let new_root_round = new_root_block.round;
        commit.push(new_root_block.clone());

        let mut cur = new_root_block.qc.info.vote.id;
        while cur != self.root {
            let block = self.tree.remove(&cur).unwrap();
            cur = block.block.qc.info.vote.id;
            commit.push(block.block);
        }
        self.root = *new_root;

        // garbage collect old blocks
        // remove any blocks lower than round `n`
        // should only keep `n`, `n+1` blocks
        self.tree.retain(|_, b| b.block.round >= new_root_round);

        commit.reverse();
        Ok(commit)
    }

    pub fn add(&mut self, b: Block<T>) -> Result<()> {
        // blocks must be inserted in increasing rounds
        assert!(b.round > self.high_round);

        let new_bid = b.get_id();
        let new_round = b.round;
        let parent_bid = &b.qc.info.vote.id;
        self.tree
            .get_mut(parent_bid)
            .ok_or(BlockTreeError::ParentNotPresent(format!(
                "{:?}",
                parent_bid
            )))?
            .children
            .push(new_bid);
        self.tree.insert(new_bid, BlockTreeBlock::new(b));
        self.high_round = new_round;
        Ok(())
    }

    pub fn debug_print(&self) -> std::io::Result<()> {
        let mut tree = TreeBuilder::new("BlockTree".to_owned());
        let root = self.tree.get(&self.root).unwrap();
        root.build_ptree(&self, &mut tree);

        print_tree(&tree.build())
    }
}

#[cfg(test)]
mod test {
    use monad_consensus::types::block::{Block as ConsensusBlock, TransactionList};
    use monad_consensus::types::ledger::LedgerCommitInfo;
    use monad_consensus::types::quorum_certificate::{QcInfo, QuorumCertificate};
    use monad_consensus::types::voting::VoteInfo;
    use monad_consensus::validation::hashing::Sha256Hash;
    use monad_crypto::secp256k1::KeyPair;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{BlockId, NodeId, Round};

    use super::BlockTree;
    use super::BlockTreeError;

    type Block = ConsensusBlock<MockSignatures>;
    type QC = QuorumCertificate<MockSignatures>;

    fn node_id() -> NodeId {
        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();
        NodeId(keypair.pubkey())
    }

    #[test]
    fn test_prune() {
        let txlist = TransactionList(vec![]);
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: VoteInfo::default(),
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId::default(),
            parent_round: Round::default(),
        };

        let b1 = Block::new::<Sha256Hash>(
            node_id(),
            Round(1),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v1,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
        };

        let b2 = Block::new::<Sha256Hash>(
            node_id(),
            Round(2),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v2,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId::default(),
            parent_round: Round::default(),
        };

        let b3 = Block::new::<Sha256Hash>(
            node_id(),
            Round(3),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v3,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId::default(),
            parent_round: Round::default(),
        };

        let b4 = Block::new::<Sha256Hash>(
            node_id(),
            Round(4),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v4,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v5 = VoteInfo {
            id: b3.get_id(),
            round: Round(3),
            parent_id: g.get_id(),
            parent_round: Round(0),
        };

        let b5 = Block::new::<Sha256Hash>(
            node_id(),
            Round(5),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v5,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b3.get_id(),
            parent_round: Round(3),
        };

        let b6 = Block::new::<Sha256Hash>(
            node_id(),
            Round(6),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v6,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v7 = VoteInfo {
            id: b6.get_id(),
            round: Round(6),
            parent_id: b5.get_id(),
            parent_round: Round(5),
        };

        let b7 = Block::new::<Sha256Hash>(
            node_id(),
            Round(7),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v7,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        // Initial blocktree
        //        g
        //   /    |     \
        //  b1    b3    b4
        //  |     |
        //  b2    b5
        //        |
        //        b6
        let mut blocktree = BlockTree::<MockSignatures>::new(g.clone());

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(blocktree.add(b5.clone()).is_ok());
        assert!(blocktree.add(b6.clone()).is_ok());
        println!("{:?}", blocktree);

        // pruning on the old root should return no committable blocks
        let commit = blocktree.prune(&g.get_id()).unwrap();
        assert_eq!(commit.len(), 0);

        let commit = blocktree.prune(&b5.get_id()).unwrap();
        assert_eq!(
            Vec::from_iter(commit.iter().map(|b| b.get_id())),
            vec![b3.get_id(), b5.get_id()]
        );
        println!("{:?}", blocktree);

        // Pruned blocktree
        //     b5
        //     |
        //     b6

        // try pruning all other nodes should return err
        matches!(
            blocktree.prune(&g.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        );
        matches!(
            blocktree.prune(&b1.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        );
        matches!(
            blocktree.prune(&b2.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        );
        matches!(
            blocktree.prune(&b4.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        );

        // Pruned blocktree after insertion
        //     b5
        //   /    \
        //  b6    b8
        //  |
        //  b7

        assert!(blocktree.add(b7.clone()).is_ok());

        let v8 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b3.get_id(),
            parent_round: Round(3),
        };

        let b8 = Block::new::<Sha256Hash>(
            node_id(),
            Round(8),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v8,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        assert!(blocktree.add(b8.clone()).is_ok());
        println!("{:?}", blocktree);
    }

    #[test]
    fn test_add_parent_not_exist() {
        let txlist = TransactionList(vec![]);
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: VoteInfo::default(),
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId::default(),
            parent_round: Round::default(),
        };

        let b1 = Block::new::<Sha256Hash>(
            node_id(),
            Round(1),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v1,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
        };

        let b2 = Block::new::<Sha256Hash>(
            node_id(),
            Round(2),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v2,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures,
            ),
        );

        let mut blocktree = BlockTree::new(g);
        matches!(
            blocktree.add(b2.clone()).unwrap_err(),
            BlockTreeError::ParentNotPresent(_)
        );
    }
}
