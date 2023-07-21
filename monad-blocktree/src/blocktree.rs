use std::{collections::HashMap, fmt, result::Result as StdResult};

use monad_consensus_types::{block::Block, signature::SignatureCollection};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, Round};
use ptree::{builder::TreeBuilder, print_tree};
use tracing::trace;

type Result<T> = StdResult<T, BlockTreeError>;

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum BlockTreeError {
    BlockNotExist(String),
}

impl fmt::Display for BlockTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockNotExist(s) => write!(f, "Block not exist: {}", s),
        }
    }
}

impl std::error::Error for BlockTreeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug)]
pub struct BlockTreeBlock<T> {
    block: Block<T>,
    parent: Option<BlockId>,
    children: Vec<BlockId>,
}

impl<T: SignatureCollection> BlockTreeBlock<T> {
    const CHILD_INDENT: &str = "    ";
    fn new(b: Block<T>, parent: Option<BlockId>) -> Self {
        BlockTreeBlock {
            block: b,
            parent,
            children: Vec::new(),
        }
    }

    fn tree_fmt(
        &self,
        tree: &BlockTree<T>,
        f: &mut fmt::Formatter<'_>,
        indent: &String,
    ) -> std::fmt::Result {
        writeln!(
            f,
            "{}({:?}){:?}",
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

#[derive(Debug, PartialEq, Eq)]
pub enum RootKind {
    /// There is a valid root for the BlockTree which has been committed
    Rooted(BlockId),
    /// The BlockTree does not have a committed root but knows which round
    /// should be committed next. Multiple blocks could be added at the RootRound
    /// and the BlockTree should keep them all until one of the branches gets
    /// extended enough to commit
    Unrooted(Round),
}

pub struct BlockTree<T> {
    root: RootKind,
    tree: HashMap<BlockId, BlockTreeBlock<T>>,
    high_round: Round,
}

impl<T: SignatureCollection> std::fmt::Debug for BlockTree<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.root {
            RootKind::Rooted(b) => {
                let root = self.tree.get(&b).unwrap();
                root.tree_fmt(self, f, &"".to_owned())?;
            }
            RootKind::Unrooted(r) => {
                for (_, block) in self.tree.iter().filter(|a| a.1.block.round == r) {
                    block.tree_fmt(self, f, &"".to_owned())?;
                }
            }
        }

        Ok(())
    }
}

impl<T: SignatureCollection> BlockTree<T> {
    pub fn new(genesis_block: Block<T>) -> Self {
        let bid = genesis_block.get_id();
        let round = genesis_block.round;
        let mut tree = HashMap::new();
        tree.insert(bid, BlockTreeBlock::new(genesis_block, None));
        Self {
            root: RootKind::Rooted(bid),
            tree,
            high_round: round,
        }
    }

    pub fn new_unrooted(root_round: Round) -> Self {
        Self {
            root: RootKind::Unrooted(root_round),
            tree: HashMap::new(),
            high_round: Round(0),
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
        let new_root_block = &self
            .tree
            .get(new_root)
            .ok_or(BlockTreeError::BlockNotExist(format!("{:?}", new_root)))?
            .block;
        let new_root_round = new_root_block.round;
        match self.root {
            RootKind::Rooted(b) => {
                if b == *new_root {
                    inc_count!(blocktree.prune.noop);
                    return Ok(commit);
                }
            }
            RootKind::Unrooted(r) => {
                if r == new_root_round {
                    inc_count!(blocktree.prune.noop);
                    return Ok(commit);
                }
            }
        }
        commit.push(new_root_block.clone());

        let mut cur = new_root_block.qc.info.vote.id;
        let mut cur_round = new_root_block.qc.info.vote.round;

        let root_cmp = |b_cur, r_cur| match self.root {
            RootKind::Rooted(b) => b != b_cur,
            RootKind::Unrooted(r) => r != r_cur,
        };

        // traverse up the branch from new_root, removing blocks
        // and pushing to the commit list. The old root in a
        // RootKind::Rooted tree has already been committed so
        // should not be added to the list now
        while root_cmp(cur, cur_round) {
            let block = self.tree.remove(&cur).unwrap();
            cur = block.block.qc.info.vote.id;
            cur_round = block.block.qc.info.vote.round;
            commit.push(block.block);
        }

        // if the tree was unrooted, the old root of the branch
        // traversed was never committed before so it should be
        // added to the list now
        if matches!(self.root, RootKind::Unrooted(_)) {
            let block = self.tree.remove(&cur).unwrap();
            commit.push(block.block);
        }

        self.root = RootKind::Rooted(*new_root);

        // garbage collect old blocks
        // remove any blocks less than or equal to round `n`
        // should only keep the new root at round `n`, and `n+1` blocks
        self.tree.retain(|_, b| {
            b.block.round > new_root_round || RootKind::Rooted(b.block.get_id()) == self.root
        });

        commit.reverse();
        inc_count!(blocktree.prune.success);
        Ok(commit)
    }

    pub fn add(&mut self, b: Block<T>) -> Result<()> {
        if self.tree.contains_key(&b.get_id()) {
            inc_count!(blocktree.add.duplicate);
            return Ok(());
        }

        let new_bid = b.get_id();
        let new_round = b.round;
        let parent_bid = b.qc.info.vote.id;

        // if the new block's parent is already in the tree, add
        // new block to the parent's children
        if let Some(parent) = self.tree.get_mut(&parent_bid) {
            parent.children.push(new_bid);
        }

        let mut new_btb = BlockTreeBlock::new(b, Some(parent_bid));
        // find all nodes who would be children of the new block
        let children = self
            .tree
            .iter()
            .filter(|(_k, v)| v.parent == Some(new_bid))
            .map(|k| k.0)
            .collect::<Vec<_>>();

        new_btb.children.extend(children);
        self.tree.insert(new_bid, new_btb);
        self.high_round = new_round;
        inc_count!(blocktree.add.success);
        Ok(())
    }

    pub fn path_to_root(&self, b: &BlockId) -> bool {
        let mut bid = b;
        let root_match = |block_tree_block: &BlockTreeBlock<T>| match self.root {
            RootKind::Rooted(b) => block_tree_block.block.get_id() == b,
            RootKind::Unrooted(r) => block_tree_block.block.round == r,
        };
        loop {
            let Some(i) = self.tree.get(bid) else {
                return false;
            };
            if root_match(i) {
                return true;
            }
            let Some(parent_id) = &i.parent else {
                return false;
            };
            bid = parent_id;
        }
    }

    /// returns true if the parent block id in the QC of a block
    /// exists in the blocktree.
    /// Root blocks also return true.
    pub fn has_parent(&self, b: &Block<T>) -> bool {
        match self.root {
            RootKind::Rooted(root_id) => {
                if root_id == b.get_id() {
                    return true;
                }
            }
            RootKind::Unrooted(round) => {
                if round == b.round {
                    return true;
                }
            }
        }

        self.tree.contains_key(&b.qc.info.vote.id)
    }

    pub fn debug_print(&self) -> std::io::Result<()> {
        let mut tree = TreeBuilder::new("BlockTree".to_owned());

        match self.root {
            RootKind::Rooted(r) => {
                let root = self.tree.get(&r).unwrap();
                root.build_ptree(self, &mut tree);

                print_tree(&tree.build())
            }
            RootKind::Unrooted(_) => Ok(()),
        }
    }

    pub fn tree(&self) -> &HashMap<BlockId, BlockTreeBlock<T>> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_consensus_types::{
        block::{Block as ConsensusBlock, TransactionList},
        ledger::LedgerCommitInfo,
        quorum_certificate::{QcInfo, QuorumCertificate},
        validation::Sha256Hash,
        voting::VoteInfo,
    };
    use monad_crypto::{secp256k1::KeyPair, GenericKeyPair};
    use monad_testutil::signing::MockSignatures;
    use monad_types::{BlockId, Hash, NodeId, Round};

    use super::{BlockTree, BlockTreeError, RootKind};

    type Block = ConsensusBlock<MockSignatures>;
    type QC = QuorumCertificate<MockSignatures>;

    fn node_id() -> NodeId {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
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
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
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
                MockSignatures::with_pubkeys(&[]),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
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
                MockSignatures::with_pubkeys(&[]),
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
                MockSignatures::with_pubkeys(&[]),
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
                MockSignatures::with_pubkeys(&[]),
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
                MockSignatures::with_pubkeys(&[]),
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

        assert!(blocktree.path_to_root(&b1.get_id()));
        assert!(blocktree.path_to_root(&b2.get_id()));
        assert!(blocktree.path_to_root(&b3.get_id()));
        assert!(blocktree.path_to_root(&b4.get_id()));
        assert!(blocktree.path_to_root(&b5.get_id()));
        assert!(blocktree.path_to_root(&b6.get_id()));

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
        assert!(matches!(
            blocktree.prune(&g.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        ));
        assert!(matches!(
            blocktree.prune(&b1.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        ));
        assert!(matches!(
            blocktree.prune(&b2.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        ));
        assert!(matches!(
            blocktree.prune(&b4.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        ));
        assert!(!blocktree.path_to_root(&g.get_id()));
        assert!(!blocktree.path_to_root(&b1.get_id()));
        assert!(!blocktree.path_to_root(&b2.get_id()));
        assert!(!blocktree.path_to_root(&b1.get_id()));

        // Pruned blocktree after insertion
        //     b5
        //   /    \
        //  b6    b8
        //  |
        //  b7

        assert!(blocktree.add(b7).is_ok());

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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        assert!(blocktree.add(b8).is_ok());
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
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
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
                MockSignatures::with_pubkeys(&[]),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let gid = g.get_id();
        let mut blocktree = BlockTree::new(g);

        assert_eq!(blocktree.tree.get(&gid).unwrap().parent, None);
        assert!(blocktree.add(b2.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 2);
        assert_eq!(
            blocktree.tree.get(&b2.get_id()).unwrap().parent,
            Some(b1.get_id())
        );
        assert!(!blocktree.path_to_root(&b2.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 3);
        assert!(blocktree.path_to_root(&b1.get_id()));
        assert!(blocktree.path_to_root(&b2.get_id()));
        assert_eq!(
            blocktree.tree.get(&b2.get_id()).unwrap().parent,
            Some(b1.get_id())
        );
        assert_eq!(blocktree.tree.get(&b1.get_id()).unwrap().parent, Some(gid));
    }

    #[test]
    fn equal_level_branching() {
        let txlist = TransactionList(vec![]);
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let b1 = Block::new::<Sha256Hash>(
            node_id(),
            Round(1),
            &TransactionList(vec![1]),
            &QC::new(
                QcInfo {
                    vote: v1,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b2 = Block::new::<Sha256Hash>(
            node_id(),
            Round(1),
            &TransactionList(vec![2]),
            &QC::new(
                QcInfo {
                    vote: v1,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
        };

        let b3 = Block::new::<Sha256Hash>(
            node_id(),
            Round(2),
            &TransactionList(vec![3]),
            &QC::new(
                QcInfo {
                    vote: v2,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        // Initial blocktree
        //        g
        //   /    |
        //  b1    b2
        //  |
        //  b3
        let mut blocktree = BlockTree::<MockSignatures>::new(g);
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.add(b3).is_ok());

        // prune called on b1, we expect new tree to be
        // b1
        // |
        // b3
        // and the commit blocks should only contain b1 (not b2)
        let commit = blocktree.prune(&b1.get_id()).unwrap();
        matches!(
            blocktree.prune(&b2.get_id()).unwrap_err(),
            BlockTreeError::BlockNotExist(_)
        );
        assert_eq!(commit.len(), 1);
        assert_eq!(commit[0].get_id(), b1.get_id());
    }

    #[test]
    fn duplicate_blocks() {
        let txlist = TransactionList(vec![]);
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let b1 = Block::new::<Sha256Hash>(
            node_id(),
            Round(1),
            &TransactionList(vec![1]),
            &QC::new(
                QcInfo {
                    vote: v1,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut blocktree = BlockTree::<MockSignatures>::new(g);
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b1).is_ok());

        assert_eq!(blocktree.tree.len(), 2);
        assert!(matches!(blocktree.root, RootKind::Rooted(_)));
        match blocktree.root {
            RootKind::Rooted(b) => {
                assert_eq!(blocktree.tree[&b].children.len(), 1);
            }
            RootKind::Unrooted(_) => {}
        }
    }

    #[test]
    fn paths_to_root() {
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let b1 = Block::new::<Sha256Hash>(
            node_id(),
            Round(1),
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: v1,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
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
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: v2,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b3 = Block::new::<Sha256Hash>(
            node_id(),
            Round(3),
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: v2,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b4 = Block::new::<Sha256Hash>(
            node_id(),
            Round(4),
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: v2,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut blocktree = BlockTree::<MockSignatures>::new(g.clone());
        assert!(blocktree.path_to_root(&g.get_id()));
        assert!(!blocktree.path_to_root(&b1.get_id()));

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(!blocktree.path_to_root(&b2.get_id()));

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(!blocktree.path_to_root(&b3.get_id()));

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(!blocktree.path_to_root(&b4.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.path_to_root(&b1.get_id()));
        assert!(blocktree.path_to_root(&b2.get_id()));
        assert!(blocktree.path_to_root(&b3.get_id()));
        assert!(blocktree.path_to_root(&b4.get_id()));
        let children = [&b2, &b3, &b4]
            .iter()
            .map(|b| b.get_id())
            .collect::<Vec<_>>();
        let b: HashSet<&BlockId> = HashSet::from_iter(children.iter());
        let a: HashSet<&BlockId> =
            HashSet::from_iter(blocktree.tree.get(&b1.get_id()).unwrap().children.iter());
        assert_eq!(a, b);

        assert!(blocktree.prune(&b3.get_id()).is_ok());
        assert!(blocktree.path_to_root(&b3.get_id()));

        assert!(!blocktree.path_to_root(&g.get_id()));
        assert!(!blocktree.path_to_root(&b1.get_id()));
        assert!(!blocktree.path_to_root(&b2.get_id()));
        assert!(!blocktree.path_to_root(&b4.get_id()));
    }

    #[test]
    fn unrooted_transition_to_rooted() {
        let mut blocktree = BlockTree::<MockSignatures>::new_unrooted(Round(4));

        let txlist = TransactionList(vec![]);
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );
        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v5 = VoteInfo {
            id: b4.get_id(),
            round: Round(4),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b4.get_id(),
            parent_round: Round(4),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b4.get_id()));

        assert!(blocktree.add(b5.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b5.get_id()));

        assert!(blocktree.add(b6.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b6.get_id()));

        let commit = blocktree.prune(&b6.get_id()).unwrap();
        assert_eq!(
            Vec::from_iter(commit.iter().map(|b| b.get_id())),
            vec![b4.get_id(), b5.get_id(), b6.get_id()]
        );

        assert!(matches!(blocktree.root, RootKind::Rooted(b) if b == b6.get_id()));
    }

    #[test]
    fn unrooted_many_potential_roots() {
        let mut blocktree = BlockTree::<MockSignatures>::new_unrooted(Round(4));

        let txlist = TransactionList(vec![]);
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v2 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let b2 = Block::new::<Sha256Hash>(
            node_id(),
            Round(4),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v2,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let b3 = Block::new::<Sha256Hash>(
            node_id(),
            Round(4),
            &txlist,
            &QC::new(
                QcInfo {
                    vote: v3,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v5 = VoteInfo {
            id: b4.get_id(),
            round: Round(4),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b4.get_id(),
            parent_round: Round(4),
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
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b2.get_id()));

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b3.get_id()));

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b4.get_id()));

        assert!(blocktree.add(b5.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b5.get_id()));

        assert!(blocktree.add(b6.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.path_to_root(&b6.get_id()));

        let commit = blocktree.prune(&b6.get_id()).unwrap();
        assert_eq!(
            Vec::from_iter(commit.iter().map(|b| b.get_id())),
            vec![b4.get_id(), b5.get_id(), b6.get_id()]
        );

        assert_eq!(blocktree.tree.len(), 1);
        assert!(matches!(blocktree.root, RootKind::Rooted(b) if b == b6.get_id()));
    }

    #[test]
    fn test_has_parent() {
        let g = Block::new::<Sha256Hash>(
            node_id(),
            Round(0),
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let b1 = Block::new::<Sha256Hash>(
            node_id(),
            Round(1),
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: v1,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v4 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(3),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(3),
        };

        let b4 = Block::new::<Sha256Hash>(
            node_id(),
            Round(4),
            &TransactionList(Vec::new()),
            &QC::new(
                QcInfo {
                    vote: v4,
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut rooted_blocktree = BlockTree::<MockSignatures>::new(g.clone());
        assert!(rooted_blocktree.add(b1.clone()).is_ok());
        assert!(rooted_blocktree.add(b4.clone()).is_ok());

        assert!(rooted_blocktree.has_parent(&b1));
        assert!(rooted_blocktree.has_parent(&g));
        assert!(!rooted_blocktree.has_parent(&b4));

        let mut unrooted_blocktree = BlockTree::<MockSignatures>::new_unrooted(Round(4));
        assert!(unrooted_blocktree.add(b4.clone()).is_ok());
        assert!(unrooted_blocktree.has_parent(&b4));
    }
}
