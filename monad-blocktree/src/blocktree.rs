use std::{collections::HashMap, fmt, result::Result as StdResult};

use monad_consensus_types::{
    block::{BlockType, FullBlock},
    payload::TransactionHashList,
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, Round};
use ptree::{builder::TreeBuilder, print_tree};
use tracing::trace;

type Result<T> = StdResult<T, BlockTreeError>;

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum BlockTreeError {
    BlockNotExist(BlockId),
}

impl fmt::Display for BlockTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockNotExist(bid) => write!(f, "Block not exist: {:?}", bid),
        }
    }
}

impl std::error::Error for BlockTreeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// Block in the block tree
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTreeBlock<T> {
    /// Consensus full block
    block: FullBlock<T>,

    /// Link to the parent block in the tree. `p_bid == block.qc.vote.block_id`
    ///
    /// Meaningless for the root node [BlockTree::root]
    parent: BlockId,

    /// Link to the child blocks in the tree
    ///
    /// Only used to print the tree top-down for debugging
    children: Vec<BlockId>,
}

impl<T: SignatureCollection> BlockTreeBlock<T> {
    const CHILD_INDENT: &'static str = "    ";
    fn new(b: FullBlock<T>) -> Self {
        BlockTreeBlock {
            parent: b.get_parent_id(),
            block: b,
            children: Vec::new(),
        }
    }

    /// Fetches the consensus block from the tree block
    pub fn get_block(&self) -> &FullBlock<T> {
        &self.block
    }

    /// Recursively print the tree top-down. Child blocks are indented and
    /// printed below their parent, like a numbered list
    ///
    /// Format: (round)block_id
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
            self.block.get_round().0,
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

    /// An alternative tree formatting with [ptree]
    fn build_ptree<'a>(
        &self,
        tree: &BlockTree<T>,
        tree_print: &'a mut TreeBuilder,
    ) -> &'a mut TreeBuilder {
        let mut result = tree_print.begin_child(format!(
            "({}){:?}",
            self.block.get_round().0,
            self.block.get_id()
        ));
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

#[derive(PartialEq, Eq)]
pub struct BlockTree<T> {
    pub root: RootKind,
    tree: HashMap<BlockId, BlockTreeBlock<T>>,
}

impl<T: SignatureCollection> std::fmt::Debug for BlockTree<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.root {
            RootKind::Rooted(b) => {
                let root = self.tree.get(&b).unwrap();
                root.tree_fmt(self, f, &"".to_owned())?;
            }
            RootKind::Unrooted(r) => {
                for (_, block) in self.tree.iter().filter(|a| a.1.block.get_round() == r) {
                    block.tree_fmt(self, f, &"".to_owned())?;
                }
            }
        }

        Ok(())
    }
}

impl<T: SignatureCollection> BlockTree<T> {
    pub fn new(genesis_block: FullBlock<T>) -> Self {
        let bid = genesis_block.get_id();
        let mut tree = HashMap::new();
        tree.insert(bid, BlockTreeBlock::new(genesis_block));
        Self {
            root: RootKind::Rooted(bid),
            tree,
        }
    }

    pub fn new_unrooted(root_round: Round) -> Self {
        Self {
            root: RootKind::Unrooted(root_round),
            tree: HashMap::new(),
        }
    }

    /// Prune the block tree and returns the blocks to commit along that branch
    /// in increasing round. After a successful prune, `new_root` is the root of
    /// the block tree. All blocks pruned are deallocated
    ///
    /// The prune algorithm removes all blocks with lower round than `new_root`
    ///
    /// Short proof on the correctness of the algorithm
    ///
    /// - On commit, this function is called with the block to commit as
    ///   the`new_root`. Recall the commit rule stating a QC-of-QC commits the
    ///   block
    ///
    /// - Nodes that aren't descendents of `new_root` are added before
    ///   `new_root` and have round number smaller than `new_root` It only
    ///   prunes blocks not part of the `new_root` subtree -> accurate
    ///
    /// - When prune is called on round `new_root` block, only round `n+1` block
    ///   is added (as `new_root`'s children) All the blocks that remains
    ///   shouldn't be pruned -> complete
    pub fn prune(&mut self, new_root: &BlockId) -> Result<Vec<FullBlock<T>>> {
        // retain the new root block, remove all other committable block to
        // avoid copying
        let mut commit: Vec<FullBlock<T>> = Vec::new();
        let new_root_block = &self
            .tree
            .get(new_root)
            .ok_or(BlockTreeError::BlockNotExist(*new_root))?
            .block;
        let new_root_round = new_root_block.get_round();

        if self.root_match(new_root_block.get_id(), new_root_round) {
            inc_count!(blocktree.prune.noop);
            return Ok(commit);
        }

        commit.push(new_root_block.clone());

        let mut visit_bid = new_root_block.get_parent_id();
        let mut visit_round = new_root_block.get_parent_round();

        // traverse up the branch from new_root, removing blocks and pushing to
        // the commit list. The old root in a RootKind::Rooted tree has already
        // been committed so should not be added to the list now
        while !self.root_match(visit_bid, visit_round) {
            let block = self.tree.remove(&visit_bid).unwrap();
            visit_bid = block.block.get_parent_id();
            visit_round = block.block.get_parent_round();
            commit.push(block.block);
        }

        // if the tree was unrooted, the old root of the branch traversed was
        // never committed before so it should be added to the list now
        if matches!(self.root, RootKind::Unrooted(_)) {
            let block = self.tree.remove(&visit_bid).unwrap();
            commit.push(block.block);
        }

        self.root = RootKind::Rooted(*new_root);

        // garbage collect old blocks
        // remove any blocks less than or equal to round `n` should only keep
        // the new root at round `n`, and `n+1` blocks
        self.tree.retain(|_, b| {
            b.block.get_round() > new_root_round || RootKind::Rooted(b.block.get_id()) == self.root
        });

        commit.reverse();
        inc_count!(blocktree.prune.success);
        Ok(commit)
    }

    /// Add a new block to the block tree if it's not in the tree and is higher
    /// than the root block's round number
    pub fn add(&mut self, b: FullBlock<T>) -> Result<()> {
        if !self.is_valid_to_insert(&b) {
            inc_count!(blocktree.add.duplicate);
            return Ok(());
        }

        let new_bid = b.get_id();
        let parent_bid = b.get_parent_id();

        // if the new block's parent is already in the tree, add new block to
        // the parent's children
        if let Some(parent) = self.tree.get_mut(&parent_bid) {
            parent.children.push(new_bid);
        }

        let mut new_btb = BlockTreeBlock::new(b);
        // find all nodes who would be children of the new block
        let children = self
            .tree
            .iter()
            .filter(|(_k, v)| v.parent == new_bid)
            .map(|k| k.0)
            .collect::<Vec<_>>();

        new_btb.children.extend(children);
        self.tree.insert(new_bid, new_btb);
        inc_count!(blocktree.add.success);
        Ok(())
    }

    fn root_match(&self, block_id: BlockId, block_round: Round) -> bool {
        match self.root {
            RootKind::Rooted(b) => block_id == b,
            RootKind::Unrooted(r) => block_round == r,
        }
    }

    /// Find the missing block along the path from `qc` to the tree root.
    /// Returns the QC certifying that block
    pub fn get_missing_ancestor(&self, qc: &QuorumCertificate<T>) -> Option<QuorumCertificate<T>> {
        match self.root {
            RootKind::Rooted(b) => {
                if self
                    .tree
                    .get(&b)
                    .expect("Rooted blocktree doesn't have the corresponding root block")
                    .get_block()
                    .get_round()
                    >= qc.info.vote.round
                {
                    return None;
                }
            }
            RootKind::Unrooted(r) => {
                if r > qc.info.vote.round {
                    return None;
                }
            }
        };

        let mut bid = &qc.info.vote.id;
        let mut current_qc = qc;
        while let Some(i) = self.tree.get(bid) {
            if self.root_match(i.block.get_id(), i.block.get_round()) {
                return None;
            }
            bid = &i.parent;
            current_qc = &i.block.get_block().qc;
        }
        Some(current_qc.clone())
    }

    pub fn has_path_to_root(&self, b: &BlockId) -> bool {
        let mut visit = b;

        while let Some(btb) = self.tree.get(visit) {
            if self.root_match(btb.block.get_id(), btb.block.get_round()) {
                return true;
            }
            visit = &btb.parent;
        }
        false
    }

    /// Fetches transactions on the path in [`b`, root)
    pub fn get_txs_on_path_to_root(&self, b: &BlockId) -> Option<Vec<TransactionHashList>> {
        // The tree must be rooted, because the path visit stops at the root node
        debug_assert!(matches!(self.root, RootKind::Rooted(_)));
        let mut txs = Vec::default();

        let mut visit = b;

        while let Some(btb) = self.tree.get(visit) {
            if self.root_match(btb.block.get_id(), btb.block.get_round()) {
                return Some(txs);
            }

            txs.push(btb.block.get_block().payload.txns.clone());

            visit = &btb.parent;
        }
        None
    }

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &FullBlock<T>) -> bool {
        return !self.tree.contains_key(&b.get_id()) && {
            match self.root {
                RootKind::Rooted(root_id) => {
                    b.get_round() > self.tree.get(&root_id).unwrap().block.get_round()
                }
                RootKind::Unrooted(round) => b.get_round() >= round,
            }
        };
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
        block::{Block as ConsensusBlock, BlockType, FullBlock},
        ledger::LedgerCommitInfo,
        payload::{
            ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal, TransactionHashList,
        },
        quorum_certificate::{QcInfo, QuorumCertificate},
        transaction_validator::MockValidator,
        voting::VoteInfo,
    };
    use monad_crypto::{hasher::Hash, secp256k1::KeyPair};
    use monad_eth_types::EthAddress;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{BlockId, NodeId, Round, SeqNum};

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
        let payload = Payload {
            txns: TransactionHashList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b3 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(3),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v3,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: b3.get_id(),
            round: Round(3),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b5 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(5),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b3.get_id(),
            parent_round: Round(3),
            seq_num: SeqNum(0),
        };

        let b6 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(6),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v6,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v7 = VoteInfo {
            id: b6.get_id(),
            round: Round(6),
            parent_id: b5.get_id(),
            parent_round: Round(5),
            seq_num: SeqNum(0),
        };

        let b7 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(7),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v7,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

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

        assert!(blocktree.has_path_to_root(&b1.get_id()));
        assert!(blocktree.has_path_to_root(&b2.get_id()));
        assert!(blocktree.has_path_to_root(&b3.get_id()));
        assert!(blocktree.has_path_to_root(&b4.get_id()));
        assert!(blocktree.has_path_to_root(&b5.get_id()));
        assert!(blocktree.has_path_to_root(&b6.get_id()));

        // pruning on the old root should return no committable blocks
        let commit = blocktree.prune(&g.get_id()).unwrap();
        assert_eq!(commit.len(), 0);

        let commit = blocktree.prune(&b5.get_id()).unwrap();
        assert_eq!(
            Vec::from_iter(commit.iter().map(|b| b.get_id())),
            vec![b3.get_id(), b5.get_id()]
        );
        println!("{:?}", blocktree);
        println!("{:?}", blocktree.tree);

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
        assert!(!blocktree.has_path_to_root(&g.get_id()));
        assert!(!blocktree.has_path_to_root(&b1.get_id()));
        assert!(!blocktree.has_path_to_root(&b2.get_id()));
        assert!(!blocktree.has_path_to_root(&b1.get_id()));

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
            seq_num: SeqNum(0),
        };

        let b8 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(8),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v8,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        assert!(blocktree.add(b8).is_ok());
        println!("{:?}", blocktree);
    }

    #[test]
    fn test_add_parent_not_exist() {
        let payload = Payload {
            txns: TransactionHashList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let gid = g.get_id();
        let mut blocktree = BlockTree::new(g);

        assert!(blocktree.add(b2.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 2);
        assert_eq!(
            blocktree.tree.get(&b2.get_id()).unwrap().parent,
            b1.get_id()
        );
        assert!(!blocktree.has_path_to_root(&b2.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 3);
        assert!(blocktree.has_path_to_root(&b1.get_id()));
        assert!(blocktree.has_path_to_root(&b2.get_id()));
        assert_eq!(
            blocktree.tree.get(&b2.get_id()).unwrap().parent,
            b1.get_id()
        );
        assert_eq!(blocktree.tree.get(&b1.get_id()).unwrap().parent, gid);
    }

    #[test]
    fn equal_level_branching() {
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &Payload {
                    txns: TransactionHashList::empty(),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::new(vec![1].into()),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let b2 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::new(vec![2].into()),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b3 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
                &Payload {
                    txns: TransactionHashList::new(vec![3].into()),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

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
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &Payload {
                    txns: TransactionHashList::empty(),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::new(vec![1].into()),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

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
        let payload = Payload {
            txns: TransactionHashList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let b3 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(3),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let mut blocktree = BlockTree::<MockSignatures>::new(g.clone());
        assert!(blocktree.has_path_to_root(&g.get_id()));
        assert!(!blocktree.has_path_to_root(&b1.get_id()));

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(!blocktree.has_path_to_root(&b2.get_id()));

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(!blocktree.has_path_to_root(&b3.get_id()));

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(!blocktree.has_path_to_root(&b4.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.has_path_to_root(&b1.get_id()));
        assert!(blocktree.has_path_to_root(&b2.get_id()));
        assert!(blocktree.has_path_to_root(&b3.get_id()));
        assert!(blocktree.has_path_to_root(&b4.get_id()));
        let children = [&b2, &b3, &b4]
            .iter()
            .map(|b| b.get_id())
            .collect::<Vec<_>>();
        let b: HashSet<&BlockId> = HashSet::from_iter(children.iter());
        let a: HashSet<&BlockId> =
            HashSet::from_iter(blocktree.tree.get(&b1.get_id()).unwrap().children.iter());
        assert_eq!(a, b);

        assert!(blocktree.prune(&b3.get_id()).is_ok());
        assert!(blocktree.has_path_to_root(&b3.get_id()));

        assert!(!blocktree.has_path_to_root(&g.get_id()));
        assert!(!blocktree.has_path_to_root(&b1.get_id()));
        assert!(!blocktree.has_path_to_root(&b2.get_id()));
        assert!(!blocktree.has_path_to_root(&b4.get_id()));
    }

    #[test]
    fn unrooted_transition_to_rooted() {
        let mut blocktree = BlockTree::<MockSignatures>::new_unrooted(Round(4));

        let payload = Payload {
            txns: TransactionHashList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };

        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: b4.get_id(),
            round: Round(4),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b5 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(5),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b4.get_id(),
            parent_round: Round(4),
            seq_num: SeqNum(0),
        };

        let b6 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(6),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v6,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b4.get_id()));

        assert!(blocktree.add(b5.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b5.get_id()));

        assert!(blocktree.add(b6.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b6.get_id()));

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

        let payload = Payload {
            txns: TransactionHashList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b3 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v3,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: b4.get_id(),
            round: Round(4),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b5 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(5),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b4.get_id(),
            parent_round: Round(4),
            seq_num: SeqNum(0),
        };

        let b6 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(6),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v6,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b2.get_id()));

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b3.get_id()));

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b4.get_id()));

        assert!(blocktree.add(b5.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b5.get_id()));

        assert!(blocktree.add(b6.clone()).is_ok());
        assert!(matches!(blocktree.root, RootKind::Unrooted(Round(4))));
        assert!(blocktree.has_path_to_root(&b6.get_id()));

        let commit = blocktree.prune(&b6.get_id()).unwrap();
        assert_eq!(
            Vec::from_iter(commit.iter().map(|b| b.get_id())),
            vec![b4.get_id(), b5.get_id(), b6.get_id()]
        );

        assert_eq!(blocktree.tree.len(), 1);
        assert!(matches!(blocktree.root, RootKind::Rooted(b) if b == b6.get_id()));
    }

    #[test]
    fn test_is_valid() {
        let blocktree = BlockTree::<MockSignatures>::new_unrooted(Round(4));
        let payload = Payload {
            txns: TransactionHashList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b5 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(5),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        // genesis should not be valid since its round is lower than root
        assert!(!blocktree.is_valid_to_insert(&g));
        // block 4 should va valid, since the tree is unrooted and is one round 4
        assert!(blocktree.is_valid_to_insert(&b4));
        // b5 will be valid because its at least (root round) + 1
        assert!(blocktree.is_valid_to_insert(&b5))
    }

    #[test]
    fn test_get_missing_ancestor() {
        // Initial blocktree
        //        g
        //        |
        //        b1
        //  |  -  |  -  |
        //  b2    b3    b4

        let payload = Payload {
            txns: TransactionHashList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(0),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let b3 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(3),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
                &payload,
                &QC::new(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let mut blocktree = BlockTree::<MockSignatures>::new(g.clone());
        assert!(blocktree.get_missing_ancestor(&g.get_block().qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.get_block().qc).unwrap() == b2.get_block().qc);

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.get_block().qc).unwrap() == b3.get_block().qc);

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b4.get_block().qc).unwrap() == b4.get_block().qc);

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b1.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b2.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b3.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b4.get_block().qc).is_none());

        blocktree.prune(&b1.get_id()).unwrap();
        assert!(blocktree.get_missing_ancestor(&g.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b1.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b2.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b3.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b4.get_block().qc).is_none());

        assert!(blocktree.size() == 4);
    }
}
