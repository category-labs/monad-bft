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

#[cfg_attr(feature = "monad_test", derive(PartialEq, Eq))]
#[derive(Debug, Clone)]
pub struct BlockTreeBlock<T> {
    block: FullBlock<T>,
    parent: Option<BlockId>,
    children: Vec<BlockId>,
}

impl<T: SignatureCollection> BlockTreeBlock<T> {
    const CHILD_INDENT: &str = "    ";
    fn new(b: FullBlock<T>, parent: Option<BlockId>) -> Self {
        BlockTreeBlock {
            block: b,
            parent,
            children: Vec::new(),
        }
    }

    pub fn get_block(&self) -> &FullBlock<T> {
        &self.block
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

#[cfg_attr(feature = "monad_test", derive(PartialEq, Eq))]
pub struct BlockTree<T> {
    pub root: RootKind,
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
        let round = genesis_block.get_round();
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
    pub fn prune(&mut self, new_root: &BlockId) -> Result<Vec<FullBlock<T>>> {
        // retain the new root block, remove all other committable block to avoid copying
        let mut commit: Vec<FullBlock<T>> = Vec::new();
        let new_root_block = &self
            .tree
            .get(new_root)
            .ok_or(BlockTreeError::BlockNotExist(format!("{:?}", new_root)))?
            .block;
        let new_root_round = new_root_block.get_round();
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

        let mut cur = new_root_block.get_parent_id();
        let mut cur_round = new_root_block.get_parent_round();

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
            cur = block.block.get_parent_id();
            cur_round = block.block.get_parent_round();
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
            b.block.get_round() > new_root_round || RootKind::Rooted(b.block.get_id()) == self.root
        });

        commit.reverse();
        inc_count!(blocktree.prune.success);
        Ok(commit)
    }

    pub fn add(&mut self, b: FullBlock<T>) -> Result<()> {
        if !self.is_valid(&b) {
            inc_count!(blocktree.add.duplicate);
            return Ok(());
        }

        let new_bid = b.get_id();
        let new_round = b.get_round();
        let parent_bid = b.get_parent_id();

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

    fn root_match(&self, block_tree_block: &BlockTreeBlock<T>) -> bool {
        match self.root {
            RootKind::Rooted(b) => block_tree_block.block.get_id() == b,
            RootKind::Unrooted(r) => block_tree_block.block.get_round() == r,
        }
    }

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
                if r >= qc.info.vote.round {
                    return None;
                }
            }
        };

        let mut bid = &qc.info.vote.id;
        let mut current_qc = qc;
        loop {
            let Some(i) = self.tree.get(bid) else {
                return Some(current_qc.clone());
            };
            if self.root_match(i) {
                return None;
            }
            let Some(parent_id) = &i.parent else {
                return None;
            };
            bid = parent_id;
            current_qc = &i.block.get_block().qc;
        }
    }

    pub fn path_to_root(&self, b: &BlockId) -> bool {
        self.get_txs_on_path_to_root(b).is_some()
    }

    pub fn get_txs_on_path_to_root(&self, b: &BlockId) -> Option<Vec<TransactionHashList>> {
        let mut txs = Vec::default();

        let mut bid = b;

        loop {
            let Some(i) = self.tree.get(bid) else {
                return None;
            };

            txs.push(i.block.get_block().payload.txns.clone());

            if self.root_match(i) {
                return Some(txs);
            }

            let Some(parent_id) = &i.parent else {
                return None;
            };

            bid = parent_id;
        }
    }

    pub fn get_blocks_on_path_to_root(&self, b: &BlockId) -> Option<Vec<FullBlock<T>>> {
        let mut blocks = Vec::default();

        let mut bid = b;

        loop {
            let Some(i) = self.tree.get(bid) else {
                return None;
            };

            blocks.push(i.block.clone());

            if self.root_match(i) {
                return Some(blocks);
            }

            let Some(parent_id) = &i.parent else {
                return None;
            };

            bid = parent_id;
        }
    }

    /// returns true if the parent block id in the QC of a block
    /// exists in the blocktree.
    /// Root blocks also return true.
    pub fn has_parent(&self, b: &FullBlock<T>) -> bool {
        match self.root {
            RootKind::Rooted(root_id) => {
                if root_id == b.get_id() {
                    return true;
                }
            }
            RootKind::Unrooted(round) => {
                if round == b.get_round() {
                    return true;
                }
            }
        }

        self.tree.contains_key(&b.get_parent_id())
    }

    pub fn is_valid(&self, b: &FullBlock<T>) -> bool {
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

    pub fn get_seq_num(&self, block_id: BlockId) -> Option<u64> {
        self.tree.get(&block_id).map(|b| b.block.get_seq_num())
    }

    pub fn get_root_seq_num(&self) -> Option<u64> {
        match self.root {
            RootKind::Rooted(block_id) => self.get_seq_num(block_id),
            RootKind::Unrooted(_) => None,
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
    use monad_crypto::{
        hasher::{Hash, HasherType},
        secp256k1::KeyPair,
    };
    use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
    use monad_testutil::signing::MockSignatures;
    use monad_types::{BlockId, NodeId, Round};

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
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b1 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b2 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(2),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b3 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(3),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v3,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b4 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: b3.get_id(),
            round: Round(3),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b5 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(5),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b3.get_id(),
            parent_round: Round(3),
            seq_num: 0,
        };

        let b6 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(6),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v6,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v7 = VoteInfo {
            id: b6.get_id(),
            round: Round(6),
            parent_id: b5.get_id(),
            parent_round: Round(5),
            seq_num: 0,
        };

        let b7 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(7),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v7,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
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
            seq_num: 0,
        };

        let b8 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(8),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v8,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        assert!(blocktree.add(b8).is_ok());
        println!("{:?}", blocktree);
    }

    #[test]
    fn test_add_parent_not_exist() {
        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b1 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b2 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(2),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

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
        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &Payload {
                    txns: TransactionHashList::default(),
                    header: ExecutionArtifacts::zero(),
                    seq_num: 0,
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b1 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::new(vec![1]),
                    header: ExecutionArtifacts::zero(),
                    seq_num: 0,
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let b2 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::new(vec![2]),
                    header: ExecutionArtifacts::zero(),
                    seq_num: 0,
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b3 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(2),
                &Payload {
                    txns: TransactionHashList::new(vec![3]),
                    header: ExecutionArtifacts::zero(),
                    seq_num: 0,
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
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
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &Payload {
                    txns: TransactionHashList::default(),
                    header: ExecutionArtifacts::zero(),
                    seq_num: 0,
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b1 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::new(vec![1]),
                    header: ExecutionArtifacts::zero(),
                    seq_num: 0,
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
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
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b1 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b2 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(2),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let b3 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(3),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let b4 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

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

        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };

        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b4 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: b4.get_id(),
            round: Round(4),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b5 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(5),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b4.get_id(),
            parent_round: Round(4),
            seq_num: 0,
        };

        let b6 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(6),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v6,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

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

        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b2 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b3 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v3,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b4 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: b4.get_id(),
            round: Round(4),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b5 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(5),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b4.get_id(),
            parent_round: Round(4),
            seq_num: 0,
        };

        let b6 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(6),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v6,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

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
        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };

        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b1 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(3),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(3),
            seq_num: 0,
        };

        let b4 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

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

    #[test]
    fn test_is_valid() {
        let blocktree = BlockTree::<MockSignatures>::new_unrooted(Round(4));
        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b4 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v4,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v5 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b5 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(5),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v5,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        // genesis should not be valid since its round is lower than root
        assert!(!blocktree.is_valid(&g));
        // block 4 should va valid, since the tree is unrooted and is one round 4
        assert!(blocktree.is_valid(&b4));
        // b5 will be valid because its at least (root round) + 1
        assert!(blocktree.is_valid(&b5))
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
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(0),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: 0,
                        },
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b1 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(1),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v1,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(1),
            parent_id: g.get_id(),
            parent_round: Round(0),
            seq_num: 0,
        };

        let b2 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(2),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let b3 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(3),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator {},
        )
        .unwrap();

        let b4 = FullBlock::from_block(
            Block::new::<HasherType>(
                node_id(),
                Round(4),
                &payload,
                &QC::new::<HasherType>(
                    QcInfo {
                        vote: v2,
                        ledger_commit: LedgerCommitInfo::default(),
                    },
                    MockSignatures::with_pubkeys(&[]),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
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
