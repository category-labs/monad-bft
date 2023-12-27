use std::{collections::HashMap, fmt, result::Result as StdResult};

use monad_consensus_types::{
    block::{BlockType, FullBlock},
    payload::TransactionHashList,
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, Round};
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

#[derive(Debug, PartialEq, Eq)]
struct Root {
    round: Round,
    block_id: BlockId,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlockTree<T> {
    /// The round and block_id of last committed block
    root: Root,
    /// Uncommitted blocks
    /// First level of blocks in the tree have block.get_parent_id() == root.block_id
    tree: HashMap<BlockId, FullBlock<T>>,
}

impl<T: SignatureCollection> BlockTree<T> {
    pub fn new(root: QuorumCertificate<T>) -> Self {
        Self {
            root: Root {
                round: root.info.vote.round,
                block_id: root.info.vote.id,
            },
            tree: HashMap::new(),
        }
    }

    /// Prune the block tree and returns the blocks to commit along that branch
    /// in increasing round. After a successful prune, `new_root` is the root of
    /// the block tree. All blocks pruned are deallocated
    ///
    /// Must be called IFF has_path_to_root
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
    pub fn prune(&mut self, new_root: &BlockId) -> Vec<FullBlock<T>> {
        assert!(self.has_path_to_root(new_root));
        let mut commit: Vec<FullBlock<T>> = Vec::new();

        if new_root == &self.root.block_id {
            return commit;
        }

        let new_root_block = self
            .tree
            .remove(new_root)
            .expect("new root must exist in blocktree");
        let mut block_to_commit = new_root_block.clone();

        // traverse up the branch from new_root, removing blocks and pushing to
        // the commit list.
        loop {
            let parent_id = block_to_commit.get_parent_id();
            commit.push(block_to_commit);
            if parent_id == self.root.block_id {
                break;
            }
            block_to_commit = self
                .tree
                .remove(&parent_id)
                .expect("path to root must exist");
        }

        // garbage collect old blocks
        // remove any blocks less than or equal to round `n`
        self.tree
            .retain(|_, b| b.get_parent_round() >= new_root_block.get_round());
        // new root should be set to QC of the block that's the new root
        self.root = Root {
            round: new_root_block.get_round(),
            block_id: new_root_block.get_id(),
        };

        inc_count!(blocktree.prune.success);

        commit.reverse();
        commit
    }

    /// Add a new block to the block tree if it's not in the tree and is higher
    /// than the root block's round number
    pub fn add(&mut self, b: FullBlock<T>) -> Result<()> {
        if !self.is_valid_to_insert(&b) {
            inc_count!(blocktree.add.duplicate);
            return Ok(());
        }

        self.tree.insert(b.get_id(), b);
        inc_count!(blocktree.add.success);
        Ok(())
    }

    /// Find the missing block along the path from `qc` to the tree root.
    /// Returns the QC certifying that block
    pub fn get_missing_ancestor(&self, qc: &QuorumCertificate<T>) -> Option<QuorumCertificate<T>> {
        if self.root.round >= qc.info.vote.round {
            return None;
        }

        let mut maybe_unknown_block_qc = qc;
        let mut maybe_unknown_bid = maybe_unknown_block_qc.info.vote.id;
        while let Some(known_block) = self.tree.get(&maybe_unknown_bid) {
            maybe_unknown_block_qc = &known_block.get_block().qc;
            maybe_unknown_bid = maybe_unknown_block_qc.info.vote.id;
            // If the unknown block's round == self.root, that means we've already committed it
            if maybe_unknown_block_qc.info.vote.round == self.root.round {
                return None;
            }
        }
        Some(maybe_unknown_block_qc.clone())
    }

    pub fn has_path_to_root(&self, b: &BlockId) -> bool {
        if b == &self.root.block_id {
            return true;
        }
        let mut visit = *b;

        while let Some(btb) = self.tree.get(&visit) {
            if btb.get_parent_id() == self.root.block_id {
                return true;
            }
            visit = btb.get_parent_id();
        }
        false
    }

    /// Fetches transactions on the path in [`b`, root)
    pub fn get_txs_on_path_to_root(&self, b: &BlockId) -> Option<Vec<TransactionHashList>> {
        let mut txs = Vec::default();

        if b == &self.root.block_id {
            return Some(txs);
        }

        let mut visit = *b;

        while let Some(btb) = self.tree.get(&visit) {
            if btb.get_parent_id() == self.root.block_id {
                return Some(txs);
            }

            txs.push(btb.get_block().payload.txns.clone());

            visit = btb.get_parent_id();
        }
        None
    }

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &FullBlock<T>) -> bool {
        !self.tree.contains_key(&b.get_id()) && b.get_round() > self.root.round
    }

    pub fn tree(&self) -> &HashMap<BlockId, FullBlock<T>> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }
}

#[cfg(test)]
mod test {
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

    use super::BlockTree;

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
            Block::new(node_id(), Round(1), &payload, &QC::genesis_prime_qc()),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
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
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b3 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b5 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(3),
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
            round: Round(3),
            parent_id: b5.get_parent_id(),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let b6 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(4),
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
        let mut blocktree = BlockTree::<MockSignatures>::new(QuorumCertificate::genesis_prime_qc());
        assert!(blocktree.add(g.clone()).is_ok());

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
        let commit = blocktree.prune(&g.get_parent_id());
        assert_eq!(commit.len(), 0);

        let commit = blocktree.prune(&b5.get_id());
        assert_eq!(
            Vec::from_iter(commit.iter().map(|b| b.get_id())),
            vec![g.get_id(), b3.get_id(), b5.get_id()]
        );
        println!("{:?}", blocktree);
        println!("{:?}", blocktree.tree);

        // Pruned blocktree
        //     b5
        //     |
        //     b6

        // try pruning all other nodes should return err
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
            Block::new(node_id(), Round(1), &payload, &QC::genesis_prime_qc()),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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
            round: Round(2),
            parent_id: g.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
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

        let gid = g.get_id();
        let mut blocktree = BlockTree::new(QC::genesis_prime_qc());
        assert!(blocktree.add(g).is_ok());

        assert!(blocktree.add(b2.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 2);
        assert_eq!(
            blocktree.tree.get(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert!(!blocktree.has_path_to_root(&b2.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 3);
        assert!(blocktree.has_path_to_root(&b1.get_id()));
        assert!(blocktree.has_path_to_root(&b2.get_id()));
        assert_eq!(
            blocktree.tree.get(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert_eq!(
            blocktree.tree.get(&b1.get_id()).unwrap().get_parent_id(),
            gid
        );
    }

    #[test]
    fn equal_level_branching() {
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::empty(),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::genesis_prime_qc(),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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
                Round(2),
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
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b3 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(3),
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
        let mut blocktree = BlockTree::new(QC::genesis_prime_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.add(b3).is_ok());

        assert_eq!(blocktree.size(), 4);

        // prune called on b1, we expect new tree to be
        // b1
        // |
        // b3
        // and the commit blocks should only contain b1 (not b2)
        assert!(blocktree.has_path_to_root(&g.get_id()));
        assert!(blocktree.has_path_to_root(&b1.get_id()));
        let commit = blocktree.prune(&b1.get_id());
        assert_eq!(commit.len(), 2);
        assert_eq!(commit[0].get_id(), g.get_id());
        assert_eq!(commit[1].get_id(), b1.get_id());
        assert_eq!(blocktree.size(), 1);
        assert!(!blocktree.has_path_to_root(&b2.get_id()));
    }

    #[test]
    fn duplicate_blocks() {
        let g = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(1),
                &Payload {
                    txns: TransactionHashList::empty(),
                    header: ExecutionArtifacts::zero(),
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QC::genesis_prime_qc(),
            ),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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

        let mut blocktree = BlockTree::new(QC::genesis_prime_qc());
        assert!(blocktree.add(g).is_ok());
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b1).is_ok());

        assert_eq!(blocktree.tree.len(), 2);
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
            Block::new(node_id(), Round(1), &payload, &QC::genesis_prime_qc()),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
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

        let b3 = FullBlock::from_block(
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

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(5),
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

        let mut blocktree = BlockTree::new(QC::genesis_prime_qc());
        assert!(blocktree.add(g.clone()).is_ok());
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

        blocktree.prune(&b3.get_id());

        assert!(!blocktree.has_path_to_root(&g.get_id()));
        assert!(!blocktree.has_path_to_root(&b1.get_id()));
        assert!(!blocktree.has_path_to_root(&b2.get_id()));
        assert!(!blocktree.has_path_to_root(&b4.get_id()));
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
            Block::new(node_id(), Round(1), &payload, &QC::genesis_prime_qc()),
            FullTransactionList::empty(),
            &MockValidator {},
        )
        .unwrap();

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(2),
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
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = FullBlock::from_block(
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

        let b3 = FullBlock::from_block(
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

        let b4 = FullBlock::from_block(
            Block::new(
                node_id(),
                Round(5),
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

        let mut blocktree = BlockTree::new(QC::genesis_prime_qc());
        assert!(blocktree.add(g.clone()).is_ok());
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

        blocktree.prune(&b1.get_id());
        assert!(blocktree.get_missing_ancestor(&g.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b1.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b2.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b3.get_block().qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b4.get_block().qc).is_none());

        assert_eq!(blocktree.size(), 3);
    }
}
