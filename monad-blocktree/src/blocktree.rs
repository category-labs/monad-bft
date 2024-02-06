use std::{collections::HashMap, fmt, result::Result as StdResult};

use monad_consensus_types::{
    block::{Block, BlockType},
    payload::FullTransactionList,
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
pub struct BlockTree<SCT: SignatureCollection> {
    /// The round and block_id of last committed block
    root: Root,
    /// Uncommitted blocks
    /// First level of blocks in the tree have block.get_parent_id() == root.block_id
    tree: HashMap<BlockId, Block<SCT>>,
}

impl<SCT: SignatureCollection> BlockTree<SCT> {
    pub fn new(root: QuorumCertificate<SCT>) -> Self {
        Self {
            root: Root {
                round: root.info.get_round(),
                block_id: root.get_block_id(),
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
    ///
    /// Returns a list of all blocks to commit and a list of pruned blocks
    pub fn prune(&mut self, new_root: &BlockId) -> (Vec<Block<SCT>>, Vec<Block<SCT>>) {
        assert!(self.has_path_to_root(new_root));
        let mut commit: Vec<Block<SCT>> = Vec::new();

        if new_root == &self.root.block_id {
            return (commit, Vec::new());
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
        let mut blocks_ids_to_remove = Vec::new();
        for (block_id, block) in self.tree.iter() {
            if block.get_parent_round() < new_root_block.get_round() {
                blocks_ids_to_remove.push(*block_id);
            }
        }
        let mut old_blocks = Vec::new();
        for block_id in blocks_ids_to_remove {
            old_blocks.push(self.tree.remove(&block_id).unwrap())
        }

        // new root should be set to QC of the block that's the new root
        self.root = Root {
            round: new_root_block.get_round(),
            block_id: new_root_block.get_id(),
        };

        inc_count!(blocktree.prune.success);

        commit.reverse();
        (commit, old_blocks)
    }

    /// Add a new block to the block tree if it's not in the tree and is higher
    /// than the root block's round number
    pub fn add(&mut self, b: Block<SCT>) -> Result<()> {
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
    pub fn get_missing_ancestor(
        &self,
        qc: &QuorumCertificate<SCT>,
    ) -> Option<QuorumCertificate<SCT>> {
        if self.root.round >= qc.get_round() {
            return None;
        }

        let mut maybe_unknown_block_qc = qc;
        let mut maybe_unknown_bid = maybe_unknown_block_qc.get_block_id();
        while let Some(known_block) = self.tree.get(&maybe_unknown_bid) {
            maybe_unknown_block_qc = &known_block.qc;
            maybe_unknown_bid = maybe_unknown_block_qc.get_block_id();
            // If the unknown block's round == self.root, that means we've already committed it
            if maybe_unknown_block_qc.get_round() == self.root.round {
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
    pub fn get_txs_on_path_to_root(&self, b: &BlockId) -> Option<Vec<FullTransactionList>> {
        let mut txs = Vec::default();

        if b == &self.root.block_id {
            return Some(txs);
        }

        let mut visit = *b;

        while let Some(btb) = self.tree.get(&visit) {
            if btb.get_parent_id() == self.root.block_id {
                return Some(txs);
            }

            txs.push(btb.payload.txns.clone());

            visit = btb.get_parent_id();
        }
        None
    }

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &Block<SCT>) -> bool {
        !self.tree.contains_key(&b.get_id()) && b.get_round() > self.root.round
    }

    pub fn tree(&self) -> &HashMap<BlockId, Block<SCT>> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::{Block as ConsensusBlock, BlockType},
        ledger::CommitResult,
        payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
        quorum_certificate::{QcInfo, QuorumCertificate},
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::Hash,
        NopSignature,
    };
    use monad_eth_types::EthAddress;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{BlockId, NodeId, Round, SeqNum};

    use super::BlockTree;

    type SignatureType = NopSignature;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;
    type Block = ConsensusBlock<MockSignatures<SignatureType>>;
    type QC = QuorumCertificate<MockSignatures<SignatureType>>;

    fn node_id() -> NodeId<PubKeyType> {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair =
            <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
                &mut privkey,
            )
            .unwrap();
        NodeId::new(keypair.pubkey())
    }

    #[test]
    fn test_prune() {
        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v1,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b3 = Block::new(
            node_id(),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v3,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b4 = Block::new(
            node_id(),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v4,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v5 = VoteInfo {
            id: b3.get_id(),
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b5 = Block::new(
            node_id(),
            Round(3),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v5,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(3),
            parent_id: b5.get_parent_id(),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let b6 = Block::new(
            node_id(),
            Round(4),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v6,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v7 = VoteInfo {
            id: b6.get_id(),
            round: Round(6),
            parent_id: b5.get_id(),
            parent_round: Round(5),
            seq_num: SeqNum(0),
        };

        let b7 = Block::new(
            node_id(),
            Round(7),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v7,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
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
        let mut blocktree = BlockTree::<MockSignatures<_>>::new(QuorumCertificate::genesis_qc());
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
        let (commit, _pruned) = blocktree.prune(&g.get_parent_id());
        assert_eq!(commit.len(), 0);

        let (commit, _pruned) = blocktree.prune(&b5.get_id());
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

        let b8 = Block::new(
            node_id(),
            Round(8),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v8,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        assert!(blocktree.add(b8).is_ok());
        println!("{:?}", blocktree);
    }

    #[test]
    fn test_add_parent_not_exist() {
        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v1,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: g.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Round(3),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let gid = g.get_id();
        let mut blocktree = BlockTree::new(QC::genesis_qc());
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
        let g = Block::new(
            node_id(),
            Round(1),
            &Payload {
                txns: FullTransactionList::empty(),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Round(2),
            &Payload {
                txns: FullTransactionList::new(vec![1].into()),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v1,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b2 = Block::new(
            node_id(),
            Round(2),
            &Payload {
                txns: FullTransactionList::new(vec![2].into()),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v1,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b3 = Block::new(
            node_id(),
            Round(3),
            &Payload {
                txns: FullTransactionList::new(vec![3].into()),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
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
        let mut blocktree = BlockTree::new(QC::genesis_qc());
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
        let (commit, _pruned) = blocktree.prune(&b1.get_id());
        assert_eq!(commit.len(), 2);
        assert_eq!(commit[0].get_id(), g.get_id());
        assert_eq!(commit[1].get_id(), b1.get_id());
        assert_eq!(blocktree.size(), 1);
        assert!(!blocktree.has_path_to_root(&b2.get_id()));
    }

    #[test]
    fn duplicate_blocks() {
        let g = Block::new(
            node_id(),
            Round(1),
            &Payload {
                txns: FullTransactionList::empty(),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Round(2),
            &Payload {
                txns: FullTransactionList::new(vec![1].into()),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v1,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut blocktree = BlockTree::new(QC::genesis_qc());
        assert!(blocktree.add(g).is_ok());
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b1).is_ok());

        assert_eq!(blocktree.tree.len(), 2);
    }

    #[test]
    fn paths_to_root() {
        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v1,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Round(3),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b3 = Block::new(
            node_id(),
            Round(4),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b4 = Block::new(
            node_id(),
            Round(5),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut blocktree = BlockTree::new(QC::genesis_qc());
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
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v1,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Round(3),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b3 = Block::new(
            node_id(),
            Round(4),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let b4 = Block::new(
            node_id(),
            Round(5),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v2,
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut blocktree = BlockTree::new(QC::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b4.qc).unwrap() == b4.qc);

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b1.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b2.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b3.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b4.qc).is_none());

        blocktree.prune(&b1.get_id());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b1.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b2.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b3.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b4.qc).is_none());

        assert_eq!(blocktree.size(), 3);
    }
}
