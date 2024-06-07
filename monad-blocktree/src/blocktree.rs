use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    result::Result as StdResult,
};

use monad_consensus_types::{
    block::{BlockPolicy, BlockType},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, Round, SeqNum};
use tracing::trace;

type Result<T> = StdResult<T, BlockTreeError>;

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum BlockTreeError {
    BlockNotExist(BlockId),
    HashPolicy((BlockId, String)),
}

impl fmt::Display for BlockTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockNotExist(bid) => write!(f, "Block not exist: {:?}", bid),
            Self::HashPolicy((bid, description)) => {
                write!(f, "HashPolicy decoding error {:?}: {:?}", bid, description)
            }
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
    seq_num: SeqNum,
    block_id: BlockId,
    children_blocks: Vec<BlockId>,
}

pub struct BlockTreeEntry<SCT: SignatureCollection, BP: BlockPolicy<SCT>> {
    pub validated_block: BP::ValidatedBlock,
    /// A blocktree entry is coherent if there is a path to root from the entry and it
    /// is a valid extension of the chain
    pub is_coherent: bool,
    /// A vector of all the block ids that extend this validated block in the blocktree
    pub children_blocks: Vec<BlockId>,
}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> Clone for BlockTreeEntry<SCT, BP> {
    fn clone(&self) -> Self {
        Self {
            validated_block: self.validated_block.clone(),
            is_coherent: self.is_coherent,
            children_blocks: self.children_blocks.clone(),
        }
    }
}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> fmt::Debug for BlockTreeEntry<SCT, BP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockTreeEntry")
            .field("validated_block", &self.validated_block)
            .field("is_coherent", &self.is_coherent)
            .field("children_blocks", &self.children_blocks)
            .finish()
    }
}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> PartialEq<Self> for BlockTreeEntry<SCT, BP> {
    fn eq(&self, other: &Self) -> bool {
        self.validated_block == other.validated_block
            && self.is_coherent == other.is_coherent
            && self.children_blocks == other.children_blocks
    }
}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> Eq for BlockTreeEntry<SCT, BP> {}

type Tree<T> = HashMap<BlockId, T>;

pub struct BlockTree<SCT: SignatureCollection, BP: BlockPolicy<SCT>> {
    /// The round and block_id of last committed block
    root: Root,
    /// Uncommitted blocks
    /// First level of blocks in the tree have block.get_parent_id() == root.block_id
    tree: Tree<BlockTreeEntry<SCT, BP>>,
}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> PartialEq<Self> for BlockTree<SCT, BP> {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.tree == other.tree
    }
}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> Eq for BlockTree<SCT, BP> {}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> fmt::Debug for BlockTree<SCT, BP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockTree")
            .field("root", &self.root)
            .field("tree", &self.tree)
            .finish_non_exhaustive()
    }
}

impl<SCT: SignatureCollection, BP: BlockPolicy<SCT>> BlockTree<SCT, BP> {
    pub fn new(root: QuorumCertificate<SCT>) -> Self {
        Self {
            root: Root {
                round: root.info.get_round(),
                seq_num: root.get_seq_num(),
                block_id: root.get_block_id(),
                children_blocks: Vec::new(),
            },
            tree: HashMap::new(),
        }
    }

    /// Prune the block tree and returns the blocks to commit along that branch
    /// in increasing round. After a successful prune, `new_root` is the root of
    /// the block tree. All blocks pruned are deallocated
    ///
    /// Must be called IFF is_coherent
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
    pub fn prune(&mut self, new_root: &BlockId) -> Vec<BP::ValidatedBlock> {
        assert!(self.is_coherent(new_root));
        let mut commit: Vec<BP::ValidatedBlock> = Vec::new();

        if new_root == &self.root.block_id {
            return commit;
        }

        let new_root_entry = self
            .tree
            .remove(new_root)
            .expect("new root must exist in blocktree");
        let mut entry_to_commit = new_root_entry.clone();

        // traverse up the branch from new_root, removing blocks and pushing to
        // the commit list.
        loop {
            assert!(entry_to_commit.is_coherent, "must be coherent");

            let validated_block = entry_to_commit.validated_block;

            let parent_id = validated_block.get_parent_id();

            commit.push(validated_block);

            if parent_id == self.root.block_id {
                break;
            }
            entry_to_commit = self
                .tree
                .remove(&parent_id)
                .expect("path to root must exist")
        }

        // garbage collect old blocks
        // remove any blocks less than or equal to round `n`
        self.tree.retain(|_, b| {
            b.validated_block.get_parent_round() >= new_root_entry.validated_block.get_round()
        });
        // new root should be set to QC of the block that's the new root
        self.root = Root {
            round: new_root_entry.validated_block.get_round(),
            seq_num: new_root_entry.validated_block.get_seq_num(),
            block_id: new_root_entry.validated_block.get_id(),
            children_blocks: new_root_entry.children_blocks,
        };

        inc_count!(blocktree.prune.success);

        commit.reverse();
        commit
    }

    /// Add a new block to the block tree if it's not in the tree and is higher
    /// than the root block's round number
    pub fn add(&mut self, b: BP::ValidatedBlock) -> Result<()> {
        if !self.is_valid_to_insert(&b) {
            inc_count!(blocktree.add.duplicate);
            return Ok(());
        }

        let new_block_id = b.get_id();
        let parent_id = b.get_parent_id();
        let is_coherent = false;

        // Get all the children blocks in the blocktree
        let mut children_blocks = Vec::new();
        for (block_id, blocktree_entry) in self.tree.iter() {
            if blocktree_entry.validated_block.get_parent_id() == new_block_id {
                children_blocks.push(*block_id);
            }
        }

        // Create the new blocktree entry
        let new_block_entry = BlockTreeEntry {
            validated_block: b,
            is_coherent,
            children_blocks,
        };

        self.tree.insert(new_block_id, new_block_entry);

        // Retrieve the parent block's coherency and children blocks
        let (is_parent_coherent, children_blocks_to_update) = if parent_id == self.root.block_id {
            (true, Some(&mut self.root.children_blocks))
        } else if let Some(parent_entry) = self.tree.get_mut(&parent_id) {
            (
                parent_entry.is_coherent,
                Some(&mut parent_entry.children_blocks),
            )
        } else {
            // Parent missing, should be requested. Skip coherency check
            (false, None)
        };

        // Push the new block to the children blocks in the parent
        if let Some(children_blocks_to_update) = children_blocks_to_update {
            children_blocks_to_update.push(new_block_id);
        }

        // Update coherency if parent is coherent
        if is_parent_coherent {
            self.update_coherency(new_block_id);
        }

        inc_count!(blocktree.add.success);

        Ok(())
    }

    pub fn update_coherency(&mut self, block_id: BlockId) {
        let mut block_ids_to_update: VecDeque<BlockId> = vec![block_id].into();

        while !block_ids_to_update.is_empty() {
            // Next block to check coherency
            let next_block = block_ids_to_update.pop_front().unwrap();

            self.tree
                .entry(next_block)
                .and_modify(|entry| entry.is_coherent = true);

            block_ids_to_update.extend(
                self.tree
                    .get(&next_block)
                    .expect("should be in tree")
                    .children_blocks
                    .iter()
                    .cloned(),
            );
        }

        for block_id in block_ids_to_update {
            self.update_coherency(block_id);
        }
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
        while let Some(known_block_entry) = self.tree.get(&maybe_unknown_bid) {
            maybe_unknown_block_qc = known_block_entry.validated_block.get_qc();
            maybe_unknown_bid = known_block_entry.validated_block.get_parent_id();
            // If the unknown block's round == self.root, that means we've already committed it
            if maybe_unknown_block_qc.get_round() == self.root.round {
                return None;
            }
        }
        Some(maybe_unknown_block_qc.clone())
    }

    pub fn is_coherent(&self, b: &BlockId) -> bool {
        if b == &self.root.block_id {
            return true;
        }

        if let Some(blocktree_entry) = self.tree.get(b) {
            return blocktree_entry.is_coherent;
        }

        false
    }

    /// Fetches transactions on the path in [`b`, root)
    pub fn get_tx_hashes_on_path_to_root(
        &self,
        b: &BlockId,
    ) -> Option<HashSet<<BP::ValidatedBlock as BlockType<SCT>>::TxnHash>> {
        let mut txs = HashSet::new();
        if b == &self.root.block_id {
            return Some(txs);
        }

        let mut visit = *b;

        while let Some(blocktree_entry) = self.tree.get(&visit) {
            let btb = &blocktree_entry.validated_block;
            if btb.get_parent_id() == self.root.block_id {
                return Some(txs);
            }

            txs.extend(btb.get_txn_hashes());

            visit = btb.get_parent_id();
        }
        None
    }

    /// Remove all blocks which are older than the given sequence number
    pub fn remove_old_blocks(&mut self, seq_num: SeqNum) {
        self.tree
            .retain(|_, b| b.validated_block.get_seq_num() >= seq_num);
    }

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &BP::ValidatedBlock) -> bool {
        !self.tree.contains_key(&b.get_id()) && b.get_round() > self.root.round
    }

    pub fn tree(&self) -> &Tree<BlockTreeEntry<SCT, BP>> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }

    pub fn get_root_seq_num(&self) -> SeqNum {
        self.root.seq_num
    }

    pub fn get_block(&self, block_id: &BlockId) -> Option<&BP::ValidatedBlock> {
        self.tree.get(block_id).map(|block| &block.validated_block)
    }

    pub fn get_entry(&self, block_id: &BlockId) -> Option<&BlockTreeEntry<SCT, BP>> {
        self.tree.get(block_id)
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::{Block as ConsensusBlock, BlockType, PassthruBlockPolicy},
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
    use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};

    use super::BlockTree;

    type SignatureType = NopSignature;
    type BlockPolicyType = PassthruBlockPolicy;
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
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b3 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b4 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b5 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(3),
            parent_id: b5.get_parent_id(),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let b6 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(6),
            parent_id: b5.get_id(),
            parent_round: Round(5),
            seq_num: SeqNum(0),
        };

        let b7 = Block::new(
            node_id(),
            Epoch(1),
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
        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(blocktree.add(b5.clone()).is_ok());
        assert!(blocktree.add(b6.clone()).is_ok());
        println!("{:?}", blocktree);

        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert!(blocktree.is_coherent(&b3.get_id()));
        assert!(blocktree.is_coherent(&b4.get_id()));
        assert!(blocktree.is_coherent(&b5.get_id()));
        assert!(blocktree.is_coherent(&b6.get_id()));

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
        assert!(!blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));

        // Pruned blocktree after insertion
        //     b5
        //   /    \
        //  b6    b8
        //  |
        //  b7

        assert!(blocktree.add(b7).is_ok());

        let v8 = VoteInfo {
            id: b5.get_id(),
            epoch: Epoch(1),
            round: Round(5),
            parent_id: b3.get_id(),
            parent_round: Round(3),
            seq_num: SeqNum(0),
        };

        let b8 = Block::new(
            node_id(),
            Epoch(1),
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
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: g.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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
        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g).is_ok());

        assert!(blocktree.add(b2.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 2);
        assert_eq!(
            blocktree.get_block(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert!(!blocktree.is_coherent(&b2.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert_eq!(blocktree.tree.len(), 3);
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert_eq!(
            blocktree.get_block(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert_eq!(
            blocktree.get_block(&b1.get_id()).unwrap().get_parent_id(),
            gid
        );
    }

    #[test]
    fn equal_level_branching() {
        let g = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b3 = Block::new(
            node_id(),
            Epoch(1),
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
        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
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
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        let commit = blocktree.prune(&b1.get_id());
        assert_eq!(commit.len(), 2);
        assert_eq!(commit[0].get_id(), g.get_id());
        assert_eq!(commit[1].get_id(), b1.get_id());
        assert_eq!(blocktree.size(), 1);
        assert!(!blocktree.is_coherent(&b2.get_id()));
    }

    #[test]
    fn duplicate_blocks() {
        let g = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
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
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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
            Epoch(1),
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
            Epoch(1),
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

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(!blocktree.is_coherent(&b2.get_id()));

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(!blocktree.is_coherent(&b3.get_id()));

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(!blocktree.is_coherent(&b4.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert!(blocktree.is_coherent(&b3.get_id()));
        assert!(blocktree.is_coherent(&b4.get_id()));

        blocktree.prune(&b3.get_id());

        assert!(!blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b4.get_id()));
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
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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
            Epoch(1),
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
            Epoch(1),
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

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
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

    #[test]
    fn test_update_coherency_one_block() {
        // Initial blocktree
        //  g
        //  |
        // ...
        //  |
        //  b2
        //
        // blocktree is updated with b1

        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        // root must be coherent but b2 isn't
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).is_none());

        // all blocks must be coherent
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
    }

    #[test]
    fn test_update_coherency_two_blocks_scenario_one() {
        // Initial blocktree
        //  g
        //  |
        // ...
        //  |
        //  b3
        //
        // blocktree is updated with b2 followed by b1

        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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

        let v3 = VoteInfo {
            id: b2.get_id(),
            epoch: Epoch(1),
            round: Round(3),
            parent_id: b2.get_parent_id(),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let b3 = Block::new(
            node_id(),
            Epoch(1),
            Round(4),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v3,
                        ledger_commit_info: CommitResult::Commit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root must be coherent but b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b2.qc);
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        // root must be coherent but b3 and b2 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        // add block 1
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b2.qc).is_none());

        // all blocks must be coherent
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert!(blocktree.is_coherent(&b3.get_id()));
    }

    #[test]
    fn test_update_coherency_two_blocks_scenario_two() {
        // Initial blocktree
        //  g
        //  |
        // ...
        //  |
        //  b3
        //
        // blocktree is updated with b1 followed by b2

        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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

        let v3 = VoteInfo {
            id: b2.get_id(),
            epoch: Epoch(1),
            round: Round(3),
            parent_id: b2.get_parent_id(),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let b3 = Block::new(
            node_id(),
            Epoch(1),
            Round(4),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: v3,
                        ledger_commit_info: CommitResult::Commit,
                    },
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root must be coherent but b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 1
        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root and block 1 must be coherent but b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b2.qc).is_none());

        // all blocks must be coherent
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert!(blocktree.is_coherent(&b3.get_id()));
    }

    #[test]
    fn test_update_coherency_multiple_children() {
        // Initial blocktree
        //      g
        //      |
        //  ___...___
        //  |       |
        //  b2      b3
        //
        // blocktree is updated with b1

        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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
            Epoch(1),
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

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root must be coherent but b2 and b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b3.qc).is_none());

        // all blocks must be coherent
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert!(blocktree.is_coherent(&b3.get_id()));
    }

    #[test]
    fn test_update_coherency_multiple_grandchildren() {
        // Initial blocktree
        //      g
        //      |
        //  ___...____
        //  |        |
        //  b2    __ b3 __
        //  |     |      |
        //  b4    b5     b6
        //
        // blocktree is updated with missing b1

        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let g = Block::new(node_id(), Epoch(1), Round(1), &payload, &QC::genesis_qc());

        let v1 = VoteInfo {
            id: g.get_id(),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let b1 = Block::new(
            node_id(),
            Epoch(1),
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
            epoch: Epoch(1),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            seq_num: SeqNum(0),
        };

        let b2 = Block::new(
            node_id(),
            Epoch(1),
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

        let v3 = VoteInfo {
            id: b2.get_id(),
            epoch: Epoch(1),
            round: Round(3),
            parent_id: b2.get_parent_id(),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let b3 = Block::new(
            node_id(),
            Epoch(1),
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

        let v4 = VoteInfo {
            id: b3.get_id(),
            epoch: Epoch(1),
            round: Round(4),
            parent_id: b3.get_parent_id(),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let b4 = Block::new(
            node_id(),
            Epoch(1),
            Round(5),
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

        let b5 = Block::new(
            node_id(),
            Epoch(1),
            Round(6),
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

        let b6 = Block::new(
            node_id(),
            Epoch(1),
            Round(7),
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

        let mut blocktree =
            BlockTree::<MockSignatures<_>, BlockPolicyType>::new(QuorumCertificate::genesis_qc());
        assert!(blocktree.add(g.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree.add(b2.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        assert!(blocktree.add(b3.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        assert!(blocktree.add(b4.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b4.qc).unwrap() == b2.qc);

        assert!(blocktree.add(b5.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b5.qc).unwrap() == b3.qc);

        assert!(blocktree.add(b6.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b6.qc).unwrap() == b3.qc);

        // root must be coherent but rest of the blocks should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b4.get_id()));
        assert!(!blocktree.is_coherent(&b5.get_id()));
        assert!(!blocktree.is_coherent(&b6.get_id()));

        assert!(blocktree.add(b1.clone()).is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b3.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b4.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b5.qc).is_none());
        assert!(blocktree.get_missing_ancestor(&b6.qc).is_none());

        // all blocks must be coherent
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert!(blocktree.is_coherent(&b3.get_id()));
        assert!(blocktree.is_coherent(&b4.get_id()));
        assert!(blocktree.is_coherent(&b5.get_id()));
        assert!(blocktree.is_coherent(&b6.get_id()));
    }
}
