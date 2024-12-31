use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
    result::Result as StdResult,
};

use monad_consensus_types::{
    block::{BlockPolicy, BlockRange, ConsensusBlockHeader, ExecutionProtocol},
    checkpoint::RootInfo,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, Round, SeqNum};

use crate::tree::{BlockTreeEntry, Tree};

type Result<T> = StdResult<T, BlockTreeError>;

#[derive(Debug, PartialEq)]
pub enum BlockTreeError {
    BlockNotCoherent(BlockId),
    StateBackendError(StateBackendError),
}

impl fmt::Display for BlockTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockNotCoherent(bid) => write!(f, "Block not coherent {:?}", bid),
            Self::StateBackendError(err) => write!(f, "StateBackend error: {:?}", err),
        }
    }
}

impl std::error::Error for BlockTreeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Root {
    info: RootInfo,
    children_blocks: Vec<BlockId>,
}

pub struct BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    /// The round and block_id of last committed block
    root: Root,
    /// Uncommitted blocks
    /// First level of blocks in the tree have block.get_parent_id() == root.block_id
    tree: Tree<ST, SCT, EPT, BPT, SBT>,

    // TODO delete this once tree is keyed by blocktree round
    inserted_rounds: BTreeSet<Round>,
}

impl<ST, SCT, EPT, BPT, SBT> fmt::Debug for BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockTree")
            .field("root", &self.root)
            .field("tree", &self.tree)
            .finish_non_exhaustive()
    }
}

impl<ST, SCT, EPT, BPT, SBT> PartialEq<Self> for BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.tree == other.tree
    }
}

impl<ST, SCT, EPT, BPT, SBT> Eq for BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
}

impl<ST, SCT, EPT, BPT, SBT> BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    pub fn new(root: RootInfo) -> Self {
        Self {
            root: Root {
                info: root,
                children_blocks: Vec::new(),
            },
            tree: Default::default(),

            inserted_rounds: Default::default(),
        }
    }

    pub fn root(&self) -> &RootInfo {
        &self.root.info
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
    pub fn prune(&mut self, new_root: &BlockId) -> Vec<BPT::ValidatedBlock> {
        assert!(self.is_coherent(new_root));
        let mut commit: Vec<BPT::ValidatedBlock> = Vec::new();

        if new_root == &self.root.info.block_id {
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

            if parent_id == self.root.info.block_id {
                break;
            }
            entry_to_commit = self
                .tree
                .remove(&parent_id)
                .expect("path to root must exist")
        }

        // garbage collect old blocks
        // remove any blocks less than or equal to round `n`
        let blocks_to_delete: Vec<_> = self
            .tree
            .iter()
            .filter_map(|(block_id, block)| {
                if block.validated_block.get_parent_round()
                    < new_root_entry.validated_block.get_round()
                {
                    Some(block_id)
                } else {
                    None
                }
            })
            .copied()
            .collect();
        for block_to_delete in blocks_to_delete {
            self.tree.remove(&block_to_delete);
        }
        while self.inserted_rounds.first().is_some_and(|inserted_round| {
            inserted_round <= &new_root_entry.validated_block.get_round()
        }) {
            self.inserted_rounds.pop_first();
        }
        self.root = Root {
            info: RootInfo {
                round: new_root_entry.validated_block.get_round(),
                seq_num: new_root_entry.validated_block.get_seq_num(),
                epoch: new_root_entry.validated_block.get_epoch(),
                block_id: new_root_entry.validated_block.get_id(),
            },
            children_blocks: new_root_entry.children_blocks,
        };

        commit.reverse();
        commit
    }

    /// Add a new block to the block tree if it's not in the tree and is higher
    /// than the root block's round number
    pub fn add(&mut self, block: BPT::ValidatedBlock) {
        if !self.is_valid_to_insert(block.header()) {
            return;
        }

        let new_block_id = block.get_id();
        let parent_id = block.get_parent_id();

        self.inserted_rounds.insert(block.get_round());
        self.tree.insert(block);

        if parent_id == self.root.info.block_id {
            self.root.children_blocks.push(new_block_id);
        }
    }

    pub fn try_update_coherency(
        &mut self,
        block_id: BlockId,
        block_policy: &mut BPT,
        state_backend: &SBT,
    ) -> Vec<BPT::ValidatedBlock> {
        let Some(path_from_root) = self.get_blocks_on_path_from_root(&block_id) else {
            return Vec::new();
        };
        let Some(incoherent_parent_or_self) = path_from_root.iter().find(|block| {
            !self
                .tree
                .get(&block.get_id())
                .expect("block doesn't exist")
                .is_coherent
        }) else {
            // no incoherent_parent_or_self, already is coherent
            return Vec::new();
        };

        let mut block_ids_to_update: VecDeque<BlockId> =
            vec![incoherent_parent_or_self.get_id()].into();

        let mut retval = vec![];
        while !block_ids_to_update.is_empty() {
            // Next block to check coherency
            let next_block_id = block_ids_to_update.pop_front().unwrap();
            let mut extending_blocks = self
                .get_blocks_on_path_from_root(&next_block_id)
                .expect("path to root must exist");
            // Remove the block itself
            let next_block = extending_blocks
                .pop()
                .expect("next_block is included in path_from_root");

            // extending blocks are always coherent, because we only call
            // update_coherency on the first incoherent block in the chain
            if block_policy
                .check_coherency(next_block, extending_blocks, self.root.info, state_backend)
                .is_ok()
            {
                let next_block = next_block.clone();
                self.tree
                    .set_coherent(&next_block_id, true)
                    .expect("should be in tree");

                retval.push(next_block);

                // Can check coherency of children blocks now
                block_ids_to_update.extend(
                    self.tree
                        .get(&next_block_id)
                        .expect("should be in tree")
                        .children_blocks
                        .iter()
                        .cloned(),
                );
            }
        }
        retval
    }

    /// Iterate the block tree and return highest QC that have path to block tree
    /// root and is committable, if exists
    pub fn get_high_committable_qc(&self) -> Option<QuorumCertificate<SCT>> {
        let mut high_commit_qc: Option<QuorumCertificate<SCT>> = None;
        let mut iter: VecDeque<BlockId> = self.root.children_blocks.clone().into();
        while let Some(bid) = iter.pop_front() {
            let block = self.tree.get(&bid).expect("block in tree");
            let qc = block.validated_block.get_qc();

            if qc.is_commitable()
                && self.is_coherent(&qc.info.parent_id)
                && high_commit_qc
                    .as_ref()
                    .map(|high_commit_qc| high_commit_qc.get_round() < qc.get_round())
                    .unwrap_or(true)
            {
                high_commit_qc = Some(qc.clone());
            }

            iter.extend(block.children_blocks.iter().cloned())
        }
        high_commit_qc
    }

    /// returns a BlockRange that should be requested to fill the path from `qc` to the tree root.
    pub fn maybe_fill_path_to_root(&self, qc: &QuorumCertificate<SCT>) -> Option<BlockRange> {
        if self.root.info.round >= qc.get_round() {
            // root cannot be an ancestor of qc
            return None;
        }

        let mut maybe_unknown_bid = qc.get_block_id();
        let mut maybe_unknown_seq_num = None;
        while let Some(known_block_entry) = self.tree.get(&maybe_unknown_bid) {
            // If the parent round == self.root, we have path to root
            if known_block_entry.validated_block.get_parent_round() == self.root.info.round {
                assert_eq!(
                    known_block_entry.validated_block.get_parent_id(),
                    self.root.info.block_id
                );
                return None;
            }
            maybe_unknown_bid = known_block_entry.validated_block.get_parent_id();
            // this can never underflow, because it must be at least 1 greater than GENESIS_SEQ_NUM
            let unknown_bid_seq_num = known_block_entry.validated_block.get_seq_num() - SeqNum(1);
            assert!(unknown_bid_seq_num > self.root.info.seq_num);
            maybe_unknown_seq_num = Some(unknown_bid_seq_num);
        }

        Some(BlockRange {
            last_block_id: maybe_unknown_bid,
            num_blocks: match maybe_unknown_seq_num {
                // request all blocks up to root
                Some(last_block_seq_num) => {
                    assert!(last_block_seq_num > self.root.info.seq_num);
                    // if last_block_seq_num=1 and root=0, then we need to request 1 block
                    last_block_seq_num - self.root.info.seq_num
                }
                // request only the missing block, because we don't know its sequence number
                None => SeqNum(1),
            },
        })
    }

    pub fn is_coherent(&self, b: &BlockId) -> bool {
        if b == &self.root.info.block_id {
            return true;
        }

        if let Some(blocktree_entry) = self.tree.get(b) {
            return blocktree_entry.is_coherent;
        }

        false
    }

    /// Fetches blocks on path from root
    pub fn get_blocks_on_path_from_root(&self, b: &BlockId) -> Option<Vec<&BPT::ValidatedBlock>> {
        let mut blocks = Vec::new();
        if b == &self.root.info.block_id {
            return Some(blocks);
        }

        let mut visit = *b;

        while let Some(blocktree_entry) = self.tree.get(&visit) {
            let btb = &blocktree_entry.validated_block;
            blocks.push(btb);

            if btb.get_parent_id() == self.root.info.block_id {
                blocks.reverse();
                return Some(blocks);
            }

            visit = btb.get_parent_id();
        }

        None
    }

    // Take a QC and look for the block it certifies in the blocktree. If it exists, return its
    // seq_num
    pub fn get_seq_num_of_qc(&self, qc: &QuorumCertificate<SCT>) -> Option<SeqNum> {
        let block_id = qc.get_block_id();

        if self.root.info.block_id == block_id {
            return Some(self.get_root_seq_num());
        }

        let Some(certified_block) = self.tree.get(&block_id) else {
            return None;
        };

        Some(certified_block.validated_block.get_seq_num())
    }

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &ConsensusBlockHeader<ST, SCT, EPT>) -> bool {
        !self.tree.contains_key(&b.get_id()) && b.round > self.root.info.round
    }

    pub fn tree(&self) -> &Tree<ST, SCT, EPT, BPT, SBT> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }

    pub fn get_root_seq_num(&self) -> SeqNum {
        self.root.info.seq_num
    }

    /// Note that this returns None if the block_id is root!
    pub fn get_block(&self, block_id: &BlockId) -> Option<&BPT::ValidatedBlock> {
        self.tree.get(block_id).map(|block| &block.validated_block)
    }

    pub fn get_payload(
        &self,
        block_body_id: &ConsensusBlockBodyId,
    ) -> Option<ConsensusBlockBody<EPT>> {
        self.tree.get_payload(block_body_id).cloned()
    }

    pub fn get_entry(&self, block_id: &BlockId) -> Option<&BlockTreeEntry<ST, SCT, EPT, BPT, SBT>> {
        self.tree.get(block_id)
    }

    /// Notably does NOT need to be a chain to root
    /// chain is returned in order of lowest round to highest
    pub fn get_parent_block_chain(&self, block_id: &BlockId) -> Vec<&BPT::ValidatedBlock> {
        let Some(base_block) = self.tree.get(block_id) else {
            return Default::default();
        };

        let mut chain = vec![&base_block.validated_block];
        while let Some(parent) = self.tree.get(&chain.last().unwrap().get_parent_id()) {
            chain.push(&parent.validated_block);
        }
        chain.reverse();

        chain
    }

    pub fn round_exists(&self, round: &Round) -> bool {
        self.inserted_rounds.contains(round)
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::{Block as ConsensusBlock, BlockType, FullBlock, PassthruBlockPolicy},
        ledger::CommitResult,
        payload::{ExecutionProtocol, FullTransactionList, Payload},
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
    use monad_state_backend::{InMemoryState, InMemoryStateInner};
    use monad_testutil::signing::MockSignatures;
    use monad_types::{BlockId, DontCare, Epoch, MonadVersion, NodeId, Round, SeqNum};

    use super::BlockTree;
    use crate::blocktree::RootInfo;

    type SignatureType = NopSignature;
    type StateBackendType = InMemoryState;
    type BlockPolicyType = PassthruBlockPolicy;
    type BlockTreeType =
        BlockTree<MockSignatures<SignatureType>, BlockPolicyType, StateBackendType>;
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

    fn get_vote(block: &Block) -> Vote {
        Vote {
            id: block.get_id(),
            epoch: block.get_epoch(),
            round: block.get_round(),
            parent_id: block.get_parent_id(),
            parent_round: block.get_parent_round(),
            seq_num: block.get_seq_num(),
            timestamp: 0,
            version: MonadVersion::version(),
        }
    }

    pub fn mock_qc(vote_info: Vote) -> QC {
        QC::new(vote_info, MockSignatures::with_pubkeys(&[]))
    }

    pub fn mock_qc_for_block(block: &Block) -> QC {
        let vote = Vote {
            id: block.get_id(),
            epoch: block.get_epoch(),
            round: block.get_round(),
            parent_id: block.get_parent_id(),
            parent_round: block.get_parent_round(),
            seq_num: block.get_seq_num(),
            timestamp: block.get_timestamp(),
            version: MonadVersion::version(),
        };
        QC::new(vote, MockSignatures::with_pubkeys(&[]))
    }

    fn full_block_new(b: &Block, p: &Payload) -> FullBlock<MockSignatures<SignatureType>> {
        FullBlock {
            block: b.clone(),
            payload: p.clone(),
        }
    }

    #[test]
    fn test_prune() {
        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            ..DontCare::dont_care()
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let v3 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v3),
        );

        let v4 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v4),
        );

        let v5 = VoteInfo {
            id: b3.get_id(),
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b5 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v5),
        );

        let v6 = VoteInfo {
            id: b5.get_id(),
            round: Round(3),
            parent_id: b5.get_parent_id(),
            parent_round: Round(2),
            ..DontCare::dont_care()
        };
        let b6 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc(v6),
        );

        let v7 = VoteInfo {
            id: b6.get_id(),
            round: Round(6),
            parent_id: b5.get_id(),
            parent_round: Round(5),
            ..DontCare::dont_care()
        };
        let b7 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(7),
            &execution,
            payload.get_id(),
            &mock_qc(v7),
        );

        // Initial blocktree
        //        g
        //   /    |     \
        //  b1    b3    b4
        //  |     |
        //  b2    b5
        //        |
        //        b6
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b4, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b5, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b6, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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

        assert!(blocktree
            .add(
                full_block_new(&b7, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());

        let v8 = VoteInfo {
            id: b5.get_id(),
            round: Round(5),
            parent_id: b3.get_id(),
            parent_round: Round(3),
            ..DontCare::dont_care()
        };

        let b8 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(8),
            &execution,
            payload.get_id(),
            &mock_qc(v8),
        );

        assert!(blocktree
            .add(
                full_block_new(&b8, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        println!("{:?}", blocktree);
    }

    #[test]
    fn test_add_parent_not_exist() {
        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };

        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: g.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };

        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let gid = g.get_id();
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());

        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert_eq!(blocktree.tree.len(), 2);
        assert_eq!(
            blocktree.get_block(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert!(!blocktree.is_coherent(&b2.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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
        let execution = ExecutionProtocol::dont_care();
        let p0 = Payload::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            p0.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };

        let p1 = Payload {
            txns: FullTransactionList::new(vec![1].into()),
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            p1.get_id(),
            &mock_qc(v1),
        );

        let p2 = Payload {
            txns: FullTransactionList::new(vec![2].into()),
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            p2.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: g.get_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };

        let p3 = Payload {
            txns: FullTransactionList::new(vec![3].into()),
        };
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            p3.get_id(),
            &mock_qc(v2),
        );

        // Initial blocktree
        //        g
        //   /    |
        //  b1    b2
        //  |
        //  b3
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(full_block_new(&g, &p0), &mut block_policy, &state_backend)
            .is_ok());
        assert!(blocktree
            .add(full_block_new(&b1, &p1), &mut block_policy, &state_backend)
            .is_ok());
        assert!(blocktree
            .add(full_block_new(&b2, &p2), &mut block_policy, &state_backend)
            .is_ok());
        assert!(blocktree
            .add(full_block_new(&b3, &p3), &mut block_policy, &state_backend)
            .is_ok());

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
        let execution = ExecutionProtocol::dont_care();
        let p0 = Payload::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            p0.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };

        let p1 = Payload {
            txns: FullTransactionList::new(vec![1].into()),
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            p1.get_id(),
            &mock_qc(v1),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(full_block_new(&g, &p0), &mut block_policy, &state_backend)
            .is_ok());
        assert!(blocktree
            .add(full_block_new(&b1, &p1), &mut block_policy, &state_backend)
            .is_ok());
        assert!(blocktree
            .add(full_block_new(&b1, &p1), &mut block_policy, &state_backend)
            .is_ok());
        assert!(blocktree
            .add(full_block_new(&b1, &p1), &mut block_policy, &state_backend)
            .is_ok());

        assert_eq!(blocktree.tree.len(), 2);
    }

    #[test]
    fn paths_to_root() {
        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );
        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(5),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(!blocktree.is_coherent(&b2.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(!blocktree.is_coherent(&b3.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b4, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(!blocktree.is_coherent(&b4.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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

        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );
        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(5),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        assert!(blocktree
            .add(
                full_block_new(&b4, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b4.qc).unwrap() == b4.qc);

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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
    fn test_parent_update_coherency() {
        // Initial blocktree
        //  g
        //  |
        //  b1 (coherent = false)
        //  |
        //  ...
        //
        // blocktree is updated with b2

        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            epoch: genesis_qc.get_epoch(),
            seq_num: genesis_qc.get_seq_num(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());

        let b1_entry = blocktree.tree.get(&b1.get_id()).unwrap();
        assert!(b1_entry.is_coherent);
        // set b1 to be incoherent
        blocktree.tree.set_coherent(&b1.get_id(), false).unwrap();
        assert!(!blocktree.is_coherent(&b1.get_id()));

        // when b2 is added, b1 coherency should be updated
        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());

        // all blocks must be coherent
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(blocktree.is_coherent(&b2.get_id()));
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

        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        // root must be coherent but b2 isn't
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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

        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let v3 = VoteInfo {
            id: b2.get_id(),
            round: Round(3),
            parent_id: b2.get_parent_id(),
            parent_round: Round(2),
            ..DontCare::dont_care()
        };
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc(v3),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root must be coherent but b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b2.qc);
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        // root must be coherent but b3 and b2 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        // add block 1
        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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

        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let v3 = VoteInfo {
            id: b2.get_id(),
            round: Round(3),
            parent_id: b2.get_parent_id(),
            parent_round: Round(2),
            ..DontCare::dont_care()
        };
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc(v3),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root must be coherent but b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 1
        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root and block 1 must be coherent but b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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

        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };

        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        // root must be coherent but b2 and b3 should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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

        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let v1 = VoteInfo {
            id: g.get_id(),
            round: Round(1),
            parent_id: g.get_parent_id(),
            parent_round: Round(0),
            ..DontCare::dont_care()
        };

        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc(v1),
        );

        let v2 = VoteInfo {
            id: b1.get_id(),
            round: Round(2),
            parent_id: b1.get_parent_id(),
            parent_round: Round(1),
            ..DontCare::dont_care()
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let v3 = VoteInfo {
            id: b2.get_id(),
            round: Round(3),
            parent_id: b2.get_parent_id(),
            parent_round: Round(2),
            ..DontCare::dont_care()
        };
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc(v2),
        );

        let v4 = VoteInfo {
            id: b3.get_id(),
            round: Round(4),
            parent_id: b3.get_parent_id(),
            parent_round: Round(2),
            ..DontCare::dont_care()
        };
        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(5),
            &execution,
            payload.get_id(),
            &mock_qc(v3),
        );
        let b5 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(6),
            &execution,
            payload.get_id(),
            &mock_qc(v4),
        );
        let b6 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(7),
            &execution,
            payload.get_id(),
            &mock_qc(v4),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&g.qc).is_none()); // root naturally don't have missing ancestor

        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b2.qc).unwrap() == b2.qc);

        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b3.qc).unwrap() == b3.qc);

        assert!(blocktree
            .add(
                full_block_new(&b4, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b4.qc).unwrap() == b2.qc);

        assert!(blocktree
            .add(
                full_block_new(&b5, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b5.qc).unwrap() == b3.qc);

        assert!(blocktree
            .add(
                full_block_new(&b6, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.get_missing_ancestor(&b6.qc).unwrap() == b3.qc);

        // root must be coherent but rest of the blocks should not
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b4.get_id()));
        assert!(!blocktree.is_coherent(&b5.get_id()));
        assert!(!blocktree.is_coherent(&b6.get_id()));

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
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

    #[test]
    fn test_children_update() {
        // Initial block tree
        //   root
        //    |
        //   ... missing b1
        //    |
        //   b2
        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b1),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        assert!(blocktree
            .add(
                full_block_new(&b2, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree.root.children_blocks.is_empty());

        assert!(blocktree
            .add(
                full_block_new(&b1, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert_eq!(blocktree.root.children_blocks, vec![b1.get_id()]);
        let b1_children = blocktree
            .tree
            .get(&b1.get_id())
            .unwrap()
            .children_blocks
            .clone();
        assert_eq!(b1_children, vec![b2.get_id()]);
    }

    #[test]
    fn test_get_high_committable_qc() {
        // Initial block tree. It can't be constructed with honest consensus.
        // Just created for testing purpose
        //      g(b1)
        //      |
        //     (b3) - not received
        //    /   \
        //  b4     b9
        //  |      |
        //  b5     b10
        //  |      |
        //  b6     b11
        //  |
        //  b7

        // Need to craft b4 and b6 block id such that b6 is before b4 when
        // populating b3.children
        let payload = Payload::dont_care();
        let execution = ExecutionProtocol::dont_care();
        let g = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(1),
            &execution,
            payload.get_id(),
            &QC::genesis_qc(),
        );

        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&g),
        );

        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b3),
        );

        let b5 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(5),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b4),
        );

        let b6 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(6),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b5),
        );

        let b7 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(7),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b6),
        );

        let b9 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(9),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b3),
        );

        let b10 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(10),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b9),
        );

        let b11 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(11),
            &execution,
            payload.get_id(),
            &mock_qc_for_block(&b10),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
        });
        let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;

        // insertion order: insert all blocks except b3, then b3
        assert!(blocktree
            .add(
                full_block_new(&g, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b4, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b5, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b6, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b7, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b9, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b10, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());
        assert!(blocktree
            .add(
                full_block_new(&b11, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());

        assert!(blocktree
            .add(
                full_block_new(&b3, &payload),
                &mut block_policy,
                &state_backend
            )
            .is_ok());

        let high_commit_qc = blocktree.get_high_committable_qc();
        assert_eq!(high_commit_qc, Some(b11.get_qc().clone()));
    }
}
