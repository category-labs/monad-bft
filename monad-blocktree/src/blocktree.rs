use std::{
    collections::{HashMap, VecDeque},
    fmt,
    result::Result as StdResult,
};

use monad_consensus_types::{
    block::{BlockPolicy, BlockPolicyError, BlockType},
    checkpoint::RootInfo,
    payload::{Payload, PayloadId},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    state_root_hash::StateRootHash,
};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, SeqNum};
use tracing::trace;

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

pub struct BlockTreeEntry<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    pub validated_block: BPT::ValidatedBlock,
    /// A blocktree entry is coherent if there is a path to root from the entry and it
    /// is a valid extension of the chain
    pub is_coherent: bool,
    /// A vector of all the block ids that extend this validated block in the blocktree
    pub children_blocks: Vec<BlockId>,
}

impl<SCT, BPT, SBT> Clone for BlockTreeEntry<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn clone(&self) -> Self {
        Self {
            validated_block: self.validated_block.clone(),
            is_coherent: self.is_coherent,
            children_blocks: self.children_blocks.clone(),
        }
    }
}

impl<SCT, BPT, SBT> fmt::Debug for BlockTreeEntry<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockTreeEntry")
            .field("validated_block", &self.validated_block)
            .field("is_coherent", &self.is_coherent)
            .field("children_blocks", &self.children_blocks)
            .finish()
    }
}

impl<SCT, BPT, SBT> PartialEq<Self> for BlockTreeEntry<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn eq(&self, other: &Self) -> bool {
        self.validated_block == other.validated_block
            && self.is_coherent == other.is_coherent
            && self.children_blocks == other.children_blocks
    }
}

impl<SCT, BPT, SBT> Eq for BlockTreeEntry<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
}

type Tree<T> = HashMap<BlockId, T>;

pub struct BlockTree<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    /// The round and block_id of last committed block
    root: Root,
    /// Uncommitted blocks
    /// First level of blocks in the tree have block.get_parent_id() == root.block_id
    tree: Tree<BlockTreeEntry<SCT, BPT, SBT>>,
}

impl<SCT, BPT, SBT> fmt::Debug for BlockTree<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockTree")
            .field("root", &self.root)
            .field("tree", &self.tree)
            .finish_non_exhaustive()
    }
}

impl<SCT, BPT, SBT> PartialEq<Self> for BlockTree<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.tree == other.tree
    }
}

impl<SCT, BPT, SBT> Eq for BlockTree<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
}

impl<SCT, BPT, SBT> BlockTree<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    pub fn new(root: RootInfo) -> Self {
        Self {
            root: Root {
                info: root,
                children_blocks: Vec::new(),
            },
            tree: HashMap::new(),
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
        self.tree.retain(|_, b| {
            b.validated_block.get_parent_round() >= new_root_entry.validated_block.get_round()
        });
        self.root = Root {
            info: RootInfo {
                round: new_root_entry.validated_block.get_round(),
                seq_num: new_root_entry.validated_block.get_seq_num(),
                epoch: new_root_entry.validated_block.get_epoch(),
                block_id: new_root_entry.validated_block.get_id(),
                state_root: new_root_entry.validated_block.get_state_root(),
            },
            children_blocks: new_root_entry.children_blocks,
        };

        commit.reverse();
        commit
    }

    /// Add a new block to the block tree if it's not in the tree and is higher
    /// than the root block's round number
    pub fn add(
        &mut self,
        block: BPT::ValidatedBlock,
        block_policy: &mut BPT,
        state_backend: &SBT,
    ) -> Result<()> {
        if !self.is_valid_to_insert(&block) {
            return Ok(());
        }

        let new_block_id = block.get_id();
        let parent_id = block.get_parent_id();
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
            validated_block: block,
            is_coherent,
            children_blocks,
        };

        self.tree.insert(new_block_id, new_block_entry);

        // Retrieve the parent block's coherency and children blocks
        let children_blocks_to_update = if parent_id == self.root.info.block_id {
            Some(&mut self.root.children_blocks)
        } else if let Some(parent_entry) = self.tree.get_mut(&parent_id) {
            Some(&mut parent_entry.children_blocks)
        } else {
            // Parent missing, should be requested. Skip coherency check
            None
        };

        // Push the new block to the children blocks in the parent
        if let Some(children_blocks_to_update) = children_blocks_to_update {
            children_blocks_to_update.push(new_block_id);
        }

        if let Some(path_to_root) = self.get_blocks_on_path_from_root(&new_block_id) {
            let incoherent_parent_or_self = path_to_root
                .iter()
                .find(|block| {
                    !self
                        .tree
                        .get(&block.get_id())
                        .expect("block doesn't exist")
                        .is_coherent
                })
                .expect("new_block is not coherent (yet)");

            self.update_coherency(
                incoherent_parent_or_self.get_id(),
                block_policy,
                state_backend,
            )?
        }

        Ok(())
    }

    pub fn update_coherency(
        &mut self,
        block_id: BlockId,
        block_policy: &mut BPT,
        state_backend: &SBT,
    ) -> Result<()> {
        let mut block_ids_to_update: VecDeque<BlockId> = vec![block_id].into();

        while !block_ids_to_update.is_empty() {
            // Next block to check coherency
            let next_block = block_ids_to_update.pop_front().unwrap();
            let block = &self
                .tree
                .get(&next_block)
                .expect("block should exist")
                .validated_block;

            let mut extending_blocks = self
                .get_blocks_on_path_from_root(&next_block)
                .expect("path to root must exist");
            // Remove the block itself
            extending_blocks.pop();

            // extending blocks are always coherent, because we only call
            // update_coherency on the first incoherent block in the chain
            if let Err(err) = block_policy.check_coherency(block, extending_blocks, state_backend) {
                trace!("check_coherency returned an error: {:?}", err);
                return match err {
                    BlockPolicyError::StateBackendError(err) => {
                        Err(BlockTreeError::StateBackendError(err))
                    }
                    BlockPolicyError::BlockNotCoherent => {
                        Err(BlockTreeError::BlockNotCoherent(block.get_id()))
                    }
                };
            } else {
                self.tree.entry(next_block).and_modify(|entry| {
                    entry.is_coherent = true;
                });

                // Can check coherency of children blocks now
                block_ids_to_update.extend(
                    self.tree
                        .get(&next_block)
                        .expect("should be in tree")
                        .children_blocks
                        .iter()
                        .cloned(),
                );
            }
        }
        Ok(())
    }

    /// Iterate the block tree and return highest QC that have path to block tree
    /// root and is committable, if exists
    pub fn get_high_committable_qc(&self) -> Option<QuorumCertificate<SCT>> {
        let mut high_commit_qc: Option<QuorumCertificate<SCT>> = None;
        let mut iter: VecDeque<BlockId> = self.root.children_blocks.clone().into();
        while let Some(bid) = iter.pop_front() {
            let qc = self
                .tree
                .get(&bid)
                .expect("block in tree")
                .validated_block
                .get_qc();
            if qc.info.vote.ledger_commit_info.is_commitable()
                && self.is_coherent(&qc.info.vote.vote_info.parent_id)
                && high_commit_qc
                    .as_ref()
                    .map(|high_commit_qc| high_commit_qc.get_round() < qc.get_round())
                    .unwrap_or(true)
            {
                high_commit_qc = Some(qc.clone());
            }

            iter.extend(
                self.tree
                    .get(&bid)
                    .expect("should be in tree")
                    .children_blocks
                    .iter()
                    .cloned(),
            )
        }
        high_commit_qc
    }

    /// Find the missing block along the path from `qc` to the tree root.
    /// Returns the QC certifying that block
    pub fn get_missing_ancestor(
        &self,
        qc: &QuorumCertificate<SCT>,
    ) -> Option<QuorumCertificate<SCT>> {
        if self.root.info.round >= qc.get_round() {
            return None;
        }

        let mut maybe_unknown_block_qc = qc;
        let mut maybe_unknown_bid = maybe_unknown_block_qc.get_block_id();
        while let Some(known_block_entry) = self.tree.get(&maybe_unknown_bid) {
            maybe_unknown_block_qc = known_block_entry.validated_block.get_qc();
            maybe_unknown_bid = known_block_entry.validated_block.get_parent_id();
            // If the unknown block's round == self.root, that means we've already committed it
            if maybe_unknown_block_qc.get_round() == self.root.info.round {
                return None;
            }
        }
        Some(maybe_unknown_block_qc.clone())
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

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &BPT::ValidatedBlock) -> bool {
        !self.tree.contains_key(&b.get_id()) && b.get_round() > self.root.info.round
    }

    pub fn tree(&self) -> &Tree<BlockTreeEntry<SCT, BPT, SBT>> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }

    pub fn get_root_seq_num(&self) -> SeqNum {
        self.root.info.seq_num
    }

    /// Note that this returns None if the block_id is root!
    /// Use get_block_state_root instead?
    pub fn get_block(&self, block_id: &BlockId) -> Option<&BPT::ValidatedBlock> {
        self.tree.get(block_id).map(|block| &block.validated_block)
    }

    pub fn get_payload(&self, payload_id: &PayloadId) -> Option<Payload> {
        if let Some((_, block)) = self.tree.iter().find(|(_, blocktree_entry)| {
            &blocktree_entry.validated_block.get_payload_id() == payload_id
        }) {
            Some(block.validated_block.get_payload())
        } else {
            None
        }
    }

    pub fn get_block_state_root(&self, block_id: &BlockId) -> Option<StateRootHash> {
        if &self.root.info.block_id == block_id {
            return Some(self.root.info.state_root);
        }
        self.tree
            .get(block_id)
            .map(|block| block.validated_block.get_state_root())
    }

    pub fn get_entry(&self, block_id: &BlockId) -> Option<&BlockTreeEntry<SCT, BPT, SBT>> {
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
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::{Block as ConsensusBlock, BlockKind, BlockType, FullBlock, PassthruBlockPolicy},
        ledger::CommitResult,
        payload::{ExecutionProtocol, FullTransactionList, Payload, TransactionPayload},
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
    use monad_types::{BlockId, DontCare, Epoch, NodeId, Round, SeqNum};

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
        let ledger_commit_info = if block.get_round() == block.get_parent_round() + Round(1) {
            CommitResult::Commit
        } else {
            CommitResult::NoCommit
        };
        Vote {
            vote_info: VoteInfo {
                id: block.get_id(),
                epoch: block.get_epoch(),
                round: block.get_round(),
                parent_id: block.get_parent_id(),
                parent_round: block.get_parent_round(),
                seq_num: block.get_seq_num(),
                timestamp: 0,
            },
            ledger_commit_info,
        }
    }

    pub fn mock_qc(vote_info: VoteInfo) -> QC {
        QC::new(
            QcInfo {
                vote: Vote {
                    vote_info,
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        )
    }

    pub fn mock_qc_for_block(block: &Block) -> QC {
        let ledger_commit_info = if block.get_round() == block.get_parent_round() + Round(1) {
            CommitResult::Commit
        } else {
            CommitResult::NoCommit
        };
        let vote = Vote {
            vote_info: VoteInfo {
                id: block.get_id(),
                epoch: block.get_epoch(),
                round: block.get_round(),
                parent_id: block.get_parent_id(),
                parent_round: block.get_parent_round(),
                seq_num: block.get_seq_num(),
                timestamp: block.get_timestamp(),
            },
            ledger_commit_info,
        };
        QC::new(QcInfo { vote }, MockSignatures::with_pubkeys(&[]))
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v2),
        );

        let gid = g.get_id();
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            txns: TransactionPayload::List(FullTransactionList::new(vec![1].into())),
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            p1.get_id(),
            BlockKind::Executable,
            &mock_qc(v1),
        );

        let p2 = Payload {
            txns: TransactionPayload::List(FullTransactionList::new(vec![2].into())),
        };
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            p2.get_id(),
            BlockKind::Executable,
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
            txns: TransactionPayload::List(FullTransactionList::new(vec![3].into())),
        };
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            p3.get_id(),
            BlockKind::Executable,
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
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            txns: TransactionPayload::List(FullTransactionList::new(vec![1].into())),
        };
        let b1 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            p1.get_id(),
            BlockKind::Executable,
            &mock_qc(v1),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v2),
        );
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc(v2),
        );
        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(5),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v2),
        );
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc(v2),
        );
        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(5),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            epoch: genesis_qc.get_epoch(),
            seq_num: genesis_qc.get_seq_num(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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

        let b1_entry = blocktree.tree.get_mut(&b1.get_id()).unwrap();
        assert!(b1_entry.is_coherent);
        // set b1 to be incoherent
        b1_entry.is_coherent = false;
        assert!(!b1_entry.is_coherent);

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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v3),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v3),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v2),
        );
        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc(v2),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
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
            BlockKind::Executable,
            &mock_qc(v3),
        );
        let b5 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(6),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc(v4),
        );
        let b6 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(7),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc(v4),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
            &QC::genesis_qc(),
        );
        let b2 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(2),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b1),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
            BlockKind::Executable,
            &QC::genesis_qc(),
        );

        let b3 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(3),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&g),
        );

        let b4 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(4),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b3),
        );

        let b5 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(5),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b4),
        );

        let b6 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(6),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b5),
        );

        let b7 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(7),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b6),
        );

        let b9 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(9),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b3),
        );

        let b10 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(10),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b9),
        );

        let b11 = Block::new(
            node_id(),
            0,
            Epoch(1),
            Round(11),
            &execution,
            payload.get_id(),
            BlockKind::Executable,
            &mock_qc_for_block(&b10),
        );

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: genesis_qc.get_seq_num(),
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            state_root: Default::default(),
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
