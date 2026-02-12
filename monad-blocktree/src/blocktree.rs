// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::VecDeque,
    fmt::{self, Debug},
};

use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{
    block::{BlockPolicy, BlockPolicyError, BlockRange, ConsensusBlockHeader},
    checkpoint::RootInfo,
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    quorum_certificate::QuorumCertificate,
    timeout::HighExtend,
    RoundCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, ExecutionProtocol, Round, SeqNum};
use monad_validator::signature_collection::SignatureCollection;

use crate::tree::{BlockTreeEntry, Tree};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Root {
    info: RootInfo,
    children_blocks: Vec<BlockId>,
}

pub struct BlockTree<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    /// The round and block_id of last committed block
    root: Root,
    /// Uncommitted blocks
    /// First level of blocks in the tree have block.get_parent_id() == root.block_id
    tree: Tree<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> Debug for BlockTree<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockTree")
            .field("root", &self.root)
            .field("tree", &self.tree)
            .finish_non_exhaustive()
    }
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> PartialEq<Self>
    for BlockTree<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.tree == other.tree
    }
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> Eq for BlockTree<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> BlockTree<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new(root: RootInfo) -> Self {
        Self {
            root: Root {
                info: root,
                children_blocks: Vec::new(),
            },
            tree: Default::default(),
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
                    < new_root_entry.validated_block.get_block_round()
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
        self.root = Root {
            info: RootInfo {
                round: new_root_entry.validated_block.get_block_round(),
                seq_num: new_root_entry.validated_block.get_seq_num(),
                epoch: new_root_entry.validated_block.get_epoch(),
                block_id: new_root_entry.validated_block.get_id(),
                timestamp_ns: new_root_entry.validated_block.get_timestamp(),
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

        self.tree.insert(block);

        if parent_id == self.root.info.block_id {
            self.root.children_blocks.push(new_block_id);
        }
    }

    pub fn try_update_coherency(
        &mut self,
        metrics: &mut Metrics,
        block_id: BlockId,
        block_policy: &mut BPT,
        state_backend: &SBT,
        chain_config: &CCT,
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
            match block_policy.check_coherency(
                next_block,
                extending_blocks,
                self.root.info,
                state_backend,
                chain_config,
            ) {
                Ok(()) => {
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
                Err(BlockPolicyError::StateBackendError(StateBackendError::NotAvailableYet)) => {
                    metrics.consensus_events.rx_execution_lagging += 1;
                }
                Err(BlockPolicyError::ExecutionResultMismatch) => {
                    metrics.consensus_events.rx_bad_state_root += 1;
                }
                Err(BlockPolicyError::BaseFeeError) => {
                    metrics.consensus_events.rx_base_fee_error += 1;
                }
                Err(
                    BlockPolicyError::BlockPolicyBlockValidatorError(_)
                    | BlockPolicyError::BlockNotCoherent
                    | BlockPolicyError::Eip7702Error
                    | BlockPolicyError::TimestampError
                    | BlockPolicyError::StateBackendError(StateBackendError::NeverAvailable)
                    | BlockPolicyError::SystemTransactionError,
                ) => {
                    // TODO add metrics
                }
            }
        }
        retval
    }

    /// Iterate the block tree and return highest QC that have path to block tree
    /// root and is committable, if exists
    ///
    /// FIXME this does not take high_qc into account, which makes this more pessimistic than it
    /// needs to be
    pub fn get_high_committable_qc(&self) -> Option<QuorumCertificate<SCT>> {
        let mut high_commit_qc: Option<QuorumCertificate<SCT>> = None;
        let mut iter: VecDeque<BlockId> = self.root.children_blocks.clone().into();
        while let Some(bid) = iter.pop_front() {
            let block = self.tree.get(&bid).expect("block in tree");

            // queue up children
            iter.extend(block.children_blocks.iter().cloned());

            let qc = block.validated_block.get_qc();
            if high_commit_qc
                .as_ref()
                .is_some_and(|high_commit_qc| high_commit_qc.get_round() >= qc.get_round())
            {
                // we already have observed a higher committable QC
                continue;
            }

            let Some(qc_parent_block) = self.tree.get(&qc.get_block_id()) else {
                // parent block doesn't exist, or parent block is root
                continue;
            };

            let Some(committable_block_id) =
                qc.get_committable_id(qc_parent_block.validated_block.header())
            else {
                // qc is not committable (not consecutive rounds)
                continue;
            };

            if committable_block_id == self.root.info.block_id {
                // nothing new to commit
                continue;
            }

            if !self.is_coherent(&committable_block_id) {
                // the committable block is not (yet) coherent, likely because execution is lagging
                // can also happen if committable_block_id is the parent of root
                //
                // TODO can we return out early here, because we're BFS?
                continue;
            }

            high_commit_qc = Some(qc.clone());
        }
        high_commit_qc
    }

    /// returns a BlockRange that should be requested to fill the path from `qc` to the tree root.
    pub fn maybe_fill_path_to_root(&self, qc: &QuorumCertificate<SCT>) -> Option<BlockRange> {
        if self.root.info.round >= qc.get_round() || self.root.info.block_id == qc.get_block_id() {
            // root cannot be an ancestor of qc
            return None;
        }

        let mut maybe_unknown_bid = qc.get_block_id();
        let mut num_blocks = SeqNum(1);
        while let Some(known_block_entry) = self.tree.get(&maybe_unknown_bid) {
            // If the parent round == self.root, we have path to root
            if known_block_entry.validated_block.get_parent_id() == self.root.info.block_id {
                return None;
            }
            maybe_unknown_bid = known_block_entry.validated_block.get_parent_id();
            // FIXME replace with below once null blocks are deleted
            num_blocks = (known_block_entry.validated_block.get_seq_num() - self.root.info.seq_num)
                .max(SeqNum(2))
                - SeqNum(1);
            // num_blocks = known_block_entry.validated_block.get_seq_num() - self.root.info.seq_num - SeqNum(1);
        }

        Some(BlockRange {
            last_block_id: maybe_unknown_bid,
            num_blocks,
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

    /// Returns the highest coherent block on the path from root to the given block.
    pub fn get_highest_coherent_block_on_path_from_root(
        &self,
        b: &BlockId,
    ) -> Option<&BPT::ValidatedBlock> {
        let path = self.get_blocks_on_path_from_root(b)?;
        path.into_iter()
            .rev()
            .find(|block| self.is_coherent(&block.get_id()))
    }

    // Take a QC and look for the block it certifies in the blocktree. If it exists, return its
    // seq_num
    pub fn get_seq_num_of_qc(&self, qc: &QuorumCertificate<SCT>) -> Option<SeqNum> {
        let block_id = qc.get_block_id();
        if self.root.info.block_id == block_id {
            return Some(self.get_root_seq_num());
        }
        let certified_block = self.tree.get(&block_id)?;
        Some(certified_block.validated_block.get_seq_num())
    }

    // Take a QC and look for the block it certifies in the blocktree. If it exists, return its
    // timestamp
    pub fn get_timestamp_of_qc(&self, qc: &QuorumCertificate<SCT>) -> Option<u128> {
        let block_id = qc.get_block_id();
        if self.root.info.block_id == block_id {
            return Some(self.get_root_timestamp());
        }
        let certified_block = self.tree.get(&block_id)?;
        Some(certified_block.validated_block.get_timestamp())
    }

    // Take a QC and look for the block it certifies in the blocktree. If it exists, return its
    // round
    pub fn get_block_round_of_qc(&self, qc: &QuorumCertificate<SCT>) -> Option<Round> {
        let block_id = qc.get_block_id();
        if self.root.info.block_id == block_id {
            return Some(self.root.info.round);
        }
        let certified_block = self.tree.get(&block_id)?;
        Some(certified_block.validated_block.get_block_round())
    }

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &ConsensusBlockHeader<ST, SCT, EPT>) -> bool {
        !self.tree.contains_key(&b.get_id()) && b.block_round > self.root.info.round
    }

    pub fn tree(&self) -> &Tree<ST, SCT, EPT, BPT, SBT, CCT, CRT> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }

    pub fn get_root_seq_num(&self) -> SeqNum {
        self.root.info.seq_num
    }

    pub fn get_root_timestamp(&self) -> u128 {
        self.root.info.timestamp_ns
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

    pub fn get_entry(
        &self,
        block_id: &BlockId,
    ) -> Option<&BlockTreeEntry<ST, SCT, EPT, BPT, SBT, CCT, CRT>> {
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

    /// Returns the highest-round coherent descendant in the subtree rooted at `block_id`
    fn highest_round_coherent_descendant(&self, block_id: BlockId) -> BlockId {
        let mut best_id = block_id;
        let Some(mut best_round) = (if block_id == self.root.info.block_id {
            Some(self.root.info.round)
        } else {
            self.tree
                .get(&block_id)
                .map(|entry| entry.validated_block.get_block_round())
        }) else {
            return block_id;
        };

        // Traverse the coherent subtree rooted at `block_id`
        let mut queue = VecDeque::from([block_id]);
        while let Some(current_id) = queue.pop_front() {
            let Some(children) = (if current_id == self.root.info.block_id {
                Some(&self.root.children_blocks)
            } else {
                self.tree
                    .get(&current_id)
                    .map(|entry| &entry.children_blocks)
            }) else {
                continue;
            };

            for child_id in children.iter().filter(|id| self.is_coherent(id)) {
                let Some(child_entry) = self.tree.get(child_id) else {
                    continue;
                };
                let child_round = child_entry.validated_block.get_block_round();
                // Keep the highest-round coherent descendant seen so far.
                if child_round > best_round {
                    best_id = *child_id;
                    best_round = child_round;
                }
                // Continue exploring descendants from this coherent child.
                queue.push_back(*child_id);
            }
        }
        best_id
    }

    /// Find the highest coherent block on the canonical chain.
    /// Uses highest QC or TC high extend tip and then finds the highest round coherent child from that tip.
    /// If there's no coherent child, find the highest round coherent child from root. Or return root.
    pub fn get_canonical_coherent_tip(
        &self,
        high_cert: &RoundCertificate<ST, SCT, EPT>,
    ) -> BlockId {
        // First, set an anchor that is either high cert qc block or the tc high extend tip
        let canonical_anchor = high_cert
            .tc()
            .and_then(|tc| match &tc.high_extend {
                HighExtend::Tip(tip) => Some(tip.block_header.get_id()),
                HighExtend::Qc(_) => None,
            })
            .unwrap_or(high_cert.qc().get_block_id());

        let maybe_coherent_tip: Option<BlockId> =
            if let Some(path) = self.get_blocks_on_path_from_root(&canonical_anchor) {
                path.into_iter()
                    .rev()
                    .find(|block| self.is_coherent(&block.get_id()))
                    .map(|block| block.get_id())
            } else {
                // find highest round coherent child from root
                // this is to keep execution running when there's gap in blocktree
                return self.highest_round_coherent_descendant(self.root.info.block_id);
            };

        if let Some(coherent_tip) = maybe_coherent_tip {
            self.highest_round_coherent_descendant(coherent_tip)
        } else {
            // hit this branch if there's a path from canonical_anchor to root,
            // but no coherent blocks on the path. root is always coherent
            self.root.info.block_id
        }
    }
}

#[cfg(test)]
mod test {
    use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
    use monad_consensus_types::{
        block::{
            ConsensusBlockHeader, ConsensusFullBlock, MockExecutionBody,
            MockExecutionProposedHeader, MockExecutionProtocol, PassthruBlockPolicy,
            GENESIS_TIMESTAMP,
        },
        metrics::Metrics,
        payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
        voting::Vote,
        RoundCertificate,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        NopKeyPair, NopSignature,
    };
    use monad_eth_types::EMPTY_RLP_TX_LIST;
    use monad_state_backend::{InMemoryState, InMemoryStateInner};
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Balance, Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};

    use super::BlockTree;
    use crate::blocktree::RootInfo;

    const BASE_FEE: u64 = 100_000_000_000;
    const BASE_FEE_TREND: u64 = 0;
    const BASE_FEE_MOMENT: u64 = 0;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;
    type BlockTreeType = BlockTree<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
        BlockPolicyType,
        StateBackendType,
        ChainConfigType,
        ChainRevisionType,
    >;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;
    type Block =
        ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>;
    type FullBlock =
        ConsensusFullBlock<SignatureType, SignatureCollectionType, ExecutionProtocolType>;
    type QC = QuorumCertificate<SignatureCollectionType>;

    fn node_id() -> NodeId<PubKeyType> {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair =
            <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
                &mut privkey,
            )
            .unwrap();
        NodeId::new(keypair.pubkey())
    }

    pub fn mock_qc_for_block(block: &Block) -> QC {
        let vote = Vote {
            id: block.get_id(),
            epoch: block.epoch,
            round: block.block_round,
        };
        QC::new(vote, MockSignatures::with_pubkeys(&[]))
    }

    type RC = RoundCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

    fn mock_rc_for_block(block: &FullBlock) -> RC {
        RoundCertificate::Qc(mock_qc_for_block(block.header()))
    }

    /// Build a RoundCertificate::Tc whose high_extend is Tip(tip_block) and whose
    /// inner QC points to qc_block.
    fn mock_rc_tc_with_tip(qc_block: &FullBlock, tip_block: &FullBlock) -> RC {
        use monad_consensus_types::{
            timeout::{HighExtend, HighTipRoundSigColTuple, TimeoutCertificate},
            tip::ConsensusTip,
        };
        let keypair = NopKeyPair::from_bytes(&mut [1_u8; 32]).unwrap();
        let tip = ConsensusTip::new(&keypair, tip_block.header().clone(), None);
        RoundCertificate::Tc(TimeoutCertificate {
            epoch: Epoch(0),
            round: tip_block.header().block_round,
            tip_rounds: vec![HighTipRoundSigColTuple {
                high_qc_round: qc_block.header().block_round,
                high_tip_round: tip_block.header().block_round,
                sigs: MockSignatures::with_pubkeys(&[]),
            }],
            high_extend: HighExtend::Tip(tip),
        })
    }

    fn get_genesis_block() -> FullBlock {
        let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: MockExecutionBody {
                data: Default::default(),
            },
        });
        let header = Block::new(
            node_id(),
            Epoch(1),
            Round(1),
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            body.get_id(),
            QC::genesis_qc(),
            SeqNum(1),
            1,
            RoundSignature::new(Round(1), &NopKeyPair::from_bytes(&mut [1_u8; 32]).unwrap()),
            Some(BASE_FEE),
            Some(BASE_FEE_TREND),
            Some(BASE_FEE_MOMENT),
        );

        FullBlock::new(header, body).unwrap()
    }

    fn get_next_block(
        parent: &FullBlock,
        maybe_round: Option<Round>,
        tx_bytes: &[u8],
    ) -> FullBlock {
        let parent = parent.header();
        let round = maybe_round.unwrap_or(parent.block_round + Round(1));

        let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: MockExecutionBody {
                data: tx_bytes.to_vec().into(),
            },
        });
        let header = Block::new(
            node_id(),
            parent.epoch,
            round,
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            body.get_id(),
            mock_qc_for_block(parent),
            parent.seq_num + SeqNum(1),
            parent.timestamp_ns + 1,
            RoundSignature::new(round, &NopKeyPair::from_bytes(&mut [1_u8; 32]).unwrap()),
            Some(BASE_FEE),
            Some(BASE_FEE_TREND),
            Some(BASE_FEE_MOMENT),
        );

        FullBlock::new(header, body).unwrap()
    }

    #[test]
    fn test_prune() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();

        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);
        let b3 = get_next_block(&g, None, &[3]);
        let b4 = get_next_block(&g, None, &[4]);
        let b5 = get_next_block(&b3, None, &[5]);
        let b6 = get_next_block(&b5, None, &[6]);
        let b7 = get_next_block(&b6, None, &[7]);

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
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());

        blocktree.add(b1.clone().into());
        blocktree.add(b2.clone().into());
        blocktree.add(b3.clone().into());
        blocktree.add(b4.clone().into());
        blocktree.add(b5.clone().into());
        blocktree.add(b6.clone().into());
        println!("{:?}", blocktree);

        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b4.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b5.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b5.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b6.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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

        blocktree.add(b7.into());

        let b8 = get_next_block(&b5, None, &[8]);

        blocktree.add(b8.into());
        println!("{:?}", blocktree);
    }

    #[test]
    fn test_add_parent_not_exist() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();

        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);

        let gid = g.get_id();
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.into());

        blocktree.add(b2.clone().into());
        assert_eq!(blocktree.tree.len(), 2);
        assert_eq!(
            blocktree.get_block(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert!(!blocktree.is_coherent(&b2.get_id()));

        blocktree.add(b1.clone().into());
        assert_eq!(blocktree.tree.len(), 3);
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&g, None, &[2]);
        let b3 = get_next_block(&b1, None, &[3]);

        // Initial blocktree
        //        g
        //   /    |
        //  b1    b2
        //  |
        //  b3
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.add(b1.clone().into());
        blocktree.add(b2.clone().into());
        blocktree.add(b3.into());

        assert_eq!(blocktree.size(), 4);

        // prune called on b1, we expect new tree to be
        // b1
        // |
        // b3
        // and the commit blocks should only contain b1 (not b2)
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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
        let metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend =
            InMemoryStateInner::<NopSignature, MockSignatures<NopSignature>>::genesis(
                Balance::MAX,
                SeqNum(4),
            );
        let block_policy = PassthruBlockPolicy;
        blocktree.add(g.into());
        blocktree.add(b1.clone().into());
        blocktree.add(b1.clone().into());
        blocktree.add(b1.into());

        assert_eq!(blocktree.tree.len(), 2);
    }

    #[test]
    fn path_to_root_repair_update_coherency_all_children() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);
        let b4 = get_next_block(&b1, Some(Round(5)), &[4]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));

        blocktree.add(b2.clone().into());
        assert!(!blocktree.is_coherent(&b2.get_id()));

        blocktree.add(b3.clone().into());
        assert!(!blocktree.is_coherent(&b3.get_id()));

        blocktree.add(b4.clone().into());
        assert!(!blocktree.is_coherent(&b4.get_id()));

        blocktree.add(b1.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);
        let b4 = get_next_block(&b1, Some(Round(5)), &[4]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b3.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        blocktree.add(b4.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b4.header().qc)
                .unwrap()
                .last_block_id
                == b4.header().get_parent_id()
        );

        blocktree.add(b1.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );

        assert!(blocktree.maybe_fill_path_to_root(&b1.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b4.header().qc).is_none());

        blocktree.prune(&b1.get_id());

        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b1.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b4.header().qc).is_none());

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

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            epoch: genesis_qc.get_epoch(),
            seq_num: GENESIS_SEQ_NUM,
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.add(b1.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );

        let b1_entry = blocktree.tree.get(&b1.get_id()).unwrap();
        assert!(b1_entry.is_coherent);
        // set b1 to be incoherent
        blocktree.tree.set_coherent(&b1.get_id(), false).unwrap();
        assert!(!blocktree.is_coherent(&b1.get_id()));

        // when b2 is added, b1 coherency should be updated
        blocktree.add(b2.clone().into());

        // all blocks must be coherent
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        // root must be coherent but b2 isn't
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);
        let b3 = get_next_block(&b2, None, &[3]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root must be coherent but b3 should not
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        // root must be coherent but b3 and b2 should not
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        // add block 1
        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);
        let b3 = get_next_block(&b2, None, &[3]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root must be coherent but b3 should not
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 1
        blocktree.add(b1.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root and block 1 must be coherent but b3 should not
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        blocktree.add(b2.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root must be coherent but b2 and b3 should not
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, Some(Round(2)), &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);
        let b4 = get_next_block(&b2, Some(Round(5)), &[4]);
        let b5 = get_next_block(&b3, Some(Round(6)), &[5]);
        let b6 = get_next_block(&b3, Some(Round(7)), &[6]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        blocktree.add(b4.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b4.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b5.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b5.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        blocktree.add(b6.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b6.header().qc)
                .unwrap()
                .last_block_id
                == b3.get_parent_id()
        );

        // root must be coherent but rest of the blocks should not
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b4.get_id()));
        assert!(!blocktree.is_coherent(&b5.get_id()));
        assert!(!blocktree.is_coherent(&b6.get_id()));

        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b4.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b5.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b6.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b4.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b5.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b5.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b6.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
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

        let metrics = Metrics::default();
        let b1 = get_genesis_block();
        let b2 = get_next_block(&b1, None, &[1]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend =
            InMemoryStateInner::<NopSignature, MockSignatures<NopSignature>>::genesis(
                Balance::MAX,
                SeqNum(4),
            );
        let block_policy = PassthruBlockPolicy;
        blocktree.add(b2.clone().into());
        assert!(blocktree.root.children_blocks.is_empty());

        blocktree.add(b1.clone().into());
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

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b3 = get_next_block(&g, Some(Round(3)), &[EMPTY_RLP_TX_LIST]);
        let b4 = get_next_block(&b3, Some(Round(4)), &[EMPTY_RLP_TX_LIST]);
        let b5 = get_next_block(&b4, Some(Round(5)), &[EMPTY_RLP_TX_LIST]);
        let b6 = get_next_block(&b5, Some(Round(6)), &[EMPTY_RLP_TX_LIST]);
        let b7 = get_next_block(&b6, Some(Round(7)), &[EMPTY_RLP_TX_LIST]);
        let b9 = get_next_block(&b3, Some(Round(9)), &[EMPTY_RLP_TX_LIST]);
        let b10 = get_next_block(&b9, Some(Round(10)), &[EMPTY_RLP_TX_LIST]);
        let b11 = get_next_block(&b10, Some(Round(11)), &[EMPTY_RLP_TX_LIST]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;

        // insertion order: insert all blocks except b3, then b3
        blocktree.add(g.clone().into());
        blocktree.add(b4.into());
        blocktree.add(b5.into());
        blocktree.add(b6.into());
        blocktree.add(b7.into());
        blocktree.add(b9.into());
        blocktree.add(b10.into());
        blocktree.add(b11.clone().into());

        blocktree.add(b3.into());

        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        let high_commit_qc = blocktree.get_high_committable_qc();
        assert_eq!(high_commit_qc, Some(b11.get_qc().clone()));
    }

    #[test]
    fn test_get_highest_coherent_block_on_path_from_root() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;

        blocktree.add(g.clone().into());
        blocktree.add(b1.clone().into());
        blocktree.add(b2.clone().into());

        // Make all blocks coherent
        blocktree.try_update_coherency(
            &mut metrics,
            g.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );

        // All coherent: should return b2 (highest on path)
        let highest = blocktree.get_highest_coherent_block_on_path_from_root(&b2.get_id());
        assert!(highest.is_some());
        assert_eq!(highest.unwrap().get_id(), b2.get_id());

        // Make b2 incoherent: should return b1
        blocktree.tree.set_coherent(&b2.get_id(), false).unwrap();
        let highest = blocktree.get_highest_coherent_block_on_path_from_root(&b2.get_id());
        assert!(highest.is_some());
        assert_eq!(highest.unwrap().get_id(), b1.get_id());

        // Make b1 also incoherent: should return g
        blocktree.tree.set_coherent(&b1.get_id(), false).unwrap();
        let highest = blocktree.get_highest_coherent_block_on_path_from_root(&b2.get_id());
        assert!(highest.is_some());
        assert_eq!(highest.unwrap().get_id(), g.get_id());

        // Make g also incoherent: should return None
        blocktree.tree.set_coherent(&g.get_id(), false).unwrap();
        let highest = blocktree.get_highest_coherent_block_on_path_from_root(&b2.get_id());
        assert!(highest.is_none());
    }

    #[test]
    fn test_proposed_head_stays_on_canonical_chain() {
        // Tree: A <- B <- C <- D (canonical, high_qc = D)
        //          <- B' (side branch)
        // When B' becomes coherent: proposed_head = D (not B')
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b = get_next_block(&g, Some(Round(2)), &[1]);
        let c = get_next_block(&b, Some(Round(3)), &[2]);
        let d = get_next_block(&c, Some(Round(4)), &[3]);
        // B' is on a side branch (same parent as B, higher round)
        let b_prime = get_next_block(&g, Some(Round(5)), &[4]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(10));
        let mut block_policy = PassthruBlockPolicy;

        // Add and make blocks on canonical chain coherent
        blocktree.add(g.into());
        blocktree.add(b.into());
        blocktree.add(c.into());
        blocktree.add(d.clone().into());

        blocktree.try_update_coherency(
            &mut metrics,
            d.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&d.get_id()));

        // high_qc points to D (canonical chain tip)
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_for_block(&d));
        assert_eq!(proposed_head, d.get_id());

        // Now add B' on side branch and make it coherent
        blocktree.add(b_prime.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b_prime.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b_prime.get_id()));

        // proposed_head should still point to D (canonical chain), not B'
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_for_block(&d));
        assert_eq!(proposed_head, d.get_id());
    }

    #[test]
    fn test_proposed_head_follows_high_qc_branch() {
        // Tree: A <- B <- C (all coherent)
        //          <- B' <- C' (all coherent)
        // high_qc changes from C to C': proposed_head should follow
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let a = get_next_block(&g, Some(Round(2)), &[1]);
        let b = get_next_block(&a, Some(Round(3)), &[2]);
        let c = get_next_block(&b, Some(Round(4)), &[3]);
        let b_prime = get_next_block(&a, Some(Round(5)), &[4]);
        let c_prime = get_next_block(&b_prime, Some(Round(6)), &[5]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(10));
        let mut block_policy = PassthruBlockPolicy;

        // Add all blocks
        blocktree.add(g.into());
        blocktree.add(a.into());
        blocktree.add(b.into());
        blocktree.add(c.clone().into());
        blocktree.add(b_prime.into());
        blocktree.add(c_prime.clone().into());

        // Make all blocks coherent
        blocktree.try_update_coherency(
            &mut metrics,
            c.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        blocktree.try_update_coherency(
            &mut metrics,
            c_prime.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );

        assert!(blocktree.is_coherent(&c.get_id()));
        assert!(blocktree.is_coherent(&c_prime.get_id()));

        // When high_qc points to C, proposed_head is C
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_for_block(&c));
        assert_eq!(proposed_head, c.get_id());

        // When high_qc changes to C', proposed_head should be C'
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_for_block(&c_prime));
        assert_eq!(proposed_head, c_prime.get_id());
    }

    #[test]
    fn test_canonical_coherent_tip_extends_beyond_high_qc() {
        // Tree: root <- A <- B (high_qc) <- C <- D (all coherent)
        // get_canonical_coherent_tip(&B) should return D (deepest coherent descendant)
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let a = get_next_block(&g, Some(Round(2)), &[1]);
        let b = get_next_block(&a, Some(Round(3)), &[2]);
        let c = get_next_block(&b, Some(Round(4)), &[3]);
        let d = get_next_block(&c, Some(Round(5)), &[4]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(10));
        let mut block_policy = PassthruBlockPolicy;

        blocktree.add(g.into());
        blocktree.add(a.clone().into());
        blocktree.add(b.clone().into());
        blocktree.add(c.clone().into());
        blocktree.add(d.clone().into());

        blocktree.try_update_coherency(
            &mut metrics,
            d.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&c.get_id()));
        assert!(blocktree.is_coherent(&d.get_id()));

        // high_qc points to B, C and D are coherent beyond it
        // should return D (the deepest coherent descendant)
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_for_block(&b));
        assert_eq!(proposed_head, d.get_id());

        // Add a fork: C' at round 4 (sibling of C), extended by D' and E'
        // Tree: root <- A <- B (high_qc) <- C (round 4) <- D (round 5)
        //                                 <- C' (round 4) <- D' (round 5) <- E' (round 6)
        let c_prime = get_next_block(&b, Some(Round(4)), &[5]);
        let d_prime = get_next_block(&c_prime, Some(Round(5)), &[6]);
        let e_prime = get_next_block(&d_prime, Some(Round(6)), &[7]);
        blocktree.add(c_prime.clone().into());
        blocktree.add(d_prime.clone().into());
        blocktree.add(e_prime.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            e_prime.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&c_prime.get_id()));
        assert!(blocktree.is_coherent(&d_prime.get_id()));
        assert!(blocktree.is_coherent(&e_prime.get_id()));

        // With hint pointing to E' (TC.high_extend.tip), prefer the C' branch → returns E'
        let proposed_head =
            blocktree.get_canonical_coherent_tip(&mock_rc_tc_with_tip(&b, &e_prime));
        assert_eq!(proposed_head, e_prime.get_id());

        // With hint pointing to D (TC.high_extend.tip), prefer the C branch → returns D
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_tc_with_tip(&b, &d));
        assert_eq!(proposed_head, d.get_id());

        // A(high_qc, round 2) <- B(round 3) <- C(round 4) <- D(round 5)
        //                      <- F(round 4)
        // B <- C <- D is the deeper chain, but high_extend.tip points to F.
        // Pick F because the network was building on it.
        let f = get_next_block(&a, Some(Round(4)), &[8]);
        blocktree.add(f.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            f.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&f.get_id()));

        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_tc_with_tip(&a, &f));
        assert_eq!(proposed_head, f.get_id());

        // A(high_qc, round 2) <- B(round 3) <- ... <- E'(round 6)
        //                      <- F(round 4)
        //                      <- G(round 2) (lower round, but high_extend.tip)
        // Without hint, E' wins (highest coherent descendant in A's subtree).
        // With hint pointing to G, G wins because anchor moves to G's subtree.
        let g2 = get_next_block(&a, Some(Round(2)), &[9]);
        blocktree.add(g2.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            g2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&g2.get_id()));

        // Without hint, E' wins (highest round coherent descendant in A's subtree)
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_for_block(&a));
        assert_eq!(proposed_head, e_prime.get_id());

        // With hint pointing to G, G wins despite lowest round
        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_tc_with_tip(&a, &g2));
        assert_eq!(proposed_head, g2.get_id());

        // Non-greedy descendant selection case:
        // A has child B(round 100) and child C2(round 3) with descendant D2(round 200)
        // Must return D2 (highest coherent descendant in subtree), not B
        let b_high = get_next_block(&a, Some(Round(100)), &[10]);
        let c2 = get_next_block(&a, Some(Round(3)), &[11]);
        let d2 = get_next_block(&c2, Some(Round(200)), &[12]);
        blocktree.add(b_high.clone().into());
        blocktree.add(c2.clone().into());
        blocktree.add(d2.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b_high.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        blocktree.try_update_coherency(
            &mut metrics,
            d2.get_id(),
            &mut block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        assert!(blocktree.is_coherent(&b_high.get_id()));
        assert!(blocktree.is_coherent(&c2.get_id()));
        assert!(blocktree.is_coherent(&d2.get_id()));

        let proposed_head = blocktree.get_canonical_coherent_tip(&mock_rc_for_block(&a));
        assert_eq!(proposed_head, d2.get_id());
    }

    #[test]
    fn test_canonical_coherent_tip_returns_root_for_root() {
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });

        // When high_qc points to root with no children, returns root
        let rc = RoundCertificate::Qc(genesis_qc.clone());
        let result = blocktree.get_canonical_coherent_tip(&rc);
        assert_eq!(result, genesis_qc.get_block_id());
    }
}
