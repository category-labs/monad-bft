#![no_main]
use libfuzzer_sys::fuzz_target;
use monad_consensus::{messages::message::ProposalMessage, validation::signing::Unvalidated};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_message_fuzz::fuzz_test_utils::VerifyBlockData;
use monad_testutil::{block::set_block_and_qc, validators::setup_val_state};
use monad_types::{Epoch, NodeId, Round, SeqNum};

static _NUM_NODES: u32 = 4;
static _VAL_SET_UPDATE_INTERVAL: SeqNum = SeqNum(2000);
static _EPOCH_START_DELAY: Round = Round(50);

fuzz_target!(|data: VerifyBlockData| {
    let (f_block_round, _, f_parent_round, _, f_block_seq_num) = data.unwrap();
    let known_epoch = Epoch(1); // Can be Constant
    let known_round = Round(0); // Can be Constant
    let val_epoch = known_epoch; // Can be Constant
    let qc_epoch = Epoch(1); // Can be Constant
    let block_epoch = known_epoch; // Can be Constant

    let block_round = f_block_round; //Free variable
    let block_seq_num = f_block_seq_num; //Free variable
    let qc_parent_round = f_parent_round; // Free variable

    if block_round > Round(1) && block_seq_num > SeqNum(0) {
        let qc_round = f_block_round - Round(1); // Relation
        let qc_seq_num = f_block_seq_num - SeqNum(1); // Relation

        let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
            known_epoch,
            known_round,
            val_epoch,
            _NUM_NODES,
            _VAL_SET_UPDATE_INTERVAL,
            _EPOCH_START_DELAY,
        );

        let proposal = Unvalidated::new(ProposalMessage {
            block: set_block_and_qc(
                NodeId::new(keypairs[0].pubkey()),
                block_epoch,
                block_round,
                block_seq_num,
                qc_epoch,
                qc_round,
                qc_parent_round,
                qc_seq_num,
                &[
                    keypairs[0].pubkey(),
                    keypairs[1].pubkey(),
                    keypairs[2].pubkey(),
                    keypairs[3].pubkey(),
                ],
            ),
            last_round_tc: None,
        });

        assert!(proposal.validate(&epoch_manager, &val_epoch_map).is_ok());
    }
});
