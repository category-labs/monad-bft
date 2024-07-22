#![no_main]
use libfuzzer_sys::fuzz_target;
use monad_consensus::validation::signing::Unvalidated;
use monad_message_fuzz::fuzz_test_utils::VerifyBlockData;
use monad_testutil::proposal::define_proposal_with_tc;
use monad_types::{Epoch, Round, SeqNum};
static _NUM_NODES: u32 = 4;
static _VAL_SET_UPDATE_INTERVAL: SeqNum = SeqNum(2000);
static _EPOCH_START_DELAY: Round = Round(50);

// fuzz free variables during validation of proposal message
fuzz_target!(|data: VerifyBlockData| {
    let (f_block_round, _, f_parent_round, _, f_block_seq_num) = data.unwrap();
    let known_epoch = Epoch(1); // Can be Constant
    let known_round = Round(0); // Can be Constant
    let val_epoch = known_epoch; // Can be Constant
    let qc_epoch = Epoch(1); // Can be Constant
    let block_epoch = known_epoch; // Can be Constant
    let tc_epoch = known_epoch;
    let tc_epoch_signed = known_epoch;

    let block_round = f_block_round; //Free variable
    let block_seq_num = f_block_seq_num; //Free variable
    let qc_parent_round = f_parent_round; // Free variable

    if block_round > Round(2) && block_seq_num > SeqNum(0) {
        let tc_round = block_round - Round(1);
        let tc_round_signed = tc_round;

        let qc_round = f_block_round - Round(2); // Relation
        let qc_seq_num = f_block_seq_num - SeqNum(1); // Relation

        let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
            known_epoch,
            known_round,
            val_epoch,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            tc_epoch,
            tc_round,
            tc_epoch_signed,
            tc_round_signed,
            _NUM_NODES,
            _VAL_SET_UPDATE_INTERVAL,
            _EPOCH_START_DELAY,
        );

        let proposal = Unvalidated::new(proposal);
        assert!(proposal.validate(&epoch_manager, &val_epoch_map).is_ok());
    }
});
