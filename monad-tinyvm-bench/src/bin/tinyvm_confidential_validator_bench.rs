use std::{fs, path::PathBuf, time::Instant};

use alloy_consensus::{SignableTransaction, Transaction, TxEip1559, TxEnvelope};
use alloy_primitives::{Address, B256, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_chain_config::{ChainConfig, MockChainConfig};
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{NopKeyPair, NopSignature, certificate_signature::CertificateKeyPair};
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ProposedEthHeader};
use monad_testutil::signing::MockSignatures;
use monad_types::{Epoch, Round, SeqNum, GENESIS_SEQ_NUM, NodeId};
use serde::Serialize;
use monad_state_backend::tinyvm_entrypoint;
use tiny_privacy_vm_confidential_backend::{
    CryptoConfig, encode_transfer_call_with_sidecars, generate_auditor_keypair, new_local_account,
    prove_transfer_with_sidecars, sample_token,
};

#[derive(Debug, Clone, Serialize)]
struct BatchReport {
    batch_size: usize,
    iterations: usize,
    avg_total_ms: f64,
    avg_per_tx_ms: f64,
    estimated_max_txs_in_400ms_sequential: usize,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    tx_payload_bytes: usize,
    proof_bundle_bytes_raw: usize,
    results: Vec<BatchReport>,
}

fn signer_from_index(index: u64) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&(index + 1).to_be_bytes());
    PrivateKeySigner::from_bytes(&B256::from(bytes)).unwrap()
}

fn make_tinyvm_transfer_tx(
    signer: &PrivateKeySigner,
    nonce: u64,
    bundle: &tiny_privacy_vm_confidential_backend::TransferBundle,
    sidecars: &tiny_privacy_vm_confidential_backend::TransferSidecars,
) -> TxEnvelope {
    let transaction = TxEip1559 {
        chain_id: MockChainConfig::DEFAULT.chain_id(),
        nonce,
        gas_limit: 400_000,
        max_fee_per_gas: 200_000_000_000,
        max_priority_fee_per_gas: 2_000_000_000,
        to: TxKind::Call(tinyvm_entrypoint()),
        value: U256::ZERO,
        access_list: Default::default(),
        input: encode_transfer_call_with_sidecars(bundle, sidecars.clone()).into(),
    };
    let signature = signer.sign_hash_sync(&transaction.signature_hash()).unwrap();
    transaction.into_signed(signature).into()
}

fn get_header(
    payload_id: monad_consensus_types::payload::ConsensusBlockBodyId,
) -> ConsensusBlockHeader<NopSignature, MockSignatures<NopSignature>, EthExecutionProtocol> {
    let nop_keypair = NopKeyPair::from_bytes(&mut [0_u8; 32]).unwrap();
    ConsensusBlockHeader::new(
        NodeId::new(nop_keypair.pubkey()),
        Epoch(1),
        Round(1),
        Vec::new(),
        ProposedEthHeader::default(),
        payload_id,
        QuorumCertificate::genesis_qc(),
        GENESIS_SEQ_NUM + SeqNum(1),
        1,
        RoundSignature::new(Round(1), &nop_keypair),
        100_000_000_000u64,
        0,
        0,
    )
}

fn parse_args() -> (usize, Option<PathBuf>) {
    let mut iterations = 10usize;
    let mut json_output = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iterations" => {
                let value = args.next().expect("missing value for --iterations");
                iterations = value.parse().expect("invalid --iterations");
            }
            "--json-output" => {
                json_output = Some(PathBuf::from(
                    args.next().expect("missing value for --json-output"),
                ));
            }
            other => panic!("unknown arg: {other}"),
        }
    }
    (iterations, json_output)
}

fn run_batch_case(batch_size: usize, iterations: usize) -> (BatchReport, usize, usize) {
    let cfg = CryptoConfig::default();
    let token = sample_token();
    let mut total_ms = 0.0f64;
    let mut proof_bundle_bytes_raw = 0usize;
    let mut tx_payload_bytes = 0usize;

    for iter in 0..iterations {
        let mut rng = rand::thread_rng();
        let mut txs = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let signer = signer_from_index((iter * batch_size + i) as u64 + 100);
            let recipient = signer_from_index((iter * batch_size + i) as u64 + 10_000);
            let sender = new_local_account(&cfg, signer.address().into_array(), token, 32, &mut rng);
            let recipient = new_local_account(
                &cfg,
                recipient.address().into_array(),
                token,
                1,
                &mut rng,
            );
            let (_auditor_secret_keys, auditor_public_keys) = generate_auditor_keypair(&mut rng);
            let (call, _) = prove_transfer_with_sidecars(
                &cfg,
                &sender,
                &recipient,
                1,
                Some(auditor_public_keys),
                &mut rng,
            )
            .unwrap();
            let tx = make_tinyvm_transfer_tx(&signer, i as u64, &call.bundle, &call.sidecars);
            tx_payload_bytes = tx.input().len();
            proof_bundle_bytes_raw = call.bundle.proof.relation.announcement_sender_old.0.len()
                + call.bundle.proof.relation.announcement_sender_new.0.len()
                + call.bundle.proof.relation.announcement_sender_delta.0.len()
                + call.bundle.proof.relation.announcement_recipient_delta.0.len()
                + call.bundle.proof.relation.announcement_arithmetic.0.len()
                + call.bundle.proof.relation.response_sender_old_balance.len()
                + call.bundle.proof.relation.response_sender_old_blinding.len()
                + call.bundle.proof.relation.response_sender_new_balance.len()
                + call.bundle.proof.relation.response_sender_new_blinding.len()
                + call.bundle.proof.relation.response_amount.len()
                + call.bundle.proof.relation.response_sender_delta_blinding.len()
                + call.bundle.proof.relation.response_recipient_delta_blinding.len()
                + call.bundle.proof.range_proof_bytes.len();
            txs.push(tx);
        }

        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs.into(),
                ommers: Default::default(),
                withdrawals: Default::default(),
            },
        });
        let header = get_header(payload.get_id());

        let started = Instant::now();
        let result = EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::validate_block_body(
            &header,
            &payload,
            &MockChainConfig::DEFAULT,
        );
        result.unwrap();
        total_ms += started.elapsed().as_secs_f64() * 1000.0;
    }

    let avg_total_ms = total_ms / iterations as f64;
    let avg_per_tx_ms = avg_total_ms / batch_size as f64;
    (
        BatchReport {
            batch_size,
            iterations,
            avg_total_ms,
            avg_per_tx_ms,
            estimated_max_txs_in_400ms_sequential: (400.0 / avg_per_tx_ms).floor() as usize,
        },
        proof_bundle_bytes_raw,
        tx_payload_bytes,
    )
}

fn main() {
    let (iterations, json_output) = parse_args();
    let mut results = Vec::new();
    let mut proof_bundle_bytes_raw = 0usize;
    let mut tx_payload_bytes = 0usize;
    for batch_size in [1usize, 8, 16, 32, 48, 64] {
        let (report, proof_bytes, payload_bytes) = run_batch_case(batch_size, iterations);
        proof_bundle_bytes_raw = proof_bytes;
        tx_payload_bytes = payload_bytes;
        results.push(report);
    }

    let json = serde_json::to_string_pretty(&BenchmarkReport {
        tx_payload_bytes,
        proof_bundle_bytes_raw,
        results,
    })
    .unwrap();
    if let Some(path) = json_output {
        fs::write(path, &json).unwrap();
    }
    println!("{json}");
}
