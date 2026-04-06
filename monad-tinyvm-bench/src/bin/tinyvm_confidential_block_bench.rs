use std::{collections::BTreeMap, fs, path::PathBuf, time::Instant};

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{Address, B256, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_crypto::{NopSignature, hasher::Hash};
use monad_multi_sig::MultiSig;
use monad_state_backend::{
    AccountState, InMemoryBlockState, InMemoryStateInner, MockExecution,
    encode_register_token_call, encode_shield_public_call,
    encode_transfer_call_with_sidecars, tinyvm_entrypoint,
};
use monad_types::{BlockId, Round, SeqNum, GENESIS_BLOCK_ID};
use serde::Serialize;
use tiny_privacy_vm_confidential_backend::{
    AccountState as BackendAccountState, CryptoConfig,
    TokenConfig as ConfidentialTokenConfig, apply_encrypted_recipient_memo,
    generate_auditor_keypair, native_mon_public_asset, new_empty_local_account,
    prove_shield_public, prove_transfer_with_sidecars, sample_token,
};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;

#[derive(Debug, Clone, Serialize)]
struct BlockSizeReport {
    block_size: usize,
    iterations: usize,
    amount_per_transfer: u128,
    avg_block_ms: f64,
    avg_commit_ms: f64,
    avg_per_tx_ms: f64,
    estimated_max_txs_in_400ms_sequential: usize,
    fits_400ms: bool,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    proof_bundle_bytes_raw: usize,
    tx_payload_bytes: usize,
    results: Vec<BlockSizeReport>,
}

fn signer_from_index(index: u64) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&(index + 1).to_be_bytes());
    PrivateKeySigner::from_bytes(&B256::from(bytes)).unwrap()
}

fn address_bytes(address: Address) -> [u8; 20] {
    address.into_array()
}

fn make_tinyvm_transfer_tx(
    signer: &PrivateKeySigner,
    nonce: u64,
    bundle: &tiny_privacy_vm_confidential_backend::TransferBundle,
    sidecars: &tiny_privacy_vm_confidential_backend::TransferSidecars,
) -> TxEnvelope {
    let transaction = TxEip1559 {
        chain_id: 1337,
        nonce,
        gas_limit: 400_000,
        max_fee_per_gas: 200_000_000_000,
        max_priority_fee_per_gas: 2_000_000_000,
        to: TxKind::Call(tinyvm_entrypoint()),
        value: U256::ZERO,
        access_list: Default::default(),
        input: encode_transfer_call_with_sidecars(bundle, sidecars.clone()).into(),
    };

    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    transaction.into_signed(signature).into()
}

fn sample_token_config(token: [u8; 32]) -> ConfidentialTokenConfig {
    ConfidentialTokenConfig {
        token,
        public_asset: native_mon_public_asset(),
    }
}

fn parse_args() -> (usize, Option<PathBuf>) {
    let mut iterations = 5usize;
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

fn run_block_case(block_size: usize, iterations: usize) -> (BlockSizeReport, usize, usize) {
    let cfg = CryptoConfig::default();
    let token = sample_token();
    let amount = 1u128;
    let mut total_block_ms = 0.0;
    let mut total_commit_ms = 0.0;
    let mut proof_bundle_bytes_raw = 0usize;
    let mut tx_payload_bytes = 0usize;

    for iter in 0..iterations {
        let mut rng = rand::thread_rng();
        let mut public_accounts = BTreeMap::new();
        let mut txs = Vec::with_capacity(block_size);

        // Build genesis with token registered
        // We'll add a dummy account for the first sender to register the token
        let first_sender_signer = signer_from_index((iter * block_size) as u64 + 10);
        let first_sender_addr = Address::from(address_bytes(first_sender_signer.address()));
        public_accounts.insert(first_sender_signer.address(), AccountState::max_balance_with_nonce(0));
        let mut genesis = InMemoryBlockState::genesis(public_accounts.clone());
        let reg_input = encode_register_token_call(&sample_token_config(token));
        genesis.tinyvm_mut().process_tx(first_sender_addr, U256::ZERO, &reg_input).unwrap();

        for i in 0..block_size {
            let signer_index = (iter * block_size + i) as u64;
            let sender_signer = signer_from_index(signer_index + 10);
            let recipient_signer = signer_from_index(signer_index + 10_000);
            let sender_addr = Address::from(address_bytes(sender_signer.address()));

            // Shield sender with enough balance
            let sender_empty = new_empty_local_account(&cfg, address_bytes(sender_signer.address()), token);
            let shield_amount = 8u128;
            let (shield_bundle, sender_after) = prove_shield_public(&cfg, &sender_empty, shield_amount).unwrap();
            let shield_input = encode_shield_public_call(&shield_bundle);
            genesis.tinyvm_mut().process_tx(sender_addr, U256::from(shield_amount), &shield_input).unwrap();

            // Recipient starts empty (auto-created on transfer)
            let recipient_empty = new_empty_local_account(&cfg, address_bytes(recipient_signer.address()), token);

            let (_auditor_secret_keys, auditor_public_keys) = generate_auditor_keypair(&mut rng);
            let (call, _next_sender) = prove_transfer_with_sidecars(
                &cfg, &sender_after, &recipient_empty, amount,
                Some(auditor_public_keys), &mut rng,
            ).unwrap();

            let payload = encode_transfer_call_with_sidecars(&call.bundle, call.sidecars.clone());
            tx_payload_bytes = payload.len();
            proof_bundle_bytes_raw = call.bundle.proof.range_proof_bytes.len();
            txs.push(make_tinyvm_transfer_tx(&sender_signer, 0, &call.bundle, &call.sidecars));
        }

        let mut state =
            InMemoryStateInner::<SignatureType, SignatureCollectionType>::new(SeqNum(4), genesis);
        let mut block_bytes = [0u8; 32];
        block_bytes[..8].copy_from_slice(&(iter as u64 + block_size as u64).to_be_bytes());
        let block_id = BlockId(Hash(block_bytes));

        let block_started = Instant::now();
        state.ledger_propose(block_id, SeqNum(1), Round(1), GENESIS_BLOCK_ID, txs);
        total_block_ms += block_started.elapsed().as_secs_f64() * 1000.0;

        let commit_started = Instant::now();
        state.ledger_commit(&block_id, &SeqNum(1));
        total_commit_ms += commit_started.elapsed().as_secs_f64() * 1000.0;
    }

    let avg_block_ms = total_block_ms / iterations as f64;
    let avg_commit_ms = total_commit_ms / iterations as f64;
    let avg_per_tx_ms = avg_block_ms / block_size as f64;
    let report = BlockSizeReport {
        block_size,
        iterations,
        amount_per_transfer: amount,
        avg_block_ms,
        avg_commit_ms,
        avg_per_tx_ms,
        estimated_max_txs_in_400ms_sequential: (400.0 / avg_per_tx_ms).floor() as usize,
        fits_400ms: avg_block_ms <= 400.0,
    };

    (report, proof_bundle_bytes_raw, tx_payload_bytes)
}

fn main() {
    let (iterations, json_output) = parse_args();
    let block_sizes = [1usize, 8, 16, 32, 48, 64];
    let mut results = Vec::new();
    let mut proof_bundle_bytes_raw = 0usize;
    let mut tx_payload_bytes = 0usize;

    for block_size in block_sizes {
        let (report, proof_bytes, payload_bytes) = run_block_case(block_size, iterations);
        proof_bundle_bytes_raw = proof_bytes;
        tx_payload_bytes = payload_bytes;
        results.push(report);
    }

    let report = BenchmarkReport {
        proof_bundle_bytes_raw,
        tx_payload_bytes,
        results,
    };

    let json = serde_json::to_string_pretty(&report).unwrap();
    if let Some(path) = json_output {
        fs::write(&path, &json).unwrap();
    }
    println!("{json}");
}

#[cfg(test)]
mod tests {
    use super::run_block_case;

    #[test]
    fn block_benchmark_smoke_fits_400ms_for_small_parallel_batch() {
        let (report, proof_bytes, payload_bytes) = run_block_case(8, 1);
        assert!(report.avg_block_ms > 0.0);
        assert!(report.avg_block_ms < 2_000.0);
        assert!(report.estimated_max_txs_in_400ms_sequential >= 8);
        assert!(proof_bytes > 0);
        assert!(payload_bytes > 0);
    }
}
