use std::{collections::BTreeMap, fs, path::PathBuf, time::Instant};

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{Address, B256, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_crypto::{NopSignature, hasher::Hash};
use monad_multi_sig::MultiSig;
use monad_state_backend::{
    AccountState, InMemoryBlockState, InMemoryState, InMemoryStateInner, MockExecution,
    encode_register_token_call, encode_shield_public_call,
    encode_transfer_call_with_sidecars, tinyvm_entrypoint,
};
use monad_types::{BlockId, Round, SeqNum, GENESIS_BLOCK_ID};
use serde::Serialize;
use tiny_privacy_vm_confidential_backend::{
    AccountState as BackendAccountState, CryptoConfig, LocalAccount,
    TokenConfig as ConfidentialTokenConfig, apply_encrypted_recipient_memo,
    generate_auditor_keypair, native_mon_public_asset, new_empty_local_account,
    prove_shield_public, prove_transfer_with_sidecars, sample_token, verify_transfer_call,
};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    iterations: usize,
    amount_per_transfer: u64,
    proof_bundle_bytes_raw: usize,
    tx_payload_bytes: usize,
    prove_ms_avg: f64,
    verify_ms_avg: f64,
    ledger_propose_ms_avg: f64,
    ledger_propose_commit_ms_avg: f64,
    final_sender_balance: u128,
    final_recipient_balance: u128,
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

fn seed_state(
    iterations: usize,
) -> (
    InMemoryState<SignatureType, SignatureCollectionType>,
    CryptoConfig,
    PrivateKeySigner,
    PrivateKeySigner,
    LocalAccount,
    LocalAccount,
) {
    let sender_signer = signer_from_index(0x11);
    let recipient_signer = signer_from_index(0x22);
    let cfg = CryptoConfig::default();
    let token = sample_token();
    let sender_addr = Address::from(address_bytes(sender_signer.address()));
    let recip_addr = Address::from(address_bytes(recipient_signer.address()));

    let mut public_accounts = BTreeMap::new();
    public_accounts.insert(sender_signer.address(), AccountState::max_balance_with_nonce(0));
    public_accounts.insert(recipient_signer.address(), AccountState::max_balance_with_nonce(0));
    let mut genesis = InMemoryBlockState::genesis(public_accounts);

    // Register token via process_tx
    let reg_input = encode_register_token_call(&sample_token_config(token));
    genesis.tinyvm_mut().process_tx(sender_addr, U256::ZERO, &reg_input).unwrap();

    // Shield sender with enough balance for iterations
    let sender_empty = new_empty_local_account(&cfg, address_bytes(sender_signer.address()), token);
    let shield_amount = (iterations as u128 + 32) * 1;
    let (shield_bundle, sender_after) = prove_shield_public(&cfg, &sender_empty, shield_amount).unwrap();
    let shield_input = encode_shield_public_call(&shield_bundle);
    genesis.tinyvm_mut().process_tx(sender_addr, U256::from(shield_amount), &shield_input).unwrap();

    // Shield recipient with 0 (just register the account)
    let recip_empty = new_empty_local_account(&cfg, address_bytes(recipient_signer.address()), token);

    let state = InMemoryStateInner::<SignatureType, SignatureCollectionType>::new(SeqNum(4), genesis);
    (state, cfg, sender_signer, recipient_signer, sender_after, recip_empty)
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

fn run_benchmark(iterations: usize) -> BenchmarkReport {
    let (mut state, cfg, sender_signer, recipient_signer, mut sender, mut recipient) =
        seed_state(iterations);

    let mut total_prove = 0.0f64;
    let mut total_verify = 0.0f64;
    let mut total_propose = 0.0f64;
    let mut total_propose_commit = 0.0f64;
    let mut last_payload_bytes = 0usize;
    let mut last_proof_bytes = 0usize;

    for i in 0..iterations {
        let prove_started = Instant::now();
        let mut rng = rand::thread_rng();
        let (_auditor_secret_keys, auditor_public_keys) = generate_auditor_keypair(&mut rng);
        let (call, next_sender) = prove_transfer_with_sidecars(
            &cfg,
            &sender,
            &recipient,
            1,
            Some(auditor_public_keys),
            &mut rng,
        )
        .unwrap();
        total_prove += prove_started.elapsed().as_secs_f64() * 1000.0;

        let verify_started = Instant::now();
        verify_transfer_call(&cfg, &call).unwrap();
        total_verify += verify_started.elapsed().as_secs_f64() * 1000.0;

        let tx = make_tinyvm_transfer_tx(&sender_signer, i as u64, &call.bundle, &call.sidecars);
        let payload = encode_transfer_call_with_sidecars(&call.bundle, call.sidecars.clone());
        last_payload_bytes = payload.len();
        last_proof_bytes = call.bundle.proof.relation.announcement_sender_old.0.len()
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

        let seq = SeqNum((i + 1) as u64);
        let block_id_bytes = {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
            bytes
        };
        let block_id = BlockId(Hash(block_id_bytes));

        let propose_started = Instant::now();
        state.ledger_propose(block_id, seq, Round(seq.0), if i == 0 { GENESIS_BLOCK_ID } else {
            let mut prev = [0u8; 32];
            prev[..8].copy_from_slice(&(i as u64).to_be_bytes());
            BlockId(Hash(prev))
        }, vec![tx]);
        total_propose += propose_started.elapsed().as_secs_f64() * 1000.0;

        let propose_commit_started = Instant::now();
        state.ledger_commit(&block_id, &seq);
        total_propose_commit += propose_commit_started.elapsed().as_secs_f64() * 1000.0;

        sender = next_sender;
        recipient = apply_encrypted_recipient_memo(
            &cfg,
            &recipient,
            &BackendAccountState {
                owner: address_bytes(recipient_signer.address()),
                token: sample_token(),
                version: recipient.public.version + 1,
                commitment: call.bundle.public.recipient_new_commitment,
            },
            call.sidecars
                .recipient_encrypted_memo
                .as_ref()
                .expect("recipient memo missing from transfer call"),
        )
        .unwrap();
    }

    BenchmarkReport {
        iterations,
        amount_per_transfer: 1,
        proof_bundle_bytes_raw: last_proof_bytes,
        tx_payload_bytes: last_payload_bytes,
        prove_ms_avg: total_prove / iterations as f64,
        verify_ms_avg: total_verify / iterations as f64,
        ledger_propose_ms_avg: total_propose / iterations as f64,
        ledger_propose_commit_ms_avg: (total_propose + total_propose_commit) / iterations as f64,
        final_sender_balance: sender.private.balance,
        final_recipient_balance: recipient.private.balance,
    }
}

fn main() {
    let (iterations, json_output) = parse_args();
    let report = run_benchmark(iterations);
    let json = serde_json::to_string_pretty(&report).unwrap();
    if let Some(path) = json_output {
        fs::write(&path, &json).unwrap();
    }
    println!("{json}");
}

#[cfg(test)]
mod tests {
    use super::run_benchmark;

    #[test]
    fn single_transfer_benchmark_smoke_has_positive_metrics() {
        let report = run_benchmark(1);
        assert!(report.prove_ms_avg > 0.0);
        assert!(report.verify_ms_avg > 0.0);
        assert!(report.ledger_propose_ms_avg > 0.0);
        assert!(report.ledger_propose_ms_avg < 400.0);
        assert!(report.tx_payload_bytes > 0);
        assert!(report.proof_bundle_bytes_raw > 0);
    }
}
