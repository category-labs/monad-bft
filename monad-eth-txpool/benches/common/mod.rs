use alloy_primitives::Address;
use criterion::{black_box, BatchSize, Criterion};
use monad_consensus_types::{
    payload::{EthExecutionProtocol, RoundSignature},
    txpool::TxPool,
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    NopSignature,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_types::EthAddress;
use monad_perf_util::PerfController;
use monad_state_backend::InMemoryState;
use monad_testutil::signing::MockSignatures;
use monad_types::Round;

pub use self::controller::BenchController;
use self::controller::BenchControllerConfig;

mod controller;

pub const EXECUTION_DELAY: u64 = 4;
pub const MOCK_TIMESTAMP: u128 = 100;
pub const MOCK_BENEFICIARY: EthAddress = EthAddress(Address::ZERO);

pub type SignatureType = NopSignature;
pub type SignatureCollectionType = MockSignatures<SignatureType>;
pub type StateBackendType = InMemoryState;
type KeyPairType = <SignatureType as CertificateSignature>::KeyPairType;

pub type Pool = dyn TxPool<
    SignatureType,
    SignatureCollectionType,
    EthExecutionProtocol,
    EthBlockPolicy<SignatureType, SignatureCollectionType>,
    StateBackendType,
>;

const BENCH_CONFIGS: [(&str, BenchControllerConfig); 5] = [
    (
        "simple",
        BenchControllerConfig {
            accounts: 10_000,
            txs: 10_000,
            nonce_var: 0,
            pending_blocks: 2,
            proposal_tx_limit: 10_000,
        },
    ),
    (
        "random",
        BenchControllerConfig {
            accounts: 30_000,
            txs: 10_000,
            nonce_var: 0,
            pending_blocks: 2,
            proposal_tx_limit: 10_000,
        },
    ),
    (
        "duplicate_nonce",
        BenchControllerConfig {
            accounts: 1_000,
            txs: 10_000,
            nonce_var: 0,
            pending_blocks: 2,
            proposal_tx_limit: 1_000,
        },
    ),
    (
        "increasing_nonce",
        BenchControllerConfig {
            accounts: 100,
            txs: 10_000,
            nonce_var: 50,
            pending_blocks: 2,
            proposal_tx_limit: 10_000,
        },
    ),
    (
        "nonce_gaps",
        BenchControllerConfig {
            accounts: 50,
            txs: 10_000,
            nonce_var: 100,
            pending_blocks: 2,
            proposal_tx_limit: 10_000,
        },
    ),
];

pub fn make_test_round_signature() -> RoundSignature<SignatureType> {
    let mut s = [127_u8; 32];
    let keypair = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();
    RoundSignature::new(Round(1), &keypair)
}

pub fn run_txpool_benches<T>(
    c: &mut Criterion,
    func_name: &'static str,
    mut setup: impl FnMut(&BenchControllerConfig) -> T,
    mut routine: impl FnMut(&mut T),
) {
    let mut group = c.benchmark_group("txpool");

    let mut perf_controller = match PerfController::from_env() {
        Ok(perf) => Some(perf),
        Err(e) => {
            println!(
                "Failed to initialize perf controller, continuing without sampling. Did you define the `PERF_CTL_FD` and `PERF_CTL_FD_ACK` environment variables? Error: {e:?}",
            );

            None
        }
    };

    for (bench_name, controller_config) in &BENCH_CONFIGS {
        let name = format!("{func_name}-{bench_name}");

        group.bench_function(name, |b| {
            b.iter_batched_ref(
                || setup(controller_config),
                |t| {
                    if let Some(perf) = perf_controller.as_mut() {
                        perf.enable();
                    }

                    routine(black_box(t));

                    if let Some(perf) = perf_controller.as_mut() {
                        perf.disable();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
}
