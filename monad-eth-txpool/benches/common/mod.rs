use criterion::{black_box, BatchSize, Criterion};
use monad_crypto::NopSignature;
use monad_perf_util::PerfController;
use monad_testutil::signing::MockSignatures;

pub use self::controller::BenchController;
use self::controller::BenchControllerConfig;

mod controller;

pub const EXECUTION_DELAY: u64 = 4;

pub type SignatureCollectionType = MockSignatures<NopSignature>;

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
