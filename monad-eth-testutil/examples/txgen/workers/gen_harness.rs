use super::*;
use crate::{generators::native_transfer_priority_fee, prelude::*};

pub trait Generator {
    // todo: come up with a way to mint too
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)>;
}

pub struct GenCtx {
    pub base_fee: u128,
    pub chain_id: u64,
}

pub struct GeneratorHarness {
    pub generator: Box<dyn Generator + Send + Sync>,

    pub refresh_rx: mpsc::Receiver<Accounts>,
    pub rpc_sender: mpsc::Sender<AccountsWithTxs>,

    pub client: ReqwestClient,
    pub root_accts: VecDeque<SimpleAccount>,
    pub min_native: U256,
    pub seed_native_amt: U256,
    pub metrics: Arc<Metrics>,
    pub base_fee: u128,
    pub chain_id: u64,
}

impl GeneratorHarness {
    pub fn new(
        generator: Box<dyn Generator + Send + Sync>,
        refresh_rx: mpsc::Receiver<Accounts>,
        rpc_sender: mpsc::Sender<AccountsWithTxs>,
        client: &ReqwestClient,
        min_native: U256,
        seed_native_amt: U256,
        metrics: &Arc<Metrics>,
        base_fee: u128,
        chain_id: u64,
    ) -> Self {
        Self {
            generator,
            refresh_rx,
            rpc_sender,
            client: client.clone(),
            root_accts: VecDeque::with_capacity(10),
            min_native,
            metrics: Arc::clone(metrics),
            seed_native_amt,
            base_fee,
            chain_id,
        }
    }

    pub async fn run(mut self) {
        info!("Starting main gen loop");
        while let Some(accts) = self.refresh_rx.recv().await {
            info!(
                num_accts = accts.len(),
                channel_len = self.refresh_rx.len(),
                "Gen received accounts"
            );
            if let Some(root) = accts.root {
                self.root_accts.push_back(root);
            }
            let mut accts = accts.accts;
            let seeded_idx = itertools::partition(&mut accts, |a: &SimpleAccount| {
                a.native_bal < self.min_native
            });

            let mut txs = self.generator.handle_acct_group(
                &mut accts[seeded_idx..],
                &GenCtx {
                    base_fee: self.base_fee,
                    chain_id: self.chain_id,
                },
            );

            // handle low native bals
            let root = if seeded_idx != 0 {
                let mut root = self.root_accts.pop_front();
                if let Some(root) = root.as_mut() {
                    info!("Root {root}");
                    for acct in &accts[0..seeded_idx] {
                        let tx = native_transfer_priority_fee(
                            root,
                            acct.addr,
                            self.seed_native_amt,
                            1000,
                            &GenCtx {
                                base_fee: self.base_fee,
                                chain_id: self.chain_id,
                            },
                        );
                        txs.push((tx, acct.addr));
                    }
                    info!("Root2 {root}");
                }
                info!(
                    seeded_idx,
                    num_accts = accts.len(),
                    root_available = root.is_some(),
                    "Found accounts that need seeding"
                );
                root
            } else {
                None
            };

            let accts_with_txs = AccountsWithTxs {
                accts: Accounts { accts, root },
                txs,
            };

            let num_txs: usize = accts_with_txs.txs.len();

            self.rpc_sender
                .send(accts_with_txs)
                .await
                .expect("rpc sender channel closed");

            debug!(num_txs, "Gen pushed txs to rpc sender");
        }
    }
}
