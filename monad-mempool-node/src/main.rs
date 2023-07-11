use std::env;

use monad_mempool_controller::{Controller, ControllerConfig};
use monad_mempool_server::start;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: monad-mempool <port>");
        return Ok(());
    }

    let mut controller = Controller::new(&ControllerConfig::default());
    controller.start().await.unwrap();
    let sender = controller.get_sender().unwrap();

    start(sender, args.get(1).unwrap().parse()?).await;

    Ok(())
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use ethers::{
        signers::LocalWallet,
        types::{transaction::eip2718::TypedTransaction, Address, TransactionRequest},
    };
    use monad_mempool_controller::{Controller, ControllerConfig};
    use monad_mempool_types::tx::PriorityTx;
    use oorandom::Rand32;

    const NUM_CONTROLLER: u16 = 4;
    const NUM_TX: u16 = 10;

    #[tokio::test(flavor = "multi_thread")]
    /// Starts NUM_CONTROLLER controllers, sends NUM_TX messages to one controller
    /// and checks that all controllers create the same proposal.
    async fn test_multi_controller() {
        let mut controllers = (0..NUM_CONTROLLER)
            .map(|_| Controller::new(&ControllerConfig::default().with_wait_for_peers(0)))
            .collect::<Vec<_>>();

        for controller in &mut controllers {
            controller.start().await.unwrap();
        }

        let sender_controller = controllers.get_mut(0).unwrap();
        let sender = sender_controller.get_sender().unwrap();

        let hex_txs = create_hex_txs(0, NUM_TX);
        for hex_tx in hex_txs {
            sender.send(hex_tx).await.unwrap();
        }

        // Allow time for controllers to receive the messages
        tokio::time::sleep(Duration::from_secs(2)).await;

        let sender_proposal = sender_controller.create_proposal();

        for controller in &controllers {
            let proposal = controller.create_proposal();
            assert_eq!(sender_proposal, proposal);
        }
    }

    const LOCAL_TEST_KEY: &str = "046507669b0b9d460fe9d48bb34642d85da927c566312ea36ac96403f0789b69";

    fn create_hex_txs(seed: u64, count: u16) -> Vec<String> {
        let wallet = LOCAL_TEST_KEY.parse::<LocalWallet>().unwrap();

        create_txs(seed, count)
            .into_iter()
            .map(|tx| {
                let signature = wallet.sign_transaction_sync(&tx).unwrap();
                hex::encode(tx.rlp_signed(&signature))
            })
            .collect()
    }

    fn create_txs(seed: u64, count: u16) -> Vec<TypedTransaction> {
        let mut rng = Rand32::new(seed);

        (0..count)
            .map(|_| {
                TransactionRequest::new()
                    .to("0xc582768697b4a6798f286a03A2A774c8743163BB"
                        .parse::<Address>()
                        .unwrap())
                    .gas(21337)
                    .gas_price(42)
                    .value(rng.rand_u32())
                    .nonce(0)
                    .into()
            })
            .collect()
    }
}
