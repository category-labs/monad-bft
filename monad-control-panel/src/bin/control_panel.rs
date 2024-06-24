use std::io::Error;

use futures::{sink::SinkExt, StreamExt};
use inquire::{Confirm, InquireError, Select};
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_executor_glue::{
    ClearMetrics, ControlPanelCommand, GetValidatorSet, ReadCommand, WriteCommand,
};
use monad_secp::SecpSignature;
use tokio::net::UnixStream;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

const COMMANDS: [&str; 3] = ["validators", "update-validators", "clear-metrics"];

type SignatureType = SecpSignature;
type SignatureCollectionType = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
type Command = ControlPanelCommand<SignatureCollectionType>;

fn main() -> Result<(), i32> {
    let mut args = std::env::args();
    if args.len() != 2 {
        eprintln!("ERROR: pass in the path to the node control panel IPC socket");
        return Err(-1);
    }
    let socket_path = args.nth(1).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (mut read, mut write) = rt
        .block_on(async move {
            let client_stream = UnixStream::connect(socket_path.as_str()).await?;

            let (read, write) = client_stream.into_split();

            let read = FramedRead::new(read, LengthDelimitedCodec::default());
            let write = FramedWrite::new(write, LengthDelimitedCodec::default());

            Ok::<_, Error>((read, write))
        })
        .map_err(|e| {
            eprintln!("failed to initialize socket {:?}", e);
            -1
        })?;

    loop {
        let available_commands: Vec<&'static str> = Vec::from(COMMANDS);
        let command = rt.block_on(async {
            Select::new("monad-consensus-cli $", available_commands)
                .with_help_message("↑-↓ or j-k to move, enter to select, type to filter]")
                .with_vim_mode(true)
                .prompt()
        });

        match command {
            Ok(command) => match command {
                "validators" => {
                    let request =
                        Command::Read(ReadCommand::GetValidatorSet(GetValidatorSet::Request));
                    let bytes = bincode::serialize(&request).unwrap();
                    if let Err(e) = rt.block_on(write.send(bytes.into())) {
                        println!("Failed to send command {:?} to server: {:?}", &request, e);
                        continue;
                    };

                    let Some(Ok(response)) = rt.block_on(read.next()) else {
                        println!("Did not receive response from server");
                        continue;
                    };

                    let response = bincode::deserialize::<
                        ControlPanelCommand<SignatureCollectionType>,
                    >(&response)
                    .unwrap();

                    dbg!(response);
                }
                "clear-metrics" => {
                    let Ok(confirmation) = rt.block_on(async {
                        Confirm::new("Are you sure you want to reset metrics?")
                            .with_default(false)
                            .with_help_message("This will set all metrics/counters/stats to zero.")
                            .prompt()
                    }) else {
                        eprintln!("Error getting confirmation. Try again.");
                        continue;
                    };

                    if !confirmation {
                        println!("Interrupted reset metrics.");
                        continue;
                    }

                    let request = Command::Write(WriteCommand::ClearMetrics(ClearMetrics::Request));
                    let bytes = bincode::serialize(&request).unwrap();
                    if let Err(e) = rt.block_on(write.send(bytes.into())) {
                        println!("Failed to send command {:?} to server: {:?}", &request, e);
                        continue;
                    };

                    let Some(Ok(response)) = rt.block_on(read.next()) else {
                        println!("Did not receive response from server");
                        continue;
                    };

                    let response = bincode::deserialize::<
                        ControlPanelCommand<SignatureCollectionType>,
                    >(&response)
                    .unwrap();

                    dbg!(response);
                    println!("🔥 metrics reset 🔥");
                }
                _ => println!("Unknown command `{command}`"),
            },
            Err(e) => match e {
                InquireError::OperationInterrupted => {
                    println!();
                    break;
                }
                _ => panic!("unhandled error {:?}", e),
            },
        }
    }

    Ok(())
}
