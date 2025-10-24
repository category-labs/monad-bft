use std::{error::Error, fs::File, io::Read};

use alloy_consensus::Transaction;
use alloy_primitives::{hex, U256};
use monad_bls::{BlsSignature, BlsSignatureCollection};
use monad_chain_config::{revision::MonadChainRevision, MonadChainConfig, MONAD_TESTNET_CHAIN_ID};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock, PassthruBlockPolicy},
    block_validator::BlockValidator,
    payload::ConsensusBlockBody, tip::ConsensusTip,
};
use monad_eth_block_policy::{compute_txn_max_gas_cost, EthBlockPolicy};
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::EthExecutionProtocol;
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::NopStateBackend;

fn read_block(
    block_id: &str,
) -> Result<
    ConsensusFullBlock<SecpSignature, BlsSignatureCollection<PubKey>, EthExecutionProtocol>,
    Box<dyn Error>,
> {
    println!("reading block_id: {block_id:?}");
    let mut file = File::open(block_id)?;
    let size = file.metadata()?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf)?;

    let header: ConsensusBlockHeader<
        SecpSignature,
        BlsSignatureCollection<PubKey>,
        EthExecutionProtocol,
    > = alloy_rlp::decode_exact(&buf).map_err(|err| {
        std::io::Error::other(format!(
            "failed to rlp decode ledger proposed_head bft header, err={:?}",
            err
        ))
    })?;

    let payload_id = hex::encode(header.block_body_id.0 .0);
    println!("reading payload_id: {payload_id:?}");

    let mut file = File::open(payload_id)?;
    let size = file.metadata()?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf)?;

    let body: ConsensusBlockBody<EthExecutionProtocol> = alloy_rlp::decode_exact(&buf).unwrap();

    Ok(ConsensusFullBlock::new(header, body).unwrap())
}

// scp bue-004:~/monad-bft/ledger/headers/ab72b3e099da793e805f71ee495450fa6f02815df6e813a59bdb08829ee9bbe9 .
// scp bue-004:~/monad-bft/ledger/bodies/616d8d87c5c977d07bf84cb9a58e8adc9a2c0d47940a67317465ff9ae615e5ed .

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::try_init().unwrap();
    // let mut block_id =
    //     "cb7ac9eda8c7566836f6335a9e2b06a47f0b6cd982e452b441990305e6bae863".to_owned();
    // let mut blocks = Vec::new();
    // while blocks.len() < 7 {
    //     let block = read_block(&block_id).unwrap();
    //     block_id = hex::encode(block.get_parent_id().0 .0);
    //     blocks.push(block);
    // }
    // blocks.reverse();

    // let mut total = U256::ZERO;
    // for block in &blocks {
    //     println!("scanning block {}...", block.get_seq_num().0);
    //     for (idx, tx) in block.body().execution_body.transactions.iter().enumerate() {
    //         if tx.recover_signer().unwrap()
    //             == alloy_primitives::address!("0xbf8619905d0667766bec7dbae2f20f757cb2b496")
    //         {
    //             println!("{} idx: {:?}, tx: {:?}", block.get_seq_num().0, idx, tx);
    //             total += compute_txn_max_gas_cost(tx, block.get_base_fee().unwrap());
    //         }
    //     }
    // }
    // println!("total max gas cost: {}", total);
    // // println!(
    // //     "354={:?}, signer={:?}",
    // //     body.execution_body.transactions[354],
    // //     body.execution_body.transactions[354].recover_signer()
    // // );

    // // let block = ConsensusFullBlock::new(header, body).unwrap();

    // // let validator: EthBlockValidator<SecpSignature, BlsSignatureCollection<PubKey>> =
    // //     EthBlockValidator::default();

    // // let chain_config = MonadChainConfig::new(MONAD_TESTNET_CHAIN_ID, None).unwrap();
    // // BlockValidator::<
    // //     SecpSignature,
    // //     BlsSignatureCollection<PubKey>,
    // //     EthExecutionProtocol,
    // //     EthBlockPolicy<
    // //         SecpSignature,
    // //         BlsSignatureCollection<PubKey>,
    // //         MonadChainConfig,
    // //         MonadChainRevision,
    // //     >,
    // //     NopStateBackend,
    // //     MonadChainConfig,
    // //     MonadChainRevision,
    // // >::validate(&validator, header, body, None, &chain_config)
    // // .unwrap();

    // let tip = "f90567f905218331905182026af8a7e8a0741103c2468ad400f31a2668d65030ed241da1e79899e1aef81471ad7e0d41f68331905082026af87cd981ac960ffd77d77af1dc36c94aa7ff7fcb72f3a99dceb775deb8608e3c9eb75fcc063bbf93fb2ad7146181607b690fd303a54d43ad94edd83534ca95317840bc5f7bb818e4de39ff197fb8157f876be1e25d116af87625291f3370ab70c720991a818dbd6d919dc29e38f6f423622d31e7c62c6000945ae2f57440a10322b40febf89fa3cf6a05e9bbd8904b1979a0e43814d92de43ecdf81408037423832f19e18818718401fb1ff154b86088c3a90c7ff64b4c9f266c66d017729f41125541f58f5f23a36f7f3f7cb4201d36fae57b5b79a1c9f7443f67edb385130216ac2d0ff39b0a6e5e4071ce9f383c0411f0cd97037ab4775ff3064d9bb2717c090d03be7d0f1885d09188d8d0f76bf9028af90287a096179d10a673582986a5e1519cdf1b9931d21fb2bc34b9f5919005930a20f6ffa01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d493479427f8ed73e262420f8794ea12bcb0dc0d3c52c08aa0037e53faf16613e239c5fe88336a95e050cb73dde921d64f1132d4ba1ae751f7a0f21137fe08f5fbccd696efa13f99106149d9db3b1b17f6080adbacf0ae12be5ba09278dd00b75ce633c29f438a503857fc33667c9cfb3e56747101f7727155e12ab90100bffffa7fdf5ffbf67ffbffbfdfeffff1fffdfefb6ffffbfceefbfff9ffdf7fffffffd7efffffffdfdff59f7fbffdfd5ffbafff7feff7ffdfefff6ff7ffbefffb7effeffebbfffdfaffffffbfff6feffeff77dedffbffefffbffeffffffffd5feff5ffbfffdfeefdfffbfffffefafff7f8fffefffdfffbfeffffffefff9fff7fd52fffefffe3fbfbfffebfffa7dfdffe9fbffffe7ff7fffffdfff777dfaf6ffff9f7f79fffffaffffffbfffaff7f7fb7fffef7ffd7fbbd5dff6fdbfffff9f7fffefffbfffffffdacbbfffeedfffbc7fdffffdffffffbeef9bffeedfffeff7fe7feebb5fffff7b3beffff7fbefffdf7fffffffffeffd97ffffbfffff7fffedbffb80832f19de840bebc2008405afb0408468fbd338a00000000000000000000000000000000000000000000000000000000000000000a0def364a7bc12054201cf74ebeecd7ca5f3172335d346506f61b9b1659a8762c088000000000000000085174876e800a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000f9011ca01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d493479410d3a880ab77a9024f721fc5e095f97e832f00a7a047bb68b9e2f763ec4919fe0e38699e90557241cf272149abf8a4e8a0ea3da75280832f19e1840bebc2008468fbd339a00000000000000000000000000000000000000000000000000000000000000000a0feb6c0dd8e259bd06fc35cd389bcc3427809436b6b6d35dbdbbc3eaa5434d12888000000000000000085174876e800a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a0c76baabe43428b0af63ef3ba752c1f49ccbb5c68948737a381adba3ddeb1c55b85174876e800840333a1f2870f1ed6ded1ddccb8410a9b18163bdf61b0698883ad52f6fb6268731f0079feb29b73db986e61e95add69f6ecd72f66f0e8d0c585c8612988bac5f40db32dce733a8e658d6ea96b3fa201";
    let tip = "f906a5f905218331b4f082026bf8a7e8a0037cde0e79bf622e25afa4e730504a11d2b5997fbb2fda65a5e63a90a5c8020c8331a03c82026bf87cd981ac960ffc77f77ef5f53beb7af6d779cb76f7a9959ab019deb860b82c4d1feb0840ade7198645d8b7a3295b3a974e3fe1ef1801a596777aa12dcf5c1e1d4fe24fafee102062c3c971b742083e43c76af1d1ac2f9fb6cb62ce222a98b302b49c4c163d7bb65ecfebd7d9371a4fdfcd03cd8b3ec2649b5d265f5b95a1033d747f648922932aaef65067d733a56022439d08357ad18cf68b8ffd354db2eb832f28088818718a53d7c167cdb86084be35f4b00c10864907190540a95c995da7ee2daf4abe0b996f26fb965736c488c41d164552bdc9312b67b3f829dd7b022bfaad3f4e9dea4cff839542355acc3bf1073f850b6eb94b0ffc330285211684c48e7055048621f3f5cbc36dc5bc57f9028af90287a04c2227b0ad2417cb387dfca33a268935f782f69ec37908889f60b03ec0c00677a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d493479478218e0dffa60b955c46571de36c960ae1d1520aa01965c2c88844b628a2f11f85541c3fda133dcac58b807428f830d1ef895d8acfa0db380569bbc97a03b138d39b1c3c675ab27b38172ae6cf5fcbebce8d8c839ef3a0ea54e99c23d8ee78b1ed1e3ac23d385abd67b8b9cedf9003926dee162402de6fb901000000000000000000010000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000200000000000800000000000000000000000002000000000000000000000000000000000000001000000000000000000000000000000000000400000000000000000040000000000000000000000000000000000100000000000000000400000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080832f2805840bebc2008401e0a6e08468fbdac1a00000000000000000000000000000000000000000000000000000000000000000a028e83b96131935456d43c5cef87ca036458120e752782b983445e90b788bec3f8800000000000000008517d87954cca056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000f9011ca01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d4934794242dbe87cb81f7913d3ab1d3a8d7419ab936f182a0c8650be7696dad5fbbe0d6e21851955d5a752ee2acb8e817d6b1f1085377551880832f2808840bebc2008468fbee5da00000000000000000000000000000000000000000000000000000000000000000a0aad0f4c3170cc0014d3ac7cde3d13975730254c949416914615851f229418df288000000000000000085174876e800a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a02f59e7db1a10a2961601a8affe89ec5a93ae330935293801cad2d750ccac988285174876e8008403dd53d8871b5fed65c1a57ab8419db99e99a937339d00e5cf5e1d91946afca4f94de468b64ca32cf96ba9379d547f8f89d74b6675559a3af0cfe845fb5b22cd7f37b0738a3f84d07bb20d3d782101f9013b02f9013782026b8331b4eff885f8838331a03c80f87cd981ac960ffd77f77ae1d437eb4ae6df7dcbfaf3a995ceb76ddeb860b385d5b1974d62df23be21fffd8b96e3dcee56689fe61c4f58e5642cfa8d655e40a6dc44b54a745e0669eb4a885857ad16af87ba04202f6f4703a2027e40a6d4b82661695b092b458a9479e335ffce0993ed451b13186ff00e562a058ded9e07f8a7e8a0037cde0e79bf622e25afa4e730504a11d2b5997fbb2fda65a5e63a90a5c8020c8331a03c82026bf87cd981ac960ffc77f77ef5f53beb7af6d779cb76f7a9959ab019deb860b82c4d1feb0840ade7198645d8b7a3295b3a974e3fe1ef1801a596777aa12dcf5c1e1d4fe24fafee102062c3c971b742083e43c76af1d1ac2f9fb6cb62ce222a98b302b49c4c163d7bb65ecfebd7d9371a4fdfcd03cd8b3ec2649b5d265f5b95";

    let tip = hex::decode(&tip).unwrap();
    let header: ConsensusTip<
        SecpSignature,
        BlsSignatureCollection<PubKey>,
        EthExecutionProtocol,
    > = alloy_rlp::decode_exact(&tip).map_err(|err| {
        std::io::Error::other(format!(
            "failed to rlp decode ledger proposed_head bft header, err={:?}",
            err
        ))
    })?;
    println!("{:#?}", header);


    Ok(())
}
