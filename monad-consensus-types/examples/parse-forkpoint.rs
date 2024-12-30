use monad_bls::BlsSignatureCollection;
use monad_secp::PubKey;
use monad_consensus_types::checkpoint::Checkpoint;

fn main() {
    let forkpoint_toml = r#"
root = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[high_qc]
signatures = "0x0a3ba4656f72646572736269747665633a3a6f726465723a3a4c7362306468656164a26577696474680865696e6465780064626974730064646174618012c001400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
signature_hash = "0xc4790cb8377454f7e88f3e0edff7e70119cdfe972209f5fbc05d16fcfc19fa6f"

[high_qc.info.vote]
ledger_commit_info = "NoCommit"

[high_qc.info.vote.vote_info]
id = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
epoch = 1
round = 0
parent_id = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
parent_round = 0
seq_num = "0"
timestamp = 0

[high_qc.info.vote.vote_info.version]
protocol_version = 1
client_version_maj = 0
client_version_min = 1
hash_version = 1
serialize_version = 1

[[validator_sets]]
epoch = 1
round = 0

[[validator_sets.validators]]
node_id = "0x03867f9709744698e0c140c309d93baf53655176b215931de52ff6f371101c02a9"
cert_pubkey = "0xacaf321539a7b5c7b90cfc20358a55937e131c4eff0a93494d20f6b8ff59a53a0cfd99315d9529dc21a0b12fd825e174"
stake = 1

[[validator_sets]]
epoch = 2

[[validator_sets.validators]]
node_id = "0x03867f9709744698e0c140c309d93baf53655176b215931de52ff6f371101c02a9"
cert_pubkey = "0xacaf321539a7b5c7b90cfc20358a55937e131c4eff0a93494d20f6b8ff59a53a0cfd99315d9529dc21a0b12fd825e174"
stake = 1
"#;

    let maybe_checkpoint = toml::from_str::<Checkpoint<BlsSignatureCollection<PubKey>>>(&forkpoint_toml);
    if let Err(err) = maybe_checkpoint {
        eprintln!("{err}")
    }
}
