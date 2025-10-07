use monad_block_persist::FileBlockPersist;
use monad_node_config::{ExecutionProtocolType, SignatureCollectionType, SignatureType};

use crate::prelude::*;

pub fn bft_iter() {
    let ledger_path = std::path::PathBuf::from("ledger");
    let bft_block_persist =
        FileBlockPersist::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::new(
            ledger_path,
        );

    
}
