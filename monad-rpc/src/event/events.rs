use std::sync::Arc;

use crate::{eth_json_types::MonadNotification, serialize::SharedJsonSerialized};

type Header = SharedJsonSerialized<alloy_rpc_types::eth::Header>;
type Log = SharedJsonSerialized<alloy_rpc_types::eth::Log>;
type Block =
    SharedJsonSerialized<alloy_rpc_types::eth::Block<alloy_rpc_types::Transaction, Header>>;

#[derive(Clone, Debug)]
pub enum EventServerEvent {
    Gap,

    Block {
        header: SharedJsonSerialized<MonadNotification<Header>>,
        block: SharedJsonSerialized<MonadNotification<Block>>,
        logs: Arc<Vec<SharedJsonSerialized<MonadNotification<Log>>>>,
    },
}

#[cfg(test)]
mod test {
    use crate::event::EventServerEvent;

    #[test]
    fn size() {
        assert_eq!(std::mem::size_of::<EventServerEvent>(), 24);
    }
}
