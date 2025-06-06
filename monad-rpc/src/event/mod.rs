pub(super) use self::events::EventServerEvent;
pub use self::{
    client::{EventServerClient, EventServerClientError},
    server::EventServer,
};

const BROADCAST_CHANNEL_SIZE: usize = 1024 * 10;

mod client;
mod events;
mod server;
