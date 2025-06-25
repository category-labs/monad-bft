pub mod ipc;

use tracing::Subscriber;
use tracing_subscriber::{reload::Handle, EnvFilter};

pub trait TracingReload: Send {
    fn reload(&self, filter: EnvFilter) -> Result<(), Box<dyn std::error::Error>>;
}

impl<S: Subscriber> TracingReload for Handle<EnvFilter, S> {
    fn reload(&self, filter: EnvFilter) -> Result<(), Box<dyn std::error::Error>> {
        self.reload(filter)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
}

#[cfg(test)]
mod tests {}
