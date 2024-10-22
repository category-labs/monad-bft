pub trait Discovery {
    fn handle_discovery_message(&mut self);
}

pub struct NopDiscovery;
impl Discovery for NopDiscovery {
    fn handle_discovery_message(&mut self) {
        todo!()
    }
}
