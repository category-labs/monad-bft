use monad_types::{CounterCommand, Executor};
use std::collections::HashMap;

#[derive(Debug)]
pub struct CounterServiceCommand {
    pub key: String,
}

// implement executor trait for the counter service
pub struct CounterService {
    counters: HashMap<String, u32>,
}

impl Default for CounterService {
    fn default() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }
}

impl CounterService {
    pub fn new() -> Self {
        Self::default()
    }

    fn count(&mut self, key: &String) {
        let entry = self.counters.entry(key.to_string()).or_insert(0);
        *entry += 1;
    }

    fn print_count(&self) {
        let mut freq = self.counters.iter().collect::<Vec<_>>();
        freq.sort_by_key(|&(k, _v)| k);
        println!("Counter service: {:?}", freq);
    }
}

impl Executor for CounterService {
    type Command = CounterCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for cmd in commands {
            match cmd {
                CounterCommand::Increment { key } => self.count(&key),
            }
        }
    }
}

impl Drop for CounterService {
    fn drop(&mut self) {
        self.print_count()
    }
}
