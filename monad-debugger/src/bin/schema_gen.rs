use std::{fs::File, io::Write};

use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use monad_debugger::GraphQLRoot;

fn main() {
    let schema = Schema::new(GraphQLRoot, EmptyMutation, EmptySubscription);
    let mut f = File::create("pkg/schema.graphql").expect("Unable to create file");
    f.write_all(schema.sdl().as_bytes())
        .expect("Unable to write data");
}
