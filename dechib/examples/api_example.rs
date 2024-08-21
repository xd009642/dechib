use dechib_api::api::launch_server;
use dechib_core::{setup_logging, Instance};

fn main() {
    setup_logging();

    let instance = Instance::new();
    launch_server(instance).expect("error while starting api server");
}
