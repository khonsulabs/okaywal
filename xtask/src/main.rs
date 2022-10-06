use khonsu_tools::{
    universal::{anyhow, clap::Parser, DefaultConfig},
    Commands,
};

fn main() -> anyhow::Result<()> {
    Commands::parse().execute::<Config>()
}

enum Config {}

impl khonsu_tools::Config for Config {
    type Publish = Self;
    type Universal = DefaultConfig;
}

impl khonsu_tools::publish::Config for Config {
    fn paths() -> Vec<String> {
        vec![String::from(".")]
    }
}
