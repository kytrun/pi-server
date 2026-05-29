#![forbid(unsafe_code)]

use clap::Parser;
use pi_server::config::{Cli, ServerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = ServerConfig::from_cli(Cli::parse())?;
    pi_server::server::serve(config).await?;
    Ok(())
}
