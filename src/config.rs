use std::net::IpAddr;
use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    /// Hostname or IP address to bind.
    #[arg(long, default_value = "127.0.0.1")]
    pub hostname: IpAddr,

    /// Port to listen on. Use 0 to ask the OS for a free port.
    #[arg(long, short, default_value_t = 4096)]
    pub port: u16,

    /// Path to the pi binary. Defaults to PI_BIN_PATH, then ~/.local/bin/pi.
    #[arg(long, env = "PI_BIN_PATH")]
    pub pi_bin: Option<PathBuf>,

    /// Working directory assigned to new sessions.
    #[arg(long, env = "PI_SERVER_WORKDIR")]
    pub directory: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub hostname: IpAddr,
    pub port: u16,
    pub pi_bin: PathBuf,
    pub directory: PathBuf,
}

impl ServerConfig {
    pub fn from_cli(cli: Cli) -> anyhow::Result<Self> {
        let pi_bin = cli.pi_bin.unwrap_or_else(default_pi_bin);
        let directory = cli.directory.unwrap_or(std::env::current_dir()?);
        Ok(Self {
            hostname: cli.hostname,
            port: cli.port,
            pi_bin,
            directory,
        })
    }
}

fn default_pi_bin() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local/bin/pi")
}
