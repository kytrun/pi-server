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

    /// SQLite database path for project/session storage.
    #[arg(long, env = "PI_SERVER_DB")]
    pub database: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub hostname: IpAddr,
    pub port: u16,
    pub pi_bin: PathBuf,
    pub directory: PathBuf,
    pub database: PathBuf,
}

impl ServerConfig {
    pub fn from_cli(cli: Cli) -> anyhow::Result<Self> {
        let pi_bin = cli.pi_bin.unwrap_or_else(default_pi_bin);
        let directory = cli.directory.unwrap_or(std::env::current_dir()?);
        let database = cli.database.unwrap_or_else(default_database);
        Ok(Self {
            hostname: cli.hostname,
            port: cli.port,
            pi_bin,
            directory,
            database,
        })
    }
}

fn default_pi_bin() -> PathBuf {
    if cfg!(windows) {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("pi/pi.exe")
    } else {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".local/bin/pi")
    }
}

fn default_database() -> PathBuf {
    default_data_dir().join("pi-server.db")
}

fn default_data_dir() -> PathBuf {
    if cfg!(windows) {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("pi-server")
    } else {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".local/share/pi-server")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_database_lives_under_local_share_pi_server() {
        assert_eq!(default_database(), default_data_dir().join("pi-server.db"));
        assert!(default_database().ends_with(".local/share/pi-server/pi-server.db"));
    }
}
