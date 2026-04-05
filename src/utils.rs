use anyhow::{Context, Result};
use std::{env, num::ParseIntError, sync::atomic::AtomicU64, time::Duration};

pub const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(15000);
pub const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(30000);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5000);

pub const RPC_TIMEOUT: Duration = Duration::from_secs(2);

pub fn timestamp() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let total_secs = d.as_secs();
    let h = (total_secs / 3600) % 24;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    let ms = d.subsec_millis();
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

/// Timestamped, flush-on-every-line logging macro.
/// Fixes Docker's block-buffered stdout so logs stream continuously.
#[macro_export]
macro_rules! tlog {
    ($($arg:tt)*) => {{
        use std::io::Write;
        print!("{} ", $crate::utils::timestamp());
        println!($($arg)*);
        let _ = std::io::stdout().flush();
    }};
}

pub fn parse_peers(peers_string: &str) -> Result<Vec<usize>, ParseIntError> {
    peers_string
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<usize>())
        .collect()
}

/// Parse node configuration from environment variables.
pub fn load_config() -> Result<(usize, Vec<usize>)> {
    let node_id: usize = env::var("NODE_ID")
        .context("NODE_ID env variable must be set")?
        .parse()
        .context("NODE_ID must be a valid u16")?;

    let peers = env::var("PEERS").context("PEERS env variable must be set")?;
    let peers = parse_peers(&peers).context("PEERS must be comma-separated u16 values")?;

    Ok((node_id, peers))
}

pub const UNIQUE_ID: AtomicU64 = AtomicU64::new(0);
