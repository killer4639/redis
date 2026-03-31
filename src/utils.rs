use std::num::ParseIntError;
use tokio::time::{Duration};

use rand::Rng;

pub const ELECTION_TIMEOUT_MIN: u64 = 15000;
pub const ELECTION_TIMEOUT_MAX: u64 = 30000;
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5000);

pub const RPC_TIMEOUT: Duration = Duration::from_secs(2);

pub fn random_election_timeout() -> Duration {
    let millis = rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX);
    Duration::from_millis(millis)
}

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

pub fn parse_peers(peers_string: &str) -> Result<Vec<u16>, ParseIntError> {
    peers_string
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<u16>())
        .collect()
}
