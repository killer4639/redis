use std::num::ParseIntError;
use std::time::Duration;

use rand::Rng;

pub const ELECTION_TIMEOUT_MIN: u64 = 150;
pub const ELECTION_TIMEOUT_MAX: u64 = 300;
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

pub fn random_election_timeout() -> Duration {
    let millis = rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX);
    Duration::from_millis(millis)
}

pub fn parse_peers(peers_string: &str) -> Result<Vec<u16>, ParseIntError> {
    peers_string
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<u16>())
        .collect()
}
