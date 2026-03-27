use std::num::ParseIntError;

pub fn parse_peers(peers_string: &str) -> Result<Vec<u16>, ParseIntError> {
    peers_string
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<u16>())
        .collect()
}
