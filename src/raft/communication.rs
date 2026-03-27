use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    index: u64,
    command: String,
    term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVote {
    term: u64,
    node_id: u16,
    last_log_idx: u64,
    last_log_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RequestVoteResponse {
    term: u64,
    vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntries {
    term: u64,
    leader_id: u16,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResponse {
    term: u64,
    success: bool,
}
