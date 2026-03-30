use anyhow::Error;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub command: String,
    pub term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVote {
    pub term: u64,
    pub node_id: u16,
    pub last_log_idx: u64,
    pub last_log_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u16,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum RaftMessage {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
}


const RAFT_PORT: u16 = 6380;

/// Sends a Raft message to a peer and reads back the response.
pub async fn send_raft_message(message: &RaftMessage, node_id: u16) -> Result<RaftMessage, Error> {
    let addr = format!("node{node_id}:{RAFT_PORT}");
    let mut stream = TcpStream::connect(&addr).await?;

    let payload = serde_json::to_vec(message)?;
    stream.write_all(&payload).await?;
    stream.shutdown().await?;

    let mut response_buf = Vec::new();
    stream.read_to_end(&mut response_buf).await?;
    let response: RaftMessage = serde_json::from_slice(&response_buf)?;
    Ok(response)
}