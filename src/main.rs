mod command;
mod memory;
mod raft;
mod resp;
mod utils;

use std::{env, sync::Arc};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    command::Command,
    memory::Store,
    raft::{communication::RaftMessage, node::Node, server::RaftServer},
    resp::RespValue,
    utils::parse_peers,
};

const REDIS_ADDR: &str = "0.0.0.0:6379";
const RAFT_ADDR: &str = "0.0.0.0:6380";

pub struct Server {
    pub redis: Arc<Store>,
    pub raft: Arc<RaftServer>,
}

impl Server {
    pub fn new(redis: Arc<Store>, raft: Arc<RaftServer>) -> Self {
        Self { redis, raft }
    }
}

/// Handles a single client connection.
/// Reads commands in a loop until the client disconnects.
async fn handle_redis_connection(mut socket: TcpStream, server: Arc<Server>) {
    let peer = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("unknown".into());
    tlog!("Redis Client connected: {peer}");

    let mut buf = [0u8; 512];
    loop {
        let bytes_read = match socket.read(&mut buf).await {
            Ok(0) => break, // client disconnected
            Ok(n) => n,
            Err(_) => break, // read error, drop connection
        };

        let response = process_redis_request(&buf[..bytes_read], &server).await;
        if socket
            .write_all(response.to_string().as_bytes())
            .await
            .is_err()
        {
            break; // write error, drop connection
        }
    }

    tlog!("Redis Client disconnected: {peer}");
}

/// Handles a single raft connection.
/// Reads commands in a loop until the client disconnects.
async fn handle_raft_connection(mut socket: TcpStream, raft_server: Arc<RaftServer>) {
    let peer = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("unknown".into());
    tlog!("Raft Client connected: {peer}");

    let mut buf = [0u8; 512];
    loop {
        let bytes_read = match socket.read(&mut buf).await {
            Ok(0) => break, // client disconnected
            Ok(n) => n,
            Err(_) => break, // read error, drop connection
        };

        let raft_message: RaftMessage = match serde_json::from_slice(&buf[..bytes_read]) {
            Ok(msg) => msg,
            Err(e) => {
                tlog!("Invalid Raft message from {peer}: {e}");
                continue;
            }
        };

        let response = match raft_server.process_request(raft_message).await {
            Ok(resp) => resp,
            Err(e) => {
                tlog!("Error processing Raft message from {peer}: {e}");
                continue;
            }
        };
        if socket
            .write_all(&serde_json::to_vec(&response).unwrap())
            .await
            .is_err()
        {
            break; // write error, drop connection
        }
    }

    tlog!("Raft Client disconnected: {peer}");
}

/// Parses and executes a single RESP request.
async fn process_redis_request(buf: &[u8], server: &Server) -> RespValue {
    let request = RespValue::deserialize(buf);

    let RespValue::Array(Some(items)) = request else {
        return RespValue::err("invalid command format");
    };

    if items.is_empty() {
        return RespValue::err("empty command");
    }

    let Some(command_name) = items[0].as_str() else {
        return RespValue::err("invalid command name");
    };

    let Some(command) = Command::parse(command_name) else {
        return RespValue::err(&format!("unknown command '{command_name}'"));
    };

    // items[0] is the command name, the rest are arguments
    command.execute(&items[1..], server).await
}

/// Parse node configuration from environment variables.
fn load_config() -> Result<(u16, Vec<u16>)> {
    let node_id: u16 = env::var("NODE_ID")
        .context("NODE_ID env variable must be set")?
        .parse()
        .context("NODE_ID must be a valid u16")?;

    let peers = env::var("PEERS").context("PEERS env variable must be set")?;
    let peers = parse_peers(&peers).context("PEERS must be comma-separated u16 values")?;

    Ok((node_id, peers))
}

/// Accept loop for Redis client connections.
async fn serve_redis(server: Arc<Server>) -> Result<()> {
    let listener = TcpListener::bind(REDIS_ADDR)
        .await
        .with_context(|| format!("failed to bind Redis listener on {REDIS_ADDR}"))?;
    tlog!("Redis server listening on {REDIS_ADDR}");

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(handle_redis_connection(socket, server.clone()));
            }
            Err(e) => tlog!("Failed to accept Redis connection: {e}"),
        }
    }
}

/// Accept loop for Raft peer connections.
async fn serve_raft(server: Arc<Server>) -> Result<()> {
    let listener = TcpListener::bind(RAFT_ADDR)
        .await
        .with_context(|| format!("failed to bind Raft listener on {RAFT_ADDR}"))?;
    tlog!("Raft server listening on {RAFT_ADDR}");

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(handle_raft_connection(socket, server.raft.clone()));
            }
            Err(e) => tlog!("Failed to accept Raft connection: {e}"),
        }
    }
}

/// Periodic heartbeat checker for the Raft node.
async fn run_heartbeat_loop(node: Arc<Node>) -> Result<()> {
    loop {
        node.check_heartbeat().await;
    }
}

async fn apply_committed_entries(
    mut rx: tokio::sync::watch::Receiver<u64>,
    server: Arc<Server>,
) -> Result<()> {
    loop {
        rx.changed().await?;
        let commit_idx = *rx.borrow();
        let node = &server.raft.node;

        let applied_idx = node.volatile_state.lock().await.last_applied;
        let ps = node.persistent_state.lock().await;
        let range_len = commit_idx as usize - applied_idx as usize;

        if range_len > 0 {
            tlog!(
                "[N{} apply] applying entries {}..{} ({} entries)",
                node.id, applied_idx + 1, commit_idx, range_len
            );
        }

        for entry in &ps.log[applied_idx as usize..commit_idx as usize] {
            let parts: Vec<&str> = entry.command.split_whitespace().collect();
            if let Some(command) = Command::parse(parts[0]) {
                let args: Vec<RespValue> = parts[1..]
                    .iter()
                    .map(|s| RespValue::BulkString(Some(s.to_string())))
                    .collect();
                let _ = command.execute_inner(&args, &server.redis);
                tlog!(
                    "[N{} apply] applied index={} \"{}\"",
                    node.id, entry.index, entry.command
                );
            }
        }
        drop(ps);

        node.volatile_state.lock().await.last_applied = commit_idx;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (node_id, peers) = load_config()?;
    tlog!("[N{node_id}] starting with peers={peers:?}");

    let (tx, rx) = tokio::sync::watch::channel(0u64);

    let store = Arc::new(Store::new());
    let node = Arc::new(Node::new(node_id, peers, tx));
    let raft_server = Arc::new(RaftServer::new(node.clone()));
    let server = Arc::new(Server::new(store.clone(), raft_server));

    tokio::try_join!(
        serve_redis(server.clone()),
        serve_raft(server.clone()),
        run_heartbeat_loop(node.clone()),
        apply_committed_entries(rx, server.clone())
    )?;

    Ok(())
}
