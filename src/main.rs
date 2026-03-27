mod cache;
mod command;
mod memory;
mod raft;
mod resp;
mod utils;

use std::{env, sync::Arc};

use anyhow::Result;
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{command::Command, raft::server::RaftServer};
use crate::{
    memory::Store,
    raft::node::{self, Node},
    utils::parse_peers,
};
use crate::{raft::communication::RequestVoteResponse, resp::RespValue};

/// Handles a single client connection.
/// Reads commands in a loop until the client disconnects.
async fn handle_redis_connection(mut socket: TcpStream, store: Arc<Store>) {
    let peer = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("unknown".into());
    println!("Redis Client connected: {peer}");

    let mut buf = [0u8; 512];
    loop {
        let bytes_read = match socket.read(&mut buf).await {
            Ok(0) => break, // client disconnected
            Ok(n) => n,
            Err(_) => break, // read error, drop connection
        };

        let response = process_request(&buf[..bytes_read], &store);
        if socket
            .write_all(response.to_string().as_bytes())
            .await
            .is_err()
        {
            break; // write error, drop connection
        }
    }

    println!("Redis Client disconnected: {peer}");
}

/// Handles a single raft connection.
/// Reads commands in a loop until the client disconnects.
async fn handle_raft_connection(mut socket: TcpStream, raft_server: Arc<RaftServer>) {
    let peer = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("unknown".into());
    println!("Raft Client connected: {peer}");

    let mut buf = [0u8; 512];
    loop {
        let bytes_read = match socket.read(&mut buf).await {
            Ok(0) => break, // client disconnected
            Ok(n) => n,
            Err(_) => break, // read error, drop connection
        };

        let response = RequestVoteResponse::default();
        if socket
            .write_all(serde_json::to_string(&response).unwrap().as_bytes())
            .await
            .is_err()
        {
            break; // write error, drop connection
        }
    }

    println!("Raft Client disconnected: {peer}");
}

/// Parses and executes a single RESP request.
fn process_request(buf: &[u8], store: &Store) -> RespValue {
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
    command.execute(&items[1..], store)
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "0.0.0.0:6379";
    let redis_listener = TcpListener::bind(addr).await?;
    println!("Redis server listening on {addr}");

    let store = Arc::new(Store::new());
    tokio::spawn(async move {
        loop {
            let (socket, _) = redis_listener.accept().await.unwrap();
            let store = store.clone();
            tokio::spawn(handle_redis_connection(socket, store));
        }
    });

    let node_id: u16 = env::var("NODE_ID")
        .expect("NODE_ID env variable must be set")
        .parse()
        .expect("NODE_ID must be a valid u16");

    println!("Starting node {node_id}");

    let peers = env::var("PEERS").expect("Peers expected");
    let peers = parse_peers(&peers).unwrap();
    let cur_node = Node::new(node_id, peers);

    let raft_addr = "0.0.0.0:6380";
    let raft_listener = TcpListener::bind(raft_addr).await?;
    let raft_server = Arc::new(RaftServer::new(cur_node));
    loop {
        let (socket, _) = raft_listener.accept().await?;
        let raft_server = raft_server.clone();
        tokio::spawn(handle_raft_connection(socket, raft_server));
    }
}
