mod cluster;
mod command;
mod engine;
mod memory;
mod message_bus;
mod node;
mod resp;
mod utils;
mod metrics;

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Instant,
    u64,
};

use anyhow::{Context, Result};
use little_raft::replica::Replica;

use crate::{
    cluster::{MessageType, RedisCluster, RedisTransition},
    command::Command,
    engine::Engine,
    message_bus::MessageBus,
    metrics::{ConnectionGuard},
    node::Redis,
    resp::RespValue,
    utils::{
        ELECTION_TIMEOUT_MAX, ELECTION_TIMEOUT_MIN, HEARTBEAT_INTERVAL, UNIQUE_ID, load_config,
    },
};

const REDIS_ADDR: &str = "0.0.0.0:6379";
const RAFT_ADDR: &str = "0.0.0.0:6380";

/// Handles a single client connection.
/// Reads commands in a loop until the client disconnects.
fn handle_redis_connection(
    mut socket: TcpStream,
    message_bus: Arc<MessageBus<RedisTransition>>,
    engine: Arc<Engine>,
    node: Arc<Mutex<Redis>>,
    node_id: usize,
    leader_id: Arc<Mutex<Option<usize>>>,
) {
    let peer = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("unknown".into());
    tlog!("Redis Client connected: {peer}");
    let _conn_guard = ConnectionGuard::new();

    let mut buf = [0u8; 512];
    loop {
        let bytes_read = match socket.read(&mut buf) {
            Ok(0) => break, // client disconnected
            Ok(n) => n,
            Err(_) => break, // read error, drop connection
        };

        let command = String::from_utf8(buf[..bytes_read].to_vec()).unwrap();
        let command_id = (node_id as u64 * 100000
            + UNIQUE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

        let Some(parsed) = Command::parse_str(&command) else {
            let response = RespValue::Error("Invalid command format".to_string());
            let _ = socket.write_all(response.to_string().as_bytes());
            continue;
        };

        metrics::REDIS_COMMANDS_TOTAL
            .with_label_values(&[parsed.name()])
            .inc();

        if parsed.is_write_command() {
            metrics::REDIS_WRITE_COMMANDS.inc();

            // Redirect to leader if we're not the leader
            let current_leader = *leader_id.lock().unwrap();
            if current_leader != Some(node_id) {
                metrics::REDIS_REDIRECTS.inc();
                let response = match current_leader {
                    Some(lid) => RespValue::Error(format!("MOVED node{lid}:6379")),
                    None => RespValue::Error("CLUSTERDOWN no leader elected".to_string()),
                };
                let _ = socket.write_all(response.to_string().as_bytes());
                continue;
            }

            let timer = Instant::now();
            let msg: RedisTransition = RedisTransition::new(command_id, command);

            let (tx, rx) = crossbeam_channel::unbounded();
            node.lock().unwrap().pending.insert(command_id, tx);
            message_bus.push(msg);

            let response = rx.recv().unwrap();
            metrics::REDIS_COMMAND_DURATION.observe(timer.elapsed().as_secs_f64());
            node.lock().unwrap().pending.remove(&command_id);
            socket.write_all(response.as_bytes()).unwrap();
        } else {
            metrics::REDIS_READ_COMMANDS.inc();

            let timer = Instant::now();
            let response = engine.execute(&command);
            metrics::REDIS_COMMAND_DURATION.observe(timer.elapsed().as_secs_f64());
            socket.write_all(response.as_bytes()).unwrap()
        }
    }

    tlog!("Redis Client disconnected: {peer}");
}

/// Handles a single raft connection.
/// Reads commands in a loop until the client disconnects.
fn handle_raft_connection(mut socket: TcpStream, message_bus: Arc<MessageBus<MessageType>>) {
    let peer = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("unknown".into());
    tlog!("Raft Client connected: {peer}");

    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let bytes_read = match socket.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        buf.extend_from_slice(&tmp[..bytes_read]);

        let msg: MessageType = match serde_json::from_slice(&buf) {
            Ok(msg) => msg,
            Err(_) => continue, // incomplete message, keep reading
        };

        buf.clear();
        message_bus.push(msg);
    }

    tlog!("Raft Client disconnected: {peer}");
}

/// Accept loop for Redis client connections.
fn serve_redis(
    message_bus: Arc<MessageBus<RedisTransition>>,
    engine: Arc<Engine>,
    node: Arc<Mutex<Redis>>,
    node_id: usize,
    leader_id: Arc<Mutex<Option<usize>>>,
) {
    let listener = TcpListener::bind(REDIS_ADDR)
        .with_context(|| format!("failed to bind Redis listener on {REDIS_ADDR}"))
        .unwrap();
    tlog!("Redis server listening on {REDIS_ADDR}");

    thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((socket, _)) => {
                    let message_bus = message_bus.clone();
                    let engine = engine.clone();
                    let node = node.clone();
                    let leader_id = leader_id.clone();
                    thread::spawn(move || {
                        handle_redis_connection(socket, message_bus, engine, node, node_id, leader_id)
                    });
                }
                Err(e) => tlog!("Failed to accept Redis connection: {e}"),
            }
        }
    });
}

/// Accept loop for Raft peer connections.
fn serve_raft(message_bus: Arc<MessageBus<MessageType>>) {
    let listener = TcpListener::bind(RAFT_ADDR)
        .with_context(|| format!("failed to bind Raft listener on {RAFT_ADDR}"))
        .unwrap();
    tlog!("Raft server listening on {RAFT_ADDR}");
    let message_bus = message_bus.clone();

    thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((socket, _)) => {
                    let message_bus = message_bus.clone();
                    thread::spawn(move || handle_raft_connection(socket, message_bus));
                }
                Err(e) => tlog!("Failed to accept Raft connection: {e}"),
            }
        }
    });
}

fn main() -> Result<()> {
    let (node_id, peers) = load_config()?;
    tlog!("[N{node_id}] starting with peers={peers:?}");
    let (raft_sender, raft_receiver) = crossbeam_channel::unbounded();
    let raft_message_bus = MessageBus::new(Mutex::new(Vec::new()), raft_sender);
    let raft_message_bus = Arc::new(raft_message_bus);

    let leader_id: Arc<Mutex<Option<usize>>> = Arc::new(Mutex::new(None));
    let cluster = Arc::new(Mutex::new(RedisCluster::new(node_id, raft_message_bus.clone(), leader_id.clone())));

    let (redis_sender, redis_receiver) = crossbeam_channel::unbounded();
    let redis_message_bus = MessageBus::new(Mutex::new(Vec::new()), redis_sender);
    let redis_message_bus = Arc::new(redis_message_bus);

    let engine = Arc::new(Engine::new());
    let node = Redis::new(engine.clone(), redis_message_bus.clone());
    let state_machine = Arc::new(Mutex::new(node));
    let noop_transition = RedisTransition::new(u64::MAX, "PING".to_string());

    serve_raft(raft_message_bus.clone());
    serve_redis(
        redis_message_bus.clone(),
        engine.clone(),
        state_machine.clone(),
        node_id,
        leader_id.clone(),
    );
    metrics::register_metrics();
    metrics::NODE_INFO.set(node_id as i64);
    metrics::serve_metrics();

    Replica::new(
        node_id,
        peers,
        cluster,
        state_machine,
        0 as usize,
        noop_transition,
        HEARTBEAT_INTERVAL,
        (ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX),
    )
    .start(raft_receiver, redis_receiver);

    Ok(())
}
