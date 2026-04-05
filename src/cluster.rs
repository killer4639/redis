use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use little_raft::message::Message;
use little_raft::replica::ReplicaID;
use little_raft::{cluster::Cluster, state_machine::StateMachineTransition};
use serde::{Deserialize, Serialize};

use crate::message_bus::MessageBus;
use crate::utils::RPC_TIMEOUT;

const RAFT_PORT: u16 = 6380;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisTransition {
    id: u64,         // unique identifier
    pub command: String, // e.g. "SET foo bar"
}

impl RedisTransition {
    pub fn new(id: u64, command: String) -> Self {
        Self { id, command }
    }
}

impl StateMachineTransition for RedisTransition {
    type TransitionID = u64;

    fn get_id(&self) -> u64 {
        self.id
    }
}

pub type MessageType = Message<RedisTransition, Vec<u8>>;

pub struct RedisCluster {
    message_bus: Arc<MessageBus<MessageType>>,
    halted: Arc<AtomicBool>,
    leader_id: Arc<Mutex<Option<ReplicaID>>>,
}

impl RedisCluster {
    pub fn new(
        message_bus: Arc<MessageBus<MessageType>>,
        leader_id: Arc<Mutex<Option<ReplicaID>>>,
    ) -> Self {
        let halted = Arc::new(AtomicBool::new(false));
        Self {
            message_bus,
            halted,
            leader_id,
        }
    }
}

impl Cluster<RedisTransition, Vec<u8>> for RedisCluster {
    fn send_message(&mut self, node_id: usize, message: MessageType) {
        let addr = format!("node{node_id}:{RAFT_PORT}");

        let Ok(mut stream) = TcpStream::connect(&addr) else {
            return;
        };
        stream.set_write_timeout(Some(RPC_TIMEOUT)).ok();
        stream.set_read_timeout(Some(RPC_TIMEOUT)).ok();

        let Ok(payload) = serde_json::to_vec(&message) else {
            return;
        };
        if stream.write_all(&payload).is_err() {
            return;
        }
    }

    fn receive_messages(&mut self) -> Vec<MessageType> {
        self.message_bus.drain()
    }

    fn halt(&self) -> bool {
        self.halted.load(Ordering::Relaxed)
    }

    fn register_leader(&mut self, leader_id: Option<ReplicaID>) {
        *self.leader_id.lock().unwrap() = leader_id;
    }
}
