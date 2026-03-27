use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use rand::Rng;

use crate::raft::communication::LogEntry;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct PersistentState {
    cur_term: u64,
    voted_for: Option<u16>,
    log: Vec<LogEntry>,
}

pub struct VolatileState {
    commit_idx: u64,
    last_applied: u64,
}

pub struct LeaderVolatileState {
    next_idx: HashMap<u16, u64>,
    match_idx: HashMap<u16, u64>,
}

pub struct Node {
    id: u16,
    peers: Vec<u16>,
    state: Arc<Mutex<NodeState>>,
    last_heartbeat: Arc<Mutex<Instant>>,
    polling_thread: JoinHandle<()>,
    persistent_state: PersistentState,
    volatile_state: VolatileState,
    leader_volatile_state: Option<LeaderVolatileState>,
}

impl Node {
    pub fn new(id: u16, peers: Vec<u16>) -> Self {
        let last_heartbeat = Arc::new(Mutex::new(Instant::now()));
        let state = Arc::new(Mutex::new(NodeState::Follower));
        let polling_heartbeat = Arc::clone(&last_heartbeat);
        let cur_state = Arc::clone(&state);

        let polling_thread = thread::spawn(move || {
            let mut rng = rand::thread_rng();

            loop {
                let poll_interval_ms = rng.gen_range(150..=300);
                thread::sleep(Duration::from_millis(poll_interval_ms));

                let elapsed = polling_heartbeat
                    .lock()
                    .expect("polling thread mutex poisoned")
                    .elapsed();

                if elapsed.is_zero() {
                    let mut locked_state = cur_state.lock().expect("state mutex poisoned");
                    *locked_state = NodeState::Candidate;
                }

                println!(
                    "Polling node after {}ms since last heartbeat",
                    elapsed.as_millis()
                );
            }
        });

        Self {
            id,
            state,
            last_heartbeat,
            polling_thread,
            persistent_state: PersistentState {
                cur_term: 0,
                voted_for: None,
                log: Vec::new(),
            },
            volatile_state: VolatileState {
                commit_idx: 0,
                last_applied: 0,
            },
            leader_volatile_state: None,
            peers
        }
    }
}