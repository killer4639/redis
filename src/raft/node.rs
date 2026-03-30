use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::{sync::Mutex, task::JoinSet};

use crate::{
    raft::{
        communication::{AppendEntries, LogEntry, RaftMessage, send_raft_message},
        leader_election::LeaderElection,
    },
    utils::{HEARTBEAT_INTERVAL, random_election_timeout},
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct PersistentState {
    pub cur_term: u64,
    pub voted_for: Option<u16>,
    pub log: Vec<LogEntry>,
}

pub struct VolatileState {
    pub commit_idx: u64,
    pub last_applied: u64,
}

pub struct LeaderVolatileState {
    pub next_idx: HashMap<u16, u64>,
    pub match_idx: HashMap<u16, u64>,
}

pub struct Node {
    pub id: u16,
    pub peers: Vec<u16>,
    pub state: Arc<Mutex<NodeState>>,
    pub last_heartbeat: Arc<Mutex<Instant>>,
    pub persistent_state: Mutex<PersistentState>,
    pub volatile_state: Mutex<VolatileState>,
    pub leader_volatile_state: Option<LeaderVolatileState>,
}

impl Node {
    pub fn new(id: u16, peers: Vec<u16>) -> Self {
        let last_heartbeat = Arc::new(Mutex::new(Instant::now()));
        let state = Arc::new(Mutex::new(NodeState::Follower));

        Self {
            id,
            state,
            last_heartbeat,
            persistent_state: Mutex::new(PersistentState {
                cur_term: 0,
                voted_for: None,
                log: Vec::new(),
            }),
            volatile_state: Mutex::new(VolatileState {
                commit_idx: 0,
                last_applied: 0,
            }),
            leader_volatile_state: None,
            peers,
        }
    }

    pub async fn append_log(&self, log: LogEntry) {
        self.persistent_state.lock().await.log.push(log);
    }

    pub async fn get_cur_term(&self) -> u64 {
        self.persistent_state.lock().await.cur_term
    }

    pub async fn get_last_log_idx(&self) -> u64 {
        self.persistent_state
            .lock()
            .await
            .log
            .last()
            .map(|e| e.index)
            .unwrap_or(0)
    }

    pub async fn get_last_log_term(&self) -> u64 {
        self.persistent_state
            .lock()
            .await
            .log
            .last()
            .map(|e| e.term)
            .unwrap_or(0)
    }

    pub async fn check_heartbeat(&self) {
        let is_leader = matches!(*self.state.lock().await, NodeState::Leader);

        if is_leader {
            self.send_heartbeats().await;
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            return;
        }

        let elapsed = self.last_heartbeat.lock().await.elapsed();
        let timeout = random_election_timeout();

        if elapsed >= timeout {
            *self.state.lock().await = NodeState::Candidate;
            println!("Node {} — election timeout, starting election", self.id);

            match (LeaderElection {}).run_leader_election(self).await {
                Ok(true) => {
                    *self.state.lock().await = NodeState::Leader;
                    println!("Node {} won election — became Leader", self.id);
                }
                Ok(false) => println!("Node {} lost election", self.id),
                Err(e) => eprintln!("Node {} election error: {e}", self.id),
            }
        }

        tokio::time::sleep(random_election_timeout()).await;
    }

    /// Sends empty AppendEntries (heartbeats) to all peers.
    /// Steps down if any peer responds with a higher term.
    async fn send_heartbeats(&self) {
        let term = self.persistent_state.lock().await.cur_term;

        let mut tasks = JoinSet::new();
        for &peer in &self.peers {
            let message = RaftMessage::AppendEntries(AppendEntries {
                term,
                leader_id: self.id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0, // TODO: use actual commit index
            });
            tasks.spawn(async move { send_raft_message(&message, peer).await });
        }

        while let Some(result) = tasks.join_next().await {
            if let Ok(Ok(RaftMessage::AppendEntriesResponse(resp))) = result {
                if resp.term > term {
                    let mut ps = self.persistent_state.lock().await;
                    ps.cur_term = resp.term;
                    ps.voted_for = None;
                    drop(ps);
                    *self.state.lock().await = NodeState::Follower;
                    println!(
                        "Node {} stepped down — saw higher term {}",
                        self.id, resp.term
                    );
                    return;
                }
            }
        }
    }
}
