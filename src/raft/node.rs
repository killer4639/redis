use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::sync::watch::Sender;
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
    pub leader_volatile_state: Mutex<Option<LeaderVolatileState>>,
    pub current_leader: Mutex<Option<u16>>,
    pub tx: Sender<u64>,
}

impl Node {
    pub fn new(id: u16, peers: Vec<u16>, tx: Sender<u64>) -> Self {
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
            leader_volatile_state: Mutex::new(None),
            current_leader: Mutex::new(None),
            peers,
            tx,
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
            println!(
                "[N{} heartbeat] election timeout ({:.0?} >= {:.0?}), starting election",
                self.id, elapsed, timeout
            );

            match (LeaderElection {}).run_leader_election(self).await {
                Ok(true) => {
                    *self.state.lock().await = NodeState::Leader;
                    let mut leader_volatile_state = LeaderVolatileState {
                        next_idx: HashMap::new(),
                        match_idx: HashMap::new(),
                    };

                    let last_idx = self.get_last_log_idx().await;
                    for peer in &self.peers {
                        leader_volatile_state
                            .next_idx
                            .insert(peer.clone(), last_idx + 1);

                        leader_volatile_state.match_idx.insert(peer.clone(), 0);
                    }

                    let mut leader_vol_state_lock = self.leader_volatile_state.lock().await;
                    *leader_vol_state_lock = Some(leader_volatile_state);

                    *self.current_leader.lock().await = Some(self.id);

                    println!(
                        "[N{} election] became Leader for term {} (next_idx initialized to {})",
                        self.id,
                        self.persistent_state.lock().await.cur_term,
                        last_idx + 1
                    );
                }
                Ok(false) => println!("[N{} election] lost election", self.id),
                Err(e) => eprintln!("[N{} election] error: {e}", self.id),
            }
        }

        tokio::time::sleep(random_election_timeout()).await;
    }

    /// Sends empty AppendEntries (heartbeats) to all peers.
    /// Steps down if any peer responds with a higher term.
    async fn send_heartbeats(&self) {
        let ps = self.persistent_state.lock().await;
        let leader_state = self.leader_volatile_state.lock().await;
        let term = ps.cur_term;
        let commit_idx = self.volatile_state.lock().await.commit_idx;

        let mut tasks = JoinSet::new();
        for &peer in &self.peers {
            let next = *leader_state.as_ref().unwrap().next_idx.get(&peer).unwrap();
            let prev_log_index = next - 1;
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                ps.log[prev_log_index as usize - 1].term
            };
            let entries = ps.log[prev_log_index as usize..].to_vec();

            let last_sent_index = if entries.is_empty() {
                prev_log_index
            } else {
                entries.last().unwrap().index
            };

            println!(
                "[N{} leader] → N{peer}: prev=({prev_log_index},{prev_log_term}) entries={} commit={commit_idx}",
                self.id, entries.len()
            );

            let message = RaftMessage::AppendEntries(AppendEntries {
                term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_idx,
            });
            tasks.spawn(async move {
                (
                    peer,
                    last_sent_index,
                    send_raft_message(&message, peer).await,
                )
            });
        }

        let last_log_idx = ps.log.last().map(|e| e.index).unwrap_or(0);
        drop(ps);
        drop(leader_state);

        while let Some(result) = tasks.join_next().await {
            if let Ok((peer, last_sent, Ok(RaftMessage::AppendEntriesResponse(resp)))) = result {
                if resp.term > term {
                    let mut ps = self.persistent_state.lock().await;
                    ps.cur_term = resp.term;
                    ps.voted_for = None;
                    drop(ps);
                    *self.state.lock().await = NodeState::Follower;
                    println!(
                        "[N{} leader] stepped down — N{peer} has term {}",
                        self.id, resp.term
                    );
                    return;
                }

                let mut ls = self.leader_volatile_state.lock().await;
                let state = ls.as_mut().unwrap();
                if resp.success {
                    state.match_idx.insert(peer, last_sent);
                    state.next_idx.insert(peer, last_sent + 1);
                    println!(
                        "[N{} leader] ← N{peer}: success, match_idx={last_sent} next_idx={}",
                        self.id, last_sent + 1
                    );
                } else {
                    let next = state.next_idx.get(&peer).copied().unwrap_or(1);
                    let new_next = next.saturating_sub(1).max(1);
                    state.next_idx.insert(peer, new_next);
                    println!(
                        "[N{} leader] ← N{peer}: failed, next_idx {next} → {new_next}",
                        self.id
                    );
                }
            }
        }

        let ls = self.leader_volatile_state.lock().await;
        let state = ls.as_ref().unwrap();
        let mut all_match: Vec<u64> = state.match_idx.values().copied().collect();
        all_match.push(last_log_idx); // leader has its own entries
        all_match.sort_unstable_by(|a, b| b.cmp(a)); // descending

        let majority_pos = (self.peers.len() + 1) / 2; // e.g. 3 nodes → pos 1
        let candidate = all_match[majority_pos];

        let ps = self.persistent_state.lock().await;
        if candidate > commit_idx && ps.log[candidate as usize - 1].term == term {
            self.volatile_state.lock().await.commit_idx = candidate;
            self.tx.send(candidate).unwrap();
            println!(
                "[N{} leader] commit advanced: {} → {} (match_idx={:?})",
                self.id, commit_idx, candidate, all_match
            );
        }
    }
}
