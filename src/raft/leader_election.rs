use std::time::Instant;

use anyhow::Error;
use tokio::task::JoinSet;

use crate::raft::{
    communication::{send_raft_message, RaftMessage, RequestVote},
    node::{Node, NodeState},
};

pub struct LeaderElection {}

impl LeaderElection {
    /// Increments term, votes for self, sends RequestVote to all peers
    /// concurrently, and returns whether we won a majority.
    pub async fn run_leader_election(&self, node: &Node) -> Result<bool, Error> {
        // Prepare: increment term, vote for self, snapshot log state
        let (term, last_log_idx, last_log_term) = {
            let mut ps = node.persistent_state.lock().await;
            ps.cur_term += 1;
            ps.voted_for = Some(node.id);
            let idx = ps.log.last().map(|e| e.index).unwrap_or(0);
            let t = ps.log.last().map(|e| e.term).unwrap_or(0);
            (ps.cur_term, idx, t)
        };

        // Reset election timer
        *node.last_heartbeat.lock().await = Instant::now();

        // Send RequestVote RPCs to all peers concurrently
        let mut tasks = JoinSet::new();
        for &peer in &node.peers {
            let message = RaftMessage::RequestVote(RequestVote {
                term,
                node_id: node.id,
                last_log_idx,
                last_log_term,
            });
            tasks.spawn(async move { send_raft_message(&message, peer).await });
        }

        // Count votes (starting at 1 for self-vote)
        let mut votes: usize = 1;
        let majority = (node.peers.len() + 1) / 2 + 1;

        while let Some(result) = tasks.join_next().await {
            if let Ok(Ok(RaftMessage::RequestVoteResponse(resp))) = result {
                // Step down if a peer has a higher term
                if resp.term > term {
                    let mut ps = node.persistent_state.lock().await;
                    ps.cur_term = resp.term;
                    ps.voted_for = None;
                    drop(ps);
                    *node.state.lock().await = NodeState::Follower;
                    println!(
                        "Node {} — aborting election, saw higher term {}",
                        node.id, resp.term
                    );
                    tasks.abort_all();
                    return Ok(false);
                }
                if resp.vote_granted && resp.term == term {
                    votes += 1;
                }
            }
        }

        Ok(votes >= majority)
    }
}
