use std::{cmp::min, sync::Arc, time::Instant};

use anyhow::{Error, Ok};

use crate::raft::{
    communication::{AppendEntriesResponse, LogEntry, RaftMessage, RequestVoteResponse},
    node::{Node, NodeState},
};

pub struct RaftServer {
    pub node: Arc<Node>,
}

impl RaftServer {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    pub async fn append_log(&self, command: &str) -> Result<(), Error> {
        let mut ps = self.node.persistent_state.lock().await;
        let entry = LogEntry {
            index: ps.log.last().map(|e| e.index).unwrap_or(0) + 1,
            term: ps.cur_term,
            command: command.to_string(),
        };
        ps.log.push(entry);

        Ok(())
    }

    pub async fn process_request(&self, raft_request: RaftMessage) -> Result<RaftMessage, Error> {
        match raft_request {
            RaftMessage::RequestVote(req) => {
                let mut ps = self.node.persistent_state.lock().await;

                // Step down if we see a higher term
                let mut step_down = false;
                if req.term > ps.cur_term {
                    ps.cur_term = req.term;
                    ps.voted_for = None;
                    step_down = true;
                }

                // Reject if candidate's term is stale
                if req.term < ps.cur_term {
                    let term = ps.cur_term;
                    drop(ps);
                    if step_down {
                        *self.node.state.lock().await = NodeState::Follower;
                    }
                    return Ok(RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term,
                        vote_granted: false,
                    }));
                }

                // Check voted_for: haven't voted, or already voted for this candidate
                let can_vote = ps.voted_for.is_none() || ps.voted_for == Some(req.node_id);

                // Log freshness (Raft §5.4.1): compare terms first, then index
                let my_last_term = ps.log.last().map(|e| e.term).unwrap_or(0);
                let my_last_idx = ps.log.last().map(|e| e.index).unwrap_or(0);
                let log_ok = req.last_log_term > my_last_term
                    || (req.last_log_term == my_last_term && req.last_log_idx >= my_last_idx);

                let response = if can_vote && log_ok {
                    ps.voted_for = Some(req.node_id);
                    RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term: ps.cur_term,
                        vote_granted: true,
                    })
                } else {
                    RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term: ps.cur_term,
                        vote_granted: false,
                    })
                };

                let _term = ps.cur_term;
                drop(ps);
                if step_down {
                    *self.node.state.lock().await = NodeState::Follower;
                }

                Ok(response)
            }

            RaftMessage::AppendEntries(req) => {
                let mut ps = self.node.persistent_state.lock().await;

                let mut step_down = false;
                if req.term > ps.cur_term {
                    ps.cur_term = req.term;
                    ps.voted_for = None;
                    step_down = true;
                }

                if req.term < ps.cur_term {
                    let term = ps.cur_term;
                    drop(ps);
                    if step_down {
                        *self.node.state.lock().await = NodeState::Follower;
                    }
                    return Ok(RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                        term,
                        success: false,
                    }));
                }

                let term = ps.cur_term;

                // Valid heartbeat — reset election timer and step down to Follower
                *self.node.last_heartbeat.lock().await = Instant::now();
                *self.node.state.lock().await = NodeState::Follower;

                if ps.log.len() as u64 >= req.prev_log_index {
                    if req.prev_log_index == 0
                        || (ps.log[(req.prev_log_index as usize) - 1].term == req.prev_log_term)
                    {
                        for entry in req.entries {
                            let idx = entry.index as usize - 1;
                            if idx < ps.log.len() && ps.log[idx].term != entry.term {
                                ps.log.truncate(idx);
                                if idx >= ps.log.len() {
                                    ps.log.push(entry);
                                }
                            }
                        }

                        let mut vol_state = self.node.volatile_state.lock().await;

                        if req.leader_commit > vol_state.commit_idx {
                            vol_state.commit_idx = min(
                                req.leader_commit,
                                ps.log.last().map(|e| e.index).unwrap_or(0),
                            )
                        }

                        Ok(RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                            term,
                            success: true,
                        }))
                    } else {
                        Ok(RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                            term,
                            success: false,
                        }))
                    }
                } else {
                    Ok(RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                        term,
                        success: false,
                    }))
                }
            }

            // Responses are handled by the sender, not the server
            _ => anyhow::bail!("unexpected message type in server handler"),
        }
    }
}
