use crate::tlog;
use std::{cmp::min, sync::Arc, time::Instant};

use anyhow::Error;

use crate::raft::{
    communication::{
        AppendEntries, AppendEntriesResponse, LogEntry, RaftMessage, RequestVote,
        RequestVoteResponse,
    },
    node::{Node, NodeState, PersistentState},
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
        let cur_term = ps.cur_term.clone();
        let index = ps.log.last().map(|e| e.index).unwrap_or(0) + 1;
        tlog!(
            "[N{} raft] append_log: index={} term={} cmd=\"{}\"",
            self.node.id, index, cur_term, command
        );
        ps.log.push(LogEntry {
            index,
            term: cur_term,
            command: command.to_string(),
        });
        Ok(())
    }

    pub async fn process_request(&self, message: RaftMessage) -> Result<RaftMessage, Error> {
        match message {
            RaftMessage::RequestVote(req) => self.handle_request_vote(req).await,
            RaftMessage::AppendEntries(req) => self.handle_append_entries(req).await,
            _ => anyhow::bail!("unexpected message type"),
        }
    }

    // ── RequestVote ──────────────────────────────────────────────

    async fn handle_request_vote(&self, req: RequestVote) -> Result<RaftMessage, Error> {
        let id = self.node.id;
        tlog!(
            "[N{id} raft] RequestVote from N{} term={} log=({},{})",
            req.node_id, req.term, req.last_log_term, req.last_log_idx
        );
        let mut ps = self.node.persistent_state.lock().await;
        self.step_down_if_stale(&mut ps, req.term).await;

        if req.term < ps.cur_term {
            tlog!("[N{id} raft] → rejected: stale term (ours={})", ps.cur_term);
            return Ok(vote_response(ps.cur_term, false));
        }

        let available = ps.voted_for.is_none() || ps.voted_for == Some(req.node_id);
        let up_to_date = is_log_up_to_date(&ps, req.last_log_term, req.last_log_idx);

        if available && up_to_date {
            ps.voted_for = Some(req.node_id);
            tlog!("[N{id} raft] → granted vote to N{}", req.node_id);
            Ok(vote_response(ps.cur_term, true))
        } else {
            tlog!(
                "[N{id} raft] → denied: available={available} up_to_date={up_to_date} voted_for={:?}",
                ps.voted_for
            );
            Ok(vote_response(ps.cur_term, false))
        }
    }

    // ── AppendEntries ────────────────────────────────────────────

    async fn handle_append_entries(&self, req: AppendEntries) -> Result<RaftMessage, Error> {
        *self.node.last_heartbeat.lock().await = Instant::now();
        let id = self.node.id;
        tlog!(
            "[N{id} raft] AppendEntries from N{} term={} prev=({},{}) entries={} commit={}",
            req.leader_id,
            req.term,
            req.prev_log_index,
            req.prev_log_term,
            req.entries.len(),
            req.leader_commit
        );
        let mut ps = self.node.persistent_state.lock().await;
        self.step_down_if_stale(&mut ps, req.term).await;

        if req.term < ps.cur_term {
            tlog!("[N{id} raft] → rejected: stale term (ours={})", ps.cur_term);
            return Ok(entries_response(ps.cur_term, false));
        }

        self.accept_leader(req.leader_id).await;
        let term = ps.cur_term;

        // Log consistency check (Raft §5.3)
        if !log_matches(&ps, req.prev_log_index, req.prev_log_term) {
            tlog!(
                "[N{id} raft] → rejected: log mismatch at index={} (log_len={})",
                req.prev_log_index,
                ps.log.len()
            );
            return Ok(entries_response(term, false));
        }

        let entry_count = req.entries.len();
        merge_entries(&mut ps, req.entries);

        // Advance commit index
        let mut vol = self.node.volatile_state.lock().await;
        let old_commit = vol.commit_idx;
        if req.leader_commit > vol.commit_idx {
            vol.commit_idx = min(
                req.leader_commit,
                ps.log.last().map(|e| e.index).unwrap_or(0),
            );
            self.node.tx.send(vol.commit_idx).unwrap();
            tlog!(
                "[N{id} raft] → success: merged {entry_count} entries, commit {} → {}",
                old_commit, vol.commit_idx
            );
        } else if entry_count > 0 {
            tlog!("[N{id} raft] → success: merged {entry_count} entries");
        }

        Ok(entries_response(term, true))
    }

    // ── State transitions ────────────────────────────────────────

    /// If `incoming_term` is newer, adopt it and revert to Follower.
    async fn step_down_if_stale(&self, ps: &mut PersistentState, incoming_term: u64) {
        if incoming_term > ps.cur_term {
            tlog!(
                "[N{} raft] stepping down: term {} → {}",
                self.node.id, ps.cur_term, incoming_term
            );
            ps.cur_term = incoming_term;
            ps.voted_for = None;
            *self.node.state.lock().await = NodeState::Follower;
        }
    }

    /// Acknowledge a valid leader: reset heartbeat timer and become Follower.
    async fn accept_leader(&self, leader_id: u16) {
        *self.node.last_heartbeat.lock().await = Instant::now();
        *self.node.state.lock().await = NodeState::Follower;
        *self.node.current_leader.lock().await = Some(leader_id);
        tlog!("[N{} raft] recognized N{leader_id} as leader", self.node.id);
    }
}

// ── Pure helpers (no state needed) ───────────────────────────────

/// Raft §5.4.1 — candidate's log must be at least as up-to-date as ours.
fn is_log_up_to_date(ps: &PersistentState, candidate_term: u64, candidate_idx: u64) -> bool {
    let my_term = ps.log.last().map(|e| e.term).unwrap_or(0);
    let my_idx = ps.log.last().map(|e| e.index).unwrap_or(0);
    candidate_term > my_term || (candidate_term == my_term && candidate_idx >= my_idx)
}

/// Raft §5.3 — check that our log contains an entry at `prev_index` with `prev_term`.
fn log_matches(ps: &PersistentState, prev_index: u64, prev_term: u64) -> bool {
    if prev_index == 0 {
        return true;
    }
    ps.log
        .get(prev_index as usize - 1)
        .is_some_and(|e| e.term == prev_term)
}

/// Raft §5.3 — append new entries, truncating the log on conflict.
fn merge_entries(ps: &mut PersistentState, entries: Vec<LogEntry>) {
    for entry in entries {
        let idx = entry.index as usize - 1;
        if idx < ps.log.len() {
            if ps.log[idx].term != entry.term {
                ps.log.truncate(idx);
                ps.log.push(entry);
            }
        } else {
            ps.log.push(entry);
        }
    }
}

fn vote_response(term: u64, granted: bool) -> RaftMessage {
    RaftMessage::RequestVoteResponse(RequestVoteResponse {
        term,
        vote_granted: granted,
    })
}

fn entries_response(term: u64, success: bool) -> RaftMessage {
    RaftMessage::AppendEntriesResponse(AppendEntriesResponse { term, success })
}
