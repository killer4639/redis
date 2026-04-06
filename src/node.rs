use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex},
};

use crossbeam_channel::Sender;
use little_raft::state_machine::{
    Snapshot, StateMachine, StateMachineTransition, TransitionAbandonedReason, TransitionState,
};

use crate::{
    cluster::RedisTransition, engine::Engine, message_bus::MessageBus, metrics, resp::RespValue,
    tlog,
};

type PendingResponses = HashMap<u64, Sender<String>>;
pub struct Redis {
    engine: Arc<Engine>,
    message_bus: Arc<MessageBus<RedisTransition>>,
    results: HashMap<u64, String>,
    pub pending: PendingResponses,
}

impl Redis {
    pub fn new(engine: Arc<Engine>, message_bus: Arc<MessageBus<RedisTransition>>) -> Self {
        Self {
            engine,
            message_bus,
            results: HashMap::new(),
            pending: HashMap::new(),
        }
    }
}

impl StateMachine<RedisTransition, Vec<u8>> for Redis {
    fn register_transition_state(
        &mut self,
        transition_id: <RedisTransition as StateMachineTransition>::TransitionID,
        state: TransitionState,
    ) {
        // Skip noop transitions from metrics
        if transition_id == u64::MAX {
            return;
        }

        match state {
            TransitionState::Abandoned(TransitionAbandonedReason::NotLeader)
            | TransitionState::Abandoned(TransitionAbandonedReason::ConflictWithLeader) => {
                metrics::RAFT_TRANSITIONS_ABANDONED.inc();
                let response = RespValue::Error("Error".to_string());
                if let Some(tx) = self.pending.remove(&transition_id) {
                    let _ = tx.send(response.to_string());
                }
            }
            TransitionState::Applied => {
                if self.pending.contains_key(&transition_id) {
                    let result = self.results.remove(&transition_id).unwrap();
                    let tx = self.pending.remove(&transition_id).unwrap();
                    let _ = tx.send(result);
                } else {
                    tlog!("Registered Sender not found for {}", transition_id)
                }
            }
            TransitionState::Committed => {
                metrics::RAFT_TRANSITIONS_COMMITTED.inc();
            }
            _ => {}
        }
    }

    fn apply_transition(&mut self, transition: RedisTransition) {
        // Noop transitions (used by Raft for leader commit) — skip execution
        if transition.get_id() == u64::MAX {
            return;
        }

        metrics::RAFT_TRANSITIONS_APPLIED.inc();
        let result = self.engine.execute(&transition.command);
        if self.pending.contains_key(&transition.get_id()) {
            self.results.insert(transition.get_id(), result);
        }
    }

    fn create_snapshot(
        &mut self,
        last_included_index: usize,
        last_included_term: usize,
    ) -> Snapshot<Vec<u8>> {
        Snapshot {
            last_included_index,
            last_included_term,
            data: Vec::new(),
        }
    }

    fn get_pending_transitions(&mut self) -> Vec<RedisTransition> {
        let transitions = self.message_bus.drain();
        metrics::RAFT_PENDING_TRANSITIONS.set(transitions.len() as i64);
        transitions
    }
    fn get_snapshot(&mut self) -> Option<Snapshot<Vec<u8>>> {
        None
    }

    fn set_snapshot(&mut self, snapshot: Snapshot<Vec<u8>>) {}
}
