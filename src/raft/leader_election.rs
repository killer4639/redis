use anyhow::Error;

use crate::raft::node::Node;

struct LeaderElection {}

impl LeaderElection {
    pub fn run_leader_election(node: &Node) -> Result<u16, Error> {
        Ok(10)
    }
}
