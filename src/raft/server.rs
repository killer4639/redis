use crate::raft::node::Node;

pub struct RaftServer {
    node: Node,
}

impl RaftServer {
    pub fn new(node: Node) -> Self {
        Self { node }
    }
}
