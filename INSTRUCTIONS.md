# Distributed Systems Learning Roadmap

## ⚠️ Copilot Ground Rules
- **DO NOT write code for the user.** This is a learning project.
- Only guide, explain concepts, and answer questions. Let the user write the code.
- Help debug when asked. Review code when asked. But never implement for them.
- The goal is to learn distributed systems and Rust — not to ship fast.

## What We've Built So Far
- In-memory Redis clone in Rust (TCP server, RESP protocol, strings, lists, hashes, TTL/expiry)
- LRU cache with proper eviction

## Current Focus: Raft Consensus
Integrate Raft directly into this Redis server to make it a distributed, replicated database.

### What Raft Gives Us
- Multiple Redis nodes that agree on the same state
- A leader handles writes, replicates to followers
- If the leader dies, a new one is elected automatically
- Clients always see consistent data

### Implementation Phases (all inside this project)

#### Phase 1: Core Types
- Node states: Follower, Candidate, Leader
- LogEntry: (term, index, command)
- RPC messages: RequestVote, AppendEntries + responses
- New modules: `src/raft.rs` or `src/raft/` directory

#### Phase 2: Node & Cluster Networking
- Each Redis instance knows about its peers (config: node ID + peer addresses)
- Nodes communicate via a separate TCP channel (not the RESP client port)
- JSON-serialized Raft RPCs between nodes

#### Phase 3: Leader Election
- Election timeout → become Candidate → request votes from peers
- Vote granting rules (one vote per term, log freshness check)
- Majority wins → become Leader → send periodic heartbeats
- Split vote → increment term, try again

#### Phase 4: Log Replication
- Client writes go to the Leader only
- Leader appends to log → sends AppendEntries to followers
- Followers check log consistency, append entries
- Leader commits once a majority has replicated → applies to Store

#### Phase 5: Wire It Together
- Client SET/GET commands flow through the Raft log before hitting the Store
- Followers redirect write commands to the Leader
- Reads can be served locally (eventual consistency) or via Leader (strong consistency)

### Design Decisions
- **Chaos testing**: Use **Blockade** for real Docker-based network partitions, node kills, latency injection
- **Client → Leader discovery**: Use **redirect approach** — followers respond with `-MOVED leader_address:port`, client reconnects to leader and caches it. Falls back to trying any node if leader dies.

### Key References
- Raft paper: https://raft.github.io/raft.pdf
- Raft visualization: https://thesecretlivesofdata.com/raft/

## Future Concepts (after Raft)

### Consistent Hashing
- Shard keys across multiple nodes
- Hash rings, virtual nodes, minimal redistribution

### Gossip Protocol
- Cluster membership and failure detection
- Epidemic-style info dissemination

### CRDTs
- Conflict-free replicated data types
- Eventual consistency without coordination

### Two-Phase Commit (2PC)
- Distributed transactions across shards
- Prepare/commit phases, coordinator failure

## Notes
- Everything lives in this project — extending the Redis server into a distributed system
- All async networking via tokio
- Learn by doing: understand the concept first, then implement it yourself
