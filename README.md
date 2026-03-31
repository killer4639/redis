# Distributed Redis in Rust

I built a Redis clone from scratch in Rust, then made it distributed using the Raft consensus algorithm.

No libraries for the hard parts. No `raft-rs`. Just the paper, Tokio, and a lot of debugging Docker logs.

## What it does

It's a key-value store that replicates across multiple nodes. Kill the leader, a new one gets elected. Write to the leader, the data shows up on followers. The usual distributed systems stuff — except I wrote all of it.

**Supported Redis commands:** `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `PTTL`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `HSET`, `HGET`, `HGETALL`, `HDEL`, `PING`, `ECHO`

**Raft implementation includes:**
- Leader election with randomized timeouts
- Log replication with consistency checks
- Commit index advancement (majority quorum)
- Automatic failover — kill a node, cluster keeps going
- Follower redirect (`-MOVED`) for write commands
- Heartbeats to maintain leader authority

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node 1    │     │   Node 2    │     │   Node 3    │
│  (Leader)   │◄───►│ (Follower)  │◄───►│ (Follower)  │
│             │     │             │     │             │
│ :6379 Redis │     │ :6379 Redis │     │ :6379 Redis │
│ :6380 Raft  │     │ :6380 Raft  │     │ :6380 Raft  │
└─────────────┘     └─────────────┘     └─────────────┘
```

Each node runs two TCP servers:
- **Port 6379** — Redis protocol (RESP) for client commands
- **Port 6380** — Raft protocol (JSON over TCP) for peer communication

Writes go to the leader → appended to the Raft log → replicated to followers → committed once a majority acknowledges → applied to the in-memory store.

Reads can go to any node (eventual consistency).

## How it works

**Leader Election:** When a follower doesn't hear from the leader within a randomized timeout (150–300ms), it starts an election. It increments its term, votes for itself, and asks peers for votes. Majority wins. The new leader immediately sends heartbeats to prevent further elections.

**Log Replication:** The leader appends client write commands to its log, then sends `AppendEntries` RPCs to all followers. Followers check that their log matches the leader's (using `prev_log_index` and `prev_log_term`), then append. If there's a mismatch, the leader backs up and retries. Once a majority has replicated an entry, it's committed and applied to the store.

**Failover:** Kill a node — the cluster keeps running as long as a majority is alive (3 of 5). Kill the leader — a new election happens within a few hundred milliseconds. Bring a node back — it catches up from the current leader.

## Running it

Spin up a 5-node cluster:

```bash
docker compose up --build
```

Talk to the cluster:

```bash
# Write to the leader (check logs to find which node won)
redis-cli -p 6391 SET hello world

# Read from any node
redis-cli -p 6393 GET hello

# If you hit a follower with a write, you get:
# (error) MOVED node3:6379
```

Kill a node and watch the election happen:

```bash
docker compose stop node1
# Check logs — a new leader gets elected
docker compose logs -f
```

## The stack

- **Rust** with **Tokio** for async networking
- **serde/serde_json** for Raft message serialization
- **Docker Compose** for running the cluster
- Hand-written RESP protocol parser
- Hand-written Raft implementation (following the [Raft paper](https://raft.github.io/raft.pdf))

## What I learned

- Distributed consensus is mostly about edge cases. The happy path is easy — it's the "what if the leader dies mid-replication" scenarios that get you.
- Async Rust + Mutexes is a puzzle. Holding locks across `.await` points and avoiding deadlocks took real thought.
- Docker Compose is surprisingly good for testing distributed systems locally.
- Reading the Raft paper is one thing. Implementing it is a completely different experience. You don't really understand the protocol until you've debugged an election storm at 2am.
