use std::io::Write;
use std::net::TcpListener;
use std::thread;

use lazy_static::lazy_static;
use prometheus::{
    Encoder, Histogram, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
    histogram_opts,
};

const METRICS_ADDR: &str = "0.0.0.0:9090";

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // -- Raft metrics --
    pub static ref RAFT_LEADER_CHANGES: IntCounter =
        IntCounter::new("raft_leader_changes_total", "Total leader changes").unwrap();
    pub static ref RAFT_CURRENT_LEADER: IntGauge =
        IntGauge::new("raft_current_leader", "Current known leader ID (-1 if none)").unwrap();
    pub static ref RAFT_IS_LEADER: IntGauge =
        IntGauge::new("raft_is_leader", "1 if this node is the leader, 0 otherwise").unwrap();
    pub static ref RAFT_MESSAGES_SENT: IntCounter =
        IntCounter::new("raft_messages_sent_total", "Total Raft messages sent").unwrap();
    pub static ref RAFT_MESSAGES_RECEIVED: IntCounter =
        IntCounter::new("raft_messages_received_total", "Total Raft messages received").unwrap();
    pub static ref RAFT_TRANSITIONS_APPLIED: IntCounter =
        IntCounter::new("raft_transitions_applied_total", "Total transitions applied").unwrap();
    pub static ref RAFT_TRANSITIONS_COMMITTED: IntCounter =
        IntCounter::new("raft_transitions_committed_total", "Total transitions committed").unwrap();
    pub static ref RAFT_TRANSITIONS_ABANDONED: IntCounter =
        IntCounter::new("raft_transitions_abandoned_total", "Total transitions abandoned").unwrap();
    pub static ref RAFT_PENDING_TRANSITIONS: IntGauge =
        IntGauge::new("raft_pending_transitions", "Transitions in last pending batch").unwrap();

    // -- Redis metrics --
    pub static ref REDIS_COMMANDS_TOTAL: IntCounterVec =
        IntCounterVec::new(Opts::new("redis_commands_total", "Total Redis commands by type"), &["command"]).unwrap();
    pub static ref REDIS_WRITE_COMMANDS: IntCounter =
        IntCounter::new("redis_write_commands_total", "Total write commands").unwrap();
    pub static ref REDIS_READ_COMMANDS: IntCounter =
        IntCounter::new("redis_read_commands_total", "Total read commands").unwrap();
    pub static ref REDIS_CONNECTIONS_ACTIVE: IntGauge =
        IntGauge::new("redis_connections_active", "Currently active Redis connections").unwrap();
    pub static ref REDIS_COMMAND_DURATION: Histogram =
        Histogram::with_opts(histogram_opts!(
            "redis_command_duration_seconds",
            "Command execution duration in seconds"
        )).unwrap();
    pub static ref REDIS_REDIRECTS: IntCounter =
        IntCounter::new("redis_redirects_total", "Total MOVED redirects sent").unwrap();

    // -- Node metadata --
    pub static ref NODE_INFO: IntGauge =
        IntGauge::new("node_info", "This node's ID").unwrap();
}

pub fn register_metrics() {
    let metrics: Vec<Box<dyn prometheus::core::Collector>> = vec![
        Box::new(RAFT_LEADER_CHANGES.clone()),
        Box::new(RAFT_CURRENT_LEADER.clone()),
        Box::new(RAFT_IS_LEADER.clone()),
        Box::new(RAFT_MESSAGES_SENT.clone()),
        Box::new(RAFT_MESSAGES_RECEIVED.clone()),
        Box::new(RAFT_TRANSITIONS_APPLIED.clone()),
        Box::new(RAFT_TRANSITIONS_COMMITTED.clone()),
        Box::new(RAFT_TRANSITIONS_ABANDONED.clone()),
        Box::new(RAFT_PENDING_TRANSITIONS.clone()),
        Box::new(REDIS_COMMANDS_TOTAL.clone()),
        Box::new(REDIS_WRITE_COMMANDS.clone()),
        Box::new(REDIS_READ_COMMANDS.clone()),
        Box::new(REDIS_CONNECTIONS_ACTIVE.clone()),
        Box::new(REDIS_COMMAND_DURATION.clone()),
        Box::new(REDIS_REDIRECTS.clone()),
        Box::new(NODE_INFO.clone()),
    ];
    for m in metrics {
        REGISTRY.register(m).unwrap();
    }
}

pub fn serve_metrics() {
    thread::spawn(|| {
        let listener = TcpListener::bind(METRICS_ADDR).unwrap();
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let encoder = TextEncoder::new();
            let metric_families = REGISTRY.gather();
            let mut body = Vec::new();
            encoder.encode(&metric_families, &mut body).unwrap();

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                encoder.format_type(),
                body.len(),
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
}

/// RAII guard that decrements REDIS_CONNECTIONS_ACTIVE on drop.
pub struct ConnectionGuard;

impl ConnectionGuard {
    pub fn new() -> Self {
        REDIS_CONNECTIONS_ACTIVE.inc();
        Self
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        REDIS_CONNECTIONS_ACTIVE.dec();
    }
}
