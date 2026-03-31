use crate::tlog;
use crate::Server;
use crate::memory::{Store, StoreError};
use crate::raft::node::NodeState;
use crate::resp::RespValue;
use std::time::Duration;

/// All supported Redis commands.
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Del,
    Exists,
    Expire,
    Ttl,
    Pttl,
    LPush,
    RPush,
    LPop,
    RPop,
    LRange,
    LLen,
    HSet,
    HGet,
    HGetAll,
    HDel,
}

/// Reconstructs the full Redis command as a string (e.g. "SET key value EX 10").
fn get_raw_command(command: &Command, args: &[RespValue]) -> String {
    let mut parts = vec![command.name().to_string()];
    for arg in args {
        if let Some(s) = arg.as_str() {
            parts.push(s.to_string());
        }
    }
    parts.join(" ")
}

impl Command {
    /// Parses a command name string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PING" => Some(Self::Ping),
            "ECHO" => Some(Self::Echo),
            "SET" => Some(Self::Set),
            "GET" => Some(Self::Get),
            "DEL" => Some(Self::Del),
            "EXISTS" => Some(Self::Exists),
            "EXPIRE" => Some(Self::Expire),
            "TTL" => Some(Self::Ttl),
            "PTTL" => Some(Self::Pttl),
            "LPUSH" => Some(Self::LPush),
            "RPUSH" => Some(Self::RPush),
            "LPOP" => Some(Self::LPop),
            "RPOP" => Some(Self::RPop),
            "LRANGE" => Some(Self::LRange),
            "LLEN" => Some(Self::LLen),
            "HSET" => Some(Self::HSet),
            "HGET" => Some(Self::HGet),
            "HGETALL" => Some(Self::HGetAll),
            "HDEL" => Some(Self::HDel),
            _ => None,
        }
    }

    pub fn is_write_command(&self) -> bool {
        matches!(
            self,
            Self::Set
                | Self::Del
                | Self::Expire
                | Self::LPush
                | Self::RPush
                | Self::LPop
                | Self::RPop
                | Self::HSet
                | Self::HDel
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Ping => "PING",
            Self::Echo => "ECHO",
            Self::Set => "SET",
            Self::Get => "GET",
            Self::Del => "DEL",
            Self::Exists => "EXISTS",
            Self::Expire => "EXPIRE",
            Self::Ttl => "TTL",
            Self::Pttl => "PTTL",
            Self::LPush => "LPUSH",
            Self::RPush => "RPUSH",
            Self::LPop => "LPOP",
            Self::RPop => "RPOP",
            Self::LRange => "LRANGE",
            Self::LLen => "LLEN",
            Self::HSet => "HSET",
            Self::HGet => "HGET",
            Self::HGetAll => "HGETALL",
            Self::HDel => "HDEL",
        }
    }

    /// Executes the command with the given arguments against the store.
    /// Returns a RespValue response ready to send to the client.
    pub async fn execute(&self, args: &[RespValue], server: &Server) -> RespValue {
        let id = server.raft.node.id;
        if self.is_write_command() {
            if !matches!(*server.raft.node.state.lock().await, NodeState::Leader) {
                let leader = server.raft.node.current_leader.lock().await;
                return match *leader {
                    Some(lid) => {
                        tlog!("[N{id} redis] write redirect → N{lid}");
                        RespValue::Error(format!("MOVED node{lid}:6379"))
                    }
                    None => {
                        tlog!("[N{id} redis] write rejected — no leader");
                        RespValue::Error("CLUSTERDOWN no leader elected".to_string())
                    }
                };
            }
            let raw_command = get_raw_command(self, args);
            tlog!("[N{id} redis] write command: \"{raw_command}\"");
            match server.raft.append_log(&raw_command).await {
                Ok(()) => {
                    // TODO: execute after Raft commit
                    RespValue::ok()
                }
                Err(e) => RespValue::err(&format!("RAFT {e}")),
            }
        } else {
            match self.execute_inner(args, &server.redis) {
                Ok(response) => response,
                Err(err_response) => err_response,
            }
        }
    }

    pub fn execute_inner(&self, args: &[RespValue], store: &Store) -> Result<RespValue, RespValue> {
        match self {
            Self::Ping => Ok(RespValue::pong()),

            Self::Echo => {
                Self::require_exact_args("ECHO", args, 1)?;
                Ok(RespValue::BulkString(Some(
                    args[0].as_str().unwrap().to_string(),
                )))
            }

            Self::Set => {
                Self::require_exact_or_with_option_args("SET", args, 2, 4)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let value = Self::parse_string_arg(&args[1], "value")?;
                let expires_after = Self::parse_set_expiry(&args[2..])?;

                store.set(key, value, expires_after);
                Ok(RespValue::ok())
            }

            Self::Get => {
                Self::require_exact_args("GET", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                match Self::map_store_error(store.get(key))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }

            Self::Del => {
                Self::require_exact_args("DEL", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                Ok(RespValue::Integer(if store.del(key) { 1 } else { 0 }))
            }

            Self::Exists => {
                Self::require_exact_args("EXISTS", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                Ok(RespValue::Integer(if store.exists(key) { 1 } else { 0 }))
            }
            Self::Expire => {
                Self::require_exact_args("EXPIRE", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let ttl = Self::parse_u64_arg(&args[1])?;

                Ok(RespValue::Integer(
                    if store.update_expiry(key, Duration::from_secs(ttl)) {
                        1
                    } else {
                        0
                    },
                ))
            }
            Self::Ttl => {
                Self::require_exact_args("TTL", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let response = match store.get_pttl(key) {
                    (false, _) => -2,
                    (true, None) => -1,
                    (true, Some(ttl)) => ttl.as_secs() as i64,
                };
                Ok(RespValue::Integer(response))
            }
            Self::Pttl => {
                Self::require_exact_args("PTTL", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let response = match store.get_pttl(key) {
                    (false, _) => -2,
                    (true, None) => -1,
                    (true, Some(ttl)) => ttl.as_millis() as i64,
                };
                Ok(RespValue::Integer(response))
            }
            Self::LPush => {
                Self::require_min_args("LPUSH", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let values = Self::parse_string_args(&args[1..], "list element")?;
                Ok(RespValue::Integer(Self::map_store_error(
                    store.lpush(key, &values),
                )?))
            }
            Self::RPush => {
                Self::require_min_args("RPUSH", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let values = Self::parse_string_args(&args[1..], "list element")?;
                Ok(RespValue::Integer(Self::map_store_error(
                    store.rpush(key, &values),
                )?))
            }
            Self::LPop => {
                Self::require_exact_args("LPOP", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                match Self::map_store_error(store.lpop(key))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }
            Self::RPop => {
                Self::require_exact_args("RPOP", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                match Self::map_store_error(store.rpop(key))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }
            Self::LRange => {
                Self::require_exact_args("LRANGE", args, 3)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let start = Self::parse_isize_arg(&args[1])?;
                let stop = Self::parse_isize_arg(&args[2])?;
                let items = Self::map_store_error(store.lrange(key, start, stop))?;
                Ok(RespValue::Array(Some(
                    items
                        .into_iter()
                        .map(|value| RespValue::BulkString(Some(value)))
                        .collect(),
                )))
            }
            Self::LLen => {
                Self::require_exact_args("LLEN", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                Ok(RespValue::Integer(Self::map_store_error(store.llen(key))?))
            }
            Self::HSet => {
                Self::require_min_args("HSET", args, 3)?;
                if args.len() % 2 == 0 {
                    return Err(Self::wrong_number_of_arguments("HSET"));
                }

                let key = Self::parse_string_arg(&args[0], "key")?;
                let field_values = Self::parse_string_args(&args[1..], "field or value")?;
                let pairs: Vec<(String, String)> = field_values
                    .chunks(2)
                    .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                    .collect();

                Ok(RespValue::Integer(Self::map_store_error(
                    store.hset(key, &pairs),
                )?))
            }
            Self::HGet => {
                Self::require_exact_args("HGET", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let field = Self::parse_string_arg(&args[1], "field")?;
                match Self::map_store_error(store.hget(key, field))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }
            Self::HGetAll => {
                Self::require_exact_args("HGETALL", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let entries = Self::map_store_error(store.hgetall(key))?;
                let mut values = Vec::with_capacity(entries.len() * 2);
                for (field, value) in entries {
                    values.push(RespValue::BulkString(Some(field)));
                    values.push(RespValue::BulkString(Some(value)));
                }
                Ok(RespValue::Array(Some(values)))
            }
            Self::HDel => {
                Self::require_min_args("HDEL", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let fields = Self::parse_string_args(&args[1..], "field")?;
                Ok(RespValue::Integer(Self::map_store_error(
                    store.hdel(key, &fields),
                )?))
            }
        }
    }

    fn map_store_error<T>(result: Result<T, StoreError>) -> Result<T, RespValue> {
        result.map_err(|_| Self::wrong_type())
    }

    fn parse_string_arg<'a>(arg: &'a RespValue, name: &str) -> Result<&'a str, RespValue> {
        arg.as_str()
            .ok_or_else(|| RespValue::err(&format!("invalid {name}")))
    }

    fn parse_string_args(args: &[RespValue], name: &str) -> Result<Vec<String>, RespValue> {
        args.iter()
            .map(|arg| Self::parse_string_arg(arg, name).map(str::to_string))
            .collect()
    }

    fn parse_u64_arg(arg: &RespValue) -> Result<u64, RespValue> {
        Self::parse_string_arg(arg, "integer")?
            .parse()
            .map_err(|_| RespValue::err("value is not an integer or out of range"))
    }

    fn parse_isize_arg(arg: &RespValue) -> Result<isize, RespValue> {
        Self::parse_string_arg(arg, "integer")?
            .parse()
            .map_err(|_| RespValue::err("value is not an integer or out of range"))
    }

    fn parse_set_expiry(args: &[RespValue]) -> Result<Option<Duration>, RespValue> {
        if args.is_empty() {
            return Ok(None);
        }

        let option = Self::parse_string_arg(&args[0], "expiration option")?;
        let ttl = Self::parse_u64_arg(&args[1])?;

        match option.to_ascii_uppercase().as_str() {
            "EX" => Ok(Some(Duration::from_secs(ttl))),
            "PX" => Ok(Some(Duration::from_millis(ttl))),
            _ => Err(RespValue::err("unsupported expiration option")),
        }
    }

    fn require_exact_args(
        name: &str,
        args: &[RespValue],
        expected: usize,
    ) -> Result<(), RespValue> {
        if args.len() == expected {
            Ok(())
        } else {
            Err(Self::wrong_number_of_arguments(name))
        }
    }

    fn require_min_args(name: &str, args: &[RespValue], expected: usize) -> Result<(), RespValue> {
        if args.len() >= expected {
            Ok(())
        } else {
            Err(Self::wrong_number_of_arguments(name))
        }
    }

    fn require_exact_or_with_option_args(
        name: &str,
        args: &[RespValue],
        exact: usize,
        with_option: usize,
    ) -> Result<(), RespValue> {
        if args.len() == exact || args.len() == with_option {
            Ok(())
        } else {
            Err(Self::wrong_number_of_arguments(name))
        }
    }

    fn wrong_number_of_arguments(name: &str) -> RespValue {
        RespValue::err(&format!("wrong number of arguments for '{name}' command"))
    }

    fn wrong_type() -> RespValue {
        RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        )
    }
}
