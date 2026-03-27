use crate::memory::{Store, StoreError};
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

    /// Executes the command with the given arguments against the store.
    /// Returns a RespValue response ready to send to the client.
    pub fn execute(&self, args: &[RespValue], store: &Store) -> RespValue {
        match self.execute_inner(args, store) {
            Ok(response) => response,
            Err(err_response) => err_response,
        }
    }

    fn execute_inner(&self, args: &[RespValue], store: &Store) -> Result<RespValue, RespValue> {
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
                Ok(RespValue::Integer(Self::map_store_error(store.lpush(
                    key,
                    &values,
                ))?))
            }
            Self::RPush => {
                Self::require_min_args("RPUSH", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let values = Self::parse_string_args(&args[1..], "list element")?;
                Ok(RespValue::Integer(Self::map_store_error(store.rpush(
                    key,
                    &values,
                ))?))
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

                Ok(RespValue::Integer(Self::map_store_error(store.hset(
                    key,
                    &pairs,
                ))?))
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
                Ok(RespValue::Integer(Self::map_store_error(store.hdel(
                    key,
                    &fields,
                ))?))
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

    fn require_exact_args(name: &str, args: &[RespValue], expected: usize) -> Result<(), RespValue> {
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
