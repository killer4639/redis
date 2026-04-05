use std::time::Duration;

use crate::{
    command::Command,
    memory::{Store, StoreError},
    resp::RespValue,
};

pub struct Engine {
    store: Store,
    // later: raft state, pending transitions queue, etc.
}

impl Engine {
    pub fn new() -> Self {
        Self { store: Store::new() }
    }
    pub fn execute(&self, command: &str) -> String {
        let command = Command::parse_str(command);
        let result = self.execute_inner(command);

        if result.is_ok() {
            result.unwrap().to_string()
        } else {
            result.err().unwrap().to_string()
        }
    }

    fn execute_inner(&self, command: Option<Command>) -> Result<RespValue, RespValue> {
        if command.is_none() {
            return Err(RespValue::Error("Invalid command".to_string()));
        }

        let command = command.unwrap();
        let store = &self.store;

        let args = command.args();
        match command {
            Command::Ping(_) => Ok(RespValue::pong()),

            Command::Echo(_) => {
                Self::require_exact_args("ECHO", args, 1)?;
                Ok(RespValue::BulkString(Some(
                    args[0].as_str().unwrap().to_string(),
                )))
            }

            Command::Set(_) => {
                Self::require_exact_or_with_option_args("SET", args, 2, 4)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let value = Self::parse_string_arg(&args[1], "value")?;
                let expires_after = Self::parse_set_expiry(&args[2..])?;

                store.set(key, value, expires_after);
                Ok(RespValue::ok())
            }

            Command::Get(_) => {
                Self::require_exact_args("GET", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                match Self::map_store_error(store.get(key))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }

            Command::Del(_) => {
                Self::require_exact_args("DEL", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                Ok(RespValue::Integer(if store.del(key) { 1 } else { 0 }))
            }

            Command::Exists(_) => {
                Self::require_exact_args("EXISTS", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                Ok(RespValue::Integer(if store.exists(key) { 1 } else { 0 }))
            }
            Command::Expire(_) => {
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
            Command::Ttl(_) => {
                Self::require_exact_args("TTL", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let response = match store.get_pttl(key) {
                    (false, _) => -2,
                    (true, None) => -1,
                    (true, Some(ttl)) => ttl.as_secs() as i64,
                };
                Ok(RespValue::Integer(response))
            }
            Command::Pttl(_) => {
                Self::require_exact_args("PTTL", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let response = match store.get_pttl(key) {
                    (false, _) => -2,
                    (true, None) => -1,
                    (true, Some(ttl)) => ttl.as_millis() as i64,
                };
                Ok(RespValue::Integer(response))
            }
            Command::LPush(_) => {
                Self::require_min_args("LPUSH", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let values = Self::parse_string_args(&args[1..], "list element")?;
                Ok(RespValue::Integer(Self::map_store_error(
                    store.lpush(key, &values),
                )?))
            }
            Command::RPush(_) => {
                Self::require_min_args("RPUSH", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let values = Self::parse_string_args(&args[1..], "list element")?;
                Ok(RespValue::Integer(Self::map_store_error(
                    store.rpush(key, &values),
                )?))
            }
            Command::LPop(_) => {
                Self::require_exact_args("LPOP", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                match Self::map_store_error(store.lpop(key))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }
            Command::RPop(_) => {
                Self::require_exact_args("RPOP", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                match Self::map_store_error(store.rpop(key))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }
            Command::LRange(_) => {
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
            Command::LLen(_) => {
                Self::require_exact_args("LLEN", args, 1)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                Ok(RespValue::Integer(Self::map_store_error(store.llen(key))?))
            }
            Command::HSet(_) => {
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
            Command::HGet(_) => {
                Self::require_exact_args("HGET", args, 2)?;
                let key = Self::parse_string_arg(&args[0], "key")?;
                let field = Self::parse_string_arg(&args[1], "field")?;
                match Self::map_store_error(store.hget(key, field))? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }
            Command::HGetAll(_) => {
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
            Command::HDel(_) => {
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
