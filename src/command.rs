use crate::memory::Store;
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
    Tll,
    Pttl,
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
            "TTL" => Some(Self::Tll),
            "PTTL" => Some(Self::Pttl),
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
                Self::require_args("ECHO", args, 1)?;
                Ok(RespValue::BulkString(Some(
                    args[0].as_str().unwrap().to_string(),
                )))
            }

            Self::Set => {
                Self::require_args("SET", args, 2)?;
                let key = args[0].as_str().unwrap();
                let value = args[1].as_str().unwrap();

                let expires_after = match args.len() {
                    2 => None,
                    4 => {
                        let option = args[2]
                            .as_str()
                            .ok_or_else(|| RespValue::err("invalid expiration option"))?;
                        let value = args[3]
                            .as_str()
                            .ok_or_else(|| RespValue::err("invalid expiration value"))?;
                        let ttl: u64 = value
                            .parse()
                            .map_err(|_| RespValue::err("invalid expiration value"))?;

                        match option.to_ascii_uppercase().as_str() {
                            "EX" => Some(Duration::from_secs(ttl)),
                            "PX" => Some(Duration::from_millis(ttl)),
                            _ => return Err(RespValue::err("unsupported expiration option")),
                        }
                    }
                    _ => {
                        return Err(RespValue::err(
                            "wrong number of arguments for 'SET' command",
                        ));
                    }
                };

                store.set(key, value, expires_after);
                Ok(RespValue::ok())
            }

            Self::Get => {
                Self::require_args("GET", args, 1)?;
                let key = args[0].as_str().unwrap();
                match store.get(key) {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::null_bulk_string()),
                }
            }

            Self::Del => {
                Self::require_args("DEL", args, 1)?;
                let key = args[0].as_str().unwrap();
                Ok(RespValue::Integer(if store.del(key) { 1 } else { 0 }))
            }

            Self::Exists => {
                Self::require_args("EXISTS", args, 1)?;
                let key = args[0].as_str().unwrap();
                Ok(RespValue::Integer(if store.exists(key) { 1 } else { 0 }))
            }
            Self::Expire => {
                Self::require_args("EXPIRE", args, 2)?;
                let key = args[0].as_str().unwrap();
                let value = args[1]
                    .as_str()
                    .ok_or_else(|| RespValue::err("invalid expiration value"))?;
                let ttl: u64 = value
                    .parse()
                    .map_err(|_| RespValue::err("invalid expiration value"))?;

                Ok(RespValue::Integer(
                    if store.update_expiry(key, Duration::from_secs(ttl)) {
                        1
                    } else {
                        0
                    },
                ))
            }
            Self::Tll => {
                Self::require_args("TTL", args, 1)?;
                let key = args[0].as_str().unwrap();
                let response = match store.get_pttl(key) {
                    (false, _) => -2,
                    (true, None) => -1,
                    (true, Some(ttl)) => ttl.as_secs() as i64,
                };
                Ok(RespValue::Integer(response))
            }
            Self::Pttl => {
                Self::require_args("PTTL", args, 1)?;
                let key = args[0].as_str().unwrap();
                let response = match store.get_pttl(key) {
                    (false, _) => -2,
                    (true, None) => -1,
                    (true, Some(ttl)) => ttl.as_millis() as i64,
                };
                Ok(RespValue::Integer(response))
            }
        }
    }

    /// Validates argument count. Returns an error RespValue if wrong.
    fn require_args(name: &str, args: &[RespValue], expected: usize) -> Result<(), RespValue> {
        if args.len() < expected {
            Err(RespValue::err(&format!(
                "wrong number of arguments for '{name}' command"
            )))
        } else {
            Ok(())
        }
    }
}
