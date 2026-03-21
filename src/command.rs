use crate::memory::Store;
use crate::resp::RespValue;

/// All supported Redis commands.
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Del,
    Exists,
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
                Ok(RespValue::BulkString(Some(args[0].as_str().unwrap().to_string())))
            }

            Self::Set => {
                Self::require_args("SET", args, 2)?;
                let key = args[0].as_str().unwrap();
                let value = args[1].as_str().unwrap();
                store.set(key, value);
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
        }
    }

    /// Validates argument count. Returns an error RespValue if wrong.
    fn require_args(name: &str, args: &[RespValue], expected: usize) -> Result<(), RespValue> {
        if args.len() < expected {
            Err(RespValue::err(
                &format!("wrong number of arguments for '{name}' command"),
            ))
        } else {
            Ok(())
        }
    }
}