use crate::resp::RespValue;

/// All supported Redis commands, each carrying its arguments.
pub enum Command {
    Ping(Vec<RespValue>),
    Echo(Vec<RespValue>),
    Set(Vec<RespValue>),
    Get(Vec<RespValue>),
    Del(Vec<RespValue>),
    Exists(Vec<RespValue>),
    Expire(Vec<RespValue>),
    Ttl(Vec<RespValue>),
    Pttl(Vec<RespValue>),
    LPush(Vec<RespValue>),
    RPush(Vec<RespValue>),
    LPop(Vec<RespValue>),
    RPop(Vec<RespValue>),
    LRange(Vec<RespValue>),
    LLen(Vec<RespValue>),
    HSet(Vec<RespValue>),
    HGet(Vec<RespValue>),
    HGetAll(Vec<RespValue>),
    HDel(Vec<RespValue>),
}

impl Command {
    /// Returns the arguments stored with this command.
    pub fn args(&self) -> &[RespValue] {
        match self {
            Self::Ping(a)
            | Self::Echo(a)
            | Self::Set(a)
            | Self::Get(a)
            | Self::Del(a)
            | Self::Exists(a)
            | Self::Expire(a)
            | Self::Ttl(a)
            | Self::Pttl(a)
            | Self::LPush(a)
            | Self::RPush(a)
            | Self::LPop(a)
            | Self::RPop(a)
            | Self::LRange(a)
            | Self::LLen(a)
            | Self::HSet(a)
            | Self::HGet(a)
            | Self::HGetAll(a)
            | Self::HDel(a) => a,
        }
    }

    /// Reconstructs the full Redis command as a string (e.g. "SET key value EX 10").
    pub fn get_raw_command(&self) -> String {
        let mut parts = vec![self.name().to_string()];
        for arg in self.args() {
            if let Some(s) = arg.as_str() {
                parts.push(s.to_string());
            }
        }
        parts.join(" ")
    }

    pub fn parse_str(s: &str) -> Option<Self> {
        let buf = s.as_bytes();
        let request = RespValue::deserialize(buf);

        let RespValue::Array(Some(mut items)) = request else {
            return None;
        };

        if items.is_empty() {
            return None;
        }

        let Some(command_name) = items[0].as_str() else {
            return None;
        };

        // Split: items[0] is the command name, the rest are arguments
        let command_name = command_name.to_string();
        let args = items.split_off(1);

        let Some(command) = Self::parse(&command_name, args) else {
            return None;
        };
        Some(command)
    }

    /// Parses a command name string (case-insensitive) and attaches the given arguments.
    pub fn parse(s: &str, args: Vec<RespValue>) -> Option<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PING" => Some(Self::Ping(args)),
            "ECHO" => Some(Self::Echo(args)),
            "SET" => Some(Self::Set(args)),
            "GET" => Some(Self::Get(args)),
            "DEL" => Some(Self::Del(args)),
            "EXISTS" => Some(Self::Exists(args)),
            "EXPIRE" => Some(Self::Expire(args)),
            "TTL" => Some(Self::Ttl(args)),
            "PTTL" => Some(Self::Pttl(args)),
            "LPUSH" => Some(Self::LPush(args)),
            "RPUSH" => Some(Self::RPush(args)),
            "LPOP" => Some(Self::LPop(args)),
            "RPOP" => Some(Self::RPop(args)),
            "LRANGE" => Some(Self::LRange(args)),
            "LLEN" => Some(Self::LLen(args)),
            "HSET" => Some(Self::HSet(args)),
            "HGET" => Some(Self::HGet(args)),
            "HGETALL" => Some(Self::HGetAll(args)),
            "HDEL" => Some(Self::HDel(args)),
            _ => None,
        }
    }

    pub fn is_write_command(&self) -> bool {
        matches!(
            self,
            Self::Set(_)
                | Self::Del(_)
                | Self::Expire(_)
                | Self::LPush(_)
                | Self::RPush(_)
                | Self::LPop(_)
                | Self::RPop(_)
                | Self::HSet(_)
                | Self::HDel(_)
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Ping(_) => "PING",
            Self::Echo(_) => "ECHO",
            Self::Set(_) => "SET",
            Self::Get(_) => "GET",
            Self::Del(_) => "DEL",
            Self::Exists(_) => "EXISTS",
            Self::Expire(_) => "EXPIRE",
            Self::Ttl(_) => "TTL",
            Self::Pttl(_) => "PTTL",
            Self::LPush(_) => "LPUSH",
            Self::RPush(_) => "RPUSH",
            Self::LPop(_) => "LPOP",
            Self::RPop(_) => "RPOP",
            Self::LRange(_) => "LRANGE",
            Self::LLen(_) => "LLEN",
            Self::HSet(_) => "HSET",
            Self::HGet(_) => "HGET",
            Self::HGetAll(_) => "HGETALL",
            Self::HDel(_) => "HDEL",
        }
    }
}
