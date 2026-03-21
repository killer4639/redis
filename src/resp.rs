use std::fmt;

/// Represents a value in the Redis Serialization Protocol (RESP).
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Option<Vec<RespValue>>),
}

// -- Convenience constructors --
// These read much better at the call site:
//   RespValue::ok()  vs  RespValue::SimpleString("OK".to_string())
impl RespValue {
    pub fn ok() -> Self {
        Self::SimpleString("OK".to_string())
    }

    pub fn pong() -> Self {
        Self::SimpleString("PONG".to_string())
    }

    pub fn err(msg: &str) -> Self {
        Self::Error(format!("ERR {msg}"))
    }

    pub fn null_bulk_string() -> Self {
        Self::BulkString(None)
    }

    /// Extracts the inner string from SimpleString or BulkString variants.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::BulkString(Some(s)) | Self::SimpleString(s) => Some(s),
            _ => None,
        }
    }
}

// -- Serialization via Display --
// Using Display instead of a custom serialize() method gives us:
//   - format!("{}", value)  works
//   - value.to_string()     works
//   - print!("{}", value)   works
impl fmt::Display for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SimpleString(s) => write!(f, "+{s}\r\n"),
            Self::Error(s) => write!(f, "-{s}\r\n"),
            Self::Integer(n) => write!(f, ":{n}\r\n"),
            Self::BulkString(Some(s)) => write!(f, "${}\r\n{s}\r\n", s.len()),
            Self::BulkString(None) => write!(f, "$-1\r\n"),
            Self::Array(Some(items)) => {
                write!(f, "*{}\r\n", items.len())?;
                for item in items {
                    write!(f, "{item}")?;
                }
                Ok(())
            }
            Self::Array(None) => write!(f, "*-1\r\n"),
        }
    }
}

// -- Deserialization --
impl RespValue {
    /// Parses a RESP value from a byte buffer.
    pub fn deserialize(buf: &[u8]) -> Self {
        Self::parse(buf).0
    }

    fn parse(buf: &[u8]) -> (Self, usize) {
        match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            byte => panic!("unsupported RESP prefix: 0x{byte:02x}"),
        }
    }

    /// Reads one line from buf, skipping the first byte (the type prefix).
    /// Returns (line_content, total_bytes_consumed).
    fn read_line(buf: &[u8]) -> (&str, usize) {
        let s = std::str::from_utf8(buf).unwrap();
        let end = s.find("\r\n").unwrap();
        (&s[1..end], end + 2)
    }

    fn parse_simple_string(buf: &[u8]) -> (Self, usize) {
        let (line, consumed) = Self::read_line(buf);
        (Self::SimpleString(line.to_string()), consumed)
    }

    fn parse_error(buf: &[u8]) -> (Self, usize) {
        let (line, consumed) = Self::read_line(buf);
        (Self::Error(line.to_string()), consumed)
    }

    fn parse_integer(buf: &[u8]) -> (Self, usize) {
        let (line, consumed) = Self::read_line(buf);
        (Self::Integer(line.parse().unwrap()), consumed)
    }

    fn parse_bulk_string(buf: &[u8]) -> (Self, usize) {
        let (header, header_len) = Self::read_line(buf);
        let length: isize = header.parse().unwrap();

        if length == -1 {
            return (Self::BulkString(None), header_len);
        }

        let length = length as usize;
        let data = std::str::from_utf8(&buf[header_len..header_len + length]).unwrap();
        (Self::BulkString(Some(data.to_string())), header_len + length + 2)
    }

    fn parse_array(buf: &[u8]) -> (Self, usize) {
        let (header, header_len) = Self::read_line(buf);
        let count: isize = header.parse().unwrap();

        if count == -1 {
            return (Self::Array(None), header_len);
        }

        let mut items = Vec::with_capacity(count as usize);
        let mut consumed = header_len;

        for _ in 0..count {
            let (value, len) = Self::parse(&buf[consumed..]);
            items.push(value);
            consumed += len;
        }

        (Self::Array(Some(items)), consumed)
    }
}
