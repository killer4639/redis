mod command;
mod memory;
mod resp;

use std::sync::Arc;

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::command::Command;
use crate::memory::Store;
use crate::resp::RespValue;

/// Handles a single client connection.
/// Reads commands in a loop until the client disconnects.
async fn handle_connection(mut socket: TcpStream, store: Arc<Store>) {
    let peer = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("unknown".into());
    println!("Client connected: {peer}");

    let mut buf = [0u8; 512];
    loop {
        let bytes_read = match socket.read(&mut buf).await {
            Ok(0) => break,  // client disconnected
            Ok(n) => n,
            Err(_) => break, // read error, drop connection
        };

        let response = process_request(&buf[..bytes_read], &store);
        if socket
            .write_all(response.to_string().as_bytes())
            .await
            .is_err()
        {
            break; // write error, drop connection
        }
    }

    println!("Client disconnected: {peer}");
}

/// Parses and executes a single RESP request.
fn process_request(buf: &[u8], store: &Store) -> RespValue {
    let request = RespValue::deserialize(buf);

    let RespValue::Array(Some(items)) = request else {
        return RespValue::err("invalid command format");
    };

    if items.is_empty() {
        return RespValue::err("empty command");
    }

    let Some(command_name) = items[0].as_str() else {
        return RespValue::err("invalid command name");
    };

    let Some(command) = Command::parse(command_name) else {
        return RespValue::err(&format!("unknown command '{command_name}'"));
    };

    // items[0] is the command name, the rest are arguments
    command.execute(&items[1..], store)
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "0.0.0.0:6380";
    let listener = TcpListener::bind(addr).await?;
    println!("Redis server listening on {addr}");

    let store = Arc::new(Store::new());

    loop {
        let (socket, _) = listener.accept().await?;
        let store = store.clone();
        tokio::spawn(handle_connection(socket, store));
    }
}
