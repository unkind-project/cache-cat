use crate::protocol::command::CommandFactory;
use crate::protocol::resp::Parser;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::CacheCatApp;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};
pub struct RedisServer {
    pub(crate) app: Arc<CacheCatApp>,
    pub redis_addr: String,
    pub cmd_factory: Arc<CommandFactory>,
}
impl RedisServer {
    async fn process_command(&self, value: Value) -> Value {
        self.cmd_factory.execute(value, self).await
    }
    /// Handle a single client connection
    async fn handle_connection(
        self: Arc<Self>,
        mut stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> IoResult<()> {
        let mut buffer = vec![0u8; 8192]; // 8KB buffer
        let mut pending = Vec::new(); // Buffer for incomplete commands

        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => {
                    info!("Connection closed by client: {}", peer_addr);
                    break;
                }
                Ok(n) => {
                    // Append new data to pending buffer
                    pending.extend_from_slice(&buffer[..n]);

                    // Try to parse and process complete commands
                    let mut processed = 0;
                    while let Some((value, consumed)) = Parser::parse(&pending[processed..]) {
                        processed += consumed;

                        // Log the parsed command
                        debug!("Received command from {}: {:?}", peer_addr, value);

                        // Process the command and get response
                        let response = self.process_command(value).await;
                        let encoded = response.encode();

                        // Send response
                        if let Err(e) = stream.write_all(&encoded).await {
                            warn!("Failed to write response to {}: {}", peer_addr, e);
                            break;
                        }
                    }

                    // Remove processed data from pending buffer
                    if processed > 0 {
                        pending = pending.split_off(processed);
                    }
                }
                Err(e) => {
                    error!("Error reading from {}: {}", peer_addr, e);
                    break;
                }
            }
        }

        info!("Connection handler ended for {}", peer_addr);
        Ok(())
    }
    pub async fn start_redis_server(self: Arc<Self>) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.redis_addr.clone()).await?;
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("New connection accepted from {}", peer_addr);
                    // Clone the Arc<Server> for the new connection
                    let server = Arc::clone(&self);
                    // Spawn an independent task for each connection
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(stream, peer_addr).await {
                            error!("Error handling connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}
