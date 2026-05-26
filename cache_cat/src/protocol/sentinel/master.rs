use crate::error::CacheCatError;
use crate::protocol::command::Client;
use crate::protocol::sentinel::sentinel::SubCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::Node;
use async_trait::async_trait;
use openraft::async_runtime::WatchReceiver;

// SENTINEL MASTERS
pub struct SentinelMastersCommand;

#[async_trait]
impl SubCommand for SentinelMastersCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        _items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // for (node_id,node) in server.app.raft.metrics().borrow_watched().membership_config.nodes() {
        //
        // }
        //
        // let masters = server.sentinel.get_masters().await?;
        //
        // let mut result = Vec::new();
        // for master in masters {
        //     let mut master_info = Vec::new();
        //     master_info.push(Value::BulkString(Some(b"name".to_vec())));
        //     master_info.push(Value::BulkString(Some(master.name.into_bytes())));
        //     master_info.push(Value::BulkString(Some(b"ip".to_vec())));
        //     master_info.push(Value::BulkString(Some(master.ip.into_bytes())));
        //     master_info.push(Value::BulkString(Some(b"port".to_vec())));
        //     master_info.push(Value::BulkString(Some(
        //         master.port.to_string().into_bytes(),
        //     )));
        //     master_info.push(Value::BulkString(Some(b"flags".to_vec())));
        //     master_info.push(Value::BulkString(Some(master.flags.into_bytes())));
        //     // Add more fields as needed...
        //
        //     result.push(Value::Array(Some(master_info)));
        // }
        //
        // Ok(Value::Array(Some(result)))
        todo!()
    }
}
