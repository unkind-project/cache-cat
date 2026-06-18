//! RPUSH command implementation
//!
//! RPUSH key element [element ...]
//! Insert all the specified values at the tail of the list stored at key.
//! If key does not exist, it is created as empty list before performing the push.
//! When key holds a value that is not a list, an error is returned.
//!
//! Returns:
//! - The length of the list after the push operations (integer reply)
//!
//! Note: Elements are inserted one after the other from leftmost to rightmost.
//! `RPUSH mylist a b c` results in `[a, b, c]`.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::RPush;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::lock_api::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

/// RPUSH command handler
pub struct RPushCommand;

impl RPushCommand {
    /// Parse arguments from RESP items
    /// Format: RPUSH key element [element ...]
    fn parse_args(items: &[Value]) -> Result<RPushArgs, ProtocolError> {
        // Minimum: RPUSH key element
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("rpush"));
        }

        // Parse key
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse elements
        let mut elements = Vec::with_capacity(items.len() - 2);

        for item in &items[2..] {
            let elem = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("element")),
            };

            elements.push(elem);
        }

        Ok(RPushArgs {
            key: key.into(),
            elements,
        })
    }
}

/// Parsed RPUSH arguments
struct RPushArgs {
    key: Bytes,
    elements: Vec<Vec<u8>>,
}

impl RaftCommand for RPushCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        let mut elements = Vec::new();
        for v in params.elements {
            elements.push(Arc::new(v));
        }

        Ok(Operation::Base(RPush(RPushReq {
            key: params.key,
            elements,
        })))
    }
}

#[async_trait]
impl Command for RPushCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // MULTI transaction support
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        // Execute through Raft
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPushReq {
    pub key: Bytes,
    pub elements: Vec<Arc<Vec<u8>>>,
}

impl Display for RPushReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RPushReq {{ key: {}, elements: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

impl ComputeCommand for RPushReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::RPush(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let len = {
                    let mut list = data_arc.lock();
                    for element in self.elements {
                        list.push_back(element);
                    }
                    list.len() as i64
                };
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(len),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error("Key exists but is not a List".to_string()),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let deque: VecDeque<_> = VecDeque::from(self.elements);
        let len = deque.len() as i64;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::List(Arc::new(Mutex::new(deque)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len),
        )
    }
}
