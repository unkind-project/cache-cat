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
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_i64;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parameters for DECRBY command
#[derive(Debug, Clone, PartialEq)]
pub struct DecrByParams {
    pub key: Bytes,
    pub decrement: i64,
}

impl DecrByParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("DECRBY"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let decrement = items[2]
            .parse_i64()
            .ok_or(ProtocolError::InvalidArgument("decrement"))?;

        Ok(DecrByParams { key, decrement })
    }
}

/// DECRBY command executor
pub struct DecrByCommand;

impl RaftCommand for DecrByCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = DecrByParams::parse(items)?;
        Ok(Operation::Base(BaseOperation::DecrBy(DecrByReq {
            key: params.key,
            decrement: params.decrement,
        })))
    }
}

#[async_trait]
impl Command for DecrByCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DecrByReq {
    pub key: Bytes,
    pub decrement: i64,
}

impl fmt::Display for DecrByReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DecrByReq {{ key: {}, decrement: {} }}",
            String::from_utf8_lossy(&self.key),
            self.decrement
        )
    }
}

impl ComputeCommand for DecrByReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::DecrBy(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (result, value) = match &entry.value.data {
            ValueObject::Int(n) => {
                let num = n - self.decrement;
                (ValueObject::Int(num), Value::Integer(num))
            }

            ValueObject::String(s) => {
                let Some(mut value) = parse_i64(s) else {
                    return (
                        MochaOperation::Abort,
                        Value::Error("Value is not an integer".to_string()),
                    );
                };
                value -= self.decrement;
                (ValueObject::Int(value), Value::Integer(value))
            }

            _ => {
                return (
                    MochaOperation::Abort,
                    Value::Error("Key exists but is not an Integer".to_string()),
                );
            }
        };
        (
            MochaOperation::Insert {
                value: MyValue::new(result),
                expire: entry.get_expire_policy(),
            },
            value,
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let v = -self.decrement;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Int(v)),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(v),
        )
    }
}