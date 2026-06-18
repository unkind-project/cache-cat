use std::fmt;
use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::SetBit;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetBitParams {
    pub key: Bytes,
    pub offset: u64,
    pub value: u8, // 0 或 1
}

impl Display for SetBitParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SetBitParams {{ key: {:?}, offset: {}, value: {} }}",
            self.key, self.offset, self.value
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SetBitReq {
    pub key: Bytes,
    pub offset: u64,
    pub value: u8,
}

impl Display for SetBitReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SetBitReq {{ key: {}, offset: {}, value: {} }}",
            String::from_utf8_lossy(&self.key),
            self.offset,
            self.value
        )
    }
}


impl ComputeCommand for SetBitReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SetBit(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        // 获取字符串表示的字节数组
        let bytes = match &entry.value.data {
            ValueObject::String(data_arc) => (**data_arc).clone(),
            ValueObject::Int(int_value) => {
                // 整数转换为字符串表示
                int_value.to_string().into_bytes()
            }
            _ => {
                return (
                    MochaOperation::Abort,
                    Value::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                );
            }
        };

        let mut bytes = bytes;
        let offset = self.offset;
        let bit_value = self.value & 1; // Ensure only 0 or 1

        // Calculate byte and bit positions
        let byte_index = (offset / 8) as usize;
        let bit_position = 7 - (offset % 8); // Most significant bit first (Redis behavior)

        // Expand the byte array if needed
        if byte_index >= bytes.len() {
            bytes.resize(byte_index + 1, 0);
        }

        // Get the old bit value
        let old_byte = bytes[byte_index];
        let old_bit = (old_byte >> bit_position) & 1;

        // Set the new bit
        if bit_value == 1 {
            bytes[byte_index] = old_byte | (1 << bit_position);
        } else {
            bytes[byte_index] = old_byte & !(1 << bit_position);
        }

        let new_value = MyValue::new(ValueObject::String(Arc::new(bytes)));
        (
            MochaOperation::Insert {
                value: new_value,
                expire: entry.get_expire_policy(),
            },
            Value::Integer(old_bit as i64),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let offset = self.offset;
        let bit_value = self.value & 1;

        // Calculate byte and bit positions
        let byte_index = (offset / 8) as usize;
        let bit_position = 7 - (offset % 8);

        // Create byte array of appropriate size
        let mut bytes = vec![0u8; byte_index + 1];

        // Set the bit
        if bit_value == 1 {
            bytes[byte_index] = 1 << bit_position;
        } else {
            bytes[byte_index] = 0;
        }

        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(Arc::new(bytes))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(0), // Original bit value is 0 for new key
        )
    }
}


pub struct SetBitCommand;

impl SetBitCommand {
    fn parse_args(items: &[Value]) -> Result<SetBitParams, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("setbit"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("setbit")),
        };

        let offset = match &items[2] {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                match s.parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(ProtocolError::Custom(
                            "ERR bit offset is not an integer or out of range",
                        ));
                    }
                }
            }
            Value::SimpleString(s) => match s.parse::<u64>() {
                Ok(v) => v,
                Err(_) => {
                    return Err(ProtocolError::Custom(
                        "ERR bit offset is not an integer or out of range",
                    ));
                }
            },
            Value::Integer(i) => {
                if *i < 0 {
                    return Err(ProtocolError::Custom(
                        "ERR bit offset is not an integer or out of range",
                    ));
                }
                *i as u64
            }
            _ => {
                return Err(ProtocolError::Custom(
                    "ERR bit offset is not an integer or out of range",
                ));
            }
        };

        let value = match &items[3] {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                match s.parse::<u8>() {
                    Ok(v) if v <= 1 => v,
                    _ => {
                        return Err(ProtocolError::Custom(
                            "ERR bit is not an integer or out of range",
                        ));
                    }
                }
            }
            Value::SimpleString(s) => match s.parse::<u8>() {
                Ok(v) if v <= 1 => v,
                _ => {
                    return Err(ProtocolError::Custom(
                        "ERR bit is not an integer or out of range",
                    ));
                }
            },
            Value::Integer(i) => {
                if *i < 0 || *i > 1 {
                    return Err(ProtocolError::Custom(
                        "ERR bit is not an integer or out of range",
                    ));
                }
                *i as u8
            }
            _ => {
                return Err(ProtocolError::Custom(
                    "ERR bit is not an integer or out of range",
                ));
            }
        };

        Ok(SetBitParams {
            key: key.into(),
            offset,
            value,
        })
    }
}

impl RaftCommand for SetBitCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = SetBitCommand::parse_args(items)?;
        Ok(Operation::Base(SetBit(SetBitReq {
            key: params.key,
            offset: params.offset,
            value: params.value,
        })))
    }
}

#[async_trait]
impl Command for SetBitCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("SETBIT")));
        }
        // Build raft operation
        let operation = self.raft_request(items)?; // Execute write
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
