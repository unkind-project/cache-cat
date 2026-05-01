use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::rpc::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

struct ZAddArgs {
    key: Vec<u8>,
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
    members: Vec<(Vec<u8>, f64)>,
}

pub struct ZAddCommand;

impl ZAddCommand {
    fn parse_args(items: &[Value]) -> Result<ZAddArgs, ProtocolError> {
        // Minimum: ZADD key score member (4 items)
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("zadd"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;

        // Parse flags from items[2..] until we hit a score (number)
        let mut i = 2;
        while i < items.len() {
            let flag = match &items[i] {
                Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
                Value::SimpleString(s) => s.clone(),
                _ => break,
            };

            match flag.to_uppercase().as_str() {
                "NX" => {
                    nx = true;
                    i += 1;
                }
                "XX" => {
                    xx = true;
                    i += 1;
                }
                "GT" => {
                    gt = true;
                    i += 1;
                }
                "LT" => {
                    lt = true;
                    i += 1;
                }
                "CH" => {
                    ch = true;
                    i += 1;
                }
                _ => break,
            }
        }

        if nx && xx {
            return Err(ProtocolError::Custom(
                "ERR XX and NX options at the same time are not compatible",
            ));
        }

        if gt && lt {
            return Err(ProtocolError::Custom(
                "ERR GT and LT options at the same time are not compatible",
            ));
        }

        // Remaining items must be score-member pairs
        let remaining = &items[i..];
        if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("zadd"));
        }

        let mut members = Vec::with_capacity(remaining.len() / 2);
        let mut j = 0;
        while j < remaining.len() {
            let score = match &remaining[j] {
                Value::BulkString(Some(data)) => {
                    let s = String::from_utf8_lossy(data);
                    match s.parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => {
                            return Err(ProtocolError::Custom("ERR value is not a valid float"));
                        }
                    }
                }
                Value::SimpleString(s) => match s.parse::<f64>() {
                    Ok(v) => v,
                    Err(_) => return Err(ProtocolError::Custom("ERR value is not a valid float")),
                },
                _ => return Err(ProtocolError::Custom("ERR value is not a valid float")),
            };

            let member = match &remaining[j + 1] {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("member")),
            };

            members.push((member, score));
            j += 2;
        }

        Ok(ZAddArgs {
            key,
            nx,
            xx,
            gt,
            lt,
            ch,
            members,
        })
    }

    #[allow(dead_code)]
    fn parse_score(data: &[u8]) -> Option<f64> {
        let s = String::from_utf8_lossy(data);
        s.parse::<f64>().ok()
    }
}

#[async_trait]
impl Command for ZAddCommand {
    async fn execute(&self, items: &[Value], server: &RedisServer) -> Result<Value, CacheCatError> {
        let args = Self::parse_args(items)?;
        todo!()
    }
}
