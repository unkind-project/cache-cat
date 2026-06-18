use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::hash::hgetall::HGetAllParams;
use crate::protocol::hash::hincrby::HIncrReq;
use crate::protocol::hash::hkeys::HKeysParams;
use crate::protocol::hash::hmget::HMGetParams;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::hash::hvals::HValsParams;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use bytes::Bytes;

impl ReadCommand for HValsParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();

                    let mut result = Vec::with_capacity(guard.len());

                    for value in guard.values() {
                        let value_bytes = match value {
                            HashValue::Str(str) => str.as_ref().clone(),
                            HashValue::Int(int) => int.to_string().into_bytes(),
                        };

                        result.push(Value::BulkString(Some(value_bytes)));
                    }

                    Value::Array(Some(result))
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl ReadCommand for HGetAllParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Map(Vec::new()),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let mut result = Vec::with_capacity(guard.len());
                    for (field, value) in guard.iter() {
                        let value_bytes = match value {
                            HashValue::Str(str) => str.as_ref().clone(),
                            HashValue::Int(int) => int.to_string().into_bytes(),
                        };
                        result.push((
                            Value::BulkString(Some(field.as_ref().clone())),
                            Value::BulkString(Some(value_bytes)),
                        ));
                    }
                    Value::Map(result)
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl ReadCommand for HGetParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let option = guard.get(&self.field);
                    match option {
                        None => Value::BulkString(None),
                        Some(value) => match value {
                            HashValue::Str(str) => Value::BulkString(Some(str.as_ref().clone())),
                            HashValue::Int(int) => {
                                Value::BulkString(Some(int.to_string().as_bytes().to_vec()))
                            }
                        },
                    }
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}
impl MyCache {
    pub fn h_m_get(&self, param: HMGetParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_keys(&self, param: HKeysParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_vals(&self, param: HValsParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_get_all(
        &self,
        param: HGetAllParams,
        db_number: u16,
        read_clock: Option<u64>,
    ) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_get(&self, param: HGetParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }
    pub fn h_del(&self, param: HDelReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn h_set(&self, param: HSetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_incr(&self, param: HIncrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
