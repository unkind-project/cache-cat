use crate::error::ProtocolError;
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::set::smembers::SMembersParams;
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, SAddReq};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

impl ComputeCommand for SAddReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SAdd(self.clone())
    }

    fn mutate(self, data: &mut MyValue) -> (bool, Value) {
        if let ValueObject::Set(map_arc) = &data.data {
            let mut count = 0;
            let mut map = map_arc.lock();
            for v in &self.elements {
                if map.insert(v.clone()) {
                    count += 1;
                }
            }
            // 返回 true 表示数据已变动，需要更新缓存
            (true, Value::Integer(count))
        } else {
            (
                false,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            )
        }
    }

    fn init(self) -> (ValueObject, Value) {
        let mut set = HashSet::new();
        let len = self.elements.len();
        for v in self.elements {
            set.insert(v);
        }
        (
            ValueObject::Set(Arc::new(Mutex::new(set))),
            Value::Integer(len as i64),
        )
    }
}

impl MyCache {
    pub fn s_member(&self, param: SMembersParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        match cache.get(&param.key) {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.data {
                ValueObject::Set(set) => {
                    let guard = set.lock();
                    let mut array = Vec::new();
                    for member in guard.iter() {
                        array.push(Value::BulkString(Some(member.as_ref().clone())));
                    }
                    Value::Array(Some(array))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }

    pub fn s_add(&self, sadd: SAddReq, update: &mut Update) -> Value {
        self.execute_compute(sadd, update)
    }
}
