use crate::protocol::zset::zadd::ZAddReq;
use crate::protocol::zset::zrem::ZRemReq;
use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn z_add(&self, zadd: ZAddReq, update: &mut Update) -> Value {
        self.execute_compute(zadd, update)
    }
    pub fn z_rem(&self, zrem: ZRemReq, update: &mut Update) -> Value {
        self.execute_compute(zrem, update)
    }
}
