use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::hash::hincrby::HIncrReq;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::hash::hsetnx::HSetNxReq;
use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn h_del(&self, param: HDelReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_set(&self, param: HSetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_set_nx(&self, param: HSetNxReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_incr(&self, param: HIncrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
