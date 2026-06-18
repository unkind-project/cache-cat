use crate::protocol::zset::zadd::ZAddReq;
use crate::protocol::zset::zrange::ZRangeParams;
use crate::protocol::zset::zrangegetscore::ZRangeByScoreParams;
use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn z_range_by_score(
        &self,
        params: ZRangeByScoreParams,
        db_number: u16,
        read_clock: Option<u64>,
    ) -> Value {
        self.execute_read(params, db_number, read_clock)
    }

    pub fn z_range(&self, params: ZRangeParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(params, db_number, read_clock)
    }
    pub fn z_add(&self, zadd: ZAddReq, update: &mut Update) -> Value {
        self.execute_compute(zadd, update)
    }
}
