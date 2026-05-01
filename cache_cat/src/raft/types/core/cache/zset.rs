use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{SetReq, ZAddReq};
use ordered_float::OrderedFloat;

impl MyCache {
    pub async fn z_add(&self, zadd: ZAddReq, update: &mut UpdateType<'_>) -> Value {
        todo!()
    }
}
