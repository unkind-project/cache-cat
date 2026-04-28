use crate::protocol::key::expire::ExpireCondition;
use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::{BaseOperation, DelReq, ExpireReq};
use crate::raft::types::entry::request::AtomicRequest;
use std::sync::Arc;

impl MyCache {
    pub async fn expire(&self, expire_req: ExpireReq, update: &mut UpdateType<'_>) -> Value {
        let mut v = match self.cache.get(&expire_req.key).await {
            Some(v) => v,
            None => return Value::Integer(0),
        };
        let should_update = match expire_req.condition {
            None => true,
            Some(ref condition) => match condition {
                ExpireCondition::Nx => v.expires_at == 0,
                ExpireCondition::Xx => v.expires_at != 0,
                ExpireCondition::Gt => v.expires_at != 0 && v.expires_at <= expire_req.expires_at,
                ExpireCondition::Lt => v.expires_at != 0 && v.expires_at >= expire_req.expires_at,
            },
        };
        if !should_update {
            return Value::Integer(0);
        }

        v.expires_at = expire_req.expires_at;
        match update {
            UpdateType::None => {
                self.cache.insert(expire_req.key, v).await;
            }
            UpdateType::Snapshot(queue) => {
                let key = expire_req.key.clone();
                v.version = v.version + 1;
                queue.push(AtomicRequest {
                    version: v.version,
                    request: BaseOperation::Expire(expire_req),
                });

                self.cache.insert(key, v).await;
            }
            UpdateType::CAS(version) => {
                if *version == v.version {
                    self.cache.insert(expire_req.key, v).await;
                }
            }
        }
        Value::Integer(1)
    }

    pub async fn del(&self, del_req: DelReq, update: &mut UpdateType<'_>) -> Value {
        let keys = (*del_req.keys).clone();
        let mut deleted = 0;

        match update {
            UpdateType::None => {
                for key in keys {
                    let existed = self.cache.remove(&key).await;
                    if existed.is_some() {
                        deleted += 1;
                    }
                }
                Value::Integer(deleted)
            }

            UpdateType::Snapshot(queue) => {
                for key in keys {
                    // 计算 version
                    let version = if let Some(entry) = self.cache.get(&key).await {
                        entry.version + 1
                    } else {
                        0
                    };

                    queue.push(AtomicRequest {
                        version,
                        request: BaseOperation::Del(DelReq {
                            keys: Arc::from(vec![key.clone()]), // 保持单 key 语义
                        }),
                    });

                    let existed = self.cache.remove(&key).await;
                    if existed.is_some() {
                        deleted += 1;
                    }
                }
                Value::Integer(deleted)
            }

            UpdateType::CAS(version) => {
                for key in keys {
                    if let Some(entry) = self.cache.get(&key).await {
                        if entry.version == *version {
                            self.cache.remove(&key).await;
                            deleted += 1;
                        }
                    }
                }
                Value::Integer(deleted)
            }
        }
    }
}
