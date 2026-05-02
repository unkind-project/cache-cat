use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, HSetReq};
use crate::raft::types::entry::request::AtomicRequest;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

impl MyCache {
    pub async fn h_set(&self, hset: HSetReq, update: &mut UpdateType<'_>) -> Value {
        let my_value = self.cache.get(&hset.key).await;

        match update {
            UpdateType::None => match my_value {
                None => {
                    let element_len = hset.elements.len();
                    let mut map = HashMap::new();
                    for element in hset.elements {
                        map.insert(element.0, element.1);
                    }
                    let value = MyValue {
                        version: 1,
                        expires_at: 0,
                        data: ValueObject::Hash(Arc::new(Mutex::new(map))),
                    };
                    self.cache.insert(hset.key, value).await;
                    Value::Integer(element_len as i64)
                }
                Some(entry) => match entry.data {
                    ValueObject::Hash(mut value) => {
                        let mut counter = 0;
                        let mut map = value.lock();
                        for element in hset.elements {
                            match map.insert(element.0, element.1) {
                                None => counter += 1,
                                _ => {}
                            }
                        }
                        Value::Integer(counter)
                    }
                    _ => Value::Error("Key exists but is not a hash".to_string()),
                },
            },
            UpdateType::Snapshot(queue) => match my_value {
                None => {
                    let element_len = hset.elements.len();
                    let hset_copy = hset.clone();
                    let mut map = HashMap::new();
                    for element in hset.elements {
                        map.insert(element.0, element.1);
                    }
                    let mut value = MyValue {
                        version: 1,
                        expires_at: 0,
                        data: ValueObject::Hash(Arc::new(Mutex::new(map))),
                    };
                    queue.push(AtomicRequest {
                        version: value.version,
                        request: BaseOperation::HSet(hset_copy),
                    });
                    self.cache.insert(hset.key, value).await;
                    Value::Integer(element_len as i64)
                }
                Some(mut entry) => match entry.data {
                    ValueObject::Hash(mut value) => {
                        entry.version += 1;
                        queue.push(AtomicRequest {
                            version: entry.version,
                            request: BaseOperation::HSet(HSetReq {
                                key: hset.key,
                                elements: hset.elements.clone(),
                            }),
                        });
                        let mut counter = 0;
                        let mut map = value.lock();
                        for element in hset.elements {
                            match map.insert(element.0, element.1) {
                                None => counter += 1,
                                _ => {}
                            }
                        }

                        Value::Integer(counter)
                    }
                    _ => Value::Error("Key exists but is not a hash".to_string()),
                },
            },
            UpdateType::CAS(version) => match my_value {
                None => {
                    let element_len = hset.elements.len();
                    let mut map = HashMap::new();
                    for element in hset.elements {
                        map.insert(element.0, element.1);
                    }
                    let value = MyValue {
                        version: 1,
                        expires_at: 0,
                        data: ValueObject::Hash(Arc::new(Mutex::new(map))),
                    };
                    self.cache.insert(hset.key, value).await;
                    Value::Integer(element_len as i64)
                }
                Some(cas_version) => match cas_version.data {
                    ValueObject::Hash(mut value) => {
                        if cas_version.version != *version - 1 {
                            return Value::Integer(0);
                        }
                        let mut counter = 0;
                        let mut map = value.lock();
                        for element in hset.elements {
                            match map.insert(element.0, element.1) {
                                None => counter += 1,
                                _ => {}
                            }
                        }
                        Value::Integer(counter)
                    }
                    _ => Value::Error("Key exists but is not a hash".to_string()),
                },
            },
        }
    }
}
