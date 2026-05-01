use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde::de::Deserializer;  // 添加这个导入
use std::collections::{BTreeMap, HashMap, HashSet, LinkedList, VecDeque};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),

    String(Arc<Vec<u8>>),

    #[serde(with = "mutex_vecdeque_serde")]
    List(Arc<Mutex<VecDeque<Arc<Vec<u8>>>>>),
    #[serde(with = "mutex_hashmap_serde")]
    Hash(Arc<Mutex<HashMap<Arc<Vec<u8>>, Arc<Vec<u8>>>>>),

    ZSet(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(Arc<HashSet<Arc<Vec<u8>>>>),
}

// 通用序列化宏
macro_rules! impl_mutex_serde {
    ($mod_name:ident, $inner_type:ty) => {
        mod $mod_name {
            use super::*;
            use serde::{Serialize, Deserialize};  // 添加这个导入
            use serde::de::Deserializer;  // 添加这个导入

            pub fn serialize<S>(
                data: &Arc<Mutex<$inner_type>>,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,  // 使用完整的 trait 路径
            {
                let guard = data.lock();
                guard.serialize(serializer)
            }

            pub fn deserialize<'de, D>(
                deserializer: D,
            ) -> Result<Arc<Mutex<$inner_type>>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = <$inner_type>::deserialize(deserializer)?;
                Ok(Arc::new(Mutex::new(value)))
            }
        }
    };
}

impl_mutex_serde!(mutex_vecdeque_serde, VecDeque<Arc<Vec<u8>>>);
impl_mutex_serde!(mutex_hashmap_serde, HashMap<Arc<Vec<u8>>, Arc<Vec<u8>>>);