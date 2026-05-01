use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, LinkedList, VecDeque};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),

    String(Arc<Vec<u8>>),

    #[serde(with = "mutex_vecdeque_serde")]
    List(Arc<Mutex<VecDeque<Arc<Vec<u8>>>>>),

    ZSet(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(Arc<HashSet<Arc<Vec<u8>>>>),
    Hash(Arc<HashMap<Arc<Vec<u8>>, Arc<Vec<u8>>>>),
}
mod mutex_vecdeque_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(
        data: &Arc<Mutex<VecDeque<Arc<Vec<u8>>>>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let guard = data.lock();
        guard.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Arc<Mutex<VecDeque<Arc<Vec<u8>>>>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = VecDeque::<Arc<Vec<u8>>>::deserialize(deserializer)?;
        Ok(Arc::new(Mutex::new(vec)))
    }
}
