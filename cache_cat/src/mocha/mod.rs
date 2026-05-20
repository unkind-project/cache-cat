use crossbeam_channel::{select, unbounded, Receiver, Sender};
use flurry::{Guard, HashMap};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

const IDLE_EXPIRE_INTERVAL: Duration = Duration::from_secs(10);

const WHEEL_BITS: usize = 8;
const WHEEL_SIZE: usize = 1 << WHEEL_BITS;
const WHEEL_MASK: u64 = (WHEEL_SIZE as u64) - 1;
const WHEEL_LEVELS: usize = 8;
const LARGE_ADVANCE_THRESHOLD: u64 = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Expiry {
    pub at: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpirePolicy {
    Persistent,
    Absolute(u64),
    Ttl(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntrySnapshot<V> {
    pub value: V,
    pub expire_at: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct EntryRef<'a, V> {
    pub value: &'a V,
    pub expire_at: Option<u64>,
}

impl<'a, V> EntryRef<'a, V> {
    pub fn get_expire_policy(&self) -> ExpirePolicy {
        match self.expire_at {
            None => ExpirePolicy::Persistent,
            Some(v) => ExpirePolicy::Absolute(v),
        }
    }
}

#[derive(Clone, Debug)]
struct Entry<V> {
    value: V,
    expire_at: Option<u64>,
}

impl<V> Entry<V> {
    fn snapshot(&self) -> EntrySnapshot<V>
    where
        V: Clone,
    {
        EntrySnapshot {
            value: self.value.clone(),
            expire_at: self.expire_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MochaOperation<V> {
    Insert { value: V, expire: ExpirePolicy },
    Remove,
    Abort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MochaCompute<K, V> {
    Unchanged,
    Inserted(K, EntrySnapshot<V>),
    Updated {
        old: (K, EntrySnapshot<V>),
        new: (K, EntrySnapshot<V>),
    },
    Removed(K, EntrySnapshot<V>),
}

#[derive(Clone, Debug)]
struct ExpireCommand<K> {
    key: K,
    expire_at: u64,
}

#[derive(Clone, Debug)]
struct TimerItem<K> {
    key: K,
    expire_at: u64,
}

#[derive(Debug)]
struct HierarchicalTimeWheel<K> {
    current: u64,
    wheels: Vec<Vec<Vec<TimerItem<K>>>>,
}

impl<K> HierarchicalTimeWheel<K> {
    fn new(now: u64) -> Self {
        let mut wheels = Vec::with_capacity(WHEEL_LEVELS);
        for _ in 0..WHEEL_LEVELS {
            let mut level = Vec::with_capacity(WHEEL_SIZE);
            for _ in 0..WHEEL_SIZE {
                level.push(Vec::new());
            }
            wheels.push(level);
        }

        Self {
            current: now,
            wheels,
        }
    }

    fn insert(&mut self, item: TimerItem<K>) {
        let delta = item.expire_at.saturating_sub(self.current);
        let level = Self::level_for_delta(delta);
        let slot = Self::slot_for(item.expire_at, level);
        self.wheels[level][slot].push(item);
    }

    fn advance_to<F>(&mut self, now: u64, mut expire: F)
    where
        F: FnMut(K, u64),
    {
        if now <= self.current {
            return;
        }

        if now.saturating_sub(self.current) > LARGE_ADVANCE_THRESHOLD {
            self.advance_large(now, expire);
            return;
        }

        while self.current < now {
            self.current += 1;

            for level in 1..WHEEL_LEVELS {
                let lower_mask = (1u64 << (level * WHEEL_BITS)) - 1;
                if self.current & lower_mask == 0 {
                    self.cascade(level);
                } else {
                    break;
                }
            }

            let slot = Self::slot_for(self.current, 0);
            let due = std::mem::take(&mut self.wheels[0][slot]);
            for item in due {
                if item.expire_at <= self.current {
                    expire(item.key, item.expire_at);
                } else {
                    self.insert(item);
                }
            }
        }
    }

    fn advance_large<F>(&mut self, now: u64, mut expire: F)
    where
        F: FnMut(K, u64),
    {
        self.current = now;

        let mut pending = Vec::new();
        for level in 0..WHEEL_LEVELS {
            for slot in 0..WHEEL_SIZE {
                pending.extend(std::mem::take(&mut self.wheels[level][slot]));
            }
        }

        for item in pending {
            if item.expire_at <= now {
                expire(item.key, item.expire_at);
            } else {
                self.insert(item);
            }
        }
    }

    fn cascade(&mut self, level: usize) {
        let slot = Self::slot_for(self.current, level);
        let items = std::mem::take(&mut self.wheels[level][slot]);
        for item in items {
            self.insert(item);
        }
    }

    fn level_for_delta(delta: u64) -> usize {
        if delta < WHEEL_SIZE as u64 {
            return 0;
        }

        let mut level = 1;
        let mut span = WHEEL_SIZE as u64;

        while level + 1 < WHEEL_LEVELS && delta >= span {
            level += 1;
            span <<= WHEEL_BITS;
        }

        level
    }

    fn slot_for(at: u64, level: usize) -> usize {
        ((at >> (level * WHEEL_BITS)) & WHEEL_MASK) as usize
    }
}

#[derive(Clone, Debug)]
pub struct Mocha<K, V>
where
    K: Clone + Eq + Hash + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    map: HashMap<K, Entry<V>>,
    logic_clock: Arc<AtomicU64>,
    expire_tx: Sender<ExpireCommand<K>>,
    expire_rx: Receiver<ExpireCommand<K>>,
}

impl<K, V> Mocha<K, V>
where
    K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(logic_clock: Arc<AtomicU64>) -> Self {
        let (expire_tx, expire_rx) = unbounded();

        Self {
            map: HashMap::new(),
            logic_clock,
            expire_tx,
            expire_rx,
        }
    }

    fn now_logical(&self) -> u64 {
        self.logic_clock.load(Ordering::Relaxed)
    }

    fn resolve_expire_at(&self, policy: ExpirePolicy) -> Option<u64> {
        match policy {
            ExpirePolicy::Persistent => None,
            ExpirePolicy::Absolute(at) => Some(at),
            ExpirePolicy::Ttl(ttl) => Some(self.now_logical().saturating_add(ttl)),
        }
    }

    fn make_entry(&self, value: V, policy: ExpirePolicy) -> Entry<V> {
        Entry {
            value,
            expire_at: self.resolve_expire_at(policy),
        }
    }

    fn enqueue_expiry(&self, key: K, expire_at: Option<u64>) {
        if let Some(expire_at) = expire_at {
            let _ = self.expire_tx.send(ExpireCommand { key, expire_at });
        }
    }

    fn write_entry(&self, key: K, value: V, policy: ExpirePolicy) -> EntrySnapshot<V> {
        let new_entry = self.make_entry(value, policy);
        let snapshot = new_entry.snapshot();
        let expire_at = new_entry.expire_at;

        {
            let mg = self.map.pin();
            mg.insert(key.clone(), new_entry);
        }

        self.enqueue_expiry(key, expire_at);
        snapshot
    }

    fn remove_expired_if_current(&self, key: K, expire_at: u64) -> bool {
        let now = self.now_logical();
        let mut removed = false;

        {
            let mg = self.map.pin();
            mg.compute_if_present(&key, |_, entry| {
                if entry.expire_at == Some(expire_at) && now >= expire_at {
                    removed = true;
                    None
                } else {
                    Some(entry.clone())
                }
            });
        }

        removed
    }

    pub fn insert_snapshot(&self, key: K, snapshot: EntrySnapshot<V>) -> EntrySnapshot<V> {
        let policy = match snapshot.expire_at {
            None => ExpirePolicy::Persistent,
            Some(at) => ExpirePolicy::Absolute(at),
        };

        self.write_entry(key, snapshot.value, policy)
    }

    pub fn insert(&self, key: K, value: V, ttl: u64) -> EntrySnapshot<V> {
        self.write_entry(key, value, ExpirePolicy::Ttl(ttl))
    }

    pub fn insert_absolute(&self, key: K, value: V, expire_at: u64) -> EntrySnapshot<V> {
        self.write_entry(key, value, ExpirePolicy::Absolute(expire_at))
    }

    pub fn insert_persistent(&self, key: K, value: V) -> EntrySnapshot<V> {
        self.write_entry(key, value, ExpirePolicy::Persistent)
    }

    pub fn try_insert(
        &self,
        key: K,
        value: V,
        expire: ExpirePolicy,
    ) -> Result<EntrySnapshot<V>, V> {
        let new_entry = self.make_entry(value, expire);
        let snapshot = new_entry.snapshot();
        let expire_at = new_entry.expire_at;

        let result = {
            let mg = self.map.pin();
            mg.try_insert(key.clone(), new_entry)
                .map(|_| ())
                .map_err(|err| err.not_inserted.value)
        };

        match result {
            Ok(()) => {
                self.enqueue_expiry(key, expire_at);
                Ok(snapshot)
            }
            Err(v) => Err(v),
        }
    }

    pub fn get_entry<Q>(&self, key: &Q) -> Option<EntrySnapshot<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        let now = self.now_logical();
        let mg = self.map.pin();
        // 1. 先读
        let expired_at = match mg.get(key) {
            Some(entry) => match entry.expire_at {
                Some(at) if now >= at => at,
                _ => return Some(entry.snapshot()),
            },
            None => return None,
        };
        // 2. 只有过期才走 compute_if_present 做惰性删除
        mg.compute_if_present(key, |_, entry| {
            if entry.expire_at == Some(expired_at) && now >= expired_at {
                None
            } else {
                Some(entry.clone())
            }
        });
        None
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.get_entry(key).map(|entry| entry.value)
    }

    pub fn get_if_alive(&self, key: &K) -> Option<V> {
        self.get_entry(key).map(|entry| entry.value)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.get_entry(key).is_some()
    }

    pub fn ttl_remaining(&self, key: &K) -> Option<u64> {
        let entry = self.get_entry(key)?;
        let expire_at = entry.expire_at?;
        Some(expire_at.saturating_sub(self.now_logical()))
    }

    fn set_expire_policy(&self, key: &K, policy: ExpirePolicy) -> Option<EntrySnapshot<V>> {
        let now = self.now_logical();
        let mut snapshot = None;
        let mut new_expire_at = None;

        {
            let mg = self.map.pin();
            mg.compute_if_present(key, |_, entry| {
                if entry.expire_at.is_some_and(|at| now >= at) {
                    None
                } else {
                    let new_entry = self.make_entry(entry.value.clone(), policy);
                    snapshot = Some(new_entry.snapshot());
                    new_expire_at = new_entry.expire_at;
                    Some(new_entry)
                }
            });
        }

        if snapshot.is_some() {
            self.enqueue_expiry(key.clone(), new_expire_at);
        }

        snapshot
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.remove_entry(key).map(|entry| entry.value)
    }

    pub fn remove_entry(&self, key: &K) -> Option<EntrySnapshot<V>> {
        let now = self.now_logical();

        let mg = self.map.pin();
        let removed = mg.remove(key)?;
        let expired = removed.expire_at.is_some_and(|at| now >= at);

        if expired {
            None
        } else {
            Some(removed.snapshot())
        }
    }

    pub fn update_or_insert_with<U, F>(
        &self,
        key: K,
        update: U,
        insert: F,
        expire: ExpirePolicy,
    ) -> EntrySnapshot<V>
    where
        U: Fn(&V) -> V,
        F: Fn() -> V,
    {
        loop {
            let now = self.now_logical();
            let mut snapshot = None;
            let mut new_expire_at = None;

            {
                let mg = self.map.pin();

                let touched = mg
                    .compute_if_present(&key, |_, entry| {
                        let expired = entry.expire_at.is_some_and(|at| now >= at);
                        let value = if expired {
                            insert()
                        } else {
                            update(&entry.value)
                        };

                        let new_entry = self.make_entry(value, expire);
                        snapshot = Some(new_entry.snapshot());
                        new_expire_at = new_entry.expire_at;
                        Some(new_entry)
                    })
                    .is_some();

                if touched {
                    drop(mg);
                    self.enqueue_expiry(key, new_expire_at);
                    return snapshot.unwrap();
                }

                let new_entry = self.make_entry(insert(), expire);
                let snap = new_entry.snapshot();
                let expire_at = new_entry.expire_at;

                if mg.try_insert(key.clone(), new_entry).is_ok() {
                    drop(mg);
                    self.enqueue_expiry(key, expire_at);
                    return snap;
                }
            }
        }
    }

    pub fn compute<F>(&self, key: K, f: F) -> MochaCompute<K, V>
    where
        F: for<'a> FnOnce(Option<EntryRef<'a, V>>) -> MochaOperation<V>,
    {
        let now = self.now_logical();
        let mut f_holder = Some(f);
        let mut result = None;
        let mut new_expire_at = None;

        {
            let mg = self.map.pin();

            mg.compute_if_present(&key, |k, entry| {
                if entry.expire_at.is_some_and(|at| now >= at) {
                    return None;
                }

                let user_f = f_holder
                    .take()
                    .expect("compute closure must be invoked at most once");

                let old_snapshot = entry.snapshot();

                match user_f(Some(EntryRef {
                    value: &entry.value,
                    expire_at: entry.expire_at,
                })) {
                    MochaOperation::Insert { value, expire } => {
                        let new_entry = self.make_entry(value, expire);
                        let new_snapshot = new_entry.snapshot();
                        new_expire_at = new_entry.expire_at;

                        result = Some(MochaCompute::Updated {
                            old: (k.clone(), old_snapshot),
                            new: (k.clone(), new_snapshot),
                        });

                        Some(new_entry)
                    }
                    MochaOperation::Remove => {
                        result = Some(MochaCompute::Removed(k.clone(), old_snapshot));
                        None
                    }
                    MochaOperation::Abort => {
                        result = Some(MochaCompute::Unchanged);
                        Some(entry.clone())
                    }
                }
            });
        }

        if let Some(user_f) = f_holder.take() {
            match user_f(None) {
                MochaOperation::Insert { value, expire } => {
                    let new_entry = self.make_entry(value, expire);
                    let new_snapshot = new_entry.snapshot();
                    let expire_at = new_entry.expire_at;

                    let inserted = {
                        let mg = self.map.pin();
                        mg.try_insert(key.clone(), new_entry).is_ok()
                    };

                    if inserted {
                        self.enqueue_expiry(key.clone(), expire_at);
                        return MochaCompute::Inserted(key, new_snapshot);
                    }

                    return MochaCompute::Unchanged;
                }
                MochaOperation::Remove | MochaOperation::Abort => {
                    return MochaCompute::Unchanged;
                }
            }
        }

        if let Some(result) = result {
            if matches!(result, MochaCompute::Updated { .. }) {
                self.enqueue_expiry(key, new_expire_at);
            }

            result
        } else {
            MochaCompute::Unchanged
        }
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&K, EntryRef<'_, V>),
    {
        let guard = self.map.guard();
        let now = self.now_logical();

        for (k, entry) in self.map.iter(&guard) {
            if entry.expire_at.is_some_and(|at| now >= at) {
                continue;
            }

            f(
                k,
                EntryRef {
                    value: &entry.value,
                    expire_at: entry.expire_at,
                },
            );
        }
    }

    pub fn spawn_active_expirer(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _ = tokio::task::spawn_blocking(move || {
                self.expire_worker();
            })
                .await;
        })
    }

    fn expire_worker(self: Arc<Self>) {
        let rx = self.expire_rx.clone();
        let mut wheel = HierarchicalTimeWheel::new(self.now_logical());

        loop {
            let timeout = crossbeam_channel::after(IDLE_EXPIRE_INTERVAL);

            select! {
                recv(rx) -> msg => {
                    let Ok(cmd) = msg else {
                        return;
                    };

                    self.advance_and_schedule(&mut wheel, cmd);

                    while let Ok(cmd) = rx.try_recv() {
                        self.schedule_or_expire(&mut wheel, cmd);
                    }
                }

                recv(timeout) -> _ => {
                    let now = self.now_logical();
                    wheel.advance_to(now, |key, expire_at| {
                        self.remove_expired_if_current(key, expire_at);
                    });
                }
            }
        }
    }

    fn advance_and_schedule(
        &self,
        wheel: &mut HierarchicalTimeWheel<K>,
        cmd: ExpireCommand<K>,
    ) {
        let now = self.now_logical();

        wheel.advance_to(now, |key, expire_at| {
            self.remove_expired_if_current(key, expire_at);
        });

        self.schedule_or_expire_at(wheel, cmd, now);
    }

    fn schedule_or_expire(&self, wheel: &mut HierarchicalTimeWheel<K>, cmd: ExpireCommand<K>) {
        let now = self.now_logical();
        self.schedule_or_expire_at(wheel, cmd, now);
    }

    fn schedule_or_expire_at(
        &self,
        wheel: &mut HierarchicalTimeWheel<K>,
        cmd: ExpireCommand<K>,
        now: u64,
    ) {
        if cmd.expire_at <= now {
            self.remove_expired_if_current(cmd.key, cmd.expire_at);
        } else {
            wheel.insert(TimerItem {
                key: cmd.key,
                expire_at: cmd.expire_at,
            });
        }
    }

    pub async fn active_expire_cycle(&self) {
        let now = self.now_logical();
        let guard = self.map.guard();

        let expired: Vec<(K, u64)> = self
            .map
            .iter(&guard)
            .filter_map(|(k, entry)| {
                let expire_at = entry.expire_at?;
                if now >= expire_at {
                    Some((k.clone(), expire_at))
                } else {
                    None
                }
            })
            .collect();

        drop(guard);

        for (key, expire_at) in expired {
            self.remove_expired_if_current(key, expire_at);
        }
    }

    pub fn guard(&self) -> Guard<'_> {
        self.map.guard()
    }

    pub fn iter<'g>(
        &'g self,
        guard: &'g Guard<'_>,
    ) -> impl Iterator<Item = (&'g K, EntryRef<'g, V>)> + 'g {
        let now = self.now_logical();

        self.map.iter(guard).filter_map(move |(k, entry)| {
            if entry.expire_at.is_some_and(|at| now >= at) {
                None
            } else {
                Some((
                    k,
                    EntryRef {
                        value: &entry.value,
                        expire_at: entry.expire_at,
                    },
                ))
            }
        })
    }

    pub fn keys<'g>(&'g self, guard: &'g Guard<'_>) -> impl Iterator<Item = &'g K> + 'g {
        self.iter(guard).map(|(k, _)| k)
    }

    pub fn iter_snapshots<'g>(
        &'g self,
        guard: &'g Guard<'_>,
    ) -> impl Iterator<Item = (K, EntrySnapshot<V>)> + 'g {
        let now = self.now_logical();

        self.map.iter(guard).filter_map(move |(k, entry)| {
            if entry.expire_at.is_some_and(|at| now >= at) {
                None
            } else {
                Some((k.clone(), entry.snapshot()))
            }
        })
    }

    pub fn for_each_snapshot<F>(&self, mut f: F)
    where
        F: FnMut(&K, EntrySnapshot<V>),
    {
        let guard = self.map.guard();
        let now = self.now_logical();

        for (k, entry) in self.map.iter(&guard) {
            if entry.expire_at.is_some_and(|at| now >= at) {
                continue;
            }

            f(k, entry.snapshot());
        }
    }

    pub fn clear(&self) -> usize {
        let mg = self.map.pin();
        let count = mg.len();
        mg.clear();
        count
    }
}