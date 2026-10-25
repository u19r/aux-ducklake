use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex},
};

use crate::{CatalogError, CatalogResult};

pub(super) struct ByteBoundedCache<K, V> {
    max_bytes: usize,
    state: Mutex<ByteBoundedCacheState<K, V>>,
}

struct ByteBoundedCacheState<K, V> {
    entries: BTreeMap<K, ByteBoundedCacheEntry<V>>,
    next_generation: u64,
    total_bytes: usize,
}

struct ByteBoundedCacheEntry<V> {
    value: Arc<V>,
    weight: usize,
    generation: u64,
    pinned: bool,
}

pub(super) struct CaptureFlightRegistry<K, V> {
    flights: Mutex<BTreeMap<K, Arc<CaptureFlight<V>>>>,
}

pub(super) struct CaptureFlight<V> {
    result: Mutex<Option<CatalogResult<Arc<V>>>>,
    ready: Condvar,
}

impl<K, V> CaptureFlightRegistry<K, V>
where
    K: Clone + Ord,
{
    pub(super) fn new() -> Self {
        Self {
            flights: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) fn get_or_build<F>(&self, key: K, builder: F) -> CatalogResult<Arc<V>>
    where
        F: FnOnce() -> CatalogResult<Arc<V>>,
    {
        let (flight, is_builder) = self.start(key.clone())?;

        if !is_builder {
            return flight.wait();
        }

        let result = builder();
        self.complete(&key, &flight, result)
    }

    pub(super) fn start(&self, key: K) -> CatalogResult<(Arc<CaptureFlight<V>>, bool)> {
        let mut flights = self.flights.lock().map_err(|_| {
            CatalogError::Backend("inline read snapshot flight registry is poisoned".to_owned())
        })?;
        if let Some(flight) = flights.get(&key) {
            return Ok((Arc::clone(flight), false));
        }
        let flight = Arc::new(CaptureFlight {
            result: Mutex::new(None),
            ready: Condvar::new(),
        });
        flights.insert(key, Arc::clone(&flight));
        Ok((flight, true))
    }

    pub(super) fn complete(
        &self,
        key: &K,
        flight: &Arc<CaptureFlight<V>>,
        result: CatalogResult<Arc<V>>,
    ) -> CatalogResult<Arc<V>> {
        let waiter_result = result.clone();
        flight.store_result(waiter_result);
        if let Ok(mut flights) = self.flights.lock()
            && flights
                .get(key)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, flight))
        {
            flights.remove(key);
        }
        // Publish the result before releasing waiters. Removing the slot first means a
        // concurrent retry after a failed build starts a fresh builder instead of
        // attaching to the completed failure, while existing waiters still own
        // the flight Arc.
        flight.notify_waiters();
        result
    }
}

impl<V> CaptureFlight<V> {
    pub(super) fn wait(&self) -> CatalogResult<Arc<V>> {
        let mut result = self.result.lock().map_err(|_| {
            CatalogError::Backend("inline read snapshot flight is poisoned".to_owned())
        })?;
        loop {
            if let Some(result) = result.as_ref() {
                return result.clone();
            }
            result = self.ready.wait(result).map_err(|_| {
                CatalogError::Backend("inline read snapshot flight is poisoned".to_owned())
            })?;
        }
    }

    fn store_result(&self, result: CatalogResult<Arc<V>>) {
        if let Ok(mut state) = self.result.lock() {
            *state = Some(result);
        }
    }

    fn notify_waiters(&self) {
        self.ready.notify_all();
    }
}

impl<K, V> ByteBoundedCache<K, V>
where
    K: Clone + Ord,
{
    pub(super) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(ByteBoundedCacheState {
                entries: BTreeMap::new(),
                next_generation: 0,
                total_bytes: 0,
            }),
        }
    }

    pub(super) fn get(&self, key: &K) -> Option<Arc<V>> {
        let mut state = self.state.lock().ok()?;
        state.next_generation = state.next_generation.wrapping_add(1);
        let generation = state.next_generation;
        let entry = state.entries.get_mut(key)?;
        entry.generation = generation;
        Some(Arc::clone(&entry.value))
    }

    pub(super) fn contains(&self, key: &K) -> bool {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return false,
        };
        state.next_generation = state.next_generation.wrapping_add(1);
        let generation = state.next_generation;
        if let Some(entry) = state.entries.get_mut(key) {
            entry.generation = generation;
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    pub(super) fn insert(&self, key: K, value: V, weight: usize, pinned: bool) -> bool {
        self.insert_shared(key, Arc::new(value), weight, pinned)
    }

    pub(super) fn insert_shared(&self, key: K, value: Arc<V>, weight: usize, pinned: bool) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if weight > self.max_bytes {
            return false;
        }
        let previous_weight = state.entries.get(&key).map_or(0, |entry| entry.weight);
        while state
            .total_bytes
            .saturating_sub(previous_weight)
            .saturating_add(weight)
            > self.max_bytes
        {
            if !state.evict_oldest_unpinned(&key) {
                return false;
            }
        }
        if let Some(previous) = state.entries.remove(&key) {
            state.total_bytes = state.total_bytes.saturating_sub(previous.weight);
        }
        state.next_generation = state.next_generation.wrapping_add(1);
        let generation = state.next_generation;
        state.total_bytes = state.total_bytes.saturating_add(weight);
        state.entries.insert(
            key,
            ByteBoundedCacheEntry {
                value,
                weight,
                generation,
                pinned,
            },
        );
        true
    }

    pub(super) fn retain(&self, mut keep: impl FnMut(&K) -> bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let removed = state
            .entries
            .keys()
            .filter(|key| !keep(key))
            .cloned()
            .collect::<Vec<_>>();
        for key in removed {
            if let Some(entry) = state.entries.remove(&key) {
                state.total_bytes = state.total_bytes.saturating_sub(entry.weight);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.state.lock().map_or(0, |state| state.entries.len())
    }

    #[cfg(test)]
    pub(super) fn total_bytes(&self) -> usize {
        self.state.lock().map_or(0, |state| state.total_bytes)
    }
}

impl<K: Clone + Ord, V> ByteBoundedCacheState<K, V> {
    fn evict_oldest_unpinned(&mut self, retained_key: &K) -> bool {
        let key = self
            .entries
            .iter()
            .filter(|(key, entry)| !entry.pinned && *key != retained_key)
            .min_by_key(|(_, entry)| entry.generation)
            .map(|(key, _)| key.clone());
        let Some(entry) = key.and_then(|key| self.entries.remove(&key)) else {
            return false;
        };
        self.total_bytes = self.total_bytes.saturating_sub(entry.weight);
        true
    }
}
