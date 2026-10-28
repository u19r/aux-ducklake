use std::collections::BTreeMap;

pub(crate) struct LruCache<K, V> {
    capacity: usize,
    entries: BTreeMap<K, LruCacheEntry<V>>,
    recency: BTreeMap<u64, K>,
    next_sequence: u64,
}

struct LruCacheEntry<V> {
    value: V,
    sequence: u64,
}

impl<K, V> LruCache<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: BTreeMap::new(),
            recency: BTreeMap::new(),
            next_sequence: 0,
        }
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.get(key)?;
        let value = entry.value.clone();
        if self.recency.last_key_value().map(|(sequence, _)| *sequence) != Some(entry.sequence) {
            self.touch(key);
        }
        Some(value)
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }
        let sequence = self.next_recency_sequence();
        if let Some(previous) = self
            .entries
            .insert(key.clone(), LruCacheEntry { value, sequence })
        {
            self.recency.remove(&previous.sequence);
        }
        self.recency.insert(sequence, key);
        while self.entries.len() > self.capacity {
            let (_, oldest) = self
                .recency
                .pop_first()
                .expect("each entry has a recency key");
            self.entries.remove(&oldest);
        }
    }

    pub(crate) fn remove(&mut self, key: &K) {
        if let Some(entry) = self.entries.remove(key) {
            self.recency.remove(&entry.sequence);
        }
    }

    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        self.entries.retain(|key, entry| keep(key, &entry.value));
        self.recency.retain(|_, key| self.entries.contains_key(key));
    }

    #[cfg(test)]
    pub(crate) fn assert_bounded(&self) {
        assert!(self.entries.len() <= self.capacity);
        assert!(self.recency.len() <= self.capacity);
        assert_eq!(self.recency.len(), self.entries.len());
    }

    fn touch(&mut self, key: &K) {
        let sequence = self.next_recency_sequence();
        if let Some(entry) = self.entries.get_mut(key) {
            self.recency.remove(&entry.sequence);
            entry.sequence = sequence;
            self.recency.insert(sequence, key.clone());
        }
    }

    fn next_recency_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        sequence
    }
}
