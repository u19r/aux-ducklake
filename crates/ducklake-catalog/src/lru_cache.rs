use std::collections::{BTreeMap, VecDeque};

pub(crate) struct LruCache<K, V> {
    capacity: usize,
    entries: BTreeMap<K, LruCacheEntry<V>>,
    recency: VecDeque<(u64, K)>,
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
            recency: VecDeque::new(),
            next_sequence: 0,
        }
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<V> {
        let value = self.entries.get(key)?.value.clone();
        self.touch(key);
        Some(value)
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }
        let sequence = self.next_recency_sequence();
        self.entries
            .insert(key.clone(), LruCacheEntry { value, sequence });
        self.recency.push_back((sequence, key));
        while self.entries.len() > self.capacity {
            let Some((sequence, oldest)) = self.recency.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&oldest)
                .is_some_and(|entry| entry.sequence == sequence)
            {
                self.entries.remove(&oldest);
            }
        }
    }

    pub(crate) fn remove(&mut self, key: &K) {
        self.entries.remove(key);
    }

    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        self.entries.retain(|key, entry| keep(key, &entry.value));
        self.recency
            .retain(|(_, key)| self.entries.contains_key(key));
    }

    fn touch(&mut self, key: &K) {
        let sequence = self.next_recency_sequence();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.sequence = sequence;
            self.recency.push_back((sequence, key.clone()));
        }
    }

    fn next_recency_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        sequence
    }
}

#[cfg(test)]
mod tests {
    use super::LruCache;

    #[test]
    fn given_capacity_is_exceeded_when_recent_entry_is_touched_then_oldest_entry_is_evicted() {
        let mut cache = LruCache::new(2);

        cache.insert(1, "first");
        cache.insert(2, "second");
        assert_eq!(cache.get(&1), Some("first"));
        cache.insert(3, "third");

        assert_eq!(cache.get(&1), Some("first"));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some("third"));
    }
}
